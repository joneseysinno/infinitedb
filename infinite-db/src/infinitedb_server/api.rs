//! API request/response types and the request dispatcher.
//!
//! This module defines the protocol-agnostic request/response layer.
//! Transport (TCP, in-process, etc.) is handled by the caller — it
//! deserialises a `Request`, passes it to `dispatch()`, and serialises
//! the resulting `Response`.
//!
//! All operations validate session access before touching any data.

use bincode::{Decode, Encode};
use crate::infinitedb_core::{
    address::{Address, DimensionVector, RevisionId, SpaceId},
    block::Record,
    branch::BranchId,
    hyperedge::{Hyperedge, HyperedgeId},
    merge::{MergeConflict, MergeResult, MergeStrategy},
    query::Query,
    signal::SignalSample,
    snapshot::SnapshotId,
};
use crate::infinitedb_server::session::Session;
use crate::InfiniteDb;

#[cfg(feature = "sync")]
use crate::infinitedb_sync::conflict_queue::resolution_record;

/// Wire representation of a persisted merge conflict.
#[derive(Debug, Clone, Encode, Decode)]
pub struct WireConflict {
    pub id: u64,
    pub target: BranchId,
    pub source: BranchId,
    pub conflict: MergeConflict,
}

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

/// A request from a client to the database.
#[derive(Debug, Encode, Decode)]
pub enum Request {
    /// Read: execute a spatial query against a pinned snapshot.
    Query {
        space: SpaceId,
        snapshot: SnapshotId,
        /// Hilbert key range [min, max]. None = full scan.
        key_range: Option<(u128, u128)>,
        as_of: Option<RevisionId>,
        include_tombstones: bool,
    },
    /// Write: append a new record revision.
    Write {
        address: Address,
        revision: RevisionId,
        /// Bincode-encoded payload.
        data: Vec<u8>,
    },
    /// Delete: append a tombstone revision.
    Delete {
        address: Address,
        revision: RevisionId,
    },
    /// Write: upsert typed hyperedge payload.
    WriteHyperedge {
        space: SpaceId,
        edge: Hyperedge,
        revision: RevisionId,
    },
    /// Delete: tombstone typed hyperedge payload.
    DeleteHyperedge {
        space: SpaceId,
        edge_id: HyperedgeId,
        revision: RevisionId,
    },
    /// Write: append typed signal sample payload.
    WriteSignal {
        space: SpaceId,
        sample: SignalSample,
        revision: RevisionId,
    },
    /// Branch: create a new branch forked from an existing one.
    CreateBranch {
        name: String,
        from_branch: BranchId,
    },
    /// Snapshot: export the current snapshot header for sync.
    GetSnapshot { branch: BranchId },
    /// Merge `source` branch into `target` using `strategy`.
    MergeBranch {
        target: BranchId,
        source: BranchId,
        strategy: MergeStrategy,
    },
    /// List unresolved merge conflicts.
    GetConflicts,
    /// Resolve a stored conflict with chosen payload bytes.
    ResolveConflict { id: u64, data: Vec<u8> },
    /// Ping: liveness check.
    Ping,
}

/// A response from the database to a client.
#[derive(Debug, Encode, Decode)]
pub enum Response {
    /// Records matching a query.
    Records(Vec<Record>),
    /// Confirmation of a successful write or delete.
    WriteAck { revision: RevisionId },
    /// A new branch was created.
    BranchCreated { branch: BranchId },
    /// Snapshot header for sync negotiation.
    Snapshot(SnapshotId),
    /// Merge completed.
    MergeComplete(MergeResult),
    /// Unresolved merge conflicts.
    Conflicts(Vec<WireConflict>),
    /// Conflict resolved.
    ConflictResolved { id: u64 },
    /// Pong.
    Pong,
    /// An error that the client should handle.
    Error(ApiError),
}

/// Structured errors returned to the client.
#[derive(Debug, Encode, Decode)]
pub enum ApiError {
    /// Session lacks required permissions.
    Unauthorised,
    /// Requested space does not exist.
    SpaceNotFound(SpaceId),
    /// Request could not be validated.
    InvalidRequest(String),
    /// Internal failure while handling request.
    Internal(String),
}

// ---------------------------------------------------------------------------
// InfiniteDb handler (Phase D)
// ---------------------------------------------------------------------------

/// Handle one API request against a live [`InfiniteDb`].
pub fn handle_request(db: &InfiniteDb, session: &Session, request: Request) -> Response {
    match request {
        Request::Query {
            space,
            snapshot: _,
            key_range,
            as_of,
            include_tombstones,
        } => {
            if session.access(space).is_none() {
                return Response::Error(ApiError::Unauthorised);
            }
            let rev = as_of.or(Some(session.opened_at));
            let mut records = match db.query_on_branch(session.branch, space, rev) {
                Ok(r) => r,
                Err(e) => return Response::Error(ApiError::Internal(e.to_string())),
            };
            if !include_tombstones {
                records.retain(|r| !r.tombstone);
            }
            if let Some((lo, hi)) = key_range {
                let spaces = db.spaces.read();
                records.retain(|r| {
                    let k = crate::engine::query::space_key(&spaces, space, &r.address.point);
                    k >= lo && k <= hi
                });
            }
            Response::Records(records)
        }

        Request::Write { address, revision: _, data } => {
            if !session.can_write(address.space) || !session.can_write_branch(session.branch) {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.insert_on_branch(session.branch, address.space, address.point, data) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(ApiError::Internal(e.to_string())),
            }
        }

        Request::Delete { address, revision: _ } => {
            if !session.can_write(address.space) || !session.can_write_branch(session.branch) {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.delete_on_branch(session.branch, address.space, address.point) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(ApiError::Internal(e.to_string())),
            }
        }

        Request::WriteHyperedge { space, edge, revision: _ } => {
            if !session.can_write(space) {
                return Response::Error(ApiError::Unauthorised);
            }
            let point = DimensionVector::new(vec![
                (edge.id.0 >> 32) as u32,
                (edge.id.0 & 0xFFFF_FFFF) as u32,
            ]);
            let data = match bincode::encode_to_vec(edge, bincode::config::standard()) {
                Ok(v) => v,
                Err(e) => return Response::Error(ApiError::InvalidRequest(e.to_string())),
            };
            match db.insert_on_branch(session.branch, space, point, data) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(ApiError::Internal(e.to_string())),
            }
        }

        Request::DeleteHyperedge { space, edge_id, revision: _ } => {
            if !session.can_write(space) {
                return Response::Error(ApiError::Unauthorised);
            }
            let point = DimensionVector::new(vec![
                (edge_id.0 >> 32) as u32,
                (edge_id.0 & 0xFFFF_FFFF) as u32,
            ]);
            match db.delete_on_branch(session.branch, space, point) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(ApiError::Internal(e.to_string())),
            }
        }

        Request::WriteSignal { space, sample, revision: _ } => {
            if !session.can_write(space) {
                return Response::Error(ApiError::Unauthorised);
            }
            let coords = match sample.scope.address_coords(&sample.local_coords) {
                Ok(v) => v,
                Err(e) => return Response::Error(ApiError::InvalidRequest(format!("{:?}", e))),
            };
            let data = match bincode::encode_to_vec(sample, bincode::config::standard()) {
                Ok(v) => v,
                Err(e) => return Response::Error(ApiError::InvalidRequest(e.to_string())),
            };
            match db.insert_on_branch(
                session.branch,
                space,
                DimensionVector::new(coords),
                data,
            ) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(ApiError::Internal(e.to_string())),
            }
        }

        Request::CreateBranch { name, from_branch } => {
            if !session.can_manage_branches() {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.create_branch(&name, from_branch) {
                Ok(id) => Response::BranchCreated { branch: id },
                Err(e) => Response::Error(ApiError::Internal(e)),
            }
        }

        Request::GetSnapshot { branch } => match db.branch_head(branch) {
            Some(snap) => Response::Snapshot(snap),
            None => Response::Error(ApiError::Internal("branch not found".into())),
        },

        Request::MergeBranch {
            target,
            source,
            strategy,
        } => {
            if !session.can_write_branch(target) {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.merge_branch(target, source, strategy, None) {
                Ok(mut result) => {
                    #[cfg(feature = "sync")]
                    if strategy == MergeStrategy::Interactive && !result.conflicts.is_empty() {
                        db.conflicts()
                            .push_all(target, source, std::mem::take(&mut result.conflicts));
                    }
                    Response::MergeComplete(result)
                }
                Err(e) => Response::Error(ApiError::Internal(e.to_string())),
            }
        }

        Request::GetConflicts => {
            #[cfg(feature = "sync")]
            {
                let list = db
                    .conflicts()
                    .list()
                    .into_iter()
                    .map(|c| WireConflict {
                        id: c.id,
                        target: c.target,
                        source: c.source,
                        conflict: c.conflict,
                    })
                    .collect();
                Response::Conflicts(list)
            }
            #[cfg(not(feature = "sync"))]
            Response::Conflicts(vec![])
        }

        Request::ResolveConflict { id, data } => {
            #[cfg(feature = "sync")]
            {
                let Some(stored) = db.conflicts().get(id) else {
                    return Response::Error(ApiError::InvalidRequest(format!(
                        "conflict {id} not found"
                    )));
                };
                let record =
                    resolution_record(&stored.conflict, data, RevisionId(db.revision() + 1));
                if let Err(e) = db.insert_on_branch(
                    stored.target,
                    record.address.space,
                    record.address.point,
                    record.data,
                ) {
                    return Response::Error(ApiError::Internal(e.to_string()));
                }
                let _ = db.conflicts().remove(id);
                return Response::ConflictResolved { id };
            }
            #[cfg(not(feature = "sync"))]
            Response::Error(ApiError::Internal("sync disabled".into()))
        }

        Request::Ping => Response::Pong,
    }
}

// ---------------------------------------------------------------------------
// Callback dispatcher (legacy tests)
// ---------------------------------------------------------------------------

/// Validate a request against the session and produce a response.
///
/// The actual database operations (reads, writes) are modelled as callbacks
/// so this dispatcher stays pure and testable without a real storage layer.
pub fn dispatch<ReadFn, WriteFn, BranchFn, SnapshotFn>(
    request: Request,
    session: &Session,
    read: ReadFn,
    write: WriteFn,
    create_branch: BranchFn,
    get_snapshot: SnapshotFn,
) -> Response
where
    ReadFn: FnOnce(Query) -> Result<Vec<Record>, String>,
    WriteFn: FnOnce(Address, RevisionId, Vec<u8>, bool) -> Result<RevisionId, String>,
    BranchFn: FnOnce(String, BranchId) -> Result<BranchId, String>,
    SnapshotFn: FnOnce(BranchId) -> Result<SnapshotId, String>,
{
    match request {
        Request::Ping => Response::Pong,

        Request::Query { space, snapshot, key_range, as_of, include_tombstones } => {
            if session.access(space).is_none() {
                return Response::Error(ApiError::Unauthorised);
            }
            let mut q = Query::new(space, snapshot);
            if let Some((lo, hi)) = key_range {
                q = q.with_key_range(lo, hi);
            }
            if let Some(rev) = as_of {
                q = q.as_of(rev);
            }
            if include_tombstones {
                q = q.include_tombstones();
            }
            match read(q) {
                Ok(records) => Response::Records(records),
                Err(e) => Response::Error(ApiError::Internal(e)),
            }
        }

        Request::Write { address, revision, data } => {
            if !session.can_write(address.space) {
                return Response::Error(ApiError::Unauthorised);
            }
            match write(address, revision, data, false) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(ApiError::Internal(e)),
            }
        }

        Request::Delete { address, revision } => {
            if !session.can_write(address.space) {
                return Response::Error(ApiError::Unauthorised);
            }
            match write(address, revision, vec![], true) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(ApiError::Internal(e)),
            }
        }

        Request::WriteHyperedge { space, edge, revision } => {
            if !session.can_write(space) {
                return Response::Error(ApiError::Unauthorised);
            }
            let point = DimensionVector::new(vec![(edge.id.0 >> 32) as u32, (edge.id.0 & 0xFFFF_FFFF) as u32]);
            let address = Address::new(space, point);
            let data = match bincode::encode_to_vec(edge, bincode::config::standard()) {
                Ok(v) => v,
                Err(e) => return Response::Error(ApiError::InvalidRequest(e.to_string())),
            };
            match write(address, revision, data, false) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(ApiError::Internal(e)),
            }
        }

        Request::DeleteHyperedge { space, edge_id, revision } => {
            if !session.can_write(space) {
                return Response::Error(ApiError::Unauthorised);
            }
            let point = DimensionVector::new(vec![(edge_id.0 >> 32) as u32, (edge_id.0 & 0xFFFF_FFFF) as u32]);
            let address = Address::new(space, point);
            match write(address, revision, vec![], true) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(ApiError::Internal(e)),
            }
        }

        Request::WriteSignal { space, sample, revision } => {
            if !session.can_write(space) {
                return Response::Error(ApiError::Unauthorised);
            }
            let coords = match sample.scope.address_coords(&sample.local_coords) {
                Ok(v) => v,
                Err(e) => return Response::Error(ApiError::InvalidRequest(format!("{:?}", e))),
            };
            let address = Address::new(space, DimensionVector::new(coords));
            let data = match bincode::encode_to_vec(sample, bincode::config::standard()) {
                Ok(v) => v,
                Err(e) => return Response::Error(ApiError::InvalidRequest(e.to_string())),
            };
            match write(address, revision, data, false) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(ApiError::Internal(e)),
            }
        }

        Request::CreateBranch { name, from_branch } => {
            match create_branch(name, from_branch) {
                Ok(id) => Response::BranchCreated { branch: id },
                Err(e) => Response::Error(ApiError::Internal(e)),
            }
        }

        Request::GetSnapshot { branch } => {
            match get_snapshot(branch) {
                Ok(snap) => Response::Snapshot(snap),
                Err(e) => Response::Error(ApiError::Internal(e)),
            }
        }

        Request::MergeBranch { .. }
        | Request::GetConflicts
        | Request::ResolveConflict { .. } => {
            Response::Error(ApiError::Internal(
                "use handle_request with InfiniteDb".into(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::{
        address::{RevisionId, SpaceId},
        branch::BranchId,
        snapshot::SnapshotId,
    };
    use crate::infinitedb_server::session::{AccessLevel, Session, SpaceGrant};

    fn rw_session() -> Session {
        Session::new(
            BranchId(1),
            SnapshotId(1),
            RevisionId(0),
            vec![SpaceGrant { space: SpaceId(1), level: AccessLevel::ReadWrite }],
        )
    }

    fn ro_session() -> Session {
        Session::new(
            BranchId(1),
            SnapshotId(1),
            RevisionId(0),
            vec![SpaceGrant { space: SpaceId(1), level: AccessLevel::ReadOnly }],
        )
    }

    #[test]
    fn ping_always_responds() {
        let s = rw_session();
        let r = dispatch(Request::Ping, &s, |_| Ok(vec![]), |_, _, _, _| Ok(RevisionId(1)), |_, _| Ok(BranchId(2)), |_| Ok(SnapshotId(1)));
        assert!(matches!(r, Response::Pong));
    }

    #[test]
    fn query_preserves_key_range_into_descriptor() {
        use std::cell::RefCell;

        let s = rw_session();
        let captured: RefCell<Option<Query>> = RefCell::new(None);
        let r = dispatch(
            Request::Query {
                space: SpaceId(1),
                snapshot: SnapshotId(1),
                key_range: Some((10, 99)),
                as_of: None,
                include_tombstones: false,
            },
            &s,
            |q| {
                *captured.borrow_mut() = Some(q);
                Ok(vec![])
            },
            |_, _, _, _| Ok(RevisionId(1)),
            |_, _| Ok(BranchId(2)),
            |_| Ok(SnapshotId(1)),
        );
        assert!(matches!(r, Response::Records(_)));
        let q = captured.borrow().clone().expect("read callback must receive a Query");
        assert_eq!(q.key_range, Some((10, 99)));
    }

    #[test]
    fn write_denied_for_read_only() {
        use crate::infinitedb_core::address::{Address, DimensionVector};
        let s = ro_session();
        let addr = Address::new(SpaceId(1), DimensionVector::new(vec![0, 0]));
        let r = dispatch(
            Request::Write { address: addr, revision: RevisionId(1), data: vec![] },
            &s,
            |_| Ok(vec![]),
            |_, _, _, _| Ok(RevisionId(1)),
            |_, _| Ok(BranchId(2)),
            |_| Ok(SnapshotId(1)),
        );
        assert!(matches!(r, Response::Error(ApiError::Unauthorised)));
    }
}
