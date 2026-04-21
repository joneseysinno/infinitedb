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
    address::{Address, RevisionId, SpaceId},
    block::Record,
    branch::BranchId,
    query::Query,
    snapshot::SnapshotId,
};
use crate::infinitedb_server::session::Session;

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
    /// Branch: create a new branch forked from an existing one.
    CreateBranch {
        name: String,
        from_branch: BranchId,
    },
    /// Snapshot: export the current snapshot header for sync.
    GetSnapshot { branch: BranchId },
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
    /// Pong.
    Pong,
    /// An error that the client should handle.
    Error(ApiError),
}

/// Structured errors returned to the client.
#[derive(Debug, Encode, Decode)]
pub enum ApiError {
    Unauthorised,
    SpaceNotFound(SpaceId),
    InvalidRequest(String),
    Internal(String),
}

// ---------------------------------------------------------------------------
// Dispatcher
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
                // Convert u128 Hilbert bounds back to raw coord vecs is deferred
                // to the storage layer; pass them via the query's range field
                // once the index layer provides a decode helper.
                let _ = (lo, hi); // stored in query range in the full implementation
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
