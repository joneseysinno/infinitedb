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
    space::SpaceConfig,
    void::{Presence, VoidOr},
    universe::UniverseGraphView,
    nexus::{ConstellationPin, NexusEdge, NexusId},
    ephemeris::{EphemerisEntry, WandererId},
};
use crate::engine::error::EngineError;
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
    /// Catalog: list direct child spaces (T3).
    ListChildren { parent: SpaceId },
    /// Catalog: depth-first subtree configs (T3).
    GetSubtree { root: SpaceId },
    /// Idempotent space registration (T8).
    RegisterOrGetSpace { config: SpaceConfig },
    /// Per-space occupied-key depth statistic (T9).
    GetSpaceDensity { space: SpaceId },
    /// Three-state point presence (`IS_VOID` ≡ `Presence::Void`) (D-V5).
    GetPresence {
        space: SpaceId,
        point: DimensionVector,
        as_of: Option<RevisionId>,
    },
    WriteNexus { edge: NexusEdge },
    DeleteNexus { id: NexusId },
    GetUniverseGraph {
        as_of: Option<RevisionId>,
        contract_constellation: Option<crate::infinitedb_core::universe::ConstellationId>,
    },
    GetUniverseCenter { as_of: Option<RevisionId> },
    GetConstellations { as_of: Option<RevisionId> },
    PinConstellation {
        pin: ConstellationPin,
        nexus_id: NexusId,
    },
    UnpinConstellation { nexus_id: NexusId },
    GetPinnedConstellations { as_of: Option<RevisionId> },
    AppendEphemeris {
        entry: EphemerisEntry,
        edge_id: HyperedgeId,
    },
    GetEphemeris {
        wanderer: WandererId,
        as_of: Option<RevisionId>,
    },
    GetWandererPresence {
        wanderer: WandererId,
        as_of: Option<RevisionId>,
    },
    StartNexusTransfer {
        source: SpaceId,
        target: SpaceId,
    },
    GetTransferStatus { id: u64 },
    PortUniverse {
        bundle: crate::engine::port::UniversePortBundle,
        options: crate::engine::port::PortUniverseOptions,
    },
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
    /// Space tower catalog entries.
    SpaceConfigs(Vec<SpaceConfig>),
    /// Space registration result.
    SpaceRegistered { id: SpaceId },
    /// Density statistic for a space (void when never observed).
    SpaceDensity {
        record_count: Option<u64>,
        max_occupied_depth: Option<u32>,
    },
    /// Three-state presence at a point.
    Presence(Presence),
    /// Universe graph view.
    UniverseGraph(UniverseGraphView),
    /// Per-component centers (void when member-void).
    UniverseCenters(Option<Vec<crate::infinitedb_core::universe::ComponentCenters>>),
    /// Constellation clusters (void when member-void).
    Constellations(Option<Vec<Vec<crate::infinitedb_core::universe::ContainerRef>>>),
    /// Ephemeris trajectory entries.
    Ephemeris(Vec<EphemerisEntry>),
    /// Wanderer presence (void when no entries).
    WandererPresence(Option<EphemerisEntry>),
    /// Nexus transfer intent id.
    TransferStarted { id: u64 },
    /// Nexus transfer progress.
    TransferStatus(crate::engine::transfer::NexusTransferIntent),
    /// Ported constellation id.
    PortComplete {
        constellation_id: crate::infinitedb_core::universe::ConstellationId,
    },
    /// Pinned constellation payloads.
    PinnedConstellations(Vec<ConstellationPin>),
    /// An error that the client should handle.
    Error(ApiError),
}

/// Project an engine error to the wire contract — the single deliberate information-loss point.
pub fn project_api_error(err: EngineError) -> ApiError {
    match err {
        EngineError::SpaceNotFound(id) => ApiError::SpaceNotFound(id),
        EngineError::InvalidHyperedge(e) => ApiError::InvalidRequest(format!("{e:?}")),
        EngineError::InvalidNexus(e) => ApiError::InvalidRequest(e.to_string()),
        e @ (
            EngineError::InvalidSpaceConfig { .. }
            | EngineError::BranchExists(_)
            | EngineError::BranchNotFound(_)
            | EngineError::RegistrySpace(crate::infinitedb_core::space::SpaceError::DuplicateId(_))
            | EngineError::RegistrySpace(crate::infinitedb_core::space::SpaceError::DuplicateName(_))
            | EngineError::RegistrySpace(crate::infinitedb_core::space::SpaceError::ParentNotFound(_))
            | EngineError::RegistrySpace(crate::infinitedb_core::space::SpaceError::Cycle(_))
            | EngineError::RegistrySpace(crate::infinitedb_core::space::SpaceError::PlacementError(_))
            | EngineError::RegistrySpace(crate::infinitedb_core::space::SpaceError::HasChildren(_))
            | EngineError::RegistrySpace(crate::infinitedb_core::space::SpaceError::NexusReferenced(_))
            | EngineError::RegistrySpace(crate::infinitedb_core::space::SpaceError::ConfigConflict { .. })
            | EngineError::RegistrySpace(crate::infinitedb_core::space::SpaceError::IndexPrecisionDominates { .. })
            | EngineError::RegistrySpace(crate::infinitedb_core::space::SpaceError::SpaceOrdinalExhausted)
            | EngineError::RegistryBranch(
                crate::infinitedb_core::branch::BranchError::DuplicateName(_),
            )
            | EngineError::RegistryBranch(crate::infinitedb_core::branch::BranchError::NotFound(_))
            | EngineError::EndpointIndexMissing
            | EngineError::ErrorSpaceMissing(_)
            | EngineError::InvalidJudgment(_)
            | EngineError::InvalidProvenance(_)
            | EngineError::ArbiterStreamExists(_)
            | EngineError::ArbiterStreamNotFound(_)
            | EngineError::ReservedArbiterId(_)
            | EngineError::FrameExists(_)
            | EngineError::FrameNotFound(_)
            | EngineError::InvalidFrame(_)
            | EngineError::InvalidComputation(_)
            | EngineError::UndefinedOverVoid { .. }
        ) => ApiError::InvalidRequest(e.to_string()),
        EngineError::DerivationBackpressure {
            pending_tasks,
            derivation_lag,
        } => ApiError::Busy {
            retry_hint_ms: EngineError::derivation_retry_hint_ms(pending_tasks, derivation_lag),
        },
        EngineError::ErrorKindCatalog(_) => ApiError::InvalidRequest(err.to_string()),
        EngineError::Storage(_)
        | EngineError::RegistrySpace(_)
        | EngineError::WatermarkViolation { .. }
        | EngineError::ErrorRecordEncode { .. }
        | EngineError::ErrorRecordDecode { .. }
        | EngineError::Other { .. } => ApiError::Internal(err.to_string()),
    }
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
    /// Transient overload — client should retry after `retry_hint_ms`.
    Busy { retry_hint_ms: u64 },
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
            match db.insert_hyperedge_on_branch(session.branch, space, edge) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(ApiError::Internal(e.to_string())),
            }
        }

        Request::DeleteHyperedge { space, edge_id, revision: _ } => {
            if !session.can_write(space) {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.delete_hyperedge_on_branch(session.branch, space, edge_id) {
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
                Err(e) => Response::Error(project_api_error(e)),
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
                #[cfg(feature = "sync")]
                Ok(mut result) => {
                    if strategy == MergeStrategy::Interactive && !result.conflicts.is_empty() {
                        if let Err(e) = db.conflicts().push_all(
                            target,
                            source,
                            std::mem::take(&mut result.conflicts),
                        ) {
                            return Response::Error(project_api_error(e.into()));
                        }
                    }
                    Response::MergeComplete(result)
                }
                #[cfg(not(feature = "sync"))]
                Ok(result) => Response::MergeComplete(result),
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
                    resolution_record(&stored.conflict, data, db.revision().next_global());
                if let Err(e) = db.insert_on_branch(
                    stored.target,
                    record.address.space,
                    record.address.point,
                    record.data,
                ) {
                    return Response::Error(ApiError::Internal(e.to_string()));
                }
                match db.conflicts().remove(id) {
                    Ok(Some(_)) => return Response::ConflictResolved { id },
                    Ok(None) => {
                        return Response::Error(ApiError::InvalidRequest(format!(
                            "conflict {id} not found"
                        )));
                    }
                    Err(e) => return Response::Error(project_api_error(e.into())),
                }
            }
            #[cfg(not(feature = "sync"))]
            {
                let _ = (id, data);
                Response::Error(ApiError::Internal("sync disabled".into()))
            }
        }

        Request::Ping => Response::Pong,

        Request::ListChildren { parent } => {
            if session.access(parent).is_none() {
                return Response::Error(ApiError::Unauthorised);
            }
            let configs: Vec<SpaceConfig> = db
                .list_children(parent)
                .into_iter()
                .filter_map(|id| db.spaces.read().get(id).cloned())
                .collect();
            Response::SpaceConfigs(configs)
        }

        Request::GetSubtree { root } => {
            if session.access(root).is_none() {
                return Response::Error(ApiError::Unauthorised);
            }
            let configs: Vec<SpaceConfig> = db
                .get_subtree(root)
                .into_iter()
                .map(|(_, c)| c)
                .collect();
            Response::SpaceConfigs(configs)
        }

        Request::RegisterOrGetSpace { config } => {
            if !session.can_manage_spaces() {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.register_or_get_space(config) {
                Ok(id) => Response::SpaceRegistered { id },
                Err(e) => Response::Error(project_api_error(e)),
            }
        }

        Request::GetSpaceDensity { space } => {
            if session.access(space).is_none() {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.space_density(space) {
                VoidOr::Void => Response::SpaceDensity {
                    record_count: None,
                    max_occupied_depth: None,
                },
                VoidOr::Known(d) => Response::SpaceDensity {
                    record_count: Some(d.record_count),
                    max_occupied_depth: Some(d.max_occupied_depth),
                },
            }
        }

        Request::GetPresence { space, point, as_of } => {
            if session.access(space).is_none() {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.presence_at_on_branch(session.branch, space, point, as_of) {
                Ok(p) => Response::Presence(p),
                Err(e) => Response::Error(project_api_error(e)),
            }
        }

        Request::WriteNexus { edge } => {
            if !session.can_manage_spaces() {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.write_nexus(edge) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(project_api_error(e)),
            }
        }

        Request::DeleteNexus { id } => {
            if !session.can_manage_spaces() {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.delete_nexus(id) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(project_api_error(e)),
            }
        }

        Request::GetUniverseGraph {
            as_of,
            contract_constellation,
        } => {
            let result = if let Some(cid) = contract_constellation {
                db.universe_graph_view_contracted(as_of, cid)
            } else {
                db.universe_graph_view(as_of)
            };
            match result {
                Ok(view) => Response::UniverseGraph(view),
                Err(e) => Response::Error(project_api_error(e)),
            }
        }

        Request::PinConstellation { pin, nexus_id } => {
            if !session.can_manage_spaces() {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.pin_constellation(pin, nexus_id) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(project_api_error(e)),
            }
        }

        Request::UnpinConstellation { nexus_id } => {
            if !session.can_manage_spaces() {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.unpin_constellation(nexus_id) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(project_api_error(e)),
            }
        }

        Request::GetPinnedConstellations { as_of } => {
            match db.pinned_constellations(as_of) {
                Ok(pins) => Response::PinnedConstellations(pins),
                Err(e) => Response::Error(project_api_error(e)),
            }
        }

        Request::GetUniverseCenter { as_of } => match db.universe_center(as_of) {
            Ok(v) => Response::UniverseCenters(v.known()),
            Err(e) => Response::Error(project_api_error(e)),
        },

        Request::GetConstellations { as_of } => match db.universe_constellations(as_of) {
            Ok(v) => Response::Constellations(v.known()),
            Err(e) => Response::Error(project_api_error(e)),
        },

        Request::AppendEphemeris { entry, edge_id } => {
            if !session.can_manage_spaces() {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.append_ephemeris(entry, edge_id) {
                Ok(rev) => Response::WriteAck { revision: rev },
                Err(e) => Response::Error(project_api_error(e)),
            }
        }

        Request::GetEphemeris { wanderer, as_of } => match db.ephemeris_of(wanderer, as_of) {
            Ok(entries) => Response::Ephemeris(entries),
            Err(e) => Response::Error(project_api_error(e)),
        },

        Request::GetWandererPresence { wanderer, as_of } => {
            match db.wanderer_presence_at(wanderer, as_of) {
                Ok(v) => Response::WandererPresence(v.known()),
                Err(e) => Response::Error(project_api_error(e)),
            }
        }

        Request::StartNexusTransfer { source, target } => {
            if !session.can_manage_spaces() {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.start_nexus_transfer(source, target) {
                Ok(id) => Response::TransferStarted { id },
                Err(e) => Response::Error(project_api_error(e)),
            }
        }

        Request::GetTransferStatus { id } => match db.nexus_transfer_status(id) {
            Some(intent) => Response::TransferStatus(intent),
            None => Response::Error(ApiError::InvalidRequest(format!("transfer {id} not found"))),
        },

        Request::PortUniverse { bundle, options } => {
            if !session.can_manage_spaces() {
                return Response::Error(ApiError::Unauthorised);
            }
            match db.port_universe(bundle, options) {
                Ok(id) => Response::PortComplete { constellation_id: id },
                Err(e) => Response::Error(project_api_error(e)),
            }
        }
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
            if let Err(e) = edge.validate() {
                return Response::Error(ApiError::InvalidRequest(format!("{:?}", e)));
            }
            let point = Hyperedge::storage_point(edge.id);
            let address = Address::new(space, point);
            let data = match crate::infinitedb_core::hyperedge_codec::encode_hyperedge(&edge) {
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
            let point = Hyperedge::storage_point(edge_id);
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
        | Request::ResolveConflict { .. }
        | Request::RegisterOrGetSpace { .. } => {
            Response::Error(ApiError::Internal(
                "use handle_request with InfiniteDb".into(),
            ))
        }

        Request::ListChildren { parent } => {
            if session.access(parent).is_none() {
                return Response::Error(ApiError::Unauthorised);
            }
            Response::SpaceConfigs(vec![])
        }

        Request::GetSubtree { root } => {
            if session.access(root).is_none() {
                return Response::Error(ApiError::Unauthorised);
            }
            Response::SpaceConfigs(vec![])
        }

        Request::GetSpaceDensity { space } => {
            if session.access(space).is_none() {
                return Response::Error(ApiError::Unauthorised);
            }
            Response::SpaceDensity {
                record_count: None,
                max_occupied_depth: None,
            }
        }

        Request::GetPresence { space, point: _, as_of: _ } => {
            if session.access(space).is_none() {
                return Response::Error(ApiError::Unauthorised);
            }
            Response::Presence(Presence::Void)
        }

        Request::WriteNexus { .. }
        | Request::DeleteNexus { .. }
        | Request::GetUniverseGraph { .. }
        | Request::PinConstellation { .. }
        | Request::UnpinConstellation { .. }
        | Request::GetPinnedConstellations { .. }
        | Request::GetUniverseCenter { .. }
        | Request::GetConstellations { .. }
        | Request::AppendEphemeris { .. }
        | Request::GetEphemeris { .. }
        | Request::GetWandererPresence { .. }
        | Request::StartNexusTransfer { .. }
        | Request::GetTransferStatus { .. }
        | Request::PortUniverse { .. } => {
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
            RevisionId::legacy(0),
            vec![SpaceGrant { space: SpaceId(1), level: AccessLevel::ReadWrite }],
        )
    }

    fn ro_session() -> Session {
        Session::new(
            BranchId(1),
            SnapshotId(1),
            RevisionId::legacy(0),
            vec![SpaceGrant { space: SpaceId(1), level: AccessLevel::ReadOnly }],
        )
    }

    #[test]
    fn ping_always_responds() {
        let s = rw_session();
        let r = dispatch(Request::Ping, &s, |_| Ok(vec![]), |_, _, _, _| Ok(RevisionId::legacy(1)), |_, _| Ok(BranchId(2)), |_| Ok(SnapshotId(1)));
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
            |_, _, _, _| Ok(RevisionId::legacy(1)),
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
            Request::Write { address: addr, revision: RevisionId::legacy(1), data: vec![] },
            &s,
            |_| Ok(vec![]),
            |_, _, _, _| Ok(RevisionId::legacy(1)),
            |_, _| Ok(BranchId(2)),
            |_| Ok(SnapshotId(1)),
        );
        assert!(matches!(r, Response::Error(ApiError::Unauthorised)));
    }

    #[test]
    fn project_backpressure_to_busy() {
        let err = EngineError::DerivationBackpressure {
            pending_tasks: 4,
            derivation_lag: 200,
        };
        match project_api_error(err) {
            ApiError::Busy { retry_hint_ms } => assert!(retry_hint_ms >= 50),
            other => panic!("expected Busy, got {other:?}"),
        }
    }

    #[test]
    fn project_undefined_over_void_to_invalid_request() {
        let err = EngineError::UndefinedOverVoid {
            operation: "density_mean",
            container: Some(SpaceId(3)),
        };
        assert!(matches!(
            project_api_error(err),
            ApiError::InvalidRequest(_)
        ));
    }
}
