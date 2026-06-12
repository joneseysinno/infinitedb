//! InfiniteDB crate root.
//!
//! This crate exposes core spatial data types and, via feature flags,
//! optional embedded storage, server, and sync layers.
//!
//! ## Database entry point
//!
//! [`InfiniteDb`] is the CRCW embedded database (formats v2–v4, v4 default):
//! concurrent reads, fire-and-forget writes, branch overlays, and merge.
//! It is `Send`/`Sync`. Use [`OpenOptions`] to tune I/O threads and format version.
//!
//! ## Feature flags
//!
//! - `embedded` (default) — [`InfiniteDb`], storage engine
//! - `server` — TCP API (`embedded` + tokio)
//! - `sync` — replication, conflict queue, branch merge helpers (`server`)
//!
//! ## Migrating from 0.2.0
//!
//! 0.3.0 replaces the single-threaded WAL [`InfiniteDb`] with a CRCW engine. Bulk session APIs
//! from 0.2.0 are removed; hypergraph write/query APIs are restored in Milestone 1.
//! Hyperedge and signal types remain in [`infinitedb_core`]; use [`InfiniteDb::insert_hyperedge`]
//! or record-level [`InfiniteDb::insert`] / [`InfiniteDb::query`]. See `README.md` and `CHANGELOG.md`.
//!
//! ## Semantic Vocabulary
//! - `Space`: named N-dimensional dataset with fixed dimensionality.
//! - `Address`: (`SpaceId`, `DimensionVector`) coordinate key for a record.
//! - `Node`: any domain entity represented as one or more records in a space.
//! - `Hyperedge`: explicit relationship linking 2+ endpoint nodes.
//! - `Signal`: scoped field/sample value over a parent hyperspace prefix.
//! - `Scope`: fixed coordinate prefix that bounds child records.
//! - `Revision`: monotonic logical write clock (`RevisionId`).
//! - `Snapshot`: immutable view assembled from sealed blocks.
//! - `Tombstone`: logical delete marker for an address/revision.
//! - `Adapter`: optional trait-based upstream layer for typed labels and spaces.

/// Core spatial and branching types.
pub mod infinitedb_core;

/// Hilbert and dimension encoding utilities.
pub mod infinitedb_index;

/// Embedded storage engine components.
#[cfg(feature = "embedded")]
pub mod infinitedb_storage;

/// Server-facing API and session management.
#[cfg(feature = "server")]
pub mod infinitedb_server;

/// Synchronization and replication primitives.
#[cfg(feature = "sync")]
pub mod infinitedb_sync;

// ---------------------------------------------------------------------------
// Top-level facade
// ---------------------------------------------------------------------------

#[cfg(feature = "embedded")]
pub use concurrent::{InfiniteDb, IoStats, OpenOptions, ReadTxn, RevisionRange};
#[cfg(feature = "embedded")]
pub use engine::session::{DurableIntent, VersionVector, WriteSession};
pub use infinitedb_core::intent_checkpoint::{IntentCheckpoint, IntentOperationKind};

#[cfg(feature = "embedded")]
pub use engine::derivation::{DerivationBackpressurePolicy, DerivationStats};
pub use engine::timed_fast_path::{
    SessionWriteStatsSnapshot, TimedFastPathPolicy,
};
#[cfg(feature = "embedded")]
pub use engine::error::EngineError;
#[cfg(feature = "embedded")]
pub use engine::import::{
    HyperedgeImportResult, HyperedgeImportSession, ImportBudget, ImportErrorClass,
    ImportErrorEntry, ImportErrorLog,
};
#[cfg(feature = "embedded")]
pub use engine::frame::{AttachedJudgment, FrameResolvedHyperedge, FrameTraversalResult};
#[cfg(feature = "embedded")]
pub use infinitedb_core::{
    error_record::{ErrorKind, OperationErrorRecord, OperationRevisionRange},
    frame::{
        merge_admission_specs, AssertionScope, FrameDefinition, FrameRegisterRequest,
        JudgmentOverlayLayer, OverlayPolicy, TestimonySource, VerdictFilter,
    },
    frame_query::{FrameQuery, FrameQueryOptions, FrameVersionPin},
    judgment::{ArbiterId, ArbiterStream, JudgmentId, JudgmentRecord, JudgmentVerdict, SubjectPin},
    provenance::{AuthoringFrameProvenance, FrameId},
    computation::ComputationProvenance,
    flow_vector::{FlowVector, FlowVectorQuantization, QuantizedDirection},
    flow_vector_index::FLOW_VECTOR_INDEX_SPACE,
    staleness::{consulted_from_frame, diagnose_assertion, ConsultedFrame, StalenessDiagnosis},
    staleness_closure::{FreshnessReport, FreshnessStatus, InputFreshness, StaleTarget},
    traversal::FrameTraversalSpec,
};
#[cfg(feature = "embedded")]
pub use infinitedb_core::flow_vector::FlowVectorRecord;
#[cfg(feature = "embedded")]
pub use infinitedb_core::query::QueryOptions;

#[cfg(feature = "embedded")]
pub use infinitedb_core::hlc::{HlcStamp, SessionId, GLOBAL_SESSION};
pub use infinitedb_core::revision_codec::{
    RevisionWireFormat, FORMAT_VERSION_HLC_REVISION, REVISION_WIRE_HLC_TAG,
};

pub use infinitedb_storage::format::{
    FormatVersion, FORMAT_VERSION_V2, FORMAT_VERSION_V3, FORMAT_VERSION_V4, FORMAT_VERSION_V5,
};

#[cfg(feature = "server")]
pub use infinitedb_server::api::{handle_request, project_api_error, ApiError, Request, Response, WireConflict};

#[cfg(feature = "server")]
pub use infinitedb_server::runtime::{admin_grants, client_roundtrip, Server, ServerConfig};

#[cfg(feature = "server")]
pub use infinitedb_server::session::{AccessLevel, Session, SessionId, SpaceGrant};

#[cfg(feature = "sync")]
pub use infinitedb_storage::cluster::ClusterMeta;

#[cfg(feature = "sync")]
pub use infinitedb_sync::conflict_queue::{
    resolution_record, resolution_tombstone, ConflictQueue, StoredConflict,
};

#[cfg(feature = "sync")]
pub use infinitedb_sync::replicate::{
    branch_sync_state, converge_main_records, converge_with_branch_merge, import_branch_overlay,
    snapshot_merkle, BranchSyncState,
};

pub use infinitedb_core::merge::{MergeConflict, MergeResult, MergeStrategy};

#[cfg(feature = "embedded")]
pub use engine::coordinator::SpaceCoordinator;

#[cfg(feature = "embedded")]
pub use engine::hilbert_coordinator::HilbertCoordinator;

#[cfg(feature = "embedded")]
pub use engine::io_thread::{IoThreadConfig, WriteRoute};

#[cfg(feature = "embedded")]
pub use engine::write_queue::WriteJob;

#[cfg(feature = "embedded")]
pub use infinitedb_storage::wal::WalDurability;

#[cfg(feature = "embedded")]
mod concurrent;
#[cfg(feature = "embedded")]
mod engine;
