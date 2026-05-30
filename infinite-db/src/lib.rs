//! InfiniteDB crate root.
//!
//! This crate exposes core spatial data types and, via feature flags,
//! optional embedded storage, server, and sync layers.
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
/// Top-level embedded database handle and diagnostics.
pub use db::{
    BulkHyperedgeImport, BulkHyperedgeImportOptions, BulkImportResult, BulkRecordImport,
    BulkSignalImport, BulkWriteOptions, BulkWriteResult, InfiniteDb, MemoryStats, OpenOptions,
};
pub use infinitedb_storage::wal::WalDurability;

#[cfg(feature = "embedded")]
mod db;

