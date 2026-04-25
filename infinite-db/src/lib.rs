//! InfiniteDB crate root.
//!
//! This crate exposes core spatial data types and, via feature flags,
//! optional embedded storage, server, and sync layers.

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
pub use db::{InfiniteDb, MemoryStats};

#[cfg(feature = "embedded")]
mod db;

