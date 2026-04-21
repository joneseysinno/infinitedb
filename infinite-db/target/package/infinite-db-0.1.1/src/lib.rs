// Core spatial types — always available
pub mod infinitedb_core;

// Index primitives — always available
pub mod infinitedb_index;

// Storage engine — always available (embedded feature)
#[cfg(feature = "embedded")]
pub mod infinitedb_storage;

// Network server — optional (server feature)
#[cfg(feature = "server")]
pub mod infinitedb_server;

// Sync / replication — optional (sync feature)
#[cfg(feature = "sync")]
pub mod infinitedb_sync;

// ---------------------------------------------------------------------------
// Top-level facade
// ---------------------------------------------------------------------------

#[cfg(feature = "embedded")]
pub use db::{InfiniteDb, MemoryStats};

#[cfg(feature = "embedded")]
mod db;

