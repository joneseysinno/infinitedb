//! Synchronization and replication protocols.

/// Snapshot delta computation and application.
pub mod delta;
/// Merkle tree verification helpers.
pub mod merkle;
/// Wire serialization for sync messages.
pub mod serial;
