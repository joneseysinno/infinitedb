//! Synchronization and replication protocols.

/// Snapshot delta computation and application.
pub mod delta;
/// Merkle tree verification helpers.
pub mod merkle;
/// Wire serialization for sync messages.
pub mod serial;
/// Durable outbox state for offline replication.
pub mod outbox;
/// Transport traits and default adapters.
pub mod transport;
/// Background replication worker loop.
pub mod worker;
