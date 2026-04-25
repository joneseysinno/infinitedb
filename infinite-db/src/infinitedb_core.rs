//! Core domain types for InfiniteDB.

/// Addressing primitives (`Address`, `SpaceId`, `DimensionVector`).
pub mod address;
/// Block-level storage records and relations.
pub mod block;
/// Branching model and branch registry.
pub mod branch;
/// Query descriptor types.
pub mod query;
/// Schema trait implemented by storable record types.
pub mod schema;
/// Snapshot model and snapshot utilities.
pub mod snapshot;
/// Space registration and configuration.
pub mod space;
