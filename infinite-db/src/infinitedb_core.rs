//! Core domain types for InfiniteDB.
//!
//! Semantics are intentionally explicit for upstream crate discoverability:
//! - Addressing (`address`) models spatial identity.
//! - Records/blocks (`block`) model immutable revisioned storage.
//! - Relationships (`hyperedge`) model external entity topology.
//! - Fields (`signal`) model scoped internal state across hyperspace.
//! - Adapter traits (`adapter`) provide typed upstream bindings.
//! - Kind catalog (`kind_catalog`) provides runtime discoverability/policy checks.

/// Addressing primitives (`Address`, `SpaceId`, `DimensionVector`).
pub mod address;
/// Block-level storage records and relations.
pub mod block;
/// Branching model and branch registry.
pub mod branch;
/// Branch merge strategies and conflict types.
pub mod merge;
/// Query descriptor types.
pub mod query;
/// Schema trait implemented by storable record types.
pub mod schema;
/// Upstream adapter traits and conversions.
pub mod adapter;
/// Hyperedge relationship primitives.
pub mod hyperedge;
/// Reverse endpoint index for hyperedge lookups.
pub mod endpoint_index;
/// Cross-space hypergraph traversal.
pub mod traversal;
/// Signal field primitives and scoped samples.
pub mod signal;
/// Runtime kind/role catalog for adapter validation.
pub mod kind_catalog;
/// Snapshot model and snapshot utilities.
pub mod snapshot;
/// Space registration and configuration.
pub mod space;
/// Named database counter persistence.
pub mod persisted_counters;
/// Hilbert key newtype and cached-key state.
pub mod hilbert_key;
/// Blake3 checksum newtype.
pub mod checksum;
/// Record/address identity keys for grouping.
pub mod record_identity;
/// Legacy coordinate packing codecs.
pub mod coords;
