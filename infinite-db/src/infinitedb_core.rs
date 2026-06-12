//! Core domain types for InfiniteDB.
//!
//! Semantics are intentionally explicit for upstream crate discoverability:
//! - Addressing (`address`) models spatial identity.
//! - Records/blocks (`block`) model immutable revisioned storage.
//! - Relationships (`hyperedge`) model external entity topology.
//! - Fields (`signal`) model scoped internal state across hyperspace.
//! - Adapter traits (`adapter`) provide typed upstream bindings.
//! - Kind catalog (`kind_catalog`) provides runtime discoverability/policy checks.

/// Addressing primitives (`Address`, `SpaceId`, `DimensionVector`, `RevisionId`).
pub mod address;
/// HLC stamp layout (peer track Phase 1).
pub mod hlc;
pub mod revision_codec;
pub mod block_codec;
/// Block-level storage records.
pub mod block;
/// Branching model and branch registry.
pub mod branch;
/// Branch merge strategies and conflict types.
pub mod merge;
/// Query descriptor types.
pub mod query;
/// Upstream adapter traits and conversions.
pub mod adapter;
/// Hyperedge relationship primitives.
pub mod hyperedge;
/// Versioned hyperedge payload codec.
pub mod hyperedge_codec;
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
/// Authoring-frame provenance on testimony (M5).
pub mod provenance;
/// Operation-level error records (M5).
pub mod error_record;
pub mod error_record_codec;
pub mod error_kind_catalog;
/// Intent checkpoint descriptors (peer track Phase 4).
pub mod intent_checkpoint;
/// Judgment testimony conventions (M5).
pub mod judgment;
pub mod judgment_codec;
pub mod judgment_index;
/// Pure staleness diagnosis helpers (M5).
pub mod staleness;
/// Named durable frame definitions (M6).
pub mod frame;
/// Frame query request types (M6).
pub mod frame_query;
/// Flow-vector displacement and quantization (M7).
pub mod flow_vector;
pub mod flow_vector_index;
/// Computation input lineage (M7).
pub mod computation;
/// Structural staleness via input pins (M7).
pub mod staleness_closure;
