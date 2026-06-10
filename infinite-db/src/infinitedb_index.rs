//! Indexing primitives used to map multi-dimensional data into ordered keys.

/// N-dimensional Hilbert encoding and decoding.
pub mod hilbert;
/// Scalar encoders preserving ordinal ordering.
pub mod ordinal;
/// Hybrid logical clock utilities for temporal dimensions.
pub mod temporal;
/// Composite key builder for heterogeneous dimensions.
pub mod composite;
/// Hilbert key derivation for spatial points (single source of truth).
pub mod key;
/// Bounding-box to Hilbert key interval decomposition.
pub mod range_decompose;

pub use key::{hilbert_key_for, hilbert_key_standard};
