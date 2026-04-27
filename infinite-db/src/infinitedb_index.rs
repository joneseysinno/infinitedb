//! Indexing primitives used to map multi-dimensional data into ordered keys.

/// N-dimensional Hilbert encoding and decoding.
pub mod hilbert;
/// Scalar encoders preserving ordinal ordering.
pub mod ordinal;
/// Hybrid logical clock utilities for temporal dimensions.
pub mod temporal;
/// Composite key builder for heterogeneous dimensions.
pub mod composite;
