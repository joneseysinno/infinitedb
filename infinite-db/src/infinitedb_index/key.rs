//! Hilbert key derivation for spatial points.
//!
//! This is the single source of truth for turning a `DimensionVector` into the
//! `u128` Hilbert key used to order records and route range queries. Every layer
//! (storage, sync, indexing) must derive keys through here so precision never
//! drifts between code paths — a mismatch would silently corrupt `BTreeMap`
//! ordering across the sync boundary.

use crate::infinitedb_core::address::DimensionVector;
use super::composite::{CompositeKey, Dimension, KeyConfig};

/// Compute the Hilbert key for a point at the given precision.
///
/// An empty point maps to `0` so callers can key block minimums uniformly even
/// for degenerate records.
pub fn hilbert_key_for(point: &DimensionVector, config: KeyConfig) -> u128 {
    if point.coords.is_empty() {
        return 0;
    }
    let mut key = CompositeKey::new(config);
    for &c in &point.coords {
        key = key.push(Dimension::new("_", c));
    }
    key.encode()
}

/// Compute the Hilbert key for a point using `KeyConfig::STANDARD` (8-bit) precision.
///
/// This is the default used by spaces that do not override their precision.
pub fn hilbert_key_standard(point: &DimensionVector) -> u128 {
    hilbert_key_for(point, KeyConfig::STANDARD)
}
