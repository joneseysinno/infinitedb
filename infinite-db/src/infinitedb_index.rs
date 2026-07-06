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
/// Top-aligned curve address newtype (D-T7).
pub mod curve_address;
/// Dyadic cell-center detection.
pub mod center;
/// Placement composition utilities (T11).
pub mod placement;

pub use center::{dyadic_center_level, is_dyadic_center, parity_center_for_extent};
pub use composite::{CompositeKey, Dimension, KeyConfig};
pub use curve_address::CurveAddress;
pub use hilbert::{decode, encode};
pub use key::{hilbert_key_for, hilbert_raw_index};
pub use placement::{
    bbox_to_child, compose, extent_in_parent, nearest_common_ancestor, placement_path_to_ancestor,
    point_to_ancestor_space, to_ancestor, Placement, PlacementError,
};

use crate::infinitedb_core::hilbert_key::HilbertKey;

impl From<CurveAddress> for HilbertKey {
    fn from(addr: CurveAddress) -> Self {
        HilbertKey::from_raw(addr.raw())
    }
}
