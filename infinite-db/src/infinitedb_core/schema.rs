use super::address::{DimensionVector, SpaceId};

/// Implemented by any Rust type that can be stored as a spatial record.
/// The type declares which space it belongs to and how to project
/// its fields into a DimensionVector for Hilbert indexing.
pub trait InfiniteSchema {
    /// The space this type is stored in. Must be consistent across all instances.
    fn space_id() -> SpaceId
    where
        Self: Sized;

    /// Number of dimensions this type occupies.
    fn dims() -> usize
    where
        Self: Sized;

    /// Convert this record's spatial fields into a coordinate point.
    /// The returned vector must have exactly `Self::dims()` coordinates.
    fn to_point(&self) -> DimensionVector;
}