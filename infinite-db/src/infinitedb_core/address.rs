use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// Identifies a logical space (a named dataset/dimension space).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct SpaceId(pub u64);

/// Monotonically increasing logical clock tick.
/// Used instead of wall-clock time so distributed nodes stay consistent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct RevisionId(pub u64);

impl RevisionId {
    /// The initial revision before any writes have occurred.
    pub const ZERO: RevisionId = RevisionId(0);

    /// Return the next logical revision.
    pub fn next(self) -> RevisionId {
        RevisionId(self.0 + 1)
    }
}

/// An N-dimensional coordinate in unsigned integer space (max 16 dims).
/// `u32` coordinates are used so Hilbert encoding can operate on them directly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct DimensionVector {
    pub coords: Vec<u32>,
}

impl DimensionVector {
    /// Construct a dimension vector. Supports up to 16 dimensions.
    pub fn new(coords: Vec<u32>) -> Self {
        assert!(
            coords.len() <= 16,
            "DimensionVector exceeds maximum of 16 dimensions"
        );
        Self { coords }
    }

    /// Number of dimensions in this coordinate.
    pub fn dims(&self) -> usize {
        self.coords.len()
    }

    /// Return the coordinate value at index `idx`.
    pub fn coord(&self, idx: usize) -> u32 {
        self.coords[idx]
    }

    /// Returns true if this point is within [min, max] inclusive on every axis.
    pub fn within(&self, min: &DimensionVector, max: &DimensionVector) -> bool {
        assert_eq!(self.dims(), min.dims());
        assert_eq!(self.dims(), max.dims());
        self.coords
            .iter()
            .zip(min.coords.iter().zip(max.coords.iter()))
            .all(|(&v, (&lo, &hi))| v >= lo && v <= hi)
    }
}

/// The primary key for a record: which space + where in that space.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct Address {
    pub space: SpaceId,
    pub point: DimensionVector,
}

impl Address {
    /// Construct a new address from its space and coordinate point.
    pub fn new(space: SpaceId, point: DimensionVector) -> Self {
        Self { space, point }
    }
}