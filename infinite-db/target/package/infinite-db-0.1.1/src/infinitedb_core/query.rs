use serde::{Deserialize, Serialize};
use super::address::{DimensionVector, RevisionId, SpaceId};
use super::snapshot::SnapshotId;

/// An axis-aligned bounding box in N-dimensional space.
/// Records whose point lies within [min, max] on every axis are included.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpatialRange {
    pub min: DimensionVector,
    pub max: DimensionVector,
}

impl SpatialRange {
    pub fn new(min: DimensionVector, max: DimensionVector) -> Self {
        assert_eq!(min.dims(), max.dims(), "Range bounds must have equal dimensions");
        Self { min, max }
    }
}

/// A read query against a snapshot.
/// Queries are pure descriptors — the storage layer executes them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub space: SpaceId,
    pub snapshot: SnapshotId,
    /// Spatial bounds to filter records by. None = all records in the space.
    pub range: Option<SpatialRange>,
    /// Only return records at or before this revision.
    /// Defaults to the snapshot's own revision when None.
    pub as_of: Option<RevisionId>,
    /// Include tombstoned (deleted) records in results.
    pub include_tombstones: bool,
}

impl Query {
    pub fn new(space: SpaceId, snapshot: SnapshotId) -> Self {
        Self {
            space,
            snapshot,
            range: None,
            as_of: None,
            include_tombstones: false,
        }
    }

    pub fn with_range(mut self, range: SpatialRange) -> Self {
        self.range = Some(range);
        self
    }

    pub fn as_of(mut self, revision: RevisionId) -> Self {
        self.as_of = Some(revision);
        self
    }

    pub fn include_tombstones(mut self) -> Self {
        self.include_tombstones = true;
        self
    }

    pub fn with_bounds(self, min: Vec<u32>, max: Vec<u32>) -> Self {
        self.with_range(SpatialRange::new(
            DimensionVector::new(min),
            DimensionVector::new(max),
        ))
    }
}