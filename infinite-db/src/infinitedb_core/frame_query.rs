//! Frame query request types (M6).

use super::{
    address::{DimensionVector, RevisionId, SpaceId},
    provenance::FrameId,
};

/// Options controlling frame query behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FrameQueryOptions {
    /// Skip assertion delta merge on incidence paths (bounded staleness, M4 contract).
    pub index_only: bool,
    /// Attach `StalenessDiagnosis` per resolved hyperedge.
    pub include_diagnosis: bool,
    /// Return suppressed edges with `suppressed: true` for audit.
    pub include_suppressed: bool,
}

/// Spatial frame query over hyperedge testimony.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameQuery {
    pub frame_id: FrameId,
    pub testimony_space: SpaceId,
    pub min: DimensionVector,
    pub max: DimensionVector,
    pub as_of: Option<RevisionId>,
    pub options: FrameQueryOptions,
}
