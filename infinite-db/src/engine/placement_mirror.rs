//! Placement mirror rows in the parent space (T4 / D-T1).

use bincode::{Decode, Encode};

use crate::engine::hypergraph::HypergraphWriteRow;
use crate::infinitedb_core::{
    address::DimensionVector,
    placement::{placement_mirror_center, Placement},
    space::SpaceConfig,
};

const PLACEMENT_MIRROR_VERSION: u32 = 1;

/// Bincode payload at the mirror coordinate in the parent space.
#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct PlacementMirrorPayload {
    pub version: u32,
    pub child_id: u64,
    pub placement: Placement,
}

pub fn encode_placement_mirror_payload(
    child: &SpaceConfig,
) -> Result<Vec<u8>, bincode::error::EncodeError> {
    let payload = PlacementMirrorPayload {
        version: PLACEMENT_MIRROR_VERSION,
        child_id: child.id.0,
        placement: child.placement.clone().unwrap_or(Placement {
            offset: vec![],
            scale_num: vec![],
            scale_den: vec![],
            extent: vec![],
            fixed_axes: vec![],
        }),
    };
    bincode::encode_to_vec(payload, bincode::config::standard())
}

pub fn prepare_placement_mirror_row(child: &SpaceConfig) -> Option<HypergraphWriteRow> {
    let parent_id = child.parent?;
    let placement = child.placement.as_ref()?;
    let coords = placement_mirror_center(placement).ok()?;
    let data = encode_placement_mirror_payload(child).ok()?;
    Some(HypergraphWriteRow {
        space: parent_id,
        point: DimensionVector::new(coords),
        data,
        tombstone: false,
        structural: true,
    })
}

pub fn prepare_placement_mirror_tombstone(child: &SpaceConfig) -> Option<HypergraphWriteRow> {
    let parent_id = child.parent?;
    let placement = child.placement.as_ref()?;
    let coords = placement_mirror_center(placement).ok()?;
    Some(HypergraphWriteRow {
        space: parent_id,
        point: DimensionVector::new(coords),
        data: vec![],
        tombstone: true,
        structural: true,
    })
}
