//! Placement mirror rows in the parent space (T4 / D-T1).

use bincode::{Decode, Encode};

use crate::engine::hypergraph::HypergraphWriteRow;
use crate::infinitedb_core::{
    address::DimensionVector,
    placement::{extent_in_parent, Placement},
    space::SpaceConfig,
};
use crate::infinitedb_index::center::parity_center_for_extent;

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

fn mirror_coords(child: &SpaceConfig, parent_bits: u32) -> Option<Vec<u32>> {
    let placement = child.placement.as_ref()?;
    let (min_i, max_i) = extent_in_parent(placement);
    let min: Vec<u32> = min_i.into_iter().map(|v| v as u32).collect();
    let max: Vec<u32> = max_i.into_iter().map(|v| v as u32).collect();
    let mut coords = parity_center_for_extent(&min, &max, parent_bits);
    for &(axis, val) in &placement.fixed_axes {
        if axis >= coords.len() {
            coords.resize(axis + 1, 0);
        }
        coords[axis] = val;
    }
    Some(coords)
}

pub fn prepare_placement_mirror_row(
    child: &SpaceConfig,
    parent_bits: u32,
) -> Option<HypergraphWriteRow> {
    let parent_id = child.parent?;
    let coords = mirror_coords(child, parent_bits)?;
    let data = encode_placement_mirror_payload(child).ok()?;
    Some(HypergraphWriteRow {
        space: parent_id,
        point: DimensionVector::new(coords),
        data,
        tombstone: false,
        structural: true,
    })
}

pub fn prepare_placement_mirror_tombstone(
    child: &SpaceConfig,
    parent_bits: u32,
) -> Option<HypergraphWriteRow> {
    let parent_id = child.parent?;
    let coords = mirror_coords(child, parent_bits)?;
    Some(HypergraphWriteRow {
        space: parent_id,
        point: DimensionVector::new(coords),
        data: vec![],
        tombstone: true,
        structural: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::SpaceId;
    use crate::infinitedb_index::center::dyadic_center_level;

    #[test]
    fn mirror_coordinate_is_valid_center() {
        let child = SpaceConfig::new(SpaceId(2), "c", 2)
            .with_parent(SpaceId(1))
            .with_placement(Placement {
                offset: vec![10, 10],
                scale_num: vec![1, 1],
                scale_den: vec![1, 1],
                extent: vec![64, 64],
                fixed_axes: vec![],
            });
        let coords = mirror_coords(&child, 8).unwrap();
        assert_eq!(coords, vec![64, 64]);
        assert_eq!(dyadic_center_level(&coords, 8), Some(1));
    }
}
