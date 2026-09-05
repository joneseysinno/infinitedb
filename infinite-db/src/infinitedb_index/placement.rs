//! Placement composition re-exports for the index module (T11).

pub use crate::infinitedb_core::placement::{
    bbox_to_child, compose, extent_in_parent, nearest_common_ancestor, placement_path_to_ancestor,
    point_to_ancestor_space, to_ancestor, Placement, PlacementError,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::{
        address::SpaceId,
        placement::Placement,
        space::{SpaceConfig, SpaceRegistry},
    };

    #[test]
    fn compose_golden_triple_maps_4_to_55() {
        let a = Placement::axis_aligned(vec![5], 2, 1, vec![32]);
        let b = Placement::axis_aligned(vec![10], 3, 1, vec![16]);
        let c = Placement::axis_aligned(vec![1], 1, 1, vec![8]);
        let left = compose(&[compose(&[a.clone(), b.clone()]).unwrap(), c.clone()]).unwrap();
        let right = compose(&[a.clone(), compose(&[b.clone(), c.clone()]).unwrap()]).unwrap();
        let flat = compose(&[a, b, c]).unwrap();
        let pt = vec![4u32];
        assert_eq!(to_ancestor(&pt, std::slice::from_ref(&left)).unwrap(), vec![55]);
        assert_eq!(to_ancestor(&pt, std::slice::from_ref(&right)).unwrap(), vec![55]);
        assert_eq!(to_ancestor(&pt, std::slice::from_ref(&flat)).unwrap(), vec![55]);
    }

    #[test]
    fn compose_is_associative_on_points() {
        let a = Placement::axis_aligned(vec![0], 2, 1, vec![32]);
        let b = Placement::axis_aligned(vec![4], 3, 1, vec![16]);
        let c = Placement::axis_aligned(vec![1], 1, 1, vec![8]);
        let left = compose(&[compose(&[a.clone(), b.clone()]).unwrap(), c.clone()]).unwrap();
        let right = compose(&[a, compose(&[b, c]).unwrap()]).unwrap();
        let pt = vec![5u32];
        assert_eq!(
            to_ancestor(&pt, std::slice::from_ref(&left)).unwrap(),
            to_ancestor(&pt, std::slice::from_ref(&right)).unwrap(),
        );
    }

    #[test]
    fn round_trip_child_ancestor_child_with_unit_scale() {
        let child = Placement::axis_aligned(vec![10], 1, 1, vec![64]);
        let pt = vec![20u32];
        let parent = transform_via_compose(&child, &pt);
        let back = inverse_point(&child, &parent).unwrap();
        assert_eq!(back, pt);
    }

    #[test]
    fn overflow_rejected_not_wrapped() {
        let a = Placement {
            offset: vec![0],
            scale_num: vec![u32::MAX],
            scale_den: vec![1],
            extent: vec![2],
            fixed_axes: vec![],
        };
        let b = Placement {
            offset: vec![1],
            scale_num: vec![u32::MAX],
            scale_den: vec![1],
            extent: vec![2],
            fixed_axes: vec![],
        };
        assert!(compose(&[a, b]).is_err());
    }

    #[test]
    fn nearest_common_ancestor_finds_root() {
        let mut reg = SpaceRegistry::new();
        reg.register(SpaceConfig::new(SpaceId(1), "root", 2)).unwrap();
        reg.register(
            SpaceConfig::new(SpaceId(2), "a", 2)
                .with_parent(SpaceId(1))
                .with_placement(Placement::axis_aligned(vec![0, 0], 1, 1, vec![64, 64])),
        )
        .unwrap();
        reg.register(
            SpaceConfig::new(SpaceId(3), "b", 2)
                .with_parent(SpaceId(1))
                .with_placement(Placement::axis_aligned(vec![32, 0], 1, 1, vec![64, 64])),
        )
        .unwrap();
        assert_eq!(
            nearest_common_ancestor(&reg, SpaceId(2), SpaceId(3)),
            Some(SpaceId(1))
        );
    }

    fn transform_via_compose(p: &Placement, pt: &[u32]) -> Vec<u32> {
        to_ancestor(pt, std::slice::from_ref(p)).unwrap()
    }

    fn inverse_point(p: &Placement, parent: &[u32]) -> Result<Vec<u32>, PlacementError> {
        if p.scale_num[0] != p.scale_den[0] {
            return Err(PlacementError::Overflow);
        }
        let child = (parent[0] as i64 - p.offset[0]) as u32;
        Ok(vec![child])
    }
}
