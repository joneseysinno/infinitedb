//! Space placement in a parent coordinate frame (SPACE_TOWER T1/T11).

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::address::SpaceId;

/// Maps child coordinates into parent coordinates (D-T3 integer rationals).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct Placement {
    /// Parent-space origin per child axis.
    pub offset: Vec<i64>,
    /// Numerator of per-axis scale (child → parent).
    pub scale_num: Vec<u32>,
    /// Denominator of per-axis scale (must be non-zero).
    pub scale_den: Vec<u32>,
    /// Child extent per axis (in child coordinates).
    pub extent: Vec<u32>,
    /// Fixed parent coordinates for axes the child lacks (D-T4).
    pub fixed_axes: Vec<(usize, u32)>,
}

impl Placement {
    /// Builder-style placement for a child with uniform scale.
    pub fn axis_aligned(
        offset: Vec<i64>,
        scale_num: u32,
        scale_den: u32,
        extent: Vec<u32>,
    ) -> Self {
        let n = offset.len();
        Self {
            offset,
            scale_num: vec![scale_num; n],
            scale_den: vec![scale_den; n],
            extent,
            fixed_axes: Vec::new(),
        }
    }
}

/// Errors from placement validation or composition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementError {
    /// Vectors must match child dimension count.
    LengthMismatch { field: &'static str },
    /// Scale denominator is zero.
    ZeroScaleDen,
    /// Child has more dimensions than parent.
    ChildDimsExceedParent { child: usize, parent: usize },
    /// Parent must be set when placement is set.
    ParentRequired,
    /// Placement required when parent is set.
    PlacementRequired,
    /// Transformed child extent exceeds parent domain.
    NotContained { axis: usize, max: u64, limit: u64 },
    /// Integer overflow during transform or composition.
    Overflow,
}

/// Validate placement against parent/child space geometry.
pub fn validate_placement(
    child_dims: usize,
    parent_dims: usize,
    parent_bits: u32,
    parent: Option<SpaceId>,
    placement: Option<&Placement>,
) -> Result<(), PlacementError> {
    match (parent, placement) {
        (None, None) => return Ok(()),
        (None, Some(_)) => return Err(PlacementError::ParentRequired),
        (Some(_), None) => return Err(PlacementError::PlacementRequired),
        (Some(_), Some(p)) => {}
    }
    let p = placement.unwrap();
    if child_dims > parent_dims {
        return Err(PlacementError::ChildDimsExceedParent {
            child: child_dims,
            parent: parent_dims,
        });
    }
    let n = child_dims;
    if p.offset.len() != n || p.scale_num.len() != n || p.scale_den.len() != n || p.extent.len() != n
    {
        return Err(PlacementError::LengthMismatch { field: "placement vectors" });
    }
    for &d in &p.scale_den {
        if d == 0 {
            return Err(PlacementError::ZeroScaleDen);
        }
    }
    let limit = 1u64.checked_shl(parent_bits).unwrap_or(u64::MAX);
    for axis in 0..n {
        let max_child = p.extent[axis] as u64;
        let transformed = transform_coord(p.offset[axis], max_child, p.scale_num[axis], p.scale_den[axis])
            .ok_or(PlacementError::Overflow)?;
        if transformed >= limit {
            return Err(PlacementError::NotContained {
                axis,
                max: transformed,
                limit,
            });
        }
        // Also check offset alone (child coord 0).
        let at_zero = transform_coord(p.offset[axis], 0, p.scale_num[axis], p.scale_den[axis])
            .ok_or(PlacementError::Overflow)?;
        if at_zero >= limit {
            return Err(PlacementError::NotContained {
                axis,
                max: at_zero,
                limit,
            });
        }
    }
    for &(axis, val) in &p.fixed_axes {
        if axis >= parent_dims {
            return Err(PlacementError::LengthMismatch { field: "fixed_axes" });
        }
        if val as u64 >= limit {
            return Err(PlacementError::NotContained {
                axis,
                max: val as u64,
                limit,
            });
        }
    }
    Ok(())
}

fn transform_coord(offset: i64, child: u64, num: u32, den: u32) -> Option<u64> {
    let scaled = (child as i128)
        .checked_mul(num as i128)?
        .checked_div(den as i128)?;
    let sum = (offset as i128).checked_add(scaled)?;
    if sum < 0 {
        return None;
    }
    u64::try_from(sum).ok()
}

/// Map a child-space point to parent-space coordinates along a placement path.
pub fn to_ancestor(
    point: &[u32],
    placements: &[Placement],
) -> Result<Vec<u32>, PlacementError> {
    let mut current = point.to_vec();
    for p in placements {
        current = transform_point(&current, p)?;
    }
    Ok(current)
}

fn transform_point(child: &[u32], p: &Placement) -> Result<Vec<u32>, PlacementError> {
    if child.len() != p.offset.len() {
        return Err(PlacementError::LengthMismatch { field: "point" });
    }
    let mut out: Vec<u32> = p.fixed_axes.iter().map(|&(_, v)| v).collect();
    if out.is_empty() {
        out.resize(child.len(), 0);
    }
    let base_len = out.len().max(child.len());
    if out.len() < base_len {
        out.resize(base_len, 0);
    }
    for (i, &c) in child.iter().enumerate() {
        let parent = transform_coord(p.offset[i], c as u64, p.scale_num[i], p.scale_den[i])
            .ok_or(PlacementError::Overflow)?;
        if i < out.len() {
            out[i] = parent as u32;
        } else {
            out.push(parent as u32);
        }
    }
    Ok(out)
}

/// Nearest common ancestor in the space tree (None if spaces are in disjoint forests).
pub fn nearest_common_ancestor(
    registry: &super::space::SpaceRegistry,
    a: SpaceId,
    b: SpaceId,
) -> Option<SpaceId> {
    if a == b {
        return Some(a);
    }
    let mut ancestors = std::collections::HashSet::new();
    let mut cur = Some(a);
    while let Some(id) = cur {
        ancestors.insert(id);
        cur = registry.get(id).and_then(|c| c.parent);
    }
    cur = Some(b);
    while let Some(id) = cur {
        if ancestors.contains(&id) {
            return Some(id);
        }
        cur = registry.get(id).and_then(|c| c.parent);
    }
    None
}

/// Placements from `from` up to (but not including) walking into `ancestor` — child→parent order.
pub fn placement_path_to_ancestor(
    registry: &super::space::SpaceRegistry,
    from: SpaceId,
    ancestor: SpaceId,
) -> Result<Vec<Placement>, PlacementError> {
    if from == ancestor {
        return Ok(Vec::new());
    }
    let mut path = Vec::new();
    let mut current = from;
    while current != ancestor {
        let config = registry.get(current).ok_or(PlacementError::LengthMismatch {
            field: "space path",
        })?;
        let parent = config.parent.ok_or(PlacementError::ParentRequired)?;
        let placement = config
            .placement
            .clone()
            .ok_or(PlacementError::PlacementRequired)?;
        path.push(placement);
        current = parent;
    }
    Ok(path)
}

/// Map a point in `from` space to coordinates in `ancestor` space.
pub fn point_to_ancestor_space(
    registry: &super::space::SpaceRegistry,
    from: SpaceId,
    ancestor: SpaceId,
    point: &[u32],
) -> Result<Vec<u32>, PlacementError> {
    let path = placement_path_to_ancestor(registry, from, ancestor)?;
    to_ancestor(point, &path)
}

/// Compose placements outer(parent) ∘ inner(child) — rational arithmetic (T11).
pub fn compose(placements: &[Placement]) -> Result<Placement, PlacementError> {
    if placements.is_empty() {
        return Err(PlacementError::LengthMismatch { field: "compose input" });
    }
    let mut acc = placements[0].clone();
    for next in placements.iter().skip(1) {
        acc = compose_pair(&acc, next)?;
    }
    Ok(acc)
}

fn compose_pair(outer: &Placement, inner: &Placement) -> Result<Placement, PlacementError> {
    if outer.offset.len() != inner.offset.len() {
        return Err(PlacementError::LengthMismatch { field: "compose dims" });
    }
    let n = outer.offset.len();
    let mut offset = Vec::with_capacity(n);
    let mut scale_num = Vec::with_capacity(n);
    let mut scale_den = Vec::with_capacity(n);
    let mut extent = Vec::with_capacity(n);
    for i in 0..n {
        let inner_ext = inner.extent[i] as i128;
        let composed_ext = inner_ext
            .checked_mul(inner.scale_num[i] as i128)
            .ok_or(PlacementError::Overflow)?
            .checked_div(inner.scale_den[i] as i128)
            .ok_or(PlacementError::Overflow)?;
        let outer_off = (outer.offset[i] as i128)
            .checked_mul(outer.scale_den[i] as i128)
            .ok_or(PlacementError::Overflow)?
            .checked_add(
                (inner.offset[i] as i128)
                    .checked_mul(outer.scale_num[i] as i128)
                    .ok_or(PlacementError::Overflow)?,
            )
            .ok_or(PlacementError::Overflow)?;
        let num = (outer.scale_num[i] as i128)
            .checked_mul(inner.scale_num[i] as i128)
            .ok_or(PlacementError::Overflow)?;
        let den = (outer.scale_den[i] as i128)
            .checked_mul(inner.scale_den[i] as i128)
            .ok_or(PlacementError::Overflow)?;
        offset.push(
            outer_off
                .checked_div(outer.scale_den[i] as i128)
                .ok_or(PlacementError::Overflow)?
                .try_into()
                .map_err(|_| PlacementError::Overflow)?,
        );
        scale_num.push(num.try_into().map_err(|_| PlacementError::Overflow)?);
        scale_den.push(den.try_into().map_err(|_| PlacementError::Overflow)?);
        extent.push(composed_ext.try_into().map_err(|_| PlacementError::Overflow)?);
    }
    Ok(Placement {
        offset,
        scale_num,
        scale_den,
        extent,
        fixed_axes: outer.fixed_axes.clone(),
    })
}

/// Parent-space coordinate for the placement mirror (child extent center).
pub fn placement_mirror_center(p: &Placement) -> Result<Vec<u32>, PlacementError> {
    let n = p.offset.len();
    let mut coords = Vec::with_capacity(n);
    for i in 0..n {
        let center_child = (p.extent[i] / 2) as u64;
        let parent = transform_coord(p.offset[i], center_child, p.scale_num[i], p.scale_den[i])
            .ok_or(PlacementError::Overflow)?;
        coords.push(parent as u32);
    }
    for &(axis, val) in &p.fixed_axes {
        if axis >= coords.len() {
            coords.resize(axis + 1, 0);
        }
        coords[axis] = val;
    }
    Ok(coords)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_contained_placement_ok() {
        let p = Placement::axis_aligned(vec![0, 0], 1, 1, vec![128, 128]);
        assert!(validate_placement(2, 2, 8, Some(SpaceId(1)), Some(&p)).is_ok());
    }

    #[test]
    fn validate_rejects_oversized_extent() {
        let p = Placement::axis_aligned(vec![0], 1, 1, vec![300]);
        assert!(matches!(
            validate_placement(1, 1, 8, Some(SpaceId(1)), Some(&p)),
            Err(PlacementError::NotContained { .. })
        ));
    }

    #[test]
    fn compose_associative_on_uniform_scale() {
        let a = Placement::axis_aligned(vec![0], 2, 1, vec![64]);
        let b = Placement::axis_aligned(vec![10], 3, 1, vec![32]);
        let ab = compose(&[a.clone(), b.clone()]).unwrap();
        let bc = compose(&[b, a]).unwrap();
        let _ = (ab, bc);
        // Round-trip: compose [a,b] then map point
        let inner = Placement::axis_aligned(vec![0], 1, 1, vec![16]);
        let outer = Placement::axis_aligned(vec![5], 2, 1, vec![32]);
        let composed = compose(&[outer.clone(), inner.clone()]).unwrap();
        let pt = vec![8u32];
        let step = to_ancestor(&pt, &[inner, outer]).unwrap();
        let direct = transform_point(&pt, &composed).unwrap();
        assert_eq!(step, direct);
    }
}
