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
        (Some(_), Some(_)) => {}
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

/// Parent-space transformed extent bounds `(min, max)` per parent axis.
pub fn extent_in_parent(p: &Placement) -> (Vec<i64>, Vec<i64>) {
    let n = p.offset.len();
    let parent_dims = n
        .max(
            p.fixed_axes
                .iter()
                .map(|&(axis, _)| axis + 1)
                .max()
                .unwrap_or(0),
        );
    let mut mins = vec![i64::MAX; parent_dims];
    let mut maxs = vec![i64::MIN; parent_dims];
    for i in 0..n {
        let min_p = transform_coord(p.offset[i], 0, p.scale_num[i], p.scale_den[i])
            .expect("extent min transform");
        let max_p = transform_coord(p.offset[i], p.extent[i] as u64, p.scale_num[i], p.scale_den[i])
            .expect("extent max transform");
        mins[i] = min_p as i64;
        maxs[i] = max_p as i64;
    }
    for &(axis, val) in &p.fixed_axes {
        mins[axis] = val as i64;
        maxs[axis] = val as i64;
    }
    (mins, maxs)
}

/// Inverse-transform a parent-frame bbox into child coordinates, clamped to extent.
pub fn bbox_to_child(
    p: &Placement,
    min: &[u32],
    max: &[u32],
) -> Option<(Vec<u32>, Vec<u32>)> {
    let n = p.offset.len();
    if min.len() < n || max.len() < n {
        return None;
    }
    for &(axis, val) in &p.fixed_axes {
        if axis < min.len() && (min[axis] > val || max[axis] < val) {
            return None;
        }
    }
    let mut child_min = Vec::with_capacity(n);
    let mut child_max = Vec::with_capacity(n);
    for i in 0..n {
        let inv = |parent: u32| -> Option<u32> {
            let num = p.scale_num[i] as i128;
            let den = p.scale_den[i] as i128;
            if num == 0 {
                return None;
            }
            let shifted = (parent as i128 - p.offset[i] as i128).checked_mul(den)?;
            if shifted < 0 {
                return None;
            }
            let child = shifted / num;
            u32::try_from(child).ok()
        };
        let c0 = inv(min[i])?;
        let c1 = inv(max[i])?;
        let lo = c0.min(c1).min(p.extent[i]);
        let hi = c0.max(c1).min(p.extent[i]);
        if lo > hi {
            return None;
        }
        child_min.push(lo);
        child_max.push(hi);
    }
    Some((child_min, child_max))
}

/// Parent-space coordinate for the placement mirror (child extent parity center).
///
/// Prefer [`crate::infinitedb_index::center::parity_center_for_extent`] with
/// [`extent_in_parent`] when parent precision is known at the call site.
pub fn placement_mirror_center(p: &Placement, parent_bits: u32) -> Result<Vec<u32>, PlacementError> {
    let (min_i, max_i) = extent_in_parent(p);
    let min: Vec<u32> = min_i.into_iter().map(|v| v as u32).collect();
    let max: Vec<u32> = max_i.into_iter().map(|v| v as u32).collect();
    let mut coords = mirror_parity_center(&min, &max, parent_bits);
    for &(axis, val) in &p.fixed_axes {
        if axis >= coords.len() {
            coords.resize(axis + 1, 0);
        }
        coords[axis] = val;
    }
    Ok(coords)
}

fn mirror_parity_center(min: &[u32], max: &[u32], bits: u32) -> Vec<u32> {
    assert_eq!(min.len(), max.len());
    let levels: Vec<u32> = min
        .iter()
        .zip(max.iter())
        .map(|(&lo, &hi)| mirror_containing_level(lo, hi, bits))
        .collect();
    let k = levels.into_iter().max().unwrap_or(1).max(1).min(bits - 1);
    let shift = bits - k;
    let half = 1u32 << (shift - 1);
    min.iter()
        .zip(max.iter())
        .map(|(&lo, _)| {
            let cell_base = (lo >> shift) << shift;
            cell_base + half
        })
        .collect()
}

fn mirror_containing_level(min: u32, max: u32, bits: u32) -> u32 {
    let (min, max) = if min <= max { (min, max) } else { (max, min) };
    let half_domain = 1u32 << (bits - 1);
    if min < half_domain && max >= half_domain {
        return 1;
    }
    let mut shared = 0u32;
    for bit in (0..bits).rev() {
        let mask = 1u32 << bit;
        if (min & mask) == (max & mask) {
            shared += 1;
        } else {
            break;
        }
    }
    shared.max(1)
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
    fn compose_associative_golden_triple() {
        let a = Placement::axis_aligned(vec![5], 2, 1, vec![32]);
        let b = Placement::axis_aligned(vec![10], 3, 1, vec![16]);
        let c = Placement::axis_aligned(vec![1], 1, 1, vec![8]);
        let left = compose(&[compose(&[a.clone(), b.clone()]).unwrap(), c.clone()]).unwrap();
        let right = compose(&[a.clone(), compose(&[b.clone(), c.clone()]).unwrap()]).unwrap();
        let flat = compose(&[a, b, c]).unwrap();
        assert_eq!(left.offset, right.offset);
        assert_eq!(left.offset, flat.offset);
        let pt = vec![4u32];
        let via_left = to_ancestor(&pt, std::slice::from_ref(&left)).unwrap();
        let via_right = to_ancestor(&pt, std::slice::from_ref(&right)).unwrap();
        let via_flat = transform_point(&pt, &flat).unwrap();
        assert_eq!(via_left, vec![55]);
        assert_eq!(via_right, vec![55]);
        assert_eq!(via_flat, vec![55]);
    }
}
