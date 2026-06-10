//! Hilbert key interval decomposition for bounding-box queries.

use crate::infinitedb_core::address::DimensionVector;
use crate::infinitedb_index::composite::KeyConfig;
use crate::infinitedb_index::key::hilbert_key_for;

/// Maximum orthant splits before emitting a conservative covering interval.
pub const MAX_INTERVALS: usize = 32;

/// Maximum recursion depth for orthant decomposition.
pub const MAX_DEPTH: u32 = 12;

/// Disjoint key intervals whose union conservatively covers `bbox`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyInterval {
    pub lo: u128,
    pub hi: u128,
}

/// Decompose a bounding box into Hilbert key intervals for pruning.
pub fn decompose_bbox(
    min: &DimensionVector,
    max: &DimensionVector,
    bits_per_dim: u32,
) -> Vec<KeyInterval> {
    assert_eq!(min.dims(), max.dims());
    let dims = min.dims();
    if dims == 0 {
        return vec![KeyInterval { lo: 0, hi: u128::MAX }];
    }

    let max_coord = (1u32 << bits_per_dim.min(31)) - 1;
    let domain_min = vec![0u32; dims];
    let domain_max = vec![max_coord; dims];
    let mut out = Vec::new();
    decompose_cell(
        &domain_min,
        &domain_max,
        min,
        max,
        bits_per_dim,
        0,
        &mut out,
    );
    if out.is_empty() {
        push_bbox_hull(min, max, bits_per_dim, &mut out);
    }
    out.sort_by_key(|i| i.lo);
    out.dedup_by(|a, b| a.lo == b.lo && a.hi == b.hi);
    out
}

fn decompose_cell(
    cell_min: &[u32],
    cell_max: &[u32],
    bbox_min: &DimensionVector,
    bbox_max: &DimensionVector,
    bits_per_dim: u32,
    depth: u32,
    out: &mut Vec<KeyInterval>,
) {
    if out.len() >= MAX_INTERVALS {
        return;
    }

    match cell_relation(cell_min, cell_max, bbox_min, bbox_max) {
        CellRelation::Outside => {}
        CellRelation::Inside => {
            let lo = hilbert_key_for(&coords_vec(cell_min), KeyConfig { bits_per_dim });
            let hi = hilbert_key_for(&coords_vec(cell_max), KeyConfig { bits_per_dim });
            let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
            out.push(KeyInterval { lo, hi });
        }
        CellRelation::Partial => {
            if depth >= MAX_DEPTH || out.len() >= MAX_INTERVALS {
                push_bbox_hull(bbox_min, bbox_max, bits_per_dim, out);
                return;
            }
            let split_dim = depth as usize % cell_min.len();
            let mid = cell_min[split_dim]
                .saturating_add(cell_max[split_dim])
                / 2;
            let mut left_max = cell_max.to_vec();
            left_max[split_dim] = mid;
            let mut right_min = cell_min.to_vec();
            right_min[split_dim] = mid.saturating_add(1);
            decompose_cell(
                cell_min,
                &left_max,
                bbox_min,
                bbox_max,
                bits_per_dim,
                depth + 1,
                out,
            );
            decompose_cell(
                &right_min,
                cell_max,
                bbox_min,
                bbox_max,
                bits_per_dim,
                depth + 1,
                out,
            );
        }
    }
}

enum CellRelation {
    Inside,
    Outside,
    Partial,
}

fn cell_relation(
    cell_min: &[u32],
    cell_max: &[u32],
    bbox_min: &DimensionVector,
    bbox_max: &DimensionVector,
) -> CellRelation {
    let mut inside = true;
    for i in 0..cell_min.len() {
        let bmin = bbox_min.coords[i];
        let bmax = bbox_max.coords[i];
        if cell_max[i] < bmin || cell_min[i] > bmax {
            return CellRelation::Outside;
        }
        if cell_min[i] < bmin || cell_max[i] > bmax {
            inside = false;
        }
    }
    if inside {
        CellRelation::Inside
    } else {
        CellRelation::Partial
    }
}

fn coords_vec(coords: &[u32]) -> DimensionVector {
    DimensionVector::new(coords.to_vec())
}

/// Conservative interval from all bbox corners (over-covering, never under-covering).
fn push_bbox_hull(
    min: &DimensionVector,
    max: &DimensionVector,
    bits_per_dim: u32,
    out: &mut Vec<KeyInterval>,
) {
    let dims = min.dims();
    let mut keys = Vec::with_capacity(1 << dims);
    for mask in 0..(1usize << dims) {
        let mut coords = Vec::with_capacity(dims);
        for d in 0..dims {
            coords.push(if mask & (1 << d) != 0 {
                max.coords[d]
            } else {
                min.coords[d]
            });
        }
        keys.push(hilbert_key_for(
            &DimensionVector::new(coords),
            KeyConfig { bits_per_dim },
        ));
    }
    let lo = *keys.iter().min().unwrap_or(&0);
    let hi = *keys.iter().max().unwrap_or(&0);
    let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
    out.push(KeyInterval { lo, hi });
}

/// True when `key` falls inside any interval (inclusive).
pub fn key_in_intervals(key: u128, intervals: &[KeyInterval]) -> bool {
    intervals.iter().any(|i| key >= i.lo && key <= i.hi)
}

/// True when block `[min_key, max_key]` overlaps any interval.
pub fn block_overlaps_intervals(min_key: u128, max_key: u128, intervals: &[KeyInterval]) -> bool {
    intervals
        .iter()
        .any(|i| min_key <= i.hi && max_key >= i.lo)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bbox_corner_keys_are_covered() {
        let min = DimensionVector::new(vec![10, 10]);
        let max = DimensionVector::new(vec![50, 50]);
        let intervals = decompose_bbox(&min, &max, 8);
        assert!(!intervals.is_empty());
        for x in [10u32, 50] {
            for y in [10u32, 50] {
                let point = DimensionVector::new(vec![x, y]);
                let key = hilbert_key_for(&point, KeyConfig::STANDARD);
                assert!(
                    key_in_intervals(key, &intervals),
                    "corner ({x},{y}) key {key} not covered"
                );
            }
        }
    }
}
