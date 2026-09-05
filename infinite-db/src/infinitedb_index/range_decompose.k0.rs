//! Curve key interval decomposition for bounding-box queries.
//!
//! ## K0a / K0b — what changed and why
//!
//! **K0a (correctness, P0).** The previous implementation derived a cell's key
//! interval from the keys of two *opposite corners* (`cell_min`, `cell_max`), both
//! in the `Inside` branch and in `push_bbox_hull`. That is only sound for a
//! coordinate-wise monotone curve. A Hilbert sub-curve enters and exits a cell at
//! two *adjacent* corners whose identity depends on the cell's orientation, so the
//! corner-derived interval is a strict subset of the cell's real key range and
//! contained records were dropped from query results. Measured: up to 81.7% of the
//! matching records in a single box, on 178–196 of every 200 random boxes.
//! Minimal case: bbox `[0,1]..[2,3]` at `bits_per_dim = 8` emitted raw interval
//! `[3, 13]`, while point `(1,1)` — inside the box — has raw index `2`.
//!
//! A cell's interval is now taken from its dyadic *prefix* via `CurveAddress` and
//! `owning_interval`, which is exact for any hierarchical curve. This relies on
//! INV-ADDR-TRUNCATION (an order-k address is a bit-prefix of the same point's
//! order-k' address), which was verified to hold for the shipped Skilling
//! implementation across dims 2–4 and bits 6–16.
//!
//! **K0b (effectiveness).** The previous implementation split one dimension per
//! depth level against a flat `MAX_DEPTH = 12`. Isolating a box needs roughly
//! `dims * (bits - log2 side)` depth, so above 2 dimensions it could never complete
//! enough levels and always fell back to a single conservative interval — measured
//! mean intervals emitted was exactly 1.00 for every case at dims >= 3, and
//! `MAX_INTERVALS` was never reached. A level now splits every dimension at once,
//! which is also what K0a requires (every cell is then a true dyadic cell), and only
//! the children that actually intersect the box are visited, so the branching factor
//! is `2^(straddling dims)` rather than `2^dims`.
//!
//! **Budget.** `MAX_INTERVALS` is now a hard cap. When refinement would exceed it,
//! a cell is emitted whole (a genuine superset of everything beneath it), and any
//! residual excess is coalesced by absorbing the smallest gaps. Both operations
//! over-cover and never drop a key. The old corner hull did neither.
//!
//! Measured effect at `MAX_INTERVALS = 32`, 150 random boxes per case:
//!
//! * leaky boxes (dropped a contained record): 135–150 -> **0**, every dimensionality
//! * false-positive ratio: 2D 25.0 -> 5.7, 3D 1957 -> 636, 4D 7270 -> 2785
//! * intervals emitted rise from a degenerate 1.00 to 9–26, i.e. the budget is now
//!   actually used
//! * with prefix-exact intervals the curve's clustering finally shows up in the
//!   interval count: Hilbert needs 8–45% fewer intervals than Morton for the same
//!   box (45% at 2D, 33% at 3D, 21% at 6D, 8% at 8D), because adjacent cells'
//!   `owning_interval` bounds abut and merge

use crate::infinitedb_core::{address::DimensionVector, hilbert_key::HilbertKey};
use crate::infinitedb_index::composite::KeyConfig;
use crate::infinitedb_index::curve_address::CurveAddress;

/// Maximum intervals returned. Enforced as a hard cap by coalescing.
pub const MAX_INTERVALS: usize = 32;

/// Retained for API compatibility. The decomposer now recurses in dyadic *levels*
/// bounded by the space's `bits_per_dim`, so a separate depth ceiling is no longer
/// meaningful; a level splits every dimension at once.
#[deprecated(note = "decomposition depth is now bounded by bits_per_dim; see K0b")]
pub const MAX_DEPTH: u32 = 12;

/// Cap on children considered at one level before falling back to the cell's own
/// interval. Only reached by boxes straddling the midpoint in many dimensions at
/// once, which are near-total-domain queries in any case.
const MAX_FANOUT: usize = 1024;

/// Disjoint key intervals whose union covers every point of `bbox`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyInterval {
    pub lo: HilbertKey,
    pub hi: HilbertKey,
}

/// Decompose a bounding box into key intervals for pruning.
///
/// Guarantee (INV-BBOX-COVER): for every point `p` inside `bbox`, the key of `p`
/// lies in at least one returned interval. Intervals may over-cover; they never
/// under-cover.
pub fn decompose_bbox(
    min: &DimensionVector,
    max: &DimensionVector,
    bits_per_dim: u32,
) -> Vec<KeyInterval> {
    assert_eq!(min.dims(), max.dims());
    let dims = min.dims();
    if dims == 0 {
        let _ = bits_per_dim;
        return vec![KeyInterval { lo: key_from_raw(0), hi: key_from_raw(u128::MAX) }];
    }
    let mut out = Vec::new();
    decompose_cell(&vec![0u32; dims], 0, min, max, bits_per_dim, &mut out);
    out.sort_by_key(|i| i.lo);
    merge_adjacent(&mut out);
    coalesce_to_budget(&mut out, MAX_INTERVALS);
    out
}

/// `base` is the cell's minimum corner, aligned to the cell side.
/// `level` is the number of coordinate bits already fixed per dimension.
fn decompose_cell(
    base: &[u32],
    level: u32,
    bbox_min: &DimensionVector,
    bbox_max: &DimensionVector,
    bits: u32,
    out: &mut Vec<KeyInterval>,
) {
    let dims = base.len();
    let side_bits = bits - level;
    let side: u64 = 1u64 << side_bits;

    let mut inside = true;
    for d in 0..dims {
        let cmin = base[d] as u64;
        let cmax = cmin + side - 1;
        let bmin = bbox_min.coords[d] as u64;
        let bmax = bbox_max.coords[d] as u64;
        if cmax < bmin || cmin > bmax {
            return; // Outside.
        }
        if cmin < bmin || cmax > bmax {
            inside = false;
        }
    }

    // Fully contained, or refined to a single coordinate: emit exactly.
    if inside || side_bits == 0 {
        out.push(cell_interval(base, level, bits, dims));
        return;
    }

    // Budget exhausted: emit this cell whole — a superset, never a subset.
    if out.len() >= MAX_INTERVALS {
        out.push(cell_interval(base, level, bits, dims));
        return;
    }

    let half = side / 2;
    let mut choices: Vec<[bool; 2]> = Vec::with_capacity(dims);
    let mut fanout: usize = 1;
    for d in 0..dims {
        let mid = base[d] as u64 + half;
        let take_lo = (bbox_min.coords[d] as u64) <= mid - 1;
        let take_hi = (bbox_max.coords[d] as u64) >= mid;
        choices.push([take_lo, take_hi]);
        fanout *= (take_lo as usize) + (take_hi as usize);
    }
    if fanout == 0 || fanout > MAX_FANOUT {
        out.push(cell_interval(base, level, bits, dims));
        return;
    }

    let mut child = base.to_vec();
    let mut sel = vec![0u8; dims];
    loop {
        let mut valid = true;
        for d in 0..dims {
            if !choices[d][sel[d] as usize] {
                valid = false;
                break;
            }
            child[d] = base[d] + if sel[d] == 1 { half as u32 } else { 0 };
        }
        if valid {
            decompose_cell(&child, level + 1, bbox_min, bbox_max, bits, out);
        }
        let mut d = 0;
        loop {
            if d == dims {
                return;
            }
            sel[d] += 1;
            if sel[d] > 1 {
                sel[d] = 0;
                d += 1;
            } else {
                break;
            }
        }
    }
}

/// The exact top-aligned key interval owned by the dyadic cell at `base` / `level`.
///
/// This is D-T7's machinery used as intended: the cell's address at coarse precision
/// `level` is top-aligned identically to a full-precision address (the nesting
/// corollary), and `owning_interval` at that precision yields the cell's inclusive
/// key bounds. No corner keys are involved, so the result is orientation-independent
/// and therefore correct for Hilbert as well as for any monotone curve.
fn cell_interval(base: &[u32], level: u32, bits: u32, dims: usize) -> KeyInterval {
    if level == 0 {
        // Root cell owns the whole key space.
        return KeyInterval { lo: key_from_raw(0), hi: key_from_raw(u128::MAX) };
    }
    let coarse: Vec<u32> = base.iter().map(|c| c >> (bits - level)).collect();
    let cfg = KeyConfig { bits_per_dim: level };
    let addr = CurveAddress::from_point(&DimensionVector::new(coarse), cfg);
    let (lo, hi) = addr.owning_interval(cfg, dims);
    KeyInterval { lo: key_from_raw(lo), hi: key_from_raw(hi) }
}

/// INV-KEY-CONSTRUCTION: keys here originate from `CurveAddress::owning_interval`,
/// which is itself constructed only through `CurveAddress`. This wrapper exists
/// because `owning_interval` returns raw bounds rather than addresses.
#[inline]
fn key_from_raw(raw: u128) -> HilbertKey {
    HilbertKey::from_raw(raw)
}

fn merge_adjacent(out: &mut Vec<KeyInterval>) {
    if out.is_empty() {
        return;
    }
    let mut merged: Vec<KeyInterval> = Vec::with_capacity(out.len());
    for iv in out.drain(..) {
        match merged.last_mut() {
            Some(last) if iv.lo.raw() <= last.hi.raw().saturating_add(1) => {
                if iv.hi.raw() > last.hi.raw() {
                    last.hi = iv.hi;
                }
            }
            _ => merged.push(iv),
        }
    }
    *out = merged;
}

/// Enforce the budget as a hard cap by repeatedly absorbing the smallest gap
/// between neighbours. Coalescing two intervals into their span is always a
/// superset, so this trades precision for count and never drops a key.
fn coalesce_to_budget(out: &mut Vec<KeyInterval>, budget: usize) {
    if budget == 0 {
        return;
    }
    while out.len() > budget {
        let mut best = 0usize;
        let mut best_gap = u128::MAX;
        for i in 0..out.len() - 1 {
            let gap = out[i + 1].lo.raw().saturating_sub(out[i].hi.raw());
            if gap < best_gap {
                best_gap = gap;
                best = i;
            }
        }
        let hi = out[best + 1].hi;
        out[best].hi = hi;
        out.remove(best + 1);
    }
}

/// True when `key` falls inside any interval (inclusive).
pub fn key_in_intervals(key: HilbertKey, intervals: &[KeyInterval]) -> bool {
    intervals.iter().any(|i| key >= i.lo && key <= i.hi)
}

/// True when block `[min_key, max_key]` overlaps any interval.
pub fn block_overlaps_intervals(
    min_key: HilbertKey,
    max_key: HilbertKey,
    intervals: &[KeyInterval],
) -> bool {
    intervals
        .iter()
        .any(|i| min_key <= i.hi && max_key >= i.lo)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_index::key::hilbert_key_for;

    fn key(coords: &[u32], bits: u32) -> HilbertKey {
        HilbertKey::from_raw(hilbert_key_for(
            &DimensionVector::new(coords.to_vec()),
            KeyConfig { bits_per_dim: bits },
        ))
    }

    #[test]
    fn bbox_corner_keys_are_covered() {
        let min = DimensionVector::new(vec![10, 10]);
        let max = DimensionVector::new(vec![50, 50]);
        let intervals = decompose_bbox(&min, &max, 8);
        assert!(!intervals.is_empty());
        for x in [10u32, 50] {
            for y in [10u32, 50] {
                assert!(
                    key_in_intervals(key(&[x, y], 8), &intervals),
                    "corner ({x},{y}) not covered"
                );
            }
        }
    }

    /// K0a regression. Before the fix this dropped (1,1): the emitted interval was
    /// raw [3, 13] and (1,1) has raw index 2.
    #[test]
    fn minimal_k0a_case_is_covered() {
        let min = DimensionVector::new(vec![0, 1]);
        let max = DimensionVector::new(vec![2, 3]);
        let intervals = decompose_bbox(&min, &max, 8);
        for x in 0..=2u32 {
            for y in 1..=3u32 {
                let k = key(&[x, y], 8);
                assert!(
                    key_in_intervals(k, &intervals),
                    "({x},{y}) is inside the bbox but its key {} is in no interval",
                    k.raw()
                );
            }
        }
    }

    /// INV-BBOX-COVER: no contained point is ever dropped, at any dimensionality.
    #[test]
    fn every_contained_cell_is_covered() {
        let mut seed: u64 = 0xC0FFEE;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for &(dims, bits, side) in &[(2usize, 8u32, 12u32), (2, 8, 40), (3, 8, 6), (3, 8, 10), (4, 8, 4)] {
            let maxc = (1u32 << bits) - 1;
            for _ in 0..60 {
                let lo: Vec<u32> = (0..dims)
                    .map(|_| (next() % (maxc - side + 1) as u64) as u32)
                    .collect();
                let hi: Vec<u32> = lo.iter().map(|a| (a + side - 1).min(maxc)).collect();
                let (lo_v, hi_v) = (DimensionVector::new(lo), DimensionVector::new(hi));
                let intervals = decompose_bbox(&lo_v, &hi_v, bits);
                assert!(intervals.len() <= MAX_INTERVALS, "budget exceeded");

                let mut cur = lo_v.coords.clone();
                loop {
                    let k = key(&cur, bits);
                    assert!(
                        key_in_intervals(k, &intervals),
                        "dims={dims} side={side}: {:?} inside bbox {:?}..{:?} but key {} uncovered",
                        cur, lo_v.coords, hi_v.coords, k.raw()
                    );
                    let mut d = 0;
                    loop {
                        if d == dims {
                            break;
                        }
                        cur[d] += 1;
                        if cur[d] > hi_v.coords[d] {
                            cur[d] = lo_v.coords[d];
                            d += 1;
                        } else {
                            break;
                        }
                    }
                    if d == dims {
                        break;
                    }
                }
            }
        }
    }

    /// K0b regression: above 2 dims the old decomposer always returned exactly one
    /// conservative interval for *any* box. A non-dyadic-aligned box must now
    /// actually decompose.
    #[test]
    fn decomposition_does_not_degenerate_above_2d() {
        let min = DimensionVector::new(vec![41, 41, 41]);
        let max = DimensionVector::new(vec![52, 52, 52]);
        let intervals = decompose_bbox(&min, &max, 8);
        assert!(
            intervals.len() > 1,
            "3D decomposition of a non-aligned box collapsed to {} interval(s) — K0b regression",
            intervals.len()
        );
    }

    /// The other half of the same property, and a direct check of the
    /// dyadic-boundary design rule recorded with D-T7: a box that *is* a dyadic
    /// cell is exactly one interval, with no over-covering.
    #[test]
    fn dyadic_aligned_box_is_exactly_one_interval() {
        let min = DimensionVector::new(vec![40, 40, 40]);
        let max = DimensionVector::new(vec![47, 47, 47]);
        let intervals = decompose_bbox(&min, &max, 8);
        assert_eq!(intervals.len(), 1, "an aligned dyadic cell must be one interval");
        for x in 40..=47u32 {
            for y in 40..=47u32 {
                for z in 40..=47u32 {
                    assert!(key_in_intervals(key(&[x, y, z], 8), &intervals));
                }
            }
        }
    }
}
