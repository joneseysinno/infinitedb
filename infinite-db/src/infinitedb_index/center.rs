//! Dyadic cell-center detection (INV-CENTER-PARITY / D-T5).

/// Per-axis primitive — never a reservation predicate on its own.
///
/// Returns the finest dyadic center level for `coord`, or `None` if not a center.
///
/// Level k in [1, bits_per_dim]: c is a level-k center iff
/// `c ≡ 2^(bits_per_dim − k − 1) (mod 2^(bits_per_dim − k))`.
pub fn is_dyadic_center(coord: u32, bits_per_dim: u32) -> Option<u32> {
    if bits_per_dim <= 1 {
        return None;
    }
    let mut finest: Option<u32> = None;
    for k in 1..bits_per_dim {
        let shift = bits_per_dim - k;
        let modulus = 1u32.checked_shl(shift)?;
        let half = 1u32.checked_shl(shift.saturating_sub(1))?;
        if coord % modulus == half {
            finest = Some(k);
        }
    }
    finest
}

/// Full-point center level: all axes share the same dyadic center level k ∈ [1, bits−1]
/// and each coordinate is a canonical cell center at that level.
pub fn dyadic_center_level(coords: &[u32], bits_per_dim: u32) -> Option<u32> {
    if coords.is_empty() || bits_per_dim <= 1 {
        return None;
    }
    let mut level: Option<u32> = None;
    for &coord in coords {
        let k = canonical_center_level(coord, bits_per_dim)?;
        level = match level {
            None => Some(k),
            Some(prev) if prev == k => Some(k),
            _ => return None,
        };
    }
    level
}

fn canonical_center_level(coord: u32, bits: u32) -> Option<u32> {
    let edge = 1u32 << (bits - 1);
    if coord == 0 || coord == edge {
        return None;
    }
    let tz = coord.trailing_zeros();
    if tz > bits - 2 {
        return None;
    }
    let odd = coord >> tz;
    if odd % 2 == 0 {
        return None;
    }
    let k = bits - tz - 1;
    let i = (odd - 1) / 2;
    if i >= (1u32 << (k - 1)) {
        return None;
    }
    Some(k)
}

/// Naive per-axis reference: literal modular arithmetic, no bit tricks.
pub fn is_dyadic_center_naive(coord: u32, bits_per_dim: u32) -> Option<u32> {
    if bits_per_dim <= 1 {
        return None;
    }
    let mut finest: Option<u32> = None;
    for k in 1..bits_per_dim {
        let modulus = 1u32 << (bits_per_dim - k);
        let half = modulus >> 1;
        if coord % modulus == half {
            finest = Some(k);
        }
    }
    finest
}

/// Naive full-point reference: nested loop over levels and axes.
pub fn dyadic_center_level_naive(coords: &[u32], bits_per_dim: u32) -> Option<u32> {
    if coords.is_empty() || bits_per_dim <= 1 {
        return None;
    }
    for k in 1..bits_per_dim {
        let shift = bits_per_dim - k;
        let modulus = 1u32 << shift;
        let half = 1u32 << shift.saturating_sub(1);
        if coords.iter().all(|&c| c % modulus == half) {
            let mut ok = true;
            for &c in coords {
                if canonical_center_level(c, bits_per_dim) != Some(k) {
                    ok = false;
                    break;
                }
            }
            if ok {
                return Some(k);
            }
        }
    }
    None
}

/// Parity-reserved center for a transformed extent in parent coordinates (D-F1).
///
/// Returns the center of the smallest dyadic cell in the parent that contains
/// `[min, max]` per axis, using the coarsest shared-prefix level across axes.
pub fn parity_center_for_extent(min: &[u32], max: &[u32], bits: u32) -> Vec<u32> {
    assert_eq!(min.len(), max.len());
    let n = min.len();
    let mut levels = Vec::with_capacity(n);
    for i in 0..n {
        levels.push(containing_level(min[i], max[i], bits));
    }
    let k = levels.into_iter().max().unwrap_or(1).max(1).min(bits - 1);
    let shift = bits - k;
    let half = 1u32 << (shift - 1);
    let mut coords = Vec::with_capacity(n);
    for i in 0..n {
        let cell_base = (min[i] >> shift) << shift;
        coords.push(cell_base + half);
    }
    coords
}

fn containing_level(min: u32, max: u32, bits: u32) -> u32 {
    if min > max {
        return containing_level(max, min, bits);
    }
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
    fn count_1d_centers_naive() {
        let mut count = 0u32;
        for c in 0..256u32 {
            if dyadic_center_level_naive(&[c], 8).is_some() {
                count += 1;
            }
        }
        assert_eq!(count, 127, "1D canonical full-point centers");
    }

    #[test]
    fn golden_dyadic_center_level() {
        assert_eq!(dyadic_center_level(&[64, 64], 8), Some(1));
        assert_eq!(dyadic_center_level(&[1, 1], 8), Some(7));
        assert_eq!(dyadic_center_level(&[1, 2], 8), None);
        assert_eq!(dyadic_center_level(&[128, 128], 8), None);
        assert_eq!(dyadic_center_level(&[0, 64], 8), None);
    }

    #[test]
    fn golden_naive_agrees_with_fast() {
        for case in [
            (&[64u32, 64], 8),
            (&[1, 1], 8),
            (&[1, 2], 8),
            (&[128, 128], 8),
            (&[0, 64], 8),
        ] {
            assert_eq!(
                dyadic_center_level(case.0, case.1),
                dyadic_center_level_naive(case.0, case.1),
            );
        }
    }

    #[test]
    fn golden_parity_center_for_extent() {
        let center = parity_center_for_extent(&[10, 10], &[73, 73], 8);
        assert_eq!(center, vec![64, 64]);
        assert_eq!(dyadic_center_level(&center, 8), Some(1));
    }

    #[test]
    fn matches_naive_u8_range() {
        for bits in 2..=8u32 {
            let max = 1u32 << bits;
            for c in 0..max.min(256) {
                assert_eq!(
                    is_dyadic_center(c, bits),
                    is_dyadic_center_naive(c, bits),
                    "mismatch at coord={c} bits={bits}"
                );
            }
        }
    }

    #[test]
    fn dyadic_center_level_matches_naive_exhaustive_2d_u4() {
        for x in 0..16u32 {
            for y in 0..16u32 {
                assert_eq!(
                    dyadic_center_level(&[x, y], 4),
                    dyadic_center_level_naive(&[x, y], 4),
                    "mismatch at ({x},{y})"
                );
            }
        }
    }

    #[test]
    fn dyadic_center_level_matches_naive_random_samples() {
        let samples: &[&[u32]] = &[
            &[3, 7, 11],
            &[15, 15, 15],
            &[2, 4, 8],
            &[9, 9, 9],
        ];
        for coords in samples {
            for bits in 4..=8u32 {
                assert_eq!(
                    dyadic_center_level(coords, bits),
                    dyadic_center_level_naive(coords, bits),
                );
            }
        }
    }
}
