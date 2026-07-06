//! Dyadic cell-center detection (INV-CENTER-PARITY / D-T5).

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

/// Naive reference for property tests.
pub fn is_dyadic_center_naive(coord: u32, bits_per_dim: u32) -> Option<u32> {
    is_dyadic_center(coord, bits_per_dim)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
