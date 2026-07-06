//! Top-aligned normalized curve addresses (D-T7 / T13).

use crate::infinitedb_core::address::DimensionVector;
use super::composite::KeyConfig;
use super::hilbert;

/// A top-aligned normalized curve address — the sole constructor for record keys.
///
/// Semantics: names the half-open dyadic cell `[d/D, (d+1)/D)` with radix at bit 128.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CurveAddress(u128);

impl CurveAddress {
    /// Encode a spatial point at the given precision (INV-ADDR-ALIGN).
    pub fn from_point(point: &DimensionVector, config: KeyConfig) -> Self {
        if point.coords.is_empty() {
            return Self(0);
        }
        let raw = {
            let coords: Vec<u32> = point.coords.clone();
            hilbert::encode(&coords, config.bits_per_dim)
        };
        Self::from_raw_index(raw, config, point.coords.len())
    }

    /// Wrap a bottom-aligned Hilbert index with top alignment.
    pub fn from_raw_index(raw: u128, config: KeyConfig, dims: usize) -> Self {
        let used = dims as u32 * config.bits_per_dim;
        assert!(used <= 128, "dims * bits_per_dim must be <= 128");
        let shift = 128u32.saturating_sub(used);
        Self(raw << shift)
    }

    /// Raw top-aligned u128 key for storage and sharding.
    pub fn raw(self) -> u128 {
        self.0
    }

    /// Bottom-aligned Hilbert index (d in d/D).
    pub fn raw_index(self, config: KeyConfig, dims: usize) -> u128 {
        let used = dims as u32 * config.bits_per_dim;
        let shift = 128u32.saturating_sub(used);
        self.0 >> shift
    }

    /// Prefix at dyadic level `level` (1 = coarsest within space precision).
    pub fn cell_prefix(self, level: u32, config: KeyConfig, dims: usize) -> u128 {
        let total = dims as u32 * config.bits_per_dim;
        let level = level.min(total);
        let keep = total - level;
        if keep >= 128 {
            return 0;
        }
        let mask = if keep == 0 {
            u128::MAX
        } else {
            u128::MAX << keep
        };
        self.0 & mask
    }

    /// Owning interval as `(lo, hi)` inclusive top-aligned keys.
    pub fn owning_interval(self, config: KeyConfig, dims: usize) -> (u128, u128) {
        let total = dims as u32 * config.bits_per_dim;
        let shift = 128u32.saturating_sub(total);
        let cell_bits = shift;
        if cell_bits == 0 {
            return (self.0, self.0);
        }
        let cell_mask = (1u128 << cell_bits) - 1;
        let base = self.0 & !cell_mask;
        (base, base | cell_mask)
    }
}

/// Shift amount for top-aligning keys in a space.
pub fn top_align_shift(config: KeyConfig, dims: usize) -> u32 {
    128u32.saturating_sub(dims as u32 * config.bits_per_dim)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::DimensionVector;

    #[test]
    fn truncation_prefix_property() {
        let pt4 = DimensionVector::new(vec![5, 7]);
        let pt8 = DimensionVector::new(vec![5 * 16, 7 * 16]);
        let k4 = CurveAddress::from_point(&pt4, KeyConfig { bits_per_dim: 4 });
        let k8 = CurveAddress::from_point(&pt8, KeyConfig { bits_per_dim: 8 });
        let idx4 = k4.raw_index(KeyConfig { bits_per_dim: 4 }, 2);
        let idx8 = k8.raw_index(KeyConfig { bits_per_dim: 8 }, 2);
        assert_eq!(idx4, idx8 >> ((8 - 4) * 2));
    }

    #[test]
    fn low_precision_populates_high_shards() {
        let mut shards = std::collections::HashSet::new();
        for x in 0..16u32 {
            for y in 0..16u32 {
                let pt = DimensionVector::new(vec![x * 16, y * 16]);
                let addr = CurveAddress::from_point(&pt, KeyConfig { bits_per_dim: 8 });
                let shard = (addr.raw() >> (128 - 4)) as u32;
                shards.insert(shard);
            }
        }
        assert!(shards.len() > 1, "degenerate shard regression");
    }
}
