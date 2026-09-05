//! Per-space occupied-key depth statistic (T9).
//!
//! Pairwise divergence: because keys are top-aligned and the observed set's
//! pairwise-max prefix divergence is achieved by the (min, max) pair under
//! integer order, tracking two u128s is a lossless O(1) fold of the statistic.

use std::collections::HashMap;

use parking_lot::Mutex;

use crate::infinitedb_core::{
    address::SpaceId,
    void::VoidOr,
};
use crate::infinitedb_index::composite::KeyConfig;

#[derive(Debug, Clone, Copy)]
struct SpaceFold {
    count: u64,
    min_key: u128,
    max_key: u128,
    initialized: bool,
    bits_per_dim: u32,
    dims: usize,
}

impl Default for SpaceFold {
    fn default() -> Self {
        Self {
            count: 0,
            min_key: 0,
            max_key: 0,
            initialized: false,
            bits_per_dim: 8,
            dims: 2,
        }
    }
}

/// Incremental density fold per space.
#[derive(Debug, Default)]
pub struct DensityTracker {
    inner: Mutex<HashMap<SpaceId, SpaceFold>>,
}

/// Observed density for a space that has seen at least one key (INV-VOID-DIV-UNDEFINED).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedDensity {
    pub record_count: u64,
    /// Deepest dyadic tower level at which two keys differ (0 = uniform single key).
    pub max_occupied_depth: u32,
}

impl DensityTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe_key(&self, space: SpaceId, key: u128, config: KeyConfig, dims: usize) {
        let mut map = self.inner.lock();
        let entry = map.entry(space).or_default();
        entry.bits_per_dim = config.bits_per_dim;
        entry.dims = dims;
        if !entry.initialized {
            entry.min_key = key;
            entry.max_key = key;
            entry.initialized = true;
        } else {
            entry.min_key = entry.min_key.min(key);
            entry.max_key = entry.max_key.max(key);
        }
        entry.count = entry.count.saturating_add(1);
    }

    pub fn get(&self, space: SpaceId) -> VoidOr<ObservedDensity> {
        self.inner
            .lock()
            .get(&space)
            .map(|f| {
                if !f.initialized {
                    VoidOr::Void
                } else {
                    VoidOr::Known(f.to_density(
                        KeyConfig {
                            bits_per_dim: f.bits_per_dim,
                        },
                        f.dims,
                    ))
                }
            })
            .unwrap_or(VoidOr::Void)
    }

    /// Rebuild fold state from observed keys (used at open).
    pub fn rebuild_from_keys<I>(&self, space: SpaceId, keys: I, config: KeyConfig, dims: usize)
    where
        I: IntoIterator<Item = u128>,
    {
        let mut map = self.inner.lock();
        let entry = map.entry(space).or_default();
        *entry = SpaceFold {
            bits_per_dim: config.bits_per_dim,
            dims,
            ..Default::default()
        };
        for key in keys {
            if !entry.initialized {
                entry.min_key = key;
                entry.max_key = key;
                entry.initialized = true;
            } else {
                entry.min_key = entry.min_key.min(key);
                entry.max_key = entry.max_key.max(key);
            }
            entry.count = entry.count.saturating_add(1);
        }
    }
}

impl SpaceFold {
    fn to_density(self, config: KeyConfig, dims: usize) -> ObservedDensity {
        ObservedDensity {
            record_count: self.count,
            max_occupied_depth: pairwise_depth(self.count, self.min_key, self.max_key, config, dims),
        }
    }
}

fn pairwise_depth(
    count: u64,
    min_key: u128,
    max_key: u128,
    config: KeyConfig,
    dims: usize,
) -> u32 {
    if count <= 1 || min_key == max_key {
        return 0;
    }
    let used = dims as u32 * config.bits_per_dim;
    if used == 0 {
        return 0;
    }
    let align_shift = 128u32.saturating_sub(used);
    let idx_xor = (min_key ^ max_key) >> align_shift;
    let shared = if idx_xor == 0 {
        used
    } else {
        let bits_in_xor = 128 - idx_xor.leading_zeros();
        used.saturating_sub(bits_in_xor)
    };
    let depth = (shared / dims as u32) + 1;
    depth.clamp(1, config.bits_per_dim)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_index::{hilbert, CurveAddress};

    fn depth_for_keys(keys: &[u128], config: KeyConfig, dims: usize) -> u32 {
        let tracker = DensityTracker::new();
        for &k in keys {
            tracker.observe_key(SpaceId(1), k, config, dims);
        }
        tracker.get(SpaceId(1)).known().unwrap().max_occupied_depth
    }

    #[test]
    fn golden_adversarial_high_magnitude_low_divergence() {
        let config = KeyConfig { bits_per_dim: 8 };
        let d = 1u128 << 16;
        let k1 = CurveAddress::from_raw_index(d / 2, config, 2).raw();
        let k2 = CurveAddress::from_raw_index(d - 1, config, 2).raw();
        assert_eq!(depth_for_keys(&[k1, k2], config, 2), 1);
    }

    #[test]
    fn golden_adjacent_finest_level() {
        let config = KeyConfig { bits_per_dim: 8 };
        let k0 = CurveAddress::from_raw_index(0, config, 2).raw();
        let k1 = CurveAddress::from_raw_index(1, config, 2).raw();
        assert_eq!(depth_for_keys(&[k0, k1], config, 2), 8);
    }

    #[test]
    fn golden_singleton_and_empty() {
        let config = KeyConfig { bits_per_dim: 8 };
        let d = 1u128 << 16;
        let k = CurveAddress::from_raw_index(d - 1, config, 2).raw();
        let tracker = DensityTracker::new();
        tracker.observe_key(SpaceId(1), k, config, 2);
        assert_eq!(
            tracker.get(SpaceId(1)).known().unwrap().max_occupied_depth,
            0
        );
        assert!(
            tracker.get(SpaceId(2)).is_void()
        );
    }

    #[test]
    fn golden_identical_keys_twice() {
        let config = KeyConfig { bits_per_dim: 8 };
        let k = CurveAddress::from_raw_index(42, config, 2).raw();
        let tracker = DensityTracker::new();
        tracker.observe_key(SpaceId(1), k, config, 2);
        tracker.observe_key(SpaceId(1), k, config, 2);
        let d = tracker.get(SpaceId(1)).known().unwrap();
        assert_eq!(d.record_count, 2);
        assert_eq!(d.max_occupied_depth, 0);
    }

    #[test]
    fn determinism_under_permutation() {
        let config = KeyConfig { bits_per_dim: 8 };
        let keys: Vec<u128> = (0..8)
            .map(|i| CurveAddress::from_raw_index(i * 100, config, 2).raw())
            .collect();
        let forward = DensityTracker::new();
        for &k in &keys {
            forward.observe_key(SpaceId(1), k, config, 2);
        }
        let reverse = DensityTracker::new();
        for &k in keys.iter().rev() {
            reverse.observe_key(SpaceId(1), k, config, 2);
        }
        assert_eq!(
            forward.get(SpaceId(1)),
            reverse.get(SpaceId(1))
        );
    }

    #[test]
    fn unobserved_space_is_void_not_zero_observed() {
        let tracker = DensityTracker::new();
        assert!(tracker.get(SpaceId(99)).is_void());
    }

    #[test]
    fn single_key_is_observed_depth_zero() {
        let config = KeyConfig { bits_per_dim: 8 };
        let k = CurveAddress::from_raw_index(42, config, 2).raw();
        let tracker = DensityTracker::new();
        tracker.observe_key(SpaceId(1), k, config, 2);
        let d = tracker.get(SpaceId(1)).known().expect("one key is observed");
        assert_eq!(d.record_count, 1);
        assert_eq!(d.max_occupied_depth, 0);
    }

    #[test]
    fn decode_golden_indices() {
        let idx1 = hilbert::encode(&[127, 0], 8);
        let idx2 = hilbert::encode(&[128, 0], 8);
        let d = 1u128 << 16;
        assert!(idx1.abs_diff(idx2) >= d / 4);
    }
}
