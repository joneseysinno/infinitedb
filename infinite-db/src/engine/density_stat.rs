//! Per-space occupied-key depth statistic (T9).

use std::collections::HashMap;
use std::sync::Mutex;

use crate::infinitedb_core::address::SpaceId;
use crate::infinitedb_index::composite::KeyConfig;

/// Incremental density fold per space.
#[derive(Debug, Default)]
pub struct DensityTracker {
    inner: Mutex<HashMap<SpaceId, SpaceDensity>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SpaceDensity {
    pub record_count: u64,
    /// Deepest dyadic level at which two keys differ (0 = empty or uniform).
    pub max_occupied_depth: u32,
}

impl DensityTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe_key(&self, space: SpaceId, key: u128, config: KeyConfig, dims: usize) {
        let depth = occupied_depth(key, config, dims);
        let mut map = self.inner.lock().expect("density lock");
        let entry = map.entry(space).or_default();
        entry.record_count = entry.record_count.saturating_add(1);
        entry.max_occupied_depth = entry.max_occupied_depth.max(depth);
    }

    pub fn get(&self, space: SpaceId) -> SpaceDensity {
        self.inner
            .lock()
            .expect("density lock")
            .get(&space)
            .copied()
            .unwrap_or_default()
    }
}

fn occupied_depth(key: u128, config: KeyConfig, dims: usize) -> u32 {
    let total = dims as u32 * config.bits_per_dim;
    if total == 0 || key == 0 {
        return 0;
    }
    let leading = key.leading_zeros();
    let used = 128u32.saturating_sub(leading);
    used.min(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_index::CurveAddress;
    use crate::infinitedb_core::address::DimensionVector;

    #[test]
    fn depth_increases_with_refinement() {
        let config = KeyConfig { bits_per_dim: 8 };
        let a = CurveAddress::from_point(&DimensionVector::new(vec![0, 0]), config).raw();
        let b = CurveAddress::from_point(&DimensionVector::new(vec![1, 0]), config).raw();
        assert!(occupied_depth(b, config, 2) >= occupied_depth(a, config, 2));
    }
}
