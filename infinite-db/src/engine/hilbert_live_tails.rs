//! Per Hilbert-shard live tail views (format v4).

use std::sync::Arc;

use dashmap::DashMap;

use super::hilbert_shard::pack_shard_key;
use super::live_tail::LiveTailView;

/// One [`LiveTailView`] per `(space_id, hilbert_shard_id)` pair.
pub struct HilbertLiveTails {
    tails: DashMap<u64, Arc<LiveTailView>>,
}

impl HilbertLiveTails {
    pub fn new() -> Self {
        Self {
            tails: DashMap::new(),
        }
    }

    pub fn get_or_create(&self, space_id: u64, shard_id: u32) -> Arc<LiveTailView> {
        let key = pack_shard_key(space_id, shard_id);
        if let Some(t) = self.tails.get(&key) {
            return Arc::clone(t.value());
        }
        let tail = Arc::new(LiveTailView::new());
        self.tails.insert(key, Arc::clone(&tail));
        tail
    }

    pub fn get(&self, space_id: u64, shard_id: u32) -> Option<Arc<LiveTailView>> {
        self.tails
            .get(&pack_shard_key(space_id, shard_id))
            .map(|e| Arc::clone(e.value()))
    }

    /// All shard tails registered for `space_id`.
    pub fn tails_for_space(&self, space_id: u64) -> Vec<Arc<LiveTailView>> {
        self.tails
            .iter()
            .filter(|e| e.key() >> 16 == space_id)
            .map(|e| Arc::clone(e.value()))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.tails.len()
    }
}

impl Default for HilbertLiveTails {
    fn default() -> Self {
        Self::new()
    }
}
