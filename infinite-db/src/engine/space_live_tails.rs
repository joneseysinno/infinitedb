//! Per-space live tail views (format v3).

use std::sync::Arc;

use dashmap::DashMap;

use crate::infinitedb_core::address::SpaceId;

use super::live_tail::LiveTailView;

/// One [`LiveTailView`] per space for parallel I/O isolation.
pub struct SpaceLiveTails {
    tails: DashMap<SpaceId, Arc<LiveTailView>>,
}

impl SpaceLiveTails {
    pub fn new() -> Self {
        Self {
            tails: DashMap::new(),
        }
    }

    pub fn get_or_create(&self, space_id: SpaceId) -> Arc<LiveTailView> {
        if let Some(t) = self.tails.get(&space_id) {
            return Arc::clone(t.value());
        }
        let tail = Arc::new(LiveTailView::new());
        self.tails.insert(space_id, Arc::clone(&tail));
        tail
    }

    pub fn get(&self, space_id: SpaceId) -> Option<Arc<LiveTailView>> {
        self.tails.get(&space_id).map(|e| Arc::clone(e.value()))
    }

    pub fn len(&self) -> usize {
        self.tails.len()
    }
}

impl Default for SpaceLiveTails {
    fn default() -> Self {
        Self::new()
    }
}
