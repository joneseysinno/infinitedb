//! Thread-safe snapshot index.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::infinitedb_core::{
    address::SpaceId,
    snapshot::Snapshot,
};

/// Concurrent snapshot index (space_id → snapshot).
pub struct SnapshotStore {
    by_space: RwLock<BTreeMap<SpaceId, Arc<Snapshot>>>,
}

impl SnapshotStore {
    pub fn new(initial: BTreeMap<u64, Snapshot>) -> Self {
        let mapped = initial
            .into_iter()
            .map(|(k, v)| (SpaceId(k), Arc::new(v)))
            .collect();
        Self {
            by_space: RwLock::new(mapped),
        }
    }

    pub fn get(&self, space: SpaceId) -> Option<Arc<Snapshot>> {
        self.by_space.read().get(&space).cloned()
    }

    pub fn publish(&self, space: SpaceId, snapshot: Snapshot) -> Arc<Snapshot> {
        let arc = Arc::new(snapshot);
        self.by_space.write().insert(space, arc.clone());
        arc
    }

    pub fn update<F>(&self, space: SpaceId, f: F) -> Arc<Snapshot>
    where
        F: FnOnce(&mut Snapshot),
    {
        let mut guard = self.by_space.write();
        let snap = guard
            .entry(space)
            .or_insert_with(|| Arc::new(Snapshot::root(crate::infinitedb_core::snapshot::SnapshotId(1), space)));
        let mut updated = (**snap).clone();
        f(&mut updated);
        let arc = Arc::new(updated);
        guard.insert(space, arc.clone());
        arc
    }

    pub fn all(&self) -> BTreeMap<SpaceId, Arc<Snapshot>> {
        self.by_space.read().clone()
    }
}
