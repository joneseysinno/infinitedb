//! In-memory branch write overlays (format v4 Phase C).

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use dashmap::DashMap;

use crate::infinitedb_core::{
    address::SpaceId,
    block::Record,
    branch::BranchId,
    snapshot::{BlockIndexEntry, Snapshot},
};

use super::live_tail::LiveTailView;

fn overlay_key(branch_id: u64, space_id: u64) -> u64 {
    (branch_id << 32) | space_id
}

/// Branch-isolated writes that are not yet merged into `main`.
pub struct BranchOverlayStore {
    live: DashMap<u64, Arc<LiveTailView>>,
    sealed: DashMap<u64, BTreeMap<u128, BlockIndexEntry>>,
    bases: DashMap<u64, Arc<Snapshot>>,
}

impl BranchOverlayStore {
    pub fn new() -> Self {
        Self {
            live: DashMap::new(),
            sealed: DashMap::new(),
            bases: DashMap::new(),
        }
    }

    pub fn register_branch(&self, branch_id: BranchId, base: Arc<Snapshot>) {
        self.bases.insert(branch_id.0, base);
    }

    pub fn append(&self, branch_id: BranchId, space: SpaceId, record: Record) {
        let key = overlay_key(branch_id.0, space.0);
        let tail = if let Some(t) = self.live.get(&key) {
            Arc::clone(t.value())
        } else {
            let tail = Arc::new(LiveTailView::new());
            self.live.insert(key, Arc::clone(&tail));
            tail
        };
        tail.append(record);
    }

    pub fn live_records(&self, branch_id: BranchId, space: SpaceId) -> Vec<Record> {
        let key = overlay_key(branch_id.0, space.0);
        self.live
            .get(&key)
            .map(|t| t.value().snapshot())
            .unwrap_or_default()
    }

    pub fn all_live_records(&self, branch_id: BranchId) -> Vec<Record> {
        self.live
            .iter()
            .filter(|e| e.key() >> 32 == branch_id.0)
            .flat_map(|e| e.value().snapshot())
            .collect()
    }

    pub fn sealed_blocks(&self, branch_id: BranchId, space: SpaceId) -> BTreeMap<u128, BlockIndexEntry> {
        let key = overlay_key(branch_id.0, space.0);
        self.sealed
            .get(&key)
            .map(|e| e.value().clone())
            .unwrap_or_default()
    }

    pub fn base_snapshot(&self, branch_id: BranchId) -> Option<Arc<Snapshot>> {
        self.bases.get(&branch_id.0).map(|e| Arc::clone(e.value()))
    }

    pub fn clear_branch(&self, branch_id: BranchId) {
        self.live.retain(|k, _| k >> 32 != branch_id.0);
        self.sealed.retain(|k, _| k >> 32 != branch_id.0);
        self.bases.remove(&branch_id.0);
    }

    pub fn has_overlay(&self, branch_id: BranchId) -> bool {
        self.live.iter().any(|e| e.key() >> 32 == branch_id.0)
            || self.sealed.iter().any(|e| e.key() >> 32 == branch_id.0)
    }

    /// Persist overlay live buffers under `spaces/<space>/branches/<branch>/overlay_live.bin`.
    pub fn persist_space(
        &self,
        branch_id: BranchId,
        space: SpaceId,
        space_dir: &std::path::Path,
    ) -> io::Result<()> {
        let records = self.live_records(branch_id, space);
        if records.is_empty() {
            return Ok(());
        }
        let dir = space_dir.join("branches").join(branch_id.0.to_string());
        std::fs::create_dir_all(&dir)?;
        let bytes = bincode::encode_to_vec(&records, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let path = dir.join("overlay_live.bin");
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, path)
    }
}

impl Default for BranchOverlayStore {
    fn default() -> Self {
        Self::new()
    }
}
