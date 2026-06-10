//! In-memory branch write overlays (format v4 Phase C).

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;

use crate::infinitedb_core::{
    address::SpaceId,
    block::Record,
    branch::BranchId,
    snapshot::{BlockIndexEntry, Snapshot},
};
use crate::infinitedb_storage::wal::{WalDurability, WalEntry, WalReader, WalWriter};

use super::live_tail::LiveTailView;

/// Composite key for `(branch_id, space_id)` overlay maps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OverlayKey {
    pub branch_id: u64,
    pub space_id: u64,
}

impl OverlayKey {
    pub fn new(branch_id: u64, space_id: u64) -> Self {
        Self { branch_id, space_id }
    }
}

/// Branch-isolated writes that are not yet merged into `main`.
pub struct BranchOverlayStore {
    live: DashMap<OverlayKey, Arc<LiveTailView>>,
    sealed: DashMap<OverlayKey, BTreeMap<u128, BlockIndexEntry>>,
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
        let key = OverlayKey::new(branch_id.0, space.0);
        let tail = if let Some(t) = self.live.get(&key) {
            Arc::clone(t.value())
        } else {
            let tail = Arc::new(LiveTailView::new());
            self.live.insert(key, Arc::clone(&tail));
            tail
        };
        tail.append(record);
    }

    /// Append to overlay log then in-memory tail (durability for branch writes).
    pub fn append_with_durability(
        &self,
        branch_id: BranchId,
        space: SpaceId,
        record: Record,
        root: &Path,
    ) -> io::Result<()> {
        let log_path = overlay_log_path(root, space, branch_id);
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let entry = if record.tombstone {
            WalEntry::Tombstone {
                address: record.address.clone(),
                revision: record.revision,
            }
        } else {
            WalEntry::Write {
                address: record.address.clone(),
                revision: record.revision,
                data: record.data.clone(),
            }
        };
        let mut writer = WalWriter::open_with_durability(
            log_path,
            WalDurability::Buffered { sync_every: 1 },
        )?;
        writer.append_frame(&entry)?;
        writer.sync()?;
        self.append(branch_id, space, record);
        Ok(())
    }

    /// Replay overlay logs for all branches under `root`.
    pub fn replay_all(&self, root: &Path) -> io::Result<()> {
        let spaces_dir = root.join("spaces");
        if !spaces_dir.exists() {
            return Ok(());
        }
        for space_entry in std::fs::read_dir(spaces_dir)? {
            let space_entry = space_entry?;
            let space_id: u64 = match space_entry.file_name().to_string_lossy().parse() {
                Ok(id) => id,
                Err(_) => continue,
            };
            let branches_dir = space_entry.path().join("branches");
            if !branches_dir.exists() {
                continue;
            }
            for branch_entry in std::fs::read_dir(branches_dir)? {
                let branch_entry = branch_entry?;
                let branch_id: u64 = match branch_entry.file_name().to_string_lossy().parse() {
                    Ok(id) => id,
                    Err(_) => continue,
                };
                let log_path = branch_entry.path().join("overlay.log");
                if !log_path.exists() {
                    continue;
                }
                let mut reader = WalReader::open(log_path)?;
                for entry in reader.entries()? {
                    if let Some(record) = crate::infinitedb_storage::hot_segment::wal_entry_to_record(entry) {
                        if record.hilbert_key == 0 {
                            // Recomputed at query if needed.
                        }
                        self.append(BranchId(branch_id), SpaceId(space_id), record);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn clear_branch(&self, branch_id: BranchId, root: &Path) -> io::Result<()> {
        self.live.retain(|k, _| k.branch_id != branch_id.0);
        self.sealed.retain(|k, _| k.branch_id != branch_id.0);
        self.bases.remove(&branch_id.0);
        let spaces_dir = root.join("spaces");
        if spaces_dir.exists() {
            for space_entry in std::fs::read_dir(spaces_dir)? {
                let branch_dir = space_entry?.path().join("branches").join(branch_id.0.to_string());
                if branch_dir.exists() {
                    let _ = std::fs::remove_dir_all(branch_dir);
                }
            }
        }
        Ok(())
    }

    pub fn live_records(&self, branch_id: BranchId, space: SpaceId) -> Vec<Record> {
        let key = OverlayKey::new(branch_id.0, space.0);
        self.live
            .get(&key)
            .map(|t| t.value().snapshot())
            .unwrap_or_default()
    }

    pub fn all_live_records(&self, branch_id: BranchId) -> Vec<Record> {
        self.live
            .iter()
            .filter(|e| e.key().branch_id == branch_id.0)
            .flat_map(|e| e.value().snapshot())
            .collect()
    }

    pub fn sealed_blocks(&self, branch_id: BranchId, space: SpaceId) -> BTreeMap<u128, BlockIndexEntry> {
        let key = OverlayKey::new(branch_id.0, space.0);
        self.sealed
            .get(&key)
            .map(|e| e.value().clone())
            .unwrap_or_default()
    }

    pub fn base_snapshot(&self, branch_id: BranchId) -> Option<Arc<Snapshot>> {
        self.bases.get(&branch_id.0).map(|e| Arc::clone(e.value()))
    }

    pub fn has_overlay(&self, branch_id: BranchId) -> bool {
        self.live.iter().any(|e| e.key().branch_id == branch_id.0)
            || self.sealed.iter().any(|e| e.key().branch_id == branch_id.0)
    }
}

fn overlay_log_path(root: &Path, space: SpaceId, branch: BranchId) -> PathBuf {
    root.join("spaces")
        .join(space.0.to_string())
        .join("branches")
        .join(branch.0.to_string())
        .join("overlay.log")
}

impl Default for BranchOverlayStore {
    fn default() -> Self {
        Self::new()
    }
}
