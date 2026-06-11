//! In-memory branch write overlays (format v4 Phase C).

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::infinitedb_core::{
    address::SpaceId,
    block::Record,
    branch::BranchId,
    hilbert_key::HilbertKey,
    snapshot::{BlockIndexEntry, Snapshot},
};
use crate::infinitedb_storage::wal::{WalDurability, WalEntry, WalReader, WalWriter};

use super::live_tail::LiveTailView;

/// Composite key for `(branch_id, space_id)` overlay maps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OverlayKey {
    pub branch_id: BranchId,
    pub space_id: SpaceId,
}

impl OverlayKey {
    pub fn new(branch_id: BranchId, space_id: SpaceId) -> Self {
        Self { branch_id, space_id }
    }
}

/// Branch-isolated writes that are not yet merged into `main`.
pub struct BranchOverlayStore {
    live: DashMap<OverlayKey, Arc<LiveTailView>>,
    sealed: DashMap<OverlayKey, BTreeMap<HilbertKey, BlockIndexEntry>>,
    bases: DashMap<OverlayKey, Arc<Snapshot>>,
    writers: DashMap<OverlayKey, Mutex<WalWriter>>,
}

impl BranchOverlayStore {
    pub fn new() -> Self {
        Self {
            live: DashMap::new(),
            sealed: DashMap::new(),
            bases: DashMap::new(),
            writers: DashMap::new(),
        }
    }

    pub fn register_branch(&self, branch_id: BranchId, base: Arc<Snapshot>) {
        let key = OverlayKey::new(branch_id, base.space);
        self.bases.insert(key, base);
    }

    pub fn append(&self, branch_id: BranchId, space: SpaceId, record: Record) {
        let key = OverlayKey::new(branch_id, space);
        let tail = if let Some(t) = self.live.get(&key) {
            Arc::clone(t.value())
        } else {
            let tail = Arc::new(LiveTailView::new());
            self.live.insert(key, Arc::clone(&tail));
            tail
        };
        tail.append(record);
    }

    /// Append one record to overlay log then in-memory tail.
    pub fn append_with_durability(
        &self,
        branch_id: BranchId,
        space: SpaceId,
        record: Record,
        root: &Path,
    ) -> io::Result<()> {
        self.append_batch_with_durability(branch_id, space, vec![record], root)
    }

    /// Append many records with one fsync per `(branch, space)` batch.
    pub fn append_batch_with_durability(
        &self,
        branch_id: BranchId,
        space: SpaceId,
        records: Vec<Record>,
        root: &Path,
    ) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let key = OverlayKey::new(branch_id, space);
        let log_path = overlay_log_path(root, space, branch_id);
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        if !self.writers.contains_key(&key) {
            let writer = WalWriter::open_with_durability(
                log_path,
                WalDurability::Buffered { sync_every: usize::MAX },
            )?;
            self.writers.insert(key, Mutex::new(writer));
        }

        let writer = self.writers.get(&key).expect("overlay writer");
        let mut guard = writer.lock();

        for record in &records {
            let entry = record_to_wal_entry(record);
            guard.append_frame(&entry)?;
        }
        guard.sync()?;

        let key = OverlayKey::new(branch_id, space);
        let tail = if let Some(t) = self.live.get(&key) {
            Arc::clone(t.value())
        } else {
            let tail = Arc::new(LiveTailView::new());
            self.live.insert(key, Arc::clone(&tail));
            tail
        };
        tail.extend_chunk(records);
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
                    if let Some(record) =
                        crate::infinitedb_storage::hot_segment::wal_entry_to_record(entry)
                    {
                        self.append(BranchId(branch_id), SpaceId(space_id), record);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn clear_branch(&self, branch_id: BranchId, root: &Path) -> io::Result<()> {
        let writer_keys: Vec<OverlayKey> = self
            .writers
            .iter()
            .filter(|e| e.key().branch_id == branch_id)
            .map(|e| *e.key())
            .collect();
        for key in writer_keys {
            if let Some((_, writer)) = self.writers.remove(&key) {
                let _ = writer.lock().sync();
            }
        }
        self.live.retain(|k, _| k.branch_id != branch_id);
        self.sealed.retain(|k, _| k.branch_id != branch_id);
        self.bases.retain(|k, _| k.branch_id != branch_id);
        let spaces_dir = root.join("spaces");
        if spaces_dir.exists() {
            for space_entry in std::fs::read_dir(spaces_dir)? {
                let branch_dir = space_entry?
                    .path()
                    .join("branches")
                    .join(branch_id.0.to_string());
                if branch_dir.exists() {
                    let _ = std::fs::remove_dir_all(branch_dir);
                }
            }
        }
        Ok(())
    }

    pub fn live_records(&self, branch_id: BranchId, space: SpaceId) -> Vec<Record> {
        let key = OverlayKey::new(branch_id, space);
        self.live
            .get(&key)
            .map(|t| t.value().snapshot())
            .unwrap_or_default()
    }

    pub fn all_live_records(&self, branch_id: BranchId) -> Vec<Record> {
        self.live
            .iter()
            .filter(|e| e.key().branch_id == branch_id)
            .flat_map(|e| e.value().snapshot())
            .collect()
    }

    pub fn sealed_blocks(&self, branch_id: BranchId, space: SpaceId) -> BTreeMap<HilbertKey, BlockIndexEntry> {
        let key = OverlayKey::new(branch_id, space);
        self.sealed
            .get(&key)
            .map(|e| e.value().clone())
            .unwrap_or_default()
    }

    pub fn base_snapshot(&self, branch_id: BranchId, space: SpaceId) -> Option<Arc<Snapshot>> {
        let key = OverlayKey::new(branch_id, space);
        self.bases.get(&key).map(|e| Arc::clone(e.value()))
    }

    /// Export fork-base snapshots for persistence (`branch_bases.bin`).
    pub fn export_bases(&self) -> BTreeMap<(u64, u64), Snapshot> {
        self.bases
            .iter()
            .map(|entry| {
                let key = *entry.key();
                ((key.branch_id.0, key.space_id.0), entry.value().as_ref().clone())
            })
            .collect()
    }

    /// Restore fork-base snapshots after reopen.
    pub fn import_bases(&self, bases: BTreeMap<(u64, u64), Snapshot>) {
        for ((branch_id, space_id), snap) in bases {
            let key = OverlayKey::new(BranchId(branch_id), SpaceId(space_id));
            self.bases.insert(key, Arc::new(snap));
        }
    }

    /// Base snapshots for active branches (block GC must retain their blocks).
    pub fn branch_base_snapshots(&self) -> Vec<Snapshot> {
        self.bases
            .iter()
            .map(|entry| entry.value().as_ref().clone())
            .collect()
    }

    pub fn has_overlay(&self, branch_id: BranchId) -> bool {
        self.live.iter().any(|e| e.key().branch_id == branch_id)
            || self.sealed.iter().any(|e| e.key().branch_id == branch_id)
    }
}

fn record_to_wal_entry(record: &Record) -> WalEntry {
    if record.tombstone {
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
