//! Persisted queue of unresolved merge conflicts (`meta/conflicts.bin`).

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bincode::{config::standard, decode_from_slice, encode_to_vec, Decode, Encode};
use parking_lot::Mutex;

use crate::infinitedb_core::{
    address::RevisionId,
    block::Record,
    branch::BranchId,
    merge::MergeConflict,
};

/// One unresolved merge conflict awaiting operator resolution.
#[derive(Debug, Clone, Encode, Decode)]
pub struct StoredConflict {
    pub id: u64,
    pub target: BranchId,
    pub source: BranchId,
    pub conflict: MergeConflict,
}

/// In-memory + on-disk conflict queue.
pub struct ConflictQueue {
    path: PathBuf,
    next_id: AtomicU64,
    entries: Mutex<Vec<StoredConflict>>,
}

impl ConflictQueue {
    pub fn open(root: &Path) -> io::Result<Self> {
        let path = root.join("meta").join("conflicts.bin");
        let (entries, next_id) = if path.exists() {
            let bytes = std::fs::read(&path)?;
            let (loaded, _): (Vec<StoredConflict>, _) =
                decode_from_slice(&bytes, standard()).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, e)
                })?;
            let next = loaded.iter().map(|c| c.id).max().unwrap_or(0) + 1;
            (loaded, next)
        } else {
            (Vec::new(), 1)
        };
        Ok(Self {
            path,
            next_id: AtomicU64::new(next_id),
            entries: Mutex::new(entries),
        })
    }

    pub fn push(&self, target: BranchId, source: BranchId, conflict: MergeConflict) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.entries.lock().push(StoredConflict {
            id,
            target,
            source,
            conflict,
        });
        let _ = self.persist();
        id
    }

    pub fn push_all(
        &self,
        target: BranchId,
        source: BranchId,
        conflicts: Vec<MergeConflict>,
    ) -> Vec<u64> {
        conflicts
            .into_iter()
            .map(|c| self.push(target, source, c))
            .collect()
    }

    pub fn list(&self) -> Vec<StoredConflict> {
        self.entries.lock().clone()
    }

    pub fn get(&self, id: u64) -> Option<StoredConflict> {
        self.entries.lock().iter().find(|c| c.id == id).cloned()
    }

    pub fn resolve(&self, id: u64, chosen: Record) -> io::Result<StoredConflict> {
        let mut guard = self.entries.lock();
        let pos = guard
            .iter()
            .position(|c| c.id == id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "conflict not found"))?;
        let removed = guard.remove(pos);
        if removed.conflict.address != chosen.address {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "resolved record address mismatch",
            ));
        }
        drop(guard);
        self.persist()?;
        Ok(removed)
    }

    pub fn remove(&self, id: u64) -> Option<StoredConflict> {
        let mut guard = self.entries.lock();
        let pos = guard.iter().position(|c| c.id == id)?;
        let removed = guard.remove(pos);
        drop(guard);
        let _ = self.persist();
        Some(removed)
    }

    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    fn persist(&self) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = encode_to_vec(&*self.entries.lock(), standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, &self.path)
    }
}

/// Build a resolution record from raw payload bytes.
pub fn resolution_record(conflict: &MergeConflict, data: Vec<u8>, revision: RevisionId) -> Record {
    Record {
        address: conflict.address.clone(),
        revision,
        data,
        tombstone: false,
    }
}

/// Tombstone resolution for a conflicting address.
pub fn resolution_tombstone(conflict: &MergeConflict, revision: RevisionId) -> Record {
    Record {
        address: conflict.address.clone(),
        revision,
        data: vec![],
        tombstone: true,
    }
}
