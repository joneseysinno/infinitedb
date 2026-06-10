//! Shared bulk write session (buffered WAL, deferred flush).

use std::collections::BTreeSet;
use std::io;

use crate::infinitedb_core::{
    address::{Address, DimensionVector, RevisionId, SpaceId},
    block::Record,
};
use crate::infinitedb_storage::wal::{WalDurability, WalEntry, WalWriter};

use super::super::LegacyDb;

/// Default WAL sync interval during bulk import (frames between fsyncs).
pub const DEFAULT_BULK_SYNC_EVERY: usize = 4096;

/// Default in-memory buffer size before auto-flush during bulk import.
pub const DEFAULT_BULK_FLUSH_THRESHOLD: usize = 8192;

/// Tuning for any bulk write session.
#[derive(Debug, Clone)]
pub struct BulkWriteOptions {
    pub sync_every: usize,
    pub flush_threshold: usize,
}

impl Default for BulkWriteOptions {
    fn default() -> Self {
        Self {
            sync_every: DEFAULT_BULK_SYNC_EVERY,
            flush_threshold: DEFAULT_BULK_FLUSH_THRESHOLD,
        }
    }
}

/// Statistics returned after a successful bulk session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkWriteResult {
    pub count: usize,
    pub wal_frames: usize,
    pub first_revision: Option<RevisionId>,
    pub last_revision: Option<RevisionId>,
}

/// Backward-compatible alias for hyperedge bulk results.
pub type BulkImportResult = BulkWriteResult;

/// Active bulk session state shared by record, hyperedge, and signal guards.
pub struct BulkSessionCore<'a> {
    pub(super) db: &'a mut LegacyDb,
    saved_durability: WalDurability,
    saved_flush_threshold: usize,
    pub(super) touched_spaces: BTreeSet<u64>,
    pub(super) count: usize,
    pub(super) wal_frames: usize,
    pub(super) first_revision: Option<RevisionId>,
    pub(super) last_revision: Option<RevisionId>,
    finished: bool,
    #[cfg(feature = "sync")]
    pub(super) pending_sync: Vec<crate::infinitedb_sync::transport::SyncOperation>,
}

impl<'a> BulkSessionCore<'a> {
    /// Start a bulk session. Only one session may be active per [`LegacyDb`].
    pub fn begin(db: &'a mut LegacyDb, options: BulkWriteOptions) -> io::Result<Self> {
        if db.bulk_session_active {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "a bulk write session is already active on this database",
            ));
        }
        db.bulk_session_active = true;
        let saved_durability = db.wal.durability();
        let saved_flush_threshold = db.flush_threshold;
        db.wal.set_durability(WalDurability::Buffered {
            sync_every: options.sync_every.max(1),
        });
        db.flush_threshold = options.flush_threshold;
        db.defer_auto_flush = true;
        Ok(Self {
            db,
            saved_durability,
            saved_flush_threshold,
            touched_spaces: BTreeSet::new(),
            count: 0,
            wal_frames: 0,
            first_revision: None,
            last_revision: None,
            finished: false,
            #[cfg(feature = "sync")]
            pending_sync: Vec::new(),
        })
    }

    pub fn touch_space(&mut self, space: SpaceId) {
        self.touched_spaces.insert(space.0);
    }

    pub fn record_operation(&mut self, rev: RevisionId, wal_frame_count: usize) {
        self.count += 1;
        self.wal_frames += wal_frame_count;
        self.first_revision = Some(self.first_revision.unwrap_or(rev));
        self.last_revision = Some(rev);
    }

    /// Append one live record using buffered WAL I/O.
    pub fn push_write(
        &mut self,
        space: SpaceId,
        point: DimensionVector,
        data: Vec<u8>,
    ) -> io::Result<RevisionId> {
        let rev = self.db.next_revision();
        let address = Address::new(space, point);
        self.push_row_raw(
            WalEntry::Write {
                address: address.clone(),
                revision: rev,
                data: data.clone(),
            },
            Record {
                address,
                revision: rev,
                data,
                tombstone: false,
                hilbert_key: 0,
            },
        )?;
        self.touch_space(space);
        self.record_operation(rev, 1);
        Ok(rev)
    }

    /// Append one tombstone using buffered WAL I/O.
    pub fn push_tombstone(
        &mut self,
        space: SpaceId,
        point: DimensionVector,
    ) -> io::Result<RevisionId> {
        let rev = self.db.next_revision();
        let address = Address::new(space, point);
        self.push_row_raw(
            WalEntry::Tombstone {
                address: address.clone(),
                revision: rev,
            },
            Record {
                address,
                revision: rev,
                data: vec![],
                tombstone: true,
                hilbert_key: 0,
            },
        )?;
        self.touch_space(space);
        self.record_operation(rev, 1);
        Ok(rev)
    }

    /// Append multiple WAL frames as one logical bulk operation.
    pub fn push_rows(&mut self, rows: Vec<(WalEntry, Record)>) -> io::Result<RevisionId> {
        if rows.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bulk push_rows requires at least one row",
            ));
        }
        let rev = rows[0].1.revision;
        let n = rows.len();
        for (entry, record) in rows {
            self.push_row_raw(entry, record)?;
        }
        self.record_operation(rev, n);
        Ok(rev)
    }

    /// Append WAL frames without incrementing the logical operation counter.
    pub fn push_rows_only(&mut self, rows: Vec<(WalEntry, Record)>) -> io::Result<RevisionId> {
        if rows.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bulk push_rows_only requires at least one row",
            ));
        }
        let rev = rows[0].1.revision;
        for (entry, record) in rows {
            self.push_row_raw(entry, record)?;
        }
        Ok(rev)
    }

    pub fn push_row_raw(&mut self, entry: WalEntry, record: Record) -> io::Result<()> {
        self.db.wal.append_frame(&entry)?;
        self.db.buffer.push(record);
        Self::maybe_sync(self.db.wal_mut())
    }

    fn maybe_sync(wal: &mut WalWriter) -> io::Result<()> {
        let every = wal.durability().sync_every();
        if wal.pending_frames() >= every {
            wal.sync()?;
        }
        Ok(())
    }

    #[cfg(feature = "sync")]
    pub fn defer_sync(&mut self, op: crate::infinitedb_sync::transport::SyncOperation) {
        self.pending_sync.push(op);
    }

    #[cfg(feature = "sync")]
    fn flush_sync_ops(&mut self) -> io::Result<()> {
        let ops = std::mem::take(&mut self.pending_sync);
        for op in ops {
            self.db.enqueue_sync(op)?;
        }
        Ok(())
    }

    pub fn result_snapshot(&self) -> BulkWriteResult {
        BulkWriteResult {
            count: self.count,
            wal_frames: self.wal_frames,
            first_revision: self.first_revision,
            last_revision: self.last_revision,
        }
    }

    pub fn finish(mut self) -> io::Result<BulkWriteResult> {
        self.db.wal.sync()?;
        for &space_id in self.touched_spaces.clone().iter() {
            self.db.flush(SpaceId(space_id))?;
        }
        #[cfg(feature = "sync")]
        self.flush_sync_ops()?;
        self.restore_inner()?;
        self.finished = true;
        Ok(self.result_snapshot())
    }

    fn restore_inner(&mut self) -> io::Result<()> {
        self.db.wal.set_durability(self.saved_durability);
        self.db.flush_threshold = self.saved_flush_threshold;
        self.db.defer_auto_flush = false;
        self.db.bulk_session_active = false;
        Ok(())
    }
}

impl Drop for BulkSessionCore<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.restore_inner();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn second_bulk_session_returns_already_exists() {
        let dir = TempDir::new().unwrap();
        let mut db = LegacyDb::open(dir.path()).unwrap();
        db.bulk_session_active = true;
        let second = BulkSessionCore::begin(&mut db, BulkWriteOptions::default());
        assert!(matches!(
            second,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists
        ));
    }

    #[test]
    fn drop_without_finish_clears_active_flag() {
        let dir = TempDir::new().unwrap();
        let mut db = LegacyDb::open(dir.path()).unwrap();
        let session = BulkSessionCore::begin(&mut db, BulkWriteOptions::default()).unwrap();
        drop(session);
        assert!(!db.bulk_session_active);
    }
}

impl LegacyDb {
    pub(crate) fn wal_mut(&mut self) -> &mut WalWriter {
        &mut self.wal
    }

    /// Force the WAL buffer to disk (strict durability escape hatch).
    pub fn sync_wal(&mut self) -> std::io::Result<()> {
        self.wal.sync()
    }

    pub(crate) fn apply_prepared_writes_strict(
        &mut self,
        rows: Vec<(WalEntry, Record)>,
    ) -> std::io::Result<RevisionId> {
        if rows.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "no rows to write",
            ));
        }
        let rev = rows[0].1.revision;
        for (entry, record) in rows {
            self.wal.append(&entry)?;
            self.buffer.push(record);
        }
        Ok(rev)
    }
}
