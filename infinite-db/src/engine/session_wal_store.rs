//! Session WAL registry, retirement meta, and recovery (peer track Phase 3).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bincode::{Decode, Encode};
use parking_lot::{Mutex, RwLock};

use crate::engine::intent_checkpoint::{split_session_wal_frames, IntentGroup};
use crate::engine::timed_fast_path::{SessionWriteStats, SessionWriteStatsSnapshot};
use crate::infinitedb_storage::session_fast_segment::{
    list_fast_segment_ids, FastSealOutcome, SessionFastSegment, HEADER_LEN as FAST_HEADER_LEN,
};
use crate::infinitedb_core::{
    address::RevisionId,
    hlc::{HlcStamp, SessionId, GLOBAL_SESSION},
};
use crate::infinitedb_storage::{
    error::StorageError,
    session_wal::{list_session_wal_ids, SessionWalReader, SessionWalWriter},
    wal::WalEntry,
};

/// Retirement gates for deleting a session WAL (Phase 3, revision-ranged hardening).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct SessionWalRetirement {
    /// Packed [`HlcStamp`] for the highest revision observed in this session WAL.
    pub highest_revision_packed: u128,
    pub sealed_through_packed: Option<u128>,
    pub replication_confirmed_through_packed: Option<u128>,
    pub collision_evaluated_through_packed: Option<u128>,
}

impl SessionWalRetirement {
    pub fn new(highest_revision: RevisionId) -> Self {
        Self {
            highest_revision_packed: highest_revision.stamp().pack(),
            sealed_through_packed: None,
            replication_confirmed_through_packed: None,
            collision_evaluated_through_packed: None,
        }
    }

    pub fn highest_revision(&self) -> RevisionId {
        RevisionId::from_stamp(HlcStamp::unpack(self.highest_revision_packed))
    }

    fn certified_through_min(&self) -> Option<RevisionId> {
        let stamps = [
            self.sealed_through_packed,
            self.replication_confirmed_through_packed,
            self.collision_evaluated_through_packed,
        ];
        if stamps.iter().any(|s| s.is_none()) {
            return None;
        }
        stamps
            .into_iter()
            .flatten()
            .map(|p| RevisionId::from_stamp(HlcStamp::unpack(p)))
            .min()
    }

    pub fn eligible_for_deletion(&self) -> bool {
        self.certified_through_min()
            .is_some_and(|min_cert| self.highest_revision() <= min_cert)
    }

    pub fn sealed_through(&self) -> Option<RevisionId> {
        self.sealed_through_packed
            .map(|p| RevisionId::from_stamp(HlcStamp::unpack(p)))
    }

    pub fn replication_confirmed_through(&self) -> Option<RevisionId> {
        self.replication_confirmed_through_packed
            .map(|p| RevisionId::from_stamp(HlcStamp::unpack(p)))
    }

    pub fn collision_evaluated_through(&self) -> Option<RevisionId> {
        self.collision_evaluated_through_packed
            .map(|p| RevisionId::from_stamp(HlcStamp::unpack(p)))
    }
}

/// Persisted session-WAL meta (`meta/session_wals.bin`).
#[derive(Debug, Clone, Default, Encode, Decode)]
pub struct SessionWalMeta {
    pub retirements: BTreeMap<u32, SessionWalRetirement>,
    /// Sessions whose WAL failed recovery — queryable, non-fatal (Phase 3).
    pub quarantined: BTreeMap<u32, String>,
}

impl SessionWalMeta {
    pub fn is_quarantined(&self, session: SessionId) -> bool {
        self.quarantined.contains_key(&session.0)
    }

    pub fn retirement(&self, session: SessionId) -> Option<&SessionWalRetirement> {
        self.retirements.get(&session.0)
    }
}

/// Result of replaying one session WAL on open.
#[derive(Debug)]
pub struct SessionWalRecovery {
    pub session: SessionId,
    pub committed_groups: Vec<IntentGroup>,
    pub uncommitted: Vec<WalEntry>,
}

/// Durable fast-segment bytes without a matching intent checkpoint (Phase 7).
#[derive(Debug)]
pub struct FastSegmentRecovery {
    pub session: SessionId,
    pub entries: Vec<WalEntry>,
}

/// Per-session WAL writers under `root/sessions/`.
pub struct SessionWalStore {
    root: PathBuf,
    writers: Mutex<BTreeMap<u32, SessionWalWriter>>,
    fast_segments: Mutex<BTreeMap<u32, SessionFastSegment>>,
    meta: RwLock<SessionWalMeta>,
    write_stats: Arc<SessionWriteStats>,
}

impl SessionWalStore {
    pub fn open(root: PathBuf, meta: SessionWalMeta) -> Arc<Self> {
        std::fs::create_dir_all(root.join("sessions")).ok();
        Arc::new(Self {
            root,
            writers: Mutex::new(BTreeMap::new()),
            fast_segments: Mutex::new(BTreeMap::new()),
            meta: RwLock::new(meta),
            write_stats: Arc::new(SessionWriteStats::default()),
        })
    }

    pub fn write_stats(&self) -> SessionWriteStatsSnapshot {
        self.write_stats.snapshot()
    }

    pub fn meta(&self) -> SessionWalMeta {
        self.meta.read().clone()
    }

    pub fn quarantined_sessions(&self) -> BTreeMap<u32, String> {
        self.meta.read().quarantined.clone()
    }

    pub fn ensure_writer(&self, session: SessionId) -> Result<(), StorageError> {
        if session.0 == GLOBAL_SESSION {
            return Ok(());
        }
        let mut map = self.writers.lock();
        if !map.contains_key(&session.0) {
            let writer = SessionWalWriter::open(&self.root, session).map_err(|e| {
                StorageError::from_io(e, Some(self.wal_path(session)))
            })?;
            map.insert(session.0, writer);
        }
        Ok(())
    }

    pub fn append_frame(&self, session: SessionId, entry: &WalEntry) -> Result<(), StorageError> {
        if session.0 == GLOBAL_SESSION {
            return Ok(());
        }
        self.ensure_writer(session)?;
        let mut map = self.writers.lock();
        let writer = map.get_mut(&session.0).unwrap();
        writer.append_frame(entry).map_err(|e| {
            StorageError::from_io(e, Some(writer.path().clone()))
        })?;
        Ok(())
    }

    pub fn append_intent_checkpoint(
        &self,
        session: SessionId,
        checkpoint: &crate::infinitedb_core::intent_checkpoint::IntentCheckpoint,
    ) -> Result<(), StorageError> {
        if session.0 == GLOBAL_SESSION {
            return Ok(());
        }
        self.ensure_writer(session)?;
        let mut map = self.writers.lock();
        let writer = map.get_mut(&session.0).unwrap();
        writer.append_intent_checkpoint(checkpoint).map_err(|e| {
            StorageError::from_io(e, Some(writer.path().clone()))
        })?;
        Ok(())
    }

    pub fn sync_group(&self, session: SessionId) -> Result<(), StorageError> {
        if session.0 == GLOBAL_SESSION {
            return Ok(());
        }
        let mut map = self.writers.lock();
        let writer = map
            .get_mut(&session.0)
            .ok_or_else(|| StorageError::WalFrame {
                message: format!("session {} wal not open", session.0),
            })?;
        let durable_before = writer.committed_bytes();
        if let Err(e) = writer.sync_group() {
            let _ = writer.truncate_to(durable_before);
            return Err(StorageError::from_io(e, Some(writer.path().clone())));
        }
        Ok(())
    }

    pub fn update_highest_revision(&self, session: SessionId, rev: RevisionId) {
        let mut meta = self.meta.write();
        let entry = meta
            .retirements
            .entry(session.0)
            .or_insert_with(|| SessionWalRetirement::new(rev));
        if rev > entry.highest_revision() {
            entry.highest_revision_packed = rev.stamp().pack();
        }
    }

    pub fn mark_sealed_through(&self, session: SessionId, through: RevisionId) {
        let mut meta = self.meta.write();
        let entry = meta
            .retirements
            .entry(session.0)
            .or_insert_with(|| SessionWalRetirement::new(through));
        entry.sealed_through_packed = Some(through.stamp().pack());
    }

    pub fn mark_replication_confirmed_through(&self, session: SessionId, through: RevisionId) {
        let mut meta = self.meta.write();
        let entry = meta
            .retirements
            .entry(session.0)
            .or_insert_with(|| SessionWalRetirement::new(through));
        entry.replication_confirmed_through_packed = Some(through.stamp().pack());
    }

    pub fn mark_collision_evaluated_through(&self, session: SessionId, through: RevisionId) {
        let mut meta = self.meta.write();
        let entry = meta
            .retirements
            .entry(session.0)
            .or_insert_with(|| SessionWalRetirement::new(through));
        entry.collision_evaluated_through_packed = Some(through.stamp().pack());
    }

    /// Delete the WAL file when all three retirement gates certify through `highest_revision`.
    pub fn try_retire_wal(&self, session: SessionId) -> Result<bool, StorageError> {
        let mut writers = self.writers.lock();
        let mut meta = self.meta.write();
        let Some(entry) = meta.retirements.get(&session.0).copied() else {
            return Ok(false);
        };
        if !entry.eligible_for_deletion() {
            return Ok(false);
        }
        writers.remove(&session.0);
        drop(writers);
        let path = self.wal_path(session);
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| StorageError::from_io(e, Some(path)))?;
        }
        meta.retirements.remove(&session.0);
        Ok(true)
    }

    pub fn wal_path(&self, session: SessionId) -> PathBuf {
        self.root.join("sessions").join(format!("{}.wal", session.0))
    }

    pub fn fast_path(&self, session: SessionId) -> PathBuf {
        self.root
            .join("sessions")
            .join(format!("{}.fast", session.0))
    }

    fn ensure_fast_segment(&self, session: SessionId) -> Result<(), StorageError> {
        if session.0 == GLOBAL_SESSION {
            return Ok(());
        }
        let mut map = self.fast_segments.lock();
        if !map.contains_key(&session.0) {
            let seg = SessionFastSegment::open(&self.root, session).map_err(|e| {
                StorageError::from_io(e, Some(self.fast_path(session)))
            })?;
            map.insert(session.0, seg);
        }
        Ok(())
    }

    /// Attempt durable fast-segment seal within `deadline`.
    pub fn try_fast_seal(
        &self,
        session: SessionId,
        entries: &[WalEntry],
        deadline: Duration,
    ) -> Result<FastSealOutcome, StorageError> {
        if session.0 == GLOBAL_SESSION || entries.is_empty() {
            return Ok(FastSealOutcome::TimedOut);
        }
        self.ensure_fast_segment(session)?;
        let mut map = self.fast_segments.lock();
        let seg = map.get_mut(&session.0).unwrap();
        let durable_before = seg.committed_bytes();
        match seg.try_seal_entries(entries, deadline) {
            Ok(FastSealOutcome::Sealed) => {
                self.write_stats.record_fast_seal_success();
                Ok(FastSealOutcome::Sealed)
            }
            Ok(FastSealOutcome::TimedOut) => {
                self.write_stats.record_fast_seal_timeout();
                Ok(FastSealOutcome::TimedOut)
            }
            Err(e) => {
                let _ = seg.truncate_to(durable_before);
                Err(StorageError::from_io(e, Some(seg.path().to_path_buf())))
            }
        }
    }

    /// Append pending frames to the session WAL (fast-path fallback).
    pub fn append_buffered_to_wal(
        &self,
        session: SessionId,
        entries: &[WalEntry],
    ) -> Result<(), StorageError> {
        for entry in entries {
            self.append_frame(session, entry)?;
        }
        self.write_stats.record_wal_fallback();
        Ok(())
    }

    pub fn reset_fast_after_commit(&self, session: SessionId) -> Result<(), StorageError> {
        if session.0 == GLOBAL_SESSION {
            return Ok(());
        }
        self.fast_segments.lock().remove(&session.0);
        if self.fast_path(session).exists() {
            let mut seg = SessionFastSegment::open(&self.root, session).map_err(|e| {
                StorageError::from_io(e, Some(self.fast_path(session)))
            })?;
            seg.reset().map_err(|e| StorageError::from_io(e, Some(seg.path().to_path_buf())))?;
        }
        Ok(())
    }

    /// Recover durable `.fast` segments that lack a committed intent checkpoint.
    pub fn recover_fast_segments(&self) -> Vec<FastSegmentRecovery> {
        let ids = list_fast_segment_ids(&self.root).unwrap_or_default();
        let mut out = Vec::new();
        for session in ids {
            if session.0 == GLOBAL_SESSION || self.meta.read().is_quarantined(session) {
                continue;
            }
            let Ok(mut seg) = SessionFastSegment::open(&self.root, session) else {
                continue;
            };
            let Ok(entries) = seg.read_committed_entries() else {
                continue;
            };
            if entries.is_empty() || seg.committed_bytes() <= FAST_HEADER_LEN {
                continue;
            }
            let wal_has_checkpoint = SessionWalReader::open(&self.root, session)
                .ok()
                .and_then(|r| r.read_committed_frames().ok())
                .map(|frames| {
                    frames
                        .iter()
                        .any(|f| matches!(f, crate::infinitedb_storage::session_wal::SessionWalFrame::Intent(_)))
                })
                .unwrap_or(false);
            if wal_has_checkpoint {
                let _ = self.reset_fast_after_commit(session);
                continue;
            }
            let all_revs = entries.iter().filter_map(wal_entry_revision);
            if let Some(max_rev) = all_revs.max() {
                self.update_highest_revision(session, max_rev);
            }
            out.push(FastSegmentRecovery { session, entries });
        }
        out
    }

    pub fn reset_fast_after_recovery(&self, session: SessionId) -> Result<(), StorageError> {
        self.reset_fast_after_commit(session)
    }

    /// Truncate a session WAL after its committed frames have been replayed into the store.
    pub fn reset_after_recovery(&self, session: SessionId) -> Result<(), StorageError> {
        if session.0 == GLOBAL_SESSION {
            return Ok(());
        }
        self.writers.lock().remove(&session.0);
        let mut writer = SessionWalWriter::open(&self.root, session).map_err(|e| {
            StorageError::from_io(e, Some(self.wal_path(session)))
        })?;
        writer
            .truncate_to(crate::infinitedb_storage::session_wal::HEADER_LEN)
            .map_err(|e| StorageError::from_io(e, Some(writer.path().clone())))?;
        writer.sync_group().map_err(|e| StorageError::from_io(e, Some(writer.path().clone())))?;
        self.writers.lock().insert(session.0, writer);
        Ok(())
    }

    /// Recover all session WALs; quarantine failures without aborting open.
    pub fn recover_all(&self) -> Vec<SessionWalRecovery> {
        let ids = list_session_wal_ids(&self.root).unwrap_or_default();
        let mut recovered = Vec::new();
        for session in ids {
            if session.0 == GLOBAL_SESSION {
                continue;
            }
            if self.meta.read().is_quarantined(session) {
                continue;
            }
            match SessionWalReader::open(&self.root, session) {
                Ok(reader) => match reader.read_committed_frames() {
                    Ok(frames) => {
                        let all_revs = frames.iter().filter_map(|f| match f {
                            crate::infinitedb_storage::session_wal::SessionWalFrame::Data(e) => {
                                wal_entry_revision(e)
                            }
                            crate::infinitedb_storage::session_wal::SessionWalFrame::Intent(cp) => {
                                Some(cp.last_revision)
                            }
                        });
                        if let Some(max_rev) = all_revs.max() {
                            self.update_highest_revision(session, max_rev);
                        }
                        let (committed_groups, uncommitted) = split_session_wal_frames(frames);
                        if !committed_groups.is_empty() || !uncommitted.is_empty() {
                            recovered.push(SessionWalRecovery {
                                session,
                                committed_groups,
                                uncommitted,
                            });
                        }
                    }
                    Err(err) => {
                        self.meta.write().quarantined.insert(
                            session.0,
                            format!("session wal recovery failed: {err}"),
                        );
                    }
                },
                Err(err) => {
                    self.meta
                        .write()
                        .quarantined
                        .insert(session.0, format!("session wal open failed: {err}"));
                }
            }
        }
        recovered
    }
}

/// Merge committed intent entries from all sessions in HLC revision order.
pub fn merge_recovered_entries(recovered: &[SessionWalRecovery]) -> Vec<WalEntry> {
    let mut all = Vec::new();
    for batch in recovered {
        for group in &batch.committed_groups {
            all.extend(group.entries.iter().cloned());
        }
    }
    all.sort_by_key(|e| wal_entry_revision(e).unwrap_or(RevisionId::ZERO));
    all
}

pub use crate::engine::intent_checkpoint::wal_entry_revision;

pub fn load_session_wal_meta(meta_dir: &Path) -> SessionWalMeta {
    let path = meta_dir.join("session_wals.bin");
    if !path.exists() {
        return SessionWalMeta::default();
    }
    match std::fs::read(&path) {
        Ok(bytes) => bincode::decode_from_slice(&bytes, bincode::config::standard())
            .map(|(m, _)| m)
            .unwrap_or_default(),
        Err(_) => SessionWalMeta::default(),
    }
}

pub fn persist_session_wal_meta(meta_dir: &Path, meta: &SessionWalMeta) -> std::io::Result<()> {
    let path = meta_dir.join("session_wals.bin");
    let tmp = path.with_extension("tmp");
    let bytes = bincode::encode_to_vec(meta, bincode::config::standard())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::{Address, DimensionVector, RevisionId, SpaceId};
    use crate::infinitedb_core::hlc::HlcStamp;
    use crate::infinitedb_core::intent_checkpoint::{IntentCheckpoint, IntentOperationKind};

    #[test]
    fn merge_recovered_order_is_hlc_not_file_order() {
        let mk = |session: u32, seq: u32| WalEntry::Write {
            address: Address::new(SpaceId(1), DimensionVector::new(vec![session, seq])),
            revision: RevisionId::from_stamp(HlcStamp {
                physical_ms: session as u64,
                logical: 0,
                session,
                sequence: seq,
            }),
            data: vec![seq as u8],
        };
        let recovered = vec![
            SessionWalRecovery {
                session: SessionId(2),
                committed_groups: vec![IntentGroup {
                    checkpoint: IntentCheckpoint::new(
                        RevisionId::from_stamp(HlcStamp {
                            physical_ms: 2,
                            logical: 0,
                            session: 2,
                            sequence: 5,
                        }),
                        RevisionId::from_stamp(HlcStamp {
                            physical_ms: 2,
                            logical: 0,
                            session: 2,
                            sequence: 5,
                        }),
                        IntentOperationKind::Insert,
                    ),
                    entries: vec![mk(2, 5)],
                }],
                uncommitted: vec![],
            },
            SessionWalRecovery {
                session: SessionId(1),
                committed_groups: vec![IntentGroup {
                    checkpoint: IntentCheckpoint::new(
                        RevisionId::from_stamp(HlcStamp {
                            physical_ms: 1,
                            logical: 0,
                            session: 1,
                            sequence: 9,
                        }),
                        RevisionId::from_stamp(HlcStamp {
                            physical_ms: 1,
                            logical: 0,
                            session: 1,
                            sequence: 9,
                        }),
                        IntentOperationKind::Insert,
                    ),
                    entries: vec![mk(1, 9)],
                }],
                uncommitted: vec![],
            },
        ];
        let merged = merge_recovered_entries(&recovered);
        assert_eq!(merged.len(), 2);
        let r0 = wal_entry_revision(&merged[0]).unwrap();
        let r1 = wal_entry_revision(&merged[1]).unwrap();
        assert!(r0 < r1);
    }
}
