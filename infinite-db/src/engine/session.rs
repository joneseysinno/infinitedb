//! Write sessions, per-session watermarks, and version vectors (peer track Phase 2).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::infinitedb_core::{
    address::RevisionId,
    hilbert_key::HilbertKey,
    hlc::{SessionId, GLOBAL_SESSION},
    intent_checkpoint::{IntentCheckpoint, IntentOperationKind},
};
use crate::infinitedb_storage::wal::WalEntry;

use super::derivation::AssertionEvent;
use super::hlc_clock::HlcClock;
use super::session_wal_store::{SessionWalMeta, SessionWalStore};
use super::timed_fast_path::DurabilityMedium;
use super::watermark::{FailedRevision, RevisionRange, RevisionWatermark};

/// Proof that a session's pending group is durably stored (Phase 4/7 typestate token).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableIntent {
    pub(crate) session: SessionId,
    pub(crate) medium: DurabilityMedium,
}

/// Buffered write awaiting intent checkpoint commit.
#[derive(Debug, Clone)]
pub(crate) struct BufferedSessionWrite {
    pub entry: WalEntry,
    pub hilbert_key: HilbertKey,
    pub revision: RevisionId,
    /// Hypergraph assertion events replayed to the derivation bus on commit (Phase 5).
    pub assertion_event: Option<AssertionEvent>,
}

#[derive(Debug, Default)]
struct PendingIntentState {
    first: Option<RevisionId>,
    last: Option<RevisionId>,
    kind: Option<IntentOperationKind>,
    buffered: Vec<BufferedSessionWrite>,
    wal_synced: bool,
    durability_medium: Option<DurabilityMedium>,
}

impl PendingIntentState {
    fn note_write(
        &mut self,
        entry: WalEntry,
        hilbert_key: HilbertKey,
        revision: RevisionId,
        kind: IntentOperationKind,
        assertion_event: Option<AssertionEvent>,
    ) {
        if self.first.is_none() {
            self.first = Some(revision);
            self.kind = Some(kind);
        }
        self.last = Some(revision);
        self.wal_synced = false;
        self.durability_medium = None;
        self.buffered.push(BufferedSessionWrite {
            entry,
            hilbert_key,
            revision,
            assertion_event,
        });
    }

    fn mark_durable(&mut self, medium: DurabilityMedium) {
        self.wal_synced = true;
        self.durability_medium = Some(medium);
    }

    fn peek_buffered_entries(&self) -> Vec<WalEntry> {
        self.buffered.iter().map(|b| b.entry.clone()).collect()
    }

    fn take_if_durable(&mut self) -> Result<(Vec<BufferedSessionWrite>, DurabilityMedium), String> {
        if self.buffered.is_empty() {
            return Err("no pending session intent to commit".into());
        }
        if !self.wal_synced {
            return Err("session WAL must be synced before commit (call sync_session_wal)".into());
        }
        let medium = self
            .durability_medium
            .unwrap_or(DurabilityMedium::SessionWal);
        let buffered = std::mem::take(&mut self.buffered);
        self.first = None;
        self.last = None;
        self.wal_synced = false;
        self.durability_medium = None;
        self.kind = None;
        Ok((buffered, medium))
    }

    fn build_checkpoint(&self) -> Result<IntentCheckpoint, String> {
        let first = self
            .first
            .ok_or_else(|| "no pending session intent to commit".to_string())?;
        let last = self
            .last
            .ok_or_else(|| "no pending session intent to commit".to_string())?;
        let kind = self
            .kind
            .unwrap_or(IntentOperationKind::General);
        Ok(IntentCheckpoint::new(first, last, kind))
    }

    fn has_pending(&self) -> bool {
        !self.buffered.is_empty()
    }
}

/// Read pin: one stable ceiling per admitted session (D-P2 / Phase 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionVector(pub BTreeMap<SessionId, RevisionId>);

impl Default for VersionVector {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl VersionVector {
    /// Scalar meet — minimum stable component across admitted sessions.
    pub fn scalar_meet(&self) -> RevisionId {
        self.0.values().min().copied().unwrap_or(RevisionId::ZERO)
    }

    pub fn get(&self, session: SessionId) -> Option<RevisionId> {
        self.0.get(&session).copied()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&SessionId, &RevisionId)> {
        self.0.iter()
    }
}

/// Per-session outstanding/stable tracking with session-keyed retire routing.
pub struct SessionWatermarks {
    sessions: RwLock<BTreeMap<u32, Arc<RevisionWatermark>>>,
    next_session_id: AtomicU32,
}

impl SessionWatermarks {
    /// `legacy_seed` hydrates the implicit session-0 stream; `next_session_id` is the
    /// next id to hand out from the local registry (D-P2).
    pub fn new(legacy_seed: u64, next_session_id: u32) -> Arc<Self> {
        let mut map = BTreeMap::new();
        map.insert(
            GLOBAL_SESSION,
            Arc::new(RevisionWatermark::new(legacy_seed)),
        );
        Arc::new(Self {
            sessions: RwLock::new(map),
            next_session_id: AtomicU32::new(next_session_id),
        })
    }

    pub fn next_session_counter(&self) -> u32 {
        self.next_session_id.load(Ordering::Relaxed)
    }

    pub fn set_next_session_counter(&self, next: u32) {
        self.next_session_id.store(next, Ordering::Relaxed);
    }

    fn ensure(&self, session: SessionId) -> Arc<RevisionWatermark> {
        if let Some(wm) = self.sessions.read().get(&session.0) {
            return Arc::clone(wm);
        }
        let mut map = self.sessions.write();
        map.entry(session.0)
            .or_insert_with(|| Arc::new(RevisionWatermark::new(0)))
            .clone()
    }

    pub fn watermark_for(&self, session: SessionId) -> Arc<RevisionWatermark> {
        self.ensure(session)
    }

    pub fn session_zero(&self) -> Arc<RevisionWatermark> {
        self.watermark_for(SessionId(GLOBAL_SESSION))
    }

    /// Implicit backward-compatible path (session 0 legacy stream).
    pub fn allocate(&self) -> RevisionId {
        self.session_zero().allocate()
    }

    pub fn allocate_n(&self, count: u64) -> RevisionRange {
        self.session_zero().allocate_n(count)
    }

    pub fn set_legacy_seed(&self, value: u64) {
        self.session_zero().set_revision(value);
    }

    pub fn allocated(&self) -> RevisionId {
        self.head_revision()
    }

    /// Highest issued revision across all sessions.
    pub fn head_revision(&self) -> RevisionId {
        self.sessions
            .read()
            .values()
            .map(|wm| wm.allocated())
            .max()
            .unwrap_or(RevisionId::ZERO)
    }

    /// Scalar stable ceiling — meet of per-session stables (frame convenience).
    pub fn stable_revision(&self) -> RevisionId {
        self.scalar_stable_meet()
    }

    pub fn scalar_stable_meet(&self) -> RevisionId {
        self.sessions
            .read()
            .values()
            .map(|wm| wm.stable_revision())
            .min()
            .unwrap_or(RevisionId::ZERO)
    }

    pub fn stable_for(&self, session: SessionId) -> RevisionId {
        self.watermark_for(session).stable_revision()
    }

    pub fn capture_version_vector(&self) -> VersionVector {
        let map = self.sessions.read();
        VersionVector(
            map.iter()
                .map(|(&sid, wm)| (SessionId(sid), wm.stable_revision()))
                .collect(),
        )
    }

    pub fn retire(&self, rev: RevisionId) {
        self.watermark_for(SessionId(rev.session())).retire(rev);
    }

    pub fn retire_failed(&self, rev: RevisionId, error: impl Into<String>) {
        self.watermark_for(SessionId(rev.session()))
            .retire_failed(rev, error);
    }

    pub fn failed_revisions(&self) -> Vec<FailedRevision> {
        let mut out = Vec::new();
        for wm in self.sessions.read().values() {
            out.extend(wm.failed_revisions());
        }
        out.sort_by_key(|f| f.revision);
        out
    }

    pub fn take_failed(&self) -> Vec<FailedRevision> {
        let mut out = Vec::new();
        for wm in self.sessions.read().values() {
            out.extend(wm.take_failed());
        }
        out.sort_by_key(|f| f.revision);
        out
    }

    /// Mint a new `SessionId` from the local registry (D-P2).
    pub fn mint_session_id(&self) -> SessionId {
        let id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        self.ensure(SessionId(id));
        SessionId(id)
    }

    /// Whether a session id has been minted or hydrated (Phase 5 frame validation).
    pub fn session_registered(&self, session: SessionId) -> bool {
        self.sessions.read().contains_key(&session.0)
    }

    /// Restore per-session allocation heads from persisted session-WAL meta (Phase 3).
    pub fn hydrate_from_wal_meta(&self, meta: &SessionWalMeta) {
        for (&sid, retirement) in &meta.retirements {
            if sid == GLOBAL_SESSION {
                continue;
            }
            if retirement.highest_revision() > RevisionId::ZERO {
                self.watermark_for(SessionId(sid))
                    .seed_allocated(retirement.highest_revision());
            }
        }
    }
}

/// Contention-free write handle for one asserting stream.
#[derive(Clone)]
pub struct WriteSession {
    id: SessionId,
    watermarks: Arc<SessionWatermarks>,
    clock: Option<Arc<Mutex<HlcClock>>>,
    wal_store: Option<Arc<SessionWalStore>>,
    pending: Arc<Mutex<PendingIntentState>>,
}

impl WriteSession {
    /// Implicit session-0 stream (legacy-compatible single-writer API).
    pub fn implicit_global(watermarks: Arc<SessionWatermarks>) -> Self {
        Self {
            id: SessionId(GLOBAL_SESSION),
            watermarks,
            clock: None,
            wal_store: None,
            pending: Arc::new(Mutex::new(PendingIntentState::default())),
        }
    }

    /// Open a new session with a locally minted id and dedicated WAL (Phase 3).
    pub fn open(
        watermarks: Arc<SessionWatermarks>,
        wal_store: Arc<SessionWalStore>,
    ) -> Self {
        let id = watermarks.mint_session_id();
        let _ = wal_store.ensure_writer(id);
        Self {
            id,
            watermarks,
            clock: Some(Arc::new(Mutex::new(HlcClock::new(id)))),
            wal_store: Some(wal_store),
            pending: Arc::new(Mutex::new(PendingIntentState::default())),
        }
    }

    pub fn id(&self) -> SessionId {
        self.id
    }

    pub fn wal_store(&self) -> Option<&Arc<SessionWalStore>> {
        self.wal_store.as_ref()
    }

    pub fn uses_session_wal(&self) -> bool {
        self.wal_store.is_some() && self.id.0 != GLOBAL_SESSION
    }

    pub fn has_pending_intent(&self) -> bool {
        self.pending.lock().has_pending()
    }

    /// Record one durable session-WAL frame awaiting intent checkpoint commit.
    pub(crate) fn note_buffered_write(
        &self,
        entry: WalEntry,
        hilbert_key: HilbertKey,
        revision: RevisionId,
        kind: IntentOperationKind,
    ) {
        self.note_buffered_write_with_event(entry, hilbert_key, revision, kind, None);
    }

    /// Record a buffered write with an optional derivation-bus sidecar (Phase 5 hyperedges).
    pub(crate) fn note_buffered_write_with_event(
        &self,
        entry: WalEntry,
        hilbert_key: HilbertKey,
        revision: RevisionId,
        kind: IntentOperationKind,
        assertion_event: Option<AssertionEvent>,
    ) {
        if self.uses_session_wal() {
            self.pending.lock().note_write(
                entry,
                hilbert_key,
                revision,
                kind,
                assertion_event,
            );
        }
    }

    pub(crate) fn peek_pending_entries(&self) -> Vec<WalEntry> {
        self.pending.lock().peek_buffered_entries()
    }

    pub(crate) fn mark_durable(&self, medium: DurabilityMedium) -> DurableIntent {
        self.pending.lock().mark_durable(medium);
        DurableIntent {
            session: self.id,
            medium,
        }
    }

    pub(crate) fn take_durable_pending(
        &self,
        durable: &DurableIntent,
    ) -> Result<(IntentCheckpoint, Vec<BufferedSessionWrite>, DurabilityMedium), String> {
        if durable.session != self.id {
            return Err("durable intent token session mismatch".into());
        }
        let mut pending = self.pending.lock();
        let checkpoint = pending.build_checkpoint()?;
        let (buffered, medium) = pending.take_if_durable()?;
        if durable.medium != medium {
            return Err("durable intent medium mismatch".into());
        }
        Ok((checkpoint, buffered, medium))
    }

    /// Validate → stamp → register outstanding (Phase 2 write path).
    pub fn stamp(&self) -> RevisionId {
        if self.id.0 == GLOBAL_SESSION {
            self.watermarks.session_zero().allocate()
        } else {
            let stamp = self.clock.as_ref().unwrap().lock().stamp();
            let rev = RevisionId::from_stamp(stamp);
            self.watermarks
                .watermark_for(self.id)
                .register_outstanding(rev);
            rev
        }
    }

    pub fn stamp_n(&self, count: u64) -> RevisionRange {
        debug_assert!(count > 0, "stamp_n requires count > 0");
        if self.id.0 == GLOBAL_SESSION {
            self.watermarks.session_zero().allocate_n(count)
        } else {
            let stamps = self.clock.as_ref().unwrap().lock().stamp_n(count);
            let first = RevisionId::from_stamp(stamps[0]);
            let last = RevisionId::from_stamp(*stamps.last().unwrap());
            let wm = self.watermarks.watermark_for(self.id);
            for stamp in stamps {
                wm.register_outstanding(RevisionId::from_stamp(stamp));
            }
            RevisionRange::new(first, last)
        }
    }
}
