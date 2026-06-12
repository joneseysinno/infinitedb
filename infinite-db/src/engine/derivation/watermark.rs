//! Per-subscriber derivation completeness watermarks (Phase 6: per-session vectors).
//!
//! Uses the outstanding-set / contiguous-prefix pattern from [`RevisionWatermark`]:
//! register on bus submit, retire on successful apply, watermark = predecessor of
//! lowest outstanding (or highest retired when empty). Failed derivations stay
//! outstanding and block advancement.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::engine::session::VersionVector;
use crate::infinitedb_core::address::RevisionId;
use crate::infinitedb_core::hlc::{SessionId, GLOBAL_SESSION};

const MAX_FAILED_RECORDS: usize = 64;

/// Record of a derivation event that could not be applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedDerivation {
    pub revision: RevisionId,
    pub error: String,
}

#[derive(Debug)]
struct DerivationWatermarkState {
    outstanding: BTreeSet<RevisionId>,
    registered_high: RevisionId,
    failed: VecDeque<FailedDerivation>,
}

/// Highest revision through which derived rows are durably complete.
fn compute_complete(state: &DerivationWatermarkState) -> RevisionId {
    if state.outstanding.is_empty() {
        state.registered_high
    } else {
        state
            .outstanding
            .first()
            .copied()
            .unwrap_or(RevisionId::ZERO)
            .predecessor()
    }
}

/// Tracks in-flight derivation events for one session stream.
#[derive(Debug)]
pub struct DerivationWatermark {
    state: Mutex<DerivationWatermarkState>,
}

impl DerivationWatermark {
    pub fn new(initial: RevisionId) -> Self {
        Self {
            state: Mutex::new(DerivationWatermarkState {
                outstanding: BTreeSet::new(),
                registered_high: initial,
                failed: VecDeque::new(),
            }),
        }
    }

    /// Register an event revision when submitted to the bus.
    pub fn register(&self, revision: RevisionId) {
        let mut state = self.state.lock();
        state.outstanding.insert(revision);
        if revision > state.registered_high {
            state.registered_high = revision;
        }
    }

    /// Retire after derived rows are durably applied.
    pub fn retire(&self, revision: RevisionId) {
        self.state.lock().outstanding.remove(&revision);
    }

    /// Record a permanent failure; revision stays outstanding and blocks advancement.
    pub fn record_failure(&self, revision: RevisionId, error: impl Into<String>) {
        let mut state = self.state.lock();
        if state.failed.len() >= MAX_FAILED_RECORDS {
            state.failed.pop_front();
        }
        state.failed.push_back(FailedDerivation {
            revision,
            error: error.into(),
        });
    }

    pub fn get(&self) -> RevisionId {
        compute_complete(&self.state.lock())
    }

    pub fn failed_derivations(&self) -> Vec<FailedDerivation> {
        self.state.lock().failed.iter().cloned().collect()
    }

    pub fn outstanding_count(&self) -> usize {
        self.state.lock().outstanding.len()
    }
}

/// Per-session derivation watermarks for one subscriber (Phase 6).
#[derive(Debug)]
pub struct DerivationSessionWatermark {
    sessions: Mutex<BTreeMap<u32, Arc<DerivationWatermark>>>,
}

impl DerivationSessionWatermark {
    pub fn new(initial: RevisionId) -> Self {
        let mut map = BTreeMap::new();
        map.insert(
            GLOBAL_SESSION,
            Arc::new(DerivationWatermark::new(initial)),
        );
        Self {
            sessions: Mutex::new(map),
        }
    }

    fn ensure(&self, session: SessionId) -> Arc<DerivationWatermark> {
        if let Some(wm) = self.sessions.lock().get(&session.0) {
            return Arc::clone(wm);
        }
        let mut map = self.sessions.lock();
        map.entry(session.0)
            .or_insert_with(|| Arc::new(DerivationWatermark::new(RevisionId::ZERO)))
            .clone()
    }

    pub fn register(&self, session: SessionId, revision: RevisionId) {
        self.ensure(session).register(revision);
    }

    pub fn retire(&self, session: SessionId, revision: RevisionId) {
        self.ensure(session).retire(revision);
    }

    pub fn record_failure(
        &self,
        session: SessionId,
        revision: RevisionId,
        error: impl Into<String>,
    ) {
        self.ensure(session).record_failure(revision, error);
    }

    pub fn get(&self, session: SessionId) -> RevisionId {
        self.sessions
            .lock()
            .get(&session.0)
            .map(|wm| wm.get())
            .unwrap_or(RevisionId::ZERO)
    }

    pub fn capture_vector(&self) -> VersionVector {
        VersionVector(
            self.sessions
                .lock()
                .iter()
                .map(|(&sid, wm)| (SessionId(sid), wm.get()))
                .collect(),
        )
    }

    pub fn total_outstanding(&self) -> usize {
        self.sessions
            .lock()
            .values()
            .map(|wm| wm.outstanding_count())
            .sum()
    }

    pub fn outstanding_for_session(&self, session: SessionId) -> usize {
        self.sessions
            .lock()
            .get(&session.0)
            .map(|wm| wm.outstanding_count())
            .unwrap_or(0)
    }

    pub fn failed_derivations(&self) -> Vec<FailedDerivation> {
        let mut out = Vec::new();
        for wm in self.sessions.lock().values() {
            out.extend(wm.failed_derivations());
        }
        out
    }

    /// Lag estimate for backpressure within one session stream.
    pub fn lag_for_session(&self, session: SessionId, head: RevisionId) -> u64 {
        let complete = self.get(session);
        let outstanding = self.outstanding_for_session(session) as u64;
        if head <= complete {
            return outstanding;
        }
        if head.session() == session.0 {
            return outstanding.saturating_add(
                head.sequence()
                    .saturating_sub(complete.sequence()) as u64,
            );
        }
        outstanding
    }
}

/// Named subscriber with its own per-session derivation watermark.
#[derive(Debug)]
pub struct SubscriberWatermark {
    pub id: &'static str,
    pub watermark: DerivationSessionWatermark,
}

impl SubscriberWatermark {
    pub fn new(id: &'static str, initial: RevisionId) -> Self {
        Self {
            id,
            watermark: DerivationSessionWatermark::new(initial),
        }
    }
}

/// Registry of subscriber watermarks.
#[derive(Debug)]
pub struct WatermarkRegistry {
    subscribers: Mutex<Vec<SubscriberWatermark>>,
}

impl WatermarkRegistry {
    pub fn new() -> Self {
        Self {
            subscribers: Mutex::new(Vec::new()),
        }
    }

    pub fn register(&self, id: &'static str, initial: RevisionId) {
        self.subscribers
            .lock()
            .push(SubscriberWatermark::new(id, initial));
    }

    pub fn register_for_session(&self, session: SessionId, revision: RevisionId) {
        for sub in self.subscribers.lock().iter() {
            sub.watermark.register(session, revision);
        }
    }

    pub fn retire_for_session(&self, session: SessionId, revision: RevisionId) {
        for sub in self.subscribers.lock().iter() {
            sub.watermark.retire(session, revision);
        }
    }

    pub fn record_failure_for_session(
        &self,
        session: SessionId,
        revision: RevisionId,
        error: impl Into<String>,
    ) {
        let message = error.into();
        for sub in self.subscribers.lock().iter() {
            sub.watermark
                .record_failure(session, revision, &message);
        }
    }

    pub fn get_vector(&self, id: &str) -> Option<VersionVector> {
        self.subscribers
            .lock()
            .iter()
            .find(|s| s.id == id)
            .map(|s| s.watermark.capture_vector())
    }

    /// Minimum complete-through revision across subscribers for one session.
    pub fn min_complete_for_session(&self, session: SessionId) -> RevisionId {
        self.subscribers
            .lock()
            .iter()
            .map(|s| s.watermark.get(session))
            .min()
            .unwrap_or(RevisionId::ZERO)
    }

    pub fn lag_for_session(&self, session: SessionId, head: RevisionId) -> u64 {
        self.subscribers
            .lock()
            .iter()
            .map(|s| s.watermark.lag_for_session(session, head))
            .max()
            .unwrap_or(0)
    }

    pub fn failed_derivations(&self) -> Vec<FailedDerivation> {
        let mut out = Vec::new();
        for sub in self.subscribers.lock().iter() {
            out.extend(sub.watermark.failed_derivations());
        }
        out
    }

    pub fn total_outstanding(&self) -> usize {
        self.subscribers
            .lock()
            .iter()
            .map(|sub| sub.watermark.total_outstanding())
            .sum()
    }

    /// Per-session minimum complete revision across all subscribers.
    pub fn min_vector(&self) -> VersionVector {
        let mut merged = BTreeMap::new();
        for sub in self.subscribers.lock().iter() {
            for (session, rev) in sub.watermark.capture_vector().0 {
                merged
                    .entry(session)
                    .and_modify(|m: &mut RevisionId| *m = (*m).min(rev))
                    .or_insert(rev);
            }
        }
        VersionVector(merged)
    }
}

impl Default for WatermarkRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::hlc::HlcStamp;

    #[test]
    fn complete_waits_on_outstanding_gap() {
        let wm = DerivationWatermark::new(RevisionId::ZERO);
        wm.register(RevisionId::legacy(1));
        wm.register(RevisionId::legacy(2));
        wm.register(RevisionId::legacy(3));
        wm.retire(RevisionId::legacy(1));
        wm.retire(RevisionId::legacy(3));
        assert_eq!(wm.get(), RevisionId::legacy(1));
        wm.retire(RevisionId::legacy(2));
        assert_eq!(wm.get(), RevisionId::legacy(3));
    }

    #[test]
    fn failure_blocks_advancement() {
        let wm = DerivationWatermark::new(RevisionId::ZERO);
        wm.register(RevisionId::legacy(1));
        wm.register(RevisionId::legacy(2));
        wm.retire(RevisionId::legacy(1));
        wm.record_failure(RevisionId::legacy(2), "sink error");
        assert_eq!(wm.get(), RevisionId::legacy(1));
        assert_eq!(wm.failed_derivations().len(), 1);
    }

    #[test]
    fn out_of_order_retire_does_not_skip() {
        let wm = DerivationWatermark::new(RevisionId::ZERO);
        wm.register(RevisionId::legacy(1));
        wm.register(RevisionId::legacy(2));
        wm.retire(RevisionId::legacy(2));
        assert_eq!(wm.get(), RevisionId::ZERO);
        wm.retire(RevisionId::legacy(1));
        assert_eq!(wm.get(), RevisionId::legacy(2));
    }

    #[test]
    fn per_session_watermark_independent_advance() {
        let wm = DerivationSessionWatermark::new(RevisionId::ZERO);
        let s1 = SessionId(1);
        let s2 = SessionId(2);
        let r1a = RevisionId::from_stamp(HlcStamp {
            physical_ms: 1,
            logical: 0,
            session: s1.0,
            sequence: 1,
        });
        let r1b = RevisionId::from_stamp(HlcStamp {
            physical_ms: 1,
            logical: 0,
            session: s1.0,
            sequence: 2,
        });
        let r2a = RevisionId::from_stamp(HlcStamp {
            physical_ms: 1,
            logical: 0,
            session: s2.0,
            sequence: 1,
        });
        wm.register(s1, r1a);
        wm.register(s1, r1b);
        wm.register(s2, r2a);
        wm.retire(s2, r2a);
        assert_eq!(wm.get(s2), r2a);
        assert_eq!(wm.get(s1), r1a.predecessor());
        wm.retire(s1, r1a);
        assert_eq!(wm.get(s1), r1a);
        let vec = wm.capture_vector();
        assert_eq!(vec.get(s1), Some(r1a));
        assert_eq!(vec.get(s2), Some(r2a));
    }
}
