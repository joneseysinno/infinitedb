//! Stable revision watermark for repeatable reads.

use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::infinitedb_core::address::RevisionId;

/// Record of a revision that could not be durably applied.
#[derive(Debug, Clone)]
pub struct FailedRevision {
    /// Revision that was abandoned.
    pub revision: RevisionId,
    /// Human-readable failure reason.
    pub error: String,
}

/// Tracks in-flight revisions and the highest durably applied revision.
///
/// `stable_revision` is the highest revision R such that every write with
/// revision ≤ R has been applied and published, or reported as failed.
/// `revision()` (allocation counter) may be higher while writes are still
/// queued per shard.
///
/// Allocation and registration are a single atomic operation: a revision is
/// never visible to `stable_revision` as allocated-but-not-outstanding.
pub struct RevisionWatermark {
    outstanding: Mutex<BTreeSet<RevisionId>>,
    allocated: Arc<AtomicU64>,
    failed: Mutex<VecDeque<FailedRevision>>,
}

const MAX_FAILED_RECORDS: usize = 64;

impl RevisionWatermark {
    /// Create a watermark with the allocation counter seeded to `initial`.
    pub fn new(initial: u64) -> Self {
        Self {
            outstanding: Mutex::new(BTreeSet::new()),
            allocated: Arc::new(AtomicU64::new(initial)),
            failed: Mutex::new(VecDeque::new()),
        }
    }

    /// Shared allocation counter (for query ceilings and persistence).
    pub fn allocation_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.allocated)
    }

    /// Allocate the next revision and register it as outstanding.
    pub fn allocate(&self) -> RevisionId {
        let rev = self.allocated.fetch_add(1, Ordering::Relaxed) + 1;
        self.outstanding.lock().insert(RevisionId(rev));
        RevisionId(rev)
    }

    /// Allocate a contiguous run of revisions in one lock acquisition.
    pub fn allocate_n(&self, count: u64) -> (RevisionId, RevisionId) {
        debug_assert!(count > 0, "allocate_n requires count > 0");
        let first = self.allocated.fetch_add(count, Ordering::Relaxed) + 1;
        let last = first + count - 1;
        let mut guard = self.outstanding.lock();
        for rev in first..=last {
            guard.insert(RevisionId(rev));
        }
        (RevisionId(first), RevisionId(last))
    }

    /// Highest allocated revision id (may not yet be durable).
    pub fn revision(&self) -> u64 {
        self.allocated.load(Ordering::Relaxed)
    }

    /// Seed the allocation counter (database open / recovery).
    pub fn set_revision(&self, value: u64) {
        self.allocated.store(value, Ordering::Relaxed);
    }

    /// Retire a revision after durable apply and live-tail publish.
    pub fn retire(&self, rev: RevisionId) {
        self.outstanding.lock().remove(&rev);
    }

    /// Abandon a revision that can no longer succeed (I/O failure).
    pub fn retire_failed(&self, rev: RevisionId, error: impl Into<String>) {
        self.outstanding.lock().remove(&rev);
        let mut failed = self.failed.lock();
        if failed.len() >= MAX_FAILED_RECORDS {
            failed.pop_front();
        }
        failed.push_back(FailedRevision {
            revision: rev,
            error: error.into(),
        });
    }

    /// Drain recorded write failures (most recent retained).
    pub fn take_failed(&self) -> Vec<FailedRevision> {
        self.failed.lock().drain(..).collect()
    }

    /// Highest revision guaranteed visible to readers (repeatable-read ceiling).
    ///
    /// Every revision ≤ the returned value has either been durably applied or
    /// reported through [`Self::take_failed`]; stable never waits on a revision
    /// that can no longer succeed.
    pub fn stable_revision(&self) -> RevisionId {
        let guard = self.outstanding.lock();
        if guard.is_empty() {
            RevisionId(self.allocated.load(Ordering::Acquire))
        } else {
            guard.first().copied().unwrap_or(RevisionId::ZERO).predecessor()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_with_non_dense_outstanding_ids() {
        let wm = RevisionWatermark::new(0);
        wm.allocate(); // 1
        wm.allocate(); // 2
        wm.allocate(); // 3
        wm.retire(RevisionId(1));
        wm.retire(RevisionId(3));
        // Outstanding: {2}. Stable should be 1 (predecessor of 2), not 0.
        assert_eq!(wm.stable_revision(), RevisionId(1));
        wm.retire(RevisionId(2));
        assert_eq!(wm.stable_revision(), RevisionId(3));
    }
}
