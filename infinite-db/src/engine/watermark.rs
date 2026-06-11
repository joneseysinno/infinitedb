//! Stable revision watermark for repeatable reads.

use std::collections::{BTreeSet, VecDeque};

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

/// Contiguous revision ids allocated in one [`RevisionWatermark::allocate_n`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RevisionRange {
    first: RevisionId,
    last: RevisionId,
}

impl RevisionRange {
    /// First revision in the range (inclusive).
    pub fn first(self) -> RevisionId {
        self.first
    }

    /// Last revision in the range (inclusive).
    pub fn last(self) -> RevisionId {
        self.last
    }

    /// Return the revision at `index` (0 = first).
    pub fn nth(self, index: u64) -> RevisionId {
        RevisionId(self.first.0 + index)
    }
}

/// Internal watermark state mutated under a single lock.
#[derive(Debug)]
struct WatermarkState {
    allocated: RevisionId,
    outstanding: BTreeSet<RevisionId>,
    failed: VecDeque<FailedRevision>,
}

/// Pure interpretation of [`WatermarkState`] for the repeatable-read ceiling.
fn compute_stable(state: &WatermarkState) -> RevisionId {
    if state.outstanding.is_empty() {
        state.allocated
    } else {
        state
            .outstanding
            .first()
            .copied()
            .unwrap_or(RevisionId::ZERO)
            .predecessor()
    }
}

/// Tracks in-flight revisions and the highest durably applied revision.
///
/// `stable_revision` is the highest revision R such that every write with
/// revision ≤ R has been applied and published, or reported as failed.
/// [`Self::allocated`] may be higher while writes are still queued per shard.
///
/// Allocation and registration are a single atomic operation under one lock: a
/// revision is never visible to `stable_revision` as allocated-but-not-outstanding.
pub struct RevisionWatermark {
    state: Mutex<WatermarkState>,
}

const MAX_FAILED_RECORDS: usize = 64;

impl RevisionWatermark {
    /// Create a watermark with the allocation counter seeded to `initial`.
    pub fn new(initial: u64) -> Self {
        Self {
            state: Mutex::new(WatermarkState {
                allocated: RevisionId(initial),
                outstanding: BTreeSet::new(),
                failed: VecDeque::new(),
            }),
        }
    }

    /// Allocate the next revision and register it as outstanding.
    pub fn allocate(&self) -> RevisionId {
        let mut state = self.state.lock();
        let rev = RevisionId(state.allocated.0 + 1);
        state.allocated = rev;
        state.outstanding.insert(rev);
        rev
    }

    /// Allocate a contiguous run of revisions in one lock acquisition.
    pub fn allocate_n(&self, count: u64) -> RevisionRange {
        debug_assert!(count > 0, "allocate_n requires count > 0");
        let mut state = self.state.lock();
        let first = RevisionId(state.allocated.0 + 1);
        let last = RevisionId(first.0 + count - 1);
        state.allocated = last;
        for rev in first.0..=last.0 {
            state.outstanding.insert(RevisionId(rev));
        }
        RevisionRange { first, last }
    }

    /// Highest allocated revision id (may not yet be durable).
    pub fn allocated(&self) -> RevisionId {
        self.state.lock().allocated
    }

    /// Seed the allocation counter (database open / recovery).
    pub fn set_revision(&self, value: u64) {
        self.state.lock().allocated = RevisionId(value);
    }

    /// Retire a revision after durable apply and live-tail publish.
    pub fn retire(&self, rev: RevisionId) {
        self.state.lock().outstanding.remove(&rev);
    }

    /// Abandon a revision that can no longer succeed (I/O failure).
    pub fn retire_failed(&self, rev: RevisionId, error: impl Into<String>) {
        let mut state = self.state.lock();
        state.outstanding.remove(&rev);
        if state.failed.len() >= MAX_FAILED_RECORDS {
            state.failed.pop_front();
        }
        state.failed.push_back(FailedRevision {
            revision: rev,
            error: error.into(),
        });
    }

    /// Drain recorded write failures (most recent retained).
    pub fn take_failed(&self) -> Vec<FailedRevision> {
        self.state.lock().failed.drain(..).collect()
    }

    /// Highest revision guaranteed visible to readers (repeatable-read ceiling).
    ///
    /// Every revision ≤ the returned value has either been durably applied or
    /// reported through [`Self::take_failed`]; stable never waits on a revision
    /// that can no longer succeed.
    pub fn stable_revision(&self) -> RevisionId {
        compute_stable(&self.state.lock())
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

    #[test]
    fn allocate_registers_before_stable_observes() {
        let wm = RevisionWatermark::new(0);
        let rev = wm.allocate();
        assert!(
            wm.stable_revision() < rev,
            "stable must lag behind an unretired allocation"
        );
        wm.retire(rev);
        assert_eq!(wm.stable_revision(), rev);
    }

    #[test]
    fn compute_stable_cases() {
        let empty = WatermarkState {
            allocated: RevisionId(5),
            outstanding: BTreeSet::new(),
            failed: VecDeque::new(),
        };
        assert_eq!(compute_stable(&empty), RevisionId(5));

        let with_gap = WatermarkState {
            allocated: RevisionId(3),
            outstanding: BTreeSet::from([RevisionId(2)]),
            failed: VecDeque::new(),
        };
        assert_eq!(compute_stable(&with_gap), RevisionId(1));

        let dense = WatermarkState {
            allocated: RevisionId(3),
            outstanding: BTreeSet::from([RevisionId(1), RevisionId(2), RevisionId(3)]),
            failed: VecDeque::new(),
        };
        assert_eq!(compute_stable(&dense), RevisionId::ZERO);
    }

    #[test]
    fn revision_range_nth() {
        let range = RevisionRange {
            first: RevisionId(10),
            last: RevisionId(12),
        };
        assert_eq!(range.nth(0), RevisionId(10));
        assert_eq!(range.nth(2), RevisionId(12));
    }

    #[test]
    fn allocation_registration_atomic_under_contention() {
        use std::sync::Arc;
        use std::thread;

        let wm = Arc::new(RevisionWatermark::new(0));
        let wm_writer = Arc::clone(&wm);
        let writer = thread::spawn(move || {
            for _ in 0..500 {
                let rev = wm_writer.allocate();
                assert!(
                    wm_writer.stable_revision() < rev,
                    "stable must lag behind an unretired allocation"
                );
                thread::yield_now();
                wm_writer.retire(rev);
            }
        });

        for _ in 0..10_000 {
            let stable = wm.stable_revision();
            let allocated = wm.allocated();
            assert!(
                stable <= allocated,
                "stable {stable:?} must not exceed allocated {allocated:?}"
            );
            thread::yield_now();
        }
        writer.join().unwrap();
    }
}
