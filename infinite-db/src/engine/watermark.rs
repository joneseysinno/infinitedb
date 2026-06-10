//! Stable revision watermark for repeatable reads.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::infinitedb_core::address::RevisionId;

/// Tracks in-flight revisions and the highest durably applied revision.
///
/// `stable_revision` is the highest revision R such that every write with
/// revision ≤ R has been applied and published. `revision()` (allocation
/// counter) may be higher while writes are still queued per shard.
pub struct RevisionWatermark {
    outstanding: Mutex<BTreeSet<u64>>,
    allocated: Arc<AtomicU64>,
}

impl RevisionWatermark {
    /// Share the allocation counter with [`InfiniteDb::revision`].
    pub fn new(allocated: Arc<AtomicU64>) -> Self {
        Self {
            outstanding: Mutex::new(BTreeSet::new()),
            allocated,
        }
    }

    /// Register a revision at enqueue time (before durable apply).
    pub fn register(&self, rev: u64) {
        self.outstanding.lock().insert(rev);
    }

    /// Retire a revision after durable apply and live-tail publish.
    pub fn retire(&self, rev: u64) {
        self.outstanding.lock().remove(&rev);
    }

    /// Highest revision guaranteed visible to readers (repeatable-read ceiling).
    pub fn stable_revision(&self) -> RevisionId {
        let guard = self.outstanding.lock();
        let stable = if guard.is_empty() {
            self.allocated.load(Ordering::Acquire)
        } else {
            guard.first().copied().unwrap_or(0).saturating_sub(1)
        };
        RevisionId(stable)
    }
}
