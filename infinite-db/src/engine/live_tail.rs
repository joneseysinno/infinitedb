//! In-memory live tail published by the I/O thread after fsync.

use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;

use crate::infinitedb_core::{
    address::RevisionId,
    block::Record,
};

/// Records visible to readers before hot segment seal.
pub struct LiveTailView {
    records: ArcSwap<Vec<Record>>,
    captured_at: AtomicU64,
}

impl LiveTailView {
    pub fn new() -> Self {
        Self {
            records: ArcSwap::from_pointee(Vec::new()),
            captured_at: AtomicU64::new(0),
        }
    }

    /// Replace the visible tail (called from the I/O thread).
    pub fn publish(&self, records: Vec<Record>) {
        let max_rev = records.iter().map(|r| r.revision.0).max().unwrap_or(0);
        self.records.store(std::sync::Arc::new(records));
        self.captured_at.store(max_rev, Ordering::Release);
    }

    /// Append one durable record to the published tail.
    pub fn append(&self, record: Record) {
        let mut next = self.snapshot();
        next.push(record);
        let max_rev = next.iter().map(|r| r.revision.0).max().unwrap_or(0);
        self.records.store(std::sync::Arc::new(next));
        self.captured_at.store(max_rev, Ordering::Release);
    }

    /// Current snapshot of the live tail.
    pub fn snapshot(&self) -> Vec<Record> {
        self.records.load_full().as_ref().clone()
    }

    pub fn captured_at(&self) -> RevisionId {
        RevisionId(self.captured_at.load(Ordering::Acquire))
    }
}

impl Default for LiveTailView {
    fn default() -> Self {
        Self::new()
    }
}
