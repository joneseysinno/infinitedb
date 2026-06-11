//! Per-subscriber derivation completeness watermarks.

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::infinitedb_core::address::RevisionId;

/// Tracks the highest assertion revision through which a derived structure is complete.
#[derive(Debug)]
pub struct DerivationWatermark {
    complete_through: AtomicU64,
}

impl DerivationWatermark {
    pub fn new(initial: RevisionId) -> Self {
        Self {
            complete_through: AtomicU64::new(initial.0),
        }
    }

    pub fn get(&self) -> RevisionId {
        RevisionId(self.complete_through.load(Ordering::Acquire))
    }

    /// Advance only when `revision` exceeds the current watermark.
    pub fn advance_to(&self, revision: RevisionId) {
        let mut current = self.complete_through.load(Ordering::Relaxed);
        while revision.0 > current {
            match self.complete_through.compare_exchange_weak(
                current,
                revision.0,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }
}

/// Named subscriber with its own derivation watermark.
#[derive(Debug)]
pub struct SubscriberWatermark {
    pub id: &'static str,
    pub watermark: DerivationWatermark,
}

impl SubscriberWatermark {
    pub fn new(id: &'static str, initial: RevisionId) -> Self {
        Self {
            id,
            watermark: DerivationWatermark::new(initial),
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

    pub fn advance_all(&self, revision: RevisionId) {
        for sub in self.subscribers.lock().iter() {
            sub.watermark.advance_to(revision);
        }
    }

    pub fn get(&self, id: &str) -> Option<RevisionId> {
        self.subscribers
            .lock()
            .iter()
            .find(|s| s.id == id)
            .map(|s| s.watermark.get())
    }

    pub fn min_watermark(&self) -> RevisionId {
        self.subscribers
            .lock()
            .iter()
            .map(|s| s.watermark.get())
            .min()
            .unwrap_or(RevisionId::ZERO)
    }

}

impl Default for WatermarkRegistry {
    fn default() -> Self {
        Self::new()
    }
}
