//! Phase 7 — timed fast-path policy and session-write observability.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::engine::io_thread::IoThreadConfig;

/// Optional timed fast path for session writes (default off).
#[derive(Debug, Clone)]
pub struct TimedFastPathPolicy {
    pub enabled: bool,
    pub direct_seal_deadline: Duration,
}

impl TimedFastPathPolicy {
    pub fn from_io_config(io: &IoThreadConfig) -> Self {
        Self {
            enabled: false,
            direct_seal_deadline: io.direct_write_timeout,
        }
    }

    pub fn enabled_with_deadline(deadline: Duration) -> Self {
        Self {
            enabled: true,
            direct_seal_deadline: deadline,
        }
    }
}

impl Default for TimedFastPathPolicy {
    fn default() -> Self {
        Self::from_io_config(&IoThreadConfig::default())
    }
}

/// Durability medium used for the current pending intent group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMedium {
    SessionWal,
    FastSegment,
}

/// Runtime counters for session fast-path decisions.
#[derive(Debug, Default)]
pub struct SessionWriteStats {
    fast_path_seal_success: AtomicU64,
    fast_path_seal_timeout: AtomicU64,
    fast_path_wal_fallback: AtomicU64,
}

impl SessionWriteStats {
    pub fn record_fast_seal_success(&self) {
        self.fast_path_seal_success.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_fast_seal_timeout(&self) {
        self.fast_path_seal_timeout.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_wal_fallback(&self) {
        self.fast_path_wal_fallback.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> SessionWriteStatsSnapshot {
        SessionWriteStatsSnapshot {
            fast_path_seal_success: self.fast_path_seal_success.load(Ordering::Relaxed),
            fast_path_seal_timeout: self.fast_path_seal_timeout.load(Ordering::Relaxed),
            fast_path_wal_fallback: self.fast_path_wal_fallback.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionWriteStatsSnapshot {
    pub fast_path_seal_success: u64,
    pub fast_path_seal_timeout: u64,
    pub fast_path_wal_fallback: u64,
}
