//! I/O thread tuning and runtime counters (format v4 Hilbert shards).

/// Tuning for per-shard I/O threads.
#[derive(Debug, Clone)]
pub struct IoThreadConfig {
    /// Deadline for session timed fast-path direct seal attempts (Phase 7).
    pub direct_write_timeout: std::time::Duration,
    /// Secondary seal trigger by record count (pathological tiny records).
    pub hot_segment_seal_threshold: usize,
    /// Primary seal trigger by committed hot-segment bytes.
    pub hot_segment_seal_bytes: usize,
    pub write_queue_capacity: usize,
    pub wal_group_commit_interval: std::time::Duration,
}

impl Default for IoThreadConfig {
    fn default() -> Self {
        Self {
            direct_write_timeout: std::time::Duration::from_millis(2),
            hot_segment_seal_threshold: 65_536,
            hot_segment_seal_bytes: 8 * 1024 * 1024,
            write_queue_capacity: 4096,
            wal_group_commit_interval: std::time::Duration::from_millis(1),
        }
    }
}

/// Runtime statistics aggregated across Hilbert shard I/O threads.
#[derive(Debug, Clone, Default)]
pub struct IoStats {
    pub queue_depth: usize,
    pub direct_writes: u64,
    pub staging_wal_frames: usize,
    pub fast_path_seal_success: u64,
    pub fast_path_seal_timeout: u64,
    pub fast_path_wal_fallback: u64,
}
