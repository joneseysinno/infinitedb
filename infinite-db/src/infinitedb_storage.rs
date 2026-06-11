//! Embedded storage engine components.

/// On-disk format version markers.
pub mod format;
/// Cluster node metadata (format v5 additive).
#[cfg(feature = "sync")]
pub mod cluster;
/// Append-only hot segment tails (direct-write path).
pub mod hot_segment;
/// Write-ahead logging and recovery.
pub mod wal;
/// NVMe-friendly block store and cache.
pub mod nvme;
/// Block compaction routines.
pub mod compaction;
/// Retention policy and garbage collection helpers.
pub mod gc;
/// Storage-layer error types.
pub mod error;
