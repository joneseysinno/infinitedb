//! Embedded storage engine components.

/// Write-ahead logging and recovery.
pub mod wal;
/// NVMe-friendly block store and cache.
pub mod nvme;
/// Block compaction routines.
pub mod compaction;
/// Retention policy and garbage collection helpers.
pub mod gc;
