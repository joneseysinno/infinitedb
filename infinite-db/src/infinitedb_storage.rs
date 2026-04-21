pub mod wal;         // write-ahead log
pub mod nvme;        // NVMe-aware block I/O
pub mod compaction;  // merge small/fragmented blocks
pub mod gc;          // tombstone + version lifecycle
