//! CRCW embedded database ([`InfiniteDb`], formats v2–v4).
//!
//! - **Phase A (v2):** global I/O thread, fire-and-forget writes, [`ReadTxn`]
//! - **Phase B (v3):** per-space I/O threads
//! - **Phase C (v4):** Hilbert shards, branch overlays, [`InfiniteDb::merge_branch`]
//! - **Phase D:** server and sync layers (see crate-root re-exports)
//!
//! New databases default to format v4 via [`OpenOptions::format_version`]
//! (`None` → v4). Use [`crate::FORMAT_VERSION_V3`] or [`crate::FORMAT_VERSION_V2`]
//! for older on-disk layouts.

mod concurrent_db;
pub mod read_txn;

pub use concurrent_db::{InfiniteDb, OpenOptions};
pub use crate::engine::io_thread::IoStats;
pub use read_txn::ReadTxn;
