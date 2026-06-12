//! CRCW embedded database ([`InfiniteDb`], format v4/v5 Hilbert shards).

mod concurrent_db;
pub mod read_txn;

pub use concurrent_db::{InfiniteDb, OpenOptions};
pub use crate::engine::watermark::RevisionRange;
pub use crate::engine::io_thread::IoStats;
pub use read_txn::ReadTxn;
