//! Transport contract for pushing local writes to a remote server.

use bincode::{Decode, Encode};
use crate::infinitedb_core::address::{Address, RevisionId};

/// Logical operation that should be replicated to a remote node.
#[derive(Debug, Clone, Encode, Decode)]
pub enum SyncOperation {
    /// Insert/update payload at an address revision.
    Write {
        address: Address,
        revision: RevisionId,
        data: Vec<u8>,
    },
    /// Logical delete at an address revision.
    Tombstone {
        address: Address,
        revision: RevisionId,
    },
}

/// One outbox operation sent over transport with stable ID.
#[derive(Debug, Clone, Encode, Decode)]
pub struct SyncEnvelope {
    pub op_id: u64,
    pub op: SyncOperation,
}

/// Result of attempting to apply one operation on the remote side.
#[derive(Debug, Clone, Encode, Decode)]
pub enum SyncResult {
    /// Operation applied successfully and can be removed from the outbox.
    Ack { op_id: u64 },
    /// Temporary failure; retry later.
    Retry { op_id: u64, error: String },
    /// Conflict resolved as stale by last-write-wins semantics; drop locally.
    ConflictStale { op_id: u64, reason: String },
}

/// Host-provided transport for remote replication.
pub trait SyncTransport: Send + Sync {
    /// Push a batch of operations and return a per-operation result.
    fn push_batch(&self, batch: &[SyncEnvelope]) -> Result<Vec<SyncResult>, String>;
}

/// Default no-op transport. Useful for embedded-only users.
pub struct NoopSyncTransport;

impl SyncTransport for NoopSyncTransport {
    fn push_batch(&self, batch: &[SyncEnvelope]) -> Result<Vec<SyncResult>, String> {
        Ok(batch
            .iter()
            .map(|item| SyncResult::Retry {
                op_id: item.op_id,
                error: "NoopSyncTransport does not send writes".to_string(),
            })
            .collect())
    }
}
