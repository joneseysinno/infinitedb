//! Durable outbox for offline-first replication.

use std::{
    fs,
    io,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bincode::{config::standard, decode_from_slice, encode_to_vec, Decode, Encode};
use crate::infinitedb_sync::transport::{SyncEnvelope, SyncOperation, SyncResult, SyncTransport};

/// One operation waiting to be replicated.
#[derive(Debug, Clone, Encode, Decode)]
pub struct OutboxEntry {
    pub op_id: u64,
    pub op: SyncOperation,
    pub attempts: u32,
    pub next_attempt_at_ms: u64,
    pub created_at_ms: u64,
    pub last_error: Option<String>,
}

/// Persisted outbox state and sync diagnostics.
#[derive(Debug, Clone, Encode, Decode)]
pub struct OutboxState {
    pub next_op_id: u64,
    pub entries: Vec<OutboxEntry>,
    pub last_success_at_ms: Option<u64>,
    pub last_error: Option<String>,
}

impl OutboxState {
    pub fn new() -> Self {
        Self {
            next_op_id: 1,
            entries: Vec::new(),
            last_success_at_ms: None,
            last_error: None,
        }
    }

    pub fn pending_count(&self) -> usize {
        self.entries.len()
    }

    pub fn enqueue(&mut self, op: SyncOperation) -> u64 {
        let now = now_ms();
        let op_id = self.next_op_id;
        self.next_op_id += 1;
        self.entries.push(OutboxEntry {
            op_id,
            op,
            attempts: 0,
            next_attempt_at_ms: now,
            created_at_ms: now,
            last_error: None,
        });
        op_id
    }

    pub fn process_once(
        &mut self,
        transport: &dyn SyncTransport,
        max_batch: usize,
    ) -> SyncReport {
        if max_batch == 0 {
            return SyncReport::default();
        }
        let now = now_ms();
        let due: Vec<SyncEnvelope> = self
            .entries
            .iter()
            .filter(|e| e.next_attempt_at_ms <= now)
            .take(max_batch)
            .map(|e| SyncEnvelope {
                op_id: e.op_id,
                op: e.op.clone(),
            })
            .collect();

        if due.is_empty() {
            return SyncReport::default();
        }

        let mut report = SyncReport {
            attempted: due.len(),
            ..SyncReport::default()
        };

        let response = transport.push_batch(&due);
        let results = match response {
            Ok(v) => v,
            Err(e) => {
                self.last_error = Some(e.clone());
                for item in &due {
                    if let Some(entry) = self.entries.iter_mut().find(|x| x.op_id == item.op_id) {
                        schedule_retry(entry, &e);
                    }
                    report.retried += 1;
                }
                return report;
            }
        };

        for item in &due {
            let result = results.iter().find_map(|r| match r {
                SyncResult::Ack { op_id }
                | SyncResult::Retry { op_id, .. }
                | SyncResult::ConflictStale { op_id, .. } => {
                    if *op_id == item.op_id {
                        Some(r)
                    } else {
                        None
                    }
                }
            });
            match result {
                Some(SyncResult::Ack { op_id }) => {
                    self.entries.retain(|e| e.op_id != *op_id);
                    self.last_success_at_ms = Some(now_ms());
                    report.acked += 1;
                }
                Some(SyncResult::ConflictStale { op_id, reason }) => {
                    self.entries.retain(|e| e.op_id != *op_id);
                    self.last_error = Some(reason.clone());
                    report.dropped_stale += 1;
                }
                Some(SyncResult::Retry { op_id, error }) => {
                    if let Some(entry) = self.entries.iter_mut().find(|x| x.op_id == *op_id) {
                        schedule_retry(entry, error);
                        report.retried += 1;
                    }
                }
                None => {
                    if let Some(entry) = self.entries.iter_mut().find(|x| x.op_id == item.op_id) {
                        schedule_retry(entry, "missing per-item sync result");
                        report.retried += 1;
                    }
                }
            }
        }

        report
    }
}

impl Default for OutboxState {
    fn default() -> Self {
        Self::new()
    }
}

/// Result summary from one replication attempt.
#[derive(Debug, Clone, Default)]
pub struct SyncReport {
    pub attempted: usize,
    pub acked: usize,
    pub retried: usize,
    pub dropped_stale: usize,
}

pub fn load_outbox(path: &Path) -> io::Result<OutboxState> {
    if !path.exists() {
        return Ok(OutboxState::new());
    }
    let bytes = fs::read(path)?;
    let (state, _): (OutboxState, _) = decode_from_slice(&bytes, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(state)
}

pub fn save_outbox(path: &Path, state: &OutboxState) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid outbox path"))?;
    fs::create_dir_all(parent)?;
    let payload = encode_to_vec(state, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, &payload)?;
    fs::rename(&tmp, path)
}

fn schedule_retry(entry: &mut OutboxEntry, error: &str) {
    entry.attempts = entry.attempts.saturating_add(1);
    entry.last_error = Some(error.to_string());
    let delay = backoff_ms(entry.attempts);
    entry.next_attempt_at_ms = now_ms().saturating_add(delay);
}

fn backoff_ms(attempts: u32) -> u64 {
    let shift = attempts.min(6);
    let secs = 1u64 << shift;
    Duration::from_secs(secs).as_millis().min(60_000) as u64
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    struct AlwaysAck;
    impl SyncTransport for AlwaysAck {
        fn push_batch(&self, batch: &[SyncEnvelope]) -> Result<Vec<SyncResult>, String> {
            Ok(batch
                .iter()
                .map(|op| SyncResult::Ack { op_id: op.op_id })
                .collect())
        }
    }

    struct AlwaysRetry;
    impl SyncTransport for AlwaysRetry {
        fn push_batch(&self, batch: &[SyncEnvelope]) -> Result<Vec<SyncResult>, String> {
            Ok(batch
                .iter()
                .map(|op| SyncResult::Retry {
                    op_id: op.op_id,
                    error: "offline".to_string(),
                })
                .collect())
        }
    }

    #[test]
    fn outbox_persists_across_reload() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("meta").join("sync_outbox.bin");
        let mut s = OutboxState::new();
        s.enqueue(SyncOperation::Tombstone {
            address: crate::infinitedb_core::address::Address::new(
                crate::infinitedb_core::address::SpaceId(1),
                crate::infinitedb_core::address::DimensionVector::new(vec![1, 2]),
            ),
            revision: crate::infinitedb_core::address::RevisionId(3),
        });
        save_outbox(&path, &s).unwrap();
        let loaded = load_outbox(&path).unwrap();
        assert_eq!(loaded.pending_count(), 1);
    }

    #[test]
    fn process_once_acks_and_clears_entries() {
        let mut s = OutboxState::new();
        s.enqueue(SyncOperation::Write {
            address: crate::infinitedb_core::address::Address::new(
                crate::infinitedb_core::address::SpaceId(1),
                crate::infinitedb_core::address::DimensionVector::new(vec![4, 5]),
            ),
            revision: crate::infinitedb_core::address::RevisionId(7),
            data: vec![1],
        });
        let report = s.process_once(&AlwaysAck, 16);
        assert_eq!(report.acked, 1);
        assert_eq!(s.pending_count(), 0);
    }

    #[test]
    fn process_once_retries_with_backoff() {
        let mut s = OutboxState::new();
        s.enqueue(SyncOperation::Write {
            address: crate::infinitedb_core::address::Address::new(
                crate::infinitedb_core::address::SpaceId(1),
                crate::infinitedb_core::address::DimensionVector::new(vec![9, 9]),
            ),
            revision: crate::infinitedb_core::address::RevisionId(1),
            data: vec![2],
        });
        let now = now_ms();
        let report = s.process_once(&AlwaysRetry, 8);
        assert_eq!(report.retried, 1);
        assert_eq!(s.pending_count(), 1);
        assert!(s.entries[0].next_attempt_at_ms >= now);
        assert!(s.entries[0].attempts >= 1);
    }
}
