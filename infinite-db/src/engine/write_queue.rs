//! Bounded fire-and-forget write queue.

use std::io;

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::infinitedb_core::{
    address::{RevisionId, SpaceId},
    block::Record,
    branch::BranchId,
    hilbert_key::{CachedHilbertKey, HilbertKey},
};
use crate::infinitedb_storage::wal::WalEntry;

/// A single write job enqueued by application threads.
///
/// Payload bytes live only in [`WalEntry`]; the I/O thread builds [`Record`] from
/// the entry plus [`hilbert_key`](Self::hilbert_key).
#[derive(Debug)]
pub struct WriteJob {
    pub branch_id: BranchId,
    pub revision: RevisionId,
    pub entry: WalEntry,
    pub hilbert_key: HilbertKey,
}

impl WriteJob {
    /// Main-branch write job.
    pub fn main(revision: RevisionId, entry: WalEntry, hilbert_key: HilbertKey) -> Self {
        Self {
            branch_id: BranchId::MAIN,
            revision,
            entry,
            hilbert_key,
        }
    }

    /// Build the published record, moving payload bytes out of the WAL entry.
    pub fn into_record(self) -> Record {
        match self.entry {
            WalEntry::Write {
                address,
                revision,
                data,
            } => Record {
                address,
                revision,
                data,
                tombstone: false,
                hilbert_key: CachedHilbertKey::set(self.hilbert_key),
            },
            WalEntry::Tombstone { address, revision } => Record {
                address,
                revision,
                data: vec![],
                tombstone: true,
                hilbert_key: CachedHilbertKey::set(self.hilbert_key),
            },
            other => panic!("unsupported WAL entry in write job: {other:?}"),
        }
    }

    /// Borrow the WAL entry for hot-segment append (no clone).
    pub fn entry(&self) -> &WalEntry {
        &self.entry
    }

    /// Space id for routing.
    pub fn space_id(&self) -> SpaceId {
        match &self.entry {
            WalEntry::Write { address, .. } | WalEntry::Tombstone { address, .. } => {
                address.space
            }
            _ => SpaceId(0),
        }
    }
}

/// I/O commands processed by the dedicated disk thread.
#[derive(Debug)]
pub enum IoCommand {
    Write(WriteJob),
    /// Pre-formed write group for one shard (from `insert_many` / coordinators).
    WriteBatch(Vec<WriteJob>),
    /// Drain all pending work and fsync hot segments.
    Sync {
        done: crossbeam_channel::Sender<io::Result<()>>,
    },
    /// Seal hot segments for a space into a block.
    Flush {
        space: SpaceId,
        done: crossbeam_channel::Sender<io::Result<()>>,
    },
    Shutdown,
}

/// Sender half of the bounded write queue.
#[derive(Clone)]
pub struct WriteQueueSender {
    tx: Sender<IoCommand>,
}

impl WriteQueueSender {
    pub fn new(capacity: usize) -> (Self, Receiver<IoCommand>) {
        let (tx, rx) = bounded(capacity);
        (
            Self { tx },
            rx,
        )
    }

    /// Enqueue a write. Blocks only when the queue is full (backpressure).
    pub fn enqueue_write(&self, job: WriteJob) -> io::Result<()> {
        self.tx
            .send(IoCommand::Write(job))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread stopped"))
    }

    /// Enqueue a pre-formed batch for one shard.
    pub fn enqueue_write_batch(&self, jobs: Vec<WriteJob>) -> io::Result<()> {
        if jobs.is_empty() {
            return Ok(());
        }
        self.tx
            .send(IoCommand::WriteBatch(jobs))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread stopped"))
    }

    /// Request sync and block until complete.
    pub fn request_sync(&self) -> io::Result<()> {
        let (done_tx, done_rx) = bounded(1);
        self.post_sync(done_tx)?;
        done_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread stopped"))?
    }

    /// Post a sync barrier without waiting (parallel fan-out).
    pub fn post_sync(&self, done: crossbeam_channel::Sender<io::Result<()>>) -> io::Result<()> {
        self.tx
            .send(IoCommand::Sync { done })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread stopped"))
    }

    pub fn request_flush(&self, space: SpaceId) -> io::Result<()> {
        let (done_tx, done_rx) = bounded(1);
        self.post_flush(space, done_tx)?;
        done_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread stopped"))?
    }

    /// Post a flush without waiting (parallel fan-out).
    pub fn post_flush(
        &self,
        space: SpaceId,
        done: crossbeam_channel::Sender<io::Result<()>>,
    ) -> io::Result<()> {
        self.tx
            .send(IoCommand::Flush { space, done })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread stopped"))
    }

    pub fn shutdown(&self) -> io::Result<()> {
        self.tx
            .send(IoCommand::Shutdown)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread stopped"))
    }

    pub fn queued_count(&self) -> usize {
        self.tx.len()
    }
}
