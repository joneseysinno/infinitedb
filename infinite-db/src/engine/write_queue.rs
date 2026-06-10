//! Bounded fire-and-forget write queue.

use std::io;

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};

use crate::infinitedb_core::{
    address::RevisionId,
    block::Record,
    branch::BranchId,
};
use crate::infinitedb_storage::wal::WalEntry;

/// A single write job enqueued by application threads.
#[derive(Debug, Clone)]
pub struct WriteJob {
    pub branch_id: BranchId,
    pub revision: RevisionId,
    pub entry: WalEntry,
    pub record: Record,
}

impl WriteJob {
    /// Main-branch write job.
    pub fn main(revision: RevisionId, entry: WalEntry, record: Record) -> Self {
        Self {
            branch_id: BranchId::MAIN,
            revision,
            entry,
            record,
        }
    }
}

/// I/O commands processed by the dedicated disk thread.
#[derive(Debug)]
pub enum IoCommand {
    Write(WriteJob),
    /// Drain all pending work and fsync staging state.
    Sync {
        done: crossbeam_channel::Sender<io::Result<()>>,
    },
    /// Seal hot segments for a space into a block.
    Flush {
        space_id: u64,
        done: crossbeam_channel::Sender<io::Result<()>>,
    },
    Shutdown,
}

/// Sender half of the bounded write queue.
#[derive(Clone)]
pub struct WriteQueueSender {
    tx: Sender<IoCommand>,
    capacity: usize,
}

impl WriteQueueSender {
    pub fn new(capacity: usize) -> (Self, Receiver<IoCommand>) {
        let (tx, rx) = bounded(capacity);
        (
            Self {
                tx,
                capacity,
            },
            rx,
        )
    }

    /// Enqueue a write. Blocks only when the queue is full (backpressure).
    pub fn enqueue_write(&self, job: WriteJob) -> io::Result<()> {
        self.tx
            .send(IoCommand::Write(job))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread stopped"))
    }

    /// Try enqueue without blocking. Returns `WouldBlock` when full.
    pub fn try_enqueue_write(&self, job: WriteJob) -> io::Result<()> {
        match self.tx.try_send(IoCommand::Write(job)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "write queue full",
            )),
            Err(TrySendError::Disconnected(_)) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "I/O thread stopped",
            )),
        }
    }

    pub fn request_sync(&self) -> io::Result<()> {
        let (done_tx, done_rx) = bounded(1);
        self.tx
            .send(IoCommand::Sync { done: done_tx })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread stopped"))?;
        done_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread stopped"))?
    }

    pub fn request_flush(&self, space_id: u64) -> io::Result<()> {
        let (done_tx, done_rx) = bounded(1);
        self.tx
            .send(IoCommand::Flush {
                space_id,
                done: done_tx,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread stopped"))?;
        done_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread stopped"))?
    }

    pub fn shutdown(&self) -> io::Result<()> {
        self.tx
            .send(IoCommand::Shutdown)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O thread stopped"))
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn queued_count(&self) -> usize {
        self.tx.len()
    }
}
