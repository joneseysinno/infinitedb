//! Per-space write coordinator (format v3 Phase B).

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};

use crate::infinitedb_core::space::SpaceRegistry;
use crate::infinitedb_storage::nvme::BlockStore;

use super::io_thread::{IoStats, IoThreadConfig};
use super::live_tail::LiveTailView;
use super::snapshot_store::SnapshotStore;
use super::space_io::{open_space_pipeline, SpaceIoHandle};
use super::space_live_tails::SpaceLiveTails;
use super::write_queue::{WriteJob, WriteQueueSender};

struct SpaceShard {
    queue: WriteQueueSender,
    io_handle: Mutex<SpaceIoHandle>,
    live_tail: Arc<LiveTailView>,
}

/// Routes fire-and-forget writes to per-space I/O threads.
pub struct SpaceCoordinator {
    root: PathBuf,
    store: Arc<BlockStore>,
    snapshots: Arc<SnapshotStore>,
    live_tails: Arc<SpaceLiveTails>,
    spaces: Arc<RwLock<SpaceRegistry>>,
    next_block_id: Arc<AtomicU64>,
    next_snapshot_id: Arc<AtomicU64>,
    config: IoThreadConfig,
    shards: DashMap<u64, Arc<SpaceShard>>,
}

impl SpaceCoordinator {
    pub fn new(
        root: PathBuf,
        store: Arc<BlockStore>,
        snapshots: Arc<SnapshotStore>,
        spaces: Arc<RwLock<SpaceRegistry>>,
        next_block_id: Arc<AtomicU64>,
        next_snapshot_id: Arc<AtomicU64>,
        config: IoThreadConfig,
    ) -> Self {
        Self {
            root,
            store,
            snapshots,
            live_tails: Arc::new(SpaceLiveTails::new()),
            spaces,
            next_block_id,
            next_snapshot_id,
            config,
            shards: DashMap::new(),
        }
    }

    pub fn live_tails(&self) -> &SpaceLiveTails {
        &self.live_tails
    }

    pub fn ensure_space(&self, space_id: u64) -> io::Result<()> {
        if self.shards.contains_key(&space_id) {
            return Ok(());
        }

        let space_dir = self.space_dir(space_id);
        std::fs::create_dir_all(space_dir.join("wal"))?;

        let live_tail = self.live_tails.get_or_create(space_id);
        let (queue, io_handle) = open_space_pipeline(
            space_id,
            space_dir,
            Arc::clone(&self.store),
            Arc::clone(&self.snapshots),
            Arc::clone(&live_tail),
            Arc::clone(&self.spaces),
            Arc::clone(&self.next_block_id),
            Arc::clone(&self.next_snapshot_id),
            self.config.clone(),
        );

        let shard = Arc::new(SpaceShard {
            queue,
            io_handle: Mutex::new(io_handle),
            live_tail,
        });
        self.shards.insert(space_id, Arc::clone(&shard));
        Ok(())
    }

    pub fn enqueue_write(&self, job: WriteJob) -> io::Result<()> {
        let space_id = job.record.address.space.0;
        self.ensure_space(space_id)?;
        let shard = self.shards.get(&space_id).expect("shard just ensured");
        shard.queue.enqueue_write(job)
    }

    /// Enqueue jobs across multiple spaces (sorted by space id to avoid deadlocks).
    pub fn enqueue_batch(&self, jobs: Vec<WriteJob>) -> io::Result<()> {
        let mut by_space: BTreeMap<u64, Vec<WriteJob>> = BTreeMap::new();
        for job in jobs {
            by_space
                .entry(job.record.address.space.0)
                .or_default()
                .push(job);
        }
        for (space_id, jobs) in by_space {
            self.ensure_space(space_id)?;
            let shard = self.shards.get(&space_id).expect("shard just ensured");
            for job in jobs {
                shard.queue.enqueue_write(job)?;
            }
        }
        Ok(())
    }

    pub fn flush_space(&self, space_id: u64) -> io::Result<()> {
        if let Some(shard) = self.shards.get(&space_id) {
            shard.queue.request_flush(space_id)?;
        }
        Ok(())
    }

    pub fn sync_all(&self) -> io::Result<()> {
        for entry in self.shards.iter() {
            entry.value().queue.request_sync()?;
        }
        Ok(())
    }

    pub fn shutdown_all(&self) -> io::Result<()> {
        for entry in self.shards.iter() {
            let _ = entry.value().queue.shutdown();
        }
        for entry in self.shards.iter() {
            let _ = entry.value().io_handle.lock().join();
        }
        Ok(())
    }

    pub fn io_stats(&self) -> IoStats {
        let mut stats = IoStats::default();
        for entry in self.shards.iter() {
            let handle = entry.value().io_handle.lock();
            stats.queue_depth += entry.value().queue.queued_count();
            stats.direct_writes += handle.direct_writes();
            stats.staged_writes += handle.staged_writes();
        }
        stats
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Bootstrap shards for spaces registered before open.
    pub fn bootstrap_registered_spaces(&self) -> io::Result<()> {
        let mut ids: std::collections::BTreeSet<u64> = self
            .spaces
            .read()
            .space_ids()
            .into_iter()
            .map(|s| s.0)
            .collect();
        for key in self.snapshots.all().keys() {
            ids.insert(*key);
        }
        if let Ok(entries) = std::fs::read_dir(self.spaces_root()) {
            for entry in entries.flatten() {
                if let Ok(n) = entry.file_name().to_string_lossy().parse::<u64>() {
                    ids.insert(n);
                }
            }
        }
        for id in ids {
            let _ = self.ensure_space(id)?;
        }
        Ok(())
    }

    fn space_dir(&self, space_id: u64) -> PathBuf {
        self.root.join("spaces").join(space_id.to_string())
    }

    pub fn spaces_root(&self) -> PathBuf {
        self.root.join("spaces")
    }
}
