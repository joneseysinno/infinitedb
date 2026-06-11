//! Per-space write coordinator (format v3 Phase B).

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};

use crate::infinitedb_core::{
    address::SpaceId,
    space::SpaceRegistry,
};
use crate::infinitedb_storage::nvme::BlockStore;

use super::branch_overlay::BranchOverlayStore;
use super::compactor::CompactionPolicyOverrides;
use super::io_thread::{IoStats, IoThreadConfig};
use super::snapshot_store::SnapshotStore;
use super::space_io::{bootstrap_live_tail_blocks, open_space_pipeline, SpaceIoHandle};
use super::watermark::RevisionWatermark;
use super::space_live_tails::SpaceLiveTails;
use super::write_queue::{WriteJob, WriteQueueSender};

struct SpaceShard {
    queue: WriteQueueSender,
    io_handle: Mutex<SpaceIoHandle>,
}

/// Routes fire-and-forget writes to per-space I/O threads.
pub struct SpaceCoordinator {
    root: PathBuf,
    store: Arc<BlockStore>,
    snapshots: Arc<SnapshotStore>,
    live_tails: Arc<SpaceLiveTails>,
    spaces: Arc<RwLock<SpaceRegistry>>,
    next_block_id: Arc<AtomicU64>,
    config: IoThreadConfig,
    watermark: Arc<RevisionWatermark>,
    compaction_overrides: CompactionPolicyOverrides,
    branch_overlays: Option<Arc<BranchOverlayStore>>,
    shards: DashMap<SpaceId, Arc<SpaceShard>>,
}

impl SpaceCoordinator {
    pub fn new(
        root: PathBuf,
        store: Arc<BlockStore>,
        snapshots: Arc<SnapshotStore>,
        spaces: Arc<RwLock<SpaceRegistry>>,
        next_block_id: Arc<AtomicU64>,
        config: IoThreadConfig,
        watermark: Arc<RevisionWatermark>,
        compaction_overrides: CompactionPolicyOverrides,
        branch_overlays: Option<Arc<BranchOverlayStore>>,
    ) -> Self {
        Self {
            root,
            store,
            snapshots,
            live_tails: Arc::new(SpaceLiveTails::new()),
            spaces,
            next_block_id,
            config,
            watermark,
            compaction_overrides,
            branch_overlays,
            shards: DashMap::new(),
        }
    }

    pub fn live_tails(&self) -> &SpaceLiveTails {
        &self.live_tails
    }

    pub fn live_tails_arc(&self) -> Arc<SpaceLiveTails> {
        Arc::clone(&self.live_tails)
    }

    pub fn ensure_space(&self, space: SpaceId) -> io::Result<()> {
        if self.shards.contains_key(&space) {
            return Ok(());
        }

        let space_dir = self.space_dir(space);
        std::fs::create_dir_all(space_dir.join("wal"))?;

        let live_tail = self.live_tails.get_or_create(space);
        bootstrap_live_tail_blocks(&live_tail, &self.snapshots, space.0, None);
        let (queue, io_handle) = open_space_pipeline(
            space.0,
            space_dir,
            Arc::clone(&self.store),
            Arc::clone(&self.snapshots),
            Arc::clone(&live_tail),
            Arc::clone(&self.spaces),
            Arc::clone(&self.next_block_id),
            self.config.clone(),
            None,
            Arc::clone(&self.watermark),
            Arc::clone(&self.compaction_overrides),
            self.branch_overlays.clone(),
        );

        let shard = Arc::new(SpaceShard {
            queue,
            io_handle: Mutex::new(io_handle),
        });

        match self.shards.entry(space) {
            dashmap::mapref::entry::Entry::Occupied(_) => {
                let _ = shard.queue.shutdown();
                let _ = shard.io_handle.lock().join();
                Ok(())
            }
            dashmap::mapref::entry::Entry::Vacant(v) => {
                v.insert(shard);
                Ok(())
            }
        }
    }

    pub fn enqueue_write(&self, job: WriteJob) -> io::Result<()> {
        let space = job.space_id();
        self.ensure_space(space)?;
        let shard = self.shards.get(&space).expect("shard just ensured");
        shard.queue.enqueue_write(job)
    }

    /// Enqueue jobs across multiple spaces (sorted by space id to avoid deadlocks).
    pub fn enqueue_batch(&self, jobs: Vec<WriteJob>) -> io::Result<()> {
        let mut by_space: BTreeMap<SpaceId, Vec<WriteJob>> = BTreeMap::new();
        for job in jobs {
            by_space.entry(job.space_id()).or_default().push(job);
        }
        for (space, jobs) in by_space {
            self.ensure_space(space)?;
            let shard = self.shards.get(&space).expect("shard just ensured");
            shard.queue.enqueue_write_batch(jobs)?;
        }
        Ok(())
    }

    pub fn compact_space(&self, space: SpaceId) -> io::Result<()> {
        if let Some(shard) = self.shards.get(&space) {
            shard.queue.request_flush(space)?;
        }
        Ok(())
    }

    pub fn flush_space(&self, space: SpaceId) -> io::Result<()> {
        if let Some(shard) = self.shards.get(&space) {
            shard.queue.request_flush(space)?;
        }
        Ok(())
    }

    pub fn sync_all(&self) -> io::Result<()> {
        let mut receivers = Vec::new();
        for entry in self.shards.iter() {
            let (done_tx, done_rx) = crossbeam_channel::bounded(1);
            entry.value().queue.post_sync(done_tx)?;
            receivers.push(done_rx);
        }
        let mut first_err = None;
        for rx in receivers {
            match rx.recv() {
                Ok(Err(e)) if first_err.is_none() => first_err = Some(e),
                Err(_) if first_err.is_none() => {
                    first_err = Some(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "I/O thread stopped",
                    ));
                }
                _ => {}
            }
        }
        first_err.map_or(Ok(()), Err)
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
        if let Ok(entries) = std::fs::read_dir(self.spaces_root()) {
            for entry in entries.flatten() {
                let space_id: u64 = match entry.file_name().to_string_lossy().parse() {
                    Ok(id) => id,
                    Err(_) => continue,
                };
                let hot = entry.path().join("hot.seg");
                if hot.exists() && hot.metadata()?.len() > 16 {
                    let _ = self.ensure_space(SpaceId(space_id))?;
                }
            }
        }
        Ok(())
    }

    fn space_dir(&self, space: SpaceId) -> PathBuf {
        self.root.join("spaces").join(space.0.to_string())
    }

    pub fn spaces_root(&self) -> PathBuf {
        self.root.join("spaces")
    }
}
