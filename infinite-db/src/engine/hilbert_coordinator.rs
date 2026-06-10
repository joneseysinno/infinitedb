//! Per Hilbert-shard I/O coordinator (format v4 Phase C).

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};

use crate::infinitedb_core::{
    address::SpaceId,
    branch::BranchId,
    space::SpaceRegistry,
};
use crate::infinitedb_storage::nvme::BlockStore;

use super::branch_overlay::BranchOverlayStore;
use super::hilbert_live_tails::HilbertLiveTails;
use super::hilbert_shard::{pack_shard_key, shard_for_point, shard_count};
use super::io_thread::{IoStats, IoThreadConfig};
use super::live_tail::LiveTailView;
use super::snapshot_store::SnapshotStore;
use super::space_io::{open_space_pipeline, SpaceIoHandle};
use super::write_queue::{WriteJob, WriteQueueSender};

struct HilbertShard {
    queue: WriteQueueSender,
    io_handle: Mutex<SpaceIoHandle>,
    live_tail: Arc<LiveTailView>,
    space_id: u64,
    shard_id: u32,
}

/// Routes main-branch writes to per Hilbert-shard I/O threads; branch writes stay in overlays.
pub struct HilbertCoordinator {
    root: PathBuf,
    store: Arc<BlockStore>,
    snapshots: Arc<SnapshotStore>,
    live_tails: Arc<HilbertLiveTails>,
    branch_overlays: Arc<BranchOverlayStore>,
    spaces: Arc<RwLock<SpaceRegistry>>,
    next_block_id: Arc<AtomicU64>,
    next_snapshot_id: Arc<AtomicU64>,
    config: IoThreadConfig,
    shards: DashMap<u64, Arc<HilbertShard>>,
}

impl HilbertCoordinator {
    pub fn new(
        root: PathBuf,
        store: Arc<BlockStore>,
        snapshots: Arc<SnapshotStore>,
        branch_overlays: Arc<BranchOverlayStore>,
        spaces: Arc<RwLock<SpaceRegistry>>,
        next_block_id: Arc<AtomicU64>,
        next_snapshot_id: Arc<AtomicU64>,
        config: IoThreadConfig,
    ) -> Self {
        Self {
            root,
            store,
            snapshots,
            live_tails: Arc::new(HilbertLiveTails::new()),
            branch_overlays,
            spaces,
            next_block_id,
            next_snapshot_id,
            config,
            shards: DashMap::new(),
        }
    }

    pub fn live_tails(&self) -> &HilbertLiveTails {
        &self.live_tails
    }

    pub fn branch_overlays(&self) -> &BranchOverlayStore {
        &self.branch_overlays
    }

    pub fn ensure_shard(&self, space_id: u64, shard_id: u32) -> io::Result<()> {
        let key = pack_shard_key(space_id, shard_id);
        if self.shards.contains_key(&key) {
            return Ok(());
        }

        let space_dir = self.space_dir(space_id);
        let shard_dir = space_dir.join("shards").join(shard_id.to_string());
        std::fs::create_dir_all(shard_dir.join("wal"))?;

        let live_tail = self.live_tails.get_or_create(space_id, shard_id);
        let (queue, io_handle) = open_space_pipeline(
            space_id,
            shard_dir,
            Arc::clone(&self.store),
            Arc::clone(&self.snapshots),
            Arc::clone(&live_tail),
            Arc::clone(&self.spaces),
            Arc::clone(&self.next_block_id),
            Arc::clone(&self.next_snapshot_id),
            self.config.clone(),
        );

        let shard = Arc::new(HilbertShard {
            queue,
            io_handle: Mutex::new(io_handle),
            live_tail,
            space_id,
            shard_id,
        });
        self.shards.insert(key, Arc::clone(&shard));
        Ok(())
    }

    fn shard_bits_for_space(&self, space_id: u64) -> u32 {
        self.spaces
            .read()
            .get(SpaceId(space_id))
            .map(|c| c.shard_bits)
            .unwrap_or(4)
    }

    pub fn enqueue_write(&self, job: WriteJob) -> io::Result<()> {
        debug_assert_eq!(job.branch_id, BranchId::MAIN);
        let space_id = job.record.address.space.0;
        let shard_bits = self.shard_bits_for_space(space_id);
        let shard_id = shard_for_point(
            &self.spaces.read(),
            SpaceId(space_id),
            &job.record.address.point,
            shard_bits,
        );
        self.ensure_shard(space_id, shard_id)?;
        let key = pack_shard_key(space_id, shard_id);
        let shard = self.shards.get(&key).expect("shard just ensured");
        shard.queue.enqueue_write(job)
    }

    pub fn enqueue_batch(&self, jobs: Vec<WriteJob>) -> io::Result<()> {
        let mut by_route: BTreeMap<(u64, u32), Vec<WriteJob>> = BTreeMap::new();
        for job in jobs {
            debug_assert_eq!(job.branch_id, BranchId::MAIN);
            let space_id = job.record.address.space.0;
            let shard_bits = self.shard_bits_for_space(space_id);
            let shard_id = shard_for_point(
                &self.spaces.read(),
                SpaceId(space_id),
                &job.record.address.point,
                shard_bits,
            );
            by_route
                .entry((space_id, shard_id))
                .or_default()
                .push(job);
        }
        for ((space_id, shard_id), jobs) in by_route {
            self.ensure_shard(space_id, shard_id)?;
            let key = pack_shard_key(space_id, shard_id);
            let shard = self.shards.get(&key).expect("shard just ensured");
            for job in jobs {
                shard.queue.enqueue_write(job)?;
            }
        }
        Ok(())
    }

    pub fn flush_space(&self, space_id: u64) -> io::Result<()> {
        let shard_bits = self.shard_bits_for_space(space_id);
        let count = shard_count(shard_bits);
        for shard_id in 0..count {
            let key = pack_shard_key(space_id, shard_id);
            if let Some(shard) = self.shards.get(&key) {
                shard.queue.request_flush(space_id)?;
            }
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
        for space_id in ids {
            let shard_bits = self.shard_bits_for_space(space_id);
            let count = shard_count(shard_bits);
            for shard_id in 0..count {
                let _ = self.ensure_shard(space_id, shard_id)?;
            }
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
