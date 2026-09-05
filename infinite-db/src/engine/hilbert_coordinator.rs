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
    hilbert_key::HilbertKey,
    space::SpaceRegistry,
};
use crate::infinitedb_storage::wal::WalEntry;
use crate::infinitedb_storage::nvme::BlockStore;

use super::compactor::CompactionPolicyOverrides;
use super::branch_overlay::BranchOverlayStore;
use super::hilbert_live_tails::HilbertLiveTails;
use super::hilbert_shard::{hilbert_shard_id, shard_for_point, shard_count, ShardKey, ShardRef, DEFAULT_SHARD_BITS};
use super::io_thread::{IoStats, IoThreadConfig};
use super::snapshot_store::SnapshotStore;
use super::space_io::{bootstrap_live_tail_blocks, open_space_pipeline, SpaceIoHandle};
use super::session::SessionWatermarks;
use super::write_queue::{WriteJob, WriteQueueSender};

struct HilbertShard {
    queue: WriteQueueSender,
    io_handle: Mutex<SpaceIoHandle>,
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
    config: IoThreadConfig,
    watermark: Arc<SessionWatermarks>,
    compaction_overrides: CompactionPolicyOverrides,
    shards: DashMap<ShardKey, Arc<HilbertShard>>,
}

impl HilbertCoordinator {
    pub fn new(
        root: PathBuf,
        store: Arc<BlockStore>,
        snapshots: Arc<SnapshotStore>,
        branch_overlays: Arc<BranchOverlayStore>,
        spaces: Arc<RwLock<SpaceRegistry>>,
        next_block_id: Arc<AtomicU64>,
        config: IoThreadConfig,
        watermark: Arc<SessionWatermarks>,
        compaction_overrides: CompactionPolicyOverrides,
    ) -> Self {
        Self {
            root,
            store,
            snapshots,
            live_tails: Arc::new(HilbertLiveTails::new()),
            branch_overlays,
            spaces,
            next_block_id,
            config,
            watermark,
            compaction_overrides,
            shards: DashMap::new(),
        }
    }

    pub fn live_tails(&self) -> &HilbertLiveTails {
        &self.live_tails
    }

    pub fn live_tails_arc(&self) -> Arc<HilbertLiveTails> {
        Arc::clone(&self.live_tails)
    }

    pub fn branch_overlays(&self) -> &BranchOverlayStore {
        &self.branch_overlays
    }

    pub fn ensure_shard(&self, space: SpaceId, shard_id: u32) -> io::Result<()> {
        let key = ShardKey::new(space, shard_id);
        if self.shards.contains_key(&key) {
            return Ok(());
        }

        let space_dir = self.space_dir(space);
        let shard_dir = space_dir.join("shards").join(shard_id.to_string());
        std::fs::create_dir_all(shard_dir.join("wal"))?;

        let live_tail = self.live_tails.get_or_create(space, shard_id);
        let shard_bits = self.shard_bits_for_space(space);
        bootstrap_live_tail_blocks(
            &live_tail,
            &self.snapshots,
            space.0,
            Some(ShardRef::new(shard_id, shard_bits)),
        );
        let (queue, io_handle) = open_space_pipeline(
            space.0,
            shard_dir,
            Arc::clone(&self.store),
            Arc::clone(&self.snapshots),
            Arc::clone(&live_tail),
            Arc::clone(&self.spaces),
            Arc::clone(&self.next_block_id),
            self.config.clone(),
            Some(ShardRef::new(shard_id, shard_bits)),
            Arc::clone(&self.watermark),
            Arc::clone(&self.compaction_overrides),
            Some(Arc::clone(&self.branch_overlays)),
        );

        let shard = Arc::new(HilbertShard {
            queue,
            io_handle: Mutex::new(io_handle),
        });

        match self.shards.entry(key) {
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

    fn shard_bits_for_space(&self, space: SpaceId) -> u32 {
        self.spaces
            .read()
            .get(space)
            .map(|c| c.shard_bits)
            .unwrap_or(DEFAULT_SHARD_BITS)
    }

    pub fn enqueue_write(&self, job: WriteJob) -> io::Result<()> {
        debug_assert_eq!(job.branch_id, BranchId::MAIN);
        let space = job.space_id();
        let shard_bits = self.shard_bits_for_space(space);
        let shard_id = if job.hilbert_key != HilbertKey::ZERO {
            hilbert_shard_id(job.hilbert_key.raw(), shard_bits)
        } else {
            shard_for_point_from_job(&self.spaces.read(), &job, shard_bits)
        };
        self.ensure_shard(space, shard_id)?;
        let key = ShardKey::new(space, shard_id);
        let shard = self.shards.get(&key).expect("shard just ensured");
        shard.queue.enqueue_write(job)
    }

    pub fn enqueue_batch(&self, jobs: Vec<WriteJob>) -> io::Result<()> {
        let mut by_route: BTreeMap<(SpaceId, u32), Vec<WriteJob>> = BTreeMap::new();
        for job in jobs {
            debug_assert_eq!(job.branch_id, BranchId::MAIN);
            let space = job.space_id();
            let shard_bits = self.shard_bits_for_space(space);
            let shard_id = if job.hilbert_key != HilbertKey::ZERO {
                hilbert_shard_id(job.hilbert_key.raw(), shard_bits)
            } else {
                shard_for_point_from_job(&self.spaces.read(), &job, shard_bits)
            };
            by_route.entry((space, shard_id)).or_default().push(job);
        }
        for ((space, shard_id), jobs) in by_route {
            self.ensure_shard(space, shard_id)?;
            let key = ShardKey::new(space, shard_id);
            let shard = self.shards.get(&key).expect("shard just ensured");
            shard.queue.enqueue_write_batch(jobs)?;
        }
        Ok(())
    }

    pub fn compact_space(&self, space: SpaceId) -> io::Result<()> {
        self.flush_space(space)
    }

    /// Run compaction immediately on every shard (honours per-space policy overrides).
    pub fn force_compact_space(&self, space: SpaceId) -> io::Result<()> {
        self.flush_shards_only(space)?;
        self.compact_all_shards(space, 1)
    }

    pub fn flush_space(&self, space: SpaceId) -> io::Result<()> {
        self.flush_shards_only(space)?;
        self.maybe_compact_all_shards(space, 1)
    }

    fn flush_shards_only(&self, space: SpaceId) -> io::Result<()> {
        let shard_bits = self.shard_bits_for_space(space);
        for shard_id in 0..shard_count(shard_bits) {
            let key = ShardKey::new(space, shard_id);
            if let Some(shard) = self.shards.get(&key) {
                shard.queue.request_flush(space)?;
            }
        }
        Ok(())
    }

    fn compact_all_shards(&self, space: SpaceId, min_blocks: usize) -> io::Result<()> {
        let shard_bits = self.shard_bits_for_space(space);
        let mut shard_ids: std::collections::HashSet<u32> = self
            .shards
            .iter()
            .filter(|entry| entry.key().space_id == space)
            .map(|entry| entry.key().shard_id)
            .collect();
        if let Some(snap) = self.snapshots.get(space) {
            for min_key in snap.blocks.keys() {
                shard_ids.insert(hilbert_shard_id(min_key.raw(), shard_bits));
            }
        }

        for shard_id in shard_ids {
            let live_tail = self.live_tails.get_or_create(space, shard_id);
            let shard_ref = ShardRef::new(shard_id, shard_bits);
            if live_tail.load_view().blocks.is_empty() {
                bootstrap_live_tail_blocks(
                    &live_tail,
                    &self.snapshots,
                    space.0,
                    Some(shard_ref),
                );
            }
            let view = live_tail.load_view();
            let source_count = view.blocks.len() + usize::from(view.tail_len() > 0);
            if source_count < min_blocks {
                continue;
            }
            super::compactor::compact_space_now(
                &self.store,
                &self.snapshots,
                &live_tail,
                &self.spaces,
                &self.next_block_id,
                space,
                Some(shard_ref),
                Some(&self.compaction_overrides),
                Some(&self.branch_overlays),
                min_blocks,
            )?;
        }
        Ok(())
    }

    /// Compact every shard that holds sealed blocks when the space is fragmented.
    fn maybe_compact_all_shards(&self, space: SpaceId, min_blocks: usize) -> io::Result<()> {
        const TIER_THRESHOLD: usize = 8;
        if self
            .snapshots
            .get(space)
            .map(|s| s.blocks.len())
            .unwrap_or(0)
            < TIER_THRESHOLD
        {
            return Ok(());
        }
        let policy = self
            .spaces
            .read()
            .get(space)
            .map(|c| c.compaction_policy.clone())
            .or_else(|| self.compaction_overrides.lock().get(&space).cloned())
            .unwrap_or_default();
        if matches!(policy, crate::infinitedb_core::space::CompactionPolicy::KeepAll) {
            return Ok(());
        }
        self.compact_all_shards(space, min_blocks)
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
        }
        stats
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn bootstrap_registered_spaces(&self) -> io::Result<()> {
        let mut bootstrapped = std::collections::BTreeSet::new();
        if let Ok(entries) = std::fs::read_dir(self.spaces_root()) {
            for entry in entries.flatten() {
                let space_raw: u64 = match entry.file_name().to_string_lossy().parse() {
                    Ok(id) => id,
                    Err(_) => continue,
                };
                let space = SpaceId(space_raw);
                let shard_bits = self.shard_bits_for_space(space);
                if let Some(snap) = self.snapshots.get(space) {
                    for min_key in snap.blocks.keys() {
                        let shard_id = hilbert_shard_id(min_key.raw(), shard_bits);
                        bootstrapped.insert((space_raw, shard_id));
                        let _ = self.ensure_shard(space, shard_id)?;
                    }
                }
                let shards_dir = entry.path().join("shards");
                if !shards_dir.exists() {
                    continue;
                }
                for shard_entry in std::fs::read_dir(shards_dir)?.flatten() {
                    let shard_id: u32 = match shard_entry.file_name().to_string_lossy().parse() {
                        Ok(id) => id,
                        Err(_) => continue,
                    };
                    if bootstrapped.contains(&(space_raw, shard_id)) {
                        continue;
                    }
                    let hot = shard_entry.path().join("hot.seg");
                    if hot.exists() && hot.metadata()?.len() > 16 {
                        let _ = self.ensure_shard(space, shard_id)?;
                    }
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

fn shard_for_point_from_job(spaces: &SpaceRegistry, job: &WriteJob, shard_bits: u32) -> u32 {
    let point = match &job.entry {
        WalEntry::Write { address, .. } | WalEntry::Tombstone { address, .. } => {
            &address.point
        }
        _ => return 0,
    };
    shard_for_point(spaces, job.space_id(), point, shard_bits)
}
