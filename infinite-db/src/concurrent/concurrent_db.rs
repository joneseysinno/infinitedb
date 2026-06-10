//! [`InfiniteDb`] — fire-and-forget writes with per-space I/O (v3) or global I/O (v2).

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bincode::{config::standard, decode_from_slice, encode_to_vec};
use parking_lot::{Mutex, RwLock};

use crate::engine::branch_overlay::BranchOverlayStore;
use crate::engine::coordinator::SpaceCoordinator;
use crate::engine::hilbert_coordinator::HilbertCoordinator;
use crate::engine::hilbert_live_tails::HilbertLiveTails;
use crate::engine::io_thread::{open_io_pipeline, IoStats, IoThreadConfig, IoThreadHandle};
use crate::engine::live_tail::LiveTailView;
use crate::engine::merge::merge_branches;
use crate::engine::query::{query_bbox, query_inner, snapshots_map_for_persist};
use crate::engine::snapshot_store::SnapshotStore;
use crate::engine::space_live_tails::SpaceLiveTails;
use crate::engine::write_queue::{WriteJob, WriteQueueSender};
use crate::infinitedb_core::{
    address::{Address, DimensionVector, RevisionId, SpaceId},
    block::Record,
    branch::{Branch, BranchId, BranchRegistry},
    merge::{MergeConflict, MergeResult, MergeStrategy},
    space::{SpaceConfig, SpaceRegistry},
    snapshot::SnapshotId,
};
use crate::infinitedb_storage::{
    format::{FormatVersion, FORMAT_VERSION_V2, FORMAT_VERSION_V3, FORMAT_VERSION_V4},
    nvme::BlockStore,
    wal::WalEntry,
};

/// Options for opening [`InfiniteDb`] (formats v2–v4).
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// I/O thread queue depth, staging, and durability settings.
    pub io_thread: IoThreadConfig,
    /// In-memory block cache size in bytes for the block store.
    pub block_cache_bytes: usize,
    /// When `None`, new databases use format v4 (Hilbert shards + branches).
    pub format_version: Option<u32>,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            io_thread: IoThreadConfig::default(),
            block_cache_bytes: 10 * 1024 * 1024,
            format_version: None,
        }
    }
}

impl OpenOptions {
    /// Open or create a database at `dir` using these options.
    pub fn open<P: AsRef<Path>>(&self, dir: P) -> io::Result<InfiniteDb> {
        InfiniteDb::open_with_options(dir, self)
    }
}

enum WriteBackend {
    /// Format v2: single global I/O thread.
    V2 {
        queue: WriteQueueSender,
        io_handle: Mutex<IoThreadHandle>,
        live_tail: Arc<LiveTailView>,
    },
    /// Format v3: one I/O thread per space.
    V3 {
        coordinator: SpaceCoordinator,
    },
    /// Format v4: Hilbert shards per space + branch overlays.
    V4 {
        coordinator: HilbertCoordinator,
    },
}

/// Thread-safe embedded database with concurrent reads and fire-and-forget writes.
pub struct InfiniteDb {
    root: PathBuf,
    format_version: u32,
    pub(crate) store: Arc<BlockStore>,
    pub(crate) spaces: Arc<RwLock<SpaceRegistry>>,
    branches: Arc<RwLock<BranchRegistry>>,
    pub(crate) snapshots: Arc<SnapshotStore>,
    pub(crate) revision: Arc<AtomicU64>,
    next_block_id: Arc<AtomicU64>,
    next_snapshot_id: Arc<AtomicU64>,
    next_branch_id: Arc<AtomicU64>,
    pub(crate) branch_overlays: Arc<BranchOverlayStore>,
    #[cfg(feature = "sync")]
    conflicts: Arc<crate::infinitedb_sync::conflict_queue::ConflictQueue>,
    backend: WriteBackend,
}

impl InfiniteDb {
    /// Open or create a database at `dir` with default [`OpenOptions`].
    pub fn open<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        OpenOptions::default().open(dir)
    }

    /// Open or create a database at `dir` with explicit tuning and format version.
    pub fn open_with_options<P: AsRef<Path>>(dir: P, options: &OpenOptions) -> io::Result<Self> {
        let root = dir.as_ref().to_path_buf();
        let store = Arc::new(BlockStore::open_with_cache(
            root.clone(),
            options.block_cache_bytes,
        )?);

        let format_version = match FormatVersion::read_from_meta(&root.join("meta"))? {
            Some(v) => v.0,
            None => options.format_version.unwrap_or(FORMAT_VERSION_V4),
        };

        match format_version {
            FORMAT_VERSION_V2 | FORMAT_VERSION_V3 | FORMAT_VERSION_V4 => {}
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported concurrent format version {other}"),
                ));
            }
        }

        if FormatVersion::read_from_meta(&root.join("meta"))?.is_none() {
            FormatVersion(format_version).write_to_meta(&root.join("meta"))?;
            if format_version == FORMAT_VERSION_V2 {
                std::fs::create_dir_all(root.join("hot"))?;
                std::fs::create_dir_all(root.join("wal"))?;
            } else {
                std::fs::create_dir_all(root.join("spaces"))?;
            }
        }

        let branch_overlays = Arc::new(BranchOverlayStore::new());
        #[cfg(feature = "sync")]
        let conflicts = Arc::new(crate::infinitedb_sync::conflict_queue::ConflictQueue::open(&root)?);

        let (spaces, branches, snapshots, next_rev, next_block, next_snap, next_branch) =
            load_meta(&store).unwrap_or_else(default_meta);

        let spaces = Arc::new(RwLock::new(spaces));
        let branches = Arc::new(RwLock::new(branches));
        let snapshots = Arc::new(SnapshotStore::new(snapshots));
        let revision = Arc::new(AtomicU64::new(next_rev));
        let next_block_id = Arc::new(AtomicU64::new(next_block));
        let next_snapshot_id = Arc::new(AtomicU64::new(next_snap));
        let next_branch_id = Arc::new(AtomicU64::new(next_branch));

        if branches.read().get_by_name("main").is_none() {
            let snap_id = SnapshotId(next_snap);
            let _ = branches.write().insert(Branch {
                id: BranchId(1),
                name: "main".to_string(),
                head: snap_id,
                parent: None,
                forked_at: RevisionId::ZERO,
            });
        }

        let backend = if format_version == FORMAT_VERSION_V4 {
            let coordinator = HilbertCoordinator::new(
                root.clone(),
                Arc::clone(&store),
                Arc::clone(&snapshots),
                Arc::clone(&branch_overlays),
                Arc::clone(&spaces),
                Arc::clone(&next_block_id),
                Arc::clone(&next_snapshot_id),
                options.io_thread.clone(),
            );
            coordinator.bootstrap_registered_spaces()?;
            WriteBackend::V4 { coordinator }
        } else if format_version == FORMAT_VERSION_V3 {
            let coordinator = SpaceCoordinator::new(
                root.clone(),
                Arc::clone(&store),
                Arc::clone(&snapshots),
                Arc::clone(&spaces),
                Arc::clone(&next_block_id),
                Arc::clone(&next_snapshot_id),
                options.io_thread.clone(),
            );
            coordinator.bootstrap_registered_spaces()?;
            WriteBackend::V3 { coordinator }
        } else {
            let live_tail = Arc::new(LiveTailView::new());
            let (queue, io_handle) = open_io_pipeline(
                root.clone(),
                Arc::clone(&store),
                Arc::clone(&snapshots),
                Arc::clone(&live_tail),
                Arc::clone(&spaces),
                Arc::clone(&revision),
                Arc::clone(&next_block_id),
                Arc::clone(&next_snapshot_id),
                options.io_thread.clone(),
            );
            WriteBackend::V2 {
                queue,
                io_handle: Mutex::new(io_handle),
                live_tail,
            }
        };

        Ok(Self {
            root,
            format_version,
            store,
            spaces,
            branches,
            snapshots,
            revision,
            next_block_id,
            next_snapshot_id,
            next_branch_id,
            branch_overlays,
            #[cfg(feature = "sync")]
            conflicts,
            backend,
        })
    }

    /// Head snapshot pointer for `branch`.
    pub fn branch_head(&self, branch: BranchId) -> Option<SnapshotId> {
        self.branches.read().get(branch).map(|b| b.head)
    }

    /// Resolve a branch id by name.
    pub fn branch_id(&self, name: &str) -> Option<BranchId> {
        self.branches.read().get_by_name(name).map(|b| b.id)
    }

    /// Conflict queue populated during sync replication (requires `sync` feature).
    #[cfg(feature = "sync")]
    pub fn conflicts(&self) -> &crate::infinitedb_sync::conflict_queue::ConflictQueue {
        &self.conflicts
    }

    /// On-disk format version (2, 3, or 4) for this database directory.
    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    /// Register a new space and persist catalog metadata. Required before writes to that space.
    pub fn register_space(&self, config: SpaceConfig) -> Result<(), String> {
        if config.bits_per_dim == 0 {
            return Err("bits_per_dim must be at least 1".to_string());
        }
        if config.dims as u32 * config.bits_per_dim > 128 {
            return Err(format!(
                "dims * bits_per_dim must be <= 128 (got {} * {})",
                config.dims, config.bits_per_dim
            ));
        }
        let space_id = config.id.0;
        let shard_bits = config.shard_bits;
        self.spaces
            .write()
            .register(config)
            .map_err(|e| format!("{:?}", e))?;
        match &self.backend {
            WriteBackend::V3 { coordinator } => {
                coordinator
                    .ensure_space(space_id)
                    .map_err(|e| e.to_string())?;
            }
            WriteBackend::V4 { coordinator } => {
                let count = crate::engine::hilbert_shard::shard_count(shard_bits);
                for shard_id in 0..count {
                    coordinator
                        .ensure_shard(space_id, shard_id)
                        .map_err(|e| e.to_string())?;
                }
            }
            WriteBackend::V2 { .. } => {}
        }
        self.persist_meta().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Fire-and-forget insert on `main`. Blocks only when the target queue is full.
    pub fn insert(
        &self,
        space: SpaceId,
        point: DimensionVector,
        data: Vec<u8>,
    ) -> io::Result<RevisionId> {
        self.insert_on_branch(BranchId::MAIN, space, point, data)
    }

    /// Fire-and-forget insert on a branch (overlay for non-`main` branches).
    pub fn insert_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        point: DimensionVector,
        data: Vec<u8>,
    ) -> io::Result<RevisionId> {
        let rev = self.next_revision();
        let address = Address::new(space, point);
        let entry = WalEntry::Write {
            address: address.clone(),
            revision: rev,
            data: data.clone(),
        };
        let record = Record {
            address,
            revision: rev,
            data,
            tombstone: false,
        };
        let job = WriteJob {
            branch_id: branch,
            revision: rev,
            entry,
            record,
        };
        self.enqueue(job)?;
        Ok(rev)
    }

    /// Fire-and-forget delete on `main`.
    pub fn delete(&self, space: SpaceId, point: DimensionVector) -> io::Result<RevisionId> {
        self.delete_on_branch(BranchId::MAIN, space, point)
    }

    /// Fire-and-forget delete on a branch.
    pub fn delete_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        point: DimensionVector,
    ) -> io::Result<RevisionId> {
        let rev = self.next_revision();
        let address = Address::new(space, point);
        let entry = WalEntry::Tombstone {
            address: address.clone(),
            revision: rev,
        };
        let record = Record {
            address,
            revision: rev,
            data: vec![],
            tombstone: true,
        };
        let job = WriteJob {
            branch_id: branch,
            revision: rev,
            entry,
            record,
        };
        self.enqueue(job)?;
        Ok(rev)
    }

    /// Fork a new branch from `from` at the current revision.
    pub fn create_branch(&self, name: &str, from: BranchId) -> Result<BranchId, String> {
        let parent = self
            .branches
            .read()
            .get(from)
            .ok_or_else(|| format!("parent branch {:?} not found", from))?
            .clone();
        let id = BranchId(self.next_branch_id.fetch_add(1, Ordering::Relaxed));
        let forked_at = RevisionId(self.revision.load(Ordering::Relaxed));
        let branch = Branch {
            id,
            name: name.to_string(),
            head: parent.head,
            parent: Some(from),
            forked_at,
        };
        self.branches
            .write()
            .insert(branch)
            .map_err(|e| format!("{:?}", e))?;
        self.persist_meta().map_err(|e| e.to_string())?;
        Ok(id)
    }

    /// Three-way merge `source` into `target` (usually `main`).
    pub fn merge_branch(
        &self,
        target: BranchId,
        source: BranchId,
        strategy: MergeStrategy,
        resolver: Option<Box<dyn Fn(MergeConflict) -> Record + Send + Sync>>,
    ) -> io::Result<MergeResult> {
        self.sync()?;
        let ctx = self.query_ctx();
        let mut result = merge_branches(
            &self.store,
            &self.snapshots,
            ctx.live_tail,
            ctx.space_tails,
            ctx.hilbert_tails,
            &self.branch_overlays,
            &self.spaces.read(),
            &self.revision,
            &self.branches.read(),
            target,
            source,
            strategy,
            resolver.as_deref(),
        )?;
        if strategy == MergeStrategy::Interactive && !result.conflicts.is_empty() {
            return Ok(result);
        }
        let applied = std::mem::take(&mut result.applied_records);
        for record in applied {
            if record.tombstone {
                self.delete_on_branch(target, record.address.space, record.address.point)?;
            } else {
                self.insert_on_branch(
                    target,
                    record.address.space,
                    record.address.point,
                    record.data,
                )?;
            }
        }
        self.branch_overlays.clear_branch(source);
        self.sync()?;
        Ok(result)
    }

    /// Query `space` through a branch overlay.
    pub fn query_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Record>> {
        let ctx = self.query_ctx();
        let branch_id = if branch == BranchId::MAIN {
            None
        } else {
            Some(branch)
        };
        query_inner(
            &self.store,
            &self.snapshots,
            ctx.live_tail,
            ctx.space_tails,
            &self.spaces.read(),
            &self.revision,
            space,
            None,
            as_of,
            false,
            ctx.hilbert_tails,
            Some(&self.branch_overlays),
            branch_id,
        )
    }

    /// Enqueue writes across multiple spaces (ordered by space id).
    ///
    /// Caller-supplied [`WriteJob::revision`] values must not exceed the global
    /// revision counter; this method advances the counter to the maximum job revision.
    pub fn enqueue_batch(&self, jobs: Vec<WriteJob>) -> io::Result<()> {
        for job in &jobs {
            self.revision
                .fetch_max(job.revision.0, Ordering::Relaxed);
        }
        let mut main_jobs = Vec::with_capacity(jobs.len());
        for job in jobs {
            if job.branch_id != BranchId::MAIN {
                self.branch_overlays
                    .append(job.branch_id, job.record.address.space, job.record);
            } else {
                main_jobs.push(job);
            }
        }
        if main_jobs.is_empty() {
            return Ok(());
        }
        match &self.backend {
            WriteBackend::V4 { coordinator } => coordinator.enqueue_batch(main_jobs),
            WriteBackend::V3 { coordinator } => coordinator.enqueue_batch(main_jobs),
            WriteBackend::V2 { queue, .. } => {
                for job in main_jobs {
                    queue.enqueue_write(job)?;
                }
                Ok(())
            }
        }
    }

    /// Query all live records in `space` on `main`, optionally capped at `as_of`.
    pub fn query(
        &self,
        space: SpaceId,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Record>> {
        self.query_on_branch(BranchId::MAIN, space, as_of)
    }

    /// Bounding-box query on `main`.
    pub fn query_bbox(
        &self,
        space: SpaceId,
        min: DimensionVector,
        max: DimensionVector,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Record>> {
        self.query_bbox_on_branch(BranchId::MAIN, space, min, max, as_of)
    }

    /// Bounding-box query through a branch overlay.
    pub fn query_bbox_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        min: DimensionVector,
        max: DimensionVector,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Record>> {
        let ctx = self.query_ctx();
        let branch_id = if branch == BranchId::MAIN {
            None
        } else {
            Some(branch)
        };
        query_bbox(
            &self.store,
            &self.snapshots,
            ctx.live_tail,
            ctx.space_tails,
            &self.spaces.read(),
            &self.revision,
            space,
            min,
            max,
            as_of,
            ctx.hilbert_tails,
            Some(&self.branch_overlays),
            branch_id,
        )
    }

    /// Flush pending writes for one space to durable storage without syncing all spaces.
    pub fn flush(&self, space: SpaceId) -> io::Result<()> {
        match &self.backend {
            WriteBackend::V4 { coordinator } => coordinator.flush_space(space.0)?,
            WriteBackend::V3 { coordinator } => coordinator.flush_space(space.0)?,
            WriteBackend::V2 { queue, .. } => queue.request_flush(space.0)?,
        }
        self.persist_meta()
    }

    /// Flush all write queues and persist metadata. Call after writes to make data queryable.
    pub fn sync(&self) -> io::Result<()> {
        match &self.backend {
            WriteBackend::V4 { coordinator } => coordinator.sync_all()?,
            WriteBackend::V3 { coordinator } => coordinator.sync_all()?,
            WriteBackend::V2 { queue, .. } => queue.request_sync()?,
        }
        self.persist_meta()
    }

    /// Current monotonic revision counter (logical write clock).
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

    /// Begin a concurrent read transaction pinned at the current revision.
    pub fn read(&self) -> crate::concurrent::read_txn::ReadTxn<'_> {
        crate::concurrent::read_txn::ReadTxn::new(self)
    }

    /// I/O queue depth and write-path counters across all backend threads.
    pub fn io_stats(&self) -> IoStats {
        match &self.backend {
            WriteBackend::V4 { coordinator } => coordinator.io_stats(),
            WriteBackend::V3 { coordinator } => coordinator.io_stats(),
            WriteBackend::V2 { queue, io_handle, .. } => {
                let handle = io_handle.lock();
                IoStats {
                    queue_depth: queue.queued_count(),
                    direct_writes: handle.direct_writes(),
                    staged_writes: handle.staged_writes(),
                    staging_wal_frames: 0,
                }
            }
        }
    }

    /// Number of I/O shards (1 for format v2, per-space or per-Hilbert-shard for v3/v4).
    pub fn space_shard_count(&self) -> usize {
        match &self.backend {
            WriteBackend::V4 { coordinator } => coordinator.shard_count(),
            WriteBackend::V3 { coordinator } => coordinator.shard_count(),
            WriteBackend::V2 { .. } => 1,
        }
    }

    pub(crate) fn live_tail_refs(&self) -> (Option<&LiveTailView>, Option<&SpaceLiveTails>) {
        let ctx = self.query_ctx();
        (ctx.live_tail, ctx.space_tails)
    }

    pub(crate) fn query_ctx(&self) -> QueryCtx<'_> {
        match &self.backend {
            WriteBackend::V2 { live_tail, .. } => QueryCtx {
                live_tail: Some(live_tail.as_ref()),
                space_tails: None,
                hilbert_tails: None,
            },
            WriteBackend::V3 { coordinator } => QueryCtx {
                live_tail: None,
                space_tails: Some(coordinator.live_tails()),
                hilbert_tails: None,
            },
            WriteBackend::V4 { coordinator } => QueryCtx {
                live_tail: None,
                space_tails: None,
                hilbert_tails: Some(coordinator.live_tails()),
            },
        }
    }

    fn enqueue(&self, job: WriteJob) -> io::Result<()> {
        if job.branch_id != BranchId::MAIN {
            self.branch_overlays
                .append(job.branch_id, job.record.address.space, job.record);
            return Ok(());
        }
        match &self.backend {
            WriteBackend::V4 { coordinator } => coordinator.enqueue_write(job),
            WriteBackend::V3 { coordinator } => coordinator.enqueue_write(job),
            WriteBackend::V2 { queue, .. } => queue.enqueue_write(job),
        }
    }

    fn next_revision(&self) -> RevisionId {
        RevisionId(self.revision.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn persist_meta(&self) -> io::Result<()> {
        let spaces_bytes = encode_to_vec(&*self.spaces.read(), standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("spaces.bin", &spaces_bytes)?;

        let branches_bytes = encode_to_vec(&*self.branches.read(), standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("branches.bin", &branches_bytes)?;

        let snapshots = snapshots_map_for_persist(&self.snapshots);
        let snapshots_bytes = encode_to_vec(&snapshots, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("snapshots.bin", &snapshots_bytes)?;

        let counters: [u64; 4] = [
            self.revision.load(Ordering::Relaxed),
            self.next_block_id.load(Ordering::Relaxed),
            self.next_snapshot_id.load(Ordering::Relaxed),
            self.next_branch_id.load(Ordering::Relaxed),
        ];
        let counters_bytes = encode_to_vec(&counters, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("counters.bin", &counters_bytes)?;
        Ok(())
    }
}

impl Drop for InfiniteDb {
    fn drop(&mut self) {
        let _ = self.persist_meta();
        match &self.backend {
            WriteBackend::V4 { coordinator } => {
                let _ = coordinator.shutdown_all();
            }
            WriteBackend::V3 { coordinator } => {
                let _ = coordinator.shutdown_all();
            }
            WriteBackend::V2 { queue, io_handle, .. } => {
                let _ = queue.shutdown();
                let _ = io_handle.lock().join();
            }
        }
    }
}

type MetaTuple = (
    SpaceRegistry,
    BranchRegistry,
    std::collections::BTreeMap<u64, crate::infinitedb_core::snapshot::Snapshot>,
    u64,
    u64,
    u64,
    u64,
);

fn load_meta(store: &BlockStore) -> Option<MetaTuple> {
    let counters_bytes = store.read_meta("counters.bin").ok()?;
    let (revision, next_block, next_snapshot, next_branch) =
        match decode_from_slice::<[u64; 4], _>(&counters_bytes, standard()) {
            Ok((c, _)) => (c[0], c[1], c[2], c[3]),
            Err(_) => {
                let (c, _): ([u64; 3], _) = decode_from_slice(&counters_bytes, standard()).ok()?;
                (c[0], c[1], c[2], 2)
            }
        };

    let spaces_bytes = store.read_meta("spaces.bin").ok()?;
    let (spaces, _): (SpaceRegistry, _) = decode_from_slice(&spaces_bytes, standard()).ok()?;

    let branches = store
        .read_meta("branches.bin")
        .ok()
        .and_then(|b| decode_from_slice::<BranchRegistry, _>(&b, standard()).ok())
        .map(|(r, _)| r)
        .unwrap_or_else(BranchRegistry::new);

    let snapshots = store
        .read_meta("snapshots.bin")
        .ok()
        .and_then(|b| {
            decode_from_slice::<
                std::collections::BTreeMap<u64, crate::infinitedb_core::snapshot::Snapshot>,
                _,
            >(&b, standard())
            .ok()
        })
        .map(|(m, _)| m)
        .unwrap_or_default();

    Some((
        spaces,
        branches,
        snapshots,
        revision,
        next_block,
        next_snapshot,
        next_branch,
    ))
}

pub(crate) struct QueryCtx<'a> {
    pub live_tail: Option<&'a LiveTailView>,
    pub space_tails: Option<&'a SpaceLiveTails>,
    pub hilbert_tails: Option<&'a HilbertLiveTails>,
}

fn default_meta() -> MetaTuple {
    (
        SpaceRegistry::new(),
        BranchRegistry::new(),
        std::collections::BTreeMap::new(),
        0,
        1,
        1,
        2,
    )
}
