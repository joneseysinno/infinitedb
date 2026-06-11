//! Dedicated disk I/O thread (fire-and-forget write drain).

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::Receiver;
use parking_lot::RwLock;

use crate::infinitedb_core::{
    address::{RevisionId, SpaceId},
    block::{Block, BlockId},
    checksum::Checksum,
    hilbert_key::HilbertKey,
    record_identity::RecordIdentityKey,
    snapshot::BlockIndexEntry,
    space::SpaceRegistry,
};
use crate::infinitedb_storage::{
    hot_segment::{wal_entry_to_record, HotSegment},
    nvme::{compute_checksum, BlockStore},
    wal::WalReader,
};

use super::branch_overlay::BranchOverlayStore;
use super::compactor::{maybe_compact_after_seal, CompactionPolicyOverrides};
use super::group_commit::{commit_group_to_hot_segment, drain_write_group, migrate_staging_to_hot, WriteGroup};
use super::live_tail::LiveTailView;
use super::query::prepare_records_for_seal;
use super::snapshot_store::SnapshotStore;
use super::watermark::RevisionWatermark;
use super::write_queue::{IoCommand, WriteQueueSender};

/// Tuning for the dedicated I/O thread.
#[derive(Debug, Clone)]
pub struct IoThreadConfig {
    /// Deprecated: ignored; all writes use group-committed hot segments.
    pub direct_write_timeout: std::time::Duration,
    /// Secondary seal trigger by record count (pathological tiny records).
    pub hot_segment_seal_threshold: usize,
    /// Primary seal trigger by committed hot-segment bytes.
    pub hot_segment_seal_bytes: usize,
    pub write_queue_capacity: usize,
    pub wal_group_commit_interval: std::time::Duration,
}

impl Default for IoThreadConfig {
    fn default() -> Self {
        Self {
            direct_write_timeout: std::time::Duration::from_millis(2),
            hot_segment_seal_threshold: 65_536,
            hot_segment_seal_bytes: 8 * 1024 * 1024,
            write_queue_capacity: 4096,
            wal_group_commit_interval: std::time::Duration::from_millis(1),
        }
    }
}

/// Runtime statistics for the I/O thread.
#[derive(Debug, Clone, Default)]
pub struct IoStats {
    pub queue_depth: usize,
    /// Number of group commits to the hot segment.
    pub direct_writes: u64,
    /// Deprecated: always zero (staging WAL removed).
    pub staged_writes: u64,
    pub staging_wal_frames: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteRoute {
    Direct,
    Staged,
}

pub struct IoThreadHandle {
    join: Option<JoinHandle<io::Result<()>>>,
    direct_writes: Arc<AtomicU64>,
    staged_writes: Arc<AtomicU64>,
}

impl IoThreadHandle {
    pub fn spawn(
        root: PathBuf,
        store: Arc<BlockStore>,
        snapshots: Arc<SnapshotStore>,
        live_tail: Arc<LiveTailView>,
        spaces: Arc<RwLock<SpaceRegistry>>,
        next_block_id: Arc<AtomicU64>,
        rx: Receiver<IoCommand>,
        config: IoThreadConfig,
        watermark: Arc<RevisionWatermark>,
        compaction_overrides: CompactionPolicyOverrides,
        branch_overlays: Option<Arc<BranchOverlayStore>>,
    ) -> Self {
        let direct_writes = Arc::new(AtomicU64::new(0));
        let staged_writes = Arc::new(AtomicU64::new(0));
        let direct_clone = Arc::clone(&direct_writes);
        let staged_clone = Arc::clone(&staged_writes);
        let watermark_clone = Arc::clone(&watermark);
        let overrides_clone = Arc::clone(&compaction_overrides);
        let overlays_clone = branch_overlays;

        let join = thread::Builder::new()
            .name("infinitedb-io".into())
            .spawn(move || {
                run_io_loop(
                    root,
                    store,
                    snapshots,
                    live_tail,
                    spaces,
                    next_block_id,
                    rx,
                    config,
                    watermark_clone,
                    overrides_clone,
                    overlays_clone,
                    direct_clone,
                    staged_clone,
                )
            })
            .expect("spawn io thread");

        Self {
            join: Some(join),
            direct_writes,
            staged_writes,
        }
    }

    pub fn direct_writes(&self) -> u64 {
        self.direct_writes.load(Ordering::Relaxed)
    }

    pub fn staged_writes(&self) -> u64 {
        self.staged_writes.load(Ordering::Relaxed)
    }

    pub fn join(&mut self) -> io::Result<()> {
        if let Some(handle) = self.join.take() {
            handle
                .join()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "io thread panicked"))??;
        }
        Ok(())
    }
}

pub fn open_io_pipeline(
    root: PathBuf,
    store: Arc<BlockStore>,
    snapshots: Arc<SnapshotStore>,
    live_tail: Arc<LiveTailView>,
    spaces: Arc<RwLock<SpaceRegistry>>,
    next_block_id: Arc<AtomicU64>,
    config: IoThreadConfig,
    watermark: Arc<RevisionWatermark>,
    compaction_overrides: CompactionPolicyOverrides,
    branch_overlays: Option<Arc<BranchOverlayStore>>,
) -> (WriteQueueSender, IoThreadHandle) {
    let (tx, rx) = WriteQueueSender::new(config.write_queue_capacity);
    let handle = IoThreadHandle::spawn(
        root,
        store,
        snapshots,
        live_tail,
        spaces,
        next_block_id,
        rx,
        config,
        watermark,
        compaction_overrides,
        branch_overlays,
    );
    (tx, handle)
}

struct IoState {
    root: PathBuf,
    store: Arc<BlockStore>,
    snapshots: Arc<SnapshotStore>,
    live_tail: Arc<LiveTailView>,
    spaces: Arc<RwLock<SpaceRegistry>>,
    next_block_id: Arc<AtomicU64>,
    config: IoThreadConfig,
    hot: HashMap<SpaceId, HotSegment>,
    hot_record_counts: HashMap<SpaceId, usize>,
    hot_committed_bytes: HashMap<SpaceId, u64>,
    watermark: Arc<RevisionWatermark>,
    compaction_overrides: CompactionPolicyOverrides,
    branch_overlays: Option<Arc<BranchOverlayStore>>,
    pending_error: Option<io::Error>,
}

fn run_io_loop(
    root: PathBuf,
    store: Arc<BlockStore>,
    snapshots: Arc<SnapshotStore>,
    live_tail: Arc<LiveTailView>,
    spaces: Arc<RwLock<SpaceRegistry>>,
    next_block_id: Arc<AtomicU64>,
    rx: Receiver<IoCommand>,
    config: IoThreadConfig,
    watermark: Arc<RevisionWatermark>,
    compaction_overrides: CompactionPolicyOverrides,
    branch_overlays: Option<Arc<BranchOverlayStore>>,
    group_commits: Arc<AtomicU64>,
    _staged_writes: Arc<AtomicU64>,
) -> io::Result<()> {
    let staging_path = store.staging_wal_path();
    let mut state = IoState {
        root: root.clone(),
        store,
        snapshots,
        live_tail,
        spaces,
        next_block_id,
        config: config.clone(),
        hot: HashMap::new(),
        hot_record_counts: HashMap::new(),
        hot_committed_bytes: HashMap::new(),
        watermark: Arc::clone(&watermark),
        compaction_overrides,
        branch_overlays,
        pending_error: None,
    };

    if staging_path.exists() {
        let mut reader = WalReader::open(staging_path.clone())?;
        let entries = reader.entries()?;
        for entry in entries {
            if let Some(record) = wal_entry_to_record(entry.clone()) {
                let space = record.address.space;
                let hot = state
                    .hot
                    .entry(space)
                    .or_insert_with(|| HotSegment::open(root.clone(), space.0).expect("open hot"));
                let rev = record.revision;
                migrate_staging_to_hot(hot, std::slice::from_ref(&entry))?;
                state.live_tail.append(record);
                watermark.retire(rev);
            }
        }
        let _ = std::fs::remove_file(staging_path);
    }

    let hot_dir = state.root.join("hot");
    if hot_dir.exists() {
        for entry in std::fs::read_dir(hot_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(stem) = name.strip_suffix(".seg") {
                if let Ok(space_raw) = stem.parse::<u64>() {
                    let space = SpaceId(space_raw);
                    let mut seg = HotSegment::open(state.root.clone(), space_raw)?;
                    let records = seg.read_all_records()?;
                    state.hot_record_counts.insert(space, records.len());
                    state
                        .hot_committed_bytes
                        .insert(space, seg.committed_bytes());
                    for record in records {
                        let rev = record.revision;
                        state.live_tail.append(record);
                        watermark.retire(rev);
                    }
                    state.hot.insert(space, seg);
                }
            }
        }
    }

    while let Ok(cmd) = rx.recv() {
        if matches!(cmd, IoCommand::Shutdown) {
            let _ = handle_barrier(&mut state, cmd, &group_commits);
            break;
        }
        if let Err(e) = dispatch_command(&mut state, &rx, cmd, &group_commits) {
            state.pending_error = Some(e);
        }
    }

    Ok(())
}

fn dispatch_command(
    state: &mut IoState,
    rx: &Receiver<IoCommand>,
    cmd: IoCommand,
    group_commits: &AtomicU64,
) -> io::Result<()> {
    match cmd {
        IoCommand::Write(_) | IoCommand::WriteBatch(_) => {
            let (group, barrier) =
                drain_write_group(rx, cmd, state.config.wal_group_commit_interval);
            let affected = commit_write_group(state, group, group_commits)?;
            for space_id in affected {
                maybe_auto_seal(state, space_id)?;
            }
            if let Some(barrier) = barrier {
                handle_barrier(state, barrier, group_commits)?;
            }
        }
        barrier => handle_barrier(state, barrier, group_commits)?,
    }
    Ok(())
}

fn commit_write_group(
    state: &mut IoState,
    group: WriteGroup,
    group_commits: &AtomicU64,
) -> io::Result<Vec<SpaceId>> {
    if group.is_empty() {
        return Ok(Vec::new());
    }

    let mut by_space: HashMap<SpaceId, WriteGroup> = HashMap::new();
    for job in group.jobs {
        by_space.entry(job.space_id()).or_default().jobs.push(job);
    }

    let affected: Vec<SpaceId> = by_space.keys().copied().collect();
    for (space, space_group) in by_space {
        let hot = state
            .hot
            .entry(space)
            .or_insert_with(|| HotSegment::open(state.root.clone(), space.0).expect("open hot"));
        let frame_bytes = space_group.frame_bytes;
        let record_count = space_group.jobs.len();
        match commit_group_to_hot_segment(
            hot,
            space_group,
            &state.live_tail,
            &state.watermark,
            group_commits,
        ) {
            Ok(()) => {
                *state.hot_record_counts.entry(space).or_insert(0) += record_count;
                *state.hot_committed_bytes.entry(space).or_insert(0) += frame_bytes as u64;
            }
            Err(e) => {
                state.pending_error = Some(e);
                return Ok(affected);
            }
        }
    }
    Ok(affected)
}

fn handle_barrier(
    state: &mut IoState,
    cmd: IoCommand,
    group_commits: &AtomicU64,
) -> io::Result<()> {
    match cmd {
        IoCommand::Sync { done } => {
            let result = state
                .pending_error
                .take()
                .map(Err)
                .unwrap_or(Ok(()));
            let _ = done.send(result);
        }
        IoCommand::Flush { space, done } => {
            let result = match state.pending_error.take() {
                Some(e) => Err(e),
                None => seal_space(state, space),
            };
            let _ = done.send(result);
        }
        IoCommand::Shutdown => {}
        IoCommand::Write(_) | IoCommand::WriteBatch(_) => unreachable!(),
    }
    let _ = group_commits;
    Ok(())
}

fn maybe_auto_seal(state: &mut IoState, space: SpaceId) -> io::Result<()> {
    let count = state.hot_record_counts.get(&space).copied().unwrap_or(0);
    let bytes = state.hot_committed_bytes.get(&space).copied().unwrap_or(0);
    if count >= state.config.hot_segment_seal_threshold
        || bytes >= state.config.hot_segment_seal_bytes as u64
    {
        seal_space(state, space)?;
    }
    Ok(())
}

fn seal_space(state: &mut IoState, space: SpaceId) -> io::Result<()> {
    let view = state.live_tail.load_view();
    let mut records: Vec<_> = view
        .tail_iter()
        .filter(|r| r.address.space == space)
        .cloned()
        .collect();
    if records.is_empty() {
        return Ok(());
    }

    let spaces = state.spaces.read();
    prepare_records_for_seal(&spaces, &mut records);

    let min_rev = records.iter().map(|r| r.revision).min().unwrap_or(RevisionId::ZERO);
    let max_rev = records.iter().map(|r| r.revision).max().unwrap_or(RevisionId::ZERO);
    let block_id = BlockId(state.next_block_id.fetch_add(1, Ordering::Relaxed));

    let hilbert_min = records
        .first()
        .and_then(|r| r.hilbert_key.get())
        .unwrap_or(HilbertKey::ZERO);
    let hilbert_max = records
        .last()
        .and_then(|r| r.hilbert_key.get())
        .unwrap_or(hilbert_min);

    let sealed: HashSet<RecordIdentityKey> = records
        .iter()
        .map(RecordIdentityKey::from_record)
        .collect();

    let mut block = Block {
        id: block_id,
        space,
        records,
        min_revision: min_rev,
        max_revision: max_rev,
        checksum: Checksum::ZERO,
    };
    block.checksum = compute_checksum(&block)?;
    state.store.write_block(&block)?;

    let block_entry = BlockIndexEntry {
        block_id,
        max_key: hilbert_max,
    };
    state.live_tail.seal(hilbert_min, block_entry, &sealed);

    state.snapshots.update(space, |snap| {
        snap.blocks.insert(hilbert_min, block_entry);
        if snap.revision < max_rev {
            snap.revision = max_rev;
        }
    });

    if let Some(hot) = state.hot.get_mut(&space) {
        hot.reset()?;
    }
    state.hot_record_counts.insert(space, 0);
    state.hot_committed_bytes.insert(space, hot_header_len());

    maybe_compact_after_seal(
        &state.store,
        &state.snapshots,
        &state.live_tail,
        &state.spaces,
        &state.next_block_id,
        space,
        None,
        Some(&state.compaction_overrides),
        state.branch_overlays.as_deref(),
    )?;

    Ok(())
}

fn hot_header_len() -> u64 {
    16
}
