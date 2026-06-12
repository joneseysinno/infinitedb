//! Per-space dedicated I/O thread (format v3/v4).

use std::collections::{BTreeMap, HashSet};
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
use super::hilbert_shard::ShardRef;
use super::query::{prepare_records_for_seal, record_identity_key};
use super::io_thread::IoThreadConfig;
use super::live_tail::LiveTailView;
use super::snapshot_store::SnapshotStore;
use super::session::SessionWatermarks;
use super::write_queue::{IoCommand, WriteQueueSender};

pub struct SpaceIoHandle {
    join: Option<JoinHandle<io::Result<()>>>,
    direct_writes: Arc<AtomicU64>,
    staged_writes: Arc<AtomicU64>,
}

impl SpaceIoHandle {
    pub fn spawn(
        space_id: u64,
        space_dir: PathBuf,
        store: Arc<BlockStore>,
        snapshots: Arc<SnapshotStore>,
        live_tail: Arc<LiveTailView>,
        spaces: Arc<RwLock<SpaceRegistry>>,
        next_block_id: Arc<AtomicU64>,
        rx: Receiver<IoCommand>,
        config: IoThreadConfig,
        shard_filter: Option<ShardRef>,
        watermark: Arc<SessionWatermarks>,
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

        let name = format!("infinitedb-io-{space_id}");
        let join = thread::Builder::new()
            .name(name)
            .spawn(move || {
                run_space_io_loop(
                    space_id,
                    space_dir,
                    store,
                    snapshots,
                    live_tail,
                    spaces,
                    next_block_id,
                    rx,
                    config,
                    shard_filter,
                    watermark_clone,
                    overrides_clone,
                    overlays_clone,
                    direct_clone,
                    staged_clone,
                )
            })
            .expect("spawn space io thread");

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
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "space io thread panicked"))??;
        }
        Ok(())
    }
}

pub fn open_space_pipeline(
    space_id: u64,
    space_dir: PathBuf,
    store: Arc<BlockStore>,
    snapshots: Arc<SnapshotStore>,
    live_tail: Arc<LiveTailView>,
    spaces: Arc<RwLock<SpaceRegistry>>,
    next_block_id: Arc<AtomicU64>,
    config: IoThreadConfig,
    shard_filter: Option<ShardRef>,
    watermark: Arc<SessionWatermarks>,
    compaction_overrides: CompactionPolicyOverrides,
    branch_overlays: Option<Arc<BranchOverlayStore>>,
) -> (WriteQueueSender, SpaceIoHandle) {
    let (tx, rx) = WriteQueueSender::new(config.write_queue_capacity);
    let handle = SpaceIoHandle::spawn(
        space_id,
        space_dir,
        store,
        snapshots,
        live_tail,
        spaces,
        next_block_id,
        rx,
        config,
        shard_filter,
        watermark,
        compaction_overrides,
        branch_overlays,
    );
    (tx, handle)
}

struct SpaceIoState {
    space_id: u64,
    space_dir: PathBuf,
    store: Arc<BlockStore>,
    snapshots: Arc<SnapshotStore>,
    live_tail: Arc<LiveTailView>,
    spaces: Arc<RwLock<SpaceRegistry>>,
    next_block_id: Arc<AtomicU64>,
    config: IoThreadConfig,
    hot: HotSegment,
    hot_record_count: usize,
    hot_committed_bytes: u64,
    shard_filter: Option<ShardRef>,
    watermark: Arc<SessionWatermarks>,
    compaction_overrides: CompactionPolicyOverrides,
    branch_overlays: Option<Arc<BranchOverlayStore>>,
    pending_error: Option<io::Error>,
}

fn run_space_io_loop(
    space_id: u64,
    space_dir: PathBuf,
    store: Arc<BlockStore>,
    snapshots: Arc<SnapshotStore>,
    live_tail: Arc<LiveTailView>,
    spaces: Arc<RwLock<SpaceRegistry>>,
    next_block_id: Arc<AtomicU64>,
    rx: Receiver<IoCommand>,
    config: IoThreadConfig,
    shard_filter: Option<ShardRef>,
    watermark: Arc<SessionWatermarks>,
    compaction_overrides: CompactionPolicyOverrides,
    branch_overlays: Option<Arc<BranchOverlayStore>>,
    group_commits: Arc<AtomicU64>,
    _staged_writes: Arc<AtomicU64>,
) -> io::Result<()> {
    let staging_path = space_dir.join("wal").join("staging.log");
    let mut state = SpaceIoState {
        space_id,
        space_dir: space_dir.clone(),
        store,
        snapshots,
        live_tail: live_tail.clone(),
        spaces,
        next_block_id,
        config: config.clone(),
        hot: HotSegment::open_in_space_dir(&space_dir)?,
        hot_record_count: 0,
        hot_committed_bytes: 0,
        shard_filter,
        watermark: Arc::clone(&watermark),
        compaction_overrides,
        branch_overlays,
        pending_error: None,
    };

    bootstrap_shard_blocks(&state)?;

    if staging_path.exists() {
        let mut reader = WalReader::open(staging_path.clone())?;
        let entries = reader.entries()?;
        for entry in entries {
            if let Some(record) = wal_entry_to_record(entry.clone()) {
                let rev = record.revision;
                migrate_staging_to_hot(&mut state.hot, std::slice::from_ref(&entry))?;
                live_tail.append(record);
                watermark.retire(rev);
            }
        }
        let _ = std::fs::remove_file(staging_path);
    }

    let records = state.hot.read_all_records()?;
    state.hot_record_count = records.len();
    state.hot_committed_bytes = state.hot.committed_bytes();
    for record in records {
        let rev = record.revision;
        live_tail.append(record);
        watermark.retire(rev);
    }

    while let Ok(cmd) = rx.recv() {
        if matches!(cmd, IoCommand::Shutdown) {
            let _ = handle_space_barrier(&mut state, cmd);
            persist_space_snapshot(&mut state)?;
            break;
        }
        if let Err(e) = dispatch_space_command(&mut state, &rx, cmd, &group_commits) {
            state.pending_error = Some(e);
        }
    }

    Ok(())
}

fn dispatch_space_command(
    state: &mut SpaceIoState,
    rx: &Receiver<IoCommand>,
    cmd: IoCommand,
    group_commits: &AtomicU64,
) -> io::Result<()> {
    match cmd {
        IoCommand::Write(_) | IoCommand::WriteBatch(_) => {
            let (group, barrier) =
                drain_write_group(rx, cmd, state.config.wal_group_commit_interval);
            commit_space_group(state, group, group_commits)?;
            maybe_auto_seal(state)?;
            if let Some(barrier) = barrier {
                handle_space_barrier(state, barrier)?;
            }
        }
        barrier => handle_space_barrier(state, barrier)?,
    }
    Ok(())
}

fn commit_space_group(
    state: &mut SpaceIoState,
    group: WriteGroup,
    group_commits: &AtomicU64,
) -> io::Result<()> {
    if group.is_empty() {
        return Ok(());
    }
    let frame_bytes = group.frame_bytes;
    let record_count = group.jobs.len();
    match commit_group_to_hot_segment(
        &mut state.hot,
        group,
        &state.live_tail,
        &state.watermark,
        group_commits,
    ) {
        Ok(()) => {
            state.hot_record_count += record_count;
            state.hot_committed_bytes += frame_bytes as u64;
        }
        Err(e) => state.pending_error = Some(e),
    }
    Ok(())
}

fn handle_space_barrier(state: &mut SpaceIoState, cmd: IoCommand) -> io::Result<()> {
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
            debug_assert_eq!(space.0, state.space_id);
            let result = match state.pending_error.take() {
                Some(e) => Err(e),
                None => seal_space(state),
            };
            let _ = done.send(result);
        }
        IoCommand::Shutdown => {}
        IoCommand::Write(_) | IoCommand::WriteBatch(_) => unreachable!(),
    }
    Ok(())
}

fn maybe_auto_seal(state: &mut SpaceIoState) -> io::Result<()> {
    if state.hot_record_count >= state.config.hot_segment_seal_threshold
        || state.hot_committed_bytes >= state.config.hot_segment_seal_bytes as u64
    {
        seal_space(state)?;
    }
    Ok(())
}

fn seal_space(state: &mut SpaceIoState) -> io::Result<()> {
    let view = state.live_tail.load_view();
    let mut records: Vec<_> = view.tail_iter().cloned().collect();
    if records.is_empty() {
        return Ok(());
    }

    let spaces = state.spaces.read();
    prepare_records_for_seal(&spaces, &mut records);

    let min_rev = records.iter().map(|r| r.revision).min().unwrap_or(RevisionId::ZERO);
    let max_rev = records.iter().map(|r| r.revision).max().unwrap_or(RevisionId::ZERO);
    let block_id = BlockId(state.next_block_id.fetch_add(1, Ordering::Relaxed));
    let space = SpaceId(state.space_id);

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
        .map(|r| record_identity_key(&spaces, r))
        .collect();
    drop(spaces);

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
    state.live_tail.seal(hilbert_min, block_entry, &sealed, &state.spaces.read());

    state.snapshots.update(space, |snap| {
        snap.blocks.insert(hilbert_min, block_entry);
        if snap.revision < max_rev {
            snap.revision = max_rev;
        }
    });

    state.hot.reset()?;
    state.hot_record_count = 0;
    state.hot_committed_bytes = 16;
    persist_space_snapshot(state)?;

    maybe_compact_after_seal(
        &state.store,
        &state.snapshots,
        &state.live_tail,
        &state.spaces,
        &state.next_block_id,
        space,
        state.shard_filter,
        Some(&state.compaction_overrides),
        state.branch_overlays.as_deref(),
    )?;

    Ok(())
}

pub fn bootstrap_live_tail_blocks(
    live_tail: &LiveTailView,
    snapshots: &SnapshotStore,
    space_id: u64,
    shard_filter: Option<ShardRef>,
) {
    let space = SpaceId(space_id);
    let Some(snap) = snapshots.get(space) else {
        return;
    };
    let blocks: BTreeMap<HilbertKey, BlockIndexEntry> = snap
        .blocks
        .iter()
        .filter(|(min_key, _)| match shard_filter {
            None => true,
            Some(shard) => shard.contains_key(**min_key),
        })
        .map(|(k, v)| (*k, v.clone()))
        .collect();
    if !blocks.is_empty() {
        live_tail.init_blocks(blocks);
    }
}

fn bootstrap_shard_blocks(state: &SpaceIoState) -> io::Result<()> {
    bootstrap_live_tail_blocks(
        &state.live_tail,
        &state.snapshots,
        state.space_id,
        state.shard_filter,
    );
    Ok(())
}

fn persist_space_snapshot(state: &mut SpaceIoState) -> io::Result<()> {
    if let Some(snap) = state.snapshots.get(SpaceId(state.space_id)) {
        let bytes = bincode::encode_to_vec(&*snap, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let path = state.space_dir.join("snapshot.bin");
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, path)?;
    }
    Ok(())
}
