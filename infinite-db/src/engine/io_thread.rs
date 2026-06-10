//! Dedicated disk I/O thread (fire-and-forget write drain).

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::Receiver;
use parking_lot::RwLock;

use crate::infinitedb_core::{
    address::{RevisionId, SpaceId},
    block::{Block, BlockId},
    snapshot::{BlockIndexEntry, SnapshotId},
    space::SpaceRegistry,
};
use crate::infinitedb_storage::{
    hot_segment::{wal_entry_to_record, HotSegment},
    nvme::{compute_checksum, BlockStore},
    wal::{WalDurability, WalEntry, WalWriter},
};

use super::live_tail::LiveTailView;
use super::query::space_key;
use super::snapshot_store::SnapshotStore;
use super::write_queue::{IoCommand, WriteJob, WriteQueueSender};

/// Tuning for the dedicated I/O thread.
#[derive(Debug, Clone)]
pub struct IoThreadConfig {
    pub direct_write_timeout: Duration,
    pub hot_segment_seal_threshold: usize,
    pub write_queue_capacity: usize,
    pub wal_group_commit_interval: Duration,
}

impl Default for IoThreadConfig {
    fn default() -> Self {
        Self {
            direct_write_timeout: Duration::from_millis(2),
            hot_segment_seal_threshold: 256,
            write_queue_capacity: 4096,
            wal_group_commit_interval: Duration::from_millis(1),
        }
    }
}

/// Runtime statistics for the I/O thread.
#[derive(Debug, Clone, Default)]
pub struct IoStats {
    pub queue_depth: usize,
    pub direct_writes: u64,
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
        revision: Arc<AtomicU64>,
        next_block_id: Arc<AtomicU64>,
        next_snapshot_id: Arc<AtomicU64>,
        rx: Receiver<IoCommand>,
        config: IoThreadConfig,
    ) -> Self {
        let direct_writes = Arc::new(AtomicU64::new(0));
        let staged_writes = Arc::new(AtomicU64::new(0));
        let direct_clone = Arc::clone(&direct_writes);
        let staged_clone = Arc::clone(&staged_writes);

        let join = thread::Builder::new()
            .name("infinitedb-io".into())
            .spawn(move || {
                run_io_loop(
                    root,
                    store,
                    snapshots,
                    live_tail,
                    spaces,
                    revision,
                    next_block_id,
                    next_snapshot_id,
                    rx,
                    config,
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
    revision: Arc<AtomicU64>,
    next_block_id: Arc<AtomicU64>,
    next_snapshot_id: Arc<AtomicU64>,
    config: IoThreadConfig,
) -> (WriteQueueSender, IoThreadHandle) {
    let (tx, rx) = WriteQueueSender::new(config.write_queue_capacity);
    let handle = IoThreadHandle::spawn(
        root,
        store,
        snapshots,
        live_tail,
        spaces,
        revision,
        next_block_id,
        next_snapshot_id,
        rx,
        config,
    );
    (tx, handle)
}

struct IoState {
    root: PathBuf,
    store: Arc<BlockStore>,
    snapshots: Arc<SnapshotStore>,
    live_tail: Arc<LiveTailView>,
    spaces: Arc<RwLock<SpaceRegistry>>,
    revision: Arc<AtomicU64>,
    next_block_id: Arc<AtomicU64>,
    next_snapshot_id: Arc<AtomicU64>,
    config: IoThreadConfig,
    hot: HashMap<u64, HotSegment>,
    staging: StagingWal,
    hot_record_counts: HashMap<u64, usize>,
}

struct StagingWal {
    writer: WalWriter,
    entries: Vec<WalEntry>,
}

impl StagingWal {
    fn open(path: PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(path.parent().unwrap())?;
        let writer =
            WalWriter::open_with_durability(path.clone(), WalDurability::Buffered { sync_every: usize::MAX })?;
        let mut reader = crate::infinitedb_storage::wal::WalReader::open(path)?;
        let entries = reader.entries()?;
        Ok(Self { writer, entries })
    }

    fn append(&mut self, entry: &WalEntry) -> io::Result<()> {
        self.writer.append_frame(entry)?;
        self.entries.push(entry.clone());
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.writer.sync()
    }

    fn rewrite_remaining(&mut self, entries: Vec<WalEntry>) -> io::Result<()> {
        self.entries = entries;
        self.writer.rewrite(&self.entries)
    }
}

fn run_io_loop(
    root: PathBuf,
    store: Arc<BlockStore>,
    snapshots: Arc<SnapshotStore>,
    live_tail: Arc<LiveTailView>,
    spaces: Arc<RwLock<SpaceRegistry>>,
    revision: Arc<AtomicU64>,
    next_block_id: Arc<AtomicU64>,
    next_snapshot_id: Arc<AtomicU64>,
    rx: Receiver<IoCommand>,
    config: IoThreadConfig,
    direct_writes: Arc<AtomicU64>,
    staged_writes: Arc<AtomicU64>,
) -> io::Result<()> {
    let staging_path = store.staging_wal_path();
    let mut state = IoState {
        root,
        store,
        snapshots,
        live_tail,
        spaces,
        revision,
        next_block_id,
        next_snapshot_id,
        config: config.clone(),
        hot: HashMap::new(),
        staging: StagingWal::open(staging_path)?,
        hot_record_counts: HashMap::new(),
    };

    // Recover staging WAL into live tail.
    for entry in state.staging.entries.clone() {
        if let Some(record) = wal_entry_to_record(entry) {
            state.live_tail.append(record);
        }
    }

    // Recover hot segments into live tail (idempotent merge handled by revision uniqueness in tests).
    let hot_dir = state.root.join("hot");
    if hot_dir.exists() {
        for entry in std::fs::read_dir(hot_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(stem) = name.strip_suffix(".seg") {
                if let Ok(space_id) = stem.parse::<u64>() {
                    let mut seg = HotSegment::open(state.root.clone(), space_id)?;
                    let records = seg.read_all_records()?;
                    state.hot_record_counts.insert(space_id, records.len());
                    for record in records {
                        state.live_tail.append(record);
                    }
                    state.hot.insert(space_id, seg);
                }
            }
        }
    }

    let mut shutting_down = false;
    while !shutting_down {
        match rx.recv_timeout(config.wal_group_commit_interval) {
            Ok(cmd) => {
                if matches!(cmd, IoCommand::Shutdown) {
                    handle_command(&mut state, cmd, &direct_writes, &staged_writes)?;
                    shutting_down = true;
                } else {
                    handle_command(&mut state, cmd, &direct_writes, &staged_writes)?;
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                promote_staging(&mut state)?;
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => shutting_down = true,
        }
    }

    Ok(())
}

fn handle_command(
    state: &mut IoState,
    cmd: IoCommand,
    direct_writes: &AtomicU64,
    staged_writes: &AtomicU64,
) -> io::Result<()> {
    match cmd {
        IoCommand::Write(job) => {
            let space_id = job_space(&job);
            process_write(state, job, direct_writes, staged_writes)?;
            maybe_auto_seal(state, space_id)?;
        }
        IoCommand::Sync { done } => {
            promote_staging(state)?;
            state.staging.sync()?;
            let _ = done.send(Ok(()));
        }
        IoCommand::Flush { space_id, done } => {
            let result = seal_space(state, space_id);
            let _ = done.send(result);
        }
        IoCommand::Shutdown => {
            promote_staging(state)?;
            state.staging.sync()?;
            return Ok(());
        }
    }
    Ok(())
}

fn job_space(job: &WriteJob) -> u64 {
    job.record.address.space.0
}

fn process_write(
    state: &mut IoState,
    job: WriteJob,
    direct_writes: &AtomicU64,
    staged_writes: &AtomicU64,
) -> io::Result<()> {
    let space_id = job_space(&job);
    let hot = state
        .hot
        .entry(space_id)
        .or_insert_with(|| {
            HotSegment::open(state.root.clone(), space_id).expect("open hot segment")
        });

    let routed = if hot.try_append_with_deadline(&job.entry, state.config.direct_write_timeout)? {
        direct_writes.fetch_add(1, Ordering::Relaxed);
        WriteRoute::Direct
    } else {
        state.staging.append(&job.entry)?;
        state.staging.sync()?;
        staged_writes.fetch_add(1, Ordering::Relaxed);
        WriteRoute::Staged
    };

    let _ = routed;
    state.live_tail.append(job.record);
    *state.hot_record_counts.entry(space_id).or_insert(0) += 1;
    Ok(())
}

fn maybe_auto_seal(state: &mut IoState, space_id: u64) -> io::Result<()> {
    let count = state.hot_record_counts.get(&space_id).copied().unwrap_or(0);
    if count >= state.config.hot_segment_seal_threshold {
        seal_space(state, space_id)?;
    }
    Ok(())
}

fn promote_staging(state: &mut IoState) -> io::Result<()> {
    if state.staging.entries.is_empty() {
        return Ok(());
    }

    let mut remaining = Vec::new();
    for entry in state.staging.entries.drain(..) {
        let space_id = match &entry {
            WalEntry::Write { address, .. } | WalEntry::Tombstone { address, .. } => address.space.0,
            _ => {
                remaining.push(entry);
                continue;
            }
        };

        let hot = state
            .hot
            .entry(space_id)
            .or_insert_with(|| HotSegment::open(state.root.clone(), space_id).expect("open hot segment"));

        if hot.append_and_sync(&entry).is_ok() {
            // promoted
        } else {
            remaining.push(entry);
        }
    }

    state.staging.rewrite_remaining(remaining)?;
    Ok(())
}

fn seal_space(state: &mut IoState, space_id: u64) -> io::Result<()> {
    let mut hot = match state.hot.remove(&space_id) {
        Some(seg) => seg,
        None => HotSegment::open(state.root.clone(), space_id)?,
    };

    let mut records = hot.read_all_records()?;
    if records.is_empty() {
        state.hot.insert(space_id, hot);
        return Ok(());
    }

    let space = SpaceId(space_id);
    let spaces = state.spaces.read();
    records.sort_by_key(|r| {
        let key = space_key(&spaces, space, &r.address.point);
        (key, r.revision.0)
    });
    drop(spaces);

    let min_rev = records.iter().map(|r| r.revision).min().unwrap_or(RevisionId::ZERO);
    let max_rev = records.iter().map(|r| r.revision).max().unwrap_or(RevisionId::ZERO);
    let block_id = BlockId(state.next_block_id.fetch_add(1, Ordering::Relaxed));

    let mut block = Block {
        id: block_id,
        space,
        records: records.clone(),
        min_revision: min_rev,
        max_revision: max_rev,
        checksum: [0u8; 32],
    };
    block.checksum = compute_checksum(&block)?;
    state.store.write_block(&block)?;

    let hilbert_min = {
        let spaces = state.spaces.read();
        block
            .records
            .first()
            .map(|r| space_key(&spaces, space, &r.address.point))
            .unwrap_or(0)
    };
    let hilbert_max = {
        let spaces = state.spaces.read();
        block
            .records
            .last()
            .map(|r| space_key(&spaces, space, &r.address.point))
            .unwrap_or(hilbert_min)
    };

    let snap_id = SnapshotId(state.next_snapshot_id.fetch_add(1, Ordering::Relaxed));
    state.snapshots.update(space, |snap| {
        snap.blocks.insert(
            hilbert_min,
            BlockIndexEntry {
                block_id,
                max_key: hilbert_max,
            },
        );
        if snap.revision < max_rev {
            snap.revision = max_rev;
        }
        if snap.id.0 == 0 {
            snap.id = snap_id;
        }
    });

    let sealed: HashSet<(Vec<u32>, u64)> = records
        .iter()
        .map(|r| (r.address.point.coords.clone(), r.revision.0))
        .collect();

    let mut tail = state.live_tail.snapshot();
    tail.retain(|r| !sealed.contains(&(r.address.point.coords.clone(), r.revision.0)));
    state.live_tail.publish(tail);

    hot.reset()?;
    state.hot.insert(space_id, hot);
    state.hot_record_counts.insert(space_id, 0);
    Ok(())
}
