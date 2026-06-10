//! Per-space dedicated I/O thread (format v3).

use std::collections::HashSet;
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
    snapshot::{BlockIndexEntry, SnapshotId},
    space::SpaceRegistry,
};
use crate::infinitedb_storage::{
    hot_segment::{wal_entry_to_record, HotSegment},
    nvme::{compute_checksum, BlockStore},
    wal::{WalDurability, WalEntry, WalWriter},
};

use super::io_thread::IoThreadConfig;
use super::live_tail::LiveTailView;
use super::query::space_key;
use super::snapshot_store::SnapshotStore;
use super::write_queue::{IoCommand, WriteJob, WriteQueueSender};

pub struct SpaceIoHandle {
    space_id: u64,
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
        next_snapshot_id: Arc<AtomicU64>,
        rx: Receiver<IoCommand>,
        config: IoThreadConfig,
    ) -> Self {
        let direct_writes = Arc::new(AtomicU64::new(0));
        let staged_writes = Arc::new(AtomicU64::new(0));
        let direct_clone = Arc::clone(&direct_writes);
        let staged_clone = Arc::clone(&staged_writes);

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
                    next_snapshot_id,
                    rx,
                    config,
                    direct_clone,
                    staged_clone,
                )
            })
            .expect("spawn space io thread");

        Self {
            space_id,
            join: Some(join),
            direct_writes,
            staged_writes,
        }
    }

    pub fn space_id(&self) -> u64 {
        self.space_id
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
    next_snapshot_id: Arc<AtomicU64>,
    config: IoThreadConfig,
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
        next_snapshot_id,
        rx,
        config,
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
    next_snapshot_id: Arc<AtomicU64>,
    config: IoThreadConfig,
    hot: HotSegment,
    staging: StagingWal,
    hot_record_count: usize,
}

struct StagingWal {
    writer: WalWriter,
    entries: Vec<WalEntry>,
}

impl StagingWal {
    fn open(path: PathBuf) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let writer = WalWriter::open_with_durability(
            path.clone(),
            WalDurability::Buffered { sync_every: usize::MAX },
        )?;
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

fn run_space_io_loop(
    space_id: u64,
    space_dir: PathBuf,
    store: Arc<BlockStore>,
    snapshots: Arc<SnapshotStore>,
    live_tail: Arc<LiveTailView>,
    spaces: Arc<RwLock<SpaceRegistry>>,
    next_block_id: Arc<AtomicU64>,
    next_snapshot_id: Arc<AtomicU64>,
    rx: Receiver<IoCommand>,
    config: IoThreadConfig,
    direct_writes: Arc<AtomicU64>,
    staged_writes: Arc<AtomicU64>,
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
        next_snapshot_id,
        config: config.clone(),
        hot: HotSegment::open_in_space_dir(&space_dir)?,
        staging: StagingWal::open(staging_path)?,
        hot_record_count: 0,
    };

    for entry in state.staging.entries.clone() {
        if let Some(record) = wal_entry_to_record(entry) {
            live_tail.append(record);
        }
    }

    let records = state.hot.read_all_records()?;
    state.hot_record_count = records.len();
    for record in records {
        live_tail.append(record);
    }

    let mut shutting_down = false;
    while !shutting_down {
        match rx.recv_timeout(config.wal_group_commit_interval) {
            Ok(cmd) => {
                if matches!(cmd, IoCommand::Shutdown) {
                    handle_space_command(&mut state, cmd, &direct_writes, &staged_writes)?;
                    shutting_down = true;
                } else {
                    handle_space_command(&mut state, cmd, &direct_writes, &staged_writes)?;
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

fn handle_space_command(
    state: &mut SpaceIoState,
    cmd: IoCommand,
    direct_writes: &AtomicU64,
    staged_writes: &AtomicU64,
) -> io::Result<()> {
    match cmd {
        IoCommand::Write(job) => {
            process_write(state, job, direct_writes, staged_writes)?;
            maybe_auto_seal(state)?;
        }
        IoCommand::Sync { done } => {
            promote_staging(state)?;
            state.staging.sync()?;
            let _ = done.send(Ok(()));
        }
        IoCommand::Flush { space_id, done } => {
            debug_assert_eq!(space_id, state.space_id);
            let result = seal_space(state);
            let _ = done.send(result);
        }
        IoCommand::Shutdown => {
            promote_staging(state)?;
            state.staging.sync()?;
            persist_space_snapshot(state)?;
        }
    }
    Ok(())
}

fn process_write(
    state: &mut SpaceIoState,
    job: WriteJob,
    direct_writes: &AtomicU64,
    staged_writes: &AtomicU64,
) -> io::Result<()> {
    if state.hot.try_append_with_deadline(&job.entry, state.config.direct_write_timeout)? {
        direct_writes.fetch_add(1, Ordering::Relaxed);
    } else {
        state.staging.append(&job.entry)?;
        state.staging.sync()?;
        staged_writes.fetch_add(1, Ordering::Relaxed);
    }
    state.live_tail.append(job.record);
    state.hot_record_count += 1;
    Ok(())
}

fn maybe_auto_seal(state: &mut SpaceIoState) -> io::Result<()> {
    if state.hot_record_count >= state.config.hot_segment_seal_threshold {
        seal_space(state)?;
    }
    Ok(())
}

fn promote_staging(state: &mut SpaceIoState) -> io::Result<()> {
    if state.staging.entries.is_empty() {
        return Ok(());
    }
    let mut remaining = Vec::new();
    for entry in state.staging.entries.drain(..) {
        if state.hot.append_and_sync(&entry).is_ok() {
            continue;
        }
        remaining.push(entry);
    }
    state.staging.rewrite_remaining(remaining)?;
    Ok(())
}

fn seal_space(state: &mut SpaceIoState) -> io::Result<()> {
    let mut records = state.hot.read_all_records()?;
    if records.is_empty() {
        return Ok(());
    }

    let space = SpaceId(state.space_id);
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

    state.hot.reset()?;
    state.hot_record_count = 0;
    persist_space_snapshot(state)?;
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
