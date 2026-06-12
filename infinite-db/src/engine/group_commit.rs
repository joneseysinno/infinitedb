//! Shared group-commit drain and hot-segment batch apply.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, TryRecvError};

use crate::infinitedb_core::block::Record;
use crate::infinitedb_storage::hot_segment::HotSegment;
use crate::infinitedb_storage::wal::WalEntry;

use super::live_tail::LiveTailView;
use super::session::SessionWatermarks;
use super::write_queue::{IoCommand, WriteJob};

/// Maximum frames coalesced into one group commit.
pub const MAX_GROUP_FRAMES: usize = 4096;

/// Maximum payload bytes coalesced into one group commit.
pub const MAX_GROUP_BYTES: usize = 4 * 1024 * 1024;

/// A drained write group ready for one hot-segment fsync and tail publish.
#[derive(Debug, Default)]
pub struct WriteGroup {
    pub jobs: Vec<WriteJob>,
    pub frame_bytes: usize,
}

impl WriteGroup {
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    pub fn into_records(self) -> Vec<Record> {
        self.jobs.into_iter().map(WriteJob::into_record).collect()
    }
}

fn push_into_group(group: &mut WriteGroup, cmd: IoCommand) -> Option<IoCommand> {
    match cmd {
        IoCommand::Write(job) => {
            group.frame_bytes = group
                .frame_bytes
                .saturating_add(estimate_frame_bytes(job.entry()));
            group.jobs.push(job);
            None
        }
        IoCommand::WriteBatch(batch) => {
            for job in batch {
                group.frame_bytes = group
                    .frame_bytes
                    .saturating_add(estimate_frame_bytes(job.entry()));
                group.jobs.push(job);
            }
            None
        }
        barrier => Some(barrier),
    }
}

/// Drain additional writes after `first`, stopping at barriers or budgets.
pub fn drain_write_group(
    rx: &Receiver<IoCommand>,
    first: IoCommand,
    interval: Duration,
) -> (WriteGroup, Option<IoCommand>) {
    let mut group = WriteGroup::default();
    let started = Instant::now();
    let mut pending_barrier = push_into_group(&mut group, first);

    while pending_barrier.is_none()
        && !group.jobs.is_empty()
        && group.jobs.len() < MAX_GROUP_FRAMES
        && group.frame_bytes < MAX_GROUP_BYTES
        && started.elapsed() < interval
    {
        match rx.try_recv() {
            Ok(cmd) => {
                if let Some(barrier) = push_into_group(&mut group, cmd) {
                    pending_barrier = Some(barrier);
                }
            }
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => break,
        }
    }

    (group, pending_barrier)
}

fn estimate_frame_bytes(entry: &WalEntry) -> usize {
    40 + match entry {
        WalEntry::Write { data, .. } => data.len(),
        WalEntry::Tombstone { .. } => 64,
        _ => 64,
    }
}

/// Append all frames, fsync once, publish tail chunk, retire watermark revisions.
pub fn commit_group_to_hot_segment(
    hot: &mut HotSegment,
    group: WriteGroup,
    live_tail: &LiveTailView,
    watermark: &SessionWatermarks,
    group_commits: &AtomicU64,
) -> io::Result<()> {
    if group.is_empty() {
        return Ok(());
    }

    let revisions: Vec<crate::infinitedb_core::address::RevisionId> =
        group.jobs.iter().map(|j| j.revision).collect();
    let durable_len = hot.committed_bytes();
    for entry in group.jobs.iter().map(WriteJob::entry) {
        let added = hot.append_frame(entry)?;
        hot.track_appended_bytes(added);
    }
    if let Err(e) = hot.sync_group() {
        let _ = hot.truncate_to(durable_len);
        let msg = e.to_string();
        for rev in &revisions {
            watermark.retire_failed(*rev, &msg);
        }
        return Err(e);
    }

    let records = group.into_records();
    live_tail.extend_chunk(records);
    for rev in &revisions {
        watermark.retire(*rev);
    }
    group_commits.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// Migrate leftover staging WAL entries into the hot segment on open.
pub fn migrate_staging_to_hot(hot: &mut HotSegment, entries: &[WalEntry]) -> io::Result<()> {
    for entry in entries {
        hot.append_and_sync(entry)?;
    }
    Ok(())
}
