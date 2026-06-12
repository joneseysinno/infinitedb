//! Per-session fast durability segments (`sessions/{id}.fast`, Phase 7).
//!
//! Append-only buffer for timed fast-path durability. Not wired into live_tail
//! or query paths until intent checkpoint commit publishes to the live store.

use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use bincode::{config::standard, decode_from_slice, encode_to_vec};
use blake3::Hasher;

use crate::infinitedb_core::hlc::SessionId;
use crate::infinitedb_storage::wal::WalEntry;

/// Test-only: when set, the next [`SessionFastSegment::sync_group`] fails.
#[doc(hidden)]
pub static TEST_FAIL_FAST_SYNC: AtomicBool = AtomicBool::new(false);

/// Test-only: inject applies only while armed.
#[doc(hidden)]
pub static TEST_FAIL_FAST_SYNC_ARMED: AtomicBool = AtomicBool::new(false);

const MAGIC: &[u8; 8] = b"IDB_FST\0";
pub const HEADER_LEN: u64 = (MAGIC.len() + 8) as u64;

/// Outcome of a timed fast-seal attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FastSealOutcome {
    Sealed,
    TimedOut,
}

/// Append-only fast durability segment for one session.
pub struct SessionFastSegment {
    path: PathBuf,
    file: File,
    committed_len: u64,
}

impl SessionFastSegment {
    pub fn open(root: &Path, session: SessionId) -> io::Result<Self> {
        let sessions_dir = root.join("sessions");
        std::fs::create_dir_all(&sessions_dir)?;
        Self::open_at_path(sessions_dir.join(format!("{}.fast", session.0)))
    }

    fn open_at_path(path: PathBuf) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;

        let committed_len = if path.metadata()?.len() == 0 {
            file.write_all(MAGIC)?;
            file.write_all(&HEADER_LEN.to_le_bytes())?;
            file.sync_all()?;
            HEADER_LEN
        } else {
            file.seek(SeekFrom::Start(0))?;
            let mut magic = [0u8; 8];
            file.read_exact(&mut magic)?;
            if &magic != MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "session fast segment magic mismatch",
                ));
            }
            let mut len_buf = [0u8; 8];
            file.read_exact(&mut len_buf)?;
            u64::from_le_bytes(len_buf)
        };

        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            path,
            file,
            committed_len,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn committed_bytes(&self) -> u64 {
        self.committed_len
    }

    pub fn append_frame(&mut self, entry: &WalEntry) -> io::Result<usize> {
        let payload = encode_to_vec(entry, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let len = payload.len() as u64;
        let checksum = blake3_hash(&payload);
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&payload)?;
        self.file.write_all(&checksum)?;
        Ok(8 + payload.len() + 32)
    }

    pub fn track_appended_bytes(&mut self, bytes: usize) {
        self.committed_len = self.committed_len.saturating_add(bytes as u64);
    }

    pub fn sync_group(&mut self) -> io::Result<()> {
        if TEST_FAIL_FAST_SYNC_ARMED.load(Ordering::SeqCst)
            && TEST_FAIL_FAST_SYNC.swap(false, Ordering::SeqCst)
        {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "injected session fast segment fsync failure",
            ));
        }
        self.file.sync_all()?;
        self.write_committed_len_header()
    }

    pub fn truncate_to(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)?;
        self.committed_len = len;
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    pub fn reset(&mut self) -> io::Result<()> {
        self.truncate_to(HEADER_LEN)?;
        self.write_committed_len_header()?;
        self.file.sync_all()
    }

    pub fn read_committed_entries(&mut self) -> io::Result<Vec<WalEntry>> {
        let durable_len = self.read_committed_len_header()?;
        self.file.seek(SeekFrom::Start(HEADER_LEN))?;
        read_frames_up_to(&mut self.file, durable_len)
    }

    /// Append all entries and attempt fsync within `deadline`.
    pub fn try_seal_entries(
        &mut self,
        entries: &[WalEntry],
        deadline: Duration,
    ) -> io::Result<FastSealOutcome> {
        let durable_before = self.committed_len;
        let started = Instant::now();
        for entry in entries {
            if started.elapsed() >= deadline {
                self.truncate_to(durable_before)?;
                return Ok(FastSealOutcome::TimedOut);
            }
            let added = self.append_frame(entry)?;
            self.track_appended_bytes(added);
        }
        if started.elapsed() >= deadline {
            self.truncate_to(durable_before)?;
            return Ok(FastSealOutcome::TimedOut);
        }
        match self.sync_group() {
            Ok(()) => Ok(FastSealOutcome::Sealed),
            Err(e) => {
                let _ = self.truncate_to(durable_before);
                Err(e)
            }
        }
    }

    fn write_committed_len_header(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(MAGIC.len() as u64))?;
        self.file.write_all(&self.committed_len.to_le_bytes())?;
        self.file.sync_all()?;
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn read_committed_len_header(&mut self) -> io::Result<u64> {
        self.file.seek(SeekFrom::Start(MAGIC.len() as u64))?;
        let mut len_buf = [0u8; 8];
        self.file.read_exact(&mut len_buf)?;
        Ok(u64::from_le_bytes(len_buf))
    }
}

pub fn list_fast_segment_ids(root: &Path) -> io::Result<Vec<SessionId>> {
    let dir = root.join("sessions");
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(stem) = name.strip_suffix(".fast") {
            if let Ok(id) = stem.parse::<u32>() {
                out.push(SessionId(id));
            }
        }
    }
    out.sort_by_key(|s| s.0);
    Ok(out)
}

fn read_frames_up_to(file: &mut File, end_offset: u64) -> io::Result<Vec<WalEntry>> {
    let mut out = Vec::new();
    let mut len_buf = [0u8; 8];
    loop {
        if file.stream_position()? >= end_offset {
            break;
        }
        match file.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let len = u64::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        let mut checksum_buf = [0u8; 32];
        file.read_exact(&mut payload)?;
        file.read_exact(&mut checksum_buf)?;
        if blake3_hash(&payload) != checksum_buf {
            break;
        }
        match decode_from_slice::<WalEntry, _>(&payload, standard()) {
            Ok((entry, _)) => out.push(entry),
            Err(_) => break,
        }
    }
    Ok(out)
}

fn blake3_hash(data: &[u8]) -> [u8; 32] {
    let mut h = Hasher::new();
    h.update(data);
    *h.finalize().as_bytes()
}
