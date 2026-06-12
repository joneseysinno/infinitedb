//! Per-session write-ahead log (`sessions/{session_id}.wal`, peer track Phase 3).
//!
//! Frame layout matches [`super::wal::WalWriter`]; a durable `committed_len`
//! header (same discipline as [`super::hot_segment::HotSegment`]) bounds replay.

use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use bincode::{config::standard, decode_from_slice, encode_to_vec, Decode, Encode};
use blake3::Hasher;

use crate::infinitedb_core::{
    address::{Address, RevisionId},
    hlc::HlcStamp,
};
use crate::infinitedb_core::hlc::SessionId;
use crate::infinitedb_core::intent_checkpoint::{IntentCheckpoint, IntentOperationKind};
use crate::infinitedb_storage::error::StorageError;
use crate::infinitedb_storage::wal::WalEntry;

const MAGIC: &[u8; 8] = b"IDB_SWAL";
/// Total header size (magic + committed length field).
pub const HEADER_LEN: u64 = (MAGIC.len() + 8) as u64;

/// One decoded frame from a session WAL (data or intent checkpoint).
#[derive(Debug, Clone)]
pub enum SessionWalFrame {
    Data(WalEntry),
    Intent(IntentCheckpoint),
}

/// Test-only: fail the next [`SessionWalWriter::sync_group`].
#[doc(hidden)]
pub static TEST_FAIL_SESSION_WAL_SYNC: AtomicBool = AtomicBool::new(false);

#[doc(hidden)]
pub static TEST_FAIL_SESSION_WAL_SYNC_ARMED: AtomicBool = AtomicBool::new(false);

/// Append-only session WAL with a committed-length durability boundary.
pub struct SessionWalWriter {
    session: SessionId,
    path: PathBuf,
    file: File,
    committed_len: u64,
    pending_bytes: u64,
}

impl SessionWalWriter {
    pub fn open(root: &Path, session: SessionId) -> io::Result<Self> {
        let dir = root.join("sessions");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.wal", session.0));
        Self::open_at_path(session, path)
    }

    fn open_at_path(session: SessionId, path: PathBuf) -> io::Result<Self> {
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
                    "session wal magic mismatch",
                ));
            }
            let mut len_buf = [0u8; 8];
            file.read_exact(&mut len_buf)?;
            u64::from_le_bytes(len_buf)
        };

        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            session,
            path,
            file,
            committed_len,
            pending_bytes: 0,
        })
    }

    pub fn session(&self) -> SessionId {
        self.session
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn committed_bytes(&self) -> u64 {
        self.committed_len
    }

    /// Append one data frame without advancing the durable boundary.
    pub fn append_frame(&mut self, entry: &WalEntry) -> io::Result<usize> {
        self.append_payload(SessionWalPayload::from_wal_entry(entry))
    }

    /// Append an intent checkpoint closing the current logical operation group.
    pub fn append_intent_checkpoint(&mut self, checkpoint: &IntentCheckpoint) -> io::Result<usize> {
        self.append_payload(SessionWalPayload::from_intent_checkpoint(checkpoint))
    }

    fn append_payload(&mut self, payload: SessionWalPayload) -> io::Result<usize> {
        let payload = encode_session_wal_payload(&payload)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let len = payload.len() as u64;
        let checksum = blake3_hash(&payload);
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&payload)?;
        self.file.write_all(&checksum)?;
        let added = 8 + payload.len() + 32;
        self.pending_bytes = self.pending_bytes.saturating_add(added as u64);
        Ok(added)
    }

    /// Fsync and commit the durable length header for this session group.
    pub fn sync_group(&mut self) -> io::Result<()> {
        if TEST_FAIL_SESSION_WAL_SYNC_ARMED.load(Ordering::SeqCst)
            && TEST_FAIL_SESSION_WAL_SYNC.swap(false, Ordering::SeqCst)
        {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "injected session wal fsync failure",
            ));
        }
        self.committed_len = self.committed_len.saturating_add(self.pending_bytes);
        self.pending_bytes = 0;
        self.file.sync_all()?;
        self.write_committed_len_header()
    }

    /// Roll back uncommitted appends after a failed group fsync.
    pub fn truncate_to(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)?;
        self.committed_len = len;
        self.pending_bytes = 0;
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn write_committed_len_header(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(MAGIC.len() as u64))?;
        self.file.write_all(&self.committed_len.to_le_bytes())?;
        self.file.sync_all()?;
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }
}

/// Recovery reader for one session WAL file.
pub struct SessionWalReader {
    session: SessionId,
    path: PathBuf,
}

impl SessionWalReader {
    pub fn open(root: &Path, session: SessionId) -> io::Result<Self> {
        let path = root.join("sessions").join(format!("{}.wal", session.0));
        Ok(Self { session, path })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn session(&self) -> SessionId {
        self.session
    }

    /// Read durable data/tombstone frames (legacy helper — prefer [`Self::read_committed_frames`]).
    pub fn read_committed_entries(&self) -> Result<Vec<WalEntry>, StorageError> {
        Ok(self
            .read_committed_frames()?
            .into_iter()
            .filter_map(|f| match f {
                SessionWalFrame::Data(e) => Some(e),
                SessionWalFrame::Intent(_) => None,
            })
            .collect())
    }

    /// Read durable frames up to the committed boundary.
    pub fn read_committed_frames(&self) -> Result<Vec<SessionWalFrame>, StorageError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let mut file = File::open(&self.path).map_err(|e| StorageError::from_io(e, Some(self.path.clone())))?;
        file.seek(SeekFrom::Start(0)).map_err(|e| StorageError::from_io(e, Some(self.path.clone())))?;
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)
            .map_err(|e| StorageError::from_io(e, Some(self.path.clone())))?;
        if &magic != MAGIC {
            return Err(StorageError::Corruption {
                message: "session wal magic mismatch".into(),
                path: Some(self.path.clone()),
            });
        }
        let mut len_buf = [0u8; 8];
        file.read_exact(&mut len_buf)
            .map_err(|e| StorageError::from_io(e, Some(self.path.clone())))?;
        let durable_len = u64::from_le_bytes(len_buf);
        read_frames_up_to(&mut file, durable_len).map_err(|e| StorageError::from_io(e, Some(self.path.clone())))
    }
}

/// Encode an intent checkpoint as a session WAL frame payload.
pub fn encode_intent_checkpoint_payload(checkpoint: &IntentCheckpoint) -> Result<Vec<u8>, bincode::error::EncodeError> {
    encode_session_wal_payload(&SessionWalPayload::from_intent_checkpoint(checkpoint))
}

/// Enumerate session ids with a WAL file present under `root/sessions/`.
pub fn list_session_wal_ids(root: &Path) -> io::Result<Vec<SessionId>> {
    let dir = root.join("sessions");
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let s = name.to_string_lossy();
        if let Some(stem) = s.strip_suffix(".wal") {
            if let Ok(n) = stem.parse::<u32>() {
                ids.push(SessionId(n));
            }
        }
    }
    ids.sort_by_key(|s| s.0);
    Ok(ids)
}

fn read_frames_up_to(file: &mut File, end_offset: u64) -> io::Result<Vec<SessionWalFrame>> {
    file.seek(SeekFrom::Start(HEADER_LEN))?;
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "session wal frame checksum mismatch",
            ));
        }
        match decode_session_wal_payload(&payload) {
            Ok((frame, _)) => out.push(frame),
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

/// Session WAL revision wire — preserves full HLC stamps outside global bincode u64.
#[derive(Debug, Clone, Encode, Decode)]
enum SessionWalRevision {
    GlobalLegacy(u64),
    Hlc(u128),
}

impl SessionWalRevision {
    fn from_revision(rev: RevisionId) -> Self {
        if rev.is_global_legacy() {
            Self::GlobalLegacy(rev.legacy_sequence())
        } else {
            Self::Hlc(rev.stamp().pack())
        }
    }

    fn into_revision(self) -> RevisionId {
        match self {
            Self::GlobalLegacy(seq) => RevisionId::legacy(seq),
            Self::Hlc(packed) => RevisionId::from_stamp(HlcStamp::unpack(packed)),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
enum SessionWalPayload {
    Write {
        address: Address,
        revision: SessionWalRevision,
        data: Vec<u8>,
    },
    Tombstone {
        address: Address,
        revision: SessionWalRevision,
    },
    Checkpoint {
        revision: SessionWalRevision,
    },
    IntentCheckpoint {
        first_revision: SessionWalRevision,
        last_revision: SessionWalRevision,
        kind: IntentOperationKind,
    },
}

impl SessionWalPayload {
    fn from_intent_checkpoint(checkpoint: &IntentCheckpoint) -> Self {
        Self::IntentCheckpoint {
            first_revision: SessionWalRevision::from_revision(checkpoint.first_revision),
            last_revision: SessionWalRevision::from_revision(checkpoint.last_revision),
            kind: checkpoint.kind,
        }
    }

    fn into_frame(self) -> SessionWalFrame {
        match self {
            Self::Write {
                address,
                revision,
                data,
            } => SessionWalFrame::Data(WalEntry::Write {
                address,
                revision: revision.into_revision(),
                data,
            }),
            Self::Tombstone { address, revision } => SessionWalFrame::Data(WalEntry::Tombstone {
                address,
                revision: revision.into_revision(),
            }),
            Self::Checkpoint { revision } => SessionWalFrame::Data(WalEntry::Checkpoint {
                revision: revision.into_revision(),
            }),
            Self::IntentCheckpoint {
                first_revision,
                last_revision,
                kind,
            } => SessionWalFrame::Intent(IntentCheckpoint {
                first_revision: first_revision.into_revision(),
                last_revision: last_revision.into_revision(),
                kind,
            }),
        }
    }

    fn from_wal_entry(entry: &WalEntry) -> Self {
        match entry {
            WalEntry::Write {
                address,
                revision,
                data,
            } => Self::Write {
                address: address.clone(),
                revision: SessionWalRevision::from_revision(*revision),
                data: data.clone(),
            },
            WalEntry::Tombstone { address, revision } => Self::Tombstone {
                address: address.clone(),
                revision: SessionWalRevision::from_revision(*revision),
            },
            WalEntry::Checkpoint { revision } => Self::Checkpoint {
                revision: SessionWalRevision::from_revision(*revision),
            },
            WalEntry::BlockSealed { .. } => {
                panic!("session wal does not store block sealed entries")
            }
        }
    }
}

fn encode_session_wal_payload(payload: &SessionWalPayload) -> Result<Vec<u8>, bincode::error::EncodeError> {
    encode_to_vec(payload, standard())
}

fn decode_session_wal_payload(bytes: &[u8]) -> Result<(SessionWalFrame, usize), bincode::error::DecodeError> {
    let (payload, consumed) = decode_from_slice::<SessionWalPayload, _>(bytes, standard())?;
    Ok((payload.into_frame(), consumed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::{Address, DimensionVector, RevisionId, SpaceId};
    use tempfile::TempDir;

    fn sample(session: u32, seq: u32) -> WalEntry {
        WalEntry::Write {
            address: Address::new(SpaceId(1), DimensionVector::new(vec![1, 2])),
            revision: RevisionId::from_stamp(crate::infinitedb_core::hlc::HlcStamp {
                physical_ms: 1,
                logical: 0,
                session,
                sequence: seq,
            }),
            data: vec![9],
        }
    }

    #[test]
    fn roundtrip_committed_frames() {
        let dir = TempDir::new().unwrap();
        let sid = SessionId(3);
        let mut wal = SessionWalWriter::open(dir.path(), sid).unwrap();
        wal.append_frame(&sample(3, 1)).unwrap();
        wal.sync_group().unwrap();
        let entries = SessionWalReader::open(dir.path(), sid)
            .unwrap()
            .read_committed_entries()
            .unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            WalEntry::Write { revision, .. } => assert_eq!(revision.session(), sid.0),
            other => panic!("unexpected entry: {other:?}"),
        }
    }

    #[test]
    fn uncommitted_frames_not_replayed() {
        let dir = TempDir::new().unwrap();
        let sid = SessionId(4);
        let mut wal = SessionWalWriter::open(dir.path(), sid).unwrap();
        wal.append_frame(&sample(4, 1)).unwrap();
        let entries = SessionWalReader::open(dir.path(), sid)
            .unwrap()
            .read_committed_entries()
            .unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn failed_sync_truncates_uncommitted_tail() {
        let dir = TempDir::new().unwrap();
        let sid = SessionId(5);
        let mut wal = SessionWalWriter::open(dir.path(), sid).unwrap();
        let durable = wal.committed_bytes();
        wal.append_frame(&sample(5, 1)).unwrap();
        TEST_FAIL_SESSION_WAL_SYNC_ARMED.store(true, Ordering::SeqCst);
        TEST_FAIL_SESSION_WAL_SYNC.store(true, Ordering::SeqCst);
        assert!(wal.sync_group().is_err());
        TEST_FAIL_SESSION_WAL_SYNC_ARMED.store(false, Ordering::SeqCst);
        wal.truncate_to(durable).unwrap();
        assert!(
            SessionWalReader::open(dir.path(), sid)
                .unwrap()
                .read_committed_entries()
                .unwrap()
                .is_empty()
        );
    }
}
