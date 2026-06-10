//! Append-only hot segment files (`hot/<space_id>.seg`).
//!
//! Owned exclusively by the I/O thread in Phase A. Records land here on the
//! direct-write fast path before eventual seal into immutable blocks.

use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use bincode::{config::standard, decode_from_slice, encode_to_vec};
use blake3::Hasher;

use crate::infinitedb_core::block::Record;
use crate::infinitedb_storage::wal::WalEntry;

/// Header prepended to each hot segment file.
const MAGIC: &[u8; 8] = b"IDB_HOT\0";

/// Append-only hot segment for one space.
pub struct HotSegment {
    path: PathBuf,
    file: File,
    committed_len: u64,
}

impl HotSegment {
    /// Open or create `hot/<space_id>.seg` under `root` (format v2).
    pub fn open(root: PathBuf, space_id: u64) -> io::Result<Self> {
        let hot_dir = root.join("hot");
        std::fs::create_dir_all(&hot_dir)?;
        Self::open_at_path(hot_dir.join(format!("{space_id}.seg")))
    }

    /// Open or create `spaces/<id>/hot.seg` (format v3).
    pub fn open_in_space_dir(space_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(space_dir)?;
        Self::open_at_path(space_dir.join("hot.seg"))
    }

    /// Open or create `spaces/<id>/shards/<shard_id>/hot.seg` (format v4).
    pub fn open_in_shard_dir(space_dir: &Path, shard_id: u32) -> io::Result<Self> {
        let shard_dir = space_dir.join("shards").join(shard_id.to_string());
        std::fs::create_dir_all(&shard_dir)?;
        Self::open_at_path(shard_dir.join("hot.seg"))
    }

    fn open_at_path(path: PathBuf) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;

        let committed_len = if path.metadata()?.len() == 0 {
            file.write_all(MAGIC)?;
            file.write_all(&0u64.to_le_bytes())?;
            file.sync_all()?;
            (MAGIC.len() + 8) as u64
        } else {
            file.seek(SeekFrom::Start(0))?;
            let mut magic = [0u8; 8];
            file.read_exact(&mut magic)?;
            if &magic != MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "hot segment magic mismatch",
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

    /// Append one record frame and fsync if completed within `deadline`.
    ///
    /// Returns `true` when the record is durably committed in the hot segment.
    /// On timeout, rolls back the in-flight append and returns `false`.
    pub fn try_append_with_deadline(
        &mut self,
        entry: &WalEntry,
        deadline: Duration,
    ) -> io::Result<bool> {
        let start = Instant::now();
        let payload = encode_to_vec(entry, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let len = payload.len() as u64;
        let checksum = blake3_hash(&payload);
        let frame_start = self.file.seek(SeekFrom::End(0))?;

        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&payload)?;
        self.file.write_all(&checksum)?;

        if start.elapsed() > deadline {
            self.file.set_len(frame_start)?;
            self.file.seek(SeekFrom::End(0))?;
            return Ok(false);
        }

        self.file.sync_all()?;
        if start.elapsed() > deadline {
            self.file.set_len(frame_start)?;
            self.file.seek(SeekFrom::End(0))?;
            return Ok(false);
        }

        self.committed_len = self.file.metadata()?.len();
        self.write_committed_len()?;
        Ok(true)
    }

    /// Append and fsync without a deadline (I/O thread promotion path).
    pub fn append_and_sync(&mut self, entry: &WalEntry) -> io::Result<()> {
        let payload = encode_to_vec(entry, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let len = payload.len() as u64;
        let checksum = blake3_hash(&payload);
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&payload)?;
        self.file.write_all(&checksum)?;
        self.file.sync_all()?;
        self.committed_len = self.file.metadata()?.len();
        self.write_committed_len()
    }

    /// Read all valid frames from disk (recovery and query).
    pub fn read_all_entries(&mut self) -> io::Result<Vec<WalEntry>> {
        self.file.seek(SeekFrom::Start((MAGIC.len() + 8) as u64))?;
        read_frames(&mut self.file)
    }

    /// Decode all records from valid frames.
    pub fn read_all_records(&mut self) -> io::Result<Vec<Record>> {
        Ok(self
            .read_all_entries()?
            .into_iter()
            .filter_map(wal_entry_to_record)
            .collect())
    }

    /// Number of bytes durably committed.
    pub fn committed_bytes(&self) -> u64 {
        self.committed_len
    }

    /// Truncate after sealing into a block.
    pub fn reset(&mut self) -> io::Result<()> {
        self.file.set_len((MAGIC.len() + 8) as u64)?;
        self.file.seek(SeekFrom::End(0))?;
        self.committed_len = (MAGIC.len() + 8) as u64;
        self.write_committed_len()?;
        self.file.sync_all()
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    fn write_committed_len(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(MAGIC.len() as u64))?;
        self.file.write_all(&self.committed_len.to_le_bytes())?;
        self.file.sync_all()?;
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }
}

fn read_frames(file: &mut File) -> io::Result<Vec<WalEntry>> {
    let mut out = Vec::new();
    let mut len_buf = [0u8; 8];
    loop {
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

pub fn wal_entry_to_record(entry: WalEntry) -> Option<Record> {
    match entry {
        WalEntry::Write {
            address,
            revision,
            data,
        } => Some(Record {
            address,
            revision,
            data,
            tombstone: false,
        }),
        WalEntry::Tombstone { address, revision } => Some(Record {
            address,
            revision,
            data: vec![],
            tombstone: true,
        }),
        _ => None,
    }
}

fn blake3_hash(data: &[u8]) -> [u8; 32] {
    let mut h = Hasher::new();
    h.update(data);
    *h.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::{Address, DimensionVector, RevisionId, SpaceId};
    use tempfile::TempDir;

    fn sample_entry() -> WalEntry {
        WalEntry::Write {
            address: Address::new(SpaceId(1), DimensionVector::new(vec![1, 2])),
            revision: RevisionId(1),
            data: vec![9],
        }
    }

    #[test]
    fn roundtrip_append_and_read() {
        let dir = TempDir::new().unwrap();
        let mut seg = HotSegment::open(dir.path().to_path_buf(), 1).unwrap();
        assert!(seg
            .try_append_with_deadline(&sample_entry(), Duration::from_secs(5))
            .unwrap());
        let records = seg.read_all_records().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].data, vec![9]);
    }
}
