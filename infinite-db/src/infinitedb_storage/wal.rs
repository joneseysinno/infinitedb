//! Write-Ahead Log (WAL)
//!
//! Every mutation is appended here before any block is written to disk.
//! On crash recovery, the WAL is replayed to reconstruct in-flight writes.
//!
//! Format of each entry on disk:
//!   [8 bytes: payload length] [N bytes: bincode payload] [32 bytes: blake3 checksum]
//!
//! The log is append-only. Rotation (truncation after successful compaction)
//! is triggered by the GC layer.

use std::{
    fs::{File, OpenOptions},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};
use bincode::{config::standard, decode_from_slice, encode_to_vec};
use blake3::Hasher;
use bincode::{Decode, Encode};
use crate::infinitedb_core::{
    address::{Address, RevisionId, SpaceId},
    block::BlockId,
    snapshot::SnapshotId,
};

// ---------------------------------------------------------------------------
// Entry types
// ---------------------------------------------------------------------------

/// A single operation recorded in the WAL.
#[derive(Debug, Clone, Encode, Decode)]
pub enum WalEntry {
    /// A new record was written. Payload is bincode-encoded record data.
    Write {
        address: Address,
        revision: RevisionId,
        data: Vec<u8>,
    },
    /// A record was logically deleted.
    Tombstone {
        address: Address,
        revision: RevisionId,
    },
    /// A block was finalised and assigned an ID.
    BlockSealed {
        block_id: BlockId,
        space: SpaceId,
        snapshot: SnapshotId,
    },
    /// A checkpoint: all entries before `revision` are durable and compacted.
    Checkpoint { revision: RevisionId },
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Append-only WAL writer. One instance per open database.
pub struct WalWriter {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl WalWriter {
    /// Open or create a WAL file at `path`. Always appends.
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            path,
        })
    }

    /// Append one entry, flush, and fsync.
    pub fn append(&mut self, entry: &WalEntry) -> io::Result<()> {
        let payload = encode_to_vec(entry, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let len = payload.len() as u64;
        let checksum = blake3_hash(&payload);

        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&payload)?;
        self.writer.write_all(&checksum)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }

    /// Return the WAL file path currently used by this writer.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

// ---------------------------------------------------------------------------
// Reader / recovery
// ---------------------------------------------------------------------------

/// Reads WAL entries sequentially for crash recovery.
pub struct WalReader {
    file: File,
}

impl WalReader {
    /// Open an existing WAL file for sequential replay.
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let file = File::open(path)?;
        Ok(Self { file })
    }

    /// Read all valid entries from the log. Stops at the first corrupted frame.
    pub fn entries(&mut self) -> io::Result<Vec<WalEntry>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut out = Vec::new();
        let mut len_buf = [0u8; 8];
        loop {
            match self.file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u64::from_le_bytes(len_buf) as usize;
            let mut payload = vec![0u8; len];
            let mut checksum_buf = [0u8; 32];
            self.file.read_exact(&mut payload)?;
            self.file.read_exact(&mut checksum_buf)?;

            // Discard entries whose checksum does not match (truncated write).
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
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn blake3_hash(data: &[u8]) -> [u8; 32] {
    let mut h = Hasher::new();
    h.update(data);
    *h.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::{DimensionVector, SpaceId};
    use tempfile::NamedTempFile;

    fn sample_entry() -> WalEntry {
        WalEntry::Write {
            address: Address::new(
                SpaceId(1),
                DimensionVector::new(vec![10, 20]),
            ),
            revision: RevisionId(1),
            data: vec![1, 2, 3],
        }
    }

    #[test]
    fn roundtrip_single_entry() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let mut writer = WalWriter::open(path.clone()).unwrap();
        writer.append(&sample_entry()).unwrap();
        drop(writer);

        let mut reader = WalReader::open(path).unwrap();
        let entries = reader.entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert!(matches!(entries[0], WalEntry::Write { .. }));
    }

    #[test]
    fn multiple_entries_roundtrip() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let mut writer = WalWriter::open(path.clone()).unwrap();
        writer.append(&sample_entry()).unwrap();
        writer.append(&WalEntry::Checkpoint { revision: RevisionId(1) }).unwrap();
        drop(writer);

        let mut reader = WalReader::open(path).unwrap();
        let entries = reader.entries().unwrap();
        assert_eq!(entries.len(), 2);
    }
}
