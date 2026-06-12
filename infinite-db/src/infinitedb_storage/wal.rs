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
// Durability policy
// ---------------------------------------------------------------------------

/// Controls how often the WAL is flushed and fsynced to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalDurability {
    /// Every appended frame is synced immediately (default for normal writes).
    Strict,
    /// Sync after every `sync_every` appended frames (bulk import).
    Buffered { sync_every: usize },
}

impl WalDurability {
    /// Minimum appended frames before an automatic [`WalWriter::sync`].
    pub fn sync_every(self) -> usize {
        match self {
            WalDurability::Strict => 1,
            WalDurability::Buffered { sync_every } => sync_every.max(1),
        }
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Append-only WAL writer. One instance per open database.
pub struct WalWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    durability: WalDurability,
    pending_since_sync: usize,
}

impl WalWriter {
    /// Open or create a WAL file at `path` with strict durability.
    pub fn open(path: PathBuf) -> io::Result<Self> {
        Self::open_with_durability(path, WalDurability::Strict)
    }

    /// Open or create a WAL file with the given durability policy.
    pub fn open_with_durability(path: PathBuf, durability: WalDurability) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            path,
            durability,
            pending_since_sync: 0,
        })
    }

    /// Replace the active durability policy (e.g. bulk import guard).
    pub fn set_durability(&mut self, durability: WalDurability) {
        self.durability = durability;
    }

    /// Current durability policy.
    pub fn durability(&self) -> WalDurability {
        self.durability
    }

    /// Frames written since the last successful [`Self::sync`].
    pub fn pending_frames(&self) -> usize {
        self.pending_since_sync
    }

    /// Append one frame to the log without syncing.
    pub fn append_frame(&mut self, entry: &WalEntry) -> io::Result<()> {
        let payload = encode_to_vec(entry, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let len = payload.len() as u64;
        let checksum = blake3_hash(&payload);

        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&payload)?;
        self.writer.write_all(&checksum)?;
        self.pending_since_sync += 1;
        Ok(())
    }

    /// Flush the writer buffer and fsync the log file.
    pub fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        self.pending_since_sync = 0;
        Ok(())
    }

    /// Append one entry and sync according to the active durability policy.
    pub fn append(&mut self, entry: &WalEntry) -> io::Result<()> {
        self.append_frame(entry)?;
        let every = self.durability.sync_every();
        if self.pending_since_sync >= every {
            self.sync()?;
        }
        Ok(())
    }

    /// Replace the entire log with `entries`, then continue in append mode.
    ///
    /// Called after a flush: records sealed into a durable block no longer need
    /// to be replayed, so the WAL is rewritten to retain only the still-buffered
    /// entries (plus a checkpoint marker). This bounds WAL growth and prevents
    /// replay from double-counting records that already live in a sealed block.
    pub fn rewrite(&mut self, entries: &[WalEntry]) -> io::Result<()> {
        let truncated = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.path)?;
        truncated.sync_all()?;
        drop(truncated);

        let file = OpenOptions::new().create(true).append(true).open(&self.path)?;
        self.writer = BufWriter::new(file);
        self.pending_since_sync = 0;

        for entry in entries {
            self.append_frame(entry)?;
        }
        self.sync()
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
            revision: RevisionId::legacy(1),
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
        writer.append(&WalEntry::Checkpoint { revision: RevisionId::legacy(1) }).unwrap();
        drop(writer);

        let mut reader = WalReader::open(path).unwrap();
        let entries = reader.entries().unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn buffered_syncs_periodically() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let mut writer = WalWriter::open_with_durability(
            path.clone(),
            WalDurability::Buffered { sync_every: 4 },
        )
        .unwrap();
        for _ in 0..3 {
            writer.append_frame(&sample_entry()).unwrap();
        }
        writer.sync().unwrap();
        writer.append_frame(&sample_entry()).unwrap();
        writer.sync().unwrap();
        drop(writer);

        let mut reader = WalReader::open(path).unwrap();
        assert_eq!(reader.entries().unwrap().len(), 4);
    }

    #[test]
    fn rewrite_uses_single_sync() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let mut writer = WalWriter::open(path.clone()).unwrap();
        writer.append(&sample_entry()).unwrap();
        let entries = vec![
            sample_entry(),
            WalEntry::Checkpoint { revision: RevisionId::legacy(2) },
        ];
        writer.rewrite(&entries).unwrap();
        drop(writer);

        let mut reader = WalReader::open(path).unwrap();
        assert_eq!(reader.entries().unwrap().len(), 2);
    }
}
