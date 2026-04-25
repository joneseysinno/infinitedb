//! NVMe-aware block store.
//!
//! On NVMe, random reads are nearly as fast as sequential reads, so we
//! optimise for simplicity and low write amplification rather than trying
//! to cluster data sequentially.
//!
//! Layout inside the database directory:
//!   blocks/<block_id>.blk   — one file per sealed Block
//!   meta/spaces.bin         — serialised SpaceRegistry
//!   meta/snapshots.bin      — serialised active Snapshot headers
//!   wal.log                 — the WAL (managed by wal.rs)
//!
//! Each block file is a single bincode-serialised `Block` struct.
//! The checksum stored in `Block::checksum` is verified on every read.

use std::{
    collections::HashMap,
    fs,
    io::{self},
    path::PathBuf,
};
use bincode::{config::standard, decode_from_slice, encode_to_vec};
use blake3::Hasher;
use crate::infinitedb_core::block::{Block, BlockId};

// ---------------------------------------------------------------------------
// LRU block cache
// ---------------------------------------------------------------------------

/// A simple LRU cache for decoded `Block` values, bounded by total byte size.
///
/// Eviction order is tracked with a monotonic generation counter — the entry
/// with the lowest last-access generation is evicted when the cache is full.
/// O(n) eviction is acceptable because at 10 MB / ~40 KB per block the cache
/// holds at most ~250 entries.
pub struct LruBlockCache {
    /// Cached blocks together with their last-access generation.
    entries: HashMap<BlockId, (Block, u64)>,
    /// Monotonically increasing access counter.
    generation: u64,
    /// Current resident size in bytes (approximate: record bytes only).
    current_bytes: usize,
    /// Hard ceiling in bytes.
    max_bytes: usize,
}

impl LruBlockCache {
    /// Create a cache capped at `max_bytes` of resident block data.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            generation: 0,
            current_bytes: 0,
            max_bytes,
        }
    }

    /// Look up a block. Returns a clone and bumps its access generation.
    pub fn get(&mut self, id: BlockId) -> Option<Block> {
        let cur_gen = self.generation + 1;
        if let Some((block, last)) = self.entries.get_mut(&id) {
            self.generation = cur_gen;
            *last = cur_gen;
            Some(block.clone())
        } else {
            None
        }
    }

    /// Insert a block into the cache, evicting LRU entries as needed.
    pub fn insert(&mut self, block: Block) {
        let size = block_byte_size(&block);
        // If a single block is larger than the whole cache, skip caching.
        if size > self.max_bytes {
            return;
        }
        // If already cached, update in-place.
        if let Some((existing, last)) = self.entries.get_mut(&block.id) {
            self.current_bytes -= block_byte_size(existing);
            self.generation += 1;
            *last = self.generation;
            *existing = block;
            self.current_bytes += size;
            return;
        }
        // Evict until there is room.
        while self.current_bytes + size > self.max_bytes && !self.entries.is_empty() {
            // Find the entry with the lowest generation (LRU).
            let lru_id = self.entries
                .iter()
                .min_by_key(|(_, (_, g))| *g)
                .map(|(id, _)| *id)
                .unwrap();
            if let Some((evicted, _)) = self.entries.remove(&lru_id) {
                self.current_bytes -= block_byte_size(&evicted);
            }
        }
        self.generation += 1;
        self.current_bytes += size;
        self.entries.insert(block.id, (block, self.generation));
    }

    /// Invalidate a cached entry (called after delete_block).
    pub fn invalidate(&mut self, id: BlockId) {
        if let Some((evicted, _)) = self.entries.remove(&id) {
            self.current_bytes -= block_byte_size(&evicted);
        }
    }

    /// Current resident size in bytes.
    pub fn resident_bytes(&self) -> usize { self.current_bytes }

    /// Number of cached blocks.
    pub fn len(&self) -> usize { self.entries.len() }
}

/// Approximate byte size of a block for cache accounting.
fn block_byte_size(block: &Block) -> usize {
    // Fixed overhead + per-record estimate.
    64 + block.records.iter().map(|r| 48 + r.data.len()).sum::<usize>()
}

// ---------------------------------------------------------------------------
// BlockStore
// ---------------------------------------------------------------------------

/// Persistent, content-addressed store for sealed blocks.
/// Maintains an in-process LRU block cache to avoid redundant disk reads.
pub struct BlockStore {
    root: PathBuf,
    cache: LruBlockCache,
}


impl BlockStore {
    /// Open (or create) a block store rooted at `dir`.
    /// `cache_bytes` sets the LRU cache ceiling (default: 10 MB).
    pub fn open(dir: PathBuf) -> io::Result<Self> {
        Self::open_with_cache(dir, 10 * 1024 * 1024)
    }

    /// Open (or create) a block store with an explicit cache size.
    pub fn open_with_cache(dir: PathBuf, cache_bytes: usize) -> io::Result<Self> {
        let blocks_dir = dir.join("blocks");
        let meta_dir = dir.join("meta");
        fs::create_dir_all(&blocks_dir)?;
        fs::create_dir_all(&meta_dir)?;
        Ok(Self { root: dir, cache: LruBlockCache::new(cache_bytes) })
    }

    /// Write a sealed block to disk and insert it into the LRU cache.
    pub fn write_block(&mut self, block: &Block) -> io::Result<()> {
        let path = self.block_path(block.id);
        let payload = encode_to_vec(block, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        // Atomic write: write to .tmp, then rename.
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, &payload)?;
        fs::rename(&tmp, &path)?;
        // Populate the cache with the freshly written block.
        self.cache.insert((*block).clone());
        Ok(())
    }

    /// Read a block by ID. Served from the LRU cache when available;
    /// falls back to disk and populates the cache on a miss.
    pub fn read_block(&mut self, id: BlockId) -> io::Result<Block> {
        if let Some(cached) = self.cache.get(id) {
            return Ok(cached);
        }
        let path = self.block_path(id);
        let bytes = fs::read(&path)?;
        let (block, _): (Block, _) = decode_from_slice(&bytes, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        verify_checksum(&block)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.cache.insert(block.clone());
        Ok(block)
    }

    /// Delete a block file (called by GC after compaction).
    pub fn delete_block(&mut self, id: BlockId) -> io::Result<()> {
        let path = self.block_path(id);
        self.cache.invalidate(id);
        fs::remove_file(&path)
    }

    /// Cache statistics: (resident_bytes, cached_block_count).
    pub fn cache_stats(&self) -> (usize, usize) {
        (self.cache.resident_bytes(), self.cache.len())
    }

    /// Return true if a block file exists on disk.
    pub fn exists(&self, id: BlockId) -> bool {
        self.block_path(id).exists()
    }

    /// List all block IDs currently on disk.
    pub fn list_blocks(&self) -> io::Result<Vec<BlockId>> {
        let dir = self.root.join("blocks");
        let mut ids = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let s = name.to_string_lossy();
            if let Some(stem) = s.strip_suffix(".blk") {
                if let Ok(n) = stem.parse::<u64>() {
                    ids.push(BlockId(n));
                }
            }
        }
        Ok(ids)
    }

    /// Write arbitrary metadata bytes under `meta/<name>`.
    pub fn write_meta(&self, name: &str, data: &[u8]) -> io::Result<()> {
        let path = self.root.join("meta").join(name);
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, data)?;
        fs::rename(&tmp, &path)
    }

    /// Read metadata bytes from `meta/<name>`.
    pub fn read_meta(&self, name: &str) -> io::Result<Vec<u8>> {
        fs::read(self.root.join("meta").join(name))
    }

    /// Return the path to the write-ahead log file.
    pub fn wal_path(&self) -> PathBuf {
        self.root.join("wal.log")
    }

    fn block_path(&self, id: BlockId) -> PathBuf {
        self.root.join("blocks").join(format!("{}.blk", id.0))
    }
}

// ---------------------------------------------------------------------------
// Checksum
// ---------------------------------------------------------------------------

/// Compute the expected checksum for a block's records.
pub fn compute_checksum(block: &Block) -> io::Result<[u8; 32]> {
    let payload = encode_to_vec(&block.records, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let mut h = Hasher::new();
    h.update(&payload);
    Ok(*h.finalize().as_bytes())
}

fn verify_checksum(block: &Block) -> Result<(), String> {
    let expected = compute_checksum(block)
        .map_err(|e| e.to_string())?;
    if block.checksum != expected {
        Err(format!("Block {:?} checksum mismatch", block.id))
    } else {
        Ok(())
    }
}
