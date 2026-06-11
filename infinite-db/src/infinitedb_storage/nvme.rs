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
//!   wal.log                 — the WAL (managed by wal.rs, v1 layout)
//!
//! Each block file is a single bincode-serialised `Block` struct.
//! The checksum stored in `Block::checksum` is verified on every read.

use std::{
    collections::HashMap,
    fs,
    io::{self},
    path::PathBuf,
    sync::Arc,
};

use bincode::{config::standard, decode_from_slice, encode_to_vec};
use blake3::Hasher;
use parking_lot::RwLock;

use crate::infinitedb_core::{
    block::{Block, BlockId},
    checksum::Checksum,
};

// ---------------------------------------------------------------------------
// LRU block cache
// ---------------------------------------------------------------------------

/// A simple LRU cache for decoded `Block` values, bounded by total byte size.
pub struct LruBlockCache {
    entries: HashMap<BlockId, (Arc<Block>, u64)>,
    generation: u64,
    current_bytes: usize,
    max_bytes: usize,
}

impl LruBlockCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            generation: 0,
            current_bytes: 0,
            max_bytes,
        }
    }

    pub fn get(&mut self, id: BlockId) -> Option<Arc<Block>> {
        let cur_gen = self.generation + 1;
        if let Some((block, last)) = self.entries.get_mut(&id) {
            self.generation = cur_gen;
            *last = cur_gen;
            Some(Arc::clone(block))
        } else {
            None
        }
    }

    pub fn insert(&mut self, block: Arc<Block>) {
        let size = block_byte_size(&block);
        if size > self.max_bytes {
            return;
        }
        if let Some((existing, last)) = self.entries.get_mut(&block.id) {
            self.current_bytes -= block_byte_size(existing);
            self.generation += 1;
            *last = self.generation;
            *existing = block;
            self.current_bytes += size;
            return;
        }
        while self.current_bytes + size > self.max_bytes && !self.entries.is_empty() {
            let lru_id = self
                .entries
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

    pub fn invalidate(&mut self, id: BlockId) {
        if let Some((evicted, _)) = self.entries.remove(&id) {
            self.current_bytes -= block_byte_size(&evicted);
        }
    }

    pub fn resident_bytes(&self) -> usize {
        self.current_bytes
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

fn block_byte_size(block: &Block) -> usize {
    64 + block.records.iter().map(|r| 48 + r.data.len()).sum::<usize>()
}

// ---------------------------------------------------------------------------
// BlockStore
// ---------------------------------------------------------------------------

/// Persistent store for sealed blocks with a concurrent LRU cache.
pub struct BlockStore {
    root: PathBuf,
    cache: RwLock<LruBlockCache>,
}

impl BlockStore {
    pub fn open(dir: PathBuf) -> io::Result<Self> {
        Self::open_with_cache(dir, 10 * 1024 * 1024)
    }

    pub fn open_with_cache(dir: PathBuf, cache_bytes: usize) -> io::Result<Self> {
        let blocks_dir = dir.join("blocks");
        let meta_dir = dir.join("meta");
        fs::create_dir_all(&blocks_dir)?;
        fs::create_dir_all(&meta_dir)?;
        Ok(Self {
            root: dir,
            cache: RwLock::new(LruBlockCache::new(cache_bytes)),
        })
    }

    pub fn write_block(&self, block: &Block) -> io::Result<()> {
        let path = self.block_path(block.id);
        let payload = encode_to_vec(block, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, &payload)?;
        fs::rename(&tmp, &path)?;
        self.cache
            .write()
            .insert(Arc::new(block.clone()));
        Ok(())
    }

    /// Shared read path for concurrent readers (Phase A).
    pub fn read_block_shared(&self, id: BlockId) -> io::Result<Arc<Block>> {
        if let Some(cached) = self.cache.write().get(id) {
            return Ok(cached);
        }
        let path = self.block_path(id);
        let bytes = fs::read(&path)?;
        let (block, _): (Block, _) = decode_from_slice(&bytes, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        verify_checksum(&block)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let arc = Arc::new(block);
        self.cache.write().insert(Arc::clone(&arc));
        Ok(arc)
    }

    /// Owned read of a block (clones the shared cache entry).
    pub fn read_block(&self, id: BlockId) -> io::Result<Block> {
        Ok((*self.read_block_shared(id)?).clone())
    }

    pub fn delete_block(&self, id: BlockId) -> io::Result<()> {
        let path = self.block_path(id);
        self.cache.write().invalidate(id);
        fs::remove_file(&path)
    }

    pub fn cache_stats(&self) -> (usize, usize) {
        let cache = self.cache.read();
        (cache.resident_bytes(), cache.len())
    }

    pub fn exists(&self, id: BlockId) -> io::Result<bool> {
        Ok(self.block_path(id).exists())
    }

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

    pub fn write_meta(&self, name: &str, data: &[u8]) -> io::Result<()> {
        let path = self.root.join("meta").join(name);
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, data)?;
        fs::rename(&tmp, &path)
    }

    pub fn read_meta(&self, name: &str) -> io::Result<Vec<u8>> {
        fs::read(self.root.join("meta").join(name))
    }

    pub fn root(&self) -> &PathBuf {
        &self.root
    }

    pub fn wal_path(&self) -> PathBuf {
        self.root.join("wal.log")
    }

    pub fn staging_wal_path(&self) -> PathBuf {
        self.root.join("wal").join("staging.log")
    }

    fn block_path(&self, id: BlockId) -> PathBuf {
        self.root.join("blocks").join(format!("{}.blk", id.0))
    }
}

pub fn compute_checksum(block: &Block) -> io::Result<Checksum> {
    let payload = encode_to_vec(&block.records, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let mut h = Hasher::new();
    h.update(&payload);
    Ok(Checksum(*h.finalize().as_bytes()))
}

fn verify_checksum(block: &Block) -> Result<(), String> {
    let expected = compute_checksum(block).map_err(|e| e.to_string())?;
    if block.checksum != expected {
        Err(format!("Block {:?} checksum mismatch", block.id))
    } else {
        Ok(())
    }
}
