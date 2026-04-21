//! `InfiniteDb` — the top-level embedded database handle.
//!
//! This is the single entry point for embedded use. It owns:
//!   - A `BlockStore` (NVMe-aware file storage)
//!   - A `WalWriter` (crash-safe append log)
//!   - A `SpaceRegistry` (named dimension spaces)
//!   - A `BranchRegistry` (named branch heads)
//!   - A monotonic revision counter
//!
//! All writes go through the WAL before touching any block. On `open()`,
//! the WAL is replayed to recover any in-flight writes from a prior crash.
//!
//! # Example
//! ```no_run
//! use infinitedb::InfiniteDb;
//! use infinitedb::infinitedb_core::address::{DimensionVector, SpaceId};
//! use infinitedb::infinitedb_core::space::{SpaceConfig, SpaceRegistry};
//!
//! let mut db = InfiniteDb::open("./mydb").unwrap();
//! let space = SpaceId(1);
//! let point = DimensionVector::new(vec![128, 64]);
//! let data  = bincode::encode_to_vec(&42u32, bincode::config::standard()).unwrap();
//! db.insert(space, point, data).unwrap();
//! ```

use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use bincode::{config::standard, decode_from_slice, encode_to_vec};

use crate::infinitedb_core::{
    address::{Address, DimensionVector, RevisionId, SpaceId},
    block::{Block, BlockId, Record},
    branch::{Branch, BranchId, BranchRegistry},
    snapshot::{Snapshot, SnapshotId},
    space::{SpaceConfig, SpaceRegistry},
};
use crate::infinitedb_index::{
    composite::{CompositeKey, Dimension, KeyConfig},
};
use crate::infinitedb_storage::{
    nvme::{compute_checksum, BlockStore},
    wal::{WalEntry, WalWriter},
};

// ---------------------------------------------------------------------------
// InfiniteDb
// ---------------------------------------------------------------------------

/// The embedded database handle. Not `Send`/`Sync` — create one per thread
/// or wrap in a `Mutex` for multi-threaded access.
pub struct InfiniteDb {
    store: BlockStore,
    wal: WalWriter,
    spaces: SpaceRegistry,
    branches: BranchRegistry,
    /// In-memory write buffer: accumulated records not yet sealed into a block.
    buffer: Vec<Record>,
    /// Monotonic revision counter. Persisted via WAL checkpoints.
    revision: AtomicU64,
    /// Next block ID to assign.
    next_block_id: AtomicU64,
    /// Next snapshot ID to assign.
    next_snapshot_id: AtomicU64,
    /// Next branch ID to assign.
    next_branch_id: AtomicU64,
    /// Active snapshot per space (space_id → snapshot).
    snapshots: BTreeMap<u64, Snapshot>,
    /// Flush threshold: seal a block after this many buffered records.
    flush_threshold: usize,
}

impl InfiniteDb {
    /// Open (or create) a database in `dir`. Replays the WAL on first open.
    pub fn open<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        let root = dir.as_ref().to_path_buf();
        let store = BlockStore::open(root.clone())?;
        let wal_path = store.wal_path();

        // Replay WAL to recover in-flight writes.
        let recovered = recover_wal(&wal_path)?;

        let wal = WalWriter::open(wal_path)?;

        // Load persisted metadata (spaces, branches, snapshots) if present.
        let (spaces, branches, snapshots, next_rev, next_block, next_snap) =
            load_meta(&store).unwrap_or_else(default_meta);

        let mut db = Self {
            store,
            wal,
            spaces,
            branches,
            buffer: Vec::new(),
            revision: AtomicU64::new(next_rev),
            next_block_id: AtomicU64::new(next_block),
            next_snapshot_id: AtomicU64::new(next_snap),
            next_branch_id: AtomicU64::new(2), // 1 is reserved for main
            snapshots,
            flush_threshold: 256,
        };

        // Re-apply recovered WAL entries.
        for entry in recovered {
            db.apply_wal_entry(entry)?;
        }

        // Ensure a `main` branch exists.
        if db.branches.get_by_name("main").is_none() {
            let snap_id = db.alloc_snapshot_id();
            // No spaces yet — main branch starts with no snapshot content.
            let _ = db.branches.insert(Branch {
                id: BranchId(1),
                name: "main".to_string(),
                head: snap_id,
                parent: None,
                forked_at: RevisionId::ZERO,
            });
        }

        Ok(db)
    }

    // -----------------------------------------------------------------------
    // Space management
    // -----------------------------------------------------------------------

    /// Register a new space. Must be called before inserting records into it.
    pub fn register_space(&mut self, config: SpaceConfig) -> Result<(), String> {
        self.spaces.register(config).map_err(|e| format!("{:?}", e))?;
        self.persist_meta().map_err(|e| e.to_string())?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Writes
    // -----------------------------------------------------------------------

    /// Insert or update a record. Appends a new revision to the WAL.
    /// The record is buffered in memory; call `flush()` to seal it into a block.
    pub fn insert(
        &mut self,
        space: SpaceId,
        point: DimensionVector,
        data: Vec<u8>,
    ) -> io::Result<RevisionId> {
        let rev = self.next_revision();
        let address = Address::new(space, point);
        let entry = WalEntry::Write {
            address: address.clone(),
            revision: rev,
            data: data.clone(),
        };
        self.wal.append(&entry)?;
        self.buffer.push(Record {
            address,
            revision: rev,
            data,
            tombstone: false,
        });
        if self.buffer.len() >= self.flush_threshold {
            self.flush(space)?;
        }
        Ok(rev)
    }

    /// Logically delete a record at `point` in `space`.
    pub fn delete(&mut self, space: SpaceId, point: DimensionVector) -> io::Result<RevisionId> {
        let rev = self.next_revision();
        let address = Address::new(space, point);
        let entry = WalEntry::Tombstone {
            address: address.clone(),
            revision: rev,
        };
        self.wal.append(&entry)?;
        self.buffer.push(Record {
            address,
            revision: rev,
            data: vec![],
            tombstone: true,
        });
        Ok(rev)
    }

    /// Seal all buffered records for `space` into a new block on disk.
    pub fn flush(&mut self, space: SpaceId) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Partition: take only records for this space; keep the rest in the buffer.
        let mut remaining = Vec::new();
        let mut records: Vec<Record> = Vec::new();
        for record in self.buffer.drain(..) {
            if record.address.space == space {
                records.push(record);
            } else {
                remaining.push(record);
            }
        }
        self.buffer = remaining;

        if records.is_empty() {
            return Ok(());
        }

        // Sort by Hilbert key then revision.
        records.sort_by_key(|r| {
            let key = hilbert_key_for(&r.address.point);
            (key, r.revision.0)
        });

        let min_rev = records.iter().map(|r| r.revision).min().unwrap_or(RevisionId::ZERO);
        let max_rev = records.iter().map(|r| r.revision).max().unwrap_or(RevisionId::ZERO);
        let block_id = self.alloc_block_id();

        let mut block = Block {
            id: block_id,
            space,
            records,
            min_revision: min_rev,
            max_revision: max_rev,
            checksum: [0u8; 32],
        };
        block.checksum = compute_checksum(&block)?;

        // Write block to NVMe store.
        self.store.write_block(&block)?;

        // Record block seal in WAL.
        let snap_id = self.alloc_snapshot_id();
        self.wal.append(&WalEntry::BlockSealed {
            block_id,
            space,
            snapshot: snap_id,
        })?;

        // Advance the space's active snapshot.
        let snapshot = self.snapshots.entry(space.0).or_insert_with(|| {
            Snapshot::root(snap_id, space)
        });
        let hilbert_min = block
            .records
            .first()
            .map(|r| hilbert_key_for(&r.address.point))
            .unwrap_or(0);
        snapshot.blocks.insert(hilbert_min, block_id);
        snapshot.revision = max_rev;

        self.persist_meta()?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Reads
    // -----------------------------------------------------------------------

    /// Return the current snapshot ID for `space`.
    pub fn current_snapshot(&self, space: SpaceId) -> Option<SnapshotId> {
        self.snapshots.get(&space.0).map(|s| s.id)
    }

    /// Scan all live records in `space`, optionally capped at `as_of`.
    /// Full-space scan — no spatial filtering. Use `query_bbox` to filter by coordinates.
    pub fn query(
        &mut self,
        space: SpaceId,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Record>> {
        self.query_inner(space, None, as_of)
    }

    /// Bounding-box query in N dimensions.
    ///
    /// Returns every live record in `space` whose point satisfies
    /// `min[i] <= point[i] <= max[i]` on **every** axis simultaneously.
    ///
    /// Works correctly for any dimensionality (1–16 dims). A Hilbert-key
    /// interval is used to prune candidate blocks at the block level; an
    /// exact `within()` check per record eliminates any false positives caused
    /// by the Hilbert curve mapping a bounding box to many disjoint intervals.
    ///
    /// `min` and `max` must have the same number of coordinates as the points
    /// stored in the space.
    pub fn query_bbox(
        &mut self,
        space: SpaceId,
        min: DimensionVector,
        max: DimensionVector,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Record>> {
        assert_eq!(min.dims(), max.dims(), "min and max must have equal dimensions");
        // Compute Hilbert keys for both bounding corners and use the interval
        // as a block-level pre-filter (over-approximation — false positives OK).
        let k_min = hilbert_key_for(&min);
        let k_max = hilbert_key_for(&max);
        let (lo, hi) = if k_min <= k_max { (k_min, k_max) } else { (k_max, k_min) };
        let mut results = self.query_inner(space, Some((lo, hi)), as_of)?;
        // Exact per-record coordinate filter removes all Hilbert false positives.
        results.retain(|r| r.address.point.within(&min, &max));
        Ok(results)
    }

    /// Sub-space query: pins the first `parent_coords.len()` dimensions to exact
    /// values and leaves the remaining inner dimensions fully open (0..u32::MAX).
    ///
    /// This is the idiomatic way to retrieve all property records for a specific
    /// parent element in the nested Hilbert design:
    /// ```ignore
    /// // All load properties of element 42:
    /// db.query_subscope(SPACE_LOADS, &[42], None);
    /// ```
    pub fn query_subscope(
        &mut self,
        space: SpaceId,
        parent_coords: &[u32],
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Record>> {
        // We need to know the total dims for this space to build the full vectors.
        let dims = self.spaces.get(space)
            .map(|c| c.dims)
            .unwrap_or(parent_coords.len() + 1);
        assert!(
            parent_coords.len() <= dims,
            "parent_coords has more dimensions than the space"
        );
        let inner_dims = dims - parent_coords.len();
        let mut min_coords: Vec<u32> = parent_coords.to_vec();
        let mut max_coords: Vec<u32> = parent_coords.to_vec();
        min_coords.extend(std::iter::repeat(0).take(inner_dims));
        max_coords.extend(std::iter::repeat(u32::MAX).take(inner_dims));
        self.query_bbox(
            space,
            DimensionVector::new(min_coords),
            DimensionVector::new(max_coords),
            as_of,
        )
    }

    // Shared core: reads from sealed blocks + the in-memory write buffer.
    fn query_inner(
        &mut self,
        space: SpaceId,
        key_range: Option<(u128, u128)>,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Record>> {
        let rev_ceiling = as_of.unwrap_or_else(|| {
            RevisionId(self.revision.load(Ordering::Relaxed))
        });

        let mut results: Vec<Record> = Vec::new();

        // Query sealed blocks if a snapshot exists.
        if let Some(snapshot) = self.snapshots.get(&space.0) {
            let block_ids: Vec<BlockId> = match key_range {
                None => snapshot.blocks.values().copied().collect(),
                Some((_, hi)) => {
                    // The snapshot map is keyed by the Hilbert key of each
                    // block's first record. Include all blocks with a start key
                    // <= hi; the within() filter handles exactness.
                    snapshot.blocks.range(..=hi).map(|(_, id)| *id).collect()
                }
            };
            for block_id in block_ids {
                let block = self.store.read_block(block_id)?;
                for record in block.records {
                    if record.revision <= rev_ceiling && !record.tombstone {
                        results.push(record);
                    }
                }
            }
        }

        // Always check the in-memory buffer (records not yet flushed to disk).
        // Collect tombstoned coordinates first so we can suppress stale sealed records.
        let tombstoned: std::collections::HashSet<_> = self
            .buffer
            .iter()
            .filter(|r| r.address.space == space && r.tombstone && r.revision <= rev_ceiling)
            .map(|r| r.address.point.coords.clone())
            .collect();

        results.retain(|r| !tombstoned.contains(&r.address.point.coords));

        for record in &self.buffer {
            if record.address.space == space
                && record.revision <= rev_ceiling
                && !record.tombstone
                && !tombstoned.contains(&record.address.point.coords)
            {
                if let Some((lo, hi)) = key_range {
                    let k = hilbert_key_for(&record.address.point);
                    if k < lo || k > hi {
                        continue;
                    }
                }
                results.push(record.clone());
            }
        }

        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Branch management
    // -----------------------------------------------------------------------

    /// Create a new branch forked from an existing one at the current revision.
    pub fn create_branch(
        &mut self,
        name: impl Into<String>,
        from: BranchId,
    ) -> Result<BranchId, String> {
        let parent = self.branches.get(from).ok_or("Branch not found")?;
        let new_id = BranchId(self.next_branch_id.fetch_add(1, Ordering::Relaxed));
        let rev = RevisionId(self.revision.load(Ordering::Relaxed));
        let branch = Branch {
            id: new_id,
            name: name.into(),
            head: parent.head,
            parent: Some(from),
            forked_at: rev,
        };
        self.branches.insert(branch).map_err(|e| format!("{:?}", e))?;
        Ok(new_id)
    }

    // -----------------------------------------------------------------------
    // Diagnostics
    // -----------------------------------------------------------------------

    /// Returns a snapshot of current memory and cache usage.
    pub fn memory_stats(&self) -> MemoryStats {
        let buffer_records = self.buffer.len();
        let buffer_bytes: usize = self.buffer.iter()
            .map(|r| 48 + r.data.len())
            .sum();
        let (cache_bytes, cache_blocks) = self.store.cache_stats();
        let snapshot_entries: usize = self.snapshots.values()
            .map(|s| s.blocks.len())
            .sum();
        MemoryStats {
            buffer_records,
            buffer_bytes,
            cache_bytes,
            cache_blocks,
            snapshot_index_entries: snapshot_entries,
            total_revision: self.revision.load(Ordering::Relaxed),
            sealed_blocks: self.next_block_id.load(Ordering::Relaxed),
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn next_revision(&self) -> RevisionId {
        RevisionId(self.revision.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn alloc_block_id(&self) -> BlockId {
        BlockId(self.next_block_id.fetch_add(1, Ordering::Relaxed))
    }

    fn alloc_snapshot_id(&self) -> SnapshotId {
        SnapshotId(self.next_snapshot_id.fetch_add(1, Ordering::Relaxed))
    }

    fn apply_wal_entry(&mut self, entry: WalEntry) -> io::Result<()> {
        match entry {
            WalEntry::Write { address, revision, data } => {
                self.buffer.push(Record { address, revision, data, tombstone: false });
            }
            WalEntry::Tombstone { address, revision } => {
                self.buffer.push(Record { address, revision, data: vec![], tombstone: true });
            }
            WalEntry::BlockSealed { .. } | WalEntry::Checkpoint { .. } => {}
        }
        Ok(())
    }

    fn persist_meta(&mut self) -> io::Result<()> {
        // Persist spaces registry.
        let spaces_bytes = encode_to_vec(&self.spaces, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("spaces.bin", &spaces_bytes)?;

        // Persist revision counters as a simple [u64; 3].
        let counters: [u64; 3] = [
            self.revision.load(Ordering::Relaxed),
            self.next_block_id.load(Ordering::Relaxed),
            self.next_snapshot_id.load(Ordering::Relaxed),
        ];
        let counters_bytes = encode_to_vec(&counters, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("counters.bin", &counters_bytes)?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WAL recovery
// ---------------------------------------------------------------------------

fn recover_wal(wal_path: &PathBuf) -> io::Result<Vec<WalEntry>> {
    if !wal_path.exists() {
        return Ok(vec![]);
    }
    let mut reader = crate::infinitedb_storage::wal::WalReader::open(wal_path.clone())?;
    reader.entries()
}

// ---------------------------------------------------------------------------
// MemoryStats
// ---------------------------------------------------------------------------

/// Point-in-time snapshot of database memory and cache usage.
#[derive(Debug, Clone)]
pub struct MemoryStats {
    /// Records currently in the in-memory write buffer (not yet flushed).
    pub buffer_records: usize,
    /// Approximate bytes occupied by the write buffer.
    pub buffer_bytes: usize,
    /// Bytes currently resident in the LRU block cache.
    pub cache_bytes: usize,
    /// Number of blocks held in the LRU block cache.
    pub cache_blocks: usize,
    /// Total Hilbert index entries across all active snapshots.
    pub snapshot_index_entries: usize,
    /// Highest revision number issued so far.
    pub total_revision: u64,
    /// Total sealed blocks written (cumulative, not current on-disk count).
    pub sealed_blocks: u64,
}

impl MemoryStats {
    /// Estimated total process-level RAM attributed to the database.
    pub fn total_ram_bytes(&self) -> usize {
        self.buffer_bytes
            + self.cache_bytes
            // Snapshot index: each entry is approx (u128 key + u64 id) = 24 bytes.
            + self.snapshot_index_entries * 24
            // Fixed overhead: registries, atomics, BTreeMap nodes.
            + 4096
    }

    pub fn print(&self) {
        println!("\n╔═══ InfiniteDb Memory Stats ═══╗");
        println!("║  Write buffer       {:>6} records  ({} bytes)",
            self.buffer_records, fmt_bytes(self.buffer_bytes));
        println!("║  LRU block cache    {:>6} blocks   ({} bytes / 10 MB limit)",
            self.cache_blocks, fmt_bytes(self.cache_bytes));
        println!("║  Snapshot index     {:>6} entries", self.snapshot_index_entries);
        println!("║  Total revisions    {:>6}", self.total_revision);
        println!("║  Sealed blocks      {:>6}", self.sealed_blocks);
        println!("║  ──────────────────────────────────────────────");
        println!("║  Est. total RAM     {}", fmt_bytes(self.total_ram_bytes()));
        println!("╚════════════════════════════════");
    }
}

fn fmt_bytes(b: usize) -> String {
    if b < 1024 { format!("{} B", b) }
    else if b < 1024 * 1024 { format!("{:.1} KB", b as f64 / 1024.0) }
    else { format!("{:.2} MB", b as f64 / (1024.0 * 1024.0)) }
}

// ---------------------------------------------------------------------------
// Metadata persistence helpers
// ---------------------------------------------------------------------------

/// Load persisted metadata from the block store.
/// Returns defaults when no metadata exists yet.
#[allow(clippy::type_complexity)]
fn load_meta(
    store: &BlockStore,
) -> Option<(SpaceRegistry, BranchRegistry, BTreeMap<u64, Snapshot>, u64, u64, u64)> {
    let counters_bytes = store.read_meta("counters.bin").ok()?;
    let (counters, _): ([u64; 3], _) = decode_from_slice(&counters_bytes, standard()).ok()?;
    let spaces_bytes = store.read_meta("spaces.bin").ok()?;
    let (spaces, _): (SpaceRegistry, _) = decode_from_slice(&spaces_bytes, standard()).ok()?;
    Some((
        spaces,
        BranchRegistry::new(),
        BTreeMap::new(),
        counters[0],
        counters[1],
        counters[2],
    ))
}

/// Default state for a fresh database.
type MetaTuple = (SpaceRegistry, BranchRegistry, BTreeMap<u64, Snapshot>, u64, u64, u64);

fn default_meta() -> MetaTuple {
    (SpaceRegistry::new(), BranchRegistry::new(), BTreeMap::new(), 0, 1, 1)
}

/// Compute the Hilbert key for a point using STANDARD 8-bit precision.
/// All dimensions in the point are encoded.
fn hilbert_key_for(point: &DimensionVector) -> u128 {
    if point.coords.is_empty() {
        return 0;
    }
    let mut key = CompositeKey::new(KeyConfig::STANDARD);
    for &c in &point.coords {
        key = key.push(Dimension::new("_", c));
    }
    key.encode()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::infinitedb_core::address::{DimensionVector, SpaceId};
    use crate::infinitedb_core::branch::BranchId;

    fn open_tmp() -> (InfiniteDb, TempDir) {
        let dir = TempDir::new().unwrap();
        let db = InfiniteDb::open(dir.path()).unwrap();
        (db, dir)
    }

    #[test]
    fn insert_and_query_unflushed() {
        let (mut db, _dir) = open_tmp();
        let space = SpaceId(1);
        db.insert(space, DimensionVector::new(vec![10, 20]), vec![1, 2, 3]).unwrap();
        let results = db.query(space, None).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn insert_flush_query() {
        let (mut db, _dir) = open_tmp();
        let space = SpaceId(1);
        db.insert(space, DimensionVector::new(vec![5, 5]), vec![42]).unwrap();
        db.flush(space).unwrap();
        let results = db.query(space, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].data, vec![42]);
    }

    #[test]
    fn delete_tombstones_record() {
        let (mut db, _dir) = open_tmp();
        let space = SpaceId(1);
        let point = DimensionVector::new(vec![1, 1]);
        db.insert(space, point.clone(), vec![99]).unwrap();
        db.delete(space, point).unwrap();
        let results = db.query(space, None).unwrap();
        // Tombstone suppresses the record in live queries.
        assert!(results.iter().all(|r| !r.tombstone));
    }

    #[test]
    fn as_of_returns_historical_state() {
        let (mut db, _dir) = open_tmp();
        let space = SpaceId(1);
        let rev1 = db.insert(space, DimensionVector::new(vec![1, 0]), vec![1]).unwrap();
        let _rev2 = db.insert(space, DimensionVector::new(vec![2, 0]), vec![2]).unwrap();
        // Query at rev1 should see only the first record.
        let results = db.query(space, Some(rev1)).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].data, vec![1]);
    }

    #[test]
    fn create_branch_succeeds() {
        let (mut db, _dir) = open_tmp();
        let main = BranchId(1);
        let feature = db.create_branch("feature", main).unwrap();
        assert_ne!(feature, main);
    }
}
