//! `LegacyDb` — the top-level embedded database handle.
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
//! ```ignore
//! use infinite_db::legacy_v1::LegacyDb;
//! use infinite_db::infinitedb_core::address::{DimensionVector, SpaceId};
//!
//! let mut db = LegacyDb::open("./mydb").unwrap();
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
#[cfg(feature = "sync")]
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use bincode::{config::standard, decode_from_slice, encode_to_vec};

use crate::infinitedb_core::{
    adapter::{AdapterEndpoint, KindLabel, SpaceBinding},
    address::{Address, DimensionVector, RevisionId, SpaceId},
    block::{Block, BlockId, Record},
    branch::{Branch, BranchId, BranchRegistry},
    endpoint_index::{
        decode_hyperedge_id, endpoint_index_point, endpoint_lookup_prefix,
        edge_endpoints, ENDPOINT_INDEX_BITS_PER_DIM, ENDPOINT_INDEX_DIMS, ENDPOINT_INDEX_SPACE,
    },
    hyperedge::{EndpointRef, Hyperedge, HyperedgeId},
    traversal::{Subgraph, TraversalSpec},
    kind_catalog::KindCatalog,
    query::Query,
    signal::SignalSample,
    snapshot::{BlockIndexEntry, Snapshot, SnapshotId},
    space::{SpaceConfig, SpaceRegistry},
};
use crate::infinitedb_index::composite::KeyConfig;
use crate::infinitedb_index::key::{hilbert_key_for, hilbert_key_standard};
use crate::infinitedb_storage::{
    compaction::{compact, CompactionConfig, CompactionResult},
    gc::safe_to_delete,
    nvme::{compute_checksum, BlockStore},
    wal::{WalDurability, WalEntry, WalWriter},
};

#[path = "bulk/mod.rs"]
mod bulk;
#[allow(unused_imports)]
pub use bulk::{
    BulkHyperedgeImport, BulkHyperedgeImportOptions, BulkImportResult, BulkRecordImport,
    BulkSignalImport, BulkWriteOptions, BulkWriteResult,
};
#[cfg(feature = "sync")]
use crate::infinitedb_sync::{delta::Delta, merkle};
#[cfg(feature = "sync")]
use crate::infinitedb_sync::{
    outbox::{load_outbox, save_outbox, OutboxState, SyncReport},
    transport::{SyncOperation, SyncTransport},
    worker::BackgroundSyncWorker,
};

// ---------------------------------------------------------------------------
// Open options
// ---------------------------------------------------------------------------

/// Options for opening an embedded database.
#[derive(Debug, Clone)]
pub struct LegacyOpenOptions {
    /// WAL fsync policy. Default: [`WalDurability::Strict`].
    pub wal_durability: WalDurability,
    /// In-memory records before auto-sealing a block. Default: 256.
    pub flush_threshold: usize,
    /// LRU cache size for decoded blocks. Default: 10 MiB.
    pub block_cache_bytes: usize,
}

impl Default for LegacyOpenOptions {
    fn default() -> Self {
        Self {
            wal_durability: WalDurability::Strict,
            flush_threshold: 256,
            block_cache_bytes: 10 * 1024 * 1024,
        }
    }
}

impl LegacyOpenOptions {
    /// Open (or create) a database directory with these options.
    pub fn open<P: AsRef<Path>>(&self, dir: P) -> io::Result<LegacyDb> {
        LegacyDb::open_with_options(dir, self)
    }
}

// ---------------------------------------------------------------------------
// LegacyDb
// ---------------------------------------------------------------------------

/// The embedded database handle. Not `Send`/`Sync` — create one per thread
/// or wrap in a `Mutex` for multi-threaded access.
pub struct LegacyDb {
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
    /// When true, auto-flush on buffer size is suppressed (bulk import).
    defer_auto_flush: bool,
    /// True while a bulk write guard holds an active session.
    bulk_session_active: bool,
    #[cfg(feature = "sync")]
    outbox_path: PathBuf,
    #[cfg(feature = "sync")]
    outbox_state: Arc<Mutex<OutboxState>>,
    #[cfg(feature = "sync")]
    sync_worker: Option<BackgroundSyncWorker>,
}

impl LegacyDb {
    /// Open (or create) a database in `dir`. Replays the WAL on first open.
    pub fn open<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        LegacyOpenOptions::default().open(dir)
    }

    /// Open with explicit tuning (cache size, flush threshold, WAL policy).
    pub fn open_with_options<P: AsRef<Path>>(dir: P, options: &LegacyOpenOptions) -> io::Result<Self> {
        let root = dir.as_ref().to_path_buf();
        let store = BlockStore::open_with_cache(root.clone(), options.block_cache_bytes)?;
        let wal_path = store.wal_path();
        #[cfg(feature = "sync")]
        let outbox_path = root.join("meta").join("sync_outbox.bin");

        // Replay WAL to recover in-flight writes.
        let recovered = recover_wal(&wal_path)?;

        let wal = WalWriter::open_with_durability(wal_path, options.wal_durability)?;

        // Load persisted metadata (spaces, branches, snapshots) if present.
        let (spaces, branches, snapshots, next_rev, next_block, next_snap, next_branch) =
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
            next_branch_id: AtomicU64::new(next_branch), // 1 is reserved for main
            snapshots,
            flush_threshold: options.flush_threshold,
            defer_auto_flush: false,
            bulk_session_active: false,
            #[cfg(feature = "sync")]
            outbox_state: Arc::new(Mutex::new(load_outbox(&outbox_path)?)),
            #[cfg(feature = "sync")]
            outbox_path,
            #[cfg(feature = "sync")]
            sync_worker: None,
        };

        // Re-apply recovered WAL entries, advancing the revision counter to
        // cover any replayed write so live queries don't filter them out.
        let mut max_rev = db.revision.load(Ordering::Relaxed);
        for entry in recovered {
            if let WalEntry::Write { revision, .. } | WalEntry::Tombstone { revision, .. } = &entry {
                max_rev = max_rev.max(revision.0);
            }
            db.apply_wal_entry(entry)?;
        }
        db.revision.store(max_rev, Ordering::Relaxed);

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
        if config.bits_per_dim == 0 {
            return Err("bits_per_dim must be at least 1".to_string());
        }
        if config.dims as u32 * config.bits_per_dim > 128 {
            return Err(format!(
                "dims * bits_per_dim must be <= 128 (got {} * {})",
                config.dims, config.bits_per_dim
            ));
        }
        self.spaces.register(config).map_err(|e| format!("{:?}", e))?;
        self.persist_meta().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Hilbert key for a point using the precision configured for its space.
    /// Falls back to standard 8-bit precision for unregistered/reserved spaces.
    fn space_key(&self, space: SpaceId, point: &DimensionVector) -> u128 {
        match self.spaces.get(space) {
            Some(config) => hilbert_key_for(point, KeyConfig { bits_per_dim: config.bits_per_dim }),
            None => hilbert_key_standard(point),
        }
    }

    /// Register the reserved endpoint index space if not already present.
    fn ensure_endpoint_index_space(&mut self) -> io::Result<()> {
        if self.spaces.get(ENDPOINT_INDEX_SPACE).is_none() {
            self.register_space(
                SpaceConfig::new(
                    ENDPOINT_INDEX_SPACE,
                    "__endpoint_index__",
                    ENDPOINT_INDEX_DIMS,
                )
                .with_bits_per_dim(ENDPOINT_INDEX_BITS_PER_DIM),
            )
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }
        Ok(())
    }

    /// True when `space` is configured to key hyperedges by endpoint centroid.
    fn uses_centroid_keying(&self, space: SpaceId) -> bool {
        self.spaces.get(space).map(|c| c.centroid_keying).unwrap_or(false)
    }

    /// Choose the storage point for a hyperedge record.
    ///
    /// Returns `(point, is_centroid)`. Centroid keying yields spatial locality
    /// for spatially-related edges; it falls back to id-based keying for spaces
    /// without the flag or for purely cross-space edges with no shared frame.
    fn edge_storage_point(&self, space: SpaceId, edge: &Hyperedge) -> (DimensionVector, bool) {
        if self.uses_centroid_keying(space) {
            if let Some(point) = centroid_hyperedge_point(edge) {
                return (point, true);
            }
        }
        (hyperedge_point(edge.id), false)
    }

    /// Register the reserved hyperedge locator space if not already present.
    fn ensure_locator_space(&mut self) -> io::Result<()> {
        if self.spaces.get(HYPEREDGE_LOCATOR_SPACE).is_none() {
            self.register_space(SpaceConfig::new(
                HYPEREDGE_LOCATOR_SPACE,
                "__hyperedge_locator__",
                HYPEREDGE_LOCATOR_DIMS,
            ))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }
        Ok(())
    }

    /// Resolve the stored point for a centroid-keyed edge via its locator.
    fn lookup_edge_locator(
        &mut self,
        space: SpaceId,
        id: HyperedgeId,
        as_of: Option<RevisionId>,
    ) -> io::Result<Option<DimensionVector>> {
        self.ensure_locator_space()?;
        let p = locator_point(space, id);
        let records = self.query_bbox(HYPEREDGE_LOCATOR_SPACE, p.clone(), p, as_of)?;
        for r in records {
            if let Ok((point, _)) = decode_from_slice::<DimensionVector, _>(&r.data, standard()) {
                return Ok(Some(point));
            }
        }
        Ok(None)
    }

    /// Tombstone the id→point locator for a centroid-keyed edge.
    fn tombstone_edge_locator(&mut self, space: SpaceId, id: HyperedgeId) -> io::Result<()> {
        self.ensure_locator_space()?;
        self.delete(HYPEREDGE_LOCATOR_SPACE, locator_point(space, id))?;
        Ok(())
    }

    /// Tombstone reverse-index records for every endpoint on a hyperedge.
    fn tombstone_hyperedge_index(&mut self, edge: &Hyperedge) -> io::Result<()> {
        self.ensure_endpoint_index_space()?;
        for ep in edge_endpoints(edge) {
            let point = endpoint_index_point(ep, edge.id);
            self.delete(ENDPOINT_INDEX_SPACE, point)?;
        }
        Ok(())
    }

    /// Return hyperedge IDs registered at an endpoint index coordinate.
    fn query_endpoint_index_ids(
        &mut self,
        prefix: &[u32],
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<HyperedgeId>> {
        // Full-space scan: Hilbert subscope can drop index entries in 16-D layout
        // (see endpoint_index_point). Prefix filter is cheap vs per-edge hyperedge scan.
        let records = self.query(ENDPOINT_INDEX_SPACE, as_of)?;
        Ok(records
            .iter()
            .filter(|r| {
                prefix
                    .iter()
                    .enumerate()
                    .all(|(i, &v)| r.address.point.coords.get(i) == Some(&v))
            })
            .filter_map(|r| decode_hyperedge_id(&r.data))
            .collect())
    }

    /// Load a single hyperedge by id from the hyperedge space.
    fn fetch_hyperedge_by_id(
        &mut self,
        space: SpaceId,
        id: HyperedgeId,
        as_of: Option<RevisionId>,
    ) -> io::Result<Option<Hyperedge>> {
        let p = if self.uses_centroid_keying(space) {
            match self.lookup_edge_locator(space, id, as_of)? {
                Some(point) => point,
                None => return Ok(None),
            }
        } else {
            hyperedge_point(id)
        };
        let records = self.query_bbox(space, p.clone(), p, as_of)?;
        for r in records {
            if let Ok((edge, _)) = decode_from_slice::<Hyperedge, _>(&r.data, standard()) {
                if edge.id == id {
                    return Ok(Some(edge));
                }
            }
        }
        Ok(None)
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
        #[cfg(feature = "sync")]
        self.enqueue_sync(SyncOperation::Write {
            address: address.clone(),
            revision: rev,
            data: data.clone(),
        })?;
        self.buffer.push(Record {
            address,
            revision: rev,
            data,
            tombstone: false,
            hilbert_key: 0,
        });
        if !self.defer_auto_flush && self.buffer.len() >= self.flush_threshold {
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
        #[cfg(feature = "sync")]
        self.enqueue_sync(SyncOperation::Tombstone {
            address: address.clone(),
            revision: rev,
        })?;
        self.buffer.push(Record {
            address,
            revision: rev,
            data: vec![],
            tombstone: true,
            hilbert_key: 0,
        });
        Ok(rev)
    }

    /// Insert or update a typed hyperedge record.
    pub fn insert_hyperedge(
        &mut self,
        space: SpaceId,
        mut edge: Hyperedge,
    ) -> io::Result<RevisionId> {
        edge.validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{:?}", e)))?;
        if self.edge_storage_point(space, &edge).1 {
            self.ensure_locator_space()?;
        }
        let build_index = true;
        self.ensure_endpoint_index_space()?;
        let rows = self.prepare_hyperedge_writes(space, &edge, build_index)?;
        let rev = self.apply_prepared_writes_strict(rows)?;
        edge.valid_from = rev;
        if !self.defer_auto_flush && self.buffer.len() >= self.flush_threshold {
            self.flush(space)?;
        }
        #[cfg(feature = "sync")]
        self.enqueue_sync(SyncOperation::WriteHyperedge {
            space,
            edge,
            revision: rev,
        })?;
        Ok(rev)
    }

    /// Logically delete a hyperedge record by ID.
    pub fn delete_hyperedge(&mut self, space: SpaceId, id: HyperedgeId) -> io::Result<RevisionId> {
        let edge = self.fetch_hyperedge_by_id(space, id, None)?;
        // Tombstone at the edge's actual stored point. For centroid-keyed
        // spaces this differs from the id-based point.
        let point = match &edge {
            Some(e) => self.edge_storage_point(space, e).0,
            None => hyperedge_point(id),
        };
        if let Some(e) = &edge {
            self.tombstone_hyperedge_index(e)?;
        }
        let rev = self.next_revision();
        let address = Address::new(space, point);
        self.wal.append(&WalEntry::Tombstone {
            address: address.clone(),
            revision: rev,
        })?;
        self.buffer.push(Record {
            address,
            revision: rev,
            data: vec![],
            tombstone: true,
            hilbert_key: 0,
        });
        if edge.is_some() && self.uses_centroid_keying(space) {
            self.tombstone_edge_locator(space, id)?;
        }
        #[cfg(feature = "sync")]
        self.enqueue_sync(SyncOperation::DeleteHyperedge {
            space,
            edge_id: id,
            revision: rev,
        })?;
        Ok(rev)
    }

    /// Query all decodable hyperedges from a space.
    pub fn query_hyperedges(
        &mut self,
        space: SpaceId,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Hyperedge>> {
        self.query(space, as_of)?
            .into_iter()
            .map(|r| {
                decode_from_slice::<Hyperedge, _>(&r.data, standard())
                    .map(|(edge, _)| edge)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            })
            .collect()
    }

    /// Query hyperedges that reference the provided endpoint.
    ///
    /// Uses the reverse endpoint index in [`ENDPOINT_INDEX_SPACE`] so this does
    /// not scan every hyperedge in the space.
    pub fn query_hyperedges_for_endpoint(
        &mut self,
        space: SpaceId,
        endpoint: &EndpointRef,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Hyperedge>> {
        self.ensure_endpoint_index_space()?;
        let prefix = endpoint_lookup_prefix(endpoint);
        let ids = self.query_endpoint_index_ids(&prefix, as_of)?;
        let rev_ceiling = as_of.unwrap_or_else(|| {
            RevisionId(self.revision.load(Ordering::Relaxed))
        });
        let mut edges = Vec::new();
        for id in ids {
            if let Some(edge) = self.fetch_hyperedge_by_id(space, id, as_of)? {
                if edge.is_active_at(rev_ceiling)
                    && edge.endpoints.iter().any(|ep| {
                        ep.space == endpoint.space && ep.node.coords == endpoint.node.coords
                    })
                {
                    edges.push(edge);
                }
            }
        }
        Ok(edges)
    }

    /// BFS walk from `spec.start`, following hyperedges up to `spec.max_depth`.
    pub fn traverse(&mut self, edge_space: SpaceId, spec: &TraversalSpec) -> io::Result<Subgraph> {
        let rev_ceiling = spec.as_of.unwrap_or_else(|| {
            RevisionId(self.revision.load(Ordering::Relaxed))
        });
        let mut subgraph = Subgraph::default();
        subgraph.add_node(spec.start.clone());

        let mut frontier: std::collections::VecDeque<(EndpointRef, usize)> =
            std::collections::VecDeque::from([(spec.start.clone(), 0)]);
        let mut visited: std::collections::HashSet<(u64, Vec<u32>)> =
            std::collections::HashSet::from([(spec.start.space.0, spec.start.node.coords.clone())]);

        while let Some((node, depth)) = frontier.pop_front() {
            let incident = self.query_hyperedges_for_endpoint(edge_space, &node, spec.as_of)?;
            for edge in incident {
                if let Some(ref kinds) = spec.follow_kinds {
                    if !kinds.iter().any(|k| k.as_str() == edge.kind.as_str()) {
                        continue;
                    }
                }
                if !edge.is_active_at(rev_ceiling) {
                    continue;
                }
                subgraph.add_edge(edge.clone());
                for ep in &edge.endpoints {
                    if ep.space == node.space && ep.node.coords == node.node.coords {
                        continue;
                    }
                    let next_depth = depth + 1;
                    if next_depth > spec.max_depth {
                        continue;
                    }
                    let key = (ep.space.0, ep.node.coords.clone());
                    if visited.insert(key) {
                        subgraph.add_node(ep.clone());
                        frontier.push_back((ep.clone(), next_depth));
                    }
                }
            }
        }
        Ok(subgraph)
    }

    /// Query hyperedges by relationship kind.
    pub fn query_hyperedges_by_kind(
        &mut self,
        space: SpaceId,
        kind: &str,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Hyperedge>> {
        let edges = self.query_hyperedges(space, as_of)?;
        Ok(edges
            .into_iter()
            .filter(|e| e.kind.as_str() == kind)
            .collect())
    }

    /// Adapter-friendly hyperedge write API with optional catalog enforcement.
    pub fn insert_hyperedge_typed<K: KindLabel>(
        &mut self,
        space: SpaceId,
        id: HyperedgeId,
        kind: K,
        endpoints: Vec<AdapterEndpoint>,
        weight_milli: Option<i64>,
        metadata: std::collections::BTreeMap<String, String>,
        valid_to: Option<RevisionId>,
        catalog: Option<&KindCatalog>,
    ) -> io::Result<RevisionId> {
        let kind_label = kind.label().to_string();
        if let Some(catalog) = catalog {
            catalog
                .validate_edge_kind(&kind_label)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
            for ep in &endpoints {
                catalog
                    .validate_endpoint_role(&ep.role)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
            }
        }
        let edge = Hyperedge {
            id,
            kind: kind_label.into(),
            endpoints: endpoints.into_iter().map(EndpointRef::from).collect(),
            weight_milli,
            metadata,
            valid_from: RevisionId::ZERO,
            valid_to,
        };
        self.insert_hyperedge(space, edge)
    }

    /// Adapter-friendly kind-filter query.
    pub fn query_hyperedges_by_kind_typed<K: KindLabel>(
        &mut self,
        space: SpaceId,
        kind: K,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Hyperedge>> {
        self.query_hyperedges_by_kind(space, kind.label(), as_of)
    }

    /// Insert a signal sample in the provided signal space.
    pub fn insert_signal_sample(
        &mut self,
        space: SpaceId,
        sample: SignalSample,
    ) -> io::Result<RevisionId> {
        sample.validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{:?}", e)))?;
        let full_coords = sample
            .scope
            .address_coords(&sample.local_coords)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{:?}", e)))?;
        if let Some(cfg) = self.spaces.get(space) {
            if cfg.dims != full_coords.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "signal coordinates do not match space dimensions",
                ));
            }
        }
        let data = encode_to_vec(&sample, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let rev = self.next_revision();
        let address = Address::new(space, DimensionVector::new(full_coords));
        self.wal.append(&WalEntry::Write {
            address: address.clone(),
            revision: rev,
            data: data.clone(),
        })?;
        self.buffer.push(Record {
            address,
            revision: rev,
            data,
            tombstone: false,
            hilbert_key: 0,
        });
        if !self.defer_auto_flush && self.buffer.len() >= self.flush_threshold {
            self.flush(space)?;
        }
        #[cfg(feature = "sync")]
        self.enqueue_sync(SyncOperation::WriteSignal {
            space,
            sample,
            revision: rev,
        })?;
        Ok(rev)
    }

    /// Adapter-friendly signal write API bound to a typed space.
    pub fn insert_signal_sample_typed<SB: SpaceBinding, K: KindLabel>(
        &mut self,
        signal_id: crate::infinitedb_core::signal::SignalId,
        kind: K,
        parent_prefix: DimensionVector,
        local_coords: Vec<u32>,
        value_milli: i64,
        source_revision: Option<RevisionId>,
        constraint: Option<crate::infinitedb_core::signal::SignalConstraint>,
        catalog: Option<&KindCatalog>,
    ) -> io::Result<RevisionId> {
        if let Some(cfg) = self.spaces.get(SB::SPACE_ID) {
            if cfg.dims != SB::DIMS {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "SpaceBinding dims mismatch for space {}: trait={}, registry={}",
                        SB::SPACE_ID.0,
                        SB::DIMS,
                        cfg.dims
                    ),
                ));
            }
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("SpaceBinding refers to unregistered space {}", SB::SPACE_ID.0),
            ));
        }
        let kind_label = kind.label().to_string();
        if let Some(catalog) = catalog {
            catalog
                .validate_signal_kind(&kind_label)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        }
        let sample = crate::infinitedb_core::signal::SignalSample {
            signal_id,
            kind: kind_label.into(),
            scope: crate::infinitedb_core::signal::SignalScope {
                parent_prefix,
                total_dims: SB::DIMS,
            },
            local_coords,
            value_milli,
            source_revision,
            constraint,
        };
        self.insert_signal_sample(SB::SPACE_ID, sample)
    }

    /// Query all signal samples under a parent scope prefix.
    pub fn query_signal_scope(
        &mut self,
        space: SpaceId,
        parent_coords: &[u32],
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<SignalSample>> {
        let rows = self.query_subscope(space, parent_coords, as_of)?;
        rows.into_iter()
            .map(|r| {
                decode_from_slice::<SignalSample, _>(&r.data, standard())
                    .map(|(sample, _)| sample)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            })
            .collect()
    }

    /// Query signal samples in a local coordinate range under a parent scope.
    pub fn query_signal_range(
        &mut self,
        space: SpaceId,
        parent_coords: &[u32],
        min_local: &[u32],
        max_local: &[u32],
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<SignalSample>> {
        if min_local.len() != max_local.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "min_local and max_local dimensions differ",
            ));
        }
        let mut min = parent_coords.to_vec();
        min.extend_from_slice(min_local);
        let mut max = parent_coords.to_vec();
        max.extend_from_slice(max_local);
        let rows = self.query_bbox(
            space,
            DimensionVector::new(min),
            DimensionVector::new(max),
            as_of,
        )?;
        rows.into_iter()
            .map(|r| {
                decode_from_slice::<SignalSample, _>(&r.data, standard())
                    .map(|(sample, _)| sample)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            })
            .collect()
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
            let key = self.space_key(space, &r.address.point);
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

        // Records are sorted by Hilbert key, so the first/last give the block's
        // key interval used for range pruning. Compute before borrowing the
        // snapshot mutably below.
        let hilbert_min = block
            .records
            .first()
            .map(|r| self.space_key(space, &r.address.point))
            .unwrap_or(0);
        let hilbert_max = block
            .records
            .last()
            .map(|r| self.space_key(space, &r.address.point))
            .unwrap_or(hilbert_min);

        // Advance the space's active snapshot.
        let snapshot = self.snapshots.entry(space.0).or_insert_with(|| {
            Snapshot::root(snap_id, space)
        });
        snapshot.blocks.insert(
            hilbert_min,
            BlockIndexEntry { block_id, max_key: hilbert_max },
        );
        snapshot.revision = max_rev;

        self.persist_meta()?;
        self.wal.sync()?;

        // The sealed records are now durable in `snapshots.bin`. Rewrite the WAL
        // so it retains only the records still buffered (other spaces, not yet
        // sealed) plus a checkpoint marker — otherwise replay on the next open
        // would re-add records that already live in the sealed block.
        self.rewrite_wal_from_buffer(max_rev)?;
        Ok(())
    }

    /// Rewrite the WAL to mirror the current buffer, terminated by a checkpoint.
    fn rewrite_wal_from_buffer(&mut self, checkpoint: RevisionId) -> io::Result<()> {
        let mut entries: Vec<WalEntry> = self
            .buffer
            .iter()
            .map(|r| {
                if r.tombstone {
                    WalEntry::Tombstone { address: r.address.clone(), revision: r.revision }
                } else {
                    WalEntry::Write {
                        address: r.address.clone(),
                        revision: r.revision,
                        data: r.data.clone(),
                    }
                }
            })
            .collect();
        entries.push(WalEntry::Checkpoint { revision: checkpoint });
        self.wal.rewrite(&entries)
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
        self.query_inner(space, None, as_of, false)
    }

    /// Execute a `Query` descriptor against the space's current snapshot.
    ///
    /// Resolution order for the block-pruning key interval:
    /// 1. `q.key_range` when present (a precomputed Hilbert interval);
    /// 2. otherwise derived from `q.range`'s corner coordinates;
    /// 3. otherwise a full-space scan.
    ///
    /// When `q.range` is set, an exact per-record coordinate filter is applied
    /// so results match `query_bbox`. Historical snapshot reads are not yet
    /// supported, so `q.snapshot` must match the space's current snapshot.
    pub fn execute(&mut self, q: &Query) -> io::Result<Vec<Record>> {
        if let Some(current) = self.current_snapshot(q.space) {
            if current != q.snapshot {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "query snapshot does not match the current snapshot for this space",
                ));
            }
        }

        let key_range = match q.key_range {
            Some(kr) => Some(kr),
            None => q.range.as_ref().map(|r| {
                let ka = self.space_key(q.space, &r.min);
                let kb = self.space_key(q.space, &r.max);
                if ka <= kb { (ka, kb) } else { (kb, ka) }
            }),
        };

        let mut results = self.query_inner(q.space, key_range, q.as_of, q.include_tombstones)?;

        if let Some(range) = &q.range {
            results.retain(|r| r.address.point.within(&range.min, &range.max));
        }

        Ok(results)
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
        let k_min = self.space_key(space, &min);
        let k_max = self.space_key(space, &max);
        let (lo, hi) = if k_min <= k_max { (k_min, k_max) } else { (k_max, k_min) };
        let mut results = self.query_inner(space, Some((lo, hi)), as_of, false)?;
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
        include_tombstones: bool,
    ) -> io::Result<Vec<Record>> {
        let rev_ceiling = as_of.unwrap_or_else(|| {
            RevisionId(self.revision.load(Ordering::Relaxed))
        });

        let mut results: Vec<Record> = Vec::new();

        let mut tombstoned: std::collections::HashSet<_> = self
            .buffer
            .iter()
            .filter(|r| r.address.space == space && r.tombstone && r.revision <= rev_ceiling)
            .map(|r| r.address.point.coords.clone())
            .collect();

        // Query sealed blocks if a snapshot exists.
        if let Some(snapshot) = self.snapshots.get(&space.0) {
            let block_ids: Vec<BlockId> = match key_range {
                None => snapshot.blocks.values().map(|e| e.block_id).collect(),
                Some((lo, hi)) => {
                    // Each block covers the Hilbert interval [min_key, max_key]
                    // (min_key is the map key). Prune to blocks whose interval
                    // overlaps the query interval [lo, hi]; the per-record
                    // within() filter still removes Hilbert false positives.
                    snapshot
                        .blocks
                        .iter()
                        .filter(|(min_key, entry)| **min_key <= hi && entry.max_key >= lo)
                        .map(|(_, entry)| entry.block_id)
                        .collect()
                }
            };
            for block_id in &block_ids {
                let block = self.store.read_block(*block_id)?;
                for record in &block.records {
                    if record.address.space == space
                        && record.tombstone
                        && record.revision <= rev_ceiling
                    {
                        tombstoned.insert(record.address.point.coords.clone());
                    }
                }
            }
            for block_id in block_ids {
                let block = self.store.read_block(block_id)?;
                for record in block.records {
                    if record.revision > rev_ceiling {
                        continue;
                    }
                    if !include_tombstones && record.tombstone {
                        continue;
                    }
                    // Point lookups (lo == hi): records are sorted by Hilbert key at seal.
                    if let Some((lo, hi)) = key_range {
                        if lo == hi {
                            let k = self.space_key(space, &record.address.point);
                            if k != lo {
                                continue;
                            }
                        }
                    }
                    results.push(record);
                }
            }
            if !include_tombstones {
                results.retain(|r| !tombstoned.contains(&r.address.point.coords));
            }
        }

        // In-memory buffer (records not yet flushed).
        for record in &self.buffer {
            let visible = record.address.space == space
                && record.revision <= rev_ceiling
                && (include_tombstones || !record.tombstone)
                && (include_tombstones || !tombstoned.contains(&record.address.point.coords));
            if visible {
                if let Some((lo, hi)) = key_range {
                    let k = self.space_key(space, &record.address.point);
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

    /// Look up a branch ID by name, if one exists.
    pub fn branch_id(&self, name: &str) -> Option<BranchId> {
        self.branches.get_by_name(name).map(|b| b.id)
    }

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
        self.persist_meta().map_err(|e| e.to_string())?;
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
    // Snapshot access, compaction, and block I/O
    // -----------------------------------------------------------------------

    /// Return the active snapshot for a space, if one exists.
    pub fn snapshot_for_space(&self, space: SpaceId) -> Option<Snapshot> {
        self.snapshots.get(&space.0).cloned()
    }

    /// Current monotonic revision counter.
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

    /// Read a sealed block from disk (through the LRU cache).
    pub fn read_block(&mut self, id: BlockId) -> io::Result<Block> {
        self.store.read_block(id)
    }

    /// Merge fragmented blocks in `space` into fewer larger blocks.
    pub fn compact_space(
        &mut self,
        space: SpaceId,
        config: &CompactionConfig,
    ) -> io::Result<CompactionResult> {
        let snapshot = self
            .snapshots
            .get(&space.0)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no snapshot for space"))?;

        let input_blocks: Vec<Block> = snapshot
            .blocks
            .values()
            .map(|e| self.store.read_block(e.block_id))
            .collect::<io::Result<_>>()?;

        if input_blocks.len() <= 1 {
            return Ok(CompactionResult {
                new_blocks: vec![],
                superseded: vec![],
            });
        }

        let result = compact(
            input_blocks,
            config,
            snapshot.id,
            || self.alloc_block_id(),
        );

        let mut new_blocks = Vec::new();
        for mut block in result.new_blocks {
            let mut recs = block.records.clone();
            recs.sort_by_key(|r| self.space_key(space, &r.address.point));
            block.records = recs;
            block.checksum = compute_checksum(&block)?;
            self.store.write_block(&block)?;
            new_blocks.push(block);
        }

        let mut updated = Snapshot::root(snapshot.id, space);
        updated.revision = snapshot.revision;
        for block in &new_blocks {
            let min_key = block
                .records
                .first()
                .map(|r| self.space_key(space, &r.address.point))
                .unwrap_or(0);
            let max_key = block
                .records
                .last()
                .map(|r| self.space_key(space, &r.address.point))
                .unwrap_or(min_key);
            updated.blocks.insert(
                min_key,
                BlockIndexEntry {
                    block_id: block.id,
                    max_key,
                },
            );
        }
        self.snapshots.insert(space.0, updated);

        let live: Vec<Snapshot> = self.snapshots.values().cloned().collect();
        let deletable = safe_to_delete(&result.superseded, &live);
        self.gc_blocks(&deletable)?;

        let rev = RevisionId(self.revision.load(Ordering::Relaxed));
        self.wal.append(&WalEntry::Checkpoint { revision: rev })?;
        self.persist_meta()?;
        Ok(CompactionResult {
            new_blocks,
            superseded: result.superseded,
        })
    }

    /// Delete block files that are no longer referenced by any live snapshot.
    pub fn gc_blocks(&mut self, superseded: &[BlockId]) -> io::Result<usize> {
        let live: Vec<Snapshot> = self.snapshots.values().cloned().collect();
        let deletable = safe_to_delete(superseded, &live);
        for id in &deletable {
            self.store.delete_block(*id)?;
        }
        Ok(deletable.len())
    }

    #[cfg(feature = "sync")]
    /// Build a Merkle tree over all records in `space`'s snapshot (Hilbert order).
    pub fn snapshot_merkle(&mut self, space: SpaceId) -> io::Result<merkle::MerkleTree> {
        let snapshot = self
            .snapshots
            .get(&space.0)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no snapshot for space"))?;

        let mut leaves = Vec::new();
        for (_min_key, entry) in &snapshot.blocks {
            let block = self.store.read_block(entry.block_id)?;
            let mut recs = block.records;
            recs.sort_by_key(|r| (self.space_key(space, &r.address.point), r.revision.0));
            for record in &recs {
                let encoded = encode_to_vec(record, standard())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                leaves.push(merkle::hash_record(&encoded));
            }
        }
        Ok(merkle::MerkleTree::build(&leaves))
    }

    #[cfg(feature = "sync")]
    /// Apply a replication delta: write blocks, update snapshot index, GC superseded files.
    pub fn apply_delta(&mut self, space: SpaceId, delta: &Delta) -> io::Result<()> {
        for block in &delta.added_blocks {
            self.store.write_block(block)?;
        }
        let current = self
            .snapshots
            .get(&space.0)
            .cloned()
            .unwrap_or_else(|| Snapshot::root(delta.target_snapshot, space));
        let updated = delta.apply(&current);
        self.snapshots.insert(space.0, updated);
        self.gc_blocks(&delta.removed_block_ids)?;
        self.persist_meta()?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Replication sync (offline-first)
    // -----------------------------------------------------------------------

    /// Run one immediate sync pass against the provided transport.
    #[cfg(feature = "sync")]
    pub fn sync_now(
        &mut self,
        transport: &dyn SyncTransport,
        max_batch: usize,
    ) -> io::Result<SyncReport> {
        let mut state = self
            .outbox_state
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "sync outbox lock poisoned"))?;
        let report = state.process_once(transport, max_batch);
        if report.attempted > 0 {
            save_outbox(&self.outbox_path, &state)?;
        }
        Ok(report)
    }

    /// Start a background worker that retries queued outbox writes.
    #[cfg(feature = "sync")]
    pub fn start_background_sync(
        &mut self,
        transport: Arc<dyn SyncTransport>,
        interval: Duration,
        batch_size: usize,
    ) -> io::Result<()> {
        self.stop_background_sync();
        let worker = BackgroundSyncWorker::start(
            Arc::clone(&self.outbox_state),
            self.outbox_path.clone(),
            transport,
            interval,
            batch_size,
        )?;
        self.sync_worker = Some(worker);
        Ok(())
    }

    /// Stop the running background sync worker (if any).
    #[cfg(feature = "sync")]
    pub fn stop_background_sync(&mut self) {
        if let Some(mut worker) = self.sync_worker.take() {
            worker.stop();
        }
    }

    /// Current number of queued operations waiting to sync.
    #[cfg(feature = "sync")]
    pub fn sync_pending_count(&self) -> usize {
        self.outbox_state
            .lock()
            .map(|s| s.pending_count())
            .unwrap_or(0)
    }

    /// Last successful outbox sync timestamp (epoch ms).
    #[cfg(feature = "sync")]
    pub fn last_successful_sync_at_ms(&self) -> Option<u64> {
        self.outbox_state
            .lock()
            .ok()
            .and_then(|s| s.last_success_at_ms)
    }

    /// Last sync error seen by the outbox processor.
    #[cfg(feature = "sync")]
    pub fn last_sync_error(&self) -> Option<String> {
        self.outbox_state
            .lock()
            .ok()
            .and_then(|s| s.last_error.clone())
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
                self.buffer.push(Record { address, revision, data, tombstone: false, hilbert_key: 0 });
            }
            WalEntry::Tombstone { address, revision } => {
                self.buffer.push(Record { address, revision, data: vec![], tombstone: true, hilbert_key: 0 });
            }
            WalEntry::BlockSealed { block_id, space, snapshot } => {
                self.reconcile_sealed_block(block_id, space, snapshot)?;
            }
            WalEntry::Checkpoint { .. } => {}
        }
        Ok(())
    }

    /// Re-link a sealed block into the snapshot index during recovery.
    ///
    /// This covers a crash between `write_block` and the `persist_meta` that
    /// records the block in `snapshots.bin`: the block exists on disk and the
    /// WAL has its `BlockSealed` marker, but the persisted snapshot does not yet
    /// reference it. We read the block, insert its index entry, and drop the now
    /// sealed records from the recovery buffer so queries don't double-count.
    ///
    /// If the block file is absent (crash before it was durably written) the
    /// records remain in the buffer via their `Write` entries and will be
    /// resealed on the next flush, so we simply skip.
    fn reconcile_sealed_block(
        &mut self,
        block_id: BlockId,
        space: SpaceId,
        snapshot_id: SnapshotId,
    ) -> io::Result<()> {
        let block = match self.store.read_block(block_id) {
            Ok(block) => block,
            Err(_) => return Ok(()),
        };

        let min_key = block
            .records
            .first()
            .map(|r| self.space_key(space, &r.address.point))
            .unwrap_or(0);
        let max_key = block
            .records
            .last()
            .map(|r| self.space_key(space, &r.address.point))
            .unwrap_or(min_key);
        let block_max_rev = block.max_revision;

        let sealed: std::collections::HashSet<(Vec<u32>, u64)> = block
            .records
            .iter()
            .map(|r| (r.address.point.coords.clone(), r.revision.0))
            .collect();

        let snapshot = self
            .snapshots
            .entry(space.0)
            .or_insert_with(|| Snapshot::root(snapshot_id, space));
        snapshot
            .blocks
            .insert(min_key, BlockIndexEntry { block_id, max_key });
        if snapshot.revision < block_max_rev {
            snapshot.revision = block_max_rev;
        }

        self.buffer
            .retain(|r| !sealed.contains(&(r.address.point.coords.clone(), r.revision.0)));
        Ok(())
    }

    fn persist_meta(&mut self) -> io::Result<()> {
        // Persist spaces registry.
        let spaces_bytes = encode_to_vec(&self.spaces, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("spaces.bin", &spaces_bytes)?;

        // Persist branch registry so user branches survive a restart.
        let branches_bytes = encode_to_vec(&self.branches, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("branches.bin", &branches_bytes)?;

        // Persist the per-space snapshot index. Without this the sealed blocks
        // on disk become unreachable after reopen.
        let snapshots_bytes = encode_to_vec(&self.snapshots, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("snapshots.bin", &snapshots_bytes)?;

        // Persist revision counters as a [u64; 4].
        let counters: [u64; 4] = [
            self.revision.load(Ordering::Relaxed),
            self.next_block_id.load(Ordering::Relaxed),
            self.next_snapshot_id.load(Ordering::Relaxed),
            self.next_branch_id.load(Ordering::Relaxed),
        ];
        let counters_bytes = encode_to_vec(&counters, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("counters.bin", &counters_bytes)?;

        Ok(())
    }

    #[cfg(feature = "sync")]
    pub(super) fn enqueue_sync(&mut self, op: SyncOperation) -> io::Result<()> {
        let mut state = self
            .outbox_state
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "sync outbox lock poisoned"))?;
        state.enqueue(op);
        save_outbox(&self.outbox_path, &state)
    }
}

#[cfg(feature = "sync")]
impl Drop for LegacyDb {
    fn drop(&mut self) {
        self.stop_background_sync();
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

    /// Pretty-print the current memory statistics to stdout.
    pub fn print(&self) {
        println!("\n╔═══ LegacyDb Memory Stats ═══╗");
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
fn load_meta(store: &BlockStore) -> Option<MetaTuple> {
    let counters_bytes = store.read_meta("counters.bin").ok()?;
    // Current format is [u64; 4]; fall back to the legacy [u64; 3] layout
    // (pre branch-persistence) and default the branch counter.
    let (revision, next_block, next_snapshot, next_branch) =
        match decode_from_slice::<[u64; 4], _>(&counters_bytes, standard()) {
            Ok((c, _)) => (c[0], c[1], c[2], c[3]),
            Err(_) => {
                let (c, _): ([u64; 3], _) =
                    decode_from_slice(&counters_bytes, standard()).ok()?;
                (c[0], c[1], c[2], 2)
            }
        };

    let spaces_bytes = store.read_meta("spaces.bin").ok()?;
    let (spaces, _): (SpaceRegistry, _) = decode_from_slice(&spaces_bytes, standard()).ok()?;

    // Branches and snapshots may be absent on databases created before they
    // were persisted — fall back to empty registries in that case.
    let branches = store
        .read_meta("branches.bin")
        .ok()
        .and_then(|b| decode_from_slice::<BranchRegistry, _>(&b, standard()).ok())
        .map(|(r, _)| r)
        .unwrap_or_else(BranchRegistry::new);

    let snapshots = store
        .read_meta("snapshots.bin")
        .ok()
        .and_then(|b| decode_from_slice::<BTreeMap<u64, Snapshot>, _>(&b, standard()).ok())
        .map(|(m, _)| m)
        .unwrap_or_default();

    Some((spaces, branches, snapshots, revision, next_block, next_snapshot, next_branch))
}

/// Persisted metadata: spaces, branches, snapshots, and the four ID counters
/// (`revision`, `next_block`, `next_snapshot`, `next_branch`).
type MetaTuple = (
    SpaceRegistry,
    BranchRegistry,
    BTreeMap<u64, Snapshot>,
    u64,
    u64,
    u64,
    u64,
);

fn default_meta() -> MetaTuple {
    (
        SpaceRegistry::new(),
        BranchRegistry::new(),
        BTreeMap::new(),
        0,
        1,
        1,
        2,
    )
}

fn hyperedge_point(id: HyperedgeId) -> DimensionVector {
    DimensionVector::new(vec![(id.0 >> 32) as u32, (id.0 & 0xFFFF_FFFF) as u32])
}

/// Reserved space mapping `(edge space, HyperedgeId)` → stored centroid point.
pub(super) const HYPEREDGE_LOCATOR_SPACE: SpaceId = SpaceId(u64::MAX - 2);
/// Locator key dims: `[space_hi, space_lo, id_hi, id_lo]`.
const HYPEREDGE_LOCATOR_DIMS: usize = 4;

/// Deterministic locator key for an `(edge space, edge id)` pair.
pub(super) fn locator_point(space: SpaceId, id: HyperedgeId) -> DimensionVector {
    DimensionVector::new(vec![
        (space.0 >> 32) as u32,
        (space.0 & 0xFFFF_FFFF) as u32,
        (id.0 >> 32) as u32,
        (id.0 & 0xFFFF_FFFF) as u32,
    ])
}

/// Centroid-based storage point for a hyperedge: `[centroid…, id_hi, id_lo]`.
///
/// Spatially-related edges (similar endpoint centroids) get numerically close
/// Hilbert keys; the trailing id dimensions keep distinct edges from colliding
/// at the same record address. Returns `None` when the edge has no shared-space
/// centroid (caller falls back to id-based keying).
fn centroid_hyperedge_point(edge: &Hyperedge) -> Option<DimensionVector> {
    let (_space, centroid) = edge.endpoint_centroid()?;
    // Reserve the final two dimensions for the id; cap the centroid accordingly.
    let mut coords = centroid;
    coords.truncate(14);
    coords.push((edge.id.0 >> 32) as u32);
    coords.push((edge.id.0 & 0xFFFF_FFFF) as u32);
    Some(DimensionVector::new(coords))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::infinitedb_core::address::{DimensionVector, SpaceId};
    use crate::infinitedb_core::adapter::{AdapterEndpoint, KindLabel, SpaceBinding};
    use crate::infinitedb_core::branch::BranchId;
    use crate::infinitedb_core::hyperedge::{EndpointRef, EndpointRole, Hyperedge, HyperedgeId, HyperedgeKind};
    use crate::infinitedb_core::kind_catalog::{KindCatalog, KindDefinition, UnknownKindPolicy};
    use crate::infinitedb_core::signal::{SignalId, SignalKind, SignalSample, SignalScope};
    use crate::infinitedb_core::space::SpaceConfig;
    #[cfg(feature = "sync")]
    use std::sync::Arc;
    #[cfg(feature = "sync")]
    use crate::infinitedb_sync::transport::{SyncEnvelope, SyncResult, SyncTransport};

    fn open_tmp() -> (LegacyDb, TempDir) {
        let dir = TempDir::new().unwrap();
        let db = LegacyDb::open(dir.path()).unwrap();
        (db, dir)
    }

    enum BeamKinds {
        BearsOn,
        BendingMoment,
    }

    impl KindLabel for BeamKinds {
        fn label(&self) -> &str {
            match self {
                BeamKinds::BearsOn => "beam.bears_on",
                BeamKinds::BendingMoment => "beam.bending_moment",
            }
        }
    }

    struct BeamSignalSpace;
    impl SpaceBinding for BeamSignalSpace {
        const SPACE_ID: SpaceId = SpaceId(88);
        const DIMS: usize = 3;
        const SPACE_NAME: &'static str = "beam_signals";
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
    fn flush_records_block_key_interval() {
        let (mut db, _dir) = open_tmp();
        let space = SpaceId(1);
        let p_lo = DimensionVector::new(vec![1, 1]);
        let p_hi = DimensionVector::new(vec![200, 200]);
        db.insert(space, p_lo.clone(), vec![1]).unwrap();
        db.insert(space, p_hi.clone(), vec![2]).unwrap();
        db.flush(space).unwrap();

        let snapshot = db.snapshots.get(&space.0).unwrap();
        assert_eq!(snapshot.blocks.len(), 1);
        let (min_key, entry) = snapshot.blocks.iter().next().unwrap();
        let ka = hilbert_key_standard(&p_lo);
        let kb = hilbert_key_standard(&p_hi);
        assert_eq!(*min_key, ka.min(kb));
        assert_eq!(entry.max_key, ka.max(kb));
    }

    #[test]
    fn range_pruning_skips_non_overlapping_blocks() {
        let (mut db, _dir) = open_tmp();
        let space = SpaceId(1);
        // Three separated points, each sealed into its own single-record block.
        let points = [
            DimensionVector::new(vec![1, 1]),
            DimensionVector::new(vec![120, 30]),
            DimensionVector::new(vec![250, 200]),
        ];
        for (i, p) in points.iter().enumerate() {
            db.insert(space, p.clone(), vec![i as u8]).unwrap();
            db.flush(space).unwrap();
        }
        assert_eq!(db.snapshots.get(&space.0).unwrap().blocks.len(), 3);

        // Query the exact key interval of the middle block only.
        let k_mid = hilbert_key_standard(&points[1]);
        let results = db.query_inner(space, Some((k_mid, k_mid)), None, false).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].address.point, points[1]);
    }

    #[test]
    fn execute_range_matches_query_bbox() {
        use crate::infinitedb_core::query::{Query, SpatialRange};
        let (mut db, _dir) = open_tmp();
        let space = SpaceId(1);
        db.register_space(SpaceConfig::new(space, "s", 2)).unwrap();
        db.insert(space, DimensionVector::new(vec![5, 5]), vec![1]).unwrap();
        db.insert(space, DimensionVector::new(vec![8, 2]), vec![2]).unwrap();
        db.insert(space, DimensionVector::new(vec![200, 200]), vec![3]).unwrap();
        db.flush(space).unwrap();

        let min = DimensionVector::new(vec![0, 0]);
        let max = DimensionVector::new(vec![10, 10]);

        let mut via_bbox = db
            .query_bbox(space, min.clone(), max.clone(), None)
            .unwrap();
        let snap = db.current_snapshot(space).unwrap();
        let q = Query::new(space, snap).with_range(SpatialRange::new(min, max));
        let mut via_execute = db.execute(&q).unwrap();

        via_bbox.sort_by_key(|r| r.data.clone());
        via_execute.sort_by_key(|r| r.data.clone());
        assert_eq!(via_bbox.len(), 2);
        assert_eq!(
            via_bbox.iter().map(|r| r.data.clone()).collect::<Vec<_>>(),
            via_execute.iter().map(|r| r.data.clone()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn execute_include_tombstones_flag() {
        use crate::infinitedb_core::query::Query;
        let (mut db, _dir) = open_tmp();
        let space = SpaceId(1);
        let point = DimensionVector::new(vec![3, 3]);
        db.insert(space, point.clone(), vec![1]).unwrap();
        db.delete(space, point).unwrap();

        // No snapshot exists yet (nothing flushed); validation is skipped.
        let snap = SnapshotId(0);
        let default = db.execute(&Query::new(space, snap)).unwrap();
        assert_eq!(default.len(), 0, "tombstoned record hidden by default");

        let with_tombstones = db
            .execute(&Query::new(space, snap).include_tombstones())
            .unwrap();
        assert!(
            with_tombstones.iter().any(|r| r.tombstone),
            "include_tombstones surfaces the tombstone"
        );
    }

    #[test]
    fn block_sealed_replay_reconciles_after_partial_crash() {
        let dir = TempDir::new().unwrap();
        let space = SpaceId(1);
        let point = DimensionVector::new(vec![7, 9]);
        {
            let mut db = LegacyDb::open(dir.path()).unwrap();
            db.register_space(SpaceConfig::new(space, "s", 2)).unwrap();
            let rev = db.insert(space, point.clone(), vec![5]).unwrap();

            // Simulate a crash between sealing a block and persisting metadata:
            // write the block durably and append its WAL marker, but never call
            // persist_meta / rewrite the WAL.
            let block_id = db.alloc_block_id();
            let snap_id = db.alloc_snapshot_id();
            let record = Record {
                address: Address::new(space, point.clone()),
                revision: rev,
                data: vec![5],
                tombstone: false,
            hilbert_key: 0,
            };
            let mut block = Block {
                id: block_id,
                space,
                records: vec![record],
                min_revision: rev,
                max_revision: rev,
                checksum: [0u8; 32],
            };
            block.checksum = compute_checksum(&block).unwrap();
            db.store.write_block(&block).unwrap();
            db.wal
                .append(&WalEntry::BlockSealed { block_id, space, snapshot: snap_id })
                .unwrap();
        }

        // Reopen: snapshots.bin never recorded the block, but BlockSealed replay
        // must relink it and the record must surface exactly once.
        let mut db = LegacyDb::open(dir.path()).unwrap();
        let results = db.query(space, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].data, vec![5]);
        assert_eq!(db.snapshots.get(&space.0).unwrap().blocks.len(), 1);
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
    fn register_space_rejects_precision_overflow() {
        let (mut db, _dir) = open_tmp();
        let err = db
            .register_space(
                SpaceConfig::new(SpaceId(99), "big", 16).with_bits_per_dim(9),
            )
            .unwrap_err();
        assert!(err.contains("dims * bits_per_dim"));
    }

    #[test]
    fn different_space_precision_produces_different_keys() {
        use crate::infinitedb_index::hilbert_key_for;
        use crate::infinitedb_index::composite::KeyConfig;
        let (mut db, _dir) = open_tmp();
        let coarse = SpaceId(10);
        let fine = SpaceId(11);
        db.register_space(SpaceConfig::new(coarse, "coarse", 2).with_bits_per_dim(4))
            .unwrap();
        db.register_space(SpaceConfig::new(fine, "fine", 2).with_bits_per_dim(8))
            .unwrap();
        let point = DimensionVector::new(vec![100, 200]);
        let k_coarse = hilbert_key_for(&point, KeyConfig { bits_per_dim: 4 });
        let k_fine = hilbert_key_for(&point, KeyConfig { bits_per_dim: 8 });
        assert_ne!(k_coarse, k_fine);
        db.insert(coarse, point.clone(), vec![1]).unwrap();
        db.insert(fine, point, vec![2]).unwrap();
        db.flush(coarse).unwrap();
        db.flush(fine).unwrap();
    }

    #[test]
    fn endpoint_index_returns_incident_edges_only() {
        let (mut db, _dir) = open_tmp();
        let edge_space = SpaceId(50);
        db.register_space(SpaceConfig::new(edge_space, "edges", 2)).unwrap();
        let shared = EndpointRef {
            role: EndpointRole::new("hub"),
            space: SpaceId(1),
            node: DimensionVector::new(vec![5]),
        };
        let other = EndpointRef {
            role: EndpointRole::new("leaf"),
            space: SpaceId(2),
            node: DimensionVector::new(vec![99]),
        };
        for (id, ep_b) in [
            (1u64, DimensionVector::new(vec![10, 0])),
            (2, DimensionVector::new(vec![20, 0])),
            (3, DimensionVector::new(vec![30, 0])),
        ] {
            let edge = Hyperedge {
                id: HyperedgeId(id),
                kind: HyperedgeKind::new("link"),
                endpoints: vec![
                    shared.clone(),
                    EndpointRef {
                        role: EndpointRole::new("peer"),
                        space: SpaceId(3),
                        node: ep_b,
                    },
                ],
                weight_milli: None,
                metadata: Default::default(),
                valid_from: RevisionId::ZERO,
                valid_to: None,
            };
            db.insert_hyperedge(edge_space, edge).unwrap();
        }
        // Two edges that do not touch `shared`.
        for id in [10u64, 11] {
            let edge = Hyperedge {
                id: HyperedgeId(id),
                kind: HyperedgeKind::new("other"),
                endpoints: vec![other.clone(), other.clone()],
                weight_milli: None,
                metadata: Default::default(),
                valid_from: RevisionId::ZERO,
                valid_to: None,
            };
            db.insert_hyperedge(edge_space, edge).unwrap();
        }
        db.flush(edge_space).unwrap();
        db.flush(ENDPOINT_INDEX_SPACE).unwrap();
        let found = db
            .query_hyperedges_for_endpoint(edge_space, &shared, None)
            .unwrap();
        assert_eq!(found.len(), 3, "only edges incident on the shared endpoint");
    }

    #[test]
    fn traverse_respects_max_depth() {
        use crate::infinitedb_core::traversal::TraversalSpec;
        let (mut db, _dir) = open_tmp();
        let edge_space = SpaceId(60);
        db.register_space(SpaceConfig::new(edge_space, "edges", 2)).unwrap();
        let n0 = EndpointRef {
            role: EndpointRole::new("n"),
            space: SpaceId(10),
            node: DimensionVector::new(vec![1]),
        };
        let n1 = EndpointRef {
            role: EndpointRole::new("n"),
            space: SpaceId(11),
            node: DimensionVector::new(vec![2]),
        };
        let n2 = EndpointRef {
            role: EndpointRole::new("n"),
            space: SpaceId(12),
            node: DimensionVector::new(vec![3]),
        };
        let n3 = EndpointRef {
            role: EndpointRole::new("n"),
            space: SpaceId(13),
            node: DimensionVector::new(vec![4]),
        };
        db.insert_hyperedge(
            edge_space,
            Hyperedge {
                id: HyperedgeId(1),
                kind: HyperedgeKind::new("chain"),
                endpoints: vec![n0.clone(), n1.clone()],
                weight_milli: None,
                metadata: Default::default(),
                valid_from: RevisionId::ZERO,
                valid_to: None,
            },
        )
        .unwrap();
        db.insert_hyperedge(
            edge_space,
            Hyperedge {
                id: HyperedgeId(2),
                kind: HyperedgeKind::new("chain"),
                endpoints: vec![n1.clone(), n2.clone()],
                weight_milli: None,
                metadata: Default::default(),
                valid_from: RevisionId::ZERO,
                valid_to: None,
            },
        )
        .unwrap();
        db.insert_hyperedge(
            edge_space,
            Hyperedge {
                id: HyperedgeId(3),
                kind: HyperedgeKind::new("chain"),
                endpoints: vec![n2.clone(), n3.clone()],
                weight_milli: None,
                metadata: Default::default(),
                valid_from: RevisionId::ZERO,
                valid_to: None,
            },
        )
        .unwrap();
        db.flush(edge_space).unwrap();
        db.flush(ENDPOINT_INDEX_SPACE).unwrap();

        assert!(
            db.query_hyperedges_for_endpoint(edge_space, &n0, None)
                .unwrap()
                .len()
                >= 1,
            "index must list edges incident on n0"
        );
        assert!(
            db.query_hyperedges_for_endpoint(edge_space, &n1, None)
                .unwrap()
                .len()
                >= 2,
            "index must list all edges incident on n1"
        );

        let depth2 = db
            .traverse(
                edge_space,
                &TraversalSpec {
                    start: n0.clone(),
                    max_depth: 2,
                    follow_kinds: None,
                    as_of: None,
                },
            )
            .unwrap();
        assert!(
            depth2.edges.iter().any(|e| e.id == HyperedgeId(2)),
            "expected edge n1–n2 in subgraph, edges={:?}",
            depth2.edges.iter().map(|e| e.id.0).collect::<Vec<_>>()
        );
        assert!(
            depth2.nodes.iter().any(|n| n.space == SpaceId(12)),
            "expected n2 at depth 2, nodes={:?}",
            depth2
                .nodes
                .iter()
                .map(|n| (n.space.0, n.node.coords.clone()))
                .collect::<Vec<_>>()
        );
        assert!(!depth2.nodes.iter().any(|n| n.space == SpaceId(13)));

        let depth1 = db
            .traverse(
                edge_space,
                &TraversalSpec {
                    start: n0,
                    max_depth: 1,
                    follow_kinds: None,
                    as_of: None,
                },
            )
            .unwrap();
        assert!(depth1.nodes.iter().any(|n| n.space == SpaceId(11)));
        assert!(!depth1.nodes.iter().any(|n| n.space == SpaceId(12)));
    }

    #[test]
    fn compact_space_reduces_block_count() {
        use crate::infinitedb_storage::compaction::CompactionConfig;
        let (mut db, _dir) = open_tmp();
        let space = SpaceId(70);
        db.register_space(SpaceConfig::new(space, "data", 1)).unwrap();
        for i in 0..4u32 {
            db.insert(space, DimensionVector::new(vec![i]), vec![i as u8]).unwrap();
            db.flush(space).unwrap();
        }
        assert_eq!(db.snapshots.get(&space.0).unwrap().blocks.len(), 4);
        let result = db
            .compact_space(
                space,
                &CompactionConfig {
                    max_records_per_block: 16,
                    retain_history: true,
                },
            )
            .unwrap();
        assert_eq!(result.superseded.len(), 4);
        assert_eq!(result.new_blocks.len(), 1);
        assert_eq!(db.snapshots.get(&space.0).unwrap().blocks.len(), 1);
        let records = db.query(space, None).unwrap();
        assert_eq!(records.len(), 4);
    }

    #[test]
    fn create_branch_succeeds() {
        let (mut db, _dir) = open_tmp();
        let main = BranchId(1);
        let feature = db.create_branch("feature", main).unwrap();
        assert_ne!(feature, main);
    }

    fn clustered_edge(id: u64, a: Vec<u32>, b: Vec<u32>) -> Hyperedge {
        Hyperedge {
            id: HyperedgeId(id),
            kind: HyperedgeKind::new("near"),
            endpoints: vec![
                EndpointRef {
                    role: EndpointRole::new("a"),
                    space: SpaceId(1),
                    node: DimensionVector::new(a),
                },
                EndpointRef {
                    role: EndpointRole::new("b"),
                    space: SpaceId(1),
                    node: DimensionVector::new(b),
                },
            ],
            weight_milli: None,
            metadata: Default::default(),
            valid_from: RevisionId::ZERO,
            valid_to: None,
        }
    }

    #[test]
    fn centroid_keying_clusters_nearby_edges() {
        // Edges over nearby endpoints should get closer Hilbert keys than edges
        // over distant endpoints.
        let a = clustered_edge(1, vec![10, 10], vec![12, 12]); // centroid ~ (11,11)
        let b = clustered_edge(2, vec![13, 13], vec![15, 15]); // centroid ~ (14,14)
        let c = clustered_edge(3, vec![240, 240], vec![250, 250]); // centroid ~ (245,245)

        let pa = centroid_hyperedge_point(&a).unwrap();
        let pb = centroid_hyperedge_point(&b).unwrap();
        let pc = centroid_hyperedge_point(&c).unwrap();

        let ka = hilbert_key_standard(&pa);
        let kb = hilbert_key_standard(&pb);
        let kc = hilbert_key_standard(&pc);

        let near = ka.abs_diff(kb);
        let far = ka.abs_diff(kc);
        assert!(
            near < far,
            "nearby edges should cluster: near={near} far={far}"
        );
    }

    #[test]
    fn centroid_keyed_edges_are_addressable_by_id() {
        let (mut db, _dir) = open_tmp();
        let edge_space = SpaceId(80);
        db.register_space(
            SpaceConfig::new(edge_space, "centroid_edges", 4).with_centroid_keying(),
        )
        .unwrap();

        let edge = clustered_edge(7, vec![20, 30], vec![22, 34]);
        db.insert_hyperedge(edge_space, edge.clone()).unwrap();
        db.flush(edge_space).unwrap();

        // Fetch by id resolves through the locator (point is not derivable from id).
        let fetched = db.fetch_hyperedge_by_id(edge_space, HyperedgeId(7), None).unwrap();
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().id, HyperedgeId(7));

        // The endpoint index still resolves incident edges.
        let incident = db
            .query_hyperedges_for_endpoint(edge_space, &edge.endpoints[0], None)
            .unwrap();
        assert!(incident.iter().any(|e| e.id == HyperedgeId(7)));

        // Delete removes it from id lookup.
        db.delete_hyperedge(edge_space, HyperedgeId(7)).unwrap();
        db.flush(edge_space).unwrap();
        let after = db.fetch_hyperedge_by_id(edge_space, HyperedgeId(7), None).unwrap();
        assert!(after.is_none(), "deleted edge must not be addressable");
    }

    #[test]
    fn hyperedge_insert_query_and_delete() {
        let (mut db, _dir) = open_tmp();
        let edge_space = SpaceId(77);
        db.register_space(SpaceConfig::new(edge_space, "hyperedges", 2))
        .unwrap();
        let edge = Hyperedge {
            id: HyperedgeId(42),
            kind: HyperedgeKind::new("beam.bears_on"),
            endpoints: vec![
                EndpointRef {
                    role: EndpointRole::new("parent"),
                    space: SpaceId(10),
                    node: DimensionVector::new(vec![100]),
                },
                EndpointRef {
                    role: EndpointRole::new("support"),
                    space: SpaceId(11),
                    node: DimensionVector::new(vec![200]),
                },
            ],
            weight_milli: Some(1_000),
            metadata: std::collections::BTreeMap::new(),
            valid_from: RevisionId::ZERO,
            valid_to: None,
        };
        db.insert_hyperedge(edge_space, edge.clone()).unwrap();
        db.flush(edge_space).unwrap();
        let by_kind = db
            .query_hyperedges_by_kind(edge_space, "beam.bears_on", None)
            .unwrap();
        assert_eq!(by_kind.len(), 1);
        assert_eq!(by_kind[0].id.0, edge.id.0);
        db.delete_hyperedge(edge_space, edge.id).unwrap();
        let after_delete = db.query_hyperedges(edge_space, None).unwrap();
        assert!(after_delete.is_empty());
    }

    #[test]
    fn signal_scope_and_range_queries() {
        let (mut db, _dir) = open_tmp();
        let signal_space = SpaceId(88);
        db.register_space(SpaceConfig::new(signal_space, "beam_signals", 3))
        .unwrap();

        let scope = SignalScope {
            parent_prefix: DimensionVector::new(vec![7]),
            total_dims: 3,
        };
        db.insert_signal_sample(
            signal_space,
            SignalSample {
                signal_id: SignalId(1),
                kind: SignalKind::new("beam.bending_moment"),
                scope: scope.clone(),
                local_coords: vec![0, 0],
                value_milli: 10_000,
                source_revision: None,
                constraint: None,
            },
        )
        .unwrap();
        db.insert_signal_sample(
            signal_space,
            SignalSample {
                signal_id: SignalId(1),
                kind: SignalKind::new("beam.bending_moment"),
                scope,
                local_coords: vec![5, 0],
                value_milli: 20_000,
                source_revision: None,
                constraint: None,
            },
        )
        .unwrap();
        db.flush(signal_space).unwrap();

        let scoped = db.query_signal_scope(signal_space, &[7], None).unwrap();
        assert_eq!(scoped.len(), 2);
        let ranged = db
            .query_signal_range(signal_space, &[7], &[0, 0], &[2, u32::MAX], None)
            .unwrap();
        assert_eq!(ranged.len(), 1);
    }

    #[test]
    fn adapter_wrappers_accept_typed_kinds_and_space_binding() {
        let (mut db, _dir) = open_tmp();
        let edge_space = SpaceId(177);
        db.register_space(SpaceConfig::new(edge_space, "adapter_edges", 2))
        .unwrap();
        db.register_space(SpaceConfig::new(
            BeamSignalSpace::SPACE_ID,
            BeamSignalSpace::SPACE_NAME,
            BeamSignalSpace::DIMS,
        ))
        .unwrap();

        let mut catalog = KindCatalog::new(UnknownKindPolicy::RejectUnknown);
        catalog.register_edge_kind(KindDefinition::new("beam.bears_on"));
        catalog.register_endpoint_role(KindDefinition::new("parent"));
        catalog.register_endpoint_role(KindDefinition::new("support"));
        catalog.register_signal_kind(KindDefinition::new("beam.bending_moment"));

        db.insert_hyperedge_typed(
            edge_space,
            HyperedgeId(900),
            BeamKinds::BearsOn,
            vec![
                AdapterEndpoint::new("parent", SpaceId(1), DimensionVector::new(vec![10])),
                AdapterEndpoint::new("support", SpaceId(2), DimensionVector::new(vec![20])),
            ],
            Some(1000),
            std::collections::BTreeMap::new(),
            None,
            Some(&catalog),
        )
        .unwrap();
        db.flush(edge_space).unwrap();
        let edges = db
            .query_hyperedges_by_kind_typed(edge_space, BeamKinds::BearsOn, None)
            .unwrap();
        assert_eq!(edges.len(), 1);

        db.insert_signal_sample_typed::<BeamSignalSpace, _>(
            SignalId(1),
            BeamKinds::BendingMoment,
            DimensionVector::new(vec![7]),
            vec![0, 0],
            1234,
            None,
            None,
            Some(&catalog),
        )
        .unwrap();
    }

    #[test]
    fn adapter_rejects_unknown_kind_under_reject_policy() {
        let (mut db, _dir) = open_tmp();
        let edge_space = SpaceId(178);
        db.register_space(SpaceConfig::new(edge_space, "adapter_edges2", 2)).unwrap();

        let catalog = KindCatalog::new(UnknownKindPolicy::RejectUnknown);
        let err = db
            .insert_hyperedge_typed(
                edge_space,
                HyperedgeId(901),
                "unknown.edge.kind",
                vec![
                    AdapterEndpoint::new("parent", SpaceId(1), DimensionVector::new(vec![1])),
                    AdapterEndpoint::new("support", SpaceId(2), DimensionVector::new(vec![2])),
                ],
                None,
                std::collections::BTreeMap::new(),
                None,
                Some(&catalog),
            )
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn adapter_rejects_space_binding_dim_mismatch() {
        struct WrongDimSpace;
        impl SpaceBinding for WrongDimSpace {
            const SPACE_ID: SpaceId = SpaceId(188);
            const DIMS: usize = 4;
        }
        let (mut db, _dir) = open_tmp();
        db.register_space(SpaceConfig::new(SpaceId(188), "wrong_dim", 3)).unwrap();
        let err = db
            .insert_signal_sample_typed::<WrongDimSpace, _>(
                SignalId(2),
                "beam.any",
                DimensionVector::new(vec![7]),
                vec![0, 0],
                999,
                None,
                None,
                None,
            )
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[cfg(feature = "sync")]
    struct AckTransport;
    #[cfg(feature = "sync")]
    impl SyncTransport for AckTransport {
        fn push_batch(&self, batch: &[SyncEnvelope]) -> Result<Vec<SyncResult>, String> {
            Ok(batch
                .iter()
                .map(|item| SyncResult::Ack { op_id: item.op_id })
                .collect())
        }
    }

    #[cfg(feature = "sync")]
    struct FlakyTransport;
    #[cfg(feature = "sync")]
    impl SyncTransport for FlakyTransport {
        fn push_batch(&self, batch: &[SyncEnvelope]) -> Result<Vec<SyncResult>, String> {
            Ok(batch
                .iter()
                .map(|item| SyncResult::Retry {
                    op_id: item.op_id,
                    error: "offline".to_string(),
                })
                .collect())
        }
    }

    #[cfg(feature = "sync")]
    struct StaleConflictTransport;
    #[cfg(feature = "sync")]
    impl SyncTransport for StaleConflictTransport {
        fn push_batch(&self, batch: &[SyncEnvelope]) -> Result<Vec<SyncResult>, String> {
            Ok(batch
                .iter()
                .map(|item| SyncResult::ConflictStale {
                    op_id: item.op_id,
                    reason: "stale write".to_string(),
                })
                .collect())
        }
    }

    #[cfg(feature = "sync")]
    #[test]
    fn outbox_survives_restart() {
        let dir = TempDir::new().unwrap();
        let space = SpaceId(1);
        {
            let mut db = LegacyDb::open(dir.path()).unwrap();
            db.insert(space, DimensionVector::new(vec![1, 2]), vec![3]).unwrap();
            assert_eq!(db.sync_pending_count(), 1);
        }
        let db = LegacyDb::open(dir.path()).unwrap();
        assert_eq!(db.sync_pending_count(), 1);
    }

    #[cfg(feature = "sync")]
    #[test]
    fn offline_queue_then_manual_sync_drains() {
        let (mut db, _dir) = open_tmp();
        let space = SpaceId(1);
        db.insert(space, DimensionVector::new(vec![10, 10]), vec![7]).unwrap();
        let retry_report = db.sync_now(&FlakyTransport, 32).unwrap();
        assert_eq!(retry_report.retried, 1);
        assert_eq!(db.sync_pending_count(), 1);
        std::thread::sleep(Duration::from_millis(2100));
        let ack_report = db.sync_now(&AckTransport, 32).unwrap();
        assert_eq!(ack_report.acked, 1);
        assert_eq!(db.sync_pending_count(), 0);
    }

    #[cfg(feature = "sync")]
    #[test]
    fn stale_conflict_is_dropped_under_lww() {
        let (mut db, _dir) = open_tmp();
        let space = SpaceId(1);
        db.insert(space, DimensionVector::new(vec![11, 11]), vec![8]).unwrap();
        let report = db.sync_now(&StaleConflictTransport, 32).unwrap();
        assert_eq!(report.dropped_stale, 1);
        assert_eq!(db.sync_pending_count(), 0);
    }

    #[cfg(feature = "sync")]
    #[test]
    fn background_worker_retries_and_acks() {
        let (mut db, _dir) = open_tmp();
        let space = SpaceId(1);
        db.insert(space, DimensionVector::new(vec![20, 20]), vec![1]).unwrap();
        db.start_background_sync(
            Arc::new(AckTransport),
            Duration::from_millis(20),
            16,
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(120));
        db.stop_background_sync();
        assert_eq!(db.sync_pending_count(), 0);
    }
}

