//! [`InfiniteDb`] — fire-and-forget writes with per-space I/O (v3) or global I/O (v2).

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bincode::{config::standard, decode_from_slice, encode_to_vec};
use parking_lot::{Mutex, RwLock};

use crate::engine::branch_overlay::{BranchOverlayStore, OverlayKey};
use crate::engine::compactor::CompactionPolicyOverrides;
use crate::engine::coordinator::SpaceCoordinator;
use crate::engine::derivation::{
    record_in_derivation_delta, AssertionEvent, DerivationBackpressurePolicy, DerivationBus,
    DerivationSink, DerivationStats, EdgeLocatorSubscriber, EndpointIndexSubscriber,
    FlowVectorSubscriber, WatermarkRegistry,
};
use crate::engine::flow_vector::{
    default_flow_vector_quantization, edge_id_from_flow_vector_index_record,
    prepare_flow_vector_derivation,
};
use crate::engine::staleness_closure::{forward_stale_closure, staleness_seed_endpoints};
use crate::engine::error::{engine_to_io, EngineError};
use crate::engine::error_record::{
    decode_error_record_payload, operation_record_checkpoint_collision,
    operation_record_from_import, operation_record_interrupted_intent, prepare_error_tombstone,
    prepare_error_write, revision_range_from_engine,
};
use crate::engine::intent_checkpoint::IntentCheckpoint;
use crate::engine::frame::{
    apply_judgment_overlay, resolve_visibility_per_source, FrameResolvedHyperedge,
    FrameTraversalResult,
};
use crate::engine::query::FrameTimePin;
use crate::engine::import::{HyperedgeImportResult, HyperedgeImportSession, ImportBudget};
use crate::engine::judgment::{
    decode_judgment_record, judgment_id_from_index_payload, prepare_judgment_writes,
};
use crate::engine::hilbert_coordinator::HilbertCoordinator;
use crate::engine::hilbert_live_tails::HilbertLiveTails;
use crate::engine::io_thread::{open_io_pipeline, IoStats, IoThreadConfig, IoThreadHandle};
use crate::engine::live_tail::LiveTailView;
use crate::engine::merge::merge_branches;
use crate::engine::query::{
    query_bbox, query_inner, query_plan_stats, reset_query_plan_stats, snapshots_map_for_persist,
    space_key, QueryPlanStats,
};
use crate::engine::snapshot_store::SnapshotStore;
use crate::engine::space_live_tails::SpaceLiveTails;
use crate::engine::session::{DurableIntent, SessionWatermarks, VersionVector, WriteSession};
use crate::engine::timed_fast_path::{DurabilityMedium, TimedFastPathPolicy};
use crate::infinitedb_storage::session_fast_segment::FastSealOutcome;
use crate::engine::session_wal_store::{
    load_session_wal_meta, merge_recovered_entries, persist_session_wal_meta, SessionWalStore,
    wal_entry_revision,
};
use crate::engine::watermark::{FailedRevision, RevisionRange};
use crate::engine::write_queue::{WriteJob, WriteQueueSender};
use crate::engine::endpoint_index_migrate::edge_spaces_from_registry;
use crate::engine::hypergraph::{
    self, decode_edge_record, endpoint_index_space_config, filter_edges_by_direction,
    incident_edge_ids_directed, incident_edge_degree, incident_edge_ids_from_records,
    partition_incident_ids_by_layout, plan_v1_to_v2_index_rewrite, prepare_assertion_tombstone,
    prepare_assertion_write, prepare_deletes, prepare_index_derivation, prepare_writes,
    registry_index_layout, rows_to_records, HypergraphWriteRow,
};
use crate::engine::traversal::run_traversal;
use crate::infinitedb_core::{
    address::{Address, DimensionVector, RevisionId, SpaceId},
    adapter::{AdapterEndpoint, KindLabel},
    block::Record,
    branch::{Branch, BranchId, BranchRegistry},
    endpoint_index::{endpoint_index_layout_from_registry, ENDPOINT_INDEX_SPACE},
    error_kind_catalog::ErrorKindCatalog,
    error_record::{OperationErrorRecord, OperationRevisionRange},
    judgment::{
        ArbiterId, ArbiterStream, JudgmentId, JudgmentRecord, JudgmentValidationError,
        SubjectIdentity, SubjectKind, SubjectPin,
    },
    computation::ComputationValidationError,
    flow_vector::{
        quantize_direction, FlowVectorRecord, QuantizedDirection,
    },
    flow_vector_index::{
        direction_in_region, flow_vector_index_space_config, pad_flow_vector_index_bbox,
        FLOW_VECTOR_INDEX_SPACE,
    },
    judgment_index::{
        JUDGMENT_INDEX_SPACE, index_matches_subject_prefix, judgment_index_space_config,
        subject_spatial_prefix,
    },
    staleness_closure::{check_computation_freshness, FreshnessReport, StaleTarget},
    frame::{
        merge_admission_specs, is_testimony_space, AssertionScope, FrameDefinition,
        FrameRegisterRequest, FrameValidationError, JudgmentOverlayLayer, TestimonySource,
        record_admitted_by_source,
    },
    frame_query::{FrameQuery, FrameQueryOptions, FrameVersionPin},
    intent_checkpoint::IntentOperationKind,
    provenance::FrameId,
    staleness::{validate_authoring_provenance, ConsultedFrame},
    hilbert_key::{CachedHilbertKey, HilbertKey},
    hlc::{SessionId, GLOBAL_SESSION},
    hyperedge::{EndpointRef, Hyperedge, HyperedgeId, HyperedgeKind},
    kind_catalog::KindCatalog,
    merge::{MergeConflict, MergeResult, MergeStrategy},
    persisted_counters::PersistedCounters,
    query::{DirectionFilter, QueryOptions},
    space::{CompactionPolicy, EndpointIndexLayout, SpaceConfig, SpaceRegistry},
    snapshot::SnapshotId,
    traversal::{
        hypergraph_acyclic_for_kinds, FrameTraversalSpec, TraversalResult, TraversalSpec,
    },
};
use crate::infinitedb_storage::{
    format::{FormatVersion, FORMAT_VERSION_V2, FORMAT_VERSION_V3, FORMAT_VERSION_V4, FORMAT_VERSION_V5},
    nvme::BlockStore,
    wal::WalEntry,
};

/// Options for opening [`InfiniteDb`] (formats v2–v4).
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// I/O thread queue depth, staging, and durability settings.
    pub io_thread: IoThreadConfig,
    /// In-memory block cache size in bytes for the block store.
    pub block_cache_bytes: usize,
    /// When `None`, new databases use format v4 (Hilbert shards + branches).
    pub format_version: Option<u32>,
    /// Derivation bus backpressure thresholds (M4).
    pub derivation: DerivationBackpressurePolicy,
    /// Timed session fast path (Phase 7, default off).
    pub timed_fast_path: TimedFastPathPolicy,
}

impl Default for OpenOptions {
    fn default() -> Self {
        let io_thread = IoThreadConfig::default();
        Self {
            io_thread: io_thread.clone(),
            block_cache_bytes: 10 * 1024 * 1024,
            format_version: None,
            derivation: DerivationBackpressurePolicy::default(),
            timed_fast_path: TimedFastPathPolicy::from_io_config(&io_thread),
        }
    }
}

impl OpenOptions {
    /// Open or create a database at `dir` using these options.
    pub fn open<P: AsRef<Path>>(&self, dir: P) -> io::Result<InfiniteDb> {
        InfiniteDb::open_with_options(dir, self)
    }
}

pub(crate) enum WriteBackend {
    /// Format v2: single global I/O thread.
    V2 {
        queue: WriteQueueSender,
        io_handle: Mutex<IoThreadHandle>,
    },
    /// Format v3: one I/O thread per space.
    V3 {
        coordinator: SpaceCoordinator,
    },
    /// Format v4: Hilbert shards per space + branch overlays.
    V4 {
        coordinator: HilbertCoordinator,
    },
}

/// Thread-safe embedded database with concurrent reads and fire-and-forget writes.
pub struct InfiniteDb {
    root: PathBuf,
    format_version: u32,
    pub(crate) store: Arc<BlockStore>,
    pub(crate) spaces: Arc<RwLock<SpaceRegistry>>,
    branches: Arc<RwLock<BranchRegistry>>,
    pub(crate) snapshots: Arc<SnapshotStore>,
    pub(crate) session_watermarks: Arc<SessionWatermarks>,
    default_write_session: WriteSession,
    session_wal_store: Arc<SessionWalStore>,
    timed_fast_path: TimedFastPathPolicy,
    compaction_overrides: CompactionPolicyOverrides,
    next_block_id: Arc<AtomicU64>,
    next_snapshot_id: Arc<AtomicU64>,
    next_branch_id: Arc<AtomicU64>,
    pub(crate) branch_overlays: Arc<BranchOverlayStore>,
    #[cfg(feature = "sync")]
    conflicts: Arc<crate::infinitedb_sync::conflict_queue::ConflictQueue>,
    backend: Arc<Mutex<WriteBackend>>,
    derivation: Arc<DerivationBus>,
    arbiter_streams: Arc<RwLock<HashMap<ArbiterId, ArbiterStream>>>,
    frames: Arc<RwLock<HashMap<FrameId, FrameDefinition>>>,
    next_frame_id: Arc<AtomicU64>,
    v2_live_tail: Option<Arc<LiveTailView>>,
    v3_space_tails: Option<Arc<SpaceLiveTails>>,
    v4_hilbert_tails: Option<Arc<HilbertLiveTails>>,
}

/// Applies derived index rows through the shared write backend.
struct DbDerivationSink {
    spaces: Arc<RwLock<SpaceRegistry>>,
    backend: Arc<Mutex<WriteBackend>>,
}

impl DerivationSink for DbDerivationSink {
    fn apply_derived_rows(
        &self,
        rows: Vec<HypergraphWriteRow>,
        source_revision: RevisionId,
    ) -> Result<(), EngineError> {
        if rows.is_empty() {
            return Ok(());
        }
        let records = rows
            .iter()
            .map(|row| Record {
                address: Address::new(row.space, row.point.clone()),
                revision: source_revision,
                data: row.data.clone(),
                tombstone: row.tombstone,
                hilbert_key: CachedHilbertKey::UNSET,
            })
            .collect::<Vec<_>>();
        let spaces = self.spaces.read();
        let mut jobs = Vec::with_capacity(records.len());
        for record in records {
            let hilbert_key = HilbertKey(space_key(
                &spaces,
                record.address.space,
                &record.address.point,
            ));
            let entry = if record.tombstone {
                WalEntry::Tombstone {
                    address: record.address.clone(),
                    revision: record.revision,
                }
            } else {
                WalEntry::Write {
                    address: record.address.clone(),
                    revision: record.revision,
                    data: record.data,
                }
            };
            jobs.push(WriteJob::main(record.revision, entry, hilbert_key));
        }
        drop(spaces);
        let mut backend = self.backend.lock();
        match &mut *backend {
            WriteBackend::V4 { coordinator } => coordinator.enqueue_batch(jobs)?,
            WriteBackend::V3 { coordinator } => coordinator.enqueue_batch(jobs)?,
            WriteBackend::V2 { queue, .. } => {
                for job in jobs {
                    queue.enqueue_write(job)?;
                }
            }
        }
        Ok(())
    }
}

impl InfiniteDb {
    /// Open or create a database at `dir` with default [`OpenOptions`].
    pub fn open<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        OpenOptions::default().open(dir)
    }

    /// Open or create a database at `dir` with explicit tuning and format version.
    pub fn open_with_options<P: AsRef<Path>>(dir: P, options: &OpenOptions) -> io::Result<Self> {
        let root = dir.as_ref().to_path_buf();
        let store = Arc::new(BlockStore::open_with_cache(
            root.clone(),
            options.block_cache_bytes,
        )?);

        let format_version = match FormatVersion::read_from_meta(&root.join("meta"))? {
            Some(v) => v.0,
            None => options.format_version.unwrap_or(FORMAT_VERSION_V4),
        };

        match format_version {
            FORMAT_VERSION_V2 | FORMAT_VERSION_V3 | FORMAT_VERSION_V4 | FORMAT_VERSION_V5 => {}
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported concurrent format version {other}"),
                ));
            }
        }

        if FormatVersion::read_from_meta(&root.join("meta"))?.is_none() {
            FormatVersion(format_version).write_to_meta(&root.join("meta"))?;
            if format_version == FORMAT_VERSION_V2 {
                std::fs::create_dir_all(root.join("hot"))?;
                std::fs::create_dir_all(root.join("wal"))?;
            } else {
                std::fs::create_dir_all(root.join("spaces"))?;
            }
        }

        let branch_overlays = Arc::new(BranchOverlayStore::new());
        if format_version == FORMAT_VERSION_V4 {
            branch_overlays.replay_all(&root)?;
        }
        if let Ok(bytes) = store.read_meta("branch_bases.bin") {
            if let Ok((bases, _)) = decode_from_slice::<
                std::collections::BTreeMap<(u64, u64), crate::infinitedb_core::snapshot::Snapshot>,
                _,
            >(&bytes, standard())
            {
                branch_overlays.import_bases(bases);
            }
        }
        #[cfg(feature = "sync")]
        let conflicts = Arc::new(crate::infinitedb_sync::conflict_queue::ConflictQueue::open(&root)?);

        let (spaces, branches, snapshots, next_rev, next_block, next_snap, next_branch, next_session) =
            load_meta(&store).unwrap_or_else(default_meta);

        let spaces = Arc::new(RwLock::new(spaces));
        let branches = Arc::new(RwLock::new(branches));
        let snapshots = Arc::new(SnapshotStore::new(snapshots));
        let session_wal_meta = load_session_wal_meta(&root.join("meta"));
        let session_wal_store = SessionWalStore::open(root.clone(), session_wal_meta.clone());
        let recovered_session_wal = session_wal_store.recover_all();

        let session_watermarks = SessionWatermarks::new(next_rev, next_session);
        session_watermarks.hydrate_from_wal_meta(&session_wal_meta);
        let default_write_session =
            WriteSession::implicit_global(Arc::clone(&session_watermarks));
        let compaction_overrides: CompactionPolicyOverrides =
            Arc::new(Mutex::new(std::collections::HashMap::new()));
        let next_block_id = Arc::new(AtomicU64::new(next_block));
        let next_snapshot_id = Arc::new(AtomicU64::new(next_snap));
        let next_branch_id = Arc::new(AtomicU64::new(next_branch));

        if branches.read().get_by_name("main").is_none() {
            let snap_id = SnapshotId(next_snap);
            let _ = branches.write().insert(Branch {
                id: BranchId(1),
                name: "main".to_string(),
                head: snap_id,
                parent: None,
                forked_at: RevisionId::ZERO,
            });
        }

        let (backend, v2_live_tail, v3_space_tails, v4_hilbert_tails) =
            if format_version == FORMAT_VERSION_V4 {
                let coordinator = HilbertCoordinator::new(
                    root.clone(),
                    Arc::clone(&store),
                    Arc::clone(&snapshots),
                    Arc::clone(&branch_overlays),
                    Arc::clone(&spaces),
                    Arc::clone(&next_block_id),
                    options.io_thread.clone(),
                    Arc::clone(&session_watermarks),
                    Arc::clone(&compaction_overrides),
                );
                coordinator.bootstrap_registered_spaces()?;
                coordinator.sync_all()?;
                let tails = coordinator.live_tails_arc();
                (
                    WriteBackend::V4 { coordinator },
                    None,
                    None,
                    Some(tails),
                )
            } else if format_version == FORMAT_VERSION_V3 {
                let coordinator = SpaceCoordinator::new(
                    root.clone(),
                    Arc::clone(&store),
                    Arc::clone(&snapshots),
                    Arc::clone(&spaces),
                    Arc::clone(&next_block_id),
                    options.io_thread.clone(),
                    Arc::clone(&session_watermarks),
                    Arc::clone(&compaction_overrides),
                    Some(Arc::clone(&branch_overlays)),
                );
                coordinator.bootstrap_registered_spaces()?;
                coordinator.sync_all()?;
                let tails = coordinator.live_tails_arc();
                (
                    WriteBackend::V3 { coordinator },
                    None,
                    Some(tails),
                    None,
                )
            } else {
                let live_tail = Arc::new(LiveTailView::new());
                let (queue, io_handle) = open_io_pipeline(
                    root.clone(),
                    Arc::clone(&store),
                    Arc::clone(&snapshots),
                    Arc::clone(&live_tail),
                    Arc::clone(&spaces),
                    Arc::clone(&next_block_id),
                    options.io_thread.clone(),
                    Arc::clone(&session_watermarks),
                    Arc::clone(&compaction_overrides),
                    Some(Arc::clone(&branch_overlays)),
                );
                (
                    WriteBackend::V2 {
                        queue,
                        io_handle: Mutex::new(io_handle),
                    },
                    Some(live_tail),
                    None,
                    None,
                )
            };

        let backend = Arc::new(Mutex::new(backend));
        let derivation_watermarks = Arc::new(WatermarkRegistry::new());
        derivation_watermarks.register("endpoint_index", RevisionId::ZERO);
        derivation_watermarks.register("edge_locator", RevisionId::ZERO);
        derivation_watermarks.register("flow_vector_index", RevisionId::ZERO);
        let subscribers: Vec<Box<dyn crate::engine::derivation::DerivationSubscriber>> = vec![
            Box::new(EndpointIndexSubscriber::new(Arc::clone(&spaces))),
            Box::new(EdgeLocatorSubscriber),
            Box::new(FlowVectorSubscriber),
        ];
        let sink = Arc::new(DbDerivationSink {
            spaces: Arc::clone(&spaces),
            backend: Arc::clone(&backend),
        });
        let derivation = Arc::new(DerivationBus::new(
            options.derivation.clone(),
            Arc::clone(&derivation_watermarks),
            subscribers,
            sink,
        ));

        let arbiter_streams = Arc::new(RwLock::new(HashMap::new()));
        if let Ok(bytes) = store.read_meta("arbiter_streams.bin") {
            if let Ok((loaded, _)) =
                decode_from_slice::<HashMap<ArbiterId, ArbiterStream>, _>(&bytes, standard())
            {
                *arbiter_streams.write() = loaded;
            }
        }

        let frames = Arc::new(RwLock::new(HashMap::new()));
        let mut next_frame = 1u64;
        if let Ok(bytes) = store.read_meta("frames.bin") {
            if let Ok((loaded, _)) =
                decode_from_slice::<HashMap<FrameId, FrameDefinition>, _>(&bytes, standard())
            {
                for id in loaded.keys() {
                    next_frame = next_frame.max(id.0.saturating_add(1));
                }
                *frames.write() = loaded;
            }
        }
        let next_frame_id = Arc::new(AtomicU64::new(next_frame));

        let db = Self {
            root,
            format_version,
            store,
            spaces,
            branches,
            snapshots,
            session_watermarks,
            default_write_session,
            session_wal_store,
            timed_fast_path: options.timed_fast_path.clone(),
            compaction_overrides,
            next_block_id,
            next_snapshot_id,
            next_branch_id,
            branch_overlays,
            #[cfg(feature = "sync")]
            conflicts,
            backend,
            derivation,
            arbiter_streams,
            frames,
            next_frame_id,
            v2_live_tail,
            v3_space_tails,
            v4_hilbert_tails,
        };
        db.apply_recovered_session_wal(&recovered_session_wal)?;
        db.apply_recovered_fast_segments()?;
        db.recover_derivation_on_open()?;
        Ok(db)
    }

    /// Replay committed session-WAL intent groups recovered on open (HLC merge order).
    fn apply_recovered_session_wal(
        &self,
        recovered: &[crate::engine::session_wal_store::SessionWalRecovery],
    ) -> io::Result<()> {
        let entries = merge_recovered_entries(recovered);
        if !entries.is_empty() {
            self.replay_session_wal_entries(&entries)?;
        }
        for batch in recovered {
            if !batch.uncommitted.is_empty() {
                self.persist_uncommitted_session_fragments(batch.session, &batch.uncommitted)?;
            }
            if !batch.committed_groups.is_empty() || !batch.uncommitted.is_empty() {
                self.session_wal_store
                    .reset_after_recovery(batch.session)
                    .map_err(EngineError::from)
                    .map_err(engine_to_io)?;
            }
        }
        Ok(())
    }

    fn apply_recovered_fast_segments(&self) -> io::Result<()> {
        let recovered = self.session_wal_store.recover_fast_segments();
        for batch in recovered {
            if !batch.entries.is_empty() {
                self.persist_uncommitted_session_fragments(batch.session, &batch.entries)?;
            }
            self.session_wal_store
                .reset_fast_after_recovery(batch.session)
                .map_err(EngineError::from)
                .map_err(engine_to_io)?;
        }
        Ok(())
    }

    fn replay_session_wal_entries(&self, entries: &[WalEntry]) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let spaces = self.spaces.read();
        let mut jobs = Vec::with_capacity(entries.len());
        for entry in entries {
            let revision = wal_entry_revision(entry).unwrap_or(RevisionId::ZERO);
            self.session_watermarks
                .watermark_for(crate::infinitedb_core::hlc::SessionId(revision.session()))
                .register_outstanding(revision);
            let hilbert_key = match entry {
                WalEntry::Write { address, .. } | WalEntry::Tombstone { address, .. } => {
                    HilbertKey(space_key(&spaces, address.space, &address.point))
                }
                _ => continue,
            };
            jobs.push(WriteJob::main(revision, entry.clone(), hilbert_key));
        }
        drop(spaces);
        self.enqueue_batch(jobs)?;
        self.sync()?;
        Ok(())
    }

    fn persist_uncommitted_session_fragments(
        &self,
        session: crate::infinitedb_core::hlc::SessionId,
        entries: &[WalEntry],
    ) -> io::Result<()> {
        let revisions: Vec<RevisionId> = entries.iter().filter_map(wal_entry_revision).collect();
        let Some(first) = revisions.iter().min().copied() else {
            return Ok(());
        };
        let Some(last) = revisions.iter().max().copied() else {
            return Ok(());
        };
        let source_space = entries
            .iter()
            .find_map(|e| match e {
                WalEntry::Write { address, .. } | WalEntry::Tombstone { address, .. } => {
                    Some(address.space)
                }
                _ => None,
            })
            .unwrap_or(SpaceId(0));
        let record = operation_record_interrupted_intent(
            source_space,
            session.0,
            first,
            last,
            entries.len(),
        );
        let _ = self
            .persist_operation_errors(source_space, record)
            .map_err(engine_to_io)?;
        self.sync()?;
        Ok(())
    }

    /// Head snapshot pointer for `branch`.
    pub fn branch_head(&self, branch: BranchId) -> Option<SnapshotId> {
        self.branches.read().get(branch).map(|b| b.head)
    }

    /// Resolve a branch id by name.
    pub fn branch_id(&self, name: &str) -> Option<BranchId> {
        self.branches.read().get_by_name(name).map(|b| b.id)
    }

    /// Conflict queue populated during sync replication (requires `sync` feature).
    #[cfg(feature = "sync")]
    pub fn conflicts(&self) -> &crate::infinitedb_sync::conflict_queue::ConflictQueue {
        &self.conflicts
    }

    /// On-disk format version (2, 3, or 4) for this database directory.
    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    /// Register a new space and persist catalog metadata. Required before writes to that space.
    ///
    /// Unless [`SpaceConfig::without_error_space`] is set, auto-registers a companion
    /// `{name}_errors` space for operation-level error records (M5).
    pub fn register_space(&self, mut config: SpaceConfig) -> Result<(), EngineError> {
        if config.bits_per_dim == 0 {
            return Err(EngineError::InvalidSpaceConfig {
                message: "bits_per_dim must be at least 1".into(),
            });
        }
        if config.dims as u32 * config.bits_per_dim > 128 {
            return Err(EngineError::InvalidSpaceConfig {
                message: format!(
                    "dims * bits_per_dim must be <= 128 (got {} * {})",
                    config.dims, config.bits_per_dim
                ),
            });
        }
        let needs_error_space = !config.skip_error_space
            && config.id != ENDPOINT_INDEX_SPACE
            && config.id != JUDGMENT_INDEX_SPACE
            && config.id != FLOW_VECTOR_INDEX_SPACE
            && config.error_space.is_none();
        let space_id = config.id.0;
        if needs_error_space {
            let err_id = SpaceRegistry::derive_error_space_id(config.id);
            let mut registry = self.spaces.write();
            if registry.get(err_id).is_none() {
                let err_config = SpaceConfig::new(err_id, format!("{}_errors", config.name), 2)
                    .without_error_space();
                registry.register(err_config)?;
                let err_dir = self.root.join("spaces").join(err_id.0.to_string());
                std::fs::create_dir_all(&err_dir)?;
            }
            config.error_space = Some(err_id);
            registry.register(config)?;
        } else {
            self.spaces.write().register(config)?;
        }
        let space_dir = self.root.join("spaces").join(space_id.to_string());
        std::fs::create_dir_all(&space_dir)?;
        self.persist_meta()?;
        Ok(())
    }

    /// Companion error space for a registered data space (M5).
    pub fn error_space_for(&self, data_space: SpaceId) -> Option<SpaceId> {
        self.spaces.read().error_space_for(data_space)
    }

    pub(crate) fn ensure_endpoint_index_space(&self) -> Result<(), EngineError> {
        if self.spaces.read().get(ENDPOINT_INDEX_SPACE).is_some() {
            return Ok(());
        }
        self.register_space(endpoint_index_space_config())
    }

    fn ensure_judgment_index_space(&self) -> Result<(), EngineError> {
        if self.spaces.read().get(JUDGMENT_INDEX_SPACE).is_some() {
            return Ok(());
        }
        self.register_space(judgment_index_space_config())
    }

    fn ensure_flow_vector_index_space(&self) -> Result<(), EngineError> {
        if self.spaces.read().get(FLOW_VECTOR_INDEX_SPACE).is_some() {
            return Ok(());
        }
        self.register_space(flow_vector_index_space_config())
    }

    fn error_space_for_data(&self, data_space: SpaceId) -> Result<SpaceId, EngineError> {
        self.spaces
            .read()
            .error_space_for(data_space)
            .ok_or(EngineError::ErrorSpaceMissing(data_space))
    }

    /// Endpoint reverse-index layout for the reserved index space.
    pub fn endpoint_index_layout(&self) -> EndpointIndexLayout {
        endpoint_index_layout_from_registry(&self.spaces.read())
    }

    /// Upgrade the reserved endpoint index to M2 polarity-dimension layout (lazy rewrite via compaction).
    pub fn upgrade_endpoint_index_layout(&self) -> Result<(), EngineError> {
        self.ensure_endpoint_index_space()?;
        let mut registry = self.spaces.write();
        let config = registry
            .get(ENDPOINT_INDEX_SPACE)
            .cloned()
            .ok_or(EngineError::EndpointIndexMissing)?;
        let updated = config.with_endpoint_index_layout(EndpointIndexLayout::V2PolarityDim);
        registry.update(updated)?;
        drop(registry);
        self.persist_meta()?;
        Ok(())
    }

    /// Derivation bus observability (M4).
    pub fn derivation_stats(&self) -> DerivationStats {
        self.derivation.stats()
    }

    /// Derivation events that failed to apply (peer-track Phase 0).
    pub fn failed_derivations(&self) -> Vec<crate::engine::derivation::FailedDerivation> {
        self.derivation.failed_derivations()
    }

    /// Block until background derivation catches up with submitted assertions.
    pub fn sync_derivation(&self) {
        self.derivation.flush();
    }

    /// Replay durable assertions with revision above derivation watermarks (crash recovery, M4).
    fn recover_derivation_on_open(&self) -> io::Result<()> {
        use std::collections::HashMap;

        let wm_vector = self.derivation.min_watermark_vector();
        let edge_spaces = edge_spaces_from_registry(&self.spaces.read());
        for space in edge_spaces {
            let mut records = self.query_history_on_branch(BranchId::MAIN, space)?;
            records.sort_by_key(|r| r.revision);
            let mut live_edges: HashMap<HyperedgeId, Hyperedge> = HashMap::new();
            for record in records {
                let session = SessionId(record.revision.session());
                let session_wm = wm_vector
                    .get(session)
                    .unwrap_or(RevisionId::ZERO);
                if record.revision <= session_wm {
                    continue;
                }
                let Some(id) = Hyperedge::id_from_storage_point(&record.address.point) else {
                    continue;
                };
                if Hyperedge::storage_point(id) != record.address.point {
                    continue;
                }
                if record.tombstone {
                    if let Some(edge) = live_edges.remove(&id) {
                        self.derivation
                            .submit(AssertionEvent::delete(
                                space,
                                edge,
                                record.revision,
                                BranchId::MAIN,
                            ))
                            .map_err(engine_to_io)?;
                    }
                } else if let Ok(edge) = decode_edge_record(&record.data) {
                    if edge.id != id {
                        continue;
                    }
                    live_edges.insert(edge.id, edge.clone());
                    self.derivation
                        .submit(AssertionEvent::upsert(
                            space,
                            edge,
                            record.revision,
                            BranchId::MAIN,
                        ))
                        .map_err(engine_to_io)?;
                }
            }
        }
        self.derivation.flush();
        Ok(())
    }

    /// Full revision history for a space (used by derivation recovery).
    fn query_history_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
    ) -> io::Result<Vec<Record>> {
        let ctx = self.query_ctx();
        let branch_id = if branch == BranchId::MAIN {
            None
        } else {
            Some(branch)
        };
        query_inner(
            &self.store,
            &self.snapshots,
            ctx.live_tail,
            ctx.space_tails,
            &self.spaces.read(),
            &self.session_watermarks,
            space,
            None,
            None,
            true,
            ctx.hilbert_tails,
            Some(&self.branch_overlays),
            branch_id,
        )
    }

    fn check_derivation_backpressure(&self) -> Result<(), EngineError> {
        self.check_derivation_backpressure_for(
            SessionId(GLOBAL_SESSION),
            self.session_watermarks.allocated(),
        )
    }

    fn check_derivation_backpressure_for(
        &self,
        session: SessionId,
        head: RevisionId,
    ) -> Result<(), EngineError> {
        self.derivation.check_backpressure(session, head)
    }

    /// Insert or update a hyperedge assertion and maintain the endpoint index.
    pub fn insert_hyperedge(&self, space: SpaceId, edge: Hyperedge) -> io::Result<RevisionId> {
        self.insert_hyperedge_on_branch(BranchId::MAIN, space, edge)
    }

    /// Branch-aware hyperedge insert.
    pub fn insert_hyperedge_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        mut edge: Hyperedge,
    ) -> io::Result<RevisionId> {
        edge.validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{:?}", e)))?;
        self.ensure_endpoint_index_space()
            .map_err(|e| engine_to_io(e))?;
        self.ensure_flow_vector_index_space()
            .map_err(|e| engine_to_io(e))?;
        if branch == BranchId::MAIN {
            self.check_derivation_backpressure()
                .map_err(engine_to_io)?;
            let rev = self.default_write_session.stamp();
            if let Some(ref prov) = edge.authoring_frame {
                validate_authoring_provenance(prov, rev)
                    .map_err(|e| engine_to_io(EngineError::from(e)))?;
            }
            if let Some(ref comp) = edge.computation {
                self.validate_computation_inputs(comp)
                    .map_err(engine_to_io)?;
            }
            edge.valid_from = rev;
            let row = prepare_assertion_write(space, &edge)?;
            let records = rows_to_records(&[row], rev);
            self.apply_records_on_branch(branch, records)?;
            self.derivation.submit(AssertionEvent::upsert(
                space,
                edge,
                rev,
                branch,
            ))
            .map_err(engine_to_io)?;
            Ok(rev)
        } else {
            let count = 1 + edge.endpoints.len();
            let range = self.default_write_session.stamp_n(count as u64);
            edge.valid_from = range.first();
            let index_layout = registry_index_layout(&self.spaces.read());
            let rows = prepare_writes(space, &edge, index_layout)?;
            let records = rows_to_records(&rows, range.first());
            self.apply_records_on_branch(branch, records)?;
            Ok(range.first())
        }
    }

    /// Adapter-friendly hyperedge write with optional catalog enforcement.
    pub fn insert_hyperedge_typed<K: KindLabel>(
        &self,
        space: SpaceId,
        id: HyperedgeId,
        kind: K,
        endpoints: Vec<AdapterEndpoint>,
        directionality: crate::infinitedb_core::hyperedge::Directionality,
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
            catalog
                .validate_edge_directionality(&kind_label, directionality)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        }
        let edge = Hyperedge {
            id,
            kind: kind_label.into(),
            endpoints: endpoints.into_iter().map(EndpointRef::from).collect(),
            weight_milli,
            metadata,
            valid_from: RevisionId::ZERO,
            valid_to,
            directionality,
            authoring_frame: None,
            computation: None,
        };
        edge.validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{:?}", e)))?;
        self.insert_hyperedge(space, edge)
    }

    /// Logically delete a hyperedge by id (payload + endpoint index rows).
    pub fn delete_hyperedge(&self, space: SpaceId, id: HyperedgeId) -> io::Result<RevisionId> {
        self.delete_hyperedge_on_branch(BranchId::MAIN, space, id)
    }

    /// Branch-aware hyperedge delete.
    pub fn delete_hyperedge_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        id: HyperedgeId,
    ) -> io::Result<RevisionId> {
        self.ensure_endpoint_index_space()
            .map_err(engine_to_io)?;
        let edge = self.fetch_hyperedge_by_id_on_branch(branch, space, id, None)?;
        if branch == BranchId::MAIN {
            self.check_derivation_backpressure()
                .map_err(engine_to_io)?;
            let rev = self.default_write_session.stamp();
            let row = if let Some(ref e) = edge {
                prepare_assertion_tombstone(space, e.id)
            } else {
                prepare_assertion_tombstone(space, id)
            };
            let records = rows_to_records(&[row], rev);
            self.apply_records_on_branch(branch, records)?;
            if let Some(e) = edge {
                self.derivation
                    .submit(AssertionEvent::delete(space, e, rev, branch))
                    .map_err(engine_to_io)?;
            }
            Ok(rev)
        } else {
            let index_layout = registry_index_layout(&self.spaces.read());
            let rows = match edge {
                Some(e) => prepare_deletes(space, &e, index_layout),
                None => vec![prepare_assertion_tombstone(space, id)],
            };
            let count = rows.len() as u64;
            let range = self.default_write_session.stamp_n(count);
            let records = rows_to_records(&rows, range.first());
            self.apply_records_on_branch(branch, records)?;
            Ok(range.first())
        }
    }

    /// Load a hyperedge by id from `space`.
    pub fn fetch_hyperedge_by_id(
        &self,
        space: SpaceId,
        id: HyperedgeId,
        as_of: Option<RevisionId>,
    ) -> io::Result<Option<Hyperedge>> {
        self.fetch_hyperedge_by_id_on_branch(BranchId::MAIN, space, id, as_of)
    }

    fn fetch_hyperedge_by_id_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        id: HyperedgeId,
        as_of: Option<RevisionId>,
    ) -> io::Result<Option<Hyperedge>> {
        let point = Hyperedge::storage_point(id);
        let records = self.query_bbox_on_branch(branch, space, point.clone(), point, as_of)?;
        if let Some(latest) = records.iter().max_by_key(|r| r.revision) {
            if latest.tombstone {
                return Ok(None);
            }
        }
        for r in records {
            if r.tombstone {
                continue;
            }
            if let Ok(mut edge) = decode_edge_record(&r.data) {
                if edge.id == id {
                    edge.valid_from = r.revision;
                    return Ok(Some(edge));
                }
            }
        }
        Ok(None)
    }

    /// Query all decodable hyperedges in a space.
    pub fn query_hyperedges(
        &self,
        space: SpaceId,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Hyperedge>> {
        Ok(self
            .query_on_branch(BranchId::MAIN, space, as_of)?
            .into_iter()
            .filter(|r| !r.tombstone)
            .filter_map(|r| decode_edge_record(&r.data).ok())
            .collect())
    }

    /// Query hyperedges incident on an endpoint (symmetric index scan).
    pub fn query_hyperedges_for_endpoint(
        &self,
        edge_space: SpaceId,
        endpoint: &EndpointRef,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Hyperedge>> {
        self.query_hyperedges_for_endpoint_directed(
            edge_space,
            endpoint,
            as_of,
            DirectionFilter::Any,
        )
    }

    /// Incidence query with index-resident direction when M2 layout is active.
    pub fn query_hyperedges_for_endpoint_directed(
        &self,
        edge_space: SpaceId,
        endpoint: &EndpointRef,
        as_of: Option<RevisionId>,
        direction: DirectionFilter,
    ) -> io::Result<Vec<Hyperedge>> {
        self.query_hyperedges_for_endpoint_directed_with_options(
            edge_space,
            endpoint,
            as_of,
            direction,
            QueryOptions::default(),
        )
    }

    /// Incidence query with optional index-only (bounded staleness) mode (M4).
    pub fn query_hyperedges_for_endpoint_directed_with_options(
        &self,
        edge_space: SpaceId,
        endpoint: &EndpointRef,
        as_of: Option<RevisionId>,
        direction: DirectionFilter,
        options: QueryOptions,
    ) -> io::Result<Vec<Hyperedge>> {
        self.ensure_endpoint_index_space().map_err(engine_to_io)?;
        let registry_layout = self.endpoint_index_layout();
        let wm_vec = self.derivation.endpoint_index_watermark_vector();
        let wm = wm_vec.scalar_meet();
        let rev_ceiling = as_of.unwrap_or_else(|| self.revision());
        let index_as_of = if !options.index_only && rev_ceiling > wm {
            Some(wm)
        } else {
            as_of
        };
        let index_records =
            self.query_on_branch(BranchId::MAIN, ENDPOINT_INDEX_SPACE, index_as_of)?;
        let mut ids = self.collect_incident_edge_ids_for_query(
            endpoint,
            direction,
            registry_layout,
            &index_records,
            index_as_of,
        )?;
        if !options.index_only {
            ids = self.merge_incident_ids_from_assertion_delta(
                edge_space,
                endpoint,
                direction,
                ids,
                rev_ceiling,
                &wm_vec,
            )?;
        }
        let (_, v1_ids) = if registry_layout == EndpointIndexLayout::V2PolarityDim
            && direction != DirectionFilter::Any
        {
            partition_incident_ids_by_layout(&index_records, endpoint, &ids)
        } else {
            (Vec::new(), Vec::new())
        };
        let v1_set: std::collections::HashSet<_> = v1_ids.into_iter().collect();
        let rev_ceiling = as_of.unwrap_or_else(|| self.revision());
        let mut edges = Vec::new();
        for id in &ids {
            if let Some(edge) = self.fetch_hyperedge_by_id(edge_space, *id, as_of)? {
                if edge.is_active_at(rev_ceiling)
                    && edge.endpoints.iter().any(|ep| {
                        ep.space == endpoint.space && ep.node.coords == endpoint.node.coords
                    })
                {
                    edges.push(edge);
                }
            }
        }
        if registry_layout == EndpointIndexLayout::V2PolarityDim
            && direction != DirectionFilter::Any
        {
            edges.retain(|edge| {
                if v1_set.contains(&edge.id) {
                    filter_edges_by_direction(vec![edge.clone()], endpoint, direction)
                        .pop()
                        .is_some()
                } else {
                    true
                }
            });
            Ok(edges)
        } else {
            Ok(filter_edges_by_direction(edges, endpoint, direction))
        }
    }

    /// Count incident hyperedges on an endpoint (index-resident when M2 layout is active).
    pub fn count_incident_edges_for_endpoint(
        &self,
        endpoint: &EndpointRef,
        as_of: Option<RevisionId>,
    ) -> io::Result<usize> {
        self.count_incident_edges_for_endpoint_directed(endpoint, as_of, DirectionFilter::Any)
    }

    /// Count incident hyperedges with index-resident direction filtering (M2).
    pub fn count_incident_edges_for_endpoint_directed(
        &self,
        endpoint: &EndpointRef,
        as_of: Option<RevisionId>,
        direction: DirectionFilter,
    ) -> io::Result<usize> {
        self.count_incident_edges_for_endpoint_directed_with_options(
            endpoint,
            as_of,
            direction,
            QueryOptions::default(),
        )
    }

    /// Count incident edges with delta-merge over un-derived assertions (M4).
    pub fn count_incident_edges_for_endpoint_directed_with_options(
        &self,
        endpoint: &EndpointRef,
        as_of: Option<RevisionId>,
        direction: DirectionFilter,
        options: QueryOptions,
    ) -> io::Result<usize> {
        self.ensure_endpoint_index_space().map_err(engine_to_io)?;
        let registry_layout = self.endpoint_index_layout();
        let wm_vec = self.derivation.endpoint_index_watermark_vector();
        let wm = wm_vec.scalar_meet();
        let rev_ceiling = as_of.unwrap_or_else(|| self.revision());
        let index_as_of = if !options.index_only && rev_ceiling > wm {
            Some(wm)
        } else {
            as_of
        };
        let index_records =
            self.query_on_branch(BranchId::MAIN, ENDPOINT_INDEX_SPACE, index_as_of)?;
        if !options.index_only {
            let edge_spaces = edge_spaces_from_registry(&self.spaces.read());
            let merged = self.merge_incident_ids_from_assertion_delta_multi_space(
                &edge_spaces,
                endpoint,
                direction,
                incident_edge_ids_directed(
                    &index_records,
                    endpoint,
                    direction,
                    registry_layout,
                ),
                rev_ceiling,
                &wm_vec,
                None,
            )?;
            return Ok(merged.len());
        }
        if registry_layout == EndpointIndexLayout::V2PolarityDim {
            let index_count =
                incident_edge_degree(&index_records, endpoint, direction, registry_layout);
            if direction == DirectionFilter::Any {
                return Ok(index_count);
            }
            let prefix = hypergraph::endpoint_prefix(endpoint);
            let ids = incident_edge_ids_from_records(&index_records, &prefix);
            let (_, v1_ids) =
                partition_incident_ids_by_layout(&index_records, endpoint, &ids);
            if v1_ids.is_empty() {
                return Ok(index_count);
            }
            let edge_spaces = edge_spaces_from_registry(&self.spaces.read());
            let v2_count = index_count.saturating_sub(v1_ids.len());
            let mut v1_match = 0usize;
            for id in v1_ids {
                for &space in &edge_spaces {
                    if let Ok(Some(edge)) = self.fetch_hyperedge_by_id(space, id, as_of) {
                        if filter_edges_by_direction(vec![edge], endpoint, direction)
                            .pop()
                            .is_some()
                        {
                            v1_match += 1;
                        }
                        break;
                    }
                }
            }
            Ok(v2_count + v1_match)
        } else {
            let prefix = hypergraph::endpoint_prefix(endpoint);
            let ids = incident_edge_ids_from_records(&index_records, &prefix);
            if direction == DirectionFilter::Any {
                return Ok(ids.len());
            }
            let edge_spaces = edge_spaces_from_registry(&self.spaces.read());
            let mut count = 0usize;
            for id in ids {
                for &space in &edge_spaces {
                    if let Ok(Some(edge)) = self.fetch_hyperedge_by_id(space, id, as_of) {
                        if filter_edges_by_direction(vec![edge], endpoint, direction)
                            .pop()
                            .is_some()
                        {
                            count += 1;
                        }
                        break;
                    }
                }
            }
            Ok(count)
        }
    }

    /// Rewrite V1 endpoint-index rows to M2 layout and compact the index space.
    pub fn compact_endpoint_index(&self, edge_spaces: &[SpaceId]) -> io::Result<()> {
        self.sync_derivation();
        self.ensure_endpoint_index_space().map_err(engine_to_io)?;
        if self.endpoint_index_layout() != EndpointIndexLayout::V2PolarityDim {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "endpoint index layout must be upgraded to V2 before compaction rewrite",
            ));
        }
        self.sync()?;
        let index_records =
            self.query_on_branch(BranchId::MAIN, ENDPOINT_INDEX_SPACE, None)?;
        let spaces: Vec<SpaceId> = if edge_spaces.is_empty() {
            edge_spaces_from_registry(&self.spaces.read())
        } else {
            edge_spaces.to_vec()
        };
        let rewrite_rows = plan_v1_to_v2_index_rewrite(&index_records, |id| {
            for &space in &spaces {
                if let Ok(Some(edge)) = self.fetch_hyperedge_by_id(space, id, None) {
                    return Some(edge);
                }
            }
            None
        });
        if !rewrite_rows.is_empty() {
            let range = self.default_write_session.stamp_n(rewrite_rows.len() as u64);
            let records = rows_to_records(&rewrite_rows, range.first());
            self.apply_records_on_branch(BranchId::MAIN, records)?;
            self.sync()?;
        }
        self.compact(ENDPOINT_INDEX_SPACE)
    }

    fn collect_incident_edge_ids_for_query(
        &self,
        endpoint: &EndpointRef,
        direction: DirectionFilter,
        registry_layout: EndpointIndexLayout,
        index_records: &[Record],
        _as_of: Option<RevisionId>,
    ) -> io::Result<Vec<HyperedgeId>> {
        let prefix = hypergraph::endpoint_prefix(endpoint);
        match registry_layout {
            EndpointIndexLayout::V2PolarityDim => Ok(incident_edge_ids_directed(
                index_records,
                endpoint,
                direction,
                registry_layout,
            )),
            EndpointIndexLayout::V1Symmetric => Ok(incident_edge_ids_from_records(
                index_records,
                &prefix,
            )),
        }
    }

    /// Query hyperedges by relationship kind string.
    pub fn query_hyperedges_by_kind(
        &self,
        space: SpaceId,
        kind: &str,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Hyperedge>> {
        Ok(self
            .query_hyperedges(space, as_of)?
            .into_iter()
            .filter(|e| e.kind.as_str() == kind)
            .collect())
    }

    /// Directional hypergraph traversal from a starting endpoint (M3).
    pub fn traverse_hypergraph(&self, spec: &TraversalSpec) -> io::Result<TraversalResult> {
        self.traverse_hypergraph_with_options(spec, QueryOptions::default())
    }

    /// Traversal with delta-merge over un-derived index rows (M4).
    pub fn traverse_hypergraph_with_options(
        &self,
        spec: &TraversalSpec,
        options: QueryOptions,
    ) -> io::Result<TraversalResult> {
        self.ensure_endpoint_index_space().map_err(engine_to_io)?;
        let registry_layout = self.endpoint_index_layout();
        let rev_ceiling = spec.as_of.unwrap_or_else(|| self.revision());
        let wm_vec = self.derivation.endpoint_index_watermark_vector();
        let wm = wm_vec.scalar_meet();
        let index_as_of = if !options.index_only && rev_ceiling > wm {
            Some(wm)
        } else {
            spec.as_of
        };
        let mut index_records =
            self.query_on_branch(BranchId::MAIN, ENDPOINT_INDEX_SPACE, index_as_of)?;
        if !options.index_only && rev_ceiling > wm {
            index_records.extend(self.synthetic_index_records_from_delta(
                spec.edge_space,
                rev_ceiling,
                &wm_vec,
                registry_layout,
                None,
            )?);
        }
        let edge_space = spec.edge_space;
        let as_of = spec.as_of;
        run_traversal(spec, &index_records, registry_layout, rev_ceiling, |id| {
            self.fetch_hyperedge_by_id(edge_space, id, as_of)
                .ok()
                .flatten()
                .filter(|edge| edge.is_active_at(rev_ceiling))
        })
    }

    /// Per-kind acyclicity check over directed hyperedges in `edge_space` (M3).
    pub fn check_hypergraph_acyclic(
        &self,
        edge_space: SpaceId,
        kinds: &[HyperedgeKind],
        as_of: Option<RevisionId>,
    ) -> io::Result<bool> {
        let rev_ceiling = as_of.unwrap_or_else(|| self.revision());
        let edges: Vec<Hyperedge> = self
            .query_hyperedges(edge_space, as_of)?
            .into_iter()
            .filter(|e| e.is_directed() && e.is_active_at(rev_ceiling))
            .filter(|e| kinds.is_empty() || kinds.iter().any(|k| k == &e.kind))
            .collect();
        Ok(hypergraph_acyclic_for_kinds(&edges, kinds))
    }

    /// Fire-and-forget insert on `main`. Blocks only when the target queue is full.
    pub fn insert(
        &self,
        space: SpaceId,
        point: DimensionVector,
        data: Vec<u8>,
    ) -> io::Result<RevisionId> {
        self.insert_on_branch(BranchId::MAIN, space, point, data)
    }

    /// Fire-and-forget insert on a branch (overlay for non-`main` branches).
    pub fn insert_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        point: DimensionVector,
        data: Vec<u8>,
    ) -> io::Result<RevisionId> {
        let rev = self.next_revision();
        let address = Address::new(space, point.clone());
        let hilbert_key = HilbertKey(space_key(&self.spaces.read(), space, &point));
        let entry = WalEntry::Write {
            address,
            revision: rev,
            data,
        };
        let job = WriteJob {
            branch_id: branch,
            revision: rev,
            entry,
            hilbert_key,
        };
        self.enqueue(job)?;
        Ok(rev)
    }

    /// Fire-and-forget delete on `main`.
    pub fn delete(&self, space: SpaceId, point: DimensionVector) -> io::Result<RevisionId> {
        self.delete_on_branch(BranchId::MAIN, space, point)
    }

    /// Fire-and-forget delete on a branch.
    pub fn delete_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        point: DimensionVector,
    ) -> io::Result<RevisionId> {
        let rev = self.next_revision();
        let address = Address::new(space, point.clone());
        let hilbert_key = HilbertKey(space_key(&self.spaces.read(), space, &point));
        let entry = WalEntry::Tombstone {
            address,
            revision: rev,
        };
        let job = WriteJob {
            branch_id: branch,
            revision: rev,
            entry,
            hilbert_key,
        };
        self.enqueue(job)?;
        Ok(rev)
    }

    /// Fork a new branch from `from` at the current revision.
    pub fn create_branch(&self, name: &str, from: BranchId) -> Result<BranchId, EngineError> {
        let parent = self
            .branches
            .read()
            .get(from)
            .cloned()
            .ok_or(EngineError::BranchNotFound(from))?;
        let id = BranchId(self.next_branch_id.fetch_add(1, Ordering::Relaxed));
        let forked_at = self.session_watermarks.allocated();
        let branch = Branch {
            id,
            name: name.to_string(),
            head: parent.head,
            parent: Some(from),
            forked_at,
        };
        self.branches.write().insert(branch)?;
        for (_, snap) in self.snapshots.all() {
            self.branch_overlays.register_branch(id, snap);
        }
        self.persist_meta()?;
        Ok(id)
    }

    /// Begin an applicative bulk hyperedge import session (M4).
    pub fn begin_hyperedge_import(
        &self,
        space: SpaceId,
        budget: ImportBudget,
    ) -> Result<HyperedgeImportSession, EngineError> {
        if self.spaces.read().get(space).is_none() {
            return Err(EngineError::SpaceNotFound(space));
        }
        Ok(HyperedgeImportSession::new(space, budget))
    }

    /// Commit a bulk import session — assertions sync, index derives on the bus.
    pub fn commit_hyperedge_import(
        &self,
        mut session: HyperedgeImportSession,
    ) -> Result<HyperedgeImportResult, EngineError> {
        let space = session.space;
        if session.is_aborted() || session.is_over_budget() {
            let result = HyperedgeImportResult {
                admitted: RevisionRange::empty(),
                errors: session.take_errors(),
                aborted: true,
            };
            self.maybe_persist_import_errors(space, &result)?;
            return Ok(result);
        }
        let queued = session.take_queued();
        let errors = session.take_errors();
        if queued.is_empty() {
            let result = HyperedgeImportResult {
                admitted: RevisionRange::empty(),
                errors,
                aborted: false,
            };
            self.maybe_persist_import_errors(space, &result)?;
            return Ok(result);
        }
        self.ensure_endpoint_index_space()?;
        self.check_derivation_backpressure()?;
        let mut first_rev = None;
        let mut last_rev = RevisionId::ZERO;
        for item in queued {
            let mut edge = item.edge;
            let rev = self.default_write_session.stamp();
            edge.valid_from = rev;
            let row = prepare_assertion_write(space, &edge)?;
            let records = rows_to_records(&[row], rev);
            self.apply_records_on_branch(BranchId::MAIN, records)?;
            self.derivation
                .submit(AssertionEvent::upsert(space, edge, rev, BranchId::MAIN))?;
            if first_rev.is_none() {
                first_rev = Some(rev);
            }
            last_rev = rev;
        }
        self.derivation.flush();
        let result = HyperedgeImportResult {
            admitted: RevisionRange::new(first_rev.unwrap_or(RevisionId::ZERO), last_rev),
            errors,
            aborted: false,
        };
        self.maybe_persist_import_errors(space, &result)?;
        Ok(result)
    }

    /// Persist an operation error record in the companion error space for `data_space` (M5).
    pub fn persist_operation_errors(
        &self,
        data_space: SpaceId,
        record: OperationErrorRecord,
    ) -> Result<RevisionId, EngineError> {
        ErrorKindCatalog::default().validate_kind(&record.kind)?;
        let error_space = self.error_space_for_data(data_space)?;
        let rev = self.default_write_session.stamp();
        let row = prepare_error_write(error_space, &record)
            .map_err(|e| EngineError::ErrorRecordEncode {
                message: e.to_string(),
            })?;
        let records = rows_to_records(&[row], rev);
        self.apply_records_on_branch(BranchId::MAIN, records)?;
        Ok(rev)
    }

    /// Query operation error records for a data space (M5).
    pub fn query_operation_errors(
        &self,
        data_space: SpaceId,
        range: Option<OperationRevisionRange>,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<OperationErrorRecord>> {
        let error_space = self
            .error_space_for(data_space)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "error space missing"))?;
        let records = self.query_on_branch(BranchId::MAIN, error_space, as_of)?;
        let mut out = Vec::new();
        for r in records {
            if r.tombstone {
                continue;
            }
            let Ok(record) = decode_error_record_payload(&r.data) else {
                continue;
            };
            if let Some(filter) = range {
                if !filter.contains(record.revision_range.first) {
                    continue;
                }
            }
            out.push(record);
        }
        Ok(out)
    }

    /// Tombstone-resolve an operation error record (audit trail preserved, M5).
    pub fn resolve_operation_error(
        &self,
        data_space: SpaceId,
        range_start: RevisionId,
    ) -> Result<(), EngineError> {
        let error_space = self.error_space_for_data(data_space)?;
        let rev = self.default_write_session.stamp();
        let row = prepare_error_tombstone(error_space, range_start);
        let records = rows_to_records(&[row], rev);
        self.apply_records_on_branch(BranchId::MAIN, records)?;
        Ok(())
    }

    /// Register a named durable frame definition (M6).
    pub fn register_frame(
        &self,
        request: FrameRegisterRequest,
    ) -> Result<FrameDefinition, EngineError> {
        self.validate_frame_request(&request, None)?;
        let id = request.id.unwrap_or_else(|| {
            FrameId(self.next_frame_id.fetch_add(1, Ordering::Relaxed))
        });
        if self.frames.read().contains_key(&id) {
            return Err(EngineError::InvalidFrame(FrameValidationError::DuplicateId(id)));
        }
        self.next_frame_id
            .fetch_max(id.0.saturating_add(1), Ordering::Relaxed);
        let def = FrameDefinition {
            id,
            name: request.name,
            assertion_scope: request.assertion_scope,
            judgment_overlay: request.judgment_overlay,
            default_as_of: request.default_as_of,
        };
        self.frames.write().insert(id, def.clone());
        self.persist_meta()?;
        Ok(def)
    }

    /// Lookup a durable frame by id (M6).
    pub fn get_frame(&self, id: FrameId) -> Option<FrameDefinition> {
        self.frames.read().get(&id).cloned()
    }

    /// List all registered frames (M6).
    pub fn list_frames(&self) -> Vec<FrameDefinition> {
        self.frames.read().values().cloned().collect()
    }

    /// Reset query-plan instrumentation (M6 performance tests).
    pub fn reset_query_plan_stats(&self) {
        reset_query_plan_stats();
    }

    /// Read query-plan instrumentation (M6 performance tests).
    pub fn query_plan_stats(&self) -> QueryPlanStats {
        query_plan_stats()
    }

    /// Spatial hyperedge query resolved within a named frame (M6).
    pub fn query_hyperedges_in_frame(
        &self,
        query: FrameQuery,
    ) -> Result<Vec<FrameResolvedHyperedge>, EngineError> {
        let def = self
            .get_frame(query.frame_id)
            .ok_or(EngineError::FrameNotFound(query.frame_id))?;
        let pin = self.resolve_frame_pin(
            query.as_of,
            query.version_vector.clone(),
            def.default_as_of,
        );
        let sources = merge_admission_specs(&def.assertion_scope, query.testimony_space);
        if sources.is_empty() {
            return Err(EngineError::InvalidFrame(FrameValidationError::EmptyScope));
        }
        let fetch_ceiling = match &pin {
            FrameTimePin::Scalar(r) => Some(*r),
            FrameTimePin::Vector(v) => Some(
                v.0.values()
                    .max()
                    .copied()
                    .unwrap_or_else(|| self.revision()),
            ),
        };
        let mut by_source = Vec::new();
        for source in &sources {
            let branch = source.branch.unwrap_or(BranchId::MAIN);
            let records = self
                .query_bbox_on_branch(
                    branch,
                    source.space,
                    query.min.clone(),
                    query.max.clone(),
                    fetch_ceiling,
                )
                ?;
            by_source.push((source.clone(), records));
        }
        let spaces = self.spaces.read();
        let watermarks = Arc::clone(&self.session_watermarks);
        let sourced = resolve_visibility_per_source(&spaces, &by_source, &pin, move |session| {
            watermarks.stable_for(session)
        });
        drop(spaces);
        let mut edges: Vec<FrameResolvedHyperedge> = Vec::new();
        for sr in sourced {
            if let Ok(mut edge) = decode_edge_record(&sr.record.data) {
                edge.valid_from = sr.record.revision;
                edges.push(FrameResolvedHyperedge {
                    edge,
                    source: sr.source,
                    judgments: Vec::new(),
                    diagnosis: None,
                    suppressed: false,
                });
            }
        }
        let judgments_by_subject = self.collect_frame_judgments(
            &def.judgment_overlay,
            &query.min,
            &query.max,
            pin.scalar_ceiling(),
        )?;
        let consulted = ConsultedFrame {
            frame_id: def.id,
            as_of: pin.scalar_ceiling(),
        };
        Ok(apply_judgment_overlay(
            edges,
            &def.judgment_overlay,
            &judgments_by_subject,
            consulted,
            query.options.include_suppressed,
            query.options.include_diagnosis,
        ))
    }

    /// Incidence query filtered through frame resolution (M6).
    pub fn query_hyperedges_for_endpoint_in_frame(
        &self,
        frame_id: FrameId,
        edge_space: SpaceId,
        endpoint: &EndpointRef,
        as_of: Option<RevisionId>,
        direction: DirectionFilter,
        options: FrameQueryOptions,
    ) -> Result<Vec<FrameResolvedHyperedge>, EngineError> {
        self.query_hyperedges_for_endpoint_in_frame_with_pin(
            frame_id,
            edge_space,
            endpoint,
            as_of,
            None,
            direction,
            options,
        )
    }

    /// Incidence query with optional version-vector pin (Phase 5).
    pub fn query_hyperedges_for_endpoint_in_frame_with_pin(
        &self,
        frame_id: FrameId,
        edge_space: SpaceId,
        endpoint: &EndpointRef,
        as_of: Option<RevisionId>,
        version_vector: Option<FrameVersionPin>,
        direction: DirectionFilter,
        options: FrameQueryOptions,
    ) -> Result<Vec<FrameResolvedHyperedge>, EngineError> {
        if !options.index_only {
            self.sync_derivation();
        }
        let def = self
            .get_frame(frame_id)
            .ok_or(EngineError::FrameNotFound(frame_id))?;
        let pin = self.resolve_frame_pin(as_of, version_vector, def.default_as_of);
        let fetch_ceiling = match &pin {
            FrameTimePin::Scalar(r) => Some(*r),
            FrameTimePin::Vector(v) => Some(
                v.0.values()
                    .max()
                    .copied()
                    .unwrap_or_else(|| self.revision()),
            ),
        };
        let candidates = self
            .query_hyperedges_for_endpoint_directed_with_options(
                edge_space,
                endpoint,
                fetch_ceiling,
                direction,
                QueryOptions {
                    index_only: options.index_only,
                },
            )
            ?;
        let sources = merge_admission_specs(&def.assertion_scope, edge_space);
        let watermarks = Arc::clone(&self.session_watermarks);
        let edges: Vec<FrameResolvedHyperedge> = candidates
            .into_iter()
            .filter(|edge| {
                Self::edge_visible_at_pin(edge, &pin, |session| watermarks.stable_for(session))
            })
            .filter_map(|edge| {
                Self::hyperedge_admitted(&edge, edge.valid_from, &sources, edge_space).map(|source| {
                    FrameResolvedHyperedge {
                        edge,
                        source,
                        judgments: Vec::new(),
                        diagnosis: None,
                        suppressed: false,
                    }
                })
            })
            .collect();
        let (min, max) = Self::endpoint_judgment_bbox(endpoint);
        let judgments_by_subject = self.collect_frame_judgments(
            &def.judgment_overlay,
            &min,
            &max,
            pin.scalar_ceiling(),
        )?;
        let consulted = ConsultedFrame {
            frame_id: def.id,
            as_of: pin.scalar_ceiling(),
        };
        Ok(apply_judgment_overlay(
            edges,
            &def.judgment_overlay,
            &judgments_by_subject,
            consulted,
            options.include_suppressed,
            options.include_diagnosis,
        ))
    }

    /// Directional traversal with frame admission and judgment overlay (M6).
    pub fn traverse_in_frame(
        &self,
        spec: &FrameTraversalSpec,
    ) -> io::Result<FrameTraversalResult> {
        let def = self.get_frame(spec.frame_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("frame {:?} not found", spec.frame_id))
        })?;
        let pin = self.resolve_frame_pin(
            spec.as_of.or(spec.base.as_of),
            spec.version_vector.clone(),
            def.default_as_of,
        );
        let pin_ceiling = pin.scalar_ceiling();
        let judgments_cache = self
            .collect_frame_judgments(
                &def.judgment_overlay,
                &DimensionVector::new(vec![0, 0]),
                &DimensionVector::new(vec![u32::MAX, u32::MAX]),
                pin_ceiling,
            )
            .map_err(engine_to_io)?;
        let consulted = ConsultedFrame {
            frame_id: def.id,
            as_of: pin_ceiling,
        };
        let sources = merge_admission_specs(&def.assertion_scope, spec.base.edge_space);
        let testimony_space = spec.base.edge_space;
        let overlay = def.judgment_overlay.clone();
        let options = spec.options;
        let watermarks = Arc::clone(&self.session_watermarks);
        let allowed_filter = |edge: &Hyperedge| -> bool {
            if Self::hyperedge_admitted(edge, edge.valid_from, &sources, testimony_space).is_none() {
                return false;
            }
            if !Self::edge_visible_at_pin(edge, &pin, |session| watermarks.stable_for(session)) {
                return false;
            }
            let (min, max) = {
                let p = Hyperedge::storage_point(edge.id);
                (p.clone(), p)
            };
            let mut judgments = judgments_cache.clone();
            if let Ok(extra) = self.collect_frame_judgments(&overlay, &min, &max, pin_ceiling) {
                for (k, v) in extra {
                    judgments.entry(k).or_default().extend(v);
                }
            }
            let source = Self::hyperedge_admitted(edge, edge.valid_from, &sources, testimony_space).unwrap();
            let resolved = apply_judgment_overlay(
                vec![FrameResolvedHyperedge {
                    edge: edge.clone(),
                    source,
                    judgments: Vec::new(),
                    diagnosis: None,
                    suppressed: false,
                }],
                &overlay,
                &judgments,
                consulted,
                options.include_suppressed,
                options.include_diagnosis,
            );
            resolved
                .first()
                .map(|e| !e.suppressed)
                .unwrap_or(false)
        };
        self.ensure_endpoint_index_space().map_err(engine_to_io)?;
        let registry_layout = self.endpoint_index_layout();
        let rev_ceiling = spec.base.as_of.unwrap_or(pin_ceiling);
        let wm_vec = self.derivation.endpoint_index_watermark_vector();
        let wm = wm_vec.scalar_meet();
        let index_as_of = if !spec.options.index_only && rev_ceiling > wm {
            Some(wm)
        } else {
            spec.base.as_of
        };
        let mut index_records =
            self.query_on_branch(BranchId::MAIN, ENDPOINT_INDEX_SPACE, index_as_of)?;
        if !spec.options.index_only && rev_ceiling > wm {
            index_records.extend(self.synthetic_index_records_from_delta(
                spec.base.edge_space,
                rev_ceiling,
                &wm_vec,
                registry_layout,
                None,
            )?);
        }
        let edge_space = spec.base.edge_space;
        let base_as_of = spec.base.as_of.or(Some(pin_ceiling));
        let mut resolved_edges: Vec<FrameResolvedHyperedge> = Vec::new();
        let traversal = run_traversal(
            &spec.base,
            &index_records,
            registry_layout,
            rev_ceiling,
            |id| {
                let edge = self
                    .fetch_hyperedge_by_id(edge_space, id, base_as_of)
                    .ok()
                    .flatten()
                    .filter(|edge| edge.is_active_at(rev_ceiling))?;
                if !allowed_filter(&edge) {
                    return None;
                }
                Some(edge)
            },
        )?;
        for edge in &traversal.edges {
            let (min, max) = {
                let p = Hyperedge::storage_point(edge.id);
                (p.clone(), p)
            };
            let mut judgments = judgments_cache.clone();
            if let Ok(extra) = self.collect_frame_judgments(&overlay, &min, &max, pin_ceiling) {
                for (k, v) in extra {
                    judgments.entry(k).or_default().extend(v);
                }
            }
            let source = Self::hyperedge_admitted(edge, edge.valid_from, &sources, testimony_space)
                .unwrap_or_else(|| TestimonySource {
                    space: testimony_space,
                    branch: None,
                    sessions: Some(vec![SessionId(edge.valid_from.session())]),
                });
            resolved_edges.extend(apply_judgment_overlay(
                vec![FrameResolvedHyperedge {
                    edge: edge.clone(),
                    source,
                    judgments: Vec::new(),
                    diagnosis: None,
                    suppressed: false,
                }],
                &overlay,
                &judgments,
                consulted,
                options.include_suppressed,
                options.include_diagnosis,
            ));
        }
        Ok(FrameTraversalResult {
            traversal,
            resolved: resolved_edges,
        })
    }

    /// Register an arbiter testimony stream backed by a dedicated assertion space (M5).
    pub fn register_arbiter_stream(
        &self,
        id: ArbiterId,
        name: impl Into<String>,
        dims: usize,
    ) -> Result<ArbiterStream, EngineError> {
        if self.arbiter_streams.read().contains_key(&id) {
            return Err(EngineError::ArbiterStreamExists(id.0));
        }
        let assertion_space = SpaceId(id.0.wrapping_add(0xA000_0000_0000_0000));
        if self.spaces.read().get(assertion_space).is_none() {
            self.register_space(SpaceConfig::new(assertion_space, name, dims))?;
        }
        let stream = ArbiterStream {
            id,
            assertion_space,
        };
        self.arbiter_streams.write().insert(id, stream.clone());
        self.persist_meta()?;
        Ok(stream)
    }

    /// Assert a judgment in an arbiter stream (M5).
    pub fn assert_judgment(
        &self,
        stream: ArbiterId,
        mut record: JudgmentRecord,
    ) -> Result<RevisionId, EngineError> {
        let arbiter = self
            .arbiter_streams
            .read()
            .get(&stream)
            .cloned()
            .ok_or(EngineError::ArbiterStreamNotFound(stream.0))?;
        self.ensure_judgment_index_space()?;
        self.validate_judgment_subject(&record.subject)?;
        record.arbiter = stream;
        let rows = prepare_judgment_writes(arbiter.assertion_space, &record)
            .map_err(|e| EngineError::Other {
                message: e.to_string(),
            })?;
        let range = self.default_write_session.stamp_n(rows.len() as u64);
        let rev = range.first();
        if let Some(ref prov) = record.authoring_frame {
            validate_authoring_provenance(prov, rev)?;
        }
        let records = rows_to_records(&rows, rev);
        self.apply_records_on_branch(BranchId::MAIN, records)?;
        Ok(rev)
    }

    /// Fetch a judgment by id from an arbiter stream (M5).
    pub fn fetch_judgment_by_id(
        &self,
        stream: ArbiterId,
        id: JudgmentId,
        as_of: Option<RevisionId>,
    ) -> io::Result<Option<JudgmentRecord>> {
        let assertion_space = self
            .arbiter_streams
            .read()
            .get(&stream)
            .map(|s| s.assertion_space)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "arbiter stream not found"))?;
        let point = crate::infinitedb_core::judgment::judgment_storage_point(id);
        let records = self.query_bbox_on_branch(
            BranchId::MAIN,
            assertion_space,
            point.clone(),
            point,
            as_of,
        )?;
        for r in records {
            if r.tombstone {
                continue;
            }
            if let Ok(j) = decode_judgment_record(&r.data) {
                if j.id == id {
                    return Ok(Some(j));
                }
            }
        }
        Ok(None)
    }

    /// Judgments whose subjects fall in a spatial region (M5).
    pub fn query_judgments_in_region(
        &self,
        stream: ArbiterId,
        min: DimensionVector,
        max: DimensionVector,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<JudgmentRecord>> {
        let assertion_space = self
            .arbiter_streams
            .read()
            .get(&stream)
            .map(|s| s.assertion_space)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "arbiter stream not found"))?;
        let (min, max) = Self::pad_judgment_index_bbox(min, max);
        let index_records =
            self.query_bbox_on_branch(BranchId::MAIN, JUDGMENT_INDEX_SPACE, min, max, as_of)?;
        let mut out = Vec::new();
        for r in index_records {
            if r.tombstone {
                continue;
            }
            let Some(id) = judgment_id_from_index_payload(&r.data) else {
                continue;
            };
            if let Some(j) = self.fetch_judgment_by_id(stream, id, as_of)? {
                if j.arbiter == stream {
                    out.push(j);
                }
            }
        }
        let _ = assertion_space;
        Ok(out)
    }

    /// Judgments pinned to a subject (M5).
    pub fn query_judgments_for_subject(
        &self,
        stream: ArbiterId,
        pin: &SubjectPin,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<JudgmentRecord>> {
        let prefix = subject_spatial_prefix(pin);
        let index_records =
            self.query_on_branch(BranchId::MAIN, JUDGMENT_INDEX_SPACE, as_of)?;
        let mut ids = Vec::new();
        for r in index_records {
            if r.tombstone {
                continue;
            }
            if !index_matches_subject_prefix(&r.address.point.coords, &prefix) {
                continue;
            }
            if let Some(id) = judgment_id_from_index_payload(&r.data) {
                ids.push(id);
            }
        }
        let mut out = Vec::new();
        for id in ids {
            if let Some(j) = self.fetch_judgment_by_id(stream, id, as_of)? {
                if j.subject == *pin {
                    out.push(j);
                }
            }
        }
        Ok(out)
    }

    /// Endpoint index derivation watermark (scalar meet across sessions, M4).
    pub fn endpoint_index_watermark(&self) -> RevisionId {
        self.derivation.endpoint_index_watermark()
    }

    /// Per-session endpoint index derivation watermark (Phase 6).
    pub fn endpoint_index_watermark_vector(&self) -> VersionVector {
        self.derivation.endpoint_index_watermark_vector()
    }

    /// Flow-vector index derivation watermark (M7).
    pub fn flow_vector_index_watermark(&self) -> RevisionId {
        self.derivation.flow_vector_index_watermark()
    }

    /// Per-session flow-vector index derivation watermark (Phase 6).
    pub fn flow_vector_index_watermark_vector(&self) -> VersionVector {
        self.derivation.flow_vector_index_watermark_vector()
    }

    /// Scan flow vectors whose quantized direction falls in `min_dir`..=`max_dir` (M7).
    pub fn query_flow_vectors_in_region(
        &self,
        min_dir: QuantizedDirection,
        max_dir: QuantizedDirection,
        as_of: Option<RevisionId>,
        options: QueryOptions,
    ) -> io::Result<Vec<FlowVectorRecord>> {
        self.ensure_flow_vector_index_space()
            .map_err(engine_to_io)?;
        let rev_ceiling = as_of.unwrap_or_else(|| self.revision());
        let wm_vec = self.derivation.flow_vector_index_watermark_vector();
        let _wm = wm_vec.scalar_meet();
        let (min, max) = pad_flow_vector_index_bbox(min_dir.clone(), max_dir.clone());
        let mut index_records =
            self.query_bbox_on_branch(BranchId::MAIN, FLOW_VECTOR_INDEX_SPACE, min, max, as_of)?;
        if !options.index_only {
            index_records.extend(self.synthetic_flow_vector_records_from_delta(
                rev_ceiling,
                &wm_vec,
            )?);
        }
        let q = default_flow_vector_quantization();
        let edge_spaces = edge_spaces_from_registry(&self.spaces.read());
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for r in index_records {
            if r.tombstone {
                continue;
            }
            if !direction_in_region(&r.address.point.coords, &min_dir, &max_dir) {
                continue;
            }
            let Some(id) = edge_id_from_flow_vector_index_record(&r.address.point.coords, &r.data)
            else {
                continue;
            };
            if !seen.insert(id) {
                continue;
            }
            if let Some(edge) = self.fetch_hyperedge_for_flow_merge(
                &edge_spaces,
                id,
                as_of,
                rev_ceiling,
                &wm_vec,
                options.index_only,
            )? {
                if !edge.is_active_at(rev_ceiling) {
                    continue;
                }
                let Some(vector) = edge.flow_vector() else {
                    continue;
                };
                let quantized = quantize_direction(&vector.delta, &q);
                if !direction_in_region(&quantized.coords, &min_dir, &max_dir) {
                    continue;
                }
                out.push(FlowVectorRecord {
                    edge,
                    vector,
                    quantized,
                });
            }
        }
        Ok(out)
    }

    /// Flow vector for a single hyperedge assertion (M7).
    pub fn query_flow_vector_for_edge(
        &self,
        edge_space: SpaceId,
        id: HyperedgeId,
        as_of: Option<RevisionId>,
    ) -> io::Result<Option<FlowVectorRecord>> {
        let rev_ceiling = as_of.unwrap_or_else(|| self.revision());
        let Some(edge) = self.fetch_hyperedge_by_id(edge_space, id, as_of)? else {
            return Ok(None);
        };
        if !edge.is_active_at(rev_ceiling) {
            return Ok(None);
        }
        let Some(vector) = edge.flow_vector() else {
            return Ok(None);
        };
        let quantized = quantize_direction(&vector.delta, &default_flow_vector_quantization());
        Ok(Some(FlowVectorRecord {
            edge,
            vector,
            quantized,
        }))
    }

    /// Backward freshness for a hyperedge with computation provenance (M7).
    pub fn check_hyperedge_freshness(
        &self,
        edge_space: SpaceId,
        id: HyperedgeId,
        as_of: Option<RevisionId>,
    ) -> io::Result<FreshnessReport> {
        let edge = self
            .fetch_hyperedge_by_id(edge_space, id, as_of)?
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("hyperedge {:?} not found", id))
            })?;
        check_computation_freshness(&edge, &|pin| self.fetch_subject_revision_at(pin, as_of))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "hyperedge has no computation provenance",
                )
            })
    }

    /// Forward stale closure from a changed subject pin (M7).
    pub fn query_stale_downstream(
        &self,
        subject: SubjectPin,
        edge_space: SpaceId,
        max_depth: usize,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<StaleTarget>> {
        self.query_staleness_by_source_revision(subject, edge_space, max_depth, as_of)
    }

    /// Observability alias for source-revision staleness closure (M7).
    pub fn query_staleness_by_source_revision(
        &self,
        source: SubjectPin,
        edge_space: SpaceId,
        max_depth: usize,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<StaleTarget>> {
        let rev_ceiling = as_of.unwrap_or_else(|| self.revision());
        let wm_vec = self.derivation.endpoint_index_watermark_vector();
        let wm = wm_vec.scalar_meet();
        let registry_layout = registry_index_layout(&self.spaces.read());
        let mut index_records =
            self.query_on_branch(BranchId::MAIN, ENDPOINT_INDEX_SPACE, as_of)?;
        if wm < rev_ceiling {
            index_records.extend(self.synthetic_index_records_from_delta(
                edge_space,
                rev_ceiling,
                &wm_vec,
                registry_layout,
                None,
            )?);
        }
        let fetch_edge = |id: HyperedgeId| {
            self.fetch_hyperedge_by_id(edge_space, id, as_of)
                .ok()
                .flatten()
                .filter(|e| e.is_active_at(rev_ceiling))
        };
        let seeds = staleness_seed_endpoints(&source, &fetch_edge);
        forward_stale_closure(
            &source,
            edge_space,
            max_depth,
            as_of,
            &index_records,
            registry_layout,
            rev_ceiling,
            &fetch_edge,
            &|pin| self.fetch_subject_revision_at(pin, as_of),
            &seeds,
        )
    }

    fn validate_frame_request(
        &self,
        request: &FrameRegisterRequest,
        existing_id: Option<FrameId>,
    ) -> Result<(), EngineError> {
        if request.name.is_empty() {
            return Err(FrameValidationError::EmptyName.into());
        }
        for frame in self.frames.read().values() {
            if frame.name == request.name && existing_id != Some(frame.id) {
                return Err(EngineError::FrameExists(request.name.clone()));
            }
        }
        self.validate_assertion_scope(&request.assertion_scope)?;
        for layer in &request.judgment_overlay {
            if !self.arbiter_streams.read().contains_key(&layer.arbiter) {
                return Err(FrameValidationError::ArbiterNotRegistered(layer.arbiter).into());
            }
        }
        Ok(())
    }

    fn validate_assertion_scope(&self, scope: &AssertionScope) -> Result<(), EngineError> {
        match scope {
            AssertionScope::Spaces(spaces) => {
                if spaces.is_empty() {
                    return Err(FrameValidationError::EmptyScope.into());
                }
                for space in spaces {
                    if !is_testimony_space(*space) {
                        return Err(FrameValidationError::ReservedSpace(*space).into());
                    }
                    if self.spaces.read().get(*space).is_none() {
                        return Err(FrameValidationError::SpaceNotRegistered(*space).into());
                    }
                }
            }
            AssertionScope::Branches(branches) => {
                if branches.is_empty() {
                    return Err(FrameValidationError::EmptyScope.into());
                }
                for branch in branches {
                    if self.branches.read().get(*branch).is_none() {
                        return Err(FrameValidationError::BranchNotFound(*branch).into());
                    }
                }
            }
            AssertionScope::Session(sessions) => {
                if sessions.is_empty() {
                    return Err(FrameValidationError::EmptyScope.into());
                }
                for session in sessions {
                    if !self.session_watermarks.session_registered(*session) {
                        return Err(FrameValidationError::SessionNotRegistered(*session).into());
                    }
                }
            }
            AssertionScope::Union(parts) => {
                if parts.is_empty() {
                    return Err(FrameValidationError::EmptyScope.into());
                }
                for part in parts {
                    self.validate_assertion_scope(part)?;
                }
            }
        }
        Ok(())
    }

    fn resolve_frame_pin(
        &self,
        as_of: Option<RevisionId>,
        version_vector: Option<FrameVersionPin>,
        default_as_of: Option<RevisionId>,
    ) -> FrameTimePin {
        if let Some(map) = version_vector {
            FrameTimePin::Vector(VersionVector(map))
        } else {
            FrameTimePin::Scalar(
                as_of
                    .or(default_as_of)
                    .unwrap_or_else(|| self.revision()),
            )
        }
    }

    fn edge_visible_at_pin<F>(edge: &Hyperedge, pin: &FrameTimePin, stable_for_session: F) -> bool
    where
        F: Fn(SessionId) -> RevisionId,
    {
        let rev = edge.valid_from;
        match pin {
            FrameTimePin::Scalar(ceiling) => rev <= *ceiling,
            FrameTimePin::Vector(vector) => {
                let session = SessionId(rev.session());
                let ceiling = vector
                    .get(session)
                    .unwrap_or_else(|| stable_for_session(session));
                rev <= ceiling
            }
        }
    }

    fn hyperedge_admitted(
        _edge: &Hyperedge,
        authorship: RevisionId,
        sources: &[TestimonySource],
        edge_space: SpaceId,
    ) -> Option<TestimonySource> {
        for source in sources {
            if source.space != edge_space {
                continue;
            }
            if record_admitted_by_source(authorship.session(), source) {
                return Some(TestimonySource {
                    space: edge_space,
                    branch: source.branch,
                    sessions: Some(vec![SessionId(authorship.session())]),
                });
            }
        }
        None
    }

    fn collect_frame_judgments(
        &self,
        layers: &[JudgmentOverlayLayer],
        min: &DimensionVector,
        max: &DimensionVector,
        as_of: RevisionId,
    ) -> Result<HashMap<SubjectPin, Vec<JudgmentRecord>>, EngineError> {
        let mut judgments_by_subject: HashMap<SubjectPin, Vec<JudgmentRecord>> = HashMap::new();
        for layer in layers {
            let js = self
                .query_judgments_in_region(layer.arbiter, min.clone(), max.clone(), Some(as_of))
                .map_err(EngineError::from)?;
            for j in js {
                judgments_by_subject
                    .entry(j.subject.clone())
                    .or_default()
                    .push(j);
            }
        }
        Ok(judgments_by_subject)
    }

    fn endpoint_judgment_bbox(endpoint: &EndpointRef) -> (DimensionVector, DimensionVector) {
        let coords = endpoint.node.coords.clone();
        let min = DimensionVector::new(coords.clone());
        let max = DimensionVector::new(coords);
        Self::pad_judgment_index_bbox(min, max)
    }

    fn maybe_persist_import_errors(
        &self,
        space: SpaceId,
        result: &HyperedgeImportResult,
    ) -> Result<(), EngineError> {
        if result.errors.is_empty() && !result.aborted {
            return Ok(());
        }
        let admitted = revision_range_from_engine(result.admitted);
        let record = operation_record_from_import(space, admitted, &result.errors, result.aborted);
        let _ = self.persist_operation_errors(space, record)?;
        Ok(())
    }

    fn validate_computation_inputs(
        &self,
        comp: &crate::infinitedb_core::computation::ComputationProvenance,
    ) -> Result<(), EngineError> {
        if comp.inputs.is_empty() {
            return Err(ComputationValidationError::EmptyInputs.into());
        }
        for (index, pin) in comp.inputs.iter().enumerate() {
            if let Err(e) = self.validate_judgment_subject(pin) {
                return Err(match e {
                    EngineError::InvalidJudgment(JudgmentValidationError::SubjectNotFound {
                        space,
                        revision,
                    }) => ComputationValidationError::InputNotFound {
                        index,
                        space,
                        revision,
                    }
                    .into(),
                    EngineError::InvalidJudgment(
                        JudgmentValidationError::SubjectRevisionMismatch {
                            expected,
                            observed,
                        },
                    ) => ComputationValidationError::InputRevisionMismatch {
                        index,
                        expected,
                        observed,
                    }
                    .into(),
                    EngineError::InvalidJudgment(JudgmentValidationError::InvalidSubjectPin(
                        message,
                    )) => ComputationValidationError::InvalidInputPin { index, message }.into(),
                    other => other,
                });
            }
        }
        Ok(())
    }

    fn fetch_subject_revision_at(
        &self,
        pin: &SubjectPin,
        as_of: Option<RevisionId>,
    ) -> Option<RevisionId> {
        match (&pin.kind, &pin.identity) {
            (SubjectKind::Hyperedge, SubjectIdentity::Hyperedge(id)) => self
                .fetch_hyperedge_by_id(pin.space, *id, as_of)
                .ok()
                .flatten()
                .map(|e| e.valid_from),
            (SubjectKind::Node, SubjectIdentity::Address(addr)) => {
                let records = self
                    .query_bbox_on_branch(
                        BranchId::MAIN,
                        pin.space,
                        addr.point.clone(),
                        addr.point.clone(),
                        as_of,
                    )
                    .ok()?;
                records
                    .iter()
                    .filter(|r| !r.tombstone)
                    .map(|r| r.revision)
                    .max()
            }
            _ => None,
        }
    }

    fn validate_judgment_subject(&self, pin: &SubjectPin) -> Result<(), EngineError> {
        match (&pin.kind, &pin.identity) {
            (SubjectKind::Hyperedge, SubjectIdentity::Hyperedge(id)) => {
                let edge = self
                    .fetch_hyperedge_by_id(pin.space, *id, None)?
                    .ok_or(JudgmentValidationError::SubjectNotFound {
                        space: pin.space,
                        revision: pin.subject_revision,
                    })?;
                if edge.valid_from != pin.subject_revision {
                    return Err(JudgmentValidationError::SubjectRevisionMismatch {
                        expected: pin.subject_revision,
                        observed: edge.valid_from,
                    }
                    .into());
                }
                Ok(())
            }
            (SubjectKind::Node, SubjectIdentity::Address(addr)) => {
                let records = self.query_bbox_on_branch(
                    BranchId::MAIN,
                    pin.space,
                    addr.point.clone(),
                    addr.point.clone(),
                    Some(pin.subject_revision),
                )?;
                if records.iter().any(|r| !r.tombstone && r.revision == pin.subject_revision) {
                    Ok(())
                } else {
                    Err(JudgmentValidationError::SubjectNotFound {
                        space: pin.space,
                        revision: pin.subject_revision,
                    }
                    .into())
                }
            }
            _ => Err(JudgmentValidationError::InvalidSubjectPin(
                "subject kind does not match identity".into(),
            )
            .into()),
        }
    }

    /// Three-way merge `source` into `target` (usually `main`).
    ///
    /// Applied records receive **fresh global revisions** (the merge is a new
    /// commit, not a replay of source revision ids).
    pub fn merge_branch(
        &self,
        target: BranchId,
        source: BranchId,
        strategy: MergeStrategy,
        resolver: Option<Box<dyn Fn(MergeConflict) -> Record + Send + Sync>>,
    ) -> io::Result<MergeResult> {
        self.sync()?;
        let ctx = self.query_ctx();
        let mut result = merge_branches(
            &self.store,
            &self.snapshots,
            ctx.live_tail,
            ctx.space_tails,
            ctx.hilbert_tails,
            &self.branch_overlays,
            &self.spaces.read(),
            &self.session_watermarks,
            &self.branches.read(),
            target,
            source,
            strategy,
            resolver.as_deref(),
        )?;
        if strategy == MergeStrategy::Interactive && !result.conflicts.is_empty() {
            return Ok(result);
        }
        let applied = std::mem::take(&mut result.applied_records);
        self.apply_records_on_branch(target, applied)?;
        self.branch_overlays.clear_branch(source, &self.root)?;
        self.sync()?;
        Ok(result)
    }

    /// Query `space` through a branch overlay.
    pub fn query_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Record>> {
        let ctx = self.query_ctx();
        let branch_id = if branch == BranchId::MAIN {
            None
        } else {
            Some(branch)
        };
        query_inner(
            &self.store,
            &self.snapshots,
            ctx.live_tail,
            ctx.space_tails,
            &self.spaces.read(),
            &self.session_watermarks,
            space,
            None,
            as_of,
            false,
            ctx.hilbert_tails,
            Some(&self.branch_overlays),
            branch_id,
        )
    }

    /// Enqueue writes across multiple spaces (ordered by space id).
    ///
    /// Every [`WriteJob::revision`] must have been allocated through
    /// [`RevisionWatermark::allocate`] or [`RevisionWatermark::allocate_n`]
    /// (via `insert`, `insert_many`, etc.) so the revision is already
    /// registered as outstanding.
    pub fn enqueue_batch(&self, jobs: Vec<WriteJob>) -> io::Result<()> {
        let mut main_jobs = Vec::with_capacity(jobs.len());
        let mut branch_batches: std::collections::BTreeMap<OverlayKey, Vec<Record>> =
            std::collections::BTreeMap::new();
        for job in jobs {
            if job.branch_id != BranchId::MAIN {
                let branch_id = job.branch_id;
                let record = job.into_record();
                let key = OverlayKey::new(branch_id, record.address.space);
                branch_batches.entry(key).or_default().push(record);
            } else {
                main_jobs.push(job);
            }
        }
        for (key, records) in branch_batches {
            let revs: Vec<RevisionId> = records.iter().map(|r| r.revision).collect();
            let result = self.branch_overlays.append_batch_with_durability(
                key.branch_id,
                key.space_id,
                records,
                &self.root,
            );
            if let Err(ref e) = result {
                for rev in &revs {
                    self.session_watermarks.retire_failed(*rev, e.to_string());
                }
                return result;
            }
            for rev in revs {
                self.session_watermarks.retire(rev);
            }
        }
        if main_jobs.is_empty() {
            return Ok(());
        }
        let main_revs: Vec<RevisionId> = main_jobs.iter().map(|j| j.revision).collect();
        let result = match &mut *self.backend.lock() {
            WriteBackend::V4 { coordinator } => coordinator.enqueue_batch(main_jobs),
            WriteBackend::V3 { coordinator } => coordinator.enqueue_batch(main_jobs),
            WriteBackend::V2 { queue, .. } => {
                for job in main_jobs {
                    queue.enqueue_write(job)?;
                }
                Ok(())
            }
        };
        if let Err(ref e) = result {
            for rev in &main_revs {
                self.session_watermarks.retire_failed(*rev, e.to_string());
            }
        }
        result
    }

    /// Query all live records in `space` on `main`, optionally capped at `as_of`.
    pub fn query(
        &self,
        space: SpaceId,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Record>> {
        self.query_on_branch(BranchId::MAIN, space, as_of)
    }

    /// Bounding-box query on `main`.
    pub fn query_bbox(
        &self,
        space: SpaceId,
        min: DimensionVector,
        max: DimensionVector,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Record>> {
        self.query_bbox_on_branch(BranchId::MAIN, space, min, max, as_of)
    }

    /// Bounding-box query through a branch overlay.
    pub fn query_bbox_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        min: DimensionVector,
        max: DimensionVector,
        as_of: Option<RevisionId>,
    ) -> io::Result<Vec<Record>> {
        let ctx = self.query_ctx();
        let branch_id = if branch == BranchId::MAIN {
            None
        } else {
            Some(branch)
        };
        query_bbox(
            &self.store,
            &self.snapshots,
            ctx.live_tail,
            ctx.space_tails,
            &self.spaces.read(),
            &self.session_watermarks,
            space,
            min,
            max,
            as_of,
            ctx.hilbert_tails,
            Some(&self.branch_overlays),
            branch_id,
        )
    }

    /// Flush pending writes for one space to durable storage without syncing all spaces.
    pub fn flush(&self, space: SpaceId) -> io::Result<()> {
        match &*self.backend.lock() {
            WriteBackend::V4 { coordinator } => coordinator.flush_space(space)?,
            WriteBackend::V3 { coordinator } => coordinator.flush_space(space)?,
            WriteBackend::V2 { queue, .. } => queue.request_flush(space)?,
        }
        self.persist_meta()
    }

    /// Flush all write queues and persist metadata. Call after writes to make data queryable.
    pub fn sync(&self) -> io::Result<()> {
        self.derivation.flush();
        match &*self.backend.lock() {
            WriteBackend::V4 { coordinator } => coordinator.sync_all()?,
            WriteBackend::V3 { coordinator } => coordinator.sync_all()?,
            WriteBackend::V2 { queue, .. } => queue.request_sync()?,
        }
        self.persist_meta()
    }

    /// Allocate a contiguous revision range for custom [`WriteJob`] batches.
    ///
    /// Revisions are registered as outstanding until the write path retires them.
    pub fn allocate_revisions(&self, count: u64) -> RevisionRange {
        self.default_write_session.stamp_n(count)
    }

    /// Open a new asserting write session with a locally minted `SessionId` (D-P2).
    pub fn open_session(&self) -> WriteSession {
        WriteSession::open(
            Arc::clone(&self.session_watermarks),
            Arc::clone(&self.session_wal_store),
        )
    }

    /// Quarantined session WALs from the last recovery (Phase 3).
    pub fn quarantined_session_wals(&self) -> std::collections::BTreeMap<u32, String> {
        self.session_wal_store.quarantined_sessions()
    }

    /// Insert through an explicit write session (Phase 3/4 session WAL path).
    ///
    /// Data frames are appended to the session WAL only; call [`Self::sync_session_wal`]
    /// then [`Self::commit_session_intent`] to publish to the live store.
    pub fn insert_with_session(
        &self,
        session: &WriteSession,
        space: SpaceId,
        point: DimensionVector,
        data: Vec<u8>,
    ) -> io::Result<RevisionId> {
        let rev = session.stamp();
        let address = Address::new(space, point.clone());
        let entry = WalEntry::Write {
            address: address.clone(),
            revision: rev,
            data,
        };
        let hilbert_key = HilbertKey(space_key(&self.spaces.read(), space, &address.point));
        if session.uses_session_wal() {
            if !self.timed_fast_path.enabled {
                self.session_wal_store
                    .append_frame(session.id(), &entry)
                    .map_err(EngineError::from)
                    .map_err(engine_to_io)?;
            }
            self.session_wal_store.update_highest_revision(session.id(), rev);
            session.note_buffered_write(
                entry,
                hilbert_key,
                rev,
                IntentOperationKind::Insert,
            );
            return Ok(rev);
        }
        self.enqueue(WriteJob::main(rev, entry, hilbert_key))?;
        Ok(rev)
    }

    /// Insert a hyperedge through an explicit write session (Phase 5 session WAL path).
    ///
    /// Appends to the session WAL only; call [`Self::sync_session_wal`] then
    /// [`Self::commit_session_intent`] (or [`Self::sync_session`]) to publish.
    pub fn insert_hyperedge_with_session(
        &self,
        session: &WriteSession,
        space: SpaceId,
        mut edge: Hyperedge,
    ) -> io::Result<RevisionId> {
        if !session.uses_session_wal() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "insert_hyperedge_with_session requires an explicit session WAL",
            ));
        }
        edge.validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{:?}", e)))?;
        self.ensure_endpoint_index_space()
            .map_err(engine_to_io)?;
        self.ensure_flow_vector_index_space()
            .map_err(engine_to_io)?;
        self.check_derivation_backpressure_for(
            session.id(),
            self.session_watermarks.watermark_for(session.id()).allocated(),
        )
            .map_err(engine_to_io)?;
        let rev = session.stamp();
        if let Some(ref prov) = edge.authoring_frame {
            validate_authoring_provenance(prov, rev)
                .map_err(|e| engine_to_io(EngineError::from(e)))?;
        }
        if let Some(ref comp) = edge.computation {
            self.validate_computation_inputs(comp)
                .map_err(engine_to_io)?;
        }
        edge.valid_from = rev;
        let row = prepare_assertion_write(space, &edge)?;
        let record = Record {
            address: Address::new(row.space, row.point.clone()),
            revision: rev,
            data: row.data,
            tombstone: row.tombstone,
            hilbert_key: CachedHilbertKey::UNSET,
        };
        let hilbert_key = HilbertKey(space_key(&self.spaces.read(), space, &record.address.point));
        let entry = WalEntry::Write {
            address: record.address.clone(),
            revision: rev,
            data: record.data.clone(),
        };
        if !self.timed_fast_path.enabled {
            self.session_wal_store
                .append_frame(session.id(), &entry)
                .map_err(EngineError::from)
                .map_err(engine_to_io)?;
        }
        self.session_wal_store.update_highest_revision(session.id(), rev);
        let event = AssertionEvent::upsert(space, edge, rev, BranchId::MAIN);
        session.note_buffered_write_with_event(
            entry,
            hilbert_key,
            rev,
            IntentOperationKind::HypergraphWrite,
            Some(event),
        );
        Ok(rev)
    }

    /// Fsync durable storage without publishing to the live store (Phase 4/7).
    pub fn sync_session_wal(&self, session: &WriteSession) -> io::Result<DurableIntent> {
        if session.uses_session_wal() {
            if self.timed_fast_path.enabled {
                let entries = session.peek_pending_entries();
                if !entries.is_empty() {
                    match self
                        .session_wal_store
                        .try_fast_seal(
                            session.id(),
                            &entries,
                            self.timed_fast_path.direct_seal_deadline,
                        )
                        .map_err(EngineError::from)
                        .map_err(engine_to_io)?
                    {
                        FastSealOutcome::Sealed => {
                            self.persist_meta()?;
                            return Ok(session.mark_durable(DurabilityMedium::FastSegment));
                        }
                        FastSealOutcome::TimedOut => {
                            self.session_wal_store
                                .append_buffered_to_wal(session.id(), &entries)
                                .map_err(EngineError::from)
                                .map_err(engine_to_io)?;
                        }
                    }
                }
            }
            self.session_wal_store
                .sync_group(session.id())
                .map_err(EngineError::from)
                .map_err(engine_to_io)?;
            self.persist_meta()?;
            return Ok(session.mark_durable(DurabilityMedium::SessionWal));
        }
        Ok(DurableIntent {
            session: session.id(),
            medium: DurabilityMedium::SessionWal,
        })
    }

    /// Commit a durable intent group: checkpoint frame, live-store apply, derivation flush.
    pub fn commit_session_intent(
        &self,
        session: &WriteSession,
        durable: &DurableIntent,
    ) -> Result<IntentCheckpoint, EngineError> {
        if !session.uses_session_wal() {
            return Err(EngineError::InvalidSpaceConfig {
                message: "commit_session_intent requires an explicit session WAL".into(),
            });
        }
        let (checkpoint, buffered, medium) = session
            .take_durable_pending(durable)
            .map_err(|msg| EngineError::InvalidSpaceConfig { message: msg })?;
        if medium == DurabilityMedium::FastSegment {
            for item in &buffered {
                self.session_wal_store
                    .append_frame(session.id(), &item.entry)
                    .map_err(EngineError::from)?;
            }
        }
        self.session_wal_store
            .append_intent_checkpoint(session.id(), &checkpoint)?;
        self.session_wal_store.sync_group(session.id())?;
        self.session_wal_store
            .update_highest_revision(session.id(), checkpoint.last_revision);

        if let Some(collision_space) = self.detect_checkpoint_collisions(session.id(), &buffered)? {
            let record = operation_record_checkpoint_collision(
                collision_space,
                &checkpoint,
                format!(
                    "address overlap at intent checkpoint for session {}",
                    session.id().0
                ),
            );
            self.persist_operation_errors(collision_space, record)?;
        }
        self.mark_session_wal_collision_evaluated(session.id());

        let mut jobs = Vec::with_capacity(buffered.len());
        let mut assertion_events = Vec::new();
        for item in buffered {
            if let Some(event) = item.assertion_event {
                assertion_events.push(event);
            }
            self.session_watermarks
                .watermark_for(session.id())
                .register_outstanding(item.revision);
            jobs.push(WriteJob::main(
                item.revision,
                item.entry,
                item.hilbert_key,
            ));
        }
        if !jobs.is_empty() {
            self.enqueue_batch(jobs)?;
            self.sync()?;
        }
        for event in assertion_events {
            self.derivation.submit(event).map_err(|e| {
                EngineError::Other {
                    message: format!("derivation submit after session commit: {e}"),
                }
            })?;
        }
        self.derivation.flush();
        if medium == DurabilityMedium::FastSegment {
            self.session_wal_store
                .reset_fast_after_commit(session.id())
                .map_err(EngineError::from)?;
        }
        self.persist_meta()?;
        Ok(checkpoint)
    }

    /// Session fast-path counters (Phase 7).
    pub fn session_write_stats(&self) -> crate::engine::timed_fast_path::SessionWriteStatsSnapshot {
        self.session_wal_store.write_stats()
    }

    fn detect_checkpoint_collisions(
        &self,
        session: crate::infinitedb_core::hlc::SessionId,
        buffered: &[crate::engine::session::BufferedSessionWrite],
    ) -> Result<Option<SpaceId>, EngineError> {
        for item in buffered {
            let (space, point) = match &item.entry {
                WalEntry::Write { address, .. } | WalEntry::Tombstone { address, .. } => {
                    (&address.space, &address.point)
                }
                _ => continue,
            };
            let rows = self
                .query(*space, None)
                .map_err(|e| EngineError::Other {
                    message: e.to_string(),
                })?;
            for row in rows {
                if row.address.point == *point && row.revision.session() != session.0 {
                    return Ok(Some(*space));
                }
            }
        }
        Ok(None)
    }

    /// Durably sync one session's WAL (legacy alias — prefer [`Self::sync_session_wal`] +
    /// [`Self::commit_session_intent`]).
    pub fn sync_session(&self, session: &WriteSession) -> io::Result<()> {
        let durable = self.sync_session_wal(session)?;
        if session.has_pending_intent() {
            self.commit_session_intent(session, &durable)
                .map_err(engine_to_io)?;
        } else {
            self.sync()?;
        }
        Ok(())
    }

    /// Mark retirement gates for a session WAL (Phase 3).
    pub fn mark_session_wal_sealed(&self, session: SessionId) {
        self.session_wal_store.mark_sealed(session);
    }

    pub fn mark_session_wal_replication_confirmed(&self, session: SessionId) {
        self.session_wal_store.mark_replication_confirmed(session);
    }

    pub fn mark_session_wal_collision_evaluated(&self, session: SessionId) {
        self.session_wal_store.mark_collision_evaluated(session);
    }

    /// Delete a session WAL when all retirement gates are satisfied.
    pub fn try_retire_session_wal(&self, session: SessionId) -> io::Result<bool> {
        self.session_wal_store
            .try_retire_wal(session)
            .map_err(EngineError::from)
            .map_err(engine_to_io)
    }

    /// Capture per-session stable ceilings for repeatable-read pinning (Phase 2).
    pub fn capture_version_vector(&self) -> VersionVector {
        self.session_watermarks.capture_version_vector()
    }

    /// Stable ceiling for one session's outstanding set.
    pub fn stable_for_session(
        &self,
        session: crate::infinitedb_core::hlc::SessionId,
    ) -> RevisionId {
        self.session_watermarks.stable_for(session)
    }

    /// Allocation high-water mark: highest revision id handed to a writer.
    ///
    /// A returned revision may not yet be visible; use [`Self::stable_revision`] or
    /// [`Self::sync`] before reading.
    pub fn revision(&self) -> RevisionId {
        self.session_watermarks.allocated()
    }

    /// Highest revision guaranteed applied and visible (repeatable-read ceiling).
    pub fn stable_revision(&self) -> RevisionId {
        self.session_watermarks.stable_revision()
    }

    /// Begin a concurrent read transaction pinned at the current revision.
    pub fn read(&self) -> crate::concurrent::read_txn::ReadTxn<'_> {
        crate::concurrent::read_txn::ReadTxn::new(self)
    }

    /// I/O queue depth and write-path counters across all backend threads.
    pub fn io_stats(&self) -> IoStats {
        let mut stats = match &*self.backend.lock() {
            WriteBackend::V4 { coordinator } => coordinator.io_stats(),
            WriteBackend::V3 { coordinator } => coordinator.io_stats(),
            WriteBackend::V2 { queue, io_handle, .. } => {
                let handle = io_handle.lock();
                IoStats {
                    queue_depth: queue.queued_count(),
                    direct_writes: handle.direct_writes(),
                    staged_writes: handle.staged_writes(),
                    staging_wal_frames: 0,
                    fast_path_seal_success: 0,
                    fast_path_seal_timeout: 0,
                    fast_path_wal_fallback: 0,
                }
            }
        };
        let ws = self.session_wal_store.write_stats();
        stats.fast_path_seal_success = ws.fast_path_seal_success;
        stats.fast_path_seal_timeout = ws.fast_path_seal_timeout;
        stats.fast_path_wal_fallback = ws.fast_path_wal_fallback;
        stats
    }

    /// Number of I/O shards (1 for format v2, per-space or per-Hilbert-shard for v3/v4).
    pub fn space_shard_count(&self) -> usize {
        match &*self.backend.lock() {
            WriteBackend::V4 { coordinator } => coordinator.shard_count(),
            WriteBackend::V3 { coordinator } => coordinator.shard_count(),
            WriteBackend::V2 { .. } => 1,
        }
    }

    pub(crate) fn query_ctx(&self) -> QueryCtx<'_> {
        QueryCtx {
            live_tail: self.v2_live_tail.as_deref(),
            space_tails: self.v3_space_tails.as_deref(),
            hilbert_tails: self.v4_hilbert_tails.as_deref(),
        }
    }

    /// Bulk insert on `main`; returns `(first_revision, last_revision)`.
    pub fn insert_many(
        &self,
        space: SpaceId,
        rows: Vec<(DimensionVector, Vec<u8>)>,
    ) -> io::Result<(RevisionId, RevisionId)> {
        self.insert_many_on_branch(BranchId::MAIN, space, rows)
    }

    /// Bulk insert on a branch.
    pub fn insert_many_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        rows: Vec<(DimensionVector, Vec<u8>)>,
    ) -> io::Result<(RevisionId, RevisionId)> {
        if rows.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "insert_many requires at least one row",
            ));
        }
        const CHUNK: usize = 4096;
        let count = rows.len() as u64;
        let range = self.default_write_session.stamp_n(count);
        let mut jobs = Vec::with_capacity(rows.len().min(CHUNK));
        let spaces = self.spaces.read();
        for (idx, (point, data)) in rows.into_iter().enumerate() {
            let rev = range.nth(idx as u64);
            let address = Address::new(space, point.clone());
            let hilbert_key = HilbertKey(space_key(&spaces, space, &point));
            let entry = WalEntry::Write {
                address,
                revision: rev,
                data,
            };
            jobs.push(WriteJob {
                branch_id: branch,
                revision: rev,
                entry,
                hilbert_key,
            });
            if jobs.len() >= CHUNK {
                self.enqueue_batch(jobs)?;
                jobs = Vec::new();
            }
        }
        drop(spaces);
        if !jobs.is_empty() {
            self.enqueue_batch(jobs)?;
        }
        Ok((range.first(), range.last()))
    }

    /// Manually compact small blocks in `space` using the space's configured policy.
    pub fn compact(&self, space: SpaceId) -> io::Result<()> {
        self.compact_with(space, None)
    }

    /// Manually compact `space`, optionally overriding the configured compaction policy.
    pub fn compact_with(
        &self,
        space: SpaceId,
        policy: Option<CompactionPolicy>,
    ) -> io::Result<()> {
        if let Some(p) = policy {
            self.compaction_overrides.lock().insert(space, p);
        }
        let result = (|| {
            self.sync()?;
            match &*self.backend.lock() {
                WriteBackend::V4 { coordinator } => coordinator.compact_space(space),
                WriteBackend::V3 { coordinator } => coordinator.compact_space(space),
                WriteBackend::V2 { .. } => Ok(()),
            }
        })();
        self.compaction_overrides.lock().remove(&space);
        result
    }

    fn enqueue(&self, job: WriteJob) -> io::Result<()> {
        let rev = job.revision;
        if job.branch_id != BranchId::MAIN {
            let branch_id = job.branch_id;
            let record = job.into_record();
            let space = record.address.space;
            if let Err(e) = self.branch_overlays.append_batch_with_durability(
                branch_id,
                space,
                vec![record],
                &self.root,
            ) {
                self.session_watermarks.retire_failed(rev, e.to_string());
                return Err(e);
            }
            self.session_watermarks.retire(rev);
            return Ok(());
        }
        let result = match &*self.backend.lock() {
            WriteBackend::V4 { coordinator } => coordinator.enqueue_write(job),
            WriteBackend::V3 { coordinator } => coordinator.enqueue_write(job),
            WriteBackend::V2 { queue, .. } => queue.enqueue_write(job),
        };
        if let Err(ref e) = result {
            self.session_watermarks.retire_failed(rev, e.to_string());
        }
        result
    }

    /// Revisions abandoned due to I/O failures (non-destructive observation).
    pub fn failed_revisions(&self) -> Vec<FailedRevision> {
        self.session_watermarks.failed_revisions()
    }

    /// Drain the failure log after explicit acknowledgment.
    pub fn take_failed_revisions(&self) -> Vec<FailedRevision> {
        self.session_watermarks.take_failed()
    }

    fn next_revision(&self) -> RevisionId {
        self.default_write_session.stamp()
    }

    /// Apply many records on a branch through one allocation and batch enqueue.
    pub(crate) fn apply_records_on_branch(
        &self,
        branch: BranchId,
        records: Vec<Record>,
    ) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let spaces = self.spaces.read();
        let mut jobs = Vec::with_capacity(records.len());
        for record in records.into_iter() {
            let revision = record.revision;
            let hilbert_key = if let Some(k) = record.hilbert_key.get() {
                k
            } else {
                HilbertKey(space_key(&spaces, record.address.space, &record.address.point))
            };
            let entry = if record.tombstone {
                WalEntry::Tombstone {
                    address: record.address.clone(),
                    revision,
                }
            } else {
                WalEntry::Write {
                    address: record.address.clone(),
                    revision,
                    data: record.data,
                }
            };
            jobs.push(WriteJob {
                branch_id: branch,
                revision,
                entry,
                hilbert_key,
            });
        }
        drop(spaces);
        self.enqueue_batch(jobs)
    }

    fn pad_judgment_index_bbox(
        min: DimensionVector,
        max: DimensionVector,
    ) -> (DimensionVector, DimensionVector) {
        use crate::infinitedb_core::judgment_index::JUDGMENT_INDEX_DIMS;
        let mut min_coords = min.coords;
        let mut max_coords = max.coords;
        while min_coords.len() < JUDGMENT_INDEX_DIMS {
            min_coords.push(0);
        }
        while max_coords.len() < JUDGMENT_INDEX_DIMS {
            max_coords.push(u32::MAX);
        }
        (
            DimensionVector::new(min_coords),
            DimensionVector::new(max_coords),
        )
    }

    fn hyperedge_id_from_storage_point(point: &DimensionVector) -> HyperedgeId {
        let c = &point.coords;
        let hi = c.first().copied().unwrap_or(0) as u64;
        let lo = c.get(1).copied().unwrap_or(0) as u64;
        HyperedgeId((hi << 32) | lo)
    }

    fn merge_incident_ids_from_assertion_delta(
        &self,
        edge_space: SpaceId,
        endpoint: &EndpointRef,
        direction: DirectionFilter,
        ids: Vec<HyperedgeId>,
        rev_ceiling: RevisionId,
        watermark: &VersionVector,
    ) -> io::Result<Vec<HyperedgeId>> {
        self.merge_incident_ids_from_assertion_delta_multi_space(
            &[edge_space],
            endpoint,
            direction,
            ids,
            rev_ceiling,
            watermark,
            None,
        )
    }

    fn merge_incident_ids_from_assertion_delta_multi_space(
        &self,
        edge_spaces: &[SpaceId],
        endpoint: &EndpointRef,
        direction: DirectionFilter,
        mut ids: Vec<HyperedgeId>,
        rev_ceiling: RevisionId,
        watermark: &VersionVector,
        admitted_sessions: Option<&[SessionId]>,
    ) -> io::Result<Vec<HyperedgeId>> {
        use std::collections::HashSet;
        let mut set: HashSet<HyperedgeId> = ids.drain(..).collect();
        let index_layout = registry_index_layout(&self.spaces.read());
        for &space in edge_spaces {
            let records = self.query_on_branch(BranchId::MAIN, space, Some(rev_ceiling))?;
            for r in records {
                if !record_in_derivation_delta(&r, watermark, rev_ceiling, admitted_sessions) {
                    continue;
                }
                if r.tombstone {
                    let id = Self::hyperedge_id_from_storage_point(&r.address.point);
                    set.remove(&id);
                    continue;
                }
                if let Ok(edge) = decode_edge_record(&r.data) {
                    let incident = edge.endpoints.iter().any(|ep| {
                        ep.space == endpoint.space && ep.node.coords == endpoint.node.coords
                    });
                    if incident
                        && filter_edges_by_direction(vec![edge.clone()], endpoint, direction)
                            .pop()
                            .is_some()
                    {
                        set.insert(edge.id);
                    }
                }
            }
        }
        let _ = index_layout;
        Ok(set.into_iter().collect())
    }

    fn fetch_hyperedge_for_flow_merge(
        &self,
        edge_spaces: &[SpaceId],
        id: HyperedgeId,
        as_of: Option<RevisionId>,
        rev_ceiling: RevisionId,
        watermark: &VersionVector,
        index_only: bool,
    ) -> io::Result<Option<Hyperedge>> {
        for &space in edge_spaces {
            if let Some(edge) = self.fetch_hyperedge_by_id(space, id, as_of)? {
                return Ok(Some(edge));
            }
        }
        if index_only || rev_ceiling <= watermark.scalar_meet() {
            return Ok(None);
        }
        for &space in edge_spaces {
            let records = self.query_on_branch(BranchId::MAIN, space, Some(rev_ceiling))?;
            for r in records {
                if !record_in_derivation_delta(&r, watermark, rev_ceiling, None) {
                    continue;
                }
                if r.tombstone {
                    continue;
                }
                if let Ok(edge) = decode_edge_record(&r.data) {
                    if edge.id == id {
                        return Ok(Some(edge));
                    }
                }
            }
        }
        Ok(None)
    }

    fn synthetic_flow_vector_records_from_delta(
        &self,
        rev_ceiling: RevisionId,
        watermark: &VersionVector,
    ) -> io::Result<Vec<Record>> {
        let q = default_flow_vector_quantization();
        let edge_spaces = edge_spaces_from_registry(&self.spaces.read());
        let mut synthetic = Vec::new();
        for space in edge_spaces {
            let records = self.query_on_branch(BranchId::MAIN, space, Some(rev_ceiling))?;
            for r in records {
                if !record_in_derivation_delta(&r, watermark, rev_ceiling, None) {
                    continue;
                }
                if r.tombstone {
                    continue;
                }
                if let Ok(edge) = decode_edge_record(&r.data) {
                    for row in prepare_flow_vector_derivation(&edge, q) {
                        synthetic.push(Record {
                            address: Address::new(row.space, row.point),
                            revision: r.revision,
                            data: row.data,
                            tombstone: row.tombstone,
                            hilbert_key: CachedHilbertKey::UNSET,
                        });
                    }
                }
            }
        }
        Ok(synthetic)
    }

    fn synthetic_index_records_from_delta(
        &self,
        edge_space: SpaceId,
        rev_ceiling: RevisionId,
        watermark: &VersionVector,
        index_layout: EndpointIndexLayout,
        admitted_sessions: Option<&[SessionId]>,
    ) -> io::Result<Vec<Record>> {
        let records = self.query_on_branch(BranchId::MAIN, edge_space, Some(rev_ceiling))?;
        let mut synthetic = Vec::new();
        for r in records {
            if !record_in_derivation_delta(&r, watermark, rev_ceiling, admitted_sessions) {
                continue;
            }
            if r.tombstone {
                continue;
            }
            if let Ok(edge) = decode_edge_record(&r.data) {
                for row in prepare_index_derivation(&edge, index_layout) {
                    synthetic.push(Record {
                        address: crate::infinitedb_core::address::Address::new(
                            row.space,
                            row.point,
                        ),
                        revision: r.revision,
                        data: row.data,
                        tombstone: row.tombstone,
                        hilbert_key: CachedHilbertKey::UNSET,
                    });
                }
            }
        }
        Ok(synthetic)
    }

    fn persist_meta(&self) -> io::Result<()> {
        let spaces_bytes = encode_to_vec(&*self.spaces.read(), standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("spaces.bin", &spaces_bytes)?;

        let branches_bytes = encode_to_vec(&*self.branches.read(), standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("branches.bin", &branches_bytes)?;

        let snapshots = snapshots_map_for_persist(&self.snapshots);
        let snapshots_bytes = encode_to_vec(&snapshots, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("snapshots.bin", &snapshots_bytes)?;

        let counters = PersistedCounters::new(
            self.session_watermarks.allocated().legacy_sequence(),
            self.next_block_id.load(Ordering::Relaxed),
            self.next_snapshot_id.load(Ordering::Relaxed),
            self.next_branch_id.load(Ordering::Relaxed),
            self.session_watermarks.next_session_counter(),
        );
        let counters_bytes = encode_to_vec(&counters, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("counters.bin", &counters_bytes)?;

        let branch_bases = self.branch_overlays.export_bases();
        let branch_bases_bytes = encode_to_vec(&branch_bases, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("branch_bases.bin", &branch_bases_bytes)?;

        let arbiter_streams_bytes = encode_to_vec(&*self.arbiter_streams.read(), standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store
            .write_meta("arbiter_streams.bin", &arbiter_streams_bytes)?;

        let frames_bytes = encode_to_vec(&*self.frames.read(), standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.store.write_meta("frames.bin", &frames_bytes)?;

        persist_session_wal_meta(&self.root.join("meta"), &self.session_wal_store.meta())?;
        Ok(())
    }
}

impl Drop for InfiniteDb {
    fn drop(&mut self) {
        self.derivation.shutdown();
        let _ = self.persist_meta();
        match &*self.backend.lock() {
            WriteBackend::V4 { coordinator } => {
                let _ = coordinator.shutdown_all();
            }
            WriteBackend::V3 { coordinator } => {
                let _ = coordinator.shutdown_all();
            }
            WriteBackend::V2 { queue, io_handle, .. } => {
                let _ = queue.shutdown();
                let _ = io_handle.lock().join();
            }
        }
    }
}

type MetaTuple = (
    SpaceRegistry,
    BranchRegistry,
    std::collections::BTreeMap<u64, crate::infinitedb_core::snapshot::Snapshot>,
    u64,
    u64,
    u64,
    u64,
    u32,
);

fn load_meta(store: &BlockStore) -> Option<MetaTuple> {
    let counters_bytes = store.read_meta("counters.bin").ok()?;
    let counters =
        crate::infinitedb_core::persisted_counters::decode_counters(&counters_bytes).ok()?;
    let revision = counters.revision;
    let next_block = counters.next_block;
    let next_snapshot = counters.next_snapshot;
    let next_branch = counters.next_branch;
    let next_session = counters.next_session;

    let spaces_bytes = store.read_meta("spaces.bin").ok()?;
    let (spaces, _): (SpaceRegistry, _) = decode_from_slice(&spaces_bytes, standard()).ok()?;

    let branches = store
        .read_meta("branches.bin")
        .ok()
        .and_then(|b| decode_from_slice::<BranchRegistry, _>(&b, standard()).ok())
        .map(|(r, _)| r)
        .unwrap_or_else(BranchRegistry::new);

    let snapshots = store
        .read_meta("snapshots.bin")
        .ok()
        .and_then(|b| {
            decode_from_slice::<
                std::collections::BTreeMap<u64, crate::infinitedb_core::snapshot::Snapshot>,
                _,
            >(&b, standard())
            .ok()
        })
        .map(|(m, _)| m)
        .unwrap_or_default();

    Some((
        spaces,
        branches,
        snapshots,
        revision,
        next_block,
        next_snapshot,
        next_branch,
        next_session,
    ))
}

pub(crate) struct QueryCtx<'a> {
    pub live_tail: Option<&'a LiveTailView>,
    pub space_tails: Option<&'a SpaceLiveTails>,
    pub hilbert_tails: Option<&'a HilbertLiveTails>,
}

fn default_meta() -> MetaTuple {
    (
        SpaceRegistry::new(),
        BranchRegistry::new(),
        std::collections::BTreeMap::new(),
        0,
        1,
        1,
        2,
        1,
    )
}
