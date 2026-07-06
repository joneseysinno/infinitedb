//! [`InfiniteDb`] — fire-and-forget writes with Hilbert-shard I/O (format v4/v5).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bincode::{config::standard, decode_from_slice, encode_to_vec};
use parking_lot::{Mutex, RwLock};

use crate::engine::branch_overlay::{BranchOverlayStore, OverlayKey};
use crate::engine::compactor::CompactionPolicyOverrides;
use crate::engine::derivation::{
    record_in_derivation_delta, AssertionEvent, DerivationBackpressurePolicy, DerivationBus,
    DerivationSink, DerivationStats, EdgeLocatorSubscriber, EndpointIndexSubscriber,
    FlowVectorSubscriber, WatermarkRegistry,
};
use crate::engine::flow_vector::{
    default_flow_vector_quantization, edge_id_from_flow_vector_index_record,
    prepare_flow_vector_derivation, resolve_flow_vector,
};
use crate::engine::staleness_closure::{forward_stale_closure, staleness_seed_endpoints};
use crate::engine::error::EngineError;
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
use crate::engine::io_thread::{IoStats, IoThreadConfig};
use crate::engine::merge::merge_branches;
use crate::engine::query::{
    query_bbox, query_inner, query_plan_stats, reset_query_plan_stats, snapshots_map_for_persist,
    space_key, QueryPlanStats,
};
use crate::engine::snapshot_store::SnapshotStore;
use crate::engine::session::{DurableIntent, SessionWatermarks, VersionVector, WriteSession};
use crate::engine::collision::CollisionEvaluation;
use crate::engine::replication_gate::ReplicationGatePolicy;
use crate::engine::timed_fast_path::{DurabilityMedium, TimedFastPathPolicy};
use crate::infinitedb_storage::session_fast_segment::FastSealOutcome;
use crate::engine::session_wal_store::{
    load_session_wal_meta, merge_recovered_entries, persist_session_wal_meta, SessionWalStore,
    wal_entry_revision,
};
use crate::engine::watermark::{FailedRevision, RevisionRange};
use crate::engine::write_queue::WriteJob;
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
    judgment::RESERVED_ARBITER_ID_THRESHOLD,
    space::{CompactionPolicy, EndpointIndexLayout, SpaceConfig, SpaceRegistry},
    snapshot::SnapshotId,
    traversal::{
        hypergraph_acyclic_for_kinds, FrameTraversalSpec, TraversalResult, TraversalSpec,
    },
};
use crate::infinitedb_storage::{
    format::{FormatVersion, FORMAT_VERSION_V4},
    nvme::BlockStore,
    wal::WalEntry,
};

/// Options for opening [`InfiniteDb`] (formats v4–v5).
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
    /// Session WAL replication gate posture (hardening D-P8, default `NotApplicable`).
    pub replication_gate: ReplicationGatePolicy,
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
            replication_gate: ReplicationGatePolicy::default(),
        }
    }
}

impl OpenOptions {
    /// Open or create a database at `dir` using these options.
    pub fn open<P: AsRef<Path>>(&self, dir: P) -> Result<InfiniteDb, EngineError> {
        InfiniteDb::open_with_options(dir, self)
    }
}

/// Thread-safe embedded database with concurrent reads and fire-and-forget writes.
///
/// **Blocking API:** methods may block on disk I/O, locks, and queue backpressure. Async
/// callers must offload them (see [`crate::RequestExecutor`] under the `server` feature).
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
    replication_gate: ReplicationGatePolicy,
    compaction_overrides: CompactionPolicyOverrides,
    next_block_id: Arc<AtomicU64>,
    next_snapshot_id: Arc<AtomicU64>,
    next_branch_id: Arc<AtomicU64>,
    pub(crate) branch_overlays: Arc<BranchOverlayStore>,
    #[cfg(feature = "sync")]
    conflicts: Arc<crate::infinitedb_sync::conflict_queue::ConflictQueue>,
    coordinator: Arc<HilbertCoordinator>,
    hilbert_tails: Arc<HilbertLiveTails>,
    derivation: Arc<DerivationBus>,
    arbiter_streams: Arc<RwLock<HashMap<ArbiterId, ArbiterStream>>>,
    frames: Arc<RwLock<HashMap<FrameId, FrameDefinition>>>,
    next_frame_id: Arc<AtomicU64>,
    density: Arc<crate::engine::density_stat::DensityTracker>,
    registration: parking_lot::Mutex<()>,
}

/// Applies derived index rows through the Hilbert write coordinator.
struct DbDerivationSink {
    spaces: Arc<RwLock<SpaceRegistry>>,
    coordinator: Arc<HilbertCoordinator>,
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
        self.coordinator.enqueue_batch(jobs).map_err(EngineError::from)?;
        Ok(())
    }
}

impl InfiniteDb {
    /// Open or create a database at `dir` with default [`OpenOptions`].
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self, EngineError> {
        OpenOptions::default().open(dir)
    }

    /// Open or create a database at `dir` with explicit tuning and format version.
    pub fn open_with_options<P: AsRef<Path>>(dir: P, options: &OpenOptions) -> Result<Self, EngineError> {
        let root = dir.as_ref().to_path_buf();
        let store = Arc::new(
            BlockStore::open_with_cache(root.clone(), options.block_cache_bytes)
                .map_err(EngineError::from)?,
        );

        let format_version = match FormatVersion::read_from_meta(&root.join("meta"))
            .map_err(EngineError::from)?
        {
            Some(v) => v.0,
            None => options.format_version.unwrap_or(FORMAT_VERSION_V4),
        };

        if !FormatVersion(format_version).is_supported() {
            let message = if matches!(format_version, 2 | 3) {
                format!(
                    "format version {format_version} is no longer supported; \
                     open with a v4/v5 database or create a new directory"
                )
            } else {
                format!("unsupported concurrent format version {format_version}")
            };
            return Err(EngineError::Other { message });
        }

        if FormatVersion::read_from_meta(&root.join("meta"))
            .map_err(EngineError::from)?
            .is_none()
        {
            FormatVersion(format_version)
                .write_to_meta(&root.join("meta"))
                .map_err(EngineError::from)?;
            std::fs::create_dir_all(root.join("spaces")).map_err(EngineError::from)?;
        }

        let branch_overlays = Arc::new(BranchOverlayStore::new());
        branch_overlays
            .replay_all(&root)
            .map_err(EngineError::from)?;
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
        let conflicts = Arc::new(
            crate::infinitedb_sync::conflict_queue::ConflictQueue::open(&root)
                .map_err(EngineError::from)?,
        );

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
        coordinator
            .bootstrap_registered_spaces()
            .map_err(EngineError::from)?;
        coordinator.sync_all().map_err(EngineError::from)?;
        let hilbert_tails = coordinator.live_tails_arc();
        let coordinator = Arc::new(coordinator);
        let derivation_watermarks = Arc::new(WatermarkRegistry::new());
        derivation_watermarks.register("endpoint_index", RevisionId::ZERO);
        derivation_watermarks.register("edge_locator", RevisionId::ZERO);
        derivation_watermarks.register("flow_vector_index", RevisionId::ZERO);
        let subscribers: Vec<Box<dyn crate::engine::derivation::DerivationSubscriber>> = vec![
            Box::new(EndpointIndexSubscriber::new(Arc::clone(&spaces))),
            Box::new(EdgeLocatorSubscriber),
            Box::new(FlowVectorSubscriber::new(Arc::clone(&spaces))),
        ];
        let sink = Arc::new(DbDerivationSink {
            spaces: Arc::clone(&spaces),
            coordinator: Arc::clone(&coordinator),
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
            replication_gate: options.replication_gate,
            compaction_overrides,
            next_block_id,
            next_snapshot_id,
            next_branch_id,
            branch_overlays,
            #[cfg(feature = "sync")]
            conflicts,
            coordinator,
            hilbert_tails,
            derivation,
            arbiter_streams,
            frames,
            next_frame_id,
            density: Arc::new(crate::engine::density_stat::DensityTracker::new()),
            registration: parking_lot::Mutex::new(()),
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
    ) -> Result<(), EngineError> {
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
                    .map_err(EngineError::from)?;
            }
        }
        Ok(())
    }

    fn apply_recovered_fast_segments(&self) -> Result<(), EngineError> {
        let recovered = self.session_wal_store.recover_fast_segments();
        for batch in recovered {
            if !batch.entries.is_empty() {
                self.persist_uncommitted_session_fragments(batch.session, &batch.entries)?;
            }
            self.session_wal_store
                .reset_fast_after_recovery(batch.session)
                .map_err(EngineError::from)?;
        }
        Ok(())
    }

    fn replay_session_wal_entries(&self, entries: &[WalEntry]) -> Result<(), EngineError> {
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
    ) -> Result<(), EngineError> {
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
        let _ = self.persist_operation_errors(source_space, record)?;
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

    /// On-disk format version (4 or 5) for this database directory.
    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    /// Register a new space and persist catalog metadata. Required before writes to that space.
    ///
    /// Unless [`SpaceConfig::without_error_space`] is set, auto-registers a companion
    /// `{name}_errors` space for operation-level error records (M5).
    pub fn register_space(&self, mut config: SpaceConfig) -> Result<(), EngineError> {
        let _guard = self.registration.lock();
        self.validate_space_config(&config)?;
        let needs_error_space = !config.skip_error_space
            && config.id != ENDPOINT_INDEX_SPACE
            && config.id != JUDGMENT_INDEX_SPACE
            && config.id != FLOW_VECTOR_INDEX_SPACE
            && config.error_space.is_none();
        let space_id = config.id;
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
            registry.register(config.clone())?;
        } else {
            self.spaces.write().register(config.clone())?;
        }
        let space_dir = self.root.join("spaces").join(space_id.0.to_string());
        std::fs::create_dir_all(&space_dir)?;
        self.persist_meta()?;
        self.sync_placement_mirror(&config)?;
        Ok(())
    }

    /// Idempotent space registration (INV-REGISTER-IDEMPOTENT / T8).
    pub fn register_or_get_space(&self, mut config: SpaceConfig) -> Result<SpaceId, EngineError> {
        let _guard = self.registration.lock();
        self.validate_space_config(&config)?;
        let needs_error_space = !config.skip_error_space
            && config.id != ENDPOINT_INDEX_SPACE
            && config.id != JUDGMENT_INDEX_SPACE
            && config.id != FLOW_VECTOR_INDEX_SPACE
            && config.error_space.is_none();
        let mut registry = self.spaces.write();
        if needs_error_space {
            let err_id = SpaceRegistry::derive_error_space_id(config.id);
            if registry.get(err_id).is_none() {
                let err_config = SpaceConfig::new(err_id, format!("{}_errors", config.name), 2)
                    .without_error_space();
                registry.register(err_config)?;
                let err_dir = self.root.join("spaces").join(err_id.0.to_string());
                std::fs::create_dir_all(&err_dir)?;
            }
            config.error_space = Some(err_id);
        }
        let id = registry.register_or_get(config.clone())?;
        drop(registry);
        let space_dir = self.root.join("spaces").join(id.0.to_string());
        std::fs::create_dir_all(&space_dir)?;
        self.persist_meta()?;
        if let Some(cfg) = self.spaces.read().get(id).cloned() {
            self.sync_placement_mirror(&cfg)?;
        }
        Ok(id)
    }

    fn validate_space_config(&self, config: &SpaceConfig) -> Result<(), EngineError> {
        if config.bits_per_dim == 0 {
            return Err(EngineError::InvalidSpaceConfig {
                message: "bits_per_dim must be at least 1".into(),
            });
        }
        if config.bits_per_dim > 32 {
            return Err(EngineError::InvalidSpaceConfig {
                message: format!(
                    "bits_per_dim must be <= 32 (got {})",
                    config.bits_per_dim
                ),
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
        Ok(())
    }

    fn sync_placement_mirror(&self, config: &SpaceConfig) -> Result<(), EngineError> {
        if let Some(row) = crate::engine::placement_mirror::prepare_placement_mirror_row(config) {
            let rev = self.default_write_session.stamp()?;
            self.apply_mirror_rows(vec![row], rev)?;
        }
        Ok(())
    }

    fn apply_mirror_rows(
        &self,
        rows: Vec<HypergraphWriteRow>,
        source_revision: RevisionId,
    ) -> Result<(), EngineError> {
        if rows.is_empty() {
            return Ok(());
        }
        let spaces = self.spaces.read();
        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            let config = spaces
                .get(row.space)
                .ok_or(EngineError::SpaceNotFound(row.space))?;
            if !row.structural {
                crate::engine::write_validate::validate_point_write(
                    config,
                    &row.point,
                    false,
                )?;
            } else {
                crate::engine::write_validate::validate_point_write(
                    config,
                    &row.point,
                    true,
                )?;
            }
            let hilbert_key = HilbertKey(space_key(&spaces, row.space, &row.point));
            let address = Address::new(row.space, row.point);
            let entry = if row.tombstone {
                WalEntry::Tombstone {
                    address,
                    revision: source_revision,
                }
            } else {
                WalEntry::Write {
                    address,
                    revision: source_revision,
                    data: row.data,
                }
            };
            jobs.push(WriteJob::main(source_revision, entry, hilbert_key));
        }
        drop(spaces);
        self.coordinator.enqueue_batch(jobs).map_err(EngineError::from)?;
        Ok(())
    }

    /// Direct children in the space tower (T3).
    pub fn list_children(&self, parent: SpaceId) -> Vec<SpaceId> {
        self.spaces.read().children_of(parent)
    }

    /// Depth-first subtree configs (T3).
    pub fn get_subtree(&self, root: SpaceId) -> Vec<(SpaceId, SpaceConfig)> {
        self.spaces.read().subtree(root)
    }

    /// Per-space density statistic (T9).
    pub fn space_density(&self, space: SpaceId) -> crate::engine::density_stat::SpaceDensity {
        self.density.get(space)
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
    fn recover_derivation_on_open(&self) -> Result<(), EngineError> {
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
                        self.derivation.submit(AssertionEvent::delete(
                            space,
                            edge,
                            record.revision,
                            BranchId::MAIN,
                        ))?;
                    }
                } else if let Ok(edge) = decode_edge_record(&record.data) {
                    if edge.id != id {
                        continue;
                    }
                    live_edges.insert(edge.id, edge.clone());
                    self.derivation.submit(AssertionEvent::upsert(
                        space,
                        edge,
                        record.revision,
                        BranchId::MAIN,
                    ))?;
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
    ) -> Result<Vec<Record>, EngineError> {
        let ctx = self.query_ctx();
        let branch_id = if branch == BranchId::MAIN {
            None
        } else {
            Some(branch)
        };
        query_inner(
            &self.store,
            &self.snapshots,
            None,
            &self.spaces.read(),
            &self.session_watermarks,
            space,
            None,
            None,
            None,
            true,
            Some(ctx.hilbert_tails),
            Some(&self.branch_overlays),
            branch_id,
        )
        .map_err(EngineError::from)
    }

    /// Query with optional scalar ceiling or per-session vector pin.
    pub fn query_on_branch_pinned(
        &self,
        branch: BranchId,
        space: SpaceId,
        as_of: Option<RevisionId>,
        pinned_vector: Option<&VersionVector>,
    ) -> Result<Vec<Record>, EngineError> {
        let ctx = self.query_ctx();
        let branch_id = if branch == BranchId::MAIN {
            None
        } else {
            Some(branch)
        };
        query_inner(
            &self.store,
            &self.snapshots,
            None,
            &self.spaces.read(),
            &self.session_watermarks,
            space,
            None,
            as_of,
            pinned_vector,
            false,
            Some(ctx.hilbert_tails),
            Some(&self.branch_overlays),
            branch_id,
        )
        .map_err(EngineError::from)
    }

    /// Bounding-box query with optional scalar ceiling or per-session vector pin.
    pub fn query_bbox_on_branch_pinned(
        &self,
        branch: BranchId,
        space: SpaceId,
        min: DimensionVector,
        max: DimensionVector,
        as_of: Option<RevisionId>,
        pinned_vector: Option<&VersionVector>,
    ) -> Result<Vec<Record>, EngineError> {
        let ctx = self.query_ctx();
        let branch_id = if branch == BranchId::MAIN {
            None
        } else {
            Some(branch)
        };
        query_bbox(
            &self.store,
            &self.snapshots,
            None,
            &self.spaces.read(),
            &self.session_watermarks,
            space,
            min,
            max,
            as_of,
            pinned_vector,
            Some(ctx.hilbert_tails),
            Some(&self.branch_overlays),
            branch_id,
        )
        .map_err(EngineError::from)
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
    pub fn insert_hyperedge(
        &self,
        space: SpaceId,
        edge: Hyperedge,
    ) -> Result<RevisionId, EngineError> {
        self.insert_hyperedge_on_branch(BranchId::MAIN, space, edge)
    }

    /// Branch-aware hyperedge insert.
    pub fn insert_hyperedge_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        mut edge: Hyperedge,
    ) -> Result<RevisionId, EngineError> {
        edge.validate().map_err(EngineError::from)?;
        self.ensure_endpoint_index_space()?;
        self.ensure_flow_vector_index_space()?;
        if branch == BranchId::MAIN {
            self.check_derivation_backpressure()?;
            let rev = self.default_write_session.stamp()?;
            if let Some(ref prov) = edge.authoring_frame {
                validate_authoring_provenance(prov, rev)?;
            }
            if let Some(ref comp) = edge.computation {
                self.validate_computation_inputs(comp)?;
            }
            edge.valid_from = rev;
            let row = prepare_assertion_write(space, &edge).map_err(EngineError::from)?;
            let records = rows_to_records(&[row], rev);
            self.apply_records_on_branch(branch, records)?;
            self.derivation
                .submit(AssertionEvent::upsert(space, edge, rev, branch))?;
            Ok(rev)
        } else {
            let count = 1 + edge.endpoints.len();
            let range = self.default_write_session.stamp_n(count as u64)?;
            edge.valid_from = range.first();
            let index_layout = registry_index_layout(&self.spaces.read());
            let rows = prepare_writes(space, &edge, index_layout).map_err(EngineError::from)?;
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
    ) -> Result<RevisionId, EngineError> {
        let kind_label = kind.label().to_string();
        if let Some(catalog) = catalog {
            catalog
                .validate_edge_kind(&kind_label)
                .map_err(|e| EngineError::InvalidSpaceConfig {
                    message: e.to_string(),
                })?;
            for ep in &endpoints {
                catalog
                    .validate_endpoint_role(&ep.role)
                    .map_err(|e| EngineError::InvalidSpaceConfig {
                        message: e.to_string(),
                    })?;
            }
            catalog
                .validate_edge_directionality(&kind_label, directionality)
                .map_err(|e| EngineError::InvalidSpaceConfig {
                    message: e.to_string(),
                })?;
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
        edge.validate().map_err(EngineError::from)?;
        self.insert_hyperedge(space, edge)
    }

    /// Logically delete a hyperedge by id (payload + endpoint index rows).
    pub fn delete_hyperedge(
        &self,
        space: SpaceId,
        id: HyperedgeId,
    ) -> Result<RevisionId, EngineError> {
        self.delete_hyperedge_on_branch(BranchId::MAIN, space, id)
    }

    /// Branch-aware hyperedge delete.
    pub fn delete_hyperedge_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        id: HyperedgeId,
    ) -> Result<RevisionId, EngineError> {
        self.ensure_endpoint_index_space()?;
        let edge = self
            .fetch_hyperedge_by_id_on_branch(branch, space, id, None)
            .map_err(EngineError::from)?;
        if branch == BranchId::MAIN {
            self.check_derivation_backpressure()?;
            let rev = self.default_write_session.stamp()?;
            let row = if let Some(ref e) = edge {
                prepare_assertion_tombstone(space, e.id)
            } else {
                prepare_assertion_tombstone(space, id)
            };
            let records = rows_to_records(&[row], rev);
            self.apply_records_on_branch(branch, records)?;
            if let Some(e) = edge {
                self.derivation
                    .submit(AssertionEvent::delete(space, e, rev, branch))?;
            }
            Ok(rev)
        } else {
            let index_layout = registry_index_layout(&self.spaces.read());
            let rows = match edge {
                Some(e) => prepare_deletes(space, &e, index_layout),
                None => vec![prepare_assertion_tombstone(space, id)],
            };
            let count = rows.len() as u64;
            let range = self.default_write_session.stamp_n(count)?;
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
    ) -> Result<Option<Hyperedge>, EngineError> {
        self.fetch_hyperedge_by_id_on_branch(BranchId::MAIN, space, id, as_of)
    }

    fn fetch_hyperedge_by_id_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        id: HyperedgeId,
        as_of: Option<RevisionId>,
    ) -> Result<Option<Hyperedge>, EngineError> {
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
    ) -> Result<Vec<Hyperedge>, EngineError> {
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
    ) -> Result<Vec<Hyperedge>, EngineError> {
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
    ) -> Result<Vec<Hyperedge>, EngineError> {
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
    ) -> Result<Vec<Hyperedge>, EngineError> {
        self.ensure_endpoint_index_space()?;
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
    ) -> Result<usize, EngineError> {
        self.count_incident_edges_for_endpoint_directed(endpoint, as_of, DirectionFilter::Any)
    }

    /// Count incident hyperedges with index-resident direction filtering (M2).
    pub fn count_incident_edges_for_endpoint_directed(
        &self,
        endpoint: &EndpointRef,
        as_of: Option<RevisionId>,
        direction: DirectionFilter,
    ) -> Result<usize, EngineError> {
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
    ) -> Result<usize, EngineError> {
        self.ensure_endpoint_index_space()?;
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
    pub fn compact_endpoint_index(&self, edge_spaces: &[SpaceId]) -> Result<(), EngineError> {
        self.sync_derivation();
        self.ensure_endpoint_index_space()?;
        if self.endpoint_index_layout() != EndpointIndexLayout::V2PolarityDim {
            return Err(EngineError::InvalidSpaceConfig {
                message: "endpoint index layout must be upgraded to V2 before compaction rewrite"
                    .into(),
            });
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
            let range = self.default_write_session.stamp_n(rewrite_rows.len() as u64)?;
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
    ) -> Result<Vec<HyperedgeId>, EngineError> {
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
    ) -> Result<Vec<Hyperedge>, EngineError> {
        Ok(self
            .query_hyperedges(space, as_of)?
            .into_iter()
            .filter(|e| e.kind.as_str() == kind)
            .collect())
    }

    /// Directional hypergraph traversal from a starting endpoint (M3).
    pub fn traverse_hypergraph(&self, spec: &TraversalSpec) -> Result<TraversalResult, EngineError> {
        self.traverse_hypergraph_with_options(spec, QueryOptions::default())
    }

    /// Traversal with delta-merge over un-derived index rows (M4).
    pub fn traverse_hypergraph_with_options(
        &self,
        spec: &TraversalSpec,
        options: QueryOptions,
    ) -> Result<TraversalResult, EngineError> {
        self.ensure_endpoint_index_space()?;
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
        .map_err(EngineError::from)
    }

    /// Per-kind acyclicity check over directed hyperedges in `edge_space` (M3).
    pub fn check_hypergraph_acyclic(
        &self,
        edge_space: SpaceId,
        kinds: &[HyperedgeKind],
        as_of: Option<RevisionId>,
    ) -> Result<bool, EngineError> {
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
    ) -> Result<RevisionId, EngineError> {
        self.insert_on_branch(BranchId::MAIN, space, point, data)
    }

    /// Fire-and-forget insert on a branch (overlay for non-`main` branches).
    pub fn insert_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        point: DimensionVector,
        data: Vec<u8>,
    ) -> Result<RevisionId, EngineError> {
        self.insert_on_branch_inner(branch, space, point, data, false)
    }

    /// Insert a structural record at a dyadic cell-center (D-T6).
    pub fn insert_structural(
        &self,
        space: SpaceId,
        point: DimensionVector,
        data: Vec<u8>,
    ) -> Result<RevisionId, EngineError> {
        self.insert_on_branch_inner(BranchId::MAIN, space, point, data, true)
    }

    fn insert_on_branch_inner(
        &self,
        branch: BranchId,
        space: SpaceId,
        point: DimensionVector,
        data: Vec<u8>,
        structural: bool,
    ) -> Result<RevisionId, EngineError> {
        let config = self
            .spaces
            .read()
            .get(space)
            .cloned()
            .ok_or(EngineError::SpaceNotFound(space))?;
        crate::engine::write_validate::validate_point_write(&config, &point, structural)?;
        let rev = self.next_revision();
        let address = Address::new(space, point.clone());
        let key = space_key(&self.spaces.read(), space, &point);
        let hilbert_key = HilbertKey(key);
        self.density.observe_key(
            space,
            key,
            crate::infinitedb_index::composite::KeyConfig {
                bits_per_dim: config.bits_per_dim,
            },
            config.dims,
        );
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
    pub fn delete(
        &self,
        space: SpaceId,
        point: DimensionVector,
    ) -> Result<RevisionId, EngineError> {
        self.delete_on_branch(BranchId::MAIN, space, point)
    }

    /// Fire-and-forget delete on a branch.
    pub fn delete_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        point: DimensionVector,
    ) -> Result<RevisionId, EngineError> {
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
            let rev = self.default_write_session.stamp()?;
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
        let rev = self.default_write_session.stamp()?;
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
    ) -> Result<Vec<OperationErrorRecord>, EngineError> {
        let error_space = self.error_space_for_data(data_space)?;
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
        let rev = self.default_write_session.stamp()?;
        let row = prepare_error_tombstone(error_space, range_start);
        let records = rows_to_records(&[row], rev);
        self.apply_records_on_branch(BranchId::MAIN, records)?;
        Ok(())
    }

    /// Compact resolved error records beyond the data space's [`ErrorRetentionPolicy`].
    pub fn purge_resolved_errors(&self, data_space: SpaceId) -> Result<usize, EngineError> {
        use std::collections::HashMap;

        let keep = {
            let registry = self.spaces.read();
            let config = registry
                .get(data_space)
                .ok_or(EngineError::SpaceNotFound(data_space))?;
            match config.error_retention.as_ref().and_then(|p| p.max_resolved_keep) {
                Some(k) => k,
                None => return Ok(0),
            }
        };
        let error_space = self.error_space_for_data(data_space)?;
        self.sync()?;
        let history = self.query_history_on_branch(BranchId::MAIN, error_space)?;

        let mut by_point: HashMap<DimensionVector, Vec<&Record>> = HashMap::new();
        for record in &history {
            by_point
                .entry(record.address.point.clone())
                .or_default()
                .push(record);
        }

        let mut resolved = Vec::new();
        let mut unresolved = Vec::new();
        for (point, records) in &by_point {
            let latest = records
                .iter()
                .max_by_key(|r| r.revision)
                .expect("non-empty group");
            if latest.tombstone {
                resolved.push((point.clone(), latest.revision));
            } else {
                unresolved.push(latest.revision);
            }
        }
        resolved.sort_by_key(|(_, rev)| *rev);
        if resolved.len() <= keep {
            return Ok(0);
        }
        let purge_count = resolved.len() - keep;
        let cutoff = resolved[purge_count].1;
        for rev in unresolved {
            if rev < cutoff {
                return Err(EngineError::InvalidSpaceConfig {
                    message: format!(
                        "cannot purge resolved errors: unresolved error at revision {:?} \
                         is older than retention cutoff {:?}",
                        rev, cutoff
                    ),
                });
            }
        }
        self.compact_with(
            error_space,
            Some(CompactionPolicy::RetentionWindow {
                version_horizon: cutoff,
                tombstone_horizon: cutoff,
            }),
        )?;
        Ok(purge_count)
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
    ) -> Result<FrameTraversalResult, EngineError> {
        let def = self
            .get_frame(spec.frame_id)
            .ok_or(EngineError::FrameNotFound(spec.frame_id))?;
        let pin = self.resolve_frame_pin(
            spec.as_of.or(spec.base.as_of),
            spec.version_vector.clone(),
            def.default_as_of,
        );
        let pin_ceiling = pin.scalar_ceiling();
        let judgments_cache = self.collect_frame_judgments(
            &def.judgment_overlay,
            &DimensionVector::new(vec![0, 0]),
            &DimensionVector::new(vec![u32::MAX, u32::MAX]),
            pin_ceiling,
        )?;
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
        self.ensure_endpoint_index_space()?;
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
        )
        .map_err(EngineError::from)?;
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
        if id.0 < RESERVED_ARBITER_ID_THRESHOLD {
            return Err(EngineError::ReservedArbiterId(id.0));
        }
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
        let range = self.default_write_session.stamp_n(rows.len() as u64)?;
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
    ) -> Result<Option<JudgmentRecord>, EngineError> {
        let assertion_space = self
            .arbiter_streams
            .read()
            .get(&stream)
            .map(|s| s.assertion_space)
            .ok_or(EngineError::ArbiterStreamNotFound(stream.0))?;
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
    ) -> Result<Vec<JudgmentRecord>, EngineError> {
        let assertion_space = self
            .arbiter_streams
            .read()
            .get(&stream)
            .map(|s| s.assertion_space)
            .ok_or(EngineError::ArbiterStreamNotFound(stream.0))?;
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
    ) -> Result<Vec<JudgmentRecord>, EngineError> {
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

    /// Per-session endpoint index derivation watermark (Phase 6).
    pub fn endpoint_index_watermark_vector(&self) -> VersionVector {
        self.derivation.endpoint_index_watermark_vector()
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
    ) -> Result<Vec<FlowVectorRecord>, EngineError> {
        self.ensure_flow_vector_index_space()?;
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
                let Some(vector) = resolve_flow_vector(&edge, &self.spaces.read()) else {
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
    ) -> Result<Option<FlowVectorRecord>, EngineError> {
        let rev_ceiling = as_of.unwrap_or_else(|| self.revision());
        let Some(edge) = self.fetch_hyperedge_by_id(edge_space, id, as_of)? else {
            return Ok(None);
        };
        if !edge.is_active_at(rev_ceiling) {
            return Ok(None);
        }
        let Some(vector) = resolve_flow_vector(&edge, &self.spaces.read()) else {
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
    ) -> Result<FreshnessReport, EngineError> {
        let edge = self
            .fetch_hyperedge_by_id(edge_space, id, as_of)?
            .ok_or(EngineError::Other {
                message: format!("hyperedge {:?} not found", id),
            })?;
        check_computation_freshness(&edge, &|pin| self.fetch_subject_revision_at(pin, as_of))
            .ok_or(EngineError::InvalidSpaceConfig {
                message: "hyperedge has no computation provenance".into(),
            })
    }

    /// Forward stale closure from a changed subject pin (M7).
    pub fn query_stale_downstream(
        &self,
        subject: SubjectPin,
        edge_space: SpaceId,
        max_depth: usize,
        as_of: Option<RevisionId>,
    ) -> Result<Vec<StaleTarget>, EngineError> {
        self.query_staleness_by_source_revision(subject, edge_space, max_depth, as_of)
    }

    /// Observability alias for source-revision staleness closure (M7).
    pub fn query_staleness_by_source_revision(
        &self,
        source: SubjectPin,
        edge_space: SpaceId,
        max_depth: usize,
        as_of: Option<RevisionId>,
    ) -> Result<Vec<StaleTarget>, EngineError> {
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
        .map_err(EngineError::from)
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
                .query_judgments_in_region(layer.arbiter, min.clone(), max.clone(), Some(as_of))?;
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
    ) -> Result<MergeResult, EngineError> {
        self.sync()?;
        let ctx = self.query_ctx();
        let mut result = merge_branches(
            &self.store,
            &self.snapshots,
            None,
            Some(ctx.hilbert_tails),
            &self.branch_overlays,
            &self.spaces.read(),
            &self.session_watermarks,
            &self.branches.read(),
            target,
            source,
            strategy,
            resolver.as_deref(),
        )
        .map_err(EngineError::from)?;
        if strategy == MergeStrategy::Interactive && !result.conflicts.is_empty() {
            return Ok(result);
        }
        let applied = std::mem::take(&mut result.applied_records);
        self.apply_records_on_branch(target, applied)?;
        self.branch_overlays
            .clear_branch(source, &self.root)
            .map_err(EngineError::from)?;
        self.sync()?;
        Ok(result)
    }

    /// Query `space` through a branch overlay.
    pub fn query_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        as_of: Option<RevisionId>,
    ) -> Result<Vec<Record>, EngineError> {
        self.query_on_branch_pinned(branch, space, as_of, None)
    }

    /// Enqueue writes across multiple spaces (ordered by space id).
    ///
    /// Every [`WriteJob::revision`] must have been allocated through
    /// [`RevisionWatermark::allocate`] or [`RevisionWatermark::allocate_n`]
    /// (via `insert`, `insert_many`, etc.) so the revision is already
    /// registered as outstanding.
    pub fn enqueue_batch(&self, jobs: Vec<WriteJob>) -> Result<(), EngineError> {
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
                return result.map_err(EngineError::from);
            }
            for rev in revs {
                self.session_watermarks.retire(rev);
            }
        }
        if main_jobs.is_empty() {
            return Ok(());
        }
        let main_revs: Vec<RevisionId> = main_jobs.iter().map(|j| j.revision).collect();
        let result = self
            .coordinator
            .enqueue_batch(main_jobs)
            .map_err(EngineError::from);
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
    ) -> Result<Vec<Record>, EngineError> {
        self.query_on_branch(BranchId::MAIN, space, as_of)
    }

    /// Bounding-box query on `main`.
    pub fn query_bbox(
        &self,
        space: SpaceId,
        min: DimensionVector,
        max: DimensionVector,
        as_of: Option<RevisionId>,
    ) -> Result<Vec<Record>, EngineError> {
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
    ) -> Result<Vec<Record>, EngineError> {
        self.query_bbox_on_branch_pinned(branch, space, min, max, as_of, None)
    }

    /// Flush pending writes for one space to durable storage without syncing all spaces.
    pub fn flush(&self, space: SpaceId) -> Result<(), EngineError> {
        self.coordinator
            .flush_space(space)
            .map_err(EngineError::from)?;
        self.persist_meta()
    }

    /// Flush all write queues and persist metadata. Call after writes to make data queryable.
    pub fn sync(&self) -> Result<(), EngineError> {
        self.derivation.flush();
        self.coordinator.sync_all().map_err(EngineError::from)?;
        self.persist_meta()
    }

    /// Allocate a contiguous revision range for custom [`WriteJob`] batches.
    ///
    /// Revisions are registered as outstanding until the write path retires them.
    pub fn allocate_revisions(&self, count: u64) -> RevisionRange {
        self.default_write_session
            .stamp_n(count)
            .expect("global session stamp_n")
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
    ) -> Result<RevisionId, EngineError> {
        let rev = session.stamp()?;
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
                    .append_frame(session.id(), &entry)?;
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
    /// [`Self::commit_session_intent`] to publish.
    pub fn insert_hyperedge_with_session(
        &self,
        session: &WriteSession,
        space: SpaceId,
        mut edge: Hyperedge,
    ) -> Result<RevisionId, EngineError> {
        if !session.uses_session_wal() {
            return Err(EngineError::InvalidSpaceConfig {
                message: "insert_hyperedge_with_session requires an explicit session WAL".into(),
            });
        }
        edge.validate().map_err(EngineError::from)?;
        self.ensure_endpoint_index_space()?;
        self.ensure_flow_vector_index_space()?;
        self.check_derivation_backpressure_for(
            session.id(),
            self.session_watermarks.watermark_for(session.id()).allocated(),
        )?;
        let rev = session.stamp()?;
        if let Some(ref prov) = edge.authoring_frame {
            validate_authoring_provenance(prov, rev)?;
        }
        if let Some(ref comp) = edge.computation {
            self.validate_computation_inputs(comp)?;
        }
        edge.valid_from = rev;
        let row = prepare_assertion_write(space, &edge).map_err(EngineError::from)?;
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
                .append_frame(session.id(), &entry)?;
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
    pub fn sync_session_wal(
        &self,
        session: &WriteSession,
    ) -> Result<DurableIntent, EngineError> {
        if session.uses_session_wal() {
            if self.timed_fast_path.enabled {
                let entries = session.peek_pending_entries();
                if !entries.is_empty() {
                    match self.session_wal_store.try_fast_seal(
                        session.id(),
                        &entries,
                        self.timed_fast_path.direct_seal_deadline,
                    )? {
                        FastSealOutcome::Sealed => {
                            self.persist_meta()?;
                            return Ok(session.mark_durable(DurabilityMedium::FastSegment));
                        }
                        FastSealOutcome::TimedOut => {
                            self.session_wal_store
                                .append_buffered_to_wal(session.id(), &entries)?;
                        }
                    }
                }
            }
            self.session_wal_store.sync_group(session.id())?;
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
    ) -> Result<crate::engine::collision::IntentCommitOutcome, EngineError> {
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

        let collisions =
            self.detect_checkpoint_collisions(session.id(), &checkpoint, &buffered)?;
        for eval in &collisions {
            let record = operation_record_checkpoint_collision(
                eval.address.space,
                &checkpoint,
                format!(
                    "address overlap at {:?} sessions {:?} revisions {:?}",
                    eval.address.point, eval.sessions, eval.revisions
                ),
            );
            self.persist_operation_errors(eval.address.space, record)?;
        }
        self.mark_session_wal_collision_evaluated_through(session.id(), checkpoint.last_revision);

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
        self.derivation
            .wait_for_session(session.id(), checkpoint.last_revision);
        if medium == DurabilityMedium::FastSegment {
            self.session_wal_store
                .reset_fast_after_commit(session.id())
                .map_err(EngineError::from)?;
        }
        self.persist_meta()?;
        Ok(crate::engine::collision::IntentCommitOutcome {
            checkpoint,
            collisions,
        })
    }

    /// Session fast-path counters (Phase 7).
    pub fn session_write_stats(&self) -> crate::engine::timed_fast_path::SessionWriteStatsSnapshot {
        self.session_wal_store.write_stats()
    }

    fn detect_checkpoint_collisions(
        &self,
        session: crate::infinitedb_core::hlc::SessionId,
        checkpoint: &IntentCheckpoint,
        buffered: &[crate::engine::session::BufferedSessionWrite],
    ) -> Result<Vec<CollisionEvaluation>, EngineError> {
        use std::collections::HashSet;
        let ceiling = checkpoint.last_revision;
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for item in buffered {
            let address = match &item.entry {
                WalEntry::Write { address, .. } | WalEntry::Tombstone { address, .. } => {
                    address.clone()
                }
                _ => continue,
            };
            if !seen.insert((address.space, address.point.clone())) {
                continue;
            }
            let rows = self.query_bbox_on_branch(
                BranchId::MAIN,
                address.space,
                address.point.clone(),
                address.point.clone(),
                Some(ceiling),
            )?;
            let mut sessions = vec![session];
            let mut revisions = Vec::new();
            for row in rows {
                if row.revision.session() != session.0 {
                    let sid = SessionId(row.revision.session());
                    if !sessions.contains(&sid) {
                        sessions.push(sid);
                    }
                    revisions.push(row.revision);
                }
            }
            if !revisions.is_empty() {
                out.push(CollisionEvaluation::new(address, sessions, revisions));
            }
        }
        Ok(out)
    }

    /// Mark retirement gates for a session WAL through an explicit revision.
    pub fn mark_session_wal_sealed(&self, session: SessionId, through: RevisionId) {
        self.session_wal_store
            .mark_sealed_through(session, through);
        if self.replication_gate == ReplicationGatePolicy::NotApplicable {
            self.session_wal_store
                .mark_replication_confirmed_through(session, through);
        }
    }

    pub fn mark_session_wal_replication_confirmed(
        &self,
        session: SessionId,
        through: RevisionId,
    ) {
        self.session_wal_store
            .mark_replication_confirmed_through(session, through);
    }

    pub fn mark_session_wal_collision_evaluated_through(
        &self,
        session: SessionId,
        through: RevisionId,
    ) {
        self.session_wal_store
            .mark_collision_evaluated_through(session, through);
    }

    /// Delete a session WAL when all retirement gates are satisfied.
    pub fn retire_session_wal(&self, session: SessionId) -> Result<bool, EngineError> {
        self.session_wal_store
            .try_retire_wal(session)
            .map_err(EngineError::from)
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

    /// I/O queue depth and write-path counters across all Hilbert-shard threads.
    pub fn io_stats(&self) -> IoStats {
        let mut stats = self.coordinator.io_stats();
        let ws = self.session_wal_store.write_stats();
        stats.fast_path_seal_success = ws.fast_path_seal_success;
        stats.fast_path_seal_timeout = ws.fast_path_seal_timeout;
        stats.fast_path_wal_fallback = ws.fast_path_wal_fallback;
        stats
    }

    /// Number of Hilbert I/O shards across registered spaces.
    pub fn space_shard_count(&self) -> usize {
        self.coordinator.shard_count()
    }

    pub(crate) fn query_ctx(&self) -> QueryCtx<'_> {
        QueryCtx {
            hilbert_tails: &self.hilbert_tails,
        }
    }

    /// Bulk insert on `main`; returns `(first_revision, last_revision)`.
    pub fn insert_many(
        &self,
        space: SpaceId,
        rows: Vec<(DimensionVector, Vec<u8>)>,
    ) -> Result<(RevisionId, RevisionId), EngineError> {
        self.insert_many_on_branch(BranchId::MAIN, space, rows)
    }

    /// Bulk insert on a branch.
    pub fn insert_many_on_branch(
        &self,
        branch: BranchId,
        space: SpaceId,
        rows: Vec<(DimensionVector, Vec<u8>)>,
    ) -> Result<(RevisionId, RevisionId), EngineError> {
        if rows.is_empty() {
            return Err(EngineError::InvalidSpaceConfig {
                message: "insert_many requires at least one row".into(),
            });
        }
        const CHUNK: usize = 4096;
        let count = rows.len() as u64;
        let range = self.default_write_session.stamp_n(count)?;
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
    pub fn compact(&self, space: SpaceId) -> Result<(), EngineError> {
        self.compact_with(space, None)
    }

    /// Manually compact `space`, optionally overriding the configured compaction policy.
    pub fn compact_with(
        &self,
        space: SpaceId,
        policy: Option<CompactionPolicy>,
    ) -> Result<(), EngineError> {
        let force = policy.is_some();
        if let Some(p) = policy {
            self.compaction_overrides.lock().insert(space, p);
        }
        let result = (|| {
            self.sync()?;
            if force {
                self.coordinator
                    .force_compact_space(space)
                    .map_err(EngineError::from)
            } else {
                self.coordinator
                    .compact_space(space)
                    .map_err(EngineError::from)
            }
        })();
        self.compaction_overrides.lock().remove(&space);
        result
    }

    /// Enqueue a write job on `main` or a branch overlay.
    pub fn enqueue(&self, job: WriteJob) -> Result<(), EngineError> {
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
                return Err(EngineError::from(e));
            }
            self.session_watermarks.retire(rev);
            return Ok(());
        }
        let result = self
            .coordinator
            .enqueue_write(job)
            .map_err(EngineError::from);
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
        self.default_write_session
            .stamp()
            .expect("global session stamp")
    }

    /// Apply many records on a branch through one allocation and batch enqueue.
    pub(crate) fn apply_records_on_branch(
        &self,
        branch: BranchId,
        records: Vec<Record>,
    ) -> Result<(), EngineError> {
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
    ) -> Result<Vec<HyperedgeId>, EngineError> {
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
    ) -> Result<Vec<HyperedgeId>, EngineError> {
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
    ) -> Result<Option<Hyperedge>, EngineError> {
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
    ) -> Result<Vec<Record>, EngineError> {
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
                    for row in prepare_flow_vector_derivation(&edge, &self.spaces.read(), q) {
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
    ) -> Result<Vec<Record>, EngineError> {
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

    fn persist_meta(&self) -> Result<(), EngineError> {
        let spaces_bytes = encode_to_vec(&*self.spaces.read(), standard())
            .map_err(|e| EngineError::Other {
                message: e.to_string(),
            })?;
        self.store
            .write_meta("spaces.bin", &spaces_bytes)
            .map_err(EngineError::from)?;

        let branches_bytes = encode_to_vec(&*self.branches.read(), standard())
            .map_err(|e| EngineError::Other {
                message: e.to_string(),
            })?;
        self.store
            .write_meta("branches.bin", &branches_bytes)
            .map_err(EngineError::from)?;

        let snapshots = snapshots_map_for_persist(&self.snapshots);
        let snapshots_bytes = encode_to_vec(&snapshots, standard())
            .map_err(|e| EngineError::Other {
                message: e.to_string(),
            })?;
        self.store
            .write_meta("snapshots.bin", &snapshots_bytes)
            .map_err(EngineError::from)?;

        let counters = PersistedCounters::new(
            self.session_watermarks.allocated().legacy_sequence(),
            self.next_block_id.load(Ordering::Relaxed),
            self.next_snapshot_id.load(Ordering::Relaxed),
            self.next_branch_id.load(Ordering::Relaxed),
            self.session_watermarks.next_session_counter(),
        );
        let counters_bytes = encode_to_vec(&counters, standard())
            .map_err(|e| EngineError::Other {
                message: e.to_string(),
            })?;
        self.store
            .write_meta("counters.bin", &counters_bytes)
            .map_err(EngineError::from)?;

        let branch_bases = self.branch_overlays.export_bases();
        let branch_bases_bytes = encode_to_vec(&branch_bases, standard())
            .map_err(|e| EngineError::Other {
                message: e.to_string(),
            })?;
        self.store
            .write_meta("branch_bases.bin", &branch_bases_bytes)
            .map_err(EngineError::from)?;

        let arbiter_streams_bytes = encode_to_vec(&*self.arbiter_streams.read(), standard())
            .map_err(|e| EngineError::Other {
                message: e.to_string(),
            })?;
        self.store
            .write_meta("arbiter_streams.bin", &arbiter_streams_bytes)
            .map_err(EngineError::from)?;

        let frames_bytes = encode_to_vec(&*self.frames.read(), standard())
            .map_err(|e| EngineError::Other {
                message: e.to_string(),
            })?;
        self.store
            .write_meta("frames.bin", &frames_bytes)
            .map_err(EngineError::from)?;

        persist_session_wal_meta(&self.root.join("meta"), &self.session_wal_store.meta())
            .map_err(EngineError::from)?;
        Ok(())
    }
}

impl Drop for InfiniteDb {
    fn drop(&mut self) {
        self.derivation.shutdown();
        let _ = self.persist_meta();
        let _ = self.coordinator.shutdown_all();
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
    pub hilbert_tails: &'a HilbertLiveTails,
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
