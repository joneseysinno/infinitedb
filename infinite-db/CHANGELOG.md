# Changelog

## Unreleased

### Added

- **Peer track Phase 7 — timed fast path:** optional `TimedFastPathPolicy` on `OpenOptions` (default off); when enabled, `sync_session_wal` attempts durable seal to `sessions/{id}.fast` within `direct_seal_deadline` (from `IoThreadConfig::direct_write_timeout`, default 2ms) and falls back to session WAL append on timeout; `DurableIntent` carries `DurabilityMedium` (`SessionWal` / `FastSegment`); no `live_tail` publish before `commit_session_intent`; durable-but-uncommitted `.fast` bytes quarantined on recovery as `InterruptedSessionIntent`; `session_write_stats()` / `io_stats()` fast-path counters.
- Integration tests: `tests/fast_path_phase7.rs`.

- **Peer track Phase 6 — derivation bus under plural streams:** per-session `DerivationSessionWatermark` vectors with `register_for_session` / `retire_for_session`; `endpoint_index_watermark_vector()` and `flow_vector_index_watermark_vector()`; per-session delta merge via `record_in_derivation_delta`; session-scoped backpressure in `check_backpressure(submitting_session, head)`; hash-partitioned derivation workers (`splitmix64` on `HyperedgeId`, edge-lock map removed); vector-aware `recover_derivation_on_open` replays per-session gaps only; scalar `endpoint_index_watermark()` remains the meet across sessions for backward compatibility.
- Integration tests: `tests/derivation_phase6.rs`.

- **Peer track Phase 5 — frame integration (`AssertionScope::Session`):** session-scoped stream admission from `record.revision.session()`; `merge_admission_specs` with `Union`/`Session`/`Spaces`/`Branches`; optional `FrameQuery.version_vector` per-session pin (scalar `as_of` fallback); HLC authorship-order supersession among admitted sessions; `insert_hyperedge_with_session` with derivation-bus replay on intent commit; `fetch_hyperedge_by_id` overlays authoritative `valid_from` from record revision; legacy-encoded computation pins match HLC observed revisions for freshness (Phase 5 compat).
- Integration tests: `tests/frame_session_phase5.rs`.

- **Peer track Phase 4 — intent checkpoints:** `IntentCheckpoint` session-WAL frames with revision range + operation kind; durable-but-uncommitted tail quarantined on recovery (D-P5) as `InterruptedSessionIntent` error records; `sync_session_wal` / `commit_session_intent` with `DurableIntent` typestate token; checkpoint-bound collision detection; derivation flush at commit boundary.
- Integration tests: `tests/intent_checkpoint.rs`.

- **Peer track Phase 3 — per-session WAL:** `sessions/{id}.wal` with `committed_len` durability boundary; `insert_with_session` / `sync_session`; HLC-ordered recovery on open with per-stream quarantine; three-gate WAL retirement meta (`sealed`, `replication_confirmed`, `collision_evaluated`); session WAL frames preserve full HLC stamps; reopen hydrates per-session allocation heads from `meta/session_wals.bin`.
- Integration tests: `tests/session_wal.rs`.

- **Peer track Phase 2 — session stamping and version vectors:** `open_session()`, `WriteSession::stamp` / `stamp_n`, per-session `HlcClock`, `SessionWatermarks` with session-keyed retire routing; `VersionVector` read pins and scalar stable meet; implicit session-0 default preserves legacy single-writer API.
- Integration tests: `tests/session_stamping.rs`.

- **Peer track Phase 1 — HLC `RevisionId` widening:** 128-bit `HlcStamp` composite (D-P1 layout); `RevisionId::legacy` / `from_stamp` / `legacy_sequence` / `next_global`; versioned `revision_codec` and `block_codec` for v5 wire; v4 directories remain read-write with legacy u64 wire embedding (D-P4); `FORMAT_VERSION_V5` accepted on open.
- Integration tests: `tests/hlc_revision.rs`.

- **Peer track Phase 0 — derivation watermark repair:** outstanding-set / contiguous-prefix derivation watermarks (register on submit, retire on apply); `FailedDerivation` + `derivation_stats.derivation_failures`; bus shutdown drains queued events; delta-merge boundary fix (`revision > watermark`); assertion tombstone respected in `fetch_hyperedge_by_id`.
- Peer-track decision records D-P1–D-P4 in `SEMANTICS.md`.

- **Milestone 7 — flow-vector lane and staleness closures:** `FlowVector`, `FlowVectorSubscriber`, `FLOW_VECTOR_INDEX_SPACE`; `ComputationProvenance` / Hyperedge V4 codec; `query_flow_vectors_in_region`, `query_flow_vector_for_edge`, `check_hyperedge_freshness`, `query_stale_downstream`; delta-merge reads for flow-vector index lag.
- Integration tests: `tests/hypergraph_m7.rs`.

- **Milestone 6 — frame-resolution query API:** `FrameDefinition`, `register_frame`, `query_hyperedges_in_frame`, `query_hyperedges_for_endpoint_in_frame`, `traverse_in_frame`; overlay policies (`Suppress`, `Annotate`, `SelectContested`); `QueryPlanStats` performance instrumentation.
- Integration tests: `tests/hypergraph_m6.rs`.

- **Milestone 5 — provenance, judgments, error records:** `AuthoringFrameProvenance` / Hyperedge V3 codec; `register_arbiter_stream`, `assert_judgment`, judgment index queries; per-space companion error spaces; `persist_operation_errors`, `query_operation_errors`, `resolve_operation_error`; `diagnose_assertion` helpers.
- Integration tests: `tests/hypergraph_m5.rs`.

- **Milestone 4 — derivation bus:** async endpoint-index derivation; `DerivationBus`, per-subscriber watermarks, delta-merge reads (`QueryOptions::index_only`), `sync_derivation`, `derivation_stats`, `DerivationBackpressurePolicy`.
- `StorageError`, `EngineError`, `project_api_error`; `register_space` / `create_branch` return `EngineError`.
- Bulk hyperedge import: `begin_hyperedge_import`, `commit_hyperedge_import`, `ImportErrorLog`, `ImportBudget`.
- Integration tests: `tests/hypergraph_m4.rs`.

- **Milestone 3 — directional traversal:** `TraversalDirection`, `TraversalMode`, `TraversalResult` with wave-front levels; `InfiniteDb::traverse_hypergraph`, `check_hypergraph_acyclic`; reachability BFS and opt-in B-connectivity mode.
- Integration tests: `tests/hypergraph_m3.rs`.

- **Milestone 2 — polarity-dimension endpoint index:** `EndpointIndexLayout`, V2 index coordinate encoding, dual-layout reads, `compact_endpoint_index` lazy rewrite.
- `InfiniteDb::count_incident_edges_for_endpoint`, `count_incident_edges_for_endpoint_directed`.
- `InfiniteDb::upgrade_endpoint_index_layout`, `compact_endpoint_index`.
- Integration tests: `tests/hypergraph_m2.rs`.

- **Milestone 1 — directed hypergraph on CRCW:** `EndpointPolarity`, `Directionality`, versioned `hyperedge_codec`, catalog `DirectionalityPolicy`, `engine/hypergraph` write path.
- `InfiniteDb::insert_hyperedge`, `delete_hyperedge`, `insert_hyperedge_typed`, `query_hyperedges`, `query_hyperedges_for_endpoint`, `query_hyperedges_for_endpoint_directed`.
- `DirectionFilter` for directional incidence queries (index-resident under M2 layout; V1-layout fallback post-filter until lazy rewrite).
- Integration tests: `tests/hypergraph_m1.rs`.
- `MILESTONES.md` roadmap (M1–M7) linked to design document.
- `InfiniteDb::compact(space)` manual compaction trigger.
- `InfiniteDb::compact_with(space, policy)` and per-space `CompactionPolicy` on `SpaceConfig` (keep-all default).
- `InfiniteDb::allocate_revisions(count)` for advanced `enqueue_batch` callers.
- `InfiniteDb::failed_revisions()` for observing abandoned writes after I/O failure.
- Hilbert bounding-box key decomposition (`range_decompose`) for tighter `query_bbox` pruning.
- Branch overlay durability via append-only `overlay.log` with replay on open.
- Post-compaction block-file GC (`safe_to_delete`) with branch-base snapshot pinning.
- `branch_bases.bin` persists fork-base snapshots for branch queries across reopen.

### Fixed

- Hot-segment replay now honors the `committed_len` durability boundary; failed group commits roll back uncommitted frames instead of resurrecting on reopen.
- Hilbert shard pruning in live-tail queries uses each space's configured `bits_per_dim` instead of a fixed 8-bit fallback.
- Address identity grouping uses cached Hilbert keys (with space-aware recomputation when unset) instead of cloning coordinate vectors on every query.
- `InfiniteDb::failed_revisions()` is non-destructive; use `take_failed_revisions()` to explicitly drain the failure log.
- `RevisionWatermark` allocation+registration race: counter and outstanding set now update under one lock, so `stable_revision()` never observes an allocated-but-unregistered revision (fixes non-repeatable `ReadTxn` pins).

### Removed

- Internal `legacy_v1` engine and `legacy-v1` feature flag.
- `centroid_keying`, `coords` locator helpers, `InfiniteSchema`, `block::Relation`.

### Changed

- Main-branch hyperedge writes commit assertions synchronously; endpoint index rows derive on the background bus (branch writes remain synchronous).
- `sync()` flushes the derivation bus before `sync_all()`.
- `ConflictQueue::push` / `remove` propagate persistence failures.
- Server `WriteHyperedge` / `DeleteHyperedge` route through hypergraph validation and endpoint index maintenance.
- `InfiniteDb::revision()` and `stable_revision()` return `RevisionId` (was `u64`).
- Phase 3 type hygiene: engine APIs use `SpaceId`/`BranchId`/`RevisionId` newtypes (disk boundaries still use raw `u64` where needed); `HilbertKey`/`CachedHilbertKey` replace raw `u128` hilbert fields; `Checksum` wraps block digests; `ShardRef` replaces `(shard_id, shard_bits)` tuples; `AddressKey`/`RecordIdentityKey` drive visibility and seal deduplication; `RevisionWatermark` tracks `BTreeSet<RevisionId>` with `predecessor()`-based stable ceiling.
- Auto-compaction defaults to `retain_history: true`; use `CompactionPolicy::LatestOnly` to opt into history-dropping compaction.
- `RevisionWatermark` owns allocation; `enqueue_batch` requires watermark-allocated revisions.
- Branch writes register and retire revisions through the same watermark lifecycle as main.
- Branch overlay batch append with cached WAL writers (one fsync per batch).
- `counters.bin` migrates to named `PersistedCounters` struct on next `persist_meta`.
- Merge/import/converge apply paths batch through `allocate_n` + `enqueue_batch`.
- Branch queries read sealed data from fork-base snapshots (not current main index).
- Default queries return exactly one record per address (latest revision wins at the query ceiling). Use `Query::include_tombstones()` for full revision history.
- Per-shard atomic read views (`ShardView`) pair sealed blocks and live tail so readers never observe seal-window duplicates.
- `ReadTxn` pins at `stable_revision()` (not allocation `revision()`); added `InfiniteDb::stable_revision()` for repeatable reads.
- Group commit on shard I/O loops: drain channel, batch fsync, batch tail publish, watermark retire (staging WAL removed).
- `WriteJob` deduplicates payload (WAL entry only); `IoCommand::WriteBatch` for shard-local batches.
- Live tail uses immutable chunk list; `LiveTailView::load()` returns shared `Arc` chunks.
- Seal from in-memory tail (no hot-segment re-read); byte-based `hot_segment_seal_bytes` seal threshold.
- `sync_all` / `flush_space` fan-out in parallel across shards (latency = max, not sum).
- Lazy Hilbert shard provisioning: `register_space` no longer spawns all shards eagerly.
- Per-space snapshot head is a mutable index; `SnapshotId` no longer allocated per seal.
- `IoThreadConfig::direct_write_timeout` deprecated (ignored).

## 0.3.0

### Added

- CRCW `InfiniteDb` rewrite (Phases A–D): concurrent reads, fire-and-forget writes, `Send`/`Sync` embedded database.
- On-disk formats v2, v3, and v4 with `FORMAT_VERSION_*` constants and `OpenOptions::format_version` (new databases default to v4).
- `ReadTxn` for point-in-time concurrent reads pinned at a revision ceiling.
- Per-space I/O threads (format v3) and Hilbert shard routing (format v4).
- Branch overlays: `create_branch`, `merge_branch`, `insert_on_branch`, `delete_on_branch`, branch-aware queries.
- Low-level engine re-exports: `SpaceCoordinator`, `HilbertCoordinator`, `WriteJob`, `IoThreadConfig`, `WriteRoute`, `WalDurability`.
- Sync Phase D: `replicate::*` (`branch_sync_state`, `converge_main_records`, `converge_with_branch_merge`, `import_branch_overlay`, `snapshot_merkle`), `ConflictQueue`, TCP `Server` / `client_roundtrip`.
- `legacy-v1` feature (off by default): compiles the internal v1 WAL engine for reference only; not exposed in the public API.
- Integration tests: `concurrent_db`, `concurrent_db_phase_b`, `concurrent_db_phase_c`, `durability`, `server_phase_d`.
- `README.md`, `examples/quickstart`, and expanded crate documentation.

### Changed

- Default entry point is the CRCW `InfiniteDb`; the old single-threaded WAL engine moved to a private `legacy_v1` module.
- `OpenOptions` now targets formats v2–v4 (I/O threads, block cache, format version).

### Removed / Breaking

- Public bulk write APIs from 0.2.0 (`insert_records_bulk`, `delete_records_bulk`, hyperedge/signal bulk import and delete).
- Hyperedge and signal write/query methods on `InfiniteDb` (types remain in `infinitedb_core` for upstream crates).
- Monolithic `src/db.rs` API surface replaced by the CRCW engine under `src/concurrent/` and `src/engine/`.

## 0.2.0

### Added

- Shared bulk write infrastructure (`BulkWriteOptions`, `BulkWriteResult`, session exclusivity).
- `BulkRecordImport` / `insert_records_bulk` / `delete_records_bulk` for raw records.
- `BulkSignalImport` / `insert_signals_bulk` / `delete_signals_bulk` for signal samples.
- Hyperedge bulk delete: `BulkHyperedgeImport::push_delete`, `delete_hyperedges_bulk`.
- Deferred sync outbox enqueue during bulk sessions (sync feature).

### Changed

- Hyperedge bulk import refactored onto shared `src/bulk/` module; `begin_hyperedge_import` returns `io::Result` (second session on same DB returns `AlreadyExists`).
- `BulkImportResult` is an alias for `BulkWriteResult` (`wal_frames` field).

## 0.1.6

- Buffered WAL bulk hyperedge import (`insert_hyperedges_bulk`, `BulkHyperedgeImport`).
- `OpenOptions`, WAL `rewrite` single-fsync fix, optional block cache tuning.
