# Changelog

## Unreleased

### Added

- `InfiniteDb::insert_many` / `insert_many_on_branch` bulk write API with per-shard `WriteBatch` routing.
- `InfiniteDb::compact(space)` manual compaction trigger.
- Hilbert bounding-box key decomposition (`range_decompose`) for tighter `query_bbox` pruning.
- Branch overlay durability via append-only `overlay.log` with replay on open.

### Changed

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
