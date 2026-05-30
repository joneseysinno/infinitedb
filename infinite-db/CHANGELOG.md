# Changelog

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
