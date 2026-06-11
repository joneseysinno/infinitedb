# InfiniteDB — Milestone 4 Implementation Plan
## Derivation Bus + Error Algebra Foundation

**Source:** [InfiniteDB-Spatial-Hyper-Truth-Graph-Design.txt](InfiniteDB-Spatial-Hyper-Truth-Graph-Design.txt) Section 5.3 and Section 6, Milestone 4  
**Status:** Implemented  
**Roadmap:** [MILESTONES.md](MILESTONES.md)

---

## Objective

Move endpoint-index derivation off the synchronous write path onto a background derivation bus with per-subscriber watermarks, delta-merge reads, backpressure, and applicative bulk import — plus layered error types and effect-boundary fixes from the error-algebra addendum.

---

## Delivered

### Error algebra (Phase 0)
- `StorageError`, `EngineError` with recoverability helpers
- `project_api_error` — single API projection point
- `register_space`, `create_branch`, `upgrade_endpoint_index_layout` return `EngineError`
- `ConflictQueue::push` / `remove` propagate persist failures

### Hypergraph split (Phase 1)
- `prepare_assertion_write`, `prepare_index_derivation`, `prepare_index_tombstones`
- `prepare_writes` / `prepare_deletes` remain compatibility wrappers

### Derivation bus (Phase 2)
- `engine/derivation/` — `DerivationBus`, `AssertionEvent`, subscribers, watermarks, backpressure
- `EndpointIndexSubscriber` (layout read from live registry), `EdgeLocatorSubscriber` (watermark-only)
- Main-branch writes: assertion sync + bus submit; branch writes keep synchronous full index

### Delta-merge reads (Phase 3)
- `QueryOptions::index_only` for bounded staleness
- `query_hyperedges_for_endpoint_directed_with_options`, count/traversal variants
- Merge assertion delta when `stable_revision > endpoint_index_watermark`

### Bulk import (Phase 5)
- `ImportErrorLog` monoid, `ImportBudget`, `HyperedgeImportSession`
- `begin_hyperedge_import`, `commit_hyperedge_import` on same bus pipeline

### APIs
- `sync_derivation()`, `derivation_stats()`
- `DerivationBackpressurePolicy` on `OpenOptions`

### Tests
- `tests/hypergraph_m4.rs`

---

## Explicitly deferred

- Operation-level error records (error space) — M5+
- Intent Checkpoint / HLC peer track — batch trigger via `sync()` + `sync_derivation()`
- Branch-overlay derivation
- Flow-vector / staleness closures — M7

---

## Invariants

1. Derived index rows carry the assertion `source_revision`.
2. `sync()` calls `derivation.flush()` before `sync_all()`.
3. Delta-merge default (`index_only = false`) preserves exact incidence under pipeline lag.
4. Per-edge ordering on the bus prevents delete-before-insert races for the same `HyperedgeId`.
