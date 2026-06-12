# InfiniteDB — Milestone 5 Implementation Plan
## Provenance, Judgments, and Error Records

**Source:** [InfiniteDB-Spatial-Hyper-Truth-Graph-Design.txt](InfiniteDB-Spatial-Hyper-Truth-Graph-Design.txt) §4.4, §6 M5; [InfiniteDB-Error-Algebra-and-Effect-Boundaries.md](InfiniteDB-Error-Algebra-and-Effect-Boundaries.md) §5–§7  
**Status:** Implemented  
**Roadmap:** [MILESTONES.md](MILESTONES.md)

---

## Objective

Make epistemology operational: authoring-frame provenance on assertions, judgment record conventions with spatial co-location, per-space operation error records, and error-algebra wave 2 — without M6 frame-resolution queries or peer-track HLC/Intent Checkpoint.

---

## Delivered

### Error algebra wave 2 (Phase 0)
- `SpaceConfig.error_space` + auto-registration of `{name}_errors` companion spaces
- `OperationErrorRecord`, `ErrorKind`, versioned `error_record_codec`
- `ErrorKindCatalog` with unknown-kind policy
- `persist_operation_errors`, `query_operation_errors`, `resolve_operation_error`
- Import commit persists error records when errors non-empty or session aborted
- `EngineError` extensions: `ErrorSpaceMissing`, `InvalidJudgment`, `InvalidProvenance`, etc.
- `project_api_error` updated for new variants

### Authoring provenance (Phase 1)
- `FrameId`, `AuthoringFrameProvenance` on `Hyperedge`
- Hyperedge V3 codec tag (`0xE3`) when provenance present; V2 unchanged otherwise
- Provenance validation on insert (`as_of` ≤ commit revision)

### Judgments (Phase 2)
- `JudgmentRecord`, `SubjectPin`, `JudgmentVerdict`, `ArbiterStream`
- `JUDGMENT_INDEX_SPACE` with spatial co-location key layout
- `register_arbiter_stream`, `assert_judgment`, `fetch_judgment_by_id`
- `query_judgments_for_subject`, `query_judgments_in_region`
- Arbiter streams persisted in `arbiter_streams.bin`

### Staleness diagnosis (Phase 3)
- Pure `diagnose_assertion` / `validate_authoring_provenance` in `infinitedb_core::staleness`

### Tests
- `tests/hypergraph_m5.rs` (6 scenarios)

---

## Explicitly deferred

- Named durable frames; three-axis frame query — M6
- Index-resident frame resolution (§5.5) — M6
- HLC / per-session WAL / Intent Checkpoint — peer track
- Judgment overlay policies in queries — M6
- Flow-vector / staleness closures — M7
- Server wire protocol for judgments/errors — post-M5

---

## Invariants

1. Per-space error topology: companion `{name}_errors` space linked from `SpaceConfig`.
2. Error records travel in Ok channel as values; persistence is a separate effect at operation boundaries.
3. Judgments are ordinary assertions in arbiter streams; index rows written synchronously (not on derivation bus).
4. Tombstone-resolve preserves error audit trail (no hard delete).
