# InfiniteDB — Milestone 6 Implementation Plan
## Frame-Resolution Query API

**Source:** [InfiniteDB-Spatial-Hyper-Truth-Graph-Design.txt](InfiniteDB-Spatial-Hyper-Truth-Graph-Design.txt) §4.3–4.4, §5.5, §6 M6  
**Status:** Implemented  
**Roadmap:** [MILESTONES.md](MILESTONES.md)

---

## Objective

Named durable frames and index-resident frame-resolution queries: assertion scope (spaces/branches), judgment overlay policies, revision ceiling — plus frame-aware traversal.

---

## Delivered

### Frame model (Phase 0)
- `FrameDefinition`, `AssertionScope`, `JudgmentOverlayLayer`, `OverlayPolicy`, `VerdictFilter`
- `frames.bin` persistence; `register_frame`, `get_frame`, `list_frames`
- `EngineError::FrameExists`, `FrameNotFound`, `InvalidFrame`; `project_api_error` updated

### Supersession (Phase 1)
- `engine/frame.rs`: per-source `resolve_visibility_per_source`, `TestimonySource`
- `flatten_assertion_scope` for space/branch admission (interim until HLC peer track)

### Judgment overlay (Phase 2)
- `apply_judgment_overlay`: `Suppress`, `Annotate`, `SelectContested`
- `FrameResolvedHyperedge`, `AttachedJudgment`
- `consulted_from_frame` in `staleness.rs`

### Frame queries (Phase 3)
- `FrameQuery`, `FrameQueryOptions`
- `query_hyperedges_in_frame`, `query_hyperedges_for_endpoint_in_frame`

### Traversal (Phase 4)
- `FrameTraversalSpec`, `traverse_in_frame`, `FrameTraversalResult`
- Per-edge overlay filter on expansion via allowed-edge predicate

### Performance contract (Phase 5)
- `QueryPlanStats` / `query_plan_stats()` instrumentation on `query_bbox`
- Bounded scan-count integration test

### Tests
- `tests/hypergraph_m6.rs` (9 scenarios)

---

## Interim stream admission

Assertion scope admits testimony by **registered hyperedge spaces**, **branch overlays**, and/or **asserting sessions** (`AssertionScope::Session`). Session admission reads the session component from each record's revision stamp (authoritative at the block boundary, not the hyperedge payload's encoded `valid_from`). `Union` composes scopes; `merge_admission_specs` merges session filters per `(space, branch)` bucket.

Frame queries accept scalar `as_of` or optional `version_vector` (per-session stable pin). Vector pins fetch through the max pinned component, then apply per-session ceilings in MVCC.

---

## Explicitly deferred

- Frame composition (frame over frame output)
- Server wire protocol for frames
- Full branch-isolated traversal without MAIN endpoint index

---

## Invariants

1. Frame resolution = stream admission → spatial bbox → per-source MVCC → judgment overlay.
2. Performance contract: O(admitted sources + overlay arbiters) `query_bbox` calls, not O(history).
3. Overlay policies return structured `FrameResolvedHyperedge` values (conflict-as-value).
4. Low `ArbiterId` values should use judgment region scans with care; prefer ids ≥ 10 in production overlays.
