# InfiniteDB — Milestone 2 Implementation Plan
## Polarity-Dimension Endpoint Index

**Source:** [InfiniteDB-Spatial-Hyper-Truth-Graph-Design.docx](InfiniteDB-Spatial-Hyper-Truth-Graph-Design.docx) Section 5.1 and Section 6, Milestone 2  
**Status:** Implemented  
**Roadmap:** [MILESTONES.md](MILESTONES.md)

---

## Objective

Move structural **polarity** into the reserved endpoint reverse-index coordinate layout so directional incidence and degree queries filter in the index — not by decoding every incident hyperedge payload. Preserve sealed-block immutability via dual-layout reads and compaction-driven lazy rewrite.

---

## Delivered

### Index layout version (`infinitedb_core/space.rs`)
- `EndpointIndexLayout`: `V1Symmetric` (default) / `V2PolarityDim`
- `SpaceConfig::endpoint_index_layout` (serde-defaulted)
- `SpaceRegistry::update` for in-place registry changes
- `InfiniteDb::upgrade_endpoint_index_layout()` — lazy migration trigger

### V2 coordinate encoding (`infinitedb_core/endpoint_index.rs`)
- Polarity dimension between endpoint coords and edge-id dimensions
- `endpoint_index_point_v2`, `endpoint_index_query_bounds`, `polarity_coord`
- Payload layout tag `INDEX_PAYLOAD_V2_TAG` for dual-layout detection
- `collect_incident_edge_ids` / `count_incident_edges` — index-resident direction

### Write/delete path (`engine/hypergraph.rs`)
- `prepare_writes` / `prepare_deletes` accept `EndpointIndexLayout`
- New databases register `ENDPOINT_INDEX_SPACE` with `V2PolarityDim`

### Query APIs (`concurrent/concurrent_db.rs`)
- `query_hyperedges_for_endpoint_directed` — V2 polarity in index coords; V1 fallback post-filter
- `count_incident_edges_for_endpoint`, `count_incident_edges_for_endpoint_directed`
- `compact_endpoint_index(edge_spaces)` — explicit V1 → V2 rewrite + compact

### Compaction hook (`engine/compactor.rs`, `engine/endpoint_index_migrate.rs`)
- `expand_endpoint_index_records_for_compaction` (no-op without edge resolver in background path)
- Rewrite planner: `plan_v1_to_v2_index_rewrite`

### Tests
- `tests/hypergraph_m2.rs` — layout, hub incidence, dual-layout, lazy rewrite, degree counts
- `tests/hypergraph_m1.rs` — backward compatible

---

## Explicitly deferred (M3+)

- Directional traversal engine (M3)
- Derivation bus (M4)
- Hilbert bbox pruning for polarity-pinned scans (optimization; coordinate filter is index-resident today)

---

## Invariants

1. Sealed blocks never rewritten in place; migration via compaction output + explicit rewrite API.
2. V1 hyperedge codec unchanged.
3. Polarity in edge record and V2 index row agree on write.
4. `prepare_writes` remains pure for M4 derivation bus.
5. `DirectionFilter::Any` on V2 reproduces M1 symmetric incidence.
