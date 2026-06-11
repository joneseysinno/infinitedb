# Milestone 7 — Flow-Vector Lane and Staleness Closures

**Status:** Done

## Summary

M7 adds a flow-vector derivation-bus subscriber with quantized-direction index queries, plus source-revision staleness closures (forward downstream + backward freshness) on top of M3 directional traversal and Hyperedge V4 `ComputationProvenance`.

## Deliverables

| Area | Implementation |
|------|----------------|
| Flow vectors | `infinitedb_core/flow_vector.rs`; `Hyperedge::tail_centroid`, `head_centroid`, `flow_vector` |
| Computation lineage | `ComputationProvenance` on Hyperedge V4 codec (`HYPEREDGE_PAYLOAD_V4_TAG`) |
| Index | `FLOW_VECTOR_INDEX_SPACE` (`SpaceId(u64::MAX - 3)`); `FlowVectorSubscriber` |
| Queries | `query_flow_vectors_in_region`, `query_flow_vector_for_edge`, delta-merge via `QueryOptions::index_only` |
| Staleness | `check_hyperedge_freshness`, `query_stale_downstream`, `query_staleness_by_source_revision` |
| Tests | `tests/hypergraph_m7.rs` (8 scenarios) |

## Design notes

- **Flow vector** = head centroid − tail centroid in the shared physical `SpaceId`; undirected, cross-space, or missing tail/head edges are skipped (no silent garbage vectors).
- **Quantization** default `bits_per_axis = 3` for Hilbert direction buckets.
- **Freshness** travels in the **Ok channel** (`FreshnessReport`, `StaleTarget`) per error-algebra rule 3; orthogonal to M5 `diagnose_assertion` frame diagnosis.
- **Frame-native anomaly queries** deferred — M6 frame queries can post-filter `query_flow_vectors_in_region` at the application layer.

## Deferred

- `StalenessMarkerSubscriber` (persistent marker derived space)
- Server wire protocol for flow/staleness
- HLC / per-session WAL / Intent Checkpoint (peer track)
