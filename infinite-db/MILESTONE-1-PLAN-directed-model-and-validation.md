# InfiniteDB — Milestone 1 Implementation Plan
## Directed Model, Validation, Legacy Cleanup, CRCW Hypergraph

**Source:** [InfiniteDB-Spatial-Hyper-Truth-Graph-Design.docx](InfiniteDB-Spatial-Hyper-Truth-Graph-Design.docx) Section 6, Milestone 1  
**Status:** Implemented  
**Roadmap:** [MILESTONES.md](MILESTONES.md)

---

## Objective

Make hyperedge directionality a **structural property** enforced at write time on the CRCW `InfiniteDb` path. M1 keeps the endpoint index **symmetric**; directional queries use **post-filtering** (transitional until M2).

---

## Delivered

### Core model (`infinitedb_core/hyperedge.rs`)
- `EndpointPolarity`, `Directionality`, extended `EndpointRef` / `Hyperedge`
- Validation matrix including undirected-purity and directed tail/head invariants
- `Hyperedge::storage_point` for id-keyed assertion storage

### Versioned codec (`infinitedb_core/hyperedge_codec.rs`)
- V1 decode-only (untagged legacy payloads → undirected/neutral)
- V2 tagged writes (`HYPEREDGE_PAYLOAD_V2_TAG = 0xE2`)
- Fixture helpers: `HyperedgeV1Fixture`, `encode_hyperedge_v1_fixture`

### Catalog (`infinitedb_core/kind_catalog.rs`)
- `DirectionalityPolicy`, `validate_edge_directionality`

### CRCW hypergraph engine (`engine/hypergraph.rs`)
- `prepare_writes` / `prepare_deletes` (isolated for M4 derivation bus)
- Endpoint index maintenance in `ENDPOINT_INDEX_SPACE`

### `InfiniteDb` API (`concurrent/concurrent_db.rs`)
- `insert_hyperedge`, `delete_hyperedge`, `insert_hyperedge_typed`
- `query_hyperedges`, `query_hyperedges_for_endpoint`, `query_hyperedges_for_endpoint_directed`
- `DirectionFilter` in `infinitedb_core/query.rs`

### Legacy cleanup
- Removed `legacy_v1/`, `legacy-v1` feature, `coords`, `schema`, `centroid_keying`, `block::Relation`
- Server hyperedge requests route through validation + index path

### Tests
- `tests/hypergraph_m1.rs` — insert/delete, index incidence, directional hub, catalog, V1 mixed-era

---

## Explicitly deferred (M2+)

- Polarity dimension in endpoint index (M2)
- Directional traversal (M3)
- Derivation bus (M4)
- Provenance / frames (M5–M6)
- Flow vectors (M7)

---

## Invariants

1. Sealed blocks never rewritten in place.
2. V1 payloads decode as undirected / all-neutral.
3. Polarity in edge record in M1 (not index).
4. Roles semantic; polarity structural.
5. Core owns structure, not domain meaning.
6. Index rows derivable from assertions (M4-ready `prepare_writes`).
