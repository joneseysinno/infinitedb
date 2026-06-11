# InfiniteDB — Milestone 3 Implementation Plan
## Directional Traversal

**Source:** [InfiniteDB-Spatial-Hyper-Truth-Graph-Design.txt](InfiniteDB-Spatial-Hyper-Truth-Graph-Design.txt) Section 5.2 and Section 6, Milestone 3  
**Status:** Implemented  
**Roadmap:** [MILESTONES.md](MILESTONES.md)

---

## Objective

Add directional hypergraph traversal with arrival-aware expansion, wave-front level assignments, per-kind acyclicity checks, and B-connectivity as an opt-in mode — built on the M2 polarity-dimension endpoint index.

---

## Delivered

### Core types (`infinitedb_core/traversal.rs`)
- `TraversalDirection` — Forward / Backward / Both
- `TraversalMode` — Reachability (default) / BConnectivity
- `TraversalArrival` — Start / Expanded / CoCause
- Extended `TraversalSpec` with `edge_space`, `direction`, `mode`
- `TraversalResult`, `TraversalNode` with wave-front levels
- Pure helpers: `expand_edge_reachability`, `run_b_connectivity`, `hypergraph_acyclic_for_kinds`

### Engine (`engine/traversal.rs`)
- `run_traversal` — index records loaded once per call
- Reachability BFS with arrival-aware expansion
- B-connectivity fixpoint with per-edge tail satisfaction
- V1/V2 dual-layout incidence (mirrors M2 query path)

### Query APIs (`concurrent/concurrent_db.rs`)
- `InfiniteDb::traverse_hypergraph`
- `InfiniteDb::check_hypergraph_acyclic`

### Tests
- `tests/hypergraph_m3.rs` — forward/backward/both, co-cause, B-connectivity, acyclicity, V1 fallback, depth/kind filters
- Unit tests in `infinitedb_core/traversal.rs`

---

## B-connectivity cost note

B-connectivity is **not the default** (`TraversalMode::Reachability`). It uses a fixpoint over per-edge tail counters and may collect a larger candidate edge set before activation. Prefer reachability for plain dependency closure; use B-connectivity when conjunctive semantics (all tails required) match the domain model.

---

## Explicitly deferred (M4+)

- Derivation bus (M4)
- Frame resolution / judgments (M5–M6)
- Flow-vector lane / staleness closures (M7)
- Server wire protocol for traversal
- Hilbert bbox pruning for traversal frontiers

---

## Invariants

1. `TraversalMode::Reachability` is the default; B-connectivity is opt-in.
2. Endpoint identity = space + coordinates (polarity/role ignored for visited set).
3. Undirected edges participate only in `TraversalDirection::Both`.
4. Index records loaded once per traversal call.
5. V1-layout rows post-filter until lazy rewrite (same as M2 incidence).
