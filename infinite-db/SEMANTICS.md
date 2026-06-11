# InfiniteDB Semantic Reference

This document defines the canonical language for `infinite-db` and upstream crates.

## Core Terms

- **Space**: named dataset with fixed dimensionality (`SpaceId`, `SpaceConfig`).
- **Address**: primary coordinate key (`space + point`).
- **Node**: domain entity represented in one or more spaces.
- **Revision**: monotonic logical write clock (`RevisionId`).
- **Snapshot**: immutable read view assembled from sealed blocks.
- **Tombstone**: deletion marker that suppresses prior live records.

## Testimony vs Judgment

InfiniteDB stores **testimony** (parallel assertion streams), not committed global state. **Judgments** are arbiter assertions pinned to a subject revision. **Frames** name which testimony streams and arbiters define a query of truth (M6).

### M6 terms

- **Frame definition**: durable `FrameDefinition` in `frames.bin` — three axes: `AssertionScope` (spaces/branches interim), `judgment_overlay` layers, optional `default_as_of`.
- **Frame query**: `query_hyperedges_in_frame` runs admission → spatial bbox → per-source MVCC → judgment overlay.
- **Overlay policies**: `Suppress` (drop condemned), `Annotate` (attach judgments), `SelectContested` (conflict-only).
- **Frame traversal**: `traverse_in_frame` filters expansion through overlay-resolved allowed edges.

### M7 terms

- **Flow vector**: directed displacement `head_centroid − tail_centroid` in a shared physical space; derived on the bus into `FLOW_VECTOR_INDEX_SPACE` with quantized direction keys (`bits_per_axis = 3` default).
- **Computation provenance**: optional `ComputationProvenance { inputs: Vec<SubjectPin> }` on hyperedge V4 assertions — structured input lineage for derived edges.
- **Backward freshness**: `check_hyperedge_freshness` compares pinned input revisions against observed subject revisions at `as_of`; returns `FreshnessReport` in the Ok channel.
- **Forward stale closure**: `query_stale_downstream` walks M3 forward traversal from changed subjects and collects `StaleTarget` edges whose computation inputs are stale.

### M5 terms

- **Authoring-frame provenance**: optional `frame_id` + `as_of` revision on a hyperedge assertion — the frame the author was reading when writing.
- **Judgment**: `JudgmentRecord` in an arbiter stream, spatially indexed at subject coordinates in `JUDGMENT_INDEX_SPACE`.
- **Subject pin**: `SubjectPin` binds a judgment to exact subject identity and `subject_revision`.
- **Operation error record**: structured `OperationErrorRecord` in a per-space companion `{name}_errors` space; queryable via MVCC; tombstone-resolve preserves audit trail.
- **Arbiter stream**: registered testimony space (`register_arbiter_stream`) for judgment assertions.

## Relationship Terms

- **Hyperedge**: explicit N-ary relationship among endpoint nodes.
- **Endpoint**: one participant in a hyperedge, with a semantic role and structural **polarity**.
- **Polarity**: structural `Tail`, `Head`, or `Neutral` — orthogonal to semantic role labels.
- **Directionality**: edge mode `Directed` or `Undirected`; validation enforces matching invariants.
- **Edge Kind**: user-defined relationship category string (for example `beam.bears_on`).
- **Directionality Policy**: catalog registration (`ObligateDirected`, `ObligateUndirected`, `Free`).
- **Validity Window**: `valid_from..valid_to` revision interval where an edge is active.

## Direction in the endpoint index

**M1 (`V1Symmetric`):** the reverse index is symmetric; directional queries post-filter decoded hyperedge payloads.

**M2 (`V2PolarityDim`):** polarity is a coordinate dimension between endpoint coords and edge-id dimensions. Directional incidence and degree queries filter in the index. V1-layout rows coexist until `upgrade_endpoint_index_layout()` and `compact_endpoint_index()` perform lazy rewrite.

## Signal Terms

- **Signal**: typed measured/computed value over a scoped hyperspace using a user-defined kind string.
- **Signal Scope**: fixed parent coordinate prefix plus declared total dimensions.
- **Signal Sample**: one value at local coordinates within a scope.
- **Signal Constraint**: optional value bounds and clamp policy.

## Modeling Guidance

- Use **hyperedges** for cross-entity topology and provenance.
- Use **signals** for intra-entity fields, gradients, and time/state samples.
- Use both together when results are derived from external relationships.
- Register edge kinds with directionality policy when adapters assert directional relationships (IFC relating→tail / related→head is an adapter decision per relationship class).
