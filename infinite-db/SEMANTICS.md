# InfiniteDB semantics

Decision records for durable architectural choices.

## D-EXEC — Server request execution

The TCP server MUST NOT execute `handle_request` on a tokio runtime thread. Requests are
dispatched to a dedicated OS-thread pool (`RequestExecutor`) over a bounded channel; replies
return via `tokio::sync::oneshot`. Tokio threads perform socket framing and `await` only. This
is the read/request-side mirror of the write coordinator (`WriteQueueSender` → `space_io`
threads).

## D-BLOCKING-CONTRACT — Engine is a blocking API

`InfiniteDb` methods (`query_on_branch*`, `insert_on_branch`, `sync`, `flush`, …) are
synchronous and may block on disk I/O, `parking_lot` locks, and bounded-queue backpressure.
Any async caller MUST offload them (dedicated executor or `spawn_blocking`) and MUST NOT call
them directly from an async task. The `server` feature's `RequestExecutor` is the reference
implementation of this contract; embedded callers that run their own runtime owe the same
discipline.

## D-EXEC-BACKPRESSURE — Queue-full policy

**Choice: 4a (never shed by default).** `request_queue_capacity` defaults to
`max_connections` (128). With strict per-connection serialization (read → submit → await →
write), each connection has at most one outstanding job, so the job channel is structurally
never full when capacity ≥ max connections. A full worker pool queues jobs; the connection
`await`s on the oneshot reply; no tokio thread blocks and no requests are rejected.

The `SubmitError::Busy` → `ApiError::Busy` path is implemented for optional 4b (smaller
capacity as an explicit in-flight cap) but is unreachable under default configuration.

`executor_threads` defaults to `std::thread::available_parallelism().map(NonZero::get).unwrap_or(4)`
— the steady-state read concurrency ceiling.

Cross-reference: engine write-queue backpressure (`enqueue` blocks when full) is unchanged by
this work; it now runs on request-pool threads instead of tokio threads but remains a separate
open decision.

## D-T1 — Placement registry + mirror

**Choice: both, registry-authoritative.** The registry copy on `SpaceConfig` is validated and
used for catalog queries and Bion descent. A mirrored structural record at the child's
parity-reserved center in the parent space is written by the placement-mirror deriver on
registration (single-writer via the write coordinator, idempotent on replay).

## D-T2 — Path names vs parent links

**Choice: parent links authoritative; dotted path names are convention only.** The registry
never derives structure from string parsing.

## D-T3 — Placement units

**Choice: integer rationals.** Child maps to parent as
`parent = offset + child * scale_num / scale_den` per axis.

## D-T4 — Dimensional consistency

**Choice: child `dims` ≤ parent `dims`; `fixed_axes` supplies parent coordinates for axes the
child lacks.** Child dims > parent is rejected.

## D-T5 — Center-reservation granularity

**Choice: all dyadic levels when policy is on.** Policy is per-space opt-in
(`CenterReservation::Off | StructuralOnly`), default `Off`.

## D-T6 — Structural writes at reserved centers

**Choice: mechanical `structural` marker on the write path;** `insert_structural` and the
placement-mirror deriver set it; other writes at reserved centers are rejected.

## D-T7 — Normalized top-aligned curve addresses

**Choice: yes.** Stored keys are top-aligned u128 curve addresses
(`raw_index << (128 − dims·bits_per_dim)`). Construction occurs only through `CurveAddress`.
Arc-length normalization `d/(D−1)` remains a derivable view, not the key.

**Nesting corollary:** Raising `bits_per_dim` requires no rekey — precisions interleave as
bit-prefixes.

**Dyadic boundary rule:** Semantic boundaries aligned to dyadic cells are exactly one key
interval; non-aligned boxes decompose into up to 32 — prefer dyadic-aligned application
boundaries.

**Key 0 sentinel:** Key `0` is both the origin cell's address and `CachedHilbertKey`'s unset
sentinel; consumers must treat unset as recompute, which yields the identical key — benign
by construction.

**Alignment audit (D-F4):** No pre-D-T7 persisted databases exist in any release; realignment
migration not required.

## D-F1 — Mirror coordinate definition

**Choice:** center of the smallest dyadic cell in the parent that contains the child's
transformed extent. Per axis, the containing level is the shared-prefix level of extent min
and max; the point level k is the minimum feasible level across axes (coarsest constraint
wins), clamped to k ≥ 1; coordinate per axis is `cell_base + 2^(bits−k−1)`. When the extent
spans both halves of the domain on an axis, fall back to the level-1 center of the half
containing the extent midpoint. Pure function `parity_center_for_extent`; no registry access.

## D-F2 — Density architecture (D-T9-DEV)

**Choice:** synchronous in-memory pairwise fold superseding the derivation-bus artifact.
Inputs are keys crossing the write effect boundary; the step is O(1) and pure; the statistic
is advisory. **Persistence:** rebuild-on-open by scanning sealed block records plus live-tail
keys per space (block metadata carries both min and max keys; full block read restores exact
`record_count` and pairwise depth).

## D-F3 — `cell_prefix` / depth units

**Choice:** `cell_prefix(prefix_bits)` names a raw bit count; `cell_prefix_level(level)` =
`cell_prefix(level × dims)`. `SpaceDensity.max_occupied_depth` is denominated in tower
**levels** (dims-bit groups), not raw bit counts.

## D-F4 — Prior key alignment finding

**Finding:** No release or persisted database directory predates `CurveAddress` top alignment
in `hilbert_key_for`; realignment migration not required.

## D-T8 — `register_or_get` and session leases

**Choice: lease-free by design.** Idempotent creation makes concurrent subdivision safe without
session-lease scope on space registration.

## G-NATIVE-DESCENT — Deferred

Native cross-space bbox descent in InfiniteDB remains **deferred**. Reopen when (a) Bion-side
descent over the catalog surface is measured as a round-trip bottleneck on a realistic tree
(depth ≥ 4, fan-out ≥ 16), and (b) a design keeps spaces mutually unaware except through the
registry. Baseline measurements are printed by `wave3_descent_conformance_fixture` in
`tests/space_tower.rs` (round-trip count and wall time per run, `G-NATIVE-DESCENT baseline:`
prefix).

## D-T12 — Cross-space flow vectors

When a directed edge's tail and head centroids live in different spaces, the derivation bus
composes each centroid to the nearest common ancestor via the placement path (T11), forms
`head − tail` in ancestor coordinates, and indexes the quantized direction. Same-space vectors
use the legacy V1 index payload; cross-space rows carry a V2 tag (`0xF2`) plus the ancestor
`SpaceId`. Edges in disjoint space forests produce no flow-vector index row (queryable absence,
not an error).

## D-V1 — Void naming

**Choice:** name the absence-of-data primitive `Void` (not `Vacuum`, `Nil`, `Empty`).

## D-V2 — Three tiers stay distinct

**Choice:** keep `Void` (never written) / `Tombstone` (written, then logically deleted;
revision history retained; visible to as-of queries) / `Null` (application axis, out of
scope) as three distinct tiers; never collapse.

## D-V3 — Void algebra is polymorphic over containers

**Choice:** implement the void algebra once via [`VoidState`](crate::infinitedb_core::void::VoidState)
(`Space` now; `Universe` and beyond later against the same trait).

## D-V4 — Presence representation

**Choice:** core three-state enum [`Presence`](crate::infinitedb_core::void::Presence):
`Void` | `Tombstoned { last: RevisionId }` | `Present(Record)`. `Tombstoned` carries the
deleting revision because that is what distinguishes it observably from `Void`.

## D-V5 — Where `IS_VOID` evaluates

**Choice:** engine point-read path, snapshot-pinned, honoring `as_of`. Compose the existing
point lookup with tombstones included: no record → `Void`; newest tombstone → `Tombstoned`;
else `Present`. `index_only` staleness bounds apply to presence exactly as for record reads.

## D-V6 — Void-propagating comparison type

**Choice:** [`VoidOr<T>`](crate::infinitedb_core::void::VoidOr) in the core module with
`map` / `and_then` / `zip_with` where `zip_with` propagates `Void` if either side is `Void`.
Distinct from `Presence`: `Presence` is storage absence; `VoidOr` is derived-computation
absence.

## D-V7 — Undefined-over-void error placement

**Choice:** typed [`EngineError::UndefinedOverVoid`](crate::engine::error::EngineError::UndefinedOverVoid)
`{ operation, container }`. `is_caller_correctable()` → true; `is_retryable()` → false.
Ratio-shaped derived statistics return `VoidOr` or this error — never a defaulted zero.

## D-U1 — Universe generalizes the tower

**Choice:** every `Placement` is a `Nexus`; not every `Nexus` is a `Placement`.

## D-U2 — Naming: Constellation

**Choice:** name the emergent inter-space cluster `Constellation` (not `quadrant`).

## D-U3 — Nested zoom without hierarchy

**Choice:** a dense subgraph viewed from outside is a `Constellation`; contracting and
re-detecting uses the same container-generic graph — no `Universe` entity variant.

## D-U4 — Nexus shape

**Choice:** `NexusEdge` is a sibling of `Hyperedge` at container granularity, with
`weight_milli` and `valid_to` for transfer volume and transient edges.

## D-U5 — Wanderer and Ephemeris naming

**Choice:** `Wanderer` for the unhomed object; `Ephemeris` for its trajectory log.

## D-U6 — Graph storage

**Choice:** explicit Nexus rows in `NEXUS_SPACE`; placement edges projected from the
registry only (`INV-UNI-PROJECTED`). Membership via [`is_universe_member`](crate::infinitedb_core::universe::is_universe_member).

## D-U7 — ContainerRef

**Choice:** `ContainerRef::Space | Constellation` — no `Universe` variant (D-U10).

## D-U9 — Pinned constellations

**Choice:** detection is ephemeral; durable pins are `"constellation.pin"` assertions in
`NEXUS_SPACE`. Constellation detection uses **weighted label propagation**: each node adopts
the label of a highest-`weight_milli` neighbor (projected placement edges default to
`DEFAULT_PLACEMENT_WEIGHT_MILLI`), with id-sorted tie-breaks on labels.

## D-U10 — Nesting is zoom + port

**Choice:** one ambient universe graph per database; porting imports members and pins as a
constellation — no nested-universe registry.

## D-U13 — Universe voidness

**Choice:** `VoidState` on [`UniverseGraphView`](crate::infinitedb_core::universe::UniverseGraphView)
(member-void); separate `is_relation_void(as_of)` for populated-but-disconnected graphs.
[`UndefinedOverVoid`](crate::engine::error::EngineError::UndefinedOverVoid).`container` is
`Option<SpaceId>` (`None` = ambient universe).

## D-E1 — Ephemeris queryable by default

**Choice:** ephemeris entries are frame-queryable hyperedge testimony in `EPHEMERIS_SPACE`.

## D-E2 — Ephemeris as hyperedge testimony

**Choice:** kinds `ephemeris.observed` / `ephemeris.projected` carry the D-E1 discriminant.

## D-E3 — Wanderer identity

**Choice:** caller-allocated `WandererId`; identity node in `WANDERER_REGISTRY_SPACE`.

## D-E4 — Graze traces

**Choice:** default no trace; opt-in `"graze"` Nexus edges via derivation subscriber.
Graze `NexusId` is `blake3` of the canonical `(wanderer, space, stamp, region)` tuple
truncated to u64 — a pure function of the entry so replay is idempotent, without XOR
collisions. Trace weight is `GRAZE_WEIGHT_MILLI = 0` so detection leaves the wanderer
unclustered (`INV-EPH-UNCLUSTERED`).

## D-U8 — NexusEdge sibling type

**Choice:** `NexusEdge` mirrors hyperedge field shapes as a sibling type (not a variant).

## D-U11 — Nexus bulk transfer

**Choice:** durable `IntentOperationKind::NexusTransfer` with phases
`Prepared → Copying → TargetSynced → SourceTombstoning → Complete`; target fsync
before source tombstoning (`INV-NEX-TRANSFER-ORDER`); idempotent target writes.

## D-U12 — Port bundle format

**Choice:** self-contained `UniversePortBundle` (space configs, nexus edges, optional
records by name); provenance encoded in ported space names; idempotent re-port via bundle hash.

## D-U13 — ratio statistics

**Choice:** [`mean_eccentricity`](crate::infinitedb_core::universe::mean_eccentricity),
[`edge_set_density`](crate::infinitedb_core::universe::edge_set_density), and
[`modularity`](crate::infinitedb_core::universe::modularity) return
`UniverseRatioError` on member-void or singleton input; callers map to
[`UndefinedOverVoid`](crate::engine::error::EngineError::UndefinedOverVoid), never silent zero.

## INV-INDEX-PRECISION-DOMINATES

An index space's `bits_per_dim` must be ≥ the max `bits_per_dim` of geometry spaces it
indexes. Packed-id 2D×32 assertion spaces (nexus, ephemeris, error companions, hyperedge
storage) are exempt: their coordinates are identity, not geometry. New databases use
`EndpointIndexLayout::V3CompactKey`: interned `space_ordinal`, endpoint geometry, polarity,
and a truncated `valid_from` discriminator. `HyperedgeId` lives in the payload only.
The discriminator is little-endian packed into remaining Hilbert dimensions (24 bits in
the 11-coordinate worst case). This is the cheap fix of the Universe punch list — not the
append-multiset or space-tower redesigns, which remain future options if 24-bit same-revision
collisions on one endpoint become load-bearing.

## D-DET — Deterministic iteration order

**Choice:** hash-ordered containers (`HashMap`, `HashSet`, `dashmap`) are permitted for
lookup and accumulation ONLY. Any value whose iteration order can reach an **observable
boundary** MUST have a total order established before it crosses that boundary. The
observable boundaries are:

1. a wire response (`Response` / `ApiError` payloads),
2. a codec or on-disk encoding,
3. a checksum, Merkle leaf, or bundle hash,
4. a WAL or intent-checkpoint record,
5. a test assertion or diagnostic that compares whole values.

Establishing order means either building in a `BTreeMap`/`BTreeSet`, or collecting from a
hash container and sorting on a key that is **total** — a sort key on which two distinct
elements can tie is not a total order, and `sort_by` is stable, so ties silently inherit
hash order. Where a sort key may tie, extend it until it cannot (for graph edges: include
the endpoint list, not just `(projected, kind, nexus_id)`).

Rationale: `std`'s `RandomState` is seeded per map instance, so hash iteration order differs
between two maps built identically in the same process, and between runs. A test that calls
the same function twice on the same structure will NOT catch a violation — the structure
must be rebuilt. `INV-DET-STABLE` is the general form: for any read-side request, N
independently constructed instances of identical state produce byte-identical responses.
`INV-UNI-DETERMINISTIC` is this invariant applied to the universe analytics.

Non-goals: this record does not require replacing `HashMap` on hot paths. `SpaceRegistry`,
the query engine, and the storage layer keep hash lookup; they owe the ordering step at
their output boundary. `infinitedb_sync::replicate::latest_per_address` (collect, then sort,
then Merkle) is the reference implementation of this contract.

Rejected: switching to a fixed-seed hasher (`FxHashMap`, `ahash`) to obtain reproducible
iteration. It yields an order that is arbitrary rather than canonical, changes silently with
a toolchain or dependency bump, and forfeits SipHash's HashDoS resistance on a server whose
keys (space ids, addresses, container refs) are chosen by the client.
