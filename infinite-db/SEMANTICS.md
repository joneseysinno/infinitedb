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
