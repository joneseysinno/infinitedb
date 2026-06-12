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

- **Frame definition**: durable `FrameDefinition` in `frames.bin` — three axes: `AssertionScope` (spaces, branches, sessions, or `Union`), `judgment_overlay` layers, optional `default_as_of`.
- **Frame query**: `query_hyperedges_in_frame` runs admission (including session filter on `record.revision.session()`) → spatial bbox → MVCC (scalar or version-vector pin) → judgment overlay.
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

## Peer track — decision records (Phase 0)

These records gate the HLC / per-session WAL / intent-checkpoint track. See [PEER-TRACK-PLAN-hlc-session-wal-intent-checkpoints.md](PEER-TRACK-PLAN-hlc-session-wal-intent-checkpoints.md).

### D-P1 — HLC 128-bit layout

| Field | Bits | Notes |
|-------|------|-------|
| Physical time | 48 | Milliseconds since Unix epoch |
| Logical counter | 16 | HLC tie-break within one millisecond |
| Session id | 32 | Asserting stream identity |
| Per-session sequence | 32 | Monotone within one session |

**Total order:** lexicographic `(hlc_time, session, sequence)`. Session disambiguates ties; sequence keeps one session's burst contiguous.

**Quantization:** milliseconds (not microseconds) to preserve logical-counter headroom. Sequence wraps at `u32::MAX` within a checkpoint group; a new intent checkpoint starts a fresh sequence epoch.

### D-P2 — Session identity

**Decision:** allocated from the local database registry at `open_session()`.

Each device's store mints `SessionId` values from its own counter persisted in meta. Cross-device uniqueness uses a device-prefix bit range in the high bits of `SessionId` (allocated per deployment policy). Collision posture: local allocation is authoritative on the authoring device; sync-time collision detection surfaces duplicates as structured error records, never silent merge.

This mirrors the session-prefixed entity identifier principle from design §4.1 — same axis, second dimension.

### D-P3 — Clock discipline (hardening Wave D)

- **Forward trust:** `physical = max(wall, last_physical)` — forward wall jumps (sleep/resume) are trusted.
- **Backward guard:** if `last_physical > wall + max_drift` (default 5 minutes), stamping refuses with `ClockSkewError` (detect-and-surface, never silently false times).
- **Monotonicity:** equal physical bumps `logical`; `logical` exhaustion bumps `physical_ms` by one (no wrap).
- **Pre-epoch wall:** wall reads below `LEGACY_PHYSICAL_CEILING` refuse to stamp (T17).
- **`receive(observed)`:** standard HLC absorption for replication apply (T16).

### D-P4 — Legacy revision embedding (lossless spill)

Dense `u64` revisions embed losslessly into HLC space:

- `legacy(n)` → `(physical = n >> 32, logical = 0, session = 0, sequence = n as u32)`
- `legacy_sequence()` → `(physical << 32) | sequence`
- `is_legacy_embedded` → `session == 0 ∧ logical == 0 ∧ physical < LEGACY_PHYSICAL_CEILING`

Consequences:

- Order-isomorphic across the full dense `u64` range (no `u32` wrap).
- All embedded legacy stamps sort strictly before real wall-clock HLC-era stamps.
- Session `0` is reserved as the pre-HLC global stream.
- Sealed blocks are never rewritten; embedding is decode-time only.

### D-P6 — ReadTxn vector pin (hardening Wave A)

Under a pinned `VersionVector`, a record is visible iff `record.revision ≤ vector[record.session]` **and** the session is present in the pin. Absent sessions are **invisible** (minted after capture violates repeatability). Scalar `as_of` on `ReadTxn` remains an explicit single-ceiling opt-in with a within-one-stream caveat.

### D-P8 — Replication gate policy

`OpenOptions.replication_gate`: `Required` (explicit replication marks) vs `NotApplicable` (embedded-only — auto-certifies replication through the sealed revision). WAL retirement requires all three gates certified **through** `highest_revision` (revision-ranged, not file-level booleans).

### D-P5 — Intent checkpoint recovery (Phase 4)

Durable session-WAL data frames without a trailing intent checkpoint are **durable-but-uncommitted**. On recovery they are **not** replayed into the live store; they are surfaced as queryable `InterruptedSessionIntent` operation error records in the companion error space. Committed groups (data frames followed by an intent checkpoint) replay in HLC merge order like Phase 3.

### Phase 7 — timed fast-path durability

- **`TimedFastPathPolicy`** (default off): when enabled, `insert_with_session` / `insert_hyperedge_with_session` buffer in memory only; durability at `sync_session_wal` tries `sessions/{id}.fast` seal within `direct_seal_deadline` (`IoThreadConfig::direct_write_timeout`).
- **Timeout fallback:** on deadline expiry, pending frames append to the session WAL and `sync_group` as in Phase 3–4.
- **Visibility:** fast-segment bytes are never published to `live_tail` or query paths until `commit_session_intent` writes the intent checkpoint and enqueues live-store jobs — same gate as Phase 4.
- **Recovery equivalence:** durable `.fast` data without a matching intent checkpoint is **durable-but-uncommitted** (D-P5): surfaced as `InterruptedSessionIntent` error records, not replayed into the live store. After `commit_session_intent`, fast-path groups are recorded in the session WAL (data frames + intent checkpoint) for the same HLC replay path as `SessionWal`; the `.fast` file is truncated.

### Phase 5 — session-scoped frames

- **`AssertionScope::Session`**: admits testimony whose record revision session component is listed in the frame scope.
- **Authorship supersession**: among admitted sessions sharing an address, the highest revision ≤ the frame pin wins (HLC total order — authorship time, not sync arrival).
- **Version-vector pin**: `FrameQuery.version_vector` supplies per-session stable ceilings; scalar meet remains available via `as_of` or frame default.
- **Hyperedge session writes**: `insert_hyperedge_with_session` buffers to the session WAL and publishes on intent commit with derivation-bus replay.
- **Freshness under HLC payloads**: computation input pins encoded through legacy revision wire compare equal to HLC observed revisions when sequence matches (until hyperedge payload carries full stamps).

### Derivation watermark (Phase 0.1 → Phase 6)

Subscriber watermarks use the outstanding-set / contiguous-prefix pattern (same as `RevisionWatermark`): register on bus submit, retire on successful apply, complete-through = predecessor of lowest outstanding. Failed derivations remain outstanding, block advancement, and appear in `derivation_stats` / `failed_derivations()`.

**Phase 6 — per-session vectors:** each subscriber (`endpoint_index`, `flow_vector_index`, …) tracks a `DerivationSessionWatermark` keyed by asserting session (always including session 0). Bus submit/retire routes by `event.source_revision.session()`. `endpoint_index_watermark()` returns the scalar meet across sessions; `endpoint_index_watermark_vector()` returns the full per-session map. Delta merge includes a record when `record.revision.session()` is admitted and `record.revision > derivation_wm.get(session)` (and ≤ query ceiling). Backpressure compares lag within the submitting session only (sequence gap + outstanding count, not cross-session `legacy_sequence` arithmetic). Workers are hash-partitioned on `HyperedgeId` for per-edge ordering without an edge-lock map. `recover_derivation_on_open` skips records already complete per session component in the vector watermark.

## Modeling Guidance

- Use **hyperedges** for cross-entity topology and provenance.
- Use **signals** for intra-entity fields, gradients, and time/state samples.
- Use both together when results are derived from external relationships.
- Register edge kinds with directionality policy when adapters assert directional relationships (IFC relating→tail / related→head is an adapter decision per relationship class).
