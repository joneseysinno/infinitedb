# InfiniteDB — Peer Track Implementation Plan
## HLC Revisions, Per-Session WAL, Intent Checkpoints

**Source:** InfiniteDB-Spatial-Hyper-Truth-Graph-Design.docx §3.1, §4.1, §5.5, §5.6; concurrent WAL architecture sessions; MILESTONES.md peer-track note
**Status:** In progress (Phase 7 complete)
**Gates:** M4 derivation bus at scale; M6 `AssertionScope::Session`; all multi-stream claims of §3.1
**Prerequisite:** 0.4.0 derivation-watermark fixes (see Phase 0) — the vector watermark of this track generalizes that fix; land it first

---

## Objective

Demote the global revision counter from a write-time precondition to a read-time construction. After this track, a write is: validate locally, stamp from the session's own clock, append to the session's own WAL. No write touches shared state. Global order, stable ceilings, and frame visibility are all derived at read time from stamps carried in the data.

The thesis being made true: **the only synchronous obligation in the system is each stream's durability to itself.**

---

## Scope boundary (restated, because it disciplines every decision)

The database owns: stamping, per-stream durability, intent boundaries, collision *detection*, and making cross-stream order reconstructable. It does not own: semantic conflict resolution, arbiter logic, or replication policy. Those remain application/sync-layer concerns. Nothing in this track adds a synchronization point above per-stream durability; anything that appears to need one is misdesigned and goes back for review.

---

## Design recap (decisions already reached in prior sessions, carried forward as constraints)

1. `RevisionId` becomes a 128-bit Hybrid Logical Clock value packing physical timestamp, logical counter, session identity, and per-session sequence. Total order is lexicographic over (hlc_time, session, sequence); ties are unrepresentable because session disambiguates.
2. Each session owns an independent durable WAL file. Durability = one local append + fsync cadence on that file alone.
3. Intent Checkpoints separate **durability** (the bytes survived) from **commit intent** (the author meant this group as one logical operation). They mark logical operation boundaries in the session's stream.
4. The sealed block store — not the WAL — is the source of truth. WALs are transient scaffolding; deletion is gated on branch merge + replication confirmation + collision evaluation, never on write seal alone.
5. Collision detection operates at Intent Checkpoint boundaries, not individual writes. Resolution is deferred to the application layer (conflict-as-value, per the error algebra).
6. Timed fast path: attempt direct seal within a deadline; fall back to WAL append on timeout. (Performance lane — explicitly last in this plan.)

---

## Phase 0 — Foundations and decision records

**Goal:** Close the open design decisions and land the prerequisite watermark repair, so every later phase builds on settled ground.

### 0.1 Derivation watermark repair (from the 0.4.0 review — blocking)
- Replace the max-register `DerivationWatermark::advance_to` with the outstanding-set / contiguous-prefix pattern already proven in `RevisionWatermark`: register on submit, retire on successful apply, watermark = predecessor of lowest outstanding.
- Failed derivation events: never silently absorbed. Record through the M5 error-record machinery (or a `FailedDerivation` log mirroring `FailedRevision`), surface in `derivation_stats`, exclude from watermark advancement.
- Add the crash test the 0.4.0 review identified: insert → sync → delete → crash before derivation → reopen → directional query must not resurrect the edge.
- Rationale for sequencing: Phase 6 turns this watermark into a per-session vector. Vectorizing a correct scalar is mechanical; vectorizing a broken one multiplies the bug.

### 0.2 HLC bit-layout decision record (D-P1)
Settle and document the 128-bit packing. Recommended starting point for review:
- 48 bits physical time (milliseconds since epoch — sufficient through year ~10889)
- 16 bits logical counter (HLC tie-breaking within one millisecond)
- 32 bits `SessionId`
- 32 bits per-session sequence
Decision points to record explicitly: granularity (ms vs µs trades logical-counter pressure against range), whether sequence wraps or saturates within a checkpoint group, and the canonical lexicographic comparison (time component first so cross-stream order approximates wall time; session before sequence so one session's burst stays contiguous).

### 0.3 Session identity decision record (D-P2)
Who mints `SessionId`s, given offline-first? Options to evaluate:
- **Allocated**: durable per-database registry grants ids at session open. Simple, but allocation is itself a (tiny, once-per-session) synchronization — acceptable because it is per-*session*, not per-write, and sessions open while online or against the local store.
- **Random**: 32-bit random with collision detection at sync time. No coordination ever, but collisions become a handled case.
Recommendation: allocated-from-local-store (each device's database mints from its own registry; cross-device uniqueness via a device-prefix bit range), with the decision record stating the collision posture either way. This mirrors the session-prefixed *entity* identifier decision from §4.1 — same principle, second axis.

### 0.4 Clock discipline decision record (D-P3)
- Maximum tolerated physical clock skew before a stamp is rejected or clamped (HLC standard practice: bound drift, never let a runaway clock poison the order).
- Monotonicity guarantee: HLC `now` = max(physical, last_issued + logical bump). State the invariant as a property test: stamps from one session are strictly increasing under any physical clock behavior, including backwards jumps.

### 0.5 Legacy embedding decision record (D-P4)
Every existing record carries a dense `u64` revision. Define the total, lossless embedding into HLC space: legacy revision *n* maps to (time = 0, session = 0, sequence = n). Consequences to ratify: all legacy history sorts strictly before all HLC-era history (correct — it *was* authored before), session 0 is reserved as "the pre-HLC global stream," and the embedding is a defined decode, never a rewrite of sealed blocks (compatibility rule: sealed blocks are never rewritten in place).

**Phase 0 exit criteria:** watermark repair merged with its crash test; four decision records written into SEMANTICS.md.

---

## Phase 1 — RevisionId widening under versioned encoding

**Goal:** The representation changes; the semantics do not. The system still allocates from the global counter, but every revision is now *expressed* as an HLC value in session 0. Fully releasable; behavior-identical.

- `RevisionId` becomes the 128-bit composite with accessor decomposition (time, session, sequence) and the lexicographic `Ord`.
- On-disk record format version bump (v5): record headers, WAL frames, block index entries, branch overlay logs, `counters.bin`, snapshot headers, outbox state. Every encoding change versioned with a defined decode for legacy (rule 2 of the design doc's compatibility covenant); v4 directories open read-write with decode-time embedding.
- The watermark, stable-revision, MVCC visibility, validity-window, and tombstone machinery generalize to the opaque ordered type. Audit every site that does arithmetic on revisions (`+1`, `next()`, lag subtraction, `saturating_sub`) — arithmetic on a composite is the bug class of this phase. Lag and "next" become operations *within* a session's sequence component or comparisons in HLC time, never raw integer math on the packed value.
- Type-hygiene continuation (per the 0.3.x primitive-obsession review): the packed `u128` never appears raw outside the codec membrane; engine code sees only `RevisionId` and its components.
- Tests: round-trip codec property tests; v4 → v5 open-and-query matrix; total-order property (legacy < HLC-era; within-session monotone); every 0.3.x watermark regression test re-run against the widened type.

**Risk note:** this is the widest-blast-radius phase — it touches every record — which is exactly why it ships with *zero* semantic change. If anything behaves differently with the global counter still in charge, the phase has a bug.

---

## Phase 2 — Session machinery and local stamping

**Goal:** Writers stamp themselves. The global allocation mutex leaves the write path.

- Session lifecycle: `open_session()` mints a `SessionId` (per D-P2), instantiates a per-session HLC clock and sequence counter, both contention-free by construction.
- Write path becomes: validate → `session.stamp()` → proceed. No shared lock, no shared counter. The stamp is the revision.
- **Stable ceiling rework — the conceptually hard center of the track.** A single contiguous-prefix `stable_revision` no longer exists, because there is no single sequence. Replace with:
  - Per-session watermarks: each session tracks its own outstanding set and contiguous-prefix stable point (the Phase 0.1 pattern, instantiated per stream).
  - A read pin becomes a **version vector**: one stable component per admitted session. `ReadTxn` pins the vector; repeatable read means no admitted session's component moves underneath it.
  - A scalar convenience ceiling remains available as the meet of the vector (minimum across sessions) for callers that want the old shape — but it is now explicitly a *frame choice* (admit-everything, as-of-the-slowest), not a system primitive.
- This removes the global convoy: one stalled writer freezes only its own component, and only frames that admit that stream feel it.
- The dispatcher purity discipline applies: stamping and vector comparison are pure functions over (clock state, outstanding sets); effects (registry persistence) stay at the boundary, property-testable without storage.
- Tests: N-session concurrent write storm with per-session monotonicity assertions; repeatable-read under vector pins; the old single-writer tests pass unchanged through a single implicit session.

---

## Phase 3 — Per-session WAL files

**Goal:** Durability becomes local. Each stream's fsync touches only its own file.

- Layout: `sessions/{session_id}.wal`, append-only frames with checksums, honoring the `committed_len` durability-boundary discipline from the 0.3.3 fix (that lesson transfers verbatim: replay never trusts bytes past the committed boundary).
- Each session handle owns its WAL writer; group commit operates per session (batch this session's appends, one fsync), preserving single-stream async throughput without cross-stream coupling.
- Recovery on open: enumerate session WALs, replay all frames, merge by HLC order. **Random arrival is now structurally harmless at recovery too** — replay order across files is irrelevant because order travels in the stamps, not in file position. This is §4.1 made literal at the crash boundary.
- WAL retirement: a session WAL becomes deletable only when its highest checkpointed revision satisfies all three gates — sealed into immutable blocks, replication-confirmed (when sync is configured), and collision-evaluated. Track retirement state per WAL in meta; never gate on seal alone.
- Error algebra: WAL frame failures are `StorageError::WalFrame` (already exists); per-session recovery failures are per-stream quarantine, not global open failure — one corrupted session WAL must not deny service to the other streams' testimony. Quarantined streams surface as error records (M5 machinery), queryable.
- Tests: multi-session crash matrix (kill during append, during fsync, during checkpoint, during retirement); torn-frame recovery per session; quarantine isolation; retirement gating under each combination of the three conditions.

---

## Phase 4 — Intent Checkpoints

**Goal:** Logical operation boundaries become durable, typed, and load-bearing.

- `IntentCheckpoint` frame type in the session WAL: marks "the frames since the previous checkpoint constitute one logical operation." Carries the operation's revision range and an operation kind tag.
- **Durability vs intent, decided explicitly:** assertions durable but trailing an absent checkpoint are *durable-but-uncommitted*. Recovery policy (decision D-P5 to ratify): default rolls them out of visibility (they replay into a quarantine view, not the live store) — but because this is a testimony system, prefer surfacing over silent discard: quarantined fragments become queryable error records ("interrupted operation at session S after revision R"), preserving the convergence-trajectory philosophy. Nothing is unsaid; it is annotated.
- **Typestate, from the error-algebra conversation:** an intent that has not been durably acknowledged is a distinct type from one that has; committing an unacknowledged intent does not compile. The block lifecycle typestate (`Hot`/`Sealed`) lands here too if not already — same technique, adjacent invariant.
- **Derivation bus re-triggering:** checkpoint boundaries replace `sync()` as the bus batch trigger, restoring the design invariant that no derived structure ever reflects half a logical operation. The bus consumes checkpointed groups, not raw events.
- **Collision detection at checkpoint boundaries:** when a checkpointed group's revision range overlaps another stream's writes to the same addresses, emit a collision evaluation as a value (Ok-channel, structured — the arbiter-pattern-one-step-from-being-named, now named). Resolution remains upstairs.
- Tests: crash between last frame and checkpoint (quarantine, not resurrection); bus never observes a partial group; collision evaluation fires exactly at boundaries; typestate compile-fail tests.

---

## Phase 5 — Frame integration (closing the M6 deferral)

**Goal:** A "stream" finally means an asserting session.

- `AssertionScope::Session` lands: stream admission reads the session component directly out of the revision id — co-resident with every record, exactly as §5.5 specified. The interim space/branch admission remains as additional scope vocabulary, not a replacement.
- Supersession within a frame resolves by HLC order *among admitted sessions* — authorship time, not arrival time. The offline-engineer scenario becomes a test: two sessions edit the same edge offline; the later-*authored* assertion supersedes regardless of sync order.
- The frame's as-of axis accepts either a scalar HLC time or a version vector (the Phase 2 pin), making "as of when" precise under plural clocks. Branch overlays compose unchanged.
- `query_staleness_by_source_revision` and freshness checks re-verified under composite revisions (they compare revisions; Phase 1's ordering audit should have caught arithmetic, this phase confirms semantics).
- Tests: per-session frame admission; cross-session supersession by authorship; convergence-trajectory query over one session's stream across all judgments (the §4.3 retrospective frame, now expressible natively).

---

## Phase 6 — Derivation bus and recovery under plural streams

**Status:** Complete.

**Goal:** The background machinery speaks vector.

- Subscriber watermarks become per-session vectors (the Phase 0.1 outstanding-set pattern, per stream). The delta-merge seam is per-stream: a query merges the unindexed delta of exactly those admitted sessions whose component lags the pin.
- `recover_derivation_on_open` keys replay on the vector watermark, replaying only each session's gap — bounding reopen cost to actual lag instead of full history (resolving the 0.4.0 review's Hole 3 in its general form).
- Per-edge ordering on the bus is replaced by the design doc's actual mechanism: hash-partition the event stream on edge identity so one edge always lands on one worker — ordering by construction, not by mutex. Edge-lock map (and its unbounded growth) deleted.
- Backpressure lag is computed per session against that session's watermark component; one flooding importer throttles itself, not every stream.
- Tests: `tests/derivation_phase6.rs` — per-session watermark advance, session-isolated backpressure, vector recovery, cross-session tombstone ordering, hash-partition per-edge consistency.

---

## Phase 7 — Timed fast path (performance lane, optional gate)

**Status:** Complete.

**Goal:** The write path's latency optimization, last because it is the only phase that is purely performance.

- Attempt direct seal within a configurable deadline; on timeout, fall back to the session WAL append. Both paths produce identical durable outcomes; the fast path merely skips scaffolding when the seal is cheap.
- Ships behind a default-off option until the crash matrix from Phases 3–4 passes with the fast path enabled — a fast path that weakens the durability story is a regression wearing a feature's clothes.
- `TimedFastPathPolicy` on `OpenOptions` (default off); `sessions/{id}.fast` durability buffer; `sync_session_wal` / `commit_session_intent` route by `DurabilityMedium`; recovery quarantines durable `.fast` bytes without intent checkpoint.
- Tests: `tests/fast_path_phase7.rs`; Phases 3–5 regression suites pass with fast path enabled.

---

## Costs, risks, and open questions

- **Key growth:** +8 bytes of revision in every record header, every WAL frame, every index row. Measure, don't assume — the design doc's own discipline. Mitigation if needed: per-block revision-base delta encoding inside sealed blocks (a codec concern, invisible above the membrane).
- **The ordering-arithmetic bug class** (Phase 1) is the most likely source of subtle corruption: any surviving `rev + 1` on a packed composite silently jumps streams. The audit must be exhaustive; a lint or newtype trick that makes raw arithmetic uncompilable is worth the day it costs.
- **Clock skew is now a correctness input.** A device with a badly wrong clock authors testimony that sorts into the wrong epoch. D-P3's bound plus an anchor-stream discipline (the design doc's first epistemic risk) is the containment; this plan makes the database enforce the bound and surface violations as error records, while *interpretation* of skewed testimony stays upstairs.
- **The scalar-stable habit.** Existing callers assume one number answers "what's visible." Phase 2's convenience meet keeps them compiling, but every such call site is a candidate for a deliberate frame choice instead; inventory them.
- **Open question carried from the design doc, now forced:** how far the global counter survives as a convenience ordering. This plan's answer: as session 0's embedded history and as the optional meet — never again as an allocator.
- **Sync/replication outbox** speaks per-operation state today; per-session WAL retirement (Phase 3) and checkpoint groups (Phase 4) give it sharper units. Outbox rework is *adjacent*, not in scope — flag the interface, don't redesign it here.

---

## Invariants (testable against every decision in this track)

1. No write-path operation acquires shared mutable state after session open.
2. Stamps from one session are strictly monotone under any physical clock behavior.
3. Replay outcome is independent of WAL file enumeration order and frame arrival order.
4. Durable-but-uncheckpointed assertions are never silently visible and never silently discarded.
5. Sealed blocks are never rewritten; every encoding change has a defined legacy decode.
6. A derived structure's watermark component never exceeds a revision whose rows are not durably applied.
7. One stream's failure (corruption, skew, stall) degrades only that stream's availability.
