# InfiniteDB — Peer Track Hardening Plan
## Epoch-Safe Reads, Testimony Preservation, Commit-Boundary Repair

**Source:** Production review of the peer-track implementation (Phases 0–7); PEER-TRACK-PLAN-hlc-session-wal-intent-checkpoints.md; design doc §3.1–§3.2, §7
**Status:** Complete (Waves A–E)
**Format:** Dependency-ordered tasks in the CRCW_FIX_PLAN style. Each task: Problem → Fix → Tests. Waves are ordered by severity; tasks within a wave are ordered by dependency.

**Theme of this plan:** every critical defect lives at a seam — legacy epoch meets HLC epoch (Wave A), file-level certification meets revision-level appends (Wave B), per-stream independence meets shared commit machinery (Wave D). The fixes are seam repairs, not redesigns; the architecture underneath is sound.

---

## Wave A — Epoch-safe visibility (ReadTxn is unusable with sessions until this lands)

### T1 — Per-session visibility resolution (the structural fix)

**Problem.** Visibility under a `ReadTxn` is a single scalar ceiling compared lexicographically against record revisions. Legacy session-0 stamps live at `physical_ms = 0` and sort before every HLC-era stamp by construction (D-P4), and session 0 is always present in the watermark map — so `scalar_meet()` is permanently pinned in the legacy epoch and every session-written record sits above the ceiling, invisible to every `ReadTxn`, forever. Independently, dormant sessions hydrated on reopen freeze the meet at their final stamp, hiding all later writes from live sessions. The min across incomparable epochs is not conservative; it is wrong.

**Fix.** Repeatable-read visibility becomes per-session: a record is visible under a pinned `VersionVector` iff `record.revision ≤ vector[SessionId(record.revision.session())]`. The scalar ceiling survives only as a *within-one-stream* comparison. Implement in the single visibility-resolution path used by `query_inner` so every query route (plain, bbox, branch, frame) inherits it.

**Decision record D-P6 (write into SEMANTICS.md):** posture for a record whose session is absent from the pinned vector. Recommended: **invisible**. The vector is captured at transaction open and includes every session known at that instant; an absent session was minted after capture, and admitting its writes would violate repeatability by definition. State this explicitly so it is a decision, not an accident.

**Tests.** Session write → `ReadTxn` opened after stable → record visible (the currently-failing reality). Record written by a session minted *after* txn open → invisible (D-P6). Mixed legacy + session records in one space resolve independently per stream. Concurrent writer storm: vector pinned, writer continues, visible set provably constant for the txn's lifetime.

### T2 — `ReadTxn` rework on top of T1

**Problem.** `ReadTxn::new` sets `as_of = scalar_meet()` and threads it as the query ceiling; `version_vector` is captured but decorative.

**Fix.** The vector becomes the pin; `as_of` scalar override remains available as an explicit opt-in single-ceiling mode with a documented caveat that a scalar ceiling is only meaningful within one epoch/stream (frame queries with `FrameQuery.version_vector` already model the correct shape — converge on it). `ReadTxn::new` documentation rewritten: repeatable read means no admitted session's component moves under the pin.

**Tests.** Replace the vacuous `read_txn_pins_version_vector_scalar_meet` (it asserts the meet equals itself) with: open txn, write through two live sessions, re-query inside txn → unchanged result set; new txn → sees both.

### T3 — Scalar-meet audit across the codebase

**Problem.** The meet escaped `ReadTxn`: `stable_revision()` is the meet; `endpoint_index_watermark()` is "the meet across sessions for backward compatibility"; delta-merge and backpressure compare meets against HLC stamps. Each cross-epoch comparison degrades — some safely (delta-merge always merging: correct, slow), some not. Safe-by-luck is not a posture.

**Fix.** Inventory every comparison of a scalar meet against a revision (the effect-boundary-inventory discipline, applied to ordering). Classify each: **per-session** (rewrite against the vector component), **safe-degrade** (annotate with a comment stating why the degradation is conservative), or **caller-facing** (document the epoch caveat on the public method). `stable_revision()`'s rustdoc must state it is the admit-everything frame meet and pre-dates plural epochs.

**Tests.** Per reclassified site, one test demonstrating the previous wrong/degenerate behavior is gone (delta-merge under a session-only workload no longer merges unconditionally once the watermark comparison is per-session).

### T4 — Dormant-session reopen behavior (falls out of T1; verify, don't trust)

**Problem.** Hydrated dormant sessions previously froze the meet (Hole 1's second face).

**Fix.** None beyond T1 — per-session resolution makes a dormant session's records visible up to its final stamp while leaving other streams unconstrained. This task exists to *prove* that.

**Tests.** Write via session, reopen (session id is not resumed; stream goes dormant), open new session, write again → `ReadTxn` sees both streams' records; repeat across a second reopen.

---

## Wave B — Testimony preservation (silent destruction/corruption class)

### T5 — Revision-ranged retirement gates

**Problem.** `SessionWalRetirement` gates are file-level booleans; `eligible_for_deletion()` ignores `highest_revision`. Gates marked at time T1 stay satisfied for frames appended at T2; `try_retire_session_wal` then deletes durable testimony that was never sealed, replicated, or collision-evaluated. The existing gate test marks all three gates *before* its insert — it walks the hazard without noticing.

**Fix.** Each gate carries the revision it certifies through (`sealed_through`, `replication_confirmed_through`, `collision_evaluated_through` as packed stamps). Eligibility: WAL's current `highest_revision ≤ min(certified_through)` across the three, re-verified inside `try_retire_wal` under the writer lock with appends excluded for the check-and-delete window. Marking APIs take the revision being certified; the boolean forms are removed, not deprecated — a footgun this sharp should not compile.

**Tests.** Mark all gates → append one frame → retire refused. Certify-through-R, append nothing → retire succeeds. Race: concurrent append during retire → either the append lands and retire refuses, or retire wins and the append goes to a fresh WAL — never a deleted frame.

### T6 — Replication-gate posture when sync is not configured

**Problem.** With the `sync` feature absent or unconfigured, nothing ever marks `replication_confirmed` → `sessions/*.wal` accumulates unboundedly, or operators mark gates blind (which is T5's hazard).

**Fix.** `OpenOptions` policy enum (decision D-P8): `ReplicationGate::Required` (default when sync configured) vs `NotApplicable` (auto-certifies through the sealed revision when no replication target exists). Documented in SEMANTICS.md alongside the three-gate rule so the embedded-only posture is explicit.

**Tests.** Embedded-only open with `NotApplicable`: seal + collision-evaluate → retire succeeds without manual replication marks. `Required` with no marks → never retires.

### T7 — Lossless legacy embedding (or a hard wall, never a wrap)

**Problem.** `HlcStamp::legacy(sequence: u64)` truncates to `u32`. D-P4 promised a total, lossless embedding; past 2³² session-0 revisions the embedding wraps — duplicate revision identities and ordering inversion, silently. The D-P1 layout makes a lossless u64 embedding impossible in the 32-bit sequence field alone.

**Fix (preferred — widen).** Legacy embedding spills the high 32 bits of the dense counter into `physical_ms` (zero for the entire legacy epoch): `legacy(n) = (physical = n >> 32, logical = 0, session = 0, sequence = n as u32)`. Order is preserved (lexicographic over (physical, …, sequence) reproduces dense order), and the entire embedded range tops out ~50 days after the Unix epoch — decades below any real wall-clock stamp, so epoch separation holds. `is_legacy_embedded` becomes `session == 0 ∧ logical == 0 ∧ physical < LEGACY_PHYSICAL_CEILING`. Update D-P4 in SEMANTICS.md.

**Fix (fallback — guard).** If widening is rejected, the boundary becomes a corruption-class `StorageError` (fatal per the recoverability taxonomy): allocation at `u32::MAX` halts with diagnosis. A wrap is never acceptable; a halt is.

**Tests.** Property test: dense order ↔ embedded order isomorphic across the u32 boundary. Round-trip `legacy_sequence()` lossless for values above 2³². Embedded ceiling strictly below a year-2000 stamp. Boundary allocation under the fallback → fatal, not wrap.

---

## Wave C — Commit-boundary repair (perf cliffs on the IFC workload + regressions of Phase 0 discipline)

### T8 — Point-scoped collision detection

**Problem.** `detect_checkpoint_collisions` runs `query(space, None)` — the entire space, no ceiling — per buffered write, then scans linearly. O(N·M) per commit; ~10¹¹ row visits for a 100k-write intent into a million-record space. The headline workload hits the worst case at every checkpoint.

**Fix.** Exact-coordinate lookup per buffered address (degenerate bbox — the Hilbert machinery makes this a point probe), deduplicated per address within the intent, bounded by the read ceiling at commit. Optional refinement: batch addresses per space and probe via one decomposed multi-point scan.

**Tests.** `QueryPlanStats`-style instrumentation: commit of N writes performs O(N) point probes, never a full-space scan. Throughput test: 100k-write intent into a 1M-record space commits within an asserted bound.

### T9 — Collision evaluation as structured value; commit is never vetoed

**Problem.** Detection returns `Option<SpaceId>` — first hit, space only. The address, sessions, and revisions the application needs for resolution are discarded; and if the caller fails the commit on `Some`, write-time synchronization has re-entered through the side door, contradicting §3.2 (contradictory assertions coexist as occurrence-truths; arbiters adjudicate later).

**Fix.** `CollisionEvaluation { address, sessions, revisions }` collected exhaustively (not first-hit) and returned in the **Ok channel** of the commit result (error-algebra rule 3: things the application resolves travel as values). Each evaluation also persisted via the M5 error/judgment record machinery, spatially co-located with its subject — collision history becomes a query. Commit always proceeds; durability of testimony is never hostage to the existence of disagreement. Audit the current caller and remove any abort-on-collision path. The stringly `EngineError::Other { message: e.to_string() }` inside detection becomes a typed variant while in there.

**Tests.** Two sessions, same address, both commits succeed; second commit's result carries the evaluation with both sessions and revisions; `query_operation_errors` (or judgment query) finds the persisted record at the subject's coordinates. Frame query with conflict overlay surfaces the contested address.

### T10 — Session-scoped commit flush; kill the spin

**Problem.** `commit_session_intent` calls the global `derivation.flush()` — all sessions' backlog — so one importer stalls every other stream's commit: the cross-stream convoy, re-created at the commit boundary one phase after Phase 2 removed it from the write path. And if `flush()` is still the `yield_now` spin loop, every commit burns a core while convoyed.

**Fix.** Commit waits only for its own stream: block until the committing session's derivation-watermark component (Phase 6 machinery — already per-session) reaches the intent's last revision. Replace the spin with a condvar/completion-channel signal raised on retire; the global `flush()` gets the same signal for `sync()`/shutdown.

**Tests.** Session A floods the bus; session B's small-intent commit completes within a bound independent of A's backlog. CPU assertion (or instrumented wait counter): commit wait performs zero spin iterations.

### T11 — Eradicate the regrown stderr swallow

**Problem.** Derivation submit failure during commit replay is handled with `eprintln!` — the exact pattern Phase 0 eliminated from the bus workers, regrown one layer up in new code.

**Fix.** Route through `FailedDerivation` / operation error records, surfaced in `derivation_stats`, exactly like its sibling path. Add a lint-style grep gate to CI: no `eprintln!` in `engine/` outside tests — make the pattern's return mechanically detectable.

**Tests.** Injected sink failure during commit replay → visible in `derivation_stats().derivation_failures` and queryable as an error record; nothing on the watermark claims completeness over the failed revision (Wave A's per-session watermark makes this automatic — assert it).

### T12 — Bounded intent buffer

**Problem.** `PendingIntentState` buffers every write in RAM until checkpoint. The frames are already durable in the session WAL — durability is fine — but a 4GB import inside one logical intent is an OOM with extra steps.

**Fix.** Two options; pick by decision record D-P9. (a) **Auto-segmentation:** a configurable buffered-bytes threshold forces an intermediate checkpoint, starting a fresh sequence epoch (D-P1 already defines checkpoint-scoped sequence epochs — this is its intended use); the import becomes a chain of intents with a grouping tag. (b) **WAL-replay commit:** drop the in-memory buffer entirely and rebuild the replay set from the session WAL's committed range at checkpoint time — the WAL already holds the truth. (a) is simpler and preserves streaming derivation; (b) is purer (one source of truth) at the cost of a re-read. Recommend (a) now, (b) as the eventual shape.

**Tests.** Import exceeding the threshold completes under an asserted peak-RSS bound; resulting chain of checkpoints recovers correctly across a crash between segments (quarantine only the final uncommitted tail).

### T13 — Per-commit `persist_meta` audit

**Problem.** Every intent commit writes global meta — a shared fsync per commit across all streams; minor convoy and a durability hot spot.

**Fix.** Scope: persist only the session-WAL meta delta touched by the commit, or debounce global meta behind a dirty flag synced on `sync()` / retirement / close. Measure before and after — this one is a measurement-first task per the design doc's own discipline.

**Tests.** Commit-rate benchmark across 4 concurrent sessions, before/after; correctness: crash after commit, before any later meta sync → recovery still honors the checkpoint (the WAL, not meta, is the recovery truth — assert it).

---

## Wave D — Clock discipline (correctness of authorship order)

### T14 — Trust forward wall jumps; contain poison instead of clamping

**Problem.** `advance_hlc` caps forward movement at `last + max_drift` (5 min). Laptop sleep/resume — the canonical offline-first event — produces stamps that crawl forward 5 minutes per write for hours of wall time, while a fresh session stamps true time: the slept session's later writes sort before the fresh session's earlier ones. The guard rail inverts the authorship order it exists to protect.

**Fix.** Standard HLC posture: `physical = max(wall, last_physical)` — forward jumps trusted. The drift bound guards the *opposite* direction: if `last_physical` ever exceeds wall by more than the bound (evidence a past stamp was poisoned by a transiently insane clock), the session surfaces a clock-skew error record and refuses to stamp until acknowledged — detect-and-surface, never silently manufacture false times. Update D-P3.

**Tests.** Simulated 2-hour forward jump: next stamp lands at true wall time, sorts after a fresh session's pre-jump stamps and before its post-jump stamps. Simulated poisoned `last_physical` (wall far behind): stamping refused with a structured error; no false stamps issued.

### T15 — `logical` never wraps

**Problem.** `logical` is `u16` advanced with `wrapping_add`; 65,536 stamps within one pinned millisecond wrap to zero — ordering inversion inside one session, the invariant that must never break.

**Fix.** On logical exhaustion, bump `physical_ms` by one and reset logical (the stamp remains ≤ wall under T14's posture for any realistic rate; if it would exceed wall+bound, that is T14's poison path and surfaces as the error). `saturating_add` alone is insufficient — saturation produces *equal* stamps, which is also an inversion of strictness.

**Tests.** Property test: 10⁶ stamps under a frozen wall clock are strictly increasing. The 0.x `temporal.rs` clock's saturating behavior is not imported.

### T16 — `receive()` on the revision clock (causality absorption)

**Problem.** The session clock never absorbs observed stamps. Read-then-write within one millisecond across sessions can invert causality (B reads A's `(t, 5)`, stamps `(t, 1)`); replication has no mechanism for a replica to advance past remote stamps at all. The old temporal clock has `receive()`; the clock that now defines truth order does not. Frame supersession promises authorship order and delivers it modulo these gaps.

**Fix.** Add `receive(observed)` per standard HLC (advance to `max(self, observed)` + logical bump). Wire it at two boundaries by decision record D-P10: **mandatory** — the sync/replication apply path advances the local clock past every applied remote stamp before the next local stamp; **optional, scoped** — cross-session read absorption inside one process (a `ReadTxn` hands its max observed stamp to the session on the next write). Recommend shipping the mandatory boundary now and documenting the within-millisecond single-process caveat explicitly if the optional one is deferred — a named gap, not a silent one.

**Tests.** Replication apply of a remote stamp ahead of local wall → next local stamp exceeds it. (If optional lands) read-A-then-write-B within one frozen millisecond → B's stamp exceeds A's.

### T17 — Pre-epoch wall clock guard

**Problem.** `wall_clock_ms()` returns 0 when system time precedes the epoch; a session on a broken clock issues stamps at `physical_ms = 0`, adjacent to (post-T7, potentially inside) the legacy embedding region.

**Fix.** Pre-epoch (or below `LEGACY_PHYSICAL_CEILING`) wall reads are a clock-skew error (T14's surface path), never a stamp.

**Tests.** Mocked pre-epoch clock → structured refusal; no stamp below the legacy ceiling ever issued by a live session.

---

## Wave E — Carried-forward debt (pre-multi-consumer, not blocking embedded use)

### T18 — Typed public API surface
`Result<_, EngineError>` variants of the public methods (additive; `io::Result` forms delegate through `engine_to_io` for compatibility). Library callers gain the algebra: matching `DerivationBackpressure` for backoff, `Storage(Corruption)` for halt.

### T19 — `ApiError` retryable variant
`ApiError::Busy { retry_hint }`; `project_api_error` maps `DerivationBackpressure` (and future retryables) to it instead of `InvalidRequest` — the taxonomy lie at the wire, fixed.

### T20 — Error/judgment space retention policy
Per-space retention policy for `{name}_errors` and judgment spaces (age/count-based compaction of *resolved* records only; unresolved and judgment records are testimony and keep the append-only covenant). Connects to the design doc's risk 3: retention is where the access-policy obligation becomes enforceable.

### T21 — Arbiter id reservation
`register_arbiter_stream` rejects ids below the reserved threshold instead of documenting "prefer ids ≥ 10" — make the key-layout hazard unrepresentable.

---

## Sequencing summary

A (T1→T2→T3→T4) is first and strictly ordered internally — nothing downstream is testable while `ReadTxn` cannot see session writes. B (T5, T6 parallel; T7 independent) is the destroy/corrupt class and gates any production data anyone cares about. C unblocks the IFC workload (T8→T9 ordered; T10→T11 ordered; T12, T13 independent). D is internally ordered T14→T15→T16 (T17 anywhere) and should land before any replication work begins, since T16's mandatory half is replication's prerequisite. E rides along opportunistically.

## Invariants this plan adds to the standing set

8. A scalar revision ceiling is never compared across sessions or epochs; cross-stream visibility is resolved per session against a vector.
9. WAL deletion eligibility is certified through an explicit revision; no certification outlives subsequent appends.
10. The legacy embedding is order-isomorphic and lossless over the full dense range (or halts — never wraps).
11. Detection of conflicting testimony never blocks, fails, or delays the durability of that testimony.
12. A session's stamps are strictly increasing across any wall-clock behavior, and never silently false: suspected clock poison refuses with diagnosis.
