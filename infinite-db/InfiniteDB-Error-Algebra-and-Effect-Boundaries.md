# InfiniteDB — Error Algebra & Effect Boundaries

**Design Addendum · June 2026**

A theory-level assessment of functional programming principles in InfiniteDB's error handling, an audit of where the current design holds and where it leaks, and a forward design for the error architecture of the bulk-import pipeline and the WAL / Intent Checkpoint work.

---

## 1. Purpose & Framing

InfiniteDB's error handling today is *FP-shaped at the edges, imperative in the middle*. Rust's type system enforces a functional baseline — every fallible path returns `Result`, optional state is `Option`, pattern matching is total — but the deeper functional commitments (lossless error morphisms between layers, errors as first-class domain values, effects isolated at boundaries) are applied inconsistently. Some subsystems embody them deliberately and well; others erase structure at exactly the boundaries where it matters most.

This document names the patterns, audits the current state, and proposes a target error architecture aligned with InfiniteDB's existing design principles — particularly layer-separation discipline and the sealed-block-store-as-truth philosophy.

A note on scope: this is an architecture document. It deliberately contains no code. Its purpose is to fix the *algebra* — the types, the conversions between them, and the semantics of failure — before any implementation work.

---

## 2. What Is Already Genuinely Functional

### 2.1 The pure dispatcher (functional core, imperative shell)

The API request dispatcher models database operations as injected callbacks so the dispatcher itself stays pure and testable without a real storage layer. This is the Sans-IO pattern: decision logic as a pure function over (session, request) → response, with all effects pushed to the boundary. It is the single most FP-principled component in the system, and the correct template for future subsystems. Because the validation and routing logic is pure, it can be property-tested exhaustively — every permission matrix entry, every malformed request shape — without touching disk.

### 2.2 Conflict-as-value: the merge path

The merge winner-selection function returns a result whose *error channel carries a domain value* — an unresolved `MergeConflict` — rather than a failure. This is the deeper idea underneath `Result`: errors as data. An unresolved conflict is not an exception; it is a legitimate output of the merge algebra that flows down a different track to a different owner. The type itself encodes the layer-separation principle: the database detects, the application resolves.

`MergeResult { applied, conflicts }` extends this: conflicts travel through the *success* channel as structured payload. Merge is, today, the most honestly typed error path in the system. The work described in this document is largely about making the rest of the system as honest as merge already is.

### 2.3 Strategy as higher-order function

The interactive merge resolver is injected as an optional function value (`MergeConflict → Record`), not dispatched through an enum. Strategy-as-function keeps the merge engine closed for modification and open for extension, and keeps resolution policy out of the storage layer entirely.

### 2.4 Structured domain errors where they were chosen deliberately

- **Signal validation** errors are an algebraic data type whose variants carry the diagnosis itself: dimension mismatches include expected and actual counts; invalid constraint ranges include the offending min/max.
- **Kind catalog** errors carry the label type and the offending label.

These are sum types doing real work. The error *is* the explanation, and the caller can match on it.

---

## 3. Where the Design Leaks

### 3.1 Stringly-typed error channels

Branch creation and overlay import return `Result<_, String>`. A string error is the FP equivalent of an untyped pointer: it satisfies the type checker while carrying no algebra. The caller cannot distinguish "branch name already taken" (recoverable — retry with a suffix) from "overlay store corrupted" (fatal — halt and alert). The only available moves are propagate or log.

**Principle violated:** errors must form a sum type the caller can match on. A closed enum of failure modes is a *contract*; a string is an apology.

### 3.2 Lossy morphisms at layer boundaries

The pervasive conversion pattern is: structured error → `io::Error` with kind `Other` wrapping a stringified payload → `ApiError::Internal(String)` at the API surface. Each layer possesses well-typed error objects, but the *conversions between layers are lossy* — they collapse the entire error algebra of the lower layer into a single terminal object. Twenty distinct engine failure modes all arrive at the client as one opaque `Internal` variant.

In categorical terms: the error objects are fine; the morphisms are not. The conversions should be total and information-preserving until the final serialization boundary, where a *single, deliberate* projection decides what the client is permitted to see.

### 3.3 Swallowed effects on durability paths

Conflict-queue removal discards the result of its persistence call. A conflict is removed from memory, the persist silently fails, the process restarts, and the resolved conflict resurrects. In a system whose identity is "the sealed block store is the source of truth," a silently failed persist is a truth violation — and it occurs on a *conflict resolution* path, precisely where correctness stakes are highest.

**Principle violated:** an effect whose failure is absorbed without decision is not error handling; it is error denial. Every discarded `Result` must be either propagated, recovered with explicit policy, or accompanied by a written justification of why failure is genuinely ignorable.

### 3.4 Sentinel values where the type system should speak

The cached Hilbert key uses an `UNSET` sentinel rather than an `Option` or a typestate. Sentinels reintroduce null semantics: every consumer must *remember* to check, and the compiler will not remind them. The functional encoding makes the unset state unrepresentable in contexts that require a computed key.

---

## 4. On Higher-Kinded Types

Rust deliberately lacks HKTs, and InfiniteDB should not mourn them. Generic associated types (GATs), the `Try` machinery behind `?`, and per-container traits cover the practical territory. HKT-emulation via defunctionalization exists but fights the language.

The deeper point: **a database does not need abstraction over effect containers; it needs a coherent error algebra.** HKTs let library authors write code generic over "any monad" — valuable for parser-combinator libraries, nearly useless for a storage engine where the effect is always "IO, possibly failing, with these specific failure modes." What InfiniteDB needs from FP is the substrate HKTs sit on top of: sum types with total matching, lossless conversions, effects at the boundary, and the right *applicative vs. monadic* choice per subsystem (Section 6). Rust provides all of it.

---

## 5. Target Architecture: A Layered Error Hierarchy

### 5.1 Layer-indexed error types

Define one closed error enum per architectural layer, with embedding (not erasure) between adjacent layers:

| Layer | Error domain | Owns |
|---|---|---|
| **Storage** | segment/block IO, corruption, fsync failure, WAL frame errors | physical durability |
| **Engine** | embeds Storage; adds revision/watermark violations, space and branch resolution, merge mechanics, overlay errors | logical consistency |
| **API** | *projection* of Engine plus authorization and request-validation failures | the client contract |
| **Application** | receives conflict values and staleness signals through the Ok channel | semantic resolution |

Rules:

1. **Embedding, not stringification.** Each layer's error type contains the lower layer's as a variant. Conversion upward is total and lossless.
2. **One projection point.** Information loss is permitted exactly once — at wire serialization — and that projection is an explicit, reviewed function, not an accumulation of ad-hoc `to_string` calls.
3. **Conflicts are not errors.** Anything the application layer is expected to *resolve* (merge conflicts, staleness reports, collision evaluations at Intent Checkpoint boundaries) travels in the success channel as structured data. The error channel is reserved for things the caller cannot resolve, only react to.

### 5.2 Recoverability as a type-level distinction

Within each layer's enum, partition variants by recoverability semantics:

- **Retryable** — transient IO, queue backpressure, lock contention. Carries enough context for backoff policy.
- **Caller-correctable** — invalid request, dimension mismatch, unknown kind. Carries the diagnosis.
- **Fatal** — corruption, invariant violation, truth-store inconsistency. Triggers halt-and-alert, never silent absorption.

The outbox/sync layer already distinguishes Ack / Retry / ConflictStale at the protocol level — this taxonomy extends that same discipline inward to the engine.

### 5.3 Effect-boundary inventory

Catalog every point where a `Result` is currently discarded. Each becomes one of:

- **Propagate** — the default.
- **Recover with policy** — explicit, named fallback behavior.
- **Justified ignore** — documented reasoning for why failure is harmless (rare; durability paths never qualify).

The conflict-queue persist and the API-handler conflict removal are the first two entries on this inventory, both classed as must-propagate.

---

## 6. Bulk Import: Applicative, Not Monadic, Validation

### 6.1 The semantics mismatch

`Result` with `?` is *monadic*: fail-fast, sequential, the first error aborts the computation. For a 4 GB IFC translation producing millions of hyperedges, fail-fast is the wrong semantics. The operator wants the entire file processed and *every* validation failure — every dangling endpoint reference, every dimension mismatch, every unknown kind under a reject policy — accumulated into one comprehensive report. One run, full diagnosis, fix, re-run.

The applicative alternative (the `Validated` pattern): errors form a **semigroup** and combine instead of short-circuiting. Independent validations proceed independently; their failures append.

### 6.2 Why this composes with parallelism

Monadic sequencing imposes data dependence — each step requires the previous step's success — which serializes the pipeline. Applicative validation has no such dependence: it is embarrassingly parallel, which is exactly the shape the parallel hyperedge import pipeline needs.

The design question is the **semigroup operation on import errors**. The proposed answer: a per-shard structured error log, with shard logs merged at the join point. Log-append is associative with an empty-log identity — a **monoid** — so the parallel fold is lawful regardless of shard count, work-stealing order, or join topology. Determinism of the final report follows from ordering the merged log by (source location, error class), not by arrival time.

### 6.3 Error budget and partial admission

Accumulation raises a policy question monadic handling never has to ask: when do you *stop* accumulating? Proposed: an **error budget** per import session (absolute count or rate threshold). Under budget → import completes, valid records admitted, report attached. Over budget → the input is presumed structurally broken; abort the session, admit nothing, surface the truncated report. The budget is application-layer configuration, consistent with layer-separation discipline: the engine accumulates and counts; the application decides tolerance.

### 6.4 Atomicity interaction

Partial admission must respect Intent Checkpoint semantics. An import session is one logical operation; collision detection operates at its boundary. Valid records admitted under an error budget belong to one intent, so a later decision to roll back the import is a single branch-level operation, not a record-by-record hunt. The accumulated error report should reference the intent's revision range — which connects directly to Section 7.

---

## 7. Errors as Records: Provenance Through the Database's Own Primitives

InfiniteDB is an MVCC system in which nothing is truly lost: every write is a revision, deletes are tombstones, the WAL is transient scaffolding over an immutable truth store. The unconventional proposal: **errors should follow the same philosophy.**

A failed or partially failed operation writes an *error record* — a first-class entity in a dedicated error space — keyed by the revision range in which it occurred and carrying the structured error as payload. Consequences:

- "What went wrong during the import at revision R" becomes a **query**, not a log-grep.
- Error provenance inherits MVCC semantics for free: as-of queries over failures, branch-scoped failure visibility, sync of error history alongside data.
- The planned **staleness query by `source_revision`** is adjacent machinery: staleness and failure provenance become two views through the same query surface. The database's own primitives become its observability layer.
- Error records are ordinary records: they replicate, they merge, they tombstone. A resolved error is tombstoned, not deleted — preserving the audit trail that offline-first multi-user collaboration eventually demands.

Boundary discipline: error records capture *operation-level* failures (import sessions, merge attempts, sync rounds) — not per-IO retries, which would flood the space. The granularity matches Intent Checkpoint boundaries, consistent with the established principle that collision detection operates at logical operation boundaries.

---

## 8. Typestate: Making Invalid Transitions Uncompilable

The strongest form of error handling is making the error unrepresentable.

### 8.1 Block lifecycle

Blocks have a one-way lifecycle: hot → sealing → sealed-immutable. Today the invariant lives in runtime discipline. The typestate encoding gives hot and sealed blocks *distinct types*: mutation operations exist only on the hot type, and the seal operation **consumes** the hot block, yielding the sealed one. "Writing to a sealed block" stops being an error to detect and becomes a program that does not compile. This also mechanically documents the immutability guarantee the entire MVCC and sync design depends on.

### 8.2 Intent Checkpoint lifecycle

The same construction applies to the WAL design's separation of durability from commit intent: an intent that has not been durably acknowledged is a different type from one that has. Committing an unacknowledged intent is then uncompilable, and the timed fast path (direct seal attempt, WAL fallback on timeout) becomes a typed state machine whose illegal transitions are rejected by the compiler rather than caught in review.

### 8.3 Relationship to the parallelism refactor

Typestate encodings are `Send`/`Sync`-friendly: consuming transitions are naturally race-free because the old state ceases to exist at the type level. As the interior-mutability milestone proceeds, lifecycle typestates reduce the surface area the refactor must defend with locks.

---

## 9. Principles Summary

1. **Errors form closed sum types per layer; strings are never an error type.**
2. **Layer conversions are lossless embeddings; information is dropped exactly once, at the wire, by one reviewed projection.**
3. **Anything the application is expected to resolve travels in the success channel as structured data; the error channel is for what the caller can only react to.**
4. **Every discarded Result on a durability path is a defect.** The effect-boundary inventory enumerates and classifies all discards.
5. **Bulk validation is applicative: errors accumulate through a monoid, enabling lawful parallel folds and full-file diagnosis in one pass.**
6. **Failure provenance is data: operation-level errors are records in an error space, queryable through the same MVCC machinery as everything else.**
7. **Lifecycle invariants are typestates: sealed-block immutability and intent acknowledgment are compile-time facts, not runtime checks.**
8. **No HKTs required.** The error architecture rests on sum types, total conversions, and boundary-isolated effects — all native to Rust.

---

## 10. Open Questions

- **Projection policy at the API surface.** How much engine-error structure should clients see? Diagnostic richness aids debugging; opacity aids security and forward compatibility. The single-projection-point design makes this a one-place decision, but the policy itself is unresolved.
- **Error space topology.** One global error space, or per-data-space error spaces? Per-space aligns with the multi-space architecture and sharded import; global simplifies "show me everything that failed since revision R."
- **Error record schema versioning.** Error payloads will evolve faster than data schemas. The kind-catalog mechanism (with its unknown-label policy) may be the right governance model for error kinds as well.
- **Budget semantics under parallel import.** An absolute error count is racy across shards; a per-shard budget changes meaning with shard count. A rate-based budget (errors per N records) is shard-count-invariant and likely the right primitive.
- **Interaction with WAL deletion criteria.** Error records referencing a revision range may constitute a fourth condition (alongside branch merge, replication confirmation, and collision evaluation) gating WAL cleanup — or may deliberately not, if the sealed error record fully supersedes the WAL's diagnostic value.
