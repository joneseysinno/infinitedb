# SERVER_REQUEST_EXECUTOR_PLAN

**Goal:** Stop the TCP server from running blocking database work on tokio worker threads.
Give request handling the same treatment writes already get: a bounded channel feeding a
dedicated pool of OS threads. After this change, tokio threads do **socket I/O and `await`
only**; every call into `handle_request` (which does blocking block-reads, lock acquisition,
and bounded-queue backpressure) runs on a non-runtime thread.

**Thesis (the symmetry that justifies this design):** the engine already offloads *writes*
via `WriteQueueSender` → `crossbeam_channel::bounded` → `infinitedb-io-{space}` threads
(`src/engine/write_queue.rs`, `src/engine/space_io.rs`). Reads are the unoffloaded half:
`query_inner` runs on the calling thread. This plan does for *requests* what the write
coordinator does for *write jobs*. It is the read-side mirror of an architecture already in
the tree and already trusted.

---

## 1. Scope

**In scope**
- New `RequestExecutor`: bounded job channel + worker thread pool, one `handle_request` call per job.
- Rewire `serve_connection` to submit jobs and `await` a `oneshot` reply instead of calling
  `handle_request` inline.
- `ServerConfig` knobs for pool size and queue capacity.
- Backpressure policy decision (queue-full handling).
- Tests proving non-blocking submit, per-connection ordering, and no head-of-line blocking
  across connections.
- One documented engine-contract invariant (blocking API must be offloaded from async).

**Explicitly NOT in scope** (sequenced as separate plans — do not absorb here):
- **D-session-ceiling** — the frozen `session.opened_at` read ceiling (standing connections
  read stale). Separate plan.
- **D-ack-semantics** — `WriteAck` returning at enqueue, before durability/visibility, and the
  absence of an auto-`sync()` on the server path. Separate plan.
- **Engine write-queue backpressure** — the `enqueue` "blocks only when full" deviation lives
  *inside* `handle_request` and is unchanged by this work; it still wants its own decision
  record. This plan offloads it onto a worker thread (so it no longer blocks a tokio thread)
  but does not change its semantics.
- Auth and transport encryption (known production residuals).

Keeping these out is deliberate: this change is a pure transport/threading refactor with no
intended change to read or write semantics. That property is what makes it safe and testable.

---

## 2. Decision gates (resolve before/at implementation)

Surface these answers in the PR description and write the chosen ones into `SEMANTICS.md`
(Task T0). Do not let them drift.

- **D-EXEC-1 — Reply transport.** Return leg uses `tokio::sync::oneshot` (async-friendly; the
  awaiting connection task yields its worker thread while the pool thread works). *Recommended:
  yes.* Rationale: a crossbeam reply would re-block the tokio thread on `recv()`, defeating the
  entire purpose. `tokio` is already a hard dep under the `server` feature.

- **D-EXEC-2 — Pool ownership of `db`.** The executor holds `Arc<InfiniteDb>` once at
  construction; jobs carry only `Request` + `Arc<Session>` + reply sender. *Recommended: yes.*
  Mirrors how `SpaceIoHandle` captures its `Arc` state at spawn.

- **D-EXEC-3 — Pool size default.** `executor_threads` defaults to
  `std::thread::available_parallelism().map(NonZero::get).unwrap_or(4)`. No `num_cpus` dep.
  *Confirm the default; it is the steady-state read concurrency ceiling.*

- **D-EXEC-4 — Queue-full policy (the real backpressure decision).** With strict per-connection
  serialization (Task T3 keeps the read→submit→await→write loop sequential), each connection has
  **at most one** outstanding job. Therefore if `request_queue_capacity >= max_connections`, the
  job channel can *structurally never* be full, and queue-full shedding is dead code.
  Two coherent options:
  - **(4a) Never shed (recommended).** Set `request_queue_capacity = max_connections`. A full
    pool simply queues; the connection `await`s; nothing blocks a tokio thread. Latency rises
    under load but no requests are rejected. Simplest, and correct-by-construction.
  - **(4b) Shed to `ApiError::Busy`.** Use a smaller capacity as an explicit in-flight cap;
    `try_send` failure → `Response::Error(ApiError::Busy { retry_hint_ms })`. This is a *client-
    visible semantic change* (clients must handle `Busy`). Only choose this if you want tail-
    latency protection now.
  - **Decision required.** Recommendation: ship **4a**, leave the `Busy` path implemented but
    unreachable-by-default behind capacity, so 4b is a one-line config change later. Note that
    `ApiError::Busy { retry_hint_ms }` and `project_api_error`'s `DerivationBackpressure → Busy`
    mapping already exist (`src/infinitedb_server/api.rs`), so the wire surface is ready either way.

- **D-EXEC-5 — Per-connection ordering.** Invariant: responses on a single connection are
  returned in request order. Preserved for free because `serve_connection` awaits each reply
  before reading the next frame (no pipelining introduced). Cross-connection ordering was never
  guaranteed and still isn't. *Confirm we are NOT adding pipelining in this plan* (it would break
  ordering and expand scope).

- **D-EXEC-6 — Shutdown.** Executor shuts down by dropping all job senders (workers observe
  channel disconnect and exit) then joining worker handles, mirroring
  `SpaceIoHandle::join` / `HilbertCoordinator::shutdown_all`. Decide where shutdown is triggered:
  on `Server` drop and/or an explicit `Server::shutdown`. *Recommended: implement `Drop` on the
  executor handle; `Server::run` currently loops forever, so `Drop` is the reliable trigger.*

---

## 3. Decision records to write (Task T0 → `SEMANTICS.md`)

Append these so the choices are durable, in the established D-record style:

1. **D-EXEC — Server request execution.** The TCP server MUST NOT execute `handle_request` on a
   tokio runtime thread. Requests are dispatched to a dedicated OS-thread pool
   (`RequestExecutor`) over a bounded channel; replies return via `oneshot`. Tokio threads
   perform socket framing and `await` only. This is the read/request-side mirror of the write
   coordinator (`WriteQueueSender` → `space_io` threads).

2. **D-BLOCKING-CONTRACT — Engine is a blocking API.** `InfiniteDb` methods
   (`query_on_branch*`, `insert_on_branch`, `sync`, `flush`, …) are synchronous and may block on
   disk I/O, `parking_lot` locks, and bounded-queue backpressure. Any async caller MUST offload
   them (dedicated executor or `spawn_blocking`) and MUST NOT call them directly from an async
   task. The `server` feature's `RequestExecutor` is the reference implementation of this
   contract; embedded callers that run their own runtime owe the same discipline.

3. **D-EXEC-BACKPRESSURE — Queue-full policy.** Record the 4a/4b choice from D-EXEC-4 and the
   `request_queue_capacity` / `max_connections` relationship that makes queue-full structurally
   impossible (4a) or an explicit in-flight cap (4b). Cross-reference the still-open engine
   write-queue backpressure record (the `enqueue`-blocks-when-full deviation), which this plan
   relocates off the tokio threads but does not resolve.

---

## 4. Architecture sketch

New module `src/infinitedb_server/executor.rs`. Shapes (signatures indicative; let Cursor
finalize types against the tree):

```rust
// Job carried over the bounded channel to a worker thread.
struct RequestJob {
    request: Request,
    session: Arc<Session>,
    reply: tokio::sync::oneshot::Sender<Response>,
}

pub struct RequestExecutor {
    jobs: crossbeam_channel::Sender<RequestJob>,
    workers: Vec<std::thread::JoinHandle<()>>, // joined on shutdown
}

pub enum SubmitError {
    Busy,        // queue full (only reachable under D-EXEC-4b)
    Stopped,     // pool gone / worker dropped reply
}

impl RequestExecutor {
    pub fn start(db: Arc<InfiniteDb>, threads: usize, capacity: usize) -> Self {
        let (tx, rx) = crossbeam_channel::bounded::<RequestJob>(capacity);
        let mut workers = Vec::with_capacity(threads);
        for i in 0..threads {
            let rx = rx.clone();
            let db = Arc::clone(&db);
            workers.push(
                std::thread::Builder::new()
                    .name(format!("infinitedb-req-{i}"))
                    .spawn(move || {
                        // Disconnect (all senders dropped) ends the loop → clean exit.
                        while let Ok(job) = rx.recv() {
                            let resp = handle_request(&db, &job.session, job.request);
                            let _ = job.reply.send(resp); // ignore if client vanished
                        }
                    })
                    .expect("spawn request worker"),
            );
        }
        Self { jobs: tx, workers }
    }

    /// Async submit: yields the tokio worker while the pool thread runs handle_request.
    pub async fn submit(
        &self,
        request: Request,
        session: Arc<Session>,
    ) -> Result<Response, SubmitError> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.jobs
            .try_send(RequestJob { request, session, reply: reply_tx })
            .map_err(|_| SubmitError::Busy)?;           // D-EXEC-4: try_send, never blocks
        reply_rx.await.map_err(|_| SubmitError::Stopped)
    }
}

impl Drop for RequestExecutor {
    fn drop(&mut self) {
        // Drop the sender so workers see disconnect, then join.
        // (Take/replace jobs sender or wrap in Option to drop before join.)
        for w in self.workers.drain(..) { let _ = w.join(); }
    }
}
```

Notes for Cursor:
- `handle_request(db: &InfiniteDb, session: &Session, request: Request) -> Response` is unchanged
  — it is already `Send`-compatible (all inputs are plain data / `Arc`). Do **not** modify its
  signature.
- `Session` must be shared as `Arc<Session>`; confirm `Session: Send + Sync` (it holds
  `BranchId`, `SnapshotId`, `RevisionId`, `Vec<SpaceGrant>` — all `Send + Sync`). If a `derive`
  is missing nothing is needed; `Arc` provides sharing.
- For `Drop` ordering, store the job sender as `Option<Sender<_>>` (or split a `shutdown()` that
  drops it) so it is dropped **before** `join`, otherwise workers never see disconnect and
  `join` hangs. Mirror the `WriteQueueSender::shutdown()` pattern.

---

## 5. Effect-boundary inventory

Everything this change touches. Nothing outside this list should be modified.

| File | Symbol | Change |
|------|--------|--------|
| `src/infinitedb_server/executor.rs` | *(new)* `RequestExecutor`, `RequestJob`, `SubmitError` | new module |
| `src/infinitedb_server.rs` | module list | add `pub mod executor;` |
| `src/infinitedb_server/runtime.rs` | `Server` struct | add `executor: Arc<RequestExecutor>` field |
| `src/infinitedb_server/runtime.rs` | `Server::bind` | construct executor from `db` + config |
| `src/infinitedb_server/runtime.rs` | `Server::run` | clone `Arc<RequestExecutor>` into each connection task |
| `src/infinitedb_server/runtime.rs` | `serve_connection` | take `Arc<RequestExecutor>` + `Arc<Session>`; replace inline `handle_request` with `executor.submit(...).await` |
| `src/infinitedb_server/runtime.rs` | `ServerConfig` | add `executor_threads: usize`, `request_queue_capacity: usize`; update `Default` |
| `src/infinitedb_server/api.rs` | `ApiError::Busy`, `project_api_error` | reuse only (no change) for the 4b path |
| `SEMANTICS.md` | — | append D-EXEC, D-BLOCKING-CONTRACT, D-EXEC-BACKPRESSURE |
| `src/lib.rs` | server re-exports | export `RequestExecutor` only if a public API is wanted (optional) |
| `tests/server_phase_d.rs` | existing tests | must stay green unchanged |
| `tests/server_executor.rs` | *(new)* | executor unit + concurrency tests (Task T6) |

**Invariant the inventory enforces:** no file under `src/engine/`, `src/concurrent/`, or
`src/infinitedb_storage/` is touched. This is a transport-layer change only. If a task wants to
edit the engine, the task is wrong — stop and re-scope.

---

## 6. Tasks (dependency-ordered)

### T0 — Decision records
- **Problem:** choices in §2 will silently drift if not pinned.
- **Fix:** append the three records in §3 to `SEMANTICS.md`. Record the D-EXEC-4 (4a vs 4b) and
  D-EXEC-3 (thread default) choices explicitly with their numeric values.
- **Tests:** none (doc). Gate: PR description restates the chosen values.

### T1 — `RequestExecutor` module
- **Problem:** no off-runtime execution path for requests exists.
- **Fix:** add `src/infinitedb_server/executor.rs` per §4. Register in `src/infinitedb_server.rs`.
  Implement `start`, `submit`, graceful `Drop`/`shutdown` (sender-drop-then-join). Feature-gate
  under `server` (the module is only compiled with the `server` feature, same as `runtime`).
- **Tests (unit, in-module or `tests/server_executor.rs`):**
  - `submit_returns_handler_response`: start a 2-thread executor over a real temp `InfiniteDb`;
    submit `Request::Ping`; assert `Response::Pong`.
  - `submit_does_not_block_caller_thread` (the crux, deterministic — see §7).
  - `shutdown_joins_cleanly`: drop the executor; assert no hang (wrap in a watchdog thread with a
    timeout so a regression *fails* instead of hanging CI).

### T2 — Wire executor into `Server`
- **Problem:** `Server` has no executor.
- **Fix:** add `executor: Arc<RequestExecutor>` to `Server`; construct it in `Server::bind` using
  `config.executor_threads` and `config.request_queue_capacity` and `Arc::clone(&db)`. In
  `Server::run`, `Arc::clone` the executor into each `tokio::spawn`ed connection task alongside
  the existing `db`/`grants`/`branch`.
- **Tests:** existing `tcp_roundtrip_ping`, `tcp_query_write_roundtrip` in `tests/server_phase_d.rs`
  still pass (they exercise the new path transparently).

### T3 — Rewrite `serve_connection`
- **Problem:** `let response = handle_request(&db, &session, request);` runs on the tokio thread.
- **Fix:** build the session once as `Arc<Session>` (as today, but wrapped). In the loop:
  ```rust
  let request: Request = read_frame_async(&mut stream).await?;
  let response = match executor.submit(request, Arc::clone(&session)).await {
      Ok(r) => r,
      Err(SubmitError::Busy) =>
          Response::Error(ApiError::Busy { retry_hint_ms: BUSY_RETRY_MS }), // D-EXEC-4b only
      Err(SubmitError::Stopped) =>
          Response::Error(ApiError::Internal("request executor stopped".into())),
  };
  write_frame_async(&mut stream, &response).await?;
  ```
  Keep the loop strictly sequential (read → submit → await → write). **Do not** read the next
  frame before writing the current response — that preserves D-EXEC-5 ordering.
- **Tests:** `tcp_query_write_roundtrip` green; add `per_connection_request_order` (§7).

### T4 — `ServerConfig` knobs
- **Problem:** pool size / capacity not configurable.
- **Fix:** add `executor_threads: usize` and `request_queue_capacity: usize` to `ServerConfig`;
  set defaults in `Default` per D-EXEC-3 and D-EXEC-4 (`request_queue_capacity = max_connections`
  for 4a). Keep `Debug, Clone`.
- **Tests:** `server_config_defaults` asserts `request_queue_capacity >= max_connections`
  (the structural no-shed guarantee for 4a).

### T5 — Backpressure path (only if D-EXEC-4b chosen)
- **Problem:** under 4b, queue-full must shed, not block.
- **Fix:** `submit` already returns `SubmitError::Busy` via `try_send`; map to `ApiError::Busy`
  in `serve_connection` (done in T3). Pick `BUSY_RETRY_MS` const; reuse `project_api_error`
  semantics for consistency with `DerivationBackpressure`.
- **Tests:** `queue_full_sheds_to_busy` — start a 1-thread executor with `capacity = 1`, occupy
  the worker with a barrier-blocked job, fill the queue, assert the next `submit` returns
  `Busy` (not a hang). *Skip this task entirely under 4a.*

### T6 — Concurrency / non-starvation tests
- **Problem:** the whole point — blocking work must not stall the runtime — needs a regression guard.
- **Fix:** add `tests/server_executor.rs` with the tests in §7.
- **Tests:** see §7 (this task *is* the tests).

### T7 — Docs / contract
- **Problem:** the blocking-API contract is undocumented; the next async caller repeats the bug.
- **Fix:** add the D-BLOCKING-CONTRACT note to the `runtime` module docs and to the `InfiniteDb`
  rustdoc (one line: "blocking API; offload from async tasks"). Optionally re-export
  `RequestExecutor` from `src/lib.rs` under `cfg(feature = "server")`.
- **Tests:** `cargo doc --features sync` builds clean.

### T8 — Full verification
- **Problem:** ensure no regression across feature matrix.
- **Fix / checklist:**
  - `cargo test --features sync` (server + sync tests).
  - `cargo test` (default `embedded` — confirms embedded path untouched and still compiles
    without tokio).
  - `cargo clippy --features sync -- -D warnings`.
  - `cargo doc --features sync`.
  - Confirm effect-boundary inventory respected (no engine/storage diffs).

---

## 7. Invariants & property tests

The hard guarantees this plan must lock in:

**INV-1 — Submit never blocks the caller thread.**
Deterministic unit test, no timing assertions:
- Make the executor testable with a handler that can block on a shared `std::sync::Barrier`.
  Cleanest approach that avoids a test-only `Request` variant: add a *crate-private*
  `RequestExecutor::start_with_handler(handler, threads, capacity)` where the default `start`
  passes the real `handle_request`. Tests inject a closure that parks on a `Barrier`.
- Test: 2 worker threads. Submit job A whose handler waits on the barrier. From the same async
  context, submit job B (a no-op handler) and assert B completes *before* releasing the barrier.
  Proves the pool runs jobs concurrently and `submit` did not serialize on A. No wall-clock
  thresholds → not flaky.

**INV-2 — No head-of-line blocking across connections.**
- Run the server on a **single-worker** tokio runtime
  (`tokio::runtime::Builder::new_multi_thread().worker_threads(1)`).
- Connection A submits a request whose handler is barrier-blocked (via the injected handler from
  INV-1, or a genuinely slow real query for an integration-flavored variant).
- While A is in flight, connection B sends `Ping` and must receive `Pong`.
- Under the OLD inline architecture on one worker thread, B cannot even be accepted/serviced until
  A returns (the accept loop and A share the one runtime thread). Under the executor, B's `Pong`
  returns while A is still blocked. Assert B completes, then release A.
- This is the regression test that would have caught the original bug.

**INV-3 — Per-connection ordering (D-EXEC-5).**
- On one connection, submit a sequence `[Write k0, Query, Write k1, Query]` and assert responses
  arrive in request order and the second `Query` reflects state consistent with requests issued
  before it on that connection. Holds because `serve_connection` awaits each reply before the
  next read. (Note: cross-connection visibility is governed by the separate D-session-ceiling /
  D-ack-semantics plans; do not assert cross-connection visibility here.)

**INV-4 — Clean shutdown.**
- Construct and drop a `RequestExecutor`; a watchdog thread fails the test if `Drop` does not
  return within a timeout. Guards against the sender-drop-before-join ordering bug.

**INV-5 — Embedded build unaffected.**
- `cargo test` with default features (no `server`, no tokio) compiles and passes. The executor
  module is `cfg(feature = "server")` and must not leak into the embedded build.

---

## 8. Why this is safe

- Pure transport refactor: `handle_request`, the engine, and all read/write semantics are
  byte-for-byte unchanged. The only difference is *which thread* runs `handle_request`.
- The exact channel→thread-pool→reply shape is already in production for writes
  (`WriteQueueSender` / `space_io`), so the concurrency model is not novel to the codebase.
- Per-connection serialization is retained, so no new ordering or visibility behavior is
  introduced — the two genuinely semantic questions (stale read ceiling, ack timing) are
  quarantined into their own plans and explicitly out of scope here.
- The change is reversible: deleting the executor and restoring the inline
  `handle_request` call returns the previous behavior with no schema or wire-format impact.

---

## 9. Open follow-ups (track, do not implement here)

1. **D-session-ceiling plan** — advance the per-connection read ceiling so standing connections
   observe later writes (currently frozen at `serve_connection`'s `opened_at`).
2. **D-ack-semantics plan** — define whether `WriteAck` means enqueued / durable / visible, and
   whether the server auto-`sync()`s.
3. **Engine write-queue backpressure record** — the `enqueue`-blocks-when-full deviation, now
   relocated off the tokio threads but still semantically unresolved.
4. **Read-path lock-hold duration** — `query_on_branch_pinned` holds the `spaces` read lock
   across block I/O; worth revisiting once requests run in parallel on the pool (more readers
   holding the lock concurrently across longer I/O windows).
