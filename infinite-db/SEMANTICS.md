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
