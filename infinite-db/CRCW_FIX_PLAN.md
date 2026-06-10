# InfiniteDB v0.3.x — CRCW Correctness & Performance Plan

> Drop this file into the repo root (e.g. `CRCW_FIX_PLAN.md`) and use it as the
> working brief in Cursor. Each task is self-contained: it names the file(s) to
> touch, states the problem precisely, specifies the fix in behavioral terms,
> and gives an acceptance test. Tasks are ordered by dependency — Phase 1 before
> Phase 2 before Phase 3. Within a phase, follow the listed order.

---

## 0. Context for the agent

InfiniteDB 0.3.0 is a CRCW (concurrent reads, fire-and-forget writes) embedded
spatial database. The write path is:

- `InfiniteDb::insert` allocates a `RevisionId` from a global `AtomicU64`,
  builds a `WriteJob` (a `WalEntry` plus a `Record`, payload duplicated in
  both), and enqueues it.
- Format v4 (`HilbertCoordinator`) routes the job by Hilbert shard
  (`shard_for_point`) to a per-shard I/O thread over a bounded
  crossbeam channel (`WriteQueueSender`).
- The shard I/O thread (`space_io.rs::run_space_io_loop`) appends the entry to
  an append-only `HotSegment` (direct path, per-record fsync with a 2 ms
  deadline) or to a `StagingWal` (per-record fsync), then appends the `Record`
  to the shard's `LiveTailView` (an `ArcSwap<Vec<Record>>`).
- At `hot_segment_seal_threshold` (256 records) the hot segment is re-read from
  disk, sorted by Hilbert key, sealed into an immutable `Block`, registered in
  the per-space `Snapshot` via `SnapshotStore::update`, and the sealed records
  are removed from the live tail.
- Reads (`engine/query.rs::query_inner`) assemble results from sealed blocks
  (pruned by Hilbert key interval) plus the live tail(s), filtered by a
  revision ceiling (`as_of`).

**Ground rules:**
- Keep `cargo test --all-features` green after every task.
- Match existing style: doc comments on every public item, `io::Result` for
  fallible storage ops, `bincode` Encode/Decode on persisted types.
- Prefer new modules over growing existing files.
- Public API changes are allowed but must be noted in `CHANGELOG.md` under
  `Unreleased` as you go.
- Formats v2 and v3 share most of this code; every fix to
  `engine/io_thread.rs` must be mirrored in `engine/space_io.rs` (and vice
  versa) unless the task says otherwise. Where the two loops have drifted,
  factor the shared logic into a common helper module instead of duplicating
  the fix.

---

## PHASE 1 — CORRECTNESS

### 1.1 — Latest-revision visibility resolution (fixes tombstone resurrection)

**Files:** `src/engine/query.rs` (`query_inner`)

**Problem.** Visibility is decided by a coordinate-keyed tombstone set:
`query_inner` collects every tombstone with `revision <= rev_ceiling` into a
`HashSet<Vec<u32>>` and then strips every record whose coordinates appear in
the set, regardless of revision ordering. Consequence: insert at P (rev 5),
delete (rev 6), re-insert (rev 7) — the rev-7 record is hidden forever. A
deleted address can never be visibly rewritten. Separately, the query returns
*every* revision of an address (no latest-wins resolution), so callers receive
historical duplicates.

**Fix.** Replace the tombstone-set mechanism with per-address latest-wins
resolution:

1. Collect all candidate records (blocks + tail) with `revision <= rev_ceiling`
   into a working set, exactly as today, but do **not** maintain or apply the
   `tombstoned` set.
2. Group by address coordinates. For each address, identify the record with
   the maximum revision at or below the ceiling.
3. Default behavior (`include_tombstones == false`): emit the
   latest record per address if and only if it is not a tombstone. If the
   latest record is a tombstone, emit nothing for that address — including no
   older live revisions.
4. `include_tombstones == true`: preserve current raw behavior (all
   revisions, tombstones included) so merge/sync/diagnostic callers that need
   full history are unaffected.
5. Add a doc comment on `query_inner` stating the visibility rule explicitly:
   "an address is visible iff its highest revision ≤ the ceiling is a live
   record; that record is the one returned."

Note the behavioral change: queries previously returned multiple revisions per
address; after this task they return exactly one. Audit call sites
(`replicate.rs::latest_per_address` becomes largely redundant — leave it, it's
now a harmless no-op pass) and update any test that asserted multi-revision
results.

**Acceptance.**
- New test `tombstone_resurrection`: insert P, sync, delete P, sync, insert P
  with new data, sync; `query` returns exactly one record at P carrying the
  newest data. Repeat with the delete and re-insert split across a seal
  boundary (force `flush` between them) to cover block/tail interaction.
- New test `latest_wins`: three inserts at the same P with increasing
  revisions; `query` returns one record with the highest revision;
  `query` with `as_of` pinned between rev 1 and rev 2 returns the rev-1 data.
- Existing `include_tombstones` behavior verified unchanged by a regression
  test.

---

### 1.2 — Atomic per-shard read view (fixes the seal duplicate window)

**Files:** new `src/engine/shard_view.rs`; `src/engine/live_tail.rs`,
`src/engine/io_thread.rs`, `src/engine/space_io.rs`, `src/engine/query.rs`

**Problem.** `seal_space` publishes the new block into the snapshot index
*before* truncating the live tail. A reader landing between the two steps sees
sealed records twice — once from the block, once from the tail — and
`query_inner` performs no (address, revision) dedup, so duplicates reach the
caller. Reversing the order would instead drop records in the window. The root
cause is that readers assemble a view from two independently mutating sources.

**Fix.** Introduce a per-shard view object that pairs the two sources and is
swapped atomically:

1. Define a `ShardView` struct holding (a) an `Arc` of the shard's current
   sealed-block index contribution — at minimum the `BTreeMap<u128,
   BlockIndexEntry>` of blocks this shard has sealed — and (b) an
   `Arc<Vec<Record>>` live tail. Hold it in a single `ArcSwap<ShardView>`.
2. The I/O thread is the only writer. On a normal write, it swaps in a new
   `ShardView` with the appended tail (same blocks Arc). On seal, it builds the
   post-seal tail and the post-seal block map and swaps **once**.
3. Readers (`query_inner`) load each relevant shard's view exactly once at the
   start of the query and use only that view's blocks + tail for that shard.
   The per-space `SnapshotStore` remains for persistence, recovery, branch
   bases, and sync — but the *read path* sources block membership from shard
   views so blocks and tails can never disagree.
4. Interim safety net (do this first, in a separate commit, so the invariant
   is protected even before the refactor lands): add an (address, revision)
   dedup pass at the end of `query_inner`. Task 1.1's grouping pass can absorb
   this for free. Keep the dedup even after the view refactor — defense in
   depth is cheap here.

**Acceptance.**
- Stress test: one writer thread inserting continuously at distinct points
  while forcing frequent seals (`hot_segment_seal_threshold = 8`), one reader
  thread querying in a loop; assert no query result ever contains a duplicate
  (address, revision) pair and no query ever returns fewer records than a
  previously observed count at the same `as_of`.
- Unit test: construct a view swap mid-"query" deterministically (seal between
  two reader loads is no longer possible by construction — assert via the API
  that a single loaded view is internally consistent).

---

### 1.3 — Stable-revision watermark (makes ReadTxn honest)

**Files:** new `src/engine/watermark.rs`; `src/concurrent/concurrent_db.rs`,
`src/concurrent/read_txn.rs`, shard I/O loops

**Problem.** `ReadTxn::new` pins `as_of` at `db.revision()`, but revisions are
allocated at enqueue time and applied per shard asynchronously and out of
global order. The ceiling bounds what a reader *cannot* see, not what it
*will* see: rev 12 may be visible while rev 9 is still queued in another
shard, and rev 9 materializes later under the same `as_of` — repeatable read
is violated.

**Fix.** Track the highest revision R such that every write with revision ≤ R
has been durably applied and published:

1. Maintain a concurrent low-water-mark structure over in-flight revisions:
   on enqueue, the revision is registered as outstanding; the I/O thread
   retires it after the record is durable and the live tail is published. The
   stable revision is (lowest outstanding revision − 1), or the global
   revision counter when nothing is outstanding. A sharded mutex-protected
   ordered set, or a ring of per-shard "applied prefix" counters combined with
   an outstanding-set for cross-shard gaps, both work; pick the simpler ordered
   set first and benchmark later.
2. Expose `InfiniteDb::stable_revision()` returning that value.
3. `ReadTxn::new` pins `as_of` at `stable_revision()`, not `revision()`.
   Document on `ReadTxn` that reads are repeatable: the set of visible records
   at the pinned revision can never change.
4. `revision()` keeps its current meaning (allocation high-water mark); add doc
   comments distinguishing the two so callers stop conflating them.
5. Fire-and-forget `insert` still returns the allocated `RevisionId`
   immediately; document that the write is observable once
   `stable_revision() >= returned revision` or after `sync()`.

**Acceptance.**
- Test: spawn writers across multiple shards, open a `ReadTxn`, run the same
  query twice with a forced delay and concurrent writes in between; assert
  identical results.
- Test: `stable_revision()` never exceeds `revision()`, is monotonic, and
  reaches `revision()` after `sync()` with no concurrent writers.

---

### 1.4 — Fix shard-key and overlay-key bit truncation

**Files:** `src/engine/hilbert_shard.rs` (`pack_shard_key`,
`unpack_shard_key`), `src/engine/branch_overlay.rs` (`overlay_key`),
`src/engine/hilbert_live_tails.rs`, `src/concurrent/concurrent_db.rs`
(`register_space`)

**Problem.** `pack_shard_key` shifts a `u64` space id left by 16 — space ids
≥ 2^48 silently collide, and `tails_for_space`'s shift-compare inherits it.
`overlay_key` packs branch and space into one `u64` — space ids ≥ 2^32
collide across branches. The repo's own roadmap reserves
`SpaceId(u64::MAX − 1)` for the endpoint index space, which sits squarely in
the collision zone.

**Fix.** Replace packed `u64` keys with proper composite keys: a
`(u64, u32)` tuple (or small key struct deriving Hash/Eq/Ord) for
`(space_id, shard_id)` in `HilbertLiveTails` and the coordinator's `shards`
DashMap, and `(u64, u64)` for `(branch_id, space_id)` in
`BranchOverlayStore`. Delete `pack_shard_key`/`unpack_shard_key`/`overlay_key`
or keep them only for on-disk paths where the range is validated. As a
belt-and-suspenders measure, `register_space` rejects space ids ≥ 2^48 with a
clear error until/unless every packed-key use is gone.

**Acceptance.** Test registering and writing to a space with id
`u64::MAX − 1` (or, if the validation route is chosen, asserting the explicit
rejection). Test two branches over two spaces whose old packed keys would have
collided; overlays remain isolated.

---

### 1.5 — Branch overlay durability

**Files:** `src/engine/branch_overlay.rs`, `src/concurrent/concurrent_db.rs`
(open/recovery path, `insert_on_branch`, `delete_on_branch`)

**Problem.** Non-main writes live only in `BranchOverlayStore`'s in-memory
maps. A persistence helper exists but is not invoked on the write path. A
crash silently loses all unmerged branch work — unacceptable for the
collaboration/possibility-fork use case, and undocumented either way.

**Fix.** Give each (branch, space) overlay a small append-only log, mirroring
the hot-segment pattern: append the entry (with the branch id in the frame or
implied by the path `spaces/<space>/branches/<branch>/overlay.log`) before
appending to the in-memory tail; group-commit semantics arrive with Task 2.2
and apply here identically. On open, replay overlay logs to rebuild
`BranchOverlayStore`. On `merge_branch` completion and `clear_branch`, delete
the logs. Persist the branch registry alongside (verify `branches.bin` is
already written by `persist_meta` in the concurrent engine; wire it if not).

**Acceptance.** Durability test: create branch, write records on it, drop the
db without merging, reopen; branch exists and `query_on_branch` returns the
records. Merge, reopen; overlay logs are gone and merged data is on main.

---

## PHASE 2 — WRITE PATH (batching & throughput)

### 2.1 — Compute the Hilbert key once and carry it

**Files:** `src/infinitedb_core/block.rs` (`Record`),
`src/engine/write_queue.rs` (`WriteJob`), `src/concurrent/concurrent_db.rs`,
`src/engine/hilbert_coordinator.rs`, seal paths, `src/engine/query.rs`

**Problem.** The Hilbert key for a record is recomputed at shard routing, at
seal sorting, at block min/max derivation (twice, each reacquiring the spaces
read lock), and per tail record per query for range filtering. Skilling's
transform is not free in high dimensions; this is 4–5 redundant transforms per
record over its lifetime, plus repeated lock traffic.

**Fix.** Compute the key once in `insert_on_branch` / `delete_on_branch`
(where the spaces registry is consulted anyway) and carry it on the `WriteJob`
and on the `Record` (a `u128` field; decide whether it is persisted in blocks
— recommended yes, since blocks are sorted by it and seal/min/max/queries all
reuse it; bump the block format note if so). Routing, sorting, min/max, tail
filtering, and dedup grouping (Task 1.1) all read the cached key. Recovery
paths that rebuild records from WAL entries recompute it once at decode.

**Acceptance.** A test asserting block min/max keys match
`space_key`-computed values for the same records (guards against stale-key
drift), plus an instrumentation counter in tests showing one key computation
per insert on the hot path.

---

### 2.2 — Group commit: drain, batch-fsync, batch-publish

**Files:** `src/engine/io_thread.rs`, `src/engine/space_io.rs`,
`src/infinitedb_storage/hot_segment.rs`

**Problem.** Both write routes fsync per record. The direct path additionally
performs a `metadata()` syscall and a committed-length header write per
record; the staged path calls `staging.sync()` after every append. Under load
the 2 ms deadline fails and everything degrades to staging — which also
fsyncs per record. `IoThreadConfig::wal_group_commit_interval` exists and is
never consulted. Sustained ingest is fsync-bound at hundreds to low thousands
of records per second regardless of hardware.

**Fix.** Restructure the I/O loop around greedy draining:

1. On receiving the first `Write`, drain the channel non-blockingly
   (`try_recv`) up to a frame-count and byte budget, or until
   `wal_group_commit_interval` has elapsed since the first frame of the group
   — whichever comes first. `Sync`/`Flush`/`Shutdown` commands encountered
   mid-drain terminate the group (they act as barriers; process them after the
   group commits).
2. Append all frames of the group to the hot segment, then issue **one**
   fsync, then write the committed-length header **once**, tracking length
   arithmetically instead of via `metadata()`.
3. Publish all of the group's records to the live tail in **one** swap (a
   `LiveTailView::extend`-style operation; lands fully with Task 2.5 but the
   batch entry point goes in now).
4. Retire the in-flight revisions of the group in the watermark (Task 1.3)
   after the publish.
5. Failure semantics: if the fsync fails, none of the group's records are
   published or retired; the error is surfaced on the next `sync()` call (add
   an error slot the Sync command reports), and the loop attempts recovery by
   reopening the segment.

**Acceptance.**
- Throughput smoke test (criterion or a plain timed test, behind
  `--ignored`): 100k single-record inserts followed by `sync()` completes
  with fsync count ≪ record count (assert via an injectable fsync counter or
  the existing `direct_writes` stat repurposed as group count).
- Durability test: kill the process (or drop without shutdown) mid-stream;
  reopen; every record whose insert call returned **and** was covered by a
  completed `sync()` is present; no torn frames (checksum validation already
  covers this).
- `Sync` barrier test: records enqueued before `sync()` returns are all
  visible after it returns.

---

### 2.3 — Retire the deadline/staging dual route

**Files:** `src/engine/io_thread.rs`, `src/engine/space_io.rs`,
`src/infinitedb_storage/hot_segment.rs` (`try_append_with_deadline`),
`IoThreadConfig`

**Problem.** The direct-with-deadline / staged-fallback split was designed for
a caller blocking on the write. Under fire-and-forget the bounded queue
already absorbs latency, and the mechanism now actively misbehaves: the
post-fsync deadline check truncates data that is *already durable* (a
durability take-back plus wasted I/O), and the fallback path is just a second
per-record-fsync log. With group commit (2.2) in place the distinction has no
remaining purpose.

**Fix.** Delete the deadline mechanism and the staging WAL from the shard
loops: all writes go through the group-committed hot segment.
`try_append_with_deadline` and `promote_staging` are removed;
`direct_write_timeout` is deprecated in `IoThreadConfig` (keep the field,
ignore it, note in CHANGELOG) or removed outright since 0.3.x is young.
Recovery simplifies to: replay hot segments only. Keep `WriteRoute` only if
the stats surface needs backward compatibility; otherwise remove and fold
`staged_writes` into the stats as a constant zero with a deprecation note.

Do this *after* 2.2 is green so the loop is never without a durable path.

**Acceptance.** Full test suite green; durability tests from 2.2 and 7.x of
the original plan still pass; a reopen test confirms staging-WAL files left by
older runs are still replayed once (write a one-time migration that promotes
any leftover staging entries into the hot segment on open).

---

### 2.4 — Batch write surface end-to-end

**Files:** `src/engine/write_queue.rs` (`IoCommand`),
`src/engine/hilbert_coordinator.rs`, `src/engine/coordinator.rs`,
`src/concurrent/concurrent_db.rs` (public API), `src/engine/io_thread.rs`,
`src/engine/space_io.rs`

**Problem.** There is no public batch insert; `enqueue_batch` on the
coordinators routes by shard but still sends one channel message per job, so
the I/O loop cannot see batch boundaries; each `WriteJob` duplicates the
payload in both the `WalEntry` and the `Record`.

**Fix.**
1. Add `IoCommand::WriteBatch(Vec<WriteJob>)`. Coordinators' `enqueue_batch`
   sends one message per shard containing that shard's slice. The I/O loop
   treats a batch as a pre-formed group (feeds directly into 2.2's
   append-all/fsync-once/publish-once path, and may merge with an in-progress
   drain group).
2. Public API: `insert_many(space, Vec<(DimensionVector, Vec<u8>)>) ->
   io::Result<(RevisionId, RevisionId)>` (first/last revision), and the
   branch-aware equivalent. Revisions allocated contiguously up front; Hilbert
   keys computed once per row (Task 2.1); rows routed per shard in one pass.
3. De-duplicate the payload: make `WriteJob` carry the `WalEntry` plus only
   the metadata needed to rebuild the `Record` (address, revision, key,
   tombstone flag); the I/O thread constructs the `Record` referencing the
   same payload bytes (restructure so the data `Vec<u8>` moves rather than
   clones — an enum or a small struct with the payload held once).
4. Backpressure: `WriteBatch` respects queue capacity by message, not by row;
   document that a single huge batch should be chunked by the caller, and add
   an internal chunk size (e.g. 4,096 rows per message) inside `insert_many`.

This restores a public bulk path lost in the 0.2.0 → 0.3.0 break and is the
foundation the future parallel IFC import pipeline sits on.

**Acceptance.** `insert_many` of 50k rows across points spanning all shards,
then `sync()`, then full query returns all rows exactly once; criterion bench
comparing `insert_many` vs equivalent single inserts shows the batch path
ahead by a wide margin; allocation test (or code review assertion) that
payload bytes are not cloned between entry and record.

---

### 2.5 — Live tail without quadratic copying

**Files:** `src/engine/live_tail.rs`, `src/engine/query.rs`, all `snapshot()`
call sites

**Problem.** `LiveTailView::append` deep-clones the whole `Vec<Record>` per
write (O(n²) between seals — ~32k record copies per 256-record seal cycle per
shard), and `snapshot()` deep-clones the whole vector for every reader on
every query.

**Fix.** Two independent changes:
1. `snapshot()` returns `Arc<Vec<Record>>` (rename to `load()`), and readers
   iterate the shared vector. Update `query.rs`, overlay store, replication
   helpers. Where callers genuinely need ownership, they clone explicitly.
2. Writer side: with 2.2's batch publish, appends arrive in groups, so the
   simplest sufficient structure is a small immutable chunk list — the view
   holds `Arc<Vec<Arc<Vec<Record>>>>` (a list of sealed-in-memory chunks), the
   writer appends a chunk per group and rebuilds only the outer spine
   (O(chunks), not O(records)). Seal replaces the spine with the retained
   remainder. If this feels heavy, an `im::Vector<Record>` is an acceptable
   alternative; prefer the chunk list to avoid a new dependency.

**Acceptance.** Functional parity across the existing tail tests; a bench or
counter-based test demonstrating per-append cost no longer scales with tail
length; reader test confirming a loaded tail is immutable while the writer
continues appending.

---

### 2.6 — Seal from memory, not from a disk re-read

**Files:** `src/engine/io_thread.rs`, `src/engine/space_io.rs`

**Problem.** `seal_space` re-reads and re-decodes the entire hot segment from
disk even though every record passed through the I/O thread and is present in
the live tail; it then clones the full record vector into the `Block`, and
recomputes Hilbert min/max keys with two extra `spaces` read-lock
acquisitions.

**Fix.** Maintain the shard's pending-seal record set in memory in the I/O
loop (it already exists in the live tail — partition it at seal:
records-for-this-shard become the block, the file is reset, and the tail
remainder is republished, all in one `ShardView` swap per Task 1.2). The hot
segment file becomes purely a recovery artifact, read only on open. Block
min/max come from the cached keys (Task 2.1) of the first/last records after
the sort — no extra lock acquisitions. Move records into the block instead of
cloning.

**Acceptance.** Seal correctness tests unchanged; recovery test (open after
non-clean drop with unsealed records) still rebuilds the tail from the hot
segment; an fs-operation counter in tests shows no hot-segment read during a
normal seal.

---

## PHASE 3 — READ PATH

### 3.1 — Single-pass block scan

**Files:** `src/engine/query.rs` (`query_inner`)

**Problem.** Every candidate block is read twice — one pass to harvest
tombstones, one for results — doubling block I/O on cold scans (default cache
is 10 MB; large scans evict between passes).

**Fix.** One pass per block collecting candidates; visibility (including
tombstone suppression) is resolved afterwards by Task 1.1's latest-wins
grouping, which needs no advance tombstone set. If 1.1 has landed, this task
is mostly deletion.

**Acceptance.** Block-read counter (test hook on `read_block_shared`) shows
exactly one read per candidate block per query; results identical to before
on the visibility test suite.

---

### 3.2 — Hilbert range decomposition + shard pruning

**Files:** new `src/infinitedb_index/range_decompose.rs`;
`src/engine/query.rs` (`query_bbox`), `src/concurrent/read_txn.rs`,
`src/engine/hilbert_coordinator.rs` / live-tail selection

**Problem.** `query_bbox` derives a single key interval from the two box
corners. The curve segment between corner keys can wander far outside the box;
for boxes straddling high-order curve transitions the interval approaches the
full key space — correctness survives via the per-record `within()` filter,
but read amplification is unbounded. Compounding it, v4 queries concatenate
**all** shard tails for the space regardless of the range.

**Fix.**
1. Implement recursive orthant decomposition: starting from the root Hilbert
   cell, recurse into child cells (using the same Skilling state transitions
   as key generation), keeping cells fully inside the box as complete key
   intervals, discarding cells fully outside, and splitting partial cells —
   bounded by a max sub-interval count (e.g. 32) and a max depth, beyond which
   partial cells are emitted conservatively (slightly over-covering, never
   under-covering). Output: a small sorted list of disjoint `(lo, hi)` key
   intervals whose union covers the box.
2. `query_bbox` prunes blocks against the interval *list* (a block survives if
   any interval overlaps its `[min_key, max_key]`), and filters tail records
   against the list.
3. Shard pruning: each interval maps to a shard-id range
   (`hilbert_shard_id(lo) ..= hilbert_shard_id(hi)`); only those shards' tails
   (and, post-1.2, shard views) are loaded.
4. Property test: for random boxes and random points, a point is inside the
   box ⇒ its key is covered by the decomposition (no false negatives, ever).

**Acceptance.** The property test above (proptest, high iteration count);
selectivity test: a small box positioned to straddle the top-level curve
transition reads a small bounded number of blocks where the old code read
nearly all of them (assert via block-read counter); shard-tail counter shows
only intersecting shards consulted.

---

### 3.3 — Right-size blocks: byte-based seal threshold

**Files:** `src/engine/io_thread.rs`, `src/engine/space_io.rs`,
`IoThreadConfig`

**Problem.** Sealing every 256 records produces tiny blocks: per-block fixed
costs (open, checksum, decode, snapshot map entry) dominate scans, and the
snapshot index grows fast.

**Fix.** Replace `hot_segment_seal_threshold` (records) with a byte-based
threshold (`hot_segment_seal_bytes`, default on the order of 4–16 MB; keep a
generous record-count cap as a secondary trigger for pathological tiny-record
workloads). The I/O loop already tracks committed length arithmetically after
2.2. Keep the old field as deprecated for one release if external code sets
it.

**Acceptance.** Tests that previously forced seals by record count are
updated to set the byte threshold explicitly; a steady-ingest test confirms
block sizes land near the target.

---

### 3.4 — Wire compaction into the CRCW engine

**Files:** `src/infinitedb_storage/compaction.rs` (exists, unwired), new
`src/engine/compactor.rs`, shard I/O loops, `SnapshotStore`, block GC

**Problem.** Nothing in the concurrent engine ever compacts. Small blocks
accumulate without bound, tombstones are never physically reclaimed, and the
snapshot map grows monotonically — read time degrades for the life of the
database. Compaction is a missing organ, not an optimization.

**Fix.**
1. Trigger policy (size-tiered, simplest first): after each seal, if the count
   of blocks in the shard's key range below a size threshold exceeds N (e.g.
   8), compact them. Run it **on the shard's own I/O thread** between
   commands — single-writer-per-shard is preserved and no new locking is
   needed; bound each compaction's input bytes so write latency stays sane.
2. Reuse `compaction.rs::compact` with `retain_history` honored; verify the
   dedup respects the Task 1.1 visibility rule (latest record per address;
   tombstone as latest ⇒ drop both tombstone and history when
   `retain_history` is false **and** no snapshot/branch base still references
   the old blocks).
3. Publication: new blocks written and checksummed first, then one
   `SnapshotStore::update` (and `ShardView` swap) replaces old entries with
   new, then old block files are GC'd only if unreferenced by any branch base
   snapshot or pinned read (defer-delete list keyed by an epoch is sufficient
   given 1.2/1.3 give readers stable views; document the reclamation rule).
4. Expose `InfiniteDb::compact(space)` for manual invocation and tests; keep
   the automatic trigger conservative and configurable.

**Acceptance.** Ingest-heavy test producing many small blocks; after
compaction, block count drops to the expected tier, query results are
byte-identical to pre-compaction, deleted addresses with
`retain_history=false` no longer appear in any block (verified by raw block
scan), and a concurrent reader holding a pre-compaction view completes
successfully.

---

### 3.5 — Parallelize sync and flush fan-out

**Files:** `src/engine/hilbert_coordinator.rs`, `src/engine/coordinator.rs`
(`sync_all`, `flush_space`)

**Problem.** `sync_all` sends `Sync` to one shard, blocks on its ack, then
proceeds to the next — total latency is the *sum* of per-shard fsyncs (16
shards ⇒ 16 sequential fsyncs).

**Fix.** Two-phase: send all `Sync` (or `Flush`) commands first, collecting
the `done` receivers; then wait on all receivers, returning the first error
(after draining the rest). Latency becomes the max, not the sum.

**Acceptance.** With an injectable per-shard fsync delay (test-only sleep in
the loop), `sync_all` over 8 shards completes in ~1 delay rather than ~8.

---

## PHASE 4 — STRUCTURE & OPERATIONS

### 4.1 — Lazy shard provisioning

**Files:** `src/concurrent/concurrent_db.rs` (`register_space`),
`src/engine/hilbert_coordinator.rs` (`bootstrap_registered_spaces`,
`ensure_shard`)

**Problem.** `register_space` on v4 eagerly spawns all `2^shard_bits` shards —
16 threads, 16 hot segments, 16 staging files per space before any write. One
hundred spaces ⇒ 1,600 threads.

**Fix.** Spawn shards on first write (the `ensure_shard` check already exists
on the enqueue path); `register_space` only creates the space directory and
registry entry. Bootstrap-on-open spawns only shards whose directories
contain data (non-empty hot segment or sealed blocks referenced for that
shard's key range). Flush/sync iterate only live shards (they already do).
Stretch goal, separate task if pursued: replace per-shard threads with a
small shared worker pool where shards are ownership tokens — shard-internal
ordering is the only invariant, so N workers each owning a disjoint shard set
preserves it.

**Acceptance.** Registering 50 spaces spawns zero I/O threads (assert via
`shard_count()`); first write to a space spawns exactly the target shard;
reopen of a populated db spawns only shards with data.

---

### 4.2 — Resolve the snapshot identity model

**Files:** `src/engine/snapshot_store.rs`, `src/infinitedb_core/snapshot.rs`,
seal paths, branch creation

**Problem.** Each seal allocates a fresh `SnapshotId` but installs it only if
the current id is zero; the per-space snapshot is mutated in place by every
shard. The code half-keeps the immutable snapshot-lineage model (`parent`,
branch heads pointing at `SnapshotId`s) that branch/merge and sync
conceptually depend on, and half-treats the snapshot as a single mutable
index. Branch heads can point at ids that no longer denote a fixed state.

**Fix (decision + mechanics).** Adopt the explicit position that the
per-space entry in `SnapshotStore` is a *mutable head index*, and immutable
`Snapshot` values are minted only at meaningful boundaries: branch creation
(the base the overlay reads through — `register_branch` already stores an
`Arc<Snapshot>`, which is naturally immutable; ensure the head is *cloned* at
that moment, which `SnapshotStore::update`'s copy-on-write already
guarantees), merge completion, and sync negotiation. Then: stop allocating a
`SnapshotId` per seal (delete the wasted counter traffic); give minted
snapshots real ids and parents; update doc comments on `Snapshot` to state the
two roles plainly. No on-disk format change required.

**Acceptance.** Branch created before a burst of seals still reads its
original base (regression test); no `SnapshotId` allocation occurs during
seal (counter assertion); sync `snapshot_merkle` over a minted snapshot is
stable while main continues ingesting.

---

## Suggested execution order (dependency-sorted)

1. **1.1** latest-wins visibility (correctness; unblocks 3.1, informs 3.4)
2. **1.2** interim dedup, then atomic shard view (correctness; unblocks 2.6)
3. **1.3** stable-revision watermark (correctness of the read model)
4. **1.4** key truncation (small, independent — can run any time)
5. **2.1** cached Hilbert key (unblocks 2.2 efficiency, 2.6, 3.2)
6. **2.2** group commit (the batching keystone)
7. **2.4** batch surface (`insert_many`, `WriteBatch`)
8. **2.5** live tail structure
9. **2.6** seal from memory
10. **2.3** retire deadline/staging (only after 2.2 proven)
11. **1.5** branch overlay durability (reuses 2.2 group commit)
12. **3.1** single-pass scan (mostly deletion after 1.1)
13. **3.3** byte-based seal threshold
14. **3.4** compaction (the long-term read-health fix)
15. **3.2** range decomposition (largest isolated win for bbox reads)
16. **3.5** parallel sync fan-out
17. **4.1** lazy shards
18. **4.2** snapshot identity

Phases 1 and 2 together answer the three review findings directly: 1.1/1.2/1.3
close the correctness holes, 2.1–2.6 are what "batch writes well" requires,
and 1.2 + 1.3 + 4.2 are the structural changes that make the CRCW model
internally honest.
