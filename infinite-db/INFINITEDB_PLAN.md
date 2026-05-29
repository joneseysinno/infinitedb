# InfiniteDB — Implementation Plan & Gap Closure

> Drop this file into the repo root (e.g. `PLAN.md`) and use it as the working
> brief in Cursor. Each task is self-contained: it names the file(s) to touch,
> states the problem precisely, and gives an acceptance test. Tasks are ordered
> by dependency — do them top to bottom.

---

## 0. Context for the agent

InfiniteDB is a spatial, multi-version, hypergraph database written in Rust.
The architecture is:

- **Spaces** (`SpaceId`) are independent N-dimensional coordinate graphs.
- **Records** are `(Address, RevisionId, data, tombstone)`. Immutable; updates
  append new revisions. Sealed into immutable **Blocks**.
- **Hilbert curve** (`infinitedb_index::hilbert`) maps an N-d point to a `u128`
  key so spatially close points get numerically close keys → range queries
  become contiguous key scans.
- **Hyperedges** are k-ary, cross-space relationships. Each `EndpointRef`
  carries its own `SpaceId` + `DimensionVector`, so an edge can link nodes in
  *different* spaces (pointer semantics, no storage proximity required).
- **Signals** are scoped field values: a `SignalScope` fixes a parent coordinate
  prefix and the sample lives at `local_coords` within it.
- **Branches** are named pointers to **Snapshots** (git-like possibility forks).
- **Sync** is offline-first: durable outbox + Merkle-diff delta protocol.

The crate is already well-built. This plan closes the remaining correctness gaps
and adds the traversal/indexing layer needed for the "functional hypergraph as
a scalable physics/property network" goal.

**Ground rules for the agent:**
- Do not break existing public APIs without noting it in the task.
- Every task must keep `cargo test --all-features` green.
- Match the existing code style (doc comments on every public item, `io::Result`
  for fallible storage ops, `bincode` Encode/Decode derives on persisted types).
- Prefer adding new files/modules over bloating `db.rs` (already 1,400+ lines).

---

## 1. CRITICAL CORRECTNESS BUGS

These are real defects in code paths that already exist. Fix first.

### 1.1 — Delta apply uses BlockId as the snapshot key instead of the Hilbert key

**File:** `src/infinitedb_sync/delta.rs` (`Delta::apply`, ~line 79)

**Problem.** Snapshots index blocks in a `BTreeMap<u128, BlockId>` keyed by the
block's **minimum Hilbert address** — this is what makes `query_inner`'s range
pruning (`snapshot.blocks.range(..=hi)`) correct. But `Delta::apply` inserts
synced blocks with `blocks.insert(block.id.0 as u128, block.id)` — it uses the
raw block ID as the key. After any sync, range queries over synced blocks
silently return wrong results, because the map key no longer reflects spatial
position.

**Fix.** Compute the real key from the block's first record. The block records
are sorted by Hilbert key at seal time (`db.rs::flush`), so the first record's
key is the block minimum. Add a shared helper so the key derivation lives in
exactly one place (see Task 2.1) and call it here.

```rust
// in delta.rs, replace the insert loop:
for block in &self.added_blocks {
    let key = block
        .records
        .first()
        .map(|r| crate::infinitedb_index::hilbert_key_for(&r.address.point))
        .unwrap_or(0);
    blocks.insert(key, block.id);
}
```

**Acceptance.** Add a test in `delta.rs`: build a source snapshot whose block
holds records at known coordinates, apply the delta to an empty target, then
assert the resulting snapshot's `blocks` map key equals
`hilbert_key_for(first_record_point)` — not `block.id.0`.

---

### 1.2 — API dispatcher silently drops the spatial key range

**Files:** `src/infinitedb_server/api.rs` (`dispatch`, ~line 139),
`src/infinitedb_core/query.rs`

**Problem.** `Request::Query` carries `key_range: Option<(u128, u128)>`, but in
`dispatch` the range is discarded: `let _ = (lo, hi); // stored ... in the full
implementation`. Every query over the server API therefore becomes a full-space
scan regardless of the requested bounds. The `Query` descriptor only has a
`SpatialRange` of raw coords (`min`/`max` `DimensionVector`s) and no way to carry
a precomputed Hilbert interval.

**Fix.** Add an optional Hilbert key-range field to `Query` and thread it
through. Two acceptable approaches — pick (A):

(A) Extend `Query` with `pub key_range: Option<(u128, u128)>` and a builder
`with_key_range`. In `dispatch`, set it from the request. Then in `db.rs` add a
`query_with_descriptor(&Query)` method (Task 3.1) that uses `key_range` directly
when present, falling back to `range` → Hilbert via `hilbert_key_for`.

**Acceptance.** A `dispatch` test that issues `Request::Query` with a `key_range`
and asserts (via the `read` callback capturing the `Query`) that the range
survives into the `Query` passed to storage.

---

### 1.3 — Hilbert range pruning can miss blocks (document or fix the lower bound)

**File:** `src/db.rs` (`query_inner`, ~line 698)

**Problem.** Block pre-filtering does `snapshot.blocks.range(..=hi)` — it takes
all blocks whose **start key** ≤ `hi`. This ignores the lower bound `lo`
entirely. It is *safe* (never drops a needed block, because a block starting
below `lo` may still contain points inside the box, and the per-record
`within()` check removes false positives) but it is *not selective* — for a
small box at high keys it still scans every low-keyed block.

This is a performance gap, not a correctness bug. The exact fix is non-trivial
because a Hilbert box maps to multiple disjoint key intervals.

**Action.** Two options, your call:
- (cheap) Add a doc comment making the over-approximation explicit and open a
  tracked issue. Keep behavior.
- (better) Track each block's `(min_key, max_key)` pair in the snapshot
  (Task 4.1 stores max), then prune to blocks where `min_key <= hi && max_key >= lo`.

Recommend doing the cheap version now, the better version after Task 4.1.

---

## 2. SHARED HILBERT-KEY HELPER (de-duplicate)

### 2.1 — Promote `hilbert_key_for` to the index module

**Files:** `src/db.rs` (~line 1040), `src/infinitedb_index.rs`, new
`src/infinitedb_index/key.rs` (or add to an existing index file)

**Problem.** `hilbert_key_for(&DimensionVector) -> u128` is a private free
function in `db.rs`. `delta.rs` (Task 1.1) and any future traversal/index code
need the *exact same* derivation. Duplicating it risks the two drifting (one
using 8-bit precision, another 4-bit) which would corrupt key ordering across
the sync boundary.

**Fix.** Move it to `infinitedb_index` as `pub fn hilbert_key_for(point:
&DimensionVector, config: KeyConfig) -> u128`, defaulting to `KeyConfig::STANDARD`
via a thin wrapper `pub fn hilbert_key_standard(point) -> u128`. Re-export at the
index module root. Update `db.rs` to call it. This makes precision a single
source of truth.

**Note.** Precision is a load-bearing global invariant: every block in a space
must be keyed at the same `bits_per_dim`, or `BTreeMap` ordering breaks. Consider
storing `bits_per_dim` per-space in `SpaceConfig` (Task 5.2) so it is explicit
and persisted.

**Acceptance.** `cargo test --all-features` still green; `delta.rs` and `db.rs`
both reference the one helper.

---

## 3. QUERY DESCRIPTOR EXECUTION

### 3.1 — Wire `Query` (the descriptor) to a real executor

**Files:** `src/infinitedb_core/query.rs`, `src/db.rs`

**Problem.** `Query` is a clean descriptor (`space`, `snapshot`, `range`,
`as_of`, `include_tombstones`) but nothing in `db.rs` consumes a `Query`
directly — callers use the lower-level `query`, `query_bbox`, `query_subscope`.
The server API builds a `Query` then relies on a `read: FnOnce(Query) -> ...`
callback that the embedding app has to implement by hand.

**Fix.** Add `InfiniteDb::execute(&mut self, q: &Query) -> io::Result<Vec<Record>>`
that:
1. Resolves `as_of` (default to snapshot revision).
2. If `q.key_range` is set (Task 1.2) use it; else if `q.range` is set, convert
   via `hilbert_key_standard(min/max)`; else full scan.
3. Calls the existing `query_inner`.
4. Applies `include_tombstones` (currently `query_inner` always drops
   tombstones — add a flag parameter or a post-filter).

Then the server `dispatch` `read` callback becomes a one-liner `|q| db.execute(&q)`.

**Acceptance.** Test that `execute` with a `range` returns the same records as
`query_bbox` with the same bounds, and that `include_tombstones(true)` surfaces a
tombstoned record while the default hides it.

---

## 4. HYPEREDGE TRAVERSAL LAYER (the core "hypergraph" capability)

This is the biggest *feature* gap relative to the functional-hypergraph theory.
Today you can store and scan hyperedges, and filter them by endpoint in memory
(`query_hyperedges_for_endpoint` loads *all* edges then filters). There is no
index and no cross-space walk.

### 4.1 — Reverse endpoint index

**Files:** new `src/infinitedb_core/endpoint_index.rs`, wire into `db.rs`

**Problem.** `query_hyperedges_for_endpoint` is O(edges) — it decodes every edge
in the space and filters. For a real graph this is unusable.

**Fix.** Maintain a reverse index mapping an endpoint `(SpaceId, coords)` → list
of `HyperedgeId`. Store it as records in a **reserved index space**:

```rust
pub const ENDPOINT_INDEX_SPACE: SpaceId = SpaceId(u64::MAX - 1);
```

On `insert_hyperedge`, for each endpoint write an index record whose point is
`hilbert-composed(endpoint.space.0, endpoint.node.coords...)` and whose payload
is the `HyperedgeId`. Because the index is itself Hilbert-keyed over endpoint
position, "all edges touching nodes in this spatial region of space S" becomes a
range scan. On `delete_hyperedge`, tombstone the matching index records.

**Acceptance.** Insert 3 edges sharing one endpoint and 2 that don't; assert the
index lookup for that endpoint returns exactly 3 `HyperedgeId`s without a full
edge scan (verify by asserting on a counter or by using a space with many
unrelated edges).

### 4.2 — Cross-space traversal API

**Files:** new `src/infinitedb_core/traversal.rs`, wire into `db.rs`

**Problem.** No way to walk the graph: given a start node, follow its hyperedges
into other spaces, accumulate a subgraph, bounded by depth/kind.

**Fix.** Add:

```rust
pub struct TraversalSpec {
    pub start: EndpointRef,
    pub max_depth: usize,
    pub follow_kinds: Option<Vec<HyperedgeKind>>, // None = all
    pub as_of: Option<RevisionId>,
}
pub struct Subgraph {
    pub nodes: Vec<EndpointRef>,
    pub edges: Vec<Hyperedge>,
}
impl InfiniteDb {
    pub fn traverse(&mut self, spec: &TraversalSpec) -> io::Result<Subgraph>;
}
```

BFS using the Task 4.1 index: from the frontier nodes, look up incident edges,
filter by `follow_kinds` and `is_active_at(as_of)`, enqueue the other endpoints,
stop at `max_depth`. Deduplicate visited `(space, coords)`.

**Acceptance.** Build a small 3-space graph (mechanical → thermal → damage as in
the design discussion), traverse from a mechanical node with `max_depth = 2`, and
assert the returned subgraph contains the expected thermal and damage nodes and
edges, and excludes anything at depth 3.

### 4.3 — Hyperedge centroid keying (optional, for locality)

**File:** `src/db.rs` (`hyperedge_point`, ~line 1051)

**Problem.** `hyperedge_point(id)` keys an edge purely by its `HyperedgeId`
(`id >> 32`, `id & 0xFFFF_FFFF`). That gives edges no spatial locality — edges
that relate nearby nodes land at arbitrary, scattered keys. The theory wants the
edge addressed at the **centroid** of its endpoints so spatially-related edges
cluster (the "density-adaptive incidence array" idea).

**Fix (optional / experimental).** Add an alternative keying mode that computes
the centroid of all same-space endpoints and Hilbert-encodes that, falling back
to the ID-based key for cross-space edges with no common frame. Gate behind a
`SpaceConfig` flag so existing edge spaces are unaffected. This is research-y —
do it last and behind a feature flag.

**Acceptance.** Edges relating clustered nodes produce clustered keys (assert two
edges over nearby endpoints get keys closer than two edges over distant
endpoints).

---

## 5. METADATA PERSISTENCE GAPS

### 5.1 — Branches and snapshots are not persisted across restart

**File:** `src/db.rs` (`load_meta` ~line 1014, `persist_meta` ~line 902)

**Problem.** `persist_meta` writes only `spaces.bin` and `counters.bin`.
`load_meta` reconstructs `BranchRegistry::new()` and an empty snapshot map every
open — so **branches and per-space snapshots are lost on restart**. The
`main` branch is recreated, but any user branch and all sealed-block snapshot
indexes vanish (blocks remain on disk but become unreachable by query until
re-sealed).

This is the most serious durability gap. Sealed data is effectively orphaned
after a restart because the snapshot `BTreeMap<u128, BlockId>` that points at the
blocks is never reloaded.

**Fix.** Persist and reload:
- `branches.bin` ← serialize `BranchRegistry` (needs `Encode/Decode` — it
  currently derives only `Serialize/Deserialize`; add bincode derives or
  serialize via serde+bincode like the snapshot map).
- `snapshots.bin` ← serialize the `BTreeMap<u64, Snapshot>`.
Call from `persist_meta` (after each flush) and read in `load_meta`.

**Acceptance.** Test: open db, register space, insert + flush, create a branch,
drop the db handle, reopen, then (a) query returns the flushed records, (b) the
created branch is present. Today (a) fails after restart — this test should go
red first, then green.

### 5.2 — Per-space Hilbert precision in SpaceConfig

**File:** `src/infinitedb_core/space.rs`, `src/db.rs`

**Problem.** Hilbert precision (`bits_per_dim`) is hard-coded to
`KeyConfig::STANDARD` (8 bits) in `hilbert_key_for`. Different spaces may want
different precision (e.g. a coarse thermal field vs a fine mechanical mesh). More
importantly, precision must be *stable per space forever* or key ordering breaks.

**Fix.** Add `pub bits_per_dim: u32` to `SpaceConfig` (default 8). Look it up in
the key helper. Validate `dims * bits_per_dim <= 128` at `register_space`.

**Acceptance.** Register two spaces with different precision; assert keys encode
at the configured precision and `register_space` rejects `dims*bits > 128`.

---

## 6. SYNC LAYER COMPLETION

### 6.1 — Delta protocol is not driven end-to-end

**Files:** `src/infinitedb_sync/delta.rs`, `serial.rs`, `merkle.rs`, `db.rs`

**Problem.** The pieces exist (Merkle tree, leaf diff, Delta compute/apply, wire
framing) but nothing orchestrates a full session: send root → compare → send tree
→ diff → send delta → apply → ack. The outbox path (operation replay) works
end-to-end and is tested; the *block-level delta* path is only unit-tested per
component.

**Fix.** Add a `sync_session` driver (host supplies a duplex `Read+Write`) that
runs the negotiation in `serial::SyncMessage` order and applies the resulting
delta via `Delta::apply` + writes `added_blocks` to the `BlockStore` + GC
`removed_block_ids`. Reuse Task 1.1's corrected keying.

**Acceptance.** In-memory pipe test: two `InfiniteDb` instances, diverge them,
run a session, assert both converge to identical Merkle roots.

### 6.2 — Merkle tree is built but never built *from* a snapshot in db.rs

**Files:** `src/db.rs`, `src/infinitedb_sync/merkle.rs`

**Problem.** `MerkleTree::build` takes leaf hashes, but there's no method that
walks a snapshot's blocks in Hilbert order, hashes each record, and produces the
tree. Without it, 6.1 can't compute roots.

**Fix.** Add `InfiniteDb::snapshot_merkle(&mut self, space) -> io::Result<MerkleTree>`
that reads blocks in `snapshot.blocks` key order, hashes each `Record` with
`merkle::hash_record(&encoded)`, and builds the tree. Leaf order must be
deterministic (Hilbert key, then revision).

**Acceptance.** Two databases with identical contents produce identical roots;
changing one record changes the root and `diff_leaves` points at it.

---

## 7. TEST & TOOLING HARDENING

### 7.1 — Restart/durability test matrix
Add an integration test file `tests/durability.rs` covering: WAL replay of
unflushed writes, reopen-after-flush visibility (depends on 5.1), branch
persistence (5.1), and outbox survival (already passing).

### 7.2 — Property test for Hilbert locality
Add a `proptest`/quickcheck test: for random point pairs, closeness in coordinate
space correlates with closeness in key space often enough (sanity bound, not
strict). Guards against precision/encoding regressions.

### 7.3 — Bench harness (optional)
Add `benches/` with criterion: insert throughput, `query_bbox` selectivity vs
full scan, traversal depth scaling. Useful to validate Task 1.3's pruning win.

---

## Suggested execution order (dependency-sorted)

1. **2.1** shared key helper (unblocks 1.1, 3.1, 4.x)
2. **1.1** delta key bug (correctness)
3. **5.1** branch/snapshot persistence (durability — most important data-loss fix)
4. **1.2 + 3.1** query descriptor + API range plumbing
5. **5.2** per-space precision
6. **4.1** reverse endpoint index
7. **4.2** traversal API
8. **6.2 + 6.1** Merkle-from-snapshot + delta session driver
9. **1.3** range-pruning selectivity (perf)
10. **4.3** centroid keying (experimental)
11. **7.x** test/bench hardening throughout

---

## What is already done (do NOT rebuild)

- Hilbert encode/decode (Skilling), composite keys, ordinal/float/HLC encoders.
- Block store (NVMe, LRU cache, atomic writes, blake3 checksums).
- WAL append + crash recovery replay.
- Compaction (history + dedup modes) and GC (tombstone pruning, snapshot-safe
  block deletion).
- Hyperedge + signal persistence, typed adapter APIs, kind catalog governance.
- `query`, `query_bbox`, `query_subscope`, signal scope/range queries.
- Branch creation (in-memory), snapshot diff.
- Offline outbox: enqueue, backoff retry, ack/stale handling, background worker,
  restart survival.
- Server request/response dispatch with session authz.
- Sync wire framing, Merkle tree + leaf diff, delta compute/apply (components).
