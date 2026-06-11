# infinite-db

A spatial-graph database using n-dimensional curves and hyperedges for engineering logic.

## Quick start

Add to your `Cargo.toml`:

```toml
[dependencies]
infinite-db = "0.3"
```

```rust
use infinite_db::{InfiniteDb, OpenOptions};
use infinite_db::infinitedb_core::address::{DimensionVector, SpaceId};
use infinite_db::infinitedb_core::space::SpaceConfig;

fn main() -> std::io::Result<()> {
    let db = InfiniteDb::open("./data")?;
    db.register_space(SpaceConfig::new(SpaceId(1), "demo", 2))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    db.insert(SpaceId(1), DimensionVector::new(vec![0, 0]), vec![1, 2, 3])?;
    db.sync()?;
    let rows = db.query(SpaceId(1), None)?;
    println!("{} record(s)", rows.len());
    Ok(())
}
```

Writes are fire-and-forget: call [`sync`](https://docs.rs/infinite-db/0.3.0/infinite_db/struct.InfiniteDb.html#method.sync) to flush the write queue and make data durable and visible to queries.

See also the [`quickstart`](examples/quickstart.rs) example.

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `embedded` | yes | CRCW embedded database ([`InfiniteDb`](https://docs.rs/infinite-db/0.3.0/infinite_db/struct.InfiniteDb.html)) |
| `server` | no | TCP API (`embedded` + tokio) |
| `sync` | no | Replication, conflict queue, branch merge (`server`) |

Enable server/sync in your manifest:

```toml
infinite-db = { version = "0.3", features = ["sync"] }
```

## Key concepts

- **Space** — named N-dimensional dataset with fixed dimensionality
- **Address** — `(SpaceId, DimensionVector)` coordinate key for a record
- **Revision** — monotonic logical write clock
- **Snapshot** — immutable read view assembled from sealed blocks
- **Branch** — isolated write overlay forked from another branch; merge back with `merge_branch`

Full vocabulary: [SEMANTICS.md](SEMANTICS.md). Refactor roadmap: [MILESTONES.md](MILESTONES.md).

## Hypergraph (Milestones 1–7)

`InfiniteDb` exposes directed hyperedge writes and incidence queries on the CRCW engine:

- `insert_hyperedge`, `delete_hyperedge`, `insert_hyperedge_typed`
- `query_hyperedges`, `query_hyperedges_for_endpoint`, `query_hyperedges_for_endpoint_directed`
- `query_hyperedges_for_endpoint_directed_with_options` + `QueryOptions::index_only` (M4 delta-merge)
- `count_incident_edges_for_endpoint`, `count_incident_edges_for_endpoint_directed` (M2)
- `sync_derivation`, `derivation_stats` (M4 derivation bus)
- `begin_hyperedge_import`, `commit_hyperedge_import` (M4 applicative bulk import)
- `upgrade_endpoint_index_layout`, `compact_endpoint_index` (M2 migration)
- Versioned hyperedge payloads via `infinitedb_core::hyperedge_codec` (V1 legacy → V2; V3 `authoring_frame`; V4 `computation`)
- `register_arbiter_stream`, `assert_judgment`, `query_judgments_for_subject`, `query_judgments_in_region` (M5)
- `persist_operation_errors`, `query_operation_errors`, `resolve_operation_error` (M5 per-space error records)
- `diagnose_assertion` pure staleness helpers (M5)
- `register_frame`, `query_hyperedges_in_frame`, `traverse_in_frame` (M6 frame-resolution queries)
- `query_flow_vectors_in_region`, `query_flow_vector_for_edge`, `flow_vector_index_watermark` (M7 flow-vector lane)
- `check_hyperedge_freshness`, `query_stale_downstream` (M7 structural staleness via computation inputs)

Main-branch writes commit **assertions** synchronously; the **endpoint index** and **flow-vector index** derive on a background bus. Call `sync()` (or `sync_derivation()`) before index-only queries. Default incidence queries merge the assertion delta for exact results under pipeline lag.

New databases use **M2 polarity-dimension** endpoint index layout. Existing M1 index rows migrate via `upgrade_endpoint_index_layout()` + `compact_endpoint_index()`.

## Upstream typed wrappers

Upstream crates can add typed labels and domain spaces via adapter traits without changing the storage core. See [UPSTREAM_ADAPTER.md](UPSTREAM_ADAPTER.md).

## On-disk formats

New databases default to **format v4** (Hilbert shards + branch overlays). Formats v2 and v3 are supported for existing directories. Tune via [`OpenOptions::format_version`](https://docs.rs/infinite-db/0.3.0/infinite_db/struct.OpenOptions.html#structfield.format_version).

## Migrating from 0.2.0

**This is a breaking release.** `InfiniteDb` in 0.3.0 is a new CRCW engine (concurrent reads, fire-and-forget writes), not an incremental update of the 0.2.0 single-threaded WAL implementation.

**Removed from the public API (0.3.0):**

- `insert_records_bulk`, `delete_records_bulk`
- Hyperedge/signal bulk import sessions (`begin_hyperedge_import`, etc.)

**Restored / added (Milestone 1, unreleased):**

- `insert_hyperedge`, `delete_hyperedge`, `insert_hyperedge_typed` on `InfiniteDb`
- Directed incidence queries with transitional post-filter (`DirectionFilter`)
- Structural polarity and directionality on `Hyperedge` / `EndpointRef`
- Versioned hyperedge payload codec (V1 decode-only, V2 tagged writes)

**Still available:**

- Hyperedge and signal **types** in `infinitedb_core` for upstream modeling
- Record-level `insert`, `delete`, `query`, and bounding-box queries
- Branch overlays, `create_branch`, and `merge_branch` (format v4)

If you depend on 0.2.0 bulk session APIs, stay on **0.2.0** or batch via `insert_many`.

## Links

- [Documentation (docs.rs)](https://docs.rs/infinite-db/0.3.0)
- [Repository](https://github.com/joneseysinno/infinitedb)
- [Changelog](CHANGELOG.md)
