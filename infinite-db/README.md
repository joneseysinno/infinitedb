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
| `legacy-v1` | no | Internal v1 WAL engine (compile-only reference; not part of the public API) |

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

Full vocabulary: [SEMANTICS.md](SEMANTICS.md).

## Upstream typed wrappers

Upstream crates can add typed labels and domain spaces via adapter traits without changing the storage core. See [UPSTREAM_ADAPTER.md](UPSTREAM_ADAPTER.md).

## On-disk formats

New databases default to **format v4** (Hilbert shards + branch overlays). Formats v2 and v3 are supported for existing directories. Tune via [`OpenOptions::format_version`](https://docs.rs/infinite-db/0.3.0/infinite_db/struct.OpenOptions.html#structfield.format_version).

## Migrating from 0.2.0

**This is a breaking release.** `InfiniteDb` in 0.3.0 is a new CRCW engine (concurrent reads, fire-and-forget writes), not an incremental update of the 0.2.0 single-threaded WAL implementation.

**Removed from the public API:**

- `insert_records_bulk`, `delete_records_bulk`
- Hyperedge/signal bulk import and delete (`begin_hyperedge_import`, etc.)
- Hyperedge/signal write methods on `InfiniteDb`

**Still available:**

- Hyperedge and signal **types** in `infinitedb_core` for upstream modeling
- Record-level `insert`, `delete`, `query`, and bounding-box queries on the new `InfiniteDb`
- Branch overlays, `create_branch`, and `merge_branch` (format v4)

If you depend on 0.2.0 bulk write APIs, stay on **0.2.0**. There is no public migration path to the internal `legacy-v1` engine.

## Links

- [Documentation (docs.rs)](https://docs.rs/infinite-db/0.3.0)
- [Repository](https://github.com/joneseysinno/infinitedb)
- [Changelog](CHANGELOG.md)
