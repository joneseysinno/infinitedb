# Upstream Adapter Guide

This guide explains how upstream crates add typed labels/spaces while `infinite-db`
keeps a generic storage core.

## 1) Define typed labels via traits

Implement `KindLabel` for your domain enums/newtypes:

```rust
enum BeamEdgeKind {
    BearsOn,
}

impl infinite_db::infinitedb_core::adapter::KindLabel for BeamEdgeKind {
    fn label(&self) -> &str {
        match self {
            BeamEdgeKind::BearsOn => "beam.bears_on",
        }
    }
}
```

Use `RoleLabel` similarly for endpoint roles (`parent`, `support`, etc.).

## 2) Bind domain models to spaces

Implement `SpaceBinding` for each typed domain surface:

```rust
struct BeamSignalSpace;
impl infinite_db::infinitedb_core::adapter::SpaceBinding for BeamSignalSpace {
    const SPACE_ID: infinite_db::infinitedb_core::address::SpaceId =
        infinite_db::infinitedb_core::address::SpaceId(88);
    const DIMS: usize = 3;
    const SPACE_NAME: &'static str = "beam_signals";
}
```

Adapter wrappers validate `SpaceBinding::DIMS` against the runtime `SpaceRegistry`.

## 3) Register catalog vocabulary (optional but recommended)

Use `KindCatalog` for discoverability and policy enforcement:

- register edge kinds (`beam.bears_on`)
- register signal kinds (`beam.bending_moment`)
- register endpoint roles (`parent`, `support`)
- choose unknown-kind policy:
  - `AllowUnknown`
  - `WarnUnknown`
  - `RejectUnknown`

## 4) Use typed wrapper APIs

`InfiniteDb` exposes adapter methods:

- `insert_hyperedge_typed(...)`
- `query_hyperedges_by_kind_typed(...)`
- `insert_signal_sample_typed::<SpaceBinding, _>(...)`

These wrappers convert typed labels into core string labels and can apply catalog
validation before persisting records.
