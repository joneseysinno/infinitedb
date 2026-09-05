# Void absence-convention audit (V4)

Enumeration of call sites where absence means "no data" vs other semantics.
Decision records: `SEMANTICS.md` D-V1–D-V7.

| Site | Tier | Law | Conforms | Migrate |
|------|------|-----|----------|---------|
| `engine/flow_vector.rs` `resolve_flow_vector` → `None` | Derived void (disjoint forest) | INV-VOID-ANNIHILATOR | Yes | Documented; cross-ref D-T12 |
| `engine/query.rs` `resolve_visibility` `!r.tombstone` filter | Tombstone-filtered steady state | INV-VOID-TOMBSTONE-DISTINCT | Yes (after V2 `presence_at`) | No behavior change |
| `engine/density_stat.rs` `DensityTracker::get` | Derived void (unobserved space) | INV-VOID-DIV-UNDEFINED | Yes (V5 `VoidOr`) | Migrated |
| `infinitedb_core/merge.rs` `MergeConflict::{base,target,source}: Option<Record>` | Void + Tombstone conflated | — | Partial | Document only; merge may not need distinction |
| `engine/query.rs` `query_inner` empty result | Void at query grain | INV-VOID-DECIDABLE | Yes | Use `presence_at` for point reads |
| `infinitedb_core/void.rs` `classify_presence` | Storage three-state | D-V4 | Yes | Canonical classifier |
| `concurrent/concurrent_db.rs` `space_is_void_on_branch` | Container void | INV-VOID-POLYMORPHIC | Yes | `VoidState` on `SpaceHistoryView` |
| `engine/hypergraph.rs` traversal miss | Feature / empty set | — | N/A | Not void algebra |
| `EngineError::SpaceNotFound` | Registry miss | — | N/A | Error, not void |
| `Option` returns meaning "feature off" / validation fail | — | — | N/A | Excluded from void tier |

**Follow-up:** If three-way merge outcomes ever differ on Void vs Tombstoned at an address,
split `MergeConflict` optional fields or attach `Presence` sidecars.
