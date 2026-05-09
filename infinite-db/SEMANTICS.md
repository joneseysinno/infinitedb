# InfiniteDB Semantic Reference

This document defines the canonical language for `infinite-db` and upstream crates.

## Core Terms

- **Space**: named dataset with fixed dimensionality (`SpaceId`, `SpaceConfig`).
- **Address**: primary coordinate key (`space + point`).
- **Node**: domain entity represented in one or more spaces.
- **Revision**: monotonic logical write clock (`RevisionId`).
- **Snapshot**: immutable read view assembled from sealed blocks.
- **Tombstone**: deletion marker that suppresses prior live records.

## Relationship Terms

- **Hyperedge**: explicit N-ary relationship among endpoint nodes.
- **Endpoint**: one participant in a hyperedge, with a semantic role.
- **Edge Kind**: user-defined relationship category string (for example `beam.bears_on`).
- **Validity Window**: `valid_from..valid_to` revision interval where an edge is active.

## Signal Terms

- **Signal**: typed measured/computed value over a scoped hyperspace using a user-defined kind string.
- **Signal Scope**: fixed parent coordinate prefix plus declared total dimensions.
- **Signal Sample**: one value at local coordinates within a scope.
- **Signal Constraint**: optional value bounds and clamp policy.

## Modeling Guidance

- Use **hyperedges** for cross-entity topology and provenance.
- Use **signals** for intra-entity fields, gradients, and time/state samples.
- Use both together when results are derived from external relationships.
