# InfiniteDB Refactor Milestones

Source: [InfiniteDB-Spatial-Hyper-Truth-Graph-Design.docx](InfiniteDB-Spatial-Hyper-Truth-Graph-Design.docx), Section 6.

| Milestone | Status | Summary |
|-----------|--------|---------|
| **M1** | **Done** | Directed model, validation, versioned codec, CRCW hypergraph API, legacy cleanup |
| **M2** | **Done** | Polarity dimension in endpoint index; dual-layout reads; compaction lazy rewrite |
| **M3** | **Done** | Directional traversal, arrival-aware expansion, wave-front levels, B-connectivity mode |
| **M4** | **Done** | Derivation bus, per-subscriber watermarks, delta-merge reads, bulk import, error algebra foundation |
| **M5** | **Done** | Authoring-frame provenance; judgment conventions; per-space error records |
| **M6** | **Done** | Named frames; three-axis truth query API; frame-aware traversal |
| **M7** | **Done** | Flow-vector lane; computation provenance V4; staleness closures |

**Peer track (not milestone-gated):** HLC revisions, per-session WAL, Intent Checkpoints — required before M4 derivation bus at scale.
