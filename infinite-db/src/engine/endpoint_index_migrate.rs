//! Lazy V1 → V2 endpoint index rewrite during compaction.

use std::io;

use crate::infinitedb_core::{
    address::SpaceId,
    block::Record,
    endpoint_index::ENDPOINT_INDEX_SPACE,
    flow_vector_index::FLOW_VECTOR_INDEX_SPACE,
    judgment_index::JUDGMENT_INDEX_SPACE,
    hyperedge::{Hyperedge, HyperedgeId},
    space::{EndpointIndexLayout, SpaceRegistry},
};

use super::hypergraph::{plan_v1_to_v2_index_rewrite, registry_index_layout};

/// Expand compaction input with V1 → V2 index rewrites when layout is M2.
pub fn expand_endpoint_index_records_for_compaction(
    records: Vec<Record>,
    spaces: &SpaceRegistry,
    resolve: &dyn Fn(HyperedgeId) -> Option<Hyperedge>,
) -> Vec<Record> {
    if registry_index_layout(spaces) != EndpointIndexLayout::V2PolarityDim {
        return records;
    }

    let rewrite_rows = plan_v1_to_v2_index_rewrite(&records, resolve);
    if rewrite_rows.is_empty() {
        return records;
    }

    let mut expanded = records;
    for row in rewrite_rows {
        expanded.push(Record {
            address: crate::infinitedb_core::address::Address::new(row.space, row.point),
            revision: crate::infinitedb_core::address::RevisionId::legacy(u64::MAX),
            data: row.data,
            tombstone: row.tombstone,
            hilbert_key: crate::infinitedb_core::hilbert_key::CachedHilbertKey::UNSET,
        });
    }
    expanded
}

/// Edge spaces to scan when resolving hyperedges during index migration.
pub fn edge_spaces_from_registry(spaces: &SpaceRegistry) -> Vec<SpaceId> {
    spaces
        .space_ids()
        .into_iter()
        .filter(|id| {
            *id != ENDPOINT_INDEX_SPACE
                && *id != JUDGMENT_INDEX_SPACE
                && *id != FLOW_VECTOR_INDEX_SPACE
        })
        .filter(|id| {
            spaces
                .get(*id)
                .map(|c| !c.name.ends_with("_errors"))
                .unwrap_or(true)
        })
        .collect()
}

/// Resolve a hyperedge by id across candidate edge spaces using `fetch`.
#[allow(dead_code)]
pub fn resolve_hyperedge_across_spaces(
    id: HyperedgeId,
    edge_spaces: &[SpaceId],
    fetch: &dyn Fn(SpaceId, HyperedgeId) -> io::Result<Option<Hyperedge>>,
) -> Option<Hyperedge> {
    for &space in edge_spaces {
        if let Ok(Some(edge)) = fetch(space, id) {
            return Some(edge);
        }
    }
    None
}
