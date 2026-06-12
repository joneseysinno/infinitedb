//! Hypergraph write/query helpers for CRCW [`InfiniteDb`](crate::InfiniteDb).
//!
//! M4: assertions commit synchronously; endpoint-index rows derive on the bus.
//! [`prepare_writes`] remains a compatibility wrapper for compaction and branch writes.

use std::io;

use crate::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    block::Record,
    endpoint_index::{
        collect_incident_edge_ids, count_incident_edges, decode_hyperedge_id_from_index,
        edge_endpoints, encode_index_payload, endpoint_for_v1_index_record,
        endpoint_index_layout_from_registry, endpoint_index_point_for_layout,
        endpoint_index_query_bounds, endpoint_lookup_prefix, index_record_layout,
        record_matches_endpoint_prefix, ENDPOINT_INDEX_BITS_PER_DIM, ENDPOINT_INDEX_DIMS,
        ENDPOINT_INDEX_SPACE,
    },
    hilbert_key::CachedHilbertKey,
    hyperedge::{EndpointRef, Hyperedge, HyperedgeId},
    hyperedge_codec::{decode_hyperedge, encode_hyperedge},
    query::DirectionFilter,
    space::{EndpointIndexLayout, SpaceConfig, SpaceRegistry},
};

/// Space config for the reserved endpoint reverse index (M2 layout for new registrations).
pub fn endpoint_index_space_config() -> SpaceConfig {
    SpaceConfig::new(
        ENDPOINT_INDEX_SPACE,
        "__endpoint_index__",
        ENDPOINT_INDEX_DIMS,
    )
    .with_bits_per_dim(ENDPOINT_INDEX_BITS_PER_DIM)
    .with_endpoint_index_layout(EndpointIndexLayout::V2PolarityDim)
    .without_error_space()
}

/// One row to write: `(space, point, payload)`; empty payload with tombstone flag.
#[derive(Debug, Clone)]
pub struct HypergraphWriteRow {
    pub space: SpaceId,
    pub point: DimensionVector,
    pub data: Vec<u8>,
    pub tombstone: bool,
}

/// Single assertion row for the edge space (sync write path, M4).
pub fn prepare_assertion_write(space: SpaceId, edge: &Hyperedge) -> io::Result<HypergraphWriteRow> {
    let data = encode_hyperedge(edge)?;
    Ok(HypergraphWriteRow {
        space,
        point: Hyperedge::storage_point(edge.id),
        data,
        tombstone: false,
    })
}

/// Assertion tombstone for the edge space (sync delete path, M4).
pub fn prepare_assertion_tombstone(space: SpaceId, id: HyperedgeId) -> HypergraphWriteRow {
    HypergraphWriteRow {
        space,
        point: Hyperedge::storage_point(id),
        data: vec![],
        tombstone: true,
    }
}

/// Derived endpoint-index rows (async derivation bus, M4).
pub fn prepare_index_derivation(
    edge: &Hyperedge,
    index_layout: EndpointIndexLayout,
) -> Vec<HypergraphWriteRow> {
    edge_endpoints(edge)
        .into_iter()
        .map(|ep| HypergraphWriteRow {
            space: ENDPOINT_INDEX_SPACE,
            point: endpoint_index_point_for_layout(&ep, edge.id, index_layout),
            data: encode_index_payload(edge.id, index_layout),
            tombstone: false,
        })
        .collect()
}

/// Derived endpoint-index tombstones (async derivation bus, M4).
pub fn prepare_index_tombstones(
    edge: &Hyperedge,
    index_layout: EndpointIndexLayout,
) -> Vec<HypergraphWriteRow> {
    edge_endpoints(edge)
        .into_iter()
        .map(|ep| HypergraphWriteRow {
            space: ENDPOINT_INDEX_SPACE,
            point: endpoint_index_point_for_layout(&ep, edge.id, index_layout),
            data: vec![],
            tombstone: true,
        })
        .collect()
}

/// Deterministic rows for inserting/updating a hyperedge assertion and its index.
///
/// Compatibility wrapper — branch writes and compaction still use the full row set.
pub fn prepare_writes(
    space: SpaceId,
    edge: &Hyperedge,
    index_layout: EndpointIndexLayout,
) -> io::Result<Vec<HypergraphWriteRow>> {
    let mut rows = vec![prepare_assertion_write(space, edge)?];
    rows.extend(prepare_index_derivation(edge, index_layout));
    Ok(rows)
}

/// Tombstone rows for deleting a hyperedge and its index entries.
pub fn prepare_deletes(
    space: SpaceId,
    edge: &Hyperedge,
    index_layout: EndpointIndexLayout,
) -> Vec<HypergraphWriteRow> {
    let mut rows = vec![prepare_assertion_tombstone(space, edge.id)];
    rows.extend(prepare_index_tombstones(edge, index_layout));
    rows
}

/// Convert prepared rows into engine records at a revision range start.
pub fn rows_to_records(rows: &[HypergraphWriteRow], first_revision: RevisionId) -> Vec<Record> {
    rows.iter()
        .enumerate()
        .map(|(i, row)| {
            let revision = RevisionId::legacy(first_revision.legacy_sequence() + i as u64);
            Record {
                address: crate::infinitedb_core::address::Address::new(row.space, row.point.clone()),
                revision,
                data: row.data.clone(),
                tombstone: row.tombstone,
                hilbert_key: CachedHilbertKey::UNSET,
            }
        })
        .collect()
}

/// Decode a hyperedge from raw record bytes.
pub fn decode_edge_record(data: &[u8]) -> io::Result<Hyperedge> {
    decode_hyperedge(data)
}

/// Collect hyperedge ids from endpoint index records matching `prefix` (M1 helper).
pub fn incident_edge_ids_from_records(
    records: &[Record],
    prefix: &[u32],
) -> Vec<HyperedgeId> {
    records
        .iter()
        .filter(|r| record_matches_endpoint_prefix(r, prefix))
        .filter_map(|r| decode_hyperedge_id_from_index(&r.data).map(|(_, id)| id))
        .collect()
}

/// Prefix for incidence lookup of an endpoint.
pub fn endpoint_prefix(endpoint: &EndpointRef) -> Vec<u32> {
    endpoint_lookup_prefix(endpoint)
}

/// Bounding box for M2 index-resident incidence/degree queries (Hilbert range scans).
#[allow(dead_code)]
pub fn endpoint_index_bounds(
    endpoint: &EndpointRef,
    direction: DirectionFilter,
) -> (DimensionVector, DimensionVector) {
    endpoint_index_query_bounds(endpoint, direction)
}

/// Collect incident edge ids with layout-aware index filtering.
pub fn incident_edge_ids_directed(
    records: &[Record],
    endpoint: &EndpointRef,
    direction: DirectionFilter,
    registry_layout: EndpointIndexLayout,
) -> Vec<HyperedgeId> {
    collect_incident_edge_ids(records, endpoint, direction, registry_layout)
}

/// Index-resident degree count.
pub fn incident_edge_degree(
    records: &[Record],
    endpoint: &EndpointRef,
    direction: DirectionFilter,
    registry_layout: EndpointIndexLayout,
) -> usize {
    count_incident_edges(records, endpoint, direction, registry_layout)
}

/// Filter edges by direction after symmetric index fetch (M1 / V1-layout fallback).
pub fn filter_edges_by_direction(
    edges: Vec<Hyperedge>,
    endpoint: &EndpointRef,
    filter: DirectionFilter,
) -> Vec<Hyperedge> {
    if filter == DirectionFilter::Any {
        return edges;
    }
    edges
        .into_iter()
        .filter(|edge| {
            edge.endpoints.iter().any(|ep| {
                ep.space == endpoint.space
                    && ep.node.coords == endpoint.node.coords
                    && filter.matches(ep.polarity)
            })
        })
        .collect()
}

/// Partition incident ids into V2 (direction already index-filtered) and V1 (need post-filter).
pub fn partition_incident_ids_by_layout(
    records: &[Record],
    endpoint: &EndpointRef,
    ids: &[HyperedgeId],
) -> (Vec<HyperedgeId>, Vec<HyperedgeId>) {
    let prefix = endpoint_lookup_prefix(endpoint);
    let mut v2 = Vec::new();
    let mut v1 = Vec::new();
    for &id in ids {
        let is_v1 = records.iter().any(|r| {
            !r.tombstone
                && record_matches_endpoint_prefix(r, &prefix)
                && decode_hyperedge_id_from_index(&r.data)
                    .map(|(layout, rid)| rid == id && layout == EndpointIndexLayout::V1Symmetric)
                    .unwrap_or(false)
        });
        if is_v1 {
            v1.push(id);
        } else {
            v2.push(id);
        }
    }
    (v2, v1)
}

/// Plan V1 → V2 index row rewrites for lazy compaction migration.
pub fn plan_v1_to_v2_index_rewrite(
    records: &[Record],
    resolve: impl Fn(HyperedgeId) -> Option<Hyperedge>,
) -> Vec<HypergraphWriteRow> {
    let mut rows = Vec::new();
    for record in records {
        if record.tombstone {
            continue;
        }
        if index_record_layout(&record.data) != EndpointIndexLayout::V1Symmetric {
            continue;
        }
        let Some((_, edge_id)) = decode_hyperedge_id_from_index(&record.data) else {
            continue;
        };
        match resolve(edge_id) {
            Some(edge) => {
                if let Some(ep) = endpoint_for_v1_index_record(record, &edge) {
                    rows.push(HypergraphWriteRow {
                        space: ENDPOINT_INDEX_SPACE,
                        point: record.address.point.clone(),
                        data: vec![],
                        tombstone: true,
                    });
                    rows.push(HypergraphWriteRow {
                        space: ENDPOINT_INDEX_SPACE,
                        point: endpoint_index_point_for_layout(
                            ep,
                            edge_id,
                            EndpointIndexLayout::V2PolarityDim,
                        ),
                        data: encode_index_payload(edge_id, EndpointIndexLayout::V2PolarityDim),
                        tombstone: false,
                    });
                } else {
                    rows.push(HypergraphWriteRow {
                        space: ENDPOINT_INDEX_SPACE,
                        point: record.address.point.clone(),
                        data: vec![],
                        tombstone: true,
                    });
                }
            }
            None => {
                rows.push(HypergraphWriteRow {
                    space: ENDPOINT_INDEX_SPACE,
                    point: record.address.point.clone(),
                    data: vec![],
                    tombstone: true,
                });
            }
        }
    }
    rows
}

/// Active endpoint index layout from registry.
pub fn registry_index_layout(registry: &SpaceRegistry) -> EndpointIndexLayout {
    endpoint_index_layout_from_registry(registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::hyperedge::{
        Directionality, EndpointPolarity, EndpointRole, HyperedgeKind,
    };
    use std::collections::BTreeMap;

    fn sample_edge() -> Hyperedge {
        Hyperedge {
            id: HyperedgeId(7),
            kind: HyperedgeKind::new("link"),
            endpoints: vec![
                EndpointRef::new(
                    EndpointRole::new("a"),
                    SpaceId(1),
                    DimensionVector::new(vec![0, 0]),
                )
                .with_polarity(EndpointPolarity::Tail),
                EndpointRef::new(
                    EndpointRole::new("b"),
                    SpaceId(1),
                    DimensionVector::new(vec![1, 0]),
                )
                .with_polarity(EndpointPolarity::Head),
            ],
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: RevisionId::ZERO,
            valid_to: None,
            directionality: Directionality::Directed,
            authoring_frame: None,
            computation: None,
        }
    }

    #[test]
    fn prepare_writes_edge_plus_index_rows_v2() {
        let edge = sample_edge();
        let rows = prepare_writes(SpaceId(10), &edge, EndpointIndexLayout::V2PolarityDim).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].space, SpaceId(10));
        assert_eq!(rows[1].space, ENDPOINT_INDEX_SPACE);
        assert_eq!(
            index_record_layout(&rows[1].data),
            EndpointIndexLayout::V2PolarityDim
        );
    }

    #[test]
    fn prepare_deletes_tombstones_all_rows() {
        let rows = prepare_deletes(
            SpaceId(10),
            &sample_edge(),
            EndpointIndexLayout::V2PolarityDim,
        );
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| r.tombstone));
    }
}
