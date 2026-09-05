//! Reverse endpoint index for hyperedge lookups.
//!
//! Maps each endpoint `(SpaceId, coords)` to the `HyperedgeId`s that reference
//! it. Records live in a reserved index space so spatial range scans over
//! endpoint position replace full hyperedge-space scans.

use bincode::{config::standard, encode_to_vec};
use super::{
    address::{DimensionVector, RevisionId, SpaceId},
    block::Record,
    hyperedge::{EndpointPolarity, EndpointRef, Hyperedge, HyperedgeId},
    query::DirectionFilter,
    space::{EndpointIndexLayout, SpaceRegistry},
};

/// Reserved space for endpoint → hyperedge reverse index entries.
pub const ENDPOINT_INDEX_SPACE: SpaceId = SpaceId(u64::MAX - 1);

/// Dimension count: 2 (`SpaceId`) + 1 (`polarity`) + 2 (`HyperedgeId`) + up to 11 endpoint coords.
pub const ENDPOINT_INDEX_DIMS: usize = 16;

/// Hilbert bits per dimension for the index space (`16 * 8 = 128`).
/// Raised from 7 so default 8-bit data spaces satisfy INV-INDEX-PRECISION-DOMINATES.
pub const ENDPOINT_INDEX_BITS_PER_DIM: u32 = 8;

/// Payload tag for M2 index rows (polarity in coordinates).
pub const INDEX_PAYLOAD_V2_TAG: u8 = 0xB2;

/// Payload tag for V3 compact-key rows (id in payload only).
pub const INDEX_PAYLOAD_V3_TAG: u8 = 0xB3;

/// Stable polarity coordinate encoding for the index dimension.
pub fn polarity_coord(polarity: EndpointPolarity) -> u32 {
    match polarity {
        EndpointPolarity::Tail => 0,
        EndpointPolarity::Head => 1,
        EndpointPolarity::Neutral => 2,
    }
}

/// Decode a polarity coordinate; returns `None` if not a valid polarity value.
pub fn polarity_from_coord(coord: u32) -> Option<EndpointPolarity> {
    match coord {
        0 => Some(EndpointPolarity::Tail),
        1 => Some(EndpointPolarity::Head),
        2 => Some(EndpointPolarity::Neutral),
        _ => None,
    }
}

/// Index of the polarity dimension for a given endpoint prefix length.
pub fn polarity_index(endpoint_prefix_len: usize) -> usize {
    endpoint_prefix_len
}

/// Layout for the reserved endpoint index space from registry (defaults to V1).
pub fn endpoint_index_layout_from_registry(
    registry: &super::space::SpaceRegistry,
) -> EndpointIndexLayout {
    registry
        .get(ENDPOINT_INDEX_SPACE)
        .map(|c| c.endpoint_index_layout)
        .unwrap_or(EndpointIndexLayout::V1Symmetric)
}

/// Build the M1 index-space coordinate for an `(endpoint, hyperedge)` pair.
///
/// Layout: `[space_lo, space_hi, …endpoint.node.coords, edge_lo, edge_hi, pad…]`
pub fn endpoint_index_point(endpoint: &EndpointRef, edge_id: HyperedgeId) -> DimensionVector {
    let mut coords = endpoint_lookup_prefix(endpoint);
    coords.push((edge_id.0 & 0xFFFF_FFFF) as u32);
    coords.push(((edge_id.0 >> 32) & 0xFFFF_FFFF) as u32);
    pad_coords(&mut coords);
    DimensionVector::new(coords)
}

/// Build the M2 index-space coordinate with polarity between coords and edge id.
///
/// Layout: `[space_lo, space_hi, …endpoint.node.coords, polarity, edge_lo, edge_hi, pad…]`
pub fn endpoint_index_point_v2(endpoint: &EndpointRef, edge_id: HyperedgeId) -> DimensionVector {
    let mut coords = endpoint_lookup_prefix(endpoint);
    coords.push(polarity_coord(endpoint.polarity));
    coords.push((edge_id.0 & 0xFFFF_FFFF) as u32);
    coords.push(((edge_id.0 >> 32) & 0xFFFF_FFFF) as u32);
    pad_coords(&mut coords);
    DimensionVector::new(coords)
}

/// Index point for the active registry layout.
pub fn endpoint_index_point_for_layout(
    endpoint: &EndpointRef,
    edge_id: HyperedgeId,
    layout: EndpointIndexLayout,
) -> DimensionVector {
    match layout {
        EndpointIndexLayout::V1Symmetric => endpoint_index_point(endpoint, edge_id),
        EndpointIndexLayout::V2PolarityDim => endpoint_index_point_v2(endpoint, edge_id),
        // Registry-less fallback: ordinal 0. Callers holding a registry must use
        // `endpoint_index_point_v3_for_registry` so the ordinal is interned.
        EndpointIndexLayout::V3CompactKey => endpoint_index_point_v3(
            endpoint,
            0,
            edge_id,
            ENDPOINT_INDEX_BITS_PER_DIM,
        ),
    }
}

/// V3 compact key: interned ordinal + geometry + polarity + packed `HyperedgeId`.
///
/// The edge id here is a *discriminator*, not an identifier — it exists so two edges on
/// one endpoint do not share an address. The authoritative id lives in the row payload
/// (`encode_index_payload`), which is what every reader decodes.
///
/// Remaining dimensions after the geometry prefix hold a little-endian packing of
/// `edge_id.0` at `bits_per_dim` per dimension. A 2-coordinate endpoint leaves 12 dims ×
/// 8 bits = 96 bits, carrying the id whole; the 11-coordinate worst case leaves 3 dims =
/// 24 bits, where ids agreeing in their low bits on one endpoint would collide. Endpoint
/// dimensionality × precision × discriminator width ≤ 128 is the standing budget.
///
/// `valid_from` is deliberately NOT the discriminator: it is a validity stamp two edges
/// may legitimately share — every edge built with `RevisionId::ZERO` does.
pub fn endpoint_index_point_v3(
    endpoint: &EndpointRef,
    space_ordinal: u32,
    edge_id: HyperedgeId,
    bits_per_dim: u32,
) -> DimensionVector {
    let mut coords = endpoint_lookup_prefix_v3(endpoint, space_ordinal);
    coords.push(polarity_coord(endpoint.polarity));
    let remaining = ENDPOINT_INDEX_DIMS.saturating_sub(coords.len());
    coords.extend(pack_u64_le(edge_id.0, remaining, bits_per_dim));
    pad_coords(&mut coords);
    DimensionVector::new(coords)
}

pub fn endpoint_index_point_v3_for_registry(
    endpoint: &EndpointRef,
    edge_id: HyperedgeId,
    layout: EndpointIndexLayout,
    registry: &SpaceRegistry,
    valid_from: RevisionId,
) -> DimensionVector {
    match layout {
        EndpointIndexLayout::V3CompactKey => {
            let ordinal = registry.space_ordinal(endpoint.space).unwrap_or(0);
            let bits = registry
                .get(ENDPOINT_INDEX_SPACE)
                .map(|c| c.bits_per_dim)
                .unwrap_or(ENDPOINT_INDEX_BITS_PER_DIM);
            let _ = valid_from;
            endpoint_index_point_v3(endpoint, ordinal, edge_id, bits)
        }
        _ => endpoint_index_point_for_layout(endpoint, edge_id, layout),
    }
}

fn pack_u64_le(value: u64, dims: usize, bits_per_dim: u32) -> Vec<u32> {
    let bits = bits_per_dim.min(32);
    let mask = if bits >= 32 {
        u32::MAX
    } else {
        (1u32 << bits) - 1
    };
    let mut rest = value;
    let mut out = Vec::with_capacity(dims);
    for _ in 0..dims {
        out.push((rest as u32) & mask);
        rest = rest.checked_shr(bits).unwrap_or(0);
    }
    out
}

fn coord_ceiling(bits_per_dim: u32) -> u32 {
    if bits_per_dim >= 32 {
        u32::MAX
    } else {
        (1u32 << bits_per_dim) - 1
    }
}

fn pad_coords(coords: &mut Vec<u32>) {
    while coords.len() < ENDPOINT_INDEX_DIMS {
        coords.push(0);
    }
}

/// Coordinate prefix for all edges touching an endpoint (space + node, no edge id).
pub fn endpoint_lookup_prefix(endpoint: &EndpointRef) -> Vec<u32> {
    let mut prefix = vec![
        (endpoint.space.0 & 0xFFFF_FFFF) as u32,
        ((endpoint.space.0 >> 32) & 0xFFFF_FFFF) as u32,
    ];
    prefix.extend_from_slice(&endpoint.node.coords);
    prefix
}

/// V3 lookup prefix: interned ordinal + endpoint geometry (no identity halves).
pub fn endpoint_lookup_prefix_v3(endpoint: &EndpointRef, space_ordinal: u32) -> Vec<u32> {
    let mut prefix = vec![space_ordinal];
    prefix.extend_from_slice(&endpoint.node.coords);
    prefix
}

/// Polarity min/max for a direction filter (M2 range scans).
pub fn polarity_range_for_filter(direction: DirectionFilter) -> (u32, u32) {
    match direction {
        DirectionFilter::Any => (0, 2),
        DirectionFilter::Outgoing => (0, 0),
        DirectionFilter::Incoming => (1, 1),
        DirectionFilter::NeutralOnly => (2, 2),
    }
}

/// Bounding box for M2 index queries over endpoint + optional direction pin.
pub fn endpoint_index_query_bounds(
    endpoint: &EndpointRef,
    direction: DirectionFilter,
) -> (DimensionVector, DimensionVector) {
    endpoint_index_query_bounds_with_bits(
        endpoint,
        direction,
        ENDPOINT_INDEX_BITS_PER_DIM,
    )
}

/// Query bbox with Hilbert coordinate ceiling clamped to `(1 << bits_per_dim) - 1`.
pub fn endpoint_index_query_bounds_with_bits(
    endpoint: &EndpointRef,
    direction: DirectionFilter,
    bits_per_dim: u32,
) -> (DimensionVector, DimensionVector) {
    let prefix = endpoint_lookup_prefix(endpoint);
    let (pol_lo, pol_hi) = polarity_range_for_filter(direction);
    let ceil = coord_ceiling(bits_per_dim);
    let mut min_coords = prefix.clone();
    min_coords.push(pol_lo);
    min_coords.push(0);
    min_coords.push(0);
    pad_coords(&mut min_coords);

    let mut max_coords = prefix;
    max_coords.push(pol_hi);
    max_coords.push(ceil);
    max_coords.push(ceil);
    while max_coords.len() < ENDPOINT_INDEX_DIMS {
        max_coords.push(ceil);
    }

    (
        DimensionVector::new(min_coords),
        DimensionVector::new(max_coords),
    )
}

/// V3 query bbox keyed by interned ordinal rather than raw SpaceId halves.
pub fn endpoint_index_query_bounds_v3(
    endpoint: &EndpointRef,
    space_ordinal: u32,
    direction: DirectionFilter,
    bits_per_dim: u32,
) -> (DimensionVector, DimensionVector) {
    let prefix = endpoint_lookup_prefix_v3(endpoint, space_ordinal);
    let (pol_lo, pol_hi) = polarity_range_for_filter(direction);
    let ceil = coord_ceiling(bits_per_dim);
    let mut min_coords = prefix.clone();
    min_coords.push(pol_lo);
    while min_coords.len() < ENDPOINT_INDEX_DIMS {
        min_coords.push(0);
    }
    let mut max_coords = prefix;
    max_coords.push(pol_hi);
    while max_coords.len() < ENDPOINT_INDEX_DIMS {
        max_coords.push(ceil);
    }
    (
        DimensionVector::new(min_coords),
        DimensionVector::new(max_coords),
    )
}

/// Bincode payload stored at each M1 index record.
pub fn encode_hyperedge_id(id: HyperedgeId) -> Vec<u8> {
    encode_to_vec(id, standard()).expect("HyperedgeId must encode")
}

/// M2 index payload: layout tag + `HyperedgeId`.
pub fn encode_hyperedge_id_v2(id: HyperedgeId) -> Vec<u8> {
    let mut data = vec![INDEX_PAYLOAD_V2_TAG];
    data.extend(encode_hyperedge_id(id));
    data
}

/// Encode index payload for the active layout.
pub fn encode_index_payload(id: HyperedgeId, layout: EndpointIndexLayout) -> Vec<u8> {
    match layout {
        EndpointIndexLayout::V1Symmetric => encode_hyperedge_id(id),
        EndpointIndexLayout::V2PolarityDim => encode_hyperedge_id_v2(id),
        EndpointIndexLayout::V3CompactKey => encode_hyperedge_id_v3(id),
    }
}

/// M3 index payload: layout tag + `HyperedgeId` (id is not in the curve key).
pub fn encode_hyperedge_id_v3(id: HyperedgeId) -> Vec<u8> {
    let mut data = vec![INDEX_PAYLOAD_V3_TAG];
    data.extend(encode_hyperedge_id(id));
    data
}

/// Detect index row layout from payload bytes.
pub fn index_record_layout(data: &[u8]) -> EndpointIndexLayout {
    match data.first() {
        Some(&INDEX_PAYLOAD_V3_TAG) => EndpointIndexLayout::V3CompactKey,
        Some(&INDEX_PAYLOAD_V2_TAG) => EndpointIndexLayout::V2PolarityDim,
        _ => EndpointIndexLayout::V1Symmetric,
    }
}

/// Decode `HyperedgeId` and layout from an index record payload.
pub fn decode_hyperedge_id_from_index(data: &[u8]) -> Option<(EndpointIndexLayout, HyperedgeId)> {
    match data.first() {
        Some(&INDEX_PAYLOAD_V3_TAG) => bincode::decode_from_slice(&data[1..], standard())
            .ok()
            .map(|(id, _)| (EndpointIndexLayout::V3CompactKey, id)),
        Some(&INDEX_PAYLOAD_V2_TAG) => bincode::decode_from_slice(&data[1..], standard())
            .ok()
            .map(|(id, _)| (EndpointIndexLayout::V2PolarityDim, id)),
        _ => decode_hyperedge_id(data).map(|id| (EndpointIndexLayout::V1Symmetric, id)),
    }
}

/// Decode a `HyperedgeId` from an index record payload (layout-agnostic).
pub fn decode_hyperedge_id(data: &[u8]) -> Option<HyperedgeId> {
    bincode::decode_from_slice(data, standard())
        .ok()
        .map(|(id, _)| id)
}

/// All endpoints referenced by a hyperedge.
pub fn edge_endpoints(edge: &Hyperedge) -> impl Iterator<Item = &EndpointRef> {
    edge.endpoints.iter()
}

/// Whether `record` matches the endpoint coordinate prefix.
pub fn record_matches_endpoint_prefix(record: &Record, prefix: &[u32]) -> bool {
    prefix.iter().enumerate().all(|(i, &v)| {
        record.address.point.coords.get(i) == Some(&v)
    })
}

/// Collect hyperedge ids from index records matching a coordinate prefix (M1 scan).
pub fn incident_edge_ids_from_prefix(
    records: &[Record],
    prefix: &[u32],
) -> Vec<HyperedgeId> {
    records
        .iter()
        .filter(|r| !r.tombstone)
        .filter(|r| record_matches_endpoint_prefix(r, prefix))
        .filter_map(|r| decode_hyperedge_id_from_index(&r.data).map(|(_, id)| id))
        .collect()
}

/// Collect incident edge ids with layout-aware filtering and V1/V2/V3 dual-layout reads.
pub fn collect_incident_edge_ids(
    records: &[Record],
    endpoint: &EndpointRef,
    direction: DirectionFilter,
    registry_layout: EndpointIndexLayout,
) -> Vec<HyperedgeId> {
    collect_incident_edge_ids_for_ordinal(records, endpoint, direction, registry_layout, 0)
}

/// Like [`collect_incident_edge_ids`] but matches V3 rows for `space_ordinal`.
pub fn collect_incident_edge_ids_for_ordinal(
    records: &[Record],
    endpoint: &EndpointRef,
    direction: DirectionFilter,
    registry_layout: EndpointIndexLayout,
    space_ordinal: u32,
) -> Vec<HyperedgeId> {
    let prefix_v12 = endpoint_lookup_prefix(endpoint);
    let prefix_v3 = endpoint_lookup_prefix_v3(endpoint, space_ordinal);
    let mut ids = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for record in records {
        if record.tombstone {
            continue;
        }
        let Some((record_layout, id)) = decode_hyperedge_id_from_index(&record.data) else {
            continue;
        };
        let is_v3 = record_layout == EndpointIndexLayout::V3CompactKey
            && record_matches_endpoint_prefix(record, &prefix_v3);
        let is_v12 = record_layout != EndpointIndexLayout::V3CompactKey
            && record_matches_endpoint_prefix(record, &prefix_v12);
        if !is_v3 && !is_v12 {
            continue;
        }

        if record_layout == EndpointIndexLayout::V2PolarityDim
            || record_layout == EndpointIndexLayout::V3CompactKey
        {
            if direction != DirectionFilter::Any {
                let pol_idx = if is_v3 {
                    prefix_v3.len()
                } else {
                    polarity_index(prefix_v12.len())
                };
                let Some(pol_coord) = record.address.point.coords.get(pol_idx) else {
                    continue;
                };
                let Some(polarity) = polarity_from_coord(*pol_coord) else {
                    continue;
                };
                if !direction.matches(polarity) {
                    continue;
                }
            }
            if seen.insert(id) {
                ids.push(id);
            }
            continue;
        }

        match (registry_layout, record_layout) {
            (EndpointIndexLayout::V2PolarityDim, EndpointIndexLayout::V1Symmetric)
            | (EndpointIndexLayout::V3CompactKey, EndpointIndexLayout::V1Symmetric)
            | (EndpointIndexLayout::V1Symmetric, EndpointIndexLayout::V1Symmetric) => {
                if seen.insert(id) {
                    ids.push(id);
                }
            }
            _ => {}
        }
    }

    ids
}

/// Count incident edges index-resident (no hyperedge payload decode).
pub fn count_incident_edges(
    records: &[Record],
    endpoint: &EndpointRef,
    direction: DirectionFilter,
    registry_layout: EndpointIndexLayout,
) -> usize {
    collect_incident_edge_ids_for_ordinal(
        records,
        endpoint,
        direction,
        registry_layout,
        0,
    )
    .len()
}

/// Find the endpoint on `edge` that matches a V1 index record address.
pub fn endpoint_for_v1_index_record<'a>(
    record: &Record,
    edge: &'a Hyperedge,
) -> Option<&'a EndpointRef> {
    edge.endpoints.iter().find(|ep| {
        endpoint_index_point(ep, edge.id).coords == record.address.point.coords
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::DimensionVector;
    use crate::infinitedb_core::hyperedge::EndpointRole;

    fn sample_endpoint(polarity: EndpointPolarity) -> EndpointRef {
        EndpointRef::new(
            EndpointRole::new("n"),
            SpaceId(1),
            DimensionVector::new(vec![5, 0]),
        )
        .with_polarity(polarity)
    }

    #[test]
    fn v2_inserts_polarity_between_coords_and_edge_id() {
        let ep = sample_endpoint(EndpointPolarity::Head);
        let prefix_len = endpoint_lookup_prefix(&ep).len();
        let point = endpoint_index_point_v2(&ep, HyperedgeId(0xAABB_CCDD_EEFF_0011));
        assert_eq!(point.coords[prefix_len], polarity_coord(EndpointPolarity::Head));
        assert_eq!(point.coords[prefix_len + 1], 0xEEFF_0011);
        assert_eq!(point.coords[prefix_len + 2], 0xAABB_CCDD);
        assert_eq!(point.coords.len(), ENDPOINT_INDEX_DIMS);
    }

    #[test]
    fn v2_payload_tag_roundtrip() {
        let id = HyperedgeId(99);
        let data = encode_hyperedge_id_v2(id);
        assert_eq!(index_record_layout(&data), EndpointIndexLayout::V2PolarityDim);
        let (layout, decoded) = decode_hyperedge_id_from_index(&data).unwrap();
        assert_eq!(layout, EndpointIndexLayout::V2PolarityDim);
        assert_eq!(decoded, id);
    }

    #[test]
    fn query_bounds_pin_outgoing_polarity() {
        let ep = sample_endpoint(EndpointPolarity::Tail);
        let (min, max) = endpoint_index_query_bounds(&ep, DirectionFilter::Outgoing);
        let prefix_len = endpoint_lookup_prefix(&ep).len();
        assert_eq!(min.coords[prefix_len], 0);
        assert_eq!(max.coords[prefix_len], 0);
    }

    #[test]
    fn query_bounds_clamp_to_bits_ceiling() {
        let ep = sample_endpoint(EndpointPolarity::Tail);
        let (_min, max) = endpoint_index_query_bounds(&ep, DirectionFilter::Any);
        let ceil = (1u32 << ENDPOINT_INDEX_BITS_PER_DIM) - 1;
        assert!(max.coords.iter().all(|&c| c <= ceil));
    }

    #[test]
    fn v3_point_fits_bits_and_round_trips_payload() {
        let ep = sample_endpoint(EndpointPolarity::Head);
        let point = endpoint_index_point_v3(
            &ep,
            3,
            HyperedgeId(200),
            ENDPOINT_INDEX_BITS_PER_DIM,
        );
        let ceil = (1u32 << ENDPOINT_INDEX_BITS_PER_DIM) - 1;
        assert_eq!(point.coords.len(), ENDPOINT_INDEX_DIMS);
        assert!(point.coords.iter().all(|&c| c <= ceil));
        assert_eq!(point.coords[0], 3);
        let data = encode_hyperedge_id_v3(HyperedgeId(200));
        let (layout, id) = decode_hyperedge_id_from_index(&data).unwrap();
        assert_eq!(layout, EndpointIndexLayout::V3CompactKey);
        assert_eq!(id, HyperedgeId(200));
    }
}
