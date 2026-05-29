//! Reverse endpoint index for hyperedge lookups.
//!
//! Maps each endpoint `(SpaceId, coords)` to the `HyperedgeId`s that reference
//! it. Records live in a reserved index space so spatial range scans over
//! endpoint position replace full hyperedge-space scans.

use bincode::{config::standard, encode_to_vec};
use super::{
    address::{DimensionVector, SpaceId},
    hyperedge::{EndpointRef, Hyperedge, HyperedgeId},
};

/// Reserved space for endpoint → hyperedge reverse index entries.
pub const ENDPOINT_INDEX_SPACE: SpaceId = SpaceId(u64::MAX - 1);

/// Dimension count: 2 (`SpaceId`) + 2 (`HyperedgeId`) + up to 12 endpoint coords.
pub const ENDPOINT_INDEX_DIMS: usize = 16;

/// Hilbert bits per dimension for the index space (`16 * 7 = 112 <= 128`).
pub const ENDPOINT_INDEX_BITS_PER_DIM: u32 = 7;

/// Build the index-space coordinate for an `(endpoint, hyperedge)` pair.
///
/// Layout: `[space_lo, space_hi, …endpoint.node.coords, edge_lo, edge_hi, pad…]`
/// so a subscope query on space+node does not pin the edge-id dimensions.
pub fn endpoint_index_point(endpoint: &EndpointRef, edge_id: HyperedgeId) -> DimensionVector {
    let mut coords = endpoint_lookup_prefix(endpoint);
    coords.push((edge_id.0 & 0xFFFF_FFFF) as u32);
    coords.push(((edge_id.0 >> 32) & 0xFFFF_FFFF) as u32);
    while coords.len() < ENDPOINT_INDEX_DIMS {
        coords.push(0);
    }
    DimensionVector::new(coords)
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

/// Bincode payload stored at each index record.
pub fn encode_hyperedge_id(id: HyperedgeId) -> Vec<u8> {
    encode_to_vec(id, standard()).expect("HyperedgeId must encode")
}

/// Decode a `HyperedgeId` from an index record payload.
pub fn decode_hyperedge_id(data: &[u8]) -> Option<HyperedgeId> {
    bincode::decode_from_slice(data, standard())
        .ok()
        .map(|(id, _)| id)
}

/// All endpoints referenced by a hyperedge.
pub fn edge_endpoints(edge: &Hyperedge) -> impl Iterator<Item = &EndpointRef> {
    edge.endpoints.iter()
}
