//! Spatial co-location index for judgments at subject coordinates (M5).

use super::{
    address::{DimensionVector, SpaceId},
    judgment::{ArbiterId, JudgmentId, SubjectIdentity, SubjectPin},
    space::SpaceConfig,
};

/// Reserved space for judgment → assertion co-location index.
pub const JUDGMENT_INDEX_SPACE: SpaceId = SpaceId(u64::MAX - 2);

pub const JUDGMENT_INDEX_DIMS: usize = 8;
pub const JUDGMENT_INDEX_BITS_PER_DIM: u32 = 7;

/// Space config for the reserved judgment index.
pub fn judgment_index_space_config() -> SpaceConfig {
    SpaceConfig::new(
        JUDGMENT_INDEX_SPACE,
        "__judgment_index__",
        JUDGMENT_INDEX_DIMS,
    )
    .with_bits_per_dim(JUDGMENT_INDEX_BITS_PER_DIM)
    .without_error_space()
}

/// Subject spatial prefix used for region scans (up to 4 coords).
pub fn subject_spatial_prefix(pin: &SubjectPin) -> Vec<u32> {
    match &pin.identity {
        SubjectIdentity::Hyperedge(id) => {
            let pt = super::hyperedge::Hyperedge::storage_point(*id);
            pt.coords
        }
        SubjectIdentity::Address(addr) => addr.point.coords.clone(),
    }
}

/// Index point: subject prefix (padded) + arbiter id + judgment id.
pub fn judgment_index_point(pin: &SubjectPin, arbiter: ArbiterId, id: JudgmentId) -> DimensionVector {
    let mut coords = subject_spatial_prefix(pin);
    while coords.len() < 4 {
        coords.push(0);
    }
    coords.push((arbiter.0 >> 32) as u32);
    coords.push((arbiter.0 & 0xFFFF_FFFF) as u32);
    coords.push((id.0 >> 32) as u32);
    coords.push((id.0 & 0xFFFF_FFFF) as u32);
    DimensionVector::new(coords)
}

/// Encode judgment id in index payload.
pub fn encode_judgment_index_payload(id: JudgmentId) -> Vec<u8> {
    id.0.to_le_bytes().to_vec()
}

pub fn decode_judgment_id_from_index(data: &[u8]) -> Option<JudgmentId> {
    if data.len() < 8 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[..8]);
    Some(JudgmentId(u64::from_le_bytes(buf)))
}

/// Whether an index record matches a subject prefix (region filter).
pub fn index_matches_subject_prefix(record_coords: &[u32], prefix: &[u32]) -> bool {
    if record_coords.len() < prefix.len() {
        return false;
    }
    record_coords[..prefix.len()] == *prefix
}
