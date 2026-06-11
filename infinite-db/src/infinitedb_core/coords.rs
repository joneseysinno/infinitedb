//! Legacy coordinate packing codecs (hyperedge id, locator key).

use super::address::SpaceId;

/// 2D point encoding a [`HyperedgeId`](super::hyperedge::HyperedgeId) (legacy v1).
pub fn hyperedge_id_point(id: u64) -> [u32; 2] {
    [(id >> 32) as u32, (id & 0xFFFF_FFFF) as u32]
}

/// 4D locator key for `(edge space, hyperedge id)` (legacy v1).
pub fn hyperedge_locator_point(space: SpaceId, id: u64) -> [u32; 4] {
    [
        (space.0 >> 32) as u32,
        (space.0 & 0xFFFF_FFFF) as u32,
        (id >> 32) as u32,
        (id & 0xFFFF_FFFF) as u32,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hyperedge_id_roundtrip_bits() {
        let id = 0xABCD_EF12_3456_7890u64;
        let pt = hyperedge_id_point(id);
        assert_eq!(pt[0], 0xABCD_EF12);
        assert_eq!(pt[1], 0x3456_7890);
    }

    #[test]
    fn locator_point_splits_fields() {
        let space = SpaceId(0x0001_0000_0002);
        let id = 0x0003_0000_0004u64;
        let pt = hyperedge_locator_point(space, id);
        assert_eq!(pt, [1, 2, 3, 4]);
    }
}
