//! Peer track hardening — lossless legacy embedding (Wave B T7).

use infinite_db::infinitedb_core::hlc::{HlcStamp, LEGACY_PHYSICAL_CEILING};

#[test]
fn legacy_roundtrip_above_u32() {
    let n = (1u64 << 32) + 12345;
    let stamp = HlcStamp::legacy(n);
    assert_eq!(stamp.legacy_sequence(), n);
    assert!(stamp.is_legacy_embedded());
}

#[test]
fn legacy_order_preserved_across_u32_boundary() {
    let a = HlcStamp::legacy((1u64 << 32) - 1);
    let b = HlcStamp::legacy(1u64 << 32);
    assert!(a < b);
}

#[test]
fn legacy_ceiling_below_real_hlc_era() {
    let legacy_max = HlcStamp::legacy(u64::MAX);
    assert!(legacy_max.physical_ms < LEGACY_PHYSICAL_CEILING);
    let hlc = HlcStamp {
        physical_ms: 946_684_800_000, // year 2000
        logical: 0,
        session: 1,
        sequence: 0,
    };
    assert!(legacy_max < hlc);
}
