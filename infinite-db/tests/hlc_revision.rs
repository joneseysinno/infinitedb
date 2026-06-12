//! Peer track Phase 1 — HLC revision widening integration tests.

use infinite_db::OpenOptions;
use infinite_db::infinitedb_core::{
    address::RevisionId,
    hlc::HlcStamp,
};
use tempfile::TempDir;

#[test]
fn v4_database_open_and_allocate_legacy_embedded() {
    let dir = TempDir::new().unwrap();
    let db = OpenOptions::default().open(dir.path()).unwrap();
    let space = infinite_db::infinitedb_core::address::SpaceId(1);
    db.register_space(infinite_db::infinitedb_core::space::SpaceConfig::new(
        space, "s", 2,
    ))
    .unwrap();
    let rev = db
        .insert(
            space,
            infinite_db::infinitedb_core::address::DimensionVector::new(vec![1, 2]),
            vec![9],
        )
        .unwrap();
    assert!(rev.is_global_legacy());
    assert_eq!(rev.legacy_sequence(), 1);
    assert_eq!(rev.session(), 0);
    db.sync().unwrap();
    let reopened = OpenOptions::default().open(dir.path()).unwrap();
    assert!(reopened.revision().legacy_sequence() >= 1);
}

#[test]
fn legacy_orders_before_hlc_era_stamps() {
    let legacy = RevisionId::legacy(100);
    let hlc = RevisionId::from_stamp(HlcStamp {
        physical_ms: 1,
        logical: 0,
        session: 1,
        sequence: 0,
    });
    assert!(legacy < hlc);
}

#[test]
fn global_allocator_monotone_in_session_zero() {
    let a = RevisionId::legacy(1);
    let b = a.next_global();
    let c = b.next_global();
    assert!(a < b && b < c);
    assert_eq!(b.legacy_sequence(), 2);
    assert_eq!(c.legacy_sequence(), 3);
}
