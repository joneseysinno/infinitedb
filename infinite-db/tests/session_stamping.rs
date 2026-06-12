//! Peer track Phase 2 — session stamping and version-vector read pins.

use std::sync::Arc;
use std::thread;

use infinite_db::{InfiniteDb, OpenOptions};
use infinite_db::infinitedb_core::{
    address::{DimensionVector, SpaceId},
    hlc::{SessionId, GLOBAL_SESSION},
};
use tempfile::TempDir;

fn open_db() -> (InfiniteDb, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = OpenOptions::default().open(dir.path()).unwrap();
    db.register_space(infinite_db::infinitedb_core::space::SpaceConfig::new(
        SpaceId(1),
        "main",
        2,
    ))
    .unwrap();
    (db, dir)
}

#[test]
fn implicit_session_zero_stays_legacy_compatible() {
    let (db, _dir) = open_db();
    let rev = db
        .insert(
            SpaceId(1),
            DimensionVector::new(vec![1, 1]),
            vec![1],
        )
        .unwrap();
    assert!(rev.is_global_legacy());
    assert_eq!(rev.session(), GLOBAL_SESSION);
    assert_eq!(rev.legacy_sequence(), 1);
}

#[test]
fn open_session_mints_distinct_hlc_stamps() {
    let (db, _dir) = open_db();
    let s1 = db.open_session();
    let s2 = db.open_session();
    assert_ne!(s1.id(), s2.id());
    assert_ne!(s1.id().0, GLOBAL_SESSION);
    let a = s1.stamp();
    let b = s1.stamp();
    let c = s2.stamp();
    assert!(a < b);
    assert!(b < c || a.session() != c.session());
    assert!(!a.is_global_legacy());
    assert_eq!(a.session(), s1.id().0);
}

#[test]
fn concurrent_session_stamps_are_monotone_per_session() {
    let (db, _dir) = open_db();
    let db = Arc::new(db);
    let mut handles = Vec::new();
    for _ in 0..4 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let session = db.open_session();
            let mut prev = session.stamp();
            for _ in 0..200 {
                let next = session.stamp();
                assert!(prev < next);
                assert_eq!(next.session(), session.id().0);
                prev = next;
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn read_txn_pins_version_vector_scalar_meet() {
    let (db, _dir) = open_db();
    let txn = db.read();
    let vector = txn.version_vector();
    assert!(vector.get(SessionId(GLOBAL_SESSION)).is_some());
    assert_eq!(txn.version_vector().scalar_meet(), db.stable_revision());
}

#[test]
fn per_session_watermarks_are_independent() {
    let (db, _dir) = open_db();
    let slow = db.open_session();
    let fast = db.open_session();
    let held = slow.stamp();
    let fast_rev = fast.stamp();
    let slow_stable = db.stable_for_session(slow.id());
    let fast_stable = db.stable_for_session(fast.id());
    assert!(
        held > slow_stable,
        "outstanding slow revision should keep its session stable behind allocation"
    );
    assert!(
        fast_rev > fast_stable,
        "outstanding fast revision should not couple to the slow session watermark"
    );
    let vector = db.capture_version_vector();
    assert!(vector.get(slow.id()).is_some());
    assert!(vector.get(fast.id()).is_some());
    let expected = [
        slow_stable,
        fast_stable,
        db.stable_for_session(SessionId(GLOBAL_SESSION)),
    ]
    .into_iter()
    .min()
    .unwrap();
    assert_eq!(vector.scalar_meet(), expected);
}
