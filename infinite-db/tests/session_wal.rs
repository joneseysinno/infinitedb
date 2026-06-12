//! Peer track Phase 3 — per-session WAL integration tests.

use infinite_db::{InfiniteDb, OpenOptions};
use infinite_db::infinitedb_core::{
    address::{DimensionVector, SpaceId},
};
use infinite_db::infinitedb_storage::session_wal::SessionWalReader;
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
fn insert_with_session_survives_reopen() {
    let (db, dir) = open_db();
    let session = db.open_session();
    let sid = session.id();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![3, 4]),
        vec![42],
    )
    .unwrap();
    db.sync_session(&session).unwrap();

    let before_reopen = db.query(SpaceId(1), None).unwrap();
    assert_eq!(before_reopen.len(), 1, "record must be visible before reopen");

    let reopened = OpenOptions::default().open(dir.path()).unwrap();
    let rows = reopened.query(SpaceId(1), None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].data, vec![42]);
    assert_eq!(rows[0].revision.session(), sid.0);
    assert!(
        SessionWalReader::open(dir.path(), sid)
            .unwrap()
            .read_committed_entries()
            .unwrap()
            .is_empty(),
        "recovered session wal should be truncated after replay"
    );
}

#[test]
fn quarantine_corrupt_session_wal_does_not_block_open() {
    let dir = TempDir::new().unwrap();
    let db = OpenOptions::default().open(dir.path()).unwrap();
    db.register_space(infinite_db::infinitedb_core::space::SpaceConfig::new(
        SpaceId(1),
        "s",
        2,
    ))
    .unwrap();
    let session = db.open_session();
    let sid = session.id();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![1, 1]),
        vec![1],
    )
    .unwrap();
    db.sync_session(&session).unwrap();
    drop(db);

    let wal_path = dir.path().join("sessions").join(format!("{}.wal", sid.0));
    let mut bytes = std::fs::read(&wal_path).unwrap();
    bytes[0] ^= 0xFF;
    std::fs::write(&wal_path, bytes).unwrap();

    let reopened = OpenOptions::default().open(dir.path()).unwrap();
    assert!(reopened.quarantined_session_wals().contains_key(&sid.0));
    // Hot segment still holds the first open's durable apply; corrupt session WAL is skipped.
    assert_eq!(reopened.query(SpaceId(1), None).unwrap().len(), 1);
}

#[test]
fn retirement_requires_all_three_gates() {
    let (db, _dir) = open_db();
    let session = db.open_session();
    let sid = session.id();
    assert!(!db.try_retire_session_wal(sid).unwrap());
    db.mark_session_wal_sealed(sid);
    assert!(!db.try_retire_session_wal(sid).unwrap());
    db.mark_session_wal_replication_confirmed(sid);
    assert!(!db.try_retire_session_wal(sid).unwrap());
    db.mark_session_wal_collision_evaluated(sid);
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![0, 0]),
        vec![7],
    )
    .unwrap();
    db.sync_session(&session).unwrap();
    assert!(db.try_retire_session_wal(sid).unwrap());
}
