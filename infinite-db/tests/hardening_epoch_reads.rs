//! Peer track hardening — epoch-safe ReadTxn visibility (Wave A).

use infinite_db::{InfiniteDb, OpenOptions, WriteSession};
use infinite_db::infinitedb_core::address::{DimensionVector, SpaceId};
use tempfile::TempDir;

fn commit_session(db: &InfiniteDb, session: &WriteSession) {
    let durable = db.sync_session_wal(session).unwrap();
    if session.has_pending_intent() {
        db.commit_session_intent(session, &durable).unwrap();
    } else {
        db.sync().unwrap();
    }
}
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
fn read_txn_sees_session_write_after_sync() {
    let (db, _dir) = open_db();
    let session = db.open_session();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![1, 2]),
        vec![9],
    )
    .unwrap();
    commit_session(&db, &session);

    let txn = db.read();
    let rows = txn.query(SpaceId(1)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].data, vec![9]);
}

#[test]
fn read_txn_hides_session_minted_after_open() {
    let (db, _dir) = open_db();
    let txn = db.read();
    let vector = txn.version_vector().clone();

    let late = db.open_session();
    db.insert_with_session(
        &late,
        SpaceId(1),
        DimensionVector::new(vec![3, 3]),
        vec![99],
    )
    .unwrap();
    commit_session(&db, &late);

    assert!(vector.get(late.id()).is_none());
    let rows = txn.query(SpaceId(1)).unwrap();
    assert!(rows.is_empty());
}

#[test]
fn read_txn_repeatable_across_concurrent_writes() {
    let (db, _dir) = open_db();
    let s1 = db.open_session();
    let s2 = db.open_session();
    db.insert_with_session(
        &s1,
        SpaceId(1),
        DimensionVector::new(vec![1, 1]),
        vec![1],
    )
    .unwrap();
    commit_session(&db, &s1);

    let txn = db.read();
    assert_eq!(txn.query(SpaceId(1)).unwrap().len(), 1);

    db.insert_with_session(
        &s2,
        SpaceId(1),
        DimensionVector::new(vec![2, 2]),
        vec![2],
    )
    .unwrap();
    commit_session(&db, &s2);

    assert_eq!(txn.query(SpaceId(1)).unwrap().len(), 1);
    assert_eq!(db.query(SpaceId(1), None).unwrap().len(), 2);
}

#[test]
fn dormant_session_visible_after_reopen_and_new_session_write() {
    let (db, dir) = open_db();
    let s1 = db.open_session();
    db.insert_with_session(
        &s1,
        SpaceId(1),
        DimensionVector::new(vec![5, 5]),
        vec![5],
    )
    .unwrap();
    commit_session(&db, &s1);
    drop(db);

    let db = OpenOptions::default().open(dir.path()).unwrap();
    let s2 = db.open_session();
    db.insert_with_session(
        &s2,
        SpaceId(1),
        DimensionVector::new(vec![6, 6]),
        vec![6],
    )
    .unwrap();
    commit_session(&db, &s2);

    let txn = db.read();
    assert_eq!(txn.query(SpaceId(1)).unwrap().len(), 2);
}
