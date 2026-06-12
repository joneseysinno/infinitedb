//! Peer track Phase 4 — intent checkpoint integration tests.

use infinite_db::infinitedb_core::{
    address::{DimensionVector, SpaceId},
    error_record::ErrorKind,
    intent_checkpoint::IntentOperationKind,
};
use infinite_db::infinitedb_storage::session_wal::{SessionWalFrame, SessionWalReader};
use infinite_db::{InfiniteDb, OpenOptions};
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
fn uncommitted_session_writes_not_visible_until_checkpoint() {
    let (db, _dir) = open_db();
    let session = db.open_session();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![1, 2]),
        vec![9],
    )
    .unwrap();
    assert_eq!(db.query(SpaceId(1), None).unwrap().len(), 0);

    let durable = db.sync_session_wal(&session).unwrap();
    assert_eq!(db.query(SpaceId(1), None).unwrap().len(), 0);

    db.commit_session_intent(&session, &durable).unwrap();
    assert_eq!(db.query(SpaceId(1), None).unwrap().len(), 1);
}

#[test]
fn commit_session_wal_and_intent_persists_on_reopen() {
    let (db, dir) = open_db();
    let session = db.open_session();
    let sid = session.id();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![4, 4]),
        vec![42],
    )
    .unwrap();
    let durable = db.sync_session_wal(&session).unwrap();
    db.commit_session_intent(&session, &durable).unwrap();
    assert_eq!(db.query(SpaceId(1), None).unwrap().len(), 1);

    let reopened = OpenOptions::default().open(dir.path()).unwrap();
    assert_eq!(reopened.query(SpaceId(1), None).unwrap().len(), 1);
    assert_eq!(
        reopened.query(SpaceId(1), None).unwrap()[0]
            .revision
            .session(),
        sid.0
    );
}

#[test]
fn durable_without_checkpoint_surfaces_error_on_reopen() {
    let dir = TempDir::new().unwrap();
    let db = OpenOptions::default().open(dir.path()).unwrap();
    db.register_space(infinite_db::infinitedb_core::space::SpaceConfig::new(
        SpaceId(1),
        "s",
        2,
    ))
    .unwrap();
    let session = db.open_session();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![0, 1]),
        vec![1],
    )
    .unwrap();
    db.sync_session_wal(&session).unwrap();
    drop(db);

    let reopened = OpenOptions::default().open(dir.path()).unwrap();
    assert!(reopened.query(SpaceId(1), None).unwrap().is_empty());
    let errors = reopened
        .query_operation_errors(SpaceId(1), None, None)
        .unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].kind, ErrorKind::InterruptedSessionIntent);
    assert_eq!(errors[0].source_space, SpaceId(1));
    assert!(errors[0].entries[0].message.contains("without checkpoint"));
}

#[test]
fn commit_requires_durable_wal_sync() {
    let (db, _dir) = open_db();
    let session = db.open_session();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![2, 2]),
        vec![3],
    )
    .unwrap();
    let durable = db.sync_session_wal(&session).unwrap();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![2, 3]),
        vec![4],
    )
    .unwrap();
    assert!(db.commit_session_intent(&session, &durable).is_err());
}

#[test]
fn session_wal_retains_intent_checkpoint_frame() {
    let (db, dir) = open_db();
    let session = db.open_session();
    let sid = session.id();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![7, 7]),
        vec![7],
    )
    .unwrap();
    let durable = db.sync_session_wal(&session).unwrap();
    let outcome = db.commit_session_intent(&session, &durable).unwrap();
    assert_eq!(outcome.checkpoint.kind, IntentOperationKind::Insert);

    let frames = SessionWalReader::open(dir.path(), sid)
        .unwrap()
        .read_committed_frames()
        .unwrap();
    assert!(frames.iter().any(
        |f| matches!(f, SessionWalFrame::Intent(_))
    ));
}
