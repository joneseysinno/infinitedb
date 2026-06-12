//! Peer track hardening — revision-ranged WAL retirement (Wave B).

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
fn retirement_requires_gates_through_highest_revision() {
    let (db, _dir) = open_db();
    let session = db.open_session();
    let sid = session.id();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![0, 0]),
        vec![7],
    )
    .unwrap();
    commit_session(&db, &session);
    let through = db.stable_for_session(sid);

    assert!(!db.retire_session_wal(sid).unwrap());
    db.mark_session_wal_sealed(sid, through);
    db.mark_session_wal_collision_evaluated_through(sid, through);
    assert!(db.retire_session_wal(sid).unwrap());
}

#[test]
fn append_after_gates_blocks_retirement() {
    let (db, _dir) = open_db();
    let session = db.open_session();
    let sid = session.id();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![1, 1]),
        vec![1],
    )
    .unwrap();
    commit_session(&db, &session);
    let through = db.stable_for_session(sid);
    db.mark_session_wal_sealed(sid, through);
    db.mark_session_wal_collision_evaluated_through(sid, through);

    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![2, 2]),
        vec![2],
    )
    .unwrap();
    db.sync_session_wal(&session).unwrap();
    assert!(!db.retire_session_wal(sid).unwrap());
}
