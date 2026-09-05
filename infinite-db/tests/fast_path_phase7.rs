//! Peer track Phase 7 — timed fast-path integration tests.

use std::path::Path;
use std::sync::{atomic::Ordering, Mutex, MutexGuard};
use std::time::Duration;

static FAST_PATH_TEST_LOCK: Mutex<()> = Mutex::new(());

fn fast_path_test_lock() -> MutexGuard<'static, ()> {
    reset_fast_sync_test_hooks();
    FAST_PATH_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

use infinite_db::infinitedb_core::{
    address::{DimensionVector, SpaceId},
    error_record::ErrorKind,
    hlc::SessionId,
};
use infinite_db::infinitedb_storage::session_fast_segment::{
    SessionFastSegment, HEADER_LEN as FAST_HEADER_LEN,
};
use infinite_db::infinitedb_storage::session_wal::{SessionWalFrame, SessionWalReader};
use infinite_db::{InfiniteDb, OpenOptions, TimedFastPathPolicy, WriteSession};
use tempfile::TempDir;

fn commit_session(db: &InfiniteDb, session: &WriteSession) {
    let durable = db.sync_session_wal(session).unwrap();
    if session.has_pending_intent() {
        db.commit_session_intent(session, &durable).unwrap();
    } else {
        db.sync().unwrap();
    }
}

fn reset_fast_sync_test_hooks() {
    use infinite_db::infinitedb_storage::session_fast_segment::{
        TEST_FAIL_FAST_SYNC, TEST_FAIL_FAST_SYNC_ARMED,
    };
    TEST_FAIL_FAST_SYNC.store(false, Ordering::SeqCst);
    TEST_FAIL_FAST_SYNC_ARMED.store(false, Ordering::SeqCst);
}

fn register_main_space(db: &InfiniteDb) {
    db.register_space(infinite_db::infinitedb_core::space::SpaceConfig::new(
        SpaceId(1),
        "main",
        2,
    ))
    .unwrap();
}

fn open_db_default() -> (InfiniteDb, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = OpenOptions::default().open(dir.path()).unwrap();
    register_main_space(&db);
    (db, dir)
}

fn open_db_fast(deadline: Duration) -> (InfiniteDb, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut opts = OpenOptions::default();
    opts.timed_fast_path = TimedFastPathPolicy::enabled_with_deadline(deadline);
    let db = opts.open(dir.path()).unwrap();
    register_main_space(&db);
    (db, dir)
}

fn wal_data_frame_count(root: &Path, sid: SessionId) -> usize {
    SessionWalReader::open(root, sid)
        .unwrap()
        .read_committed_frames()
        .unwrap()
        .iter()
        .filter(|f| matches!(f, SessionWalFrame::Data(_)))
        .count()
}

fn fast_committed_bytes(root: &Path, sid: SessionId) -> u64 {
    let path = root.join("sessions").join(format!("{}.fast", sid.0));
    if !path.exists() {
        return 0;
    }
    SessionFastSegment::open(root, sid)
        .unwrap()
        .committed_bytes()
}

#[test]
fn fast_path_disabled_uses_wal() {
    let _guard = fast_path_test_lock();
    let (db, dir) = open_db_default();
    let session = db.open_session();
    let sid = session.id();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![1, 2]),
        vec![9],
    )
    .unwrap();
    db.sync_session_wal(&session).unwrap();
    assert_eq!(wal_data_frame_count(dir.path(), sid), 1);
    assert_eq!(fast_committed_bytes(dir.path(), sid), 0);
    let stats = db.session_write_stats();
    assert_eq!(stats.fast_path_seal_success, 0);
    assert_eq!(stats.fast_path_seal_timeout, 0);
}

#[test]
fn fast_path_seal_within_deadline() {
    let _guard = fast_path_test_lock();
    let (db, dir) = open_db_fast(Duration::from_secs(30));
    let session = db.open_session();
    let sid = session.id();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![3, 4]),
        vec![42],
    )
    .unwrap();
    assert_eq!(wal_data_frame_count(dir.path(), sid), 0);

    let durable = db.sync_session_wal(&session).unwrap();
    assert!(fast_committed_bytes(dir.path(), sid) > FAST_HEADER_LEN);
    assert_eq!(wal_data_frame_count(dir.path(), sid), 0);
    assert_eq!(db.query(SpaceId(1), None).unwrap().len(), 0);

    db.commit_session_intent(&session, &durable).unwrap();
    assert_eq!(db.query(SpaceId(1), None).unwrap().len(), 1);
    assert_eq!(db.query(SpaceId(1), None).unwrap()[0].data, vec![42]);

    let stats = db.session_write_stats();
    assert_eq!(stats.fast_path_seal_success, 1);
    assert_eq!(stats.fast_path_wal_fallback, 0);
}

#[test]
fn fast_path_timeout_falls_back_to_wal() {
    let _guard = fast_path_test_lock();
    let (db, dir) = open_db_fast(Duration::ZERO);
    let session = db.open_session();
    let sid = session.id();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![5, 6]),
        vec![7],
    )
    .unwrap();

    let durable = db.sync_session_wal(&session).unwrap();
    assert_eq!(wal_data_frame_count(dir.path(), sid), 1);
    assert_eq!(fast_committed_bytes(dir.path(), sid), FAST_HEADER_LEN);

    let stats = db.session_write_stats();
    assert_eq!(stats.fast_path_seal_timeout, 1);
    assert_eq!(stats.fast_path_wal_fallback, 1);

    db.commit_session_intent(&session, &durable).unwrap();
    assert_eq!(db.query(SpaceId(1), None).unwrap().len(), 1);
}

#[test]
fn fast_path_injected_sync_failure_surfaces_error() {
    let _guard = fast_path_test_lock();
    use infinite_db::infinitedb_storage::session_fast_segment::{
        TEST_FAIL_FAST_SYNC, TEST_FAIL_FAST_SYNC_ARMED,
    };

    let (db, _dir) = open_db_fast(Duration::from_secs(30));
    let session = db.open_session();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![8, 9]),
        vec![1],
    )
    .unwrap();

    TEST_FAIL_FAST_SYNC_ARMED.store(true, Ordering::SeqCst);
    TEST_FAIL_FAST_SYNC.store(true, Ordering::SeqCst);
    let err = db.sync_session_wal(&session).unwrap_err();
    TEST_FAIL_FAST_SYNC_ARMED.store(false, Ordering::SeqCst);
    assert!(
        err.to_string().contains("fast segment")
            || err.to_string().contains("fsync")
            || err.to_string().contains("injected"),
        "unexpected error: {err}"
    );
}

#[test]
fn fast_path_crash_before_checkpoint_quarantines() {
    let _guard = fast_path_test_lock();
    let (db, dir) = open_db_fast(Duration::from_secs(30));
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

    let reopened = OpenOptions::default()
        .open(dir.path())
        .unwrap();
    assert!(reopened.query(SpaceId(1), None).unwrap().is_empty());
    let errors = reopened
        .query_operation_errors(SpaceId(1), None, None)
        .unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].kind, ErrorKind::InterruptedSessionIntent);
    assert!(errors[0].entries[0].message.contains("without checkpoint"));
}

#[test]
fn fast_path_crash_after_commit_survives_reopen() {
    let _guard = fast_path_test_lock();
    let (db, dir) = open_db_fast(Duration::from_secs(30));
    let session = db.open_session();
    let sid = session.id();
    db.insert_with_session(
        &session,
        SpaceId(1),
        DimensionVector::new(vec![4, 4]),
        vec![42],
    )
    .unwrap();
    commit_session(&db, &session);

    let before_reopen = db.query(SpaceId(1), None).unwrap();
    assert_eq!(before_reopen.len(), 1);

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

// Regression gate: Phases 3–5 behaviors with fast path enabled.
#[test]
fn fast_path_regression_uncommitted_not_visible_until_checkpoint() {
    let _guard = fast_path_test_lock();
    let (db, _dir) = open_db_fast(Duration::from_secs(30));
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
fn fast_path_regression_commit_session_wal_and_intent() {
    let _guard = fast_path_test_lock();
    let (db, dir) = open_db_fast(Duration::from_secs(30));
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
fn fast_path_regression_durable_without_checkpoint_surfaces_error() {
    let _guard = fast_path_test_lock();
    let dir = TempDir::new().unwrap();
    let mut opts = OpenOptions::default();
    opts.timed_fast_path = TimedFastPathPolicy::enabled_with_deadline(Duration::from_secs(30));
    let db = opts.open(dir.path()).unwrap();
    register_main_space(&db);
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
}
