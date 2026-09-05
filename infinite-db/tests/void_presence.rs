//! Void algebra integration tests (VOID_PLAN V2, V5, V7, V9).

use infinite_db::infinitedb_core::address::{DimensionVector, SpaceId};
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::infinitedb_core::void::{Presence, VoidOr};
use infinite_db::InfiniteDb;
use tempfile::TempDir;

fn space(id: u64) -> SpaceConfig {
    SpaceConfig::new(SpaceId(id), format!("s{id}"), 2)
}

fn point(x: u32, y: u32) -> DimensionVector {
    DimensionVector::new(vec![x, y])
}

#[test]
fn inv_void_decidable_never_written() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(space(1)).unwrap();
    db.sync().unwrap();
    let p = db.presence_at(SpaceId(1), point(1, 2), None).unwrap();
    assert!(matches!(p, Presence::Void));
}

#[test]
fn inv_void_tombstone_distinct_write_delete() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(space(1)).unwrap();
    let addr = point(3, 4);

    db.insert(SpaceId(1), addr.clone(), vec![9]).unwrap();
    db.sync().unwrap();
    let after_write = db.presence_at(SpaceId(1), addr.clone(), None).unwrap();
    assert!(matches!(after_write, Presence::Present(_)));

    let rev_before_delete = db.revision();
    db.delete(SpaceId(1), addr.clone()).unwrap();
    db.sync().unwrap();

    let steady = db.query(SpaceId(1), None).unwrap();
    assert!(steady.is_empty());

    let tomb = db.presence_at(SpaceId(1), addr.clone(), None).unwrap();
    assert!(matches!(tomb, Presence::Tombstoned { .. }));

    let still_present = db
        .presence_at(SpaceId(1), addr.clone(), Some(rev_before_delete))
        .unwrap();
    assert!(matches!(still_present, Presence::Present(_)));
}

#[test]
fn inv_void_polymorphic_container_levels() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(space(1)).unwrap();
    db.sync().unwrap();

    assert!(db.space_is_void_on_branch(infinite_db::infinitedb_core::branch::BranchId::MAIN, SpaceId(1)).unwrap());

    db.insert(SpaceId(1), point(0, 0), vec![1]).unwrap();
    db.sync().unwrap();
    assert!(!db.space_is_void_on_branch(infinite_db::infinitedb_core::branch::BranchId::MAIN, SpaceId(1)).unwrap());

    db.delete(SpaceId(1), point(0, 0)).unwrap();
    db.sync().unwrap();
    assert!(!db.space_is_void_on_branch(infinite_db::infinitedb_core::branch::BranchId::MAIN, SpaceId(1)).unwrap());
    assert!(db.query(SpaceId(1), None).unwrap().is_empty());
}

#[test]
fn inv_void_div_undefined_density_void() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(space(1)).unwrap();
    assert!(matches!(db.space_density(SpaceId(1)), VoidOr::Void));
}

#[test]
#[cfg(feature = "server")]
fn server_get_presence_round_trip() {
    use infinite_db::infinitedb_core::address::RevisionId;
    use infinite_db::{handle_request, Request, Response};
    use infinite_db::infinitedb_core::branch::BranchId;
    use infinite_db::infinitedb_core::snapshot::SnapshotId;
    use infinite_db::infinitedb_server::session::{AccessLevel, Session, SpaceGrant};

    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(space(1)).unwrap();
    db.insert(SpaceId(1), point(5, 5), vec![7]).unwrap();
    db.sync().unwrap();

    let session = Session::new(
        BranchId::MAIN,
        SnapshotId(0),
        RevisionId::legacy(0),
        vec![SpaceGrant {
            space: SpaceId(1),
            level: AccessLevel::ReadOnly,
        }],
    );

    let void_resp = handle_request(
        &db,
        &session,
        Request::GetPresence {
            space: SpaceId(1),
            point: point(99, 99),
            as_of: None,
        },
    );
    assert!(matches!(void_resp, Response::Presence(Presence::Void)));

    let present_resp = handle_request(
        &db,
        &session,
        Request::GetPresence {
            space: SpaceId(1),
            point: point(5, 5),
            as_of: None,
        },
    );
    assert!(matches!(present_resp, Response::Presence(Presence::Present(_))));
}
