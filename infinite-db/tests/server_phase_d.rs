//! Phase D: TCP server, conflict queue, and branch-aware replication.

use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use infinite_db::infinitedb_core::address::{DimensionVector, SpaceId};
use infinite_db::infinitedb_core::branch::BranchId;
use infinite_db::infinitedb_core::merge::MergeStrategy;
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::{
    admin_grants, client_roundtrip, converge_main_records, converge_with_branch_merge,
    snapshot_merkle, InfiniteDb, ConflictQueue, Request, Response, Server, ServerConfig,
};
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn space(id: u64) -> SpaceConfig {
    SpaceConfig::new(SpaceId(id), format!("s{id}"), 2).with_shard_bits(2)
}

fn spawn_server(db: Arc<InfiniteDb>, space_ids: &[u64]) -> (SocketAddr, thread::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel();
    let grants = admin_grants(space_ids);
    let handle = thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = Server::bind(
                "127.0.0.1:0".parse().unwrap(),
                db,
                ServerConfig::default(),
                grants,
            )
            .await
            .unwrap();
            tx.send(server.local_addr().unwrap()).unwrap();
            let _ = server.run().await;
        });
    });
    let addr = rx.recv().unwrap();
    thread::sleep(Duration::from_millis(30));
    (addr, handle)
}

#[test]
fn tcp_roundtrip_ping() {
    let _dir = TempDir::new().unwrap();
    let db = Arc::new(InfiniteDb::open(_dir.path()).unwrap());
    db.register_space(space(1)).unwrap();

    let (addr, server) = spawn_server(Arc::clone(&db), &[1]);
    let rt = Runtime::new().unwrap();
    let resp = rt.block_on(client_roundtrip(addr, Request::Ping)).unwrap();
    assert!(matches!(resp, Response::Pong));
    drop(server);
}

#[test]
fn tcp_query_write_roundtrip() {
    let _dir = TempDir::new().unwrap();
    let db = Arc::new(InfiniteDb::open(_dir.path()).unwrap());
    db.register_space(space(1)).unwrap();

    let (addr, server) = spawn_server(Arc::clone(&db), &[1]);
    let rt = Runtime::new().unwrap();

    let write_resp = rt
        .block_on(client_roundtrip(
            addr,
            Request::Write {
                address: infinite_db::infinitedb_core::address::Address::new(
                    SpaceId(1),
                    DimensionVector::new(vec![1, 2]),
                ),
                revision: infinite_db::infinitedb_core::address::RevisionId(1),
                data: vec![42],
            },
        ))
        .unwrap();
    assert!(matches!(write_resp, Response::WriteAck { .. }));

    db.sync().unwrap();
    let query_resp = rt
        .block_on(client_roundtrip(
            addr,
            Request::Query {
                space: SpaceId(1),
                snapshot: infinite_db::infinitedb_core::snapshot::SnapshotId(0),
                key_range: None,
                as_of: None,
                include_tombstones: false,
            },
        ))
        .unwrap();
    match query_resp {
        Response::Records(records) => assert!(!records.is_empty()),
        other => panic!("expected records, got {other:?}"),
    }
    drop(server);
}

#[test]
fn concurrent_connections_parallel_writes() {
    let _dir = TempDir::new().unwrap();
    let db = Arc::new(InfiniteDb::open(_dir.path()).unwrap());
    db.register_space(space(1)).unwrap();

    let (addr, server) = spawn_server(Arc::clone(&db), &[1]);

    let mut handles = Vec::new();
    for i in 0..10u64 {
        handles.push(thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let write = rt
                .block_on(client_roundtrip(
                    addr,
                    Request::Write {
                        address: infinite_db::infinitedb_core::address::Address::new(
                            SpaceId(1),
                            DimensionVector::new(vec![i as u32, i as u32 + 10]),
                        ),
                        revision: infinite_db::infinitedb_core::address::RevisionId(1),
                        data: vec![i as u8],
                    },
                ))
                .unwrap();
            assert!(matches!(write, Response::WriteAck { .. }));
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    db.sync().unwrap();
    assert_eq!(db.query(SpaceId(1), None).unwrap().len(), 10);
    drop(server);
}

#[test]
fn two_node_sync_merge() {
    let _dir_a = TempDir::new().unwrap();
    let _dir_b = TempDir::new().unwrap();
    let space_id = SpaceId(1);

    let a = InfiniteDb::open(_dir_a.path()).unwrap();
    let b = InfiniteDb::open(_dir_b.path()).unwrap();
    a.register_space(space(1)).unwrap();
    b.register_space(space(1)).unwrap();

    a.insert(space_id, DimensionVector::new(vec![1, 1]), vec![1])
        .unwrap();
    a.sync().unwrap();
    a.flush(space_id).unwrap();

    b.insert(space_id, DimensionVector::new(vec![2, 2]), vec![2])
        .unwrap();
    let feature = b.create_branch("feature", BranchId::MAIN).unwrap();
    b.insert_on_branch(feature, space_id, DimensionVector::new(vec![1, 1]), vec![9])
        .unwrap();
    b.sync().unwrap();

    converge_with_branch_merge(&a, &b, space_id, feature, MergeStrategy::PreferHigherRevision)
        .unwrap();
    a.flush(space_id).unwrap();

    converge_main_records(&b, &a, space_id).unwrap();
    b.flush(space_id).unwrap();

    let root_a = snapshot_merkle(&a, space_id, BranchId::MAIN).unwrap().root();
    let root_b = snapshot_merkle(&b, space_id, BranchId::MAIN).unwrap().root();
    assert_eq!(root_a, root_b);
}

#[test]
fn conflict_survives_restart() {
    let dir = TempDir::new().unwrap();
    {
        let q = ConflictQueue::open(dir.path()).unwrap();
        use infinite_db::infinitedb_core::address::Address;
        use infinite_db::infinitedb_core::merge::MergeConflict;
        q.push(
            BranchId::MAIN,
            BranchId(2),
            MergeConflict {
                address: Address::new(SpaceId(1), DimensionVector::new(vec![1, 1])),
                base: None,
                target: None,
                source: None,
            },
        );
        assert_eq!(q.len(), 1);
    }
    let q2 = ConflictQueue::open(dir.path()).unwrap();
    assert_eq!(q2.len(), 1);
}
