//! Request executor: off-runtime dispatch and concurrency regression tests.

use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use infinite_db::infinitedb_core::address::{DimensionVector, RevisionId, SpaceId};
use infinite_db::infinitedb_core::branch::BranchId;
use infinite_db::infinitedb_core::snapshot::SnapshotId;
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::{
    admin_grants, AccessLevel, InfiniteDb, Request, RequestExecutor, Response, Server,
    ServerConfig, Session, SpaceGrant, SubmitError,
};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;

fn space(id: u64) -> SpaceConfig {
    SpaceConfig::new(SpaceId(id), format!("s{id}"), 2).with_shard_bits(2)
}

fn test_session() -> Arc<Session> {
    Arc::new(Session::open_at_revision(
        BranchId::MAIN,
        SnapshotId(0),
        RevisionId::legacy(0),
        vec![SpaceGrant {
            space: SpaceId(1),
            level: AccessLevel::Admin,
        }],
    ))
}

#[test]
fn server_config_defaults() {
    let cfg = ServerConfig::default();
    assert!(cfg.request_queue_capacity >= cfg.max_connections);
    assert!(cfg.executor_threads >= 1);
}

#[test]
fn submit_returns_handler_response() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let dir = TempDir::new().unwrap();
        let db = Arc::new(InfiniteDb::open(dir.path()).unwrap());
        let executor = RequestExecutor::start(Arc::clone(&db), 2, 8);
        let session = test_session();
        let resp = executor.submit(Request::Ping, session).await.unwrap();
        assert!(matches!(resp, Response::Pong));
    });
}

#[tokio::test]
async fn submit_runs_concurrently_not_serial_on_blocked_job() {
    let arrived = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let arrived_c = Arc::clone(&arrived);
    let release_c = Arc::clone(&release);

    let dir = TempDir::new().unwrap();
    let db = Arc::new(InfiniteDb::open(dir.path()).unwrap());
    let executor = Arc::new(RequestExecutor::start_with_handler(
        db,
        move |_db, _session, request| {
            if matches!(request, Request::Ping) {
                arrived_c.wait();
                release_c.wait();
            }
            Response::Pong
        },
        2,
        8,
    ));
    let session = test_session();

    let exec_a = Arc::clone(&executor);
    let session_a = Arc::clone(&session);
    let blocked = thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        rt.block_on(async { exec_a.submit(Request::Ping, session_a).await.unwrap() })
    });

    arrived.wait();

    let resp_b = executor
        .submit(Request::GetConflicts, session)
        .await
        .unwrap();
    assert!(matches!(resp_b, Response::Pong));

    release.wait();
    blocked.join().unwrap();
}

#[test]
fn no_head_of_line_blocking_on_single_tokio_worker() {
    let arrived = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let arrived_c = Arc::clone(&arrived);
    let release_c = Arc::clone(&release);

    let dir = TempDir::new().unwrap();
    let db = Arc::new(InfiniteDb::open(dir.path()).unwrap());
    let executor = Arc::new(RequestExecutor::start_with_handler(
        db,
        move |_db, _session, request| {
            if matches!(request, Request::Ping) {
                arrived_c.wait();
                release_c.wait();
            }
            Response::Pong
        },
        2,
        8,
    ));
    let session = test_session();

    let exec_a = Arc::clone(&executor);
    let session_a = Arc::clone(&session);
    let blocked = thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        rt.block_on(async { exec_a.submit(Request::Ping, session_a).await.unwrap() })
    });

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        arrived.wait();

        let resp_b = executor.submit(Request::GetConflicts, session).await.unwrap();
        assert!(matches!(resp_b, Response::Pong));

        release.wait();
    });
    blocked.join().unwrap();
}

#[test]
fn shutdown_joins_cleanly() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(InfiniteDb::open(dir.path()).unwrap());
    let (done_tx, done_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let _executor = RequestExecutor::start(db, 2, 4);
        done_tx.send(()).unwrap();
    });
    done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    handle.join().unwrap();
}

#[test]
fn queue_full_sheds_to_busy() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let arrived = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let arrived_h = Arc::clone(&arrived);
        let release_h = Arc::clone(&release);

        let dir = TempDir::new().unwrap();
        let db = Arc::new(InfiniteDb::open(dir.path()).unwrap());
        let executor = Arc::new(RequestExecutor::start_with_handler(
            db,
            move |_db, _session, request| {
                if matches!(request, Request::Ping) {
                    arrived_h.wait();
                    release_h.wait();
                }
                Response::Pong
            },
            1,
            1,
        ));
        let session = test_session();

        let exec_a = Arc::clone(&executor);
        let session_a = Arc::clone(&session);
        let blocked = thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async { exec_a.submit(Request::Ping, session_a).await.unwrap() })
        });

        arrived.wait();

        let exec_fill = Arc::clone(&executor);
        let session_b = test_session();
        thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let _ = rt.block_on(async { exec_fill.submit(Request::GetConflicts, session_b).await });
        });
        thread::sleep(Duration::from_millis(50));

        let session_c = test_session();
        assert!(matches!(
            executor.submit(Request::GetConflicts, session_c).await,
            Err(SubmitError::Busy)
        ));

        release.wait();
        blocked.join().unwrap();
    });
}

fn spawn_server(db: Arc<InfiniteDb>, space_ids: &[u64]) -> (std::net::SocketAddr, thread::JoinHandle<()>) {
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

async fn roundtrip_on_stream(stream: &mut TcpStream, request: Request) -> std::io::Result<Response> {
    let payload = bincode::encode_to_vec(&request, bincode::config::standard())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let len = payload.len() as u64;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;

    let mut len_buf = [0u8; 8];
    stream.read_exact(&mut len_buf).await?;
    let len = u64::from_le_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    let (msg, _) = bincode::decode_from_slice::<Response, _>(&payload, bincode::config::standard())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(msg)
}

#[test]
fn per_connection_request_order() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(InfiniteDb::open(dir.path()).unwrap());
    db.register_space(space(1)).unwrap();

    let (addr, server) = spawn_server(Arc::clone(&db), &[1]);
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        let mut stream = TcpStream::connect(addr).await.unwrap();

        let w0 = roundtrip_on_stream(
            &mut stream,
            Request::Write {
                address: infinite_db::infinitedb_core::address::Address::new(
                    SpaceId(1),
                    DimensionVector::new(vec![1, 2]),
                ),
                revision: RevisionId::legacy(1),
                data: vec![10],
            },
        )
        .await
        .unwrap();
        assert!(matches!(w0, Response::WriteAck { .. }));

        let p0 = roundtrip_on_stream(&mut stream, Request::Ping)
            .await
            .unwrap();
        assert!(matches!(p0, Response::Pong));

        let w1 = roundtrip_on_stream(
            &mut stream,
            Request::Write {
                address: infinite_db::infinitedb_core::address::Address::new(
                    SpaceId(1),
                    DimensionVector::new(vec![3, 4]),
                ),
                revision: RevisionId::legacy(1),
                data: vec![20],
            },
        )
        .await
        .unwrap();
        assert!(matches!(w1, Response::WriteAck { .. }));

        let p1 = roundtrip_on_stream(&mut stream, Request::Ping)
            .await
            .unwrap();
        assert!(matches!(p1, Response::Pong));
    });

    drop(server);
}
