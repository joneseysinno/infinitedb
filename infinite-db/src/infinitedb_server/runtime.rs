//! Tokio TCP server wiring [`crate::InfiniteDb`] to the API layer.
//!
//! Socket I/O runs on the tokio runtime; blocking database work is offloaded to
//! [`crate::RequestExecutor`] (see D-BLOCKING-CONTRACT in `SEMANTICS.md`).

use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use crate::infinitedb_core::branch::BranchId;
use crate::infinitedb_core::snapshot::SnapshotId;
use crate::InfiniteDb;

use super::api::{ApiError, Request, Response};
use super::executor::{RequestExecutor, SubmitError};
use super::session::{AccessLevel, Session, SpaceGrant};

/// Retry hint for executor queue-full shedding (D-EXEC-4b).
const BUSY_RETRY_MS: u64 = 100;

fn default_executor_threads() -> usize {
    std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(4)
}

/// TCP server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub max_connections: usize,
    pub default_branch: BranchId,
    /// OS threads in the request executor pool (D-EXEC-3).
    pub executor_threads: usize,
    /// Bounded job channel capacity; defaults to `max_connections` (D-EXEC-4a).
    pub request_queue_capacity: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let max_connections = 128;
        Self {
            max_connections,
            default_branch: BranchId::MAIN,
            executor_threads: default_executor_threads(),
            request_queue_capacity: max_connections,
        }
    }
}

/// Length-framed TCP server over a shared [`crate::InfiniteDb`].
pub struct Server {
    listener: TcpListener,
    db: Arc<InfiniteDb>,
    config: ServerConfig,
    grants: Vec<SpaceGrant>,
    limiter: Arc<Semaphore>,
    executor: Arc<RequestExecutor>,
}

impl Server {
    /// Bind `addr` and prepare to accept connections.
    pub async fn bind(
        addr: SocketAddr,
        db: Arc<InfiniteDb>,
        config: ServerConfig,
        grants: Vec<SpaceGrant>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let limiter = Arc::new(Semaphore::new(config.max_connections));
        let executor = Arc::new(RequestExecutor::start(
            Arc::clone(&db),
            config.executor_threads,
            config.request_queue_capacity,
        ));
        Self::from_parts(listener, db, config, grants, limiter, executor)
    }

    /// Bind with a pre-built executor (integration tests).
    #[doc(hidden)]
    pub async fn bind_with_executor(
        addr: SocketAddr,
        db: Arc<InfiniteDb>,
        config: ServerConfig,
        grants: Vec<SpaceGrant>,
        executor: Arc<RequestExecutor>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let limiter = Arc::new(Semaphore::new(config.max_connections));
        Self::from_parts(listener, db, config, grants, limiter, executor)
    }

    fn from_parts(
        listener: TcpListener,
        db: Arc<InfiniteDb>,
        config: ServerConfig,
        grants: Vec<SpaceGrant>,
        limiter: Arc<Semaphore>,
        executor: Arc<RequestExecutor>,
    ) -> io::Result<Self> {
        Ok(Self {
            listener,
            db,
            config,
            grants,
            limiter,
            executor,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept connections until the listener is dropped.
    pub async fn run(self) -> io::Result<()> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            let permit = Arc::clone(&self.limiter)
                .acquire_owned()
                .await
                .map_err(|e| io::Error::other(e.to_string()))?;
            let db = Arc::clone(&self.db);
            let executor = Arc::clone(&self.executor);
            let grants = self.grants.clone();
            let branch = self.config.default_branch;
            tokio::spawn(async move {
                let _permit = permit;
                let _ = serve_connection(stream, db, executor, branch, grants).await;
            });
        }
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    db: Arc<InfiniteDb>,
    executor: Arc<RequestExecutor>,
    branch: BranchId,
    grants: Vec<SpaceGrant>,
) -> io::Result<()> {
    let pinned = db
        .branch_head(branch)
        .unwrap_or(SnapshotId(0));
    let opened_at = db.revision();
    let session = Arc::new(Session::open_at_revision(
        branch,
        pinned,
        opened_at,
        grants,
    ));

    loop {
        let request: Request = read_frame_async(&mut stream).await?;
        let response = match executor
            .submit(request, Arc::clone(&session))
            .await
        {
            Ok(r) => r,
            Err(SubmitError::Busy) => Response::Error(ApiError::Busy {
                retry_hint_ms: BUSY_RETRY_MS,
            }),
            Err(SubmitError::Stopped) => Response::Error(ApiError::Internal(
                "request executor stopped".into(),
            )),
        };
        write_frame_async(&mut stream, &response).await?;
        if matches!(response, Response::Error(_)) {
            // keep connection alive for clients
        }
    }
}

async fn read_frame_async<T: bincode::Decode<()> + Send + 'static>(
    stream: &mut TcpStream,
) -> io::Result<T> {
    let mut len_buf = [0u8; 8];
    stream.read_exact(&mut len_buf).await?;
    let len = u64::from_le_bytes(len_buf) as usize;
    if len > 64 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    let (msg, _) = bincode::decode_from_slice::<T, _>(&payload, bincode::config::standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(msg)
}

async fn write_frame_async<T: bincode::Encode + Send + Sync>(
    stream: &mut TcpStream,
    msg: &T,
) -> io::Result<()> {
    let payload = bincode::encode_to_vec(msg, bincode::config::standard())
        .map_err(io::Error::other)?;
    let len = payload.len() as u64;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&payload).await?;
    stream.flush().await
}

/// One-shot client helper for integration tests.
pub async fn client_roundtrip(
    addr: SocketAddr,
    request: Request,
) -> io::Result<Response> {
    let mut stream = TcpStream::connect(addr).await?;
    write_frame_async(&mut stream, &request).await?;
    read_frame_async(&mut stream).await
}

/// Build admin grants for every registered space id.
pub fn admin_grants(space_ids: &[u64]) -> Vec<SpaceGrant> {
    space_ids
        .iter()
        .map(|id| SpaceGrant {
            space: crate::infinitedb_core::address::SpaceId(*id),
            level: AccessLevel::Admin,
        })
        .collect()
}
