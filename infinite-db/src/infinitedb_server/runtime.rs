//! Tokio TCP server wiring [`crate::InfiniteDb`] to the API layer.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use crate::infinitedb_core::branch::BranchId;
use crate::infinitedb_core::snapshot::SnapshotId;
use crate::InfiniteDb;

use super::api::{handle_request, Request, Response};
use super::session::{AccessLevel, Session, SpaceGrant};
/// TCP server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub max_connections: usize,
    pub default_branch: BranchId,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 128,
            default_branch: BranchId::MAIN,
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
        Ok(Self {
            listener,
            db,
            config,
            grants,
            limiter,
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
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            let db = Arc::clone(&self.db);
            let grants = self.grants.clone();
            let branch = self.config.default_branch;
            tokio::spawn(async move {
                let _permit = permit;
                let _ = serve_connection(stream, db, branch, grants).await;
            });
        }
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    db: Arc<InfiniteDb>,
    branch: BranchId,
    grants: Vec<SpaceGrant>,
) -> io::Result<()> {
    let pinned = db
        .branch_head(branch)
        .unwrap_or(SnapshotId(0));
    let opened_at = db.revision();
    let session = Session::open_at_revision(branch, pinned, opened_at, grants);

    loop {
        let request: Request = read_frame_async(&mut stream).await?;
        let response = handle_request(&db, &session, request);
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
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
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
