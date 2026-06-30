//! Bounded OS-thread pool for off-runtime request execution.
//!
//! Mirrors the write path (`WriteQueueSender` → `space_io` threads): tokio tasks submit jobs
//! and `await` oneshot replies while worker threads run blocking `handle_request` calls.

use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{bounded, Sender, TrySendError};

use crate::InfiniteDb;

use super::api::{handle_request, Request, Response};
use super::session::Session;

struct RequestJob {
    request: Request,
    session: Arc<Session>,
    reply: tokio::sync::oneshot::Sender<Response>,
}

/// Error returned when a job cannot be accepted or the reply channel closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitError {
    /// Job channel is full (D-EXEC-4b; unreachable when capacity ≥ max connections).
    Busy,
    /// Executor shut down or the client dropped before reading the reply.
    Stopped,
}

/// Dedicated thread pool that runs `handle_request` off the tokio runtime.
pub struct RequestExecutor {
    jobs: Option<Sender<RequestJob>>,
    workers: Vec<JoinHandle<()>>,
}

impl RequestExecutor {
    /// Start a pool that dispatches to the real `handle_request` implementation.
    pub fn start(db: Arc<InfiniteDb>, threads: usize, capacity: usize) -> Self {
        Self::start_with_handler(db, handle_request, threads, capacity)
    }

    /// Start a pool with a custom handler (integration tests).
    #[doc(hidden)]
    pub fn start_with_handler<F>(
        db: Arc<InfiniteDb>,
        handler: F,
        threads: usize,
        capacity: usize,
    ) -> Self
    where
        F: Fn(&InfiniteDb, &Session, Request) -> Response + Send + Sync + Clone + 'static,
    {
        let (tx, rx) = bounded::<RequestJob>(capacity);
        let mut workers = Vec::with_capacity(threads);
        for i in 0..threads {
            let rx = rx.clone();
            let db = Arc::clone(&db);
            let handler = handler.clone();
            workers.push(
                thread::Builder::new()
                    .name(format!("infinitedb-req-{i}"))
                    .spawn(move || {
                        while let Ok(job) = rx.recv() {
                            let resp = handler(&db, &job.session, job.request);
                            let _ = job.reply.send(resp);
                        }
                    })
                    .expect("spawn request worker"),
            );
        }
        Self {
            jobs: Some(tx),
            workers,
        }
    }

    /// Enqueue a job without blocking the caller thread; await the oneshot reply.
    pub async fn submit(
        &self,
        request: Request,
        session: Arc<Session>,
    ) -> Result<Response, SubmitError> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let job = RequestJob {
            request,
            session,
            reply: reply_tx,
        };
        let tx = self.jobs.as_ref().ok_or(SubmitError::Stopped)?;
        match tx.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => return Err(SubmitError::Busy),
            Err(TrySendError::Disconnected(_)) => return Err(SubmitError::Stopped),
        }
        reply_rx.await.map_err(|_| SubmitError::Stopped)
    }
}

impl Drop for RequestExecutor {
    fn drop(&mut self) {
        self.jobs.take();
        for w in self.workers.drain(..) {
            let _ = w.join();
        }
    }
}
