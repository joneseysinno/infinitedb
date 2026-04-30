//! Background worker that retries outbox replication.

use std::{
    io,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::infinitedb_sync::{
    outbox::{save_outbox, OutboxState},
    transport::SyncTransport,
};

/// Handle for a background sync loop.
pub struct BackgroundSyncWorker {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl BackgroundSyncWorker {
    pub fn start(
        state: Arc<Mutex<OutboxState>>,
        outbox_path: PathBuf,
        transport: Arc<dyn SyncTransport>,
        interval: Duration,
        batch_size: usize,
    ) -> io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let join = thread::Builder::new()
            .name("infinitedb-sync-worker".to_string())
            .spawn(move || {
                while !stop_clone.load(Ordering::Relaxed) {
                    if let Ok(mut guard) = state.lock() {
                        let report = guard.process_once(transport.as_ref(), batch_size);
                        if report.attempted > 0 {
                            let _ = save_outbox(&outbox_path, &guard);
                        }
                    }
                    thread::sleep(interval);
                }
            })?;
        Ok(Self {
            stop,
            join: Some(join),
        })
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for BackgroundSyncWorker {
    fn drop(&mut self) {
        self.stop();
    }
}
