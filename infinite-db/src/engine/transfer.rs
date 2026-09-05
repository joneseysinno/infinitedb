//! Nexus bulk-transfer executor (D-U11, INV-NEX-TRANSFER-ORDER).

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

use bincode::{Decode, Encode};
use parking_lot::RwLock;

use crate::infinitedb_core::address::{Address, SpaceId};
use crate::infinitedb_core::block::Record;

/// Transfer phase cursor (durable between crash points).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum NexusTransferPhase {
    Prepared,
    Copying,
    TargetSynced,
    SourceTombstoning,
    Complete,
}

/// Durable Nexus transfer intent state.
#[derive(Debug, Clone, Encode, Decode)]
pub struct NexusTransferIntent {
    pub id: u64,
    pub source: SpaceId,
    pub target: SpaceId,
    pub phase: NexusTransferPhase,
    pub rows_copied: u64,
    pub rows_tombstoned: u64,
    pub copy_cursor: usize,
    pub tombstone_cursor: usize,
    /// Captured source addresses so cursor meaning cannot shift mid-transfer.
    pub captured_addresses: Vec<Address>,
}

impl NexusTransferIntent {
    pub fn new(id: u64, source: SpaceId, target: SpaceId) -> Self {
        Self {
            id,
            source,
            target,
            phase: NexusTransferPhase::Prepared,
            rows_copied: 0,
            rows_tombstoned: 0,
            copy_cursor: 0,
            tombstone_cursor: 0,
            captured_addresses: Vec::new(),
        }
    }
}

static NEXT_TRANSFER_ID: AtomicU64 = AtomicU64::new(1);

pub fn next_transfer_id() -> u64 {
    NEXT_TRANSFER_ID.fetch_add(1, Ordering::Relaxed)
}

/// In-memory transfer registry (persisted by the engine between operations).
#[derive(Debug, Default)]
pub struct NexusTransferRegistry {
    intents: RwLock<HashMap<u64, NexusTransferIntent>>,
}

impl NexusTransferRegistry {
    pub fn insert(&self, intent: NexusTransferIntent) {
        self.intents.write().insert(intent.id, intent);
    }

    pub fn get(&self, id: u64) -> Option<NexusTransferIntent> {
        self.intents.read().get(&id).cloned()
    }

    pub fn update(&self, intent: NexusTransferIntent) {
        self.intents.write().insert(intent.id, intent);
    }

    pub fn snapshot(&self) -> BTreeMap<u64, NexusTransferIntent> {
        self.intents.read().iter().map(|(k, v)| (*k, v.clone())).collect()
    }

    pub fn load(&self, intents: HashMap<u64, NexusTransferIntent>) {
        *self.intents.write() = intents;
    }

    pub fn load_btree(&self, intents: BTreeMap<u64, NexusTransferIntent>) {
        *self.intents.write() = intents.into_iter().collect();
    }
}

/// Steady-state source rows eligible for transfer (latest non-tombstone per address).
pub fn steady_source_rows(records: &[Record]) -> Vec<Record> {
    let mut latest: HashMap<Address, Record> = HashMap::new();
    for rec in records {
        let addr = rec.address.clone();
        match latest.get(&addr) {
            None => {
                latest.insert(addr, rec.clone());
            }
            Some(prev) if rec.revision > prev.revision => {
                latest.insert(addr, rec.clone());
            }
            _ => {}
        }
    }
    latest
        .into_values()
        .filter(|r| !r.tombstone)
        .collect()
}

pub fn sort_transfer_records(records: &mut [Record]) {
    records.sort_by(|a, b| {
        a.address
            .space
            .0
            .cmp(&b.address.space.0)
            .then_with(|| a.address.point.coords.cmp(&b.address.point.coords))
    });
}
