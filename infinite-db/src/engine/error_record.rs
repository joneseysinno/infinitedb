//! Engine helpers for operation-level error records (M5).

use std::io;

use crate::engine::hypergraph::HypergraphWriteRow;
use crate::engine::import::{ImportErrorClass, ImportErrorEntry, ImportErrorLog};
use crate::infinitedb_core::{
    address::{RevisionId, SpaceId},
    error_record::{
        ErrorKind, ErrorRecordEntry, OperationErrorRecord, OperationRevisionRange,
        error_storage_point,
    },
    error_record_codec::{decode_error_record, encode_error_record},
};

pub fn import_error_kind(aborted: bool) -> ErrorKind {
    if aborted {
        ErrorKind::ImportBudgetExceeded
    } else {
        ErrorKind::ImportValidation
    }
}

pub fn operation_record_from_import(
    source_space: SpaceId,
    admitted: OperationRevisionRange,
    errors: &ImportErrorLog,
    aborted: bool,
) -> OperationErrorRecord {
    let kind = import_error_kind(aborted);
    let entries = errors
        .entries()
        .iter()
        .map(import_entry_to_record_entry)
        .collect();
    OperationErrorRecord {
        kind,
        revision_range: admitted,
        source_space,
        entries,
    }
}

fn import_entry_to_record_entry(entry: &ImportErrorEntry) -> ErrorRecordEntry {
    let class = match entry.class {
        ImportErrorClass::Validation => ErrorKind::ImportValidation,
        ImportErrorClass::Catalog => ErrorKind::Custom(1),
        ImportErrorClass::Other => ErrorKind::Custom(2),
    };
    ErrorRecordEntry {
        index: entry.index,
        item_id: entry.edge_id,
        class,
        message: entry.message.clone(),
    }
}

pub fn prepare_error_write(
    error_space: SpaceId,
    record: &OperationErrorRecord,
) -> Result<HypergraphWriteRow, io::Error> {
    let point = error_storage_point(record.revision_range.first);
    let data = encode_error_record(record)?;
    Ok(HypergraphWriteRow::new_row(error_space, point, data, false))
}

pub fn prepare_error_tombstone(
    error_space: SpaceId,
    range_start: RevisionId,
) -> HypergraphWriteRow {
    HypergraphWriteRow::new_row(error_space, error_storage_point(range_start), vec![], true)
}

pub fn decode_error_record_payload(data: &[u8]) -> io::Result<OperationErrorRecord> {
    decode_error_record(data)
}

pub fn revision_range_from_engine(range: crate::engine::watermark::RevisionRange) -> OperationRevisionRange {
    OperationRevisionRange::new(range.first(), range.last())
}

pub fn operation_record_interrupted_intent(
    source_space: SpaceId,
    session: u32,
    first: RevisionId,
    last: RevisionId,
    frame_count: usize,
) -> OperationErrorRecord {
    OperationErrorRecord {
        kind: ErrorKind::InterruptedSessionIntent,
        revision_range: OperationRevisionRange::new(first, last),
        source_space,
        entries: vec![ErrorRecordEntry {
            index: 0,
            item_id: session as u64,
            class: ErrorKind::InterruptedSessionIntent,
            message: format!(
                "interrupted session intent: session {session} durable frames ({frame_count}) without checkpoint after revision {first:?}"
            ),
        }],
    }
}

pub fn operation_record_checkpoint_collision(
    source_space: SpaceId,
    checkpoint: &crate::infinitedb_core::intent_checkpoint::IntentCheckpoint,
    message: impl Into<String>,
) -> OperationErrorRecord {
    OperationErrorRecord {
        kind: ErrorKind::CheckpointCollision,
        revision_range: checkpoint.revision_range(),
        source_space,
        entries: vec![ErrorRecordEntry {
            index: 0,
            item_id: checkpoint.first_revision.session() as u64,
            class: ErrorKind::CheckpointCollision,
            message: message.into(),
        }],
    }
}
