//! Judgment write path and index rows (M5).

use std::io;

use crate::engine::hypergraph::HypergraphWriteRow;
use crate::infinitedb_core::{
    judgment::{
        JudgmentId, JudgmentRecord, judgment_storage_point,
    },
    judgment_codec::{decode_judgment, encode_judgment},
    judgment_index::{
        JUDGMENT_INDEX_SPACE, encode_judgment_index_payload, judgment_index_point,
    },
};

pub fn prepare_judgment_assertion(
    assertion_space: crate::infinitedb_core::address::SpaceId,
    record: &JudgmentRecord,
) -> Result<HypergraphWriteRow, io::Error> {
    Ok(HypergraphWriteRow {
        space: assertion_space,
        point: judgment_storage_point(record.id),
        data: encode_judgment(record)?,
        tombstone: false,
    })
}

pub fn prepare_judgment_index_row(record: &JudgmentRecord) -> HypergraphWriteRow {
    HypergraphWriteRow {
        space: JUDGMENT_INDEX_SPACE,
        point: judgment_index_point(&record.subject, record.arbiter, record.id),
        data: encode_judgment_index_payload(record.id),
        tombstone: false,
    }
}

pub fn prepare_judgment_writes(
    assertion_space: crate::infinitedb_core::address::SpaceId,
    record: &JudgmentRecord,
) -> Result<Vec<HypergraphWriteRow>, io::Error> {
    Ok(vec![
        prepare_judgment_assertion(assertion_space, record)?,
        prepare_judgment_index_row(record),
    ])
}

pub fn decode_judgment_record(data: &[u8]) -> io::Result<JudgmentRecord> {
    decode_judgment(data)
}

pub fn judgment_id_from_index_payload(data: &[u8]) -> Option<JudgmentId> {
    crate::infinitedb_core::judgment_index::decode_judgment_id_from_index(data)
}
