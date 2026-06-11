//! Versioned codec for [`JudgmentRecord`] payloads.

use std::io;

use bincode::{config::standard, decode_from_slice, encode_to_vec};

use super::judgment::JudgmentRecord;

pub const JUDGMENT_PAYLOAD_V1_TAG: u8 = 0x4A;

pub fn encode_judgment(record: &JudgmentRecord) -> Result<Vec<u8>, io::Error> {
    let body = encode_to_vec(record, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(JUDGMENT_PAYLOAD_V1_TAG);
    out.extend(body);
    Ok(out)
}

pub fn decode_judgment(data: &[u8]) -> Result<JudgmentRecord, io::Error> {
    if data.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty judgment payload",
        ));
    }
    if data[0] != JUDGMENT_PAYLOAD_V1_TAG {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown judgment tag",
        ));
    }
    let (record, consumed) = decode_from_slice::<JudgmentRecord, _>(&data[1..], standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if consumed != data.len() - 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing bytes in judgment payload",
        ));
    }
    Ok(record)
}
