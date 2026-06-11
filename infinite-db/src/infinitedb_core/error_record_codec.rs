//! Versioned codec for [`OperationErrorRecord`] payloads.

use std::io;

use bincode::{config::standard, decode_from_slice, encode_to_vec};

use super::error_record::OperationErrorRecord;

pub const ERROR_PAYLOAD_V1_TAG: u8 = 0xEE;

pub fn encode_error_record(record: &OperationErrorRecord) -> Result<Vec<u8>, io::Error> {
    let body = encode_to_vec(record, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(ERROR_PAYLOAD_V1_TAG);
    out.extend(body);
    Ok(out)
}

pub fn decode_error_record(data: &[u8]) -> Result<OperationErrorRecord, io::Error> {
    if data.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty error record payload",
        ));
    }
    if data[0] != ERROR_PAYLOAD_V1_TAG {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown error record tag",
        ));
    }
    let (record, consumed) = decode_from_slice::<OperationErrorRecord, _>(&data[1..], standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if consumed != data.len() - 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing bytes in error record payload",
        ));
    }
    Ok(record)
}
