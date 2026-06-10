//! Length-prefixed bincode framing for API requests and responses.

use std::io::{self, Read, Write};

use bincode::{config::standard, decode_from_slice, encode_to_vec};

use super::api::{Request, Response};

const MAX_FRAME: usize = 64 * 1024 * 1024;

/// Write one framed [`Request`] or [`Response`].
pub fn write_frame<W: Write, T: bincode::Encode>(sink: &mut W, msg: &T) -> io::Result<()> {
    let payload = encode_to_vec(msg, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let len = payload.len() as u64;
    sink.write_all(&len.to_le_bytes())?;
    sink.write_all(&payload)?;
    sink.flush()
}

/// Read one framed message.
pub fn read_frame<R: Read, T: bincode::Decode<()>>(src: &mut R) -> io::Result<T> {
    let mut len_buf = [0u8; 8];
    src.read_exact(&mut len_buf)?;
    let len = u64::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds 64 MiB limit",
        ));
    }
    let mut payload = vec![0u8; len];
    src.read_exact(&mut payload)?;
    let (msg, _) = decode_from_slice::<T, _>(&payload, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(msg)
}

pub fn write_request<W: Write>(sink: &mut W, req: &Request) -> io::Result<()> {
    write_frame(sink, req)
}

pub fn read_request<R: Read>(src: &mut R) -> io::Result<Request> {
    read_frame(src)
}

pub fn write_response<W: Write>(sink: &mut W, resp: &Response) -> io::Result<()> {
    write_frame(sink, resp)
}

pub fn read_response<R: Read>(src: &mut R) -> io::Result<Response> {
    read_frame(src)
}
