//! The **pure** half of the length-prefixed client-frame wire codec (ADR
//! 0061 rung C3a): the `u32` big-endian length-prefix arithmetic, the
//! [`MAX_FRAME_LEN`] bound check, and the `serde_json` encode/decode calls
//! that back `animusd`'s `write_frame`/`read_frame`. No socket type crosses
//! this boundary — `animusd` keeps the actual `TcpStream` reads/writes
//! (and the `MAX_FRAME_LEN` doc's own sizing rationale) in its own
//! `write_frame`/`read_frame`, calling straight into
//! [`encode_client_frame`]/[`frame_payload_len`]/[`decode_client_frame`]
//! for everything that doesn't touch a socket. See that crate's `lib.rs`
//! for the two thin wrappers.

use std::io;

use serde::Serialize;
use serde::de::DeserializeOwned;

/// The maximum length, in bytes, of one length-prefixed frame's JSON
/// payload — moved verbatim from `animusd::lib` (rung C3a). See that
/// constant's own historical doc (now on `animusd`'s re-export, since the
/// sizing rationale cites `animusd`-specific numbers like `SEED_BATCH_MAX_
/// BYTES`/`http::MAX_BODY`) for why 64 MiB.
pub const MAX_FRAME_LEN: usize = 64 << 20;

/// Encode `msg` as a full length-prefixed frame: a 4-byte big-endian length
/// prefix followed by its `serde_json` encoding — exactly the bytes
/// `animusd`'s `write_frame` used to build inline before writing them to a
/// socket.
///
/// # Errors
/// Rejects a payload over [`MAX_FRAME_LEN`] (the receiver would drop the
/// connection anyway — failing at the sender names the culprit instead of
/// surfacing as a mysterious peer hang-up).
pub fn encode_client_frame<T: Serialize>(msg: &T) -> io::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(msg).expect("client message serializes");
    if bytes.len() > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "frame of {} bytes exceeds MAX_FRAME_LEN ({MAX_FRAME_LEN})",
                bytes.len()
            ),
        ));
    }
    let mut framed = Vec::with_capacity(4 + bytes.len());
    framed.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    framed.extend_from_slice(&bytes);
    Ok(framed)
}

/// Validate an already-read 4-byte big-endian length prefix, returning the
/// declared payload length as a `usize` a caller may safely allocate.
///
/// # Errors
/// A declared length over [`MAX_FRAME_LEN`] is an `InvalidData` error
/// **before any allocation** (the length prefix is untrusted — see
/// [`MAX_FRAME_LEN`]).
pub fn frame_payload_len(len_prefix: u32) -> io::Result<usize> {
    let len = len_prefix as usize;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("declared frame length {len} exceeds MAX_FRAME_LEN ({MAX_FRAME_LEN})"),
        ));
    }
    Ok(len)
}

/// Decode one frame's already-read payload bytes (the exact slice
/// `frame_payload_len` bytes long) into `T`.
///
/// # Errors
/// Propagates a `serde_json` decode error as `InvalidData`.
pub fn decode_client_frame<T: DeserializeOwned>(payload: &[u8]) -> io::Result<T> {
    serde_json::from_slice(payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{ClientRequest, ClientResponse};

    #[test]
    fn round_trips_a_client_request() {
        let req = ClientRequest::Status;
        let framed = encode_client_frame(&req).expect("encodes");
        // First 4 bytes are the big-endian length prefix.
        let len_prefix = u32::from_be_bytes(framed[0..4].try_into().unwrap());
        let payload = &framed[4..];
        assert_eq!(payload.len(), len_prefix as usize);
        let len = frame_payload_len(len_prefix).expect("within MAX_FRAME_LEN");
        assert_eq!(len, payload.len());
        let decoded: ClientRequest = decode_client_frame(payload).expect("decodes");
        assert!(matches!(decoded, ClientRequest::Status));
    }

    #[test]
    fn round_trips_a_client_response() {
        let resp = ClientResponse::Error("boom".to_string());
        let framed = encode_client_frame(&resp).expect("encodes");
        let payload = &framed[4..];
        let decoded: ClientResponse = decode_client_frame(payload).expect("decodes");
        match decoded {
            ClientResponse::Error(msg) => assert_eq!(msg, "boom"),
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[test]
    fn encode_rejects_a_payload_over_max_frame_len() {
        // A JSON string long enough that its *encoded* form exceeds
        // MAX_FRAME_LEN — the quotes/escaping only grow it further, so a
        // plain ASCII string of this length is already over the bound.
        let huge = "x".repeat(MAX_FRAME_LEN + 1);
        let err = encode_client_frame(&huge).expect_err("must reject oversized payload");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("exceeds MAX_FRAME_LEN"));
    }

    #[test]
    fn frame_payload_len_rejects_an_over_cap_declared_length() {
        let err = frame_payload_len((MAX_FRAME_LEN + 1) as u32)
            .expect_err("must reject an over-cap declared length");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("exceeds MAX_FRAME_LEN"));
    }

    #[test]
    fn frame_payload_len_accepts_exactly_max_frame_len() {
        assert_eq!(
            frame_payload_len(MAX_FRAME_LEN as u32).expect("exactly at the bound is legal"),
            MAX_FRAME_LEN
        );
    }

    #[test]
    fn decode_client_frame_reports_malformed_json_as_invalid_data() {
        let err = decode_client_frame::<ClientRequest>(b"not json")
            .expect_err("malformed payload must not decode");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
