//! The CQL v4 binary protocol frame and body primitives.
//!
//! A frame is a fixed 9-byte header followed by a body:
//!
//! ```text
//! 0         8        16          32                              64
//! +---------+--------+-----------+------------+-------------------+
//! | version | flags  |  stream (i16)          |    opcode (u8)    |
//! +---------+--------+------------------------+-------------------+
//! |                     length (i32, big-endian)                 |
//! +--------------------------------------------------------------+
//! |                          body (length bytes)                 |
//! +--------------------------------------------------------------+
//! ```
//!
//! - `version` is `0x04` on a request and `0x84` on a response (the high bit is
//!   the direction).
//! - `stream` correlates a response with its request; we echo it back.
//! - All multi-byte integers are big-endian.
//!
//! This module is pure: it parses/encodes bytes and never touches I/O. The
//! socket reader in `custosd` reads the 9-byte header, then `length` more bytes,
//! and hands the whole thing to [`Frame::decode`].

use std::fmt;

/// The protocol version byte on a **request** frame (CQL v4).
pub const REQUEST_VERSION: u8 = 0x04;
/// The protocol version byte on a **response** frame (CQL v4, direction bit set).
pub const RESPONSE_VERSION: u8 = 0x84;
/// The fixed frame header size in bytes.
pub const HEADER_LEN: usize = 9;

/// Frame flag bits (CQL v4). We do not negotiate compression or tracing, so a
/// well-behaved client sends `0`; we reject anything we do not understand.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Flags(pub u8);

impl Flags {
    /// No flags set.
    pub const NONE: Flags = Flags(0);
    /// Compression flag — unsupported (we advertise no compression in SUPPORTED).
    pub const COMPRESSION: u8 = 0x01;
}

/// The request/response opcodes this minimal subset understands.
///
/// The numeric values are fixed by the CQL v4 spec; we only model the ones we
/// handle (plus `ERROR`/`RESULT` for replies).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    /// `ERROR` (server → client).
    Error = 0x00,
    /// `STARTUP` (client → server): begin a connection.
    Startup = 0x01,
    /// `READY` (server → client): connection is ready, no auth required.
    Ready = 0x02,
    /// `OPTIONS` (client → server): ask what the server supports.
    Options = 0x05,
    /// `SUPPORTED` (server → client): the answer to `OPTIONS`.
    Supported = 0x06,
    /// `QUERY` (client → server): a CQL query string + parameters.
    Query = 0x07,
    /// `RESULT` (server → client): the answer to a `QUERY`.
    Result = 0x08,
}

impl Opcode {
    /// Map a raw opcode byte to the subset we model.
    #[must_use]
    pub fn from_u8(byte: u8) -> Option<Opcode> {
        Some(match byte {
            0x00 => Opcode::Error,
            0x01 => Opcode::Startup,
            0x02 => Opcode::Ready,
            0x05 => Opcode::Options,
            0x06 => Opcode::Supported,
            0x07 => Opcode::Query,
            0x08 => Opcode::Result,
            _ => return None,
        })
    }
}

/// A decoded CQL frame: its header fields plus the raw body bytes. The body is
/// interpreted by the opcode-specific parsers (e.g. [`super::query::parse_query`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    /// Version byte (`0x04` request / `0x84` response).
    pub version: u8,
    /// Flag bits.
    pub flags: Flags,
    /// Stream id (echoed between a request and its response).
    pub stream: i16,
    /// The operation.
    pub opcode: Opcode,
    /// The raw, opcode-specific body.
    pub body: Vec<u8>,
}

/// Why a frame (or a body primitive) failed to decode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// Fewer than [`HEADER_LEN`] bytes were supplied.
    ShortHeader,
    /// The body was shorter than the header's declared length.
    ShortBody { declared: usize, actual: usize },
    /// The version byte was not CQL v4 (`0x04`).
    UnsupportedVersion(u8),
    /// A flag we do not support was set (e.g. compression).
    UnsupportedFlags(u8),
    /// The opcode byte was not one we model.
    UnknownOpcode(u8),
    /// A `[string]`/`[bytes]` length ran past the end of the body.
    Truncated,
    /// A `[string]` was not valid UTF-8.
    BadUtf8,
    /// The declared body length exceeds [`MAX_BODY`].
    TooLarge(usize),
}

/// Upper bound on a frame body, so a malformed length cannot make us allocate
/// unbounded memory. CQL allows up to 256 MiB; we cap far below that since our
/// subset only moves tiny rows.
pub const MAX_BODY: usize = 1 << 20;

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::ShortHeader => write!(f, "frame header is shorter than 9 bytes"),
            FrameError::ShortBody { declared, actual } => {
                write!(
                    f,
                    "frame body is {actual} bytes, header declared {declared}"
                )
            }
            FrameError::UnsupportedVersion(v) => write!(f, "unsupported protocol version {v:#x}"),
            FrameError::UnsupportedFlags(b) => write!(f, "unsupported frame flags {b:#x}"),
            FrameError::UnknownOpcode(b) => write!(f, "unknown opcode {b:#x}"),
            FrameError::Truncated => write!(f, "body primitive ran past the end of the body"),
            FrameError::BadUtf8 => write!(f, "string was not valid UTF-8"),
            FrameError::TooLarge(n) => write!(f, "declared body length {n} exceeds the cap"),
        }
    }
}

impl std::error::Error for FrameError {}

impl Frame {
    /// Parse the declared body length from a 9-byte header without consuming the
    /// body. The socket reader uses this to know how many more bytes to read.
    ///
    /// # Errors
    /// [`FrameError::ShortHeader`] if fewer than 9 bytes; [`FrameError::TooLarge`]
    /// if the declared length exceeds [`MAX_BODY`].
    pub fn body_len(header: &[u8]) -> Result<usize, FrameError> {
        if header.len() < HEADER_LEN {
            return Err(FrameError::ShortHeader);
        }
        let len = i32::from_be_bytes([header[5], header[6], header[7], header[8]]);
        let len = len.max(0) as usize;
        if len > MAX_BODY {
            return Err(FrameError::TooLarge(len));
        }
        Ok(len)
    }

    /// Decode a full frame from `header` (9 bytes) and `body` (exactly the
    /// declared length).
    ///
    /// # Errors
    /// Returns a [`FrameError`] for any malformed header, unsupported
    /// version/flags, unknown opcode, or body-length mismatch.
    pub fn decode(header: &[u8], body: &[u8]) -> Result<Frame, FrameError> {
        if header.len() < HEADER_LEN {
            return Err(FrameError::ShortHeader);
        }
        let version = header[0];
        if version != REQUEST_VERSION {
            return Err(FrameError::UnsupportedVersion(version));
        }
        let flags = header[1];
        // We negotiate no compression/tracing; reject any flag we do not model.
        if flags & Flags::COMPRESSION != 0 {
            return Err(FrameError::UnsupportedFlags(flags));
        }
        let stream = i16::from_be_bytes([header[2], header[3]]);
        let opcode = Opcode::from_u8(header[4]).ok_or(FrameError::UnknownOpcode(header[4]))?;
        let declared = Frame::body_len(header)?;
        if body.len() != declared {
            return Err(FrameError::ShortBody {
                declared,
                actual: body.len(),
            });
        }
        Ok(Frame {
            version,
            flags: Flags(flags),
            stream,
            opcode,
            body: body.to_vec(),
        })
    }

    /// Encode a **response** frame (`0x84` version) with the given stream,
    /// opcode, and body into a freshly allocated byte vector.
    #[must_use]
    pub fn encode_response(stream: i16, opcode: Opcode, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + body.len());
        out.push(RESPONSE_VERSION);
        out.push(0); // no flags
        out.extend_from_slice(&stream.to_be_bytes());
        out.push(opcode as u8);
        out.extend_from_slice(&(body.len() as i32).to_be_bytes());
        out.extend_from_slice(body);
        out
    }
}

// --- body primitives (CQL "notations") -------------------------------------

/// Read a CQL `[string]`: a `u16` length followed by that many UTF-8 bytes.
/// Advances `pos`.
///
/// # Errors
/// [`FrameError::Truncated`] if the length runs past the buffer;
/// [`FrameError::BadUtf8`] if the bytes are not UTF-8.
pub fn read_string(buf: &[u8], pos: &mut usize) -> Result<String, FrameError> {
    let len = read_u16(buf, pos)? as usize;
    let end = pos.checked_add(len).ok_or(FrameError::Truncated)?;
    if end > buf.len() {
        return Err(FrameError::Truncated);
    }
    let s = std::str::from_utf8(&buf[*pos..end])
        .map_err(|_| FrameError::BadUtf8)?
        .to_owned();
    *pos = end;
    Ok(s)
}

/// Read a CQL `[long string]`: an `i32` length followed by that many UTF-8
/// bytes. Advances `pos`.
///
/// # Errors
/// As [`read_string`].
pub fn read_long_string(buf: &[u8], pos: &mut usize) -> Result<String, FrameError> {
    let len = read_i32(buf, pos)?.max(0) as usize;
    let end = pos.checked_add(len).ok_or(FrameError::Truncated)?;
    if end > buf.len() {
        return Err(FrameError::Truncated);
    }
    let s = std::str::from_utf8(&buf[*pos..end])
        .map_err(|_| FrameError::BadUtf8)?
        .to_owned();
    *pos = end;
    Ok(s)
}

/// Write a CQL `[string]` (u16 length prefix) to `out`.
pub fn write_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// Write a CQL `[bytes]`: an `i32` length prefix followed by the bytes. A
/// negative length encodes "null"; pass `None` for that.
pub fn write_bytes(out: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        Some(b) => {
            out.extend_from_slice(&(b.len() as i32).to_be_bytes());
            out.extend_from_slice(b);
        }
        None => out.extend_from_slice(&(-1i32).to_be_bytes()),
    }
}

/// Write a CQL `[string list]`: a `u16` count followed by that many `[string]`s.
pub fn write_string_list(out: &mut Vec<u8>, items: &[&str]) {
    out.extend_from_slice(&(items.len() as u16).to_be_bytes());
    for item in items {
        write_string(out, item);
    }
}

/// Write a CQL `[string multimap]`: a `u16` count followed by `[string]` keys
/// each paired with a `[string list]` value. Used by `SUPPORTED`.
pub fn write_string_multimap(out: &mut Vec<u8>, entries: &[(&str, &[&str])]) {
    out.extend_from_slice(&(entries.len() as u16).to_be_bytes());
    for (key, values) in entries {
        write_string(out, key);
        write_string_list(out, values);
    }
}

/// Read a CQL `[string map]` (used by `STARTUP`): a `u16` count of
/// `[string]`→`[string]` pairs. Returns a deterministic [`std::collections::BTreeMap`].
///
/// # Errors
/// As [`read_string`].
pub fn read_string_map(
    buf: &[u8],
    pos: &mut usize,
) -> Result<std::collections::BTreeMap<String, String>, FrameError> {
    let count = read_u16(buf, pos)?;
    let mut map = std::collections::BTreeMap::new();
    for _ in 0..count {
        let key = read_string(buf, pos)?;
        let value = read_string(buf, pos)?;
        map.insert(key, value);
    }
    Ok(map)
}

fn read_u16(buf: &[u8], pos: &mut usize) -> Result<u16, FrameError> {
    let end = pos.checked_add(2).ok_or(FrameError::Truncated)?;
    if end > buf.len() {
        return Err(FrameError::Truncated);
    }
    let v = u16::from_be_bytes([buf[*pos], buf[*pos + 1]]);
    *pos = end;
    Ok(v)
}

fn read_i32(buf: &[u8], pos: &mut usize) -> Result<i32, FrameError> {
    let end = pos.checked_add(4).ok_or(FrameError::Truncated)?;
    if end > buf.len() {
        return Err(FrameError::Truncated);
    }
    let v = i32::from_be_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]);
    *pos = end;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trips_through_body_len() {
        let body = b"hello";
        let encoded = Frame::encode_response(7, Opcode::Ready, body);
        assert_eq!(encoded[0], RESPONSE_VERSION);
        assert_eq!(Frame::body_len(&encoded[..HEADER_LEN]).unwrap(), body.len());
    }

    #[test]
    fn decode_rejects_wrong_version() {
        let mut header = [0u8; HEADER_LEN];
        header[0] = 0x03; // v3, unsupported
        header[4] = Opcode::Startup as u8;
        assert_eq!(
            Frame::decode(&header, &[]),
            Err(FrameError::UnsupportedVersion(0x03))
        );
    }

    #[test]
    fn decode_rejects_compression_flag() {
        let mut header = [0u8; HEADER_LEN];
        header[0] = REQUEST_VERSION;
        header[1] = Flags::COMPRESSION;
        header[4] = Opcode::Startup as u8;
        assert_eq!(
            Frame::decode(&header, &[]),
            Err(FrameError::UnsupportedFlags(Flags::COMPRESSION))
        );
    }

    #[test]
    fn decode_a_startup_frame() {
        // Build a STARTUP body: {"CQL_VERSION": "3.0.0"}.
        let mut body = Vec::new();
        body.extend_from_slice(&1u16.to_be_bytes());
        write_string(&mut body, "CQL_VERSION");
        write_string(&mut body, "3.0.0");

        let mut header = [0u8; HEADER_LEN];
        header[0] = REQUEST_VERSION;
        header[2..4].copy_from_slice(&42i16.to_be_bytes());
        header[4] = Opcode::Startup as u8;
        header[5..9].copy_from_slice(&(body.len() as i32).to_be_bytes());

        let frame = Frame::decode(&header, &body).unwrap();
        assert_eq!(frame.opcode, Opcode::Startup);
        assert_eq!(frame.stream, 42);

        let mut pos = 0;
        let map = read_string_map(&frame.body, &mut pos).unwrap();
        assert_eq!(map.get("CQL_VERSION").map(String::as_str), Some("3.0.0"));
    }

    #[test]
    fn read_string_detects_truncation() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&10u16.to_be_bytes()); // claims 10 bytes
        buf.extend_from_slice(b"short"); // only 5
        let mut pos = 0;
        assert_eq!(read_string(&buf, &mut pos), Err(FrameError::Truncated));
    }
}
