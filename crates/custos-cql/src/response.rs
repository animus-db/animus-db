//! Pure builders for the CQL response bodies this subset emits: `READY`,
//! `SUPPORTED`, `RESULT` (Void and Rows), and `ERROR`. Plus the `QUERY` request
//! body parser. All I/O-free, so the wire layer in `custosd` just frames the
//! bytes these return.

use crate::frame::{
    self, Frame, Opcode, read_long_string, write_bytes, write_string, write_string_multimap,
};

/// CQL `RESULT` kinds (the leading `i32` of a RESULT body). We emit `Void` (for
/// an executed `INSERT`) and `Rows` (for a `SELECT`).
#[allow(dead_code)]
mod result_kind {
    pub const VOID: i32 = 0x0001;
    pub const ROWS: i32 = 0x0002;
}

/// CQL native type ids (in the Rows metadata). We model every column as
/// `varchar` (`0x000D`) since our no-schema convention stores text bytes.
const TYPE_VARCHAR: i16 = 0x000D;

/// The CQL `STARTUP` body, parsed to its options map (e.g. `CQL_VERSION`).
/// We accept any well-formed startup and reply `READY` (no authentication).
///
/// # Errors
/// Propagates a [`frame::FrameError`] if the body is malformed.
pub fn parse_startup(
    body: &[u8],
) -> Result<std::collections::BTreeMap<String, String>, frame::FrameError> {
    let mut pos = 0;
    frame::read_string_map(body, &mut pos)
}

/// A parsed `QUERY` request: the CQL text. (We ignore the trailing consistency
/// level + flags + bound values for this minimal subset; a real implementation
/// would honor them.)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryRequest {
    /// The CQL query string.
    pub cql: String,
}

/// Parse a `QUERY` request body: a `[long string]` query followed by a
/// `[consistency]` and flags we currently ignore.
///
/// # Errors
/// Propagates a [`frame::FrameError`] if the query string is malformed.
pub fn parse_query_request(body: &[u8]) -> Result<QueryRequest, frame::FrameError> {
    let mut pos = 0;
    let cql = read_long_string(body, &mut pos)?;
    Ok(QueryRequest { cql })
}

/// Encode a `READY` frame (response to `STARTUP`).
#[must_use]
pub fn ready(stream: i16) -> Vec<u8> {
    Frame::encode_response(stream, Opcode::Ready, &[])
}

/// Encode a `SUPPORTED` frame (response to `OPTIONS`). We advertise CQL 3.0.0
/// and no compression — matching what we actually accept in `STARTUP`.
#[must_use]
pub fn supported(stream: i16) -> Vec<u8> {
    let mut body = Vec::new();
    write_string_multimap(
        &mut body,
        &[("CQL_VERSION", &["3.0.0"]), ("COMPRESSION", &[])],
    );
    Frame::encode_response(stream, Opcode::Supported, &body)
}

/// Encode a `RESULT` of kind `Void` (response to a successful `INSERT`).
#[must_use]
pub fn void_result(stream: i16) -> Vec<u8> {
    let body = result_kind::VOID.to_be_bytes().to_vec();
    Frame::encode_response(stream, Opcode::Result, &body)
}

/// Encode a `RESULT` of kind `Rows` for a single-row `SELECT pk, v`. `row` is
/// `Some((pk, v))` when the key exists, `None` for an empty result set. Both
/// columns are advertised as `varchar` and carry the raw text bytes.
#[must_use]
pub fn rows_result(stream: i16, table: &str, row: Option<(&str, &str)>) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&result_kind::ROWS.to_be_bytes());

    // <metadata>: flags (i32) | columns_count (i32) | [global table spec] |
    //             col_spec*  (Global_tables_spec flag = 0x0001).
    const GLOBAL_TABLES_SPEC: i32 = 0x0001;
    body.extend_from_slice(&GLOBAL_TABLES_SPEC.to_be_bytes());
    body.extend_from_slice(&2i32.to_be_bytes()); // two columns: pk, v
    // Global table spec: <keyspace><table>.
    write_string(&mut body, "custos");
    write_string(&mut body, table);
    // Column specs (name + type). With the global spec flag we only write
    // <name><type> per column.
    for name in [crate::query::PK_COLUMN, crate::query::V_COLUMN] {
        write_string(&mut body, name);
        body.extend_from_slice(&TYPE_VARCHAR.to_be_bytes());
    }

    // <rows_count> then each row as a sequence of [bytes] cells.
    match row {
        Some((pk, v)) => {
            body.extend_from_slice(&1i32.to_be_bytes());
            write_bytes(&mut body, Some(pk.as_bytes()));
            write_bytes(&mut body, Some(v.as_bytes()));
        }
        None => body.extend_from_slice(&0i32.to_be_bytes()),
    }

    Frame::encode_response(stream, Opcode::Result, &body)
}

/// Encode an `ERROR` frame. `code` is a CQL error code (`0x2200` =
/// Invalid, `0x0000` = ServerError); `message` is a `[string]`.
#[must_use]
pub fn error(stream: i16, code: i32, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&code.to_be_bytes());
    write_string(&mut body, message);
    Frame::encode_response(stream, Opcode::Error, &body)
}

/// CQL error code for an invalid / unsupported query.
pub const ERR_INVALID: i32 = 0x2200;
/// CQL error code for a server-side failure (no quorum, corrupt bytes).
pub const ERR_SERVER: i32 = 0x0000;
/// CQL error code for a protocol violation (bad frame / opcode).
pub const ERR_PROTOCOL: i32 = 0x000A;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{HEADER_LEN, RESPONSE_VERSION};

    #[test]
    fn parse_query_request_extracts_cql() {
        let cql = "SELECT * FROM t WHERE pk = 'a'";
        let mut body = Vec::new();
        body.extend_from_slice(&(cql.len() as i32).to_be_bytes());
        body.extend_from_slice(cql.as_bytes());
        // trailing consistency + flags (ignored)
        body.extend_from_slice(&1u16.to_be_bytes());
        body.push(0);
        let req = parse_query_request(&body).unwrap();
        assert_eq!(req.cql, cql);
    }

    #[test]
    fn ready_is_a_well_formed_response_frame() {
        let bytes = ready(9);
        assert_eq!(bytes[0], RESPONSE_VERSION);
        assert_eq!(bytes[4], Opcode::Ready as u8);
        assert_eq!(Frame::body_len(&bytes[..HEADER_LEN]).unwrap(), 0);
    }

    #[test]
    fn rows_result_present_and_absent_decode_consistently() {
        let present = rows_result(3, "t", Some(("a", "hello")));
        let absent = rows_result(3, "t", None);
        // The body must declare the RESULT/Rows kind in both cases.
        assert_eq!(&present[HEADER_LEN..HEADER_LEN + 4], &2i32.to_be_bytes());
        assert_eq!(&absent[HEADER_LEN..HEADER_LEN + 4], &2i32.to_be_bytes());
        // The present body is longer (it carries a row).
        assert!(present.len() > absent.len());
    }

    #[test]
    fn supported_advertises_cql_version() {
        let bytes = supported(1);
        let body = &bytes[HEADER_LEN..];
        // Crude check: the advertised version string appears in the body.
        assert!(
            body.windows(5).any(|w| w == b"3.0.0"),
            "SUPPORTED should advertise CQL_VERSION 3.0.0"
        );
    }
}
