//! Pure parsers + builders for the CQL request/response bodies this subset
//! speaks: the `STARTUP`/`OPTIONS` handshake, `QUERY`/`PREPARE`/`EXECUTE`
//! requests, and the `READY`/`SUPPORTED`/`RESULT`/`ERROR` replies. All I/O-free,
//! so the wire layer in `animusd` just frames the bytes these return.

use crate::frame::{
    self, Frame, Opcode, read_bytes, read_int, read_long_string, read_short, read_short_bytes,
    read_string, write_bytes, write_short_bytes, write_string, write_string_multimap,
};
use crate::plan::ColumnSpec;
use crate::types::CqlValue;

/// CQL `RESULT` kinds (the leading `i32` of a RESULT body).
mod result_kind {
    /// An executed statement with nothing to return (`INSERT`).
    pub const VOID: i32 = 0x0001;
    /// A row set (`SELECT`).
    pub const ROWS: i32 = 0x0002;
    /// `SetKeyspace` (`USE`).
    pub const SET_KEYSPACE: i32 = 0x0003;
    /// A prepared-statement handle (`PREPARE`).
    pub const PREPARED: i32 = 0x0004;
    /// A schema change (`CREATE TABLE`/`CREATE KEYSPACE`).
    pub const SCHEMA_CHANGE: i32 = 0x0005;
}

/// `Rows` result metadata flag: a single `[global table spec]` precedes the
/// column specs (rather than a per-column keyspace/table).
const GLOBAL_TABLES_SPEC: i32 = 0x0001;

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
/// level + query flags for this subset; a `QUERY` carries no bound values in our
/// path — those arrive via `PREPARE`/`EXECUTE`.)
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

/// Parse a `PREPARE` request body: a single `[long string]` CQL statement.
///
/// # Errors
/// Propagates a [`frame::FrameError`] if the string is malformed.
pub fn parse_prepare_request(body: &[u8]) -> Result<String, frame::FrameError> {
    let mut pos = 0;
    read_long_string(body, &mut pos)
}

/// A parsed `EXECUTE` request: the prepared-statement id and the raw bound
/// value cells (each `Some(bytes)` or `None` for a null). The values are
/// type-resolved later against the prepared statement's metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecuteRequest {
    /// The opaque prepared-statement id (echoed from the `PREPARED` result).
    pub id: Vec<u8>,
    /// The bound value cells, in order. `None` is a CQL null.
    pub values: Vec<Option<Vec<u8>>>,
}

/// `EXECUTE` query-flag bit indicating bound values follow.
const FLAG_VALUES: u8 = 0x01;

/// Parse an `EXECUTE` request body: `[short bytes] id`, `[consistency]`,
/// `[byte] flags`, then (if the VALUES flag is set) `[short] n` value cells.
///
/// The protocol allows named values and other flags; this subset reads only
/// positional values and ignores the rest of the flag bits.
///
/// # Errors
/// Propagates a [`frame::FrameError`] for a malformed body.
pub fn parse_execute_request(body: &[u8]) -> Result<ExecuteRequest, frame::FrameError> {
    let mut pos = 0;
    let id = read_short_bytes(body, &mut pos)?;
    let _consistency = read_short(body, &mut pos)?;
    let flags = read_byte(body, &mut pos)?;
    let mut values = Vec::new();
    if flags & FLAG_VALUES != 0 {
        let n = read_short(body, &mut pos)?;
        for _ in 0..n {
            values.push(read_bytes(body, &mut pos)?);
        }
    }
    Ok(ExecuteRequest { id, values })
}

fn read_byte(buf: &[u8], pos: &mut usize) -> Result<u8, frame::FrameError> {
    let b = *buf.get(*pos).ok_or(frame::FrameError::Truncated)?;
    *pos += 1;
    Ok(b)
}

/// Parse the prepared-statement id out of an `EXECUTE` body without fully
/// decoding the values — used to look the statement up before resolving values.
/// (Re-implemented for convenience; equivalent to `parse_execute_request(..).id`.)
///
/// # Errors
/// Propagates a [`frame::FrameError`] for a malformed body.
pub fn execute_statement_id(body: &[u8]) -> Result<Vec<u8>, frame::FrameError> {
    let mut pos = 0;
    read_short_bytes(body, &mut pos)
}

// --- response builders ------------------------------------------------------

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

/// Encode a `RESULT` of kind `Void` (response to a successful `INSERT`/EXECUTE).
#[must_use]
pub fn void_result(stream: i16) -> Vec<u8> {
    let body = result_kind::VOID.to_be_bytes().to_vec();
    Frame::encode_response(stream, Opcode::Result, &body)
}

/// Encode a `RESULT` of kind `SetKeyspace` (response to `USE <keyspace>`).
#[must_use]
pub fn set_keyspace_result(stream: i16, keyspace: &str) -> Vec<u8> {
    let mut body = result_kind::SET_KEYSPACE.to_be_bytes().to_vec();
    write_string(&mut body, keyspace);
    Frame::encode_response(stream, Opcode::Result, &body)
}

/// Encode a `RESULT` of kind `SchemaChange` (response to `CREATE TABLE` /
/// `CREATE KEYSPACE`). `target` is `"TABLE"` or `"KEYSPACE"`.
#[must_use]
pub fn schema_change_result(
    stream: i16,
    change: &str,
    target: &str,
    keyspace: &str,
    name: &str,
) -> Vec<u8> {
    let mut body = result_kind::SCHEMA_CHANGE.to_be_bytes().to_vec();
    write_string(&mut body, change); // e.g. "CREATED"
    write_string(&mut body, target); // "KEYSPACE" | "TABLE"
    write_string(&mut body, keyspace);
    if target != "KEYSPACE" {
        write_string(&mut body, name);
    }
    Frame::encode_response(stream, Opcode::Result, &body)
}

/// Write a `<metadata>` block: flags + column count + global table spec +
/// per-column `<name><type option>`.
fn write_metadata(out: &mut Vec<u8>, keyspace: &str, table: &str, columns: &[ColumnSpec]) {
    out.extend_from_slice(&GLOBAL_TABLES_SPEC.to_be_bytes());
    out.extend_from_slice(&(columns.len() as i32).to_be_bytes());
    write_string(out, keyspace);
    write_string(out, table);
    for col in columns {
        write_string(out, &col.name);
        col.ty.write_option(out);
    }
}

/// Encode a `RESULT/Prepared`: the statement id plus the bind-variable metadata
/// (the `?` markers' column specs) and an empty result metadata (filled by the
/// later `EXECUTE`/`SELECT`). `keyspace`/`table` name the prepared statement's
/// table for the metadata's global spec.
#[must_use]
pub fn prepared_result(
    stream: i16,
    id: &[u8],
    keyspace: &str,
    table: &str,
    bind_markers: &[ColumnSpec],
) -> Vec<u8> {
    let mut body = result_kind::PREPARED.to_be_bytes().to_vec();
    write_short_bytes(&mut body, id);
    // <metadata> for the bind variables (the `?` markers).
    write_metadata(&mut body, keyspace, table, bind_markers);
    // <result_metadata>: no columns (the result shape is determined at EXECUTE
    // time for our subset). flags=0, columns_count=0.
    body.extend_from_slice(&0i32.to_be_bytes());
    body.extend_from_slice(&0i32.to_be_bytes());
    Frame::encode_response(stream, Opcode::Result, &body)
}

/// Encode a `RESULT/Rows` with typed column metadata and at most one row. The
/// columns are `(spec, cell)` aligned: `row` is `Some(cells)` where each cell is
/// the value's wire bytes (`None` for a null cell), or `None` for an empty
/// result set.
#[must_use]
pub fn typed_rows_result(
    stream: i16,
    keyspace: &str,
    table: &str,
    columns: &[ColumnSpec],
    row: Option<&[Option<Vec<u8>>]>,
) -> Vec<u8> {
    let mut body = result_kind::ROWS.to_be_bytes().to_vec();
    write_metadata(&mut body, keyspace, table, columns);
    match row {
        Some(cells) => {
            body.extend_from_slice(&1i32.to_be_bytes());
            for cell in cells {
                write_bytes(&mut body, cell.as_deref());
            }
        }
        None => body.extend_from_slice(&0i32.to_be_bytes()),
    }
    Frame::encode_response(stream, Opcode::Result, &body)
}

/// Convenience: build the single result row's cells from typed values, encoding
/// each per its column spec. A `None` value produces a null cell.
///
/// # Errors
/// Propagates a [`crate::types::ValueError`] if a value does not match its spec.
pub fn encode_row_cells(
    columns: &[ColumnSpec],
    values: &[Option<CqlValue>],
) -> Result<Vec<Option<Vec<u8>>>, crate::types::ValueError> {
    let mut cells = Vec::with_capacity(columns.len());
    for (col, val) in columns.iter().zip(values) {
        match val {
            Some(v) => cells.push(Some(col.ty.encode(v)?)),
            None => cells.push(None),
        }
    }
    Ok(cells)
}

/// Encode an `ERROR` frame. `code` is a CQL error code (`0x2200` = Invalid,
/// `0x0000` = ServerError); `message` is a `[string]`.
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
/// CQL error code for an unprepared statement id (client should re-prepare).
pub const ERR_UNPREPARED: i32 = 0x2500;

/// Helper for tests / callers that read a `[string]` directly off a body.
///
/// # Errors
/// Propagates a [`frame::FrameError`].
pub fn read_body_string(body: &[u8], pos: &mut usize) -> Result<String, frame::FrameError> {
    read_string(body, pos)
}

/// Helper for tests / callers that read an `[int]` directly off a body.
///
/// # Errors
/// Propagates a [`frame::FrameError`].
pub fn read_body_int(body: &[u8], pos: &mut usize) -> Result<i32, frame::FrameError> {
    read_int(body, pos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{HEADER_LEN, RESPONSE_VERSION};
    use crate::types::CqlType;

    fn spec(name: &str, ty: CqlType) -> ColumnSpec {
        ColumnSpec {
            name: name.into(),
            ty,
            schema_index: 0,
        }
    }

    #[test]
    fn parse_query_request_extracts_cql() {
        let cql = "SELECT * FROM t WHERE pk = 'a'";
        let mut body = Vec::new();
        body.extend_from_slice(&(cql.len() as i32).to_be_bytes());
        body.extend_from_slice(cql.as_bytes());
        body.extend_from_slice(&1u16.to_be_bytes());
        body.push(0);
        let req = parse_query_request(&body).unwrap();
        assert_eq!(req.cql, cql);
    }

    #[test]
    fn execute_request_round_trips() {
        // id | consistency | flags(VALUES) | n | [bytes]* .
        let mut body = Vec::new();
        write_short_bytes(&mut body, &[1, 2, 3, 4]);
        body.extend_from_slice(&0x0001u16.to_be_bytes()); // consistency
        body.push(FLAG_VALUES);
        body.extend_from_slice(&2u16.to_be_bytes());
        write_bytes(&mut body, Some(&[0, 0, 0, 7])); // an int
        write_bytes(&mut body, Some(b"Ada"));
        let req = parse_execute_request(&body).unwrap();
        assert_eq!(req.id, vec![1, 2, 3, 4]);
        assert_eq!(req.values.len(), 2);
        assert_eq!(req.values[1].as_deref(), Some(&b"Ada"[..]));
        assert_eq!(execute_statement_id(&body).unwrap(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn ready_is_a_well_formed_response_frame() {
        let bytes = ready(9);
        assert_eq!(bytes[0], RESPONSE_VERSION);
        assert_eq!(bytes[4], Opcode::Ready as u8);
        assert_eq!(Frame::body_len(&bytes[..HEADER_LEN]).unwrap(), 0);
    }

    #[test]
    fn typed_rows_present_and_absent() {
        let cols = [spec("id", CqlType::Int), spec("name", CqlType::Text)];
        let cells = vec![Some(7i32.to_be_bytes().to_vec()), Some(b"Ada".to_vec())];
        let present = typed_rows_result(3, "ks", "t", &cols, Some(&cells));
        let absent = typed_rows_result(3, "ks", "t", &cols, None);
        assert_eq!(&present[HEADER_LEN..HEADER_LEN + 4], &2i32.to_be_bytes());
        assert_eq!(&absent[HEADER_LEN..HEADER_LEN + 4], &2i32.to_be_bytes());
        assert!(present.len() > absent.len());
        // The int type id appears in both metadata blocks.
        assert!(
            present
                .windows(2)
                .any(|w| w == CqlType::Int.type_id().to_be_bytes()),
            "metadata should carry the int type id"
        );
    }

    #[test]
    fn prepared_result_carries_id_and_bind_metadata() {
        let binds = [spec("id", CqlType::Int)];
        let bytes = prepared_result(1, &[9, 9], "ks", "t", &binds);
        assert_eq!(&bytes[HEADER_LEN..HEADER_LEN + 4], &4i32.to_be_bytes());
        // kind(4) | short-bytes len(2) | id .
        assert_eq!(&bytes[HEADER_LEN + 4..HEADER_LEN + 6], &2u16.to_be_bytes());
        assert_eq!(&bytes[HEADER_LEN + 6..HEADER_LEN + 8], &[9, 9]);
    }

    #[test]
    fn supported_advertises_cql_version() {
        let bytes = supported(1);
        let body = &bytes[HEADER_LEN..];
        assert!(body.windows(5).any(|w| w == b"3.0.0"));
    }

    #[test]
    fn encode_row_cells_types_each_value() {
        let cols = [spec("id", CqlType::Int), spec("name", CqlType::Text)];
        let vals = [Some(CqlValue::Int(7)), Some(CqlValue::Text("Ada".into()))];
        let cells = encode_row_cells(&cols, &vals).unwrap();
        assert_eq!(cells[0].as_deref(), Some(&7i32.to_be_bytes()[..]));
    }
}
