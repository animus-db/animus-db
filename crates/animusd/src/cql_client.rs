//! A minimal **loopback CQL client** used by the admin dashboard's CQL editor
//! (ADR 0021, `POST /admin/data/cql`). The browser cannot speak the CQL binary
//! protocol, and the CQL edge's executor emits binary RESULT frames — so rather
//! than refactoring that 1000-line edge, the admin handler runs the editor's
//! statements by connecting as an ordinary CQL client to **this node's own CQL
//! port** (`STARTUP → READY`, then one `QUERY` per statement) and decoding the
//! RESULT frame into JSON. The whole CQL execution path (parse/plan/CP routing)
//! is reused untouched, exactly as `cqlsh` would drive it.
//!
//! It models only the slice the editor needs: scalar column types (the only ones
//! the server emits, `animus_cql::types`), one statement per `QUERY`, no paging.
//! Statements are split on `;` (naive — a `;` inside a string literal would split
//! wrongly; acceptable for a debug tool).

use std::net::SocketAddr;

use animus_cql::types::{CqlType, CqlValue};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const REQUEST_VERSION: u8 = 0x04;
const HEADER_LEN: usize = 9;
const OP_ERROR: u8 = 0x00;
const OP_STARTUP: u8 = 0x01;
const OP_READY: u8 = 0x02;
const OP_QUERY: u8 = 0x07;
const OP_RESULT: u8 = 0x08;

/// Run `cql` (one or more `;`-separated statements) against the CQL endpoint at
/// `addr`, optionally `USE`ing `keyspace` first. Returns one JSON result per
/// statement, or an `Err(message)` if the connection/handshake fails.
pub(crate) async fn run(
    addr: SocketAddr,
    keyspace: Option<&str>,
    cql: &str,
) -> Result<Vec<Value>, String> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect to CQL port {addr}: {e}"))?;
    startup(&mut stream).await?;

    let mut out = Vec::new();
    if let Some(ks) = keyspace.map(str::trim).filter(|k| !k.is_empty()) {
        out.push(query(&mut stream, &format!("USE {ks}")).await?);
    }
    for stmt in cql.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        out.push(query(&mut stream, stmt).await?);
    }
    Ok(out)
}

/// STARTUP → READY handshake (`{"CQL_VERSION": "3.0.0"}`).
async fn startup(stream: &mut TcpStream) -> Result<(), String> {
    let mut body = Vec::new();
    put_short(&mut body, 1); // one option
    put_string(&mut body, "CQL_VERSION");
    put_string(&mut body, "3.0.0");
    write_frame(stream, OP_STARTUP, &body)
        .await
        .map_err(|e| format!("send STARTUP: {e}"))?;
    let (opcode, frame_body) = read_frame(stream)
        .await
        .map_err(|e| format!("read STARTUP reply: {e}"))?;
    match opcode {
        OP_READY => Ok(()),
        OP_ERROR => Err(decode_error(&frame_body)),
        other => Err(format!(
            "unexpected opcode {other:#x} after STARTUP (auth unsupported)"
        )),
    }
}

/// Run one statement as a `QUERY` and decode its RESULT into JSON.
async fn query(stream: &mut TcpStream, statement: &str) -> Result<Value, String> {
    let mut body = Vec::new();
    put_long_string(&mut body, statement);
    put_short(&mut body, 0x0001); // consistency ONE (moot under CP, but required)
    body.push(0x00); // query flags: none
    write_frame(stream, OP_QUERY, &body)
        .await
        .map_err(|e| format!("send QUERY: {e}"))?;
    let (opcode, frame_body) = read_frame(stream)
        .await
        .map_err(|e| format!("read QUERY reply: {e}"))?;
    Ok(decode_reply(statement, opcode, &frame_body))
}

/// Map a reply frame to the editor's per-statement JSON shape.
fn decode_reply(statement: &str, opcode: u8, body: &[u8]) -> Value {
    match opcode {
        OP_ERROR => json!({"statement": statement, "kind": "error", "error": decode_error(body)}),
        OP_RESULT => decode_result(statement, body)
            .unwrap_or_else(|e| json!({"statement": statement, "kind": "error", "error": e})),
        other => {
            json!({"statement": statement, "kind": "error", "error": format!("unexpected opcode {other:#x}")})
        }
    }
}

fn decode_error(body: &[u8]) -> String {
    let mut r = Reader::new(body);
    let code = r.i32().unwrap_or(0);
    let msg = r.string().unwrap_or_default();
    format!("[{code:#06x}] {msg}")
}

/// Decode a RESULT frame body by kind: Void / Rows / SetKeyspace / SchemaChange.
fn decode_result(statement: &str, body: &[u8]) -> Result<Value, String> {
    let mut r = Reader::new(body);
    let kind = r.i32().ok_or("truncated RESULT kind")?;
    match kind {
        0x0001 => Ok(json!({"statement": statement, "kind": "void"})),
        0x0002 => decode_rows(statement, &mut r),
        0x0003 => {
            let ks = r.string().ok_or("truncated SetKeyspace")?;
            Ok(json!({"statement": statement, "kind": "set_keyspace", "keyspace": ks}))
        }
        0x0005 => {
            let change = r.string().unwrap_or_default();
            let target = r.string().unwrap_or_default();
            Ok(
                json!({"statement": statement, "kind": "schema_change", "change": change, "target": target}),
            )
        }
        other => Ok(json!({"statement": statement, "kind": "other", "result_kind": other})),
    }
}

/// Decode a RESULT/Rows body: metadata (column specs) then the row cells.
fn decode_rows(statement: &str, r: &mut Reader) -> Result<Value, String> {
    let flags = r.i32().ok_or("truncated rows flags")?;
    let col_count = r.i32().ok_or("truncated column count")?;
    let global_spec = flags & 0x0001 != 0;
    if global_spec {
        r.string().ok_or("truncated global keyspace")?;
        r.string().ok_or("truncated global table")?;
    }
    let mut columns = Vec::with_capacity(col_count.max(0) as usize);
    let mut types = Vec::with_capacity(col_count.max(0) as usize);
    for _ in 0..col_count.max(0) {
        if !global_spec {
            r.string().ok_or("truncated per-column keyspace")?;
            r.string().ok_or("truncated per-column table")?;
        }
        let name = r.string().ok_or("truncated column name")?;
        let type_id = r.i16().ok_or("truncated column type")?;
        columns.push(name);
        types.push(type_from_id(type_id));
    }
    let row_count = r.i32().ok_or("truncated row count")?;
    let mut rows = Vec::with_capacity(row_count.max(0) as usize);
    for _ in 0..row_count.max(0) {
        let mut cells = Vec::with_capacity(columns.len());
        for ty in &types {
            match r.bytes().ok_or("truncated cell")? {
                None => cells.push(Value::Null),
                Some(raw) => cells.push(render_cell(*ty, raw)),
            }
        }
        rows.push(Value::Array(cells));
    }
    Ok(json!({
        "statement": statement,
        "kind": "rows",
        "columns": columns,
        "rows": rows,
        "row_count": rows.len(),
    }))
}

/// Render a cell's bytes via the column's type, falling back to lossy UTF-8.
fn render_cell(ty: Option<CqlType>, raw: &[u8]) -> Value {
    match ty.and_then(|t| t.decode(raw).ok()) {
        Some(v) => Value::String(render_value(&v)),
        None => Value::String(String::from_utf8_lossy(raw).into_owned()),
    }
}

/// A human-readable rendering of a decoded cell value. Blobs render as unpadded
/// base64url (the display convention across the admin/dashboard surfaces — this
/// is a JSON proxy response, not CQL text, where a blob would be a `0x`
/// literal); everything else uses the value's canonical CQL text form
/// ([`CqlValue::display`] — UUIDs keep the standard hyphenated form).
fn render_value(v: &CqlValue) -> String {
    match v {
        CqlValue::Blob(b) => animus_dynamo::wire::base64url_encode(b),
        other => other.display(),
    }
}

/// Map a CQL type id to the scalar [`CqlType`]s the server emits (`animus_cql`).
fn type_from_id(id: i16) -> Option<CqlType> {
    match id {
        0x000D => Some(CqlType::Text),
        0x0009 => Some(CqlType::Int),
        0x0002 => Some(CqlType::BigInt),
        0x0004 => Some(CqlType::Boolean),
        0x0003 => Some(CqlType::Blob),
        0x000C => Some(CqlType::Uuid),
        _ => None,
    }
}

// ---- framing -----------------------------------------------------------------

async fn write_frame(stream: &mut TcpStream, opcode: u8, body: &[u8]) -> std::io::Result<()> {
    let mut frame = Vec::with_capacity(HEADER_LEN + body.len());
    frame.push(REQUEST_VERSION);
    frame.push(0x00); // flags
    frame.extend_from_slice(&1i16.to_be_bytes()); // stream id
    frame.push(opcode);
    frame.extend_from_slice(&(body.len() as i32).to_be_bytes());
    frame.extend_from_slice(body);
    stream.write_all(&frame).await?;
    stream.flush().await
}

/// Read one response frame, returning `(opcode, body)`.
async fn read_frame(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; HEADER_LEN];
    stream.read_exact(&mut header).await?;
    let opcode = header[4];
    let len = i32::from_be_bytes([header[5], header[6], header[7], header[8]]).max(0) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    Ok((opcode, body))
}

fn put_short(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_be_bytes());
}
fn put_string(buf: &mut Vec<u8>, s: &str) {
    put_short(buf, s.len() as u16);
    buf.extend_from_slice(s.as_bytes());
}
fn put_long_string(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

/// A cursor over a frame body with the CQL primitive readers the decoder needs.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }
    fn i16(&mut self) -> Option<i16> {
        self.take(2).map(|b| i16::from_be_bytes([b[0], b[1]]))
    }
    fn i32(&mut self) -> Option<i32> {
        self.take(4)
            .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    /// A `[string]`: a `u16` length then that many UTF-8 bytes.
    fn string(&mut self) -> Option<String> {
        let len = self.i16()? as usize;
        self.take(len)
            .map(|b| String::from_utf8_lossy(b).into_owned())
    }
    /// A `[bytes]`: an `i32` length (negative = null) then that many bytes.
    fn bytes(&mut self) -> Option<Option<&'a [u8]>> {
        let len = self.i32()?;
        if len < 0 {
            return Some(None);
        }
        self.take(len as usize).map(Some)
    }
}
