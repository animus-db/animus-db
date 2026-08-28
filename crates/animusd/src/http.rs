//! Thin `TcpStream` wrapper over `animus-node`'s pure HTTP helpers (ADR 0061
//! rung C4a): [`read_http_request`] does only `stream.read()`, handing every
//! parsing decision to [`animus_node::http::parse_request_head`];
//! [`write_response`]/[`write_response_with`] do only `stream.write_all()`,
//! formatting via [`animus_node::http::format_response`]. `HttpRequest`
//! itself, `query_param`, `CORS_HEADERS`, and `eof` are re-exported from
//! there — every existing `http::*` call site this crate still uses (across
//! `dynamo.rs`/`admin.rs`) keeps compiling unchanged. `percent_decode` isn't
//! re-exported here any more — its one caller (table/index-name decoding in
//! `console.rs`'s per-table routing) moved to `animus-node` whole (ADR 0061
//! rung C4c), so nothing in this crate calls it directly today. Real tokio
//! sockets — this is `ProdEnv`-only edge code, never under `SimEnv`.

pub(crate) use animus_node::http::{CORS_HEADERS, HttpRequest, MAX_BODY, eof, query_param};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Read one HTTP/1.1 request from `stream`, buffering into `buf` (which may
/// already hold bytes of the next pipelined request). Returns `None` at clean
/// EOF before any bytes of a new request. All parsing (header-block framing,
/// `Content-Length` validation, header lowercasing/comma-joining) happens in
/// `animus_node::http::parse_request_head`; this function does only the
/// socket reads.
pub(crate) async fn read_http_request(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<HttpRequest>> {
    // Read until we have the full header block (terminated by CRLFCRLF).
    let header_end = loop {
        if let Some(pos) = animus_node::http::find_subslice(buf, b"\r\n\r\n") {
            break pos + 4;
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return if buf.is_empty() {
                Ok(None)
            } else {
                Err(eof("connection closed mid-request"))
            };
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > MAX_BODY {
            return Err(eof("request headers too large"));
        }
    };

    let header_text = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let head = animus_node::http::parse_request_head(&header_text)?;

    // Read the body (some of which may already be buffered).
    let mut body_buf = buf.split_off(header_end);
    while body_buf.len() < head.content_length {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(eof("connection closed mid-body"));
        }
        body_buf.extend_from_slice(&chunk[..n]);
    }
    // Any surplus belongs to the next pipelined request.
    let leftover = body_buf.split_off(head.content_length);
    *buf = leftover;

    Ok(Some(HttpRequest {
        method: head.method,
        path: head.path,
        query: head.query,
        target: head.target,
        headers: head.headers,
        body: body_buf,
        keep_alive: head.keep_alive,
    }))
}

/// Write a minimal HTTP/1.1 response with the given `content_type` and `body`.
/// The `Connection` header echoes the client's keep-alive choice so a
/// `Connection: close` client (which then reads to EOF) is unblocked by the
/// socket closing.
pub(crate) async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
    keep_alive: bool,
) -> std::io::Result<()> {
    write_response_with(stream, status, content_type, body, keep_alive, "").await
}

/// Like [`write_response`], but splices `extra_headers` (a block of complete
/// CRLF-terminated header lines, e.g. [`CORS_HEADERS`]) into the response before
/// the blank line. Pass `""` for none. Formatting happens in
/// `animus_node::http::format_response`; this function does only the socket
/// write.
pub(crate) async fn write_response_with(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
    keep_alive: bool,
    extra_headers: &str,
) -> std::io::Result<()> {
    let response =
        animus_node::http::format_response(status, content_type, body, keep_alive, extra_headers);
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

/// Write a DynamoDB-protocol JSON response (`application/x-amz-json-1.0`).
pub(crate) async fn write_amz_json_response(
    stream: &mut TcpStream,
    status: u16,
    body: &str,
    keep_alive: bool,
) -> std::io::Result<()> {
    write_response(
        stream,
        status,
        "application/x-amz-json-1.0",
        body,
        keep_alive,
    )
    .await
}

/// Write a `text/plain` response — used by the `/metrics` route, whose body is the
/// line-oriented metrics export rather than JSON.
pub(crate) async fn write_text_response(
    stream: &mut TcpStream,
    status: u16,
    body: &str,
    keep_alive: bool,
) -> std::io::Result<()> {
    write_response(
        stream,
        status,
        "text/plain; charset=utf-8",
        body,
        keep_alive,
    )
    .await
}
