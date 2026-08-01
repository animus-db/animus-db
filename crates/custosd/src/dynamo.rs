//! The DynamoDB JSON wire endpoint (ADR 0006).
//!
//! A minimal, hand-rolled HTTP/1.1 server over a real tokio [`TcpListener`] that
//! speaks the DynamoDB JSON protocol: clients `POST /` with an
//! `X-Amz-Target: DynamoDB_20120810.<Op>` header and an AttributeValue-JSON
//! body. We decode the request with [`custos_dynamo::wire`] (pure, deterministic
//! translation) and route the resulting key/value bytes through the **same**
//! quorum coordinator the plain-TCP client API uses — so everything below this
//! HTTP edge stays on the existing `Env`-based data-plane paths. The HTTP edge
//! itself is production-only I/O, like `ProdEnv`.
//!
//! ## Why hand-rolled HTTP
//!
//! The repo deliberately avoids heavy web stacks (axum/hyper) to keep
//! `cargo deny` trivial. DynamoDB clients send simple, single-shot
//! `POST` requests with a `Content-Length` body, so a small reader that parses
//! the request line, headers, and a fixed-length body is enough.
//!
//! ## Operations and storage mapping
//!
//! Supported: `PutItem`, `GetItem`, `DeleteItem`. The data-plane key for an
//! item is `escape(table) || escape(pk) || sk` (so tables share one keyspace
//! without colliding). The data plane has no native delete, so `DeleteItem`
//! writes a tombstone value that `GetItem` reads back as absent.
//!
//! ## Schema simplification
//!
//! DynamoDB tables are created with an explicit key schema (CreateTable). We do
//! not implement CreateTable yet, so this edge uses a **fixed convention**: the
//! partition key is the attribute named `pk` and the optional sort key is named
//! `sk`. A request whose `Key`/`Item` lacks `pk` is a `ValidationException`.
//! CreateTable and per-table schemas are deferred.

use std::time::Duration;

use custos_dynamo::wire::{self, Operation, WireError};
use custos_dynamo::{AttributeValue, Item, storage_key};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::ClientCtx;

/// The convention partition-key attribute name (no CreateTable yet).
const PARTITION_KEY: &str = "pk";
/// The convention sort-key attribute name (optional).
const SORT_KEY: &str = "sk";
const DYNAMO_TIMEOUT: Duration = Duration::from_secs(5);
/// Cap on a request body, so a malformed `Content-Length` can't exhaust memory.
const MAX_BODY: usize = 1 << 20;

/// Accept loop for the DynamoDB HTTP endpoint. Each connection is handled on its
/// own task; HTTP/1.1 keep-alive lets a client reuse the connection.
pub(crate) async fn serve(listener: TcpListener, ctx: ClientCtx) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_conn(stream, ctx).await {
                        tracing::debug!(?err, "dynamo connection closed");
                    }
                });
            }
            Err(err) => {
                tracing::warn!(?err, "dynamo accept failed");
                return;
            }
        }
    }
}

async fn handle_conn(mut stream: TcpStream, ctx: ClientCtx) -> std::io::Result<()> {
    let mut buf = Vec::new();
    loop {
        let Some(request) = read_http_request(&mut stream, &mut buf).await? else {
            return Ok(()); // clean EOF
        };
        let keep_alive = request.keep_alive;
        let (status, body) = dispatch(&ctx, &request).await;
        write_http_response(&mut stream, status, &body, keep_alive).await?;
        if !keep_alive {
            // The client asked us to close (HTTP/1.0 default, or an explicit
            // `Connection: close`). Returning drops the stream, closing the
            // socket so a client doing a single request/`read_to_end` unblocks.
            return Ok(());
        }
    }
}

/// A parsed HTTP request: the `X-Amz-Target` header value, the body bytes, and
/// whether the client wants the connection kept alive.
struct HttpRequest {
    target: String,
    body: Vec<u8>,
    keep_alive: bool,
}

/// Read one HTTP/1.1 request from `stream`, buffering into `buf` (which may
/// already hold bytes of the next pipelined request). Returns `None` at clean
/// EOF before any bytes of a new request.
async fn read_http_request(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<HttpRequest>> {
    // Read until we have the full header block (terminated by CRLFCRLF).
    let header_end = loop {
        if let Some(pos) = find_subslice(buf, b"\r\n\r\n") {
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
    let mut lines = header_text.split("\r\n");
    // HTTP/1.1 defaults to keep-alive; HTTP/1.0 defaults to close. An explicit
    // `Connection` header overrides either way.
    let request_line = lines.next().unwrap_or("");
    let mut keep_alive = request_line.contains("HTTP/1.1");
    let mut target = String::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim();
            match name.as_str() {
                "x-amz-target" => target = value.to_owned(),
                "content-length" => {
                    content_length = value.parse().map_err(|_| eof("invalid Content-Length"))?;
                }
                "connection" => {
                    let v = value.to_ascii_lowercase();
                    if v.contains("close") {
                        keep_alive = false;
                    } else if v.contains("keep-alive") {
                        keep_alive = true;
                    }
                }
                _ => {}
            }
        }
    }
    if content_length > MAX_BODY {
        return Err(eof("request body too large"));
    }

    // Read the body (some of which may already be buffered).
    let mut body_buf = buf.split_off(header_end);
    while body_buf.len() < content_length {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(eof("connection closed mid-body"));
        }
        body_buf.extend_from_slice(&chunk[..n]);
    }
    // Any surplus belongs to the next pipelined request.
    let leftover = body_buf.split_off(content_length);
    *buf = leftover;

    Ok(Some(HttpRequest {
        target,
        body: body_buf,
        keep_alive,
    }))
}

fn eof(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.to_string())
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Dispatch a decoded request, returning the HTTP status code and JSON body.
async fn dispatch(ctx: &ClientCtx, request: &HttpRequest) -> (u16, String) {
    match wire::decode_request(&request.target, &request.body) {
        Ok(op) => match run_operation(ctx, op).await {
            Ok(body) => (200, body),
            Err(err) => (error_status(&err), err.to_json()),
        },
        Err(err) => (error_status(&err), err.to_json()),
    }
}

fn error_status(err: &WireError) -> u16 {
    match err.code {
        "UnknownOperationException" => 400,
        // DynamoDB returns 400 for client errors generally; 500 only for our own
        // internal failures (no quorum, corrupt stored bytes).
        "InternalServerError" => 500,
        _ => 400,
    }
}

/// Execute a decoded operation against the data plane via the shared coordinator.
async fn run_operation(ctx: &ClientCtx, op: Operation) -> Result<String, WireError> {
    match op {
        Operation::PutItem { table, item } => {
            let (pk, sk) = extract_key(&item)?;
            let key = data_key(&table, &pk, sk.as_ref());
            let value = wire::encode_stored_item(&item);
            quorum_write(ctx, &key, &value).await?;
            Ok(wire::empty_response())
        }
        Operation::DeleteItem { table, key } => {
            let (pk, sk) = extract_key(&key)?;
            let data_key = data_key(&table, &pk, sk.as_ref());
            let value = wire::encode_tombstone();
            quorum_write(ctx, &data_key, &value).await?;
            Ok(wire::empty_response())
        }
        Operation::GetItem { table, key } => {
            let (pk, sk) = extract_key(&key)?;
            let data_key = data_key(&table, &pk, sk.as_ref());
            let item = quorum_read(ctx, &data_key).await?;
            Ok(wire::get_item_response(item.as_ref()))
        }
    }
}

/// Pull the convention key attributes (`pk`, optional `sk`) out of an item.
fn extract_key(item: &Item) -> Result<(AttributeValue, Option<AttributeValue>), WireError> {
    let pk = item.get(PARTITION_KEY).cloned().ok_or_else(|| WireError {
        code: "ValidationException",
        message: format!("item is missing the partition-key attribute `{PARTITION_KEY}`"),
    })?;
    let sk = item.get(SORT_KEY).cloned();
    Ok((pk, sk))
}

/// The data-plane key for an item: `escape(table) || storage_key(pk, sk)`.
fn data_key(table: &str, pk: &AttributeValue, sk: Option<&AttributeValue>) -> Vec<u8> {
    let mut key = storage_key(&AttributeValue::S(table.to_owned()), None);
    key.extend_from_slice(&storage_key(pk, sk));
    key
}

async fn quorum_write(ctx: &ClientCtx, key: &[u8], value: &[u8]) -> Result<(), WireError> {
    let view = ctx.view_for(key).ok_or_else(internal_no_tablet)?;
    let _guard = ctx.coord_lock.lock().await;
    // Quorum-derived version (same as the plain-TCP Put path): read the current
    // version across a quorum, then write at +1 so cross-coordinator overwrites
    // are not silently dropped.
    let current = ctx
        .coordinator
        .read_version(&view, key, DYNAMO_TIMEOUT)
        .await
        .ok_or_else(|| internal("could not read current version"))?;
    let ok = ctx
        .coordinator
        .write(&view, key, value, current + 1, DYNAMO_TIMEOUT)
        .await;
    if ok {
        Ok(())
    } else {
        Err(internal("write did not reach a quorum"))
    }
}

async fn quorum_read(ctx: &ClientCtx, key: &[u8]) -> Result<Option<Item>, WireError> {
    let view = ctx.view_for(key).ok_or_else(internal_no_tablet)?;
    let _guard = ctx.coord_lock.lock().await;
    match ctx.coordinator.read(&view, key, DYNAMO_TIMEOUT).await {
        custos_data::ReadResult::Value(Some(bytes)) => wire::decode_stored_item(&bytes),
        custos_data::ReadResult::Value(None) => Ok(None),
        custos_data::ReadResult::Failed => Err(internal("read did not reach a quorum")),
    }
}

fn internal(message: &str) -> WireError {
    WireError {
        code: "InternalServerError",
        message: message.to_owned(),
    }
}

fn internal_no_tablet() -> WireError {
    internal("no tablet covers this key yet (cluster still bootstrapping)")
}

/// Write a minimal HTTP/1.1 response with a JSON body. The `Connection` header
/// echoes the client's keep-alive choice so a `Connection: close` client (which
/// then reads to EOF) is unblocked by the socket closing.
async fn write_http_response(
    stream: &mut TcpStream,
    status: u16,
    body: &str,
    keep_alive: bool,
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        500 => "Internal Server Error",
        _ => "Status",
    };
    let connection = if keep_alive { "keep-alive" } else { "close" };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: application/x-amz-json-1.0\r\n\
         Content-Length: {}\r\n\
         Connection: {connection}\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}
