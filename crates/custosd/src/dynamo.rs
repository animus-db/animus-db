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
//! Supported: `CreateTable`, `PutItem`, `GetItem`, `DeleteItem`, `Query`. The
//! data-plane key for an item is `escape(table) || escape(pk) || sk` (so tables
//! share one keyspace without colliding). The data plane has no native delete,
//! so `DeleteItem` writes a tombstone value that `GetItem` reads back as absent.
//!
//! ## Per-table schemas (CreateTable)
//!
//! `CreateTable` records a table's key schema in a process-wide
//! [`SchemaRegistry`]. Later requests resolve their key attributes against it,
//! so the key convention is no longer hard-coded. For backward compatibility, a
//! request against a table that was never `CreateTable`d **auto-registers** the
//! legacy convention (partition key `pk`, optional sort key `sk`).
//!
//! The registry is **in-memory and not durable**: schemas (and the `Query` key
//! index below) are lost on restart, and in the single-process `--cluster N`
//! dev mode every node shares one registry (in a one-process-per-node
//! deployment each process keeps its own). Replicating schemas through the
//! control plane is future work.
//!
//! ## Query
//!
//! The data plane has no quorum range scan (only point read/write/delete), so
//! `Query` is served by tracking, per table, the storage keys of written items
//! ([`SchemaRegistry::query_keys`]) and quorum-reading each matching key through
//! the same coordinator. The partition (`pk = ..`) plus an optional sort-key
//! condition (`=`, `BETWEEN`, `begins_with`) selects the contiguous key
//! sub-range; tombstoned/absent keys are skipped. Like the schema map, the key
//! index is in-memory and observation-built.

use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use custos_dynamo::wire::{self, Operation, WireError};
use custos_dynamo::{
    AttributeValue, ConditionExpression, Item, SchemaRegistry, SortKeyCondition, storage_key,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::ClientCtx;

const DYNAMO_TIMEOUT: Duration = Duration::from_secs(5);

/// Process-wide table-schema + Query-key registry. Not durable; see module docs.
fn registry() -> &'static Mutex<SchemaRegistry> {
    static REGISTRY: OnceLock<Mutex<SchemaRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(SchemaRegistry::new()))
}

/// Resolve `table`'s key attribute values from `item`. A registered table uses
/// its recorded schema; an unregistered table falls back to the legacy
/// convention (partition key `pk` required, sort key `sk` optional), so
/// pre-`CreateTable` clients keep working without change.
fn resolve_key(
    table: &str,
    item: &Item,
) -> Result<(AttributeValue, Option<AttributeValue>), WireError> {
    let mut reg = registry().lock().expect("registry poisoned");
    if !reg.has_table(table) {
        // Pre-CreateTable clients keep working under the legacy `pk`/`sk`
        // convention (pk required, sk optional); this also lets `Query` track
        // the table's keys.
        reg.create_table_legacy(table);
    }
    reg.extract_key(table, item).map_err(registry_error)
}
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
        Operation::CreateTable { table, schema } => {
            registry()
                .lock()
                .expect("registry poisoned")
                .create_table(&table, schema.clone())
                .map_err(registry_error)?;
            Ok(wire::create_table_response(&table, &schema))
        }
        Operation::PutItem {
            table,
            item,
            condition,
        } => {
            let (pk, sk) = resolve_key(&table, &item)?;
            let within = storage_key(&pk, sk.as_ref());
            let key = data_key(&table, &within);
            if let Some(cond) = &condition {
                check_condition(ctx, &key, cond).await?;
            }
            let value = wire::encode_stored_item(&item);
            quorum_write(ctx, &key, &value).await?;
            note_put(&table, &within);
            Ok(wire::empty_response())
        }
        Operation::DeleteItem {
            table,
            key,
            condition,
        } => {
            let (pk, sk) = resolve_key(&table, &key)?;
            let within = storage_key(&pk, sk.as_ref());
            let data_key = data_key(&table, &within);
            if let Some(cond) = &condition {
                check_condition(ctx, &data_key, cond).await?;
            }
            let value = wire::encode_tombstone();
            quorum_write(ctx, &data_key, &value).await?;
            note_delete(&table, &within);
            Ok(wire::empty_response())
        }
        Operation::GetItem { table, key } => {
            let (pk, sk) = resolve_key(&table, &key)?;
            let data_key = data_key(&table, &storage_key(&pk, sk.as_ref()));
            let item = quorum_read(ctx, &data_key).await?;
            Ok(wire::get_item_response(item.as_ref()))
        }
        Operation::Query {
            table,
            partition_value,
            sort_condition,
        } => run_query(ctx, &table, &partition_value, sort_condition.as_ref()).await,
    }
}

/// Resolve the partition's matching within-table keys from the registry, then
/// quorum-read each to assemble the result (the data plane has no range scan).
async fn run_query(
    ctx: &ClientCtx,
    table: &str,
    partition_value: &AttributeValue,
    sort_condition: Option<&SortKeyCondition>,
) -> Result<String, WireError> {
    let within_keys = {
        let reg = registry().lock().expect("registry poisoned");
        reg.query_keys(table, partition_value, sort_condition)
            .map_err(registry_error)?
    };
    let mut items = Vec::with_capacity(within_keys.len());
    for within in &within_keys {
        let data_key = data_key(table, within);
        if let Some(item) = quorum_read(ctx, &data_key).await? {
            items.push(item);
        }
    }
    Ok(wire::query_response(&items))
}

/// Enforce a `ConditionExpression` by reading the current item under the coord
/// lock; a false predicate is a `ConditionalCheckFailedException`.
async fn check_condition(
    ctx: &ClientCtx,
    key: &[u8],
    condition: &ConditionExpression,
) -> Result<(), WireError> {
    let current = quorum_read(ctx, key).await?;
    if condition.evaluate(current.as_ref()) {
        Ok(())
    } else {
        Err(WireError::conditional_check_failed(
            "the conditional request failed",
        ))
    }
}

fn note_put(table: &str, within_key: &[u8]) {
    let mut reg = registry().lock().expect("registry poisoned");
    if !reg.has_table(table) {
        reg.create_table_legacy(table);
    }
    let _ = reg.note_put(table, within_key);
}

fn note_delete(table: &str, within_key: &[u8]) {
    let mut reg = registry().lock().expect("registry poisoned");
    let _ = reg.note_delete(table, within_key);
}

/// Map a registry error to a DynamoDB wire error code.
fn registry_error(err: custos_dynamo::RegistryError) -> WireError {
    use custos_dynamo::RegistryError as R;
    match err {
        R::NoSuchTable(t) => WireError {
            code: "ResourceNotFoundException",
            message: format!("table `{t}` does not exist"),
        },
        R::TableExists(t) => WireError {
            code: "ResourceInUseException",
            message: format!("table `{t}` already exists"),
        },
        R::MissingKey(k) => WireError {
            code: "ValidationException",
            message: format!("missing key attribute `{k}`"),
        },
        R::SortKeyMismatch(t) => WireError {
            code: "ValidationException",
            message: format!("table `{t}` has no sort key for this condition"),
        },
    }
}

/// The data-plane key for an item: `escape(table) || within_key`, where
/// `within_key` is `storage_key(pk, sk)`. Sharing one keyspace, tables don't
/// collide because the escaped table name is prefix-free.
fn data_key(table: &str, within_key: &[u8]) -> Vec<u8> {
    let mut key = storage_key(&AttributeValue::S(table.to_owned()), None);
    key.extend_from_slice(within_key);
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
