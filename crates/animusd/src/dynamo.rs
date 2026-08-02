//! The DynamoDB JSON wire endpoint (ADR 0006).
//!
//! A minimal, hand-rolled HTTP/1.1 server over a real tokio [`TcpListener`] that
//! speaks the DynamoDB JSON protocol: clients `POST /` with an
//! `X-Amz-Target: DynamoDB_20120810.<Op>` header and an AttributeValue-JSON
//! body. We decode the request with [`animus_dynamo::wire`] (pure, deterministic
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
//! Supported: `CreateTable`, `PutItem`, `GetItem`, `DeleteItem`, `Query`,
//! `Scan`, `UpdateItem`, `BatchWriteItem`, `TransactWriteItems`. The data-plane
//! key for an item is `escape(table) || escape(pk) || sk` (so tables share one
//! keyspace without colliding). The data plane has no native delete, so
//! `DeleteItem` writes a tombstone value that `GetItem` reads back as absent.
//! `UpdateItem` is a read-modify-write (`SET`/`REMOVE`); `BatchWriteItem` applies
//! put/delete requests one by one; `TransactWriteItems` applies condition-gated
//! actions in order but **without cross-action atomicity** (true ACID via Accord,
//! ADR 0011, is deferred — see [`run_transact`]).
//!
//! ## Per-table schemas: the replicated catalog (ADR 0013)
//!
//! `CreateTable` **proposes a `MetaCommand::CreateTableSchema` to the
//! control-plane leader** and waits until it commits in `Metadata`; later requests
//! resolve their key attributes from the **replicated** `Metadata::table_schema`
//! (translated DynamoDB key attrs ↔ control `TableSchema` by
//! [`animus_dynamo::schema`]). So a created table's key schema is **durable +
//! cluster-agreed**: it survives a restart (it rode the Raft WAL) and is known on
//! every node. A request against a table that was never `CreateTable`d
//! **auto-registers** the legacy convention (partition key `pk`, optional sort key
//! `sk`). The edge reaches the leader through the cluster's set of registered
//! control handles (`ClientCtx::edge`, owned by the cluster's `ClusterEdgeState`);
//! in a one-process-per-node deployment that is the node's own handle, so
//! `CreateTable` must target the leader.
//!
//! The in-memory `SchemaRegistry` now holds only the **secondary-index
//! declarations** (the GSI/LSI `escape(hash) [|| escape(sort)] || base_key`
//! index). That remains **in-memory and not durable** — rebuilt from observed
//! writes — and is **per-cluster**: held in the cluster's `ClusterEdgeState`
//! (threaded through `ClientCtx`), not a process `OnceLock`, so two in-process
//! clusters in one test do not share a registry. In `--cluster N` dev mode the
//! cluster's nodes share one registry. Only the *table key schema* moves to the
//! control plane.
//!
//! ## Query, Scan, and secondary indexes
//!
//! A **base-table** `Query` and a `Scan` are served by a **native quorum range
//! scan** ([`DataClient::scan`]) — no in-memory written-key tracking. The data
//! plane scans a contiguous data-plane key range across the tablet's replica
//! quorum (epoch-fenced, ADR 0002; merged per-key by newest version, tombstones
//! excluded) and returns the live `(key, value)` pairs in key order:
//!
//! - `Query` (`pk = ..`) scans the partition's contiguous sub-range
//!   `[escape(table) || escape(pk), …)` (the escape is prefix-free, ending
//!   `0x00 0x00`, so a partition's keys are contiguous and sort-ordered), then
//!   applies an optional sort-key condition (`=`, `BETWEEN`, `begins_with`) on the
//!   recovered sort bytes.
//! - `Scan` scans the whole table's range `[escape(table), …)` across every
//!   partition. It paginates with `Limit` + `ExclusiveStartKey`/`LastEvaluatedKey`
//!   (the cursor is the last storage key of a truncated page, surfaced to the
//!   client as that item's key-attribute map) and applies an optional
//!   `FilterExpression` after the read. The cursor now advances over the
//!   **live data-plane keys** the scan returns, not a tracked set — so it survives
//!   a restart or a follower that never saw the write.
//!
//! Because the scan reads live storage on a read quorum, a `DeleteItem` (which
//! stores a *tombstone value*, not a data-plane tombstone) still appears as a
//! live pair to the scan; the edge drops it when `decode_stored_item` decodes a
//! tombstone. A table the scan must reject as unknown is checked against the
//! replicated catalog / legacy registration ([`table_known`]).
//!
//! `CreateTable` may declare any number of **global / local secondary indexes**,
//! each with a `Projection` (`ALL`/`KEYS_ONLY`/`INCLUDE`); the registry maintains
//! an `escape(hash) [|| escape(sort)] || base_key` index per index on every
//! `note_put`/`note_delete` (no item copies — the base item stays authoritative),
//! and a `Query` with an `IndexName` resolves an index value back to its base
//! storage keys, which are quorum-read the same way (the native scan covers the
//! base keyspace, not an index's alternate ordering, so an *index* query keeps the
//! in-memory index). An index query with no explicit `ProjectionExpression`
//! returns the index's declared projected attributes (applied at the edge after
//! the base item is read).

use std::time::Duration;

use animus_control::MetaCommand;
use animus_dynamo::wire::{
    self, Operation, Projection, ReturnValues, TransactAction, UpdateAction, UpdateReturnValues,
    WireError, WriteRequest,
};
use animus_dynamo::{
    AttributeValue, ConditionExpression, Item, SortKeyCondition, TableSchema,
    schema as schema_bridge, storage_key,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::ClientCtx;

const DYNAMO_TIMEOUT: Duration = Duration::from_secs(5);
/// How long `CreateTable` waits for its `CreateTableSchema` proposal to commit in
/// the replicated catalog before giving up.
const SCHEMA_COMMIT_TIMEOUT: Duration = Duration::from_secs(5);
/// How often `CreateTable` re-checks (and re-proposes against the current leader)
/// while waiting for the schema to commit.
const SCHEMA_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// This node's snapshot of the replicated [`Metadata`](animus_control::Metadata).
/// The schema catalog is Raft-replicated, so this node's committed view is sound
/// to read (every node applies committed metadata). Per-cluster — no process
/// globals — so two in-process clusters do not share a view.
fn metadata(ctx: &ClientCtx) -> animus_control::Metadata {
    ctx.raft.metadata()
}

/// The DynamoDB key schema for `table`, resolved from the **replicated catalog**
/// (ADR 0013) when present, else the legacy `pk`/`sk` convention so a
/// pre-`CreateTable` client keeps working.
fn schema_for(ctx: &ClientCtx, table: &str) -> TableSchema {
    match metadata(ctx).table_schema(table) {
        Some(control) => schema_bridge::to_dynamo(control),
        None => TableSchema::composite("pk", "sk"),
    }
}

/// Mirror a **catalog** table's key schema into the cluster's registry if it is
/// known to the replicated catalog (ADR 0013) but not yet mirrored — so the
/// registry's key-index / GSI machinery has the right schema after a restart or
/// on a follower that has not seen a write. A table absent from the catalog is
/// left untouched here (the read path then reports it unknown; the write path
/// legacy-registers it via [`legacy_register`]).
fn mirror_catalog_schema(ctx: &ClientCtx, table: &str) {
    if metadata(ctx).has_table_schema(table) {
        let schema = schema_for(ctx, table);
        let mut reg = ctx
            .edge
            .dynamo_registry()
            .lock()
            .expect("registry poisoned");
        if !reg.has_table(table) {
            let _ = reg.create_table(table, schema);
        }
    }
}

/// Auto-register `table` under the legacy `pk`/`sk` convention (sort key
/// optional) if it is in neither the catalog nor the registry — so a
/// pre-`CreateTable` client's writes keep working unchanged and their keys get
/// tracked for `Query`/`Scan`.
fn legacy_register(ctx: &ClientCtx, table: &str) {
    if metadata(ctx).has_table_schema(table) {
        return; // a real CreateTable'd table; mirror it instead
    }
    let mut reg = ctx
        .edge
        .dynamo_registry()
        .lock()
        .expect("registry poisoned");
    if !reg.has_table(table) {
        reg.create_table_legacy(table);
    }
}

/// Resolve `table`'s key attribute values from `item`, against the replicated
/// schema (mirrored into the cluster's registry for key-index bookkeeping). Used
/// by the *write/point* paths (Put/Get/Delete/Update), which auto-register a
/// legacy table — so an unknown table reads back as empty rather than erroring,
/// matching the prior behavior.
fn resolve_key(
    ctx: &ClientCtx,
    table: &str,
    item: &Item,
) -> Result<(AttributeValue, Option<AttributeValue>), WireError> {
    mirror_catalog_schema(ctx, table);
    legacy_register(ctx, table);
    let reg = ctx
        .edge
        .dynamo_registry()
        .lock()
        .expect("registry poisoned");
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
        // The admin `/metrics` route (ADR 0015) shares this HTTP listener — the
        // node's only HTTP edge — rather than opening a seventh port. It is a
        // plain `GET` returning the text-format snapshot, distinct from the
        // DynamoDB `POST /` + `X-Amz-Target` protocol.
        if request.method.eq_ignore_ascii_case("GET") && request.path == "/metrics" {
            let body = ctx.metrics_text();
            write_text_response(&mut stream, 200, &body, keep_alive).await?;
            if !keep_alive {
                return Ok(());
            }
            continue;
        }
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

/// A parsed HTTP request: the method and path (request line), the
/// `X-Amz-Target` header value, the body bytes, and whether the client wants the
/// connection kept alive.
struct HttpRequest {
    method: String,
    path: String,
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
    // Request line: `METHOD SP request-target SP HTTP-version`.
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or("").to_owned();
    let path = request_parts.next().unwrap_or("").to_owned();
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
        method,
        path,
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
        Operation::CreateTable {
            table,
            schema,
            indexes,
        } => create_table(ctx, &table, &schema, &indexes).await,
        Operation::PutItem {
            table,
            item,
            condition,
            return_values,
        } => {
            let (pk, sk) = resolve_key(ctx, &table, &item)?;
            let within = storage_key(&pk, sk.as_ref());
            let key = data_key(&table, &within);
            // For ALL_OLD (or a condition) we need the prior item; read it once.
            let needs_old = condition.is_some() || return_values == ReturnValues::AllOld;
            let old = if needs_old {
                quorum_read(ctx, &key).await?
            } else {
                None
            };
            if let Some(cond) = &condition {
                if !cond.evaluate(old.as_ref()) {
                    return Err(WireError::conditional_check_failed(
                        "the conditional request failed",
                    ));
                }
            }
            let value = wire::encode_stored_item(&item);
            quorum_write(ctx, &key, &value).await?;
            note_put(ctx, &table, &within, &item);
            Ok(wire::write_response(return_values, old.as_ref()))
        }
        Operation::DeleteItem {
            table,
            key,
            condition,
            return_values,
        } => {
            let (pk, sk) = resolve_key(ctx, &table, &key)?;
            let within = storage_key(&pk, sk.as_ref());
            let data_key = data_key(&table, &within);
            let needs_old = condition.is_some() || return_values == ReturnValues::AllOld;
            let old = if needs_old {
                quorum_read(ctx, &data_key).await?
            } else {
                None
            };
            if let Some(cond) = &condition {
                if !cond.evaluate(old.as_ref()) {
                    return Err(WireError::conditional_check_failed(
                        "the conditional request failed",
                    ));
                }
            }
            let value = wire::encode_tombstone();
            quorum_write(ctx, &data_key, &value).await?;
            note_delete(ctx, &table, &within);
            Ok(wire::write_response(return_values, old.as_ref()))
        }
        // (The `put_item` / `delete_item` helpers above serve the batch/transact
        // paths, which never echo `ReturnValues`, so they avoid the extra read.)
        Operation::GetItem {
            table,
            key,
            projection,
        } => {
            let (pk, sk) = resolve_key(ctx, &table, &key)?;
            let data_key = data_key(&table, &storage_key(&pk, sk.as_ref()));
            let item = quorum_read(ctx, &data_key).await?;
            let item = item.map(|i| wire::project(projection.as_ref(), &i));
            Ok(wire::get_item_response(item.as_ref()))
        }
        Operation::Query {
            table,
            index,
            partition_value,
            sort_condition,
            projection,
        } => {
            run_query(
                ctx,
                &table,
                index.as_deref(),
                &partition_value,
                sort_condition.as_ref(),
                projection.as_ref(),
            )
            .await
        }
        Operation::Scan {
            table,
            limit,
            exclusive_start_key,
            filter,
            projection,
        } => {
            run_scan(
                ctx,
                &table,
                limit,
                exclusive_start_key,
                filter.as_ref(),
                projection.as_ref(),
            )
            .await
        }
        Operation::UpdateItem {
            table,
            key,
            actions,
            condition,
            return_values,
        } => {
            run_update_item(
                ctx,
                &table,
                &key,
                &actions,
                condition.as_ref(),
                return_values,
            )
            .await
        }
        Operation::BatchWriteItem { requests } => {
            for (table, reqs) in &requests {
                for req in reqs {
                    match req {
                        WriteRequest::Put(item) => {
                            put_item(ctx, table, item, None).await?;
                        }
                        WriteRequest::Delete(key) => {
                            delete_item(ctx, table, key, None).await?;
                        }
                    }
                }
            }
            Ok(wire::batch_write_response())
        }
        Operation::TransactWriteItems { actions } => run_transact(ctx, &actions).await,
    }
}

/// Propose `table`'s key schema into the **replicated catalog** (ADR 0013) via the
/// control-plane leader and wait until it commits, then mirror the schema +
/// secondary-index declarations into the local in-memory registry (for the GSI /
/// Query-key bookkeeping that stays edge-local). The committed schema is durable
/// and cluster-agreed, so it survives a restart.
async fn create_table(
    ctx: &ClientCtx,
    table: &str,
    schema: &TableSchema,
    indexes: &[animus_dynamo::SecondaryIndex],
) -> Result<String, WireError> {
    // Reject a duplicate up front, matching DynamoDB's `ResourceInUseException`,
    // before we propose (the state machine also rejects, but this gives the right
    // wire code without waiting on a commit that will be a no-op).
    if metadata(ctx).has_table_schema(table) {
        return Err(registry_error(animus_dynamo::RegistryError::TableExists(
            table.to_owned(),
        )));
    }
    let control_schema = schema_bridge::to_control(schema, &[]);
    let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
    loop {
        // Propose against this cluster's current leader (idempotent: the create is
        // rejected as a no-op if already present, which our success check catches).
        if let Some(leader) = ctx.edge.leader_handle() {
            leader.propose(MetaCommand::CreateTableSchema {
                table: table.to_owned(),
                schema: control_schema.clone(),
            });
        }
        if metadata(ctx).has_table_schema(table) {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(internal(
                "CreateTable did not commit to the control plane in time \
                 (no leader reachable?)",
            ));
        }
        tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
    }
    // Mirror the schema + index declarations into the cluster's registry for the
    // edge-local Query/Scan key index and GSI machinery.
    {
        let mut reg = ctx
            .edge
            .dynamo_registry()
            .lock()
            .expect("registry poisoned");
        if !reg.has_table(table) {
            let _ = reg.create_table_with_indexes(table, schema.clone(), indexes.to_vec());
        }
    }
    Ok(wire::create_table_response(table, schema, indexes))
}

/// `PutItem` core (shared by the wire op and `BatchWriteItem`): resolve the key,
/// optionally gate on `condition`, quorum-write, and update the key index.
/// Returns the prior item (for `ReturnValues`).
async fn put_item(
    ctx: &ClientCtx,
    table: &str,
    item: &Item,
    condition: Option<&ConditionExpression>,
) -> Result<Option<Item>, WireError> {
    let (pk, sk) = resolve_key(ctx, table, item)?;
    let within = storage_key(&pk, sk.as_ref());
    let key = data_key(table, &within);
    let old = if condition.is_some() {
        quorum_read(ctx, &key).await?
    } else {
        None
    };
    if let Some(cond) = condition {
        if !cond.evaluate(old.as_ref()) {
            return Err(WireError::conditional_check_failed(
                "the conditional request failed",
            ));
        }
    }
    let value = wire::encode_stored_item(item);
    quorum_write(ctx, &key, &value).await?;
    note_put(ctx, table, &within, item);
    Ok(old)
}

/// `DeleteItem` core (shared by the wire op and `BatchWriteItem`): resolve the
/// key, optionally gate on `condition`, quorum-write a tombstone, and drop the
/// key from the index. Returns the prior item (for `ReturnValues`).
async fn delete_item(
    ctx: &ClientCtx,
    table: &str,
    key_item: &Item,
    condition: Option<&ConditionExpression>,
) -> Result<Option<Item>, WireError> {
    let (pk, sk) = resolve_key(ctx, table, key_item)?;
    let within = storage_key(&pk, sk.as_ref());
    let key = data_key(table, &within);
    let old = if condition.is_some() {
        quorum_read(ctx, &key).await?
    } else {
        None
    };
    if let Some(cond) = condition {
        if !cond.evaluate(old.as_ref()) {
            return Err(WireError::conditional_check_failed(
                "the conditional request failed",
            ));
        }
    }
    quorum_write(ctx, &key, &wire::encode_tombstone()).await?;
    note_delete(ctx, table, &within);
    Ok(old)
}

/// `UpdateItem`: read-modify-write. Reads the current item, applies the SET/REMOVE
/// actions (starting from the key attributes when the item is absent — an upsert,
/// as in DynamoDB), gating on an optional `condition`, then quorum-writes the new
/// item and echoes `ReturnValues`.
async fn run_update_item(
    ctx: &ClientCtx,
    table: &str,
    key_item: &Item,
    actions: &[UpdateAction],
    condition: Option<&ConditionExpression>,
    return_values: UpdateReturnValues,
) -> Result<String, WireError> {
    let (pk, sk) = resolve_key(ctx, table, key_item)?;
    let within = storage_key(&pk, sk.as_ref());
    let key = data_key(table, &within);
    let old = quorum_read(ctx, &key).await?;
    if let Some(cond) = condition {
        if !cond.evaluate(old.as_ref()) {
            return Err(WireError::conditional_check_failed(
                "the conditional request failed",
            ));
        }
    }
    // Start from the existing item, or (for an upsert) the bare key attributes.
    let base = old.clone().unwrap_or_else(|| key_item.clone());
    let new = wire::apply_update(base, actions);
    let value = wire::encode_stored_item(&new);
    quorum_write(ctx, &key, &value).await?;
    note_put(ctx, table, &within, &new);
    Ok(wire::update_response(
        return_values,
        old.as_ref(),
        Some(&new),
    ))
}

/// `TransactWriteItems`: apply each condition-gated action in order. **Not truly
/// atomic** — there is no cross-action rollback (full Accord-backed transactional
/// writes are deferred; ADR 0011): if action *k* fails its condition, actions
/// before it have already been applied. We *do* honor each action's condition (so
/// a failed `ConditionCheck`/conditional write rejects the request), giving the
/// common "assert-then-write" use the right answer; the documented gap is the
/// all-or-nothing guarantee.
async fn run_transact(ctx: &ClientCtx, actions: &[TransactAction]) -> Result<String, WireError> {
    for action in actions {
        match action {
            TransactAction::Put {
                table,
                item,
                condition,
            } => {
                put_item(ctx, table, item, condition.as_ref()).await?;
            }
            TransactAction::Delete {
                table,
                key,
                condition,
            } => {
                delete_item(ctx, table, key, condition.as_ref()).await?;
            }
            TransactAction::Update {
                table,
                key,
                actions,
                condition,
            } => {
                run_update_item(
                    ctx,
                    table,
                    key,
                    actions,
                    condition.as_ref(),
                    UpdateReturnValues::None,
                )
                .await?;
            }
            TransactAction::ConditionCheck {
                table,
                key,
                condition,
            } => {
                let (pk, sk) = resolve_key(ctx, table, key)?;
                let data_key = data_key(table, &storage_key(&pk, sk.as_ref()));
                let current = quorum_read(ctx, &data_key).await?;
                if !condition.evaluate(current.as_ref()) {
                    return Err(WireError::conditional_check_failed(
                        "a transaction condition check failed",
                    ));
                }
            }
        }
    }
    Ok(wire::empty_response())
}

/// Serve a `Query`. A **base-table** query (`index` is `None`) is a **native
/// quorum range scan** ([`DataClient::scan`]) over the partition's contiguous
/// key sub-range `[escape(table)||escape(pk), …)` — no in-memory key tracking —
/// applying an optional sort-key condition on the recovered sort bytes. An
/// **index** query still resolves the index's base storage keys from the
/// in-memory GSI/LSI index (the native scan covers the base keyspace, not an
/// index's alternate ordering) and quorum-reads each. An optional `projection`
/// keeps only the requested attributes of each returned item.
async fn run_query(
    ctx: &ClientCtx,
    table: &str,
    index: Option<&str>,
    partition_value: &AttributeValue,
    sort_condition: Option<&SortKeyCondition>,
    projection: Option<&Projection>,
) -> Result<String, WireError> {
    // Mirror a catalog table's schema (so its GSI index exists after a restart or
    // on a follower that has not seen a write). A table absent from the catalog is
    // reported unknown below (ResourceNotFoundException) — matching DynamoDB.
    mirror_catalog_schema(ctx, table);
    match index {
        Some(index) => {
            run_index_query(
                ctx,
                table,
                index,
                partition_value,
                sort_condition,
                projection,
            )
            .await
        }
        None => run_base_query(ctx, table, partition_value, sort_condition, projection).await,
    }
}

/// A base-table `Query`: native range scan over the partition's key prefix.
async fn run_base_query(
    ctx: &ClientCtx,
    table: &str,
    partition_value: &AttributeValue,
    sort_condition: Option<&SortKeyCondition>,
    projection: Option<&Projection>,
) -> Result<String, WireError> {
    // A base-table query must reject an unknown table the way the registry path
    // did (ResourceNotFoundException). A table is known iff it is in the
    // replicated catalog or auto-registered locally (legacy clients).
    if !table_known(ctx, table) {
        return Err(registry_error(animus_dynamo::RegistryError::NoSuchTable(
            table.to_owned(),
        )));
    }
    // The partition's data-plane keys are exactly those prefixed by
    // `escape(table) || escape(pk)` (each escape is prefix-free, ending `00 00`),
    // so the contiguous range is `[prefix, prefix-with-last-byte-bumped)`.
    let prefix = partition_prefix(table, partition_value);
    let end = range_end(&prefix);
    let pairs = native_scan(ctx, &prefix, &end, None).await?;
    let mut items = Vec::new();
    for (key, value) in pairs {
        // A DynamoDB delete stores a tombstone *value* (not a data-plane
        // tombstone), so the scan returns it as a live pair; decode drops it.
        let Some(item) = wire::decode_stored_item(&value)? else {
            continue;
        };
        if let Some(cond) = sort_condition {
            // The sort-key bytes are everything after the escaped table+pk
            // prefix; test the condition on those bytes directly (storage order),
            // exactly as the local-engine `query_with` does.
            let sk_bytes = AttributeValue::B(key[prefix.len()..].to_vec());
            if !cond.matches(&sk_bytes) {
                continue;
            }
        }
        items.push(wire::project(projection, &item));
    }
    Ok(wire::query_response(&items))
}

/// A secondary-index `Query`: resolve the index's base storage keys from the
/// in-memory GSI/LSI index and quorum-read each (the native scan covers the base
/// keyspace, not an index's alternate ordering).
async fn run_index_query(
    ctx: &ClientCtx,
    table: &str,
    index: &str,
    partition_value: &AttributeValue,
    sort_condition: Option<&SortKeyCondition>,
    projection: Option<&Projection>,
) -> Result<String, WireError> {
    // An index query with no explicit `ProjectionExpression` falls back to the
    // index's *declared* projection (`ALL` / `KEYS_ONLY` / `INCLUDE`), applied at
    // the edge after the base item is read (the index stores only base keys).
    let index_projection = match projection {
        None => ctx
            .edge
            .dynamo_registry()
            .lock()
            .expect("registry poisoned")
            .index_projected_attributes(table, index)
            .map_err(registry_error)?
            .map(Projection),
        Some(_) => None,
    };
    let effective = projection.or(index_projection.as_ref());
    let within_keys = {
        let reg = ctx
            .edge
            .dynamo_registry()
            .lock()
            .expect("registry poisoned");
        // A hash-only GSI takes no sort condition; a composite GSI / LSI may
        // narrow by one (the registry enforces this).
        reg.index_query_keys(table, index, partition_value, sort_condition)
            .map_err(registry_error)?
    };
    let mut items = Vec::with_capacity(within_keys.len());
    for within in &within_keys {
        let data_key = data_key(table, within);
        if let Some(item) = quorum_read(ctx, &data_key).await? {
            items.push(wire::project(effective, &item));
        }
    }
    Ok(wire::query_response(&items))
}

/// Serve a `Scan` via a **native quorum range scan** ([`DataClient::scan`]) over
/// the whole table's data-plane key range `[escape(table), …)` — no in-memory
/// key tracking. The scan returns live `(key, value)` pairs in key order across a
/// read quorum (tombstones already excluded by the data plane); the edge decodes
/// each, drops a DynamoDB tombstone *value*, applies an optional post-read
/// `filter`, then `projection`.
///
/// DynamoDB pagination is layered on top: `exclusive_start_key` resolves to the
/// storage key to scan strictly *after*; `limit` caps the **examined** (decoded,
/// live) items, applied at the edge so a DynamoDB tombstone value never consumes a
/// slot and the page boundary always lands on a live, decodable item; and when the
/// page is truncated the `LastEvaluatedKey` is that boundary item's key attributes.
/// The cursor thus advances over the **live data-plane keys** the scan returned —
/// not a tracked set — so it is correct after a restart or on a follower that never
/// saw a write.
async fn run_scan(
    ctx: &ClientCtx,
    table: &str,
    limit: Option<usize>,
    exclusive_start_key: Option<Item>,
    filter: Option<&ConditionExpression>,
    projection: Option<&Projection>,
) -> Result<String, WireError> {
    mirror_catalog_schema(ctx, table);
    if !table_known(ctx, table) {
        return Err(registry_error(animus_dynamo::RegistryError::NoSuchTable(
            table.to_owned(),
        )));
    }
    // Scan the whole table's contiguous data-plane range `[escape(table), …)`.
    let start = data_key(table, &[]);
    let end = range_end(&start);
    // `ExclusiveStartKey`: resume strictly after the cursor item's data key.
    let from = match &exclusive_start_key {
        Some(key_item) => {
            let (pk, sk) = resolve_key(ctx, table, key_item)?;
            let mut after = data_key(table, &storage_key(&pk, sk.as_ref()));
            after.push(0x00); // first key strictly past the cursor (keys are unique)
            after
        }
        None => start,
    };
    // The native scan returns live data-plane pairs in key order. A DynamoDB
    // `DeleteItem` stores a *tombstone value* (a live pair to the data plane), so
    // decode each and drop the ones that decode to a tombstone — those items are
    // logically absent and are neither examined nor counted.
    let pairs = native_scan(ctx, &from, &end, None).await?;
    let mut examined: Vec<(Vec<u8>, Item)> = Vec::new();
    for (key, value) in pairs {
        if let Some(item) = wire::decode_stored_item(&value)? {
            examined.push((key, item));
        }
    }
    // `Limit` caps the **examined** items; we apply it at the edge (over decoded
    // live items) rather than passing it to the native scan, so a tombstone value
    // never consumes a slot and the page boundary always falls on a live, decodable
    // item — its key attributes are recoverable for `LastEvaluatedKey`.
    let truncated = limit.is_some_and(|n| examined.len() > n);
    if let Some(n) = limit {
        examined.truncate(n);
    }
    let scanned = examined.len();
    // The pagination cursor is the last examined item; recover its key attributes.
    let last_evaluated_key = if truncated {
        examined
            .last()
            .and_then(|(_, item)| key_item_of(ctx, table, item))
    } else {
        None
    };
    let mut items = Vec::new();
    for (_key, item) in &examined {
        // The filter sees the whole item; projection then trims the result.
        if filter.is_none_or(|f| f.evaluate(Some(item))) {
            items.push(wire::project(projection, item));
        }
    }
    Ok(wire::scan_response(
        &items,
        scanned,
        last_evaluated_key.as_ref(),
    ))
}

/// Build the key-attribute-only [`Item`] (the `LastEvaluatedKey` shape) for a
/// full item, per `table`'s schema. `None` if the table is unknown or the item
/// lacks a key attribute (shouldn't happen for a stored item).
fn key_item_of(ctx: &ClientCtx, table: &str, item: &Item) -> Option<Item> {
    let reg = ctx
        .edge
        .dynamo_registry()
        .lock()
        .expect("registry poisoned");
    let schema = reg.schema(table)?;
    let mut key = Item::new();
    key.insert(
        schema.partition_key.clone(),
        item.get(&schema.partition_key)?.clone(),
    );
    if let Some(sk) = &schema.sort_key {
        if let Some(v) = item.get(sk) {
            key.insert(sk.clone(), v.clone());
        }
    }
    Some(key)
}

fn note_put(ctx: &ClientCtx, table: &str, within_key: &[u8], item: &Item) {
    let mut reg = ctx
        .edge
        .dynamo_registry()
        .lock()
        .expect("registry poisoned");
    if !reg.has_table(table) {
        reg.create_table_legacy(table);
    }
    let _ = reg.note_put(table, within_key, item);
}

fn note_delete(ctx: &ClientCtx, table: &str, within_key: &[u8]) {
    let mut reg = ctx
        .edge
        .dynamo_registry()
        .lock()
        .expect("registry poisoned");
    let _ = reg.note_delete(table, within_key);
}

/// Map a registry error to a DynamoDB wire error code.
fn registry_error(err: animus_dynamo::RegistryError) -> WireError {
    use animus_dynamo::RegistryError as R;
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
        R::NoSuchIndex(i) => WireError {
            code: "ValidationException",
            message: format!("index `{i}` does not exist on this table"),
        },
        R::IndexSortMismatch(i) => WireError {
            code: "ValidationException",
            message: format!("index `{i}` is hash-only and takes no sort-key condition"),
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

/// The contiguous data-plane key prefix of a `Query` partition:
/// `escape(table) || escape(partition_value)`. Every item in that partition has a
/// data key starting with this prefix, and (the escape being prefix-free, ending
/// `0x00 0x00`) no other partition's key does — so the partition is one
/// half-open range `[prefix, range_end(prefix))`.
fn partition_prefix(table: &str, partition_value: &AttributeValue) -> Vec<u8> {
    // `storage_key(pk, None) == escape(pk)`, so `data_key(table, escape(pk))`
    // is `escape(table) || escape(pk)`.
    data_key(table, &storage_key(partition_value, None))
}

/// The exclusive upper bound of the half-open range that covers exactly the keys
/// starting with `prefix`: the prefix with its final byte bumped by one. The
/// escape used to build a prefix always ends `0x00 0x00`, so the last byte is
/// `0x00` and the bumped bound is `… 0x00 0x01` — the first key past the
/// partition/table. (Mirrors `SchemaRegistry::query_keys`' former range math.)
fn range_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    *end.last_mut().expect("a data-plane prefix is non-empty") = 0x01;
    end
}

/// Native quorum range scan over the half-open data-plane range `[start, end)`,
/// returning the live `(key, value)` pairs in key order (tombstones already
/// excluded by the data plane), optionally capped at `limit` keys. Routes through
/// the shared coordinator under the coord lock, exactly like a point read/write.
/// A scan that cannot reach a read quorum is an internal error (the scan analog of
/// a failed read).
async fn native_scan(
    ctx: &ClientCtx,
    start: &[u8],
    end: &[u8],
    limit: Option<usize>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WireError> {
    let view = ctx.view_for(start).ok_or_else(internal_no_tablet)?;
    let _guard = ctx.coord_lock.lock().await;
    ctx.coordinator
        .scan(&view, start, end, limit, DYNAMO_TIMEOUT)
        .await
        .ok_or_else(|| internal("scan did not reach a read quorum"))
}

/// Whether `table` is known: present in the replicated catalog (ADR 0013) or
/// already auto-registered locally (a legacy `pk`/`sk` client). A base-table
/// `Query`/`Scan` rejects an unknown table (`ResourceNotFoundException`), matching
/// what the former written-key path did via the registry.
fn table_known(ctx: &ClientCtx, table: &str) -> bool {
    if metadata(ctx).has_table_schema(table) {
        return true;
    }
    ctx.edge
        .dynamo_registry()
        .lock()
        .expect("registry poisoned")
        .has_table(table)
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
        animus_data::ReadResult::Value(Some(bytes)) => wire::decode_stored_item(&bytes),
        animus_data::ReadResult::Value(None) => Ok(None),
        animus_data::ReadResult::Failed => Err(internal("read did not reach a quorum")),
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

/// Write a `text/plain` response — used by the admin `/metrics` route (ADR 0015),
/// whose body is the line-oriented metrics export, not DynamoDB JSON.
async fn write_text_response(
    stream: &mut TcpStream,
    status: u16,
    body: &str,
    keep_alive: bool,
) -> std::io::Result<()> {
    let connection = if keep_alive { "keep-alive" } else { "close" };
    let response = format!(
        "HTTP/1.1 {status} OK\r\n\
         Content-Type: text/plain; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Connection: {connection}\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}
