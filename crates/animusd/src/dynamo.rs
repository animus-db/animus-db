//! The DynamoDB JSON wire endpoint (ADR 0006).
//!
//! A minimal, hand-rolled HTTP/1.1 server over a real tokio [`TcpListener`] that
//! speaks the DynamoDB JSON protocol: clients `POST /` with an
//! `X-Amz-Target: DynamoDB_20120810.<Op>` header and an AttributeValue-JSON
//! body. We decode the request with [`animus_dynamo::wire`] (pure, deterministic
//! translation) and route the resulting key/value bytes (v1, ADR 0019) through the
//! **leaderful CP data plane** — `ClientCtx::cp_read`/`cp_write`/`cp_scan` to the
//! per-tablet Raft group leader (linearizable, forwarded cross-process), the same
//! CP primitives the plain-TCP client API and the CQL endpoint use. The HTTP edge
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
//! `UpdateItem` is a read-modify-write (`SET`/`REMOVE`); `BatchWriteItem` commits
//! each table's put/delete requests as **one Raft entry per tablet** (the CP
//! batch-put primitive, ADR 0017 — one consensus round for the batch instead of
//! one per key), atomic within a tablet and non-atomic across tablets (DynamoDB
//! semantics); `TransactWriteItems` applies condition-gated actions in order but
//! **without cross-action atomicity** (true ACID via Accord, ADR 0011, is deferred
//! — see [`run_transact`]).
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
//! The secondary-index **definitions** (GSI/LSI name, kind, hash/sort attributes,
//! projection) now also live in the **replicated catalog** (ADR 0013):
//! `CreateTable` proposes a `MetaCommand::CreateTableIndex` per declared index
//! (after the table schema commits) and waits for it to replicate, so the index
//! definitions are durable + cluster-agreed. The in-memory `SchemaRegistry` is
//! reconciled to that replicated set via `SchemaRegistry::sync_indexes`
//! ([`mirror_catalog_schema`]) — on `CreateTable`, and lazily on a read/write path
//! — so a freshly restarted node (or a follower that never saw a write) rebuilds
//! its index machinery from the catalog, not from process-local memory. Only the
//! index **entry data** (the `escape(hash) [|| escape(sort)] || base_key` index)
//! stays in-memory and not durable, rebuilt from observed `note_put`/`note_delete`
//! writes. The registry is **per-node** (ADR 0031 PR2 — `ClusterEdgeState` is
//! always per-node, in `--cluster N` exactly as in one-process-per-node): held in
//! the node's own `ClusterEdgeState` (threaded through `ClientCtx`), not a
//! process `OnceLock`, so two in-process clusters — or two nodes of the same
//! `--cluster N` cluster — never share a registry. The index *definitions*
//! (replicated, above) reach every node the same way regardless; a node whose
//! registry doesn't yet have an index's entry data lazily backfills it on the
//! first query against that index (`backfill_index_if_needed`), so a
//! cross-node index query is correct without a shared in-memory registry.
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

use animus_control::{MetaCommand, Metadata, ReplicationMode};
use animus_dynamo::wire::{
    self, Operation, Projection, ReturnValues, TransactAction, UpdateAction, UpdateReturnValues,
    WireError, WriteRequest,
};
use animus_dynamo::{
    AttributeValue, ConditionExpression, Item, SortKeyCondition, TableSchema,
    schema as schema_bridge, storage_key,
};
use animus_tablet::partition_token;
use tokio::net::{TcpListener, TcpStream};

use crate::ClientCtx;
use crate::http;

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
///
/// **Snapshot once per request**: this is a full deep clone of the metadata under
/// the Raft handle's lock, so [`run_operation`] takes it once at request entry
/// and threads `&Metadata` through the helpers — the schema lookups, table/tablet
/// existence checks, and index mirroring of one request all read the same
/// consistent snapshot instead of re-cloning 2+ times per request. Paths that
/// must observe *fresh* state (the `CreateTable` commit-wait polls) still call
/// this directly.
fn metadata(ctx: &ClientCtx) -> Metadata {
    ctx.raft.metadata()
}

/// The DynamoDB key schema for `table`, resolved from the **replicated catalog**
/// (ADR 0013) when present, else the legacy `pk`/`sk` convention so a
/// pre-`CreateTable` client keeps working.
fn schema_for(meta: &Metadata, table: &str) -> TableSchema {
    match meta.table_schema(table) {
        Some(control) => schema_bridge::to_dynamo(control),
        None => TableSchema::composite("pk", "sk"),
    }
}

/// Mirror a **catalog** table's key schema **and its replicated secondary-index
/// definitions** (ADR 0013) into the cluster's registry — so the registry's
/// key-index / GSI machinery is rebuilt from the cluster-agreed catalog after a
/// restart or on a follower that has not seen a write. We always reconcile the
/// index *definitions* via [`SchemaRegistry::sync_indexes`] (which registers the
/// table if absent, preserves entry data for an unchanged-shape index, clears it
/// for a changed-shape one, and drops a removed one), so a freshly restarted node
/// rebuilds its index machinery from the **replicated** definitions rather than
/// process-local memory. A table absent from the catalog is left untouched here
/// (the read path then reports it unknown; the write path legacy-registers it via
/// [`legacy_register`]).
fn mirror_catalog_schema(ctx: &ClientCtx, meta: &Metadata, table: &str) {
    if meta.has_table_schema(table) {
        let schema = schema_for(meta, table);
        let indexes = schema_bridge::indexes_to_dynamo(meta.table_indexes(table));
        let mut reg = ctx
            .edge
            .dynamo_registry()
            .lock()
            .expect("registry poisoned");
        // Reconcile the index definitions to the replicated set (registering the
        // table on first sight). `sync_indexes` preserves the in-memory entry data
        // of an unchanged-shape index, so repeated mirroring is cheap and does not
        // discard observed-write index state.
        let _ = reg.sync_indexes(table, schema, &indexes);
    }
}

/// Auto-register `table` under the legacy `pk`/`sk` convention (sort key
/// optional) if it is in neither the catalog nor the registry — so a
/// pre-`CreateTable` client's writes keep working unchanged and their keys get
/// tracked for `Query`/`Scan`.
fn legacy_register(ctx: &ClientCtx, meta: &Metadata, table: &str) {
    if meta.has_table_schema(table) {
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
    meta: &Metadata,
    table: &str,
    item: &Item,
) -> Result<(AttributeValue, Option<AttributeValue>), WireError> {
    mirror_catalog_schema(ctx, meta, table);
    legacy_register(ctx, meta, table);
    let reg = ctx
        .edge
        .dynamo_registry()
        .lock()
        .expect("registry poisoned");
    reg.extract_key(table, item).map_err(registry_error)
}

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
        let Some(request) = http::read_http_request(&mut stream, &mut buf).await? else {
            return Ok(()); // clean EOF
        };
        let keep_alive = request.keep_alive;
        // The `/metrics` route (ADR 0015) shares this listener — a plain `GET`
        // returning the text-format snapshot, distinct from the DynamoDB
        // `POST /` + `X-Amz-Target` protocol. (The richer admin interface lives
        // on its own dedicated port, ADR 0020.)
        if request.method.eq_ignore_ascii_case("GET") && request.path == "/metrics" {
            let body = ctx.metrics_text();
            http::write_text_response(&mut stream, 200, &body, keep_alive).await?;
            if !keep_alive {
                return Ok(());
            }
            continue;
        }
        let (status, body) = dispatch(&ctx, &request).await;
        http::write_amz_json_response(&mut stream, status, &body, keep_alive).await?;
        if !keep_alive {
            // The client asked us to close (HTTP/1.0 default, or an explicit
            // `Connection: close`). Returning drops the stream, closing the
            // socket so a client doing a single request/`read_to_end` unblocks.
            return Ok(());
        }
    }
}

/// Dispatch a decoded request, returning the HTTP status code and JSON body.
async fn dispatch(ctx: &ClientCtx, request: &http::HttpRequest) -> (u16, String) {
    execute(ctx, &request.target, &request.body).await
}

/// Decode + run a DynamoDB operation from its `X-Amz-Target` value and JSON body,
/// returning `(http status, json body)`. Shared by the DynamoDB HTTP edge (above)
/// and the admin dashboard's write proxy (`POST /admin/data/dynamo`, ADR 0021), so
/// both go through the identical decode + `run_operation` path.
pub(crate) async fn execute(ctx: &ClientCtx, target: &str, body: &[u8]) -> (u16, String) {
    match wire::decode_request(target, body) {
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
    // One metadata snapshot per request (see [`metadata`]): every schema lookup /
    // table-existence check below reads this consistent view instead of deep-
    // cloning the replicated metadata again. `CreateTable` is the exception — its
    // commit-wait must poll *fresh* views, so it reads live inside `create_table`.
    let meta = &metadata(ctx);
    match op {
        Operation::CreateTable {
            table,
            schema,
            key_types,
            indexes,
        } => create_table(ctx, &table, &schema, &key_types, &indexes).await,
        Operation::PutItem {
            table,
            item,
            condition,
            return_values,
        } => {
            let (pk, sk) = resolve_key(ctx, meta, &table, &item)?;
            let key = item_key(&pk, sk.as_ref());
            // For ALL_OLD (or a condition) we need the prior item; read it once.
            let needs_old = condition.is_some() || return_values == ReturnValues::AllOld;
            // A conditional (or old-echoing) put is a read-modify-write: hold the
            // per-node RMW lock across the read → evaluate → write span, as the CQL
            // edge does, so two concurrent conditional puts on one node can't both
            // read the same "old" and both pass (a lost update / double create). An
            // unconditional put does no read and takes no lock. The guard drops at
            // the end of this arm — never held across the response write.
            let _rmw = if needs_old {
                Some(ctx.rmw_lock.lock().await)
            } else {
                None
            };
            let old = if needs_old {
                quorum_read(ctx, meta, &table, &key).await?
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
            quorum_write(ctx, meta, &table, &key, &value).await?;
            note_put(ctx, &table, &key, &item);
            Ok(wire::write_response(return_values, old.as_ref()))
        }
        Operation::DeleteItem {
            table,
            key,
            condition,
            return_values,
        } => {
            let (pk, sk) = resolve_key(ctx, meta, &table, &key)?;
            let data_key = item_key(&pk, sk.as_ref());
            let needs_old = condition.is_some() || return_values == ReturnValues::AllOld;
            // Same RMW serialization as the conditional `PutItem` above: a
            // conditional delete must not interleave with another RMW between its
            // read and its write.
            let _rmw = if needs_old {
                Some(ctx.rmw_lock.lock().await)
            } else {
                None
            };
            let old = if needs_old {
                quorum_read(ctx, meta, &table, &data_key).await?
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
            quorum_write(ctx, meta, &table, &data_key, &value).await?;
            note_delete(ctx, &table, &data_key);
            Ok(wire::write_response(return_values, old.as_ref()))
        }
        // (The `put_item` / `delete_item` helpers above serve the batch/transact
        // paths, which never echo `ReturnValues`, so they avoid the extra read.)
        Operation::GetItem {
            table,
            key,
            projection,
        } => {
            let (pk, sk) = resolve_key(ctx, meta, &table, &key)?;
            let data_key = item_key(&pk, sk.as_ref());
            let item = quorum_read(ctx, meta, &table, &data_key).await?;
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
                meta,
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
                meta,
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
            // `UpdateItem` is always a read-modify-write: hold the per-node RMW
            // lock across it (taken here, not inside `run_update_item`, which is
            // also called from `run_transact` under the same lock — a tokio Mutex
            // is not reentrant).
            let _rmw = ctx.rmw_lock.lock().await;
            run_update_item(
                ctx,
                meta,
                &table,
                &key,
                &actions,
                condition.as_ref(),
                return_values,
            )
            .await
        }
        Operation::BatchWriteItem { requests } => {
            // Commit each table's items as **one Raft entry per tablet** (ADR 0017 —
            // batch put) instead of a serial loop of individual `cp_write`s. A
            // `BatchWriteItem` has no per-item condition and echoes no `ReturnValues`,
            // so neither a `Put` nor a `Delete` needs a pre-read: a `Delete` is a
            // write of the tombstone *sentinel value* (as in `delete_item`), so both
            // ride the same batch. Within a table `cp_batch_write` groups by tablet
            // (atomic per tablet, non-atomic across tablets — DynamoDB semantics).
            for (table, reqs) in &requests {
                let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(reqs.len());
                for req in reqs {
                    match req {
                        WriteRequest::Put(item) => {
                            let (pk, sk) = resolve_key(ctx, meta, table, item)?;
                            batch
                                .push((item_key(&pk, sk.as_ref()), wire::encode_stored_item(item)));
                        }
                        WriteRequest::Delete(key_item) => {
                            let (pk, sk) = resolve_key(ctx, meta, table, key_item)?;
                            batch.push((item_key(&pk, sk.as_ref()), wire::encode_tombstone()));
                        }
                    }
                }
                ctx.cp_batch_write(table, batch)
                    .await
                    .map_err(|e| internal(&e))?;
                // Update the edge-local GSI/LSI index after the durable commit (as
                // the single-item helpers do), re-resolving each item's key.
                for req in reqs {
                    match req {
                        WriteRequest::Put(item) => {
                            let (pk, sk) = resolve_key(ctx, meta, table, item)?;
                            note_put(ctx, table, &item_key(&pk, sk.as_ref()), item);
                        }
                        WriteRequest::Delete(key_item) => {
                            let (pk, sk) = resolve_key(ctx, meta, table, key_item)?;
                            note_delete(ctx, table, &item_key(&pk, sk.as_ref()));
                        }
                    }
                }
            }
            Ok(wire::batch_write_response())
        }
        Operation::TransactWriteItems { actions } => {
            // Hold the per-node RMW lock across the whole transaction so its
            // condition checks + writes can't interleave with another RMW on this
            // node. The per-action helpers (`put_item`/`delete_item`/
            // `run_update_item`) deliberately take no lock — they run under this
            // guard, and a tokio Mutex is not reentrant.
            let _rmw = ctx.rmw_lock.lock().await;
            run_transact(ctx, meta, &actions).await
        }
    }
}

/// Propose `table`'s key schema **and each declared secondary-index definition**
/// into the **replicated catalog** (ADR 0013) via the control-plane leader and
/// wait until they commit, then reconcile the cluster's in-memory registry to the
/// replicated set (for the GSI / Query-key bookkeeping that stays edge-local). The
/// committed schema + index definitions are durable and cluster-agreed, so they
/// survive a restart and are visible on every node — the edge no longer holds the
/// index *definitions* in process-local memory.
async fn create_table(
    ctx: &ClientCtx,
    table: &str,
    schema: &TableSchema,
    key_types: &[(String, String)],
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
    // v1: every wire-created table is served by the leaderful CP plane (ADR 0019),
    // so it is created in `ReplicationMode::Cp`. The edge routes its reads/writes
    // through the CP primitives regardless, but recording the mode keeps the
    // replicated catalog truthful (and the plain-client `is_cp` gate consistent).
    let control_schema =
        schema_bridge::to_control(schema, key_types).with_mode(ReplicationMode::Cp);
    let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
    loop {
        // Propose against this cluster's current leader (idempotent: the create is
        // rejected as a no-op if already present, which our success check catches).
        ctx.propose_schema(&MetaCommand::CreateTableSchema {
            table: table.to_owned(),
            schema: control_schema.clone(),
        })
        .await;
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
    // Now that the table schema exists in the catalog, propose each declared
    // secondary-index *definition* (`CreateTableIndex` is rejected unless the table
    // schema is present, hence the ordering) and wait for it to replicate, so the
    // index definitions are durable + cluster-agreed alongside the key schema.
    //
    // Each index is bridged to the control-plane `IndexDef` via
    // `schema::index_to_control`, supplying the base partition key (an LSI hashes by
    // it). We compare against the replicated `table_indexes` set by name to know it
    // committed.
    for index in indexes {
        let def = schema_bridge::index_to_control(index, &schema.partition_key);
        let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
        loop {
            ctx.propose_schema(&MetaCommand::CreateTableIndex {
                table: table.to_owned(),
                index: def.clone(),
            })
            .await;
            if metadata(ctx)
                .table_indexes(table)
                .iter()
                .any(|d| d.name == def.name)
            {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(internal(
                    "CreateTable index definition did not commit to the control \
                     plane in time (no leader reachable?)",
                ));
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }
    // Provision the table's CP tablet (ADR 0023): one tablet over the whole token
    // ring, scoped to this table, which the per-node join-host loop stands up. Until
    // this commits, the table has no tablet and its data ops would wait.
    ctx.provision_tablet(table)
        .await
        .map_err(|e| internal(&e))?;
    // Reconcile the cluster's registry to the **replicated** index set (rebuilding
    // the edge-local Query/Scan key index + GSI machinery from the catalog, not from
    // the request's declarations — so the source of truth is the committed catalog).
    // A *fresh* snapshot on purpose: the request-entry snapshot predates the schema
    // this very request just committed.
    mirror_catalog_schema(ctx, &metadata(ctx), table);
    Ok(wire::create_table_response(table, schema, indexes))
}

/// `PutItem` core (the `TransactWriteItems` per-action path): resolve the key,
/// optionally gate on `condition`, quorum-write, and update the key index.
/// Returns the prior item (for `ReturnValues`). Takes no RMW lock itself — the
/// transact caller already holds `ctx.rmw_lock` across the whole transaction.
async fn put_item(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    item: &Item,
    condition: Option<&ConditionExpression>,
) -> Result<Option<Item>, WireError> {
    let (pk, sk) = resolve_key(ctx, meta, table, item)?;
    let key = item_key(&pk, sk.as_ref());
    let old = if condition.is_some() {
        quorum_read(ctx, meta, table, &key).await?
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
    quorum_write(ctx, meta, table, &key, &value).await?;
    note_put(ctx, table, &key, item);
    Ok(old)
}

/// `DeleteItem` core (the `TransactWriteItems` per-action path): resolve the
/// key, optionally gate on `condition`, quorum-write a tombstone, and drop the
/// key from the index. Returns the prior item (for `ReturnValues`). Takes no RMW
/// lock itself — the transact caller already holds `ctx.rmw_lock`.
async fn delete_item(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    key_item: &Item,
    condition: Option<&ConditionExpression>,
) -> Result<Option<Item>, WireError> {
    let (pk, sk) = resolve_key(ctx, meta, table, key_item)?;
    let key = item_key(&pk, sk.as_ref());
    let old = if condition.is_some() {
        quorum_read(ctx, meta, table, &key).await?
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
    quorum_write(ctx, meta, table, &key, &wire::encode_tombstone()).await?;
    note_delete(ctx, table, &key);
    Ok(old)
}

/// `UpdateItem`: read-modify-write. Reads the current item, applies the SET/REMOVE
/// actions (starting from the key attributes when the item is absent — an upsert,
/// as in DynamoDB), gating on an optional `condition`, then quorum-writes the new
/// item and echoes `ReturnValues`. Takes no RMW lock itself — both callers (the
/// `UpdateItem` arm and `run_transact`) hold `ctx.rmw_lock` around the call.
async fn run_update_item(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    key_item: &Item,
    actions: &[UpdateAction],
    condition: Option<&ConditionExpression>,
    return_values: UpdateReturnValues,
) -> Result<String, WireError> {
    let (pk, sk) = resolve_key(ctx, meta, table, key_item)?;
    let key = item_key(&pk, sk.as_ref());
    let old = quorum_read(ctx, meta, table, &key).await?;
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
    quorum_write(ctx, meta, table, &key, &value).await?;
    note_put(ctx, table, &key, &new);
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
/// all-or-nothing guarantee. The caller holds `ctx.rmw_lock` across the call, so
/// the whole transaction is serialized against this node's other RMWs.
async fn run_transact(
    ctx: &ClientCtx,
    meta: &Metadata,
    actions: &[TransactAction],
) -> Result<String, WireError> {
    for action in actions {
        match action {
            TransactAction::Put {
                table,
                item,
                condition,
            } => {
                put_item(ctx, meta, table, item, condition.as_ref()).await?;
            }
            TransactAction::Delete {
                table,
                key,
                condition,
            } => {
                delete_item(ctx, meta, table, key, condition.as_ref()).await?;
            }
            TransactAction::Update {
                table,
                key,
                actions,
                condition,
            } => {
                run_update_item(
                    ctx,
                    meta,
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
                let (pk, sk) = resolve_key(ctx, meta, table, key)?;
                let data_key = item_key(&pk, sk.as_ref());
                let current = quorum_read(ctx, meta, table, &data_key).await?;
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
    meta: &Metadata,
    table: &str,
    index: Option<&str>,
    partition_value: &AttributeValue,
    sort_condition: Option<&SortKeyCondition>,
    projection: Option<&Projection>,
) -> Result<String, WireError> {
    // Mirror a catalog table's schema (so its GSI index exists after a restart or
    // on a follower that has not seen a write). A table absent from the catalog is
    // reported unknown below (ResourceNotFoundException) — matching DynamoDB.
    mirror_catalog_schema(ctx, meta, table);
    match index {
        Some(index) => {
            run_index_query(
                ctx,
                meta,
                table,
                index,
                partition_value,
                sort_condition,
                projection,
            )
            .await
        }
        None => {
            run_base_query(
                ctx,
                meta,
                table,
                partition_value,
                sort_condition,
                projection,
            )
            .await
        }
    }
}

/// A base-table `Query`: native range scan over the partition's key prefix.
async fn run_base_query(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    partition_value: &AttributeValue,
    sort_condition: Option<&SortKeyCondition>,
    projection: Option<&Projection>,
) -> Result<String, WireError> {
    // A base-table query must reject an unknown table the way the registry path
    // did (ResourceNotFoundException). A table is known iff it is in the
    // replicated catalog or auto-registered locally (legacy clients).
    if !table_known(ctx, meta, table) {
        return Err(registry_error(animus_dynamo::RegistryError::NoSuchTable(
            table.to_owned(),
        )));
    }
    // The partition's data-plane keys are exactly those prefixed by
    // `escape(table) || escape(pk)` (each escape is prefix-free, ending `00 00`),
    // so the contiguous range is `[prefix, prefix-with-last-byte-bumped)`.
    let prefix = partition_prefix(partition_value);
    let end = range_end(&prefix);
    let pairs = native_scan(ctx, table, &prefix, Some(&end), None).await?;
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
/// keyspace, not an index's alternate ordering). Backfills the index's entry data
/// lazily first (see [`backfill_index_if_needed`]).
async fn run_index_query(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    index: &str,
    partition_value: &AttributeValue,
    sort_condition: Option<&SortKeyCondition>,
    projection: Option<&Projection>,
) -> Result<String, WireError> {
    backfill_index_if_needed(ctx, table, index).await?;
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
    for base_key in &within_keys {
        // The index stores the full engine key (`item_key`) as its base key, so it
        // reads back directly — no table prefix to reattach (ADR 0023).
        if let Some(item) = quorum_read(ctx, meta, table, base_key).await? {
            items.push(wire::project(effective, &item));
        }
    }
    Ok(wire::query_response(&items))
}

/// **Lazy restart backfill** for a GSI/LSI's entry data (ADR 0013): the index
/// *definitions* are replicated (and rebuilt via `sync_indexes`), but the entry
/// data is edge-local, populated only from writes *this process* observed — so
/// after a restart (or on a node that never saw the writes) an index query would
/// silently return nothing. Rather than scanning the base table inline on every
/// `sync_indexes` (which runs on read/write paths), the rebuild happens **here,
/// on the first index query** against a freshly-created index: one linearizable
/// base-table scan, replayed through `note_put` (which populates *every* index of
/// the table, so the whole table is then marked backfilled).
///
/// The scan runs without the registry lock (it is a network read); a write that
/// lands between the scan and the replay may be replayed with its pre-scan
/// attributes — the same last-writer-wins imprecision any observed-write index
/// has, acceptable for this best-effort edge structure (the base item, quorum-read
/// afterwards, is always the source of truth for the returned data).
async fn backfill_index_if_needed(
    ctx: &ClientCtx,
    table: &str,
    index: &str,
) -> Result<(), WireError> {
    let needs = {
        let reg = ctx
            .edge
            .dynamo_registry()
            .lock()
            .expect("registry poisoned");
        reg.index_needs_backfill(table, index)
    };
    if !needs {
        return Ok(());
    }
    // Full base-table scan — the same live source a base `Scan` reads.
    let pairs = native_scan(ctx, table, &[], None, None).await?;
    let mut reg = ctx
        .edge
        .dynamo_registry()
        .lock()
        .expect("registry poisoned");
    // Re-check under the lock: a concurrent index query may have backfilled while
    // we scanned (the replay is idempotent, but skipping repeats the work less).
    if !reg.index_needs_backfill(table, index) {
        return Ok(());
    }
    for (key, value) in &pairs {
        // DynamoDB tombstone values decode to `None` — logically absent, skipped.
        if let Some(item) = wire::decode_stored_item(value)? {
            let _ = reg.note_put(table, key, &item);
        }
    }
    reg.mark_table_backfilled(table);
    Ok(())
}

/// Serve a `Scan` via a **native quorum range scan** ([`DataClient::scan`]) over
/// the whole table's data-plane key range `[escape(table), …)` — no in-memory
/// key tracking. The scan returns live `(key, value)` pairs in key order across a
/// read quorum (tombstones already excluded by the data plane); the edge decodes
/// each, drops a DynamoDB tombstone *value*, applies an optional post-read
/// `filter`, then `projection`.
///
/// DynamoDB pagination is layered on top: `exclusive_start_key` resolves to the
/// storage key to scan strictly *after* (so each page's range starts at the
/// cursor); `limit` caps the **examined** (decoded, live) items and is **pushed
/// down** to the native scan (fetching windows of the remaining count, continuing
/// past DynamoDB tombstone values so they never consume a slot) — a small page on
/// a large table reads ~limit rows, not the whole table. The page boundary always
/// lands on a live, decodable item; when the page is truncated the
/// `LastEvaluatedKey` is that boundary item's key attributes. The cursor thus
/// advances over the **live data-plane keys** the scan returned — not a tracked
/// set — so it is correct after a restart or on a follower that never saw a write.
async fn run_scan(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    limit: Option<usize>,
    exclusive_start_key: Option<Item>,
    filter: Option<&ConditionExpression>,
    projection: Option<&Projection>,
) -> Result<String, WireError> {
    mirror_catalog_schema(ctx, meta, table);
    if !table_known(ctx, meta, table) {
        return Err(registry_error(animus_dynamo::RegistryError::NoSuchTable(
            table.to_owned(),
        )));
    }
    // Scan the table's whole ring (ADR 0023): the tablet engines hold only this
    // table's rows, so the range is `[from, ∞)` — unbounded above (`end = None`),
    // fanned out across the table's tablets in token order by `cp_scan`.
    let from = match &exclusive_start_key {
        Some(key_item) => {
            let (pk, sk) = resolve_key(ctx, meta, table, key_item)?;
            let mut after = item_key(&pk, sk.as_ref());
            after.push(0x00); // first key strictly past the cursor (keys are unique)
            after
        }
        None => Vec::new(),
    };
    // The native scan returns live data-plane pairs in key order. A DynamoDB
    // `DeleteItem` stores a *tombstone value* (a live pair to the data plane), so
    // decode each and drop the ones that decode to a tombstone — those items are
    // logically absent and are neither examined nor counted.
    //
    // `Limit` is **pushed down** to the native scan (which pushes it per tablet —
    // `cp_scan` passes each tablet's leader only the remaining count and stops
    // fanning out once filled), so a `Limit=10` page on a large table ships ~10
    // rows instead of the whole table, and pagination stays O(page) per page (the
    // cursor becomes the next page's range start). We fetch `limit + 1` live items
    // to know whether the page is truncated. A DynamoDB tombstone *value* is live
    // to the data plane but must not consume a `Limit` slot, so when a fetched
    // window decodes short (tombstones in range), continue the scan from just past
    // the last raw key until the window is filled or the range is exhausted —
    // the page boundary then always lands on a live, decodable item, so its key
    // attributes are recoverable for `LastEvaluatedKey`.
    let want = limit.map(|n| n.saturating_add(1));
    let mut examined: Vec<(Vec<u8>, Item)> = Vec::new();
    let mut cursor = from;
    loop {
        let fetch = want.map(|w| w - examined.len());
        let pairs = native_scan(ctx, table, &cursor, None, fetch).await?;
        // Fewer raw pairs than asked (or an unbounded fetch) ⇒ the range is done.
        let exhausted = fetch.is_none_or(|f| pairs.len() < f);
        let last_raw_key = pairs.last().map(|(k, _)| k.clone());
        for (key, value) in pairs {
            if let Some(item) = wire::decode_stored_item(&value)? {
                examined.push((key, item));
            }
        }
        if exhausted || want.is_some_and(|w| examined.len() >= w) {
            break;
        }
        // Tombstone values consumed part of the window: resume strictly past the
        // last raw key scanned (keys are unique, so append a 0x00).
        let mut next = last_raw_key.expect("non-exhausted fetch returned pairs");
        next.push(0x00);
        cursor = next;
    }
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

/// The data-plane (engine) key for an item (ADR 0023): `partition_token(pk) ||
/// escape(pk) || sk`. **No table prefix** — the item's tablet is its own engine
/// holding only this table's rows, so the table is the routing argument, not key
/// bytes. The token (Murmur3, fixed 8 bytes) spreads partitions across the table's
/// ring; it is over the partition key only, so a partition's rows share the
/// `token || escape(pk)` prefix and stay contiguous + sort-ordered.
fn item_key(pk: &AttributeValue, sk: Option<&AttributeValue>) -> Vec<u8> {
    let pk_escaped = storage_key(pk, None); // == escape(pk)
    let mut key = partition_token(&pk_escaped).to_vec();
    key.extend_from_slice(&storage_key(pk, sk)); // escape(pk) || sk
    key
}

/// The contiguous key prefix of a `Query` partition (ADR 0023): `token(pk) ||
/// escape(pk)`. Every item in that partition starts with it, and (the escape being
/// prefix-free, ending `0x00 0x00`) no other partition's key does — so the
/// partition is the one half-open range `[prefix, range_end(prefix))`.
fn partition_prefix(partition_value: &AttributeValue) -> Vec<u8> {
    item_key(partition_value, None)
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

/// Linearizable CP range scan over the half-open range `[start, end)`, returning
/// the live `(key, value)` pairs in key order (tombstones already excluded by the
/// engine), optionally capped at `limit` keys. Routes to the CP group leader via
/// [`ClientCtx::cp_scan`] (ReadIndex; forwarded cross-process). A scan the leader
/// cannot serve is an internal error (the scan analog of a failed read).
async fn native_scan(
    ctx: &ClientCtx,
    table: &str,
    start: &[u8],
    end: Option<&[u8]>,
    limit: Option<usize>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WireError> {
    ctx.cp_scan(table, start.to_vec(), end.map(<[u8]>::to_vec), limit)
        .await
        .map_err(|e| internal(&e))
}

/// Whether `table` is known: present in the replicated catalog (ADR 0013) or
/// already auto-registered locally (a legacy `pk`/`sk` client). A base-table
/// `Query`/`Scan` rejects an unknown table (`ResourceNotFoundException`), matching
/// what the former written-key path did via the registry.
fn table_known(ctx: &ClientCtx, meta: &Metadata, table: &str) -> bool {
    if meta.has_table_schema(table) {
        return true;
    }
    ctx.edge
        .dynamo_registry()
        .lock()
        .expect("registry poisoned")
        .has_table(table)
}

/// CP write of `value` at `key` (ADR 0017): proposed on the per-tablet Raft group
/// leader and waited to durable+applied before returning (durable-before-ack). The
/// Raft index is the MVCC version, so no client-assigned version is needed (the
/// AP path's `read_version`+1 dance is gone).
async fn quorum_write(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    key: &[u8],
    value: &[u8],
) -> Result<(), WireError> {
    // Auto-provision the table's tablet on first write (ADR 0023). A `CreateTable`
    // provisions up front, but a legacy `pk`/`sk` client that never `CreateTable`d
    // still needs a tablet to route to — stand one up on demand here. Idempotent
    // (a request-start snapshot is sound: a stale "absent" just re-proposes the
    // idempotent provisioning) and fast once the tablet exists.
    if !meta.has_table_tablet(table) {
        ctx.provision_tablet(table)
            .await
            .map_err(|e| internal(&e))?;
    }
    ctx.cp_write(table, key.to_vec(), value.to_vec())
        .await
        .map_err(|e| internal(&e))
}

/// Linearizable CP read of `key`, decoding the stored DynamoDB item (an absent
/// key — including one tombstoned by a `DeleteItem` sentinel — reads as `None`).
async fn quorum_read(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    key: &[u8],
) -> Result<Option<Item>, WireError> {
    // A table with no tablet has no data (ADR 0023) — read as absent without waiting
    // on routing for a tablet that does not exist. The gate short-circuits a
    // **linearizable** read, so it must not conclude "absent" from the (possibly
    // stale) request-entry snapshot: a concurrent first write may have provisioned
    // the tablet after this request began — under the RMW lock a conditional
    // writer's read *must* see it (two racing `attribute_not_exists` puts both
    // "succeeding" was the failure mode). Trust the snapshot on the hit path
    // (tablets are only removed by drop-table); re-check **live** on the miss.
    if !meta.has_table_tablet(table) && !metadata(ctx).has_table_tablet(table) {
        return Ok(None);
    }
    match ctx
        .cp_read(table, key.to_vec())
        .await
        .map_err(|e| internal(&e))?
    {
        Some(bytes) => wire::decode_stored_item(&bytes),
        None => Ok(None),
    }
}

fn internal(message: &str) -> WireError {
    WireError {
        code: "InternalServerError",
        message: message.to_owned(),
    }
}
