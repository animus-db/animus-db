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
//! `Scan`, `UpdateItem`, `BatchWriteItem`, `TransactWriteItems`,
//! `TransactGetItems`. The data-plane key for an item is `escape(table) ||
//! escape(pk) || sk` (so tables share one keyspace without colliding). The data
//! plane has no native delete, so `DeleteItem` writes a tombstone value that
//! `GetItem` reads back as absent. `UpdateItem` is a read-modify-write
//! (`SET`/`REMOVE`); `BatchWriteItem` commits each table's put/delete requests
//! as **one Raft entry per tablet** (the CP batch-put primitive, ADR 0017 — one
//! consensus round for the batch instead of one per key), atomic within a
//! tablet and non-atomic across tablets (DynamoDB semantics).
//!
//! `TransactWriteItems` **is atomic** (ADR 0018 §2/PR7): every condition-gated
//! `Put`/`Delete`/`Update`/`ConditionCheck` action commits whole-or-nothing
//! across however many tablets/tables it spans, via
//! [`ClientCtx::cp_txn`](crate::ClientCtx::cp_txn) — see [`run_transact`]'s doc
//! for the exact condition-evaluation/precondition layering and the deferred
//! per-action `CancellationReasons` fidelity. `TransactGetItems` is a
//! **consistent multi-key read** (new, ADR 0018 §2/PR7) — see
//! [`run_transact_get`]'s doc for its quiescence-confirmation semantics (a
//! serializable snapshot via retry-on-contention, not a wait-free one).
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
//! projection) also live in the **replicated catalog** (ADR 0013):
//! `CreateTable` proposes a `MetaCommand::CreateTableIndex` per declared index
//! (after the table schema commits) and waits for it to replicate, so the index
//! definitions are durable + cluster-agreed. The in-memory `SchemaRegistry` is
//! reconciled to that replicated set via `SchemaRegistry::sync_indexes`
//! ([`mirror_catalog_schema`]) — on `CreateTable`, and lazily on a read/write path
//! — so a freshly restarted node (or a follower that never saw a write) rebuilds
//! its key/index *definition* bookkeeping from the catalog, not from
//! process-local memory. The registry is **per-node** (ADR 0031 PR2 —
//! `ClusterEdgeState` is always per-node, in `--cluster N` exactly as in
//! one-process-per-node): held in the node's own `ClusterEdgeState` (threaded
//! through `ClientCtx`), not a process `OnceLock`, so two in-process clusters —
//! or two nodes of the same `--cluster N` cluster — never share a registry.
//!
//! **The index *entries* themselves are no longer edge-local (ADR 0041).** Where
//! this crate used to maintain a per-index in-memory `escape(hash) [||
//! escape(sort)] || base_key` map from observed writes (with a lazy
//! restart/cross-node backfill to paper over what a given process never
//! observed), index rows are now ordinary **replicated data-plane rows** — see
//! below.
//!
//! ## Query, Scan, and secondary indexes (ADR 0041 §5)
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
//! **An index `Query` is now a second native range scan, not an in-memory
//! lookup.** `CreateTable` may declare any number of **global / local secondary
//! indexes**, each with a `Projection` (`ALL`/`KEYS_ONLY`/`INCLUDE`); an indexed
//! (or streamed) write is now **evaluated at the item's own tablet leader**
//! (`ClientCtx::cp_kind_write_item` / [`kind_write_item_at_leader`], ADR 0046
//! U3), which maintains this item's LSI rows and a change-log record
//! atomically with the base row (ADR 0041 §2/§4) via [`kind_writes_for_item`]
//! — unchanged diff logic, just moved off the edge that received the request
//! and onto the tablet's own leader, closing a cross-node race the prior
//! edge-evaluated `index_aware_write` design had (see
//! [`kind_write_item_at_leader`]'s own doc for the incident). The GSI drain
//! (`index_drain.rs`) asynchronously materializes GSI rows
//! into the index's own hidden table (`index_table_name`). Every index row's
//! *stored value* is already the declared projection (`projected_item`, applied
//! by the writer/drain) — an index `Query` therefore decodes it directly, with
//! **no per-key base-table read-back**:
//!
//! - A **GSI** `Query` ([`run_gsi_query`]) scans the hidden table
//!   `<base>$<index>` over `[token(ihash) || escape(ihash), …)` (narrowed to
//!   `escape(ihash) || escape(isort)` for an `Equals` sort condition), the same
//!   primitive a base `Query` uses. A hidden table with no tablet yet (the
//!   index has never drained anything) reads as **empty** rather than waiting
//!   on routing — this is DynamoDB's own eventually-consistent GSI contract,
//!   not a bug: the drain provisions the hidden table lazily, on its first tick
//!   with records to apply.
//! - An **LSI** `Query` ([`run_lsi_query`]) scans the *base table's own tablet*
//!   over its `KIND_LSI` scope (`ClientCtx::cp_scan_kind`, a linearizable
//!   ReadIndex scan, ADR 0041 §3/§5) — strongly consistent, since LSI rows
//!   commit in the same Raft entry as the base row they derive from.
//!
//! A sort condition narrows the scan (an `Equals` GSI condition) or filters the
//! decoded rows by recovering the sort segment from the row's own key
//! (`animus_dynamo::index::parse_gsi_row_key`/`parse_lsi_row_key`) — a sort
//! condition against a hash-only index is rejected (`IndexSortMismatch`) before
//! either path runs. An explicit `ProjectionExpression` still applies on top of
//! the stored (already-projected) item; without one, the stored item *is* the
//! index's declared projection, returned as-is.
//!
//! **There is no backfill.** Indexes are only declarable at `CreateTable` time
//! today, so a pre-existing item that predates an index can never exist —
//! nothing to backfill. `UpdateTable` (adding an index to a populated table)
//! will need a real backfill when it lands (ADR 0041 §5).

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::schema::{IndexDef, IndexKind, IndexProjection as CtlProjection, IndexStatus};
use animus_control::{MetaCommand, Metadata, ReplicationMode};
use animus_cp_data::{KIND_BASE, KIND_LSI};
use animus_dynamo::wire::{
    self, Operation, Projection, ReturnValues, TransactAction, TransactGet, WireError, WriteRequest,
};
use animus_dynamo::{
    AttributeValue, ChangeRecord, ConditionExpression, Item, SortKeyCondition, TableSchema,
    index as dynamo_index, schema as schema_bridge, storage_key,
};
use animus_env::{Clock, Env, Metric};
use animus_tablet::{TOKEN_BYTES, TabletId, partition_token};
use tokio::net::{TcpListener, TcpStream};

use crate::http;
use crate::{ClientCtx, CpGroup, KindWriteOp, SnapshotRead};

/// How long `CreateTable` waits for its `CreateTableSchema` proposal to commit in
/// the replicated catalog before giving up.
const SCHEMA_COMMIT_TIMEOUT: Duration = Duration::from_secs(5);
/// How often `CreateTable` re-checks (and re-proposes against the current leader)
/// while waiting for the schema to commit.
const SCHEMA_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Max actions per `TransactWriteItems` request / keys per `TransactGetItems`
/// request (ADR 0018 §2/PR7) — DynamoDB's own limit (1-100 items); we don't
/// replicate AWS's fuller request-size validation, just this simple cap.
const MAX_TRANSACT_ITEMS: usize = 100;

/// Bounded rounds `run_transact_get`'s quiescent read gives a multi-key
/// snapshot to stabilize: the first round, a confirming round, then up to
/// [`TRANSACT_GET_MAX_ROUNDS`] `- 2` further retries (ADR 0018 §2/PR6's
/// corpus finding — see `run_transact_get`'s doc for why two-round agreement,
/// not a single coordinator-minted timestamp, is what actually closes this).
const TRANSACT_GET_MAX_ROUNDS: usize = 4;
/// Delay between `run_transact_get` rounds once two consecutive rounds have
/// disagreed — gives an in-flight transaction touching the read keys a
/// moment to finish landing before the next round samples again.
const TRANSACT_GET_POLL: Duration = Duration::from_millis(20);

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
/// must observe *fresh* state (the `CreateTable` commit-wait polls, and the
/// conditional-write existence gate's live re-check) use
/// [`metadata_fresh`] instead — see that function's doc.
///
/// Cache-tolerant (ADR 0035 PR1: `ctx.effective_metadata()`, not
/// `ctx.control.metadata_cached()` directly) — on a control-plane-follower-less
/// growth node (ADR 0030) this is the difference between resolving a table's
/// schema at all and a permanently-empty local view, since that node's own
/// control raft never replicates.
fn metadata(ctx: &ClientCtx) -> Metadata {
    ctx.effective_metadata()
}

/// Read-your-writes view of the replicated `Metadata` (ADR 0035 PR1) — never
/// the growth-node mirror [`metadata`] can substitute. Used only where a poll
/// must observe its own just-proposed command (or a concurrent writer's)
/// landing in the authoritative state: the `CreateTable` commit-wait loops
/// below, and [`quorum_read`]'s live re-check on a snapshot miss.
async fn metadata_fresh(ctx: &ClientCtx) -> Metadata {
    ctx.metadata_fresh().await
}

/// The DynamoDB key schema for `table`, resolved from the **replicated catalog**
/// (ADR 0013) when present, else the legacy `pk`/`sk` convention so a
/// pre-`CreateTable` client keeps working.
pub(crate) fn schema_for(meta: &Metadata, table: &str) -> TableSchema {
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
///
/// **Same listener, two services (ADR 0042 §3's decided same-listener
/// F-fork)**: a `DynamoDBStreams_20120810.*` target routes to
/// `dynamo_streams::execute`; everything else (the `DynamoDB_20120810.*`
/// item API this module owns) goes through [`execute`] unchanged. Delegates
/// to [`execute_routed`], the fork's single implementation.
async fn dispatch(ctx: &ClientCtx, request: &http::HttpRequest) -> (u16, String) {
    execute_routed(ctx, &request.target, &request.body).await
}

/// Route a fully-qualified `X-Amz-Target` to whichever of the two services on
/// this listener owns it, and run it: a `DynamoDBStreams_20120810.*` target
/// goes to [`crate::dynamo_streams::execute`] (the Streams read API, ADR 0042
/// §3), everything else to [`execute`] (the `DynamoDB_20120810.*` item API).
/// The **single** place this fork is expressed — shared by the real edge's
/// [`dispatch`] and the admin dashboard's write/read proxy (`POST
/// /admin/data/dynamo`, ADR 0021), so both resolve a target identically.
pub(crate) async fn execute_routed(ctx: &ClientCtx, target: &str, body: &[u8]) -> (u16, String) {
    if target.starts_with(animus_dynamo::streams_wire::TARGET_PREFIX) {
        crate::dynamo_streams::execute(ctx, target, body).await
    } else {
        execute(ctx, target, body).await
    }
}

/// Decode + run a DynamoDB **item-API** operation from its `X-Amz-Target`
/// value and JSON body, returning `(http status, json body)`. Called directly
/// only where the target is already known to be item-API (this module's
/// `dispatch`, via [`execute_routed`]); everything else should go through
/// [`execute_routed`] so a `DynamoDBStreams_20120810.*` target isn't handed to
/// the wrong decoder.
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
            stream_view_type,
        } => create_table(ctx, &table, &schema, &key_types, &indexes, stream_view_type).await,
        Operation::UpdateTable {
            table,
            stream,
            index_update,
        } => update_table(ctx, &table, stream, index_update).await,
        Operation::DescribeTable { table } => describe_table(ctx, meta, &table),
        Operation::PutItem {
            table,
            item,
            condition,
            return_values,
        } => {
            let (pk, sk) = resolve_key(ctx, meta, &table, &item)?;
            // ADR 0049 fast arm: nothing to read, nothing to evaluate —
            // the edge commits base row + marker record directly (see
            // `fast_marker_write`'s doc for why this must NOT go through
            // the leader funnel).
            if condition.is_none()
                && return_values == ReturnValues::None
                && !table_change_records_carry_images(meta, &table)
            {
                let value = wire::encode_stored_item(&item);
                fast_marker_write(ctx, &table, &pk, sk.as_ref(), value).await?;
                return Ok(wire::write_response(return_values, None));
            }
            // ADR 0046 U3: an evaluated write (a condition, an old-image
            // echo, or an images-carrying table) is evaluated **at the
            // tablet leader**, not here — see
            // `dynamo::kind_write_item_at_leader`'s doc for why (the
            // cross-node LSI/change-record orphan race a node-local
            // `rmw_lock` here could never close). No local read, no local
            // lock: the leader does both, and every write of this item from
            // any edge node reaches the same leader.
            match ctx
                .cp_kind_write_item(
                    meta,
                    &table,
                    &pk,
                    sk.as_ref(),
                    KindWriteOp::Put(item),
                    condition.as_ref(),
                )
                .await?
            {
                KindWriteOutcome::ConditionFailed => Err(WireError::conditional_check_failed(
                    "the conditional request failed",
                )),
                KindWriteOutcome::Ok { old, .. } => {
                    Ok(wire::write_response(return_values, old.as_ref()))
                }
            }
        }
        Operation::DeleteItem {
            table,
            key,
            condition,
            return_values,
        } => {
            let (pk, sk) = resolve_key(ctx, meta, &table, &key)?;
            // ADR 0049 fast arm — see `PutItem`'s identical fork above. A
            // delete's base write is the tombstone *sentinel value*, so the
            // routed probe still confirms on a `Some`.
            if condition.is_none()
                && return_values == ReturnValues::None
                && !table_change_records_carry_images(meta, &table)
            {
                let value = wire::encode_tombstone();
                fast_marker_write(ctx, &table, &pk, sk.as_ref(), value).await?;
                return Ok(wire::write_response(return_values, None));
            }
            // See `PutItem`'s identical fork above for why an evaluated
            // write goes to the leader instead.
            match ctx
                .cp_kind_write_item(
                    meta,
                    &table,
                    &pk,
                    sk.as_ref(),
                    KindWriteOp::Delete,
                    condition.as_ref(),
                )
                .await?
            {
                KindWriteOutcome::ConditionFailed => Err(WireError::conditional_check_failed(
                    "the conditional request failed",
                )),
                KindWriteOutcome::Ok { old, .. } => {
                    Ok(wire::write_response(return_values, old.as_ref()))
                }
            }
        }
        // (The `put_item` / `delete_item` helpers above serve the batch/transact
        // paths, which never echo `ReturnValues`, so they avoid the extra read.)
        Operation::GetItem {
            table,
            key,
            projection,
            // Accept-and-ignore (ADR 0041 §5): a base-table `GetItem` is
            // already linearizable here, so `ConsistentRead: true` needs no
            // enforcement — only a GSI `Query` ever rejects it.
            consistent_read: _,
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
            consistent_read,
        } => {
            run_query(
                ctx,
                meta,
                &table,
                index.as_deref(),
                &partition_value,
                sort_condition.as_ref(),
                projection.as_ref(),
                consistent_read,
            )
            .await
        }
        Operation::Scan {
            table,
            index,
            limit,
            exclusive_start_key,
            filter,
            projection,
            consistent_read,
        } => {
            run_scan(
                ctx,
                meta,
                &table,
                index.as_deref(),
                limit,
                exclusive_start_key,
                filter.as_ref(),
                projection.as_ref(),
                consistent_read,
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
            // ADR 0046 U3: an indexed/streamed table's `UpdateItem` also
            // evaluates at the leader now — it has the identical cross-node
            // base-value read-modify-write hazard `PutItem`/`DeleteItem` did,
            // closed by the same mechanism at no extra cost (`KindWriteOp::
            // Update` folds the update expression itself into the leader's
            // own evaluation). See `dynamo::kind_write_item_at_leader`'s doc.
            let (pk, sk) = resolve_key(ctx, meta, &table, &key)?;
            match ctx
                .cp_kind_write_item(
                    meta,
                    &table,
                    &pk,
                    sk.as_ref(),
                    KindWriteOp::Update {
                        key_item: key,
                        actions,
                    },
                    condition.as_ref(),
                )
                .await?
            {
                KindWriteOutcome::ConditionFailed => Err(WireError::conditional_check_failed(
                    "the conditional request failed",
                )),
                KindWriteOutcome::Ok { old, new } => Ok(wire::update_response(
                    return_values,
                    old.as_ref(),
                    new.as_ref(),
                )),
            }
        }
        Operation::BatchWriteItem { requests } => {
            // Commit each table's items as **one Raft entry per tablet** (ADR 0017 —
            // batch put) instead of a serial loop of individual `cp_write`s. A
            // `BatchWriteItem` has no per-item condition and echoes no `ReturnValues`,
            // so neither a `Put` nor a `Delete` needs a pre-read: a `Delete` is a
            // write of the tombstone *sentinel value* (as in `delete_item`), so both
            // ride the same batch. Within a table `cp_batch_write` groups by tablet
            // (atomic per tablet, non-atomic across tablets — DynamoDB semantics).
            //
            // **Every table now routes each request through
            // `ClientCtx::cp_kind_write_item` individually** (ADR 0049) — ADR
            // 0046 U3's evaluate-at-leader write path, the identical primitive
            // `PutItem`/`DeleteItem` use, which reads the old item and
            // evaluates on the tablet leader rather than here. `BatchWriteItem`
            // has no per-item condition, so `KindWriteOutcome::ConditionFailed`
            // can never come back here (`cp_kind_write_item`'s own `condition:
            // None`). **Per-item atomicity only**, matching DynamoDB's own
            // non-atomic `BatchWriteItem` contract (one request's outcome never
            // affects another's).
            //
            // (History: this arm once had a plain `cp_batch_write` fast path
            // whose gate had silently drifted to `table_indexes(..).is_empty()`
            // — losing a streamed-but-unindexed table's records entirely;
            // deleted with the rest of the plain branches in Train A's rung 5.
            // Regression that pins the fixed behavior: `stream_write_path_
            // tests::batch_write_on_a_streamed_table_emits_change_records`;
            // the general same-predicate lesson is in
            // `docs/engineering-lessons.md`.)
            for (table, reqs) in &requests {
                // ADR 0049 fast arm: a marker table's batch needs no
                // evaluation — commit **one `KindBatch` Raft entry per
                // tablet**, carrying every one of that tablet's base rows
                // AND every one of their marker records, exactly the
                // entry-granularity the old `cp_batch_write` fast path had
                // (one entry, one WAL record, one apply per tablet). The
                // first cut of this arm proposed one entry per ITEM
                // (concurrently), which looked amortized but is 100x the
                // entries/WAL bytes/apply work for a 100-item batch — a
                // measured throughput regression (`tests/backfill_seeder.rs`'s
                // populate-then-backfill blew its convergence budget under
                // load), and a break of ADR 0049 §1's own "the marker rides
                // the same entry — no extra fsync" contract. Per-item
                // atomicity still holds trivially: one tablet's entry is
                // whole-or-nothing per tablet, non-atomic across tablets —
                // DynamoDB's own `BatchWriteItem` contract either way.
                if !table_change_records_carry_images(meta, table) {
                    let mut rows = Vec::with_capacity(reqs.len());
                    for req in reqs {
                        rows.push(match req {
                            WriteRequest::Put(item) => {
                                let (pk, sk) = resolve_key(ctx, meta, table, item)?;
                                (pk, sk, wire::encode_stored_item(item))
                            }
                            WriteRequest::Delete(key_item) => {
                                let (pk, sk) = resolve_key(ctx, meta, table, key_item)?;
                                (pk, sk, wire::encode_tombstone())
                            }
                        });
                    }
                    marker_batch_write(ctx, table, rows)
                        .await
                        .map_err(|e| internal(&e))?;
                    continue;
                }
                for req in reqs {
                    match req {
                        WriteRequest::Put(item) => {
                            let (pk, sk) = resolve_key(ctx, meta, table, item)?;
                            ctx.cp_kind_write_item(
                                meta,
                                table,
                                &pk,
                                sk.as_ref(),
                                KindWriteOp::Put(item.clone()),
                                None,
                            )
                            .await?;
                        }
                        WriteRequest::Delete(key_item) => {
                            let (pk, sk) = resolve_key(ctx, meta, table, key_item)?;
                            ctx.cp_kind_write_item(
                                meta,
                                table,
                                &pk,
                                sk.as_ref(),
                                KindWriteOp::Delete,
                                None,
                            )
                            .await?;
                        }
                    }
                }
            }
            Ok(wire::batch_write_response())
        }
        Operation::TransactWriteItems { actions } => {
            // Unlike the old serial-loop implementation, atomicity now comes
            // from `ClientCtx::cp_txn` (a real cross-tablet 2PC), not this
            // node's `rmw_lock` — `run_transact` still takes it across its own
            // pre-read/evaluate pass (mirroring every other conditional write
            // here) so two transactions on this node can't interleave their
            // condition checks, but the lock is not what makes the *commit*
            // atomic.
            run_transact(ctx, meta, &actions).await
        }
        Operation::TransactGetItems { gets } => run_transact_get(ctx, meta, &gets).await,
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
    stream_view_type: Option<animus_control::StreamViewType>,
) -> Result<String, WireError> {
    // Reject a name that collides with the control plane's reserved system
    // keyspace (ADR 0038) up front, client-side, with a clear message — the
    // state machine also rejects this (`Metadata::apply`'s `CreateTableSchema`
    // arm), but that would otherwise surface as an opaque commit-wait timeout.
    if animus_control::syskv::is_reserved_name(table) {
        return Err(WireError {
            code: "ValidationException",
            message: format!("table name `{table}` collides with the reserved system namespace"),
        });
    }
    // Reject a duplicate up front, matching DynamoDB's `ResourceInUseException`,
    // before we propose (the state machine also rejects, but this gives the right
    // wire code without waiting on a commit that will be a no-op). Fresh, not
    // `metadata(ctx)`: this whole function is a commit-wait poll (ADR 0035 PR1).
    if metadata_fresh(ctx).await.has_table_schema(table) {
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
        if metadata_fresh(ctx).await.has_table_schema(table) {
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
            if metadata_fresh(ctx)
                .await
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
    // Enable the stream, if requested, the same commit-wait shape as the
    // index definitions above (ADR 0042 §2/§9): mint a fresh label (this is
    // the table's first-ever enable, so `SetTableStream`'s apply-time
    // already-enabled guard can never fire here) and propose/wait.
    let stream_spec = match stream_view_type {
        Some(view_type) => Some(enable_stream(ctx, table, view_type).await?),
        None => None,
    };
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
    mirror_catalog_schema(ctx, &metadata_fresh(ctx).await, table);
    let stream_desc = stream_spec.as_ref().map(stream_description);
    Ok(wire::create_table_response(
        table,
        schema,
        indexes,
        stream_desc.as_ref(),
    ))
}

/// Mint a fresh DynamoDB Streams label (ADR 0042 §4) and propose/wait for
/// `MetaCommand::SetTableStream{Some(spec)}` to commit, exactly the same
/// commit-wait shape `create_table`'s index-definition loop already uses.
/// Callers must have already established that no stream is currently
/// enabled for `table` (the apply-time guard rejects otherwise) — used by
/// both `create_table`'s first-enable and `update_table`'s enable path.
async fn enable_stream(
    ctx: &ClientCtx,
    table: &str,
    view_type: animus_control::StreamViewType,
) -> Result<animus_control::StreamSpec, WireError> {
    let spec = animus_control::StreamSpec {
        view_type,
        label: mint_stream_label(ctx),
    };
    let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
    loop {
        ctx.propose_schema(&MetaCommand::SetTableStream {
            table: table.to_owned(),
            spec: Some(spec.clone()),
        })
        .await;
        if metadata_fresh(ctx)
            .await
            .table_stream(table)
            .is_some_and(|s| s.label == spec.label)
        {
            return Ok(spec);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(internal(
                "stream enable did not commit to the control plane in time \
                 (no leader reachable?)",
            ));
        }
        tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
    }
}

/// Disable `table`'s stream (`SetTableStream{None}`) and wait for it to
/// commit. A no-op wait (still proposed, harmlessly idempotent) if no
/// stream is currently enabled.
///
/// **F12-b's disable-triggered final seal (ADR 0042 §11, ADR 0043 §A3)
/// happens first**, before the write gate ever closes: every one of
/// `table`'s current tablets is force-sealed (`ClientCtx::force_seal_tablet`
/// — one hop to wherever that tablet's own leader actually runs, calling the
/// *identical* `index_drain::seal_now` the periodic seal arm calls, just
/// unconditionally rather than trigger-gated — the "one seal code path"
/// this PR's design keeps to) so every record written before disable,
/// delivered to a consumer or not, reaches the readable (segment) tier
/// before `SetTableStream{None}` ever proposes. If any tablet's final seal
/// fails to confirm, this returns an error and the caller (the DynamoDB
/// edge) never proposes the disable at all — a retried `UpdateTable` simply
/// re-seals (idempotent: a repeat seal of an already-fully-sealed hot tail
/// finds nothing pending and is a no-op) rather than risk disabling with an
/// un-sealed tail.
async fn disable_stream(ctx: &ClientCtx, table: &str) -> Result<(), WireError> {
    let tablets: Vec<TabletId> = metadata_fresh(ctx)
        .await
        .tablets_for_table(table)
        .map(|(&t, _)| t)
        .collect();
    for tablet in tablets {
        ctx.force_seal_tablet(tablet).await.map_err(|e| {
            internal(&format!(
                "final seal of tablet {} before disabling table `{table}`'s stream: {e}",
                tablet.0
            ))
        })?;
    }
    let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
    loop {
        ctx.propose_schema(&MetaCommand::SetTableStream {
            table: table.to_owned(),
            spec: None,
        })
        .await;
        if metadata_fresh(ctx).await.table_stream(table).is_none() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(internal(
                "stream disable did not commit to the control plane in time \
                 (no leader reachable?)",
            ));
        }
        tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
    }
}

/// `wire::StreamDescription` for a replicated `StreamSpec` — the tiny bridge
/// between the control-plane type and the wire response type.
fn stream_description(spec: &animus_control::StreamSpec) -> wire::StreamDescription {
    wire::StreamDescription {
        view_type: spec.view_type,
        label: spec.label.clone(),
    }
}

/// `UpdateTable`: dispatches whichever of the wire decoder's two mutually
/// exclusive change shapes this call carries (ADR 0045 §6 Fork C — never
/// both; the decoder already rejected that combination, and any other
/// index/key/throughput change). A stream change goes through
/// [`enable_stream`]/[`disable_stream`], unchanged from ADR 0042 §2/§9. An
/// index change dispatches to [`create_index`] (`Create` — adds a live-
/// backfilling GSI to a possibly-populated table, ADR 0045 §2/§6) or
/// [`drop_index`] (`Delete` — the four-step convergent drop cascade, ADR
/// 0045 §5).
async fn update_table(
    ctx: &ClientCtx,
    table: &str,
    stream: Option<wire::StreamUpdate>,
    index_update: Option<wire::IndexUpdate>,
) -> Result<String, WireError> {
    if !metadata_fresh(ctx).await.has_table_schema(table) {
        return Err(registry_error(animus_dynamo::RegistryError::NoSuchTable(
            table.to_owned(),
        )));
    }
    match (stream, index_update) {
        (Some(stream), None) => match stream {
            wire::StreamUpdate::Enable(view_type) => {
                if metadata_fresh(ctx).await.table_stream(table).is_some() {
                    return Err(WireError::validation(format!(
                        "table `{table}` already has a stream enabled — disable it before \
                         re-enabling (ADR 0042 §9: re-enable always mints a fresh, empty stream)"
                    )));
                }
                enable_stream(ctx, table, view_type).await?;
            }
            wire::StreamUpdate::Disable => disable_stream(ctx, table).await?,
        },
        (None, Some(update)) => match update {
            wire::IndexUpdate::Create(index) => create_index(ctx, table, &index).await?,
            wire::IndexUpdate::Delete(index) => drop_index(ctx, table, &index).await?,
        },
        // Unreachable via the wire decoder (it always sets exactly one), but
        // handled explicitly rather than assumed — a future direct
        // `Operation` construction (e.g. a test) gets a clear error instead
        // of silently no-op-ing through to `describe_table`.
        (None, None) | (Some(_), Some(_)) => {
            return Err(WireError::validation(
                "UpdateTable requires exactly one of a StreamSpecification or a \
                 GlobalSecondaryIndexUpdates change",
            ));
        }
    }
    let meta = metadata_fresh(ctx).await;
    describe_table(ctx, &meta, table)
}

/// Add a new global secondary index to a (possibly populated) table (ADR
/// 0045 §2/§6). Validates the request client-side, exactly as `create_table`
/// validates a table name up front rather than waiting on a commit that will
/// only be rejected later:
///
/// - **`Local` kind rejected** — real DynamoDB has no `LocalSecondaryIndexUpdates`
///   at all (LSIs are create-time-only); the wire decoder never actually
///   produces this variant from `GlobalSecondaryIndexUpdates` today, but a
///   directly-constructed `Operation` (or a future decoder change) still
///   can't slip past this defensively.
/// - **Reserved/`$` name rejected** — an index name becomes half of its
///   hidden table's own name (`index_table_name`, `<base>$<index>`), so it is
///   checked against the same two gates `CreateTableSchema`'s apply-time
///   guard already enforces for a *table* name: the reserved system
///   namespace (`syskv::is_reserved_name`) and the `$` separator itself.
/// - **Duplicate name rejected** — `CreateTableIndex`'s apply arm calls
///   `upsert_index`, an **add-or-replace-by-name** primitive (`schema.rs`'s
///   own doc); proposing a same-named index unchecked would silently
///   *replace* an existing definition (a different kind/attrs) rather than
///   erroring, so the duplicate check must happen here, client-side, before
///   ever proposing.
///
/// Bridges the validated declaration to the control-plane `IndexDef` via
/// [`schema_bridge::index_to_control`], **overriding its status to
/// `Creating`** — that function's own doc: its default `Active` is correct
/// only for `create_table`'s always-empty-by-construction caller, and this is
/// the caller its doc names as needing the override. Proposes
/// `CreateTableIndex` with the same commit-wait shape [`enable_stream`] uses
/// (presence-by-name, not a status match — the completion aggregator can
/// flip `Creating` to `Active` before this function's own next poll on a
/// small/fast-converging table, so waiting for `status == Creating`
/// specifically would spuriously time out).
///
/// **No `provision_tablet` call**: the GSI drain lazily provisions the
/// hidden table's first tablet (`index_drain.rs`) on its own next tick. From
/// the moment `CreateTableIndex` commits, `table_change_records_carry_images`
/// already gates on index *presence*, not status, so every write from this
/// instant forward is already covered by a full-image change-log record; the backfill
/// seeder + completion aggregator (PR1-4 of this stack) cover every
/// pre-existing row and flip the index to `Active` with no further action
/// here.
async fn create_index(
    ctx: &ClientCtx,
    table: &str,
    index: &animus_dynamo::SecondaryIndex,
) -> Result<(), WireError> {
    let animus_dynamo::SecondaryIndex::Global(gsi) = index else {
        return Err(WireError::validation(
            "UpdateTable cannot add a LocalSecondaryIndex to an existing table — LSIs are \
             create-time-only in DynamoDB (declare it in CreateTable instead)",
        ));
    };
    let name = gsi.name.as_str();
    if name.contains(dynamo_index::INDEX_TABLE_SEPARATOR)
        || animus_control::syskv::is_reserved_name(name)
    {
        return Err(WireError::validation(format!(
            "index name `{name}` is reserved or contains `{}` (reserved as the hidden index \
             table's own name separator)",
            dynamo_index::INDEX_TABLE_SEPARATOR
        )));
    }
    let meta = metadata_fresh(ctx).await;
    let Some(control_schema) = meta.table_schema(table) else {
        return Err(registry_error(animus_dynamo::RegistryError::NoSuchTable(
            table.to_owned(),
        )));
    };
    if meta.table_indexes(table).iter().any(|d| d.name == name) {
        return Err(WireError::validation(format!(
            "index `{name}` already exists on table `{table}`"
        )));
    }
    let mut def = schema_bridge::index_to_control(index, &control_schema.partition_key);
    def.status = IndexStatus::Creating;
    let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
    loop {
        ctx.propose_schema(&MetaCommand::CreateTableIndex {
            table: table.to_owned(),
            index: def.clone(),
        })
        .await;
        if metadata_fresh(ctx)
            .await
            .table_indexes(table)
            .iter()
            .any(|d| d.name == def.name)
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(internal(&format!(
                "index `{name}` creation did not commit to the control plane in time \
                 (no leader reachable?)"
            )));
        }
        tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
    }
}

/// The four-step convergent drop-index cascade (ADR 0045 §5): tearing down a
/// GSI that may still be `Creating`/backfilling needs more than
/// `DropTableIndex`'s single atomic catalog removal — the index's hidden
/// table's own tablets need reclaiming, and the drain/seeder must stop
/// touching the index before that reclaim runs, or a live drain tick can
/// re-provision the very tablet being dropped. Each step is independently
/// idempotent (a no-op if already applied on this or a prior attempt), so a
/// crash between any two steps converges correctly on retry:
///
/// 1. [`set_index_status`] `Deleting` — `change_consumer_loop`'s `gsis`
///    filter excludes `Deleting`, so the drain/seeder stop touching this
///    index from their very next tick, before anything is torn down.
/// 2. `MetaCommand::DropTableTablets` scoped to the index's own hidden table
///    (`index_table_name`) — the exact primitive `ClientCtx::drop_table`'s
///    own GSI cascade uses, just for one index instead of every one.
/// 3. [`drop_table_index`] — the catalog definition's removal, which also
///    prunes every `index_backfill` row for this index (`meta.rs`'s own
///    apply arm, ADR 0045 §4).
/// 4. A belt-and-suspenders re-scan mirroring `ClientCtx::drop_table`'s own
///    defense: the drain provisions a hidden table's first tablet lazily
///    (ADR 0023) and can race step 2's drop, re-creating it after the
///    drop's own commit-wait already observed zero tablets. A final sweep
///    for a tablet still named exactly the hidden table's name catches that
///    race and drops it too.
///
/// Rejects an unknown index (the same `NoSuchIndex` dispatch
/// `run_index_query` uses) or a **local** index (`ValidationException` —
/// LSIs are create-time-only in real DynamoDB, so deleting one is not a
/// real operation either) before proposing anything.
///
/// **A fifth, cross-cutting concern folded into steps 1 and 4 rather than
/// its own numbered step**: `index_backfill_seeder`'s cursor row
/// (`ClientCtx::clear_backfill_cursor_for_table`, ADR 0045 §5) is deleted
/// from every one of the **base** table's own tablets — not the hidden
/// table's, which step 2 already reclaims wholesale. Left alone, that row
/// would silently poison a later `CreateTableIndex` of the exact same
/// name (see `index_drain::clear_backfill_cursor`'s doc) — bounded garbage
/// was considered and rejected as an option for exactly this reason (see
/// this PR's own report). Run **twice**: once right after step 1 (the
/// index's own `Deleting` transition has already committed by then, so
/// `change_consumer_loop`'s `gsis` filter excludes it from every *new*
/// seeder tick going forward) and once more at the very end, alongside
/// step 4 — closing the one residual race a single pass can't (a seeder
/// tick that had already read the schema, and so is still mid-flight,
/// a moment *before* the `Deleting` transition landed, finishing its own
/// write after the first clear). Both passes are plain idempotent
/// tombstone writes; running twice costs nothing.
async fn drop_index(ctx: &ClientCtx, table: &str, index: &str) -> Result<(), WireError> {
    let meta = metadata_fresh(ctx).await;
    let Some(def) = meta
        .table_indexes(table)
        .iter()
        .find(|d| d.name == index)
        .cloned()
    else {
        return Err(registry_error(animus_dynamo::RegistryError::NoSuchIndex(
            index.to_owned(),
        )));
    };
    if def.kind == IndexKind::Local {
        return Err(WireError::validation(format!(
            "local secondary index `{index}` cannot be deleted (LSIs are create-time-only \
             and live for the base table's whole lifetime, matching DynamoDB)"
        )));
    }

    // Step 1: signal `Deleting` so the drain/seeder stop touching this index
    // before anything about it is torn down.
    set_index_status(ctx, table, index, IndexStatus::Deleting).await?;
    ctx.clear_backfill_cursor_for_table(table, index)
        .await
        .map_err(|e| internal(&e))?;

    // Step 2: reclaim the hidden table's own tablets.
    let hidden_table = dynamo_index::index_table_name(table, index);
    ctx.drop_table_tablets(hidden_table.clone())
        .await
        .map_err(|e| internal(&e))?;

    // Step 3: remove the catalog definition (also prunes `index_backfill` rows).
    drop_table_index(ctx, table, index).await?;

    // Step 4: belt-and-suspenders — see this function's own doc for both halves.
    if metadata_fresh(ctx).await.has_table_tablet(&hidden_table) {
        ctx.drop_table_tablets(hidden_table)
            .await
            .map_err(|e| internal(&e))?;
    }
    ctx.clear_backfill_cursor_for_table(table, index)
        .await
        .map_err(|e| internal(&e))?;
    Ok(())
}

/// Propose `MetaCommand::SetIndexStatus{table, index, status}` and wait for
/// it to commit — the same commit-wait shape [`enable_stream`]/
/// [`disable_stream`] already use for their own schema-catalog proposals.
/// Idempotent: returns `Ok(())` immediately (on the first poll) if `index`
/// is already at `status`, whether from an earlier attempt at this same
/// call or a genuinely-concurrent transition.
async fn set_index_status(
    ctx: &ClientCtx,
    table: &str,
    index: &str,
    status: IndexStatus,
) -> Result<(), WireError> {
    let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
    loop {
        ctx.propose_schema(&MetaCommand::SetIndexStatus {
            table: table.to_owned(),
            index: index.to_owned(),
            status,
        })
        .await;
        if metadata_fresh(ctx)
            .await
            .table_indexes(table)
            .iter()
            .any(|d| d.name == index && d.status == status)
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(internal(&format!(
                "index `{index}` status change to {status:?} did not commit to the control \
                 plane in time (no leader reachable?)"
            )));
        }
        tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
    }
}

/// Propose `MetaCommand::DropTableIndex{table, index}` and wait for the
/// index definition to disappear from the replicated catalog. Idempotent:
/// returns `Ok(())` immediately if `index` is already absent (a retry after
/// a prior attempt's proposal already committed).
async fn drop_table_index(ctx: &ClientCtx, table: &str, index: &str) -> Result<(), WireError> {
    let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
    loop {
        ctx.propose_schema(&MetaCommand::DropTableIndex {
            table: table.to_owned(),
            index: index.to_owned(),
        })
        .await;
        if !metadata_fresh(ctx)
            .await
            .table_indexes(table)
            .iter()
            .any(|d| d.name == index)
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(internal(&format!(
                "index `{index}` definition did not drop from the control plane in time \
                 (no leader reachable?)"
            )));
        }
        tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
    }
}

/// `DescribeTable` (ADR 0042 §2): a pure read of the replicated catalog — key
/// schema, secondary-index definitions (each with its **real** lifecycle
/// `IndexStatus`, ADR 0045 §6 Fork D), and stream configuration (+ ARN).
/// `ctx` is unused today (every input comes from `meta`) but kept for
/// signature symmetry with the other operation handlers and in case a
/// future addition needs it (e.g. live tablet stats).
#[allow(clippy::unnecessary_wraps)] // matches every other operation handler's `Result` shape
fn describe_table(_ctx: &ClientCtx, meta: &Metadata, table: &str) -> Result<String, WireError> {
    let Some(control_schema) = meta.table_schema(table) else {
        return Err(registry_error(animus_dynamo::RegistryError::NoSuchTable(
            table.to_owned(),
        )));
    };
    let dynamo_schema = schema_bridge::to_dynamo(control_schema);
    let key_types = schema_bridge::key_attribute_types(control_schema);
    let index_defs = meta.table_indexes(table);
    let indexes = schema_bridge::indexes_to_dynamo(index_defs);
    // The Fork-D side channel (`wire::describe_table_response`'s doc): each
    // index's real replicated-catalog status, kept separate from
    // `SecondaryIndex` (a pure `CreateTable`-input shape).
    let index_statuses: Vec<(String, IndexStatus)> = index_defs
        .iter()
        .map(|d| (d.name.clone(), d.status))
        .collect();
    let stream_desc = meta.table_stream(table).map(stream_description);
    Ok(wire::describe_table_response(
        table,
        &dynamo_schema,
        &key_types,
        &indexes,
        &index_statuses,
        stream_desc.as_ref(),
    ))
}

/// Mint a fresh DynamoDB Streams label (ADR 0042 §4): an ISO8601-ish
/// timestamp derived from this node's own `env.now()` — never the wall clock
/// directly (ADR 0003's `Env`-seam determinism rule) — suffixed with this
/// node's own id so two different nodes minting at a coincidentally
/// identical elapsed time can never collide. Not a genuine calendar
/// timestamp: `ProdEnv::now()` is monotonic since **process start**, not
/// wall-clock epoch, so the rendered date drifts from real time the longer a
/// process has been up. That's an accepted cosmetic gap, not a correctness
/// one — a stream's identity is `(table, label)`, and this adapter's own
/// `DescribeStream`/`GetRecords`/`GetShardIterator` (ADR 0042 §4) validate
/// the *current* label byte-for-byte, never parse it as a date; the ISO8601
/// shape only matters for fidelity with real DynamoDB's own label format.
fn mint_stream_label(ctx: &ClientCtx) -> String {
    format!("{}-{}", iso8601_ish(ctx.env.now().0), ctx.env.node_id())
}

/// Render `nanos` (as this node's own `env.now()` reports it — see
/// [`mint_stream_label`]'s doc) as a UTC ISO8601-shaped timestamp with
/// millisecond precision (`"1970-01-01T00:00:00.000"`), matching real
/// DynamoDB's own stream-label shape. A small, dependency-free Gregorian
/// calendar conversion ([`civil_from_days`]) — this crate takes no date/time
/// crate dependency for one cosmetic label format.
fn iso8601_ish(nanos: u64) -> String {
    let total_ms = nanos / 1_000_000;
    let secs = i64::try_from(total_ms / 1000).unwrap_or(i64::MAX);
    let ms = total_ms % 1000;
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400); // seconds of day, always in [0, 86400)
    let (y, m, d) = civil_from_days(days);
    let hh = sod / 3600;
    let mm = (sod % 3600) / 60;
    let ss = sod % 60;
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{ms:03}")
}

/// Howard Hinnant's `civil_from_days` (public-domain date algorithm):
/// days-since-1970-01-01 → `(year, month, day)`, proleptic Gregorian, valid
/// for every `i64` input.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 }.div_euclid(146_097);
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = i64::try_from(yoe).unwrap_or(0) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = u32::try_from(doy - (153 * mp + 2) / 5 + 1).unwrap_or(1); // [1, 31]
    let m = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).unwrap_or(1); // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// `TransactWriteItems`: apply every condition-gated action **atomically**
/// (ADR 0018 §2/PR7), replacing the old serial-loop implementation's
/// documented non-atomicity gap. Every action either lands, or none do — no
/// partial application is ever observable, whether the failure is a
/// condition evaluating false or a losing race in the underlying 2PC.
///
/// **Condition evaluation, precisely — this is a layered design, not a
/// direct translation.** A DynamoDB `ConditionExpression`
/// (`attribute_exists`/`attribute_not_exists`/`attr = value`) is not
/// something [`ClientCtx::cp_txn`] understands directly — `cp_txn` (and
/// the `animus-cp-data` primitive underneath it, `KvCommand::TxnStage`'s
/// `conditions` field) speaks plain byte-level OCC: "the key's current
/// committed value must equal exactly these bytes" (or must be absent).
/// This function evaluates the *semantic* condition itself, once, via an
/// ordinary linearizable pre-read of every condition-gated key (a
/// `ConditionCheck`, or a `Put`/`Delete`/`Update` carrying its own
/// `condition`), then **compiles a true evaluation into an equality
/// condition on the exact bytes just read** — a false condition rejects
/// the whole request **before `cp_txn` is ever called**, so nothing has
/// been staged and there is nothing to unwind.
///
/// **Two different `cp_txn` mechanisms, one per condition kind — since the
/// ADR 0018 §2 apply-time write-key conditions amendment, both give full
/// cross-node OCC.** A `ConditionCheck`'s observed value becomes an
/// ordinary `preconditions` entry (`cp_txn`'s pre-existing cross-key
/// mechanism: re-read once before staging, once more right before the
/// commit decision) — its key is, by construction, never one this
/// transaction writes (the duplicate-item check below guarantees every
/// action targets a distinct key), so that re-read always observes an
/// ordinary committed value. A **write** action's own key is different:
/// its `preconditions`-style re-read would retry against *this same
/// transaction's* own freshly-staged, still-unresolved intent (found the
/// hard way while building the original PR7 design — see the ADR's PR7
/// amendment for the full stall account: the re-read blocked in `cp_read`'s
/// retry loop for several seconds until the background `txn_resolver_loop`
/// forced a decision, producing a spurious "value changed" cancellation
/// unrelated to any real conflict). This amendment's fix is a **different
/// primitive for exactly this case**: a write action's own condition
/// becomes a `write_conditions` entry, checked once against the key's
/// *pre-intent committed* value directly inside `TxnStage`'s own apply arm
/// — no re-read, so no self-reference to stall against. `ctx.data().
/// rmw_lock`, held below, is no longer what makes a write's own condition
/// correct across nodes (that's `cp_txn`'s apply-time OCC now, proven by
/// `animusd/tests/dynamo_txn.rs`'s cross-node racing-conditional-writes
/// regression); it stays only to serialize this node's own conditional
/// writes against each other for throughput/ordering, the same role it
/// plays for a plain single-item `PutItem`/`DeleteItem`/`UpdateItem`.
///
/// **Every key is touched by at most one action** (validated up front,
/// matching DynamoDB's own "cannot include multiple operations on one
/// item" rule, and the structural precondition of the paragraph above) —
/// `cp_txn`'s `writes` has no concept of "these two entries are for the
/// same key," so two actions racing to write the same key within one
/// request would otherwise silently resolve by list order, not a
/// client-visible error.
///
/// **Failure exception shape**: any condition failure, or a `cp_txn` abort
/// (a lost 2PC race, an own-key `write_conditions` failure, or a
/// `ConditionCheck` precondition that changed underneath this request), is
/// reported as `TransactionCanceledException` — the real DynamoDB
/// exception type for a transaction (as opposed to
/// `ConditionalCheckFailedException`, which only a single-item conditional
/// write returns) — in **simple form**: one message, not AWS's per-action
/// `CancellationReasons` array (explicitly deferred, ADR 0018 PR1 amendment
/// decision 4 / the PR7 amendment).
///
/// **The all-`ConditionCheck` corner case**: `cp_txn` requires at least one
/// write to anchor its 2PC record on. A request with no `Put`/`Delete`/
/// `Update` at all (every action a bare `ConditionCheck`) has nothing to
/// stage, so this falls back to a second, immediate by-value re-check of
/// every condition (mirroring `cp_txn`'s own pre-commit refresh) instead of
/// calling it — the same OCC guarantee, just without a durable transaction
/// record backing the window. A narrow, documented limitation of this
/// corner case (see the PR7 ADR amendment), not the common path.
///
/// **A write action against an indexed/streamed table (ADR 0046 A1/U3,
/// `TxnStage` kind-writes stack) now participates like any other.** Earlier
/// revisions rejected these outright, up front: `cp_txn`'s
/// `KvCommand::TxnStage` only ever staged the base row, so committing one
/// would have left that table's indexes permanently stale (or its stream
/// permanently missing that write) with no error. That gap is closed —
/// `TxnStage` now stages the derived kind-writes/change-log payload inside
/// the base write's own intent, materialized atomically by `TxnResolve` at
/// its own resolve ts (ADR 0046 Decision 2's "materialize-at-resolve",
/// never as a separately-staged intent). **The payload itself is never
/// computed here**: for every table (the kind path is universal, ADR 0049), this
/// function builds a [`crate::PendingKindWrite`] (the item identity + op +
/// condition) instead of precomputing a value from a coordinator-local
/// read — evaluation (old-image read, condition check, LSI/change-log
/// diff) happens **at the item's own tablet leader**, at stage time
/// (`ClientCtx::txn_stage_local`, mirroring [`kind_write_item_at_leader`]'s
/// U3 shape exactly), closing the identical cross-node stale-diff race a
/// coordinator-evaluated design would reintroduce. A `ConditionCheck`
/// against such a table is unaffected either way (it writes nothing, so
/// stays the ordinary cross-key precondition below).
async fn run_transact(
    ctx: &ClientCtx,
    meta: &Metadata,
    actions: &[TransactAction],
) -> Result<String, WireError> {
    if actions.is_empty() {
        return Err(WireError::validation(
            "TransactWriteItems requires at least one action",
        ));
    }
    if actions.len() > MAX_TRANSACT_ITEMS {
        return Err(WireError::validation(format!(
            "TransactWriteItems supports at most {MAX_TRANSACT_ITEMS} actions"
        )));
    }

    // Serialize against this node's other RMWs across the pre-read/evaluate
    // span below — exactly like every other conditional write here
    // (`PutItem`/`DeleteItem`/`UpdateItem`). `cp_txn`'s own cross-tablet 2PC
    // (now including apply-time `write_conditions` OCC for a write action's
    // own condition, ADR 0018 §2 amendment) is what makes the commit —
    // *and* every condition's cross-node correctness — atomic; this lock
    // only smooths same-node throughput/ordering between two conditional
    // writes on THIS node, it is no longer load-bearing for correctness.
    //
    // **Scoped to end BEFORE `cp_txn` is called, deliberately** (ADR 0046
    // U3, PR2) — `cp_txn` → `txn_prepare` → `ClientCtx::txn_stage_local`
    // takes this SAME node-local `ctx.data().rmw_lock` again for any
    // kind-write-path write action, at the moment it evaluates that action
    // at the tablet's own leader. On a combined-role node hosting the
    // tablet leader itself, `CpRoute::Local` runs that evaluation
    // in-process on this exact `ClientCtx` — holding this guard across the
    // `cp_txn` call would self-deadlock a `tokio::sync::Mutex` (not
    // reentrant) the instant a write action targets a locally-led
    // kind-write-path table (every single-node/combined-role deployment
    // hits this immediately; a real regression a genuinely single-node
    // `ProdEnv` transactional-write test caught).
    let mut writes: Vec<crate::TxnTableWrite> = Vec::new();
    let mut preconditions: Vec<crate::TxnPrecondition> = Vec::new();
    // Always empty since Train A rung 5 deleted the coordinator-valued write
    // path: a write action's own condition rides `PendingKindWrite::
    // condition` now. Kept as an explicit empty argument because `cp_txn`'s
    // own `write_conditions` mechanism (ADR 0018 §2's apply-time write-key
    // conditions) is still real machinery the raw `ClientRequest::Txn`
    // protocol can exercise.
    let write_conditions: Vec<crate::TxnWriteCondition> = Vec::new();
    let _rmw = ctx.data().rmw_lock.lock().await;
    let mut seen: BTreeSet<(String, Vec<u8>)> = BTreeSet::new();

    for action in actions {
        let table = action.table().to_owned();
        let (key_item, condition): (&Item, Option<&ConditionExpression>) = match action {
            TransactAction::Put {
                item, condition, ..
            } => (item, condition.as_ref()),
            TransactAction::Delete { key, condition, .. } => (key, condition.as_ref()),
            TransactAction::Update { key, condition, .. } => (key, condition.as_ref()),
            TransactAction::ConditionCheck { key, condition, .. } => (key, Some(condition)),
        };
        let is_condition_check = matches!(action, TransactAction::ConditionCheck { .. });
        let (pk, sk) = resolve_key(ctx, meta, &table, key_item)?;
        let data_key = item_key(&pk, sk.as_ref());
        if !seen.insert((table.clone(), data_key.clone())) {
            return Err(WireError::validation(
                "Transaction request cannot include multiple operations on one item",
            ));
        }

        // ADR 0046 U3 (universal since ADR 0049): EVERY write action — never
        // a bare `ConditionCheck`, which writes nothing and stays the
        // ordinary precondition path below — defers entirely to the
        // participant leader: no coordinator-local read, no
        // coordinator-local condition evaluation, no coordinator-computed
        // diff. See this function's own doc and `PendingKindWrite`'s doc.
        if !is_condition_check {
            let op = match action {
                TransactAction::Put { item, .. } => KindWriteOp::Put(item.clone()),
                TransactAction::Delete { .. } => KindWriteOp::Delete,
                TransactAction::Update {
                    key,
                    actions: update_actions,
                    ..
                } => KindWriteOp::Update {
                    key_item: key.clone(),
                    actions: update_actions.clone(),
                },
                TransactAction::ConditionCheck { .. } => {
                    unreachable!("is_condition_check excludes this arm")
                }
            };
            writes.push(crate::TxnTableWrite {
                table: table.clone(),
                key: data_key,
                value: None,
                pending: Some(crate::PendingKindWrite {
                    pk,
                    sk,
                    op,
                    condition: condition.cloned(),
                }),
            });
            continue;
        }

        // Only a `ConditionCheck` reaches here (every write action deferred
        // to its participant leader above, since ADR 0049 made the kind
        // path universal — the coordinator-valued write path and its
        // own-key `write_conditions` mechanism were deleted with it in
        // Train A rung 5; a write action's condition rides
        // `PendingKindWrite::condition` + the C1 own-key OCC instead). A
        // `ConditionCheck`'s condition evaluates against a read here, and
        // its observed value becomes an ordinary cross-key `cp_txn`
        // precondition.
        let raw = raw_quorum_read(ctx, meta, &table, &data_key).await?;
        let decoded = match &raw {
            Some(bytes) => wire::decode_stored_item(bytes)?,
            _ => None,
        };
        if let Some(cond) = condition
            && !cond.evaluate(decoded.as_ref())
        {
            return Err(WireError::transaction_canceled(
                "a transaction condition check failed",
            ));
        }
        preconditions.push((table, data_key, raw));
    }
    // ADR 0046 U3, PR2: released HERE, before any `raw_quorum_read`/`cp_txn`
    // call below — see this guard's own doc for why holding it any longer
    // would self-deadlock on a combined-role node.
    drop(_rmw);

    if writes.is_empty() {
        // Every action was a `ConditionCheck` — see this function's doc for
        // why this is a documented, narrow fallback rather than a call to
        // `cp_txn` (which requires at least one write to anchor on).
        for (table, key, expected) in &preconditions {
            let actual = raw_quorum_read(ctx, meta, table, key).await?;
            if &actual != expected {
                ctx.data()
                    .raftkv_metrics
                    .incr(Metric::DynamoTransactWritesCanceled);
                return Err(WireError::transaction_canceled(
                    "a transaction condition check failed",
                ));
            }
        }
        ctx.data()
            .raftkv_metrics
            .incr(Metric::DynamoTransactWritesCommitted);
        return Ok(wire::empty_response());
    }

    match ctx.cp_txn(writes, preconditions, write_conditions).await {
        Ok(_commit_ts) => {
            ctx.data()
                .raftkv_metrics
                .incr(Metric::DynamoTransactWritesCommitted);
            Ok(wire::empty_response())
        }
        Err(e) => {
            ctx.data()
                .raftkv_metrics
                .incr(Metric::DynamoTransactWritesCanceled);
            Err(WireError::transaction_canceled(format!(
                "transaction cancelled: {e}"
            )))
        }
    }
}

/// `TransactGetItems`: a consistent multi-key read (ADR 0018 §2/PR7, new — no
/// prior non-atomic implementation to replace).
///
/// **Semantics, precisely — a serializable snapshot via
/// quiescence-confirmation, not a wait-free one.** This design went through
/// **four** redesigns of the same underlying problem before it stopped
/// producing false-positive torn reads — three at the pure-protocol level
/// (see `animus-test/tests/txn_serializable.rs`'s `quiescent_multi_read` doc
/// for that full account) and a fourth, **live in production**, found by
/// this exact function after the first three had already landed (the
/// torn-pair-fix stack's ADR 0018 §2 amendment — read that amendment for
/// the full incident):
///
/// 1. A single coordinator-minted `read_at` snapshot timestamp is
///    **structurally unsound** — `RaftKvNode::mint_pushed`'s write-conflict
///    floor stamps a write *above* whatever ceiling a prior future-padded
///    read already pushed that group's committed ceiling to, and since a
///    group's `Hlc` only ever ratchets forward, that becomes a **permanent**
///    floor no fixed or dynamically-sampled margin can close.
/// 2. Force-resolving once then reading sequentially is undermined by a
///    slow key observing a much later moment than a fast one.
/// 3. Reading every key **at latest, concurrently**
///    (`futures::future::join_all`, never a sequential per-key loop) closes
///    (1) and (2), but is still only sound if **every key's own per-call
///    read is itself a single point-in-time sample** — which the third
///    redesign assumed without enforcing.
/// 4. **It wasn't.** `ClientCtx::cp_read` (what this function used to call
///    per key) is deliberately *asymmetric* by design for its real job
///    (serving `GetItem`, a genuinely single-key op): a locally-`Pending`
///    intent gets a **bounded blocking chase** (`RaftKvNode::
///    linearizable_get_served`, correct there — waiting out a contended
///    intent is the right behavior for a lone reader), while a **foreign**
///    intent gets one status query + push and, if still undecided, an
///    immediate `"; retry"` this function's own per-key `join_all` call
///    re-issues on the *next* round rather than waiting inline. Under a
///    tight, back-to-back writer alternating two keys of the *same*
///    transaction, this made the two calls inside one round systematically
///    sample **different instants** — the blocking-chase key kept
///    fast-forwarding to whatever the writer had most recently committed by
///    the time its own wait finished, while the give-up-immediately key
///    lagged by however many extra 50ms outer-loop passes its own retries
///    needed. Two consecutive rounds could then agree byte-for-byte on the
///    exact same torn `[key_a: N, key_b: N-1]` pair, satisfying the
///    two-round check while reporting a snapshot that was never true at any
///    single instant.
///
/// **The fix — every key's read is a uniform, non-blocking, single-shot
/// sample; only the ROUND loop retries.** [`quiescent_multi_get`] now calls
/// [`ClientCtx::cp_read_snapshot`] instead of `cp_read`: `Value` if resolved,
/// `Unresolved` if the key's intent (local or foreign — both now carry the
/// identical `IntentInfo` shape, see `animus_cp_data::FastRead`'s doc) did
/// not resolve within one status-query-plus-push attempt. A round where
/// *any* key comes back `Unresolved` is discarded outright — `previous`
/// resets to `None` rather than comparing a partial round — since a round
/// with an unresolved key proves nothing about whether the *other* keys'
/// values are stale. This removes the latency asymmetry (1)–(4) needed:
/// every key in a round now costs the same one-or-two round trips, so no
/// systematic per-key skew survives round to round. Bounded to
/// [`TRANSACT_GET_MAX_ROUNDS`] rounds; a snapshot that never quiesces
/// (sustained contention on one of the requested keys) reports a retryable
/// `TransactionCanceledException` rather than ever returning a possibly-torn
/// result. Regression: `animusd/tests/dynamo_txn.rs::
/// transact_get_items_never_observes_a_torn_pair_under_concurrent_writes`
/// (0/20 solo runs after this fix, vs. a reproducible ~15–30% failure rate
/// before it — see the ADR amendment for the exact before/after numbers).
async fn run_transact_get(
    ctx: &ClientCtx,
    meta: &Metadata,
    gets: &[TransactGet],
) -> Result<String, WireError> {
    if gets.is_empty() {
        return Err(WireError::validation(
            "TransactGetItems requires at least one item",
        ));
    }
    if gets.len() > MAX_TRANSACT_ITEMS {
        return Err(WireError::validation(format!(
            "TransactGetItems supports at most {MAX_TRANSACT_ITEMS} items"
        )));
    }

    let mut keys: Vec<(String, Vec<u8>)> = Vec::with_capacity(gets.len());
    let mut seen: BTreeSet<(String, Vec<u8>)> = BTreeSet::new();
    for g in gets {
        let (pk, sk) = resolve_key(ctx, meta, &g.table, &g.key)?;
        let data_key = item_key(&pk, sk.as_ref());
        if !seen.insert((g.table.clone(), data_key.clone())) {
            return Err(WireError::validation(
                "Transaction request cannot include multiple operations on one item",
            ));
        }
        keys.push((g.table.clone(), data_key));
    }

    let raw = quiescent_multi_get(ctx, &keys).await?;

    let mut items: Vec<Option<Item>> = Vec::with_capacity(gets.len());
    for bytes in raw {
        let item = match bytes {
            Some(b) => wire::decode_stored_item(&b)?,
            None => None,
        };
        items.push(item);
    }
    for (g, item) in gets.iter().zip(items.iter_mut()) {
        if let Some(projection) = &g.projection
            && let Some(present) = item.take()
        {
            *item = Some(wire::project(Some(projection), &present));
        }
    }
    Ok(wire::transact_get_response(&items))
}

/// The quiescence-confirmation read loop backing [`run_transact_get`]
/// (ADR 0018 §2, uniform-single-shot rounds per the torn-pair-fix stack's
/// amendment — see [`run_transact_get`]'s doc for the full incident this
/// design closes): read every `(table, key)` concurrently
/// (`futures::future::join_all` — never a sequential per-key loop, which
/// would let a slow key observe a much later moment than a fast one) via
/// [`ClientCtx::cp_read_snapshot`] (works from any node — routes/forwards to
/// each key's own tablet leader, resolves any intent it meets with exactly
/// one status-query-plus-push attempt, **never** a per-key wait).
///
/// **Invariant: one round samples one instant for every key.** A round
/// where *every* key resolves is compared against the previous round for
/// byte-for-byte agreement, exactly like before; a round where *any* key
/// comes back [`SnapshotRead::Unresolved`] is discarded outright —
/// `previous` resets to `None`, never compared, since a partial round
/// proves nothing about whether the *other* keys' values are stale (this is
/// the exact case the fourth abandoned design, documented on
/// [`run_transact_get`], let slip through as a false-positive quiesced
/// snapshot). Retried as a whole until two consecutive **complete** rounds
/// agree on every key, bounded by [`TRANSACT_GET_MAX_ROUNDS`].
async fn quiescent_multi_get(
    ctx: &ClientCtx,
    keys: &[(String, Vec<u8>)],
) -> Result<Vec<Option<Vec<u8>>>, WireError> {
    let mut previous: Option<Vec<Option<Vec<u8>>>> = None;
    for round_idx in 0..TRANSACT_GET_MAX_ROUNDS {
        let futs = keys
            .iter()
            .map(|(table, key)| ctx.cp_read_snapshot(table, key.clone()));
        let round: Vec<Result<SnapshotRead, String>> = futures::future::join_all(futs).await;

        let mut values = Vec::with_capacity(round.len());
        let mut unresolved = false;
        for r in round {
            match r.map_err(|e| internal(&e))? {
                SnapshotRead::Value(v) => values.push(v),
                SnapshotRead::Unresolved => {
                    unresolved = true;
                    break;
                }
            }
        }

        if unresolved {
            // Never feed a partial/unresolved round into the two-round
            // agreement check, and never let it stand as `previous` either
            // — both would let a torn instant slip through as one half of
            // a "matching" pair (exactly the production bug this design
            // closes; see `run_transact_get`'s doc).
            previous = None;
        } else if previous.as_ref() == Some(&values) {
            if round_idx > 1 {
                ctx.data()
                    .raftkv_metrics
                    .incr(Metric::DynamoTransactGetsRetried);
            } else {
                ctx.data().raftkv_metrics.incr(Metric::DynamoTransactGetsOk);
            }
            return Ok(values);
        } else {
            previous = Some(values);
        }

        if round_idx + 1 < TRANSACT_GET_MAX_ROUNDS {
            tokio::time::sleep(TRANSACT_GET_POLL).await;
        }
    }
    ctx.data()
        .raftkv_metrics
        .incr(Metric::DynamoTransactGetsRetried);
    Err(WireError::transaction_canceled(
        "TransactGetItems could not observe a quiescent snapshot of every key within budget; \
         retry",
    ))
}

/// Serve a `Query`. A **base-table** query (`index` is `None`) is a **native
/// quorum range scan** ([`DataClient::scan`]) over the partition's contiguous
/// key sub-range `[escape(table)||escape(pk), …)` — no in-memory key tracking —
/// applying an optional sort-key condition on the recovered sort bytes. An
/// **index** query (ADR 0041 §5) is now a *second* native range scan — over the
/// GSI's hidden table or the LSI's `KIND_LSI` scope — decoding each row's
/// already-projected stored value directly, with no base-table read-back. An
/// optional `projection` keeps only the requested attributes of each returned
/// item. `consistent_read` (ADR 0041 §5) is only ever meaningful for an
/// **index** query — a base query is always linearizable here regardless, so
/// it's accepted-and-ignored on that branch.
#[allow(clippy::too_many_arguments)] // one `Query`'s full decoded shape, no natural grouping
async fn run_query(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    index: Option<&str>,
    partition_value: &AttributeValue,
    sort_condition: Option<&SortKeyCondition>,
    projection: Option<&Projection>,
    consistent_read: bool,
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
                consistent_read,
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

/// A secondary-index `Query` (ADR 0041 §5): dispatches to the GSI or LSI native
/// scan per the index's replicated **kind** (`meta.table_indexes`, not the
/// registry — an unknown index is the same `NoSuchIndex` `ValidationException`
/// as before). A sort condition against a hash-only index is rejected
/// (`IndexSortMismatch`) before either path runs. `ConsistentRead: true`
/// against a **global** index is rejected here too, matching DynamoDB exactly
/// (§5: "`ConsistentRead=true` against a GSI is an error… against an LSI it is
/// honoured and already true") — an LSI is strongly consistent by
/// construction (written atomically with the base row), so `consistent_read`
/// is simply dropped on that branch, same as a base `Query`/`Scan`/`GetItem`.
/// A non-`Active` index (`Creating`/`Deleting`, ADR 0045 §6) is rejected too,
/// beside that same `ConsistentRead` check.
#[allow(clippy::too_many_arguments)] // mirrors `run_query`'s own full decoded shape
async fn run_index_query(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    index: &str,
    partition_value: &AttributeValue,
    sort_condition: Option<&SortKeyCondition>,
    projection: Option<&Projection>,
    consistent_read: bool,
) -> Result<String, WireError> {
    if !table_known(ctx, meta, table) {
        return Err(registry_error(animus_dynamo::RegistryError::NoSuchTable(
            table.to_owned(),
        )));
    }
    let Some(idx) = meta
        .table_indexes(table)
        .iter()
        .find(|d| d.name == index)
        .cloned()
    else {
        return Err(registry_error(animus_dynamo::RegistryError::NoSuchIndex(
            index.to_owned(),
        )));
    };
    if sort_condition.is_some() && idx.sort_attribute.is_none() {
        return Err(registry_error(
            animus_dynamo::RegistryError::IndexSortMismatch(index.to_owned()),
        ));
    }
    if consistent_read && idx.kind == IndexKind::Global {
        return Err(WireError::validation(format!(
            "ConsistentRead is not supported for global secondary index `{index}` \
             (a GSI is maintained asynchronously; use a base-table or LSI Query for \
             a strongly consistent read)"
        )));
    }
    // ADR 0045 §6: a GSI that is still `Creating` (mid-backfill) or
    // `Deleting` (torn down, drain/seeder no longer touching it) is not
    // queryable — matching DynamoDB, which only serves reads against an
    // `ACTIVE` index. An LSI's status is always `Active` by construction
    // (create-time-only; see `index_to_control`'s own invariant), so this
    // never fires on that branch in practice.
    if idx.status != IndexStatus::Active {
        return Err(WireError::validation(format!(
            "cannot Query index `{index}`: its status is {:?}, not ACTIVE (a secondary index \
             is only queryable once fully backfilled)",
            idx.status
        )));
    }
    match idx.kind {
        IndexKind::Global => {
            run_gsi_query(
                ctx,
                meta,
                table,
                &idx,
                partition_value,
                sort_condition,
                projection,
            )
            .await
        }
        IndexKind::Local => {
            run_lsi_query(
                ctx,
                meta,
                table,
                &idx,
                partition_value,
                sort_condition,
                projection,
            )
            .await
        }
    }
}

/// A **GSI** `Query` (ADR 0041 §5): a native quorum range scan of the index's
/// own hidden table (`index_table_name`), over `token(ihash) || escape(ihash)`
/// (narrowed to `escape(ihash) || escape(isort)` for an `Equals` sort
/// condition — [`dynamo_index::gsi_hash_sort_prefix`]) — the same scan
/// primitive [`run_base_query`] uses, mirroring `index_drain.rs::gsi_row_key`
/// byte-for-byte. Row values are already `wire::encode_stored_item(projected
/// item)` (the drain applies the index's declared projection when it
/// materializes each row), so this decodes them directly — **no per-key
/// base-table read-back**.
///
/// **Eventually consistent, by DynamoDB's own contract**: a hidden table with
/// no tablet yet (this index has never drained anything) reads as **empty**
/// rather than waiting on routing for a tablet that may not exist yet — the
/// same gate [`ClientCtx::cp_get`] uses for an unprovisioned table.
async fn run_gsi_query(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    idx: &IndexDef,
    partition_value: &AttributeValue,
    sort_condition: Option<&SortKeyCondition>,
    projection: Option<&Projection>,
) -> Result<String, WireError> {
    let index_table = dynamo_index::index_table_name(table, &idx.name);
    if !meta.has_table_tablet(&index_table) {
        return Ok(wire::query_response(&[]));
    }
    let composite = idx.sort_attribute.is_some();
    // Narrow to the `Equals` sub-prefix when possible (an engine-level
    // optimization); every other shape (no condition, `Between`,
    // `BeginsWith`) scans the whole hash value's rows and filters below.
    let (within_prefix, narrowed) = match sort_condition {
        Some(SortKeyCondition::Equals(v)) if composite => {
            (dynamo_index::gsi_hash_sort_prefix(partition_value, v), true)
        }
        _ => (dynamo_index::gsi_hash_prefix(partition_value), false),
    };
    let mut prefix = partition_token(&storage_key(partition_value, None)).to_vec();
    prefix.extend_from_slice(&within_prefix);
    let end = dynamo_index::range_end(&prefix);
    let pairs = native_scan(ctx, &index_table, &prefix, Some(&end), None).await?;
    let mut items = Vec::with_capacity(pairs.len());
    for (key, value) in pairs {
        // A pruned/undecodable row shouldn't normally occur (the drain deletes
        // stale rows outright, never tombstones them), but skip rather than
        // fail a whole query on one corrupt row.
        let Some(item) = wire::decode_stored_item(&value)? else {
            continue;
        };
        if !narrowed && let Some(cond) = sort_condition {
            let within = key.get(TOKEN_BYTES..).unwrap_or(&[]);
            let Some(parsed) = dynamo_index::parse_gsi_row_key(within, composite) else {
                continue;
            };
            let Some(sort_bytes) = parsed.sort else {
                continue;
            };
            if !cond.matches(&AttributeValue::B(sort_bytes)) {
                continue;
            }
        }
        items.push(wire::project(projection, &item));
    }
    Ok(wire::query_response(&items))
}

/// An **LSI** `Query` (ADR 0041 §5): a **linearizable** range scan of the
/// *base table's own tablet*, over its `KIND_LSI` scope
/// (`ClientCtx::cp_scan_kind`) — strongly consistent, since LSI rows commit in
/// the same Raft entry as the base row they derive from (ADR 0041 §2). Scans
/// the partition's whole LSI-index sub-range (`lsi_index_prefix`) and filters
/// by any sort condition on the recovered alt-sort segment
/// (`parse_lsi_row_key`) — LSI rows also store the projected item (see
/// `kind_writes_for_item`), so this decodes them directly.
async fn run_lsi_query(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    idx: &IndexDef,
    partition_value: &AttributeValue,
    sort_condition: Option<&SortKeyCondition>,
    projection: Option<&Projection>,
) -> Result<String, WireError> {
    if !meta.has_table_tablet(table) {
        return Ok(wire::query_response(&[]));
    }
    let prefix = token_prefixed(
        partition_value,
        &dynamo_index::lsi_index_prefix(partition_value, &idx.name),
    );
    let end = dynamo_index::range_end(&prefix);
    let pairs = ctx
        .cp_scan_kind(table, KIND_LSI, prefix, end)
        .await
        .map_err(|e| internal(&e))?;
    let mut items = Vec::with_capacity(pairs.len());
    for (key, value) in pairs {
        let Some(item) = wire::decode_stored_item(&value)? else {
            continue;
        };
        if let Some(cond) = sort_condition {
            let within = key.get(TOKEN_BYTES..).unwrap_or(&[]);
            let Some(parsed) = dynamo_index::parse_lsi_row_key(within) else {
                continue;
            };
            if !cond.matches(&AttributeValue::B(parsed.alt_sort)) {
                continue;
            }
        }
        items.push(wire::project(projection, &item));
    }
    Ok(wire::query_response(&items))
}

/// A `Scan`: the base table, or (ADR 0041 §5) a secondary index when `index`
/// is set. `mirror_catalog_schema` is hoisted here (rather than duplicated in
/// each of the three bodies below) so both the base and index paths see an
/// up-to-date registry mirror before resolving anything.
#[allow(clippy::too_many_arguments)] // mirrors `run_query`'s own full decoded shape
async fn run_scan(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    index: Option<&str>,
    limit: Option<usize>,
    exclusive_start_key: Option<Item>,
    filter: Option<&ConditionExpression>,
    projection: Option<&Projection>,
    consistent_read: bool,
) -> Result<String, WireError> {
    mirror_catalog_schema(ctx, meta, table);
    match index {
        Some(index) => {
            run_index_scan(
                ctx,
                meta,
                table,
                index,
                limit,
                exclusive_start_key,
                filter,
                projection,
                consistent_read,
            )
            .await
        }
        None => {
            run_base_scan(
                ctx,
                meta,
                table,
                limit,
                exclusive_start_key,
                filter,
                projection,
            )
            .await
        }
    }
}

/// Serve a base-table `Scan` via a **native quorum range scan** (`cp_scan`) over
/// the whole table's data-plane key range `[escape(table), …)` — no in-memory
/// key tracking. The scan returns live `(key, value)` pairs in key order across a
/// read quorum (tombstones already excluded by the data plane); the edge decodes
/// each, drops a DynamoDB tombstone *value*, applies an optional post-read
/// `filter`, then `projection`.
///
/// DynamoDB pagination is layered on top: `exclusive_start_key` resolves to the
/// storage key to scan strictly *after* (so each page's range starts at the
/// cursor); `limit` caps the **examined** (decoded, live) items and is **pushed
/// down** to the native scan via [`paginated_table_examine`] (fetching windows of
/// the remaining count, continuing past DynamoDB tombstone values so they never
/// consume a slot) — a small page on a large table reads ~limit rows, not the
/// whole table. The page boundary always lands on a live, decodable item; when
/// the page is truncated the `LastEvaluatedKey` is that boundary item's key
/// attributes. The cursor thus advances over the **live data-plane keys** the
/// scan returned — not a tracked set — so it is correct after a restart or on a
/// follower that never saw a write.
async fn run_base_scan(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    limit: Option<usize>,
    exclusive_start_key: Option<Item>,
    filter: Option<&ConditionExpression>,
    projection: Option<&Projection>,
) -> Result<String, WireError> {
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
    let want = limit.map(|n| n.saturating_add(1));
    // A DynamoDB `DeleteItem` stores a *tombstone value* (a live pair to the
    // data plane, decoding to `None`); `paginated_table_examine` continues past
    // it without consuming a `Limit` slot.
    let (mut examined, _exhausted) =
        paginated_table_examine(ctx, table, from, want, |_key, value| {
            wire::decode_stored_item(value)
        })
        .await?;
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

/// A secondary-index `Scan` (ADR 0041 §5): dispatches to the GSI or LSI native
/// scan per the index's replicated **kind**, mirroring [`run_index_query`]'s
/// identical dispatch (an unknown index is the same `NoSuchIndex`
/// `ValidationException`). `ConsistentRead: true` against a **global** index
/// is rejected here too, matching DynamoDB exactly and `run_index_query`'s own
/// enforcement point — see that function's doc, including the non-`Active`
/// index rejection (ADR 0045 §6).
#[allow(clippy::too_many_arguments)] // mirrors `run_index_query`'s own full decoded shape
async fn run_index_scan(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    index: &str,
    limit: Option<usize>,
    exclusive_start_key: Option<Item>,
    filter: Option<&ConditionExpression>,
    projection: Option<&Projection>,
    consistent_read: bool,
) -> Result<String, WireError> {
    if !table_known(ctx, meta, table) {
        return Err(registry_error(animus_dynamo::RegistryError::NoSuchTable(
            table.to_owned(),
        )));
    }
    let Some(idx) = meta
        .table_indexes(table)
        .iter()
        .find(|d| d.name == index)
        .cloned()
    else {
        return Err(registry_error(animus_dynamo::RegistryError::NoSuchIndex(
            index.to_owned(),
        )));
    };
    if consistent_read && idx.kind == IndexKind::Global {
        return Err(WireError::validation(format!(
            "ConsistentRead is not supported for global secondary index `{index}` \
             (a GSI is maintained asynchronously; use a base-table or LSI Scan for \
             a strongly consistent read)"
        )));
    }
    // ADR 0045 §6 — see `run_index_query`'s identical gate for the rationale.
    if idx.status != IndexStatus::Active {
        return Err(WireError::validation(format!(
            "cannot Scan index `{index}`: its status is {:?}, not ACTIVE (a secondary index \
             is only queryable once fully backfilled)",
            idx.status
        )));
    }
    match idx.kind {
        IndexKind::Global => {
            run_gsi_scan(
                ctx,
                meta,
                table,
                &idx,
                limit,
                exclusive_start_key,
                filter,
                projection,
            )
            .await
        }
        IndexKind::Local => {
            run_lsi_scan(
                ctx,
                meta,
                table,
                &idx,
                limit,
                exclusive_start_key,
                filter,
                projection,
            )
            .await
        }
    }
}

/// A **GSI** `Scan` (ADR 0041 §5): the base-`Scan` machinery
/// ([`paginated_table_examine`]) reused against the index's own hidden table
/// (`index_table_name`), fanned across *its* tablets in token order exactly
/// like [`run_base_scan`] — the only differences are the target table and the
/// pagination cursor's shape (see [`gsi_key_item_of`]/[`gsi_resume_key`]'s
/// docs for why a GSI cursor carries both the index's own key and the base
/// table's key). A hidden table with no tablet yet (nothing has drained) reads
/// as empty, the same gate [`run_gsi_query`] uses.
#[allow(clippy::too_many_arguments)] // mirrors `run_gsi_query`'s own full decoded shape
async fn run_gsi_scan(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    idx: &IndexDef,
    limit: Option<usize>,
    exclusive_start_key: Option<Item>,
    filter: Option<&ConditionExpression>,
    projection: Option<&Projection>,
) -> Result<String, WireError> {
    let index_table = dynamo_index::index_table_name(table, &idx.name);
    if !meta.has_table_tablet(&index_table) {
        return Ok(wire::scan_response(&[], 0, None));
    }
    let base = schema_for(meta, table);
    let from = match &exclusive_start_key {
        Some(key_item) => gsi_resume_key(key_item, &base, idx)?,
        None => Vec::new(),
    };
    let want = limit.map(|n| n.saturating_add(1));
    // A GSI row is never stored as a DynamoDB tombstone (ADR 0041 §4's
    // as-built note — the drain prunes with a real engine delete), so `keep`
    // only needs to guard against a corrupt row, mirroring `run_gsi_query`'s
    // own "skip rather than fail the whole query" defensiveness.
    let (mut examined, _exhausted) =
        paginated_table_examine(ctx, &index_table, from, want, |_key, value| {
            wire::decode_stored_item(value)
        })
        .await?;
    let truncated = limit.is_some_and(|n| examined.len() > n);
    if let Some(n) = limit {
        examined.truncate(n);
    }
    let scanned = examined.len();
    let last_evaluated_key = if truncated {
        examined
            .last()
            .and_then(|(_, item)| gsi_key_item_of(item, &base, idx))
    } else {
        None
    };
    let mut items = Vec::new();
    for (_key, item) in &examined {
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

/// An **LSI** `Scan` (ADR 0041 §5): a **table-wide** linearizable fan-out over
/// the base table's `KIND_LSI` scope (`ClientCtx::cp_scan_kind_table`) — unlike
/// an LSI `Query`, which is scoped to one base partition and hence one tablet,
/// a table-wide `Scan` sweeps every tablet of `table`'s own ring. One
/// partition's LSI rows across *every* declared index interleave within that
/// scope ([`animus_dynamo::index::lsi_index_prefix`]'s layout — sorted by
/// index name ahead of the alt-sort value), so [`paginated_kind_examine`]'s
/// `keep` closure filters each raw row to the requested index by its own key
/// (`parse_lsi_row_key`) — a row of a *different* index is skipped without
/// consuming a `Limit` slot, exactly like a tombstone in the base scan.
/// `ConsistentRead: true` is accepted (already true — an LSI row commits
/// atomically with its base row).
#[allow(clippy::too_many_arguments)] // mirrors `run_lsi_query`'s own full decoded shape
async fn run_lsi_scan(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    idx: &IndexDef,
    limit: Option<usize>,
    exclusive_start_key: Option<Item>,
    filter: Option<&ConditionExpression>,
    projection: Option<&Projection>,
) -> Result<String, WireError> {
    if !meta.has_table_tablet(table) {
        return Ok(wire::scan_response(&[], 0, None));
    }
    let base = schema_for(meta, table);
    let from = match &exclusive_start_key {
        Some(key_item) => lsi_resume_key(key_item, &base, idx)?,
        None => Vec::new(),
    };
    let want = limit.map(|n| n.saturating_add(1));
    let idx_name = idx.name.clone();
    let (mut examined, _exhausted) =
        paginated_kind_examine(ctx, table, KIND_LSI, from, want, move |key, value| {
            let within = key.get(TOKEN_BYTES..).unwrap_or(&[]);
            let Some(parsed) = dynamo_index::parse_lsi_row_key(within) else {
                return Ok(None); // a malformed key; skip defensively
            };
            if parsed.index != idx_name {
                return Ok(None); // this partition's *other* LSI's row
            }
            wire::decode_stored_item(value)
        })
        .await?;
    let truncated = limit.is_some_and(|n| examined.len() > n);
    if let Some(n) = limit {
        examined.truncate(n);
    }
    let scanned = examined.len();
    let last_evaluated_key = if truncated {
        examined
            .last()
            .and_then(|(_, item)| lsi_key_item_of(item, &base, idx))
    } else {
        None
    };
    let mut items = Vec::new();
    for (_key, item) in &examined {
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

/// Fetch up to `want` (`None` = unbounded) *kept* rows starting at `cursor`,
/// from a plain table-wide native scan (`ClientCtx::cp_scan`, via
/// [`native_scan`]) — the pagination discipline every base/GSI `Scan` shares:
/// `limit` is pushed down as the remaining count, and a window that comes up
/// short because `keep` skipped some of it (a DynamoDB delete tombstone) is
/// topped up by resuming strictly past the last raw key seen, so a page's
/// count is never short just because a fetch window happened to land on
/// skipped rows. Returns the examined `(raw key, decoded item)` pairs and
/// whether the underlying range is now exhausted.
async fn paginated_table_examine(
    ctx: &ClientCtx,
    table: &str,
    mut cursor: Vec<u8>,
    want: Option<usize>,
    keep: impl Fn(&[u8], &[u8]) -> Result<Option<Item>, WireError>,
) -> Result<(Vec<(Vec<u8>, Item)>, bool), WireError> {
    let mut examined: Vec<(Vec<u8>, Item)> = Vec::new();
    loop {
        let fetch = want.map(|w| w - examined.len());
        let pairs = native_scan(ctx, table, &cursor, None, fetch).await?;
        let exhausted = fetch.is_none_or(|f| pairs.len() < f);
        let last_raw_key = pairs.last().map(|(k, _)| k.clone());
        for (key, value) in &pairs {
            if let Some(item) = keep(key, value)? {
                examined.push((key.clone(), item));
            }
        }
        if exhausted || want.is_some_and(|w| examined.len() >= w) {
            return Ok((examined, exhausted));
        }
        let mut next = last_raw_key.expect("non-exhausted fetch returned pairs");
        next.push(0x00);
        cursor = next;
    }
}

/// [`paginated_table_examine`]'s dual over a table-wide **kind-scoped** fan-out
/// (`ClientCtx::cp_scan_kind_table`) — the LSI `Scan` read primitive. Identical
/// windowed-continuation discipline, generalized so `run_lsi_scan`'s `keep`
/// can skip an interleaved *other* index's row without consuming a `Limit`
/// slot, the same way the table-wide variant skips a tombstone.
async fn paginated_kind_examine(
    ctx: &ClientCtx,
    table: &str,
    kind: u8,
    mut cursor: Vec<u8>,
    want: Option<usize>,
    keep: impl Fn(&[u8], &[u8]) -> Result<Option<Item>, WireError>,
) -> Result<(Vec<(Vec<u8>, Item)>, bool), WireError> {
    let mut examined: Vec<(Vec<u8>, Item)> = Vec::new();
    loop {
        let fetch = want.map(|w| w - examined.len());
        let pairs = ctx
            .cp_scan_kind_table(table, kind, cursor.clone(), None, fetch)
            .await
            .map_err(|e| internal(&e))?;
        let exhausted = fetch.is_none_or(|f| pairs.len() < f);
        let last_raw_key = pairs.last().map(|(k, _)| k.clone());
        for (key, value) in &pairs {
            if let Some(item) = keep(key, value)? {
                examined.push((key.clone(), item));
            }
        }
        if exhausted || want.is_some_and(|w| examined.len() >= w) {
            return Ok((examined, exhausted));
        }
        let mut next = last_raw_key.expect("non-exhausted fetch returned pairs");
        next.push(0x00);
        cursor = next;
    }
}

/// The `LastEvaluatedKey`/`ExclusiveStartKey` shape for a **GSI** scan page
/// boundary: the index's own hash/sort attributes *and* the base table's key
/// attributes — real DynamoDB's GSI cursor carries both (a GSI is sparse and
/// can duplicate an index key across items), and resuming needs the full row
/// key ([`dynamo_index::gsi_row_key`]), not just the index's own key. Both
/// attribute sets are always present in the stored item regardless of the
/// index's declared projection (`projected_item` always keeps the key
/// attributes), so this never needs a base-table read-back. `None` if `item`
/// is somehow missing one of them (shouldn't happen for a row this edge
/// wrote).
fn gsi_key_item_of(item: &Item, base: &TableSchema, idx: &IndexDef) -> Option<Item> {
    let mut key = Item::new();
    key.insert(
        idx.hash_attribute.clone(),
        item.get(&idx.hash_attribute)?.clone(),
    );
    if let Some(sort) = &idx.sort_attribute {
        key.insert(sort.clone(), item.get(sort)?.clone());
    }
    key.insert(
        base.partition_key.clone(),
        item.get(&base.partition_key)?.clone(),
    );
    if let Some(sk) = &base.sort_key
        && let Some(v) = item.get(sk)
    {
        key.insert(sk.clone(), v.clone());
    }
    Some(key)
}

/// Invert [`gsi_key_item_of`]: rebuild the raw GSI row key an
/// `ExclusiveStartKey` names, exactly matching [`dynamo_index::gsi_row_key`]'s
/// own layout, then advance one byte past it (keys are unique) so the resumed
/// scan starts strictly after the cursor.
fn gsi_resume_key(
    key_item: &Item,
    base: &TableSchema,
    idx: &IndexDef,
) -> Result<Vec<u8>, WireError> {
    let missing =
        |attr: &str| WireError::validation(format!("ExclusiveStartKey missing attribute `{attr}`"));
    let ihash = key_item
        .get(&idx.hash_attribute)
        .ok_or_else(|| missing(&idx.hash_attribute))?;
    let isort = match &idx.sort_attribute {
        Some(sort) => Some(key_item.get(sort).ok_or_else(|| missing(sort))?),
        None => None,
    };
    let base_pk = key_item
        .get(&base.partition_key)
        .ok_or_else(|| missing(&base.partition_key))?;
    let base_sk = base.sort_key.as_ref().and_then(|sk| key_item.get(sk));
    let mut after = token_prefixed(
        ihash,
        &dynamo_index::gsi_row_key(ihash, isort, base_pk, base_sk),
    );
    after.push(0x00);
    Ok(after)
}

/// The `LastEvaluatedKey`/`ExclusiveStartKey` shape for an **LSI** scan page
/// boundary: the index's own alternate-sort attribute *and* the base table's
/// key attributes (an LSI's hash is always the base partition key, so that
/// attribute alone would be ambiguous across items). `None` if `idx` is
/// malformed (no sort attribute — shouldn't occur for a real LSI) or `item`
/// is missing an expected attribute.
fn lsi_key_item_of(item: &Item, base: &TableSchema, idx: &IndexDef) -> Option<Item> {
    let sort_attr = idx.sort_attribute.as_ref()?;
    let mut key = Item::new();
    key.insert(sort_attr.clone(), item.get(sort_attr)?.clone());
    key.insert(
        base.partition_key.clone(),
        item.get(&base.partition_key)?.clone(),
    );
    if let Some(sk) = &base.sort_key
        && let Some(v) = item.get(sk)
    {
        key.insert(sk.clone(), v.clone());
    }
    Some(key)
}

/// Invert [`lsi_key_item_of`]: rebuild the raw LSI row key an
/// `ExclusiveStartKey` names, exactly matching [`dynamo_index::lsi_row_key`]'s
/// own layout, then advance one byte past it.
fn lsi_resume_key(
    key_item: &Item,
    base: &TableSchema,
    idx: &IndexDef,
) -> Result<Vec<u8>, WireError> {
    let missing =
        |attr: &str| WireError::validation(format!("ExclusiveStartKey missing attribute `{attr}`"));
    let sort_attr = idx.sort_attribute.as_ref().ok_or_else(|| {
        WireError::validation(format!("index `{}` has no sort attribute", idx.name))
    })?;
    let alt_sort = key_item.get(sort_attr).ok_or_else(|| missing(sort_attr))?;
    let base_pk = key_item
        .get(&base.partition_key)
        .ok_or_else(|| missing(&base.partition_key))?;
    let base_sk = base.sort_key.as_ref().and_then(|sk| key_item.get(sk));
    let mut after = token_prefixed(
        base_pk,
        &dynamo_index::lsi_row_key(base_pk, &idx.name, alt_sort, base_sk),
    );
    after.push(0x00);
    Ok(after)
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
    if let Some(sk) = &schema.sort_key
        && let Some(v) = item.get(sk)
    {
        key.insert(sk.clone(), v.clone());
    }
    Some(key)
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
///
/// `pub(crate)` because the admin bulk seeder (`admin::action_data_seed`, ADR
/// 0021) builds its rows through this exact function — seeded keys must match
/// what this edge computes byte-for-byte, or seeded items are unreachable via
/// `GetItem`/`Query`.
///
/// The result of [`kind_write_item_at_leader`] once the entry (or its plain
/// fallback) has actually landed — mirrors [`ClientResponse::KindWriteOk`]/
/// [`ConditionFailed`](ClientResponse::ConditionFailed) exactly, since those
/// two wire variants exist purely to carry this value across a forwarding
/// hop.
pub(crate) enum KindWriteOutcome {
    /// The write landed. `new: None` for a `Delete` op.
    Ok {
        old: Option<Item>,
        new: Option<Item>,
    },
    /// The caller's own `condition` did not match the leader's own read of
    /// the current item — no diff was ever computed, nothing was proposed.
    ConditionFailed,
}

/// **The evaluate-at-leader write path (ADR 0046 U3)** for `PutItem`/
/// `DeleteItem`/`UpdateItem` on an indexed or streamed table — replaces
/// `index_aware_write`'s edge-evaluated design, which had a real cross-node
/// race: two edge nodes writing the same item never contended on the same
/// **node-local** `ctx.data().rmw_lock`, so both could read → diff against
/// the same stale `old` and the loser's stale LSI row orphaned forever
/// (nothing reconciles a stale LSI row; only the GSI drain self-heals — see
/// [`kind_writes_for_item`]'s doc for why an LSI can even ride this
/// entry). Change-record `OLD_IMAGE` fidelity had the identical staleness.
///
/// This function always runs **on the tablet's own leader** — called
/// in-process by `ClientCtx::cp_kind_write_item`'s `Local` branch (this
/// node hosts the leader) or by `ClientCtx::cp_serve_forwarded`'s
/// `KindWriteItem` arm (a forwarded hop already landed on the leader's own
/// node). Every write of this item, from whichever edge node received the
/// client request, therefore reaches this same function on this same node —
/// which is what makes locking `ctx.data().rmw_lock` **here**, instead of at
/// the edge, actually serialize concurrent writes of one item rather than
/// merely of one item *observed by one node*.
///
/// Reads its own `old` via `ClientCtx::cp_get_local_resolving` (the
/// identical primitive `cp_serve_forwarded`'s own `Get` arm uses) rather
/// than trusting anything the caller computed, evaluates `condition`
/// against it (a mismatch short-circuits to `ConditionFailed` before any
/// diff is ever computed — no read-modify-write hazard to consider, since
/// nothing has been written yet), computes `new` from `op` (an `Update`
/// applies `actions` to `old` — or `key_item` on an upsert-from-absent,
/// matching `UpdateItem`'s existing upsert contract), then defers to
/// [`kind_writes_for_item`] for the actual LSI/change-log diff — unchanged
/// logic, just moved onto the leader. A `None` result (this item's table
/// lost its last index/stream in the gap between routing and evaluation)
/// falls back to a plain leader-local write, mirroring
/// `index_aware_write`'s own `None` fallback exactly; that fallback carries
/// no OCC seatbelt (`cp_put_local`'s plain `Put` has no `conditions`
/// mechanism) — the identical, already-documented gap plain (unindexed,
/// unstreamed) tables have always had (ADR 0046 §2's named follow-up),
/// unaffected by this change either way.
///
/// **The OCC seatbelt** (ADR 0046, the PR1 `KindBatch.conditions` field):
/// the `KindBatch` proposed below carries `conditions: vec![(base_key,
/// raw_old)]` — the exact raw bytes this function's own read just observed,
/// compared byte-for-byte at apply. `rmw_lock` above already serializes
/// every write of this item that goes through *this* function, but a
/// `txn_resolver_loop` recovery push resolving a transaction's intent on
/// this same key never takes that lock — unreachable today (transactions
/// are rejected outright on an indexed/streamed table) but real the moment
/// a future transaction stack lifts that restriction, and this field is
/// exactly the mechanism that stack will also need. A failed seatbelt
/// no-ops the whole `KindBatch` silently, indistinguishable from a fence
/// miss — `ClientCtx::cp_kind_local`'s own probe-poll times out with the
/// same generic error every other silent no-op produces (deliberately no
/// new outcome channel here either, matching `KvCommand::KindBatch`'s own
/// documented choice).
#[allow(clippy::too_many_arguments)] // one item write's full identity + before/after
pub(crate) async fn kind_write_item_at_leader(
    ctx: &ClientCtx,
    leader: &CpGroup,
    meta: &Metadata,
    table: &str,
    pk: &AttributeValue,
    sk: Option<&AttributeValue>,
    op: KindWriteOp,
    condition: Option<&ConditionExpression>,
) -> Result<KindWriteOutcome, WireError> {
    let base_key = item_key(pk, sk);
    let _rmw = ctx.data().rmw_lock.lock().await;
    let raw_old = ctx
        .cp_get_local_resolving(leader, &base_key)
        .await
        .map_err(|e| internal(&format!("leader-side old-image read failed: {e}")))?;
    let old = match &raw_old {
        Some(bytes) => wire::decode_stored_item(bytes)?,
        None => None,
    };
    if let Some(cond) = condition
        && !cond.evaluate(old.as_ref())
    {
        return Ok(KindWriteOutcome::ConditionFailed);
    }
    let new = match &op {
        KindWriteOp::Put(item) => Some(item.clone()),
        KindWriteOp::Delete => None,
        KindWriteOp::Update { key_item, actions } => {
            let base = old.clone().unwrap_or_else(|| key_item.clone());
            Some(wire::apply_update(base, actions))
        }
    };
    let value = match &new {
        Some(item) => wire::encode_stored_item(item),
        None => wire::encode_tombstone(),
    };
    let (writes, change_log) = kind_writes_for_item(
        meta,
        table,
        pk,
        sk,
        &base_key,
        value.clone(),
        old.as_ref(),
        new.as_ref(),
    );
    let seatbelt = vec![(base_key, raw_old)];
    ClientCtx::cp_kind_local(leader, writes, vec![change_log], seatbelt)
        .await
        .map_err(|e| internal(&format!("index-maintaining write failed: {e}")))?;
    Ok(KindWriteOutcome::Ok { old, new })
}

/// One entry of a multi-kind atomic batch: `(row kind, key, value-or-tombstone)`.
type KindWrite = (u8, Vec<u8>, Option<Vec<u8>>);

/// A change-log record to append in the same entry: `(key prefix, encoded
/// record)`. The commit-timestamp suffix is added at apply (ADR 0041 §4a).
type ChangeLog = (Vec<u8>, Vec<u8>);

/// Everything one item write commits beyond a plain base-row put: the
/// multi-kind writes and the change-log record that accompanies them.
type IndexedWrite = (Vec<KindWrite>, ChangeLog);

/// The result of [`eval_kind_txn_write`]: everything `ClientCtx::
/// txn_stage_local` needs to build the [`animus_cp_data::TxnWrite`] a
/// kind-write-path item's transactional write stages.
pub(crate) struct KindTxnWriteEval {
    /// The item's own base key — `TxnWrite::key`.
    pub(crate) key: Vec<u8>,
    /// The encoded new item, or the Dynamo-level tombstone marker for a
    /// delete (never the engine's own `None`/tombstone — see
    /// `kind_write_item_at_leader`'s identical convention) — `TxnWrite::value`.
    pub(crate) value: Vec<u8>,
    /// The raw bytes this evaluation's own old-image read observed (`None`
    /// if absent) — used to build the mandatory own-key OCC `conditions`
    /// entry (ADR 0046 Fork C1) the caller adds alongside this write.
    pub(crate) raw_old: Option<Vec<u8>>,
    /// The ADR 0049 §3 stage-marker `(change_key_prefix, record)` pair —
    /// `TxnWrite::stage_marker`, written by `TxnStage`'s apply arm at the
    /// stage entry's own HLC (see [`stage_marker_change_log`]).
    pub(crate) stage_marker: (Vec<u8>, Vec<u8>),
    /// The derived kind-scope writes (LSI rows), EXCLUDING the base row
    /// itself (that's `key`/`value` above) — `TxnWrite::kind_writes`. Empty
    /// if `table` lost its last index/stream in the gap between routing and
    /// evaluation (mirrors `kind_write_item_at_leader`'s own `None`
    /// fallback — still correct, just no longer index/stream-maintaining).
    pub(crate) kind_writes: Vec<KindWrite>,
    /// The change-log record to materialize alongside `kind_writes` at
    /// resolve — `TxnWrite::change_log`.
    pub(crate) change_log: Option<ChangeLog>,
}

/// **ADR 0046 U3, extended to the transactional path (`TxnStage` kind-writes
/// stack PR2)**: evaluate one item's write **at the tablet's own leader**,
/// the identical read → evaluate-`condition` → diff span
/// [`kind_write_item_at_leader`] runs for the ordinary write path — but
/// returning the diff instead of proposing it. The actual stage propose
/// happens immediately afterward, in the SAME `TxnPrepare` call this runs
/// inside of (`ClientCtx::txn_stage_local`), under the identical
/// `ctx.data().rmw_lock` `kind_write_item_at_leader` takes — so every write
/// of this item, transactional or not, from whichever edge node received
/// it, still funnels through one lock on one node (the race U3 exists to
/// close).
///
/// Returns `Ok(None)` for a condition mismatch (no diff computed, mirroring
/// [`KindWriteOutcome::ConditionFailed`]); `Ok(Some(..))` otherwise.
#[allow(clippy::too_many_arguments)] // one item write's full identity + before/after
pub(crate) async fn eval_kind_txn_write(
    ctx: &ClientCtx,
    leader: &CpGroup,
    meta: &Metadata,
    table: &str,
    pk: &AttributeValue,
    sk: Option<&AttributeValue>,
    op: &KindWriteOp,
    condition: Option<&ConditionExpression>,
) -> Result<Option<KindTxnWriteEval>, WireError> {
    let base_key = item_key(pk, sk);
    let raw_old = ctx
        .cp_get_local_resolving(leader, &base_key)
        .await
        .map_err(|e| internal(&format!("leader-side old-image read failed: {e}")))?;
    let old = match &raw_old {
        Some(bytes) => wire::decode_stored_item(bytes)?,
        None => None,
    };
    if let Some(cond) = condition
        && !cond.evaluate(old.as_ref())
    {
        return Ok(None);
    }
    let new = match op {
        KindWriteOp::Put(item) => Some(item.clone()),
        KindWriteOp::Delete => None,
        KindWriteOp::Update { key_item, actions } => {
            let base = old.clone().unwrap_or_else(|| key_item.clone());
            Some(wire::apply_update(base, actions))
        }
    };
    let value = match &new {
        Some(item) => wire::encode_stored_item(item),
        None => wire::encode_tombstone(),
    };
    let (writes, change_log) = kind_writes_for_item(
        meta,
        table,
        pk,
        sk,
        &base_key,
        value.clone(),
        old.as_ref(),
        new.as_ref(),
    );
    // The base row itself rides as `TxnWrite::key`/`value`, not inside
    // `kind_writes` — strip it out (it's always `writes[0]` by
    // `kind_writes_for_item`'s own construction).
    let (kind_writes, change_log) = (
        writes
            .into_iter()
            .filter(|(kind, _, _)| *kind != KIND_BASE)
            .collect(),
        Some(change_log),
    );
    Ok(Some(KindTxnWriteEval {
        key: base_key,
        value,
        raw_old,
        kind_writes,
        change_log,
        stage_marker: item_stage_marker_change_log(pk, sk),
    }))
}

/// The data-plane key for a within-table key of `pk`'s partition: the ADR 0022
/// token prefix plus `within`. The token is over `escape(pk)`, exactly as
/// [`item_key`] computes it, so every row kind of one item lands in the same
/// tablet — which is what lets them commit atomically (ADR 0041 §2).
fn token_prefixed(pk: &AttributeValue, within: &[u8]) -> Vec<u8> {
    let mut key = partition_token(&storage_key(pk, None)).to_vec();
    key.extend_from_slice(within);
    key
}

/// The attributes an index row carries, per its declared projection.
/// `None` means "every attribute" (`ALL`).
pub(crate) fn projected_item(item: &Item, base: &TableSchema, idx: &IndexDef) -> Item {
    let keep: Option<Vec<&str>> = match &idx.projection {
        CtlProjection::All => None,
        CtlProjection::KeysOnly => Some(Vec::new()),
        CtlProjection::Include(extra) => Some(extra.iter().map(String::as_str).collect()),
    };
    let Some(extra) = keep else {
        return item.clone();
    };
    // The key attributes are always present, whatever the projection: the base
    // table's keys (so the row can name its item) plus this index's own.
    let mut names: Vec<&str> = vec![base.partition_key.as_str()];
    if let Some(sk) = &base.sort_key {
        names.push(sk.as_str());
    }
    names.push(idx.hash_attribute.as_str());
    if let Some(sort) = &idx.sort_attribute {
        names.push(sort.as_str());
    }
    names.extend(extra);
    item.iter()
        .filter(|(name, _)| names.contains(&name.as_str()))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// The **fast arm** of the ADR 0049 universal kind-write path: a
/// `Put`/`Delete` on a table whose change records carry no images
/// (`table_change_records_carry_images` is `false`), with no condition and
/// no old-image echo, needs nothing read before it commits — no LSI diff,
/// no image, no evaluation. So the *edge* builds the whole `KindBatch`
/// (base row + image-less marker record) and proposes it routed
/// (`ClientCtx::cp_kind_write_raw`), skipping the evaluate-at-leader funnel
/// entirely: no leader-side pre-read, and — critically — no node-global
/// `rmw_lock` held across a full commit round trip. That lock-across-commit
/// is what made routing plain-table `BatchWriteItem`s through the funnel
/// serialize N items into N sequential WAL-fsync round trips, resurrecting
/// the exact disk-starvation failure `tests/backfill_seeder.rs`'s
/// population comment documents (`Backend(..)` panics under three replicas'
/// group commits); fast-arm proposals carry no lock and no read, so
/// concurrent items amortize through Raft group commit like the old
/// `cp_batch_write` path did. Two concurrent unconditional writes of one
/// item are simply log-ordered — the identical semantics the plain path
/// always had. Anything needing evaluation (a condition, an `Update`'s RMW,
/// an `ALL_OLD` echo, or an images-carrying table) keeps the ADR 0046 U3
/// funnel.
async fn fast_marker_write(
    ctx: &ClientCtx,
    table: &str,
    pk: &AttributeValue,
    sk: Option<&AttributeValue>,
    value: Vec<u8>,
) -> Result<(), WireError> {
    // Auto-provision the table's tablet on first write (ADR 0023), exactly
    // as `cp_kind_write_item`/`cp_batch_write` do — `cp_kind_write_raw`
    // itself never provisions (its other callers only ever write to tablets
    // that exist).
    if !ctx.effective_metadata().has_table_tablet(table) {
        ctx.provision_tablet(table)
            .await
            .map_err(|e| internal(&e))?;
    }
    let base_key = item_key(pk, sk);
    ctx.cp_kind_write_raw(
        table,
        vec![(animus_cp_data::KIND_BASE, base_key, Some(value))],
        vec![item_marker_change_log(pk, sk)],
    )
    .await
    .map_err(|e| internal(&e))
}

/// The per-tablet single-entry batch commit for a **marker** table's rows
/// (ADR 0049 §1/§4): groups `(pk, sk, stored value)` rows by tablet and
/// commits each tablet's group as ONE `KindBatch` Raft entry carrying every
/// base row + every marker record — the entry granularity the plain
/// `cp_batch_write` path had (one entry, one WAL record, one apply per
/// tablet; see `BatchWriteItem`'s arm for the measured regression a
/// per-item shape caused). A thin item-shaped wrapper over
/// [`marker_batch_write_raw`] (the ONE grouping implementation, below),
/// used by `BatchWriteItem`'s marker arm and the admin seeder
/// (`admin::action_data_seed`) — never a second grouping copy. Only ever
/// call it for a table whose records carry no images
/// (`!table_change_records_carry_images`); an images table's batch must go
/// through the per-item evaluate-at-leader funnel instead.
pub(crate) async fn marker_batch_write(
    ctx: &ClientCtx,
    table: &str,
    rows: Vec<(AttributeValue, Option<AttributeValue>, Vec<u8>)>,
) -> Result<(), String> {
    let raw = rows
        .into_iter()
        .map(|(pk, sk, value)| {
            let marker = item_marker_change_log(&pk, sk.as_ref());
            (item_key(&pk, sk.as_ref()), Some(value), marker)
        })
        .collect();
    marker_batch_write_raw(ctx, table, raw, true).await
}

/// One [`marker_batch_write_raw`] row: `(base key, value-or-engine-delete,
/// the row's marker record pair)`.
pub(crate) type MarkerRow = (Vec<u8>, Option<Vec<u8>>, (Vec<u8>, Vec<u8>));

/// The raw-key core of [`marker_batch_write`] — the ONE per-tablet grouping
/// implementation, shared with the plain client-protocol arms
/// (`ClientRequest::Put`/`PutBatch`/`Delete`, ADR 0049 Train A rung 5),
/// whose keys are arbitrary caller bytes with no `pk`/`sk` decomposition
/// (their markers use the full-key-as-prefix convention, like CQL's and the
/// GSI drain's — see [`marker_change_log`]'s doc). A `None` value is a
/// genuine engine delete (`KindBatch`'s real tombstone), matching the old
/// `cp_delete` semantics. `provision_if_absent` mirrors the primitives each
/// caller replaces: the put paths auto-provision (ADR 0023), a bare delete
/// never did (deleting from a table nothing provisioned must not conjure an
/// empty tablet).
pub(crate) async fn marker_batch_write_raw(
    ctx: &ClientCtx,
    table: &str,
    rows: Vec<MarkerRow>,
    provision_if_absent: bool,
) -> Result<(), String> {
    if provision_if_absent && !ctx.effective_metadata().has_table_tablet(table) {
        ctx.provision_tablet(table).await?;
    }
    let route_meta = ctx.effective_metadata();
    // One `(kind writes, marker records)` pair per tablet — the per-tablet
    // single-entry batches proposed below.
    type TabletBatch = (Vec<(u8, Vec<u8>, Option<Vec<u8>>)>, Vec<(Vec<u8>, Vec<u8>)>);
    let mut by_tablet: std::collections::BTreeMap<Option<animus_tablet::TabletId>, TabletBatch> =
        std::collections::BTreeMap::new();
    for (base_key, value, marker) in rows {
        let tablet =
            crate::topology::tablet_for_key(route_meta.tablets_for_table(table), &base_key);
        let entry = by_tablet.entry(tablet).or_default();
        entry.1.push(marker);
        entry.0.push((animus_cp_data::KIND_BASE, base_key, value));
    }
    for (tablet, (writes, markers)) in by_tablet {
        if tablet.is_none() {
            // An unroutable key (a racing split/provision moved the map
            // under this snapshot): surface the house retryable error — the
            // caller's own retry re-resolves against fresher metadata, the
            // same shape every routed write already has.
            return Err(format!("no tablet for a batch key of table {table}; retry"));
        }
        ctx.cp_kind_write_raw(table, writes, markers).await?;
    }
    Ok(())
}

/// The image-less **marker record** one mutation of a no-images Dynamo table
/// leaves (ADR 0049 §1), as the `(key prefix, encoded record)` pair
/// `KindBatch`'s `change_log` carries — the key's HLC suffix is completed at
/// apply. A thin item-shaped wrapper over `marker_change_log` (the one shared
/// constructor, below), used by the single-item fast arm above and
/// `BatchWriteItem`'s per-tablet batch arm.
fn item_marker_change_log(pk: &AttributeValue, sk: Option<&AttributeValue>) -> (Vec<u8>, Vec<u8>) {
    let base_sk = storage_key(pk, sk)[storage_key(pk, None).len()..].to_vec();
    marker_change_log(
        &token_prefixed(pk, &dynamo_index::change_prefix(pk)),
        base_sk,
    )
}

/// The item-shaped wrapper over [`stage_marker_change_log`] (ADR 0049 §3) —
/// `item_marker_change_log`'s stage-marker sibling, used by
/// [`eval_kind_txn_write`] for every transactional write's
/// `TxnWrite::stage_marker`.
fn item_stage_marker_change_log(
    pk: &AttributeValue,
    sk: Option<&AttributeValue>,
) -> (Vec<u8>, Vec<u8>) {
    let base_sk = storage_key(pk, sk)[storage_key(pk, None).len()..].to_vec();
    stage_marker_change_log(
        &token_prefixed(pk, &dynamo_index::change_prefix(pk)),
        base_sk,
    )
}

/// The one construction site for an ADR 0049 §1 image-less **marker**
/// record's `(change_key_prefix, encoded_record)` pair, shared by every edge
/// that commits one (this file's fast arm above; the CQL edge's partition
/// writes, `cql::kind_partition_write`; the GSI drain's hidden-index-table
/// row writes, `index_drain::reconcile_partition` — full row key as prefix,
/// empty `base_sk`, the CQL convention). `partition_prefix` is the change
/// key's apply-completed prefix — the base row's own partition-scoped key
/// bytes, token first, so the record lands in the same tablet as the base
/// row and sorts per-partition (Dynamo: `token(escape(pk)) || escape(pk)`;
/// CQL: `token(pk_bytes) || pk_bytes`, its `data_key` shape — the two edges'
/// escaping conventions differ, which is exactly why the prefix is an
/// argument rather than derived here); apply appends `hlc::pack(ts)` (ADR
/// 0041 §4a). `base_sk` is the Dynamo sort-key suffix a consumer rebuilds an
/// item key from — empty for CQL, whose partition is one value with no sort
/// dimension.
pub(crate) fn marker_change_log(partition_prefix: &[u8], base_sk: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
    marker_record(partition_prefix, base_sk, false)
}

/// The **stage-marker** sibling of [`marker_change_log`] (ADR 0049 §3): the
/// image-less record `KvCommand::TxnStage`'s apply arm writes for the anchor
/// key it stages, so a change-log consumer re-reading dirty keys (ADR 0050's
/// split-build tail) observes a freshly staged intent envelope. Same key
/// prefix convention, same apply-completed HLC; distinguished only by
/// `ChangeRecord::staged` (and hidden from consumers by the same
/// `consumer_hidden` predicate `marker` already is). Built at the edge —
/// never at apply — because record bytes are opaque to `animus-cp-data`
/// (ADR 0043's layering rule); it rides `TxnWrite::stage_marker`.
pub(crate) fn stage_marker_change_log(
    partition_prefix: &[u8],
    base_sk: Vec<u8>,
) -> (Vec<u8>, Vec<u8>) {
    marker_record(partition_prefix, base_sk, true)
}

/// The one construction core both marker shapes share — never two literals
/// that could drift (the same rule that keeps `marker_change_log` itself
/// the single site for every edge).
fn marker_record(partition_prefix: &[u8], base_sk: Vec<u8>, staged: bool) -> (Vec<u8>, Vec<u8>) {
    let record = ChangeRecord {
        base_sk,
        old_image: None,
        new_image: None,
        seeded: false,
        marker: true,
        staged,
    };
    (partition_prefix.to_vec(), record.encode())
}

/// Whether `table`'s change records carry the old/new item images — `true`
/// when something consumes them: a stream (wire-visible events need images
/// for every view type's read-time projection) or at least one secondary
/// index (an LSI diff and the drain's fidelity contract predate ADR 0049).
/// A table with neither writes an image-less **marker** record
/// (`ChangeRecord::marker`, ADR 0049 §1): the dirty-key signal change-log
/// consumers need, at a fixed few tens of bytes per write instead of two
/// item images. This is exactly the predicate the old kind-path gate
/// used to be, renamed to what it now actually decides — the record's
/// *shape*, never whether one exists ("images follow the stream/index
/// declarations; the record itself follows nothing — it always exists").
pub(crate) fn table_change_records_carry_images(meta: &Metadata, table: &str) -> bool {
    !meta.table_indexes(table).is_empty() || meta.table_stream(table).is_some()
}

/// Whether `cp_txn` must **await** its post-commit resolve under the ADR
/// 0046 D1 bounded budget (`TXN_RESOLVE_ALL_AWAIT_BUDGET`,
/// `resolve_all_parallel`) rather than the original fire-and-forget
/// sequential spawn: only when the transaction stages a pending kind write
/// against a table whose change records **carry images** — an index or a
/// stream. D1's rationale is consumer visibility (LSI rows and the
/// GSI/stream change record only exist from resolve onward, so an
/// ack-then-async window would leave a committed write transiently absent
/// from its own index/stream); a marker-only table has no such consumer, so
/// nothing observable rides on resolve latency and the proven-stable
/// sequential spawn applies. This scoping is **load-bearing**, not a
/// latency nicety: keying it on the old kind-path gate (constant-
/// true since ADR 0049) silently universalized the awaited-parallel
/// configuration onto every plain-table transaction — the exact
/// configuration `resolve_all_parallel`'s own comment records as
/// reproduced-red on `dynamo_txn.rs`'s torn-pair hard-gate test, which
/// promptly went intermittently red again (a budget-expired ack racing the
/// writer's next same-key stage into `TXN_STAGE_PUSH_ATTEMPTS` exhaustion).
pub(crate) fn txn_resolve_awaited(meta: &Metadata, writes: &[crate::TxnTableWrite]) -> bool {
    writes
        .iter()
        .any(|w| w.pending.is_some() && table_change_records_carry_images(meta, &w.table))
}

#[cfg(test)]
mod txn_resolve_awaited_tests {
    use animus_control::{ApplyOutcome, MetaCommand, Metadata, StreamSpec, StreamViewType};

    use super::txn_resolve_awaited;
    use crate::TxnTableWrite;

    fn meta_with_tables() -> Metadata {
        let mut m = Metadata::default();
        for table in ["plain", "streamed"] {
            assert!(matches!(
                m.apply(&MetaCommand::CreateTableSchema {
                    table: table.to_owned(),
                    schema: animus_control::TableSchema::simple(
                        "pk",
                        animus_control::ColumnType::String
                    ),
                }),
                ApplyOutcome::Applied
            ));
        }
        assert!(matches!(
            m.apply(&MetaCommand::SetTableStream {
                table: "streamed".to_owned(),
                spec: Some(StreamSpec {
                    view_type: StreamViewType::NewAndOldImages,
                    label: "L1".to_owned(),
                }),
            }),
            ApplyOutcome::Applied
        ));
        m
    }

    fn pending_write(table: &str) -> TxnTableWrite {
        TxnTableWrite {
            table: table.to_owned(),
            key: vec![0u8; 8],
            value: None,
            pending: Some(crate::PendingKindWrite {
                pk: animus_dynamo::AttributeValue::S("p".to_owned()),
                sk: None,
                op: crate::KindWriteOp::Delete,
                condition: None,
            }),
        }
    }

    /// A transaction whose pending writes touch only marker tables (no
    /// index, no stream) keeps the original fire-and-forget sequential
    /// resolve — the proven-stable configuration the torn-pair hard gate
    /// pins; one images-carrying participant is what flips it to the D1
    /// awaited-parallel branch.
    #[test]
    fn marker_only_transactions_are_not_awaited_images_ones_are() {
        let meta = meta_with_tables();
        assert!(!txn_resolve_awaited(&meta, &[pending_write("plain")]));
        assert!(txn_resolve_awaited(&meta, &[pending_write("streamed")]));
        assert!(txn_resolve_awaited(
            &meta,
            &[pending_write("plain"), pending_write("streamed")]
        ));
        // A plain (already-valued, no-pending) write never awaits, whatever
        // its table.
        let plain_valued = TxnTableWrite {
            table: "streamed".to_owned(),
            key: vec![0u8; 8],
            value: Some(vec![1]),
            pending: None,
        };
        assert!(!txn_resolve_awaited(&meta, &[plain_valued]));
    }
}

/// Build the multi-kind atomic batch one item write commits (ADR 0041 §2/§4;
/// ADR 0042 §1 for the streamed case): the base row, this item's LSI rows
/// (adding the new, removing whatever the previous value occupied), and a
/// change-log record.
///
/// **GSI rows are deliberately absent.** A GSI hashes by its own key, so its
/// rows live in a different table's tablets and cannot join this entry; the
/// drain materializes them asynchronously from the change record this writes.
/// An LSI *can* be here precisely because it hashes by the base partition key.
///
/// Returns `Some` for every table since ADR 0049 (`table_takes_kind_write_
/// path` is constant-true; the `None` arm below is kept only until Train
/// A's deletion rung). **A streamed-but-unindexed table**: `indexes` is
/// empty, so the LSI loop below is simply a no-op, and the entry commits
/// exactly base row + change record — the change record is what the sealer
/// reads (ADR 0043 §A1), carrying both images (a stream's
/// `NEW_AND_OLD_IMAGES` fidelity needs the same old image an LSI diff
/// needs). **A table with no stream and no index** commits base row + an
/// image-less *marker* record instead (ADR 0049 §1,
/// `table_change_records_carry_images`) — same key, same apply-time HLC
/// completion, no images.
#[allow(clippy::too_many_arguments)] // one item write's full identity + before/after
fn kind_writes_for_item(
    meta: &Metadata,
    table: &str,
    pk: &AttributeValue,
    sk: Option<&AttributeValue>,
    base_key: &[u8],
    base_value: Vec<u8>,
    old: Option<&Item>,
    new: Option<&Item>,
) -> IndexedWrite {
    let indexes = meta.table_indexes(table);
    let base = schema_for(meta, table);
    let base = &base;
    let mut writes: Vec<KindWrite> = vec![(KIND_BASE, base_key.to_vec(), Some(base_value))];

    for idx in indexes.iter().filter(|i| i.kind == IndexKind::Local) {
        let Some(sort_attr) = &idx.sort_attribute else {
            continue; // an LSI always declares one; a malformed def is skipped
        };
        let old_alt = old.and_then(|i| i.get(sort_attr));
        let new_alt = new.and_then(|i| i.get(sort_attr));
        // Remove the row the previous value occupied, unless it is the very row
        // the new value writes (an unchanged sort attribute) — deleting and
        // re-putting the same key in one entry would depend on ordering.
        if let Some(prev) = old_alt
            && old_alt != new_alt
        {
            writes.push((
                KIND_LSI,
                token_prefixed(pk, &dynamo_index::lsi_row_key(pk, &idx.name, prev, sk)),
                None,
            ));
        }
        if let Some(next) = new_alt {
            let item = new.expect("a new alt value implies a new item");
            writes.push((
                KIND_LSI,
                token_prefixed(pk, &dynamo_index::lsi_row_key(pk, &idx.name, next, sk)),
                Some(wire::encode_stored_item(&projected_item(item, base, idx))),
            ));
        }
    }

    // The sort key's raw bytes, derived through the public key codec rather than
    // `AttributeValue::key_bytes` (crate-private to `animus-dynamo`): the full
    // storage key minus the partition-key prefix is exactly that suffix.
    let base_sk = storage_key(pk, sk)[storage_key(pk, None).len()..].to_vec();
    // ADR 0049 §1: the record always exists; only its *shape* follows the
    // table's declarations. With a stream or an index the images ride along
    // (view-type projection is read-time; the drain/LSI fidelity contract
    // needs the old image); with neither, an image-less marker is the whole
    // record — the dirty-key signal change-log consumers re-read rows from.
    let carries_images = table_change_records_carry_images(meta, table);
    let record = ChangeRecord {
        base_sk,
        old_image: if carries_images { old.cloned() } else { None },
        new_image: if carries_images { new.cloned() } else { None },
        seeded: false,
        marker: !carries_images,
        staged: false,
    };
    let change_log = (
        token_prefixed(pk, &dynamo_index::change_prefix(pk)),
        record.encode(),
    );
    (writes, change_log)
}

pub(crate) fn item_key(pk: &AttributeValue, sk: Option<&AttributeValue>) -> Vec<u8> {
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

/// Linearizable CP read of `key`, returning the **raw stored bytes** (the
/// tagged envelope `quorum_read` decodes, or a `DeleteItem` tombstone
/// sentinel verbatim) — the building block for anything that needs to hand
/// the exact observed bytes onward (a `cp_txn` OCC precondition,
/// `TransactGetItems`'s quiescent read), not just the decoded [`Item`].
async fn raw_quorum_read(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    key: &[u8],
) -> Result<Option<Vec<u8>>, WireError> {
    // A table with no tablet has no data (ADR 0023) — read as absent without waiting
    // on routing for a tablet that does not exist. The gate short-circuits a
    // **linearizable** read, so it must not conclude "absent" from the (possibly
    // stale) request-entry snapshot: a concurrent first write may have provisioned
    // the tablet after this request began — under the RMW lock a conditional
    // writer's read *must* see it (two racing `attribute_not_exists` puts both
    // "succeeding" was the failure mode). Trust the snapshot on the hit path
    // (tablets are only removed by drop-table); re-check **live** on the miss.
    // Fresh, not `metadata(ctx)` (ADR 0035 PR1): this is the conditional-write
    // existence gate, which must not conclude "absent" from a growth-node
    // mirror that could still be a poll interval behind a concurrent writer's
    // just-committed provisioning.
    if !meta.has_table_tablet(table) && !metadata_fresh(ctx).await.has_table_tablet(table) {
        return Ok(None);
    }
    ctx.cp_read(table, key.to_vec())
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
    match raw_quorum_read(ctx, meta, table, key).await? {
        Some(bytes) => wire::decode_stored_item(&bytes),
        None => Ok(None),
    }
}

pub(crate) fn internal(message: &str) -> WireError {
    WireError {
        code: "InternalServerError",
        message: message.to_owned(),
    }
}

/// The streamed write-path regressions (ADR 0042 §1): whether a table with a
/// stream enabled — but no secondary index — takes the `kind_writes_for_item`
/// path and commits *exactly* base row + change record, never an LSI or
/// footprint row; and that an unstreamed, unindexed table still takes the
/// plain fast path (no change log at all). These need `CpGroup`'s private
/// kind-scan accessors (`pending_changes`/`local_scan_kind_bounded`) an
/// external `tests/` crate cannot reach — the same reason
/// `index_drain::gsi_drain_cursor_tests` lives in-crate.
#[cfg(test)]
mod stream_write_path_tests {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::time::Duration;

    use animus_cp_data::{KIND_FOOTPRINT, KIND_LSI};
    use animus_dynamo::ChangeRecord;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::{sleep, timeout};

    use crate::config::NodeRole;
    use crate::{
        ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, read_frame, run_node,
        write_frame,
    };

    fn free_addrs(count: usize) -> Vec<SocketAddr> {
        let ls: Vec<std::net::TcpListener> = (0..count)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        ls.iter().map(|l| l.local_addr().unwrap()).collect()
    }

    fn single_node_config() -> ClusterConfig {
        let addrs = free_addrs(6);
        ClusterConfig {
            nodes: vec![RoleAddrs {
                id: crate::config::node_id(0),
                role: NodeRole::Both,
                internal: addrs[0],
                client: addrs[1],
                dynamo: addrs[2],
                cql: addrs[3],
                admin: addrs[4],
                intra: addrs[5],
            }],
        }
    }

    async fn await_control_leader(node: &Node) {
        timeout(Duration::from_secs(10), async {
            loop {
                if node.is_control_leader() {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("node did not become control leader in time");
    }

    async fn single_node(dir: &Path) -> Node {
        let config = single_node_config();
        let node = run_node(&config, 0, dir.join("node-0"))
            .await
            .expect("bring up single node");
        await_control_leader(&node).await;
        node
    }

    /// Mirrors `index_drain::gsi_drain_cursor_tests`'s identical helper — a
    /// different compilation unit, so duplicated rather than shared.
    async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
        let mut s = TcpStream::connect(addr).await.expect("connect");
        let req = format!(
            "POST / HTTP/1.1\r\nHost: x\r\nX-Amz-Target: {target}\r\n\
             Connection: close\r\n\
             Content-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        s.write_all(req.as_bytes()).await.expect("write");
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.expect("read");
        let text = String::from_utf8_lossy(&buf).into_owned();
        let status = text
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        let body = text.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
        (status, body.to_owned())
    }

    /// One named counter off the public `GET /metrics` text export (shares
    /// the DynamoDB listener) — the trim-safe half of the marker-emission
    /// accounting (see the tests' own comments).
    async fn metrics_value(addr: SocketAddr, name: &str) -> u64 {
        let mut s = TcpStream::connect(addr).await.expect("connect");
        s.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .expect("write");
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.expect("read");
        let text = String::from_utf8_lossy(&buf);
        text.lines()
            .find_map(|l| l.strip_prefix(&format!("{name} ")))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or_else(|| panic!("metric {name} absent from /metrics"))
    }

    async fn create_streamed_table(addr: SocketAddr, table: &str) {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.CreateTable",
            &format!(
                r#"{{"TableName":"{table}",
                    "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                    "StreamSpecification":{{"StreamEnabled":true,
                        "StreamViewType":"KEYS_ONLY"}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");
    }

    async fn await_group(node: &Node, table: &str) -> crate::CpGroup {
        timeout(Duration::from_secs(10), async {
            loop {
                let meta = node.metadata();
                if let Some((&tablet, _)) = meta.tablets_for_table(table).next()
                    && let Some(group) = node.edge.local_cp(tablet)
                {
                    return group;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("table's tablet never hosted locally")
    }

    /// A streamed-but-unindexed table's `PutItem`/`UpdateItem`/`DeleteItem`
    /// each commit exactly base row + change record (ADR 0042 §1) — never an
    /// LSI or footprint row, since `indexes` is empty on this table and the
    /// kind-write gate is pulled open only by the stream.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streamed_unindexed_table_writes_base_and_change_only() {
        let dir = tempfile::TempDir::new().unwrap();
        let node = single_node(dir.path()).await;
        create_streamed_table(node.dynamo_addr(), "s1").await;
        let group = await_group(&node, "s1").await;
        assert_eq!(group.pending_changes().await.len(), 0);

        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.PutItem",
            r#"{"TableName":"s1","Item":{"id":{"S":"a"},"n":{"N":"1"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "PutItem failed: {body}");
        assert_eq!(
            group.pending_changes().await.len(),
            1,
            "PutItem must leave exactly one change record"
        );
        assert!(
            group
                .local_scan_kind_bounded(KIND_LSI, &[], None)
                .await
                .is_empty(),
            "an unindexed table must never write an LSI row, streamed or not"
        );
        assert!(
            group
                .local_scan_kind_bounded(KIND_FOOTPRINT, &[], None)
                .await
                .is_empty(),
            "an unindexed table must never write a footprint row (that's GSI-only)"
        );
        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.GetItem",
            r#"{"TableName":"s1","Key":{"id":{"S":"a"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "GetItem failed: {body}");
        assert!(body.contains("\"n\""), "base row must be readable: {body}");

        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.UpdateItem",
            r#"{"TableName":"s1","Key":{"id":{"S":"a"}},
                "UpdateExpression":"SET n = :v",
                "ExpressionAttributeValues":{":v":{"N":"2"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "UpdateItem failed: {body}");
        assert_eq!(group.pending_changes().await.len(), 2);

        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.DeleteItem",
            r#"{"TableName":"s1","Key":{"id":{"S":"a"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "DeleteItem failed: {body}");
        assert_eq!(group.pending_changes().await.len(), 3);
        assert!(
            group
                .local_scan_kind_bounded(KIND_LSI, &[], None)
                .await
                .is_empty()
        );
        assert!(
            group
                .local_scan_kind_bounded(KIND_FOOTPRINT, &[], None)
                .await
                .is_empty()
        );
    }

    /// ADR 0049 §1 (inverts this test's pre-0049 ancestor, which asserted a
    /// plain table pays *nothing* for the change log): a table with no
    /// stream and no index now emits exactly one **image-less marker
    /// record** per mutation — `marker: true`, `seeded: false`, no images,
    /// The **entry-granularity contract** (ADR 0049 §1 "the marker rides
    /// the same entry — no extra fsync", restored by the Train A rung-1
    /// fixup): a marker-table `BatchWriteItem` commits **one `KindBatch`
    /// Raft entry per tablet**, never one per item. Proven without touching
    /// Raft internals: every change record in one entry is completed with
    /// that entry's own apply timestamp, so on a single-tablet table all N
    /// markers of one batch must share exactly ONE distinct HLC suffix —
    /// the per-item shape this replaces produced N distinct ones (one
    /// entry each), which is how the `backfill_seeder` populate-then-
    /// backfill regression got in.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_write_on_a_marker_table_commits_one_entry_per_tablet() {
        let dir = tempfile::TempDir::new().unwrap();
        let node = single_node(dir.path()).await;
        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"mb",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");
        let group = await_group(&node, "mb").await;

        let puts: Vec<String> = (0..40)
            .map(|i| format!(r#"{{"PutRequest":{{"Item":{{"id":{{"S":"k{i:03}"}}}}}}}}"#))
            .collect();
        let body_json = format!(r#"{{"RequestItems":{{"mb":[{}]}}}}"#, puts.join(","));
        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.BatchWriteItem",
            &body_json,
        )
        .await;
        assert_eq!(status, 200, "BatchWriteItem failed: {body}");

        // Trim-safe accounting (ADR 0049 rung 4, see
        // `plain_table_writes_emit_one_marker_record_each`): live + trimmed
        // markers must total the batch. The one-entry property is asserted
        // over whatever is still live — every live marker of this batch must
        // share ONE apply HLC (the per-item regression shape shows up as
        // several distinct HLCs the instant any two markers survive a tick,
        // which under a 40-item batch they essentially always do; a
        // fully-trimmed-before-observation batch skips only this half, never
        // the count).
        let records = group.pending_changes().await;
        let trimmed = metrics_value(node.dynamo_addr(), "change_log_trimmed_total").await;
        assert_eq!(
            records.len() as u64 + trimmed,
            40,
            "exactly one marker per batched item (live {} + trimmed {trimmed})",
            records.len()
        );
        let distinct_hlcs: std::collections::BTreeSet<u64> = records
            .iter()
            .map(|(key, _)| u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap()))
            .collect();
        assert!(
            distinct_hlcs.len() <= 1,
            "one tablet's whole batch must ride ONE Raft entry (one shared \
             apply HLC) — {} distinct HLCs means that many entries, the \
             per-item regression shape",
            distinct_hlcs.len()
        );
    }

    /// The ADR 0049 §5 bench gate's harness — `#[ignore]`d (a wall-clock
    /// measurement, not an assertion; run explicitly with
    /// `cargo test -p animusd --lib bench_plain_table_put -- --ignored
    /// --nocapture`). Sequential `PutItem`s on a plain table through the
    /// real Dynamo edge on one node: the exact path whose per-write cost the
    /// universal kind path (base row + image-less marker in the same entry)
    /// changes. Compare medians of ≥3 runs on the pre-train baseline
    /// (749b4b8) vs the train tip; the recorded numbers live in ADR 0049's
    /// as-built amendment.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "bench gate: wall-clock measurement, run explicitly"]
    async fn bench_plain_table_put_wall_clock() {
        const N: usize = 200;
        let dir = tempfile::TempDir::new().unwrap();
        let node = single_node(dir.path()).await;
        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"benchp",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");
        // One warm-up write outside the timed window (tablet formation +
        // first-leader election are not the write path under test).
        let (status, _) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.PutItem",
            r#"{"TableName":"benchp","Item":{"id":{"S":"warmup"}}}"#,
        )
        .await;
        assert_eq!(status, 200);

        let started = std::time::Instant::now();
        for i in 0..N {
            let (status, body) = dynamo(
                node.dynamo_addr(),
                "DynamoDB_20120810.PutItem",
                &format!(
                    r#"{{"TableName":"benchp","Item":{{"id":{{"S":"k{i:05}"}},"n":{{"N":"{i}"}}}}}}"#
                ),
            )
            .await;
            assert_eq!(status, 200, "PutItem {i} failed: {body}");
        }
        let elapsed = started.elapsed();
        println!(
            "bench_plain_table_put_wall_clock: {N} sequential PutItems in {:?} ({:.2} ms/op)",
            elapsed,
            elapsed.as_secs_f64() * 1000.0 / N as f64
        );
    }

    /// Send one plain-client-protocol request and return the reply.
    async fn raw_request(addr: SocketAddr, request: &ClientRequest) -> ClientResponse {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        write_frame(&mut stream, request).await.expect("send");
        read_frame(&mut stream)
            .await
            .expect("read reply")
            .expect("a reply")
    }

    /// ADR 0049 Train A rung 5: the plain client protocol
    /// (`ClientRequest::Put`/`PutBatch`/`Delete` — `animus-cli put`'s wire
    /// surface) is a real write path, so its mutations ride the kind path
    /// and each leaves exactly one image-less marker (full-raw-key-as-prefix
    /// convention). Red on the pre-rung arms (plain
    /// `cp_put`/`cp_batch_write`/`cp_delete`): zero records, ever. The
    /// delete arm also stays a genuine engine delete, and never
    /// auto-provisions a table nothing created.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn raw_client_protocol_writes_emit_one_marker_each() {
        let dir = tempfile::TempDir::new().unwrap();
        let node = single_node(dir.path()).await;

        // Delete against a table nothing provisioned: no auto-provision
        // (the old `cp_delete` contract) — it must fail its routing wait,
        // not conjure an empty tablet.
        let resp = raw_request(
            node.client_addr(),
            &ClientRequest::Delete {
                table: "never-created".into(),
                key: b"k".to_vec(),
            },
        )
        .await;
        assert!(
            matches!(resp, ClientResponse::Error(_)),
            "a bare delete must not conjure a tablet: {resp:?}"
        );
        assert!(
            node.metadata().tablets_for_table("never-created").count() == 0,
            "delete auto-provisioned a tablet"
        );

        // Put auto-provisions (the old `cp_put` contract), then the batch
        // and the delete land on the same table.
        let put = raw_request(
            node.client_addr(),
            &ClientRequest::Put {
                table: "rawt".into(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            },
        )
        .await;
        assert!(matches!(put, ClientResponse::PutOk), "put failed: {put:?}");
        let batch = raw_request(
            node.client_addr(),
            &ClientRequest::PutBatch {
                table: "rawt".into(),
                entries: vec![
                    (b"k2".to_vec(), b"v2".to_vec()),
                    (b"k3".to_vec(), b"v3".to_vec()),
                ],
            },
        )
        .await;
        assert!(
            matches!(batch, ClientResponse::PutOk),
            "batch failed: {batch:?}"
        );
        let del = raw_request(
            node.client_addr(),
            &ClientRequest::Delete {
                table: "rawt".into(),
                key: b"k1".to_vec(),
            },
        )
        .await;
        assert!(matches!(del, ClientResponse::PutOk), "del failed: {del:?}");

        // Reads confirm the base effects: k2 present, k1 genuinely deleted.
        let got = raw_request(
            node.client_addr(),
            &ClientRequest::Get {
                table: "rawt".into(),
                key: b"k2".to_vec(),
            },
        )
        .await;
        assert!(
            matches!(&got, ClientResponse::Value(Some(v)) if v == b"v2"),
            "get k2: {got:?}"
        );
        let gone = raw_request(
            node.client_addr(),
            &ClientRequest::Get {
                table: "rawt".into(),
                key: b"k1".to_vec(),
            },
        )
        .await;
        assert!(
            matches!(gone, ClientResponse::Value(None)),
            "k1 must be deleted: {gone:?}"
        );

        // Trim-safe accounting (ADR 0049 rung 4's union rule): one marker
        // per mutation — 4 total (put + 2 batched + delete), live or
        // already trimmed.
        let group = await_group(&node, "rawt").await;
        let records = group.pending_changes().await;
        let trimmed = metrics_value(node.dynamo_addr(), "change_log_trimmed_total").await;
        assert_eq!(
            records.len() as u64 + trimmed,
            4,
            "exactly one marker per raw-protocol mutation (live {} + trimmed {trimmed})",
            records.len()
        );
        for (key, value) in &records {
            let record = ChangeRecord::decode(value).expect("marker decodes");
            assert!(record.marker && record.consumer_hidden());
            assert!(record.old_image.is_none() && record.new_image.is_none());
            // Full-raw-key-as-prefix: change key = the raw key + the 8-byte
            // apply-completed HLC suffix.
            let prefix = &key[..key.len() - 8];
            assert!(
                [b"k1".as_slice(), b"k2".as_slice(), b"k3".as_slice()].contains(&prefix),
                "marker prefix must be the raw key itself: {prefix:?}"
            );
        }
    }

    /// its key's HLC suffix completed at apply (nonzero, strictly
    /// increasing in commit order) — and still never an LSI or footprint
    /// row.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plain_table_writes_emit_one_marker_record_each() {
        let dir = tempfile::TempDir::new().unwrap();
        let node = single_node(dir.path()).await;
        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"plain",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");
        let group = await_group(&node, "plain").await;

        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.PutItem",
            r#"{"TableName":"plain","Item":{"id":{"S":"a"},"n":{"N":"1"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "PutItem failed: {body}");
        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.UpdateItem",
            r#"{"TableName":"plain","Key":{"id":{"S":"a"}},
                "UpdateExpression":"SET n = :v",
                "ExpressionAttributeValues":{":v":{"N":"2"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "UpdateItem failed: {body}");
        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.DeleteItem",
            r#"{"TableName":"plain","Key":{"id":{"S":"a"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "DeleteItem failed: {body}");

        // Trim-safe accounting (ADR 0049 rung 4): the every-table hot-trim
        // arm keeps a plain table's markers transient, so a racing trim
        // tick may have already deleted some — an emitted marker is either
        // still pending or counted by `change_log_trimmed_total` (the
        // union a trim cannot erase; both zero would mean emission itself
        // regressed).
        let records = group.pending_changes().await;
        let trimmed = metrics_value(node.dynamo_addr(), "change_log_trimmed_total").await;
        assert_eq!(
            records.len() as u64 + trimmed,
            3,
            "exactly one marker per mutation (live {} + trimmed {trimmed})",
            records.len()
        );
        let mut last_hlc = 0u64;
        for (key, value) in &records {
            let record = ChangeRecord::decode(value).expect("marker record decodes");
            assert!(record.marker, "a plain table's record is a marker");
            assert!(!record.seeded, "a live write is never a seed");
            assert!(record.old_image.is_none(), "a marker carries no images");
            assert!(record.new_image.is_none(), "a marker carries no images");
            let hlc_suffix = u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap());
            assert!(
                hlc_suffix > last_hlc,
                "marker HLCs are apply-time-completed and strictly increasing \
                 in commit order ({hlc_suffix} after {last_hlc})"
            );
            last_hlc = hlc_suffix;
        }
        assert!(
            group
                .local_scan_kind_bounded(KIND_LSI, &[], None)
                .await
                .is_empty(),
            "a plain table still never writes an LSI row"
        );
        assert!(
            group
                .local_scan_kind_bounded(KIND_FOOTPRINT, &[], None)
                .await
                .is_empty(),
            "a plain table still never writes a footprint row"
        );

        // The base row's own read path is untouched by the marker.
        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.GetItem",
            r#"{"TableName":"plain","Key":{"id":{"S":"a"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "GetItem failed: {body}");
        assert!(
            !body.contains("\"n\""),
            "the delete must have removed the item: {body}"
        );
    }

    /// ADR 0049's gate-drift regression: `BatchWriteItem` on a
    /// streamed-but-unindexed table must emit one change record per request
    /// — its fast path's old gate (`table_indexes(table).is_empty()`) had
    /// silently drifted from the then-shared kind-path gate (since deleted;
    /// the kind path is structural now), so such a table's batch writes bypassed the kind path and
    /// its stream silently lost every one of them (red on the pre-ADR-0049
    /// code).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_write_on_a_streamed_table_emits_change_records() {
        let dir = tempfile::TempDir::new().unwrap();
        let node = single_node(dir.path()).await;
        create_streamed_table(node.dynamo_addr(), "sb").await;
        let group = await_group(&node, "sb").await;

        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.BatchWriteItem",
            r#"{"RequestItems":{"sb":[
                {"PutRequest":{"Item":{"id":{"S":"a"},"n":{"N":"1"}}}},
                {"PutRequest":{"Item":{"id":{"S":"b"},"n":{"N":"2"}}}}
            ]}}"#,
        )
        .await;
        assert_eq!(status, 200, "BatchWriteItem failed: {body}");

        let records = group.pending_changes().await;
        assert_eq!(
            records.len(),
            2,
            "a streamed table's batch writes must each leave a change record \
             (the drifted fast-path gate used to lose both)"
        );
        for (_, value) in &records {
            let record = ChangeRecord::decode(value).expect("change record decodes");
            assert!(!record.marker, "a streamed table's record is no marker");
            assert!(
                record.new_image.is_some(),
                "a streamed table's record carries images"
            );
        }
    }

    /// A shard record's stored form always carries both images regardless of
    /// the table's declared `StreamViewType` (ADR 0042 §3: projection is a
    /// read-time decision, made later in the stack — not yet built — never a
    /// storage-time one). This table declares `KEYS_ONLY`; the change record
    /// this write leaves still decodes both the old and new image.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn change_record_carries_both_images_regardless_of_view_type() {
        let dir = tempfile::TempDir::new().unwrap();
        let node = single_node(dir.path()).await;
        create_streamed_table(node.dynamo_addr(), "s2").await;
        let group = await_group(&node, "s2").await;

        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.PutItem",
            r#"{"TableName":"s2","Item":{"id":{"S":"a"},"n":{"N":"1"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "PutItem failed: {body}");
        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.PutItem",
            r#"{"TableName":"s2","Item":{"id":{"S":"a"},"n":{"N":"2"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "second PutItem failed: {body}");

        let records = group.pending_changes().await;
        assert_eq!(records.len(), 2, "one record per write");
        let (_, second) = &records[1];
        let record = ChangeRecord::decode(second).expect("change record decodes as Some");
        assert!(
            record.old_image.is_some(),
            "old image must be stored even under KEYS_ONLY"
        );
        assert!(
            record.new_image.is_some(),
            "new image must be stored even under KEYS_ONLY"
        );
    }

    /// ADR 0042 §7's trim rule extended to a stream: a table with a stream
    /// enabled expects a `"copier"` cursor row, which doesn't exist yet (the
    /// copier lands in PR B8) — so trim stays fully blocked and every change
    /// record survives, mirroring `index_drain::gsi_drain_cursor_tests::
    /// trim_never_deletes_past_an_expected_consumers_missing_cursor`'s GSI
    /// version of the same property.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn trim_blocked_on_a_streamed_table_with_no_copier_cursor_yet() {
        let dir = tempfile::TempDir::new().unwrap();
        let node = single_node(dir.path()).await;
        create_streamed_table(node.dynamo_addr(), "s3").await;
        let group = await_group(&node, "s3").await;

        for i in 0..5 {
            let (status, body) = dynamo(
                node.dynamo_addr(),
                "DynamoDB_20120810.PutItem",
                &format!(r#"{{"TableName":"s3","Item":{{"id":{{"S":"k{i}"}}}}}}"#),
            )
            .await;
            assert_eq!(status, 200, "PutItem({i}) failed: {body}");
        }
        // Give the index-drain loop (which runs the trim janitor too) several
        // ticks' worth of time to (wrongly, if this regressed) trim past the
        // missing "copier" cursor.
        sleep(Duration::from_millis(500)).await;
        assert_eq!(
            group.pending_changes().await.len(),
            5,
            "every change record must survive — the streamed table's \
             expected \"copier\" tag has no cursor row yet"
        );
    }
}
