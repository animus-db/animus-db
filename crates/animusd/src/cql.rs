//! The CQL (Cassandra) binary-protocol endpoint (ADR 0006).
//!
//! A hand-rolled server over a real tokio [`TcpListener`] that speaks a
//! practical subset of the **Apache Cassandra CQL v4 binary protocol**: it
//! reads framed requests, does the `STARTUP → READY` (and `OPTIONS →
//! SUPPORTED`) handshake, and runs `QUERY` / `PREPARE` / `EXECUTE`. Every
//! statement is parsed, type-checked, and planned by the pure, I/O-free
//! `animus_cql` crate, then routed through the **same** quorum coordinator the
//! plain-TCP client API and the DynamoDB endpoint use — so everything below this
//! socket edge stays on the existing `Env`-based data-plane paths. The edge
//! itself is production-only I/O, like `ProdEnv`.
//!
//! ## Why hand-rolled
//!
//! The repo deliberately avoids heavy dependencies (no CQL/Cassandra client or
//! server crate) to keep `cargo deny` trivial and the protocol logic
//! deterministic. A CQL frame is a fixed 9-byte header + a length-delimited
//! body, so a small reader is enough.
//!
//! ## Supported subset
//!
//! - `STARTUP` (any options) → `READY` (no authentication).
//! - `OPTIONS` → `SUPPORTED` (CQL 3.0.0, no compression).
//! - `QUERY`:
//!   - `CREATE KEYSPACE` / `USE <keyspace>` — record / select a keyspace.
//!   - `CREATE TABLE` — record a schema (one partition key column + typed
//!     columns) in the in-memory catalog.
//!   - `INSERT` / `SELECT` — resolved against the catalog, with a real
//!     `text/int/bigint/boolean/blob/uuid` type system; quorum write / read.
//! - `PREPARE` → `RESULT/Prepared` (statement id + bind metadata); `EXECUTE`
//!   (bound values) → the same result a `QUERY` would give. A real driver's
//!   prepare/execute path works.
//!
//! ## State that lives at the edge (not below it)
//!
//! Two pieces of mutable state are owned by this **production edge** because
//! they are not (yet) replicated through the control plane:
//!
//! - the **schema catalog** ([`animus_cql::Catalog`]) — *in-memory, not
//!   durable*: lost on restart, shared across in-process nodes in `--cluster N`
//!   dev mode. Control-plane-replicated schemas are future work (ADR 0006), as
//!   on the DynamoDB side.
//! - the **prepared-statement store** — content-addressed: a statement's id is a
//!   stable hash of its text, so `PREPARE` then `EXECUTE` works even across
//!   connections.
//!
//! The pure protocol/type/catalog logic stays in `animus_cql`; only the socket
//! loop and this shared state live here.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use animus_cql::frame::{self, Frame, Opcode};
use animus_cql::{
    Catalog, ColumnSpec, Consistency, CqlType, CqlValue, DeletePlan, InsertPlan, Partition,
    ReadPlan, Statement, UpdatePlan, response,
};
use animus_data::TabletView;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::ClientCtx;

const CQL_TIMEOUT: Duration = Duration::from_secs(5);

/// A prepared statement: the original CQL text plus the resolved bind-variable
/// column specs (so `EXECUTE` can type the bound cells). Re-parsed on `EXECUTE`
/// (cheap) so the planning path is shared with `QUERY`.
#[derive(Clone)]
struct Prepared {
    cql: String,
    bind_specs: Vec<ColumnSpec>,
}

/// Process-wide CQL edge state shared across all connections of a node: the
/// schema catalog and the prepared-statement store, both behind one async mutex
/// (contention here is negligible — these are tiny in-memory maps).
#[derive(Default)]
struct CqlState {
    catalog: Catalog,
    /// statement id → prepared statement (id is a content hash of the text).
    prepared: BTreeMap<Vec<u8>, Prepared>,
}

/// Per-connection mutable state: the keyspace selected by `USE`.
#[derive(Default)]
struct Session {
    keyspace: Option<String>,
}

/// The process-wide CQL edge state. It is shared across **all nodes' CQL
/// listeners in one process** — matching the DynamoDB `SchemaRegistry`'s
/// behavior, so in single-process `--cluster N` dev mode a `CREATE TABLE` on one
/// node is visible to a `SELECT` on another (the catalog is not control-plane
/// replicated). In a one-process-per-node deployment each process has its own
/// catalog, so schemas must be (re)created per process — an accepted limitation
/// until schemas are replicated (ADR 0006).
fn shared_state() -> Arc<Mutex<CqlState>> {
    static STATE: OnceLock<Arc<Mutex<CqlState>>> = OnceLock::new();
    STATE
        .get_or_init(|| Arc::new(Mutex::new(CqlState::default())))
        .clone()
}

/// Accept loop for the CQL endpoint. Each connection is handled on its own task.
/// The schema catalog and prepared statements live in the process-wide
/// [`shared_state`].
pub(crate) async fn serve(listener: TcpListener, ctx: ClientCtx) {
    let state = shared_state();
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let ctx = ctx.clone();
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_conn(stream, ctx, state).await {
                        tracing::debug!(?err, "cql connection closed");
                    }
                });
            }
            Err(err) => {
                tracing::warn!(?err, "cql accept failed");
                return;
            }
        }
    }
}

async fn handle_conn(
    mut stream: TcpStream,
    ctx: ClientCtx,
    state: Arc<Mutex<CqlState>>,
) -> std::io::Result<()> {
    let mut session = Session::default();
    loop {
        let Some(frame) = read_frame(&mut stream).await? else {
            return Ok(()); // clean EOF
        };
        let response = dispatch(&ctx, &state, &mut session, &frame).await;
        stream.write_all(&response).await?;
        stream.flush().await?;
    }
}

/// Read one CQL frame: a 9-byte header then the declared body. Returns `None` at
/// a clean EOF before any header bytes.
async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Option<Frame>> {
    let mut header = [0u8; frame::HEADER_LEN];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let body_len = Frame::body_len(&header).map_err(invalid)?;
    let mut body = vec![0u8; body_len];
    stream.read_exact(&mut body).await?;
    let frame = Frame::decode(&header, &body).map_err(invalid)?;
    Ok(Some(frame))
}

fn invalid(e: frame::FrameError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
}

/// Turn a decoded request frame into the response frame bytes.
async fn dispatch(
    ctx: &ClientCtx,
    state: &Arc<Mutex<CqlState>>,
    session: &mut Session,
    frame: &Frame,
) -> Vec<u8> {
    let stream = frame.stream;
    match frame.opcode {
        Opcode::Startup => match response::parse_startup(&frame.body) {
            Ok(_) => response::ready(stream),
            Err(e) => response::error(stream, response::ERR_PROTOCOL, &e.to_string()),
        },
        Opcode::Options => response::supported(stream),
        Opcode::Query => match response::parse_query_request(&frame.body) {
            Ok(req) => run_cql(ctx, state, session, stream, &req.cql, &[], req.consistency).await,
            Err(e) => response::error(stream, response::ERR_PROTOCOL, &e.to_string()),
        },
        Opcode::Prepare => match response::parse_prepare_request(&frame.body) {
            Ok(cql) => prepare(state, session, stream, &cql).await,
            Err(e) => response::error(stream, response::ERR_PROTOCOL, &e.to_string()),
        },
        Opcode::Execute => execute(ctx, state, session, stream, &frame.body).await,
        // The client should only ever send the request opcodes above.
        other => response::error(
            stream,
            response::ERR_PROTOCOL,
            &format!("unexpected opcode {other:?}"),
        ),
    }
}

/// A stable, content-addressed prepared-statement id: a 16-byte id derived from
/// the statement text (FNV-1a over two salts) so the same statement always maps
/// to the same id (and `EXECUTE` works across connections), with no RNG (ADR
/// 0003 — the edge stays deterministic too).
fn statement_id(cql: &str) -> Vec<u8> {
    let a = fnv1a(cql.as_bytes(), 0xcbf2_9ce4_8422_2325);
    let b = fnv1a(cql.as_bytes(), 0x0100_0000_01b3_dead);
    let mut id = Vec::with_capacity(16);
    id.extend_from_slice(&a.to_be_bytes());
    id.extend_from_slice(&b.to_be_bytes());
    id
}

fn fnv1a(bytes: &[u8], mut hash: u64) -> u64 {
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// `PREPARE`: parse + resolve the statement's bind markers against the catalog,
/// store it under its content id, and reply `RESULT/Prepared`.
async fn prepare(
    state: &Arc<Mutex<CqlState>>,
    session: &Session,
    stream: i16,
    cql: &str,
) -> Vec<u8> {
    let stmt = match animus_cql::parse_statement(cql) {
        Ok(s) => s,
        Err(e) => return response::error(stream, response::ERR_INVALID, &e.to_string()),
    };
    let mut guard = state.lock().await;
    let selected = session.keyspace.clone();
    let (bind_specs, keyspace, table) = match &stmt {
        Statement::Insert(ins) => {
            match animus_cql::plan::insert_bind_types(&guard.catalog, selected.as_deref(), ins) {
                Ok(specs) => {
                    let ks = ins
                        .keyspace
                        .clone()
                        .or_else(|| selected.clone())
                        .unwrap_or_default();
                    (specs, ks, ins.table.clone())
                }
                Err(e) => return response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
        Statement::Select(sel) => {
            match animus_cql::plan::select_bind_types(&guard.catalog, selected.as_deref(), sel) {
                Ok(specs) => {
                    let ks = sel
                        .keyspace
                        .clone()
                        .or_else(|| selected.clone())
                        .unwrap_or_default();
                    (specs, ks, sel.table.clone())
                }
                Err(e) => return response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
        _ => {
            return response::error(
                stream,
                response::ERR_INVALID,
                "only INSERT and SELECT can be prepared",
            );
        }
    };
    let id = statement_id(cql);
    guard.prepared.insert(
        id.clone(),
        Prepared {
            cql: cql.to_owned(),
            bind_specs: bind_specs.clone(),
        },
    );
    response::prepared_result(stream, &id, &keyspace, &table, &bind_specs)
}

/// `EXECUTE`: look up the prepared statement, decode + type the bound values,
/// and run the statement exactly as a `QUERY` would.
async fn execute(
    ctx: &ClientCtx,
    state: &Arc<Mutex<CqlState>>,
    session: &mut Session,
    stream: i16,
    body: &[u8],
) -> Vec<u8> {
    let req = match response::parse_execute_request(body) {
        Ok(r) => r,
        Err(e) => return response::error(stream, response::ERR_PROTOCOL, &e.to_string()),
    };
    let prepared = {
        let guard = state.lock().await;
        guard.prepared.get(&req.id).cloned()
    };
    let Some(prepared) = prepared else {
        return response::error(
            stream,
            response::ERR_UNPREPARED,
            "unknown prepared statement id; re-PREPARE",
        );
    };
    // Decode each bound cell against its resolved spec.
    if req.values.len() != prepared.bind_specs.len() {
        return response::error(
            stream,
            response::ERR_INVALID,
            &format!(
                "expected {} bound values, got {}",
                prepared.bind_specs.len(),
                req.values.len()
            ),
        );
    }
    let mut binds = Vec::with_capacity(req.values.len());
    for (spec, cell) in prepared.bind_specs.iter().zip(&req.values) {
        match cell {
            Some(bytes) => match spec.ty.decode(bytes) {
                Ok(v) => binds.push(v),
                Err(e) => return response::error(stream, response::ERR_INVALID, &e.to_string()),
            },
            None => {
                return response::error(
                    stream,
                    response::ERR_INVALID,
                    "null bound values are not supported",
                );
            }
        }
    }
    run_cql(
        ctx,
        state,
        session,
        stream,
        &prepared.cql,
        &binds,
        req.consistency,
    )
    .await
}

/// Parse, plan, and execute a CQL statement (shared by `QUERY` and `EXECUTE`).
/// `binds` supplies the values for any `?` markers; `consistency` is the
/// requested level, mapped to the data-plane quorum for any read/write.
async fn run_cql(
    ctx: &ClientCtx,
    state: &Arc<Mutex<CqlState>>,
    session: &mut Session,
    stream: i16,
    cql: &str,
    binds: &[CqlValue],
    consistency: Consistency,
) -> Vec<u8> {
    let stmt = match animus_cql::parse_statement(cql) {
        Ok(s) => s,
        Err(e) => return response::error(stream, response::ERR_INVALID, &e.to_string()),
    };
    match stmt {
        Statement::Use { keyspace } => {
            let known = {
                let guard = state.lock().await;
                guard.catalog.has_keyspace(&keyspace)
            };
            if !known {
                return response::error(
                    stream,
                    response::ERR_INVALID,
                    &format!("keyspace `{keyspace}` does not exist"),
                );
            }
            session.keyspace = Some(keyspace.clone());
            response::set_keyspace_result(stream, &keyspace)
        }
        Statement::CreateKeyspace {
            keyspace,
            if_not_exists: _,
        } => {
            let mut guard = state.lock().await;
            guard.catalog.create_keyspace(&keyspace);
            response::schema_change_result(stream, "CREATED", "KEYSPACE", &keyspace, "")
        }
        Statement::CreateTable(ct) => {
            let keyspace = match ct.keyspace.clone().or_else(|| session.keyspace.clone()) {
                Some(k) => k,
                None => {
                    return response::error(
                        stream,
                        response::ERR_INVALID,
                        "no keyspace selected; USE one or qualify the table name",
                    );
                }
            };
            let schema = animus_cql::schema_of(&ct);
            let mut guard = state.lock().await;
            match guard
                .catalog
                .create_table(&keyspace, schema, ct.if_not_exists)
            {
                Ok(()) => {
                    response::schema_change_result(stream, "CREATED", "TABLE", &keyspace, &ct.table)
                }
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
        Statement::Insert(ins) => {
            let plan = {
                let guard = state.lock().await;
                animus_cql::plan_insert(&guard.catalog, session.keyspace.as_deref(), &ins, binds)
            };
            match plan {
                Ok(plan) => run_insert(ctx, state, session, stream, plan, consistency).await,
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
        Statement::Update(upd) => {
            let plan = {
                let guard = state.lock().await;
                animus_cql::plan_update(&guard.catalog, session.keyspace.as_deref(), &upd, binds)
            };
            match plan {
                Ok(plan) => run_update(ctx, state, session, stream, plan, consistency).await,
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
        Statement::Delete(del) => {
            let plan = {
                let guard = state.lock().await;
                animus_cql::plan_delete(&guard.catalog, session.keyspace.as_deref(), &del, binds)
            };
            match plan {
                Ok(plan) => run_delete(ctx, state, session, stream, plan, consistency).await,
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
        Statement::Select(sel) => {
            let plan = {
                let guard = state.lock().await;
                animus_cql::plan_select(&guard.catalog, session.keyspace.as_deref(), &sel, binds)
            };
            match plan {
                Ok(plan) => run_select(ctx, state, session, stream, plan, consistency).await,
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
    }
}

/// Resolve the table schema for a planned op, cloned out from under the catalog
/// lock so the partition (de)serialization can run without holding it.
async fn schema_for(
    state: &Arc<Mutex<CqlState>>,
    selected: Option<&str>,
    table: &str,
) -> Result<animus_cql::TableSchema, String> {
    let guard = state.lock().await;
    guard
        .catalog
        .resolve(None, selected, table)
        .cloned()
        .map_err(|e| e.to_string())
}

/// Execute a planned `INSERT` as a partition read-modify-write: read the current
/// partition, upsert this row by its clustering key, and write the partition
/// back. Reply `RESULT/Void`.
async fn run_insert(
    ctx: &ClientCtx,
    _state: &Arc<Mutex<CqlState>>,
    _session: &Session,
    stream: i16,
    plan: InsertPlan,
    consistency: Consistency,
) -> Vec<u8> {
    // The partition format stores each row's clustering blob verbatim, so the
    // read-modify-write does not need the schema: existing rows round-trip
    // untyped (their blob is the map key) and this new row is merged by its
    // clustering bytes.
    let result = mutate_partition(ctx, &plan.key, consistency, |part| {
        part.rows.insert(plan.clustering.clone(), plan.row.clone());
    })
    .await;
    match result {
        Ok(()) => response::void_result(stream),
        Err(msg) => response::error(stream, response::ERR_SERVER, &msg),
    }
}

/// Execute a planned `UPDATE`: read-modify-write the partition, applying the cell
/// assignments over the addressed row (creating it if absent — CQL upsert).
async fn run_update(
    ctx: &ClientCtx,
    state: &Arc<Mutex<CqlState>>,
    session: &Session,
    stream: i16,
    plan: UpdatePlan,
    consistency: Consistency,
) -> Vec<u8> {
    let schema = match schema_for(state, session.keyspace.as_deref(), &plan.table).await {
        Ok(s) => s,
        Err(msg) => return response::error(stream, response::ERR_SERVER, &msg),
    };
    let result = mutate_partition_with_schema(ctx, &plan.key, consistency, &schema, |part| {
        let clustering_values = decode_clustering_for(&plan.clustering, &schema);
        let row = part
            .rows
            .entry(plan.clustering.clone())
            .or_insert_with(|| animus_cql::Row {
                clustering: clustering_values,
                cells: std::collections::BTreeMap::new(),
            });
        for (idx, cell) in &plan.assignments {
            row.cells.insert(*idx, cell.clone());
        }
    })
    .await;
    match result {
        Ok(()) => response::void_result(stream),
        Err(msg) => response::error(stream, response::ERR_SERVER, &msg),
    }
}

/// Execute a planned `DELETE`: a full primary key removes one row from the
/// partition; a partition-key-only delete removes the whole partition. When the
/// partition becomes empty, the data-plane key is tombstoned (a data-plane
/// `delete`); otherwise the remaining partition is written back.
async fn run_delete(
    ctx: &ClientCtx,
    state: &Arc<Mutex<CqlState>>,
    session: &Session,
    stream: i16,
    plan: DeletePlan,
    consistency: Consistency,
) -> Vec<u8> {
    let schema = match schema_for(state, session.keyspace.as_deref(), &plan.table).await {
        Ok(s) => s,
        Err(msg) => return response::error(stream, response::ERR_SERVER, &msg),
    };
    let view = match view_for(ctx, &plan.key, consistency) {
        Ok(v) => v,
        Err(msg) => return response::error(stream, response::ERR_SERVER, &msg),
    };
    let _guard = ctx.coord_lock.lock().await;
    let (current, bytes) = match read_partition_bytes(ctx, &view, &plan.key).await {
        Ok(r) => r,
        Err(msg) => return response::error(stream, response::ERR_SERVER, &msg),
    };
    let mut part = match Partition::decode(&bytes.unwrap_or_default(), &schema) {
        Ok(p) => p,
        Err(e) => return response::error(stream, response::ERR_SERVER, &e.to_string()),
    };
    match &plan.clustering {
        Some(clustering) => {
            part.rows.remove(clustering);
        }
        None => part.rows.clear(),
    }
    let ok = if part.is_empty() {
        // Whole partition gone → tombstone the data-plane key so it reads absent.
        ctx.coordinator
            .delete(&view, &plan.key, current + 1, CQL_TIMEOUT)
            .await
    } else {
        ctx.coordinator
            .write(&view, &plan.key, &part.encode(), current + 1, CQL_TIMEOUT)
            .await
    };
    if ok {
        response::void_result(stream)
    } else {
        response::error(
            stream,
            response::ERR_SERVER,
            "delete did not reach a quorum",
        )
    }
}

/// Execute a planned `SELECT`: quorum-read the partition, return every matching
/// row (the clustering prefix filters; an empty prefix returns the whole
/// partition) in clustering order as a typed `RESULT/Rows`.
async fn run_select(
    ctx: &ClientCtx,
    state: &Arc<Mutex<CqlState>>,
    session: &Session,
    stream: i16,
    plan: ReadPlan,
    consistency: Consistency,
) -> Vec<u8> {
    let schema = match schema_for(state, session.keyspace.as_deref(), &plan.table).await {
        Ok(s) => s,
        Err(msg) => return response::error(stream, response::ERR_SERVER, &msg),
    };
    let view = match view_for(ctx, &plan.key, consistency) {
        Ok(v) => v,
        Err(msg) => return response::error(stream, response::ERR_SERVER, &msg),
    };
    let bytes = {
        let _guard = ctx.coord_lock.lock().await;
        match ctx.coordinator.read(&view, &plan.key, CQL_TIMEOUT).await {
            animus_data::ReadResult::Value(v) => v,
            animus_data::ReadResult::Failed => {
                return response::error(
                    stream,
                    response::ERR_SERVER,
                    "read did not reach a quorum",
                );
            }
        }
    };
    let Some(bytes) = bytes else {
        return response::typed_rows_multi(stream, "animus", &plan.table, &plan.projection, &[]);
    };
    let part = match Partition::decode(&bytes, &schema) {
        Ok(p) => p,
        Err(e) => return response::error(stream, response::ERR_SERVER, &e.to_string()),
    };
    let mut rows = Vec::new();
    for row in part.rows_matching(&plan.clustering_prefix) {
        match build_row(
            &plan.projection,
            &plan.pk_name,
            &plan.pk_value,
            &schema,
            row,
        ) {
            Ok(cells) => rows.push(cells),
            Err(msg) => return response::error(stream, response::ERR_SERVER, &msg),
        }
    }
    response::typed_rows_multi(stream, "animus", &plan.table, &plan.projection, &rows)
}

/// Decode the typed clustering values for a row from its clustering blob, used
/// when `UPDATE` creates a previously-absent row.
fn decode_clustering_for(clustering: &[u8], schema: &animus_cql::TableSchema) -> Vec<CqlValue> {
    let mut part = Partition::default();
    part.rows.insert(
        clustering.to_vec(),
        animus_cql::Row {
            clustering: Vec::new(),
            cells: std::collections::BTreeMap::new(),
        },
    );
    Partition::decode(&part.encode(), schema)
        .ok()
        .and_then(|p| p.rows.into_values().next())
        .map(|r| r.clustering)
        .unwrap_or_default()
}

/// Build one projected row's cells from a decoded [`animus_cql::Row`]. A
/// projected primary-key column is reconstructed (partition key from the
/// predicate value; a clustering key from the row's clustering values, located
/// by its position in the schema's clustering order); a non-key column is looked
/// up by schema index in the row's cells.
fn build_row(
    projection: &[ColumnSpec],
    pk_name: &str,
    pk_value: &CqlValue,
    schema: &animus_cql::TableSchema,
    row: &animus_cql::Row,
) -> Result<Vec<Option<Vec<u8>>>, String> {
    let mut cells = Vec::with_capacity(projection.len());
    for spec in projection {
        if spec.name.eq_ignore_ascii_case(pk_name) {
            cells.push(Some(encode_typed(spec.ty, pk_value)?));
        } else if let Some(pos) = schema
            .clustering_keys
            .iter()
            .position(|&i| i == spec.schema_index)
        {
            match row.clustering.get(pos) {
                Some(v) => cells.push(Some(encode_typed(spec.ty, v)?)),
                None => cells.push(None),
            }
        } else if let Some(cell) = row.cells.get(&spec.schema_index) {
            cells.push(Some(cell.clone()));
        } else {
            cells.push(None);
        }
    }
    Ok(cells)
}

fn encode_typed(ty: CqlType, value: &CqlValue) -> Result<Vec<u8>, String> {
    ty.encode(value).map_err(|e| e.to_string())
}

/// Build the routing view for `key`, then override its R/W quorum sizes per the
/// requested CQL consistency level (clamped to the replica count).
fn view_for(ctx: &ClientCtx, key: &[u8], consistency: Consistency) -> Result<TabletView, String> {
    let mut view = ctx
        .view_for(key)
        .ok_or_else(|| "no tablet covers this key yet (cluster still bootstrapping)".to_owned())?;
    let q = response::consistency_quorum(consistency, view.replicas.len());
    view.r = q;
    view.w = q;
    Ok(view)
}

/// Read the current partition bytes + its MVCC version for a read-modify-write.
/// Returns `(current_version, value)` where `value` is `None` for an absent key.
async fn read_partition_bytes(
    ctx: &ClientCtx,
    view: &TabletView,
    key: &[u8],
) -> Result<(u64, Option<Vec<u8>>), String> {
    let current = ctx
        .coordinator
        .read_version(view, key, CQL_TIMEOUT)
        .await
        .ok_or_else(|| "could not read current version".to_owned())?;
    let value = match ctx.coordinator.read(view, key, CQL_TIMEOUT).await {
        animus_data::ReadResult::Value(v) => v,
        animus_data::ReadResult::Failed => return Err("read did not reach a quorum".to_owned()),
    };
    Ok((current, value))
}

/// Read-modify-write a partition under the coordinator lock: decode the current
/// partition (schema-agnostic on the write path), apply `mutate`, write it back.
async fn mutate_partition(
    ctx: &ClientCtx,
    key: &[u8],
    consistency: Consistency,
    mutate: impl FnOnce(&mut Partition),
) -> Result<(), String> {
    let schema_agnostic = animus_cql::TableSchema {
        name: String::new(),
        columns: Vec::new(),
        partition_key: 0,
        clustering_keys: Vec::new(),
    };
    mutate_partition_with_schema(ctx, key, consistency, &schema_agnostic, mutate).await
}

/// As [`mutate_partition`] but decodes the partition against `schema` (so existing
/// rows' clustering values are typed — needed when `mutate` reads them).
async fn mutate_partition_with_schema(
    ctx: &ClientCtx,
    key: &[u8],
    consistency: Consistency,
    schema: &animus_cql::TableSchema,
    mutate: impl FnOnce(&mut Partition),
) -> Result<(), String> {
    let view = view_for(ctx, key, consistency)?;
    let _guard = ctx.coord_lock.lock().await;
    let (current, bytes) = read_partition_bytes(ctx, &view, key).await?;
    let mut part =
        Partition::decode(&bytes.unwrap_or_default(), schema).map_err(|e| e.to_string())?;
    mutate(&mut part);
    let ok = ctx
        .coordinator
        .write(&view, key, &part.encode(), current + 1, CQL_TIMEOUT)
        .await;
    if ok {
        Ok(())
    } else {
        Err("write did not reach a quorum".to_owned())
    }
}
