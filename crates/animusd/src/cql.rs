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
//!   - `CREATE TABLE` — **propose the schema to the control plane** and wait for
//!     it to commit (ADR 0013), so the table is durable + cluster-agreed.
//!   - `DROP TABLE` — propose `DropTableSchema` and wait for it to replicate.
//!   - `ALTER TABLE ... ADD` — append columns (drop + recreate the replicated
//!     schema with the extended column list; see the gotcha below).
//!   - `INSERT` / `SELECT` / `UPDATE` / `DELETE` — resolved against the
//!     **replicated** schema, with a real `text/int/bigint/boolean/blob/uuid`
//!     type system; quorum write / read.
//!   - `BATCH` — a sequence of `INSERT`/`UPDATE`/`DELETE` applied in order.
//! - `PREPARE` → `RESULT/Prepared` (statement id + bind metadata); `EXECUTE`
//!   (bound values) → the same result a `QUERY` would give. A real driver's
//!   prepare/execute path works.
//!
//! ## Schemas are now control-plane replicated (ADR 0013)
//!
//! `CREATE TABLE` / `DROP TABLE` mutate the **Raft-replicated** schema catalog in
//! the control plane (`MetaCommand::{CreateTableSchema, DropTableSchema}`), and
//! `INSERT`/`SELECT`/`UPDATE`/`DELETE` resolve their schema from the cached,
//! replicated `Metadata` ([`ClientCtx::table_schema`]). So a created table is
//! **durable** (survives a node restart — recovered from the Raft WAL/snapshot)
//! and **cluster-agreed** (every node sees the same schema), replacing the old
//! per-process in-memory catalog. The control-plane catalog keys tables by a
//! `ks.table` convention (the `TableName` is opaque to the control plane), so the
//! CQL edge namespaces every table as `keyspace.table`.
//!
//! ## State that still lives at the edge (not control-plane replicated)
//!
//! - The **prepared-statement store** — content-addressed: a statement's id is a
//!   stable hash of its text, so `PREPARE` then `EXECUTE` works even across
//!   connections. Re-parsed on `EXECUTE`, so the planning path is shared.
//! - **Keyspaces** are *not* separately replicated (the control-plane catalog
//!   models tables, not keyspaces): the edge keeps a process-local set of created
//!   keyspaces for `USE`/qualifier validation, and additionally treats a keyspace
//!   as existing if any replicated `ks.table` belongs to it (so a keyspace with
//!   tables is recognized after a restart). Replicating keyspace metadata itself
//!   is future work (ADR 0006/0013).
//!
//! The pure protocol/type/catalog logic stays in `animus_cql`; only the socket
//! loop and this shared state live here.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use animus_control::{ColumnDef, ColumnType, TableSchema as ControlSchema};
use animus_cql::frame::{self, Frame, Opcode};
use animus_cql::{
    AlterTable, Catalog, Column, ColumnSpec, Consistency, CqlType, CqlValue, DeletePlan,
    InsertPlan, Partition, ReadPlan, Statement, TableSchema as CqlSchema, UpdatePlan, response,
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
/// known keyspaces (not control-plane replicated, see the module docs) and the
/// prepared-statement store, both behind one async mutex (contention here is
/// negligible — these are tiny in-memory maps). The *table schemas* no longer
/// live here: they are in the control plane's replicated catalog (ADR 0013).
#[derive(Default)]
struct CqlState {
    /// Created keyspaces (lowercased). Keyspace metadata is not replicated; a
    /// keyspace also counts as existing if a replicated `ks.table` belongs to it.
    keyspaces: BTreeSet<String>,
    /// statement id → prepared statement (id is a content hash of the text).
    prepared: BTreeMap<Vec<u8>, Prepared>,
}

/// Per-connection mutable state: the keyspace selected by `USE`.
#[derive(Default)]
struct Session {
    keyspace: Option<String>,
}

/// The process-wide CQL edge state (keyspaces + prepared statements), shared
/// across all nodes' CQL listeners in one process. Table schemas are **not** here
/// anymore — they live in the control plane's replicated catalog.
fn shared_state() -> Arc<Mutex<CqlState>> {
    static STATE: OnceLock<Arc<Mutex<CqlState>>> = OnceLock::new();
    STATE
        .get_or_init(|| Arc::new(Mutex::new(CqlState::default())))
        .clone()
}

// --- schema mapping: CQL <-> control-plane (ADR 0013) -----------------------

/// The control-plane catalog key for a CQL table: the keyspace-qualified name
/// `keyspace.table`, lowercased so it is case-insensitive like CQL identifiers.
/// The control plane treats this as an opaque `TableName`.
fn control_table_name(keyspace: &str, table: &str) -> String {
    format!(
        "{}.{}",
        keyspace.to_ascii_lowercase(),
        table.to_ascii_lowercase()
    )
}

/// Map a CQL scalar type to the control plane's shared `ColumnType` vocabulary.
fn cql_to_column_type(ty: CqlType) -> ColumnType {
    match ty {
        CqlType::Text => ColumnType::String,
        CqlType::Int => ColumnType::Int,
        CqlType::BigInt => ColumnType::BigInt,
        CqlType::Boolean => ColumnType::Bool,
        CqlType::Blob => ColumnType::Binary,
        CqlType::Uuid => ColumnType::Uuid,
    }
}

/// Map a control-plane `ColumnType` back to a CQL scalar type. The DynamoDB-only
/// `Number` family (no fixed CQL width) is surfaced to CQL as `bigint`.
fn column_type_to_cql(ty: ColumnType) -> CqlType {
    match ty {
        ColumnType::String => CqlType::Text,
        ColumnType::Int => CqlType::Int,
        ColumnType::BigInt | ColumnType::Number => CqlType::BigInt,
        ColumnType::Bool => CqlType::Boolean,
        ColumnType::Binary => CqlType::Blob,
        ColumnType::Uuid => CqlType::Uuid,
    }
}

/// Convert a pure-`animus_cql` table schema (from `schema_of(&CreateTable)`) into
/// the control plane's replicated `TableSchema`. Column order is preserved (the
/// partition-storage format keys cells by schema index, so order is part of the
/// contract), and the partition/clustering keys are recorded by name.
fn cql_schema_to_control(schema: &CqlSchema) -> ControlSchema {
    let columns: Vec<ColumnDef> = schema
        .columns
        .iter()
        .map(|c| ColumnDef::new(c.name.clone(), cql_to_column_type(c.ty)))
        .collect();
    let partition_key = schema.pk_column().name.clone();
    let clustering_keys: Vec<String> = schema
        .clustering_keys
        .iter()
        .map(|&i| schema.columns[i].name.clone())
        .collect();
    ControlSchema::with_columns(partition_key, clustering_keys, columns)
}

/// Convert a replicated control-plane `TableSchema` back into the pure
/// `animus_cql` schema the planner needs (the same column order, with the
/// partition/clustering keys resolved to their column indices). `table` is the
/// CQL table name to display in result metadata.
///
/// Returns `None` if the control schema is internally inconsistent (a key names
/// a missing column) — which `TableSchema::validate` already prevents on write,
/// so this is defensive only.
fn control_schema_to_cql(table: &str, schema: &ControlSchema) -> Option<CqlSchema> {
    let columns: Vec<Column> = schema
        .columns
        .iter()
        .map(|c| Column {
            name: c.name.clone(),
            ty: column_type_to_cql(c.ty),
        })
        .collect();
    let partition_key = columns
        .iter()
        .position(|c| c.name == schema.partition_key)?;
    let mut clustering_keys = Vec::with_capacity(schema.clustering_keys.len());
    for ck in &schema.clustering_keys {
        clustering_keys.push(columns.iter().position(|c| &c.name == ck)?);
    }
    Some(CqlSchema {
        name: table.to_owned(),
        columns,
        partition_key,
        clustering_keys,
    })
}

/// Build a one-table [`Catalog`] so the existing pure planner functions
/// (`plan_insert`/`plan_select`/...) can resolve `table` against its replicated
/// schema without holding any edge-side catalog. The keyspace is whatever the
/// statement qualified or the session `USE`d.
fn ephemeral_catalog(keyspace: &str, table: &str, schema: &ControlSchema) -> Option<Catalog> {
    let cql_schema = control_schema_to_cql(table, schema)?;
    let mut cat = Catalog::new();
    cat.create_keyspace(keyspace);
    // `create_table` only fails on a missing keyspace (just created) or a
    // duplicate (the catalog is fresh), so this cannot error here.
    cat.create_table(keyspace, cql_schema, false).ok()?;
    Some(cat)
}

/// Resolve a CQL table reference to `(keyspace, control_name, control_schema)`
/// from the replicated catalog, given the optional `keyspace.` qualifier and the
/// session's `USE`d keyspace. `Err` carries a client-facing message.
fn resolve_table(
    ctx: &ClientCtx,
    qualifier: Option<&str>,
    selected: Option<&str>,
    table: &str,
) -> Result<(String, String, ControlSchema), String> {
    let keyspace = qualifier
        .or(selected)
        .ok_or_else(|| "no keyspace selected; USE one or qualify the table name".to_owned())?
        .to_owned();
    let control_name = control_table_name(&keyspace, table);
    let schema = ctx
        .table_schema(&control_name)
        .ok_or_else(|| format!("table `{keyspace}.{table}` does not exist"))?;
    Ok((keyspace, control_name, schema))
}

/// Accept loop for the CQL endpoint. Each connection is handled on its own task.
/// The keyspace set and prepared statements live in the process-wide
/// [`shared_state`]; table schemas live in the control plane.
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
            Ok(cql) => prepare(ctx, state, session, stream, &cql).await,
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

/// `PREPARE`: parse + resolve the statement's bind markers against the
/// **replicated** schema, store it under its content id, and reply
/// `RESULT/Prepared`.
async fn prepare(
    ctx: &ClientCtx,
    state: &Arc<Mutex<CqlState>>,
    session: &Session,
    stream: i16,
    cql: &str,
) -> Vec<u8> {
    let stmt = match animus_cql::parse_statement(cql) {
        Ok(s) => s,
        Err(e) => return response::error(stream, response::ERR_INVALID, &e.to_string()),
    };
    let selected = session.keyspace.clone();
    let (bind_specs, keyspace, table) = match &stmt {
        Statement::Insert(ins) => {
            let (ks, _name, schema) = match resolve_table(
                ctx,
                ins.keyspace.as_deref(),
                selected.as_deref(),
                &ins.table,
            ) {
                Ok(t) => t,
                Err(msg) => return response::error(stream, response::ERR_INVALID, &msg),
            };
            let Some(cat) = ephemeral_catalog(&ks, &ins.table, &schema) else {
                return response::error(stream, response::ERR_SERVER, "corrupt schema");
            };
            match animus_cql::plan::insert_bind_types(&cat, Some(&ks), ins) {
                Ok(specs) => (specs, ks, ins.table.clone()),
                Err(e) => return response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
        Statement::Select(sel) => {
            let (ks, _name, schema) = match resolve_table(
                ctx,
                sel.keyspace.as_deref(),
                selected.as_deref(),
                &sel.table,
            ) {
                Ok(t) => t,
                Err(msg) => return response::error(stream, response::ERR_INVALID, &msg),
            };
            let Some(cat) = ephemeral_catalog(&ks, &sel.table, &schema) else {
                return response::error(stream, response::ERR_SERVER, "corrupt schema");
            };
            match animus_cql::plan::select_bind_types(&cat, Some(&ks), sel) {
                Ok(specs) => (specs, ks, sel.table.clone()),
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
    {
        let mut guard = state.lock().await;
        guard.prepared.insert(
            id.clone(),
            Prepared {
                cql: cql.to_owned(),
                bind_specs: bind_specs.clone(),
            },
        );
    }
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
    run_statement(ctx, state, session, stream, stmt, binds, consistency).await
}

/// Execute one parsed statement. Split out from [`run_cql`] so a `BATCH` can run
/// each member through it.
async fn run_statement(
    ctx: &ClientCtx,
    state: &Arc<Mutex<CqlState>>,
    session: &mut Session,
    stream: i16,
    stmt: Statement,
    binds: &[CqlValue],
    consistency: Consistency,
) -> Vec<u8> {
    match stmt {
        Statement::Use { keyspace } => {
            if !keyspace_exists(ctx, state, &keyspace).await {
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
            // Keyspaces are not control-plane replicated; record it process-local.
            {
                let mut guard = state.lock().await;
                guard.keyspaces.insert(keyspace.to_ascii_lowercase());
            }
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
            let cql_schema = animus_cql::schema_of(&ct);
            let control_schema = cql_schema_to_control(&cql_schema);
            let control_name = control_table_name(&keyspace, &ct.table);
            // IF NOT EXISTS: a table already present is a no-op success.
            if ct.if_not_exists && ctx.has_table_schema(&control_name) {
                return response::schema_change_result(
                    stream, "CREATED", "TABLE", &keyspace, &ct.table,
                );
            }
            match ctx.create_table_schema(control_name, control_schema).await {
                Ok(()) => {
                    response::schema_change_result(stream, "CREATED", "TABLE", &keyspace, &ct.table)
                }
                Err(msg) => response::error(stream, response::ERR_INVALID, &msg),
            }
        }
        Statement::DropTable(dt) => {
            let keyspace = match dt.keyspace.clone().or_else(|| session.keyspace.clone()) {
                Some(k) => k,
                None => {
                    return response::error(
                        stream,
                        response::ERR_INVALID,
                        "no keyspace selected; USE one or qualify the table name",
                    );
                }
            };
            let control_name = control_table_name(&keyspace, &dt.table);
            if !ctx.has_table_schema(&control_name) {
                if dt.if_exists {
                    return response::schema_change_result(
                        stream, "DROPPED", "TABLE", &keyspace, &dt.table,
                    );
                }
                return response::error(
                    stream,
                    response::ERR_INVALID,
                    &format!("table `{keyspace}.{}` does not exist", dt.table),
                );
            }
            match ctx.drop_table_schema(control_name).await {
                Ok(()) => {
                    response::schema_change_result(stream, "DROPPED", "TABLE", &keyspace, &dt.table)
                }
                Err(msg) => response::error(stream, response::ERR_SERVER, &msg),
            }
        }
        Statement::AlterTable(at) => alter_table(ctx, session, stream, at).await,
        Statement::Batch(batch) => {
            // Run each member in order, sharing the session/binds. Returns the
            // first error; otherwise a single RESULT/Void (CQL batches return
            // void). NOTE: not atomic across members — see the crate docs.
            for member in batch.statements {
                let reply = Box::pin(run_statement(
                    ctx,
                    state,
                    session,
                    stream,
                    member,
                    binds,
                    consistency,
                ))
                .await;
                if is_error_frame(&reply) {
                    return reply;
                }
            }
            response::void_result(stream)
        }
        Statement::Insert(ins) => {
            let (ks, _name, schema) = match resolve_table(
                ctx,
                ins.keyspace.as_deref(),
                session.keyspace.as_deref(),
                &ins.table,
            ) {
                Ok(t) => t,
                Err(msg) => return response::error(stream, response::ERR_INVALID, &msg),
            };
            let Some(cat) = ephemeral_catalog(&ks, &ins.table, &schema) else {
                return response::error(stream, response::ERR_SERVER, "corrupt schema");
            };
            match animus_cql::plan_insert(&cat, Some(&ks), &ins, binds) {
                Ok(plan) => run_insert(ctx, stream, plan, consistency).await,
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
        Statement::Update(upd) => {
            let (ks, _name, schema) = match resolve_table(
                ctx,
                upd.keyspace.as_deref(),
                session.keyspace.as_deref(),
                &upd.table,
            ) {
                Ok(t) => t,
                Err(msg) => return response::error(stream, response::ERR_INVALID, &msg),
            };
            let Some(cat) = ephemeral_catalog(&ks, &upd.table, &schema) else {
                return response::error(stream, response::ERR_SERVER, "corrupt schema");
            };
            let cql_schema = match control_schema_to_cql(&upd.table, &schema) {
                Some(s) => s,
                None => return response::error(stream, response::ERR_SERVER, "corrupt schema"),
            };
            match animus_cql::plan_update(&cat, Some(&ks), &upd, binds) {
                Ok(plan) => run_update(ctx, stream, plan, &cql_schema, consistency).await,
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
        Statement::Delete(del) => {
            let (ks, _name, schema) = match resolve_table(
                ctx,
                del.keyspace.as_deref(),
                session.keyspace.as_deref(),
                &del.table,
            ) {
                Ok(t) => t,
                Err(msg) => return response::error(stream, response::ERR_INVALID, &msg),
            };
            let Some(cat) = ephemeral_catalog(&ks, &del.table, &schema) else {
                return response::error(stream, response::ERR_SERVER, "corrupt schema");
            };
            let cql_schema = match control_schema_to_cql(&del.table, &schema) {
                Some(s) => s,
                None => return response::error(stream, response::ERR_SERVER, "corrupt schema"),
            };
            match animus_cql::plan_delete(&cat, Some(&ks), &del, binds) {
                Ok(plan) => run_delete(ctx, stream, plan, &cql_schema, consistency).await,
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
        Statement::Select(sel) => {
            let (ks, _name, schema) = match resolve_table(
                ctx,
                sel.keyspace.as_deref(),
                session.keyspace.as_deref(),
                &sel.table,
            ) {
                Ok(t) => t,
                Err(msg) => return response::error(stream, response::ERR_INVALID, &msg),
            };
            let Some(cat) = ephemeral_catalog(&ks, &sel.table, &schema) else {
                return response::error(stream, response::ERR_SERVER, "corrupt schema");
            };
            let cql_schema = match control_schema_to_cql(&sel.table, &schema) {
                Some(s) => s,
                None => return response::error(stream, response::ERR_SERVER, "corrupt schema"),
            };
            match animus_cql::plan_select(&cat, Some(&ks), &sel, binds) {
                Ok(plan) => run_select(ctx, stream, plan, &cql_schema, consistency).await,
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
    }
}

/// Whether `keyspace` is known: created process-locally, or implied by a
/// replicated `ks.table` schema (so a keyspace with tables is recognized even
/// after a restart, before any in-process `CREATE KEYSPACE`).
async fn keyspace_exists(ctx: &ClientCtx, state: &Arc<Mutex<CqlState>>, keyspace: &str) -> bool {
    let ks = keyspace.to_ascii_lowercase();
    {
        let guard = state.lock().await;
        if guard.keyspaces.contains(&ks) {
            return true;
        }
    }
    let prefix = format!("{ks}.");
    ctx.has_table_schema_with_prefix(&prefix)
}

/// `ALTER TABLE ... ADD`: append the new columns to the replicated schema. The
/// control plane has no in-place schema update, so this **drops and recreates**
/// the schema with the extended column list. Appending columns preserves every
/// existing column's index, and the partition-storage format keys cells by index,
/// so stored rows still decode correctly under the new schema. NOTE: drop +
/// recreate is **not atomic** — a crash between them could leave the table
/// dropped; an in-place schema-mutation `MetaCommand` is future work (ADR 0013).
async fn alter_table(ctx: &ClientCtx, session: &Session, stream: i16, at: AlterTable) -> Vec<u8> {
    let keyspace = match at.keyspace.clone().or_else(|| session.keyspace.clone()) {
        Some(k) => k,
        None => {
            return response::error(
                stream,
                response::ERR_INVALID,
                "no keyspace selected; USE one or qualify the table name",
            );
        }
    };
    let control_name = control_table_name(&keyspace, &at.table);
    let Some(mut schema) = ctx.table_schema(&control_name) else {
        return response::error(
            stream,
            response::ERR_INVALID,
            &format!("table `{keyspace}.{}` does not exist", at.table),
        );
    };
    for (name, ty) in &at.add_columns {
        if schema.column(name).is_some() {
            return response::error(
                stream,
                response::ERR_INVALID,
                &format!(
                    "column `{name}` already exists in `{keyspace}.{}`",
                    at.table
                ),
            );
        }
        schema
            .columns
            .push(ColumnDef::new(name.clone(), cql_to_column_type(*ty)));
    }
    // Drop then recreate with the extended schema (see the doc comment).
    if let Err(msg) = ctx.drop_table_schema(control_name.clone()).await {
        return response::error(stream, response::ERR_SERVER, &msg);
    }
    match ctx.create_table_schema(control_name, schema).await {
        Ok(()) => response::schema_change_result(stream, "UPDATED", "TABLE", &keyspace, &at.table),
        Err(msg) => response::error(stream, response::ERR_SERVER, &msg),
    }
}

/// Whether `reply` is a CQL `ERROR` frame (opcode byte at index 4 of the header).
fn is_error_frame(reply: &[u8]) -> bool {
    reply.get(4) == Some(&(Opcode::Error as u8))
}

/// Execute a planned `INSERT` as a partition read-modify-write: read the current
/// partition, upsert this row by its clustering key, and write the partition
/// back. Reply `RESULT/Void`.
async fn run_insert(
    ctx: &ClientCtx,
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
    stream: i16,
    plan: UpdatePlan,
    schema: &CqlSchema,
    consistency: Consistency,
) -> Vec<u8> {
    let result = mutate_partition_with_schema(ctx, &plan.key, consistency, schema, |part| {
        let clustering_values = decode_clustering_for(&plan.clustering, schema);
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
    stream: i16,
    plan: DeletePlan,
    schema: &CqlSchema,
    consistency: Consistency,
) -> Vec<u8> {
    let view = match view_for(ctx, &plan.key, consistency) {
        Ok(v) => v,
        Err(msg) => return response::error(stream, response::ERR_SERVER, &msg),
    };
    let _guard = ctx.coord_lock.lock().await;
    let (current, bytes) = match read_partition_bytes(ctx, &view, &plan.key).await {
        Ok(r) => r,
        Err(msg) => return response::error(stream, response::ERR_SERVER, &msg),
    };
    let mut part = match Partition::decode(&bytes.unwrap_or_default(), schema) {
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
    stream: i16,
    plan: ReadPlan,
    schema: &CqlSchema,
    consistency: Consistency,
) -> Vec<u8> {
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
    let part = match Partition::decode(&bytes, schema) {
        Ok(p) => p,
        Err(e) => return response::error(stream, response::ERR_SERVER, &e.to_string()),
    };
    let mut rows = Vec::new();
    for row in part.rows_matching(&plan.clustering_prefix) {
        match build_row(&plan.projection, &plan.pk_name, &plan.pk_value, schema, row) {
            Ok(cells) => rows.push(cells),
            Err(msg) => return response::error(stream, response::ERR_SERVER, &msg),
        }
    }
    response::typed_rows_multi(stream, "animus", &plan.table, &plan.projection, &rows)
}

/// Decode the typed clustering values for a row from its clustering blob, used
/// when `UPDATE` creates a previously-absent row.
fn decode_clustering_for(clustering: &[u8], schema: &CqlSchema) -> Vec<CqlValue> {
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
    schema: &CqlSchema,
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
    let schema_agnostic = CqlSchema {
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
    schema: &CqlSchema,
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
