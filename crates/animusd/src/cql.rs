//! The CQL (Cassandra) binary-protocol endpoint (ADR 0006).
//!
//! A hand-rolled server over a real tokio [`TcpListener`] that speaks a
//! practical subset of the **Apache Cassandra CQL v4 binary protocol**: it
//! reads framed requests, does the `STARTUP → READY` (and `OPTIONS →
//! SUPPORTED`) handshake, and runs `QUERY` / `PREPARE` / `EXECUTE`. Every
//! statement is parsed, type-checked, and planned by the pure, I/O-free
//! `animus_cql` crate, then routed (v1, ADR 0019) through the **leaderful CP data
//! plane** — `ClientCtx::cp_read` for reads, and (ADR 0049 Train A rung 2)
//! `cp_kind_write_raw` for every mutation's commit ([`kind_partition_write`]:
//! one `KindBatch` entry = the partition's base row or tombstone + an
//! image-less marker record) — to the per-tablet Raft group leader
//! (linearizable, forwarded cross-process), the same CP primitives the
//! plain-TCP client API and the DynamoDB endpoint use. The edge itself is
//! production-only I/O, like `ProdEnv`.
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
//!     type system; linearizable CP read / write (the requested consistency level
//!     is accepted but moot — CP is at least as strong as any level).
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
//!
//! Keyspaces **are** now control-plane replicated too (v1 A3): `CREATE KEYSPACE`
//! proposes `MetaCommand::CreateKeyspace` (durable + cluster-agreed, surviving
//! restart), and `USE`/qualifier validation reads the replicated keyspace set
//! ([`keyspace_exists`]), with a `ks.table`-prefix fallback so a keyspace
//! that has tables is still recognized. Keyspace *properties* (replication
//! strategy/factor) are not modelled — only the name namespace.
//!
//! The pure protocol/type/catalog logic stays in `animus_cql`; only the socket
//! loop and this shared state live here.

use std::collections::BTreeMap;
use std::sync::Arc;

use animus_control::{ColumnDef, ColumnType, ReplicationMode, TableSchema as ControlSchema};
use animus_cql::frame::{self, Frame, Opcode};
use animus_cql::{
    AlterTable, Catalog, Column, ColumnSpec, Consistency, CqlType, CqlValue, DeletePlan,
    InsertPlan, Partition, ReadPlan, Statement, TableSchema as CqlSchema, UpdatePlan, response,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::ClientCtx;

/// A prepared statement: the original CQL text plus the resolved bind-variable
/// column specs (so `EXECUTE` can type the bound cells). Re-parsed on `EXECUTE`
/// (cheap) so the planning path is shared with `QUERY`.
#[derive(Clone)]
struct Prepared {
    cql: String,
    bind_specs: Vec<ColumnSpec>,
}

/// Per-cluster CQL edge state shared across all connections of a cluster: just the
/// prepared-statement store, behind one async mutex (contention here is negligible
/// — a tiny in-memory map). Table *schemas* and now *keyspaces* (v1 A3) both live
/// in the control plane's replicated catalog (ADR 0013), no longer at the edge.
///
/// It is owned by the cluster's [`ClusterEdgeState`](crate::ClusterEdgeState)
/// (threaded through [`ClientCtx`]) rather than a process `OnceLock`, so two
/// in-process clusters in one test do not share prepared statements.
#[derive(Default)]
pub(crate) struct CqlState {
    /// statement id → prepared statement (id is a content hash of the text).
    /// Keyspaces are now **control-plane replicated** (v1 A3), no longer held here.
    prepared: BTreeMap<Vec<u8>, Prepared>,
}

/// Per-connection mutable state: the keyspace selected by `USE`.
#[derive(Default)]
struct Session {
    keyspace: Option<String>,
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
/// The keyspace set and prepared statements live in this node's own per-node
/// [`CqlState`](crate::ClusterEdgeState) (ADR 0031 PR2); table schemas live in
/// the control plane's replicated catalog, reachable identically from every
/// node.
pub(crate) async fn serve(listener: TcpListener, ctx: ClientCtx) {
    let state = ctx.edge.cql_state().clone();
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
    // v1 (ADR 0019): reads/writes are served by the leaderful CP plane, which is
    // **linearizable** — there is no tunable quorum to size per request. The CQL
    // consistency level is therefore accepted and satisfied (CP is at least as
    // strong as any level a client can ask for), but it no longer selects a quorum.
    let _ = consistency;
    match stmt {
        Statement::Use { keyspace } => {
            if !keyspace_exists(ctx, &keyspace).await {
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
            let lowered = keyspace.to_ascii_lowercase();
            // Reject a name that collides with the control plane's reserved
            // system keyspace (ADR 0038) up front, client-side, with a clear
            // message — the state machine also rejects this
            // (`Metadata::apply`'s `CreateKeyspace` arm), but that would
            // otherwise surface as an opaque commit-wait timeout.
            if animus_control::syskv::is_reserved_name(&lowered) {
                return response::error(
                    stream,
                    response::ERR_INVALID,
                    &format!("keyspace `{keyspace}` collides with the reserved system namespace"),
                );
            }
            // Replicated through the control plane (v1 A3): durable + cluster-agreed,
            // surviving restart (routed to the leader, so a follower-connected
            // `CREATE KEYSPACE` still commits).
            match ctx.create_keyspace(lowered).await {
                Ok(()) => {
                    response::schema_change_result(stream, "CREATED", "KEYSPACE", &keyspace, "")
                }
                Err(msg) => response::error(stream, response::ERR_INVALID, &msg),
            }
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
            // v1 (ADR 0019): every wire-created table is served by the leaderful CP
            // plane, so it is created in `ReplicationMode::Cp`.
            let control_schema = cql_schema_to_control(&cql_schema).with_mode(ReplicationMode::Cp);
            let control_name = control_table_name(&keyspace, &ct.table);
            // Reject a (keyspace-qualified) name that collides with the
            // control plane's reserved system keyspace (ADR 0038) up front,
            // client-side, with a clear message — the state machine also
            // rejects this (`Metadata::apply`'s `CreateTableSchema` arm), but
            // that would otherwise surface as an opaque commit-wait timeout.
            if animus_control::syskv::is_reserved_name(&control_name) {
                return response::error(
                    stream,
                    response::ERR_INVALID,
                    &format!(
                        "table `{keyspace}.{}` collides with the reserved system namespace",
                        ct.table
                    ),
                );
            }
            // IF NOT EXISTS: a table already present is a no-op success.
            if ct.if_not_exists && ctx.has_table_schema(&control_name) {
                return response::schema_change_result(
                    stream, "CREATED", "TABLE", &keyspace, &ct.table,
                );
            }
            match ctx
                .create_table_schema(control_name.clone(), control_schema)
                .await
            {
                Ok(()) => {
                    // Provision the table's CP tablet (ADR 0023): one tablet over the
                    // whole token ring, scoped to this table, stood up by the join-host
                    // loop. Without it the table's data ops would have nowhere to route.
                    if let Err(msg) = ctx.provision_tablet(&control_name).await {
                        return response::error(stream, response::ERR_SERVER, &msg);
                    }
                    // A CREATED result must mean the client's first INSERT serves
                    // promptly — same ack-vs-tablet-ready race as the DynamoDB
                    // edge's `CreateTable` (see `ClientCtx::await_table_serveable`):
                    // the metadata commit above races the group's asynchronous
                    // formation/election, so wait for it to actually serve before
                    // replying.
                    if let Err(msg) = ctx.await_table_serveable(&control_name).await {
                        return response::error(stream, response::ERR_SERVER, &msg);
                    }
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
            // The full drop (ADR 0024): schema out of the catalog *and* the
            // table's tablets out of the map, so every replica's GC loop
            // reclaims the table's on-disk data. (`ALTER TABLE` mutates the
            // schema in place via `ReplaceTableSchema` and never drops.)
            match ctx.drop_table(control_name).await {
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
            let (ks, name, schema) = match resolve_table(
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
                Ok(plan) => run_insert(ctx, stream, plan, &name).await,
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
        Statement::Update(upd) => {
            let (ks, name, schema) = match resolve_table(
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
                Ok(plan) => run_update(ctx, stream, plan, &cql_schema, &name).await,
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
        Statement::Delete(del) => {
            let (ks, name, schema) = match resolve_table(
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
                Ok(plan) => run_delete(ctx, stream, plan, &cql_schema, &name).await,
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
        Statement::Select(sel) => {
            let (ks, name, schema) = match resolve_table(
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
                Ok(plan) => run_select(ctx, stream, plan, &cql_schema, &name).await,
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
    }
}

/// Whether `keyspace` is known: registered in the **replicated** catalog (v1 A3),
/// or implied by a replicated `ks.table` schema (so a keyspace with tables is
/// recognized even if its `CREATE KEYSPACE` predates replicated keyspaces).
async fn keyspace_exists(ctx: &ClientCtx, keyspace: &str) -> bool {
    let ks = keyspace.to_ascii_lowercase();
    // One metadata snapshot for both checks — calling `Metadata::has_keyspace`
    // and re-deriving the table-schema fallback separately would each
    // deep-clone the replicated metadata under the Raft handle's lock.
    // Cache-tolerant (`ctx.effective_metadata()`, not
    // `ctx.control.metadata_cached()` directly) — a plain lookup, not a
    // commit-wait poll.
    let meta = ctx.effective_metadata();
    meta.has_keyspace(&ks)
        || meta
            .table_schemas()
            .any(|(name, _)| name.starts_with(&format!("{ks}.")))
}

/// `ALTER TABLE ... ADD`: append the new columns to the replicated schema via the
/// **atomic in-place replacement** (`MetaCommand::ReplaceTableSchema`, one command
/// / one apply — the former drop-then-recreate could strand the table schema-less
/// if a crash landed between the two commands, and let a concurrent reader on a
/// replica between the two applies see the table missing). Appending columns
/// preserves every existing column's index, and the partition-storage format keys
/// cells by index, so stored rows still decode correctly under the new schema.
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
    // One atomic replacement (see the doc comment) — never a schema-less window.
    match ctx.replace_table_schema(control_name, schema).await {
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
async fn run_insert(ctx: &ClientCtx, stream: i16, plan: InsertPlan, table: &str) -> Vec<u8> {
    // The partition format stores each row's clustering blob verbatim, so the
    // read-modify-write does not need the schema: existing rows round-trip
    // untyped (their blob is the map key) and this new row is merged by its
    // clustering bytes.
    let result = mutate_partition(ctx, table, &plan.key, |part| {
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
    table: &str,
) -> Vec<u8> {
    let result = mutate_partition_with_schema(ctx, table, &plan.key, schema, |part| {
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
    table: &str,
) -> Vec<u8> {
    // Read-modify-write the partition on the CP plane under the coord lock (which
    // serializes this node's RMWs so the read+write is atomic per node). The Raft
    // index is the MVCC version, so no client-assigned version is needed.
    let _guard = ctx.data().rmw_lock.lock().await;
    let bytes = match ctx.cp_read(table, plan.key.clone()).await {
        Ok(b) => b,
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
    let result = if part.is_empty() {
        // Whole partition gone → commit a CP tombstone so the key reads absent
        // (plus its marker record — a delete is a mutation like any other).
        kind_partition_write(ctx, table, &plan.key, None).await
    } else {
        kind_partition_write(ctx, table, &plan.key, Some(part.encode())).await
    };
    match result {
        Ok(()) => response::void_result(stream),
        Err(msg) => response::error(stream, response::ERR_SERVER, &msg),
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
    table: &str,
) -> Vec<u8> {
    let bytes = match ctx.cp_read(table, plan.key.clone()).await {
        Ok(v) => v,
        Err(msg) => return response::error(stream, response::ERR_SERVER, &msg),
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
/// Read-modify-write a partition under the coordinator lock: decode the current
/// partition (schema-agnostic on the write path), apply `mutate`, write it back.
async fn mutate_partition(
    ctx: &ClientCtx,
    table: &str,
    key: &[u8],
    mutate: impl FnOnce(&mut Partition),
) -> Result<(), String> {
    let schema_agnostic = CqlSchema {
        name: String::new(),
        columns: Vec::new(),
        partition_key: 0,
        clustering_keys: Vec::new(),
    };
    mutate_partition_with_schema(ctx, table, key, &schema_agnostic, mutate).await
}

/// As [`mutate_partition`] but decodes the partition against `schema` (so existing
/// rows' clustering values are typed — needed when `mutate` reads them).
///
/// The CP plane is the source of truth (ADR 0019): the coord lock serializes this
/// node's read-modify-writes so the linearizable read + CP write are atomic per
/// node, and the Raft index is the MVCC version (no client-assigned version).
async fn mutate_partition_with_schema(
    ctx: &ClientCtx,
    table: &str,
    key: &[u8],
    schema: &CqlSchema,
    mutate: impl FnOnce(&mut Partition),
) -> Result<(), String> {
    let _guard = ctx.data().rmw_lock.lock().await;
    let bytes = ctx.cp_read(table, key.to_vec()).await?;
    let mut part =
        Partition::decode(&bytes.unwrap_or_default(), schema).map_err(|e| e.to_string())?;
    mutate(&mut part);
    kind_partition_write(ctx, table, key, Some(part.encode())).await
}

/// Commit one CQL partition write through the universal kind-write path
/// (ADR 0049 Train A rung 2): one `KindBatch` Raft entry carrying the
/// partition's base row (`Some` = write-back, `None` = the whole-partition
/// tombstone) plus an image-less **marker** record — the dirty-key signal
/// every change-log consumer re-reads rows from. The change key's prefix is
/// the partition's own data-plane key (`token(pk_bytes) || pk_bytes`,
/// [`animus_cql::query::data_key`]'s shape — deliberately *this* edge's own
/// unescaped convention, not Dynamo's escaped one; the record must land in
/// the same tablet as the base row, which routing derives from these exact
/// bytes), completed at apply with the entry's own HLC. `base_sk` is empty:
/// a CQL partition is one value with no sort dimension. A CQL table can
/// carry no stream or index, so its record is always the marker shape — no
/// images branch exists on this edge, and no evaluate-at-leader funnel is
/// needed (the RMW itself stays this edge's own documented
/// node-local-`rmw_lock` scope, unchanged by this function).
///
/// The table's tablet always exists before any write reaches here: the CQL
/// `CREATE TABLE` handler provisions it synchronously (`provision_tablet`)
/// before reporting success, so unlike the Dynamo fast arm there is no
/// lazy-provisioning step.
async fn kind_partition_write(
    ctx: &ClientCtx,
    table: &str,
    key: &[u8],
    value: Option<Vec<u8>>,
) -> Result<(), String> {
    let change_log = crate::dynamo::marker_change_log(key, Vec::new());
    ctx.cp_kind_write_raw(
        table,
        vec![(animus_cp_data::KIND_BASE, key.to_vec(), value)],
        vec![change_log],
    )
    .await
}

/// The CQL edge's writes commit through the universal kind-write path (ADR
/// 0049 Train A rung 2): in-crate because the marker-record assertions need
/// `CpGroup::pending_changes` (private), like `dynamo.rs`'s own
/// `stream_write_path_tests`. Statements are driven over the **real CQL
/// socket** (a minimal per-statement frame client below — `cql_client::run`
/// splits on `;`, which would shred a `BATCH`), so the whole
/// parse/plan/RMW/commit path is exercised, not a private shortcut.
#[cfg(test)]
mod cql_kind_write_tests {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::time::Duration;

    use animus_cp_data::{KIND_FOOTPRINT, KIND_LSI};
    use animus_dynamo::ChangeRecord;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::{sleep, timeout};

    use crate::config::NodeRole;
    use crate::{ClusterConfig, Node, RoleAddrs, run_node};

    // Mirrors `dynamo::stream_write_path_tests`'s identical bring-up helpers —
    // a sibling `#[cfg(test)]` mod, so duplicated rather than shared (the
    // established per-mod precedent there).
    fn free_addrs(count: usize) -> Vec<SocketAddr> {
        let ls: Vec<std::net::TcpListener> = (0..count)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        ls.iter().map(|l| l.local_addr().unwrap()).collect()
    }

    fn cluster_config(n: usize) -> ClusterConfig {
        let addrs = free_addrs(6 * n);
        ClusterConfig {
            nodes: (0..n)
                .map(|i| RoleAddrs {
                    id: crate::config::node_id(i),
                    role: NodeRole::Both,
                    internal: addrs[6 * i],
                    client: addrs[6 * i + 1],
                    dynamo: addrs[6 * i + 2],
                    cql: addrs[6 * i + 3],
                    admin: addrs[6 * i + 4],
                    intra: addrs[6 * i + 5],
                })
                .collect(),
        }
    }

    fn single_node_config() -> ClusterConfig {
        cluster_config(1)
    }

    /// One named counter off the public `GET /metrics` text export (the
    /// DynamoDB listener) — mirrors `stream_write_path_tests`'s identical
    /// helper (sibling-mod duplication, per the precedent above).
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

    /// Bring up a single node, retrying against the documented port-TOCTOU
    /// race (`docs/engineering-lessons.md`): `single_node_config()`'s
    /// `free_addrs` probe releases its ports before the real bind, so
    /// another test binary can steal one under `cargo test --workspace`
    /// contention. Each attempt allocates a **fresh** config.
    async fn single_node(dir: &Path) -> Node {
        let mut last_err = None;
        for attempt in 0..16 {
            let config = single_node_config();
            match run_node(&config, 0, dir.join(format!("node-{attempt}"))).await {
                Ok(node) => {
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
                    return node;
                }
                Err(e) => {
                    last_err = Some(e);
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        panic!(
            "could not bring up single node after retries (ports kept getting stolen): {last_err:?}"
        );
    }

    /// Bring up an `n`-node cluster, one process per node, retrying the
    /// (allocate-fresh-ports + start-all) unit against the same port-TOCTOU
    /// race as [`single_node`] — the canonical shape also used by
    /// `tests/split_build.rs::bring_up`. On a partial failure, shut down
    /// whatever already started before retrying with fresh addresses.
    async fn bring_up_cluster(n: usize, dir: &Path) -> Vec<Node> {
        let mut last_err = None;
        for attempt in 0..16 {
            let config = cluster_config(n);
            let mut nodes = Vec::new();
            let mut failed = None;
            for i in 0..n {
                match run_node(&config, i, dir.join(format!("node-{attempt}-{i}"))).await {
                    Ok(node) => nodes.push(node),
                    Err(e) => {
                        failed = Some(e);
                        break;
                    }
                }
            }
            match failed {
                None => return nodes,
                Some(e) => {
                    for node in &nodes {
                        node.shutdown_graceful().await;
                    }
                    last_err = Some(e);
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        panic!(
            "could not bring up cluster after retries (ports kept getting stolen): {last_err:?}"
        );
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

    // Minimal CQL v4 frame client (one connection, STARTUP then one QUERY per
    // call) — the same constants/framing as `cql_client`, which is not
    // reusable here (its `run` splits statements on `;`).
    const REQUEST_VERSION: u8 = 0x04;
    const HEADER_LEN: usize = 9;
    const OP_ERROR: u8 = 0x00;
    const OP_STARTUP: u8 = 0x01;
    const OP_READY: u8 = 0x02;
    const OP_QUERY: u8 = 0x07;

    async fn write_frame(stream: &mut TcpStream, opcode: u8, body: &[u8]) {
        let mut frame = Vec::with_capacity(HEADER_LEN + body.len());
        frame.push(REQUEST_VERSION);
        frame.push(0x00);
        frame.extend_from_slice(&1i16.to_be_bytes());
        frame.push(opcode);
        frame.extend_from_slice(&(body.len() as i32).to_be_bytes());
        frame.extend_from_slice(body);
        stream.write_all(&frame).await.expect("write frame");
        stream.flush().await.expect("flush frame");
    }

    async fn read_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
        let mut header = [0u8; HEADER_LEN];
        stream.read_exact(&mut header).await.expect("read header");
        let opcode = header[4];
        let len = i32::from_be_bytes([header[5], header[6], header[7], header[8]]).max(0) as usize;
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).await.expect("read body");
        (opcode, body)
    }

    /// Run one statement (optionally after `USE ks`) on a fresh connection;
    /// panics on an ERROR reply, returns the raw reply body otherwise.
    async fn cql_exec(addr: SocketAddr, keyspace: Option<&str>, stmt: &str) -> Vec<u8> {
        let mut stream = TcpStream::connect(addr).await.expect("connect CQL");
        let mut body = Vec::new();
        body.extend_from_slice(&1u16.to_be_bytes());
        body.extend_from_slice(&(b"CQL_VERSION".len() as u16).to_be_bytes());
        body.extend_from_slice(b"CQL_VERSION");
        body.extend_from_slice(&(b"3.0.0".len() as u16).to_be_bytes());
        body.extend_from_slice(b"3.0.0");
        write_frame(&mut stream, OP_STARTUP, &body).await;
        let (opcode, _) = read_frame(&mut stream).await;
        assert_eq!(opcode, OP_READY, "STARTUP handshake failed");
        for s in keyspace
            .map(|ks| format!("USE {ks}"))
            .iter()
            .map(String::as_str)
            .chain([stmt])
        {
            let mut q = Vec::new();
            q.extend_from_slice(&(s.len() as i32).to_be_bytes());
            q.extend_from_slice(s.as_bytes());
            q.extend_from_slice(&1u16.to_be_bytes()); // consistency ONE
            q.push(0x00); // no flags
            write_frame(&mut stream, OP_QUERY, &q).await;
            let (opcode, reply) = read_frame(&mut stream).await;
            assert_ne!(
                opcode,
                OP_ERROR,
                "statement `{s}` failed: {}",
                String::from_utf8_lossy(&reply)
            );
            if s == stmt {
                return reply;
            }
        }
        unreachable!("the statement loop always returns on `stmt`")
    }

    /// ADR 0049 Train A rung 2 regression — a whole-partition `DELETE`
    /// (`kind_partition_write` with a tombstone base write, the first raw
    /// kind batch whose base value is `None`) must serve from **every**
    /// node, not only the tablet leader's. `cp_serve_forwarded`'s
    /// `KindWrite` arm used to confirm via `cp_kind_local`, whose confirm
    /// *requires* a `Some`-valued base write — so the forwarded serve
    /// erred deterministically while the leader-local serve succeeded, a
    /// leader-placement-bimodal failure (`cql_clustering` caught it by
    /// luck of election). One tablet across three nodes guarantees at
    /// least two of these deletes take the forward arm — the house
    /// forwarded-command rule: every internal RPC needs at least one
    /// non-leader-issued call in its suite.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cql_whole_partition_delete_serves_from_every_node() {
        let dir = tempfile::TempDir::new().unwrap();
        let nodes = bring_up_cluster(3, dir.path()).await;
        timeout(Duration::from_secs(20), async {
            loop {
                if nodes.iter().any(Node::is_control_leader) {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("cluster did not elect a control leader in time");

        let cql0 = nodes[0].cql_addr();
        cql_exec(cql0, None, "CREATE KEYSPACE ks").await;
        cql_exec(
            cql0,
            Some("ks"),
            "CREATE TABLE t (id text, seq int, val text, PRIMARY KEY (id, seq))",
        )
        .await;

        for (i, node) in nodes.iter().enumerate() {
            let part = format!("p{i}");
            cql_exec(
                cql0,
                Some("ks"),
                &format!("INSERT INTO t (id, seq, val) VALUES ('{part}', 1, 'x')"),
            )
            .await;
            // The regression: the delete is served by THIS node, leader or
            // not (`cql_exec` panics on an ERROR reply).
            cql_exec(
                node.cql_addr(),
                Some("ks"),
                &format!("DELETE FROM t WHERE id = '{part}'"),
            )
            .await;
        }
    }

    /// ADR 0049 Train A rung 2: every CQL mutation — `INSERT`, `UPDATE`,
    /// `DELETE` (row and whole-partition/tombstone), and each `BATCH`
    /// member — commits exactly one **image-less marker record** alongside
    /// its partition write: `marker: true`, `seeded: false`, no images,
    /// empty `base_sk` (a CQL partition is one value, no sort dimension),
    /// the key's HLC suffix completed at apply; never an LSI/footprint row;
    /// the base read path untouched.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cql_writes_emit_one_marker_record_each() {
        let dir = tempfile::TempDir::new().unwrap();
        let node = single_node(dir.path()).await;
        let cql = node.cql_addr();

        cql_exec(cql, None, "CREATE KEYSPACE ks").await;
        cql_exec(
            cql,
            Some("ks"),
            "CREATE TABLE t (id text, seq int, val text, PRIMARY KEY (id, seq))",
        )
        .await;
        let group = await_group(&node, "ks.t").await;
        assert_eq!(group.pending_changes().await.len(), 0);

        cql_exec(
            cql,
            Some("ks"),
            "INSERT INTO t (id, seq, val) VALUES ('a', 1, 'x')",
        )
        .await;
        cql_exec(
            cql,
            Some("ks"),
            "INSERT INTO t (id, seq, val) VALUES ('a', 2, 'y')",
        )
        .await;
        cql_exec(
            cql,
            Some("ks"),
            "UPDATE t SET val = 'z' WHERE id = 'a' AND seq = 1",
        )
        .await;
        // Row delete: the partition keeps row 1 — the write-back arm.
        cql_exec(cql, Some("ks"), "DELETE FROM t WHERE id = 'a' AND seq = 2").await;
        // A BATCH runs each member through the same path: one marker each.
        cql_exec(
            cql,
            Some("ks"),
            "BEGIN BATCH \
             INSERT INTO t (id, seq, val) VALUES ('b', 1, 'p'); \
             INSERT INTO t (id, seq, val) VALUES ('b', 2, 'q'); \
             APPLY BATCH",
        )
        .await;
        // Whole-partition delete: the partition empties — the tombstone arm.
        cql_exec(cql, Some("ks"), "DELETE FROM t WHERE id = 'b'").await;

        // Trim-safe accounting (ADR 0049 rung 4): the every-table hot-trim
        // arm keeps a CQL table's markers transient, so a racing trim tick
        // may already have deleted some — an emitted marker is either still
        // pending or counted by `change_log_trimmed_total`.
        let records = group.pending_changes().await;
        let trimmed = metrics_value(node.dynamo_addr(), "change_log_trimmed_total").await;
        assert_eq!(
            records.len() as u64 + trimmed,
            7,
            "exactly one marker per mutation (4 singles + 2 batch members + \
             the whole-partition tombstone delete; live {} + trimmed {trimmed})",
            records.len()
        );
        let mut prefixes = std::collections::BTreeSet::new();
        let mut hlcs = std::collections::BTreeSet::new();
        for (key, value) in &records {
            let record = ChangeRecord::decode(value).expect("marker record decodes");
            assert!(record.marker, "a CQL table's record is a marker");
            assert!(!record.seeded, "a live write is never a seed");
            assert!(record.old_image.is_none(), "a marker carries no images");
            assert!(record.new_image.is_none(), "a marker carries no images");
            assert!(
                record.base_sk.is_empty(),
                "a CQL partition has no sort dimension"
            );
            let hlc = u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap());
            assert_ne!(hlc, 0, "the HLC suffix is apply-time-completed");
            hlcs.insert(hlc);
            prefixes.insert(key[..key.len() - 8].to_vec());
        }
        assert_eq!(
            hlcs.len(),
            records.len(),
            "each live mutation gets its own commit HLC"
        );
        assert!(
            prefixes.len() <= 2,
            "the change keys share their partitions' own key prefixes \
             (two partitions were written): {prefixes:?}"
        );
        if records.len() == 7 {
            assert_eq!(
                prefixes.len(),
                2,
                "with every marker still live, both partitions' prefixes appear"
            );
        }
        assert!(
            group
                .local_scan_kind_bounded(KIND_LSI, &[], None)
                .await
                .is_empty(),
            "a CQL table never writes an LSI row"
        );
        assert!(
            group
                .local_scan_kind_bounded(KIND_FOOTPRINT, &[], None)
                .await
                .is_empty(),
            "a CQL table never writes a footprint row"
        );

        // The base read path is untouched: the updated row reads back, the
        // tombstoned partition reads empty.
        let reply = cql_exec(
            cql,
            Some("ks"),
            "SELECT val FROM t WHERE id = 'a' AND seq = 1",
        )
        .await;
        assert!(
            reply.windows(1).any(|w| w == b"z"),
            "the updated row must read back through the kind-path commit"
        );
        let reply = cql_exec(cql, Some("ks"), "SELECT val FROM t WHERE id = 'b'").await;
        assert!(
            !reply.windows(1).any(|w| w == b"p") && !reply.windows(1).any(|w| w == b"q"),
            "the tombstoned partition must read empty"
        );
    }
}
