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
    Catalog, ColumnSpec, CqlType, CqlValue, ReadPlan, Statement, WritePlan, response,
};
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
            Ok(req) => run_cql(ctx, state, session, stream, &req.cql, &[]).await,
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
    run_cql(ctx, state, session, stream, &prepared.cql, &binds).await
}

/// Parse, plan, and execute a CQL statement (shared by `QUERY` and `EXECUTE`).
/// `binds` supplies the values for any `?` markers.
async fn run_cql(
    ctx: &ClientCtx,
    state: &Arc<Mutex<CqlState>>,
    session: &mut Session,
    stream: i16,
    cql: &str,
    binds: &[CqlValue],
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
                Ok(plan) => run_insert(ctx, stream, plan).await,
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
        Statement::Select(sel) => {
            let plan = {
                let guard = state.lock().await;
                animus_cql::plan_select(&guard.catalog, session.keyspace.as_deref(), &sel, binds)
            };
            match plan {
                Ok(plan) => run_select(ctx, stream, plan).await,
                Err(e) => response::error(stream, response::ERR_INVALID, &e.to_string()),
            }
        }
    }
}

/// Execute a planned `INSERT`: quorum-write the encoded row, reply `RESULT/Void`.
async fn run_insert(ctx: &ClientCtx, stream: i16, plan: WritePlan) -> Vec<u8> {
    match quorum_write(ctx, &plan.key, &plan.value).await {
        Ok(()) => response::void_result(stream),
        Err(msg) => response::error(stream, response::ERR_SERVER, &msg),
    }
}

/// Execute a planned `SELECT`: quorum-read the row, decode the projected cells,
/// reply `RESULT/Rows`.
async fn run_select(ctx: &ClientCtx, stream: i16, plan: ReadPlan) -> Vec<u8> {
    let read = match quorum_read(ctx, &plan.key).await {
        Ok(r) => r,
        Err(msg) => return response::error(stream, response::ERR_SERVER, &msg),
    };
    let ReadPlan {
        table,
        projection,
        pk_value,
        pk_name,
        ..
    } = plan;
    match read {
        Some(bytes) => {
            let cells = match build_row(&projection, &pk_name, &pk_value, &bytes) {
                Ok(c) => c,
                Err(msg) => return response::error(stream, response::ERR_SERVER, &msg),
            };
            response::typed_rows_result(stream, "animus", &table, &projection, Some(&cells))
        }
        None => response::typed_rows_result(stream, "animus", &table, &projection, None),
    }
}

/// Build the projected row cells from a stored row value. Each projected column
/// is either the partition key (echoed from the predicate value) or a non-key
/// column looked up in the decoded row by its schema index (the row blob is
/// keyed by schema index; see `animus_cql::plan`).
fn build_row(
    projection: &[ColumnSpec],
    pk_name: &str,
    pk_value: &CqlValue,
    stored: &[u8],
) -> Result<Vec<Option<Vec<u8>>>, String> {
    let decoded = animus_cql::decode_row(stored).map_err(|e| e.to_string())?;
    let mut cells = Vec::with_capacity(projection.len());
    for spec in projection {
        if spec.name.eq_ignore_ascii_case(pk_name) {
            let cell = encode_typed(spec.ty, pk_value)?;
            cells.push(Some(cell));
        } else {
            match decoded.get(&spec.schema_index) {
                Some(bytes) => cells.push(Some(bytes.clone())),
                None => cells.push(None),
            }
        }
    }
    Ok(cells)
}

fn encode_typed(ty: CqlType, value: &CqlValue) -> Result<Vec<u8>, String> {
    ty.encode(value).map_err(|e| e.to_string())
}

/// Quorum write through the shared coordinator (same quorum-derived version as
/// the plain-TCP and DynamoDB write paths).
async fn quorum_write(ctx: &ClientCtx, key: &[u8], value: &[u8]) -> Result<(), String> {
    let view = ctx
        .view_for(key)
        .ok_or_else(|| "no tablet covers this key yet (cluster still bootstrapping)".to_owned())?;
    let _guard = ctx.coord_lock.lock().await;
    let current = ctx
        .coordinator
        .read_version(&view, key, CQL_TIMEOUT)
        .await
        .ok_or_else(|| "could not read current version".to_owned())?;
    let ok = ctx
        .coordinator
        .write(&view, key, value, current + 1, CQL_TIMEOUT)
        .await;
    if ok {
        Ok(())
    } else {
        Err("write did not reach a quorum".to_owned())
    }
}

/// Quorum read through the shared coordinator.
async fn quorum_read(ctx: &ClientCtx, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
    let view = ctx
        .view_for(key)
        .ok_or_else(|| "no tablet covers this key yet (cluster still bootstrapping)".to_owned())?;
    let _guard = ctx.coord_lock.lock().await;
    match ctx.coordinator.read(&view, key, CQL_TIMEOUT).await {
        animus_data::ReadResult::Value(v) => Ok(v),
        animus_data::ReadResult::Failed => Err("read did not reach a quorum".to_owned()),
    }
}
