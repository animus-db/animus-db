//! The CQL (Cassandra) binary-protocol endpoint (ADR 0006).
//!
//! A minimal, hand-rolled server over a real tokio [`TcpListener`] that speaks
//! the **Apache Cassandra CQL v4 binary protocol**: it reads framed requests,
//! does the `STARTUP → READY` (and `OPTIONS → SUPPORTED`) handshake, and runs a
//! tiny `QUERY` path. `INSERT` and `SELECT` are decoded by the pure, I/O-free
//! `custos_cql` crate and routed through the **same** quorum coordinator the
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
//! ## Supported subset and storage mapping
//!
//! - `STARTUP` (any options) → `READY` (no authentication).
//! - `OPTIONS` → `SUPPORTED` (CQL 3.0.0, no compression).
//! - `QUERY`:
//!   - `INSERT INTO t (pk, v) VALUES (<pk>, <v>)` → quorum write, replies
//!     `RESULT/Void`.
//!   - `SELECT * FROM t WHERE pk = <pk>` → quorum read, replies `RESULT/Rows`
//!     with the single `(pk, v)` row (or an empty result set).
//!
//! There is no schema catalog (`CREATE TABLE`) yet, so a row is the fixed
//! `(pk, v)` convention documented in `custos_cql`: the data-plane key is
//! `escape(table) || pk_bytes` and the stored value is the `v` column's bytes.
//! The data plane has no native delete, so an empty-string value written by a
//! future delete path would read back present; deletes are out of scope here.

use std::time::Duration;

use custos_cql::frame::{self, Frame, Opcode};
use custos_cql::{Query, parse_query, response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::ClientCtx;

const CQL_TIMEOUT: Duration = Duration::from_secs(5);

/// Accept loop for the CQL endpoint. Each connection is handled on its own task.
pub(crate) async fn serve(listener: TcpListener, ctx: ClientCtx) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_conn(stream, ctx).await {
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

async fn handle_conn(mut stream: TcpStream, ctx: ClientCtx) -> std::io::Result<()> {
    loop {
        let Some(frame) = read_frame(&mut stream).await? else {
            return Ok(()); // clean EOF
        };
        let response = dispatch(&ctx, &frame).await;
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
async fn dispatch(ctx: &ClientCtx, frame: &Frame) -> Vec<u8> {
    let stream = frame.stream;
    match frame.opcode {
        Opcode::Startup => {
            // We accept any STARTUP options (no compression / auth negotiation).
            match response::parse_startup(&frame.body) {
                Ok(_) => response::ready(stream),
                Err(e) => response::error(stream, response::ERR_PROTOCOL, &e.to_string()),
            }
        }
        Opcode::Options => response::supported(stream),
        Opcode::Query => match response::parse_query_request(&frame.body) {
            Ok(req) => run_query(ctx, stream, &req.cql).await,
            Err(e) => response::error(stream, response::ERR_PROTOCOL, &e.to_string()),
        },
        // The client should only ever send STARTUP/OPTIONS/QUERY; anything else
        // is a protocol error in this subset.
        other => response::error(
            stream,
            response::ERR_PROTOCOL,
            &format!("unexpected opcode {other:?}"),
        ),
    }
}

/// Parse and execute a CQL `QUERY` string against the data plane.
async fn run_query(ctx: &ClientCtx, stream: i16, cql: &str) -> Vec<u8> {
    let query = match parse_query(cql) {
        Ok(q) => q,
        Err(e) => return response::error(stream, response::ERR_INVALID, &e.to_string()),
    };
    match query {
        Query::Insert { table, pk, value } => {
            let key = custos_cql::data_key(&table, &pk);
            match quorum_write(ctx, &key, value.as_bytes()).await {
                Ok(()) => response::void_result(stream),
                Err(msg) => response::error(stream, response::ERR_SERVER, &msg),
            }
        }
        Query::Select { table, pk } => {
            let key = custos_cql::data_key(&table, &pk);
            match quorum_read(ctx, &key).await {
                Ok(Some(bytes)) => {
                    let value = String::from_utf8_lossy(&bytes);
                    response::rows_result(stream, &table, Some((&pk, &value)))
                }
                Ok(None) => response::rows_result(stream, &table, None),
                Err(msg) => response::error(stream, response::ERR_SERVER, &msg),
            }
        }
    }
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
        custos_data::ReadResult::Value(v) => Ok(v),
        custos_data::ReadResult::Failed => Err("read did not reach a quorum".to_owned()),
    }
}
