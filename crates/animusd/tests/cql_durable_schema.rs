//! End-to-end test of the **control-plane-replicated CQL schema catalog** (ADR
//! 0013) over the real CQL binary wire.
//!
//! `CREATE TABLE` now proposes its schema into the control plane's Raft-replicated
//! catalog (rather than a per-process in-memory one), so a created table is
//! durable and cluster-agreed. This test, mirroring `durable_restart.rs` (a
//! single node pinned to fixed addresses, stopped and restarted on the same data
//! dir) and `cql_wire.rs` (the CQL framing helpers), proves:
//!
//! 1. `CREATE TABLE` + an `INSERT`'d row survive a node **restart** — the schema
//!    is recovered from the control-plane Raft WAL, so a `SELECT` after restart
//!    still resolves the table and reads the row back (the data survives via the
//!    on-disk LSM, as in `durable_restart.rs`).
//! 2. The extended CQL surface works over the wire: a `BATCH` of mutations,
//!    `ALTER TABLE ... ADD`, and `DROP TABLE`.
//!
//! Like the other `animusd` tests it uses real TCP/time and polls with timeouts.

use std::time::Duration;

use animus_cql::frame::{self, Frame, Opcode, REQUEST_VERSION};
use animusd::{Node, StorageBackend};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

// --- CQL framing helpers (shared shape with cql_wire.rs) --------------------

fn request(stream: i16, opcode: Opcode, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(REQUEST_VERSION);
    out.push(0);
    out.extend_from_slice(&stream.to_be_bytes());
    out.push(opcode as u8);
    out.extend_from_slice(&(body.len() as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn put_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn startup_body() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&1u16.to_be_bytes());
    put_string(&mut body, "CQL_VERSION");
    put_string(&mut body, "3.0.0");
    body
}

/// A `QUERY` body: `[long string]` + `[consistency] ONE` + flags.
fn query_body(cql: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(cql.len() as i32).to_be_bytes());
    body.extend_from_slice(cql.as_bytes());
    body.extend_from_slice(&0x0001u16.to_be_bytes()); // consistency = ONE
    body.push(0);
    body
}

async fn read_frame(stream: &mut TcpStream) -> Frame {
    let mut header = [0u8; frame::HEADER_LEN];
    stream.read_exact(&mut header).await.expect("read header");
    let body_len = Frame::body_len(&header).expect("body len");
    let mut body = vec![0u8; body_len];
    stream.read_exact(&mut body).await.expect("read body");
    Frame {
        version: header[0],
        flags: frame::Flags(header[1]),
        stream: i16::from_be_bytes([header[2], header[3]]),
        opcode: Opcode::from_u8(header[4]).expect("known opcode"),
        body,
    }
}

async fn round_trip(stream: &mut TcpStream, req: &[u8]) -> Frame {
    stream.write_all(req).await.expect("write request");
    stream.flush().await.expect("flush");
    read_frame(stream).await
}

async fn handshake(stream: &mut TcpStream) {
    let ready = round_trip(stream, &request(1, Opcode::Startup, &startup_body())).await;
    assert_eq!(ready.opcode, Opcode::Ready, "STARTUP → READY");
}

/// A `query` that must succeed (RESULT). Returns the frame.
async fn ok_query(stream: &mut TcpStream, id: i16, cql: &str) -> Frame {
    let f = round_trip(stream, &request(id, Opcode::Query, &query_body(cql))).await;
    assert_eq!(f.opcode, Opcode::Result, "`{cql}` should succeed: {:?}", f);
    f
}

/// Parse a `RESULT/Rows` body into its rows (per-row per-column raw cells).
fn parse_rows(body: &[u8]) -> Vec<Vec<Option<Vec<u8>>>> {
    let mut pos = 0;
    let kind = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    assert_eq!(kind, 2, "expected RESULT/Rows");
    pos += 4;
    pos += 4; // flags
    let col_count =
        i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]) as usize;
    pos += 4;
    for _ in 0..2 {
        let len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
        pos += 2 + len;
    }
    for _ in 0..col_count {
        let len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
        pos += 2 + len; // name
        pos += 2; // type option
    }
    let row_count =
        i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]) as usize;
    pos += 4;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let mut cells = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            let len = i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]);
            pos += 4;
            if len < 0 {
                cells.push(None);
            } else {
                let len = len as usize;
                cells.push(Some(body[pos..pos + len].to_vec()));
                pos += len;
            }
        }
        rows.push(cells);
    }
    rows
}

// --- node lifecycle helpers (shared shape with durable_restart.rs) ----------

async fn await_bootstrap(node: &Node) {
    let ready = async {
        loop {
            if node.is_control_leader() && !node.metadata().members.is_empty() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("node did not bootstrap in 20s");
}

async fn stop(node: Node) {
    node.shutdown_graceful().await;
    drop(node);
    sleep(Duration::from_millis(200)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cql_schema_and_row_survive_node_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let node_dir = dir.path().join("node-0");

    // --- First incarnation: CREATE TABLE (replicated) + INSERT a row. ---
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let cql_addr = config.nodes[0].cql;
    await_bootstrap(&node).await;

    {
        let mut conn = TcpStream::connect(cql_addr).await.expect("connect cql");
        handshake(&mut conn).await;
        ok_query(&mut conn, 2, "CREATE KEYSPACE app").await;
        ok_query(&mut conn, 3, "USE app").await;
        ok_query(
            &mut conn,
            4,
            "CREATE TABLE users (id int, name text, PRIMARY KEY (id))",
        )
        .await;
        ok_query(
            &mut conn,
            5,
            "INSERT INTO users (id, name) VALUES (1, 'ada')",
        )
        .await;

        // Read it back while up.
        let rows = parse_rows(
            &ok_query(&mut conn, 6, "SELECT id, name FROM users WHERE id = 1")
                .await
                .body,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_deref(), Some(&1i32.to_be_bytes()[..]));
        assert_eq!(rows[0][1].as_deref(), Some(&b"ada"[..]));
    }

    stop(node).await;

    // --- Second incarnation: SAME data dir + addresses. The CREATE TABLE schema
    // was committed to the control-plane Raft WAL, so it is recovered on restart
    // — a SELECT resolves the table (no re-CREATE) and reads the durable row. ---
    let node =
        support::restart_same_addrs(&config, 0, &node_dir, animusd::StorageBackend::default())
            .await;
    await_bootstrap(&node).await;

    {
        let mut conn = TcpStream::connect(cql_addr).await.expect("reconnect cql");
        handshake(&mut conn).await;
        // No CREATE TABLE here — the schema must have survived. Qualify the table
        // (the per-connection USE is fresh, but the replicated schema persists).
        let rows = parse_rows(
            &ok_query(&mut conn, 2, "SELECT id, name FROM app.users WHERE id = 1")
                .await
                .body,
        );
        assert_eq!(
            rows.len(),
            1,
            "the schema + row should survive the restart (got {} rows)",
            rows.len()
        );
        assert_eq!(rows[0][0].as_deref(), Some(&1i32.to_be_bytes()[..]));
        assert_eq!(rows[0][1].as_deref(), Some(&b"ada"[..]), "durable row name");

        // An INSERT against the recovered schema works (proves the schema is
        // fully usable post-restart, not just present).
        ok_query(
            &mut conn,
            3,
            "INSERT INTO app.users (id, name) VALUES (2, 'grace')",
        )
        .await;
        let rows = parse_rows(
            &ok_query(&mut conn, 4, "SELECT name FROM app.users WHERE id = 2")
                .await
                .body,
        );
        assert_eq!(rows[0][0].as_deref(), Some(&b"grace"[..]));
    }

    stop(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cql_batch_alter_drop_surface() {
    let dir = tempfile::TempDir::new().unwrap();
    let (node, config) =
        support::start_single_node(&dir.path().join("node-0"), StorageBackend::default()).await;
    let cql_addr = config.nodes[0].cql;
    await_bootstrap(&node).await;

    let mut conn = TcpStream::connect(cql_addr).await.expect("connect cql");
    handshake(&mut conn).await;
    ok_query(&mut conn, 1, "CREATE KEYSPACE app").await;
    ok_query(&mut conn, 2, "USE app").await;
    ok_query(
        &mut conn,
        3,
        "CREATE TABLE t (id int, v text, PRIMARY KEY (id))",
    )
    .await;

    // BATCH: several mutations applied together (returns RESULT/Void).
    ok_query(
        &mut conn,
        4,
        "BEGIN BATCH \
         INSERT INTO t (id, v) VALUES (1, 'a'); \
         INSERT INTO t (id, v) VALUES (2, 'b'); \
         UPDATE t SET v = 'B' WHERE id = 2; \
         APPLY BATCH",
    )
    .await;
    let rows = parse_rows(
        &ok_query(&mut conn, 5, "SELECT v FROM t WHERE id = 1")
            .await
            .body,
    );
    assert_eq!(rows[0][0].as_deref(), Some(&b"a"[..]), "batch insert 1");
    let rows = parse_rows(
        &ok_query(&mut conn, 6, "SELECT v FROM t WHERE id = 2")
            .await
            .body,
    );
    assert_eq!(
        rows[0][0].as_deref(),
        Some(&b"B"[..]),
        "batch insert+update 2"
    );

    // ALTER TABLE ... ADD a column, then write + read it.
    ok_query(&mut conn, 7, "ALTER TABLE t ADD extra bigint").await;
    ok_query(&mut conn, 8, "UPDATE t SET extra = 42 WHERE id = 1").await;
    let rows = parse_rows(
        &ok_query(&mut conn, 9, "SELECT id, v, extra FROM t WHERE id = 1")
            .await
            .body,
    );
    assert_eq!(rows[0].len(), 3, "the added column is now projectable");
    assert_eq!(
        rows[0][2].as_deref(),
        Some(&42i64.to_be_bytes()[..]),
        "added bigint column round-trips"
    );

    // DROP TABLE: afterwards a query against it errors (schema gone).
    let dropped = round_trip(
        &mut conn,
        &request(10, Opcode::Query, &query_body("DROP TABLE t")),
    )
    .await;
    assert_eq!(dropped.opcode, Opcode::Result, "DROP TABLE succeeds");
    let after = round_trip(
        &mut conn,
        &request(
            11,
            Opcode::Query,
            &query_body("SELECT v FROM t WHERE id = 1"),
        ),
    )
    .await;
    assert_eq!(
        after.opcode,
        Opcode::Error,
        "SELECT on a dropped table errors"
    );

    // DROP TABLE IF EXISTS on the already-dropped table is a no-op success.
    let again = round_trip(
        &mut conn,
        &request(12, Opcode::Query, &query_body("DROP TABLE IF EXISTS t")),
    )
    .await;
    assert_eq!(
        again.opcode,
        Opcode::Result,
        "DROP TABLE IF EXISTS is idempotent"
    );

    stop(node).await;
}

/// Read a `RESULT/Error` body's `[string]` message (a 4-byte error code
/// followed by the message), for asserting on the client-facing text.
fn error_message(body: &[u8]) -> String {
    let mut pos = 4usize;
    animus_cql::response::read_body_string(body, &mut pos).expect("error message string")
}

/// `CREATE KEYSPACE` and `CREATE TABLE` both reject a name that collides with
/// the control plane's reserved system-keyspace namespace (ADR 0038 PR1),
/// client-side, with a clear `ERR_INVALID` — not a commit-wait timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cql_rejects_reserved_namespace() {
    let dir = tempfile::TempDir::new().unwrap();
    let node_dir = dir.path().join("node-0");
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let cql_addr = config.nodes[0].cql;
    await_bootstrap(&node).await;

    let mut conn = TcpStream::connect(cql_addr).await.expect("connect cql");
    handshake(&mut conn).await;

    // CREATE KEYSPACE __animus_system is rejected outright.
    let bad_ks = round_trip(
        &mut conn,
        &request(
            2,
            Opcode::Query,
            &query_body("CREATE KEYSPACE __animus_system"),
        ),
    )
    .await;
    assert_eq!(
        bad_ks.opcode,
        Opcode::Error,
        "reserved keyspace name should ERROR"
    );
    assert!(
        error_message(&bad_ks.body).contains("reserved system namespace"),
        "expected a clear message, got: {}",
        error_message(&bad_ks.body)
    );

    // An ordinary keyspace works, but a table whose keyspace-qualified name
    // collides with the reserved namespace is still rejected at CREATE TABLE.
    ok_query(&mut conn, 3, "CREATE KEYSPACE app").await;
    let bad_table = round_trip(
        &mut conn,
        &request(
            4,
            Opcode::Query,
            &query_body("CREATE TABLE __animus_system.t (id int, PRIMARY KEY (id))"),
        ),
    )
    .await;
    assert_eq!(
        bad_table.opcode,
        Opcode::Error,
        "reserved-namespace-qualified table name should ERROR"
    );
    assert!(
        error_message(&bad_table.body).contains("reserved system namespace"),
        "expected a clear message, got: {}",
        error_message(&bad_table.body)
    );

    // An ordinary table in an ordinary keyspace is unaffected.
    let ok_table = round_trip(
        &mut conn,
        &request(
            5,
            Opcode::Query,
            &query_body("CREATE TABLE app.users (id int, PRIMARY KEY (id))"),
        ),
    )
    .await;
    assert_eq!(
        ok_table.opcode,
        Opcode::Result,
        "ordinary CREATE TABLE should succeed"
    );

    stop(node).await;
}
