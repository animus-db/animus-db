//! End-to-end test of the CQL binary-protocol endpoint over real TCP.
//!
//! Starts a 3-node in-process cluster, then drives the full prepare/execute path
//! a real CQL driver uses, speaking the actual Cassandra CQL v4 binary protocol:
//! `STARTUP → READY`, `CREATE KEYSPACE`/`USE`/`CREATE TABLE` (recording a typed
//! schema), `PREPARE` an `INSERT`, `EXECUTE` it with typed bound values, then
//! `SELECT` the row back and assert the typed columns round-trip. Like the other
//! `animusd` tests this uses real time and sockets, so it polls with generous
//! timeouts.

use std::time::Duration;

use animus_cql::frame::{self, Frame, Opcode, REQUEST_VERSION};
use animusd::{Node, bind_cluster, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// Wait until every node has the bootstrap tablet replicated, or panic.
async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            let leader = nodes.iter().any(Node::is_control_leader);
            let everyone_has_tablet = nodes.iter().all(|n| !n.metadata().tablets.is_empty());
            if leader && everyone_has_tablet {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not elect a leader and bootstrap within 20s");
}

/// Encode a CQL v4 **request** frame.
fn request(stream: i16, opcode: Opcode, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(REQUEST_VERSION);
    out.push(0); // no flags
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

/// A `STARTUP` body: `{"CQL_VERSION": "3.0.0"}`.
fn startup_body() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&1u16.to_be_bytes());
    put_string(&mut body, "CQL_VERSION");
    put_string(&mut body, "3.0.0");
    body
}

/// A `QUERY` body: a `[long string]` query + a `[consistency]` (ONE) + flags.
fn query_body(cql: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(cql.len() as i32).to_be_bytes());
    body.extend_from_slice(cql.as_bytes());
    body.extend_from_slice(&0x0001u16.to_be_bytes()); // consistency = ONE
    body.push(0); // query flags = none
    body
}

/// A `PREPARE` body: a single `[long string]` CQL statement.
fn prepare_body(cql: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(cql.len() as i32).to_be_bytes());
    body.extend_from_slice(cql.as_bytes());
    body
}

/// An `EXECUTE` body: `[short bytes] id`, `[consistency]`, flags (VALUES set),
/// `[short] n`, then each value as a `[bytes]` cell.
fn execute_body(id: &[u8], values: &[Vec<u8>]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(id.len() as u16).to_be_bytes());
    body.extend_from_slice(id);
    body.extend_from_slice(&0x0001u16.to_be_bytes()); // consistency = ONE
    body.push(0x01); // flags: VALUES
    body.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for v in values {
        body.extend_from_slice(&(v.len() as i32).to_be_bytes());
        body.extend_from_slice(v);
    }
    body
}

/// Read one response frame from the stream.
async fn read_frame(stream: &mut TcpStream) -> Frame {
    let mut header = [0u8; frame::HEADER_LEN];
    stream.read_exact(&mut header).await.expect("read header");
    let body_len = Frame::body_len(&header).expect("body len");
    let mut body = vec![0u8; body_len];
    stream.read_exact(&mut body).await.expect("read body");
    // The response uses version 0x84, which `Frame::decode` rejects (it only
    // decodes requests), so build the struct directly here.
    Frame {
        version: header[0],
        flags: frame::Flags(header[1]),
        stream: i16::from_be_bytes([header[2], header[3]]),
        opcode: Opcode::from_u8(header[4]).expect("known opcode"),
        body,
    }
}

/// Send a request frame and read the single response frame.
async fn round_trip(stream: &mut TcpStream, req: &[u8]) -> Frame {
    stream.write_all(req).await.expect("write request");
    stream.flush().await.expect("flush");
    read_frame(stream).await
}

/// Extract the prepared-statement id from a `RESULT/Prepared` body:
/// kind(i32=4) | `[short bytes] id`.
fn parse_prepared_id(body: &[u8]) -> Vec<u8> {
    assert_eq!(&body[..4], &4i32.to_be_bytes(), "expected RESULT/Prepared");
    let len = u16::from_be_bytes([body[4], body[5]]) as usize;
    body[6..6 + len].to_vec()
}

/// Parse a `RESULT/Rows` body into its single row's cells (as raw bytes per
/// column), or `None` if the result set is empty. Returns `(column_count, cells)`.
fn parse_single_row(body: &[u8]) -> Option<Vec<Option<Vec<u8>>>> {
    let mut pos = 0;
    let kind = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    assert_eq!(kind, 2, "expected RESULT/Rows");
    pos += 4;
    let _flags = i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]);
    pos += 4;
    let col_count =
        i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]) as usize;
    pos += 4;
    // Global table spec: [string keyspace][string table].
    for _ in 0..2 {
        let len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
        pos += 2 + len;
    }
    // Column specs: [string name][short type].
    for _ in 0..col_count {
        let len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
        pos += 2 + len; // name
        pos += 2; // type option (unparameterized)
    }
    let rows = i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]);
    pos += 4;
    if rows == 0 {
        return None;
    }
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
    Some(cells)
}

async fn handshake(stream: &mut TcpStream) {
    let ready = round_trip(stream, &request(1, Opcode::Startup, &startup_body())).await;
    assert_eq!(ready.opcode, Opcode::Ready, "STARTUP should yield READY");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cql_wire_prepare_execute_typed_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap(); // R = W = 2 over 3 replicas
    await_bootstrap(&nodes).await;

    let addr0 = nodes[0].cql_addr();
    let addr1 = nodes[1].cql_addr();

    // Handshake + schema setup on node 0.
    let mut conn0 = TcpStream::connect(addr0).await.expect("connect cql node 0");
    handshake(&mut conn0).await;

    // OPTIONS → SUPPORTED.
    let supported = round_trip(&mut conn0, &request(2, Opcode::Options, &[])).await;
    assert_eq!(supported.opcode, Opcode::Supported);

    // CREATE KEYSPACE, USE, CREATE TABLE — record a typed schema.
    let ck = round_trip(
        &mut conn0,
        &request(3, Opcode::Query, &query_body("CREATE KEYSPACE app")),
    )
    .await;
    assert_eq!(ck.opcode, Opcode::Result, "CREATE KEYSPACE should succeed");

    let use_ks = round_trip(
        &mut conn0,
        &request(4, Opcode::Query, &query_body("USE app")),
    )
    .await;
    assert_eq!(use_ks.opcode, Opcode::Result, "USE should succeed");
    assert_eq!(&use_ks.body[..4], &3i32.to_be_bytes(), "SetKeyspace result");

    let create = round_trip(
        &mut conn0,
        &request(
            5,
            Opcode::Query,
            &query_body("CREATE TABLE users (id int, name text, active boolean, PRIMARY KEY (id))"),
        ),
    )
    .await;
    assert_eq!(create.opcode, Opcode::Result, "CREATE TABLE should succeed");

    // PREPARE an INSERT with three bind markers.
    let prepared = round_trip(
        &mut conn0,
        &request(
            6,
            Opcode::Prepare,
            &prepare_body("INSERT INTO users (id, name, active) VALUES (?, ?, ?)"),
        ),
    )
    .await;
    assert_eq!(prepared.opcode, Opcode::Result, "PREPARE should succeed");
    let id = parse_prepared_id(&prepared.body);
    assert!(!id.is_empty(), "prepared id should be non-empty");

    // EXECUTE with typed values: id=7 (int), name='Ada' (text), active=true.
    let values = vec![
        7i32.to_be_bytes().to_vec(),
        b"Ada".to_vec(),
        vec![1u8], // boolean true
    ];
    let exec = round_trip(
        &mut conn0,
        &request(7, Opcode::Execute, &execute_body(&id, &values)),
    )
    .await;
    assert_eq!(exec.opcode, Opcode::Result, "EXECUTE should yield RESULT");
    assert_eq!(&exec.body[..4], &1i32.to_be_bytes(), "INSERT → RESULT/Void");

    // SELECT it back on node 1 (quorum read across the cluster) after its own
    // handshake + USE.
    let mut conn1 = TcpStream::connect(addr1).await.expect("connect cql node 1");
    handshake(&mut conn1).await;
    let use1 = round_trip(
        &mut conn1,
        &request(1, Opcode::Query, &query_body("USE app")),
    )
    .await;
    assert_eq!(use1.opcode, Opcode::Result);

    // The CREATE TABLE committed on node 0's leader; node 1 resolves the table
    // from its own replicated `Metadata`, which may still be catching up the
    // committed entry, so poll the cross-node SELECT until the schema has
    // replicated (it returns an error frame, not a RESULT, until then).
    let rows = {
        let mut stream = 2i16;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let rows = round_trip(
                &mut conn1,
                &request(
                    stream,
                    Opcode::Query,
                    &query_body("SELECT * FROM users WHERE id = 7"),
                ),
            )
            .await;
            if rows.opcode == Opcode::Result {
                break rows;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "node 1 never saw the replicated `users` schema within 10s",
            );
            stream += 1;
            sleep(Duration::from_millis(50)).await;
        }
    };
    assert_eq!(rows.opcode, Opcode::Result);
    let cells = parse_single_row(&rows.body).expect("row should be present");
    assert_eq!(cells.len(), 3, "id, name, active");
    // id = 7 (int, 4 BE bytes).
    assert_eq!(cells[0].as_deref(), Some(&7i32.to_be_bytes()[..]), "id");
    // name = "Ada" (text bytes).
    assert_eq!(cells[1].as_deref(), Some(&b"Ada"[..]), "name");
    // active = true (1 byte 0x01).
    assert_eq!(cells[2].as_deref(), Some(&[1u8][..]), "active");

    // A projection of a subset of typed columns also round-trips.
    let proj = round_trip(
        &mut conn1,
        &request(
            3,
            Opcode::Query,
            &query_body("SELECT name, id FROM users WHERE id = 7"),
        ),
    )
    .await;
    let proj_cells = parse_single_row(&proj.body).expect("projected row present");
    assert_eq!(proj_cells.len(), 2);
    assert_eq!(proj_cells[0].as_deref(), Some(&b"Ada"[..]), "name first");
    assert_eq!(
        proj_cells[1].as_deref(),
        Some(&7i32.to_be_bytes()[..]),
        "id second"
    );

    // A missing key yields an empty Rows result.
    let missing = round_trip(
        &mut conn1,
        &request(
            4,
            Opcode::Query,
            &query_body("SELECT * FROM users WHERE id = 999"),
        ),
    )
    .await;
    assert_eq!(missing.opcode, Opcode::Result);
    assert!(
        parse_single_row(&missing.body).is_none(),
        "missing key → zero rows"
    );

    // EXECUTE on conn1 (a different connection) using the same content-addressed
    // id proves the prepared store is shared across connections.
    let values2 = vec![8i32.to_be_bytes().to_vec(), b"Grace".to_vec(), vec![0u8]];
    let exec2 = round_trip(
        &mut conn1,
        &request(5, Opcode::Execute, &execute_body(&id, &values2)),
    )
    .await;
    assert_eq!(
        exec2.opcode,
        Opcode::Result,
        "cross-connection EXECUTE works"
    );
    let back = round_trip(
        &mut conn1,
        &request(
            6,
            Opcode::Query,
            &query_body("SELECT name FROM users WHERE id = 8"),
        ),
    )
    .await;
    let back_cells = parse_single_row(&back.body).expect("row present");
    assert_eq!(back_cells[0].as_deref(), Some(&b"Grace"[..]));

    // An unsupported statement is a CQL ERROR, not a panic. (`DROP TABLE` is now
    // supported, so use a statement still outside the subset.)
    let bad = round_trip(
        &mut conn1,
        &request(7, Opcode::Query, &query_body("TRUNCATE users")),
    )
    .await;
    assert_eq!(bad.opcode, Opcode::Error, "unsupported query should ERROR");

    // A type-mismatched EXECUTE (text where int expected) is a clean ERROR.
    let bad_vals = vec![b"notanint".to_vec(), b"x".to_vec(), vec![1u8]];
    let bad_exec = round_trip(
        &mut conn1,
        &request(8, Opcode::Execute, &execute_body(&id, &bad_vals)),
    )
    .await;
    assert_eq!(
        bad_exec.opcode,
        Opcode::Error,
        "a wrong-width int cell should ERROR, not panic"
    );
}
