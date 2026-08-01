//! End-to-end test of the CQL binary-protocol endpoint over real TCP.
//!
//! Starts a 3-node in-process cluster, then drives the `STARTUP → READY`
//! handshake and an `INSERT` / `SELECT` round trip against one node's `cql`
//! endpoint by speaking the actual Cassandra CQL v4 binary protocol (a framed
//! `STARTUP`, then framed `QUERY`s). Like the other `custosd` tests this uses
//! real time and sockets, so it polls with generous timeouts.

use std::time::Duration;

use custos_cql::frame::{self, Frame, Opcode, REQUEST_VERSION};
use custosd::{Node, bind_cluster, start_cluster};
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

/// A `[string]` (u16 length prefix).
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

/// Read one response frame from the stream.
async fn read_frame(stream: &mut TcpStream) -> Frame {
    let mut header = [0u8; frame::HEADER_LEN];
    stream.read_exact(&mut header).await.expect("read header");
    let body_len = Frame::body_len(&header).expect("body len");
    let mut body = vec![0u8; body_len];
    stream.read_exact(&mut body).await.expect("read body");
    // Decode tolerantly: the response uses version 0x84, which `Frame::decode`
    // rejects (it only decodes requests), so build the struct directly here.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cql_wire_startup_insert_select_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound, 2, 2); // R = W = 2 over 3 replicas
    await_bootstrap(&nodes).await;

    let addr0 = nodes[0].cql_addr();
    let addr1 = nodes[1].cql_addr();

    // Handshake on node 0.
    let mut conn0 = TcpStream::connect(addr0).await.expect("connect cql node 0");
    let ready = round_trip(&mut conn0, &request(1, Opcode::Startup, &startup_body())).await;
    assert_eq!(ready.opcode, Opcode::Ready, "STARTUP should yield READY");

    // OPTIONS → SUPPORTED.
    let supported = round_trip(&mut conn0, &request(2, Opcode::Options, &[])).await;
    assert_eq!(supported.opcode, Opcode::Supported);
    assert!(
        supported.body.windows(5).any(|w| w == b"3.0.0"),
        "SUPPORTED should advertise CQL_VERSION 3.0.0"
    );

    // INSERT a row on node 0.
    let insert = query_body("INSERT INTO users (pk, v) VALUES ('u1', 'Ada')");
    let result = round_trip(&mut conn0, &request(3, Opcode::Query, &insert)).await;
    assert_eq!(result.opcode, Opcode::Result, "INSERT should yield RESULT");
    // RESULT/Void: the body is just the kind (0x0001).
    assert_eq!(&result.body[..4], &1i32.to_be_bytes());

    // SELECT it back on node 1 (quorum read across the cluster), after its own
    // handshake.
    let mut conn1 = TcpStream::connect(addr1).await.expect("connect cql node 1");
    let ready = round_trip(&mut conn1, &request(1, Opcode::Startup, &startup_body())).await;
    assert_eq!(ready.opcode, Opcode::Ready);

    let select = query_body("SELECT * FROM users WHERE pk = 'u1'");
    let rows = round_trip(&mut conn1, &request(2, Opcode::Query, &select)).await;
    assert_eq!(rows.opcode, Opcode::Result);
    // RESULT/Rows: kind 0x0002, and the stored value bytes ride along.
    assert_eq!(&rows.body[..4], &2i32.to_be_bytes());
    assert!(
        contains_subslice(&rows.body, b"Ada"),
        "SELECT should return the inserted value `Ada`; got {:?}",
        rows.body
    );
    assert!(
        contains_subslice(&rows.body, b"u1"),
        "SELECT should return the partition key `u1`"
    );

    // A missing key yields a Rows result with zero rows.
    let select_missing = query_body("SELECT * FROM users WHERE pk = 'nobody'");
    let empty = round_trip(&mut conn1, &request(3, Opcode::Query, &select_missing)).await;
    assert_eq!(empty.opcode, Opcode::Result);
    assert_eq!(
        &empty.body[..4],
        &2i32.to_be_bytes(),
        "should be a Rows result"
    );
    assert!(
        !contains_subslice(&empty.body, b"Ada"),
        "a missing key must not carry the value of another row"
    );

    // An unsupported statement is a CQL ERROR, not a panic.
    let bad = query_body("DROP TABLE users");
    let err = round_trip(&mut conn1, &request(4, Opcode::Query, &bad)).await;
    assert_eq!(err.opcode, Opcode::Error, "unsupported query should ERROR");
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}
