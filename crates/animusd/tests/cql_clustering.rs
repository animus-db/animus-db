//! End-to-end test of the CQL clustering-key + `UPDATE`/`DELETE` surface over
//! real TCP.
//!
//! Starts a 3-node in-process cluster, then over the actual Cassandra CQL v4
//! binary protocol: `STARTUP → READY`, `CREATE KEYSPACE`/`USE`, a `CREATE TABLE`
//! with a **compound primary key** (`PRIMARY KEY (room, seq)`), several
//! `INSERT`s into the same partition out of clustering order, a `SELECT` that
//! returns them **clustering-ordered**, a single-row `SELECT` by full primary
//! key, an `UPDATE` of a non-key column, and `DELETE`s (one row, then whole
//! partition). Like the other `animusd` tests this uses real time and sockets,
//! so it polls with generous timeouts.

use std::time::Duration;

use animus_cql::frame::{self, Frame, Opcode, REQUEST_VERSION};
use animusd::{Node, bind_cluster, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

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

/// A `QUERY` body with an explicit consistency level (so the test also exercises
/// the consistency → quorum mapping path: QUORUM over 3 replicas = 2).
fn query_body_cl(cql: &str, consistency: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(cql.len() as i32).to_be_bytes());
    body.extend_from_slice(cql.as_bytes());
    body.extend_from_slice(&consistency.to_be_bytes());
    body.push(0); // query flags = none
    body
}

/// QUORUM = 0x0004.
fn query_body(cql: &str) -> Vec<u8> {
    query_body_cl(cql, 0x0004)
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

/// Parse a `RESULT/Rows` body into all rows' cells (raw bytes per column).
fn parse_rows(body: &[u8]) -> Vec<Vec<Option<Vec<u8>>>> {
    let mut pos = 0;
    let kind = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    assert_eq!(kind, 2, "expected RESULT/Rows");
    pos += 4;
    let _flags = i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]);
    pos += 4;
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

async fn handshake(stream: &mut TcpStream) {
    let ready = round_trip(stream, &request(1, Opcode::Startup, &startup_body())).await;
    assert_eq!(ready.opcode, Opcode::Ready);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cql_clustering_update_delete_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound, 2, 2).await.unwrap();
    await_bootstrap(&nodes).await;

    let mut conn = TcpStream::connect(nodes[0].cql_addr())
        .await
        .expect("connect cql");
    handshake(&mut conn).await;

    // CREATE KEYSPACE / USE.
    assert_eq!(
        round_trip(
            &mut conn,
            &request(2, Opcode::Query, &query_body("CREATE KEYSPACE app"))
        )
        .await
        .opcode,
        Opcode::Result
    );
    assert_eq!(
        round_trip(
            &mut conn,
            &request(3, Opcode::Query, &query_body("USE app"))
        )
        .await
        .opcode,
        Opcode::Result
    );

    // CREATE TABLE with a compound primary key (partition `room`, clustering `seq`).
    let create = round_trip(
        &mut conn,
        &request(
            4,
            Opcode::Query,
            &query_body(
                "CREATE TABLE events (room text, seq int, msg text, PRIMARY KEY (room, seq))",
            ),
        ),
    )
    .await;
    assert_eq!(create.opcode, Opcode::Result, "CREATE TABLE clustered");

    // INSERT several rows into one partition, out of clustering order.
    for (i, (seq, msg)) in [(3, "third"), (1, "first"), (2, "second")]
        .iter()
        .enumerate()
    {
        let cql = format!("INSERT INTO events (room, seq, msg) VALUES ('r1', {seq}, '{msg}')");
        let ins = round_trip(
            &mut conn,
            &request(10 + i as i16, Opcode::Query, &query_body(&cql)),
        )
        .await;
        assert_eq!(ins.opcode, Opcode::Result, "INSERT {seq}");
        assert_eq!(&ins.body[..4], &1i32.to_be_bytes(), "INSERT → Void");
    }
    // A second partition, to prove SELECT only returns the addressed partition.
    let _ = round_trip(
        &mut conn,
        &request(
            19,
            Opcode::Query,
            &query_body("INSERT INTO events (room, seq, msg) VALUES ('r2', 1, 'other')"),
        ),
    )
    .await;

    // SELECT * WHERE room = 'r1' → three rows, clustering-ordered (seq 1,2,3).
    let rows = parse_rows(
        &round_trip(
            &mut conn,
            &request(
                20,
                Opcode::Query,
                &query_body("SELECT * FROM events WHERE room = 'r1'"),
            ),
        )
        .await
        .body,
    );
    assert_eq!(rows.len(), 3, "three rows in partition r1");
    let seqs: Vec<i32> = rows
        .iter()
        .map(|r| {
            let b = r[1].as_deref().expect("seq cell");
            i32::from_be_bytes([b[0], b[1], b[2], b[3]])
        })
        .collect();
    assert_eq!(seqs, vec![1, 2, 3], "rows come back clustering-ordered");
    // Column order is room(text), seq(int), msg(text); check first row's values.
    assert_eq!(rows[0][0].as_deref(), Some(&b"r1"[..]), "room echoed");
    assert_eq!(rows[0][2].as_deref(), Some(&b"first"[..]), "msg of seq 1");

    // SELECT one row by full primary key.
    let one = parse_rows(
        &round_trip(
            &mut conn,
            &request(
                21,
                Opcode::Query,
                &query_body("SELECT msg FROM events WHERE room = 'r1' AND seq = 2"),
            ),
        )
        .await
        .body,
    );
    assert_eq!(one.len(), 1);
    assert_eq!(one[0][0].as_deref(), Some(&b"second"[..]));

    // UPDATE the non-key column of one row.
    let upd = round_trip(
        &mut conn,
        &request(
            22,
            Opcode::Query,
            &query_body("UPDATE events SET msg = 'edited' WHERE room = 'r1' AND seq = 2"),
        ),
    )
    .await;
    assert_eq!(upd.opcode, Opcode::Result, "UPDATE");
    assert_eq!(&upd.body[..4], &1i32.to_be_bytes(), "UPDATE → Void");

    let after_upd = parse_rows(
        &round_trip(
            &mut conn,
            &request(
                23,
                Opcode::Query,
                &query_body("SELECT msg FROM events WHERE room = 'r1' AND seq = 2"),
            ),
        )
        .await
        .body,
    );
    assert_eq!(
        after_upd[0][0].as_deref(),
        Some(&b"edited"[..]),
        "msg updated"
    );

    // DELETE one row (seq = 2) — the other two remain, still ordered.
    let del = round_trip(
        &mut conn,
        &request(
            24,
            Opcode::Query,
            &query_body("DELETE FROM events WHERE room = 'r1' AND seq = 2"),
        ),
    )
    .await;
    assert_eq!(del.opcode, Opcode::Result, "single-row DELETE");

    let after_del = parse_rows(
        &round_trip(
            &mut conn,
            &request(
                25,
                Opcode::Query,
                &query_body("SELECT * FROM events WHERE room = 'r1'"),
            ),
        )
        .await
        .body,
    );
    let remaining: Vec<i32> = after_del
        .iter()
        .map(|r| {
            let b = r[1].as_deref().unwrap();
            i32::from_be_bytes([b[0], b[1], b[2], b[3]])
        })
        .collect();
    assert_eq!(
        remaining,
        vec![1, 3],
        "seq 2 deleted, 1 and 3 remain ordered"
    );

    // DELETE the whole partition (no clustering predicate).
    let del_all = round_trip(
        &mut conn,
        &request(
            26,
            Opcode::Query,
            &query_body("DELETE FROM events WHERE room = 'r1'"),
        ),
    )
    .await;
    assert_eq!(del_all.opcode, Opcode::Result, "whole-partition DELETE");

    let empty = parse_rows(
        &round_trip(
            &mut conn,
            &request(
                27,
                Opcode::Query,
                &query_body("SELECT * FROM events WHERE room = 'r1'"),
            ),
        )
        .await
        .body,
    );
    assert!(empty.is_empty(), "partition r1 is gone");

    // The other partition is untouched.
    let r2 = parse_rows(
        &round_trip(
            &mut conn,
            &request(
                28,
                Opcode::Query,
                &query_body("SELECT * FROM events WHERE room = 'r2'"),
            ),
        )
        .await
        .body,
    );
    assert_eq!(r2.len(), 1, "r2 partition survives");
    assert_eq!(r2[0][2].as_deref(), Some(&b"other"[..]));
}
