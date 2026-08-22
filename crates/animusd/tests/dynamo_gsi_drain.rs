//! The **GSI drain** end to end (ADR 0041 §4): an indexed write leaves a
//! change-log record, and the background drain materializes the table's global
//! secondary index rows into that index's own hidden table (`<base>$<index>`).
//!
//! Asserted against the hidden table's **real rows**, read through the plain
//! client protocol, rather than through a DynamoDB `Query` — the index read path
//! is a later change, and this test is about the rows genuinely existing in the
//! data plane, replicated and durable, not about the surface that will serve
//! them.
//!
//! A GSI is **eventually** consistent by design (that is DynamoDB's contract and
//! the whole reason the drain is asynchronous), so every assertion here is a
//! converged-or-timeout poll — never a fixed sleep followed by a one-shot check.

use std::net::SocketAddr;
use std::time::Duration;

use animus_dynamo::wire;
use animusd::{ClientRequest, ClientResponse, Node, bind_cluster, read_frame, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not bootstrap within 20s");
}

/// One DynamoDB JSON request over the real HTTP wire.
async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    // `Connection: close` is load-bearing: this helper reads to EOF, and an
    // HTTP/1.1 request without it is kept alive by the server, which then
    // waits for a next request that never comes — deadlocking the test.
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

/// How many live rows a table holds, via a whole-table client-protocol scan.
///
/// Counts **decoded live items**, not raw pairs: a DynamoDB `DeleteItem`
/// stores a tombstone *value* (a real stored pair the raw scan returns), so a
/// raw count would keep counting a deleted item forever. Decoding through the
/// same stored-item codec the Dynamo edge uses drops those.
///
/// Every step is individually bounded. A scan of a table whose tablet does not
/// exist yet can legitimately block on routing until the server's own client
/// timeout, and this is called from a poll loop — an unbounded read here turns
/// "the drain is slow" into "the test hangs forever", which is exactly what it
/// did before this bound existed.
async fn row_count(addr: SocketAddr, table: &str) -> Option<usize> {
    let once = async {
        let mut s = TcpStream::connect(addr).await.ok()?;
        let req = ClientRequest::Scan {
            start: Vec::new(),
            end: None,
            limit: None,
            reverse: false,
            table: table.to_owned(),
        };
        animusd::write_frame(&mut s, &req).await.ok()?;
        match read_frame(&mut s).await.ok()?? {
            ClientResponse::Pairs(rows) => Some(
                rows.iter()
                    .filter(|(_, v)| matches!(wire::decode_stored_item(v), Ok(Some(_))))
                    .count(),
            ),
            // A table with no tablet yet reads as empty rather than as an error
            // to fail on: the drain provisions an index table lazily, on its
            // first write, exactly like any other table (ADR 0023).
            _ => Some(0),
        }
    };
    timeout(Duration::from_secs(5), once).await.ok().flatten()
}

/// Poll until `table` holds exactly `want` rows, reporting the last count
/// actually observed on failure.
///
/// The observed count is captured **inside** the loop rather than re-read in
/// the panic: re-reading there means the diagnostic itself can block, turning a
/// clean timeout failure into a hang.
async fn await_row_count(addr: SocketAddr, table: &str, want: usize, what: &str) {
    let last = std::sync::Arc::new(std::sync::Mutex::new(None::<usize>));
    let seen = std::sync::Arc::clone(&last);
    let converged = async move {
        loop {
            let got = row_count(addr, table).await;
            *seen.lock().unwrap() = got;
            if got == Some(want) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    if timeout(Duration::from_secs(30), converged).await.is_err() {
        let got = *last.lock().unwrap();
        panic!("{what}: `{table}` never reached {want} rows (last saw {got:?})");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_drain_materializes_and_prunes_a_gsis_rows() {
    let dir = tempfile::TempDir::new().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let dynamo_addr = nodes[0].dynamo_addr();
    let client_addr = nodes[0].client_addr();
    let index_table = "users$by-email";

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"users",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-email",
                 "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    for (id, email) in [("u1", "a@x"), ("u2", "b@x"), ("u3", "a@x")] {
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"users","Item":{{"id":{{"S":"{id}"}},"email":{{"S":"{email}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem({id}) failed: {body}");
    }

    // One index row per item — materialized asynchronously, so this converges
    // rather than being true the instant the writes ack.
    await_row_count(client_addr, index_table, 3, "after three puts").await;

    // Overwriting an item's indexed attribute must MOVE its row, not add one:
    // the drain is derivative, so the row `u3` used to occupy is exactly the one
    // the recomputation no longer produces, and falls out as stale.
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"users","Item":{"id":{"S":"u3"},"email":{"S":"c@x"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "overwrite failed: {body}");
    await_row_count(client_addr, index_table, 3, "after re-indexing u3").await;

    // Deleting an item removes its index row.
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.DeleteItem",
        r#"{"TableName":"users","Key":{"id":{"S":"u3"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "DeleteItem failed: {body}");
    await_row_count(client_addr, index_table, 2, "after deleting u3").await;

    // The change log is trimmed as it is consumed (ADR 0041 §4): the drain
    // deletes the records it reconciles, so a quiesced table's log does not
    // grow without bound. Nothing is asserted about the base table's own row
    // count here beyond it being unaffected by index maintenance.
    await_row_count(client_addr, "users", 2, "base table after the delete").await;

    // The acceptance the whole mechanism exists for: a real DynamoDB `Query`
    // against the GSI, over the actual wire, returns the drain's materialized
    // rows — not just the raw row counts asserted above. Still a
    // converged-or-timeout poll (a GSI is eventually consistent by contract),
    // even though the row-count polls above already imply convergence at this
    // point.
    await_gsi_query(
        dynamo_addr,
        r#"{"TableName":"users","IndexName":"by-email",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#,
        |b| b.contains("\"Count\":1") && b.contains(r#""id":{"S":"u1"}"#),
    )
    .await;

    // c@x was u3's overwritten email, and u3 was then deleted — the GSI must
    // show it gone, not merely absent-because-never-written.
    await_gsi_query(
        dynamo_addr,
        r#"{"TableName":"users","IndexName":"by-email",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"c@x"}}}"#,
        |b| b.contains("\"Count\":0"),
    )
    .await;
}

/// Poll a GSI `Query` until `accept` is satisfied. A GSI is materialized
/// **asynchronously** by the drain (ADR 0041 §4/§5) — DynamoDB's own
/// eventually-consistent contract — so this is a converged-or-timeout poll,
/// never a fixed sleep followed by a one-shot check, mirroring this file's own
/// `await_row_count` discipline above.
async fn await_gsi_query(addr: SocketAddr, body: &str, accept: impl Fn(&str) -> bool) {
    let last = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let seen = std::sync::Arc::clone(&last);
    let converged = async move {
        loop {
            let (status, got) = dynamo(addr, "DynamoDB_20120810.Query", body).await;
            if status == 200 && accept(&got) {
                return;
            }
            *seen.lock().unwrap() = got;
            sleep(Duration::from_millis(100)).await;
        }
    };
    if timeout(Duration::from_secs(30), converged).await.is_err() {
        panic!(
            "GSI query never converged within 30s (last saw: {})",
            last.lock().unwrap()
        );
    }
}
