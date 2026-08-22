//! Drop-table cascade to hidden GSI index tables (ADR 0041 §5): dropping a
//! table with a global secondary index must remove the index's own hidden
//! table's tablets too — not just the base table's, which is all
//! `drop_table_gc.rs` covers (it predates ADR 0041 and carries no index).
//!
//! Borrows `dynamo_gsi_drain.rs`'s converged-or-timeout idiom to reach a
//! stable pre-drop state (the hidden table must actually exist, provisioned
//! lazily by the drain, before the cascade has anything real to remove) and
//! `drop_table_gc.rs`'s on-disk WAL-file check to prove the hidden table's
//! data — not just its metadata entry — is genuinely reclaimed.

mod support;

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_dynamo::wire;
use animusd::{ClientRequest, ClientResponse, Node, StorageBackend, read_frame};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// One DynamoDB JSON request over the real HTTP wire (identical copy to
/// `dynamo_gsi_drain.rs` — `Connection: close` is load-bearing there, see its
/// doc).
async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).await.expect("connect");
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

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed
/// JSON)` (identical copy to `drop_table_gc.rs`).
async fn admin(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.0\r\nHost: animus\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );
    stream.write_all(request.as_bytes()).await.expect("send");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let value: Value = serde_json::from_str(payload).unwrap_or(Value::Null);
    (status, value)
}

/// Whether the replicated metadata (as `node` sees it) has any tablet scoped
/// to `table`.
fn has_table_tablet(node: &Node, table: &str) -> bool {
    node.metadata()
        .tablets
        .values()
        .any(|t| t.table.as_deref() == Some(table))
}

/// The tablet id(s) currently scoped to `table`, in ascending order.
fn tablet_ids_for(node: &Node, table: &str) -> Vec<u64> {
    node.metadata()
        .tablets
        .iter()
        .filter(|(_, t)| t.table.as_deref() == Some(table))
        .map(|(id, _)| id.0)
        .collect()
}

/// The file names directly inside `dir` (empty if the dir does not exist).
fn files_in(dir: &Path) -> Vec<String> {
    match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
            .filter_map(|e| e.file_name().into_string().ok())
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Whether `tablet`'s own per-tablet Raft WAL file (`raftkv.wal.<tablet>`)
/// exists in `dir` (identical copy to `drop_table_gc.rs`).
fn tablet_wal_present(dir: &Path, tablet: u64) -> bool {
    files_in(dir).contains(&animus_cp_data::wal_file(tablet))
}

/// Poll until `cond` holds, panicking with `what` after `secs` seconds.
async fn await_true<F: Fn() -> bool>(secs: u64, what: &str, cond: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !cond() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        sleep(Duration::from_millis(100)).await;
    }
}

/// How many live rows a table holds, via a whole-table client-protocol scan
/// (identical copy to `dynamo_gsi_drain.rs` — see its doc for why every step
/// is individually bounded).
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
            _ => Some(0),
        }
    };
    timeout(Duration::from_secs(5), once).await.ok().flatten()
}

/// Poll until `table` holds exactly `want` rows (identical copy to
/// `dynamo_gsi_drain.rs`).
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

/// Create a table with a GSI, write items, wait for the GSI to converge
/// (provisioning its hidden table lazily via the drain), then drop the base
/// table and confirm BOTH the base table's and the hidden index table's
/// tablets disappear from the replicated map — and that the hidden table's
/// data is genuinely reclaimed on disk, not merely orphaned.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn drop_table_cascades_to_its_gsis_hidden_table() {
    timeout(Duration::from_secs(120), async {
        let tmp = tempfile::tempdir().unwrap();
        let (node, _config) =
            support::start_single_node(tmp.path(), StorageBackend::default()).await;
        let dynamo_addr = node.dynamo_addr();
        let admin_addr = node.admin_addr();
        let client_addr = node.client_addr();
        let raftkv_dir = tmp.path().join("internal");
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

        for (id, email) in [("u1", "a@x"), ("u2", "b@x")] {
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

        // The base tablet must exist, and the drain must have actually
        // provisioned + materialized the hidden table — both before the
        // drop, so the cascade genuinely has a hidden table to remove
        // (a fresh, never-drained index has no tablet at all, ADR 0041 §4).
        await_true(10, "base tablet provisioned", || {
            has_table_tablet(&node, "users")
        })
        .await;
        await_row_count(client_addr, index_table, 2, "GSI converges before drop").await;
        await_true(10, "hidden index table's tablet exists", || {
            has_table_tablet(&node, index_table)
        })
        .await;

        let base_tablets = tablet_ids_for(&node, "users");
        let index_tablets = tablet_ids_for(&node, index_table);
        assert!(!base_tablets.is_empty() && !index_tablets.is_empty());

        // DROP the base table via the admin sink (same path as CQL
        // `DROP TABLE`) — the DynamoDB wire itself has no `DeleteTable`.
        let (status, body) = admin(
            admin_addr,
            "POST",
            "/admin/data/drop-table",
            Some(r#"{"table":"users"}"#),
        )
        .await;
        assert_eq!(status, 200, "drop-table: {body}");

        // Both the base table's tablet AND the hidden index table's tablet
        // must leave the replicated map — the cascade this fix adds. Before
        // the fix, only the base table's tablet would ever disappear here.
        await_true(30, "base table's tablet dropped", || {
            !has_table_tablet(&node, "users")
        })
        .await;
        await_true(30, "hidden index table's tablet dropped", || {
            !has_table_tablet(&node, index_table)
        })
        .await;

        // Data is reclaimed on disk too (`drop_table_gc.rs`'s own
        // discipline): every tablet's own WAL file — base and hidden index
        // table alike — is deleted by the per-node GC loop.
        for tablet in base_tablets.into_iter().chain(index_tablets) {
            await_true(30, "tablet's WAL file reclaimed", || {
                !tablet_wal_present(&raftkv_dir, tablet)
            })
            .await;
        }

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}
