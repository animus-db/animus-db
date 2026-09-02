//! Regression for the `CreateTable`-ack-vs-tablet-ready race: a 200 from
//! `CreateTable` must mean the table's first tablet's Raft group is already
//! formed, elected, and serving — so a client's immediately-following first
//! write lands promptly instead of riding out the formation window via the
//! election-wait machinery (`cp_forward`'s backoff pass / the local
//! `RouteDecision::Wait`), which under unlucky timing used to burn much of
//! `CLIENT_TIMEOUT` or fail outright.
//!
//! The mechanism under test is `ClientCtx::await_table_serveable` (a
//! linearizable probe read, converged-or-timeout), called by the DynamoDB
//! `CreateTable` edge before it acks.
//!
//! The load-bearing assertion is deliberately **one-shot, not a poll**: the
//! moment the 200 arrives, some node must *already* report itself leader of
//! the table's tablet in its own `/admin/raftkv` view. Pre-fix, the ack
//! raced the per-node tablet-host reconciler standing the group up (metadata
//! commit → host → election, ≥ one election timeout), so this check reliably
//! observed a leaderless group. The follow-up single-shot first write is the
//! end-to-end property the one-shot view check grounds.
//!
//! Real time/sockets (the ProdEnv edge) — the *bring-up* waits poll with
//! generous timeouts as usual; only the post-ack readiness check is one-shot,
//! because "already true at ack time" is exactly the property under test.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// One DynamoDB request over a fresh HTTP/1.1 connection → `(status, body)`.
async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to dynamo");
    let request = format!(
        "POST / HTTP/1.1\r\n\
         Host: animus\r\n\
         X-Amz-Target: {target}\r\n\
         Content-Type: application/x-amz-json-1.0\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .expect("read full response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    (status, payload.to_string())
}

/// One HTTP/1.0 GET to the admin endpoint; returns `(status, parsed JSON)`.
async fn admin_get(addr: SocketAddr, path: &str) -> (u16, serde_json::Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let request = format!(
        "GET {path} HTTP/1.0\r\n\
         Host: animus\r\n\
         Connection: close\r\n\
         \r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
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
    let value: serde_json::Value = serde_json::from_str(payload).expect("admin body is JSON");
    (status, value)
}

async fn await_cluster_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            let leader = nodes.iter().any(Node::is_control_leader);
            let everyone_registered = nodes.iter().all(|n| !n.metadata().members.is_empty());
            if leader && everyone_registered {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not bootstrap within 20s");
}

/// `CreateTable`'s 200 already implies the table's tablet group has an
/// elected, serving leader — checked one-shot against `/admin/raftkv` the
/// instant the ack arrives — and a single-shot first write (no client retry
/// loop) therefore lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_table_ack_implies_tablet_group_serves() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_cluster_bootstrap(&nodes).await;
    let addr0 = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"ready_t",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    // The table's (single) tablet must already be in the replicated map —
    // `provision_tablet`'s commit-wait guarantees this half even pre-fix.
    let tablet: u64 = nodes[0]
        .metadata()
        .tablets
        .values()
        .find(|t| t.table.as_deref() == Some("ready_t"))
        .map(|t| t.id.0)
        .expect("CreateTable's 200 implies the tablet is in the metadata map");

    // ONE-SHOT (the regression's heart): at ack time, some node must already
    // report itself leader of that tablet in its own node-local
    // `/admin/raftkv` view. No poll, no sleep — polling here would mask the
    // exact race this test exists to pin (pre-fix, the group's formation and
    // election were still in flight when the 200 arrived).
    let mut views = Vec::new();
    for node in &nodes {
        let (s, v) = admin_get(node.admin_addr(), "/admin/raftkv").await;
        assert_eq!(s, 200, "/admin/raftkv failed: {v}");
        views.push(v);
    }
    let leader_seen = views.iter().any(|v| {
        v["groups"].as_array().is_some_and(|groups| {
            groups.iter().any(|g| {
                g["tablet"].as_u64() == Some(tablet) && g["is_leader"].as_bool() == Some(true)
            })
        })
    });
    assert!(
        leader_seen,
        "CreateTable acked 200 but no node reports a leader for tablet {tablet} — \
         the ack raced the group's formation/election window; views: {views:?}"
    );

    // The end-to-end property the view check grounds: ONE first-write
    // attempt, no client-side retry loop, must land. The outer bound only
    // guards a hang; the server's own routing budget (`CLIENT_TIMEOUT`, 10s)
    // is what the write works within — and post-fix it should never need the
    // election-wait machinery at all.
    let (status, body) = timeout(
        Duration::from_secs(15),
        dynamo(
            addr0,
            "DynamoDB_20120810.PutItem",
            r#"{"TableName":"ready_t","Item":{"pk":{"S":"first"},"v":{"S":"w1"}}}"#,
        ),
    )
    .await
    .expect("single-shot first write hung");
    assert_eq!(status, 200, "single-shot first write failed: {body}");

    // Read-your-write through the same edge (linearizable read).
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"ready_t","Key":{"pk":{"S":"first"}},"ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {body}");
    assert!(
        body.contains(r#""S":"w1""#),
        "first write not readable after ack'd CreateTable: {body}"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}
