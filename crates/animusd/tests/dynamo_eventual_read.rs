//! ADR 0055: the **eventually-consistent read** contract at the DynamoDB wire,
//! end to end over real HTTP on a 3-node cluster.
//!
//! What this proves, and what it deliberately does not:
//!
//! - **`ConsistentRead: true` is immediately correct on every node**, including
//!   the two that host only followers of the item's tablet. That is a plain
//!   one-shot assert because the strong path guarantees it.
//! - **`ConsistentRead: false` converges on every node** — a
//!   converged-or-timeout poll, never a fixed-deadline one-shot assert (the
//!   house rule for every eventual property). Asserting that an eventual read
//!   *is* stale at some instant would be asserting a race; asserting that it
//!   *becomes* correct is the actual contract.
//! - **Both agree once the cluster is quiet**, on a point read, a `Query`, and
//!   a `Scan`.
//!
//! It cannot prove the cheap path was *taken* rather than silently falling back
//! to the strong one (ADR 0055 §4's fallback is invisible from the wire by
//! design) — that is `animus-cp-data`'s `tests/stale_read.rs`'s job at the
//! primitive level.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

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

/// Poll one DynamoDB request until `accept` is satisfied, returning the last
/// body seen — the converged-or-timeout idiom every eventual property in this
/// suite uses.
async fn await_response(
    addr: SocketAddr,
    target: &'static str,
    body: &str,
    what: &str,
    accept: impl Fn(u16, &str) -> bool,
) -> String {
    let last = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let seen = std::sync::Arc::clone(&last);
    let body = body.to_owned();
    let converged = async move {
        loop {
            let (status, got) = dynamo(addr, target, &body).await;
            if accept(status, &got) {
                return got;
            }
            *seen.lock().unwrap() = got;
            sleep(Duration::from_millis(50)).await;
        }
    };
    match timeout(Duration::from_secs(15), converged).await {
        Ok(body) => body,
        Err(_) => panic!(
            "{what} never converged within 15s (last saw: {})",
            last.lock().unwrap()
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eventual_reads_converge_on_every_node_while_consistent_reads_are_immediate() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let addrs: Vec<SocketAddr> = nodes.iter().map(Node::dynamo_addr).collect();

    let (status, body) = dynamo(
        addrs[0],
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"reads","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    // Write through node 0. Which node leads the tablet is not this test's
    // business — the point is that the other two are, or may be, followers.
    let (status, body) = dynamo(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"reads","Item":{
            "pk":{"S":"p1"},"sk":{"S":"a"},"v":{"S":"first"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");

    // `ConsistentRead: true` is correct on EVERY node immediately, follower or
    // not: the strong path resolves the leader and takes the ReadIndex barrier
    // regardless of which node received the request.
    for (i, &addr) in addrs.iter().enumerate() {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.GetItem",
            r#"{"TableName":"reads","Key":{"pk":{"S":"p1"},"sk":{"S":"a"}},
                "ConsistentRead":true}"#,
        )
        .await;
        assert_eq!(status, 200, "node {i}: strong GetItem failed: {body}");
        assert!(
            body.contains("\"first\""),
            "node {i}: a strong read must see the committed write immediately: {body}"
        );
    }

    // `ConsistentRead: false` — the wire default — converges on every node.
    // Deliberately NOT asserted to be stale first: that would be asserting a
    // race. What ADR 0055 promises is that it lands, and that when it lands it
    // agrees with the strong read.
    for (i, &addr) in addrs.iter().enumerate() {
        await_response(
            addr,
            "DynamoDB_20120810.GetItem",
            r#"{"TableName":"reads","Key":{"pk":{"S":"p1"},"sk":{"S":"a"}}}"#,
            &format!("node {i}'s eventual GetItem"),
            |status, body| status == 200 && body.contains("\"first\""),
        )
        .await;
    }

    // An overwrite, then the same convergence check — this is the case an
    // eventual read is actually allowed to get wrong for a moment.
    let (status, body) = dynamo(
        addrs[1],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"reads","Item":{
            "pk":{"S":"p1"},"sk":{"S":"a"},"v":{"S":"second"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "overwrite failed: {body}");

    for (i, &addr) in addrs.iter().enumerate() {
        await_response(
            addr,
            "DynamoDB_20120810.GetItem",
            r#"{"TableName":"reads","Key":{"pk":{"S":"p1"},"sk":{"S":"a"}}}"#,
            &format!("node {i}'s eventual GetItem after the overwrite"),
            |status, body| status == 200 && body.contains("\"second\""),
        )
        .await;
        // And the strong read on the same node agrees, immediately.
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.GetItem",
            r#"{"TableName":"reads","Key":{"pk":{"S":"p1"},"sk":{"S":"a"}},
                "ConsistentRead":true}"#,
        )
        .await;
        assert_eq!(status, 200, "node {i}: strong GetItem failed: {body}");
        assert!(
            body.contains("\"second\""),
            "node {i}: strong and eventual reads must agree once quiet: {body}"
        );
    }

    // A second item, so the range reads below have something to page over.
    let (status, body) = dynamo(
        addrs[2],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"reads","Item":{
            "pk":{"S":"p1"},"sk":{"S":"b"},"v":{"S":"other"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "second PutItem failed: {body}");

    // `Query` and `Scan` take the same fork, per tablet — check both flavors
    // on every node.
    for (i, &addr) in addrs.iter().enumerate() {
        for (target, body) in [
            (
                "DynamoDB_20120810.Query",
                r#"{"TableName":"reads","KeyConditionExpression":"pk = :p",
                    "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
            ),
            ("DynamoDB_20120810.Scan", r#"{"TableName":"reads"}"#),
        ] {
            await_response(
                addr,
                target,
                body,
                &format!("node {i}'s eventual {target}"),
                |status, got| status == 200 && got.contains("\"Count\":2"),
            )
            .await;
        }

        // The strong forms are immediately correct on every node.
        let (status, got) = dynamo(
            addr,
            "DynamoDB_20120810.Query",
            r#"{"TableName":"reads","ConsistentRead":true,
                "KeyConditionExpression":"pk = :p",
                "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "node {i}: strong Query failed: {got}");
        assert!(
            got.contains("\"Count\":2"),
            "node {i}: strong Query must see both items: {got}"
        );
    }
}

/// A `DeleteItem` must become visible to an eventual read as a real absence —
/// the deletion propagates, rather than the tombstone being mistaken for a row
/// or the row lingering forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_converges_to_absence_on_an_eventual_read() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let addrs: Vec<SocketAddr> = nodes.iter().map(Node::dynamo_addr).collect();

    let (status, body) = dynamo(
        addrs[0],
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"gone","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    let (status, body) = dynamo(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"gone","Item":{"pk":{"S":"x"},"v":{"S":"here"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");

    for (i, &addr) in addrs.iter().enumerate() {
        await_response(
            addr,
            "DynamoDB_20120810.GetItem",
            r#"{"TableName":"gone","Key":{"pk":{"S":"x"}}}"#,
            &format!("node {i}'s eventual GetItem"),
            |status, body| status == 200 && body.contains("\"here\""),
        )
        .await;
    }

    let (status, body) = dynamo(
        addrs[1],
        "DynamoDB_20120810.DeleteItem",
        r#"{"TableName":"gone","Key":{"pk":{"S":"x"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "DeleteItem failed: {body}");

    for (i, &addr) in addrs.iter().enumerate() {
        // `{}` — an item-less `GetItem` response — is DynamoDB's own spelling
        // of "no such item".
        await_response(
            addr,
            "DynamoDB_20120810.GetItem",
            r#"{"TableName":"gone","Key":{"pk":{"S":"x"}}}"#,
            &format!("node {i}'s eventual GetItem after the delete"),
            |status, body| status == 200 && !body.contains("\"Item\""),
        )
        .await;
    }
}
