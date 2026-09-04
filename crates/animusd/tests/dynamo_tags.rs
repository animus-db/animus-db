//! End-to-end tests for DynamoDB **resource tagging** (roadmap W-06) over
//! the real DynamoDB JSON/HTTP wire: `TagResource`/`UntagResource`/
//! `ListTagsOfResource`, mirroring `dynamo_table_ops.rs`'s bring-up and
//! request-helper idioms.
//!
//! - `tag_untag_list_round_trip` proves the basic add/overwrite/remove/read
//!   cycle over a single-node cluster.
//! - `tag_resource_rejects_a_malformed_or_unknown_resource_arn` proves the
//!   two distinct failure shapes: a `ResourceArn` that isn't a well-formed
//!   table ARN at all (`ValidationException`, a decode-time structural
//!   error `animus-dynamo` can already detect from the string alone) versus
//!   a well-formed one naming a table that genuinely doesn't exist
//!   (`ResourceNotFoundException`, since only `animusd` holds the
//!   replicated catalog).
//! - `tag_resource_on_a_follower_is_relayed_to_the_leader` is this file's
//!   instance of the root `CLAUDE.md`'s "grep every gating match site"
//!   regression class: `MetaCommand::TagResource` must be on
//!   `is_relayable_command`'s allowlist, or a `TagResource` that happens to
//!   land on a control-plane follower silently times out instead of
//!   relaying (the exact bimodal per-process flake that lesson warns
//!   about) — mirrors `schema_ddl_relay.rs`'s own per-command relay tests.

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
    s.flush().await.expect("flush");
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).await.expect("read");
    let text = String::from_utf8(raw).expect("utf8");
    let (head, payload) = text.split_once("\r\n\r\n").expect("has body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .expect("status line");
    (status, payload.to_string())
}

async fn create_table(addr: SocketAddr, name: &str) {
    let body = format!(
        r#"{{"TableName":"{name}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}]}}"#
    );
    let (status, resp) = dynamo(addr, "DynamoDB_20120810.CreateTable", &body).await;
    assert_eq!(status, 200, "CreateTable `{name}` failed: {resp}");
}

fn table_arn(table: &str) -> String {
    format!("arn:aws:dynamodb:animus:0:table/{table}")
}

/// The `{Key, Value}` pairs of a `ListTagsOfResource` response's `Tags`
/// array, as `(key, value)` pairs sorted by key (this adapter's own
/// `BTreeMap`-backed order, ADR-faithful to nothing in particular — the
/// test sorts explicitly instead of relying on it, so it isn't coupled to
/// that internal detail).
fn tags_of(body: &str) -> Vec<(String, String)> {
    let v: serde_json::Value = serde_json::from_str(body).expect("valid JSON");
    let mut out: Vec<(String, String)> = v["Tags"]
        .as_array()
        .expect("Tags array")
        .iter()
        .map(|e| {
            (
                e["Key"].as_str().expect("Key").to_owned(),
                e["Value"].as_str().expect("Value").to_owned(),
            )
        })
        .collect();
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tag_untag_list_round_trip() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    create_table(addr, "orders").await;

    // A table with no tags yet: `ListTagsOfResource` returns an empty set,
    // not an error.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.ListTagsOfResource",
        &format!(r#"{{"ResourceArn":"{}"}}"#, table_arn("orders")),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(tags_of(&body), Vec::<(String, String)>::new());

    // `TagResource` with two tags — AWS-faithfully returns a bare `{}`.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TagResource",
        &format!(
            r#"{{"ResourceArn":"{}","Tags":[{{"Key":"env","Value":"prod"}},
                {{"Key":"team","Value":"payments"}}]}}"#,
            table_arn("orders")
        ),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body, "{}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.ListTagsOfResource",
        &format!(r#"{{"ResourceArn":"{}"}}"#, table_arn("orders")),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        tags_of(&body),
        vec![
            ("env".to_owned(), "prod".to_owned()),
            ("team".to_owned(), "payments".to_owned()),
        ]
    );

    // Overwrite: re-`TagResource`-ing an existing key replaces its value
    // (last writer wins, DynamoDB's own semantics) rather than erroring or
    // being ignored.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TagResource",
        &format!(
            r#"{{"ResourceArn":"{}","Tags":[{{"Key":"env","Value":"staging"}}]}}"#,
            table_arn("orders")
        ),
    )
    .await;
    assert_eq!(status, 200, "{body}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.ListTagsOfResource",
        &format!(r#"{{"ResourceArn":"{}"}}"#, table_arn("orders")),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        tags_of(&body),
        vec![
            ("env".to_owned(), "staging".to_owned()),
            ("team".to_owned(), "payments".to_owned()),
        ]
    );

    // `UntagResource` removes one key, leaving the other untouched.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UntagResource",
        &format!(
            r#"{{"ResourceArn":"{}","TagKeys":["env"]}}"#,
            table_arn("orders")
        ),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body, "{}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.ListTagsOfResource",
        &format!(r#"{{"ResourceArn":"{}"}}"#, table_arn("orders")),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        tags_of(&body),
        vec![("team".to_owned(), "payments".to_owned())]
    );

    // `UntagResource` naming an already-absent key is a no-op, not an
    // error, matching DynamoDB.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UntagResource",
        &format!(
            r#"{{"ResourceArn":"{}","TagKeys":["env"]}}"#,
            table_arn("orders")
        ),
    )
    .await;
    assert_eq!(status, 200, "{body}");

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tag_resource_rejects_a_malformed_or_unknown_resource_arn() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    // A `ResourceArn` that isn't a well-formed table ARN at all — a
    // decode-time structural error this crate can already detect from the
    // string alone, so `ValidationException`, never `ResourceNotFoundException`.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TagResource",
        r#"{"ResourceArn":"not-an-arn","Tags":[{"Key":"env","Value":"prod"}]}"#,
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("ValidationException"), "got: {body}");

    // A stream ARN (well-formed, but not a *table* ARN) is rejected the
    // same way.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TagResource",
        r#"{"ResourceArn":"arn:aws:dynamodb:animus:0:table/orders/stream/L1",
            "Tags":[{"Key":"env","Value":"prod"}]}"#,
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("ValidationException"), "got: {body}");

    // A well-formed table ARN naming a table that was never created —
    // `animusd`'s own call, `ResourceNotFoundException`.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TagResource",
        &format!(
            r#"{{"ResourceArn":"{}","Tags":[{{"Key":"env","Value":"prod"}}]}}"#,
            table_arn("no-such-table")
        ),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("ResourceNotFoundException"), "got: {body}");

    // Same for `ListTagsOfResource` (a pure read) and `UntagResource`.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.ListTagsOfResource",
        &format!(r#"{{"ResourceArn":"{}"}}"#, table_arn("no-such-table")),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("ResourceNotFoundException"), "got: {body}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UntagResource",
        &format!(
            r#"{{"ResourceArn":"{}","TagKeys":["env"]}}"#,
            table_arn("no-such-table")
        ),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("ResourceNotFoundException"), "got: {body}");

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

/// The `is_relayable_command` regression this file exists for:
/// `TagResource` issued against a DynamoDB listener on a node that is
/// **not** the control-plane leader must still commit —
/// `MetaCommand::TagResource` must be on the relay allowlist, or this times
/// out on exactly this shape (works only when the connected node happens to
/// be the leader).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn tag_resource_on_a_follower_is_relayed_to_the_leader() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let leader = nodes.iter().position(Node::is_control_leader).unwrap();
    let follower = (0..nodes.len()).find(|&i| i != leader).unwrap();
    let leader_dynamo = nodes[leader].dynamo_addr();
    let follower_dynamo = nodes[follower].dynamo_addr();

    create_table(leader_dynamo, "tag_relay_t").await;

    // The regression: `TagResource`, issued against the FOLLOWER's own
    // DynamoDB listener, must relay to the leader and commit.
    let (status, body) = timeout(Duration::from_secs(20), async {
        loop {
            let (status, body) = dynamo(
                follower_dynamo,
                "DynamoDB_20120810.TagResource",
                &format!(
                    r#"{{"ResourceArn":"{}","Tags":[{{"Key":"env","Value":"prod"}}]}}"#,
                    table_arn("tag_relay_t")
                ),
            )
            .await;
            if status == 200 {
                return (status, body);
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("follower-issued TagResource did not commit via relay in 20s");
    assert_eq!(status, 200, "body: {body}");

    // It replicated to *every* node's own replicated catalog — a
    // converged-or-timeout poll, never a one-shot assert: the 200 above only
    // proves the FOLLOWER's own replicated view committed; every other
    // node's own ADR 0038 async apply task can lag that commit by a beat.
    for (i, n) in nodes.iter().enumerate() {
        timeout(Duration::from_secs(20), async {
            loop {
                if n.metadata()
                    .table_tags("tag_relay_t")
                    .is_some_and(|t| t.get("env").map(String::as_str) == Some("prod"))
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("node {i}: tag missing 20s after follower-relayed TagResource"));
    }

    // `UntagResource` against the (different) leader must also converge —
    // exercising the relay's other direction is covered by
    // `schema_ddl_relay.rs`'s own suite; this file's own regression is
    // scoped to `TagResource`, matching that file's per-command precedent.
    let (status, body) = dynamo(
        leader_dynamo,
        "DynamoDB_20120810.ListTagsOfResource",
        &format!(r#"{{"ResourceArn":"{}"}}"#, table_arn("tag_relay_t")),
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    assert!(body.contains("\"Value\":\"prod\""), "body: {body}");

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}
