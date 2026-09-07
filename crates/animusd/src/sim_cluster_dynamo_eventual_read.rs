//! `SimCluster`-driven tests for ADR 0055's eventually-consistent read
//! contract over the DynamoDB wire (ADR 0061 rung D3 PR 1) — replaces the
//! real-socket `ProdEnv` binary `crates/animusd/tests/dynamo_eventual_read.rs`.
//! Driven through `SimCluster::dynamo` — see `sim_cluster_dynamo.rs`'s own
//! module doc for the shared generic core.
//!
//! What this proves, and what it deliberately does not:
//!
//! - `ConsistentRead: true` is immediately correct on every node, including
//!   ones that host only followers of the item's tablet.
//! - `ConsistentRead: false` converges on every node — a
//!   converged-or-timeout poll (`cluster.run_for` between attempts), never a
//!   fixed-deadline one-shot assert.
//! - Both agree once the cluster is quiet, on a point read, a `Query`, and a
//!   `Scan`.
//!
//! It cannot prove the cheap path was *taken* rather than silently falling
//! back to the strong one — that is `animus-cp-data`'s own `stale_read`
//! coverage's job at the primitive level.
//!
//! Seed replay: `ANIMUS_SEED=<seed> cargo test -p animusd --lib <test name>`.

use std::time::Duration;

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Poll a DynamoDB request until `accept(status, body)` holds, or panic
/// after a bounded number of converged-or-timeout attempts — the sim
/// analogue of the `ProdEnv` fixture's own `await_response` helper.
fn await_response(
    cluster: &mut SimCluster,
    node: u64,
    target: &str,
    body: &[u8],
    what: &str,
    accept: impl Fn(u16, &str) -> bool,
) -> String {
    const ATTEMPTS: usize = 20;
    let seed = cluster.seed();
    let mut last = String::new();
    for _ in 0..ATTEMPTS {
        let (status, got) = cluster.dynamo(node, target, body);
        if accept(status, &got) {
            return got;
        }
        last = format!("status={status} body={got}");
        cluster.run_for(Duration::from_millis(100));
    }
    panic!("{what} never converged within {ATTEMPTS} attempts (last={last}, seed={seed})");
}

#[test]
fn eventual_reads_converge_on_every_node_while_consistent_reads_are_immediate() {
    let seed = env_seed(0xE0EA_0001);
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("reads");

    // Write through node 0. Which node leads the tablet is not this test's
    // business — the point is that the other two are, or may be, followers.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"reads","Item":{
            "pk":{"S":"p1"},"sk":{"S":"a"},"v":{"S":"first"}}}"#,
    );
    assert_eq!(status, 200, "PutItem failed (seed={seed}): {body}");

    // `ConsistentRead: true` is correct on EVERY node immediately.
    for node in 0..cluster.node_count() as u64 {
        let (status, body) = cluster.dynamo(
            node,
            "DynamoDB_20120810.GetItem",
            br#"{"TableName":"reads","Key":{"pk":{"S":"p1"},"sk":{"S":"a"}},
                "ConsistentRead":true}"#,
        );
        assert_eq!(
            status, 200,
            "node {node}: strong GetItem failed (seed={seed}): {body}"
        );
        assert!(
            body.contains("\"first\""),
            "node {node}: a strong read must see the committed write immediately (seed={seed}): {body}"
        );
    }

    // `ConsistentRead: false` — the wire default — converges on every node.
    for node in 0..cluster.node_count() as u64 {
        await_response(
            &mut cluster,
            node,
            "DynamoDB_20120810.GetItem",
            br#"{"TableName":"reads","Key":{"pk":{"S":"p1"},"sk":{"S":"a"}}}"#,
            &format!("node {node}'s eventual GetItem (seed={seed})"),
            |status, body| status == 200 && body.contains("\"first\""),
        );
    }

    // An overwrite, then the same convergence check.
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"reads","Item":{
            "pk":{"S":"p1"},"sk":{"S":"a"},"v":{"S":"second"}}}"#,
    );
    assert_eq!(status, 200, "overwrite failed (seed={seed}): {body}");

    for node in 0..cluster.node_count() as u64 {
        await_response(
            &mut cluster,
            node,
            "DynamoDB_20120810.GetItem",
            br#"{"TableName":"reads","Key":{"pk":{"S":"p1"},"sk":{"S":"a"}}}"#,
            &format!("node {node}'s eventual GetItem after the overwrite (seed={seed})"),
            |status, body| status == 200 && body.contains("\"second\""),
        );
        // And the strong read on the same node agrees, immediately.
        let (status, body) = cluster.dynamo(
            node,
            "DynamoDB_20120810.GetItem",
            br#"{"TableName":"reads","Key":{"pk":{"S":"p1"},"sk":{"S":"a"}},
                "ConsistentRead":true}"#,
        );
        assert_eq!(
            status, 200,
            "node {node}: strong GetItem failed (seed={seed}): {body}"
        );
        assert!(
            body.contains("\"second\""),
            "node {node}: strong and eventual reads must agree once quiet (seed={seed}): {body}"
        );
    }

    // A second item, so the range reads below have something to page over.
    let (status, body) = cluster.dynamo(
        2,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"reads","Item":{
            "pk":{"S":"p1"},"sk":{"S":"b"},"v":{"S":"other"}}}"#,
    );
    assert_eq!(status, 200, "second PutItem failed (seed={seed}): {body}");

    // `Query` and `Scan` take the same fork, per tablet — check both
    // flavors on every node.
    for node in 0..cluster.node_count() as u64 {
        for (target, body) in [
            (
                "DynamoDB_20120810.Query",
                br#"{"TableName":"reads","KeyConditionExpression":"pk = :p",
                    "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#
                    .as_slice(),
            ),
            (
                "DynamoDB_20120810.Scan",
                br#"{"TableName":"reads"}"#.as_slice(),
            ),
        ] {
            await_response(
                &mut cluster,
                node,
                target,
                body,
                &format!("node {node}'s eventual {target} (seed={seed})"),
                |status, got| status == 200 && got.contains("\"Count\":2"),
            );
        }

        // The strong forms are immediately correct on every node.
        let (status, got) = cluster.dynamo(
            node,
            "DynamoDB_20120810.Query",
            br#"{"TableName":"reads","ConsistentRead":true,
                "KeyConditionExpression":"pk = :p",
                "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
        );
        assert_eq!(
            status, 200,
            "node {node}: strong Query failed (seed={seed}): {got}"
        );
        assert!(
            got.contains("\"Count\":2"),
            "node {node}: strong Query must see both items (seed={seed}): {got}"
        );
    }
}

/// A `DeleteItem` must become visible to an eventual read as a real
/// absence. The `ProdEnv` fixture used a hash-only table (`"gone"`,
/// `pk` only); `SimCluster::create_table` only builds a composite
/// `(pk, sk)` schema, so this uses a fixed `sk` throughout — a fixture
/// data shape, not a change to what's under test.
#[test]
fn a_delete_converges_to_absence_on_an_eventual_read() {
    let seed = env_seed(0xE0EA_0002);
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("gone");

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"gone","Item":{"pk":{"S":"x"},"sk":{"S":"k"},"v":{"S":"here"}}}"#,
    );
    assert_eq!(status, 200, "PutItem failed (seed={seed}): {body}");

    for node in 0..cluster.node_count() as u64 {
        await_response(
            &mut cluster,
            node,
            "DynamoDB_20120810.GetItem",
            br#"{"TableName":"gone","Key":{"pk":{"S":"x"},"sk":{"S":"k"}}}"#,
            &format!("node {node}'s eventual GetItem (seed={seed})"),
            |status, body| status == 200 && body.contains("\"here\""),
        );
    }

    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.DeleteItem",
        br#"{"TableName":"gone","Key":{"pk":{"S":"x"},"sk":{"S":"k"}}}"#,
    );
    assert_eq!(status, 200, "DeleteItem failed (seed={seed}): {body}");

    for node in 0..cluster.node_count() as u64 {
        // `{}` — an item-less `GetItem` response — is DynamoDB's own
        // spelling of "no such item".
        await_response(
            &mut cluster,
            node,
            "DynamoDB_20120810.GetItem",
            br#"{"TableName":"gone","Key":{"pk":{"S":"x"},"sk":{"S":"k"}}}"#,
            &format!("node {node}'s eventual GetItem after the delete (seed={seed})"),
            |status, body| status == 200 && !body.contains("\"Item\""),
        );
    }
}
