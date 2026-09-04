//! `UpdateTable`'s `GlobalSecondaryIndexUpdates` `Create` path (ADR 0045
//! §2/§6): adding a live-backfilling GSI to a **populated** table via the
//! real DynamoDB wire request — the counterpart to
//! `tests/update_table_drop_index.rs`'s `Delete` half, and the last piece
//! `tests/backfill_seeder.rs`/`tests/index_backfill.rs` left for a later PR
//! (they still hand-drive `MetaCommand::CreateTableIndex` since
//! `UpdateTable`'s own index-*creating* half didn't exist yet).
//!
//! Headline scenario: create a GSI on a table that already has rows,
//! observe it immediately report `CREATING`/`Backfilling: true` and reject a
//! `Query` against it, write a few more rows while the backfill is still
//! running (proving live-write coverage, not just the seeder), converge to
//! `ACTIVE`, confirm the query returns exactly the expected rows, then drop
//! it via the already-shipped `Delete` path to prove the two halves compose.
//! The remaining tests cover the client-side validation `create_index`
//! (`animusd/src/dynamo.rs`) performs before ever proposing anything, plus a
//! follower-connected relay regression.

mod support;

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_control::IndexStatus;
use animus_dynamo::wire::MAX_GSI_PER_TABLE;
use animusd::Node;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// One DynamoDB JSON request over the real HTTP wire (duplicated per this
/// codebase's own "every sibling test file that needs the DynamoDB wire
/// keeps its own copy of this helper" convention — see
/// `tests/update_table_drop_index.rs`).
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

async fn create_table_no_index(addr: SocketAddr, table: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
}

async fn put_item(addr: SocketAddr, table: &str, id: &str, attr: &str, value: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        &format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"{attr}":{{"S":"{value}"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "PutItem({id}) failed: {body}");
}

/// `UpdateTable` with a single `GlobalSecondaryIndexUpdates` `Create`
/// element — a hash-only GSI on `hash_attr` — the real wire path this file
/// tests.
async fn create_index_via_wire(
    addr: SocketAddr,
    table: &str,
    index: &str,
    hash_attr: &str,
) -> (u16, String) {
    dynamo(
        addr,
        "DynamoDB_20120810.UpdateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"{hash_attr}","AttributeType":"S"}}],
                "GlobalSecondaryIndexUpdates":[{{"Create":{{
                    "IndexName":"{index}",
                    "KeySchema":[{{"AttributeName":"{hash_attr}","KeyType":"HASH"}}],
                    "Projection":{{"ProjectionType":"ALL"}}}}}}]}}"#
        ),
    )
    .await
}

async fn delete_index_via_wire(addr: SocketAddr, table: &str, index: &str) -> (u16, String) {
    dynamo(
        addr,
        "DynamoDB_20120810.UpdateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "GlobalSecondaryIndexUpdates":[{{"Delete":{{"IndexName":"{index}"}}}}]}}"#
        ),
    )
    .await
}

async fn describe_table(addr: SocketAddr, table: &str) -> (u16, String) {
    dynamo(
        addr,
        "DynamoDB_20120810.DescribeTable",
        &format!(r#"{{"TableName":"{table}"}}"#),
    )
    .await
}

async fn query_index(
    addr: SocketAddr,
    table: &str,
    index: &str,
    hash_attr: &str,
    value: &str,
) -> (u16, String) {
    dynamo(
        addr,
        "DynamoDB_20120810.Query",
        &format!(
            r#"{{"TableName":"{table}","IndexName":"{index}",
                "KeyConditionExpression":"{hash_attr} = :v",
                "ExpressionAttributeValues":{{":v":{{"S":"{value}"}}}}}}"#
        ),
    )
    .await
}

/// Pull `(IndexStatus, Backfilling)` for `index` out of a `DescribeTable`/
/// `UpdateTable` response body's `GlobalSecondaryIndexes` array. `None` if
/// the index isn't listed at all (dropped, or never created).
/// `Backfilling` defaults to `false` when the attribute is absent — matching
/// AWS, which omits it entirely once backfilling has finished rather than
/// rendering it `false`.
fn index_status(body: &str, index: &str) -> Option<(String, bool)> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let table = v.get("Table").or_else(|| v.get("TableDescription"))?;
    let gsis = table.get("GlobalSecondaryIndexes")?.as_array()?;
    let entry = gsis
        .iter()
        .find(|g| g.get("IndexName").and_then(|n| n.as_str()) == Some(index))?;
    let status = entry.get("IndexStatus")?.as_str()?.to_owned();
    let backfilling = entry
        .get("Backfilling")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    Some((status, backfilling))
}

async fn await_true<F: Fn() -> bool>(secs: u64, what: &str, cond: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !cond() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        sleep(Duration::from_millis(100)).await;
    }
}

/// Bring up an `n`-node per-process combined cluster (duplicated from
/// `tests/update_table_drop_index.rs`'s own copy of this fixture).
async fn bring_up(n: usize, dir: &Path) -> (Vec<Node>, animusd::ClusterConfig) {
    let mut brought_up = None;
    'attempts: for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
                advertise_host: None,
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };
        let mut nodes = Vec::new();
        for i in 0..n {
            match animusd::run_node(&config, i, dir.join(format!("node-{attempt}-{i}"))).await {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    for node in &nodes {
                        node.shutdown_graceful().await;
                    }
                    sleep(Duration::from_millis(50)).await;
                    continue 'attempts;
                }
            }
        }
        brought_up = Some((nodes, config));
        break;
    }
    let (nodes, config) =
        brought_up.expect("could not bring up cluster after retries (ports kept getting stolen)");
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap in 20s");
    (nodes, config)
}

/// The headline scenario (ADR 0045 §2/§6): a GSI created on a table that
/// already has rows immediately reports `CREATING`/`Backfilling: true` and
/// rejects a `Query`, a row written while backfill is still running is
/// covered by the live-write path (not merely the seeder), the index
/// converges to `ACTIVE` reporting exactly the expected rows, and dropping
/// it afterward (PR5's already-shipped path) removes it from
/// `DescribeTable` — proving the two UpdateTable halves compose end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn create_index_on_populated_table_backfills_live_and_pre_existing_rows() {
    timeout(Duration::from_secs(120), async {
        let tmp = support::panic_safe_tempdir();
        let (node, _config) =
            support::start_single_node(tmp.path(), animusd::StorageBackend::default()).await;
        let addr = node.dynamo_addr();
        let table = "orders";
        let index = "by-cat";

        create_table_no_index(addr, table).await;
        // Pre-existing rows, written before the index is even declared.
        for (id, cat) in [("p1", "a"), ("p2", "a"), ("p3", "b")] {
            put_item(addr, table, id, "cat", cat).await;
        }

        let (status, body) = create_index_via_wire(addr, table, index, "cat").await;
        assert_eq!(status, 200, "UpdateTable Create failed: {body}");

        // Immediately: CREATING + Backfilling:true, and unqueryable.
        let (ds, db) = describe_table(addr, table).await;
        assert_eq!(ds, 200, "DescribeTable failed: {db}");
        let (ist, backfilling) = index_status(&db, index)
            .unwrap_or_else(|| panic!("index missing from DescribeTable: {db}"));
        assert_eq!(
            ist, "CREATING",
            "expected CREATING right after Create: {db}"
        );
        assert!(
            backfilling,
            "expected Backfilling:true right after Create: {db}"
        );

        let (qs, qb) = query_index(addr, table, index, "cat", "a").await;
        assert_ne!(qs, 200, "Query against a CREATING index should fail: {qb}");
        assert!(
            qb.contains("ValidationException"),
            "expected ValidationException, got: {qb}"
        );

        // A write racing the backfill — must be covered by the live-write
        // path (`table_takes_kind_write_path` gates on index presence, not
        // status), not merely by the seeder's own forward sweep.
        put_item(addr, table, "p4", "cat", "a").await;

        // Converge to ACTIVE.
        timeout(Duration::from_secs(30), async {
            loop {
                let (_, db) = describe_table(addr, table).await;
                if index_status(&db, index).is_some_and(|(s, _)| s == "ACTIVE") {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("index never reached ACTIVE");

        let (_, db) = describe_table(addr, table).await;
        let (ist, backfilling) = index_status(&db, index).expect("index still listed");
        assert_eq!(ist, "ACTIVE");
        assert!(
            !backfilling,
            "Backfilling must not be reported once ACTIVE: {db}"
        );

        // The GSI query is eventually consistent by DynamoDB's own contract
        // (ADR 0041 §5) — every GSI query assertion in this codebase is a
        // converged-or-timeout poll, even after the index itself is ACTIVE.
        timeout(Duration::from_secs(30), async {
            loop {
                let (qs, qb) = query_index(addr, table, index, "cat", "a").await;
                if qs == 200
                    && qb.contains("\"Count\":3")
                    && ["p1", "p2", "p4"]
                        .iter()
                        .all(|id| qb.contains(&format!(r#""id":{{"S":"{id}"}}"#)))
                {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("GSI query for cat=a never converged to the expected 3 rows");

        let (qs, qb) = query_index(addr, table, index, "cat", "a").await;
        assert_eq!(qs, 200, "Query after ACTIVE failed: {qb}");
        assert!(
            !qb.contains("\"S\":\"p3\""),
            "cat=b row leaked into a cat=a query: {qb}"
        );

        // Drop it (PR5's already-shipped path) — proves the two halves compose.
        let (status, body) = delete_index_via_wire(addr, table, index).await;
        assert_eq!(status, 200, "UpdateTable Delete failed: {body}");
        timeout(Duration::from_secs(30), async {
            loop {
                let (_, db) = describe_table(addr, table).await;
                if index_status(&db, index).is_none() {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("index never removed from DescribeTable after Delete");

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// Client-side validation, all rejected before ever proposing anything
/// (`create_index`'s own doc, `crates/animusd/src/dynamo.rs`): a duplicate
/// index name, a reserved-namespace name, and a name containing the hidden
/// index table's own `$` separator. (An LSI-`Create` rejection has no wire
/// path to exercise it through: `GlobalSecondaryIndexUpdates` never decodes
/// to `SecondaryIndex::Local` — there is no `LocalSecondaryIndexUpdates` in
/// the real API either — so that check is defense-in-depth only, verified
/// by inspection rather than an HTTP-level test.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_table_create_validation_rejects_bad_index_declarations() {
    timeout(Duration::from_secs(60), async {
        let tmp = support::panic_safe_tempdir();
        let (node, _config) =
            support::start_single_node(tmp.path(), animusd::StorageBackend::default()).await;
        let addr = node.dynamo_addr();

        // Duplicate name: the table already has this GSI from `CreateTable`.
        let table = "dup_name";
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.CreateTable",
            &format!(
                r#"{{"TableName":"{table}",
                    "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}},
                                             {{"AttributeName":"x","AttributeType":"S"}}],
                    "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                    "GlobalSecondaryIndexes":[
                        {{"IndexName":"by-x","KeySchema":[{{"AttributeName":"x","KeyType":"HASH"}}],
                         "Projection":{{"ProjectionType":"ALL"}}}}]}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");
        let (status, body) = create_index_via_wire(addr, table, "by-x", "y").await;
        assert_ne!(
            status, 200,
            "duplicate index name should be rejected: {body}"
        );
        assert!(
            body.contains("ValidationException"),
            "expected ValidationException, got: {body}"
        );

        // Reserved-namespace name.
        let table2 = "reserved_name";
        create_table_no_index(addr, table2).await;
        let (status, body) = create_index_via_wire(addr, table2, "__animus_system_by_x", "x").await;
        assert_ne!(
            status, 200,
            "reserved index name should be rejected: {body}"
        );
        assert!(
            body.contains("ValidationException"),
            "expected ValidationException, got: {body}"
        );

        // `$`-containing name (the hidden index table's own separator).
        let (status, body) = create_index_via_wire(addr, table2, "by$x", "x").await;
        assert_ne!(
            status, 200,
            "`$`-containing index name should be rejected: {body}"
        );
        assert!(
            body.contains("ValidationException"),
            "expected ValidationException, got: {body}"
        );

        // Create on a table that was never created at all.
        let (status, body) = create_index_via_wire(addr, "no_such_table", "by-x", "x").await;
        assert_ne!(
            status, 200,
            "Create on a nonexistent table should be rejected: {body}"
        );
        assert!(
            body.contains("ResourceNotFoundException"),
            "expected ResourceNotFoundException, got: {body}"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// `create_index` (the `UpdateTable`-`Create` path) enforces AWS's
/// [`MAX_GSI_PER_TABLE`] (20) cap against the table's *current* replicated
/// GSI count — the wire decoder enforces the same cap declaratively at
/// `CreateTable` time (`animus_dynamo::wire`'s own decode-level tests), but
/// only this edge function has the replicated catalog in hand to check a
/// table that accumulated its GSIs one `UpdateTable` call at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn update_table_create_rejects_past_the_gsi_cap() {
    timeout(Duration::from_secs(60), async {
        let tmp = support::panic_safe_tempdir();
        let (node, _config) =
            support::start_single_node(tmp.path(), animusd::StorageBackend::default()).await;
        let addr = node.dynamo_addr();

        // A table declared with exactly the cap's worth of GSIs at
        // `CreateTable` time — accepted, since it is exactly at the limit.
        let table = "at_gsi_cap";
        let gsis: Vec<String> = (0..MAX_GSI_PER_TABLE)
            .map(|i| {
                format!(
                    r#"{{"IndexName":"gsi{i}","KeySchema":[{{"AttributeName":"a{i}","KeyType":"HASH"}}],
                        "Projection":{{"ProjectionType":"ALL"}}}}"#
                )
            })
            .collect();
        let gsi_attribute_defs: Vec<String> = (0..MAX_GSI_PER_TABLE)
            .map(|i| format!(r#"{{"AttributeName":"a{i}","AttributeType":"S"}}"#))
            .collect();
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.CreateTable",
            &format!(
                r#"{{"TableName":"{table}",
                    "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}},{}],
                    "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                    "GlobalSecondaryIndexes":[{}]}}"#,
                gsi_attribute_defs.join(","),
                gsis.join(",")
            ),
        )
        .await;
        assert_eq!(
            status, 200,
            "CreateTable with exactly {MAX_GSI_PER_TABLE} GSIs failed: {body}"
        );

        // A 21st GSI via `UpdateTable`, past the cap, is rejected.
        let (status, body) = create_index_via_wire(addr, table, "one_too_many", "over").await;
        assert_ne!(status, 200, "a 21st GSI should be rejected: {body}");
        assert!(
            body.contains("ValidationException"),
            "expected ValidationException, got: {body}"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// `CreateTableIndex` is already in `is_relayable_command` (PR1) and this
/// scenario proves the whole `UpdateTable`-`Create` path rides that relay
/// correctly end to end: issued against a node that is **not** the
/// control-plane leader, on a 3-node cluster, it must still commit, backfill,
/// and converge to `ACTIVE` on every node — not just the one the HTTP
/// request landed on.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn update_table_create_via_a_non_leader_node_converges_on_every_node() {
    timeout(Duration::from_secs(120), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        let leader = nodes.iter().position(Node::is_control_leader).unwrap();
        let follower = (0..nodes.len()).find(|&i| i != leader).unwrap();
        let follower_addr = nodes[follower].dynamo_addr();
        let table = "relay_create";
        let index = "by-cat";

        create_table_no_index(follower_addr, table).await;
        for (id, cat) in [("r1", "a"), ("r2", "a")] {
            put_item(follower_addr, table, id, "cat", cat).await;
        }

        let (status, body) = create_index_via_wire(follower_addr, table, index, "cat").await;
        assert_eq!(
            status, 200,
            "UpdateTable Create via a follower failed: {body}"
        );

        for n in &nodes {
            await_true(30, "index reaches ACTIVE on every node", || {
                n.metadata()
                    .table_indexes(table)
                    .iter()
                    .any(|i| i.name == index && i.status == IndexStatus::Active)
            })
            .await;
        }

        timeout(Duration::from_secs(30), async {
            loop {
                let (qs, qb) = query_index(follower_addr, table, index, "cat", "a").await;
                if qs == 200 && qb.contains("\"Count\":2") {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("relayed GSI never converged to the expected 2 rows");

        for n in &nodes {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}
