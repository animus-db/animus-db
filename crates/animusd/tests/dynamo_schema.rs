//! End-to-end tests of the DynamoDB edge consuming the **replicated schema
//! catalog** (ADR 0013) and the **extended surface** (per-index projections,
//! document-path projections, `UpdateItem`/`BatchWriteItem`/`TransactWriteItems`)
//! over the real DynamoDB JSON/HTTP wire.
//!
//! - `create_table_survives_node_restart` mirrors `durable_restart.rs`: a
//!   single-node cluster `CreateTable`s, the node is stopped and restarted on the
//!   **same dir + addresses**, and the table is still known (its key schema rode
//!   the control-plane Raft WAL, not the in-memory registry). This is the headline
//!   ADR 0013 consumption: a created table is now durable + cluster-agreed.
//! - `scan_and_query_read_live_storage_after_restart` proves the native range
//!   scan reads **live storage**, not an in-memory written-key index: after a
//!   restart wipes the registry, a base `Query` and a `Scan` still return the
//!   previously-written rows (they come from the durable data plane via
//!   `DataClient::scan`, not a tracked key set).
//! - `create_table_index_replicates_to_second_node` proves a `CreateTable`'s GSI
//!   **definition** replicates through the catalog (`MetaCommand::CreateTableIndex`):
//!   it is visible in every node's `Metadata`, and a GSI `Query` resolves on a
//!   *second* node whose registry never saw the `CreateTable` (it rebuilt the index
//!   machinery from the replicated definition).
//! - `create_table_index_survives_node_restart` proves the GSI definition survives a
//!   restart (Raft WAL): after the registry is wiped, a GSI `Query` still works,
//!   recovered from the replicated catalog, not process-local memory — and returns
//!   the **pre-restart item without re-writing it** (the first index query lazily
//!   backfills the edge-local entry data from a base-table scan).
//! - `extended_surface` mirrors `dynamo_extended.rs`: a 3-node in-process cluster
//!   exercises UpdateItem, BatchWriteItem, TransactWriteItems, a document-path
//!   projection, and a `KEYS_ONLY` GSI projection.
//!
//! Real time/sockets (the ProdEnv edge), so we poll with generous timeouts.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, StorageBackend, bind_cluster, start_cluster};
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

async fn await_cluster_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            let leader = nodes.iter().any(Node::is_control_leader);
            let everyone_has_tablet = nodes.iter().all(|n| !n.metadata().members.is_empty());
            if leader && everyone_has_tablet {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not bootstrap within 20s");
}

async fn await_node_bootstrap(node: &Node) {
    let ready = async {
        loop {
            if node.is_control_leader() && !node.metadata().members.is_empty() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("node did not bootstrap within 20s");
}

/// Wait (bounded) until `table`'s schema is visible in this node's replicated
/// catalog.
///
/// `await_node_bootstrap` only gates on leadership + a non-empty tablet map, which
/// does **not** imply the Raft state machine has finished applying a
/// *previously-committed* `CreateTable`: a freshly-restarted leader can be elected
/// (and report a non-empty tablet map) a beat before it replays the catalog entry
/// from its WAL. Probing the schema in that window races recovery — the table
/// briefly looks absent, so a re-`CreateTable` spuriously succeeds (200/ACTIVE)
/// instead of being rejected. These are real-time `ProdEnv` tests (no `SimEnv`
/// determinism), so the sound fix is to poll for the recovered artifact before
/// asserting on it — the same pattern the GSI restart test already uses.
async fn await_table_schema(node: &Node, table: &str) {
    let visible = async {
        loop {
            if node.metadata().has_table_schema(table) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), visible)
        .await
        .unwrap_or_else(|_| panic!("table {table} schema not recovered within 20s"));
}

async fn stop(node: Node) {
    // Graceful: durably flush the control-plane WAL before aborting tasks, so a
    // just-acked `CreateTable` schema survives the restart (a bare `shutdown`
    // races the driver's async fsync — see `Node::shutdown_graceful`).
    node.shutdown_graceful().await;
    drop(node);
    sleep(Duration::from_millis(200)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_table_survives_node_restart() {
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("node-0");

    // --- First incarnation: create a composite table, write + read an item. ---
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let dynamo_addr = config.nodes[0].dynamo;
    await_node_bootstrap(&node).await;

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"events",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                    {"AttributeName":"sk","AttributeType":"S"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    let (status, _) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"events","Item":{"pk":{"S":"u1"},"sk":{"S":"a"},"v":{"N":"1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);

    stop(node).await;

    // --- Second incarnation: SAME dir + addresses. The schema must survive, so a
    // bare PutItem (no re-CreateTable) resolves the composite key correctly and
    // a re-CreateTable is rejected as already existing. The restart reuses the
    // exact config that bound on the first bring-up (same addresses). ---
    let node =
        support::restart_same_addrs(&config, 0, &node_dir, animusd::StorageBackend::default())
            .await;
    await_node_bootstrap(&node).await;
    // Wait for the catalog to recover the table from the Raft WAL before probing —
    // otherwise the re-CreateTable below races recovery and spuriously succeeds.
    await_table_schema(&node, "events").await;

    // Re-creating the surviving table is rejected (ResourceInUseException).
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"events",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}]}"#,
    )
    .await;
    assert_eq!(status, 400, "re-create should be rejected: {body}");
    assert!(
        body.contains("ResourceInUseException"),
        "expected ResourceInUseException, got: {body}"
    );

    // The previously-written item is still readable using the surviving composite
    // schema (its data rode the durable LSM; the schema rode the Raft WAL).
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"u1"},"sk":{"S":"a"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {body}");
    assert!(body.contains(r#""v":{"N":"1"}"#), "item missing: {body}");

    stop(node).await;
}

/// `CreateTable` rejects a table name that collides with the control plane's
/// reserved system-keyspace namespace (ADR 0038 PR1), client-side, with a
/// clear `ValidationException` — not a commit-wait timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_table_rejects_reserved_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("node-0");
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let dynamo_addr = config.nodes[0].dynamo;
    await_node_bootstrap(&node).await;

    // An exact match on the reserved namespace.
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"__animus_system",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}"#,
    )
    .await;
    assert_eq!(status, 400, "reserved name should be rejected: {body}");
    assert!(
        body.contains("ValidationException"),
        "expected ValidationException, got: {body}"
    );
    assert!(
        body.contains("reserved system namespace"),
        "expected a clear message, got: {body}"
    );

    // A name merely prefixed by the reserved namespace also collides.
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"__animus_system_backup",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}"#,
    )
    .await;
    assert_eq!(
        status, 400,
        "prefix-colliding name should be rejected: {body}"
    );
    assert!(body.contains("ValidationException"), "{body}");

    // An ordinary table name is unaffected.
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"orders",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "ordinary CreateTable should succeed: {body}");

    stop(node).await;
}

/// The native range scan reads **live storage**, not an in-memory written-key
/// index: after a node restart the in-memory `SchemaRegistry` is empty (no
/// `note_put` was ever replayed), yet a base-table `Query` and a `Scan` still
/// return the previously-written rows — because the rows come from the durable
/// data plane via `DataClient::scan`, not from any tracked key set. This is the
/// end-to-end proof that the former written-key tracking is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_and_query_read_live_storage_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("node-0");

    // --- First incarnation: create a composite table and write three rows in two
    // partitions, then stop the node (this wipes the in-memory registry). ---
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let dynamo_addr = config.nodes[0].dynamo;
    await_node_bootstrap(&node).await;

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"events",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                    {"AttributeName":"sk","AttributeType":"S"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    for (pk, sk) in [("u1", "a"), ("u1", "b"), ("u2", "a")] {
        let (status, _) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"events","Item":{{"pk":{{"S":"{pk}"}},
                    "sk":{{"S":"{sk}"}},"v":{{"S":"{pk}-{sk}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200);
    }

    stop(node).await;

    // --- Second incarnation: SAME dir + addresses. The schema survives (Raft WAL)
    // and the rows survive (durable LSM), but the registry's written-key set does
    // NOT — it was never tracked and is never replayed. A Scan/Query must still
    // find the rows, proving they are read from live storage. ---
    let node =
        support::restart_same_addrs(&config, 0, &node_dir, animusd::StorageBackend::default())
            .await;
    await_node_bootstrap(&node).await;
    // The Query below resolves the composite key from the recovered schema; wait
    // for the catalog to replay it before probing (it races bootstrap otherwise).
    await_table_schema(&node, "events").await;

    // A full Scan returns all three rows (read straight from the data plane).
    let (status, all) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"events"}"#,
    )
    .await;
    assert_eq!(status, 200, "Scan failed: {all}");
    assert!(all.contains("\"Count\":3"), "scan after restart: {all}");
    assert!(all.contains(r#""v":{"S":"u1-a"}"#), "u1-a missing: {all}");
    assert!(all.contains(r#""v":{"S":"u1-b"}"#), "u1-b missing: {all}");
    assert!(all.contains(r#""v":{"S":"u2-a"}"#), "u2-a missing: {all}");

    // A base-table Query (pk = u1) returns just that partition's two rows, in sort
    // order, again with no in-memory tracking to lean on.
    let (status, q) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"u1"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "Query failed: {q}");
    assert!(q.contains("\"Count\":2"), "query after restart: {q}");
    assert!(q.contains(r#""v":{"S":"u1-a"}"#), "u1-a missing: {q}");
    assert!(q.contains(r#""v":{"S":"u1-b"}"#), "u1-b missing: {q}");
    assert!(!q.contains(r#""v":{"S":"u2-a"}"#), "u2 leaked into u1: {q}");

    // A base Query with a sort-key condition narrows within the partition.
    let (status, narrowed) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events",
            "KeyConditionExpression":"pk = :p AND sk = :s",
            "ExpressionAttributeValues":{":p":{"S":"u1"},":s":{"S":"b"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "narrowed Query failed: {narrowed}");
    assert!(narrowed.contains("\"Count\":1"), "narrowed: {narrowed}");
    assert!(
        narrowed.contains(r#""v":{"S":"u1-b"}"#),
        "narrowed: {narrowed}"
    );

    stop(node).await;
}

/// A `CreateTable` declaring a GSI replicates the **index definition** through the
/// control plane's schema catalog (ADR 0013), so it is visible on a *second* node
/// — proving `animusd` proposes `MetaCommand::CreateTableIndex` rather than keeping
/// the index in process-local memory. We then query the GSI from the second node's
/// edge after writing through the first, proving that node rebuilt its index
/// machinery from the **replicated** definition (its registry never saw the
/// `CreateTable`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_table_index_replicates_to_second_node() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_cluster_bootstrap(&nodes).await;
    let addr0 = nodes[0].dynamo_addr();

    // CreateTable with a GSI on `email`, projecting ALL.
    let (status, body) = dynamo(
        addr0,
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

    // The index DEFINITION must replicate to every node's `Metadata` (cluster-wide
    // and durable, not process-local). Poll, since replication is async.
    let replicated = async {
        loop {
            let everywhere = nodes.iter().all(|n| {
                n.metadata()
                    .table_indexes("users")
                    .iter()
                    .any(|d| d.name == "by-email")
            });
            if everywhere {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(10), replicated)
        .await
        .expect("the GSI definition did not replicate to all nodes");

    // Write an item through node 0.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"users","Item":{"id":{"S":"u1"},"email":{"S":"a@x"},"v":{"N":"7"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");

    // Query the GSI on the SECOND node's edge. That node never saw the CreateTable
    // through its own registry — it must have rebuilt the index machinery from the
    // replicated definition (mirror_catalog_schema → sync_indexes), then indexed the
    // write it observed. Poll until the write has propagated/observed.
    let addr1 = nodes[1].dynamo_addr();
    let queried = async {
        loop {
            let (status, body) = dynamo(
                addr1,
                "DynamoDB_20120810.Query",
                r#"{"TableName":"users","IndexName":"by-email",
                    "KeyConditionExpression":"email = :e",
                    "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#,
            )
            .await;
            // A 200 with the row means the second node knew the index (from the
            // catalog) and resolved the GSI query against it.
            if status == 200 && body.contains(r#""v":{"N":"7"}"#) {
                return body;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    let body = timeout(Duration::from_secs(10), queried)
        .await
        .expect("GSI query on second node never returned the item");
    assert!(body.contains(r#""id":{"S":"u1"}"#), "id missing: {body}");

    for node in nodes {
        node.shutdown();
    }
}

/// A `CreateTable`'s GSI **definition** survives a node restart because it rode the
/// control-plane Raft WAL (ADR 0013), not the in-memory registry. After a restart
/// the registry is empty; a GSI `Query` must still work — proving the edge rebuilt
/// the index machinery from the **replicated catalog** on the freshly restarted
/// node, and a re-CreateTable is rejected as already existing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_table_index_survives_node_restart() {
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("node-0");

    // --- First incarnation: create a table with a GSI + write an indexed item. ---
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let dynamo_addr = config.nodes[0].dynamo;
    await_node_bootstrap(&node).await;

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

    // The definition committed to the catalog before we restart.
    assert!(
        node.metadata()
            .table_indexes("users")
            .iter()
            .any(|d| d.name == "by-email"),
        "GSI definition not in catalog before restart"
    );

    let (status, _) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"users","Item":{"id":{"S":"u1"},"email":{"S":"a@x"},"v":{"N":"7"}}}"#,
    )
    .await;
    assert_eq!(status, 200);

    stop(node).await;

    // --- Second incarnation: SAME dir + addresses. The registry is empty, but the
    // GSI definition rode the Raft WAL and is recovered into `Metadata`. ---
    let node =
        support::restart_same_addrs(&config, 0, &node_dir, animusd::StorageBackend::default())
            .await;
    await_node_bootstrap(&node).await;

    // The definition survived the restart (recovered from the replicated catalog).
    // Poll for it: the index entry replays from the Raft WAL *after* the table
    // schema, a beat behind leadership/bootstrap, so a bare assert here races
    // recovery (same hazard as `await_table_schema`).
    let recovered = async {
        loop {
            if node
                .metadata()
                .table_indexes("users")
                .iter()
                .any(|d| d.name == "by-email")
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), recovered)
        .await
        .expect("GSI definition lost across restart");

    // Re-creating the surviving table is rejected (ResourceInUseException) — the
    // schema (and its index) is known from the catalog, not local memory.
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"users",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    )
    .await;
    assert_eq!(status, 400, "re-create should be rejected: {body}");
    assert!(
        body.contains("ResourceInUseException"),
        "expected ResourceInUseException, got: {body}"
    );

    // A GSI Query must work after the restart — **without re-writing anything**:
    // the edge rebuilds the index *machinery* from the recovered catalog
    // (mirror_catalog_schema → sync_indexes), and the first index query lazily
    // **backfills** the entry data from a base-table scan of the durably stored
    // items (previously the entries were rebuilt only from writes observed by
    // this process, so a post-restart index query silently returned nothing
    // until the item was re-put).
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"users","IndexName":"by-email",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "GSI query after restart failed: {body}");
    assert!(body.contains("\"Count\":1"), "expected one match: {body}");
    assert!(body.contains(r#""v":{"N":"7"}"#), "value missing: {body}");

    stop(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_surface() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_cluster_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    // A simple table with a KEYS_ONLY GSI on `email`.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"users",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-email",
                 "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"KEYS_ONLY"}}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    // PutItem with a nested map attribute (for the document-path projection).
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"users","Item":{"id":{"S":"u1"},"email":{"S":"a@x"},
            "secret":{"S":"hush"},
            "profile":{"M":{"city":{"S":"Paris"},"zip":{"S":"75001"}}}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");

    // UpdateItem: SET a new attr + REMOVE the secret, return ALL_NEW.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"users","Key":{"id":{"S":"u1"}},
            "UpdateExpression":"SET age = :a REMOVE secret",
            "ExpressionAttributeValues":{":a":{"N":"30"}},
            "ReturnValues":"ALL_NEW"}"#,
    )
    .await;
    assert_eq!(status, 200, "UpdateItem failed: {body}");
    assert!(body.contains(r#""age":{"N":"30"}"#), "age not set: {body}");
    assert!(!body.contains("\"secret\""), "secret not removed: {body}");

    // Document-path projection: only `profile.city`.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"users","Key":{"id":{"S":"u1"}},
            "ProjectionExpression":"profile.city"}"#,
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {body}");
    assert!(body.contains("Paris"), "city missing: {body}");
    assert!(
        !body.contains("75001"),
        "zip should be projected out: {body}"
    );
    assert!(
        !body.contains("\"age\""),
        "age should be projected out: {body}"
    );

    // KEYS_ONLY GSI query returns only the key attributes (id + email), not the
    // base item's other attributes (age/profile).
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"users","IndexName":"by-email",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "index Query failed: {body}");
    assert!(body.contains("\"Count\":1"), "expected one match: {body}");
    assert!(body.contains(r#""id":{"S":"u1"}"#), "id missing: {body}");
    assert!(
        body.contains(r#""email":{"S":"a@x"}"#),
        "email missing: {body}"
    );
    assert!(!body.contains("\"age\""), "KEYS_ONLY leaked age: {body}");
    assert!(
        !body.contains("\"profile\""),
        "KEYS_ONLY leaked profile: {body}"
    );

    // BatchWriteItem: put two more users + delete u1.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.BatchWriteItem",
        r#"{"RequestItems":{"users":[
            {"PutRequest":{"Item":{"id":{"S":"u2"},"email":{"S":"b@x"}}}},
            {"PutRequest":{"Item":{"id":{"S":"u3"},"email":{"S":"c@x"}}}},
            {"DeleteRequest":{"Key":{"id":{"S":"u1"}}}}]}}"#,
    )
    .await;
    assert_eq!(status, 200, "BatchWriteItem failed: {body}");
    // u1 is now gone.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"users","Key":{"id":{"S":"u1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body, "{}", "u1 should be deleted: {body}");

    // TransactWriteItems: a ConditionCheck that u2 exists + a conditional Put of
    // u4 only if absent. Both hold, so it succeeds.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"ConditionCheck":{"TableName":"users","Key":{"id":{"S":"u2"}},
                               "ConditionExpression":"attribute_exists(id)"}},
            {"Put":{"TableName":"users","Item":{"id":{"S":"u4"},"email":{"S":"d@x"}},
                    "ConditionExpression":"attribute_not_exists(id)"}}]}"#,
    )
    .await;
    assert_eq!(status, 200, "TransactWriteItems failed: {body}");
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"users","Key":{"id":{"S":"u4"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains(r#""email":{"S":"d@x"}"#),
        "u4 not written: {body}"
    );

    // A failing transaction condition rejects the whole request. Since ADR
    // 0018 §2/PR7 (atomic TransactWriteItems), a transaction's own condition
    // failure is a `TransactionCanceledException` — the real DynamoDB
    // exception type for a transaction — not the bare
    // `ConditionalCheckFailedException` a single-item conditional write
    // returns (see `dynamo.rs::run_transact`'s doc).
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"ConditionCheck":{"TableName":"users","Key":{"id":{"S":"nope"}},
                               "ConditionExpression":"attribute_exists(id)"}}]}"#,
    )
    .await;
    assert_eq!(status, 400, "expected condition failure: {body}");
    assert!(
        body.contains("TransactionCanceledException"),
        "expected TransactionCanceledException, got: {body}"
    );
}
