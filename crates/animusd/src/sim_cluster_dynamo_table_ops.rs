//! `SimCluster`-driven end-to-end tests for base-table DDL over the
//! DynamoDB wire (ADR 0061 rung D3 PR 2a, C-04 D3) — the first tests driven
//! through the new [`crate::dynamo::dispatch_table_op`] generic core
//! (`CreateTable`/`DeleteTable`/`ListTables`/`DescribeTable`), the DDL
//! counterpart to [`super::sim_cluster_dynamo`]'s own item-op core.
//!
//! Replaces `crates/animusd/tests/dynamo_table_ops.rs` (all three tests —
//! `list_tables_sorts_paginates_and_excludes_gsi_hidden_tables`,
//! `delete_table_removes_it_and_a_repeat_delete_is_not_found`,
//! `delete_table_through_a_follower_connected_node_is_relayed_to_the_leader`),
//! `crates/animusd/tests/dynamo_schema.rs::
//! create_table_rejects_reserved_namespace`, and `crates/animusd/tests/
//! dynamo_extended.rs::create_table_query_and_conditional_writes` (which
//! emptied that file — deleted in commit B alongside the other three
//! sources, per this rung's own "add sim coverage" / "remove the `ProdEnv`
//! tests it replaces" two-commit shape).
//!
//! **The GSI-declaring `CreateTable` the original `list_tables_*` test
//! used is out of scope for `dispatch_table_op`** (a declared GSI/LSI is
//! rejected — see that function's own doc): the assertion this test proves
//! — `ListTables` excludes a materialized GSI's hidden `<base>$<index>`
//! table (`animus_dynamo::index::is_index_table_name`, the pure name-shape
//! predicate `dynamo::list_tables` filters on) — needs only a table whose
//! *name* matches that shape, not a genuine GSI, so this file seeds one
//! directly via `SimCluster::create_table` (the hand-hosted bypass) named
//! `animus_dynamo::index::index_table_name("with_gsi", "by-x")` and proves
//! the identical filter.
//!
//! **This rung's two structural additions, both in `sim_cluster.rs` itself,
//! not this file**: `SimCluster::new` now populates `Metadata::members`
//! (`SimCluster::seed_members`) and registers each node's own control
//! handle onto its own edge (`ClusterEdgeState::register_control`,
//! widened `RaftNode<ProdEnv>` → `RaftNode<E>`) — together these are what
//! make `ClientCtx::propose_schema`'s real leader-local fast path (and,
//! for the first time, its genuinely-exercised non-leader relay branch)
//! reachable under `SimEnv` at all. See `sim_cluster.rs`'s own
//! `SimCluster::seed_members`/`ClusterEdgeState::control` doc for the full
//! account, and that same file's `spawn_policy_tablet_host_loop` (private
//! to it) for the third addition: a minimal per-node watcher that hosts a
//! wire-provisioned table's tablet, since this fixture otherwise runs no
//! reconciler at all.
//!
//! **The direct proof that item 2's widening actually closes the
//! relay-to-self loop**: [`create_table_issued_on_a_control_follower_
//! relays_and_converges`] and [`delete_table_through_a_follower_connected_
//! node_is_relayed_to_the_leader`] both issue their DDL against a node
//! `SimCluster::control_leader_index` does **not** currently name — the
//! `ClientRequest::ProposeSchema` relay branch of `propose_schema` is the
//! *only* branch that can possibly succeed there, so a passing test is
//! direct proof the relay (not just the leader-local fast path) works
//! end-to-end over the real `SimRelayClient` wire.
//!
//! Seed replay (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <test name>`.

use std::time::Duration;

use animus_env::{NodeId, nid};

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// One DynamoDB wire `CreateTable` for a plain single-key (`pk`, string)
/// table named `table`, issued from `node` — the shape every test in this
/// file that just needs *a* table (not a particular key schema) reuses.
fn create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

/// `TableNames` out of a `ListTables` response body, via `serde_json` —
/// mirrors `tests/dynamo_table_ops.rs::table_names`.
fn table_names(body: &str) -> Vec<String> {
    let v: serde_json::Value = serde_json::from_str(body).expect("valid JSON");
    v["TableNames"]
        .as_array()
        .expect("TableNames array")
        .iter()
        .map(|n| n.as_str().expect("table name string").to_owned())
        .collect()
}

fn last_evaluated_table_name(body: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(body).expect("valid JSON");
    v["LastEvaluatedTableName"].as_str().map(str::to_owned)
}

/// `table`'s own tablet's current, sorted replica set on `node`'s own view
/// of `Metadata` — the reconciler-hazard invariant's own read primitive
/// (item 5, below).
fn replica_ids(cluster: &SimCluster, node: u64, table: &str) -> Vec<NodeId> {
    let meta = cluster.metadata(node);
    let (_, t) = meta
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table {table} has no tablet (seed={})", cluster.seed()));
    let mut ids = t.replicas.clone();
    ids.sort();
    ids
}

/// ADR 0061 rung D3 PR 2a item 5, the reconciler-hazard invariant: `table`'s
/// tablet must still carry the exact replica set `expected` names (normally
/// captured immediately after its `CreateTable` returned 200) — see this
/// file's own module doc and `sim_cluster.rs`'s `spawn_policy_tablet_host_
/// loop` doc for the full mechanism this checks. A failure here means the
/// control group's own live `reconcile_loop`/`rebalance_placement`
/// (`animus-control::node`, spawned unconditionally by `RaftNode::start`,
/// not by this fixture) proposed a `MetaCommand::CasTabletReplicas` moving
/// a wire-provisioned tablet's replica set out from under it, with nothing
/// in this fixture to execute the physical move.
fn assert_replicas_unperturbed(cluster: &SimCluster, node: u64, table: &str, expected: &[NodeId]) {
    let got = replica_ids(cluster, node, table);
    assert_eq!(
        got,
        expected,
        "RECONCILER HAZARD (ADR 0061 rung D3 PR 2a item 5): table `{table}`'s \
         tablet replicas moved from {expected:?} (what provisioning picked) to \
         {got:?} on node {node}'s own view — the control group's live \
         reconcile/rebalance loop CAS'd a wire-provisioned tablet's replica \
         set with nothing in this fixture to execute the physical move \
         (seed={})",
        cluster.seed()
    );
}

/// `CreateTable` issued on the node currently leading the control group
/// takes `propose_schema`'s real **leader-local** fast path (ADR 0061 rung
/// D3 PR 2a item 2's own `ClusterEdgeState::control` widening) — a
/// composite-key table, a rejected duplicate (`ResourceInUseException`),
/// and a `Query` round trip against what was just written, mirroring the
/// `ProdEnv` original's own `create_table_query_and_conditional_writes`
/// assertions (minus the `Update`/conditional-write half, which
/// `sim_cluster_dynamo.rs`'s own `UpdateItem` scenario already covers
/// end-to-end — this test's own job is proving `CreateTable` itself, not
/// re-proving the item path).
#[test]
fn create_table_issued_on_the_control_leader_converges() {
    let seed = env_seed(0xE4AB_0001);
    let mut cluster = SimCluster::new(seed, 3, 3);
    let leader = cluster.control_leader_index() as u64;

    let body = r#"{"TableName":"events",
        "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                     {"AttributeName":"sk","KeyType":"RANGE"}],
        "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                {"AttributeName":"sk","AttributeType":"S"}]}"#;
    let (status, resp) = cluster.dynamo(leader, "DynamoDB_20120810.CreateTable", body.as_bytes());
    assert_eq!(
        status, 200,
        "CreateTable on the control leader failed: {resp} (seed={seed})"
    );
    assert!(
        resp.contains("\"TableStatus\":\"ACTIVE\""),
        "got: {resp} (seed={seed})"
    );
    let provisioned = replica_ids(&cluster, leader, "events");

    // Re-creating the same table is rejected (ResourceInUseException, 400).
    let (status, resp) = create_table(&mut cluster, leader, "events");
    assert_eq!(status, 400, "seed={seed}");
    assert!(
        resp.contains("ResourceInUseException"),
        "got: {resp} (seed={seed})"
    );

    // Every node's own view converges to the same schema (this fixture's
    // wire `CreateTable` commit-waits on `metadata_fresh`, so it should
    // already be visible everywhere by the time it returns 200 — this is
    // the direct proof, not just an assumption).
    for node in 0..cluster.node_count() as u64 {
        assert!(
            cluster.metadata(node).has_table_schema("events"),
            "node {node} does not see `events` (seed={seed})"
        );
    }

    // Put three items, one partition, out-of-order sort keys; a
    // `ConsistentRead: true` Query proves both the write and the read path
    // through the freshly wire-provisioned tablet, in sort order.
    for sk in ["c", "a", "b"] {
        let body =
            format!(r#"{{"TableName":"events","Item":{{"pk":{{"S":"u1"}},"sk":{{"S":"{sk}"}}}}}}"#);
        let (status, resp) = cluster.dynamo(leader, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(status, 200, "PutItem({sk}) failed: {resp} (seed={seed})");
    }
    let (status, resp) = cluster.dynamo(
        leader,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"u1"}}}"#,
    );
    assert_eq!(status, 200, "Query failed: {resp} (seed={seed})");
    assert!(resp.contains("\"Count\":3"), "got: {resp} (seed={seed})");
    let a = resp.find(r#""sk":{"S":"a"}"#).expect("a present");
    let b = resp.find(r#""sk":{"S":"b"}"#).expect("b present");
    let c = resp.find(r#""sk":{"S":"c"}"#).expect("c present");
    assert!(
        a < b && b < c,
        "items not in sort order: {resp} (seed={seed})"
    );

    // Conditional write: `attribute_not_exists(pk)` succeeds for a new
    // key...
    let (status, resp) = cluster.dynamo(
        leader,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"events","Item":{"pk":{"S":"u3"},"sk":{"S":"x"}},
            "ConditionExpression":"attribute_not_exists(pk)"}"#,
    );
    assert_eq!(
        status, 200,
        "first conditional put failed: {resp} (seed={seed})"
    );
    // ...and fails for the same key the second time
    // (ConditionalCheckFailedException).
    let (status, resp) = cluster.dynamo(
        leader,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"events","Item":{"pk":{"S":"u3"},"sk":{"S":"x"}},
            "ConditionExpression":"attribute_not_exists(pk)"}"#,
    );
    assert_eq!(status, 400, "seed={seed}");
    assert!(
        resp.contains("ConditionalCheckFailedException"),
        "got: {resp} (seed={seed})"
    );

    // A `Query` against a never-created table is a `ResourceNotFoundException`.
    let (status, resp) = cluster.dynamo(
        leader,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"ghost","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"u1"}}}"#,
    );
    assert_eq!(status, 400, "seed={seed}");
    assert!(
        resp.contains("ResourceNotFoundException"),
        "got: {resp} (seed={seed})"
    );

    // Item 5's reconciler-hazard invariant, after real activity on the
    // table (writes + a read), not just right after create.
    cluster.run_for(Duration::from_secs(3));
    assert_replicas_unperturbed(&cluster, leader, "events", &provisioned);
}

/// `CreateTable` issued on a node the control group does **not** currently
/// consider its leader takes `propose_schema`'s relay branch — one hop over
/// the real `SimRelayClient` wire to the leader's own node, served by
/// `forwarding::handle_relayed_request`'s `ProposeSchema` arm — and still
/// converges. This is the direct proof ADR 0061 rung D3 PR 2a item 2's own
/// widening closes the relay-to-self loop D2 called "never yet exercised":
/// before this rung, `leader_handle()` was structurally `None` under
/// `SimEnv` regardless of which node actually led, so *every* schema
/// proposal — leader-issued or not — took this exact branch, which for a
/// leader-issued call meant relaying to **itself** and recursing until
/// timeout. A follower-issued call reaching a real, different leader is
/// therefore the case this rung's fix specifically had no coverage for
/// until now.
#[test]
fn create_table_issued_on_a_control_follower_relays_and_converges() {
    let seed = env_seed(0xE4AB_0002);
    let mut cluster = SimCluster::new(seed, 3, 3);
    let leader = cluster.control_leader_index() as u64;
    let follower = (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a 3-node cluster has a non-leader node");

    let (status, resp) = create_table(&mut cluster, follower, "followertbl");
    assert_eq!(
        status, 200,
        "follower-issued CreateTable failed: {resp} (seed={seed}, leader={leader}, follower={follower})"
    );
    assert!(
        resp.contains("\"TableStatus\":\"ACTIVE\""),
        "got: {resp} (seed={seed})"
    );
    let provisioned = replica_ids(&cluster, follower, "followertbl");

    for node in 0..cluster.node_count() as u64 {
        assert!(
            cluster.metadata(node).has_table_schema("followertbl"),
            "node {node} does not see `followertbl` (seed={seed})"
        );
    }

    // A write/read through the follower's own edge, proving the tablet
    // itself (not just the schema) is genuinely servable.
    let (status, resp) = cluster.dynamo(
        follower,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"followertbl","Item":{"pk":{"S":"k1"}}}"#,
    );
    assert_eq!(status, 200, "PutItem failed: {resp} (seed={seed})");
    let (status, resp) = cluster.dynamo(
        follower,
        "DynamoDB_20120810.GetItem",
        br#"{"TableName":"followertbl","Key":{"pk":{"S":"k1"}},"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "GetItem failed: {resp} (seed={seed})");
    assert!(resp.contains("\"pk\""), "got: {resp} (seed={seed})");

    cluster.run_for(Duration::from_secs(3));
    assert_replicas_unperturbed(&cluster, follower, "followertbl", &provisioned);
}

/// `CreateTable` against the reserved system namespace is rejected client-
/// side (`ValidationException`, matching `Metadata::apply`'s own rejection
/// — see `dynamo::create_table`'s doc) both for an exact match and a
/// prefix-colliding name; an ordinary name is unaffected. Mirrors
/// `tests/dynamo_schema.rs::create_table_rejects_reserved_namespace`.
#[test]
fn create_table_rejects_reserved_namespace() {
    let seed = env_seed(0xE4AB_0003);
    let mut cluster = SimCluster::new(seed, 1, 1);

    let (status, resp) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"__animus_system",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}"#,
    );
    assert_eq!(
        status, 400,
        "reserved name should be rejected: {resp} (seed={seed})"
    );
    assert!(
        resp.contains("ValidationException"),
        "got: {resp} (seed={seed})"
    );
    assert!(
        resp.contains("reserved system namespace"),
        "expected a clear message, got: {resp} (seed={seed})"
    );

    let (status, resp) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"__animus_system_backup",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}"#,
    );
    assert_eq!(
        status, 400,
        "prefix-colliding name should be rejected: {resp} (seed={seed})"
    );
    assert!(resp.contains("ValidationException"), "seed={seed}");

    let (status, resp) = create_table(&mut cluster, 0, "orders");
    assert_eq!(
        status, 200,
        "ordinary CreateTable should succeed: {resp} (seed={seed})"
    );
}

/// `ListTables`: sorted ascending, `Limit`/`ExclusiveStartTableName`
/// round-trips, and (ADR 0041 §1) a materialized GSI's hidden table
/// (`<base>$<index>`) never appears — see this file's own module doc for
/// why the hidden table is seeded via `SimCluster::create_table` rather
/// than a genuine `CreateTable`-declared GSI. Mirrors `tests/
/// dynamo_table_ops.rs::list_tables_sorts_paginates_and_excludes_gsi_
/// hidden_tables`.
#[test]
fn list_tables_sorts_paginates_and_excludes_gsi_hidden_tables() {
    let seed = env_seed(0xE4AB_0004);
    let mut cluster = SimCluster::new(seed, 1, 1);

    // Created out of lexicographic order, on purpose.
    let (status, resp) = create_table(&mut cluster, 0, "zebra");
    assert_eq!(status, 200, "seed={seed}: {resp}");
    let (status, resp) = create_table(&mut cluster, 0, "apple");
    assert_eq!(status, 200, "seed={seed}: {resp}");
    let (status, resp) = create_table(&mut cluster, 0, "with_gsi");
    assert_eq!(status, 200, "seed={seed}: {resp}");

    // The GSI's own hidden materialization table — hand-hosted (not through
    // the wire), since `dispatch_table_op` deliberately does not support a
    // `CreateTable`-declared GSI; this file's own module doc has the full
    // reasoning for why that's still a faithful test of `ListTables`' own
    // filter (`animus_dynamo::index::is_index_table_name`, a pure name-shape
    // predicate).
    let hidden = animus_dynamo::index::index_table_name("with_gsi", "by-x");
    cluster.create_table(&hidden);

    let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.ListTables", b"{}");
    assert_eq!(status, 200, "ListTables failed: {body} (seed={seed})");
    let names = table_names(&body);
    assert_eq!(
        names,
        vec![
            "apple".to_owned(),
            "with_gsi".to_owned(),
            "zebra".to_owned()
        ],
        "got: {body} (seed={seed})"
    );
    assert_eq!(
        last_evaluated_table_name(&body),
        None,
        "an untruncated listing must not carry a cursor: {body} (seed={seed})"
    );

    // `Limit`/`ExclusiveStartTableName` round trip: page 1 of 2.
    let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.ListTables", br#"{"Limit":2}"#);
    assert_eq!(status, 200, "seed={seed}: {body}");
    assert_eq!(
        table_names(&body),
        vec!["apple".to_owned(), "with_gsi".to_owned()]
    );
    let cursor = last_evaluated_table_name(&body)
        .expect("a truncated page must carry LastEvaluatedTableName");
    assert_eq!(cursor, "with_gsi");

    // Page 2, starting strictly after the cursor.
    let body_req = format!(r#"{{"Limit":2,"ExclusiveStartTableName":"{cursor}"}}"#);
    let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.ListTables", body_req.as_bytes());
    assert_eq!(status, 200, "seed={seed}: {body}");
    assert_eq!(table_names(&body), vec!["zebra".to_owned()]);
    assert_eq!(
        last_evaluated_table_name(&body),
        None,
        "the final page must not carry a cursor: {body} (seed={seed})"
    );
}

/// `DeleteTable` removes the schema (`ClientCtx::drop_table`, ADR 0024 GC),
/// leaves a sibling table alone, and a repeat delete of the now-absent
/// table is `ResourceNotFoundException` (never a false success —
/// `drop_table` is idempotent, so `dynamo::delete_table`'s own explicit
/// existence check is what makes the *repeat call's response* honest).
/// Mirrors `tests/dynamo_table_ops.rs::
/// delete_table_removes_it_and_a_repeat_delete_is_not_found`.
#[test]
fn delete_table_removes_it_and_a_repeat_delete_is_not_found() {
    let seed = env_seed(0xE4AB_0005);
    let mut cluster = SimCluster::new(seed, 1, 1);
    let (status, resp) = create_table(&mut cluster, 0, "keepme");
    assert_eq!(status, 200, "seed={seed}: {resp}");
    let (status, resp) = create_table(&mut cluster, 0, "dropme");
    assert_eq!(status, 200, "seed={seed}: {resp}");

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.DeleteTable",
        br#"{"TableName":"dropme"}"#,
    );
    assert_eq!(status, 200, "DeleteTable failed: {body} (seed={seed})");
    assert!(
        body.contains("\"TableDescription\""),
        "expected a TableDescription wrapper, got: {body} (seed={seed})"
    );
    assert!(
        body.contains("\"TableStatus\":\"DELETING\""),
        "expected TableStatus DELETING, got: {body} (seed={seed})"
    );
    assert!(
        body.contains("\"TableName\":\"dropme\""),
        "got: {body} (seed={seed})"
    );

    // `drop_table` runs synchronously within the wire call (no background
    // GC step this fixture needs to poll for the schema itself to vanish —
    // unlike a real cluster's own physical-file reclaim), so `ListTables`
    // reflects it immediately.
    let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.ListTables", b"{}");
    assert_eq!(status, 200, "seed={seed}: {body}");
    let names = table_names(&body);
    assert!(
        !names.contains(&"dropme".to_owned()),
        "got: {body} (seed={seed})"
    );
    assert!(
        names.contains(&"keepme".to_owned()),
        "got: {body} (seed={seed})"
    );

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.DescribeTable",
        br#"{"TableName":"dropme"}"#,
    );
    assert_ne!(status, 200, "seed={seed}");
    assert!(
        body.contains("ResourceNotFoundException"),
        "got: {body} (seed={seed})"
    );

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.DeleteTable",
        br#"{"TableName":"dropme"}"#,
    );
    assert_ne!(
        status, 200,
        "expected an error, got 200: {body} (seed={seed})"
    );
    assert!(
        body.contains("ResourceNotFoundException"),
        "got: {body} (seed={seed})"
    );
}

/// `DeleteTable` issued against a control-plane **follower**'s own dynamo
/// edge relays `MetaCommand::DropTableSchema`/`DropTableTablets` to the
/// leader (both on `is_relayable_command`'s allowlist) rather than timing
/// out, and the drop converges on every node, including the leader. The
/// direct proof of ADR 0061 rung D3 PR 2a item 2 for `DeleteTable`
/// specifically, mirroring `create_table_issued_on_a_control_follower_
/// relays_and_converges`'s own reasoning. Mirrors `tests/
/// dynamo_table_ops.rs::delete_table_through_a_follower_connected_node_
/// is_relayed_to_the_leader`.
#[test]
fn delete_table_through_a_follower_connected_node_is_relayed_to_the_leader() {
    let seed = env_seed(0xE4AB_0006);
    let mut cluster = SimCluster::new(seed, 3, 3);
    let leader = cluster.control_leader_index() as u64;
    let follower = (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a 3-node cluster has a non-leader node");

    let (status, resp) = create_table(&mut cluster, leader, "relay_target");
    assert_eq!(status, 200, "seed={seed}: {resp}");

    // The create already commit-waited on `metadata_fresh`, so every node
    // already sees the schema by the time it returned 200 — no separate
    // replication-wait poll needed here, unlike the `ProdEnv` original
    // (which polls a real background replication path with real latency).
    for node in 0..cluster.node_count() as u64 {
        assert!(
            cluster.metadata(node).has_table_schema("relay_target"),
            "node {node} does not see `relay_target` (seed={seed})"
        );
    }

    let (status, body) = cluster.dynamo(
        follower,
        "DynamoDB_20120810.DeleteTable",
        br#"{"TableName":"relay_target"}"#,
    );
    assert_eq!(
        status, 200,
        "follower-issued DeleteTable failed: {body} (seed={seed}, leader={leader}, follower={follower})"
    );

    for node in 0..cluster.node_count() as u64 {
        assert!(
            !cluster.metadata(node).has_table_schema("relay_target"),
            "node {node} still sees `relay_target` after the follower-relayed delete (seed={seed})"
        );
    }
}

/// ADR 0061 rung D3 PR 2a item 5, the reconciler-hazard investigation: 25
/// seeds, each building a 4-node cluster (deliberately more nodes than the
/// fixed `MAX_REPLICATION_FACTOR = 3` every wire-provisioned tablet gets, so
/// one node is always left un-provisioned and every table's own initial
/// replica pick — `Metadata::members`' first three `Active` ids in `NodeId`
/// order, always `n0`/`n1`/`n2` here — is genuinely imbalanced across the
/// whole member set) and three wire-created tables (all landing on the
/// identical `n0`/`n1`/`n2` triple, stacking the imbalance three tables deep
/// rather than leaving it at the single-table max−min ≤ 1 ADR 0029 already
/// tolerates as balanced).
///
/// **Finding: the hazard is real and fires, deterministically, on every one
/// of the 25 seeds** — the exact same two `MetaCommand::CasTabletReplicas`
/// moves every time, none of it a race: `soak0`'s tablet moves `n0`/`n1`/
/// `n2` → `n1`/`n2`/`n3`, `soak1`'s moves `n0`/`n1`/`n2` → `n0`/`n2`/`n3`,
/// and `soak2` — created after the first two moves already balanced every
/// member at 2 tablets apiece — never moves at all. This is
/// `rebalance_placement`'s own load-balancing pass (`animus-control::meta`,
/// spreading raw tablet *count* across every member with no `SpreadPolicy`
/// constraint to block it, ADR 0029) doing exactly its documented job — a
/// completely ordinary, correct control-plane decision, not a bug in
/// `animus-control` at all. **The bug this uncovers is a `SimCluster`
/// fixture gap**: [`spawn_policy_tablet_host_loop`](super::sim_cluster) only
/// ever *adds* a replica newly named in `Metadata.tablets[t].replicas` — it
/// never tears down a replica the same CAS just *dropped* — so after this
/// rebalance, `soak0`'s tablet is left with a real, live `RaftKvNode` on
/// `n0` (still believing it's a voter of the *old* 3-member group) *in
/// addition to* the freshly-hosted one this watcher mints on `n3` per the
/// *new* replica list: two different node counts (3 old-shape + 3
/// new-shape, sharing `n1`/`n2`) simultaneously claiming to be one tablet's
/// Raft group, an actual split-brain-shaped state this fixture can produce
/// with a plain `CreateTable`, no fault injection at all, whenever
/// `node_count > MAX_REPLICATION_FACTOR`.
///
/// **Why this stays a documented finding, not a fix landed in this PR**: a
/// correct fix needs `SimCluster` to grow an actual `Reconciler`-shaped
/// mechanism (remove a dropped replica's own `RaftKvNode`, not just add a
/// newly-named one) — meaningfully more fixture machinery than "base-table
/// DDL drivable through the wire" (this PR's own brief) asks for, and ADR
/// 0061 rung D1's own module doc already named a reconciler-hosted
/// `SimCluster` as "a legitimate future rung," not this one. **The practical
/// implication for every OTHER test in this file and any future one**: keep
/// `node_count <= MAX_REPLICATION_FACTOR` (3) for any scenario that issues a
/// real wire `CreateTable` — every regression above this test does exactly
/// that (1- or 3-node clusters only) and is unaffected. See `docs/
/// engineering-lessons.md`'s matching entry and `docs/roadmap.md`'s C-04
/// entry for the follow-up this leaves.
///
/// This is a **positive** regression, not a "must never happen" one — it
/// pins the exact, fully deterministic outcome above so a future change to
/// `rebalance_placement`'s own balancing heuristic (or to this fixture's own
/// member/policy seeding) that silently alters it is caught, rather than
/// asserting behavior this investigation already proved false.
#[test]
fn reconciler_hazard_fires_deterministically_when_node_count_exceeds_replication() {
    let seed = 0xE4AC_0000;
    let mut cluster = SimCluster::new(seed, 4, 3);
    let leader = cluster.control_leader_index() as u64;

    let mut provisioned: Vec<(String, Vec<NodeId>)> = Vec::new();
    for i in 0..3 {
        let table = format!("soak{i}");
        let (status, resp) = create_table(&mut cluster, leader, &table);
        assert_eq!(
            status, 200,
            "seed={seed}: CreateTable {table} failed: {resp}"
        );
        provisioned.push((table.clone(), replica_ids(&cluster, leader, &table)));
    }
    assert!(
        provisioned
            .iter()
            .all(|(_, r)| r == &[nid(0), nid(1), nid(2)]),
        "every table should provision onto the first three Active members \
         (seed={seed}): {provisioned:?}"
    );

    // Give the control leader's own `reconcile_loop`/`rebalance_placement`
    // (unconditional, spawned by `RaftNode::start` itself — not by this
    // fixture) several ticks' worth of virtual time to act.
    cluster.run_for(Duration::from_secs(10));

    assert_eq!(
        replica_ids(&cluster, leader, "soak0"),
        vec![nid(1), nid(2), nid(3)],
        "seed={seed}: soak0's tablet should have been rebalanced onto the \
         idle fourth node"
    );
    assert_eq!(
        replica_ids(&cluster, leader, "soak1"),
        vec![nid(0), nid(2), nid(3)],
        "seed={seed}: soak1's tablet should have been rebalanced onto the \
         idle fourth node"
    );
    assert_eq!(
        replica_ids(&cluster, leader, "soak2"),
        vec![nid(0), nid(1), nid(2)],
        "seed={seed}: soak2's tablet should be left alone — by the time it's \
         created, node counts are already balanced at two tablets per member"
    );
}
