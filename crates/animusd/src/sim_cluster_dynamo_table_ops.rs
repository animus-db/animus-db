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
//! account.
//!
//! **ADR 0061 rung D4 PR 1 (2026-09-07) replaced this rung's own third
//! addition** — a minimal per-node watcher (`spawn_policy_tablet_host_
//! loop`, since deleted) that only ever *added* a wire-provisioned table's
//! own newly-named replica, never tearing one down a rebalance dropped —
//! **with a real per-node `animus_cp_data::host::Reconciler`** (`sim_
//! cluster.rs`'s `build_reconciler`/`spawn_reconciler_loop`), closing issue
//! #715. [`reconciler_hazard_fires_deterministically_when_node_count_
//! exceeds_replication`] below, this rung's own characterization of the gap,
//! is now [`every_node_hosts_exactly_its_replica_set_after_rebalance`] — a
//! convergence proof instead of a documented hazard.
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

/// `table`'s tablet must still carry the exact replica set `expected` names
/// (normally captured immediately after its `CreateTable` returned 200) —
/// a proof that the control group's own live `reconcile_loop`/`rebalance_
/// placement` (`animus-control::node`, spawned unconditionally by
/// `RaftNode::start`, not by this fixture) had nothing to move for this
/// scenario's own node/table count (every caller below stays at
/// `node_count <= MAX_REPLICATION_FACTOR`, where a single table's own
/// initial placement is already balanced). Since ADR 0061 rung D4 PR 1 a
/// genuine rebalance move is no longer a hazard either way — see
/// [`every_node_hosts_exactly_its_replica_set_after_rebalance`] — but this
/// check stays useful as a "nothing moved for a reason unrelated to what
/// this test means to prove" regression.
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
///
/// **4 nodes, not 3 (ADR 0061 rung D4 PR 1)**: this test used to stay at
/// `node_count <= MAX_REPLICATION_FACTOR` (3) like every other real-wire
/// `CreateTable` scenario in this module, per D3 PR 2a's own reconciler-
/// hazard finding. Bumped as this rung's own proof that the restriction no
/// longer applies — the fourth, initially-idle node is `follower`-eligible
/// too now, so this also strengthens the scenario slightly (a follower that
/// may itself host no replica of the fresh tablet at all, the same
/// "genuine forward" shape `sim_cluster.rs`'s own scenario 2 tests).
#[test]
fn create_table_issued_on_a_control_follower_relays_and_converges() {
    let seed = env_seed(0xE4AB_0002);
    let mut cluster = SimCluster::new(seed, 4, 3);
    let leader = cluster.control_leader_index() as u64;
    let follower = (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a 4-node cluster has a non-leader node");

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

/// ADR 0061 rung D4 PR 1 (closing issue #715): the identical 4-node, three-
/// wire-table scenario ADR 0061 rung D3 PR 2a's own reconciler-hazard
/// investigation used (deliberately more nodes than `MAX_REPLICATION_
/// FACTOR = 3` every wire-provisioned tablet gets, so one node is always
/// left un-provisioned and every table's own initial replica pick —
/// `Metadata::members`' first three `Active` ids in `NodeId` order, always
/// `n0`/`n1`/`n2` here — is genuinely imbalanced across the whole member
/// set), but flipped from a characterization of the hazard to a
/// **convergence proof it's closed**: `rebalance_placement`'s own
/// load-balancing pass (`animus-control::meta`, spreading raw tablet
/// *count* across every member, ADR 0029 — a completely ordinary, correct
/// control-plane decision) still moves `soak0`'s and `soak1`'s tablets onto
/// the idle fourth node exactly as it always did (leaving `soak2` alone,
/// already balanced by the time it's created), but now every node's own
/// real `host::Reconciler` (`sim_cluster.rs`'s `build_reconciler`/`spawn_
/// reconciler_loop`, ADR 0061 rung D4 PR 1) tears down the dropped replica
/// exactly as production's own reconciler does — so this is renamed from
/// `reconciler_hazard_fires_deterministically_when_node_count_exceeds_
/// replication` and now asserts the **absence** of the old finding's own
/// split-brain-shaped state, via [`assert_no_zombie_groups`]'s "every
/// node's own hosted set equals exactly its `Metadata` replica set"
/// invariant, at the identical pinned seed plus a loop of ten more.
fn run_every_node_hosts_exactly_its_replica_set_after_rebalance(seed: u64) {
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
    // fixture) several ticks' worth of virtual time to act, then let every
    // node's own reconciler converge onto whatever `Metadata` now says.
    cluster.run_for(Duration::from_secs(10));
    assert_no_zombie_groups(&mut cluster, seed);

    // The rebalance itself still happened exactly as the original
    // investigation found — this is what makes the invariant check above
    // meaningful (a scenario where nothing ever moved would prove nothing
    // about teardown).
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

/// The shared "no zombie groups" invariant (ADR 0061 rung D4 PR 1): every
/// node's own [`SimCluster::hosted_tablets`] set equals EXACTLY the tablets
/// whose current `Metadata` replica set names that node — no stale handle
/// for a replica a rebalance dropped, and no missing host for one it just
/// added. Converged-or-timeout polled, never a one-shot assert: a real
/// reconciler's own teardown is itself async
/// (`animus_cp_data::host::RECLAIM_STOP_TIMEOUT`-bounded on the production
/// side), so a snapshot taken mid-teardown can legitimately still show a
/// stale entry for one more tick.
fn assert_no_zombie_groups(cluster: &mut SimCluster, seed: u64) {
    const BUDGET: Duration = Duration::from_secs(10);
    const STEP: Duration = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    loop {
        let mut mismatch: Option<String> = None;
        for node in 0..cluster.node_count() as u64 {
            let meta = cluster.metadata(node);
            let expected: std::collections::BTreeSet<_> = meta
                .tablets
                .iter()
                .filter(|(_, t)| t.replicas.contains(&nid(node)))
                .map(|(&id, _)| id)
                .collect();
            let got = cluster.hosted_tablets(node);
            if got != expected {
                mismatch = Some(format!(
                    "node {node}: hosted={got:?} expected(from Metadata)={expected:?} \
                     (seed={seed})"
                ));
                break;
            }
        }
        match mismatch {
            None => return,
            Some(detail) => {
                assert!(
                    elapsed < BUDGET,
                    "hosted-tablet sets never converged to Metadata's own \
                     replica sets within {BUDGET:?} (seed={seed}): {detail}"
                );
                cluster.run_for(STEP);
                elapsed += STEP;
            }
        }
    }
}

#[test]
fn every_node_hosts_exactly_its_replica_set_after_rebalance() {
    run_every_node_hosts_exactly_its_replica_set_after_rebalance(0xE4AC_0000);
}

/// `docs/engineering-lessons.md`'s "a fixture that reimplements half of a
/// production loop inherits the other half's hazards" lesson, proven across
/// ten more seeds beyond the pinned one above — every seed reproduces the
/// identical rebalance the original investigation found (`rebalance_
/// placement` is deterministic given a fixed member/policy sequence), so
/// this is really re-proving convergence under ten different Raft
/// election/replication interleavings, not ten different rebalance
/// outcomes.
#[test]
fn every_node_hosts_exactly_its_replica_set_after_rebalance_over_seeds() {
    for i in 0..10 {
        run_every_node_hosts_exactly_its_replica_set_after_rebalance(0xE4AC_1000 + i);
    }
}

/// ADR 0061 rung D3 PR 3a documented a boundary here (`dispatch_item_op`'s
/// `Query`/`Scan` arms dispatch a named index through the generic
/// `run_index_query`/`run_index_scan`, but a **GSI** row used to be
/// materialized only by `index_drain::change_consumer_loop` — a background
/// loop `SimCluster` never spawns) and named this test
/// `gsi_query_reads_empty_under_the_fixture_until_the_drain_generalizes`,
/// pinning the pre-PR-3b empty-`Count` outcome as a deliberate placeholder
/// for the day the drain generalized. **PR 3b is that day**: `[SimCluster::
/// drain_gsi]` (`sim_cluster.rs`) is a test-only stand-in for
/// `change_consumer_loop`'s own GSI-drain arm, so this test is flipped to
/// the positive assertion it was always meant to become, and renamed to
/// match — a `PutItem` followed by `drain_gsi` followed by a `Query` now
/// returns the row.
///
/// Also confirms the hidden index table actually got a tablet
/// (`Metadata::has_table_tablet`, PR 3a's own empty-page gate) — the fact
/// that used to make every GSI query here read as an unconditional empty
/// `200` regardless of anything else about the request, including a
/// malformed cursor. With a real tablet behind it, that gate no longer
/// masks whatever a query's own cursor-shape/condition validation would
/// otherwise catch — see `sim_cluster_dynamo_query_pagination.rs::
/// cross_index_cursor_mismatch_is_rejected`, which PR 3b restored a fourth
/// sub-case to for exactly this reason.
#[test]
fn gsi_query_materializes_rows_after_a_drain() {
    let seed = env_seed(0xE4AD_0001);
    let mut cluster = SimCluster::new(seed, 1, 1);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"users","AttributeDefinitions":[{"AttributeName":"email","AttributeType":"S"},{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-email",
                 "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    );
    assert_eq!(
        status, 200,
        "CreateTable with a declared GSI failed: {body} (seed={seed})"
    );

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"users","Item":{"id":{"S":"u1"},"email":{"S":"a@x"}}}"#,
    );
    assert_eq!(status, 200, "PutItem failed: {body} (seed={seed})");

    // Before the drain: the hidden index table has no tablet yet, and the
    // query reads as an honest, gate-driven empty page.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"users","IndexName":"by-email",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#,
    );
    assert_eq!(
        status, 200,
        "GSI query itself must not error: {body} (seed={seed})"
    );
    assert!(
        body.contains("\"Count\":0"),
        "before any drain, a GSI query reads empty: {body} (seed={seed})"
    );
    let hidden_table = animus_dynamo::index_table_name("users", "by-email");
    assert!(
        !cluster.metadata(0).has_table_tablet(&hidden_table),
        "the hidden index table must have no tablet before the first drain \
         (seed={seed})"
    );

    let meta0 = cluster.metadata(0);
    let (tablet, _) = meta0
        .tablets_for_table("users")
        .next()
        .unwrap_or_else(|| panic!("users has no tablet (seed={seed})"));
    let tablet = *tablet;
    drop(meta0);
    let leader = cluster
        .leader_index_of(tablet)
        .expect("users tablet has a leader");
    cluster.drain_gsi(leader, "users");

    assert!(
        cluster.metadata(0).has_table_tablet(&hidden_table),
        "the hidden index table must have a tablet after the drain \
         (seed={seed})"
    );

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"users","IndexName":"by-email",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#,
    );
    assert_eq!(
        status, 200,
        "GSI query after the drain must not error: {body} (seed={seed})"
    );
    assert!(
        body.contains("\"Count\":1") && body.contains("\"u1\""),
        "after the drain, the GSI query returns the materialized row: \
         {body} (seed={seed})"
    );
}
