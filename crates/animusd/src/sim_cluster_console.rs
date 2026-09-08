//! `SimCluster`-driven deterministic siblings for the console's table-list/
//! item-CRUD/create-table/error-mapping test suites (ADR 0061 rung H, C-08
//! PR 3) — the first real scenario coverage of [`SimCluster::console`]
//! itself (PR 2's own two smoke tests, `sim_cluster.rs::
//! admin_status_is_reachable_from_sim_cluster`/`console_tables_lists_a_
//! created_table`, only proved the primitive reaches live state at all).
//!
//! **No `console.rs`/`dynamo.rs`/`sim_cluster.rs` change was needed** — PR 2
//! already built every primitive this PR reuses: [`SimCluster::console`]
//! (goes through [`crate::GenericConsoleBackend`], never `ClientCtx`'s own
//! concrete `impl ConsoleBackend`, per that PR's own newtype-split
//! correction), [`SimCluster::dynamo`] (real DynamoDB-wire `CreateTable`/
//! `PutItem`/`GetItem`/`UpdateTimeToLive`, used to build the fixtures a
//! console call then reads/mutates — matching every real-socket original's
//! own "create over the wire, read/write over the console" split), and
//! [`SimCluster::drain_gsi`] (materializes a GSI's hidden table on demand,
//! standing in for `index_drain::change_consumer_loop`'s periodic drain arm
//! this fixture never spawns — the identical role it already plays for
//! `sim_cluster_dynamo_query_filter.rs` and its siblings).
//!
//! ## Scenarios (seed-parameterized, `_over_seeds` at 5 seeds each)
//!
//! Every scenario issues a **control-plane** mutation (`CreateTable`, the
//! console's own `POST /console/api/tables`) from a control **follower**
//! node wherever one is picked at all — proving the relay path
//! `sim_cluster_dynamo_table_ops.rs::create_table_issued_on_a_control_
//! follower_relays_and_converges` already established still holds reached
//! through the console's own generic dispatch. A tablet-scoped read/write
//! (an item CRUD/Scan/Query call, or a `GetItem`/`PutItem` fixture setup
//! call over the raw wire) is issued from a **tablet non-leader** instead,
//! mirroring every `sim_cluster_dynamo_*` sibling's own convention — the
//! console's tables-LIST endpoint is the one exception: it is a pure local
//! read off `effective_metadata()` with no leader/forwarding concept at all
//! ([`crate::console_table_summaries`]), so those calls use a fixed node
//! with no non-leader search needed. Every read that verifies a write asks
//! for `ConsistentRead: true` (ADR 0055).
//!
//! (1) [`run_tables_endpoint_projects_the_schema_catalog_correctly`] —
//!     mirrors `tests/console_tables.rs`'s one test: five tables (a sort
//!     key, no sort key, GSI+LSI, a stream, TTL) created over the wire, the
//!     console's `GET /console/api/tables` projection checked field by
//!     field, the hidden GSI materialization table's absence, and the
//!     no-cluster-shaped-field property.
//! (2) [`run_create_minimal_table_appears_in_tables_list`] — `POST
//!     /console/api/tables` with only a partition key, the echoed response,
//!     the tables-list projection, and a real `PutItem`/`GetItem` round
//!     trip over the wire proving the console-created table actually works.
//! (3) [`run_create_full_table_declares_everything_exactly`] — a full
//!     declaration (sort key, LSI, GSI with an `INCLUDE` projection, a
//!     stream, TTL) through the console, re-read fresh via `GET
//!     /console/api/tables/{name}` rather than trusting only the echoed
//!     create response.
//! (4) [`run_create_table_rejects_a_duplicate_name`] — a second create under
//!     an already-taken name is a client error, and the original table is
//!     left untouched.
//! (5) [`run_create_table_rejects_an_lsi_with_no_sort_key`] — both shapes of
//!     "an LSI needs a sort key" (the LSI's own attribute blank; the table
//!     itself has no sort key at all) are client errors, and neither leaves
//!     a half-made table (`GET` on the rejected name still 404s).
//! (6) [`run_scan_paginates_and_visits_every_item_exactly_once`] — `POST
//!     .../items/scan`, walked with `Limit`/`exclusive_start_key`/
//!     `last_evaluated_key`, visits every row exactly once.
//! (7) [`run_query_by_partition_key_and_sort_condition`] — `POST
//!     .../items/query`, plain partition-key and then a `between` sort
//!     condition.
//! (8) [`run_put_get_delete_item_round_trip`] — `GetItem` of a never-written
//!     key (200 + null), `PutItem`, `GetItem` reading the exact shape back,
//!     an overwrite (whole-item replace), `DeleteItem`, and a final
//!     `GetItem` confirming it's gone.
//! (9) [`run_scan_and_query_a_gsi_by_name`] — the Items tab's `index_name`
//!     parameter reaches a GSI the same way it reaches the base table, once
//!     [`SimCluster::drain_gsi`] has materialized its hidden table (standing
//!     in for the real-socket original's own converged-or-timeout poll,
//!     which existed only to wait out the periodic drain this fixture
//!     never spawns).
//! (10) [`run_console_error_mapping_and_json_routing_assertions`] — the
//!     `tests/console_endpoint.rs::console_serves_shell_assets_and_deep_
//!     links_on_combined_node` tail this PR's brief named by name: a
//!     freshly-booted cluster's tables list is a valid, empty JSON array,
//!     and an unrecognized console path 404s (the two assertions from that
//!     real-socket test that are genuinely about JSON routing, not shell/
//!     static-asset HTTP framing — see `tests/console_endpoint.rs`'s own
//!     updated doc comment for why the rest of that test, and its two
//!     siblings, stay `ProdEnv`). Extended with the console's own error-
//!     mapping contract this PR's brief also named: a missing table's
//!     detail 404s with a real error body (never a 500), and a malformed
//!     JSON body on a mutating endpoint is a 400 (never a 500).
//!
//! ## ADR 0061 rung H, C-08 PR 3: `console_tables.rs`/`console_create_
//! table.rs`/`console_items.rs` siblings
//!
//! Converts every test in the first three files whole (1 + 4 + 4 = 9 real-
//! socket tests → 9 scenarios above, `(1)`–`(9)`), trimming all three files
//! to nothing (both deleted, per this crate's own "a file left with zero
//! tests is deleted, `Cargo.toml` checked for a stale `[[test]]` entry"
//! discipline — neither file had one, `tests/*.rs` binaries are implicit).
//! `tests/console_endpoint.rs` keeps its own three tests (real HTTP framing/
//! CORS/static-asset/role-split — none of it reachable from `SimCluster`,
//! whose console primitive builds a bare `HttpRequest` with no framing at
//! all and has no node-role concept), each now carrying a one-line reason
//! naming why, plus scenario `(10)` above as this rung's own new coverage
//! of that file's JSON-routing/error-mapping assertions — not a literal
//! conversion (nothing was removed from that file), since scenario `(10)`
//! only reaches a slice of one three-part real-socket test.
//!
//! **No product bug found.** Every scenario passed at its pinned seed and
//! every `_over_seeds` seed on the first clean run once the wire/console
//! JSON shapes matched this module's own fixtures.
//!
//! Replays (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <scenario name>`.

use std::collections::{BTreeMap, BTreeSet};

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

/// No node/tablet/replica/raft/leader/quorum/placement/health/epoch-shaped
/// key anywhere in `body` — the same forbidden-substring list every
/// `console_*.rs` real-socket file checks its own responses against.
fn assert_no_cluster_shape(body: &str) {
    let lower = body.to_ascii_lowercase();
    for forbidden in [
        "\"node",
        "\"tablet",
        "\"replica",
        "\"raft",
        "\"leader",
        "\"quorum",
        "\"placement",
        "\"health",
        "\"epoch",
    ] {
        assert!(
            !lower.contains(forbidden),
            "found cluster-shaped key `{forbidden}` in the console's response: {body}"
        );
    }
}

/// One DynamoDB wire `CreateTable`, issued from `node` — mirrors every
/// other `sim_cluster_*` module's identically-named helper, taking a
/// caller-supplied body since this module's own fixtures need several
/// distinct table shapes (unlike `sim_cluster_dynamo_streams.rs`'s fixed
/// single-key shape).
fn create_table_via_wire(cluster: &mut SimCluster, node: u64, body: &str) -> (u16, String) {
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

fn put_item_via_wire(cluster: &mut SimCluster, node: u64, body: &str) -> (u16, String) {
    cluster.dynamo(node, "DynamoDB_20120810.PutItem", body.as_bytes())
}

fn get_item_via_wire(cluster: &mut SimCluster, node: u64, body: &str) -> (u16, String) {
    cluster.dynamo(node, "DynamoDB_20120810.GetItem", body.as_bytes())
}

/// `table`'s own tablet id, resolved from the replicated catalog — every
/// table in this module is created over the real wire, never via
/// `SimCluster::create_table`'s own hand-hosted-only bookkeeping, mirroring
/// `sim_cluster_dynamo_streams.rs`'s identical helper.
fn tablet_of_table(cluster: &SimCluster, table: &str) -> animus_tablet::TabletId {
    *cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table `{table}` has no tablet"))
        .0
}

fn leader_of_table(cluster: &SimCluster, table: &str) -> u64 {
    let tablet = tablet_of_table(cluster, table);
    cluster
        .leader_index_of(tablet)
        .unwrap_or_else(|| panic!("tablet {} has no leader", tablet.0))
}

/// A node id that does **not** lead `table`'s own tablet.
fn non_leader_of_table(cluster: &SimCluster, table: &str) -> u64 {
    let leader = leader_of_table(cluster, table);
    (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a 3-node cluster always has a non-leader node")
}

/// `(control_leader, a_control_follower)` — mirrors `sim_cluster_dynamo_
/// table_ops.rs`'s own `control_leader_index`-then-find-a-different-node
/// idiom, used here for every console call that mutates the schema catalog
/// (`POST /console/api/tables`), proving the identical control-plane relay
/// path a follower-issued wire `CreateTable` already covers.
fn control_leader_and_follower(cluster: &mut SimCluster) -> (u64, u64) {
    let leader = cluster.control_leader_index() as u64;
    let follower = (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a 3-node cluster always has a control follower");
    (leader, follower)
}

// ---------------------------------------------------------------------------
// (1) console_tables.rs::tables_endpoint_projects_the_schema_catalog_correctly
// ---------------------------------------------------------------------------

fn run_tables_endpoint_projects_the_schema_catalog_correctly(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"with_sort_key",
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"},
                                     {"AttributeName":"ts","AttributeType":"N"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"},
                         {"AttributeName":"ts","KeyType":"RANGE"}]}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(with_sort_key): {body}"
    );

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"without_sort_key","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(without_sort_key): {body}"
    );

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"with_indexes",
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                     {"AttributeName":"sk","AttributeType":"S"},
                                     {"AttributeName":"cat","AttributeType":"S"},
                                     {"AttributeName":"score","AttributeType":"N"},
                                     {"AttributeName":"rank","AttributeType":"N"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-cat",
                 "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-score",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"score","KeyType":"RANGE"}]},
                {"IndexName":"by-rank",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"rank","KeyType":"RANGE"}]}]}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(with_indexes): {body}"
    );

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"with_stream","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_IMAGE"}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable(with_stream): {body}");

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"with_ttl","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable(with_ttl): {body}");
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.UpdateTimeToLive",
        br#"{"TableName":"with_ttl",
            "TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTimeToLive(with_ttl): {body}"
    );

    let (status, _ct, body) = cluster.console(0, "GET", "/console/api/tables", "", &[]);
    assert_eq!(status, 200, "seed={seed}: tables endpoint failed: {body}");
    let value = json(&body);
    let tables: BTreeMap<String, serde_json::Value> = value["tables"]
        .as_array()
        .expect("tables is an array")
        .iter()
        .map(|t| (t["name"].as_str().unwrap().to_string(), t.clone()))
        .collect();
    assert_eq!(
        tables.len(),
        5,
        "seed={seed}: exactly the five created tables, nothing hidden or extra: {tables:?}"
    );

    let t = &tables["with_sort_key"];
    assert_eq!(t["partition_key"]["name"], "id");
    assert_eq!(t["partition_key"]["attribute_type"], "S");
    assert_eq!(t["sort_key"]["name"], "ts");
    assert_eq!(t["sort_key"]["attribute_type"], "N");
    assert_eq!(
        t["gsi_count"], 0,
        "seed={seed}: zero GSIs still renders as 0"
    );
    assert_eq!(
        t["lsi_count"], 0,
        "seed={seed}: a sort key is present, so zero LSIs renders as 0, not null"
    );
    assert_eq!(t["stream"]["enabled"], false);
    assert!(t["stream"]["view_type"].is_null());
    assert_eq!(t["ttl"]["enabled"], false);
    assert!(t["ttl"]["attribute_name"].is_null());

    let t = &tables["without_sort_key"];
    assert_eq!(t["partition_key"]["name"], "id");
    assert!(
        t["sort_key"].is_null(),
        "seed={seed}: no sort key reads as null, not an empty object"
    );
    assert!(
        t["lsi_count"].is_null(),
        "seed={seed}: no sort key means LSIs are structurally impossible: null, not 0"
    );
    assert_eq!(t["gsi_count"], 0);

    let t = &tables["with_indexes"];
    assert_eq!(t["gsi_count"], 1, "seed={seed}: one GSI (by-cat)");
    assert_eq!(
        t["lsi_count"], 2,
        "seed={seed}: two LSIs (by-score, by-rank)"
    );
    assert_eq!(t["sort_key"]["name"], "sk");

    let t = &tables["with_stream"];
    assert_eq!(t["stream"]["enabled"], true);
    assert_eq!(t["stream"]["view_type"], "NEW_IMAGE");
    assert_eq!(t["ttl"]["enabled"], false);

    let t = &tables["with_ttl"];
    assert_eq!(t["ttl"]["enabled"], true);
    assert_eq!(t["ttl"]["attribute_name"], "expiresAt");
    assert_eq!(t["stream"]["enabled"], false);

    assert!(
        !tables.contains_key("with_indexes$by-cat"),
        "seed={seed}: the hidden index table must never appear as a table of its own: {tables:?}"
    );
    for name in tables.keys() {
        assert!(
            !name.contains('$'),
            "seed={seed}: no table name in the response names a hidden index table: {name}"
        );
    }

    let (_, _ct, raw_body) = cluster.console(0, "GET", "/console/api/tables", "", &[]);
    assert_no_cluster_shape(&raw_body);
}

#[test]
fn tables_endpoint_projects_the_schema_catalog_correctly() {
    run_tables_endpoint_projects_the_schema_catalog_correctly(env_seed(0xC083_0001));
}

#[test]
fn tables_endpoint_projects_the_schema_catalog_correctly_over_seeds() {
    for i in 0..5 {
        run_tables_endpoint_projects_the_schema_catalog_correctly(0xC083_0100 + i);
    }
}

// ---------------------------------------------------------------------------
// (2) console_create_table.rs::create_minimal_table_appears_in_tables_list
// ---------------------------------------------------------------------------

fn run_create_minimal_table_appears_in_tables_list(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_leader, follower) = control_leader_and_follower(&mut cluster);

    let (status, _ct, body) = cluster.console(
        follower,
        "POST",
        "/console/api/tables",
        "",
        br#"{"table_name":"simple","partition_key":{"name":"id","attribute_type":"S"}}"#,
    );
    assert_eq!(status, 201, "seed={seed}: create_table failed: {body}");
    let resp = json(&body);
    assert_eq!(resp["table"]["name"], "simple");
    assert_eq!(resp["table"]["partition_key"]["name"], "id");
    assert_eq!(resp["table"]["partition_key"]["attribute_type"], "S");
    assert!(resp["table"]["sort_key"].is_null());
    assert!(resp["table"]["gsis"].as_array().unwrap().is_empty());
    assert!(resp["table"]["lsis"].as_array().unwrap().is_empty());
    assert_eq!(resp["table"]["stream"]["enabled"], false);
    assert_eq!(resp["table"]["ttl"]["enabled"], false);
    assert_no_cluster_shape(&body);

    let (status, _ct, body) = cluster.console(follower, "GET", "/console/api/tables", "", &[]);
    assert_eq!(status, 200);
    let tables = json(&body)["tables"].as_array().unwrap().clone();
    let simple = tables
        .iter()
        .find(|t| t["name"] == "simple")
        .unwrap_or_else(|| panic!("seed={seed}: `simple` missing from the tables list: {body}"));
    assert_eq!(simple["partition_key"]["name"], "id");
    assert!(simple["sort_key"].is_null());
    assert!(
        simple["lsi_count"].is_null(),
        "seed={seed}: a hash-only table structurally has no LSI count: {body}"
    );
    assert_no_cluster_shape(&body);

    // The table the console created is a real, working table — prove it
    // over the real DynamoDB wire, issued from a tablet non-leader.
    let writer = non_leader_of_table(&cluster, "simple");
    let (status, body) = put_item_via_wire(
        &mut cluster,
        writer,
        r#"{"TableName":"simple","Item":{"id":{"S":"x1"}}}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: PutItem on a console-created table: {body}"
    );
    let (status, body) = get_item_via_wire(
        &mut cluster,
        writer,
        r#"{"ConsistentRead":true,"TableName":"simple","Key":{"id":{"S":"x1"}}}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: GetItem on a console-created table: {body}"
    );
    assert_eq!(json(&body)["Item"]["id"]["S"], "x1");
}

#[test]
fn create_minimal_table_appears_in_tables_list() {
    run_create_minimal_table_appears_in_tables_list(env_seed(0xC083_1001));
}

#[test]
fn create_minimal_table_appears_in_tables_list_over_seeds() {
    for i in 0..5 {
        run_create_minimal_table_appears_in_tables_list(0xC083_1100 + i);
    }
}

// ---------------------------------------------------------------------------
// (3) console_create_table.rs::create_full_table_declares_everything_exactly
// ---------------------------------------------------------------------------

fn run_create_full_table_declares_everything_exactly(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_leader, follower) = control_leader_and_follower(&mut cluster);

    let req = r#"{
        "table_name": "orders",
        "partition_key": {"name": "order_id", "attribute_type": "S"},
        "sort_key": {"name": "created_at", "attribute_type": "N"},
        "lsis": [{"index_name": "by-score", "sort_attribute": "score"}],
        "gsis": [{
            "index_name": "by-region",
            "hash_attribute": "region",
            "sort_attribute": "priority",
            "projection_type": "INCLUDE",
            "projection_non_key_attributes": ["total"]
        }],
        "stream_enabled": true,
        "stream_view_type": "NEW_AND_OLD_IMAGES",
        "ttl_enabled": true,
        "ttl_attribute_name": "expiresAt"
    }"#;
    let (status, _ct, body) =
        cluster.console(follower, "POST", "/console/api/tables", "", req.as_bytes());
    assert_eq!(status, 201, "seed={seed}: create_table failed: {body}");
    assert_no_cluster_shape(&body);

    let (status, _ct, body) =
        cluster.console(follower, "GET", "/console/api/tables/orders", "", &[]);
    assert_eq!(status, 200, "seed={seed}: table detail failed: {body}");
    let d = json(&body);

    assert_eq!(d["name"], "orders");
    assert_eq!(d["partition_key"]["name"], "order_id");
    assert_eq!(d["partition_key"]["attribute_type"], "S");
    assert_eq!(d["sort_key"]["name"], "created_at");
    assert_eq!(d["sort_key"]["attribute_type"], "N");

    let lsis = d["lsis"].as_array().unwrap();
    assert_eq!(lsis.len(), 1);
    assert_eq!(lsis[0]["name"], "by-score");
    assert_eq!(lsis[0]["sort_attribute"]["name"], "score");
    assert_eq!(
        lsis[0]["sort_attribute"]["attribute_type"], "S",
        "seed={seed}: an LSI's own alternate sort attribute defaults to a declared S \
         type when declared at CreateTable time: {body}"
    );
    assert!(
        lsis[0].get("status").is_none(),
        "seed={seed}: an LSI row carries no lifecycle status field: {body}"
    );

    let gsis = d["gsis"].as_array().unwrap();
    assert_eq!(gsis.len(), 1);
    assert_eq!(gsis[0]["name"], "by-region");
    assert_eq!(gsis[0]["hash_attribute"]["name"], "region");
    assert_eq!(
        gsis[0]["hash_attribute"]["attribute_type"], "S",
        "seed={seed}: a GSI's hash attribute defaults to a declared S type: {body}"
    );
    assert_eq!(gsis[0]["sort_attribute"]["name"], "priority");
    assert_eq!(gsis[0]["sort_attribute"]["attribute_type"], "S");
    assert_eq!(
        gsis[0]["status"], "ACTIVE",
        "seed={seed}: a CreateTable-declared index is Active immediately: {body}"
    );
    assert_eq!(gsis[0]["projection"]["projection_type"], "INCLUDE");
    assert_eq!(
        gsis[0]["projection"]["non_key_attributes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["total"]
    );

    assert_eq!(d["stream"]["enabled"], true);
    assert_eq!(d["stream"]["view_type"], "NEW_AND_OLD_IMAGES");
    assert_eq!(d["ttl"]["enabled"], true);
    assert_eq!(d["ttl"]["attribute_name"], "expiresAt");
    assert_no_cluster_shape(&body);
}

#[test]
fn create_full_table_declares_everything_exactly() {
    run_create_full_table_declares_everything_exactly(env_seed(0xC083_2001));
}

#[test]
fn create_full_table_declares_everything_exactly_over_seeds() {
    for i in 0..5 {
        run_create_full_table_declares_everything_exactly(0xC083_2100 + i);
    }
}

// ---------------------------------------------------------------------------
// (4) console_create_table.rs::create_table_rejects_a_duplicate_name
// ---------------------------------------------------------------------------

fn run_create_table_rejects_a_duplicate_name(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_leader, follower) = control_leader_and_follower(&mut cluster);

    let req = r#"{"table_name":"dup","partition_key":{"name":"id","attribute_type":"S"}}"#;
    let (status, _ct, body) =
        cluster.console(follower, "POST", "/console/api/tables", "", req.as_bytes());
    assert_eq!(status, 201, "seed={seed}: first create failed: {body}");

    let dup_req = r#"{
        "table_name": "dup",
        "partition_key": {"name": "id", "attribute_type": "S"},
        "sort_key": {"name": "sk", "attribute_type": "S"}
    }"#;
    let (status, _ct, body) = cluster.console(
        follower,
        "POST",
        "/console/api/tables",
        "",
        dup_req.as_bytes(),
    );
    assert!(
        (400..500).contains(&status),
        "seed={seed}: duplicate create must be a client error, not a {status}: {body}"
    );
    assert!(!json(&body)["error"].as_str().unwrap_or_default().is_empty());
    assert_no_cluster_shape(&body);

    let (status, _ct, body) = cluster.console(follower, "GET", "/console/api/tables/dup", "", &[]);
    assert_eq!(status, 200, "seed={seed}: original table gone: {body}");
    assert!(json(&body)["sort_key"].is_null());
}

#[test]
fn create_table_rejects_a_duplicate_name() {
    run_create_table_rejects_a_duplicate_name(env_seed(0xC083_3001));
}

#[test]
fn create_table_rejects_a_duplicate_name_over_seeds() {
    for i in 0..5 {
        run_create_table_rejects_a_duplicate_name(0xC083_3100 + i);
    }
}

// ---------------------------------------------------------------------------
// (5) console_create_table.rs::create_table_rejects_an_lsi_with_no_sort_key
// ---------------------------------------------------------------------------

fn run_create_table_rejects_an_lsi_with_no_sort_key(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_leader, follower) = control_leader_and_follower(&mut cluster);

    // The LSI's own `sort_attribute` is blank.
    let req = r#"{
        "table_name": "no_lsi_sort",
        "partition_key": {"name": "id", "attribute_type": "S"},
        "sort_key": {"name": "sk", "attribute_type": "S"},
        "lsis": [{"index_name": "broken", "sort_attribute": ""}]
    }"#;
    let (status, _ct, body) =
        cluster.console(follower, "POST", "/console/api/tables", "", req.as_bytes());
    assert!(
        (400..500).contains(&status),
        "seed={seed}: an LSI with a blank sort attribute must be a client error, not a {status}: {body}"
    );
    assert!(!json(&body)["error"].as_str().unwrap_or_default().is_empty());
    assert_no_cluster_shape(&body);
    let (status, _ct, _body) =
        cluster.console(follower, "GET", "/console/api/tables/no_lsi_sort", "", &[]);
    assert_eq!(
        status, 404,
        "seed={seed}: rejected create must not leave a half-made table"
    );

    // The table itself has no sort key at all, but still declares an LSI.
    let req = r#"{
        "table_name": "no_table_sort",
        "partition_key": {"name": "id", "attribute_type": "S"},
        "lsis": [{"index_name": "broken", "sort_attribute": "score"}]
    }"#;
    let (status, _ct, body) =
        cluster.console(follower, "POST", "/console/api/tables", "", req.as_bytes());
    assert!(
        (400..500).contains(&status),
        "seed={seed}: an LSI on a sort-key-less table must be a client error, not a {status}: {body}"
    );
    assert_no_cluster_shape(&body);
    let (status, _ct, _body) = cluster.console(
        follower,
        "GET",
        "/console/api/tables/no_table_sort",
        "",
        &[],
    );
    assert_eq!(
        status, 404,
        "seed={seed}: rejected create must not leave a half-made table"
    );
}

#[test]
fn create_table_rejects_an_lsi_with_no_sort_key() {
    run_create_table_rejects_an_lsi_with_no_sort_key(env_seed(0xC083_4001));
}

#[test]
fn create_table_rejects_an_lsi_with_no_sort_key_over_seeds() {
    for i in 0..5 {
        run_create_table_rejects_an_lsi_with_no_sort_key(0xC083_4100 + i);
    }
}

// ---------------------------------------------------------------------------
// (6) console_items.rs::scan_paginates_and_visits_every_item_exactly_once
// ---------------------------------------------------------------------------

fn run_scan_paginates_and_visits_every_item_exactly_once(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"widgets","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let writer = non_leader_of_table(&cluster, "widgets");
    let mut expected_ids = BTreeSet::new();
    for i in 0..7 {
        let id = format!("w{i}");
        let (status, body) = put_item_via_wire(
            &mut cluster,
            writer,
            &format!(
                r#"{{"TableName":"widgets","Item":{{"id":{{"S":"{id}"}},"n":{{"N":"{i}"}}}}}}"#
            ),
        );
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
        expected_ids.insert(id);
    }

    let reader = non_leader_of_table(&cluster, "widgets");
    let (status, _ct, body) = cluster.console(
        reader,
        "POST",
        "/console/api/tables/widgets/items/scan",
        "",
        b"{}",
    );
    assert_eq!(status, 200, "seed={seed}: scan failed: {body}");
    assert_no_cluster_shape(&body);
    let v = json(&body);
    assert_eq!(v["items"].as_array().unwrap().len(), 7);
    assert!(v["last_evaluated_key"].is_null());

    let mut seen = BTreeSet::new();
    let mut cursor: Option<serde_json::Value> = None;
    let mut pages = 0;
    loop {
        let req = match &cursor {
            Some(key) => serde_json::json!({"limit": 2, "exclusive_start_key": key}).to_string(),
            None => serde_json::json!({"limit": 2}).to_string(),
        };
        let (status, _ct, body) = cluster.console(
            reader,
            "POST",
            "/console/api/tables/widgets/items/scan",
            "",
            req.as_bytes(),
        );
        assert_eq!(status, 200, "seed={seed}: paginated scan failed: {body}");
        assert_no_cluster_shape(&body);
        let page = json(&body);
        pages += 1;
        for item in page["items"].as_array().unwrap() {
            let id = item["id"]["S"].as_str().unwrap().to_string();
            assert!(
                seen.insert(id.clone()),
                "seed={seed}: item {id} was returned by more than one page"
            );
        }
        assert!(
            pages < 20,
            "seed={seed}: pagination did not converge within 20 pages: seen so far {seen:?}"
        );
        if page["last_evaluated_key"].is_null() {
            break;
        }
        cursor = Some(page["last_evaluated_key"].clone());
    }
    assert!(
        pages > 1,
        "seed={seed}: the walk should have taken more than one page at Limit=2 over 7 items"
    );
    assert_eq!(
        seen, expected_ids,
        "seed={seed}: every item must be visited exactly once across the whole walk"
    );
}

#[test]
fn scan_paginates_and_visits_every_item_exactly_once() {
    run_scan_paginates_and_visits_every_item_exactly_once(env_seed(0xC083_5001));
}

#[test]
fn scan_paginates_and_visits_every_item_exactly_once_over_seeds() {
    for i in 0..5 {
        run_scan_paginates_and_visits_every_item_exactly_once(0xC083_5100 + i);
    }
}

// ---------------------------------------------------------------------------
// (7) console_items.rs::query_by_partition_key_and_sort_condition
// ---------------------------------------------------------------------------

fn run_query_by_partition_key_and_sort_condition(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"orders",
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                     {"AttributeName":"sk","AttributeType":"N"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let writer = non_leader_of_table(&cluster, "orders");
    for (pk, sk) in [("cust-1", 1), ("cust-1", 2), ("cust-1", 3), ("cust-2", 1)] {
        let (status, body) = put_item_via_wire(
            &mut cluster,
            writer,
            &format!(
                r#"{{"TableName":"orders","Item":{{"pk":{{"S":"{pk}"}},"sk":{{"N":"{sk}"}}}}}}"#
            ),
        );
        assert_eq!(status, 200, "seed={seed}: PutItem failed: {body}");
    }

    let reader = non_leader_of_table(&cluster, "orders");
    let (status, _ct, body) = cluster.console(
        reader,
        "POST",
        "/console/api/tables/orders/items/query",
        "",
        br#"{"partition_value":{"S":"cust-1"}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: query failed: {body}");
    assert_no_cluster_shape(&body);
    let v = json(&body);
    let items = v["items"].as_array().unwrap();
    assert_eq!(items.len(), 3, "seed={seed}: query result: {body}");
    for item in items {
        assert_eq!(item["pk"]["S"], "cust-1");
    }

    let (status, _ct, body) = cluster.console(
        reader,
        "POST",
        "/console/api/tables/orders/items/query",
        "",
        br#"{"partition_value":{"S":"cust-1"},
            "sort_condition":{"kind":"between","lo":{"N":"2"},"hi":{"N":"3"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: narrowed query failed: {body}");
    let v = json(&body);
    let items = v["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "seed={seed}: narrowed query result: {body}");
    let mut sks: Vec<i64> = items
        .iter()
        .map(|i| i["sk"]["N"].as_str().unwrap().parse().unwrap())
        .collect();
    sks.sort_unstable();
    assert_eq!(sks, vec![2, 3]);
}

#[test]
fn query_by_partition_key_and_sort_condition() {
    run_query_by_partition_key_and_sort_condition(env_seed(0xC083_6001));
}

#[test]
fn query_by_partition_key_and_sort_condition_over_seeds() {
    for i in 0..5 {
        run_query_by_partition_key_and_sort_condition(0xC083_6100 + i);
    }
}

// ---------------------------------------------------------------------------
// (8) console_items.rs::put_get_delete_item_round_trip
// ---------------------------------------------------------------------------

fn run_put_get_delete_item_round_trip(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"sessions","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let node = non_leader_of_table(&cluster, "sessions");

    let (status, _ct, body) = cluster.console(
        node,
        "POST",
        "/console/api/tables/sessions/items/get",
        "",
        br#"{"key":{"id":{"S":"s1"}}}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: get of a missing item must still be 200: {body}"
    );
    assert_no_cluster_shape(&body);
    assert!(json(&body)["item"].is_null());

    let (status, _ct, body) = cluster.console(
        node,
        "POST",
        "/console/api/tables/sessions/items/put",
        "",
        br#"{"item":{"id":{"S":"s1"},"active":{"BOOL":true},"tag":{"S":"first"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: put failed: {body}");
    assert_no_cluster_shape(&body);
    assert_eq!(json(&body)["ok"], true);

    let (status, _ct, body) = cluster.console(
        node,
        "POST",
        "/console/api/tables/sessions/items/get",
        "",
        br#"{"key":{"id":{"S":"s1"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: get failed: {body}");
    let v = json(&body);
    assert_eq!(v["item"]["id"]["S"], "s1");
    assert_eq!(v["item"]["active"]["BOOL"], true);
    assert_eq!(v["item"]["tag"]["S"], "first");

    let (status, _ct, body) = cluster.console(
        node,
        "POST",
        "/console/api/tables/sessions/items/put",
        "",
        br#"{"item":{"id":{"S":"s1"},"active":{"BOOL":false}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: overwrite put failed: {body}");

    let (status, _ct, body) = cluster.console(
        node,
        "POST",
        "/console/api/tables/sessions/items/get",
        "",
        br#"{"key":{"id":{"S":"s1"}}}"#,
    );
    assert_eq!(status, 200);
    let v = json(&body);
    assert_eq!(v["item"]["active"]["BOOL"], false);
    assert!(
        v["item"].get("tag").is_none(),
        "seed={seed}: PutItem wholesale-replaces the item — the old `tag` attribute must be gone: {body}"
    );

    let (status, _ct, body) = cluster.console(
        node,
        "POST",
        "/console/api/tables/sessions/items/delete",
        "",
        br#"{"key":{"id":{"S":"s1"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: delete failed: {body}");
    assert_no_cluster_shape(&body);
    assert_eq!(json(&body)["ok"], true);

    let (status, _ct, body) = cluster.console(
        node,
        "POST",
        "/console/api/tables/sessions/items/get",
        "",
        br#"{"key":{"id":{"S":"s1"}}}"#,
    );
    assert_eq!(status, 200);
    assert!(
        json(&body)["item"].is_null(),
        "seed={seed}: item must be gone after delete: {body}"
    );
}

#[test]
fn put_get_delete_item_round_trip() {
    run_put_get_delete_item_round_trip(env_seed(0xC083_7001));
}

#[test]
fn put_get_delete_item_round_trip_over_seeds() {
    for i in 0..5 {
        run_put_get_delete_item_round_trip(0xC083_7100 + i);
    }
}

// ---------------------------------------------------------------------------
// (9) console_items.rs::scan_and_query_a_gsi_by_name
// ---------------------------------------------------------------------------

fn run_scan_and_query_a_gsi_by_name(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"orders",
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"},
                                     {"AttributeName":"status","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-status",
                 "KeySchema":[{"AttributeName":"status","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let writer = non_leader_of_table(&cluster, "orders");
    for (id, status_val) in [("o1", "open"), ("o2", "open"), ("o3", "closed")] {
        let (status, body) = put_item_via_wire(
            &mut cluster,
            writer,
            &format!(
                r#"{{"TableName":"orders","Item":{{"id":{{"S":"{id}"}},"status":{{"S":"{status_val}"}}}}}}"#
            ),
        );
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
    }

    // Materialize the GSI's hidden table on demand — this fixture never
    // spawns the periodic drain the real-socket original's own converged-
    // or-timeout poll exists to wait out.
    let leader = leader_of_table(&cluster, "orders");
    cluster.drain_gsi(leader, "orders");

    let reader = non_leader_of_table(&cluster, "orders");
    let (status, _ct, body) = cluster.console(
        reader,
        "POST",
        "/console/api/tables/orders/items/scan",
        "",
        br#"{"index_name":"by-status"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: gsi scan failed: {body}");
    assert_no_cluster_shape(&body);
    assert_eq!(
        json(&body)["items"].as_array().unwrap().len(),
        3,
        "seed={seed}: gsi scan result: {body}"
    );

    let (status, _ct, body) = cluster.console(
        reader,
        "POST",
        "/console/api/tables/orders/items/query",
        "",
        br#"{"index_name":"by-status","partition_value":{"S":"open"}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: gsi query failed: {body}");
    assert_no_cluster_shape(&body);
    let items = json(&body)["items"].as_array().unwrap().clone();
    assert_eq!(items.len(), 2, "seed={seed}: gsi query result: {body}");
    for item in &items {
        assert_eq!(item["status"]["S"], "open");
    }
}

#[test]
fn scan_and_query_a_gsi_by_name() {
    run_scan_and_query_a_gsi_by_name(env_seed(0xC083_8001));
}

#[test]
fn scan_and_query_a_gsi_by_name_over_seeds() {
    for i in 0..5 {
        run_scan_and_query_a_gsi_by_name(0xC083_8100 + i);
    }
}

// ---------------------------------------------------------------------------
// (10) console_endpoint.rs's JSON-routing assertions + the console's own
// error-mapping contract (missing table -> 404, malformed body -> 400).
// ---------------------------------------------------------------------------

fn run_console_error_mapping_and_json_routing_assertions(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    // A freshly-booted node's tables list is a valid, empty JSON array —
    // `tests/console_endpoint.rs::console_serves_shell_assets_and_deep_
    // links_on_combined_node`'s own tables-endpoint assertion.
    let (status, ct, body) = cluster.console(0, "GET", "/console/api/tables", "", &[]);
    assert_eq!(status, 200, "seed={seed}: tables endpoint failed: {body}");
    assert_eq!(ct, "application/json", "seed={seed}: {body}");
    assert_eq!(
        json(&body)["tables"],
        serde_json::json!([]),
        "seed={seed}: a freshly-booted node has no tables yet: {body}"
    );

    // An unrecognized path still 404s — that same test's own last
    // assertion.
    let (status, _ct, _body) = cluster.console(0, "GET", "/console/api/nonexistent", "", &[]);
    assert_eq!(
        status, 404,
        "seed={seed}: an unrecognized path must still 404"
    );

    // A missing table's own detail 404s with a real error body, never a
    // 500 — the console's error-mapping contract this rung's brief named
    // by name (a `ResourceNotFoundException`-shaped 404, DynamoDB's own
    // vocabulary for "no such resource").
    let (status, _ct, body) =
        cluster.console(0, "GET", "/console/api/tables/does-not-exist", "", &[]);
    assert_eq!(
        status, 404,
        "seed={seed}: a missing table's detail must 404, not 500: {body}"
    );
    assert!(
        !json(&body)["error"].as_str().unwrap_or_default().is_empty(),
        "seed={seed}: the 404 must carry a real error message: {body}"
    );

    // A malformed JSON body on a mutating endpoint is a 400, never a 500.
    let (status, _ct, body) = cluster.console(0, "POST", "/console/api/tables", "", b"not json");
    assert_eq!(
        status, 400,
        "seed={seed}: a malformed create-table body must be a 400, not 500: {body}"
    );
    assert!(
        !json(&body)["error"].as_str().unwrap_or_default().is_empty(),
        "seed={seed}: the 400 must carry a real error message: {body}"
    );
}

#[test]
fn console_error_mapping_and_json_routing_assertions() {
    run_console_error_mapping_and_json_routing_assertions(env_seed(0xC083_9001));
}

#[test]
fn console_error_mapping_and_json_routing_assertions_over_seeds() {
    for i in 0..5 {
        run_console_error_mapping_and_json_routing_assertions(0xC083_9100 + i);
    }
}
