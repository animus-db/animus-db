//! `SimCluster`-driven deterministic siblings for the console's table page
//! Config tab endpoints (ADR 0052 PR3) — `tests/console_table_config.rs`
//! (ADR 0061 rung H, C-08 PR 4). Reuses every helper `sim_cluster_
//! console.rs` (PR 3) already built (`env_seed`/`json`/`assert_no_cluster_
//! shape`/`create_table_via_wire`/`non_leader_of_table`/`control_leader_
//! and_follower`) — see that module's own doc for why they were widened to
//! `pub(crate)` for exactly this reuse.
//!
//! **No `console.rs`/`dynamo.rs`/`sim_cluster.rs` change was needed** —
//! every primitive this module calls was already generic before this PR
//! (`GenericConsoleBackend::table_detail`/`set_stream`/`set_ttl`/`delete_
//! table`, ADR 0061 rung H C-08 PR 2) — pure test authorship.
//!
//! ## Test-by-test disposition (5 converted, 4 kept `ProdEnv`)
//!
//! | Real-socket test | Disposition |
//! |---|---|
//! | `table_detail_projects_full_configuration` | Converted → [`run_table_detail_projects_full_configuration`] |
//! | `add_and_drop_gsi_round_trip` | **KEPT** `ProdEnv` — blocker (d): `UpdateTable` with an index change has no `dispatch_table_op` sub-arm (`add_gsi`/`drop_gsi` both route through it and hit `unsupported_by_generic_dispatch`); the index-DDL residual this whole rung's opener named out of scope |
//! | `add_gsi_records_a_declared_attribute_type` | **KEPT** `ProdEnv` — identical blocker (d) |
//! | `add_gsi_rejects_an_unknown_attribute_type` | **KEPT** `ProdEnv` — identical blocker (d) |
//! | `stream_toggle_round_trips` | Converted → [`run_stream_toggle_round_trips`] |
//! | `ttl_set_and_clear_round_trips` | Converted → [`run_ttl_set_and_clear_round_trips`] |
//! | `delete_table_works` | Converted → [`run_delete_table_works`] |
//! | `table_detail_with_no_pitr_or_backups_is_null_and_empty` | Converted → [`run_table_detail_with_no_pitr_or_backups_is_null_and_empty`] |
//! | `table_detail_shows_pitr_status_and_backups` | **KEPT** `ProdEnv` — this rung's own brief allowed converting this one only if the PITR data it reads is producible under `SimCluster` via a generic `UpdateContinuousBackups` path; checked against the code and there isn't one: `Operation::UpdateContinuousBackups` is absent from both `dispatch_item_op`'s and `dispatch_table_op`'s `match` arms (`dynamo.rs`), so it falls to `unsupported_by_generic_dispatch` — unlike `CreateBackup`/`DeleteBackup`/`UpdateTimeToLive`, which PR 2 did widen. Backup/PITR data stays a separate residual, per the brief's own fallback reason |
//!
//! **Issuing discipline**: `table_detail`/`set_stream`/`set_ttl`/`delete_
//! table` are all reached from a **control follower** — `set_stream`/
//! `set_ttl`/`delete_table` are genuine schema-catalog mutations
//! (`propose_schema`-shaped, same as `sim_cluster_console.rs`'s own
//! console-issued `create_table` scenarios), and `table_detail` is a pure
//! local `effective_metadata()` read with no leader concept — reusing the
//! same follower a mutation already targeted keeps every scenario to one
//! node rather than introducing a second, arbitrary one with nothing to
//! prove by being different.
//!
//! **No product bug found.** Every scenario passed at its pinned seed and
//! every `_over_seeds` seed on the first clean run.
//!
//! Replays (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <scenario name>`.

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{
    assert_no_cluster_shape, control_leader_and_follower, create_table_via_wire, env_seed, json,
};

// ---------------------------------------------------------------------------
// (1) table_detail_projects_full_configuration
// ---------------------------------------------------------------------------

fn run_table_detail_projects_full_configuration(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_leader, follower) = control_leader_and_follower(&mut cluster);

    // ---- a hash-only table, no sort key --------------------------
    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"simple","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(simple) failed: {body}"
    );

    let (status, _ct, body) =
        cluster.console(follower, "GET", "/console/api/tables/simple", "", &[]);
    assert_eq!(status, 200, "seed={seed}: table detail failed: {body}");
    let d = json(&body);
    assert_eq!(d["name"], "simple");
    assert_eq!(d["partition_key"]["name"], "id");
    assert!(d["sort_key"].is_null());
    assert!(d["gsis"].as_array().unwrap().is_empty());
    assert!(d["lsis"].as_array().unwrap().is_empty());
    assert_eq!(d["stream"]["enabled"], false);
    assert_eq!(d["ttl"]["enabled"], false);
    assert_no_cluster_shape(&body);

    // ---- a full-featured table: sort key + GSI + LSI + stream ----
    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"full",
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                     {"AttributeName":"sk","AttributeType":"S"},
                                     {"AttributeName":"cat","AttributeType":"S"},
                                     {"AttributeName":"score","AttributeType":"N"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-cat",
                 "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-score",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"score","KeyType":"RANGE"}]}],
            "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable(full) failed: {body}");
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.UpdateTimeToLive",
        br#"{"TableName":"full",
            "TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTimeToLive(full) failed: {body}"
    );

    let (status, _ct, body) = cluster.console(follower, "GET", "/console/api/tables/full", "", &[]);
    assert_eq!(status, 200, "seed={seed}: table detail failed: {body}");
    let d = json(&body);
    assert_eq!(d["sort_key"]["name"], "sk");
    let gsis = d["gsis"].as_array().unwrap();
    assert_eq!(gsis.len(), 1);
    assert_eq!(gsis[0]["name"], "by-cat");
    assert_eq!(gsis[0]["hash_attribute"]["name"], "cat");
    assert!(gsis[0]["sort_attribute"].is_null());
    assert_eq!(
        gsis[0]["status"], "ACTIVE",
        "seed={seed}: a table created non-empty gets its GSIs Active immediately (ADR 0041 §5): {body}"
    );
    let lsis = d["lsis"].as_array().unwrap();
    assert_eq!(lsis.len(), 1);
    assert_eq!(lsis[0]["name"], "by-score");
    assert_eq!(lsis[0]["sort_attribute"]["name"], "score");
    assert!(
        lsis[0].get("status").is_none(),
        "seed={seed}: an LSI row carries no lifecycle status field: {body}"
    );
    assert_eq!(d["stream"]["enabled"], true);
    assert_eq!(d["stream"]["view_type"], "NEW_AND_OLD_IMAGES");
    assert_eq!(d["ttl"]["enabled"], true);
    assert_eq!(d["ttl"]["attribute_name"], "expiresAt");
    assert_no_cluster_shape(&body);

    // ---- an unknown table 404s -------------------------------------
    let (status, _ct, body) = cluster.console(follower, "GET", "/console/api/tables/nope", "", &[]);
    assert_eq!(status, 404, "seed={seed}: unknown table: {body}");
    assert_eq!(json(&body)["error"], "no such table");
}

#[test]
fn table_detail_projects_full_configuration() {
    run_table_detail_projects_full_configuration(env_seed(0xC084_5001));
}

#[test]
fn table_detail_projects_full_configuration_over_seeds() {
    for i in 0..5 {
        run_table_detail_projects_full_configuration(0xC084_5100 + i);
    }
}

// ---------------------------------------------------------------------------
// (2) stream_toggle_round_trips
// ---------------------------------------------------------------------------

fn run_stream_toggle_round_trips(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_leader, follower) = control_leader_and_follower(&mut cluster);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"events","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    // ---- enable --------------------------------------------------
    let (status, _ct, body) = cluster.console(
        follower,
        "POST",
        "/console/api/tables/events/stream",
        "",
        br#"{"enabled":true,"view_type":"NEW_IMAGE"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: enable stream failed: {body}");
    let resp = json(&body);
    assert_eq!(resp["stream"]["enabled"], true);
    assert_eq!(resp["stream"]["view_type"], "NEW_IMAGE");
    assert_no_cluster_shape(&body);

    let (status, _ct, body) =
        cluster.console(follower, "GET", "/console/api/tables/events", "", &[]);
    assert_eq!(status, 200);
    let d = json(&body);
    assert_eq!(d["stream"]["enabled"], true);
    assert_eq!(d["stream"]["view_type"], "NEW_IMAGE");

    // ---- disable ---------------------------------------------------
    let (status, _ct, body) = cluster.console(
        follower,
        "POST",
        "/console/api/tables/events/stream",
        "",
        br#"{"enabled":false}"#,
    );
    assert_eq!(status, 200, "seed={seed}: disable stream failed: {body}");
    let resp = json(&body);
    assert_eq!(resp["stream"]["enabled"], false);
    assert!(resp["stream"]["view_type"].is_null());

    let (status, _ct, body) =
        cluster.console(follower, "GET", "/console/api/tables/events", "", &[]);
    assert_eq!(status, 200);
    assert_eq!(json(&body)["stream"]["enabled"], false);
}

#[test]
fn stream_toggle_round_trips() {
    run_stream_toggle_round_trips(env_seed(0xC084_6001));
}

#[test]
fn stream_toggle_round_trips_over_seeds() {
    for i in 0..5 {
        run_stream_toggle_round_trips(0xC084_6100 + i);
    }
}

// ---------------------------------------------------------------------------
// (3) ttl_set_and_clear_round_trips
// ---------------------------------------------------------------------------

fn run_ttl_set_and_clear_round_trips(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_leader, follower) = control_leader_and_follower(&mut cluster);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"sessions","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    // ---- set ---------------------------------------------------------
    let (status, _ct, body) = cluster.console(
        follower,
        "POST",
        "/console/api/tables/sessions/ttl",
        "",
        br#"{"enabled":true,"attribute_name":"expiresAt"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: set ttl failed: {body}");
    let resp = json(&body);
    assert_eq!(resp["ttl"]["enabled"], true);
    assert_eq!(resp["ttl"]["attribute_name"], "expiresAt");
    assert_no_cluster_shape(&body);

    let (status, _ct, body) =
        cluster.console(follower, "GET", "/console/api/tables/sessions", "", &[]);
    assert_eq!(status, 200);
    let d = json(&body);
    assert_eq!(d["ttl"]["enabled"], true);
    assert_eq!(d["ttl"]["attribute_name"], "expiresAt");

    // ---- clear (disable) ----------------------------------------------
    let (status, _ct, body) = cluster.console(
        follower,
        "POST",
        "/console/api/tables/sessions/ttl",
        "",
        br#"{"enabled":false,"attribute_name":"expiresAt"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: clear ttl failed: {body}");
    let resp = json(&body);
    assert_eq!(resp["ttl"]["enabled"], false);

    let (status, _ct, body) =
        cluster.console(follower, "GET", "/console/api/tables/sessions", "", &[]);
    assert_eq!(status, 200);
    assert_eq!(json(&body)["ttl"]["enabled"], false);
}

#[test]
fn ttl_set_and_clear_round_trips() {
    run_ttl_set_and_clear_round_trips(env_seed(0xC084_7001));
}

#[test]
fn ttl_set_and_clear_round_trips_over_seeds() {
    for i in 0..5 {
        run_ttl_set_and_clear_round_trips(0xC084_7100 + i);
    }
}

// ---------------------------------------------------------------------------
// (4) delete_table_works
// ---------------------------------------------------------------------------

fn run_delete_table_works(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_leader, follower) = control_leader_and_follower(&mut cluster);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"scratch","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let (status, _ct, body) =
        cluster.console(follower, "GET", "/console/api/tables/scratch", "", &[]);
    assert_eq!(
        status, 200,
        "seed={seed}: table exists before delete: {body}"
    );

    let (status, _ct, body) =
        cluster.console(follower, "DELETE", "/console/api/tables/scratch", "", &[]);
    assert_eq!(status, 200, "seed={seed}: delete_table failed: {body}");
    assert_eq!(json(&body)["ok"], true);
    assert_no_cluster_shape(&body);

    let (status, _ct, body) =
        cluster.console(follower, "GET", "/console/api/tables/scratch", "", &[]);
    assert_eq!(status, 404, "seed={seed}: table gone after delete: {body}");

    let (status, _ct, body) = cluster.console(follower, "GET", "/console/api/tables", "", &[]);
    assert_eq!(status, 200);
    let names: Vec<String> = json(&body)["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    assert!(
        !names.contains(&"scratch".to_string()),
        "seed={seed}: dropped table absent from the tables list: {names:?}"
    );

    // Deleting an already-gone table 404s rather than pretending success.
    let (status, _ct, body) =
        cluster.console(follower, "DELETE", "/console/api/tables/scratch", "", &[]);
    assert_eq!(status, 404, "seed={seed}: double-delete: {body}");
}

#[test]
fn delete_table_works() {
    run_delete_table_works(env_seed(0xC084_8001));
}

#[test]
fn delete_table_works_over_seeds() {
    for i in 0..5 {
        run_delete_table_works(0xC084_8100 + i);
    }
}

// ---------------------------------------------------------------------------
// (5) table_detail_with_no_pitr_or_backups_is_null_and_empty
// ---------------------------------------------------------------------------

fn run_table_detail_with_no_pitr_or_backups_is_null_and_empty(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_leader, follower) = control_leader_and_follower(&mut cluster);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"plain",
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let (status, _ct, body) =
        cluster.console(follower, "GET", "/console/api/tables/plain", "", &[]);
    assert_eq!(status, 200, "seed={seed}: table detail failed: {body}");
    let d = json(&body);
    assert!(d["pitr"].is_null(), "seed={seed}: no PITR enabled: {body}");
    assert!(
        d["backups"].as_array().unwrap().is_empty(),
        "seed={seed}: no backups created: {body}"
    );
    assert_no_cluster_shape(&body);
}

#[test]
fn table_detail_with_no_pitr_or_backups_is_null_and_empty() {
    run_table_detail_with_no_pitr_or_backups_is_null_and_empty(env_seed(0xC084_9001));
}

#[test]
fn table_detail_with_no_pitr_or_backups_is_null_and_empty_over_seeds() {
    for i in 0..5 {
        run_table_detail_with_no_pitr_or_backups_is_null_and_empty(0xC084_9100 + i);
    }
}
