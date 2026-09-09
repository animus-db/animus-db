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
//! table`, ADR 0061 rung H C-08 PR 2) — pure test authorship. **The three
//! GSI-DDL scenarios added in C-10 PR 6 needed no primitive change either**
//! — blocker (d) (`UpdateTable` with an index change had no `dispatch_
//! table_op` sub-arm) was closed by C-10 PR 2's own `(None, Some(update),
//! None)` sub-arm, so `GenericConsoleBackend::add_gsi`/`drop_gsi` (already
//! generic since C-08 PR 2, previously dead-ending in `unsupported_by_
//! generic_dispatch`) now reach the real `create_index`/`drop_index` path —
//! again pure test authorship, this time on top of PR 2's product change
//! rather than this file's own.
//!
//! ## Test-by-test disposition (8 converted, 1 kept `ProdEnv`)
//!
//! | Real-socket test | Disposition |
//! |---|---|
//! | `table_detail_projects_full_configuration` | Converted → [`run_table_detail_projects_full_configuration`] |
//! | `add_and_drop_gsi_round_trip` | Converted → [`run_add_and_drop_gsi_round_trip`] (C-10 PR 6 — blocker (d) closed by PR 2) |
//! | `add_gsi_records_a_declared_attribute_type` | Converted → [`run_add_gsi_records_a_declared_attribute_type`] (C-10 PR 6) |
//! | `add_gsi_rejects_an_unknown_attribute_type` | Converted → [`run_add_gsi_rejects_an_unknown_attribute_type`] (C-10 PR 6 — pure client-side validation, never reaches dispatch at all) |
//! | `stream_toggle_round_trips` | Converted → [`run_stream_toggle_round_trips`] |
//! | `ttl_set_and_clear_round_trips` | Converted → [`run_ttl_set_and_clear_round_trips`] |
//! | `delete_table_works` | Converted → [`run_delete_table_works`] |
//! | `table_detail_with_no_pitr_or_backups_is_null_and_empty` | Converted → [`run_table_detail_with_no_pitr_or_backups_is_null_and_empty`] |
//! | `table_detail_shows_pitr_status_and_backups` | **KEPT** `ProdEnv` — this rung's own brief allowed converting this one only if the PITR data it reads is producible under `SimCluster` via a generic `UpdateContinuousBackups` path; checked against the code and there isn't one: `Operation::UpdateContinuousBackups` is absent from both `dispatch_item_op`'s and `dispatch_table_op`'s `match` arms (`dynamo.rs`), so it falls to `unsupported_by_generic_dispatch` — unlike `CreateBackup`/`DeleteBackup`/`UpdateTimeToLive`, which PR 2 did widen. Backup/PITR data stays a separate residual, per the brief's own fallback reason |
//!
//! **Issuing discipline**: `table_detail`/`set_stream`/`set_ttl`/`delete_
//! table`/`add_gsi`/`drop_gsi` are all reached from a **control follower** —
//! `set_stream`/`set_ttl`/`delete_table`/`add_gsi`/`drop_gsi` are all
//! genuine schema-catalog mutations (`propose_schema`-shaped, same as
//! `sim_cluster_console.rs`'s own console-issued `create_table` scenarios),
//! and `table_detail` is a pure local `effective_metadata()` read with no
//! leader concept — reusing the same follower a mutation already targeted
//! keeps every scenario to one node rather than introducing a second,
//! arbitrary one with nothing to prove by being different. The one new
//! scenario whose GSI actually needs a live backfill
//! (`add_and_drop_gsi_round_trip` — the other two either never reach
//! dispatch or never populate the table) additionally drives `SimCluster::
//! drive_backfill_seed`/`drain_gsi` on the table's own **tablet leader**
//! (`sim_cluster_console.rs::leader_of_table`) in a bounded converged-or-
//! panic loop of further console `GET`s — mirroring `sim_cluster_index_
//! ddl.rs::converge_gsi_active`'s own shape one file over, duplicated
//! rather than shared per this crate's per-file-fixture convention (that
//! module drives convergence through `DescribeTable`; this one through the
//! console's own `GET /console/api/tables/{name}`, since that is the
//! surface under test here) — never a single call assumed to finish a
//! populated table's backfill in one pass.
//!
//! **No product bug found.** Every scenario passed at its pinned seed and
//! every `_over_seeds` seed on the first clean run.
//!
//! Replays (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <scenario name>`.

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{
    assert_no_cluster_shape, control_leader_and_follower, create_table_via_wire, env_seed, json,
    leader_of_table, put_item_via_wire,
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

// ---------------------------------------------------------------------------
// (6) add_gsi_rejects_an_unknown_attribute_type
// ---------------------------------------------------------------------------

fn run_add_gsi_rejects_an_unknown_attribute_type(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_leader, follower) = control_leader_and_follower(&mut cluster);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"orders","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    // A malformed `hash_attribute_type` (anything but S/N/B, case-
    // insensitively) is rejected by `console_add_gsi_payload`'s own
    // client-side validation before an `UpdateTable` is even built — never
    // reaches `dispatch_table_op`, so this scenario needs no backfill/
    // convergence machinery at all.
    let (status, _ct, body) = cluster.console(
        follower,
        "POST",
        "/console/api/tables/orders/gsi",
        "",
        br#"{"index_name":"by-status","hash_attribute":"status","hash_attribute_type":"X"}"#,
    );
    assert_eq!(status, 400, "seed={seed}: expected a client error: {body}");
}

#[test]
fn add_gsi_rejects_an_unknown_attribute_type() {
    run_add_gsi_rejects_an_unknown_attribute_type(env_seed(0xC084_A001));
}

#[test]
fn add_gsi_rejects_an_unknown_attribute_type_over_seeds() {
    for i in 0..5 {
        run_add_gsi_rejects_an_unknown_attribute_type(0xC084_A100 + i);
    }
}

// ---------------------------------------------------------------------------
// (7) add_gsi_records_a_declared_attribute_type
// ---------------------------------------------------------------------------

fn run_add_gsi_records_a_declared_attribute_type(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_leader, follower) = control_leader_and_follower(&mut cluster);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"readings","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let (status, _ct, body) = cluster.console(
        follower,
        "POST",
        "/console/api/tables/readings/gsi",
        "",
        br#"{"index_name":"by-score","hash_attribute":"score",
            "hash_attribute_type":"N","sort_attribute":"rank",
            "sort_attribute_type":"B"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: add_gsi failed: {body}");
    let resp = json(&body);
    assert_eq!(resp["gsi"]["hash_attribute"]["name"], "score");
    assert_eq!(resp["gsi"]["hash_attribute"]["attribute_type"], "N");
    assert_eq!(resp["gsi"]["sort_attribute"]["name"], "rank");
    assert_eq!(resp["gsi"]["sort_attribute"]["attribute_type"], "B");
    assert_no_cluster_shape(&body);

    // Re-read the table detail fresh — the type is durably in the
    // replicated catalog, not merely echoed off the request.
    let (status, _ct, body) =
        cluster.console(follower, "GET", "/console/api/tables/readings", "", &[]);
    assert_eq!(status, 200, "seed={seed}: table detail failed: {body}");
    let d = json(&body);
    assert_eq!(d["gsis"][0]["hash_attribute"]["attribute_type"], "N");
    assert_eq!(d["gsis"][0]["sort_attribute"]["attribute_type"], "B");

    // And `DescribeTable`'s own `AttributeDefinitions` — the original
    // issue #319 complaint — covers both, for real, not `"S"`.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.DescribeTable",
        br#"{"TableName":"readings"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: DescribeTable failed: {body}");
    assert!(
        body.contains(r#"{"AttributeName":"score","AttributeType":"N"}"#),
        "seed={seed}: score's declared N type missing from AttributeDefinitions: {body}"
    );
    assert!(
        body.contains(r#"{"AttributeName":"rank","AttributeType":"B"}"#),
        "seed={seed}: rank's declared B type missing from AttributeDefinitions: {body}"
    );
}

#[test]
fn add_gsi_records_a_declared_attribute_type() {
    run_add_gsi_records_a_declared_attribute_type(env_seed(0xC084_B001));
}

#[test]
fn add_gsi_records_a_declared_attribute_type_over_seeds() {
    for i in 0..5 {
        run_add_gsi_records_a_declared_attribute_type(0xC084_B100 + i);
    }
}

// ---------------------------------------------------------------------------
// (8) add_and_drop_gsi_round_trip
// ---------------------------------------------------------------------------

/// Pull the `status` string for `index` out of a console table-detail
/// response's `gsis` array — `None` if the index isn't listed at all
/// (dropped, or never created). The console-surface twin of
/// `sim_cluster_index_ddl.rs::index_status`'s own `DescribeTable`-shaped
/// helper, duplicated rather than shared per this crate's per-file-fixture
/// convention.
fn console_gsi_status(body: &str, index: &str) -> Option<String> {
    let d: serde_json::Value = serde_json::from_str(body).ok()?;
    d["gsis"]
        .as_array()?
        .iter()
        .find(|g| g["name"].as_str() == Some(index))?
        .get("status")?
        .as_str()
        .map(str::to_owned)
}

/// Drive the backfill seeder + GSI drain to exhaustion on `table`'s own
/// tablet leader, polling the console's own table-detail endpoint (a real
/// op call each time, which is what actually gives the always-on `index_
/// backfill_loop` completion aggregator a chance to observe the freshly-
/// reported tablet) until `index` converges to `ACTIVE` — never a single
/// call assumed to finish a populated table's backfill in one pass. Mirrors
/// `sim_cluster_index_ddl.rs::converge_gsi_active`'s exact shape, through
/// the console surface instead of `DescribeTable`.
fn converge_gsi_active_via_console(
    cluster: &mut SimCluster,
    follower: u64,
    table: &str,
    index: &str,
) -> String {
    let leader = leader_of_table(cluster, table);
    for _ in 0..10 {
        cluster.drive_backfill_seed(leader, table);
        cluster.drain_gsi(leader, table);
        let path = format!("/console/api/tables/{table}");
        let (status, _ct, body) = cluster.console(follower, "GET", &path, "", &[]);
        assert_eq!(status, 200, "table detail failed: {body}");
        if console_gsi_status(&body, index).as_deref() == Some("ACTIVE") {
            return body;
        }
    }
    panic!("index `{index}` on `{table}` did not converge to ACTIVE within 10 rounds");
}

fn run_add_and_drop_gsi_round_trip(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_leader, follower) = control_leader_and_follower(&mut cluster);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"orders","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    // Populate the table so the added GSI actually needs a backfill — an
    // empty table's index would go straight to Active, which would not
    // distinguish this scenario from the create-time GSI case in
    // `run_table_detail_projects_full_configuration` above.
    let (status, body) = put_item_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"orders","Item":{"id":{"S":"o1"},"status":{"S":"open"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: PutItem failed: {body}");

    let (status, _ct, body) = cluster.console(
        follower,
        "POST",
        "/console/api/tables/orders/gsi",
        "",
        br#"{"index_name":"by-status","hash_attribute":"status"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: add_gsi failed: {body}");
    let resp = json(&body);
    assert_eq!(resp["gsi"]["name"], "by-status");
    assert_eq!(resp["gsi"]["hash_attribute"]["name"], "status");
    // No declared type: this request gave no `hash_attribute_type` (issue
    // #319's fields are optional). Roadmap W-11: `AttributeDefinitions`
    // must now cover every key attribute a request's `KeySchema` names, so
    // `add_gsi` defaults an omitted type to `"S"` — see
    // `add_gsi_records_a_declared_attribute_type` above for the case where
    // a genuinely different type round-trips instead of this default.
    assert_eq!(
        resp["gsi"]["hash_attribute"]["attribute_type"], "S",
        "seed={seed}: an added GSI's key attribute defaults to a declared S type: {body}"
    );
    assert!(resp["gsi"]["sort_attribute"].is_null());
    assert_eq!(
        resp["gsi"]["status"], "CREATING",
        "seed={seed}: a populated table's added GSI starts backfilling: {body}"
    );
    assert_no_cluster_shape(&body);

    let (status, _ct, body) =
        cluster.console(follower, "GET", "/console/api/tables/orders", "", &[]);
    assert_eq!(status, 200);
    let gsis = json(&body)["gsis"].as_array().unwrap().clone();
    assert_eq!(gsis.len(), 1);
    assert_eq!(gsis[0]["name"], "by-status");

    // ---- converges to Active -------------------------------------------
    converge_gsi_active_via_console(&mut cluster, follower, "orders", "by-status");

    // ---- drop it ---------------------------------------------------------
    let (status, _ct, body) = cluster.console(
        follower,
        "DELETE",
        "/console/api/tables/orders/gsi/by-status",
        "",
        &[],
    );
    assert_eq!(status, 200, "seed={seed}: drop_gsi failed: {body}");
    assert_eq!(json(&body)["ok"], true);

    let (status, _ct, body) =
        cluster.console(follower, "GET", "/console/api/tables/orders", "", &[]);
    assert_eq!(status, 200);
    assert!(
        json(&body)["gsis"].as_array().unwrap().is_empty(),
        "seed={seed}: GSI still present after drop: {body}"
    );
}

#[test]
fn add_and_drop_gsi_round_trip() {
    run_add_and_drop_gsi_round_trip(env_seed(0xC084_C001));
}

#[test]
fn add_and_drop_gsi_round_trip_over_seeds() {
    for i in 0..5 {
        run_add_and_drop_gsi_round_trip(0xC084_C100 + i);
    }
}
