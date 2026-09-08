//! `SimCluster`-driven deterministic siblings for the console's table page
//! Stream data tab (ADR 0052) — `tests/console_stream.rs` (ADR 0061 rung H,
//! C-08 PR 4). Reuses every helper `sim_cluster_console.rs` (PR 3) already
//! built (`env_seed`/`json`/`assert_no_cluster_shape`/`create_table_via_
//! wire`/`put_item_via_wire`/`leader_of_table`/`non_leader_of_table`), plus
//! [`SimCluster::drive_stream_seal`] (ADR 0061 rung G, C-07 PR 2) for the
//! one scenario below that needs a bounded, deterministic page walk — the
//! identical primitive `sim_cluster_dynamo_streams.rs::next_shard_
//! iterator_pagination_with_small_limit_visits_each_record_once` already
//! uses for the same reason (an OPEN tail's own `GetRecords` has nothing
//! to page across once every write is already visible in one call, so a
//! genuine multi-page walk needs a sealed shard's bounded record range).
//!
//! **No `console.rs`/`dynamo.rs`/`sim_cluster.rs` change was needed** —
//! every primitive this module calls (`SimCluster::console` through
//! [`crate::GenericConsoleBackend`], whose `stream_shards`/`get_shard_
//! iterator`/`get_stream_records` methods route through `dynamo::execute_
//! routed_as_generic` → `dynamo_streams::execute_streams_op_as`, already
//! generic since ADR 0061 rung G C-07 PR 3) was already reachable before
//! this PR — pure test authorship, mirroring PR 3's own "no dispatch
//! change needed" precedent.
//!
//! ## Test-by-test disposition (3 converted, 1 kept `ProdEnv`)
//!
//! | Real-socket test | Disposition |
//! |---|---|
//! | `table_with_no_stream_reports_the_honest_disabled_answer` | Converted → [`run_table_with_no_stream_reports_the_honest_disabled_answer`] |
//! | `stream_enabled_lists_shards_and_records_reflect_real_writes` | Converted → [`run_stream_enabled_lists_shards_and_records_reflect_real_writes`] |
//! | `walking_a_shard_with_next_shard_iterator_visits_every_record_exactly_once` | Converted → [`run_walking_a_shard_with_next_shard_iterator_visits_every_record_exactly_once`] |
//! | `ttl_deletion_carries_the_service_user_identity_through_the_console` | **KEPT** `ProdEnv` — no primitive drives `animusd::ttl_reaper::ttl_reaper_loop` under `SimEnv`: `SimCluster::new`/`restart` never spawn it (unlike `heartbeat_loop`/the reconciler/the backup janitor/the segment janitor, all always-on since D4/rung G), so there is nothing in this fixture that would ever reap the expired item and mint the REMOVE record this test reads. The TTL reaper is its own unowned residual group (this rung's own brief: "do NOT build a reaper driver in this PR") |
//!
//! **Issuing discipline**, mirroring `sim_cluster_console.rs`'s own: every
//! table is created over the real DynamoDB wire from node 0 (the wire path
//! already does its own internal routing regardless of which node issues
//! it, the identical convention PR 3's item-CRUD scenarios use), every
//! write and every console Stream-tab read from a **non-leader** of the
//! table's own tablet.
//!
//! **No product bug found.** Every scenario passed at its pinned seed and
//! every `_over_seeds` seed on the first clean run.
//!
//! Replays (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <scenario name>`.

use std::collections::BTreeSet;

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{
    assert_no_cluster_shape, create_table_via_wire, env_seed, json, leader_of_table,
    non_leader_of_table, put_item_via_wire,
};

// ---------------------------------------------------------------------------
// (1) table_with_no_stream_reports_the_honest_disabled_answer
// ---------------------------------------------------------------------------

fn run_table_with_no_stream_reports_the_honest_disabled_answer(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"plain","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let reader = non_leader_of_table(&cluster, "plain");
    let (status, _ct, body) = cluster.console(
        reader,
        "GET",
        "/console/api/tables/plain/stream/shards",
        "",
        &[],
    );
    assert_eq!(
        status, 200,
        "seed={seed}: stream/shards must be 200 even with no stream: {body}"
    );
    assert_no_cluster_shape(&body);
    let v = json(&body);
    assert_eq!(v["enabled"], false);
    assert!(v["shards"].as_array().unwrap().is_empty());
    assert!(v["stream_arn"].is_null());
    assert!(v["view_type"].is_null());

    // Minting an iterator against a never-enabled table is a clean client
    // error, not a panic/500.
    let (status, _ct, body) = cluster.console(
        reader,
        "POST",
        "/console/api/tables/plain/stream/iterator",
        "",
        br#"{"shard_id":"shardId-1-0","iterator_type":"TRIM_HORIZON"}"#,
    );
    assert_ne!(
        status, 200,
        "seed={seed}: an iterator on an unstreamed table must fail: {body}"
    );
    assert_no_cluster_shape(&body);
}

#[test]
fn table_with_no_stream_reports_the_honest_disabled_answer() {
    run_table_with_no_stream_reports_the_honest_disabled_answer(env_seed(0xC084_0001));
}

#[test]
fn table_with_no_stream_reports_the_honest_disabled_answer_over_seeds() {
    for i in 0..5 {
        run_table_with_no_stream_reports_the_honest_disabled_answer(0xC084_0100 + i);
    }
}

// ---------------------------------------------------------------------------
// (2) stream_enabled_lists_shards_and_records_reflect_real_writes
// ---------------------------------------------------------------------------

fn run_stream_enabled_lists_shards_and_records_reflect_real_writes(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"orders","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let writer = non_leader_of_table(&cluster, "orders");
    for id in ["o1", "o2", "o3"] {
        let (status, body) = put_item_via_wire(
            &mut cluster,
            writer,
            &format!(r#"{{"TableName":"orders","Item":{{"id":{{"S":"{id}"}}}}}}"#),
        );
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
    }

    // ---- shard list -------------------------------------------------
    let reader = non_leader_of_table(&cluster, "orders");
    let (status, _ct, body) = cluster.console(
        reader,
        "GET",
        "/console/api/tables/orders/stream/shards",
        "",
        &[],
    );
    assert_eq!(status, 200, "seed={seed}: stream/shards failed: {body}");
    assert_no_cluster_shape(&body);
    let v = json(&body);
    assert_eq!(v["enabled"], true);
    assert_eq!(v["view_type"], "NEW_AND_OLD_IMAGES");
    assert!(
        v["stream_arn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:dynamodb:"),
        "seed={seed}: stream_arn must be DynamoDB's own ARN shape: {body}"
    );
    let shards = v["shards"].as_array().unwrap();
    assert_eq!(
        shards.len(),
        1,
        "seed={seed}: a fresh single-tablet table has exactly one open shard: {body}"
    );
    let shard_id = shards[0]["shard_id"].as_str().unwrap().to_string();
    assert!(
        shard_id.starts_with("shardId-"),
        "seed={seed}: unexpected shard id shape: {shard_id}"
    );
    assert!(
        shards[0]["ending_sequence_number"].is_null(),
        "seed={seed}: the table's only shard must still be open: {body}"
    );

    // ---- mint an iterator from TRIM_HORIZON --------------------------
    let (status, _ct, body) = cluster.console(
        reader,
        "POST",
        "/console/api/tables/orders/stream/iterator",
        "",
        format!(r#"{{"shard_id":"{shard_id}","iterator_type":"TRIM_HORIZON"}}"#).as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: stream/iterator failed: {body}");
    assert_no_cluster_shape(&body);
    let iterator = json(&body)["shard_iterator"]
        .as_str()
        .expect("shard_iterator")
        .to_string();

    // ---- read every write in one page ---------------------------------
    // The writes above already fully committed (`put_item_via_wire`
    // blocks to completion, ADR 0061 rung H's own `spawn_and_capture`
    // shape) before this open-tail read runs, so — unlike the real-socket
    // original's own converged-or-timeout poll, which exists to wait out
    // an independent async flush — one `GetRecords` call already sees
    // everything: `sim_cluster_dynamo_streams.rs::get_records_over_the_
    // open_tail_before_any_seal`'s own identical single-call shape.
    let (status, _ct, body) = cluster.console(
        reader,
        "POST",
        "/console/api/tables/orders/stream/records",
        "",
        format!(r#"{{"shard_iterator":"{iterator}"}}"#).as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: stream/records failed: {body}");
    assert_no_cluster_shape(&body);
    let v = json(&body);
    let mut seen_ids = BTreeSet::new();
    for record in v["records"].as_array().cloned().unwrap_or_default() {
        assert_eq!(record["eventName"], "INSERT");
        assert!(
            record["userIdentity"].is_null(),
            "seed={seed}: a client write must carry no userIdentity: {record}"
        );
        let pk = record["dynamodb"]["Keys"]["id"]["S"]
            .as_str()
            .unwrap()
            .to_string();
        seen_ids.insert(pk);
    }
    assert_eq!(
        seen_ids,
        ["o1", "o2", "o3"].into_iter().map(String::from).collect(),
        "seed={seed}: every written row's INSERT record must appear: {body}"
    );
}

#[test]
fn stream_enabled_lists_shards_and_records_reflect_real_writes() {
    run_stream_enabled_lists_shards_and_records_reflect_real_writes(env_seed(0xC084_1001));
}

#[test]
fn stream_enabled_lists_shards_and_records_reflect_real_writes_over_seeds() {
    for i in 0..5 {
        run_stream_enabled_lists_shards_and_records_reflect_real_writes(0xC084_1100 + i);
    }
}

// ---------------------------------------------------------------------------
// (3) walking_a_shard_with_next_shard_iterator_visits_every_record_exactly_once
// ---------------------------------------------------------------------------

/// Unlike the real-socket original (which walks the still-OPEN tail with a
/// small `Limit`, relying on production's own periodic `seal_tick` never
/// actually firing within the test's short lifetime), this scenario seals
/// the tablet first via [`SimCluster::drive_stream_seal`] — this fixture
/// never spawns `index_drain::change_consumer_loop` at all (no periodic
/// seal, no periodic anything), and an OPEN tail's own `GetRecords` returns
/// every already-committed record in one page with no `next_shard_
/// iterator` to walk (see scenario (2) above), so forcing a genuine
/// multi-page walk needs a SEALED shard's own bounded record range —
/// exactly `sim_cluster_dynamo_streams.rs::next_shard_iterator_pagination_
/// with_small_limit_visits_each_record_once`'s own reasoning, reused here
/// through the console's own Stream tab endpoints instead of the raw
/// `DynamoDBStreams_20120810.*` wire.
fn run_walking_a_shard_with_next_shard_iterator_visits_every_record_exactly_once(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"widgets","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let writer = non_leader_of_table(&cluster, "widgets");
    let mut expected = BTreeSet::new();
    for i in 0..9 {
        let id = format!("w{i}");
        let (status, body) = put_item_via_wire(
            &mut cluster,
            writer,
            &format!(r#"{{"TableName":"widgets","Item":{{"id":{{"S":"{id}"}}}}}}"#),
        );
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
        expected.insert(id);
    }

    // Seal the tablet's only shard so the walk below has a real, bounded
    // multi-page range to cross — see this function's own doc.
    let leader = leader_of_table(&cluster, "widgets");
    cluster.drive_stream_seal(leader);

    let reader = non_leader_of_table(&cluster, "widgets");
    let (status, _ct, body) = cluster.console(
        reader,
        "GET",
        "/console/api/tables/widgets/stream/shards",
        "",
        &[],
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    assert_no_cluster_shape(&body);
    let shards = json(&body)["shards"].as_array().unwrap().clone();
    // The now-sealed epoch-0 shard covering all 9 writes, plus its still-
    // open, still-empty epoch-1 tail — mirrors `sim_cluster_dynamo_
    // streams.rs`'s own identical post-seal shape.
    assert_eq!(
        shards.len(),
        2,
        "seed={seed}: sealed shard plus its open tail: {body}"
    );
    let shard_id = shards[0]["shard_id"]
        .as_str()
        .expect("shard_id")
        .to_string();
    assert!(
        shards[0]["ending_sequence_number"].is_string(),
        "seed={seed}: the walked shard must be sealed: {body}"
    );

    let (status, _ct, body) = cluster.console(
        reader,
        "POST",
        "/console/api/tables/widgets/stream/iterator",
        "",
        format!(r#"{{"shard_id":"{shard_id}","iterator_type":"TRIM_HORIZON"}}"#).as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    let mut iterator = json(&body)["shard_iterator"]
        .as_str()
        .expect("shard_iterator")
        .to_string();

    // `Limit: 2` forces at least 5 `GetRecords` calls to see all 9
    // records — the pagination this test is actually exercising. The
    // sealed shard's own record range is bounded, so the walk terminates
    // deterministically on `next_shard_iterator: null`, no deadline
    // needed.
    let mut seen = BTreeSet::new();
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(
            pages < 20,
            "seed={seed}: pagination did not converge: seen so far {seen:?}"
        );
        let (status, _ct, body) = cluster.console(
            reader,
            "POST",
            "/console/api/tables/widgets/stream/records",
            "",
            format!(r#"{{"shard_iterator":"{iterator}","limit":2}}"#).as_bytes(),
        );
        assert_eq!(status, 200, "seed={seed}: stream/records failed: {body}");
        assert_no_cluster_shape(&body);
        let v = json(&body);
        let records = v["records"].as_array().cloned().unwrap_or_default();
        assert!(
            records.len() <= 2,
            "seed={seed}: Limit=2 violated: {records:?}"
        );
        for record in &records {
            let pk = record["dynamodb"]["Keys"]["id"]["S"]
                .as_str()
                .unwrap()
                .to_string();
            assert!(
                seen.insert(pk.clone()),
                "seed={seed}: record for `{pk}` was returned by more than one page"
            );
        }
        match v["next_shard_iterator"].as_str() {
            Some(next) => iterator = next.to_string(),
            None => break,
        }
    }
    assert!(
        pages > 1,
        "seed={seed}: the walk should have taken more than one GetRecords call at Limit=2 over 9 records"
    );
    assert_eq!(
        seen, expected,
        "seed={seed}: every record must be visited exactly once across the whole walk"
    );
}

#[test]
fn walking_a_shard_with_next_shard_iterator_visits_every_record_exactly_once() {
    run_walking_a_shard_with_next_shard_iterator_visits_every_record_exactly_once(env_seed(
        0xC084_2001,
    ));
}

#[test]
fn walking_a_shard_with_next_shard_iterator_visits_every_record_exactly_once_over_seeds() {
    for i in 0..5 {
        run_walking_a_shard_with_next_shard_iterator_visits_every_record_exactly_once(
            0xC084_2100 + i,
        );
    }
}
