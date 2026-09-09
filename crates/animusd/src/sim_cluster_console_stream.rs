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
//! ## Test-by-test disposition (4 converted, 0 kept `ProdEnv`)
//!
//! | Real-socket test | Disposition |
//! |---|---|
//! | `table_with_no_stream_reports_the_honest_disabled_answer` | Converted → [`run_table_with_no_stream_reports_the_honest_disabled_answer`] |
//! | `stream_enabled_lists_shards_and_records_reflect_real_writes` | Converted → [`run_stream_enabled_lists_shards_and_records_reflect_real_writes`] |
//! | `walking_a_shard_with_next_shard_iterator_visits_every_record_exactly_once` | Converted → [`run_walking_a_shard_with_next_shard_iterator_visits_every_record_exactly_once`] |
//! | `ttl_deletion_carries_the_service_user_identity_through_the_console` | Converted → [`run_ttl_deletion_carries_the_service_user_identity_through_the_console`] (ADR 0061 rung I, C-09 PR 4 — the always-on TTL reaper is now real under `SimEnv`, C-09 PR 2; `tests/console_stream.rs` deleted whole, this was its only test) |
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
use std::time::Duration;

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{
    assert_no_cluster_shape, create_table_via_wire, env_seed, get_item_via_wire, json,
    leader_of_table, non_leader_of_table, put_item_via_wire,
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

// ---------------------------------------------------------------------------
// (4) ttl_deletion_carries_the_service_user_identity_through_the_console
//     (ADR 0061 rung I, C-09 PR 4)
// ---------------------------------------------------------------------------

/// Bounded converged-or-timeout poll for the always-on TTL reaper — mirrors
/// `sim_cluster_ttl.rs`'s own private `poll_until_reaped` (not importable
/// across modules, an independent copy of the identical shape, the same
/// convention this module's own module doc already follows for `describe_
/// stream_via_wire`-shaped helpers) for the absence case (a bare `GetItem`
/// response with no `"Item"` key at all).
fn poll_until_reaped(cluster: &mut SimCluster, node: u64, get_body: &str, seed: u64) {
    const ATTEMPTS: usize = 20;
    const STEP: Duration = Duration::from_millis(400);
    let mut last = String::new();
    for _ in 0..ATTEMPTS {
        let (status, body) = get_item_via_wire(cluster, node, get_body);
        if status == 200 && !body.contains("\"Item\"") {
            return;
        }
        last = format!("status={status} body={body}");
        cluster.run_for(STEP);
    }
    panic!(
        "seed={seed}: item was never reaped by the always-on TTL loop within \
         {ATTEMPTS} attempts (last={last})"
    );
}

/// ADR 0051 §7: a TTL-reaper delete's stream record carries `userIdentity`
/// (`{"PrincipalId": "dynamodb.amazonaws.com", "Type": "Service"}`) when
/// read through the console's own `stream/records` endpoint, exactly as it
/// does over the raw DynamoDB Streams wire — the console passes the wire
/// `Record` shape straight through (`console::StreamRecordsPage`'s own
/// doc), so this is the console-side half of the regression `sim_cluster_
/// ttl.rs::run_ttl_deletion_is_visible_in_the_stream_with_a_service_user_
/// identity` already proves over the raw wire (C-09 PR 3).
///
/// Converted from `tests/console_stream.rs`'s only test (ADR 0061 rung I,
/// C-09 PR 4), now that the always-on TTL reaper actually runs under
/// `SimEnv` (C-09 PR 2) — `tests/console_stream.rs` deleted whole, since
/// this was its sole remaining test.
fn run_ttl_deletion_carries_the_service_user_identity_through_the_console(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "sessions";

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "StreamSpecification":{{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
        ),
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.UpdateTimeToLive",
        format!(
            r#"{{"TableName":"{table}","TimeToLiveSpecification":{{"Enabled":true,"AttributeName":"expiresAt"}}}}"#
        )
        .as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: UpdateTimeToLive failed: {body}");

    let non_leader = non_leader_of_table(&cluster, table);
    let past = cluster.wall_now_secs(non_leader).saturating_sub(3600);
    let (status, body) = put_item_via_wire(
        &mut cluster,
        non_leader,
        &format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"s1"}},"expiresAt":{{"N":"{past}"}}}}}}"#
        ),
    );
    assert_eq!(status, 200, "seed={seed}: PutItem failed: {body}");

    // Wait for the reaper to actually delete it (its own independent
    // asynchronous path) before looking for the stream record — rides the
    // always-on loop the same way the real-socket original's own
    // converged-or-timeout `GetItem` poll rode out the real (fast) sweep
    // interval, never a real `sleep`.
    let get_body =
        format!(r#"{{"ConsistentRead":true,"TableName":"{table}","Key":{{"id":{{"S":"s1"}}}}}}"#);
    poll_until_reaped(&mut cluster, non_leader, &get_body, seed);

    let reader = non_leader_of_table(&cluster, table);
    let (status, _ct, body) = cluster.console(
        reader,
        "GET",
        &format!("/console/api/tables/{table}/stream/shards"),
        "",
        &[],
    );
    assert_eq!(status, 200, "seed={seed}: stream/shards failed: {body}");
    assert_no_cluster_shape(&body);
    let shard_id = json(&body)["shards"][0]["shard_id"]
        .as_str()
        .unwrap_or_else(|| panic!("seed={seed}: no shard_id: {body}"))
        .to_string();

    let (status, _ct, body) = cluster.console(
        reader,
        "POST",
        &format!("/console/api/tables/{table}/stream/iterator"),
        "",
        format!(r#"{{"shard_id":"{shard_id}","iterator_type":"TRIM_HORIZON"}}"#).as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: stream/iterator failed: {body}");
    assert_no_cluster_shape(&body);
    let mut iterator = json(&body)["shard_iterator"]
        .as_str()
        .unwrap_or_else(|| panic!("seed={seed}: no shard_iterator: {body}"))
        .to_string();

    // Both the TTL delete and the poll above are already committed by this
    // point (the poll only returns once `s1` is gone) — this is a bounded
    // pagination walk, not a further convergence wait, mirroring
    // `sim_cluster_ttl.rs`'s own scenario (i).
    let mut ttl_record = None;
    for _ in 0..10 {
        if ttl_record.is_some() {
            break;
        }
        let (status, _ct, body) = cluster.console(
            reader,
            "POST",
            &format!("/console/api/tables/{table}/stream/records"),
            "",
            format!(r#"{{"shard_iterator":"{iterator}"}}"#).as_bytes(),
        );
        assert_eq!(status, 200, "seed={seed}: stream/records failed: {body}");
        assert_no_cluster_shape(&body);
        let v = json(&body);
        for record in v["records"].as_array().cloned().unwrap_or_default() {
            if record["eventName"] == "REMOVE" {
                ttl_record = Some(record);
            }
        }
        match v["next_shard_iterator"].as_str() {
            Some(next) => iterator = next.to_string(),
            None => break,
        }
    }
    let ttl_record = ttl_record.unwrap_or_else(|| {
        panic!(
            "seed={seed}: the TTL delete's REMOVE record never appeared through \
             the console"
        )
    });
    assert_eq!(
        ttl_record["userIdentity"]["PrincipalId"], "dynamodb.amazonaws.com",
        "seed={seed}: a TTL delete read through the console must carry the \
         service userIdentity: {ttl_record}"
    );
    assert_eq!(
        ttl_record["userIdentity"]["Type"], "Service",
        "seed={seed}: {ttl_record}"
    );
}

#[test]
fn ttl_deletion_carries_the_service_user_identity_through_the_console() {
    run_ttl_deletion_carries_the_service_user_identity_through_the_console(env_seed(0xC084_4001));
}

#[test]
fn ttl_deletion_carries_the_service_user_identity_through_the_console_over_seeds() {
    for i in 0..5 {
        run_ttl_deletion_carries_the_service_user_identity_through_the_console(0xC084_4100 + i);
    }
}
