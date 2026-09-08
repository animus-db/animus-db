//! Deterministic, seed-reproducible regression for the admin seeder's
//! images-arm pipelining (`admin::action_data_seed`'s `SEED_IMAGES_
//! CONCURRENCY`, `docs/engineering-lessons.md`'s matching 2026-09-08
//! entry) — see `lib.rs`'s own `mod sim_cluster_seed_latency` doc comment
//! for the one-paragraph summary; this is the detailed account.
//!
//! ## Why the baseline is eight single-row calls, not one eight-row call
//!
//! The admin seeder's images-carrying-table arm (any table with a
//! Stream/GSI/LSI/PITR) pipelines up to `SEED_IMAGES_CONCURRENCY` (32)
//! `cp_kind_write_item` calls at once. A red-before/green-after
//! sequential-vs-concurrent comparison needs a **sequential** baseline
//! that the fixed (concurrent) code cannot itself distort — but seeding
//! N > 1 rows in one `/admin/data/seed` call always dispatches up to
//! `SEED_IMAGES_CONCURRENCY` of them at once under the current code, so
//! measuring (say) an eight-row seed directly would measure the
//! *pipelined* cost of eight rows (all in one wave, since 8 < 32), not
//! the per-row cost a strictly sequential loop would pay. Seeding exactly
//! **one** row per call sidesteps this: concurrency is irrelevant when
//! there is only one item to write, so eight separate single-row
//! (`count:1`) seed calls give a true per-row baseline regardless of
//! which arm — sequential or pipelined — is actually live in the binary
//! under test.
//!
//! The 96-row scenario then seeds all 96 rows in **one** call (three
//! waves of up to 32 at `SEED_IMAGES_CONCURRENCY = 32`) and asserts its
//! own virtual elapsed time is under 25% of the naive `96 x baseline`
//! sequential extrapolation — comfortably above what three concurrent
//! waves cost (plus per-call overhead) while being far below what 96
//! fully sequential round trips would cost, so the bound cannot flake on
//! ordinary scheduling noise in either direction.
//!
//! All timing is virtual `SimEnv` time (via [`SimCluster::admin_timed`],
//! which steps the simulator forward in small increments and reports back
//! *when* a request resolved, rather than [`SimCluster::admin`]'s fixed
//! `OP_BUDGET` jump) — this assertion cannot flake under real-thread
//! contention, sandbox load, or CI noise.
//!
//! **Confirmed red-before/green-after by hand**: temporarily reverting the
//! images arm (`admin.rs::action_data_seed`) to a strictly sequential `for
//! (pk, sk, item) in &rows { ctx.cp_kind_write_item(..).await?; }` loop
//! makes [`pipelined_seed_is_well_under_the_sequential_extrapolation`]
//! fail deterministically (the 96-row seed then costs ~96x a single row's
//! own cost, not the ~3x the pipelined code costs — both comfortably on
//! their own side of the 24x bound) — restored before landing.

use std::time::Duration;

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{
    create_table_via_wire, env_seed, json, leader_of_table, non_leader_of_table,
};

/// Step size for [`SimCluster::admin_timed`]'s incremental drive — fine
/// enough to resolve a single write's own virtual cost precisely without
/// needing an implausibly large `max_steps`.
const STEP: Duration = Duration::from_millis(20);
/// 2000 x 20ms = 40s of virtual time — comfortably above `OP_BUDGET`
/// (12s, [`SimCluster::admin`]'s own fixed jump), since a 96-row pipelined
/// seed plus its eight single-row baseline calls all share one cluster's
/// worth of Raft/network activity.
const MAX_STEPS: usize = 2000;

/// A Stream-enabled (`NEW_AND_OLD_IMAGES`) table with a single string
/// partition key `pk` — the images-carrying shape `action_data_seed`'s
/// evaluate-at-leader arm requires. Returns the `CreateTable` response
/// body (parsed), so the caller can read `LatestStreamArn` off it.
fn create_streamed_table(cluster: &mut SimCluster, node: u64, table: &str) -> serde_json::Value {
    let body = format!(
        r#"{{"TableName":"{table}","KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}],
            "StreamSpecification":{{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
    );
    let (status, resp) = create_table_via_wire(cluster, node, &body);
    assert_eq!(status, 200, "CreateTable {table} failed: {resp}");
    json(&resp)
}

/// One `POST /admin/data/seed {table, count, start}` call, timed via
/// [`SimCluster::admin_timed`] — panics on a non-200 or a short write
/// (every call in this module is expected to write every requested row).
fn seed_timed(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    count: u64,
    start: u64,
    seed: u64,
) -> Duration {
    let body = format!(r#"{{"table":"{table}","count":{count},"start":{start}}}"#);
    let (elapsed, status, resp) = cluster.admin_timed(
        node,
        "POST",
        "/admin/data/seed",
        "",
        body.as_bytes(),
        STEP,
        MAX_STEPS,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: seed {table} start={start} count={count}: {resp}"
    );
    assert_eq!(
        json(&resp)["written"],
        count,
        "seed={seed}: seed {table} start={start} count={count} wrote every requested row: {resp}"
    );
    elapsed
}

/// `GetShardIterator(TRIM_HORIZON)` for `table`'s own (sole, open) shard —
/// panics on a non-200 or a missing shard/iterator, mirroring
/// `sim_cluster_dynamo_streams.rs`'s own real-socket-derived helpers.
fn open_tail_iterator(cluster: &mut SimCluster, node: u64, stream_arn: &str) -> String {
    let body = format!(r#"{{"StreamArn":"{stream_arn}"}}"#);
    let (status, resp) = cluster.dynamo_streams(
        node,
        "DynamoDBStreams_20120810.DescribeStream",
        body.as_bytes(),
    );
    assert_eq!(status, 200, "DescribeStream failed: {resp}");
    let described = json(&resp);
    let shard_id = described["StreamDescription"]["Shards"][0]["ShardId"]
        .as_str()
        .unwrap_or_else(|| panic!("no open shard in: {described}"))
        .to_owned();

    let body = format!(
        r#"{{"StreamArn":"{stream_arn}","ShardId":"{shard_id}","ShardIteratorType":"TRIM_HORIZON"}}"#
    );
    let (status, resp) = cluster.dynamo_streams(
        node,
        "DynamoDBStreams_20120810.GetShardIterator",
        body.as_bytes(),
    );
    assert_eq!(status, 200, "GetShardIterator failed: {resp}");
    json(&resp)["ShardIterator"]
        .as_str()
        .unwrap_or_else(|| panic!("no ShardIterator in: {resp}"))
        .to_owned()
}

/// Walk every record off `iterator` via `GetRecords`, following
/// `NextShardIterator` until it stops advancing (an open tail keeps
/// re-issuing the same token once drained) or a page comes back empty.
/// Returns the total record count.
fn drain_all_records(cluster: &mut SimCluster, node: u64, mut iterator: String) -> usize {
    let mut total = 0usize;
    loop {
        let body = format!(r#"{{"ShardIterator":"{iterator}"}}"#);
        let (status, resp) =
            cluster.dynamo_streams(node, "DynamoDBStreams_20120810.GetRecords", body.as_bytes());
        assert_eq!(status, 200, "GetRecords failed: {resp}");
        let v = json(&resp);
        let n = v["Records"].as_array().map(Vec::len).unwrap_or(0);
        total += n;
        match v["NextShardIterator"].as_str() {
            Some(next) if next != iterator && n > 0 => iterator = next.to_owned(),
            _ => break,
        }
    }
    total
}

fn run_pipelined_seed_is_well_under_the_sequential_extrapolation(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    // (a) Sequential per-row baseline: eight single-row seed calls into
    // their own table — see this module's own doc for why one row per
    // call is the only way to measure this without the pipelined code
    // itself distorting the measurement.
    create_streamed_table(&mut cluster, 0, "lat_baseline");
    let baseline_node = non_leader_of_table(&cluster, "lat_baseline");
    let mut baseline_total = Duration::ZERO;
    for i in 0..8u64 {
        baseline_total += seed_timed(&mut cluster, baseline_node, "lat_baseline", 1, i, seed);
    }
    let per_row = baseline_total / 8;
    assert!(
        per_row > Duration::ZERO,
        "seed={seed}: a single-row seed call takes a measurable amount of virtual time \
         (baseline_total={baseline_total:?})"
    );

    // The scenario itself: 96 rows into a *different*, fresh table, one
    // `/admin/data/seed` call, so its stream carries exactly this seed's
    // own 96 records and nothing from the baseline calls above.
    let created = create_streamed_table(&mut cluster, 0, "lat96");
    let stream_arn = created["TableDescription"]["LatestStreamArn"]
        .as_str()
        .unwrap_or_else(|| panic!("lat96 has no LatestStreamArn: {created}"))
        .to_owned();
    let node96 = non_leader_of_table(&cluster, "lat96");
    let elapsed96 = seed_timed(&mut cluster, node96, "lat96", 96, 0, seed);

    let sequential_extrapolation = per_row * 96;
    let bound = sequential_extrapolation / 4;
    assert!(
        elapsed96 < bound,
        "seed={seed}: a pipelined 96-row seed took {elapsed96:?} virtual time, which is not \
         under 25% of the sequential extrapolation ({sequential_extrapolation:?}, derived from \
         a {per_row:?} per-row baseline over 8 single-row calls) — the images arm may have \
         regressed to a strictly sequential loop"
    );

    // (b) every one of the 96 rows is readable...
    let (status, scan) = cluster.dynamo(
        node96,
        "DynamoDB_20120810.Scan",
        br#"{"TableName":"lat96","Select":"COUNT"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: Scan lat96: {scan}");
    assert_eq!(
        json(&scan)["Count"],
        96,
        "seed={seed}: every seeded row is readable via Scan: {scan}"
    );

    // ...and the stream delivered EXACTLY 96 records — no loss, no
    // duplicate, under 32-way concurrent writes to 96 distinct keys.
    let leader96 = leader_of_table(&cluster, "lat96");
    let iterator = open_tail_iterator(&mut cluster, leader96, &stream_arn);
    let record_count = drain_all_records(&mut cluster, leader96, iterator);
    assert_eq!(
        record_count, 96,
        "seed={seed}: the stream delivers exactly one record per seeded row, exactly once, \
         under concurrency"
    );
}

#[test]
fn pipelined_seed_is_well_under_the_sequential_extrapolation() {
    run_pipelined_seed_is_well_under_the_sequential_extrapolation(env_seed(0x5EED_0001));
}

#[test]
fn pipelined_seed_is_well_under_the_sequential_extrapolation_over_seeds() {
    for i in 0..5 {
        run_pipelined_seed_is_well_under_the_sequential_extrapolation(0x5EED_0100 + i);
    }
}
