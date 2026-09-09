//! Deterministic, seed-reproducible regression for `POST /admin/data/seed`'s
//! throughput on a Stream/GSI/LSI-carrying (images) table — see `lib.rs`'s
//! own `mod sim_cluster_seed_latency` doc comment for the one-paragraph
//! summary; this is the detailed account.
//!
//! **Rewritten for ADR 0021's 2026-09-09 amendment**: `POST /admin/data/seed`
//! is now a thin proxy over the real `BatchWriteItem` wire operation
//! (`admin::action_data_seed`/`admin::submit_seed_chunk`), chunked at
//! DynamoDB's own 25-item `BatchWriteItem` cap (`SEED_BATCH_WRITE_CAP`) with
//! up to 8 chunks in flight at once (`SEED_CONCURRENCY`) — replacing the
//! deleted per-item `SEED_IMAGES_CONCURRENCY`-pipelined arm this module used
//! to measure. The mechanism this test now proves is different in kind, not
//! just in constant: concurrency is **per-chunk**, not per-item — an
//! images-carrying table's own `BatchWriteItem` handler
//! (`dynamo.rs`'s `Operation::BatchWriteItem` arm) still evaluates each
//! chunk's items **sequentially** (one `cp_kind_write_item` at a time, no
//! pipelining within a chunk — see that arm's own doc), so the speedup this
//! route can produce is now bounded by how many *chunks* run concurrently,
//! not by how many *items* do.
//!
//! ## Why the baseline is still eight single-row calls
//!
//! A single-row (`count:1`) seed call is one chunk of length one — sequential
//! or concurrent chunk dispatch makes no difference when there's only one
//! item in flight — so eight separate single-row calls still give a true
//! per-row baseline regardless of how many chunks a larger call fans out to,
//! identical reasoning to before this rewrite.
//!
//! ## Why the scenario seeds exactly `SEED_CONCURRENCY x SEED_BATCH_WRITE_CAP` rows
//!
//! Seeding exactly 8 x 25 = **200** rows produces exactly 8 chunks of 25
//! items each — one full "wave" that exactly saturates `SEED_CONCURRENCY`
//! with no leftover serialized second wave. Under that shape, the expected
//! virtual elapsed time is dominated by **one chunk's own fully-sequential
//! 25-item cost** (every chunk runs concurrently with every other, but each
//! chunk's own 25 items are still paid one at a time) — i.e. elapsed ≈
//! `SEED_BATCH_WRITE_CAP` per-row-costs, against a sequential extrapolation
//! of `count` (200) per-row-costs. That predicts a ratio of `25 / 200 =
//! 12.5%`, comfortably under the same 25%-of-sequential bound the pre-rewrite
//! test used (which, for the old 32-way *item*-level pipelining over 96 rows
//! in 3 waves, had a far larger margin — ~3% of sequential, not ~12.5%). A
//! smaller row count would not leave that margin: 96 rows split into 4
//! chunks of 25/25/25/21 is still one wave (4 ≤ 8), so the expected ratio
//! there is `25 / 96 ≈ 26%` — **over** the 25% bound purely from the
//! chunking-boundary shape, before any real overhead is even counted (a
//! quick manual check confirmed a 96-row call does sit right at that
//! boundary and is not a reliable regression signal under this new
//! mechanism). 200 rows is the smallest row count that is both an exact
//! multiple of `SEED_BATCH_WRITE_CAP` *and* uses exactly (not fewer than)
//! `SEED_CONCURRENCY` chunks, which is what gives this bound real margin
//! rather than sitting on its own boundary.
//!
//! All timing is virtual `SimEnv` time (via [`SimCluster::admin_timed`],
//! which steps the simulator forward in small increments and reports back
//! *when* a request resolved, rather than [`SimCluster::admin`]'s fixed
//! `OP_BUDGET` jump) — this assertion cannot flake under real-thread
//! contention, sandbox load, or CI noise.
//!
//! **Confirmed red-before/green-after by hand**: temporarily reverting
//! `admin::submit_seed_chunk`'s chunk dispatch to a strictly sequential
//! `for chunk in chunks { submit_seed_chunk(..).await; }` (no
//! `buffer_unordered`) makes
//! [`pipelined_seed_is_well_under_the_sequential_extrapolation`] fail
//! deterministically (the 200-row seed then costs ~200x a single row's own
//! cost, not the ~8x concurrent chunking costs — both comfortably on their
//! own side of the 4x bound) — restored before landing.

use std::time::Duration;

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{
    create_table_via_wire, env_seed, json, leader_of_table, non_leader_of_table,
};

/// Step size for [`SimCluster::admin_timed`]'s incremental drive — fine
/// enough to resolve a single write's own virtual cost precisely without
/// needing an implausibly large `max_steps`.
const STEP: Duration = Duration::from_millis(20);
/// 4000 x 20ms = 80s of virtual time — comfortably above `OP_BUDGET` (12s,
/// [`SimCluster::admin`]'s own fixed jump). Wider than the pre-rewrite
/// budget (2000 steps): a 200-row seed's dominant cost is one chunk's own
/// 25 *sequential* item writes (see this module's own doc), materially more
/// virtual time than the old design's ~3 pipelined waves ever needed.
const MAX_STEPS: usize = 4000;

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

/// Rows for the pipelined scenario: exactly `SEED_CONCURRENCY (8) x
/// SEED_BATCH_WRITE_CAP (25)` — see this module's own doc for why this
/// exact count, not the pre-rewrite design's 96.
const PIPELINED_ROWS: u64 = 200;

fn run_pipelined_seed_is_well_under_the_sequential_extrapolation(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    // (a) Sequential per-row baseline: eight single-row seed calls into
    // their own table — see this module's own doc for why one row per
    // call is the only way to measure this without the chunk-level
    // concurrency itself distorting the measurement.
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

    // The scenario itself: PIPELINED_ROWS rows into a *different*, fresh
    // table, one `/admin/data/seed` call, so its stream carries exactly
    // this seed's own records and nothing from the baseline calls above.
    let created = create_streamed_table(&mut cluster, 0, "lat200");
    let stream_arn = created["TableDescription"]["LatestStreamArn"]
        .as_str()
        .unwrap_or_else(|| panic!("lat200 has no LatestStreamArn: {created}"))
        .to_owned();
    let node200 = non_leader_of_table(&cluster, "lat200");
    let elapsed200 = seed_timed(&mut cluster, node200, "lat200", PIPELINED_ROWS, 0, seed);

    let sequential_extrapolation = per_row * PIPELINED_ROWS as u32;
    let bound = sequential_extrapolation / 4;
    assert!(
        elapsed200 < bound,
        "seed={seed}: a {PIPELINED_ROWS}-row seed (8 concurrent 25-item chunks) took \
         {elapsed200:?} virtual time, which is not under 25% of the sequential extrapolation \
         ({sequential_extrapolation:?}, derived from a {per_row:?} per-row baseline over 8 \
         single-row calls) — chunk-level concurrency may have regressed to a strictly \
         sequential loop"
    );

    // (b) every one of the PIPELINED_ROWS rows is readable... **from a
    // linearizable read** (`ConsistentRead: true`) — `node200` does not
    // lead this tablet (ADR 0055's wire default, `false`, is served from
    // any replica's own applied state with no read barrier), so an
    // unqualified Scan immediately after a write can race replication and
    // undercount even though every write itself already confirmed durable
    // before `seed_timed` returned (found live: 1 of 5 `_over_seeds` seeds
    // reproduced a 199/200 undercount with this field omitted, the exact
    // "a read that verifies a write must ask for `ConsistentRead: true`"
    // gotcha `animusd/CLAUDE.md`'s ADR 0055 section documents — a test bug
    // in this file's own pre-rewrite Scan call too, carried forward
    // unnoticed until this rewrite's own wider seed sweep caught it).
    let (status, scan) = cluster.dynamo(
        node200,
        "DynamoDB_20120810.Scan",
        br#"{"TableName":"lat200","Select":"COUNT","ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: Scan lat200: {scan}");
    assert_eq!(
        json(&scan)["Count"],
        PIPELINED_ROWS,
        "seed={seed}: every seeded row is readable via Scan: {scan}"
    );

    // ...and the stream delivered EXACTLY PIPELINED_ROWS records — no loss,
    // no duplicate, across 8 concurrent `BatchWriteItem` chunks each
    // writing 25 distinct keys sequentially.
    let leader200 = leader_of_table(&cluster, "lat200");
    let iterator = open_tail_iterator(&mut cluster, leader200, &stream_arn);
    let record_count = drain_all_records(&mut cluster, leader200, iterator);
    assert_eq!(
        record_count, PIPELINED_ROWS as usize,
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
