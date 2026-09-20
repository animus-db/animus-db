//! Deterministic, seed-reproducible regression for `POST /admin/data/seed`'s
//! throughput on a Stream/GSI/LSI-carrying (images) table — see `lib.rs`'s
//! own `mod sim_cluster_seed_latency` doc comment for the one-paragraph
//! summary; this is the detailed account.
//!
//! **Rewritten a second time for issue #996 layer 2 (2026-09-20)**: the
//! images-carrying `Operation::BatchWriteItem` arm (`dynamo.rs`) no longer
//! evaluates a chunk's items **sequentially** — it groups them by tablet
//! (`crate::topology::tablet_for_key`, the identical grouping
//! `marker_batch_write_raw` already used for the marker-table fast arm) and
//! proposes each tablet's own group as ONE `KvCommand::KindEvalBatch` Raft
//! entry (`animus-cp-data`, issue #996 layer 1) via `ClientCtx::
//! cp_kind_write_batch`/`dynamo::kind_write_batch_at_leader`, instead of one
//! `KvCommand::KindEval` entry per item. This module's own cost model
//! (below) and its bound both change with it — the previous revision's own
//! "one Raft entry per chunk item, evaluated sequentially" model (kept in
//! git history) is now WRONG: a chunk's own 25 items now cost one Raft round
//! trip, not 25.
//!
//! **This is still layered on ADR 0021's 2026-09-09 amendment**: `POST
//! /admin/data/seed` is a thin proxy over the real `BatchWriteItem` wire
//! operation (`admin::action_data_seed`/`admin::submit_seed_chunk`),
//! chunked at DynamoDB's own 25-item `BatchWriteItem` cap
//! (`SEED_BATCH_WRITE_CAP`) with up to 8 chunks in flight at once
//! (`SEED_CONCURRENCY`) — that layering is unchanged by this rewrite, only
//! what happens *inside* one chunk's own commit is different now.
//!
//! ## Why the baseline is still eight single-row calls
//!
//! A single-row (`count:1`) seed call is one chunk of length one — a
//! `KindEvalBatch` entry of one item costs the identical one Raft round
//! trip a plain single-item `KindEval` entry always did (the batched arm's
//! per-item throttle precharge/post-charge loop and the one-entry propose
//! degenerate to the singular case at `N=1`), so eight separate single-row
//! calls still give a true per-row baseline, unchanged by this rewrite.
//!
//! ## The new cost model
//!
//! Seeding exactly `SEED_CONCURRENCY x SEED_BATCH_WRITE_CAP` = 8 x 25 =
//! **200** rows (`PIPELINED_ROWS`, unchanged from the prior revision — see
//! that revision's own reasoning for why this is the smallest row count
//! that is both an exact multiple of `SEED_BATCH_WRITE_CAP` *and* uses
//! exactly `SEED_CONCURRENCY` chunks with no leftover second wave) now
//! produces exactly 8 chunks, each **ONE** `KindEvalBatch` Raft entry
//! (25 items evaluated in-apply, in one commit — no per-item round trip
//! inside a chunk any more). All 8 chunks target the SAME (single,
//! unsplit) tablet — this fixture never enables auto-split, and a fresh
//! `CreateTable` always mints exactly one tablet — so there is no
//! cross-tablet fan-out to account for here: **if** a future revision of
//! this scenario split the table across `T` tablets, each tablet's own
//! subset of the 8 chunks would still commit one `KindEvalBatch` entry per
//! chunk per tablet (the grouping is per-`(chunk, tablet)`, not per
//! `chunk` alone), so the dominant cost would become one chunk-worth of
//! entries per tablet running concurrently across tablets, not a multiple
//! of `T` — the model below assumes `T = 1`, and a `T > 1` fixture would
//! need re-deriving from that shape, not from this one.
//!
//! With `T = 1`, the 200-row call's own expected virtual elapsed time is
//! now dominated by ONE Raft round trip (whichever of the 8 concurrent
//! chunks is slowest) — the SAME order of cost as the one-item `per_row`
//! baseline itself, not `SEED_BATCH_WRITE_CAP` (25) baseline-costs the way
//! the sequential-per-item model predicted. The predicted ratio is
//! therefore `elapsed200 / sequential_extrapolation ≈ per_row / (200 x
//! per_row) = 1 / 200 = 0.5%` — two orders of magnitude tighter than the
//! prior revision's own sequential-model prediction of `25 / 200 = 12.5%`.
//!
//! **Measured, both directions, by hand (this revision's own red/green
//! numbers — the `eprintln!` diagnostic used to produce them is not kept
//! in the landed test, per this file's own "no wall-clock, no incidental
//! debug output" discipline)**:
//! - **Green** (the real, batched arm, as committed): `ratio ≈ 0.50%-0.85%`
//!   across the pinned seed and all five `_over_seeds` seeds
//!   (`elapsed200` pinned at exactly one `STEP` — 20ms — the finest
//!   resolution [`SimCluster::admin_timed`] can report, i.e. the true cost
//!   is below one step and gets rounded up to it).
//! - **Red** (the images arm temporarily reverted to the OLD sequential
//!   `for req in reqs { ctx.cp_kind_write_item(..).await; }` loop, no
//!   batching): `ratio ≈ 8.00%-8.50%` across the same seeds — lower than
//!   the prior revision's own *analytical* 12.5% prediction (apply-side
//!   per-item evaluation inside one commit is not free either way; the
//!   measured sequential number already includes real per-item overhead
//!   the pure "25 rounds trips" model doesn't isolate) but still an order
//!   of magnitude above the batched arm's own measured ratio, and clearly
//!   distinguishable from it.
//!
//! [`SEQUENTIAL_EXTRAPOLATION_DIVISOR`] is set to **20** (a `5%` bound) —
//! comfortably above the batched arm's own measured ceiling (~0.85%, a
//! ~6x margin) and comfortably below the reverted-sequential-arm's own
//! measured floor (~8.00%), so this bound genuinely distinguishes the two
//! mechanisms rather than merely restating whichever one happens to be
//! live today. The prior revision's own `/4` (25%) bound would have passed
//! for BOTH the batched and the sequential arm alike (0.85% and 8.00% are
//! both under 25%) — it would not have caught a regression back to the
//! sequential shape, which is exactly why this rewrite tightens it rather
//! than only updating the doc's own prose.
//!
//! All timing is virtual `SimEnv` time (via [`SimCluster::admin_timed`],
//! which steps the simulator forward in small increments and reports back
//! *when* a request resolved, rather than [`SimCluster::admin`]'s fixed
//! `OP_BUDGET` jump) — this assertion cannot flake under real-thread
//! contention, sandbox load, or CI noise.

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

/// Issue #996 layer 2: the images-carrying `BatchWriteItem` arm now costs
/// ONE `KindEvalBatch` Raft entry per chunk per tablet instead of one
/// entry per item — see this module's own doc for the full derivation and
/// the measured red (~8.00%-8.50%) / green (~0.50%-0.85%) numbers this
/// divisor (a 5% bound) sits comfortably between, with margin on both
/// sides. The prior revision's `/4` (25%) bound would not have
/// distinguished the two mechanisms — both measured ratios pass a 25%
/// bound — which is why this rewrite tightens it rather than only
/// updating the surrounding prose.
const SEQUENTIAL_EXTRAPOLATION_DIVISOR: u32 = 20;

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
    let bound = sequential_extrapolation / SEQUENTIAL_EXTRAPOLATION_DIVISOR;
    assert!(
        elapsed200 < bound,
        "seed={seed}: a {PIPELINED_ROWS}-row seed (8 concurrent chunks, each ONE \
         KindEvalBatch Raft entry since issue #996 layer 2) took {elapsed200:?} virtual time, \
         which is not under {:.2}% of the sequential extrapolation ({sequential_extrapolation:?}, \
         derived from a {per_row:?} per-row baseline over 8 single-row calls) — chunk-level \
         concurrency, or the one-entry-per-tablet-per-chunk batching, may have regressed",
        100.0 / SEQUENTIAL_EXTRAPOLATION_DIVISOR as f64
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
