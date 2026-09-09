//! `SimCluster`-driven conversion of `tests/stream_backfill_seed_filter.rs`
//! (ADR 0061 rung J, C-10 PR 5): a table streamed **while** a GSI backfill
//! runs must never surface the backfill seeder's own synthetic, image-less
//! dirty marker (`index_drain::seed_change_log_record`, `seeded: true`) as a
//! phantom `GetRecords` event. Real DynamoDB emits **no** stream event at all
//! for a GSI backfill's own coverage sweep over pre-existing data — a seeded
//! record decodes fine (it is a legitimate, well-formed `ChangeRecord`) but
//! carries neither image and an empty `Keys`, so an unfiltered leak would
//! read as an invalid, fabricated event no real client would ever see. The
//! filter itself (`ChangeRecord::consumer_hidden`) is unchanged by this
//! conversion — both real-socket originals already passed; this PR only
//! gives each its own deterministic sibling, per the "both `GetRecords` serve
//! paths get their own test" discipline the original file's own doc explains
//! (issue #267): a shared predicate only proves the two serve paths *agree*,
//! not that each is actually reached with a seed record in hand.
//!
//! **Driving primitives, both from prior rungs — no new `SimCluster`
//! mechanism needed**: [`SimCluster::drive_backfill_seed`] (ADR 0061 rung J,
//! C-10 PR 2) ticks `index_drain::backfill_seed_tick` for every `Creating`
//! GSI of a led tablet — the source of the seed markers under test — and
//! [`SimCluster::drive_stream_seal`] (ADR 0061 rung G, C-07 PR 2) seals
//! whatever is currently pending, standing in for the real-socket
//! `tiny_seal_knobs` fixture's "seal almost immediately" behavior. Both
//! scenarios add the GSI via the real `UpdateTable` wire path
//! (`dispatch_table_op`'s index sub-arm, C-10 PR 2) and converge it to
//! `ACTIVE` the identical way `sim_cluster_index_ddl.rs`'s own
//! `converge_gsi_active` does (a bounded loop of `drive_backfill_seed` +
//! `drain_gsi` + `DescribeTable` polls — never a single call assumed to
//! finish everything).
//!
//! **Knob mapping, restated from `sim_cluster_dynamo_streams.rs`'s own
//! doc**: this fixture never spawns `index_drain::change_consumer_loop`'s
//! periodic seal arm, so a table's tablet simply stays open — with
//! everything, seed markers included, still pending in the hot tail — until
//! a scenario explicitly seals it. The original's `no_seal_knobs` (never
//! fires) is therefore simply "never call `drive_stream_seal`" (scenario
//! (a) below); its `tiny_seal_knobs` (seals on any pending byte, sweeping
//! seed markers into sealed segments alongside real records, deliberately —
//! "hiding is a serve-time decision", `docs/streams-notes.md`) is one
//! `drive_stream_seal` call after the backfill converges (scenario (b)).
//! Since this fixture has no concurrent real-time race between the seeder
//! and a periodic sealer, neither scenario needs the original's
//! converged-or-timeout polling loop: every write below is already
//! committed, deterministically, before the single drain/walk that checks
//! it.
//!
//! ## Scenarios (pinned-seed test + `_over_seeds` sibling at 5 seeds each)
//!
//! (a) [`backfill_seed_markers_never_surface_as_phantom_stream_events`] —
//!     the **open-tail** path: a streamed table gets five pre-existing rows,
//!     then a GSI added via `UpdateTable` while four more writes (two new
//!     partitions, a modify, a delete) race the backfill; the tablet's one
//!     shard is never sealed, so every record is served straight from the
//!     hot change log. Drains the open shard to convergence and asserts (a)
//!     zero delivered records have the phantom shape (empty `Keys`, no
//!     images) and (b) all 9 real writes — pre-existing and concurrent
//!     alike — are delivered exactly once, no more, no less.
//! (b) [`backfill_seed_markers_never_surface_from_sealed_shards_either`] —
//!     the **sealed** path's dual: identical scenario, but
//!     [`SimCluster::drive_stream_seal`] is called once after the backfill
//!     converges, sweeping the whole pending backlog — seed markers
//!     included — into a sealed segment. Walks the resulting lineage (a
//!     sealed epoch-0 shard plus its still-open, still-empty epoch-1 tail)
//!     and asserts the identical phantom-free, exactly-9-events property,
//!     now entirely off `get_records_sealed`'s segment decode.
//!
//! Replays (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib backfill_seed_markers_never_surface_as_phantom_stream_events`.

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

/// A single-key (`id`, string) table with a stream enabled
/// (`NEW_AND_OLD_IMAGES`) — mirrors the original real-socket test's own
/// `CreateTable` body.
fn create_streamed_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
            "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
            "StreamSpecification":{{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

fn put_item(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    id: &str,
    cat: &str,
) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"cat":{{"S":"{cat}"}}}}}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.PutItem", body.as_bytes())
}

fn delete_item(cluster: &mut SimCluster, node: u64, table: &str, id: &str) -> (u16, String) {
    let body = format!(r#"{{"TableName":"{table}","Key":{{"id":{{"S":"{id}"}}}}}}"#);
    cluster.dynamo(node, "DynamoDB_20120810.DeleteItem", body.as_bytes())
}

/// `UpdateTable` with a single `GlobalSecondaryIndexUpdates` `Create`
/// element — the real wire path that triggers a backfill on a populated
/// table, duplicated from `sim_cluster_index_ddl.rs`'s own inline body per
/// this crate's per-file-fixture convention.
fn create_index_via_wire(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    index: &str,
    hash_attr: &str,
) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "AttributeDefinitions":[{{"AttributeName":"{hash_attr}","AttributeType":"S"}}],
            "GlobalSecondaryIndexUpdates":[{{"Create":{{
                "IndexName":"{index}",
                "KeySchema":[{{"AttributeName":"{hash_attr}","KeyType":"HASH"}}],
                "Projection":{{"ProjectionType":"ALL"}}}}}}]}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.UpdateTable", body.as_bytes())
}

/// Find the tablet leader of `table`'s (sole) tablet — mirrors
/// `sim_cluster_index_ddl.rs::leader_of_table` exactly (duplicated locally
/// per this crate's own per-file-fixture convention).
fn leader_of_table(cluster: &SimCluster, table: &str) -> u64 {
    let meta = cluster.metadata(0);
    let (tablet, _) = meta
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("{table} has no tablet"));
    let tablet = *tablet;
    drop(meta);
    cluster
        .leader_index_of(tablet)
        .unwrap_or_else(|| panic!("{table} tablet has no leader"))
}

/// Pull `(IndexStatus, Backfilling)` for `index` out of a `DescribeTable`/
/// `UpdateTable` response body's `GlobalSecondaryIndexes` array — mirrors
/// `sim_cluster_index_ddl.rs::index_status` exactly (duplicated locally).
fn index_status(body: &str, index: &str) -> Option<(String, bool)> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let table = v.get("Table").or_else(|| v.get("TableDescription"))?;
    let gsis = table.get("GlobalSecondaryIndexes")?.as_array()?;
    let entry = gsis
        .iter()
        .find(|g| g.get("IndexName").and_then(|n| n.as_str()) == Some(index))?;
    let status = entry.get("IndexStatus")?.as_str()?.to_owned();
    let backfilling = entry
        .get("Backfilling")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    Some((status, backfilling))
}

/// Drive the backfill seeder + GSI drain to exhaustion, then poll
/// `DescribeTable` until `index` converges to `ACTIVE` — mirrors
/// `sim_cluster_index_ddl.rs::converge_gsi_active` exactly (duplicated
/// locally): never a single call assumed to finish a populated table's
/// backfill in one pass.
fn converge_gsi_active(cluster: &mut SimCluster, table: &str, index: &str) {
    let leader = leader_of_table(cluster, table);
    for _ in 0..10 {
        cluster.drive_backfill_seed(leader, table);
        cluster.drain_gsi(leader, table);
        let (_, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.DescribeTable",
            format!(r#"{{"TableName":"{table}"}}"#).as_bytes(),
        );
        if index_status(&body, index).map(|(s, _)| s) == Some("ACTIVE".to_owned()) {
            return;
        }
    }
    panic!("index `{index}` on `{table}` did not converge to ACTIVE within 10 rounds");
}

/// `DescribeStream`, issued from `node`.
fn describe_stream(cluster: &mut SimCluster, node: u64, stream_arn: &str) -> serde_json::Value {
    let body = format!(r#"{{"StreamArn":"{stream_arn}"}}"#);
    let (status, resp) = cluster.dynamo_streams(
        node,
        "DynamoDBStreams_20120810.DescribeStream",
        body.as_bytes(),
    );
    assert_eq!(status, 200, "DescribeStream failed: {resp}");
    json(&resp)
}

/// `GetShardIterator` with `TRIM_HORIZON` — the only iterator type either
/// scenario below needs.
fn get_shard_iterator(
    cluster: &mut SimCluster,
    node: u64,
    stream_arn: &str,
    shard_id: &str,
) -> String {
    let body = format!(
        r#"{{"StreamArn":"{stream_arn}","ShardId":"{shard_id}",
            "ShardIteratorType":"TRIM_HORIZON"}}"#
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

/// `GetRecords`, issued from `node`.
fn get_records(
    cluster: &mut SimCluster,
    node: u64,
    iterator: &str,
) -> (Vec<serde_json::Value>, Option<String>) {
    let body = format!(r#"{{"ShardIterator":"{iterator}"}}"#);
    let (status, resp) =
        cluster.dynamo_streams(node, "DynamoDBStreams_20120810.GetRecords", body.as_bytes());
    assert_eq!(status, 200, "GetRecords failed: {resp}");
    let v = json(&resp);
    let records = v["Records"].as_array().cloned().unwrap_or_default();
    let next = v["NextShardIterator"].as_str().map(str::to_owned);
    (records, next)
}

/// How many delivered records name partition-key value `id` in their own
/// `dynamodb.Keys` — a real write's `Keys` always carries the base table's
/// partition key, so this both counts real deliveries and (via the caller's
/// own separate phantom-shape assertion) is never satisfied by a seed
/// marker. Mirrors the original real-socket test's own `count_for_id`.
fn count_for_id(records: &[serde_json::Value], id: &str) -> usize {
    records
        .iter()
        .filter(|r| r["dynamodb"]["Keys"]["id"]["S"] == id)
        .count()
}

/// The phantom shape: an empty `Keys` (or, equivalently, neither image
/// present) must never appear in a delivered record — that is exactly and
/// only what an unfiltered backfill seed marker decodes to. Mirrors the
/// original's own `assert_no_phantom_shape`.
fn assert_no_phantom_shape(records: &[serde_json::Value]) {
    for r in records {
        let keys = r["dynamodb"]["Keys"]
            .as_object()
            .unwrap_or_else(|| panic!("record has no `Keys` object at all: {r}"));
        assert!(
            !keys.is_empty(),
            "phantom event with an empty `Keys` field surfaced: {r}"
        );
        let has_image =
            r["dynamodb"].get("OldImage").is_some() || r["dynamodb"].get("NewImage").is_some();
        assert!(
            has_image,
            "phantom event with no image at all surfaced: {r}"
        );
    }
}

/// Drain the table's one (never-sealed) open shard from `iterator` to
/// convergence — every write below is already committed, deterministically,
/// before this is first called, so a handful of stable-empty polls (rather
/// than the original real-socket test's own time-boxed "10 stable polls
/// over 30s", which raced a live concurrent writer this fixture has no
/// equivalent of) is enough to prove nothing further arrives.
fn drain_open_shard(cluster: &mut SimCluster, node: u64, iterator: &str) -> Vec<serde_json::Value> {
    let mut collected: Vec<serde_json::Value> = Vec::new();
    let mut cur = iterator.to_owned();
    let mut stable_polls = 0;
    for _ in 0..50 {
        let (records, next) = get_records(cluster, node, &cur);
        if let Some(next) = next {
            cur = next;
        }
        if records.is_empty() {
            stable_polls += 1;
        } else {
            stable_polls = 0;
            collected.extend(records);
        }
        if stable_polls >= 3 {
            return collected;
        }
    }
    panic!(
        "open shard never converged within 50 polls (collected so far: {})",
        collected.len()
    );
}

/// One full `TRIM_HORIZON` walk of the stream's current shard lineage, split
/// by sealed vs. still-open — mirrors the original real-socket test's own
/// `walk_lineage`.
fn walk_lineage(
    cluster: &mut SimCluster,
    node: u64,
    stream_arn: &str,
) -> (Vec<serde_json::Value>, Vec<serde_json::Value>) {
    let described = describe_stream(cluster, node, stream_arn);
    let shards = described["StreamDescription"]["Shards"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut sealed_events: Vec<serde_json::Value> = Vec::new();
    let mut open_events: Vec<serde_json::Value> = Vec::new();
    for shard in &shards {
        let shard_id = shard["ShardId"].as_str().expect("ShardId");
        let is_sealed = shard["SequenceNumberRange"]["EndingSequenceNumber"].is_string();
        let mut it = Some(get_shard_iterator(cluster, node, stream_arn, shard_id));
        while let Some(iterator) = it {
            let (records, next) = get_records(cluster, node, &iterator);
            let drained = records.is_empty();
            if is_sealed {
                sealed_events.extend(records);
            } else {
                open_events.extend(records);
            }
            it = if drained { None } else { next };
        }
    }
    (sealed_events, open_events)
}

/// Build `table`'s stream ARN from a `CreateTable` response's own
/// `TableDescription.LatestStreamLabel`.
fn stream_arn_of(table: &str, create_response: &serde_json::Value) -> String {
    let label = create_response["TableDescription"]["LatestStreamLabel"]
        .as_str()
        .unwrap_or_else(|| panic!("no LatestStreamLabel in: {create_response}"));
    format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{label}")
}

// ---------------------------------------------------------------------------
// Scenario (a): the open-tail path — never sealed.
// ---------------------------------------------------------------------------

fn run_backfill_seed_markers_never_surface_as_phantom_stream_events(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "orders";

    let (status, body) = create_streamed_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let stream_arn = stream_arn_of(table, &json(&body));

    // Five pre-existing partitions, written *before* the GSI (and its
    // backfill) ever exist — exactly what `backfill_seed_tick` sweeps.
    let pre_existing: Vec<String> = (0..5).map(|i| format!("p{i}")).collect();
    for id in &pre_existing {
        let (status, body) = put_item(&mut cluster, 0, table, id, &format!("cat-{id}"));
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
    }

    // Mint the iterator from the very start of the stream now, before the
    // backfill (and its seed markers) even begins.
    let described = describe_stream(&mut cluster, 0, &stream_arn);
    let shards = described["StreamDescription"]["Shards"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        shards.len(),
        1,
        "seed={seed}: expected exactly one (open) shard: {described}"
    );
    let shard_id = shards[0]["ShardId"].as_str().unwrap().to_owned();
    let iterator = get_shard_iterator(&mut cluster, 0, &stream_arn, &shard_id);

    // Add the GSI over the real `UpdateTable` wire path — this is what
    // starts the backfill seeder sweeping the five pre-existing partitions
    // above and (once `drive_backfill_seed` runs, below) seeding one
    // image-less marker per partition.
    let (status, body) = create_index_via_wire(&mut cluster, 0, table, "by-cat", "cat");
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTable(create index) failed: {body}"
    );

    // Genuine writes racing the backfill (mirrors `tests/backfill_seeder.rs`'s
    // "live writes during backfill" scenario): two brand-new partitions, a
    // modify of an existing one, and a delete of another. Every one of these
    // must be delivered — the fix must filter *only* seed markers, never a
    // real write.
    let (status, body) = put_item(&mut cluster, 0, table, "p5", "cat-p5");
    assert_eq!(status, 200, "seed={seed}: PutItem(p5) failed: {body}");
    let (status, body) = put_item(&mut cluster, 0, table, "p6", "cat-p6");
    assert_eq!(status, 200, "seed={seed}: PutItem(p6) failed: {body}");
    let (status, body) = put_item(&mut cluster, 0, table, "p0", "cat-p0-updated"); // MODIFY on p0
    assert_eq!(
        status, 200,
        "seed={seed}: PutItem(p0 update) failed: {body}"
    );
    let (status, body) = delete_item(&mut cluster, 0, table, "p1"); // REMOVE on p1
    assert_eq!(status, 200, "seed={seed}: DeleteItem(p1) failed: {body}");

    // Drive the backfill seeder + drain to exhaustion, converging the index
    // to ACTIVE.
    converge_gsi_active(&mut cluster, table, "by-cat");

    let delivered = drain_open_shard(&mut cluster, 0, &iterator);

    // (a) The phantom shape must never appear.
    assert_no_phantom_shape(&delivered);

    // (b) Every real write, and only real writes, delivered exactly once: 5
    // pre-existing inserts + 2 new inserts + 1 modify + 1 delete = 9. A stray
    // seed marker would inflate this past 9; a filter bug eating a real
    // write would deflate it below.
    assert_eq!(
        delivered.len(),
        9,
        "seed={seed}: expected exactly 9 real events, got {}: {delivered:#?}",
        delivered.len()
    );
    for id in &pre_existing {
        let want = if id == "p0" || id == "p1" { 2 } else { 1 };
        assert_eq!(
            count_for_id(&delivered, id),
            want,
            "seed={seed}: wrong delivery count for pre-existing partition {id}: {delivered:#?}"
        );
    }
    assert_eq!(
        count_for_id(&delivered, "p5"),
        1,
        "seed={seed}: {delivered:#?}"
    );
    assert_eq!(
        count_for_id(&delivered, "p6"),
        1,
        "seed={seed}: {delivered:#?}"
    );
}

#[test]
fn backfill_seed_markers_never_surface_as_phantom_stream_events() {
    run_backfill_seed_markers_never_surface_as_phantom_stream_events(env_seed(0x5EED_F117_0001));
}

#[test]
fn backfill_seed_markers_never_surface_as_phantom_stream_events_over_seeds() {
    for i in 0..5 {
        run_backfill_seed_markers_never_surface_as_phantom_stream_events(0x5EED_F117_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (b): the sealed-shard path's dual.
// ---------------------------------------------------------------------------

fn run_backfill_seed_markers_never_surface_from_sealed_shards_either(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "orders_sealed";

    let (status, body) = create_streamed_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let stream_arn = stream_arn_of(table, &json(&body));

    // Five pre-existing partitions, written *before* the GSI (and its
    // backfill) ever exist — exactly what `backfill_seed_tick` sweeps.
    let pre_existing: Vec<String> = (0..5).map(|i| format!("p{i}")).collect();
    for id in &pre_existing {
        let (status, body) = put_item(&mut cluster, 0, table, id, &format!("cat-{id}"));
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
    }

    // Add the GSI via the real `UpdateTable` wire path — starts the backfill
    // seeder seeding one image-less marker per pre-existing partition, once
    // `drive_backfill_seed` runs below.
    let (status, body) = create_index_via_wire(&mut cluster, 0, table, "by-cat", "cat");
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTable(create index) failed: {body}"
    );

    // Genuine writes racing the backfill sweep — every one must be
    // delivered; the filter must eat only seed markers.
    let (status, body) = put_item(&mut cluster, 0, table, "p5", "cat-p5");
    assert_eq!(status, 200, "seed={seed}: PutItem(p5) failed: {body}");
    let (status, body) = put_item(&mut cluster, 0, table, "p6", "cat-p6");
    assert_eq!(status, 200, "seed={seed}: PutItem(p6) failed: {body}");
    let (status, body) = put_item(&mut cluster, 0, table, "p0", "cat-p0-updated"); // MODIFY on p0
    assert_eq!(
        status, 200,
        "seed={seed}: PutItem(p0 update) failed: {body}"
    );
    let (status, body) = delete_item(&mut cluster, 0, table, "p1"); // REMOVE on p1
    assert_eq!(status, 200, "seed={seed}: DeleteItem(p1) failed: {body}");

    converge_gsi_active(&mut cluster, table, "by-cat");

    // Seal everything currently pending — real writes and seed markers
    // alike, deliberately ("hiding is a serve-time decision",
    // `docs/streams-notes.md`) — the sim stand-in for the original
    // real-socket test's own aggressive `tiny_seal_knobs`.
    let leader = leader_of_table(&cluster, table);
    cluster.drive_stream_seal(leader);

    let (sealed, open) = walk_lineage(&mut cluster, 0, &stream_arn);
    assert_no_phantom_shape(&sealed);
    assert_no_phantom_shape(&open);
    assert!(
        open.is_empty(),
        "seed={seed}: expected the whole backlog sealed with nothing left open: {open:#?}"
    );
    // Every real write, and only real writes, delivered exactly once — the
    // same accounting as the open-path scenario, now entirely off sealed
    // segments that physically contain the seed markers too.
    assert_eq!(
        sealed.len(),
        9,
        "seed={seed}: expected exactly 9 real events sealed, got {}: {sealed:#?}",
        sealed.len()
    );
    for id in &pre_existing {
        let want = if id == "p0" || id == "p1" { 2 } else { 1 };
        assert_eq!(
            count_for_id(&sealed, id),
            want,
            "seed={seed}: wrong delivery count for pre-existing partition {id}: {sealed:#?}"
        );
    }
    assert_eq!(count_for_id(&sealed, "p5"), 1, "seed={seed}: {sealed:#?}");
    assert_eq!(count_for_id(&sealed, "p6"), 1, "seed={seed}: {sealed:#?}");
}

#[test]
fn backfill_seed_markers_never_surface_from_sealed_shards_either() {
    run_backfill_seed_markers_never_surface_from_sealed_shards_either(env_seed(0x5EED_F117_0002));
}

#[test]
fn backfill_seed_markers_never_surface_from_sealed_shards_either_over_seeds() {
    for i in 0..5 {
        run_backfill_seed_markers_never_surface_from_sealed_shards_either(0x5EED_F117_2000 + i);
    }
}
