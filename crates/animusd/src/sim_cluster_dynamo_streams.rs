//! `SimCluster`-driven deterministic smoke for DynamoDB Streams enable/
//! disable + on-demand shard sealing (ADR 0061 rung G, C-07 PR 2) — the
//! first proof that a real DynamoDB-wire `UpdateTable` stream change
//! executes through the generic dispatch core, and that a shard can be
//! sealed and durably stored under `SimEnv`, with no `ProdEnv`/real socket
//! anywhere in the run.
//!
//! **The dispatch change (`dynamo.rs`)**: `dispatch_table_op`'s
//! `UpdateTable` arm gained a stream-only sub-arm — `stream: Some(..)`,
//! `throughput_update: None` (an index change is still rejected, as before)
//! — calling the now-generic [`crate::dynamo::enable_stream`]/
//! [`crate::dynamo::disable_stream`] the exact shape `update_table`'s own
//! concrete arm already used. `disable_stream` itself widened from
//! `&ClientCtx` to `<E: Env, R: RelayClient>` (its `tokio::time` sites
//! becoming `ctx.env.now()`/`ctx.env.sleep(..)`) — a pure signature
//! widening, zero new mechanism, mirroring `enable_stream`/
//! `update_table_throughput`'s own precedent (D3 PR 2b). `CreateTable`'s
//! own arm also stopped rejecting a declared stream — `create_table`'s
//! stream branch already called the (already-generic) `enable_stream`, so
//! there was nothing left to reject. `update_table`/`run_operation` are
//! byte-identical — the D2 PR 1 lesson applied here too: this rung's
//! generic split never becomes the production dispatcher's only path.
//!
//! **The store change (`sim_cluster.rs`)**: `SimCluster::new` now builds a
//! SECOND shared `animus_sim::SimSegmentStore` (independent from the
//! backup store D4 PR 5 already wired) and wraps every node's
//! `ClientCtx::segment_store` in `SegmentStoreHandle::S3` around a clone of
//! it, in place of the inert `Fs` placeholder every other `sim_cluster_*`
//! module still carries for `backup_store` alone. `SimCluster::restart`
//! leaves it untouched (never reassigned there), the identical `backup_
//! store` precedent. [`SimCluster::segment_store`] hands a test a handle
//! for direct assertions.
//!
//! **The seal primitive (`sim_cluster.rs`)**: `SimCluster::
//! drive_stream_seal(node)` calls the trigger-free `index_drain::seal_now`
//! directly, unconditionally, once per led+streamed tablet `node` hosts,
//! looped to exhaustion — a test-only stand-in for `index_drain::
//! change_consumer_loop`'s periodic stream-seal arm, which this fixture
//! never spawns (mirroring [`SimCluster::drain_gsi`]'s own on-demand-call
//! shape for the identical reason). See that method's own doc for exactly
//! what it does and does not replicate (no `is_quiesced()`/`Building`-child
//! guard — both are structurally unreachable under this fixture).
//!
//! # Scenario
//!
//! [`create_enable_write_seal_disable`] — create a table over the wire,
//! `UpdateTable` to enable a stream through [`SimCluster::dynamo`] (proving
//! the new sub-arm), write a few items from a **non-leader** node, call
//! [`SimCluster::drive_stream_seal`] on the tablet's leader, and assert via
//! `Metadata::stream_shards` (through [`SimCluster::metadata`]) that a
//! sealed shard row exists and that [`SimCluster::segment_store`]'s
//! `stored_ids()` holds its segment object — then disable the stream
//! through the same `UpdateTable` sub-arm and assert the catalog reflects
//! it (`table_stream` gone). Every read that verifies a write asks for
//! `ConsistentRead: true` (ADR 0055 — the wire default gives no read-your-
//! writes guarantee, and a race here would prove nothing).
//!
//! Replays at a pinned seed plus a `_over_seeds` sibling at five seeds, per
//! the sibling modules' own convention: `ANIMUS_SEED=<seed> cargo test -p
//! animusd --lib create_enable_write_seal_disable`.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// One DynamoDB wire `CreateTable` for a plain single-key (`pk`, string)
/// table named `table`, issued from `node` — mirrors every other
/// `sim_cluster_*` module's identically-named helper.
fn create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

/// `UpdateTable` enabling a stream, issued from `node` — the shape the new
/// `dispatch_table_op` stream-only sub-arm accepts.
fn enable_stream_via_wire(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}","StreamSpecification":
            {{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.UpdateTable", body.as_bytes())
}

/// `UpdateTable` disabling a stream, issued from `node` — the same
/// sub-arm's `Disable` branch.
fn disable_stream_via_wire(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body =
        format!(r#"{{"TableName":"{table}","StreamSpecification":{{"StreamEnabled":false}}}}"#);
    cluster.dynamo(node, "DynamoDB_20120810.UpdateTable", body.as_bytes())
}

fn put_item(cluster: &mut SimCluster, node: u64, table: &str, pk: &str, v: &str) -> (u16, String) {
    let body =
        format!(r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"{pk}"}},"v":{{"S":"{v}"}}}}}}"#);
    cluster.dynamo(node, "DynamoDB_20120810.PutItem", body.as_bytes())
}

fn get_item(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    pk: &str,
    consistent: bool,
) -> (u16, String) {
    let body = format!(
        r#"{{"ConsistentRead":{consistent},"TableName":"{table}","Key":{{"pk":{{"S":"{pk}"}}}}}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.GetItem", body.as_bytes())
}

fn run_create_enable_write_seal_disable(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "orders";

    let (status, body) = create_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let (status, body) = enable_stream_via_wire(&mut cluster, 0, table);
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTable(enable stream) failed: {body}"
    );
    assert!(
        body.contains("\"StreamEnabled\":true"),
        "seed={seed}: enable response missing StreamEnabled: {body}"
    );
    let label = cluster
        .metadata(0)
        .table_stream(table)
        .map(|s| s.label.clone())
        .unwrap_or_else(|| panic!("seed={seed}: no stream label committed after enable"));

    // Every node's own view must agree the stream is enabled with the same
    // label — a plain catalog-convergence check, no wire round trip needed.
    for n in 0..cluster.node_count() as u64 {
        let observed = cluster
            .metadata(n)
            .table_stream(table)
            .map(|s| s.label.clone());
        assert_eq!(
            observed,
            Some(label.clone()),
            "seed={seed}: node {n} disagrees on the enabled stream's label"
        );
    }

    // The tablet's own leader, resolved from the replicated catalog (this
    // table was created over the wire, never hand-hosted — `tablet_of`/
    // `leader_index_of`'s own hand-hosted-only bookkeeping doesn't cover
    // it, mirroring every other `sim_cluster_dynamo_*` sibling's identical
    // "look it up via `Metadata`" idiom).
    let tablet = *cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("seed={seed}: table {table} has no tablet"))
        .0;
    let leader = (0..cluster.node_count() as u64)
        .find(|&n| cluster.is_leader_local(n, tablet))
        .unwrap_or_else(|| panic!("seed={seed}: tablet {} has no leader", tablet.0));

    // Write a few items from a NON-leader node — proving the writes reach
    // the tablet's real leader (forwarded, if the writer isn't it) before
    // this test ever seals anything.
    let writer = (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a 3-node cluster always has a non-leader node");
    for (pk, v) in [("a", "1"), ("b", "2"), ("c", "3")] {
        let (status, body) = put_item(&mut cluster, writer, table, pk, v);
        assert_eq!(
            status, 200,
            "seed={seed}: PutItem({pk}) from non-leader node {writer} failed: {body}"
        );
    }
    // Read one back with `ConsistentRead: true` (ADR 0055) before trusting
    // the writes landed — a read that verifies a write must ask for the
    // strong path, or this assertion races the eventually-consistent
    // default and proves nothing.
    let (status, body) = get_item(&mut cluster, leader, table, "a", true);
    assert_eq!(status, 200, "seed={seed}: GetItem(a) failed: {body}");
    assert!(
        body.contains(r#""v":{"S":"1"}"#),
        "seed={seed}: item \"a\" missing/wrong after write: {body}"
    );

    // Seal on the leader — `drive_stream_seal` loops `index_drain::
    // seal_now` to exhaustion for every led, streamed tablet.
    cluster.drive_stream_seal(leader);

    let meta = cluster.metadata(leader);
    let sealed: Vec<_> = meta
        .stream_shards
        .iter()
        .filter(|((t, _epoch), _row)| *t == tablet)
        .collect();
    assert!(
        !sealed.is_empty(),
        "seed={seed}: drive_stream_seal(leader={leader}) produced no stream_shards row \
         for tablet {}",
        tablet.0
    );
    assert_eq!(
        sealed.len(),
        1,
        "seed={seed}: expected exactly one sealed epoch for a single seal pass: {sealed:?}"
    );
    let (_, row) = sealed[0];
    assert_eq!(
        row.count, 3,
        "seed={seed}: sealed shard row's own record count doesn't match the 3 writes: {row:?}"
    );

    // The segment object the seal wrote is durably present in the shared
    // `SimSegmentStore` every node's own `segment_store` wraps a clone of.
    let store = cluster.segment_store();
    assert!(
        !store.stored_ids().is_empty(),
        "seed={seed}: segment_store().stored_ids() is empty after a real seal"
    );

    // Disable through the same generic sub-arm, issued from a DIFFERENT
    // node than the one that enabled it — the relay-allowlist shape
    // `dynamo_streams.rs`'s own real-socket regression already covers, now
    // proven reachable under `SimEnv`.
    let disabler = (leader + 1) % cluster.node_count() as u64;
    let (status, body) = disable_stream_via_wire(&mut cluster, disabler, table);
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTable(disable stream) from node {disabler} failed: {body}"
    );
    assert!(
        !body.contains("StreamSpecification"),
        "seed={seed}: disable response still names a StreamSpecification: {body}"
    );
    for n in 0..cluster.node_count() as u64 {
        assert!(
            cluster.metadata(n).table_stream(table).is_none(),
            "seed={seed}: node {n} still reports an enabled stream after disable"
        );
    }
}

/// `ANIMUS_SEED=<seed> cargo test -p animusd --lib
/// create_enable_write_seal_disable` replays this scenario at a specific
/// seed (repo convention).
#[test]
fn create_enable_write_seal_disable() {
    run_create_enable_write_seal_disable(env_seed(0xC07E_0001));
}

#[test]
fn create_enable_write_seal_disable_over_seeds() {
    for i in 0..5 {
        run_create_enable_write_seal_disable(0xC07E_1000 + i);
    }
}
