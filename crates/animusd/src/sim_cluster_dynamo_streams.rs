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
//!
//! # ADR 0061 rung G, C-07 PR 3: the Streams read API reachable from `SimCluster`
//!
//! PR 2 (above) only proved the write side (enable/disable/on-demand seal)
//! runs under `SimEnv`. This PR widens the actual **read** API — the four
//! `DynamoDBStreams_20120810.*` operations — the same way, per this crate's
//! own C-06 PR 5 "pure move" precedent (not PR 5's own copy-the-body shape,
//! which existed only to route around `execute_statement`'s pre-existing
//! mutual recursion into `run_operation` — `dynamo_streams.rs` has no such
//! recursion): `dynamo_streams::execute_as`, `run_operation`,
//! `list_streams`, `describe_stream`, `get_shard_iterator`, `get_records`,
//! `get_records_sealed`, and `get_records_open` all widened from `&ClientCtx`
//! to `<E: Env, R: RelayClient>`. Every one of the eight was already calling
//! only generic callees (`ctx.effective_metadata()`, `authz::authorize*`,
//! `ClientCtx::read_stream_hot_records`, `ctx.segment_store` — a plain,
//! non-`E`-typed `SegmentStoreHandle` enum) — so this was a genuine pure
//! move, not a rewrite: `dynamo_streams::execute_as` is now a **thin
//! production wrapper** over a new generic core,
//! [`crate::dynamo_streams::execute_streams_op_as`], monomorphized at
//! `E = ProdEnv, R = AnimusdRelayClient`. Production behavior (`dynamo::
//! dispatch`'s target-prefix fork, `dynamo_streams::execute_as` itself) is
//! byte-identical to before this PR. No handler in this module turned out to
//! be genuinely `ProdEnv`-bound (no direct socket dial, no `tokio::spawn`),
//! so `execute_streams_op_as` covers all four Streams operations with no
//! `unsupported_by_generic_dispatch` gap the way `dispatch_item_op` still
//! has for a few item-API operations.
//!
//! [`SimClusterHandle::dynamo_streams`]/[`SimCluster::dynamo_streams`]
//! mirror [`SimClusterHandle::dynamo`]/[`SimCluster::dynamo`] exactly, one
//! level over in the Streams service: decode + run a
//! `DynamoDBStreams_20120810.<Op>` target/body against one node's own
//! `ClientCtx`, through `execute_streams_op_as` — an unrestricted
//! `Principal` (this fixture has no SigV4 listener to gate through), the
//! same shape `dynamo_streams::execute_as`'s own two production callers
//! (`dynamo::dispatch`/`admin.rs::action_data_dynamo`'s
//! `execute_routed_as`) already resolve through the identical target-prefix
//! fork — but this fixture calls `execute_streams_op_as` directly rather
//! than through that fork, since a caller here already knows the target is
//! Streams-shaped (mirroring `SimClusterHandle::dynamo` calling
//! `execute_item_op_as` directly rather than through `execute_routed_as`).
//!
//! ## Scenarios (seed-parameterized, `_over_seeds` at 5 seeds each)
//!
//! Every scenario below is issued from a **non-leader** node of a 3-node
//! RF3 `SimCluster` wherever the operation itself has a leader to forward
//! to (every read/write against the streamed table's own tablet) — the
//! forwarding path this fixture's own `SimRelayClient` wire exists to
//! prove, mirroring `sim_cluster_dynamo.rs`/`sim_cluster_dynamo_
//! partiql.rs`'s identical precedent.
//!
//! (a) [`list_streams_and_describe_stream_after_enable`] — `ListStreams`
//!     names the freshly enabled table/label; `DescribeStream` shows the
//!     one still-open epoch-0 shard.
//! (b) [`get_records_over_the_open_tail_before_any_seal`] — `GetShardIterator
//!     (TRIM_HORIZON)` + `GetRecords` over the OPEN tail, with no
//!     `stream_shards` catalog row for this tablet at all — proves
//!     `get_records_open` → `ClientCtx::read_stream_hot_records` →
//!     `index_drain::hot_read` (forwarded to the tablet's own leader, ADR
//!     0042 §7/§8, no `ReadIndex` barrier).
//! (c) [`get_records_over_the_sealed_shard_from_the_shared_store`] —
//!     [`SimCluster::drive_stream_seal`], then `GetRecords` over the
//!     resulting SEALED shard — proves `get_records_sealed` genuinely reads
//!     `SegmentStoreHandle::S3` (`SimCluster::segment_store()`'s own shared
//!     `SimSegmentStore`), not the open path.
//! (d) [`iterator_obtained_before_a_seal_continues_correctly_across_the_seal`]
//!     — the sealed-vs-open handoff (ADR 0042 §2's "sealing never
//!     invalidates an open-shard iterator"): mint an iterator against the
//!     still-empty open shard, write + seal underneath it with no re-mint,
//!     and prove the SAME token now resolves through the sealed path with
//!     the correct record.
//! (e) [`next_shard_iterator_pagination_with_small_limit_visits_each_record_once`]
//!     — a small `Limit` walks a 5-record sealed shard page by page with no
//!     gaps or duplicates.
//! (f) [`iterator_types_latest_at_and_after_sequence_number`] — `LATEST` on
//!     a genuinely open shard (one hot read finds the current max; an empty
//!     poll returns the identical token; a later write becomes visible
//!     through it), then `AT_SEQUENCE_NUMBER`/`AFTER_SEQUENCE_NUMBER` on the
//!     sealed shard once it's sealed (inclusive vs. exclusive of the named
//!     sequence number).
//! (g) [`cross_node_reads_answer_the_same_records_for_the_same_iterator`] —
//!     the identical iterator token, replayed through every node of the
//!     cluster in turn, answers byte-identical `(records, NextShardIterator)`
//!     every time (a sealed shard is served by ANY node, ADR 0043 §A3).
//! (h) [`disable_then_grace_window_describe_and_get_records`] — F12-b's
//!     disable grace window (ADR 0042 §11, `docs/streams-notes.md`):
//!     `ListStreams` still names the `DISABLED` label, `DescribeStream`
//!     reports it with no open shard, its already-sealed reads keep
//!     working, and a label that never existed at all is
//!     `ResourceNotFoundException`.
//!
//! **The ninth item this PR's own brief named — a bare (non-`Forwarded`)
//! `ClientRequest::StreamHotRead` refusal — is deliberately SKIPPED, not
//! built.** `animus_node::sim_relay::SimRelayClient`'s own inbound dispatch
//! (`forwarding::handle_relayed_request`, `forwarding.rs`) is NOT the same
//! mechanism the real bare-refusal regression
//! (`tests/dynamo_streams.rs::bare_stream_hot_read_is_refused`) proves: that
//! test exercises `handle_request`'s `Surface::Intra` port guard and
//! `cp_serve_forwarded`'s own "must be sent wrapped in `Forwarded`" match
//! arm — neither of which exists on this fixture's relay path at all (no
//! `handle_request`, no ports, no `Surface` classification under `SimEnv`).
//! `handle_relayed_request`'s own match is a **three-arm allowlist**
//! (`Status`/`Forwarded`/`ProposeSchema`) with a blanket `_ =>
//! ClientResponse::Error("not relayable under sim".into())` catch-all — so
//! sending a bare `ClientRequest::StreamHotRead` through it would indeed be
//! refused, but by that generic catch-all, identically for literally any
//! other internal-only variant, never reaching (or proving anything about)
//! `cp_serve_forwarded`'s own StreamHotRead-specific bare-refusal arm the
//! real regression is actually about. Building a scenario around this would
//! prove `handle_relayed_request`'s own three-arm allowlist, not the thing
//! this PR's brief asked for — a materially different (and already
//! implicitly covered — every scenario above only ever reaches Streams
//! handling via a real `Forwarded` wrap) claim. `sim_cluster.rs` also
//! exposes no raw-relay-send primitive to any sibling module today (`fn
//! ctx(&self, node: u64) -> SimNodeCtx` — the only way to reach a node's own
//! `ClientCtx::relay` field — is private to that file), so building this
//! scenario would additionally need a new, narrowly-scoped `SimCluster`
//! accessor whose only purpose would be to prove a refusal this fixture's
//! own relay dispatcher already can't help but produce for ANY bare
//! internal-only send. Skipped for both reasons; the real mechanism stays
//! covered by the unmodified real-socket regression this PR's own gate run
//! re-verifies.
//!
//! Replays (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib list_streams_and_describe_stream_after_enable`.

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

/// `table`'s own tablet id, resolved from the replicated catalog
/// (`Metadata::tablets_for_table`) — every table in this module is created
/// over the real wire, never via `SimCluster::create_table` itself, so
/// `SimCluster::tablet_of`'s own hand-hosted-only bookkeeping never covers
/// it (the same lookup `sim_cluster_dynamo_transact.rs`/`sim_cluster_
/// dynamo_partiql.rs` already use for a wire-created table).
fn tablet_of_table(cluster: &SimCluster, table: &str) -> animus_tablet::TabletId {
    *cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table `{table}` has no tablet"))
        .0
}

/// The node id currently leading `table`'s own tablet.
fn leader_of_table(cluster: &SimCluster, table: &str) -> u64 {
    let tablet = tablet_of_table(cluster, table);
    cluster
        .leader_index_of(tablet)
        .unwrap_or_else(|| panic!("tablet {} has no leader", tablet.0))
}

/// A node id that does **not** lead `table`'s own tablet — mirrors
/// `sim_cluster_dynamo_partiql.rs`'s own `non_leader` helper, generalized to
/// this module's own `SimCluster::new(seed, 3, 3)` shape.
fn non_leader_of_table(cluster: &SimCluster, table: &str) -> u64 {
    let leader = leader_of_table(cluster, table);
    (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a 3-node cluster always has a non-leader node")
}

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

/// `ListStreams`, issued from `node` via [`SimCluster::dynamo_streams`] —
/// mirrors `tests/dynamo_streams.rs`'s own real-socket helper of the same
/// name, minus the socket.
fn list_streams_via_wire(cluster: &mut SimCluster, node: u64) -> (u16, serde_json::Value) {
    let (status, body) =
        cluster.dynamo_streams(node, "DynamoDBStreams_20120810.ListStreams", b"{}");
    (status, json(&body))
}

/// `DescribeStream`, issued from `node`.
fn describe_stream_via_wire(
    cluster: &mut SimCluster,
    node: u64,
    stream_arn: &str,
) -> (u16, serde_json::Value) {
    let body = format!(r#"{{"StreamArn":"{stream_arn}"}}"#);
    let (status, resp) = cluster.dynamo_streams(
        node,
        "DynamoDBStreams_20120810.DescribeStream",
        body.as_bytes(),
    );
    (status, json(&resp))
}

/// `GetShardIterator`, issued from `node` — panics on a non-200 (every
/// scenario below only calls this where a valid iterator is expected;
/// a scenario asserting a refusal calls `describe_stream_via_wire`/
/// `cluster.dynamo_streams` directly instead, mirroring the real-socket
/// helper's own contract).
fn get_shard_iterator_via_wire(
    cluster: &mut SimCluster,
    node: u64,
    stream_arn: &str,
    shard_id: &str,
    iterator_type: &str,
    sequence_number: Option<&str>,
) -> String {
    let seq = sequence_number
        .map(|s| format!(r#","SequenceNumber":"{s}""#))
        .unwrap_or_default();
    let body = format!(
        r#"{{"StreamArn":"{stream_arn}","ShardId":"{shard_id}","ShardIteratorType":"{iterator_type}"{seq}}}"#
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

/// `GetRecords`, issued from `node` — panics on a non-200 (the identical
/// contract [`get_shard_iterator_via_wire`] documents).
fn get_records_via_wire(
    cluster: &mut SimCluster,
    node: u64,
    iterator: &str,
    limit: Option<usize>,
) -> (Vec<serde_json::Value>, Option<String>) {
    let lim = limit
        .map(|l| format!(r#","Limit":{l}"#))
        .unwrap_or_default();
    let body = format!(r#"{{"ShardIterator":"{iterator}"{lim}}}"#);
    let (status, resp) =
        cluster.dynamo_streams(node, "DynamoDBStreams_20120810.GetRecords", body.as_bytes());
    assert_eq!(status, 200, "GetRecords failed: {resp}");
    let v = json(&resp);
    let records = v["Records"].as_array().cloned().unwrap_or_default();
    let next = v["NextShardIterator"].as_str().map(str::to_owned);
    (records, next)
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

// ---------------------------------------------------------------------------
// C-07 PR 3: the Streams read API, driven through `execute_streams_op_as`.
// ---------------------------------------------------------------------------

/// Scenario (a): `ListStreams`/`DescribeStream` after enable.
fn run_list_streams_and_describe_stream_after_enable(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "orders";

    let (status, body) = create_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = enable_stream_via_wire(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: enable failed: {body}");
    let label = cluster
        .metadata(0)
        .table_stream(table)
        .map(|s| s.label.clone())
        .unwrap_or_else(|| panic!("seed={seed}: no stream label committed after enable"));

    let reader = non_leader_of_table(&cluster, table);

    let (status, v) = list_streams_via_wire(&mut cluster, reader);
    assert_eq!(status, 200, "seed={seed}: ListStreams failed: {v}");
    assert!(
        v["Streams"]
            .as_array()
            .unwrap_or_else(|| panic!("seed={seed}: no Streams array: {v}"))
            .iter()
            .any(|s| s["TableName"] == table && s["StreamLabel"] == label),
        "seed={seed}: ListStreams is missing {table}/{label}: {v}"
    );

    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{label}");
    let (status, v) = describe_stream_via_wire(&mut cluster, reader, &stream_arn);
    assert_eq!(status, 200, "seed={seed}: DescribeStream failed: {v}");
    assert_eq!(
        v["StreamDescription"]["StreamStatus"], "ENABLED",
        "seed={seed}: {v}"
    );
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    assert_eq!(
        shards.len(),
        1,
        "seed={seed}: expected exactly the open epoch-0 shard: {v}"
    );
    assert!(
        shards[0]["SequenceNumberRange"]["EndingSequenceNumber"].is_null(),
        "seed={seed}: the only shard must still be open (no writes yet): {v}"
    );
}

#[test]
fn list_streams_and_describe_stream_after_enable() {
    run_list_streams_and_describe_stream_after_enable(env_seed(0xC07E_2001));
}

#[test]
fn list_streams_and_describe_stream_after_enable_over_seeds() {
    for i in 0..5 {
        run_list_streams_and_describe_stream_after_enable(0xC07E_2100 + i);
    }
}

/// Scenario (b): `GetRecords` over the OPEN tail, before any seal — proves
/// `get_records_open` → `read_stream_hot_records` → `hot_read` forwarding.
fn run_get_records_over_the_open_tail_before_any_seal(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "t";

    let (status, body) = create_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = enable_stream_via_wire(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: enable failed: {body}");
    let label = cluster
        .metadata(0)
        .table_stream(table)
        .map(|s| s.label.clone())
        .unwrap();
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{label}");

    let writer = non_leader_of_table(&cluster, table);
    for (pk, v) in [("a", "1"), ("b", "2")] {
        let (status, body) = put_item(&mut cluster, writer, table, pk, v);
        assert_eq!(status, 200, "seed={seed}: PutItem({pk}) failed: {body}");
    }

    let reader = non_leader_of_table(&cluster, table);
    let (status, v) = describe_stream_via_wire(&mut cluster, reader, &stream_arn);
    assert_eq!(status, 200, "seed={seed}: DescribeStream failed: {v}");
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    assert_eq!(
        shards.len(),
        1,
        "seed={seed}: expected exactly the one open shard: {v}"
    );
    let shard0 = shards[0]["ShardId"].as_str().unwrap().to_owned();
    assert!(
        shards[0]["SequenceNumberRange"]["EndingSequenceNumber"].is_null(),
        "seed={seed}: must still be open: {v}"
    );

    // No `stream_shards` catalog row exists for this tablet at all — the
    // load-bearing precondition proving this read genuinely takes the OPEN
    // path, never the sealed one.
    let tablet = tablet_of_table(&cluster, table);
    assert!(
        cluster
            .metadata(reader)
            .stream_shards
            .keys()
            .all(|(t, _)| *t != tablet),
        "seed={seed}: a stream_shards row already exists before any seal — \
         this scenario would then prove the wrong serve path"
    );

    let token = get_shard_iterator_via_wire(
        &mut cluster,
        reader,
        &stream_arn,
        &shard0,
        "TRIM_HORIZON",
        None,
    );
    let (records, next) = get_records_via_wire(&mut cluster, reader, &token, None);
    assert_eq!(records.len(), 2, "seed={seed}: {records:?}");
    for r in &records {
        assert_eq!(r["eventName"], "INSERT", "seed={seed}: {r:?}");
    }
    assert!(
        next.is_some(),
        "seed={seed}: an open shard must never null its iterator"
    );
}

#[test]
fn get_records_over_the_open_tail_before_any_seal() {
    run_get_records_over_the_open_tail_before_any_seal(env_seed(0xC07E_3001));
}

#[test]
fn get_records_over_the_open_tail_before_any_seal_over_seeds() {
    for i in 0..5 {
        run_get_records_over_the_open_tail_before_any_seal(0xC07E_3100 + i);
    }
}

/// Scenario (c): `drive_stream_seal` then `GetRecords` over the SEALED
/// shard — proves `get_records_sealed` genuinely reads
/// `SegmentStoreHandle::S3` (the shared `SimSegmentStore`).
fn run_get_records_over_the_sealed_shard_from_the_shared_store(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "t";

    let (status, body) = create_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = enable_stream_via_wire(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: enable failed: {body}");
    let label = cluster
        .metadata(0)
        .table_stream(table)
        .map(|s| s.label.clone())
        .unwrap();
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{label}");

    let writer = non_leader_of_table(&cluster, table);
    for (pk, v) in [("a", "1"), ("b", "2"), ("c", "3")] {
        let (status, body) = put_item(&mut cluster, writer, table, pk, v);
        assert_eq!(status, 200, "seed={seed}: PutItem({pk}) failed: {body}");
    }
    let leader = leader_of_table(&cluster, table);
    cluster.drive_stream_seal(leader);

    let reader = non_leader_of_table(&cluster, table);
    let (status, v) = describe_stream_via_wire(&mut cluster, reader, &stream_arn);
    assert_eq!(status, 200, "seed={seed}: DescribeStream failed: {v}");
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    // `DescribeStream` always appends the tablet's now-open (epoch 1, still
    // empty) tail behind the just-sealed epoch 0 shard while the stream stays
    // enabled — sorted ascending by epoch, so shard 0 is always the sealed
    // one regardless of how many shards a call produced.
    assert_eq!(
        shards.len(),
        2,
        "seed={seed}: expected the sealed epoch-0 shard plus its still-open epoch-1 tail: {v}"
    );
    assert!(
        shards[0]["SequenceNumberRange"]["EndingSequenceNumber"].is_string(),
        "seed={seed}: must be sealed: {v}"
    );
    let shard0 = shards[0]["ShardId"].as_str().unwrap().to_owned();

    let token = get_shard_iterator_via_wire(
        &mut cluster,
        reader,
        &stream_arn,
        &shard0,
        "TRIM_HORIZON",
        None,
    );
    let (records, next) = get_records_via_wire(&mut cluster, reader, &token, None);
    assert_eq!(records.len(), 3, "seed={seed}: {records:?}");
    assert!(
        next.is_none(),
        "seed={seed}: a fully-drained sealed shard must null its iterator: {records:?}"
    );

    let store = cluster.segment_store();
    assert!(
        !store.stored_ids().is_empty(),
        "seed={seed}: segment_store().stored_ids() is empty after a real seal"
    );
}

#[test]
fn get_records_over_the_sealed_shard_from_the_shared_store() {
    run_get_records_over_the_sealed_shard_from_the_shared_store(env_seed(0xC07E_4001));
}

#[test]
fn get_records_over_the_sealed_shard_from_the_shared_store_over_seeds() {
    for i in 0..5 {
        run_get_records_over_the_sealed_shard_from_the_shared_store(0xC07E_4100 + i);
    }
}

/// Scenario (d): an iterator obtained before a seal continues correctly
/// across the seal (the sealed-vs-open handoff, ADR 0042 §2).
fn run_iterator_obtained_before_a_seal_continues_correctly_across_the_seal(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "t";

    let (status, body) = create_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = enable_stream_via_wire(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: enable failed: {body}");
    let label = cluster
        .metadata(0)
        .table_stream(table)
        .map(|s| s.label.clone())
        .unwrap();
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{label}");

    let reader = non_leader_of_table(&cluster, table);
    let (status, v) = describe_stream_via_wire(&mut cluster, reader, &stream_arn);
    assert_eq!(status, 200, "seed={seed}: {v}");
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    assert_eq!(shards.len(), 1, "seed={seed}: {v}");
    let shard0 = shards[0]["ShardId"].as_str().unwrap().to_owned();

    // Mint against the still-empty open shard (epoch 0 — nothing sealed
    // yet).
    let token = get_shard_iterator_via_wire(
        &mut cluster,
        reader,
        &stream_arn,
        &shard0,
        "TRIM_HORIZON",
        None,
    );
    let (records, next) = get_records_via_wire(&mut cluster, reader, &token, None);
    assert!(records.is_empty(), "seed={seed}: {records:?}");
    let token = next.unwrap_or_else(|| panic!("seed={seed}: open shard, must not null"));

    // Write, then let this exact epoch seal underneath — never a re-mint.
    let writer = non_leader_of_table(&cluster, table);
    let (status, body) = put_item(&mut cluster, writer, table, "p1", "1");
    assert_eq!(status, 200, "seed={seed}: PutItem failed: {body}");
    let leader = leader_of_table(&cluster, table);
    cluster.drive_stream_seal(leader);

    let (records, next) = get_records_via_wire(&mut cluster, reader, &token, None);
    assert_eq!(
        records.len(),
        1,
        "seed={seed}: the pre-seal token must still see the record now sealed under it: \
         {records:?}"
    );
    assert_eq!(
        records[0]["eventName"], "INSERT",
        "seed={seed}: {records:?}"
    );
    assert!(
        next.is_none(),
        "seed={seed}: now sealed and fully drained, so this must null: {records:?}"
    );
}

#[test]
fn iterator_obtained_before_a_seal_continues_correctly_across_the_seal() {
    run_iterator_obtained_before_a_seal_continues_correctly_across_the_seal(env_seed(0xC07E_5001));
}

#[test]
fn iterator_obtained_before_a_seal_continues_correctly_across_the_seal_over_seeds() {
    for i in 0..5 {
        run_iterator_obtained_before_a_seal_continues_correctly_across_the_seal(0xC07E_5100 + i);
    }
}

/// Scenario (e): `NextShardIterator` pagination with a small `Limit`
/// visits every record exactly once.
fn run_next_shard_iterator_pagination_with_small_limit_visits_each_record_once(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "t";

    let (status, body) = create_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = enable_stream_via_wire(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: enable failed: {body}");
    let label = cluster
        .metadata(0)
        .table_stream(table)
        .map(|s| s.label.clone())
        .unwrap();
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{label}");

    let writer = non_leader_of_table(&cluster, table);
    let ids = ["p1", "p2", "p3", "p4", "p5"];
    for id in ids {
        let (status, body) = put_item(&mut cluster, writer, table, id, "1");
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
    }
    let leader = leader_of_table(&cluster, table);
    cluster.drive_stream_seal(leader);

    let reader = non_leader_of_table(&cluster, table);
    let (status, v) = describe_stream_via_wire(&mut cluster, reader, &stream_arn);
    assert_eq!(status, 200, "seed={seed}: {v}");
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap().clone();
    // Epoch 0 (sealed, covering all 5 writes) plus its still-open, still-empty
    // epoch-1 tail — see `get_records_over_the_sealed_shard_from_the_shared_
    // store`'s own comment on this shape.
    assert_eq!(
        shards.len(),
        2,
        "seed={seed}: expected the sealed shard covering all 5 writes plus its open tail: {v}"
    );
    let shard0 = shards[0]["ShardId"].as_str().unwrap().to_owned();

    let mut seen: Vec<String> = Vec::new();
    let mut token = get_shard_iterator_via_wire(
        &mut cluster,
        reader,
        &stream_arn,
        &shard0,
        "TRIM_HORIZON",
        None,
    );
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(
            pages < 100,
            "seed={seed}: pagination never terminated: seen={seen:?}"
        );
        let (records, next) = get_records_via_wire(&mut cluster, reader, &token, Some(2));
        assert!(
            records.len() <= 2,
            "seed={seed}: Limit=2 violated: {records:?}"
        );
        for r in &records {
            let pk = r["dynamodb"]["Keys"]["pk"]["S"]
                .as_str()
                .unwrap_or_else(|| panic!("seed={seed}: no pk in {r:?}"))
                .to_owned();
            seen.push(pk);
        }
        match next {
            Some(n) => token = n,
            None => break,
        }
    }
    seen.sort();
    let mut expected: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(
        seen, expected,
        "seed={seed}: every record must be visited exactly once, no gaps, no duplicates"
    );
}

#[test]
fn next_shard_iterator_pagination_with_small_limit_visits_each_record_once() {
    run_next_shard_iterator_pagination_with_small_limit_visits_each_record_once(env_seed(
        0xC07E_6001,
    ));
}

#[test]
fn next_shard_iterator_pagination_with_small_limit_visits_each_record_once_over_seeds() {
    for i in 0..5 {
        run_next_shard_iterator_pagination_with_small_limit_visits_each_record_once(
            0xC07E_6100 + i,
        );
    }
}

/// Scenario (f): `LATEST` on a genuinely open shard, then
/// `AT_SEQUENCE_NUMBER`/`AFTER_SEQUENCE_NUMBER` once that same content is
/// sealed.
fn run_iterator_types_latest_at_and_after_sequence_number(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "t";

    let (status, body) = create_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = enable_stream_via_wire(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: enable failed: {body}");
    let label = cluster
        .metadata(0)
        .table_stream(table)
        .map(|s| s.label.clone())
        .unwrap();
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{label}");
    let reader = non_leader_of_table(&cluster, table);
    let writer = non_leader_of_table(&cluster, table);

    // --- LATEST on the still-open shard ---
    let (status, body) = put_item(&mut cluster, writer, table, "p0", "0");
    assert_eq!(status, 200, "seed={seed}: PutItem(p0) failed: {body}");

    let (status, v) = describe_stream_via_wire(&mut cluster, reader, &stream_arn);
    assert_eq!(status, 200, "seed={seed}: {v}");
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    assert_eq!(shards.len(), 1, "seed={seed}: {v}");
    let shard0 = shards[0]["ShardId"].as_str().unwrap().to_owned();

    let latest =
        get_shard_iterator_via_wire(&mut cluster, reader, &stream_arn, &shard0, "LATEST", None);
    let (records, next) = get_records_via_wire(&mut cluster, reader, &latest, None);
    assert!(
        records.is_empty(),
        "seed={seed}: LATEST must see nothing new yet: {records:?}"
    );
    let latest = next.unwrap_or_else(|| panic!("seed={seed}: open shard, must not null"));

    let (status, body) = put_item(&mut cluster, writer, table, "p1", "1");
    assert_eq!(status, 200, "seed={seed}: PutItem(p1) failed: {body}");
    let (records, _next) = get_records_via_wire(&mut cluster, reader, &latest, None);
    assert_eq!(
        records.len(),
        1,
        "seed={seed}: LATEST must see exactly the write minted after it: {records:?}"
    );
    assert_eq!(
        records[0]["dynamodb"]["Keys"]["pk"]["S"], "p1",
        "seed={seed}: {records:?}"
    );

    // --- Seal p0+p1, then AT/AFTER_SEQUENCE_NUMBER on the sealed shard ---
    let leader = leader_of_table(&cluster, table);
    cluster.drive_stream_seal(leader);

    let (status, v) = describe_stream_via_wire(&mut cluster, reader, &stream_arn);
    assert_eq!(status, 200, "seed={seed}: {v}");
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    // Epoch 0 (sealed, p0+p1) plus its still-open, still-empty epoch-1 tail.
    assert_eq!(
        shards.len(),
        2,
        "seed={seed}: expected the sealed shard for p0+p1 plus its open tail: {v}"
    );
    assert!(
        shards[0]["SequenceNumberRange"]["EndingSequenceNumber"].is_string(),
        "seed={seed}: {v}"
    );
    let sealed_shard = shards[0]["ShardId"].as_str().unwrap().to_owned();

    let all_token = get_shard_iterator_via_wire(
        &mut cluster,
        reader,
        &stream_arn,
        &sealed_shard,
        "TRIM_HORIZON",
        None,
    );
    let (all_records, next) = get_records_via_wire(&mut cluster, reader, &all_token, None);
    assert_eq!(all_records.len(), 2, "seed={seed}: {all_records:?}");
    assert!(next.is_none(), "seed={seed}: {all_records:?}");
    let seq0 = all_records[0]["dynamodb"]["SequenceNumber"]
        .as_str()
        .unwrap()
        .to_owned();
    let seq1 = all_records[1]["dynamodb"]["SequenceNumber"]
        .as_str()
        .unwrap()
        .to_owned();

    let at1 = get_shard_iterator_via_wire(
        &mut cluster,
        reader,
        &stream_arn,
        &sealed_shard,
        "AT_SEQUENCE_NUMBER",
        Some(&seq1),
    );
    let (records, next) = get_records_via_wire(&mut cluster, reader, &at1, None);
    assert_eq!(
        records.len(),
        1,
        "seed={seed}: AT_SEQUENCE_NUMBER(seq1) must include the record AT seq1: {records:?}"
    );
    assert_eq!(
        records[0]["dynamodb"]["SequenceNumber"], seq1,
        "seed={seed}: {records:?}"
    );
    assert!(next.is_none(), "seed={seed}: fully drained: {records:?}");

    let after0 = get_shard_iterator_via_wire(
        &mut cluster,
        reader,
        &stream_arn,
        &sealed_shard,
        "AFTER_SEQUENCE_NUMBER",
        Some(&seq0),
    );
    let (records, _next) = get_records_via_wire(&mut cluster, reader, &after0, None);
    assert_eq!(
        records.len(),
        1,
        "seed={seed}: AFTER_SEQUENCE_NUMBER(seq0) must exclude seq0 itself: {records:?}"
    );
    assert_eq!(
        records[0]["dynamodb"]["SequenceNumber"], seq1,
        "seed={seed}: {records:?}"
    );

    let after1 = get_shard_iterator_via_wire(
        &mut cluster,
        reader,
        &stream_arn,
        &sealed_shard,
        "AFTER_SEQUENCE_NUMBER",
        Some(&seq1),
    );
    let (records, _next) = get_records_via_wire(&mut cluster, reader, &after1, None);
    assert!(
        records.is_empty(),
        "seed={seed}: AFTER_SEQUENCE_NUMBER(seq1) must see nothing further: {records:?}"
    );
}

#[test]
fn iterator_types_latest_at_and_after_sequence_number() {
    run_iterator_types_latest_at_and_after_sequence_number(env_seed(0xC07E_7001));
}

#[test]
fn iterator_types_latest_at_and_after_sequence_number_over_seeds() {
    for i in 0..5 {
        run_iterator_types_latest_at_and_after_sequence_number(0xC07E_7100 + i);
    }
}

/// Scenario (g): cross-node reads — every node of the cluster answers the
/// identical `(records, NextShardIterator)` for the SAME iterator token
/// (ADR 0043 §A3: a sealed shard is served by any node).
fn run_cross_node_reads_answer_the_same_records_for_the_same_iterator(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "t";

    let (status, body) = create_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = enable_stream_via_wire(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: enable failed: {body}");
    let label = cluster
        .metadata(0)
        .table_stream(table)
        .map(|s| s.label.clone())
        .unwrap();
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{label}");

    let writer = non_leader_of_table(&cluster, table);
    for (pk, v) in [("a", "1"), ("b", "2")] {
        let (status, body) = put_item(&mut cluster, writer, table, pk, v);
        assert_eq!(status, 200, "seed={seed}: PutItem({pk}) failed: {body}");
    }
    let leader = leader_of_table(&cluster, table);
    cluster.drive_stream_seal(leader);

    let (status, v) = describe_stream_via_wire(&mut cluster, leader, &stream_arn);
    assert_eq!(status, 200, "seed={seed}: {v}");
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    // Epoch 0 (sealed, a+b) plus its still-open, still-empty epoch-1 tail.
    assert_eq!(shards.len(), 2, "seed={seed}: {v}");
    let shard0 = shards[0]["ShardId"].as_str().unwrap().to_owned();

    let token = get_shard_iterator_via_wire(
        &mut cluster,
        leader,
        &stream_arn,
        &shard0,
        "TRIM_HORIZON",
        None,
    );

    let mut previous: Option<(Vec<serde_json::Value>, Option<String>)> = None;
    for node in 0..cluster.node_count() as u64 {
        let result = get_records_via_wire(&mut cluster, node, &token, None);
        if let Some(prev) = &previous {
            assert_eq!(
                &result, prev,
                "seed={seed}: node {node} disagrees with an earlier node on the SAME iterator"
            );
        }
        previous = Some(result);
    }
    let (records, next) = previous.unwrap();
    assert_eq!(records.len(), 2, "seed={seed}: {records:?}");
    assert!(next.is_none(), "seed={seed}: {records:?}");
}

#[test]
fn cross_node_reads_answer_the_same_records_for_the_same_iterator() {
    run_cross_node_reads_answer_the_same_records_for_the_same_iterator(env_seed(0xC07E_8001));
}

#[test]
fn cross_node_reads_answer_the_same_records_for_the_same_iterator_over_seeds() {
    for i in 0..5 {
        run_cross_node_reads_answer_the_same_records_for_the_same_iterator(0xC07E_8100 + i);
    }
}

/// Scenario (h): F12-b's disable grace window (ADR 0042 §11,
/// `docs/streams-notes.md`) — `ListStreams` still names the `DISABLED`
/// label, `DescribeStream` reports it with no open shard, its already-
/// sealed reads keep working, and a genuinely never-existed label is
/// `ResourceNotFoundException`.
fn run_disable_then_grace_window_describe_and_get_records(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "t";

    let (status, body) = create_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = enable_stream_via_wire(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: enable failed: {body}");
    let label = cluster
        .metadata(0)
        .table_stream(table)
        .map(|s| s.label.clone())
        .unwrap();
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{label}");

    let writer = non_leader_of_table(&cluster, table);
    let (status, body) = put_item(&mut cluster, writer, table, "p1", "1");
    assert_eq!(status, 200, "seed={seed}: PutItem failed: {body}");
    let leader = leader_of_table(&cluster, table);
    cluster.drive_stream_seal(leader);

    // Disable from a different node than the one that enabled it — proving
    // the relay-allowlist shape `dynamo_streams.rs`'s real-socket regression
    // already covers, now reachable under `SimEnv`.
    let disabler = non_leader_of_table(&cluster, table);
    let (status, body) = disable_stream_via_wire(&mut cluster, disabler, table);
    assert_eq!(status, 200, "seed={seed}: disable failed: {body}");
    for n in 0..cluster.node_count() as u64 {
        assert!(
            cluster.metadata(n).table_stream(table).is_none(),
            "seed={seed}: node {n} still reports the stream enabled after disable"
        );
    }

    let reader = non_leader_of_table(&cluster, table);
    let (status, v) = list_streams_via_wire(&mut cluster, reader);
    assert_eq!(status, 200, "seed={seed}: {v}");
    assert!(
        v["Streams"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["StreamLabel"] == label),
        "seed={seed}: a disabled-but-unreaped stream must still be listed: {v}"
    );

    let (status, v) = describe_stream_via_wire(&mut cluster, reader, &stream_arn);
    assert_eq!(status, 200, "seed={seed}: {v}");
    assert_eq!(
        v["StreamDescription"]["StreamStatus"], "DISABLED",
        "seed={seed}: {v}"
    );
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    assert!(!shards.is_empty(), "seed={seed}: {v}");
    for s in shards {
        assert!(
            s["SequenceNumberRange"]["EndingSequenceNumber"].is_string(),
            "seed={seed}: a DISABLED stream must have no open shard: {v}"
        );
    }
    let shard0 = shards[0]["ShardId"].as_str().unwrap().to_owned();

    let token = get_shard_iterator_via_wire(
        &mut cluster,
        reader,
        &stream_arn,
        &shard0,
        "TRIM_HORIZON",
        None,
    );
    let (records, _next) = get_records_via_wire(&mut cluster, reader, &token, None);
    assert_eq!(records.len(), 1, "seed={seed}: {records:?}");
    assert_eq!(
        records[0]["eventName"], "INSERT",
        "seed={seed}: {records:?}"
    );

    // A label that never existed at all (F12-b's `ResourceNotFoundException`
    // branch).
    let bogus_arn = format!("arn:aws:dynamodb:animus:0:table/{table}/stream/never-existed");
    let (status, v) = describe_stream_via_wire(&mut cluster, reader, &bogus_arn);
    assert_eq!(status, 400, "seed={seed}: {v}");
    assert!(
        v["__type"]
            .as_str()
            .unwrap_or_default()
            .contains("ResourceNotFoundException"),
        "seed={seed}: {v}"
    );
}

#[test]
fn disable_then_grace_window_describe_and_get_records() {
    run_disable_then_grace_window_describe_and_get_records(env_seed(0xC07E_9001));
}

#[test]
fn disable_then_grace_window_describe_and_get_records_over_seeds() {
    for i in 0..5 {
        run_disable_then_grace_window_describe_and_get_records(0xC07E_9100 + i);
    }
}
