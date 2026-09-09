//! `SimCluster`-driven deterministic siblings for the admin HTTP-JSON
//! interface's **mutating** actions (ADR 0061 rung H, C-08 PR 6) — the
//! sibling of `sim_cluster_admin.rs` (PR 5, the observer/`GET` routes):
//! `POST /admin/data/dynamo`, `/admin/data/drop-table`, `/admin/data/seed`,
//! `/admin/tablet/split`, `/admin/credentials`(`/rotate`/`/revoke`), and
//! `/admin/control/transfer`.
//!
//! **No `admin.rs`/`dynamo.rs`/`lib.rs` dispatch change was needed beyond
//! one real seam bug this rung found and fixed** (below) — every route
//! this module drives was already reachable through
//! [`crate::admin::GenericAdminHost`] (PR 2's own newtype, never
//! `ClientCtx`'s own concrete `impl AdminHost`), the identical seam PR 5
//! already established and this module reuses unmodified.
//!
//! **One real, previously-latent seam bug found and fixed**:
//! `ClientCtx::admin_transfer_control_leadership` (`lib.rs`) has a fully
//! generic `<E: Env, R: RelayClient>` signature (rung C5) but its own
//! commit-wait loop still read `tokio::time::Instant::now()`/called
//! `tokio::time::sleep(..)` directly instead of `self.env.now()`/
//! `self.env.sleep(..)` — a generic *signature* proves nothing about
//! whether a function's *body* actually avoids the real clock/timer (the
//! identical lesson ADR 0061 rung G, C-07 PR 2's `index_drain::seal_now`
//! finding already recorded, and rung F/G's own `recovery_grace_now_ms`/
//! `txn_recover` findings before that) — this is that lesson's **third**
//! recurrence in this crate, not a new entry; see
//! `docs/engineering-lessons.md`'s matching dated note. `SimEnv` has no
//! real Tokio reactor, so `tokio::time::sleep` panics ("there is no
//! reactor running") the instant this loop's first poll iteration is
//! reached whenever the initial arm attempt doesn't resolve on its very
//! first pass — found immediately by
//! [`run_control_transfer_moves_leadership_to_the_named_node`] below, this
//! module's own first real exercise of the route. Fixed with the same
//! `self.env.now().saturating_add(..)`/`self.env.now() >= deadline`/
//! `self.env.sleep(..)` conversion every prior rung's own `tokio::time`
//! finding used — see that method's own doc for the full behavioral
//! contract, unchanged by this fix.
//!
//! ## Scenarios (seed-parameterized, `_over_seeds` at 5 seeds each)
//!
//! (1) [`run_data_write_dynamo`] — `PutItem` then `GetItem
//!     (ConsistentRead: true)` through `POST /admin/data/dynamo`, issued
//!     from a node hosting **no replica** of the table's own tablet
//!     (`non_leader_of_table`) — proving the proxy's own internal
//!     forwarding, not just a locally-served call.
//! (2) [`run_table_management_create_and_drop`] — a composite `CreateTable`
//!     (string partition key + **numeric** sort key) through the proxy,
//!     issued from a **control follower**, converging in the replicated
//!     catalog with its declared key types intact, then a `drop-table`
//!     converging to gone — the DDL-relay regression this test's
//!     real-socket original doesn't itself exercise (it always issues from
//!     node 0), added here since a `SimCluster` scenario gets it almost
//!     for free.
//! (3) [`run_seed_writes_synthetic_keys`] — the bulk-seed endpoint: seeding
//!     a nonexistent table 404s, seeding a real one writes exactly the
//!     requested count, the seeded rows are visible in a raw storage scan
//!     AND read back through the DynamoDB proxy (including the filler
//!     `payload` attribute), a composite table's seeded rows carry both
//!     key attributes, and a scanned key's own displayed (percent-encoded)
//!     form round-trips through `/admin/storage/key`.
//! (3b) [`run_seed_reports_unprocessed_rows_when_throttled`] — ADR 0021's
//!     2026-09-09 amendment: seeding into a table with a tiny provisioned
//!     write capacity leaves rows `unprocessed` (named in the response,
//!     `written + unprocessed == requested`) rather than the deleted
//!     internal seeder's own silent-drop behavior.
//! (4) [`run_split_in_place_children_inherit_the_parents_own_replicas`] —
//!     ADR 0062 rung 4's "fork first, always local" teeth: a 4-node
//!     cluster with RF 3 leaves one node genuinely idle (never one of the
//!     table's replicas), so the pre-fork `MetaCommand::BeginSplitInPlace`
//!     intent's own two children must inherit the parent's own CURRENT
//!     replicas verbatim, never a placement-recomputed set that would have
//!     recruited the idle node.
//! (5) [`run_credentials_put_rotate_revoke_round_trip`] — the full ADR
//!     0066 admin CRUD life cycle: `Put` (a scoped, not `allow_all`,
//!     policy — proving it round-trips), `Rotate` (a grace window opens;
//!     rotating an unknown id 404s), `Revoke` (the row disappears;
//!     revoking again is idempotent, still 200).
//! (6) [`run_credentials_put_on_a_follower_is_relayed_to_the_leader`] — the
//!     `is_relayable_command` allowlist regression this catalog's own
//!     commands need (the bimodal per-process flake root `CLAUDE.md` warns
//!     a missed allowlist entry causes): a `PutCredential` issued against a
//!     control **follower**'s admin route relays to the leader and
//!     converges on every node.
//! (7) [`run_control_transfer_moves_leadership_to_the_named_node`] —
//!     `POST /admin/control/transfer {to}` moves control-plane leadership
//!     to a named live voter, retrying the whole call (never a one-shot
//!     assert) against whichever node currently leads on any retryable
//!     409 — mirrors the real-socket original's own issue #671/#688 retry
//!     discipline, simplified since this fixture has no independent
//!     third-voter election-timer jitter to race.
//! (8) [`run_control_transfer_on_a_follower_is_refused`] — the mirror-image
//!     negative case: a follower's own admin route refuses the transfer
//!     outright (never 200), the same not-relayed, local-leader-only
//!     discipline every other `control/member/*` action has.
//!
//! ## Kept `ProdEnv`, in `tests/admin_endpoint.rs` (each with its own one-
//! line reason comment)
//!
//! - `admin_interface_surfaces_state_and_actions` — the one action this
//!   sweep exercises beyond what's covered elsewhere, `POST
//!   /admin/storage/flush`, has the identical `flush_now` gap PR 5's own
//!   `admin_raftkv_default_does_not_materialize_the_dataset` KEPT reason
//!   already names (`MemoryEngine` has no LSM/SSTable concept for a
//!   forced flush to act on); kept whole as this crate's one remaining
//!   real-socket observer sweep, rather than trimmed to nothing.
//! - `seed_load_does_not_storm_cp_elections` — a real-thread election-
//!   timing liveness assertion (a CP term barely moving under sustained
//!   write load); `SimEnv`'s virtual clock structurally cannot trip a
//!   wall-clock election timeout, so there is no analog to build.
//! - `admin_system_table_split_lineage_after_a_real_split` — `GET
//!   /admin/system-table` unconditionally answers `{"available": false}`
//!   under `SimCluster` (`ctx.control_storage` is always `None` there — the
//!   identical gap `tests/system_table.rs`'s own two tests are KEPT for,
//!   PR 5's own doc), so a split-lineage row's real shape can never be
//!   observed through this route here regardless of how faithfully the
//!   split itself is reproduced.
//! - `admin_storage_compact_action` — `POST /admin/storage/compact` calls
//!   `CpGroup::compact_now()`, which is `None` for the `MemoryEngine`
//!   backend this fixture's every tablet uses (no LSM/SSTable concept to
//!   compact) — the identical backend gap `admin_storage_compact_action`'s
//!   own KEPT sibling above has for flush.
//!
//! **No product bug found beyond the `admin_transfer_control_leadership`
//! clock-conversion finding above.** Every scenario passed at its pinned
//! seed and every `_over_seeds` seed once that fix landed.
//!
//! Replays (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <scenario name>`.

use std::time::Duration;

use serde_json::Value;

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{
    control_leader_and_follower, create_table_via_wire, env_seed, json, non_leader_of_table,
};

/// Poll `GET path?query` on `node` until `pred` holds on the parsed JSON
/// body, or panic after `max_polls` calls. Each [`SimCluster::admin`] call
/// already burns a full `OP_BUDGET` (12s) of virtual time
/// (`SimCluster::spawn_and_capture`'s own doc), so this fixture's own
/// converged-or-timeout convention (root `CLAUDE.md`'s Testing rule) needs
/// no separate `run_for` — the loop's own iteration count already IS the
/// virtual-time budget. Mirrors `sim_cluster_admin.rs`'s own identically-
/// named helper (kept as a small local copy rather than a cross-module
/// `pub(crate)` export, since neither module needs the other's).
fn poll_admin(
    cluster: &mut SimCluster,
    node: u64,
    path: &str,
    query: &str,
    seed: u64,
    max_polls: usize,
    mut pred: impl FnMut(&Value) -> bool,
) -> Value {
    for _ in 0..max_polls {
        let (status, body) = cluster.admin(node, "GET", path, query, &[]);
        assert_eq!(status, 200, "seed={seed}: GET {path}?{query}: {body}");
        let v = json(&body);
        if pred(&v) {
            return v;
        }
    }
    panic!(
        "seed={seed}: GET {path}?{query} on node {node} never converged within {max_polls} \
         polls ({max_polls} x OP_BUDGET of virtual time)"
    );
}

/// Encode a query-param value the way the browser's `encodeURIComponent`
/// does (everything but unreserved characters becomes `%NN`) — mirrors
/// `tests/admin_endpoint.rs`'s now-deleted copy (its last caller,
/// `admin_seed_writes_synthetic_keys`, moved here).
fn percent_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// (1) admin_data_write_dynamo
// ---------------------------------------------------------------------------

fn run_data_write_dynamo(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, ct) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"t","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable t: {ct}");

    let node = non_leader_of_table(&cluster, "t");

    let (status, put) = cluster.admin(
        node,
        "POST",
        "/admin/data/dynamo",
        "",
        br#"{"op":"PutItem","payload":{"TableName":"t","Item":{"pk":{"S":"alice"},"v":{"N":"7"}}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: PutItem via admin proxy: {put}");

    let (status, got) = cluster.admin(
        node,
        "POST",
        "/admin/data/dynamo",
        "",
        br#"{"op":"GetItem","payload":{"TableName":"t",
            "Key":{"pk":{"S":"alice"}},"ConsistentRead":true}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: GetItem via admin proxy: {got}");
    assert_eq!(
        json(&got)["Item"]["v"]["N"].as_str(),
        Some("7"),
        "seed={seed}: GetItem reads back the written value: {got}"
    );
}

#[test]
fn data_write_dynamo() {
    run_data_write_dynamo(env_seed(0xC086_0001));
}

#[test]
fn data_write_dynamo_over_seeds() {
    for i in 0..5 {
        run_data_write_dynamo(0xC086_0100 + i);
    }
}

// ---------------------------------------------------------------------------
// (2) admin_table_management_create_and_drop
// ---------------------------------------------------------------------------

fn run_table_management_create_and_drop(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_, follower) = control_leader_and_follower(&mut cluster);

    let (status, body) = cluster.admin(
        follower,
        "POST",
        "/admin/data/dynamo",
        "",
        br#"{"op":"CreateTable","payload":{"TableName":"widgets","KeySchema":[
            {"AttributeName":"id","KeyType":"HASH"},{"AttributeName":"seq","KeyType":"RANGE"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"},
            {"AttributeName":"seq","AttributeType":"N"}]}}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable via admin proxy: {body}"
    );

    let status_body = poll_admin(&mut cluster, follower, "/admin/status", "", seed, 30, |v| {
        v["schemas"]["tables"]
            .get("widgets")
            .is_some_and(|w| !w.is_null())
    });
    let schema = &status_body["schemas"]["tables"]["widgets"];
    assert_eq!(
        schema["partition_key"], "id",
        "seed={seed}: partition key recorded: {schema}"
    );
    assert_eq!(
        schema["clustering_keys"][0], "seq",
        "seed={seed}: sort key recorded: {schema}"
    );
    let seq_ty = schema["columns"]
        .as_array()
        .and_then(|cols| cols.iter().find(|c| c["name"] == "seq"))
        .map(|c| c["ty"].clone());
    assert_eq!(
        seq_ty,
        Some(serde_json::json!("Number")),
        "seed={seed}: the numeric sort key's type reaches the catalog: {schema}"
    );

    let (status, body) = cluster.admin(
        follower,
        "POST",
        "/admin/data/drop-table",
        "",
        br#"{"table":"widgets"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: drop-table: {body}");

    poll_admin(&mut cluster, follower, "/admin/status", "", seed, 30, |v| {
        v["schemas"]["tables"]
            .get("widgets")
            .is_none_or(Value::is_null)
    });
}

#[test]
fn table_management_create_and_drop() {
    run_table_management_create_and_drop(env_seed(0xC086_0002));
}

#[test]
fn table_management_create_and_drop_over_seeds() {
    for i in 0..5 {
        run_table_management_create_and_drop(0xC086_0200 + i);
    }
}

// ---------------------------------------------------------------------------
// (3) admin_seed_writes_synthetic_keys
// ---------------------------------------------------------------------------

fn run_seed_writes_synthetic_keys(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, ct) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"seedt","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable seedt: {ct}");

    // Seeding a non-existent table is a 404 (no implicit create).
    let (status, missing) = cluster.admin(
        0,
        "POST",
        "/admin/data/seed",
        "",
        br#"{"table":"nope","count":1}"#,
    );
    assert_eq!(
        status, 404,
        "seed={seed}: seeding a non-existent table is rejected: {missing}"
    );

    let (status, body) = cluster.admin(
        0,
        "POST",
        "/admin/data/seed",
        "",
        br#"{"table":"seedt","count":60,"key_prefix":"seed:","value_bytes":8}"#,
    );
    assert_eq!(status, 200, "seed={seed}: seed returns 200: {body}");
    let v = json(&body);
    assert_eq!(
        v["written"], 60,
        "seed={seed}: seed wrote all requested keys: {v}"
    );

    // The seeded keys are durably in the leader's local storage. Walk every
    // node until one answers as the tablet's own leader (mirrors the
    // real-socket original's own scan-every-node idiom, since this
    // fixture's `leader_of_table` needs the table's tablet id resolved the
    // identical way `tablet_of_table` does and neither is imported here for
    // just this one lookup).
    let (_, rk0) = cluster.admin(0, "GET", "/admin/raftkv", "", &[]);
    let tablet = json(&rk0)["groups"][0]["tablet"]
        .as_u64()
        .expect("seedt's own bootstrap tablet id");
    let mut leader = None;
    for node in 0..cluster.node_count() as u64 {
        let (_, rk) = cluster.admin(node, "GET", "/admin/raftkv", "", &[]);
        if json(&rk)["groups"][0]["is_leader"].as_bool() == Some(true) {
            leader = Some(node);
            break;
        }
    }
    let leader = leader.expect("seed={seed}: a CP group leader exists");

    let (status, scan) = cluster.admin(
        leader,
        "GET",
        "/admin/storage/scan",
        &format!("tablet={tablet}&limit=200"),
        &[],
    );
    assert_eq!(status, 200, "seed={seed}: {scan}");
    let sv = json(&scan);
    let seeded = sv["items"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter(|it| it["key"].as_str().is_some_and(|k| k.contains("seed:")))
                .count()
        })
        .unwrap_or(0);
    assert!(
        seeded >= 60,
        "seed={seed}: all seeded keys are in the leader's storage: {sv}"
    );

    // A seeded row is a real DynamoDB item — reads back through the
    // DynamoDB proxy by its catalog key attribute, filler `payload` included.
    let (status, got) = cluster.admin(
        0,
        "POST",
        "/admin/data/dynamo",
        "",
        br#"{"op":"GetItem","payload":{"TableName":"seedt",
            "Key":{"id":{"S":"seed:000000000007"}},"ConsistentRead":true}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: GetItem on a seeded row: {got}");
    let gv = json(&got);
    assert_eq!(
        gv["Item"]["id"]["S"], "seed:000000000007",
        "seed={seed}: seeded item carries its schema partition key: {gv}"
    );
    assert!(
        gv["Item"]["payload"]["S"].is_string(),
        "seed={seed}: seeded item carries the filler payload attribute: {gv}"
    );

    // A composite table seeds items with both key attributes.
    let (status, ct2) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"seedc","KeySchema":[{"AttributeName":"id","KeyType":"HASH"},
            {"AttributeName":"rk","KeyType":"RANGE"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"},
            {"AttributeName":"rk","AttributeType":"S"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable seedc: {ct2}");
    let (status, body2) = cluster.admin(
        0,
        "POST",
        "/admin/data/seed",
        "",
        br#"{"table":"seedc","count":5,"key_prefix":"seed:","value_bytes":32}"#,
    );
    assert_eq!(status, 200, "seed={seed}: seed seedc returns 200: {body2}");
    assert_eq!(
        json(&body2)["written"],
        5,
        "seed={seed}: seed wrote all requested keys: {body2}"
    );
    let (status, got2) = cluster.admin(
        0,
        "POST",
        "/admin/data/dynamo",
        "",
        br#"{"op":"GetItem","payload":{"TableName":"seedc",
            "Key":{"id":{"S":"seed:000000000003"},"rk":{"S":"000000000003"}},
            "ConsistentRead":true}}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: GetItem on a seeded composite row: {got2}"
    );
    assert_eq!(
        json(&got2)["Item"]["rk"]["S"],
        "000000000003",
        "seed={seed}: seeded composite item carries its sort key: {got2}"
    );

    // A displayed key round-trips through the inspector URL exactly as the
    // dashboard sends it (percent-encoded).
    let shown = sv["items"]
        .as_array()
        .and_then(|items| {
            items
                .iter()
                .find_map(|it| it["key"].as_str().filter(|k| k.contains("seed:")))
        })
        .expect("seed={seed}: a seeded key is listed")
        .to_owned();
    let (status, inspect) = cluster.admin(
        leader,
        "GET",
        "/admin/storage/key",
        &format!("tablet={tablet}&key={}", percent_encode(&shown)),
        &[],
    );
    assert_eq!(status, 200, "seed={seed}: {inspect}");
    assert!(
        json(&inspect)["live"].is_string(),
        "seed={seed}: displayed key `{shown}` resolves to its live value: {inspect}"
    );
}

#[test]
fn seed_writes_synthetic_keys() {
    run_seed_writes_synthetic_keys(env_seed(0xC086_0003));
}

#[test]
fn seed_writes_synthetic_keys_over_seeds() {
    for i in 0..5 {
        run_seed_writes_synthetic_keys(0xC086_0300 + i);
    }
}

// ---------------------------------------------------------------------------
// (3b) seed_reports_unprocessed_rows_when_throttled (ADR 0021 amendment)
// ---------------------------------------------------------------------------
//
// `POST /admin/data/seed` is now a thin proxy over the real `BatchWriteItem`
// wire operation (`admin::action_data_seed`), so a per-table throttle
// (ADR 0065) refuses a seed chunk the identical way it refuses any other
// client's `BatchWriteItem` — reported back as `unprocessed`/`error`, never
// silently dropped the way the deleted internal marker-batch arm's own
// `UnprocessedItems`-free write path used to. A tiny provisioned write
// capacity (well under what 500 rows of ~64-byte items demand) proves this
// end to end: some rows land, some are left `unprocessed`, and
// `written + unprocessed == requested` — the seeder's own accounting
// invariant, not a silently-lossy count.

fn run_seed_reports_unprocessed_rows_when_throttled(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, ct) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"seedthrottled","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable seedthrottled: {ct}");

    // A tiny write budget (1 WCU/s, 300 WCU burst capacity — `ThrottleBucket`'s
    // own `capacity = 300 x rate`, `animusd::CLAUDE.md`'s ADR 0065 entry).
    // A **marker** (plain, unindexed/unstreamed) table's `BatchWriteItem`
    // chunk commits as ONE `KindBatch` Raft entry, charged ONCE for the
    // whole chunk's summed value bytes (`capacity::write_units`, 1 WCU per
    // 1 KiB) — not once per item — so a small per-item `value_bytes` (as
    // `seed_writes_synthetic_keys` uses) would barely register: 25 items x
    // 64 bytes is ~2 WCU/chunk, nowhere near enough to exhaust a 300-WCU
    // budget. A large `value_bytes` (4000 B) makes each 25-item chunk cost
    // ~98 WCU, so the 300-WCU budget admits only the first ~3 of a wave's
    // worth of concurrent chunks before refusing the rest outright.
    //
    // Exactly `SEED_CONCURRENCY (8) x SEED_BATCH_WRITE_CAP (25) = 200` rows
    // — one full wave, no serialized second wave — keeps every refused
    // chunk's own bounded retry sequence (six attempts, backoff doubling
    // 200ms→5s, ~6.2s of sleep total) running **concurrently** with its
    // siblings rather than queued behind them, so the whole call finishes
    // in ~6.2s of virtual time — comfortably inside `SimCluster::admin`'s
    // fixed 12s `OP_BUDGET` (`SimCluster::spawn_and_capture`'s own doc). A
    // bigger row count would need a second wave once the first 8 chunks'
    // own futures resolve, pushing the total past `OP_BUDGET` and timing
    // out the call itself rather than proving anything about throttling.
    cluster.set_table_throughput(
        "seedthrottled",
        Some(animus_control::ProvisionedThroughput {
            read_units: 1,
            write_units: 1,
        }),
    );

    let (status, body) = cluster.admin(
        0,
        "POST",
        "/admin/data/seed",
        "",
        br#"{"table":"seedthrottled","count":200,"key_prefix":"seed:","value_bytes":4000}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: a partial seed is still a 200 (some rows landed): {body}"
    );
    let v = json(&body);
    let written = v["written"].as_u64().expect("written is a number");
    let unprocessed = v["unprocessed"].as_u64().expect("unprocessed is a number");
    assert_eq!(
        written + unprocessed,
        200,
        "seed={seed}: every requested row is accounted for, written or unprocessed: {v}"
    );
    assert!(
        unprocessed > 0,
        "seed={seed}: a throttled table must leave rows unprocessed, not silently \
         drop or force through the whole request: {v}"
    );
    assert!(
        v["error"].is_string(),
        "seed={seed}: a partial seed names the shortfall in `error`, not just a bare count: {v}"
    );
}

#[test]
fn seed_reports_unprocessed_rows_when_throttled() {
    run_seed_reports_unprocessed_rows_when_throttled(env_seed(0xC086_0009));
}

#[test]
fn seed_reports_unprocessed_rows_when_throttled_over_seeds() {
    for i in 0..5 {
        run_seed_reports_unprocessed_rows_when_throttled(0xC086_0900 + i);
    }
}

// ---------------------------------------------------------------------------
// (4) admin_split_in_place_children_inherit_the_parents_own_replicas
// ---------------------------------------------------------------------------

fn run_split_in_place_children_inherit_the_parents_own_replicas(seed: u64) {
    // 4 nodes, RF 3 — node 3 is never one of the table's own replicas, so
    // it is exactly the kind of currently-idle, would-balance-the-load
    // candidate a placement-recomputed (pre-ADR-0062) split would have
    // recruited for at least one child.
    let mut cluster = SimCluster::new(seed, 4, 3);
    let parent = cluster.create_table_with_replication("t", 3);
    cluster
        .put_raw(0, "t", b"k".to_vec(), b"v".to_vec())
        .unwrap_or_else(|e| panic!("seed={seed}: put_raw(k) failed: {e}"));

    let (status, before) = cluster.admin(0, "GET", "/admin/status", "", &[]);
    assert_eq!(status, 200, "seed={seed}: {before}");
    let before_v = json(&before);
    let parent_key = parent.0.to_string();
    let parent_replicas: Vec<String> = before_v["tablets"][&parent_key]["replicas"]
        .as_array()
        .expect("parent has a replica list")
        .iter()
        .map(|v| v.as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        parent_replicas.len(),
        3,
        "seed={seed}: the tablet's RF must be 3: {before_v}"
    );
    assert!(
        !parent_replicas.iter().any(|n| n == "n3"),
        "seed={seed}: n3 must be idle for this test to distinguish fork-first \
         from placement-chosen homes: {parent_replicas:?}"
    );

    let split_body = format!(r#"{{"tablet":{},"split_key":"k"}}"#, parent.0);
    let (status, split) =
        cluster.admin(0, "POST", "/admin/tablet/split", "", split_body.as_bytes());
    assert_eq!(status, 200, "seed={seed}: kickoff must succeed: {split}");

    let status_v = poll_admin(&mut cluster, 0, "/admin/status", "", seed, 30, |v| {
        let p = &v["tablets"][&parent_key];
        p["state"].as_str() == Some("Splitting") && !p["inplace_split"].is_null()
    });
    let intent = status_v["tablets"][&parent_key]["inplace_split"].clone();

    let children = intent["children"]
        .as_array()
        .expect("intent carries exactly two children");
    assert_eq!(
        children.len(),
        2,
        "seed={seed}: intent must carry exactly two children"
    );
    for (i, child) in children.iter().enumerate() {
        let child_replicas: Vec<String> = child["replicas"]
            .as_array()
            .unwrap_or_else(|| panic!("seed={seed}: child {i} has a replica list: {child}"))
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            child_replicas, parent_replicas,
            "seed={seed}: child {i}'s replicas must be exactly the parent's own \
             current replicas (ADR 0062 rung 4)"
        );
    }
}

#[test]
fn split_in_place_children_inherit_the_parents_own_replicas() {
    run_split_in_place_children_inherit_the_parents_own_replicas(env_seed(0xC086_0004));
}

#[test]
fn split_in_place_children_inherit_the_parents_own_replicas_over_seeds() {
    for i in 0..5 {
        run_split_in_place_children_inherit_the_parents_own_replicas(0xC086_0400 + i);
    }
}

// ---------------------------------------------------------------------------
// (5) admin_credentials_put_rotate_revoke_round_trip
// ---------------------------------------------------------------------------

fn run_credentials_put_rotate_revoke_round_trip(seed: u64) {
    let mut cluster = SimCluster::new(seed, 1, 1);

    // Put — a scoped policy, not allow_all, to prove policy round-trips.
    let put_body = serde_json::json!({
        "id": "AKID1",
        "secret": "s0",
        "policy": {
            "tables": {"kind": "names", "names": ["orders"]},
            "ops": ["read", "write"],
        },
        "enabled": true,
    })
    .to_string();
    let (status, put_resp) =
        cluster.admin(0, "POST", "/admin/credentials", "", put_body.as_bytes());
    assert_eq!(status, 200, "seed={seed}: PutCredential: {put_resp}");
    let put_v = json(&put_resp);
    assert_eq!(
        put_v["policy"]["tables"],
        serde_json::json!({"kind": "names", "names": ["orders"]})
    );
    assert_eq!(put_v["policy"]["ops"], serde_json::json!(["read", "write"]));

    // Rotate — a grace window opens; `rotation` is non-null.
    let rotate_body =
        serde_json::json!({"id": "AKID1", "new_secret": "s1", "grace_secs": 3600}).to_string();
    let (status, rotate_resp) = cluster.admin(
        0,
        "POST",
        "/admin/credentials/rotate",
        "",
        rotate_body.as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: RotateCredential: {rotate_resp}");
    let rotate_v = json(&rotate_resp);
    assert!(
        !rotate_v["rotation"].is_null(),
        "seed={seed}: a grace window should be open right after rotating: {rotate_v}"
    );
    assert!(rotate_v["rotation"]["previous_valid_until"].is_u64());

    // Rotating an unknown id is rejected.
    let (status, err) = cluster.admin(
        0,
        "POST",
        "/admin/credentials/rotate",
        "",
        serde_json::json!({"id": "no-such-id", "new_secret": "x", "grace_secs": 1})
            .to_string()
            .as_bytes(),
    );
    assert_eq!(status, 404, "seed={seed}: unknown id rotate: {err}");

    // Revoke — the row disappears; revoking again is idempotent (still 200).
    let revoke_body = serde_json::json!({"id": "AKID1"}).to_string();
    let (status, revoke_resp) = cluster.admin(
        0,
        "POST",
        "/admin/credentials/revoke",
        "",
        revoke_body.as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: RevokeCredential: {revoke_resp}");
    let (status, view) = cluster.admin(0, "GET", "/admin/credentials", "", &[]);
    assert_eq!(status, 200);
    assert_eq!(
        json(&view)["credentials"].as_array().map(Vec::len),
        Some(0),
        "seed={seed}: revoked credential should be gone: {view}"
    );
    let (status, revoke_again) = cluster.admin(
        0,
        "POST",
        "/admin/credentials/revoke",
        "",
        revoke_body.as_bytes(),
    );
    assert_eq!(
        status, 200,
        "seed={seed}: repeated revoke should be idempotent, not an error: {revoke_again}"
    );
}

#[test]
fn credentials_put_rotate_revoke_round_trip() {
    run_credentials_put_rotate_revoke_round_trip(env_seed(0xC086_0005));
}

#[test]
fn credentials_put_rotate_revoke_round_trip_over_seeds() {
    for i in 0..5 {
        run_credentials_put_rotate_revoke_round_trip(0xC086_0500 + i);
    }
}

// ---------------------------------------------------------------------------
// (6) admin_credentials_put_on_a_follower_is_relayed_to_the_leader
// ---------------------------------------------------------------------------

fn run_credentials_put_on_a_follower_is_relayed_to_the_leader(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_, follower) = control_leader_and_follower(&mut cluster);

    let put_body =
        serde_json::json!({"id": "AKID-FOLLOWER", "secret": "s0", "enabled": true}).to_string();
    let (status, resp) = cluster.admin(
        follower,
        "POST",
        "/admin/credentials",
        "",
        put_body.as_bytes(),
    );
    assert_eq!(
        status, 200,
        "seed={seed}: PutCredential via a follower-connected admin route: {resp}"
    );

    // Every node — leader and followers alike — converges on the same
    // catalog.
    for node in 0..cluster.node_count() as u64 {
        poll_admin(
            &mut cluster,
            node,
            "/admin/credentials",
            "",
            seed,
            20,
            |v| {
                v["credentials"]
                    .as_array()
                    .is_some_and(|rows| rows.iter().any(|r| r["id"] == "AKID-FOLLOWER"))
            },
        );
    }
}

#[test]
fn credentials_put_on_a_follower_is_relayed_to_the_leader() {
    run_credentials_put_on_a_follower_is_relayed_to_the_leader(env_seed(0xC086_0006));
}

#[test]
fn credentials_put_on_a_follower_is_relayed_to_the_leader_over_seeds() {
    for i in 0..5 {
        run_credentials_put_on_a_follower_is_relayed_to_the_leader(0xC086_0600 + i);
    }
}

// ---------------------------------------------------------------------------
// (7) admin_control_transfer_moves_leadership_to_the_named_node
// ---------------------------------------------------------------------------

fn run_control_transfer_moves_leadership_to_the_named_node(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let target = {
        let leader = cluster.control_leader_index() as u64;
        (0..cluster.node_count() as u64)
            .find(|&n| n != leader)
            .unwrap()
    };
    let body = format!(r#"{{"to":"n{target}"}}"#);

    // Retry the whole call, re-resolving the current leader each time —
    // mirrors the real-socket original's own issue #671/#688 retry
    // discipline against every retryable 409 this route can answer.
    let mut accepted = None;
    for _ in 0..20 {
        let leader = cluster.control_leader_index() as u64;
        let (status, resp) = cluster.admin(
            leader,
            "POST",
            "/admin/control/transfer",
            "",
            body.as_bytes(),
        );
        match status {
            200 => {
                accepted = Some(resp);
                break;
            }
            409 => cluster.run_for(Duration::from_millis(100)),
            other => {
                panic!("seed={seed}: transfer should be accepted or retryable: {other} {resp}")
            }
        }
    }
    let accepted = accepted
        .unwrap_or_else(|| panic!("seed={seed}: transfer was never accepted within budget"));
    assert_eq!(json(&accepted)["ok"], true, "seed={seed}: {accepted}");

    let new_leader = cluster.control_leader_index() as u64;
    assert_eq!(
        new_leader, target,
        "seed={seed}: control leadership never moved to node {target} (now {new_leader})"
    );
}

#[test]
fn control_transfer_moves_leadership_to_the_named_node() {
    run_control_transfer_moves_leadership_to_the_named_node(env_seed(0xC086_0007));
}

#[test]
fn control_transfer_moves_leadership_to_the_named_node_over_seeds() {
    for i in 0..5 {
        run_control_transfer_moves_leadership_to_the_named_node(0xC086_0700 + i);
    }
}

// ---------------------------------------------------------------------------
// (8) admin_control_transfer_on_a_follower_is_refused
// ---------------------------------------------------------------------------

fn run_control_transfer_on_a_follower_is_refused(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let leader = cluster.control_leader_index() as u64;
    let follower = (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .unwrap();
    let other = (0..cluster.node_count() as u64)
        .find(|&n| n != leader && n != follower)
        .unwrap();

    let body = format!(r#"{{"to":"n{other}"}}"#);
    let (status, resp) = cluster.admin(
        follower,
        "POST",
        "/admin/control/transfer",
        "",
        body.as_bytes(),
    );
    assert_ne!(
        status, 200,
        "seed={seed}: a follower should refuse the transfer: {resp}"
    );
}

#[test]
fn control_transfer_on_a_follower_is_refused() {
    run_control_transfer_on_a_follower_is_refused(env_seed(0xC086_0008));
}

#[test]
fn control_transfer_on_a_follower_is_refused_over_seeds() {
    for i in 0..5 {
        run_control_transfer_on_a_follower_is_refused(0xC086_0800 + i);
    }
}
