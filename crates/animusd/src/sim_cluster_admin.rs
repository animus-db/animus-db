//! `SimCluster`-driven deterministic siblings for the admin HTTP-JSON
//! interface's **observer** routes — every mutation-free `GET` (ADR 0061
//! rung H, C-08 PR 5) — plus a JSON-route analog of the metrics suite.
//!
//! **No `admin.rs`/`console.rs`/`dynamo.rs` dispatch change was needed** —
//! PR 2 already built [`SimCluster::admin`] (through [`crate::admin::
//! GenericAdminHost`], never `ClientCtx`'s own concrete `impl AdminHost`)
//! and every generic handler this module's scenarios reach; PR 3's own
//! [`super::sim_cluster_console`] already carries the shared `env_seed`/
//! `json`/wire helpers this module reuses rather than duplicating, per
//! this rung's own "put shared helpers where PR 3 put them" discipline.
//!
//! **Three genuinely new `sim_cluster.rs` fixture changes were needed**,
//! surfaced by this rung's own required gate, not part of the original
//! design pass:
//!
//! 1. [`SimCluster::put_raw`]/[`SimClusterHandle::put_raw`] (new, test-only,
//!    not a background-loop driver) — see that method's own doc for why
//!    [`SimCluster::put`]'s `item_key`-encoded keys make a literal,
//!    human-readable split boundary impractical to pick from a test,
//!    unlike the raw client `Put`/`ClientRequest::Put` the real-socket
//!    original actually used.
//! 2. **A real, previously-latent `SimCluster` fixture bug, found and
//!    fixed**: `SimCluster::new`/`SimCluster::grow` built every node's own
//!    **control** `RaftNode<SimEnv>` via the plain `RaftNode::start(env,
//!    ids, engine)` constructor, which defaults its own metrics sink to
//!    `env.metrics()` — `Env::metrics`'s trait-default method, which
//!    `SimEnv` does not override, unconditionally returns `MetricsHandle::
//!    noop()`: **one process-wide `static` shared sink** (`animus-env`'s
//!    own doc). Every node's control `RaftNode`, on EVERY `SimCluster`
//!    instance in the same test binary process, therefore shared the
//!    identical mutable `is_leader` gauge (`RaftNode`'s own role-
//!    transition code calls `metrics.set_leader(..)`, `animus-control/
//!    src/node.rs`): the first control raft anywhere to become leader
//!    stamped `is_leader: 1` onto `GET /admin/metrics` for every node,
//!    permanently (until some unrelated raft's own later role transition
//!    happened to flip it back), regardless of that node's real role. A
//!    summed *counter* under the same sharing is merely inflated (harmless
//!    unless a test asserts an exact value, and none did); the `is_leader`
//!    gauge was corrupted outright. This is why `/admin/raft`'s own
//!    `is_leader` (a direct `RaftCore` role check, `ClientCtx::
//!    control.is_leader()`, never through any metrics sink) was
//!    rock-stable across a diagnostic probe while `/admin/metrics`'s was
//!    not: two different signals, only one of them broken. Caught only
//!    because scenario (7) below is the first `SimCluster` test ever to
//!    read `/admin/metrics`'s `is_leader` field at all. **Fixed**: both
//!    node-construction sites now build one private `MetricsHandle::
//!    recording()` per node and call `RaftNode::start_with_metrics`
//!    instead — `DataRole::raftkv_metrics` reuses the SAME per-node
//!    handle, matching production's own "a combined node's control Raft
//!    and CP group record into the same sink" contract. See `docs/
//!    engineering-lessons.md`'s matching new entry for the general lesson,
//!    including why a hosted `RaftKvNode`'s own *internal* metrics have an
//!    identical, separate, deliberately-unfixed instance of this same
//!    default (harmless today only because nothing reads that handle).
//! 3. `admin.rs::system_table` reads `ctx.control_storage` (the per-node
//!    system-keyspace mirror engine ADR 0038's `DRIVER_APPLIED` apply task
//!    durably writes) — `SimCluster`'s own node construction always sets
//!    it `None`, so `GET /admin/system-table` unconditionally answers
//!    `{"available": false}` under this fixture regardless of what's
//!    seeded. **Not fixed** — building that apply-task mirror is a new
//!    background-loop driver, explicitly out of this PR's scope; see the
//!    "Kept `ProdEnv`" section below for `tests/system_table.rs`'s own
//!    disposition.
//!
//! ## Scenarios (seed-parameterized, `_over_seeds` at 5 seeds each)
//!
//! (1) [`run_credentials_view_never_serves_a_secret`] — `POST
//!     /admin/credentials` then `GET /admin/credentials`: the row commits,
//!     is visible redacted, and the raw secret never appears in either
//!     response body.
//! (2) [`run_raftkv_key_count_is_scoped_per_tablet_after_split`] — ten
//!     literal keys, an in-place split via `POST /admin/tablet/split` +
//!     [`SimCluster::drive_inplace_split_cutover`] (this fixture's own
//!     manual cutover driver, see that method's own doc), then `GET
//!     /admin/raftkv?exact=1` proves each child's own `key_count` is its
//!     own scoped subset, never the node's combined total (the regression
//!     this real-socket test itself guards).
//! (3) [`run_backups_view_reflects_the_catalog`] — `BeginBackup`/
//!     `RecordBackupTabletComplete`/`CompleteBackup` proposed directly
//!     ([`SimCluster::propose_meta`], mirroring `sim_cluster_backup_
//!     janitor.rs`'s own idiom — this test predates Train 1's wire
//!     surface and stays scoped to the admin observer alone, exactly like
//!     its real-socket original), `GET /admin/backups` tracking the row
//!     from empty through `CREATING` to `AVAILABLE`.
//! (4) [`run_backup_store_reports_reclaim_progress_and_leader_state`] — a
//!     backup driven directly to `Available` via `BeginBackup`/`Record
//!     BackupTabletComplete`/`CompleteBackup` (see this scenario's own doc
//!     for why: `dynamo.rs::create_backup`'s wire path needs the real
//!     per-tablet `backup_capture` driver to ever leave `CREATING`, and no
//!     `SimCluster` primitive spawns it — mirroring `sim_cluster_backup_
//!     janitor.rs`'s own `complete_a_backup`/`seed_backup_object` idiom
//!     exactly, since that module hit and solved the identical gap first),
//!     the sim's always-on backup janitor (`sim_cluster_backup_
//!     janitor.rs`'s own always-on spawn) and shared `SimSegmentStore`
//!     (`SegmentStoreHandle::S3` on every node) reclaiming the deleted
//!     backup's seeded objects, a follower staying honestly `idle`.
//!     **Narrower than its real-socket original**: this fixture never
//!     populates `AdminInfo.backup_store` (`sim_cluster.rs`'s own node
//!     construction always leaves it `None`), so `store.kind` is always
//!     JSON `null` here; this scenario asserts `leader`/`janitor`/
//!     `objects.count` only, never `store`. **Re-resolves the control
//!     leader fresh at every poll** ([`poll_admin_leader`]) rather than
//!     trusting one captured at the top — SimEnv's own network timing
//!     jitter means control leadership can genuinely move over the many
//!     seconds of virtual time a scenario with several `OP_BUDGET`-costing
//!     calls burns, and the backup janitor only ever runs on whichever
//!     node currently IS the leader.
//! (5) [`run_gc_reports_segment_janitor_progress_and_leader_state`] — a
//!     streamed table, one write, a seal on the table's own **data-plane**
//!     tablet leader ([`SimCluster::drive_stream_seal`] only acts on
//!     tablets its own argument node leads — the *control*-plane leader
//!     this scenario also needs for the janitor route is a different node
//!     in general), `POST /admin/data/drop-table`, then `GET /admin/gc`
//!     on the sim's always-on segment janitor (`sim_cluster_stream_
//!     janitor.rs`'s own always-on spawn) converging `orphans_deleted_
//!     total >= 1` (via [`poll_admin_leader`], the identical control-
//!     leadership-churn reasoning as scenario (4)) and the `stream_shards`
//!     catalog row actually gone, a follower staying honestly `idle`/
//!     `leader: false` throughout.
//! (6) [`run_ttl_tables_lists_a_ttl_enabled_table`] — **the "tables" half
//!     only** of `admin_ttl_reports_reaper_progress_and_ttl_tables`
//!     (per this rung's own brief): `UpdateTimeToLive`, then every node's
//!     own `GET /admin/ttl` converges to listing the table
//!     `{name, attribute, enabled: true}`, carries a `reaper` snapshot,
//!     and a numeric `leader_tablets`. The real-socket original's OTHER
//!     half — an expired item actually getting reaped — needs
//!     `animusd::ttl_reaper::ttl_reaper_loop`, which no `SimCluster`
//!     primitive drives (`SimCluster::new`/`restart` never spawn it,
//!     unlike the heartbeat/reconciler/backup-janitor/segment-janitor
//!     loops, all always-on since D4/rung G) — that whole test stays
//!     `ProdEnv`, kept in `tests/admin_endpoint.rs` with a reason comment.
//! (7) [`run_admin_metrics_surfaces_control_plane_counters`] — **not a
//!     literal conversion of `tests/metrics_endpoint.rs`**, whose own
//!     subject is the raw-text `GET /metrics` listener on the dynamo port
//!     (real HTTP framing this fixture has no listener for at all,
//!     entirely separate from the `AdminHost` route table) — instead, the
//!     identical underlying claim (a real control-plane election moves
//!     the same named counters, a follower reports `is_leader: 0`) proven
//!     through the JSON `GET /admin/metrics` route the generic dispatch
//!     does reach. **The scenario that found fixture bug 2 above**: scans
//!     every node in one pass and classifies each read by its OWN
//!     `is_leader` value, a defensive design kept even after the real bug
//!     was fixed (rather than trusting a leader/follower pair captured
//!     once at the top, in case control leadership genuinely does move
//!     over a long scenario's own virtual time, the same reasoning
//!     [`poll_admin_leader`] applies to scenarios (4)/(5) — never actually
//!     observed for the CONTROL plane specifically, only for the corrupted
//!     metrics gauge). `tests/metrics_endpoint.rs`'s own test stays
//!     `ProdEnv` whole, with a reason comment.
//!
//! ## Kept `ProdEnv`, in `tests/admin_endpoint.rs` (each with its own
//! reason comment; PR 6's own mutating-action tests are untouched here,
//! not even a comment, per this rung's own scope), plus `tests/system_
//! table.rs` (2 tests, kept whole) and `tests/metrics_endpoint.rs` (1
//! test, kept whole, see scenario (7) above)
//!
//! - `admin_config_reports_auth_state_and_never_serves_the_secret` — no
//!   `SimCluster` constructor knob configures `dynamo_auth`; every node's
//!   own `ClientCtx::dynamo_auth` is `None` in this fixture, unconditionally.
//! - `admin_raftkv_default_does_not_materialize_the_dataset` — this
//!   fixture's engine is `MemoryEngine`, not `LsmEngine`: there is no
//!   SSTable/block-read counter at all under `SimEnv`, so the cost
//!   differential this test measures (`storage_sstable_block_reads`
//!   between the cheap and `?exact=1` paths) cannot exist here.
//! - `admin_ttl_reports_reaper_progress_and_ttl_tables` — see scenario (6)
//!   above; kept whole (its reaper-progress half has no driver), the
//!   tables half gets its own new scenario instead of a literal trim.
//! - `admin_segment_store_reports_shard_placement_and_local_objects` —
//!   this fixture's segment store is `SegmentStoreHandle::S3` (a single
//!   shared object store), never `Cluster`; `AdminInfo.segment_store` is
//!   always `None` (JSON `null`) here, so `/admin/segment-store`'s
//!   `is_cluster` shard-placement rendering (gated on a `kind == "cluster"`
//!   display label this fixture never sets) would need faking a display
//!   string rather than exercising the real `ClusterSegmentStore`
//!   per-node replica-placement mechanism this test's own subject is — no
//!   primitive for that exists under `SimEnv`.
//! - `admin_segment_store_reports_null_shards_for_the_fs_kind` — the
//!   identical store-kind gap: this fixture's shared store is `S3`-kind,
//!   never `fs`-kind, and this test is genuinely `fs`-kind-specific.
//! - `admin_live_is_200_while_a_genuinely_leaderless_admin_health_is_503`
//!   — `SimCluster::new`'s own doc: "Settles the control group (drives
//!   past its first election) before returning" — there is no constructor
//!   for a node that boots without ever completing bootstrap, so a
//!   genuinely-leaderless-from-the-start node (issue #710's own subject)
//!   cannot be produced; crashing peers afterward tests leadership LOSS
//!   after a known leader, a materially different condition.
//! - `tests/system_table.rs`'s two tests, kept whole — **a genuine
//!   `SimCluster` capability gap this PR's own investigation surfaced,
//!   not merely a scenario-design difficulty**: `admin.rs::system_table`
//!   reads `ctx.control_storage` (the per-node system-keyspace mirror
//!   engine ADR 0038's `DRIVER_APPLIED` apply task durably writes), and
//!   this fixture's own node construction (`sim_cluster.rs`) always sets
//!   `control_storage: None` — no apply task mirrors `Metadata` into a
//!   system-keyspace `StorageEngine` under `SimEnv` at all, so `GET
//!   /admin/system-table` unconditionally answers `{"available": false}`
//!   here regardless of what's seeded. Building that apply-task mirror is
//!   a new background-loop driver, explicitly out of this PR's scope.
//!
//! **One real, previously-latent `SimCluster` fixture bug found and fixed
//! (the shared `MetricsHandle::noop()` sink — see this module's own top
//! doc, point 2)**; no bug in the widened dispatch code itself (`admin.rs`/
//! `dynamo.rs`) was found. Two other failures this rung's own required
//! gate surfaced were scenario/fixture-authoring issues, not product bugs,
//! and are accounted for in each scenario's own doc: a WIRE `CreateBackup`
//! can never reach `AVAILABLE` under this fixture (no `backup_capture`
//! driver — scenario (4) redesigned around direct `MetaCommand`s instead),
//! and `drive_stream_seal` silently no-ops when passed the wrong (control-
//! rather than data-plane) leader (scenario (5) fixed to resolve the
//! correct node). Every scenario passed at its pinned seed and every
//! `_over_seeds` seed once these were fixed.
//!
//! Replays (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <scenario name>`.

use std::time::Duration;

use animus_control::{MetaCommand, ProposeResult};
use animus_cp_data::backup as backup_codec;
use animus_tablet::TabletId;
use serde_json::Value;

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{
    control_leader_and_follower, create_table_via_wire, env_seed, json, leader_of_table,
    tablet_of_table,
};

fn accepted(r: ProposeResult) -> bool {
    matches!(r, ProposeResult::Accepted { .. })
}

/// Poll `GET path?query` on `node` until `pred` holds on the parsed JSON
/// body, or panic after `max_polls` calls. Each [`SimCluster::admin`] call
/// already burns a full `OP_BUDGET` (12s) of virtual time
/// (`SimCluster::spawn_and_capture`'s own doc), so this fixture's own
/// converged-or-timeout convention (root `CLAUDE.md`'s Testing rule) needs
/// no separate `run_for` — the loop's own iteration count already IS the
/// virtual-time budget.
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

/// [`poll_admin`]'s leader-tracking sibling: re-resolves the CURRENT
/// control leader (`SimCluster::control_leader_index`, cheap once a leader
/// already exists — no virtual time burned) on every single poll, rather
/// than a leader captured once before the loop started. Needed by any
/// route only the control leader ever advances (the backup/segment
/// janitors) — a scenario with several `OP_BUDGET`-costing calls before
/// the poll can genuinely outlive the leader term it started with, purely
/// from SimEnv's own network timing jitter, with no fault injected at
/// all; polling a now-stale "leader" node would then wait forever on a
/// route that only progresses for whoever leads *right now*.
fn poll_admin_leader(
    cluster: &mut SimCluster,
    path: &str,
    query: &str,
    seed: u64,
    max_polls: usize,
    mut pred: impl FnMut(&Value) -> bool,
) -> Value {
    for _ in 0..max_polls {
        let node = cluster.control_leader_index() as u64;
        let (status, body) = cluster.admin(node, "GET", path, query, &[]);
        assert_eq!(
            status, 200,
            "seed={seed}: GET {path}?{query} on the current leader (node {node}): {body}"
        );
        let v = json(&body);
        if pred(&v) {
            return v;
        }
    }
    panic!(
        "seed={seed}: GET {path}?{query} on the current control leader never converged \
         within {max_polls} polls ({max_polls} x OP_BUDGET of virtual time)"
    );
}

// ---------------------------------------------------------------------------
// (1) admin_credentials_view_never_serves_a_secret
// ---------------------------------------------------------------------------

const CRED_ACCESS_KEY_ID: &str = "AKIDEXAMPLE";
const CRED_SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

fn run_credentials_view_never_serves_a_secret(seed: u64) {
    let mut cluster = SimCluster::new(seed, 1, 1);

    let put_body =
        format!(r#"{{"id":"{CRED_ACCESS_KEY_ID}","secret":"{CRED_SECRET}","enabled":true}}"#);
    let (status, put_resp) =
        cluster.admin(0, "POST", "/admin/credentials", "", put_body.as_bytes());
    assert_eq!(status, 200, "seed={seed}: PutCredential: {put_resp}");
    let put_v = json(&put_resp);
    assert_eq!(put_v["id"], CRED_ACCESS_KEY_ID);
    assert_eq!(put_v["enabled"], true);
    assert_eq!(put_v["rotation"], Value::Null);

    let (status, view) = cluster.admin(0, "GET", "/admin/credentials", "", &[]);
    assert_eq!(status, 200, "seed={seed}: credentials view: {view}");
    let view_v = json(&view);
    let rows = view_v["credentials"].as_array().expect("credentials array");
    assert_eq!(rows.len(), 1, "seed={seed}: {view_v}");
    assert_eq!(rows[0]["id"], CRED_ACCESS_KEY_ID);

    assert!(
        !put_resp.contains(CRED_SECRET),
        "seed={seed}: PutCredential's own response must never echo the secret: {put_resp}"
    );
    assert!(
        !view.contains(CRED_SECRET),
        "seed={seed}: GET /admin/credentials must never serve a secret: {view}"
    );
}

#[test]
fn credentials_view_never_serves_a_secret() {
    run_credentials_view_never_serves_a_secret(env_seed(0xC085_0001));
}

#[test]
fn credentials_view_never_serves_a_secret_over_seeds() {
    for i in 0..5 {
        run_credentials_view_never_serves_a_secret(0xC085_0100 + i);
    }
}

// ---------------------------------------------------------------------------
// (2) admin_raftkv_key_count_is_scoped_per_tablet_after_split
// ---------------------------------------------------------------------------

fn run_raftkv_key_count_is_scoped_per_tablet_after_split(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let parent = cluster.create_table("kv");

    for i in 0..10u32 {
        let key = format!("key{i:02}");
        let value = format!("v{i}");
        cluster
            .put_raw(0, "kv", key.clone().into_bytes(), value.into_bytes())
            .unwrap_or_else(|e| panic!("seed={seed}: put_raw({key:?}) failed: {e}"));
    }

    let split_body = format!(r#"{{"tablet":{},"split_key":"key05"}}"#, parent.0);
    let (status, split) =
        cluster.admin(0, "POST", "/admin/tablet/split", "", split_body.as_bytes());
    assert_eq!(status, 200, "seed={seed}: split kickoff: {split}");

    // Drive the fork/cutover to completion — this fixture never spawns
    // `index_drain::change_consumer_loop` (see `SimCluster::
    // drive_inplace_split_cutover`'s own doc), so a caller polls it
    // alongside virtual time until convergence.
    const MAX_POLLS: usize = 40;
    let mut converged = false;
    for _ in 0..MAX_POLLS {
        for node in 0..3u64 {
            cluster.drive_inplace_split_cutover(node);
        }
        let meta = cluster.metadata(0);
        if !meta.tablets.contains_key(&parent) && meta.tablets.len() == 2 {
            converged = true;
            break;
        }
        cluster.run_for(Duration::from_millis(200));
    }
    assert!(
        converged,
        "seed={seed}: split never cut over to exactly two children: {:?}",
        cluster.metadata(0).tablets
    );

    let (status, raftkv) = cluster.admin(0, "GET", "/admin/raftkv", "exact=1", &[]);
    assert_eq!(status, 200, "seed={seed}: /admin/raftkv?exact=1: {raftkv}");
    let v = json(&raftkv);
    let groups = v["groups"].as_array().expect("groups array");
    assert_eq!(
        groups.len(),
        2,
        "seed={seed}: node 0 hosts both split halves: {groups:?}"
    );
    let counts: std::collections::BTreeMap<u64, u64> = groups
        .iter()
        .map(|g| {
            (
                g["tablet"].as_u64().expect("tablet id"),
                g["key_count"].as_u64().expect("key_count"),
            )
        })
        .collect();
    let total: u64 = counts.values().sum();
    assert_eq!(
        total, 10,
        "seed={seed}: combined key_count across both tablets equals the 10 written keys: \
         {counts:?}"
    );
    for (tablet, count) in &counts {
        assert!(
            *count < 10,
            "seed={seed}: tablet {tablet}'s key_count ({count}) must be its own scoped \
             subset, not the node's combined total: {counts:?}"
        );
    }
}

#[test]
fn raftkv_key_count_is_scoped_per_tablet_after_split() {
    run_raftkv_key_count_is_scoped_per_tablet_after_split(env_seed(0xC085_0002));
}

#[test]
fn raftkv_key_count_is_scoped_per_tablet_after_split_over_seeds() {
    for i in 0..5 {
        run_raftkv_key_count_is_scoped_per_tablet_after_split(0xC085_0200 + i);
    }
}

// ---------------------------------------------------------------------------
// (3) admin_backups_view_reflects_the_catalog
// ---------------------------------------------------------------------------

fn run_backups_view_reflects_the_catalog(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, ct) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"widgets","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable widgets: {ct}");

    let (status, empty) = cluster.admin(0, "GET", "/admin/backups", "", &[]);
    assert_eq!(status, 200, "seed={seed}: {empty}");
    let empty_v = json(&empty);
    assert_eq!(
        empty_v["backups"].as_array().unwrap().len(),
        0,
        "seed={seed}: no backups yet: {empty_v}"
    );

    // `BeginBackup`, proposed directly (this test predates the Train 1 wire
    // surface and stays scoped to the admin observer alone, mirroring its
    // real-socket original — `sim_cluster_dynamo_table_ops.rs`'s Streams
    // sibling and `sim_cluster_backup_janitor.rs` cover the wire path and
    // the janitor respectively).
    let begin = MetaCommand::BeginBackup {
        backup_id: "backup-1".to_string(),
        table: "widgets".to_string(),
        created_wall_ms: 1_000,
        backup_name: "backup".to_string(),
        pitr_base: false,
    };
    assert!(
        accepted(cluster.propose_meta(begin)),
        "seed={seed}: BeginBackup rejected by the control leader"
    );

    let backups = poll_admin(&mut cluster, 0, "/admin/backups", "", seed, 20, |v| {
        v["backups"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|b| b["backup_id"] == "backup-1"))
    });
    let tablet_ids: Vec<u64> = {
        let row = backups["backups"]
            .as_array()
            .unwrap()
            .iter()
            .find(|b| b["backup_id"] == "backup-1")
            .expect("backup-1 present");
        assert_eq!(row["table"], "widgets");
        assert_eq!(row["status"]["state"], "CREATING");
        assert_eq!(row["created_wall_ms"], 1000);
        let tablets = row["tablets"].as_array().expect("tablets array");
        assert!(
            !tablets.is_empty(),
            "seed={seed}: at least one pinned tablet: {row}"
        );
        assert!(
            tablets.iter().all(|t| t["reported"] == false),
            "seed={seed}: nothing reported yet: {row}"
        );
        tablets
            .iter()
            .map(|t| t["tablet"].as_str().unwrap().parse().unwrap())
            .collect()
    };

    for tablet in &tablet_ids {
        let record = MetaCommand::RecordBackupTabletComplete {
            backup_id: "backup-1".to_string(),
            tablet: TabletId(*tablet),
            cut_version: 42,
            bytes: 4_096,
        };
        assert!(
            accepted(cluster.propose_meta(record)),
            "seed={seed}: RecordBackupTabletComplete({tablet}) rejected"
        );
    }
    let complete = MetaCommand::CompleteBackup {
        backup_id: "backup-1".to_string(),
    };
    assert!(
        accepted(cluster.propose_meta(complete)),
        "seed={seed}: CompleteBackup rejected"
    );

    let expected_total = 4_096 * tablet_ids.len() as u64;
    let available = poll_admin(&mut cluster, 0, "/admin/backups", "", seed, 20, |v| {
        v["backups"].as_array().is_some_and(|rows| {
            rows.iter()
                .any(|b| b["backup_id"] == "backup-1" && b["status"]["state"] == "AVAILABLE")
        })
    });
    let row = available["backups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["backup_id"] == "backup-1")
        .unwrap();
    assert_eq!(row["total_bytes"].as_u64().unwrap(), expected_total);
    assert!(
        row["tablets"]
            .as_array()
            .unwrap()
            .iter()
            .all(|t| t["reported"] == true),
        "seed={seed}: every pinned tablet reported: {row}"
    );
}

#[test]
fn backups_view_reflects_the_catalog() {
    run_backups_view_reflects_the_catalog(env_seed(0xC085_0003));
}

#[test]
fn backups_view_reflects_the_catalog_over_seeds() {
    for i in 0..5 {
        run_backups_view_reflects_the_catalog(0xC085_0300 + i);
    }
}

// ---------------------------------------------------------------------------
// (4) admin_backup_store_reports_reclaim_progress_and_leader_state
// ---------------------------------------------------------------------------

fn run_backup_store_reports_reclaim_progress_and_leader_state(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (leader, follower) = control_leader_and_follower(&mut cluster);

    // ---- baseline: an unconfigured/no-backups-yet leader is honestly
    //      idle, and reports itself the control leader -------------------
    let (status, baseline) = cluster.admin(leader, "GET", "/admin/backup-store", "", &[]);
    assert_eq!(status, 200, "seed={seed}: {baseline}");
    let bv = json(&baseline);
    assert_eq!(
        bv["leader"], true,
        "seed={seed}: the leader reports itself: {bv}"
    );
    assert!(
        bv["objects"]["count"].as_u64().is_some(),
        "seed={seed}: objects.count is always present: {bv}"
    );
    let baseline_count = bv["objects"]["count"].as_u64().unwrap();

    // ---- a follower never runs the janitor, and says so ----------------
    let (status, follower_view) = cluster.admin(follower, "GET", "/admin/backup-store", "", &[]);
    assert_eq!(status, 200, "seed={seed}: {follower_view}");
    let fv = json(&follower_view);
    assert_eq!(fv["leader"], false, "seed={seed}: {fv}");
    assert_eq!(
        fv["janitor"]["phase"], "idle",
        "seed={seed}: a follower's own janitor never advances past idle: {fv}"
    );

    // ---- create a table, then drive a backup all the way to `Available`
    //      via direct `MetaCommand` proposals — `dynamo.rs::create_backup`'s
    //      wire path needs the real per-tablet `backup_capture` driver to
    //      ever leave `CREATING`, and no `SimCluster` primitive spawns it
    //      (unlike the always-on heartbeat/reconciler/backup-janitor/
    //      segment-janitor loops); this mirrors `sim_cluster_backup_
    //      janitor.rs`'s own `complete_a_backup` exactly, since that module
    //      hit and solved the identical gap first ---------------------------
    let (status, ct) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"widgets","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable: {ct}");
    let tablet = tablet_of_table(&cluster, "widgets");

    let backup_id = "arn:aws:dynamodb:animus:0:table/widgets/backup/nightly";
    assert!(
        accepted(cluster.propose_meta(MetaCommand::BeginBackup {
            backup_id: backup_id.to_owned(),
            table: "widgets".to_owned(),
            created_wall_ms: 1_000,
            backup_name: "nightly".to_owned(),
            pitr_base: false,
        })),
        "seed={seed}: BeginBackup rejected"
    );
    assert!(
        accepted(
            cluster.propose_meta(MetaCommand::RecordBackupTabletComplete {
                backup_id: backup_id.to_owned(),
                tablet,
                cut_version: 10,
                bytes: 4_096,
            })
        ),
        "seed={seed}: RecordBackupTabletComplete rejected"
    );
    assert!(
        accepted(cluster.propose_meta(MetaCommand::CompleteBackup {
            backup_id: backup_id.to_owned(),
        })),
        "seed={seed}: CompleteBackup rejected"
    );
    cluster.run_for(Duration::from_millis(200));

    // Seed the manifest + one data chunk directly into the shared
    // `SimSegmentStore` — mirroring `sim_cluster_backup_janitor.rs`'s own
    // scenario (a) exactly, since real object bytes must actually land in
    // the store before the janitor has anything to reclaim.
    let manifest_id = backup_codec::backup_manifest_object_id(backup_id);
    let chunk_id = backup_codec::backup_data_object_id(backup_id, tablet.0, 0);
    cluster.seed_backup_object(&manifest_id, b"manifest-bytes");
    cluster.seed_backup_object(&chunk_id, b"chunk-bytes");

    // ---- object count on the (current) leader has grown past the
    //      baseline — re-resolved fresh every poll, since control
    //      leadership can genuinely have moved by now (see this module's
    //      own doc) -------------------------------------------------------
    poll_admin_leader(&mut cluster, "/admin/backup-store", "", seed, 20, |v| {
        v["objects"]["count"].as_u64().unwrap_or(0) > baseline_count
    });

    // ---- delete it, then poll converged-or-timeout until the janitor has
    //      reclaimed it -------------------------------------------------
    assert!(
        accepted(cluster.propose_meta(MetaCommand::MarkBackupDeleted {
            backup_id: backup_id.to_owned(),
        })),
        "seed={seed}: MarkBackupDeleted rejected"
    );

    poll_admin_leader(&mut cluster, "/admin/backup-store", "", seed, 30, |v| {
        let count = v["objects"]["count"].as_u64().unwrap_or(u64::MAX);
        let reclaimed = v["janitor"]["objects_reclaimed"].as_u64().unwrap_or(0);
        count <= baseline_count && reclaimed > 0
    });
}

#[test]
fn backup_store_reports_reclaim_progress_and_leader_state() {
    run_backup_store_reports_reclaim_progress_and_leader_state(env_seed(0xC085_0004));
}

#[test]
fn backup_store_reports_reclaim_progress_and_leader_state_over_seeds() {
    for i in 0..5 {
        run_backup_store_reports_reclaim_progress_and_leader_state(0xC085_0400 + i);
    }
}

// ---------------------------------------------------------------------------
// (5) admin_gc_reports_segment_janitor_progress_and_leader_state
// ---------------------------------------------------------------------------

fn run_gc_reports_segment_janitor_progress_and_leader_state(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (leader, follower) = control_leader_and_follower(&mut cluster);

    // ---- baseline: an unconfigured/no-drops-yet leader is honestly idle,
    //      and reports itself the control leader -------------------------
    let (status, baseline) = cluster.admin(leader, "GET", "/admin/gc", "", &[]);
    assert_eq!(status, 200, "seed={seed}: {baseline}");
    let bv = json(&baseline);
    assert_eq!(
        bv["leader"], true,
        "seed={seed}: the leader reports itself: {bv}"
    );
    assert!(
        bv["janitor"]["phase"].is_string(),
        "seed={seed}: janitor.phase is always present: {bv}"
    );

    // ---- a follower never runs the janitor, and says so ----------------
    let (status, follower_view) = cluster.admin(follower, "GET", "/admin/gc", "", &[]);
    assert_eq!(status, 200, "seed={seed}: {follower_view}");
    let fv = json(&follower_view);
    assert_eq!(fv["leader"], false, "seed={seed}: {fv}");
    assert_eq!(
        fv["janitor"]["phase"], "idle",
        "seed={seed}: a follower's own janitor never advances past idle: {fv}"
    );

    // ---- create a streamed table, write one item, and seal it on the
    //      table's own DATA-plane tablet leader — `SimCluster::drive_
    //      stream_seal` only acts on tablets its own argument node leads,
    //      a generally different node from the CONTROL-plane `leader`
    //      above (see this module's own doc) -----------------------------
    let (status, ct) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"t","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable: {ct}");
    let (status, put) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"t","Item":{"id":{"S":"p1"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: PutItem: {put}");
    let data_leader = leader_of_table(&cluster, "t");
    cluster.drive_stream_seal(data_leader);
    assert!(
        !cluster.metadata(0).stream_shards.is_empty(),
        "seed={seed}: test premise: a sealed row exists before the drop"
    );

    // ---- drop the table: the janitor's own drop-table rule reclaims its
    //      stream-shard row(s) immediately, regardless of retention ------
    let (status, drop_body) =
        cluster.admin(0, "POST", "/admin/data/drop-table", "", br#"{"table":"t"}"#);
    assert_eq!(status, 200, "seed={seed}: drop-table: {drop_body}");

    // Re-resolved fresh every poll — the identical control-leadership-
    // churn reasoning as scenario (4)'s own doc.
    poll_admin_leader(&mut cluster, "/admin/gc", "", seed, 40, |v| {
        v["janitor"]["orphans_deleted_total"].as_u64().unwrap_or(0) >= 1
    });

    // ---- the dropped table's own catalog rows are actually gone --------
    let mut meta = cluster.metadata(0);
    let mut extra = 0;
    while !meta.stream_shards.is_empty() && extra < 10 {
        cluster.run_for(Duration::from_millis(500));
        meta = cluster.metadata(0);
        extra += 1;
    }
    assert!(
        meta.stream_shards.is_empty(),
        "seed={seed}: stream-shard catalog rows were never cleared after the drop: {:?}",
        meta.stream_shards
    );

    // ---- a follower still reports leader: false throughout — re-resolved
    //      fresh, not the pair captured at the top (see this module's own
    //      doc) ------------------------------------------------------------
    let (_, follower_now) = control_leader_and_follower(&mut cluster);
    let (status, follower_after) = cluster.admin(follower_now, "GET", "/admin/gc", "", &[]);
    assert_eq!(status, 200, "seed={seed}: {follower_after}");
    let fv2 = json(&follower_after);
    assert_eq!(
        fv2["leader"], false,
        "seed={seed}: a follower still reports leader: false after the drop: {fv2}"
    );
}

#[test]
fn gc_reports_segment_janitor_progress_and_leader_state() {
    run_gc_reports_segment_janitor_progress_and_leader_state(env_seed(0xC085_0005));
}

#[test]
fn gc_reports_segment_janitor_progress_and_leader_state_over_seeds() {
    for i in 0..5 {
        run_gc_reports_segment_janitor_progress_and_leader_state(0xC085_0500 + i);
    }
}

// ---------------------------------------------------------------------------
// (6) admin_ttl_reports_reaper_progress_and_ttl_tables — "tables" half only
// ---------------------------------------------------------------------------

fn run_ttl_tables_lists_a_ttl_enabled_table(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, ct) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"widgets","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable: {ct}");

    let (status, upd) = cluster.dynamo(
        0,
        "DynamoDB_20120810.UpdateTimeToLive",
        br#"{"TableName":"widgets",
            "TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: UpdateTimeToLive: {upd}");

    // Every node's own catalog view converges to show `widgets` as
    // TTL-enabled, carries a `reaper` snapshot, and a numeric
    // `leader_tablets` — the reaper never actually running under `SimEnv`
    // means `reaper.deleted_total` stays honestly 0 throughout, which this
    // scenario deliberately never asserts on (see this module's own doc).
    for node in 0..cluster.node_count() as u64 {
        let v = poll_admin(&mut cluster, node, "/admin/ttl", "", seed, 20, |v| {
            v.get("reaper").is_some()
                && v["tables"].as_array().is_some_and(|tables| {
                    tables.iter().any(|t| {
                        t["name"] == "widgets"
                            && t["attribute"] == "expiresAt"
                            && t["enabled"] == true
                    })
                })
        });
        assert!(
            v["leader_tablets"].as_u64().is_some(),
            "seed={seed}: node {node} carries a numeric leader_tablets: {v}"
        );
    }
}

#[test]
fn ttl_tables_lists_a_ttl_enabled_table() {
    run_ttl_tables_lists_a_ttl_enabled_table(env_seed(0xC085_0006));
}

#[test]
fn ttl_tables_lists_a_ttl_enabled_table_over_seeds() {
    for i in 0..5 {
        run_ttl_tables_lists_a_ttl_enabled_table(0xC085_0600 + i);
    }
}

// ---------------------------------------------------------------------------
// (7) admin_metrics_surfaces_control_plane_counters
// ---------------------------------------------------------------------------

fn run_admin_metrics_surfaces_control_plane_counters(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    // Scan every node in ONE pass and classify each read by its OWN
    // `is_leader` value, rather than trusting a leader/follower identity
    // captured earlier — this module's own doc explains why (SimEnv's
    // network timing jitter can genuinely move control leadership between
    // two `OP_BUDGET`-costing calls with no fault injected at all).
    let mut leader_view: Option<Value> = None;
    let mut saw_follower = false;
    for node in 0..cluster.node_count() as u64 {
        let (status, body) = cluster.admin(node, "GET", "/admin/metrics", "", &[]);
        assert_eq!(
            status, 200,
            "seed={seed}: /admin/metrics on node {node}: {body}"
        );
        let v = json(&body);
        match v["is_leader"].as_i64() {
            Some(1) if leader_view.is_none() => leader_view = Some(v),
            Some(0) => saw_follower = true,
            _ => {}
        }
    }
    let v = leader_view.unwrap_or_else(|| {
        panic!("seed={seed}: no node reported is_leader: 1 across a full node scan")
    });

    // A real election happened during `SimCluster::new`'s own bootstrap: a
    // candidate started and won an election, and the leader has sent
    // `AppendEntries` (heartbeats + the bootstrap replication).
    for name in [
        "control_elections_started",
        "control_elections_won",
        "control_append_entries_sent",
    ] {
        let c = v["counters"][name]
            .as_u64()
            .unwrap_or_else(|| panic!("seed={seed}: {name} missing: {v}"));
        assert!(c >= 1, "seed={seed}: expected >=1 {name}, got {c}: {v}");
    }

    // Every known control counter name is present (closed enum → stable
    // surface) — `control_is_leader` itself is the separate top-level
    // `is_leader` field, never a `counters` entry (`metrics_json`'s own
    // doc).
    for name in [
        "control_elections_started",
        "control_elections_won",
        "control_append_entries_sent",
        "control_append_entries_rejected",
        "control_snapshot_installs",
        "control_failure_detector_down",
        "control_failure_detector_up",
    ] {
        assert!(
            v["counters"].get(name).is_some(),
            "seed={seed}: metric `{name}` missing from /admin/metrics: {v}"
        );
    }

    // Every node serves its own aggregated snapshot, not only the leader.
    assert!(
        saw_follower,
        "seed={seed}: no node in the same scan reported is_leader: 0"
    );
}

#[test]
fn admin_metrics_surfaces_control_plane_counters() {
    run_admin_metrics_surfaces_control_plane_counters(env_seed(0xC085_0007));
}

#[test]
fn admin_metrics_surfaces_control_plane_counters_over_seeds() {
    for i in 0..5 {
        run_admin_metrics_surfaces_control_plane_counters(0xC085_0700 + i);
    }
}
