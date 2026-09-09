//! `SimCluster`-driven deterministic siblings for the web dashboard's
//! render-only follow-up suite (ADR 0061 rung H, C-08 PR 7) — 12 of
//! `tests/dashboard_endpoint.rs`'s 16 tests (`docs/roadmap.md`'s U-01/
//! U-02/U-04/U-05/U-07 dashboard follow-ups). The remaining 4 stay
//! `ProdEnv` whole, each with its own reason comment in that file — see
//! the "Kept `ProdEnv`" list below.
//!
//! **The render-marker half of every converted test reads the served
//! assets' own compile-time constants directly** — `crate::dashboard::
//! {HTML, CORE_JS, OVERVIEW_JS, TABLETS_JS, TXNS_JS, STORAGE_JS,
//! BACKUPS_JS, BROWSER_JS, NODE_JS}` — rather than fetching them over a
//! socket. `animus_node::admin`'s own module doc says the dashboard's
//! static assets (the shell HTML, every per-view `.js`/`.css` file) are
//! served from `animusd`'s own `handle_conn` **before** its `AdminHost`
//! dispatch table is ever reached (`crate::admin::static_asset`/
//! `is_ui_path`); [`SimCluster::admin`] calls straight into `animus_node::
//! admin::dispatch` (through [`crate::admin::GenericAdminHost`]), never
//! `handle_conn`, so it cannot fetch `/admin/ui/*` at all. Since every one
//! of these assets is a plain `include_str!` compile-time constant with no
//! request-time computation whatsoever, reading the constant directly is
//! not a narrower proof than fetching it over a socket would have been —
//! the exact same bytes, minus a serving mechanism this rung has no reason
//! to reproduce (see this crate's own new `docs/engineering-lessons.md`
//! entry: a real-socket test whose assertion is against a byte-identical
//! compile-time-constant asset does not need its sim fixture to reproduce
//! the serving mechanism at all). Each converted test's own **live JSON
//! round trip** (`/admin/txns`, `/admin/backups`, `/admin/status`,
//! `/admin/control/members`, the four U-07 observability routes, and the
//! "route exists" probes for U-05's action families) goes through
//! [`SimCluster::admin`] instead, reusing [`super::sim_cluster_console`]'s
//! shared helpers (`env_seed`/`json`/`create_table_via_wire`/`control_
//! leader_and_follower`/`non_leader_of_table`) exactly as `sim_cluster_
//! console_stream.rs`/`sim_cluster_console_table_config.rs`/`sim_cluster_
//! admin.rs` already do.
//!
//! **No `admin.rs`/`console.rs`/`dynamo.rs`/`sim_cluster.rs` change was
//! needed for this PR** — every generic handler these scenarios reach was
//! already built by PR 2 ([`crate::admin::GenericAdminHost`]) and widened
//! by PR 2/2a/2b/3a (`update_time_to_live`/`create_backup`/`delete_
//! backup`'s widening, `dispatch_table_op`'s `CreateTable`-with-GSI/LSI/
//! stream and `UpdateTable`-throughput arms): `dashboard_u04_ttl_row`'s
//! and `dashboard_u04_create_table_form`'s own `CreateTable`/
//! `UpdateTimeToLive` calls, issued here through the identical `/admin/
//! data/dynamo` proxy the real-socket originals used, reach the same
//! already-generic `create_table`/`update_time_to_live` this rung's own
//! earlier PRs proved out — confirmed by direct inspection of `dispatch_
//! table_op`'s `CreateTable` arm (`dynamo.rs`), which accepts a declared
//! GSI/LSI/stream unconditionally since ADR 0061 rung G (C-07 PR 2).
//!
//! ## Issuing discipline
//!
//! A route with no table/tablet concept (`/admin/txns`, `/admin/backups`,
//! `/admin/restores`, `/admin/metrics/history`, the four U-07 cards,
//! `/admin/control/members`, and the U-05 action-existence probes) is
//! issued from a **control follower**
//! ([`super::sim_cluster_console::control_leader_and_follower`]) — proving
//! each one also serves correctly off a non-leader `ClientCtx`, mirroring
//! `sim_cluster_console.rs`'s own "every control-plane call from a
//! follower" convention. A table-scoped write/read (`dashboard_u04_ttl_
//! row`'s and `dashboard_u04_create_table_form`'s own `UpdateTimeToLive` +
//! `/admin/status` re-read, once a table exists) is issued from that
//! table's own tablet **non-leader**
//! ([`super::sim_cluster_console::non_leader_of_table`]) instead, mirroring
//! every other `sim_cluster_*` module's convention; the `CreateTable` call
//! that precedes it (a schema-catalog mutation, no tablet yet) stays on
//! the control follower.
//!
//! ## Scenarios (seed-parameterized, `_over_seeds` at 5 seeds each, except
//! `u05_tablet_actions` — a pure marker check against the served
//! `TABLETS_JS` constant, touching no `SimCluster` at all, so it carries
//! no seed)
//!
//! (1) [`run_u01_render_only_fixes`] — `dashboard_u01_render_only_fixes`:
//!     the Transactions tab's markers plus a live `/admin/txns`; the
//!     tablet-detail Raft-field markers; the `believes_alive` badge
//!     marker; the sparkline markers plus a live `/admin/metrics/history`;
//!     and the 16-`EntityKind` `SYSTEM_TABLE_KINDS` round trip (pure,
//!     no live call).
//! (2) [`run_u02_backups_tab`] — `dashboard_u02_backups_tab`: the Backups
//!     tab's shell/script markers, the four gated actions' real op/field
//!     names, role gating, and live `/admin/backups`/`/admin/restores`.
//! (3) [`run_u07_backup_store_card`] — `dashboard_u07_backup_store_card`:
//!     the Backup store card's markers plus a live `/admin/backup-store`.
//! (4) [`run_u07_ttl_reaper_card`] — `dashboard_u07_ttl_reaper_card`: the
//!     TTL reaper card's markers plus a live `/admin/ttl`.
//! (5) [`run_u07_gc_card`] — `dashboard_u07_gc_card`: the GC card's
//!     markers plus a live `/admin/gc`.
//! (6) [`run_u07_segment_store_card`] — `dashboard_u07_segment_store_
//!     card`: the Segment store card's markers plus a live `/admin/
//!     segment-store`.
//! (7) [`run_u04_ttl_row`] — `dashboard_u04_ttl_row`: the `#br-dy-ttl`
//!     row's markers, then a real `CreateTable` + enable/disable
//!     `UpdateTimeToLive` round trip through the admin dynamo proxy,
//!     checked against `/admin/status`.
//! (8) [`run_u04_create_table_form`] — `dashboard_u04_create_table_form`:
//!     the create-table form's field/validation markers, then a real
//!     `CreateTable` (GSI + LSI + stream) + `UpdateTimeToLive` sequence,
//!     checked against `/admin/status`.
//! (9) [`run_u05_control_members_panel`] — `dashboard_u05_control_
//!     members_panel`: the Node tab's control-members panel markers plus
//!     a converged-or-timeout live `/admin/control/members`, issued from a
//!     control follower.
//! (10) [`u05_tablet_actions`] — `dashboard_u05_tablet_actions`: pure
//!     button-id/route/`window.confirm` markers against `TABLETS_JS`; no
//!     `SimCluster` at all (see above).
//! (11) [`run_u05_node_actions`] — `dashboard_u05_node_actions`: the Node
//!     tab's Drain/Remove/Add-member button markers, then a live
//!     "route exists" (not-404) probe of all three routes.
//! (12) [`run_u05_control_member_actions`] — `dashboard_u05_control_
//!     member_actions`: the control-members panel's Transfer/Remove/Add
//!     action markers, then a live "route exists" probe of all three
//!     routes.
//!
//! ## Kept `ProdEnv`, in `tests/dashboard_endpoint.rs` (each with its own
//! reason comment)
//!
//! - `dashboard_serves_spa_with_cors_and_peers` — real HTTP framing
//!   (status line, `Content-Type`, CORS headers, `OPTIONS` preflight)
//!   [`SimCluster::admin`]/`console` cannot reproduce (they build a bare
//!   `(status, body)` pair, no framing at all).
//! - `dashboard_role_gating_split_deployment` — kept at the time of this
//!   PR for lack of a node-role concept; **corrected by ADR 0061 rung L,
//!   C-12 PR 4c**, which converts its JSON/asset-marker half using
//!   `SimCluster`'s own per-node `NodeRole` (added in C-12 PRs 2/3) —
//!   see this file's own PR 4c section, below, for what stays real-socket
//!   (the literal shell/HTTP-framing check on both roles' admin ports).
//! - `control_node_streams_read_path_is_ground_truth` — kept at the time
//!   of this PR for the identical stale role-split reason; **fully
//!   converted by C-12 PR 4c** (no real HTTP framing at all in this test —
//!   every assertion is DynamoDB-wire/admin-JSON — so it needed no
//!   real-socket residual, and PR 4c removes it from `tests/dashboard_
//!   endpoint.rs` outright). Its own two documented backend gaps (a
//!   control-only node's `GetRecords`/open-tail stall) were never
//!   exercised by this test to begin with (its own doc names them as
//!   deliberately NOT called) — nothing is lost by the conversion.
//! - `dashboard_u05_lineage_panel` — `GET /admin/system-table` reads
//!   `ctx.control_storage` (the per-node system-keyspace mirror engine ADR
//!   0038's `DRIVER_APPLIED` apply task durably writes), always `None`
//!   under `SimCluster` — the identical gap `sim_cluster_admin.rs`'s own
//!   `tests/system_table.rs` disposition already documents — so `GET
//!   /admin/system-table` unconditionally answers `{"available": false}`
//!   here regardless of what's seeded; kept whole.
//!
//! ## ADR 0061 rung L, C-12 PR 4c: this file's two role-named tests
//!
//! `SimCluster` gained per-node [`NodeRole`](crate::config::NodeRole) in
//! C-12 PRs 2/3 — the "no node-role concept" reason above no longer holds,
//! corrected in place rather than left stale. Both new scenarios use
//! `SimCluster::new_with_roles` with a `[NodeRole::Control, NodeRole::
//! Data]` cluster, mirroring the real tests' own `support::bring_up_
//! split(1, 1, ..)` shape.
//!
//! (13) [`run_dashboard_role_gating_split_deployment`] —
//!     `dashboard_role_gating_split_deployment`'s JSON/asset-marker half:
//!     the shell/`dashboard_node.js` render markers and the `ROLE_TABS`/
//!     `applyRoleGating` gating logic itself, read from the served assets'
//!     own compile-time constants directly (this file's own top-of-module
//!     note on why that's not a narrower proof); `/admin/config`'s `role`
//!     field differing across the split (`"control"` vs `"data"` — every
//!     other per-role field `dashboard_role_gating_split_deployment`
//!     checks, `backup_store`/`segment_store`/`quiesce_after_ms`/
//!     `auth_enabled`, is `null` for **every** node regardless of role
//!     under `SimCluster`'s own `AdminInfo` construction, a pre-existing
//!     fixture limitation unrelated to this rung — not reproduced here);
//!     and `/admin/raft`'s `control_mirror` converging on the data-only
//!     node while the control-bearing node's own mirror stays honestly
//!     "never synced". What stays real-socket-only, kept whole in
//!     `tests/dashboard_endpoint.rs`: the literal `GET /` shell/JS-asset
//!     HTTP framing check on both roles' admin ports (`SimCluster::admin`
//!     builds a bare `(status, body)` pair, no framing at all), and
//!     `/admin/peers`'s own per-node `role` field (`AdminInfo.peers` is
//!     always an empty map under `SimCluster`, regardless of role — the
//!     identical `backup_store`/`segment_store`-style fixture limitation,
//!     not something this rung's role split could close).
//! (14) [`run_control_node_streams_read_path_is_ground_truth`] —
//!     `control_node_streams_read_path_is_ground_truth`, in full: an open
//!     stream and a force-sealed one (via `UpdateTable{StreamEnabled:
//!     false}`, F12-b's synchronous final seal — no periodic seal loop or
//!     `SimCluster::drive_stream_seal` needed) created over the wire from
//!     the DATA node, then `/admin/status`'s converged replicated catalog
//!     and `ListStreams`/`DescribeStream` through the `/admin/data/dynamo`
//!     proxy read from the CONTROL-ONLY node's own admin port — every
//!     assertion in the original is DynamoDB-wire/admin-JSON, no HTTP
//!     framing at all, so this converts whole and is removed from `tests/
//!     dashboard_endpoint.rs` outright.
//!
//! Replays (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <scenario name>`.

use std::time::Duration;

use animus_env::nid;
use serde_json::Value;

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{
    control_leader_and_follower, env_seed, json, non_leader_of_table,
};
use crate::config::NodeRole;
use crate::dashboard::{
    BACKUPS_JS, BROWSER_JS, CORE_JS, HTML, NODE_JS, OVERVIEW_JS, STORAGE_JS, TABLETS_JS, TXNS_JS,
};

// ---------------------------------------------------------------------------
// (1) dashboard_u01_render_only_fixes
// ---------------------------------------------------------------------------

fn run_u01_render_only_fixes(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_, follower) = control_leader_and_follower(&mut cluster);

    // ---- 1. Transactions tab over /admin/txns (CpTxnView) --------------
    assert!(
        HTML.contains(r#"data-tab="txns""#) && HTML.contains(r#"<section id="txns""#),
        "the shell carries the Transactions nav link and section"
    );
    assert!(
        HTML.contains("dashboard_txns.js"),
        "the shell references the Transactions view's script asset"
    );
    assert!(
        TXNS_JS.contains("function renderTxns") && TXNS_JS.contains("txnViewsByTablet"),
        "dashboard_txns.js renders the per-hosted-tablet transaction-tracker view"
    );
    assert!(
        CORE_JS.contains("/admin/txns") && CORE_JS.contains("function txnViewsByTablet"),
        "dashboard_core.js fans out /admin/txns and merges it cluster-wide"
    );
    assert!(
        CORE_JS.contains(
            r#"control: ["overview", "placement", "tablets", "txns", "browser", "streams", "storage", "backups"]"#
        ) && CORE_JS.contains(
            r#"combined: ["overview", "placement", "tablets", "txns", "browser", "streams", "storage", "backups", "node"]"#
        ),
        "the Transactions tab is role-gated exactly like Tablets (ROLE_TABS)"
    );
    let (status, txns_body) = cluster.admin(follower, "GET", "/admin/txns", "", &[]);
    assert_eq!(status, 200, "seed={seed}: GET /admin/txns: {txns_body}");
    assert!(
        json(&txns_body).get("groups").is_some(),
        "seed={seed}: the CpTxnView list is under \"groups\": {txns_body}"
    );

    // ---- 2. Full per-group Raft detail in renderTabletDetail -----------
    for field in [
        "commit_index",
        "durable_index",
        "snapshot_index",
        "log_len",
        "g.voters",
        "g.learners",
    ] {
        assert!(
            TABLETS_JS.contains(field),
            "renderTabletDetail renders CpRaftView's own {field}"
        );
    }

    // ---- 3. believes_alive badge in renderOverview ----------------------
    assert!(
        OVERVIEW_JS.contains("believes_alive") && OVERVIEW_JS.contains("believesAlive"),
        "renderOverview surfaces the control leader's own believes_alive verdict per member"
    );

    // ---- 4. Sparklines from /admin/metrics/history ----------------------
    assert!(
        CORE_JS.contains("function sparkline"),
        "dashboard_core.js defines a shared sparkline() component"
    );
    assert!(
        CORE_JS.contains("/admin/metrics/history"),
        "dashboard_core.js fetches this node's own metrics-history ring"
    );
    assert!(
        OVERVIEW_JS.contains("sparkline("),
        "renderOverview renders sparklines"
    );
    for counter in [
        "cp_read_barriers_served",
        "cp_read_barriers_timed_out",
        "cp_eventual_reads_local",
        "cp_eventual_reads_forwarded",
        "cp_eventual_reads_fell_back",
        "cp_uncertainty_restarts",
    ] {
        assert!(
            OVERVIEW_JS.contains(counter),
            "the Overview read-path sparklines chart {counter}"
        );
    }
    let (status, history_body) = cluster.admin(follower, "GET", "/admin/metrics/history", "", &[]);
    assert_eq!(
        status, 200,
        "seed={seed}: GET /admin/metrics/history: {history_body}"
    );
    assert!(
        json(&history_body).get("samples").is_some(),
        "seed={seed}: the ring buffer is served under \"samples\": {history_body}"
    );

    // ---- 5. SYSTEM_TABLE_KINDS extended to all 16 EntityKind variants --
    use animus_control::syskv::EntityKind;
    let expected_kinds: [&str; 16] = [
        EntityKind::Tablet.as_str(),
        EntityKind::Member.as_str(),
        EntityKind::Schema.as_str(),
        EntityKind::Policy.as_str(),
        EntityKind::NodeAddrs.as_str(),
        EntityKind::Counter.as_str(),
        EntityKind::CpMemberAddr.as_str(),
        EntityKind::StreamShard.as_str(),
        EntityKind::IndexBackfill.as_str(),
        EntityKind::SplitLineage.as_str(),
        EntityKind::SplitPlacing.as_str(),
        EntityKind::Backup.as_str(),
        EntityKind::BackupProgress.as_str(),
        EntityKind::Restore.as_str(),
        EntityKind::PitrSegment.as_str(),
        EntityKind::PitrBaseBackup.as_str(),
    ];
    for kind in expected_kinds {
        assert!(
            STORAGE_JS.contains(&format!("[\"{kind}\",")),
            "SYSTEM_TABLE_KINDS lists the real EntityKind segment {kind:?}"
        );
        assert!(
            EntityKind::from_segment(kind.as_bytes()).is_some(),
            "{kind:?} round-trips through EntityKind::from_segment"
        );
    }
    assert!(
        !STORAGE_JS.contains("[\"keyspace\","),
        "the stray [\"keyspace\", ...] dropdown entry is dropped, not carried forward"
    );
}

#[test]
fn u01_render_only_fixes() {
    run_u01_render_only_fixes(env_seed(0xC087_0001));
}

#[test]
fn u01_render_only_fixes_over_seeds() {
    for i in 0..5 {
        run_u01_render_only_fixes(0xC087_0100 + i);
    }
}

// ---------------------------------------------------------------------------
// (2) dashboard_u02_backups_tab
// ---------------------------------------------------------------------------

fn run_u02_backups_tab(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_, follower) = control_leader_and_follower(&mut cluster);

    assert!(
        HTML.contains(r#"data-tab="backups""#) && HTML.contains(r#"<section id="backups""#),
        "the shell carries the Backups nav link and section"
    );
    assert!(
        HTML.contains("dashboard_backups.js"),
        "the shell references the Backups view's script asset"
    );
    assert!(
        HTML.contains(r#"id="bk-create-table""#)
            && HTML.contains(r#"id="bk-list-body""#)
            && HTML.contains(r#"id="bk-pitr-body""#)
            && HTML.contains(r#"id="bk-restores-body""#),
        "the shell carries the Create-backup form, the backup list, the \
         per-table PITR toggle list, and the restores list"
    );

    assert!(
        BACKUPS_JS.contains("function renderBackups"),
        "dashboard_backups.js renders the backup/restore/PITR catalogs"
    );
    for (op, field) in [
        ("CreateBackup", "BackupName"),
        ("DeleteBackup", "BackupArn"),
        ("RestoreTableFromBackup", "TargetTableName"),
        ("UpdateContinuousBackups", "PointInTimeRecoveryEnabled"),
    ] {
        assert!(
            BACKUPS_JS.contains(op) && BACKUPS_JS.contains(field),
            "dashboard_backups.js posts {op} with its real payload field {field}"
        );
    }
    assert!(
        BACKUPS_JS.contains("PointInTimeRecoverySpecification"),
        "UpdateContinuousBackups nests PointInTimeRecoveryEnabled under \
         PointInTimeRecoverySpecification, matching the wire decoder"
    );
    assert_eq!(
        BACKUPS_JS.matches("if (!window.confirm(").count(),
        4,
        "each of the four actions is gated behind its own window.confirm"
    );
    assert!(
        BACKUPS_JS.contains("/admin/data/dynamo"),
        "every action posts through the existing dashboard dynamo proxy"
    );
    assert!(
        BACKUPS_JS.contains("dynamoTables()"),
        "the Create-backup table picker and the PITR table list reuse the \
         Data Browser's own table source"
    );

    assert!(
        CORE_JS.contains("/admin/backups") && CORE_JS.contains("/admin/restores"),
        "dashboard_core.js fetches both catalogs once against SEED"
    );
    assert!(
        CORE_JS.contains(
            r#"control: ["overview", "placement", "tablets", "txns", "browser", "streams", "storage", "backups"]"#
        ) && !CORE_JS.contains(r#"data: ["node", "browser", "streams", "backups"]"#),
        "Backups is role-gated to control + combined, absent from the data role's own tab list"
    );

    let (status, backups_body) = cluster.admin(follower, "GET", "/admin/backups", "", &[]);
    assert_eq!(
        status, 200,
        "seed={seed}: GET /admin/backups: {backups_body}"
    );
    assert!(
        json(&backups_body).get("backups").is_some(),
        "seed={seed}: the catalog is served under \"backups\": {backups_body}"
    );
    let (status, restores_body) = cluster.admin(follower, "GET", "/admin/restores", "", &[]);
    assert_eq!(
        status, 200,
        "seed={seed}: GET /admin/restores: {restores_body}"
    );
    assert!(
        json(&restores_body).get("restores").is_some(),
        "seed={seed}: the catalog is served under \"restores\": {restores_body}"
    );
}

#[test]
fn u02_backups_tab() {
    run_u02_backups_tab(env_seed(0xC087_0002));
}

#[test]
fn u02_backups_tab_over_seeds() {
    for i in 0..5 {
        run_u02_backups_tab(0xC087_0200 + i);
    }
}

// ---------------------------------------------------------------------------
// (3) dashboard_u07_backup_store_card
// ---------------------------------------------------------------------------

fn run_u07_backup_store_card(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_, follower) = control_leader_and_follower(&mut cluster);

    assert!(
        HTML.contains(r#"id="bk-store-card""#) && HTML.contains(r#"id="bk-store-body""#),
        "the shell carries the Backup store card"
    );
    assert!(
        BACKUPS_JS.contains("function renderBackupStore"),
        "dashboard_backups.js renders the backup store card"
    );
    assert!(
        BACKUPS_JS.contains("STATE.backupStore")
            && BACKUPS_JS.contains("bs.store")
            && BACKUPS_JS.contains("bs.objects")
            && BACKUPS_JS.contains("bs.janitor")
            && BACKUPS_JS.contains("bs.leader"),
        "the card reads every field GET /admin/backup-store serves"
    );
    assert!(
        CORE_JS.contains("/admin/backup-store"),
        "dashboard_core.js fetches the route once against SEED"
    );

    let (status, body) = cluster.admin(follower, "GET", "/admin/backup-store", "", &[]);
    assert_eq!(status, 200, "seed={seed}: GET /admin/backup-store: {body}");
    let v = json(&body);
    assert!(
        v.get("store").is_some(),
        "seed={seed}: carries \"store\": {body}"
    );
    assert!(
        v.get("objects").is_some(),
        "seed={seed}: carries \"objects\": {body}"
    );
    assert!(
        v.get("janitor").is_some(),
        "seed={seed}: carries \"janitor\": {body}"
    );
    assert!(
        v.get("leader").and_then(Value::as_bool).is_some(),
        "seed={seed}: carries a boolean \"leader\": {body}"
    );
}

#[test]
fn u07_backup_store_card() {
    run_u07_backup_store_card(env_seed(0xC087_0003));
}

#[test]
fn u07_backup_store_card_over_seeds() {
    for i in 0..5 {
        run_u07_backup_store_card(0xC087_0300 + i);
    }
}

// ---------------------------------------------------------------------------
// (4) dashboard_u07_ttl_reaper_card
// ---------------------------------------------------------------------------

fn run_u07_ttl_reaper_card(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_, follower) = control_leader_and_follower(&mut cluster);

    assert!(
        HTML.contains(r#"id="ttl-card""#) && HTML.contains(r#"id="ttl-body""#),
        "the shell carries the TTL reaper card"
    );
    assert!(
        STORAGE_JS.contains("function renderTtlReaper"),
        "dashboard_storage.js renders the TTL reaper card"
    );
    assert!(
        STORAGE_JS.contains("n.ttl")
            && STORAGE_JS.contains("t.reaper")
            && STORAGE_JS.contains("t.leader_tablets"),
        "the card reads the fields GET /admin/ttl serves"
    );
    assert!(
        CORE_JS.contains(r#"getJSON(base, "/admin/ttl")"#),
        "dashboard_core.js fetches /admin/ttl per node (base, not SEED)"
    );

    let (status, body) = cluster.admin(follower, "GET", "/admin/ttl", "", &[]);
    assert_eq!(status, 200, "seed={seed}: GET /admin/ttl: {body}");
    let v = json(&body);
    assert!(
        v.get("reaper").is_some(),
        "seed={seed}: carries \"reaper\": {body}"
    );
    assert!(
        v.get("tables").is_some(),
        "seed={seed}: carries \"tables\": {body}"
    );
    assert!(
        v.get("leader_tablets").and_then(Value::as_u64).is_some(),
        "seed={seed}: carries a numeric \"leader_tablets\": {body}"
    );
}

#[test]
fn u07_ttl_reaper_card() {
    run_u07_ttl_reaper_card(env_seed(0xC087_0004));
}

#[test]
fn u07_ttl_reaper_card_over_seeds() {
    for i in 0..5 {
        run_u07_ttl_reaper_card(0xC087_0400 + i);
    }
}

// ---------------------------------------------------------------------------
// (5) dashboard_u07_gc_card
// ---------------------------------------------------------------------------

fn run_u07_gc_card(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_, follower) = control_leader_and_follower(&mut cluster);

    assert!(
        HTML.contains(r#"id="gc-card""#) && HTML.contains(r#"id="gc-body""#),
        "the shell carries the GC card"
    );
    assert!(
        STORAGE_JS.contains("function renderGcJanitor"),
        "dashboard_storage.js renders the GC card"
    );
    assert!(
        STORAGE_JS.contains("STATE.gc")
            && STORAGE_JS.contains("gc.janitor")
            && STORAGE_JS.contains("gc.leader"),
        "the card reads every field GET /admin/gc serves"
    );
    assert!(
        CORE_JS.contains(r#"getJSON(SEED, "/admin/gc")"#),
        "dashboard_core.js fetches the route once against SEED"
    );

    let (status, body) = cluster.admin(follower, "GET", "/admin/gc", "", &[]);
    assert_eq!(status, 200, "seed={seed}: GET /admin/gc: {body}");
    let v = json(&body);
    assert!(
        v.get("janitor").is_some(),
        "seed={seed}: carries \"janitor\": {body}"
    );
    assert!(
        v.get("leader").and_then(Value::as_bool).is_some(),
        "seed={seed}: carries a boolean \"leader\": {body}"
    );
}

#[test]
fn u07_gc_card() {
    run_u07_gc_card(env_seed(0xC087_0005));
}

#[test]
fn u07_gc_card_over_seeds() {
    for i in 0..5 {
        run_u07_gc_card(0xC087_0500 + i);
    }
}

// ---------------------------------------------------------------------------
// (6) dashboard_u07_segment_store_card
// ---------------------------------------------------------------------------

fn run_u07_segment_store_card(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_, follower) = control_leader_and_follower(&mut cluster);

    assert!(
        HTML.contains(r#"id="seg-store-card""#) && HTML.contains(r#"id="seg-store-body""#),
        "the shell carries the Segment store card"
    );
    assert!(
        STORAGE_JS.contains("function renderSegmentStore"),
        "dashboard_storage.js renders the Segment store card"
    );
    assert!(
        STORAGE_JS.contains("n.segmentStore")
            && STORAGE_JS.contains("segmentStore.shards")
            && STORAGE_JS.contains("s.local_objects"),
        "the card reads every field GET /admin/segment-store serves"
    );
    assert!(
        CORE_JS.contains(r#"getJSON(base, "/admin/segment-store")"#),
        "dashboard_core.js fetches /admin/segment-store per node (base, not SEED)"
    );

    let (status, body) = cluster.admin(follower, "GET", "/admin/segment-store", "", &[]);
    assert_eq!(status, 200, "seed={seed}: GET /admin/segment-store: {body}");
    let v = json(&body);
    assert!(
        v.get("store").is_some(),
        "seed={seed}: carries \"store\": {body}"
    );
    assert!(
        v.get("shards").is_some(),
        "seed={seed}: carries \"shards\": {body}"
    );
    assert!(
        v.get("local_objects").is_some(),
        "seed={seed}: carries \"local_objects\": {body}"
    );
}

#[test]
fn u07_segment_store_card() {
    run_u07_segment_store_card(env_seed(0xC087_0006));
}

#[test]
fn u07_segment_store_card_over_seeds() {
    for i in 0..5 {
        run_u07_segment_store_card(0xC087_0600 + i);
    }
}

// ---------------------------------------------------------------------------
// (7) dashboard_u04_ttl_row
// ---------------------------------------------------------------------------

fn run_u04_ttl_row(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_, follower) = control_leader_and_follower(&mut cluster);

    let stream_pos = HTML
        .find(r#"id="br-dy-stream""#)
        .expect("shell carries #br-dy-stream");
    let ttl_pos = HTML
        .find(r#"id="br-dy-ttl""#)
        .expect("shell carries #br-dy-ttl");
    assert!(
        ttl_pos > stream_pos && ttl_pos - stream_pos < 200,
        "#br-dy-ttl sits immediately beside #br-dy-stream"
    );

    assert!(
        BROWSER_JS.contains("function renderTtlRow")
            && BROWSER_JS.contains("function enableTtl")
            && BROWSER_JS.contains("function disableTtl"),
        "dashboard_browser.js defines the TTL row's render + enable/disable handlers"
    );
    assert!(
        BROWSER_JS.contains("schema.ttl"),
        "renderTtlRow reads the already-fetched schema.ttl, no extra DescribeTimeToLive round trip"
    );
    assert!(
        BROWSER_JS.contains("UpdateTimeToLive")
            && BROWSER_JS.contains("TimeToLiveSpecification")
            && BROWSER_JS.contains("AttributeName"),
        "the TTL row posts the real UpdateTimeToLive op with its real payload shape"
    );
    assert!(
        BROWSER_JS.contains("Enable TTL on") && BROWSER_JS.contains("Disable TTL on"),
        "both enable and disable are gated behind their own window.confirm"
    );

    let (status, ct_body) = cluster.admin(
        follower,
        "POST",
        "/admin/data/dynamo",
        "",
        br#"{"op":"CreateTable","payload":{"TableName":"widgets","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable widgets: {ct_body}");

    let non_leader = non_leader_of_table(&cluster, "widgets");

    let (status, en_body) = cluster.admin(
        non_leader,
        "POST",
        "/admin/data/dynamo",
        "",
        br#"{"op":"UpdateTimeToLive","payload":{"TableName":"widgets","TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: enable TTL: {en_body}");

    let (status, status_body) = cluster.admin(non_leader, "GET", "/admin/status", "", &[]);
    assert_eq!(status, 200, "seed={seed}: {status_body}");
    let status_v = json(&status_body);
    assert_eq!(
        status_v["schemas"]["tables"]["widgets"]["ttl"]["attribute_name"].as_str(),
        Some("expiresAt"),
        "seed={seed}: TTL is enabled with the declared attribute: {status_body}"
    );

    let (status, dis_body) = cluster.admin(
        non_leader,
        "POST",
        "/admin/data/dynamo",
        "",
        br#"{"op":"UpdateTimeToLive","payload":{"TableName":"widgets","TimeToLiveSpecification":{"Enabled":false,"AttributeName":"expiresAt"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: disable TTL: {dis_body}");

    let (status, status_body) = cluster.admin(non_leader, "GET", "/admin/status", "", &[]);
    assert_eq!(status, 200, "seed={seed}: {status_body}");
    let status_v = json(&status_body);
    assert!(
        status_v["schemas"]["tables"]["widgets"]["ttl"].is_null(),
        "seed={seed}: TTL is disabled: {status_body}"
    );
}

#[test]
fn u04_ttl_row() {
    run_u04_ttl_row(env_seed(0xC087_0007));
}

#[test]
fn u04_ttl_row_over_seeds() {
    for i in 0..5 {
        run_u04_ttl_row(0xC087_0700 + i);
    }
}

// ---------------------------------------------------------------------------
// (8) dashboard_u04_create_table_form
// ---------------------------------------------------------------------------

fn run_u04_create_table_form(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_, follower) = control_leader_and_follower(&mut cluster);

    for id in [
        "br-dy-ct-lsi-table",
        "br-dy-ct-lsi-add",
        "br-dy-ct-gsi-table",
        "br-dy-ct-gsi-add",
        "br-dy-ct-stream",
        "br-dy-ct-stream-vt",
        "br-dy-ct-ttl",
        "br-dy-ct-ttl-attr",
    ] {
        assert!(
            HTML.contains(&format!(r#"id="{id}""#)),
            "the create-table form carries #{id}"
        );
    }

    assert!(
        BROWSER_JS.contains("function addCtLsiRow") && BROWSER_JS.contains("function addCtGsiRow"),
        "the form's GSI/LSI row editors exist"
    );
    assert!(
        BROWSER_JS.contains("GlobalSecondaryIndexes")
            && BROWSER_JS.contains("LocalSecondaryIndexes")
            && BROWSER_JS.contains("StreamSpecification"),
        "submitTableForm sends GSIs, LSIs, and a stream spec on CreateTable"
    );
    for marker in [
        "needs a hash attribute",
        "needs a sort key attribute",
        "requires the table to have its own sort key",
        "INCLUDE projection needs at least one attribute",
    ] {
        assert!(
            BROWSER_JS.contains(marker),
            "submitTableForm validates {marker:?} client-side"
        );
    }
    assert!(
        BROWSER_JS.contains("AttributeDefinitions") && BROWSER_JS.contains("declareDefault"),
        "the form declares AttributeDefinitions for every base and index key"
    );

    let (status, ct_body) = cluster.admin(
        follower,
        "POST",
        "/admin/data/dynamo",
        "",
        br#"{"op":"CreateTable","payload":{
            "TableName":"orders",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"},{"AttributeName":"created_at","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[{"IndexName":"by-status","KeySchema":[{"AttributeName":"status","KeyType":"HASH"}]}],
            "LocalSecondaryIndexes":[{"IndexName":"by-score","KeySchema":[{"AttributeName":"id","KeyType":"HASH"},{"AttributeName":"score","KeyType":"RANGE"}]}],
            "AttributeDefinitions":[
                {"AttributeName":"id","AttributeType":"S"},
                {"AttributeName":"created_at","AttributeType":"N"},
                {"AttributeName":"status","AttributeType":"S"},
                {"AttributeName":"score","AttributeType":"S"}
            ],
            "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}
        }}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable orders: {ct_body}");

    let non_leader = non_leader_of_table(&cluster, "orders");

    let (status, ttl_body) = cluster.admin(
        non_leader,
        "POST",
        "/admin/data/dynamo",
        "",
        br#"{"op":"UpdateTimeToLive","payload":{"TableName":"orders","TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: enable TTL on orders: {ttl_body}");

    let (status, status_body) = cluster.admin(non_leader, "GET", "/admin/status", "", &[]);
    assert_eq!(status, 200, "seed={seed}: {status_body}");
    let status_v = json(&status_body);
    let schema = &status_v["schemas"]["tables"]["orders"];
    let indexes = schema["indexes"].as_array().expect("indexes array");
    assert!(
        indexes.iter().any(|i| i["name"] == "by-status"
            && i["kind"] == "Global"
            && i["hash_attribute"] == "status"),
        "seed={seed}: the GSI is declared: {schema}"
    );
    assert!(
        indexes.iter().any(|i| i["name"] == "by-score"
            && i["kind"] == "Local"
            && i["sort_attribute"] == "score"),
        "seed={seed}: the LSI is declared: {schema}"
    );
    assert_eq!(
        schema["stream"]["view_type"].as_str(),
        Some("NewAndOldImages"),
        "seed={seed}: the stream is declared: {schema}"
    );
    assert_eq!(
        schema["ttl"]["attribute_name"].as_str(),
        Some("expiresAt"),
        "seed={seed}: TTL is declared via the follow-up call: {schema}"
    );
}

#[test]
fn u04_create_table_form() {
    run_u04_create_table_form(env_seed(0xC087_0008));
}

#[test]
fn u04_create_table_form_over_seeds() {
    for i in 0..5 {
        run_u04_create_table_form(0xC087_0800 + i);
    }
}

// ---------------------------------------------------------------------------
// (9) dashboard_u05_control_members_panel
// ---------------------------------------------------------------------------

fn run_u05_control_members_panel(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_, follower) = control_leader_and_follower(&mut cluster);

    let mirror_pos = HTML
        .find(r#"id="nd-mirror""#)
        .expect("shell carries #nd-mirror");
    let members_pos = HTML
        .find(r#"id="nd-control-members""#)
        .expect("shell carries #nd-control-members");
    assert!(
        members_pos > mirror_pos && members_pos - mirror_pos < 200,
        "#nd-control-members sits immediately beside #nd-mirror"
    );

    assert!(
        NODE_JS.contains("function renderNodeControlMembers")
            && NODE_JS.contains("controlMembers")
            && NODE_JS.contains("nd-control-members"),
        "dashboard_node.js defines the control-members panel's render function"
    );
    assert!(
        CORE_JS.contains("/admin/control/members") && CORE_JS.contains("controlMembers"),
        "dashboard_core.js fetches control members into SELF alongside everything else"
    );

    // Converged-or-timeout, never a one-shot assert: `SimCluster::new`'s own
    // `seed_members` already proposes every node's `RegisterNode`/
    // `UpsertMember` and polls them to land before returning, so this
    // converges on the very first read in practice — polled anyway,
    // mirroring the real-socket original's own "a node's own address-book
    // entry can still be in flight" caution.
    let mut converged = false;
    for _ in 0..20 {
        let (status, body) = cluster.admin(follower, "GET", "/admin/control/members", "", &[]);
        assert_eq!(status, 200, "seed={seed}: {body}");
        let v = json(&body);
        let voters_ok = v["voters"].as_array().is_some_and(|arr| arr.len() == 3);
        let addrs_ok = v["addrs"].as_object().is_some_and(|addrs| {
            (0..cluster.node_count() as u64).all(|n| addrs.contains_key(&nid(n).to_string()))
        });
        if voters_ok && addrs_ok {
            converged = true;
            break;
        }
        cluster.run_for(Duration::from_millis(100));
    }
    assert!(
        converged,
        "seed={seed}: GET /admin/control/members never carried every bootstrap voter and address"
    );
}

#[test]
fn u05_control_members_panel() {
    run_u05_control_members_panel(env_seed(0xC087_0009));
}

#[test]
fn u05_control_members_panel_over_seeds() {
    for i in 0..5 {
        run_u05_control_members_panel(0xC087_0900 + i);
    }
}

// ---------------------------------------------------------------------------
// (10) dashboard_u05_tablet_actions — pure marker check, no SimCluster
// ---------------------------------------------------------------------------

/// docs/roadmap.md U-05's third slice (the TABLET action family): four
/// gated buttons on the tablet detail card — Split, Flush, Compact,
/// Reconfigure — over the four pre-existing `/admin/tablet/split`,
/// `/admin/storage/{flush,compact}`, and `/admin/raftkv/reconfigure`
/// routes. The buttons only ever exist in client-rendered `TABLETS_JS`
/// (`renderTabletDetail`'s own template), never the static shell, so this
/// scenario proves only the served JS — button ids, route paths,
/// `window.confirm` guards, the `postJSON`/`loadAll` mutation idiom, and
/// the leader-targeting helper. **Touches no `SimCluster` at all**: the
/// four routes' own wire-level round trip is already covered by
/// `tests/admin_endpoint.rs`'s `admin_interface_surfaces_state_and_
/// actions`/`admin_storage_compact_action` — untouched by this PR, PR 6's
/// own territory — so this scenario carries no seed.
#[test]
fn u05_tablet_actions() {
    assert!(
        HTML.contains(r#"id="tb-detail""#),
        "shell still carries #tb-detail"
    );

    for btn_id in [
        "tb-split-btn",
        "tb-flush-btn",
        "tb-compact-btn",
        "tb-reconfigure-btn",
    ] {
        assert!(
            TABLETS_JS.contains(btn_id),
            "dashboard_tablets.js defines button #{btn_id}"
        );
    }
    for route in [
        "/admin/tablet/split",
        "/admin/storage/flush",
        "/admin/storage/compact",
        "/admin/raftkv/reconfigure",
    ] {
        assert!(
            TABLETS_JS.contains(route),
            "dashboard_tablets.js posts to the real route {route}"
        );
    }
    assert!(
        TABLETS_JS.matches("window.confirm(").count() >= 4,
        "every one of the four actions is guarded by window.confirm"
    );
    assert!(
        TABLETS_JS.contains("postJSON(") && TABLETS_JS.contains("await loadAll()"),
        "actions use postJSON + the existing loadAll() refresh"
    );
    assert!(
        TABLETS_JS.contains("function tbLeaderBase") && TABLETS_JS.contains("lead.node.base"),
        "leader-only actions resolve the same leader address the storage panel already uses"
    );
}

// ---------------------------------------------------------------------------
// (11) dashboard_u05_node_actions
// ---------------------------------------------------------------------------

fn run_u05_node_actions(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_, follower) = control_leader_and_follower(&mut cluster);

    let members_pos = HTML
        .find(r#"id="nd-control-members""#)
        .expect("shell carries #nd-control-members");
    let actions_pos = HTML
        .find(r#"id="nd-actions""#)
        .expect("shell carries #nd-actions");
    assert!(
        actions_pos > members_pos && actions_pos - members_pos < 200,
        "#nd-actions sits immediately beside #nd-control-members"
    );

    for btn_id in ["nd-drain-btn", "nd-remove-btn", "nd-add-member-btn"] {
        assert!(
            NODE_JS.contains(btn_id),
            "dashboard_node.js defines button #{btn_id}"
        );
    }
    for route in ["/admin/drain", "/admin/member/remove", "/admin/member/add"] {
        assert!(
            NODE_JS.contains(route),
            "dashboard_node.js posts to the real route {route}"
        );
    }
    assert!(
        NODE_JS.matches("window.confirm(").count() >= 3,
        "every one of the three actions is guarded by window.confirm"
    );
    assert!(
        NODE_JS.contains("postJSON(") && NODE_JS.contains("await loadAll()"),
        "actions use postJSON + the existing loadAll() refresh"
    );
    assert!(
        NODE_JS.contains("function ndControlLeaderBase") && NODE_JS.contains("is_leader"),
        "leader-only actions resolve the live control leader's own admin address"
    );
    assert!(
        NODE_JS.contains(r#"postJSON(SEED, "/admin/member/add""#),
        "the relayed add-member action posts to SEED, needing no leader lookup"
    );

    // ---- the three routes it posts to are real and live, from a control
    //      follower — proving each also exists off a non-leader ClientCtx --
    for route in ["/admin/drain", "/admin/member/remove", "/admin/member/add"] {
        let (status, body) = cluster.admin(follower, "POST", route, "", b"{}");
        assert_ne!(status, 404, "seed={seed}: {route} exists: {body}");
    }
}

#[test]
fn u05_node_actions() {
    run_u05_node_actions(env_seed(0xC087_000A));
}

#[test]
fn u05_node_actions_over_seeds() {
    for i in 0..5 {
        run_u05_node_actions(0xC087_0A00 + i);
    }
}

// ---------------------------------------------------------------------------
// (12) dashboard_u05_control_member_actions
// ---------------------------------------------------------------------------

fn run_u05_control_member_actions(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (_, follower) = control_leader_and_follower(&mut cluster);

    let members_pos = HTML
        .find(r#"id="nd-control-members""#)
        .expect("shell carries #nd-control-members");
    let ctl_actions_pos = HTML
        .find(r#"id="nd-control-actions""#)
        .expect("shell carries #nd-control-actions");
    let actions_pos = HTML
        .find(r#"id="nd-actions""#)
        .expect("shell carries #nd-actions");
    assert!(
        members_pos < ctl_actions_pos && ctl_actions_pos < actions_pos,
        "#nd-control-actions sits between #nd-control-members and #nd-actions"
    );

    for marker in [
        "nd-cm-transfer-btn",
        "nd-cm-remove-btn",
        "nd-ctl-add-btn",
        "function ndTransferControlLeadership",
        "function ndRemoveControlMember",
        "function ndAddControlMember",
    ] {
        assert!(
            NODE_JS.contains(marker),
            "dashboard_node.js defines {marker}"
        );
    }
    for route in [
        "/admin/control/transfer",
        "/admin/control/member/remove",
        "/admin/control/member/add",
    ] {
        assert!(
            NODE_JS.contains(route),
            "dashboard_node.js posts to the real route {route}"
        );
    }
    assert!(
        NODE_JS.matches("window.confirm(").count() >= 6,
        "every action (three pre-existing plus three new) is guarded by window.confirm"
    );
    assert!(
        NODE_JS.contains("postJSON(") && NODE_JS.contains("await loadAll()"),
        "actions use postJSON + the existing loadAll() refresh"
    );
    assert!(
        NODE_JS.contains(r#"postJSON(base, "/admin/control/transfer""#)
            && NODE_JS.contains(r#"postJSON(base, "/admin/control/member/remove""#)
            && NODE_JS.contains(r#"postJSON(base, "/admin/control/member/add""#),
        "control-member actions target the resolved control leader base"
    );
    assert!(
        NODE_JS.contains("ndCtlAddNode") && NODE_JS.contains("ndCtlAddAddr"),
        "the Add control's inputs are persisted module-level state"
    );

    // ---- the three routes it posts to are real and live, from a control
    //      follower — the identical "route exists" shape as scenario (11) --
    for route in [
        "/admin/control/transfer",
        "/admin/control/member/remove",
        "/admin/control/member/add",
    ] {
        let (status, body) = cluster.admin(follower, "POST", route, "", b"{}");
        assert_ne!(status, 404, "seed={seed}: {route} exists: {body}");
    }
}

#[test]
fn u05_control_member_actions() {
    run_u05_control_member_actions(env_seed(0xC087_000B));
}

#[test]
fn u05_control_member_actions_over_seeds() {
    for i in 0..5 {
        run_u05_control_member_actions(0xC087_0B00 + i);
    }
}

// ---------------------------------------------------------------------------
// PR 4c: role split — tests/dashboard_endpoint.rs's two role-named tests
// ---------------------------------------------------------------------------

/// Local `poll_until` for both scenarios below (this file's own convention
/// has no shared one — every other convergence check here polls inline).
fn poll_until_dashboard_role(
    cluster: &mut SimCluster,
    budget: Duration,
    seed: u64,
    what: &str,
    mut cond: impl FnMut(&mut SimCluster) -> bool,
) {
    const STEP: Duration = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    loop {
        if cond(cluster) {
            return;
        }
        assert!(
            elapsed < budget,
            "seed={seed}: {what} never converged within {budget:?}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

// ---------------------------------------------------------------------------
// (13) dashboard_role_gating_split_deployment
//      — the JSON/asset-marker half only; the original stays whole for its
//      real-HTTP-framing half.
// ---------------------------------------------------------------------------

fn run_dashboard_role_gating_split_deployment(seed: u64) {
    let roles = [NodeRole::Control, NodeRole::Data];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 1);

    // ---- both roles serve the same shell + JS assets (render-only) -----
    // Read directly from the served assets' own compile-time constants
    // (this file's own top-of-module note explains why that's not a
    // narrower proof than fetching them over a socket) — the real assets
    // are the same bytes regardless of which role's admin port would have
    // served them, so no per-role dispatch is needed for this half.
    assert!(
        HTML.contains("animusd admin") && HTML.contains("dashboard_node.js"),
        "the shell references the Node view's script asset"
    );
    assert!(
        NODE_JS.contains("function renderNode")
            && NODE_JS.contains("control_mirror")
            && NODE_JS.contains("nd-tablet-sel"),
        "dashboard_node.js carries the Node view's rendering + storage-debug markers"
    );

    // ---- the gating logic itself lives in CORE_JS -----------------------
    assert!(
        CORE_JS.contains("ROLE_TABS")
            && CORE_JS.contains("applyRoleGating")
            && CORE_JS.contains(r#"data: ["node", "browser", "streams"]"#),
        "dashboard_core.js defines the per-role tab gating, including the \
         data role's node-first tab list (now with Streams, ADR 0042/0043)"
    );
    assert!(
        CORE_JS.contains(
            r#"control: ["overview", "placement", "tablets", "txns", "browser", "streams", "storage", "backups"]"#
        ),
        "the control role's own tab list includes Streams, Transactions, and Backups too"
    );

    // ---- /admin/config's role differs across the split -------------------
    // Only `role` itself differs per role under `SimCluster` — every other
    // per-role field the real test also checks (`backup_store`/
    // `segment_store`/`quiesce_after_ms`/`auth_enabled`/`auth_access_key_
    // ids`) is `null` for EVERY node here regardless of role (`AdminInfo`'s
    // own construction in `SimCluster::new_with_roles`, this file's own
    // module doc), so those aren't asserted per-role here.
    let (status, body) = cluster.admin(0, "GET", "/admin/config", "", &[]);
    assert_eq!(
        status, 200,
        "seed={seed}: /admin/config on the control node: {body}"
    );
    assert_eq!(
        json(&body)["role"],
        "control",
        "seed={seed}: a control-only node's own /admin/config reports its role: {body}"
    );

    let (status, body) = cluster.admin(1, "GET", "/admin/config", "", &[]);
    assert_eq!(
        status, 200,
        "seed={seed}: /admin/config on the data node: {body}"
    );
    assert_eq!(
        json(&body)["role"],
        "data",
        "seed={seed}: a data-only node's own /admin/config reports its role: {body}"
    );

    // ---- the data-only node's control-plane mirror actually syncs -----
    // Bounded poll — the mirror needs at least one sync/long-poll round
    // trip against the control deployment.
    poll_until_dashboard_role(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "the data-only node's own control_mirror syncing",
        |c| {
            let (status, body) = c.admin(1, "GET", "/admin/raft", "", &[]);
            assert_eq!(
                status, 200,
                "seed={seed}: /admin/raft on the data node: {body}"
            );
            let v = json(&body);
            let cm = &v["control_mirror"];
            assert!(
                cm.is_object(),
                "seed={seed}: control_mirror is present: {v}"
            );
            assert!(
                cm["watermark"].is_u64(),
                "seed={seed}: watermark is a number: {cm}"
            );
            assert!(
                cm["leader_hint"].is_null() || cm["leader_hint"].is_string(),
                "seed={seed}: leader_hint is null or a string: {cm}"
            );
            cm["has_synced"] == Value::Bool(true)
        },
    );

    // A control-bearing node IS a control-plane voter, so its own
    // `/admin/raft` reports the honest degenerate mirror (never "synced"
    // via a mirror — its own Raft state is already the ground truth).
    let (status, body) = cluster.admin(0, "GET", "/admin/raft", "", &[]);
    assert_eq!(
        status, 200,
        "seed={seed}: /admin/raft on the control node: {body}"
    );
    assert_eq!(
        json(&body)["control_mirror"]["has_synced"],
        Value::Bool(false),
        "seed={seed}: a control-plane voter's own mirror is never 'synced' (no mirror \
         involved): {body}"
    );
}

#[test]
fn dashboard_role_gating_split_deployment() {
    run_dashboard_role_gating_split_deployment(env_seed(0xC12C_0003));
}

#[test]
fn dashboard_role_gating_split_deployment_over_seeds() {
    for i in 0..5 {
        run_dashboard_role_gating_split_deployment(0xC12C_3000 + i);
    }
}

// ---------------------------------------------------------------------------
// (14) control_node_streams_read_path_is_ground_truth — full convert
// ---------------------------------------------------------------------------

fn run_control_node_streams_read_path_is_ground_truth(seed: u64) {
    let roles = [NodeRole::Control, NodeRole::Data];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 1);

    // An ENABLED stream (stays open — no seal).
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"OpenT","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
            "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"OpenT","Item":{"pk":{"S":"k1"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {body}");

    // A stream forced sealed via disable (F12-b's final seal, synchronous
    // — `dynamo::disable_stream` calls `force_seal_tablet` directly, so no
    // periodic loop or `SimCluster::drive_stream_seal` is needed).
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"SealedT","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
            "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"SealedT","Item":{"pk":{"S":"k1"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.UpdateTable",
        br#"{"TableName":"SealedT","StreamSpecification":{"StreamEnabled":false}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {body}");

    // ---- ground truth on the CONTROL-ONLY node's own admin port -------

    // `/admin/status` mirrors the full replicated catalog: both streams'
    // specs/rows, converged-or-timeout (the data-only node's writes need a
    // beat to reach the control-only node's own mirror).
    poll_until_dashboard_role(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "the control node's /admin/status converging to the sealed row",
        |c| {
            let (status, body) = c.admin(0, "GET", "/admin/status", "", &[]);
            assert_eq!(status, 200, "seed={seed}: {body}");
            let v = json(&body);
            let open_ok = v["schemas"]["tables"]["OpenT"]["stream"]["label"].is_string();
            let sealed_row_present = v["stream_shards"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|r| {
                    r["table"].as_str() == Some("SealedT")
                        && !r["expired"].as_bool().unwrap_or(true)
                });
            open_ok && sealed_row_present
        },
    );

    // `ListStreams` through the admin proxy (`/admin/data/dynamo`) —
    // metadata-only, so it must be exact, not eventually-consistent.
    let (status, body) = cluster.admin(
        0,
        "POST",
        "/admin/data/dynamo",
        "",
        br#"{"op":"ListStreams","payload":{}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    let list = json(&body);
    let streams = list["Streams"].as_array().expect("Streams array");
    let names: Vec<&str> = streams
        .iter()
        .filter_map(|s| s["TableName"].as_str())
        .collect();
    assert!(
        names.contains(&"OpenT") && names.contains(&"SealedT"),
        "seed={seed}: ListStreams from the control-only node's admin proxy lists both \
         streams: {body}"
    );

    // `DescribeStream` on each — open stream has exactly one shard with no
    // `EndingSequenceNumber`; the sealed one has exactly one shard WITH an
    // `EndingSequenceNumber` and `StreamStatus: DISABLED`.
    let open_arn = streams
        .iter()
        .find(|s| s["TableName"].as_str() == Some("OpenT"))
        .and_then(|s| s["StreamArn"].as_str())
        .expect("OpenT's stream ARN")
        .to_string();
    let sealed_arn = streams
        .iter()
        .find(|s| s["TableName"].as_str() == Some("SealedT"))
        .and_then(|s| s["StreamArn"].as_str())
        .expect("SealedT's stream ARN")
        .to_string();

    let (status, body) = cluster.admin(
        0,
        "POST",
        "/admin/data/dynamo",
        "",
        format!(r#"{{"op":"DescribeStream","payload":{{"StreamArn":"{open_arn}"}}}}"#).as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    let desc = json(&body);
    let sd = &desc["StreamDescription"];
    assert_eq!(sd["StreamStatus"], "ENABLED", "seed={seed}: {body}");
    let shards = sd["Shards"].as_array().expect("Shards array");
    assert_eq!(shards.len(), 1, "seed={seed}: {body}");
    assert!(
        shards[0]["SequenceNumberRange"]["EndingSequenceNumber"].is_null(),
        "seed={seed}: OpenT's own shard is genuinely open (no EndingSequenceNumber): {body}"
    );

    let (status, body) = cluster.admin(
        0,
        "POST",
        "/admin/data/dynamo",
        "",
        format!(r#"{{"op":"DescribeStream","payload":{{"StreamArn":"{sealed_arn}"}}}}"#).as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    let desc = json(&body);
    let sd = &desc["StreamDescription"];
    assert_eq!(sd["StreamStatus"], "DISABLED", "seed={seed}: {body}");
    let shards = sd["Shards"].as_array().expect("Shards array");
    assert_eq!(shards.len(), 1, "seed={seed}: {body}");
    assert!(
        shards[0]["SequenceNumberRange"]["EndingSequenceNumber"].is_string(),
        "seed={seed}: SealedT's own shard is genuinely sealed (has an EndingSequenceNumber): \
         {body}"
    );
}

#[test]
fn control_node_streams_read_path_is_ground_truth() {
    run_control_node_streams_read_path_is_ground_truth(env_seed(0xC12C_0004));
}

#[test]
fn control_node_streams_read_path_is_ground_truth_over_seeds() {
    for i in 0..5 {
        run_control_node_streams_read_path_is_ground_truth(0xC12C_4000 + i);
    }
}
