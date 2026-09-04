//! animusd admin (ADR 0021's "AnimusDB Console"): a self-contained single-page app served
//! from the admin port for manual cluster testing and operation — Overview,
//! Placement, Tablets, Data Browser, and Storage. ADR 0035 PR7 adds a sixth
//! view, Node; a Streams view (ADR 0042/0043) is a seventh — DynamoDB Streams'
//! shard chains and a live-tail poller. A Transactions view (ADR 0018 §2/PR7,
//! docs/roadmap.md U-01) is an eighth — a read-only render of `/admin/txns`,
//! gated like Tablets (both are cluster-wide views a data-only node can't
//! derive without local control-plane `Metadata`). A Backups view (ADR 0059,
//! docs/roadmap.md U-02) is a ninth — a render of `/admin/backups`/
//! `/admin/restores` plus gated Create/Delete/Restore/PITR actions, gated
//! like Placement (same reasoning: no per-node fan-out, just replicated
//! `Metadata` a data-only node can't read locally). Which views are shown is gated on
//! this node's own role (control/data/combined,
//! `admin.rs::config_view`'s `role` field) — see `dashboard_core.js`'s
//! `ROLE_TABS`; Streams is shown for combined/data, never control-only (a
//! control-only node hosts no CP data plane, so it has no stream state).
//!
//! It is embedded in the binary (`include_str!`) and served verbatim as
//! `text/html`/`text/css`/`text/javascript` by [`crate::admin`]; it is a pure
//! **client** of the `/admin/*` JSON surface (ADR 0020), assembling a
//! cluster-wide view by fanning out from the browser to every node's admin
//! port (seeded by `GET /admin/peers`). No build toolchain, no bundler, no
//! external assets (including fonts — ADR 0021 §1 is firm on this, so the
//! console approximates the source design's Inter/IBM Plex Mono choice with
//! system font stacks instead of a CDN fetch) — vanilla HTML/CSS/JS, so the
//! asset ships with the binary and the build stays `cargo`-only.
//!
//! The shell (`HTML`) and its CSS/JS are split into separate files — still all
//! `include_str!`'d at compile time, just served as distinct static assets
//! (`admin.rs::static_asset`) instead of inlined in one document. `CORE_JS`
//! holds shared state/fetch/routing/theme/data-derivation utilities every
//! other module depends on; `OVERVIEW_JS`/`PLACEMENT_JS`/`TABLETS_JS`/
//! `TXNS_JS`/`STREAMS_JS`/`BROWSER_JS`/`STORAGE_JS`/`BACKUPS_JS`/`NODE_JS`
//! are the nine views' own render logic, loaded in that order (each may call
//! functions defined earlier, since plain `<script src>` tags share one
//! global scope) — `STREAMS_JS` loads before `BROWSER_JS` because the Data
//! Browser's per-table Stream row (enable/disable) reuses its
//! `viewTypeLabel` helper, and `BROWSER_JS` loads before `BACKUPS_JS`
//! because the Backups view's table pickers (Create backup, PITR) reuse its
//! `dynamoTables()` helper.
//!
//! Read-only mostly — the Data Browser (item CRUD, table DDL, the per-table
//! stream enable/disable toggle, and the bulk-seed tool, which writes real
//! DynamoDB items) and the Streams tab's live-tail poller carry the real
//! mutations/reads; the ADR 0020 gated operator actions (split/flush/compact/
//! reconfigure/drain) are not yet surfaced. The ADR 0018 transaction view
//! (`TXNS_JS`) is now surfaced, read-only — no manual resolution action, by
//! design (`CpTxnView`'s own doc).

/// The console's page shell, embedded at compile time.
pub(crate) const HTML: &str = include_str!("dashboard.html");
/// The console's stylesheet (dark + light themes): the embedded webfaces, then
/// the shared design tokens (ADR 0056), then this surface's own skin. Served as
/// one stylesheet — `concat!` of `include_str!` literals, so it stays a single
/// compile-time constant with no bundler and no extra route.
pub(crate) const CSS: &str = concat!(
    include_str!("fonts.css"),
    include_str!("tokens.css"),
    include_str!("dashboard.css"),
);

/// The website's copy of the shared token file, pulled in purely so
/// [`tokens_css_matches_website_copy`] can prove the two have not drifted.
#[cfg(test)]
const WEBSITE_TOKENS_CSS: &str = include_str!("../../../website/assets/tokens.css");

/// The design tokens are duplicated on purpose: the site ships static files and
/// the consoles embed their assets in the binary, so there is no runtime they
/// can share (and ADR 0021 rules out a build step that would generate one).
/// Duplication without a check is exactly the drift ADR 0056 exists to stop, so
/// the check is this test.
#[test]
fn tokens_css_matches_website_copy() {
    assert_eq!(
        include_str!("tokens.css"),
        WEBSITE_TOKENS_CSS,
        "crates/animusd/src/tokens.css and website/assets/tokens.css have \
         drifted. They are the shared base of the design system (ADR 0056) and \
         must stay byte-identical: edit one, copy it over the other."
    );
}
/// Shared state, fetch helpers, formatting/data-derivation utilities, theme,
/// and tab routing.
pub(crate) const CORE_JS: &str = include_str!("dashboard_core.js");
/// The Overview view: health banner, stat tiles, nodes list, tables summary,
/// tablet-balance chart.
pub(crate) const OVERVIEW_JS: &str = include_str!("dashboard_overview.js");
/// The Placement view: node cards, per-node tablet list.
pub(crate) const PLACEMENT_JS: &str = include_str!("dashboard_placement.js");
/// The Tablets view: filterable list + raft-group/storage detail panel.
pub(crate) const TABLETS_JS: &str = include_str!("dashboard_tablets.js");
/// The Transactions view (ADR 0018 §2/PR7, docs/roadmap.md U-01): a read-only
/// render of `/admin/txns`, merged cluster-wide the same way `TABLETS_JS`
/// merges `/admin/raftkv` — pending and unresolved-decided multi-participant
/// transaction records per hosted tablet.
pub(crate) const TXNS_JS: &str = include_str!("dashboard_txns.js");
/// The Streams view (ADR 0042/0043): a list of currently-enabled and
/// disabled-but-in-grace-window DynamoDB Streams, per-node stream metric
/// tiles (the console's first `/admin/metrics` consumer), and a detail panel
/// with the shard chain (segment catalog merged with a live `DescribeStream`)
/// and a live-tail poller (`GetShardIterator`/`GetRecords`).
pub(crate) const STREAMS_JS: &str = include_str!("dashboard_streams.js");
/// The Data Browser view: real DynamoDB Scan/Query/item CRUD/table DDL,
/// a per-table Stream enable/disable row, plus the bulk-seed tool (it writes
/// real DynamoDB items, so it lives in the DynamoDB panel).
pub(crate) const BROWSER_JS: &str = include_str!("dashboard_browser.js");
/// The Storage view: folded-in WAL/LSM/key-inspector/browse-keys debug tools
/// (not part of the source design, preserved from the pre-redesign dashboard
/// so no capability is lost).
pub(crate) const STORAGE_JS: &str = include_str!("dashboard_storage.js");
/// The Backups view (ADR 0059, docs/roadmap.md U-02): a read-only render of
/// `/admin/backups`/`/admin/restores` plus per-table PITR status (from
/// `/admin/status`'s `schemas[*].pitr`, already fetched), and four gated
/// actions — Create backup, Delete backup, Restore from backup, and
/// per-table PITR enable/disable — each behind a `window.confirm` and
/// posted through the `/admin/data/dynamo` proxy the Data Browser's own
/// mutations already use. Role-gated like Placement (`ROLE_TABS`).
pub(crate) const BACKUPS_JS: &str = include_str!("dashboard_backups.js");
/// The Node view (ADR 0035 PR7): a data-only node's dedicated page — its own
/// identity/health/control-plane mirror status, hosted tablets, a
/// node-scoped storage debug panel, and a link to a reachable
/// control/combined node's Console. Shown instead of the cluster Console on
/// a data-only node, and appended after it on a combined node.
pub(crate) const NODE_JS: &str = include_str!("dashboard_node.js");
