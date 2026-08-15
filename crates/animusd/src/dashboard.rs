//! The AnimusDB Console (ADR 0021): a self-contained single-page app served
//! from the admin port for manual cluster testing and operation — Overview,
//! Placement, Tablets, Data Browser, and Storage. ADR 0035 PR7 adds a sixth
//! view, Node; a Streams view (ADR 0042/0043) is a seventh — DynamoDB Streams'
//! shard chains and a live-tail poller. Which views are shown is gated on
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
//! `STREAMS_JS`/`BROWSER_JS`/`STORAGE_JS`/`NODE_JS` are the seven views' own
//! render logic, loaded in that order (each may call functions defined
//! earlier, since plain `<script src>` tags share one global scope) —
//! `STREAMS_JS` loads before `BROWSER_JS` because the Data Browser's
//! per-table Stream row (enable/disable) reuses its `viewTypeLabel` helper.
//!
//! Read-only mostly — the Data Browser (item CRUD, table DDL, the per-table
//! stream enable/disable toggle, and the bulk-seed tool, which writes real
//! DynamoDB items) and the Streams tab's live-tail poller carry the real
//! mutations/reads; the ADR 0020 gated operator actions (split/flush/compact/
//! reconfigure/drain) and the ADR 0018 transaction view are not yet surfaced.

/// The console's page shell, embedded at compile time.
pub(crate) const HTML: &str = include_str!("dashboard.html");
/// The console's stylesheet (dark + light themes).
pub(crate) const CSS: &str = include_str!("dashboard.css");
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
/// The Streams view (ADR 0042/0043): a list of currently-enabled and
/// disabled-but-in-grace-window DynamoDB Streams, per-node stream metric
/// tiles (the console's first `/admin/metrics` consumer), and a detail panel
/// with the shard chain (segment catalog merged with a live `DescribeStream`)
/// and a live-tail poller (`GetShardIterator`/`GetRecords`).
pub(crate) const STREAMS_JS: &str = include_str!("dashboard_streams.js");
/// The Data Browser view: CQL + real DynamoDB Scan/Query/item CRUD/table DDL,
/// a per-table Stream enable/disable row, plus the bulk-seed tool (it writes
/// real DynamoDB items, so it lives in the DynamoDB panel).
pub(crate) const BROWSER_JS: &str = include_str!("dashboard_browser.js");
/// The Storage view: folded-in WAL/LSM/key-inspector/browse-keys debug tools
/// (not part of the source design, preserved from the pre-redesign dashboard
/// so no capability is lost).
pub(crate) const STORAGE_JS: &str = include_str!("dashboard_storage.js");
/// The Node view (ADR 0035 PR7): a data-only node's dedicated page — its own
/// identity/health/control-plane mirror status, hosted tablets, a
/// node-scoped storage debug panel, and a link to a reachable
/// control/combined node's Console. Shown instead of the cluster Console on
/// a data-only node, and appended after it on a combined node.
pub(crate) const NODE_JS: &str = include_str!("dashboard_node.js");
