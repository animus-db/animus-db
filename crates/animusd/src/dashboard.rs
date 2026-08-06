//! The static web dashboard (ADR 0021): a self-contained single-page app served
//! from the admin port for manual cluster testing — nodes, tablets, WALs, data.
//!
//! It is embedded in the binary (`include_str!`) and served verbatim as
//! `text/html`/`text/css`/`text/javascript` by [`crate::admin`]; it is a pure
//! **client** of the `/admin/*` JSON surface (ADR 0020), assembling a
//! cluster-wide view by fanning out from the browser to every node's admin
//! port (seeded by `GET /admin/peers`). No build toolchain, no bundler, no
//! external assets — vanilla HTML/CSS/JS, so the asset ships with the binary
//! and the build stays `cargo`-only (ADR 0021 §1; a vendored single-file
//! library is allowed later but not used here).
//!
//! The shell (`HTML`) and its CSS/JS are split into separate files — still all
//! `include_str!`'d at compile time, just served as distinct static assets
//! (`admin.rs::static_asset`) instead of inlined in one document — so the UI
//! can grow without every change touching one 1000+ line file. `CORE_JS` is
//! shared state/fetch/routing utilities; `MONITORING_JS` and `WRITE_JS` are the
//! two nav groups' tab logic, loaded in that order (each may call functions
//! defined earlier, since plain `<script src>` tags share one global scope).
//!
//! Read-only for now — the gated operator actions (split/flush/compact/
//! reconfigure/drain) and the ADR 0018 transaction view are the next increments.

/// The dashboard single-page app shell, embedded at compile time.
pub(crate) const HTML: &str = include_str!("dashboard.html");
/// The dashboard's stylesheet.
pub(crate) const CSS: &str = include_str!("dashboard.css");
/// Shared state, fetch helpers, formatting utilities, and tab routing.
pub(crate) const CORE_JS: &str = include_str!("dashboard_core.js");
/// The Monitoring group: cluster-health strip, Nodes, Tablets, Storage.
pub(crate) const MONITORING_JS: &str = include_str!("dashboard_monitoring.js");
/// The Actions group: DynamoDB/CQL write forms and bulk seed.
pub(crate) const WRITE_JS: &str = include_str!("dashboard_write.js");
