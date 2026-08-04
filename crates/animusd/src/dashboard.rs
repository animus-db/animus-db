//! The static web dashboard (ADR 0021): a self-contained single-page app served
//! from the admin port for manual cluster testing — nodes, tablets, WALs, data.
//!
//! It is embedded in the binary (`include_str!`) and served verbatim as
//! `text/html` by [`crate::admin`]; it is a pure **client** of the `/admin/*` JSON
//! surface (ADR 0020), assembling a cluster-wide view by fanning out from the
//! browser to every node's admin port (seeded by `GET /admin/peers`). No build
//! toolchain, no bundler, no external assets — vanilla HTML/CSS/JS, so the asset
//! ships with the binary and the build stays `cargo`-only (ADR 0021 §1; a vendored
//! single-file library is allowed later but not used here).
//!
//! Read-only for now — the gated operator actions (split/flush/compact/
//! reconfigure/drain) and the ADR 0018 transaction view are the next increments.

/// The dashboard single-page app, embedded at compile time.
pub(crate) const HTML: &str = include_str!("dashboard.html");
