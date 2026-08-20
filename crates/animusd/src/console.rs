//! The AnimusDB **Data Console** (ADR 0052): a DynamoDB-shaped data app for
//! application developers — browsing/querying/editing their own tables and
//! items — deliberately separate from the operator dashboard the admin port
//! serves (`dashboard.rs`, ADR 0021). Its defining rule, enforced structurally
//! here rather than just documented: **this listener never serves
//! cluster-shaped state** — no nodes, replicas, tablets, Raft, quorum,
//! leaders, placement, or health.
//!
//! PR1 enforced that by giving this module no [`crate::ClientCtx`] at all.
//! **This PR (the tables-list screen) keeps that guarantee while adding this
//! listener's first real JSON endpoint** — `GET /console/api/tables` — by
//! still never taking a `ClientCtx`, or `Metadata`, or any other
//! cluster-shaped type. Instead [`serve`] takes a [`TableSnapshotFn`]: a
//! plain `Fn() -> Vec<TableSummary>` closure built and owned entirely by the
//! caller (`lib.rs`'s `spawn_common_tail`/`console_table_summaries`), which
//! is the *only* code in this crate that reads `Metadata`'s schema catalog on
//! this listener's behalf. By the time a value crosses into this module it is
//! already a [`TableSummary`] — a name, a couple of typed key names, some
//! counts, and two booleans — so there is no `Metadata`/`TableSchema`/
//! `IndexKind`/etc. import anywhere in this file for a future change to
//! misuse. If a future screen here ever seems to need a cluster type, that is
//! the signal to add a narrower projection type instead, the same way this
//! one was added — never to widen this module's inputs back toward
//! `ClientCtx`.
//!
//! Two more screens (a table's own page, the create-table form) are still
//! follow-up PRs in this stack — a deep link to either already resolves to
//! this listener's shell (`is_shell_path`, unchanged since PR1), which the
//! client-side router in `console.js` renders as a plain "not built yet"
//! placeholder for any route it doesn't recognize.
//!
//! Embedded at compile time (`include_str!`), no bundler/build step/external
//! assets — the same constraints `dashboard.rs` documents for the operator
//! console.

use std::sync::Arc;

use serde::Serialize;
use tokio::net::{TcpListener, TcpStream};

use crate::http;

/// The console's page shell, embedded at compile time.
const HTML: &str = include_str!("console.html");
/// The console's stylesheet.
const CSS: &str = include_str!("console.css");
/// The console's client-side app (routing + the tables-list screen), vanilla
/// JS, no bundler — mirrors `dashboard.rs`'s `include_str!`'d asset shape.
const JS: &str = include_str!("console.js");

/// The tables-list endpoint's path — the console's first JSON route.
const TABLES_API_PATH: &str = "/console/api/tables";

/// One table's key shape, name + declared DynamoDB `AttributeType`
/// (`S`/`N`/`B`) — e.g. `{"name": "order_id", "attribute_type": "S"}` renders
/// as `order_id (S)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct KeySummary {
    pub(crate) name: String,
    pub(crate) attribute_type: String,
}

/// A table's DynamoDB Streams configuration, console-shaped: just whether one
/// is enabled and, if so, which view type — never a shard/segment/sealing
/// detail (all of that is cluster/consumer-internal, ADR 0042/0043).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct StreamSummary {
    pub(crate) enabled: bool,
    /// `Some` exactly when `enabled` — the DynamoDB wire label
    /// (`NEW_AND_OLD_IMAGES`/`NEW_IMAGE`/`OLD_IMAGE`/`KEYS_ONLY`).
    pub(crate) view_type: Option<String>,
}

/// A table's DynamoDB-style TTL configuration (ADR 0051), console-shaped:
/// whether it's enabled and, if so, which attribute holds the expiry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct TtlSummary {
    pub(crate) enabled: bool,
    /// `Some` exactly when `enabled`.
    pub(crate) attribute_name: Option<String>,
}

/// One user-visible table, projected for the tables-list screen. Plain,
/// fully owned data — no borrow, no cluster type reachable from any field.
///
/// `lsi_count` is `None`, not `Some(0)`, for a table with no sort key: an LSI
/// shares the base partition key and adds an alternate sort key, so a
/// hash-only table structurally cannot have one — that is a different fact
/// from "has a sort key, zero LSIs declared," and the console renders the two
/// differently (a dash vs. `0`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct TableSummary {
    pub(crate) name: String,
    pub(crate) partition_key: KeySummary,
    pub(crate) sort_key: Option<KeySummary>,
    pub(crate) gsi_count: u32,
    pub(crate) lsi_count: Option<u32>,
    pub(crate) stream: StreamSummary,
    pub(crate) ttl: TtlSummary,
}

/// A snapshot of the current user-visible tables, called fresh on every
/// `GET /console/api/tables` request. The **only** way this module reaches
/// outside itself — see the module doc for why it is a plain closure over
/// [`TableSummary`] rather than a `ClientCtx`/`Metadata` reference.
pub(crate) type TableSnapshotFn = Arc<dyn Fn() -> Vec<TableSummary> + Send + Sync>;

/// Accept loop for the console HTTP endpoint. One task per connection,
/// mirroring `admin::serve`/`dynamo::serve`'s own shape.
pub(crate) async fn serve(listener: TcpListener, tables: TableSnapshotFn) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let tables = tables.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_conn(stream, tables).await {
                        tracing::debug!(?err, "console connection closed");
                    }
                });
            }
            Err(err) => {
                tracing::warn!(?err, "console accept failed");
                return;
            }
        }
    }
}

async fn handle_conn(mut stream: TcpStream, tables: TableSnapshotFn) -> std::io::Result<()> {
    let mut buf = Vec::new();
    loop {
        let Some(request) = http::read_http_request(&mut stream, &mut buf).await? else {
            return Ok(()); // clean EOF
        };
        let keep_alive = request.keep_alive;
        if request.method != "GET" {
            http::write_response(&mut stream, 405, "text/plain", "GET only", keep_alive).await?;
            if !keep_alive {
                return Ok(());
            }
            continue;
        }
        // Static assets, checked by exact path FIRST (mirrors
        // `admin.rs::static_asset`'s own ordering note) — `is_shell_path`'s
        // `/console/ui/` prefix match would otherwise swallow these.
        if request.path == "/console/ui/console.css" {
            http::write_response(&mut stream, 200, "text/css; charset=utf-8", CSS, keep_alive)
                .await?;
        } else if request.path == "/console/ui/console.js" {
            http::write_response(
                &mut stream,
                200,
                "text/javascript; charset=utf-8",
                JS,
                keep_alive,
            )
            .await?;
        } else if request.path == TABLES_API_PATH {
            let summaries = tables();
            let body = tables_json(&summaries);
            http::write_response(&mut stream, 200, "application/json", &body, keep_alive).await?;
        } else if is_shell_path(&request.path) {
            http::write_response(
                &mut stream,
                200,
                "text/html; charset=utf-8",
                HTML,
                keep_alive,
            )
            .await?;
        } else {
            http::write_response(&mut stream, 404, "text/plain", "not found", keep_alive).await?;
        }
        if !keep_alive {
            return Ok(());
        }
    }
}

/// Encode the tables-list response body: `{"tables": [...]}`.
fn tables_json(tables: &[TableSummary]) -> String {
    serde_json::to_string(&serde_json::json!({ "tables": tables }))
        .unwrap_or_else(|_| "{\"tables\":[]}".to_string())
}

/// Whether `path` should serve the console shell — the root, a couple of
/// `/console` aliases, and any `/console/ui/<screen>` deep link (mirroring
/// `admin::is_ui_path`'s own shape): a bookmark/refresh of a screen's URL —
/// built or not — lands back on the shell instead of a 404, exactly like the
/// operator dashboard's own deep-link contract. The shell's own client-side
/// router (`console.js`) is what decides whether the path names a real
/// screen (the tables list) or an unbuilt one (a table's own page, the
/// create-table form).
fn is_shell_path(path: &str) -> bool {
    matches!(path, "/" | "/console" | "/console/" | "/console/ui")
        || path.starts_with("/console/ui/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_path_covers_root_aliases_and_deep_links() {
        assert!(is_shell_path("/"));
        assert!(is_shell_path("/console"));
        assert!(is_shell_path("/console/"));
        assert!(is_shell_path("/console/ui"));
        assert!(is_shell_path("/console/ui/tables"));
        assert!(is_shell_path("/console/ui/tables/orders"));
        assert!(!is_shell_path("/admin"));
        assert!(!is_shell_path("/consoleX"));
        // `is_shell_path` alone also matches the one static asset path
        // (`/console/ui/console.css`) — harmless, since `handle_conn`
        // checks the exact asset path FIRST and only falls through to this
        // predicate afterward, so the asset route always wins in practice.
        assert!(is_shell_path("/console/ui/console.css"));
        // Same for the JSON API path — not under `/console/ui/` at all, so
        // it never collides with the shell predicate in the first place.
        assert!(!is_shell_path(TABLES_API_PATH));
    }

    fn sample_table() -> TableSummary {
        TableSummary {
            name: "orders".into(),
            partition_key: KeySummary {
                name: "order_id".into(),
                attribute_type: "S".into(),
            },
            sort_key: Some(KeySummary {
                name: "created_at".into(),
                attribute_type: "N".into(),
            }),
            gsi_count: 2,
            lsi_count: Some(1),
            stream: StreamSummary {
                enabled: true,
                view_type: Some("NEW_AND_OLD_IMAGES".into()),
            },
            ttl: TtlSummary {
                enabled: true,
                attribute_name: Some("expiresAt".into()),
            },
        }
    }

    /// The JSON shape a table with every feature turned on renders as, and —
    /// the property most worth pinning here — that no field name anywhere in
    /// it is cluster-shaped (no node/tablet/replica/raft/leader/quorum/
    /// placement/health vocabulary). The full server response is proven the
    /// same way against a live cluster in `tests/console_tables.rs`; this is
    /// the type-level half of that same regression.
    #[test]
    fn table_summary_serializes_console_shaped_fields_only() {
        let json = serde_json::to_value(sample_table()).unwrap();
        assert_eq!(json["name"], "orders");
        assert_eq!(json["partition_key"]["name"], "order_id");
        assert_eq!(json["partition_key"]["attribute_type"], "S");
        assert_eq!(json["sort_key"]["name"], "created_at");
        assert_eq!(json["sort_key"]["attribute_type"], "N");
        assert_eq!(json["gsi_count"], 2);
        assert_eq!(json["lsi_count"], 1);
        assert_eq!(json["stream"]["enabled"], true);
        assert_eq!(json["stream"]["view_type"], "NEW_AND_OLD_IMAGES");
        assert_eq!(json["ttl"]["enabled"], true);
        assert_eq!(json["ttl"]["attribute_name"], "expiresAt");

        let text = json.to_string().to_ascii_lowercase();
        for forbidden in [
            "node",
            "tablet",
            "replica",
            "raft",
            "leader",
            "quorum",
            "placement",
            "health",
        ] {
            assert!(
                !text.contains(forbidden),
                "found cluster-shaped substring `{forbidden}` in {text}"
            );
        }
    }

    /// A hash-only table (no sort key) renders `sort_key: null` and —
    /// distinctly — `lsi_count: null` (structurally absent), never `0` (which
    /// would mean "has a sort key, zero LSIs declared").
    #[test]
    fn table_with_no_sort_key_has_no_lsi_count() {
        let mut table = sample_table();
        table.sort_key = None;
        table.lsi_count = None;
        table.gsi_count = 0;
        table.stream = StreamSummary {
            enabled: false,
            view_type: None,
        };
        table.ttl = TtlSummary {
            enabled: false,
            attribute_name: None,
        };
        let json = serde_json::to_value(table).unwrap();
        assert!(json["sort_key"].is_null());
        assert!(json["lsi_count"].is_null());
        assert_eq!(json["gsi_count"], 0, "a zero GSI count still renders as 0");
        assert_eq!(json["stream"]["enabled"], false);
        assert!(json["stream"]["view_type"].is_null());
        assert_eq!(json["ttl"]["enabled"], false);
        assert!(json["ttl"]["attribute_name"].is_null());
    }

    #[test]
    fn tables_json_wraps_the_list_under_a_tables_key() {
        let body = tables_json(&[sample_table()]);
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(value["tables"].is_array());
        assert_eq!(value["tables"].as_array().unwrap().len(), 1);
        assert_eq!(value["tables"][0]["name"], "orders");
    }

    #[test]
    fn tables_json_of_an_empty_catalog_is_an_empty_array() {
        let body = tables_json(&[]);
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["tables"].as_array().unwrap().len(), 0);
    }
}
