//! The AnimusDB **Data Console** (ADR 0052): a DynamoDB-shaped data app for
//! application developers — browsing/querying/editing their own tables and
//! items — deliberately separate from the operator dashboard the admin port
//! serves (`dashboard.rs`, ADR 0021). Its defining rule, enforced structurally
//! here rather than just documented: **this listener never serves
//! cluster-shaped state** — no nodes, replicas, tablets, Raft, quorum,
//! leaders, placement, or health.
//!
//! PR1 enforced that by giving this module no [`crate::ClientCtx`] at all.
//! PR2 (the tables-list screen) kept that guarantee while adding this
//! listener's first real JSON endpoint — `GET /console/api/tables` — via a
//! plain `TableSnapshotFn` closure `lib.rs` owns.
//!
//! **This PR (the table's own page, Config tab) needs more than schema
//! reads — it mutates a table's GSIs/stream/TTL and can delete the table
//! outright — so the injected seam widens from one closure to a small
//! [`ConsoleBackend`] trait.** The widening is in *shape* (async, several
//! methods) only, never in *kind*: every method still takes and returns
//! nothing but plain owned console types (`&str` table/index names,
//! [`TableDetail`], [`AddGsiRequest`], [`ConsoleError`], …) — never
//! `ClientCtx`, `Metadata`, `TableSchema`, `IndexDef`, or any other
//! cluster/schema-catalog type. `lib.rs` is still the **only** code that
//! implements this trait (on `ClientCtx`, so every method has the real
//! control-plane/CP-data primitives to call) and the only code that ever
//! imports a schema-catalog type on the console's behalf — this module
//! itself imports none. If a future screen here ever seems to need a
//! cluster type, that is the signal to add a narrower method/projection
//! type instead, the same way every method here was added — never to widen
//! this module's inputs back toward `ClientCtx`.
//!
//! Two more screens (the Items tab, the Stream tab) and the create-table
//! form are still follow-up PRs in this stack — see `is_shell_path`'s doc.
//!
//! Embedded at compile time (`include_str!`), no bundler/build step/external
//! assets — the same constraints `dashboard.rs` documents for the operator
//! console.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};

use crate::http;

/// The console's page shell, embedded at compile time.
const HTML: &str = include_str!("console.html");
/// The console's stylesheet.
const CSS: &str = include_str!("console.css");
/// The console's client-side app (routing + every screen), vanilla JS, no
/// bundler — mirrors `dashboard.rs`'s `include_str!`'d asset shape.
const JS: &str = include_str!("console.js");

/// The tables-list endpoint's path — the console's first JSON route.
const TABLES_API_PATH: &str = "/console/api/tables";
/// Prefix for every per-table endpoint (`{TABLES_API_PATH}/{name}[/...]`).
const TABLES_API_PREFIX: &str = "/console/api/tables/";

/// One table's key shape, name + declared DynamoDB `AttributeType`
/// (`S`/`N`/`B`) — e.g. `{"name": "order_id", "attribute_type": "S"}` renders
/// as `order_id (S)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct KeySummary {
    pub(crate) name: String,
    pub(crate) attribute_type: String,
}

/// One *index* key attribute's shape. Unlike [`KeySummary`] the type is
/// `Option`: an index's key attribute has no declared type of its own
/// anywhere in the catalog — `animus_control::IndexDef` stores only the
/// attribute *name*. A type is therefore knowable only when that same
/// attribute also happens to be a declared column of the base table, which
/// is the case for an index declared at `CreateTable` (its attributes come
/// in through `AttributeDefinitions`) and **not** the case for a GSI added
/// later through `UpdateTable`, whose `GlobalSecondaryIndexUpdates` decoder
/// ignores `AttributeDefinitions` entirely (issue #319). `None` renders as
/// a bare attribute name rather than a fabricated `(S)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct IndexKeySummary {
    pub(crate) name: String,
    pub(crate) attribute_type: Option<String>,
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
/// `GET /console/api/tables` request. See the module doc for why this stays
/// a plain closure (PR2's own seam) rather than folding into
/// [`ConsoleBackend`] (PR3's seam, added alongside it for the mutating/
/// per-table endpoints): the tables list needs no table-name parameter and
/// nothing here ever needs to fail, so a bare `Fn` stays the simplest shape
/// for it.
pub(crate) type TableSnapshotFn = Arc<dyn Fn() -> Vec<TableSummary> + Send + Sync>;

/// One global secondary index, console-shaped for the table detail screen:
/// its keys and its lifecycle status (ADR 0045) — never its hidden
/// materialization table's own tablet/replica placement, which is exactly
/// the cluster-shaped detail this console must never surface. `status` is a
/// plain wire-label string (`"CREATING"`/`"ACTIVE"`/`"DELETING"`) rather than
/// `animus_control::IndexStatus` itself — this module never imports that
/// type at all (see the module doc); `lib.rs` renders the label.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct GsiDetail {
    pub(crate) name: String,
    pub(crate) hash_attribute: IndexKeySummary,
    pub(crate) sort_attribute: Option<IndexKeySummary>,
    pub(crate) status: String,
}

/// One local secondary index, console-shaped: just its own alternate sort
/// key. **Deliberately no `status`/no hash key** — an LSI shares the base
/// table's partition key and its own storage scope (never a separate
/// materialized table the way a GSI is), and it is create-time-only in
/// DynamoDB, so it has no lifecycle to report and nothing to drop. The UI
/// must not reuse [`GsiDetail`]'s row template for these — see the Config
/// tab's Indexes section.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct LsiDetail {
    pub(crate) name: String,
    pub(crate) sort_attribute: IndexKeySummary,
}

/// One table's full configuration, for the table page's Config tab
/// (`GET /console/api/tables/{name}`). Everything [`TableSummary`] carries as
/// a count instead carries its full declaration here (every GSI, every LSI).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct TableDetail {
    pub(crate) name: String,
    pub(crate) partition_key: KeySummary,
    pub(crate) sort_key: Option<KeySummary>,
    pub(crate) gsis: Vec<GsiDetail>,
    pub(crate) lsis: Vec<LsiDetail>,
    pub(crate) stream: StreamSummary,
    pub(crate) ttl: TtlSummary,
}

/// A request to add a global secondary index (`POST
/// .../gsi`) — decoded straight off the client's JSON body. `hash_attribute`/
/// `sort_attribute` are free text (an attribute name is per-item, never a
/// closed set — see the module doc on why the UI must not offer a picker).
///
/// **No attribute type.** DynamoDB's own `UpdateTable` carries one in
/// `AttributeDefinitions`, but this adapter's decoder for
/// `GlobalSecondaryIndexUpdates` never reads it (issue #319), so a type sent
/// here would be silently discarded and the index would still read back
/// untyped. Rather than offer a control whose value cannot survive the round
/// trip, the console asks for the name alone; restore the type here (and the
/// picker in `console.js`) once #319 makes it durable.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct AddGsiRequest {
    pub(crate) index_name: String,
    pub(crate) hash_attribute: String,
    #[serde(default)]
    pub(crate) sort_attribute: Option<String>,
}

/// A request to enable/disable a table's stream (`POST .../stream`).
/// `view_type` is required exactly when `enabled` (checked by the backend,
/// same "required iff" shape DynamoDB's own `StreamSpecification` has).
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SetStreamRequest {
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) view_type: Option<String>,
}

/// A request to enable/disable/reconfigure a table's TTL (`POST .../ttl`).
/// `attribute_name` is required on both enable and disable (AWS's own
/// `UpdateTimeToLive` contract — a disable call still names the attribute
/// being disabled).
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SetTtlRequest {
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) attribute_name: Option<String>,
}

/// A console-shaped error: an HTTP status plus a human message — never a
/// `WireError`/cluster type. `lib.rs`'s [`ConsoleBackend`] impl translates
/// whatever underlying error it hit (a DynamoDB wire error, a control-plane
/// commit-wait timeout, a `drop_table` failure) into one of these.
#[derive(Clone, Debug)]
pub(crate) struct ConsoleError {
    pub(crate) status: u16,
    pub(crate) message: String,
}

impl ConsoleError {
    pub(crate) fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

/// The narrow, console-shaped seam for every endpoint beyond the tables
/// list — see the module doc for why this is a trait (not another bare
/// closure) and why widening it is still safe: every method's signature is
/// plain owned console types in, plain owned console types (or
/// [`ConsoleError`]) out. `lib.rs` is the one implementor (on `ClientCtx`)
/// and the one place a schema-catalog type is ever in scope while building
/// one of these methods' return values.
#[async_trait::async_trait]
pub(crate) trait ConsoleBackend: Send + Sync {
    /// One table's full configuration, or `None` if no such table exists
    /// (rendered as a 404) — used by the table page's Config tab.
    async fn table_detail(&self, table: &str) -> Option<TableDetail>;

    /// Add a global secondary index to `table` (ADR 0045 §2/§6) — routes
    /// through the same `UpdateTable` path a real DynamoDB client would use
    /// (see the module doc). Returns the newly declared index, typically
    /// `status: "CREATING"` (it flips to `"ACTIVE"` once the backfill
    /// converges, observed on this same table's next `table_detail` poll).
    async fn add_gsi(&self, table: &str, req: AddGsiRequest) -> Result<GsiDetail, ConsoleError>;

    /// Drop `index` from `table` (ADR 0045 §5's convergent drop cascade).
    /// Refuses a local index the same way the real `UpdateTable` wire path
    /// does (LSIs are create-time-only in DynamoDB — never droppable).
    async fn drop_gsi(&self, table: &str, index: &str) -> Result<(), ConsoleError>;

    /// Enable, change the view type of, or disable `table`'s stream.
    async fn set_stream(
        &self,
        table: &str,
        req: SetStreamRequest,
    ) -> Result<StreamSummary, ConsoleError>;

    /// Enable, reconfigure, or disable `table`'s TTL (ADR 0051).
    async fn set_ttl(&self, table: &str, req: SetTtlRequest) -> Result<TtlSummary, ConsoleError>;

    /// Delete `table` outright (its schema and every tablet, incl. every
    /// GSI's hidden table — the same cascade `admin.rs::action_drop_table`
    /// drives). **Not a DynamoDB wire operation** — DynamoDB itself has no
    /// `DeleteTable` in this adapter's supported subset, so this is the
    /// console's own delete primitive, same as the dashboard's.
    async fn delete_table(&self, table: &str) -> Result<(), ConsoleError>;
}

/// Accept loop for the console HTTP endpoint. One task per connection,
/// mirroring `admin::serve`/`dynamo::serve`'s own shape.
pub(crate) async fn serve(
    listener: TcpListener,
    tables: TableSnapshotFn,
    backend: Arc<dyn ConsoleBackend>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let tables = tables.clone();
                let backend = backend.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_conn(stream, tables, backend).await {
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

async fn handle_conn(
    mut stream: TcpStream,
    tables: TableSnapshotFn,
    backend: Arc<dyn ConsoleBackend>,
) -> std::io::Result<()> {
    let mut buf = Vec::new();
    loop {
        let Some(request) = http::read_http_request(&mut stream, &mut buf).await? else {
            return Ok(()); // clean EOF
        };
        let keep_alive = request.keep_alive;
        let (status, content_type, body) = route(&request, &tables, backend.as_ref()).await;
        http::write_response(&mut stream, status, content_type, &body, keep_alive).await?;
        if !keep_alive {
            return Ok(());
        }
    }
}

/// Resolve one parsed request to a `(status, content-type, body)` triple —
/// every route this listener serves, static assets and JSON API alike, in
/// one place so `handle_conn` stays a plain read/dispatch/write loop.
async fn route(
    request: &http::HttpRequest,
    tables: &TableSnapshotFn,
    backend: &dyn ConsoleBackend,
) -> (u16, &'static str, String) {
    let method = request.method.as_str();
    let path = request.path.as_str();

    // Static assets, checked by exact path FIRST (mirrors
    // `admin.rs::static_asset`'s own ordering note) — `is_shell_path`'s
    // `/console/ui/` prefix match would otherwise swallow these.
    if method == "GET" && path == "/console/ui/console.css" {
        return (200, "text/css; charset=utf-8", CSS.to_string());
    }
    if method == "GET" && path == "/console/ui/console.js" {
        return (200, "text/javascript; charset=utf-8", JS.to_string());
    }
    if method == "GET" && path == TABLES_API_PATH {
        let summaries = tables();
        return (200, "application/json", tables_json(&summaries));
    }

    if let Some(table_route) = parse_table_api_route(path) {
        return table_api_response(method, table_route, &request.body, backend).await;
    }

    if method != "GET" {
        return (405, "text/plain", "GET only".to_string());
    }
    if is_shell_path(path) {
        return (200, "text/html; charset=utf-8", HTML.to_string());
    }
    (404, "text/plain", "not found".to_string())
}

/// One `/console/api/tables/{name}[...]` request, already routed to its
/// [`TableApiRoute`] — dispatches to the matching [`ConsoleBackend`] method
/// and renders its result (or a decode/backend error) as JSON.
async fn table_api_response(
    method: &str,
    route: TableApiRoute,
    body: &[u8],
    backend: &dyn ConsoleBackend,
) -> (u16, &'static str, String) {
    match (method, route) {
        ("GET", TableApiRoute::Table(table)) => match backend.table_detail(&table).await {
            Some(detail) => (200, "application/json", table_detail_json(&detail)),
            None => (404, "application/json", error_json("no such table")),
        },
        ("DELETE", TableApiRoute::Table(table)) => match backend.delete_table(&table).await {
            Ok(()) => (200, "application/json", ok_json()),
            Err(e) => (e.status, "application/json", error_json(&e.message)),
        },
        ("POST", TableApiRoute::Gsi(table)) => match parse_json_body::<AddGsiRequest>(body) {
            Ok(req) => match backend.add_gsi(&table, req).await {
                Ok(gsi) => (
                    200,
                    "application/json",
                    wrap_json("gsi", serde_json::to_value(gsi).unwrap_or_default()),
                ),
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            },
            Err(e) => (e.status, "application/json", error_json(&e.message)),
        },
        ("DELETE", TableApiRoute::GsiNamed(table, index)) => {
            match backend.drop_gsi(&table, &index).await {
                Ok(()) => (200, "application/json", ok_json()),
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            }
        }
        ("POST", TableApiRoute::Stream(table)) => match parse_json_body::<SetStreamRequest>(body) {
            Ok(req) => match backend.set_stream(&table, req).await {
                Ok(s) => (
                    200,
                    "application/json",
                    wrap_json("stream", serde_json::to_value(s).unwrap_or_default()),
                ),
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            },
            Err(e) => (e.status, "application/json", error_json(&e.message)),
        },
        ("POST", TableApiRoute::Ttl(table)) => match parse_json_body::<SetTtlRequest>(body) {
            Ok(req) => match backend.set_ttl(&table, req).await {
                Ok(t) => (
                    200,
                    "application/json",
                    wrap_json("ttl", serde_json::to_value(t).unwrap_or_default()),
                ),
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            },
            Err(e) => (e.status, "application/json", error_json(&e.message)),
        },
        _ => (405, "application/json", error_json("method not allowed")),
    }
}

/// A decoded `/console/api/tables/{name}[...]` path, one variant per
/// resource this listener exposes beneath a table. Parsed once
/// ([`parse_table_api_route`]) and then matched against the request method
/// in [`table_api_response`] — an unrecognized method for a recognized
/// route falls to that match's own 405 arm, never here.
enum TableApiRoute {
    /// `/console/api/tables/{name}` — `GET` (detail) or `DELETE` (delete
    /// the table).
    Table(String),
    /// `/console/api/tables/{name}/gsi` — `POST` (add a GSI).
    Gsi(String),
    /// `/console/api/tables/{name}/gsi/{index}` — `DELETE` (drop a GSI).
    GsiNamed(String, String),
    /// `/console/api/tables/{name}/stream` — `POST` (set the stream).
    Stream(String),
    /// `/console/api/tables/{name}/ttl` — `POST` (set TTL).
    Ttl(String),
}

/// Parse a path under [`TABLES_API_PREFIX`] into a [`TableApiRoute`], or
/// `None` if it names neither a table nor one of its known sub-resources
/// (falls through to the shell/404 handling in [`route`], same as any other
/// unrecognized path). Table and index names are percent-decoded (mirrors
/// `console.js::tableHref`'s `encodeURIComponent` on the way in).
fn parse_table_api_route(path: &str) -> Option<TableApiRoute> {
    let rest = path.strip_prefix(TABLES_API_PREFIX)?;
    let mut parts = rest.splitn(2, '/');
    let table = http::percent_decode(parts.next().unwrap_or(""));
    if table.is_empty() {
        return None;
    }
    match parts.next() {
        None => Some(TableApiRoute::Table(table)),
        Some("gsi") => Some(TableApiRoute::Gsi(table)),
        Some(tail) => match tail.strip_prefix("gsi/") {
            Some(index) if !index.is_empty() => {
                Some(TableApiRoute::GsiNamed(table, http::percent_decode(index)))
            }
            _ if tail == "stream" => Some(TableApiRoute::Stream(table)),
            _ if tail == "ttl" => Some(TableApiRoute::Ttl(table)),
            _ => None,
        },
    }
}

/// Decode a JSON request body into `T`, mapping a decode failure to a `400`
/// [`ConsoleError`] — every `POST` endpoint's first step.
fn parse_json_body<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, ConsoleError> {
    serde_json::from_slice(body)
        .map_err(|e| ConsoleError::new(400, format!("invalid JSON body: {e}")))
}

/// Encode the tables-list response body: `{"tables": [...]}`.
fn tables_json(tables: &[TableSummary]) -> String {
    serde_json::to_string(&serde_json::json!({ "tables": tables }))
        .unwrap_or_else(|_| "{\"tables\":[]}".to_string())
}

/// Encode a [`TableDetail`] as the top-level response body (a single
/// resource, unlike the tables list — no wrapping key).
fn table_detail_json(detail: &TableDetail) -> String {
    serde_json::to_string(detail).unwrap_or_else(|_| "{}".to_string())
}

/// Wrap one JSON value under `key` — the `{"gsi": ...}`/`{"stream":
/// ...}`/`{"ttl": ...}` response shape every mutating endpoint but
/// delete/drop uses (those two use [`ok_json`] instead, since there is no
/// resource left to describe).
fn wrap_json(key: &str, value: serde_json::Value) -> String {
    serde_json::to_string(&serde_json::json!({ key: value })).unwrap_or_else(|_| "{}".to_string())
}

/// The body of a bare-success response with nothing else to report (drop a
/// GSI, delete a table).
fn ok_json() -> String {
    "{\"ok\":true}".to_string()
}

/// Encode a `{"error": message}` body — the one error shape every endpoint
/// on this listener uses.
fn error_json(message: &str) -> String {
    serde_json::to_string(&serde_json::json!({ "error": message }))
        .unwrap_or_else(|_| "{\"error\":\"internal error\"}".to_string())
}

/// Whether `path` should serve the console shell — the root, a couple of
/// `/console` aliases, and any `/console/ui/<screen>` deep link (mirroring
/// `admin::is_ui_path`'s own shape): a bookmark/refresh of a screen's URL —
/// built or not — lands back on the shell instead of a 404, exactly like the
/// operator dashboard's own deep-link contract. The shell's own client-side
/// router (`console.js`) is what decides whether the path names a real
/// screen (the tables list, a table's own page) or an unbuilt one (the
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
        // Same for the JSON API paths — not under `/console/ui/` at all, so
        // they never collide with the shell predicate in the first place.
        assert!(!is_shell_path(TABLES_API_PATH));
        assert!(!is_shell_path("/console/api/tables/orders"));
    }

    #[test]
    fn table_api_route_parses_every_shape() {
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders"),
            Some(TableApiRoute::Table(t)) if t == "orders"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/gsi"),
            Some(TableApiRoute::Gsi(t)) if t == "orders"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/gsi/by-status"),
            Some(TableApiRoute::GsiNamed(t, i)) if t == "orders" && i == "by-status"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/stream"),
            Some(TableApiRoute::Stream(t)) if t == "orders"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/ttl"),
            Some(TableApiRoute::Ttl(t)) if t == "orders"
        ));
        // A table name that needed percent-encoding round-trips.
        assert!(matches!(
            parse_table_api_route("/console/api/tables/a%20b"),
            Some(TableApiRoute::Table(t)) if t == "a b"
        ));
        // Not a per-table route at all (the list endpoint has no trailing
        // slash) and an unrecognized sub-resource both fall through.
        assert!(parse_table_api_route("/console/api/tables").is_none());
        assert!(parse_table_api_route("/console/api/tables/").is_none());
        assert!(parse_table_api_route("/console/api/tables/orders/bogus").is_none());
        assert!(parse_table_api_route("/console/api/tables/orders/gsi/").is_none());
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

    fn sample_detail() -> TableDetail {
        TableDetail {
            name: "orders".into(),
            partition_key: KeySummary {
                name: "order_id".into(),
                attribute_type: "S".into(),
            },
            sort_key: Some(KeySummary {
                name: "created_at".into(),
                attribute_type: "N".into(),
            }),
            gsis: vec![GsiDetail {
                name: "by-status".into(),
                hash_attribute: IndexKeySummary {
                    name: "status".into(),
                    // `None` on purpose: this is the shape a GSI added
                    // through `UpdateTable` really has (issue #319).
                    attribute_type: None,
                },
                sort_attribute: None,
                status: "CREATING".into(),
            }],
            lsis: vec![LsiDetail {
                name: "by-score".into(),
                sort_attribute: IndexKeySummary {
                    name: "score".into(),
                    attribute_type: Some("N".into()),
                },
            }],
            stream: StreamSummary {
                enabled: true,
                view_type: Some("NEW_IMAGE".into()),
            },
            ttl: TtlSummary {
                enabled: false,
                attribute_name: None,
            },
        }
    }

    /// The table-detail JSON shape — again pinning the no-cluster-shape
    /// property, this time including the GSI/LSI arrays (an LSI carries no
    /// `status`/hash-attribute field at all, distinct from a GSI's row
    /// shape, per the module doc on why the two must not share a template).
    #[test]
    fn table_detail_serializes_console_shaped_fields_only() {
        let json = serde_json::to_value(sample_detail()).unwrap();
        assert_eq!(json["gsis"][0]["name"], "by-status");
        assert_eq!(json["gsis"][0]["status"], "CREATING");
        assert!(json["gsis"][0]["sort_attribute"].is_null());
        assert_eq!(json["lsis"][0]["name"], "by-score");
        assert_eq!(json["lsis"][0]["sort_attribute"]["name"], "score");
        assert!(
            json["lsis"][0].get("status").is_none(),
            "an LSI row carries no lifecycle status field at all"
        );

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

    #[test]
    fn table_detail_json_is_a_bare_object_not_wrapped() {
        let body = table_detail_json(&sample_detail());
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["name"], "orders");
        assert!(value.get("table").is_none(), "no extra wrapping key");
    }

    #[test]
    fn error_json_carries_the_message_under_error() {
        let body = error_json("no such table");
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["error"], "no such table");
    }

    #[test]
    fn wrap_json_nests_under_the_given_key() {
        let body = wrap_json("gsi", serde_json::json!({"name": "by-status"}));
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["gsi"]["name"], "by-status");
    }
}
