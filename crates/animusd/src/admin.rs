//! The admin / debug interface (ADR 0020): a read-only introspection surface
//! plus a small set of gated operator actions, served as HTTP/JSON on the node's
//! **dedicated admin port** (`RoleAddrs.admin`) — isolated from the client/dynamo/
//! cql data edges so a deployment can firewall it off or bind it to a management
//! interface.
//!
//! Like the DynamoDB edge it is a *production-only I/O edge* (real tokio sockets +
//! the hand-rolled HTTP/1.1 helpers in [`crate::http`]); below the edge it only
//! **reads** node state (control + CP Raft, LSM/WAL) aggregated live at request
//! time, or drives an explicit, gated action. There is no auth yet — the
//! dedicated-port choice is what makes adding it later clean (assume the port is
//! bound to a trusted interface for now).
//!
//! It also serves the static **web dashboard** (ADR 0021) on `GET /` (and the
//! `/admin`, `/admin/ui` aliases): a self-contained single-page app that renders
//! the JSON below across the whole cluster via client-side fan-out, with CORS
//! enabled on every `/admin/*` response so the page (loaded from one node) can
//! read the others.
//!
//! Routes (`GET` read-only, `POST` actions):
//!
//! - `GET  /`                          — the web dashboard SPA (also `/admin/ui`)
//! - `GET  /admin/config`              — this node's ids, addresses, peers
//! - `GET  /admin/peers`              — every node's admin address (fan-out seed)
//! - `GET  /admin/status`              — the full replicated `Metadata`
//! - `GET  /admin/raft`                — control-plane Raft state
//! - `GET  /admin/raftkv`              — per hosted CP group Raft state
//! - `GET  /admin/storage/lsm`         — LSM levels / SSTables / memtable (`?tablet=`)
//! - `GET  /admin/storage/wal`         — WAL segments + sizes (`?tablet=`)
//! - `GET  /admin/storage/wal/segment` — decoded WAL records (`?tablet=&seg=`)
//! - `GET  /admin/storage/key`         — on-disk versions of a key (`?tablet=&key=`)
//! - `GET  /admin/storage/scan`        — first N live pairs (`?tablet=&start=&limit=`)
//! - `GET  /admin/metrics`             — the metrics snapshot as JSON
//! - `GET  /admin/metrics/history`     — periodic snapshots, ~2h ring buffer (ADR 0021 sparklines)
//! - `GET  /admin/health`              — liveness/readiness
//! - `POST /admin/tablet/split`        — `{tablet, split_key}`
//! - `POST /admin/storage/flush`       — `{tablet}`
//! - `POST /admin/storage/compact`     — `{tablet}`
//! - `POST /admin/raftkv/reconfigure`  — `{tablet, voters}`
//! - `POST /admin/drain`               — `{node}`
//! - `POST /admin/data/dynamo`         — run a DynamoDB op `{op, payload}` (ADR 0021)
//! - `POST /admin/data/cql`            — run CQL `{query, keyspace?}` (ADR 0021)
//! - `POST /admin/data/drop-table`     — drop a table's schema `{table}` (ADR 0021)
//! - `POST /admin/data/seed`           — bulk-write synthetic keys `{count, …}` (ADR 0021)

use std::time::Duration;

use animus_dynamo::wire::{base64url_decode, base64url_encode};
use animus_env::NodeId;
use animus_storage::WalRecordView;
use animus_tablet::{TOKEN_BYTES, TabletId, escape, partition_token};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tracing::Instrument;

use crate::http;
use crate::{ClientCtx, ClientResponse};

/// This group's Raft state for the `/admin/raftkv` view (ADR 0020). Built by
/// [`crate::CpGroup::raft_view`] over either engine arm.
#[derive(serde::Serialize)]
pub(crate) struct CpRaftView {
    pub(crate) tablet: u64,
    /// The `raftkv` id of the node hosting this replica (since ADR 0026 Stage
    /// B / ADR 0028 a tablet's CP group member id **is** simply this base id —
    /// no derived-id translation needed). Needed because under `--cluster N`
    /// every node's `/admin/raftkv` response lists **every** hosted replica
    /// across the whole cluster (the shared `ClusterEdgeState`, see its doc),
    /// so a client cannot infer which physical node a group belongs to from
    /// which admin port answered the request.
    pub(crate) node: NodeId,
    pub(crate) backend: &'static str,
    pub(crate) role: String,
    pub(crate) is_leader: bool,
    pub(crate) leader: Option<NodeId>,
    pub(crate) term: u64,
    pub(crate) commit_index: u64,
    pub(crate) last_applied: u64,
    pub(crate) durable_index: u64,
    pub(crate) snapshot_index: u64,
    pub(crate) log_len: usize,
    pub(crate) voters: Vec<NodeId>,
    /// This tablet's exact, `StorageScope`-scoped live key count
    /// (`CpGroup::raft_view`, via `local_pairs`) — distinct from the cheap,
    /// unscoped estimate `auto_split_loop` checks against `--auto-split K`
    /// (`CpGroup::approx_key_count`), which reads the whole shared engine and
    /// so double-counts a co-resident sibling tablet (e.g. right after a
    /// split). Always `Some` — both backends can be scanned.
    pub(crate) key_count: Option<usize>,
}

/// Accept loop for the admin HTTP endpoint. One task per connection; HTTP/1.1
/// keep-alive lets a client reuse the connection (mirrors `dynamo::serve`).
pub(crate) async fn serve(listener: TcpListener, ctx: ClientCtx) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_conn(stream, ctx).await {
                        tracing::debug!(?err, "admin connection closed");
                    }
                });
            }
            Err(err) => {
                tracing::warn!(?err, "admin accept failed");
                return;
            }
        }
    }
}

async fn handle_conn(mut stream: TcpStream, ctx: ClientCtx) -> std::io::Result<()> {
    let mut buf = Vec::new();
    loop {
        let Some(request) = http::read_http_request(&mut stream, &mut buf).await? else {
            return Ok(()); // clean EOF
        };
        let keep_alive = request.keep_alive;
        // CORS preflight: the dashboard's cross-node fan-out (ADR 0021) may send an
        // `OPTIONS` before a `POST` action. Answer it with the CORS headers + no body.
        if request.method == "OPTIONS" {
            http::write_response_with(
                &mut stream,
                204,
                "text/plain",
                "",
                keep_alive,
                http::CORS_HEADERS,
            )
            .await?;
            if !keep_alive {
                return Ok(());
            }
            continue;
        }
        // The dashboard's CSS/JS assets (ADR 0021), served self-contained from the
        // admin port under exact paths. Checked before `is_ui_path` below, since
        // that function's `/admin/ui/` prefix match would otherwise swallow these.
        if request.method == "GET" {
            if let Some((content_type, body)) = static_asset(&request.path) {
                http::write_response_with(
                    &mut stream,
                    200,
                    content_type,
                    body,
                    keep_alive,
                    http::CORS_HEADERS,
                )
                .await?;
                if !keep_alive {
                    return Ok(());
                }
                continue;
            }
        }
        // The static web dashboard asset (ADR 0021), served self-contained from the
        // admin port. A pure client of the `/admin/*` JSON below.
        if request.method == "GET" && is_ui_path(&request.path) {
            http::write_response_with(
                &mut stream,
                200,
                "text/html; charset=utf-8",
                crate::dashboard::HTML,
                keep_alive,
                http::CORS_HEADERS,
            )
            .await?;
            if !keep_alive {
                return Ok(());
            }
            continue;
        }
        let (status, body) = dispatch(&ctx, &request).await;
        http::write_response_with(
            &mut stream,
            status,
            "application/json",
            &body,
            keep_alive,
            http::CORS_HEADERS,
        )
        .await?;
        if !keep_alive {
            return Ok(());
        }
    }
}

/// Whether `path` should serve the dashboard SPA (ADR 0021). The root, a
/// couple of `/admin` aliases, and any `/admin/ui/<tab>` deep link all return the
/// single-page app — the client reads `location.pathname` to pick the active tab,
/// so a refresh or a bookmark of e.g. `/admin/ui/tablets` lands back on that tab
/// instead of always resetting to the default.
fn is_ui_path(path: &str) -> bool {
    matches!(
        path,
        "/" | "/admin" | "/admin/" | "/admin/ui" | "/index.html"
    ) || path.starts_with("/admin/ui/")
}

/// The dashboard's non-HTML static assets — CSS/JS files that live under the
/// same `/admin/ui/` prefix as the tab deep links, but name a real file rather
/// than a tab, so they need an exact match checked *before* [`is_ui_path`]'s
/// prefix match (which would otherwise serve the HTML shell for these paths
/// too). Returns `(content_type, body)`.
fn static_asset(path: &str) -> Option<(&'static str, &'static str)> {
    const JS: &str = "text/javascript; charset=utf-8";
    match path {
        "/admin/ui/dashboard.css" => Some(("text/css; charset=utf-8", crate::dashboard::CSS)),
        "/admin/ui/dashboard_core.js" => Some((JS, crate::dashboard::CORE_JS)),
        "/admin/ui/dashboard_overview.js" => Some((JS, crate::dashboard::OVERVIEW_JS)),
        "/admin/ui/dashboard_placement.js" => Some((JS, crate::dashboard::PLACEMENT_JS)),
        "/admin/ui/dashboard_tablets.js" => Some((JS, crate::dashboard::TABLETS_JS)),
        "/admin/ui/dashboard_browser.js" => Some((JS, crate::dashboard::BROWSER_JS)),
        "/admin/ui/dashboard_storage.js" => Some((JS, crate::dashboard::STORAGE_JS)),
        _ => None,
    }
}

/// Route a request to its handler, returning `(http status, json body)`.
async fn dispatch(ctx: &ClientCtx, request: &http::HttpRequest) -> (u16, String) {
    let method = request.method.as_str();
    let path = request.path.as_str();
    let q = request.query.as_str();
    let (status, value): (u16, Value) = match (method, path) {
        ("GET", "/admin/config") => (200, config_view(ctx)),
        ("GET", "/admin/peers") => (200, peers_view(ctx)),
        ("GET", "/admin/status") => (
            200,
            serde_json::to_value(ctx.raft.metadata()).unwrap_or(Value::Null),
        ),
        ("GET", "/admin/raft") => (200, raft_view(ctx)),
        ("GET", "/admin/raftkv") => (200, raftkv_view(ctx).await),
        ("GET", "/admin/storage/lsm") => storage_lsm(ctx, q).await,
        ("GET", "/admin/storage/wal") => storage_wal(ctx, q).await,
        ("GET", "/admin/storage/wal/segment") => storage_wal_segment(ctx, q).await,
        ("GET", "/admin/storage/key") => storage_key(ctx, q).await,
        ("GET", "/admin/storage/scan") => storage_scan(ctx, q).await,
        ("GET", "/admin/metrics") => (200, metrics_view(ctx)),
        ("GET", "/admin/metrics/history") => (200, metrics_history_view(ctx)),
        ("GET", "/admin/health") => health(ctx),
        ("POST", "/admin/tablet/split") => action_split(ctx, &request.body).await,
        ("POST", "/admin/storage/flush") => action_flush(ctx, &request.body).await,
        ("POST", "/admin/storage/compact") => action_compact(ctx, &request.body).await,
        ("POST", "/admin/raftkv/reconfigure") => action_reconfigure(ctx, &request.body),
        ("POST", "/admin/drain") => action_drain(ctx, &request.body),
        ("POST", "/admin/data/dynamo") => action_data_dynamo(ctx, &request.body).await,
        ("POST", "/admin/data/cql") => action_data_cql(ctx, &request.body).await,
        ("POST", "/admin/data/drop-table") => action_drop_table(ctx, &request.body).await,
        ("POST", "/admin/data/seed") => action_data_seed(ctx, &request.body).await,
        // A known admin path with the wrong verb vs an unknown path.
        ("GET" | "POST", p) if p.starts_with("/admin/") => {
            (404, json!({"error": format!("unknown admin route {p}")}))
        }
        _ => (
            404,
            json!({"error": "not found; admin routes live under /admin/"}),
        ),
    };
    (
        status,
        serde_json::to_string_pretty(&value).unwrap_or_default(),
    )
}

// ---- read-only views ----------------------------------------------------

fn config_view(ctx: &ClientCtx) -> Value {
    let a = &ctx.admin;
    let meta = ctx.raft.metadata();
    let peers: std::collections::BTreeMap<String, String> = a
        .peers
        .iter()
        .map(|(id, addr)| (id.to_string(), addr.to_string()))
        .collect();
    json!({
        "control_id": a.control_id,
        "raftkv_id": a.raftkv_id,
        "control_ids": a.control_ids,
        "addrs": {
            "control": a.control_addr.to_string(),
            "client": a.client_addr.to_string(),
            "dynamo": a.dynamo_addr.to_string(),
            "cql": a.cql_addr.to_string(),
            "raftkv": a.raftkv_addr.to_string(),
            "admin": a.admin_addr.to_string(),
        },
        "peers": peers,
        "cp_member_addrs": meta.cp_member_addrs,
        "auto_split_threshold": a.auto_split_threshold,
    })
}

/// The admin addresses of every node in the cluster (ADR 0021) — the seed list
/// the web dashboard fans out to. Each `animusd` process knows the whole cluster's
/// addresses (from its `ClusterConfig` in per-process mode, or the in-process
/// bring-up), so the dashboard need not guess ports. `this` marks the node serving
/// the page. Degrades to just this node's address when the full set is unknown.
fn peers_view(ctx: &ClientCtx) -> Value {
    let a = &ctx.admin;
    let admin_addrs: Vec<String> = a.admin_addrs.iter().map(ToString::to_string).collect();
    json!({
        "this": a.admin_addr.to_string(),
        "admin_addrs": admin_addrs,
    })
}

fn raft_view(ctx: &ClientCtx) -> Value {
    let r = &ctx.raft;
    let meta = r.metadata();
    let members: Vec<Value> = meta
        .members
        .keys()
        .map(|id| json!({"node": id, "believes_alive": r.believes_alive(*id)}))
        .collect();
    json!({
        "role": format!("{:?}", r.role()),
        "is_leader": r.is_leader(),
        "term": r.term(),
        "leader": r.leader(),
        "commit_index": r.commit_index(),
        "last_applied": r.last_applied(),
        "durable_index": r.durable_index(),
        "snapshot_index": r.snapshot_index(),
        "log_len": r.log_len(),
        "voters": r.config().into_iter().collect::<Vec<_>>(),
        "members": members,
    })
}

async fn raftkv_view(ctx: &ClientCtx) -> Value {
    let mut groups: Vec<CpRaftView> = Vec::new();
    for (t, g) in ctx.edge.hosted_groups() {
        groups.push(g.raft_view(t).await);
    }
    json!({ "hosts_cp": !groups.is_empty(), "groups": groups })
}

async fn storage_lsm(ctx: &ClientCtx, q: &str) -> (u16, Value) {
    let tablet = tablet_param(q);
    let Some(g) = ctx.edge.local_cp(tablet) else {
        return not_hosted(tablet);
    };
    let Some(views) = g.lsm_sstables() else {
        return (
            200,
            json!({"tablet": tablet.0, "backend": g.backend_name()}),
        );
    };
    let mut levels: std::collections::BTreeMap<u32, usize> = std::collections::BTreeMap::new();
    for s in &views {
        *levels.entry(s.level).or_default() += 1;
    }
    let levels: Vec<Value> = levels
        .into_iter()
        .map(|(level, tables)| json!({"level": level, "tables": tables}))
        .collect();
    let sstables: Vec<Value> = views
        .iter()
        .map(|s| {
            json!({
                "seq": s.seq,
                "level": s.level,
                "min_key": s.min_key.as_deref().map(key_display),
                "max_key": s.max_key.as_deref().map(key_display),
                "min_version": s.min_version,
                "max_version": s.max_version,
                "file_size": s.file_size,
                "has_bloom": s.has_bloom,
                "format": s.format,
            })
        })
        .collect();
    let (mt_keys, mt_bytes) = g.lsm_memtable().unwrap_or((0, 0));
    (
        200,
        json!({
            "tablet": tablet.0,
            "backend": g.backend_name(),
            "levels": levels,
            "sstables": sstables,
            "memtable": {"keys": mt_keys, "approx_bytes": mt_bytes},
        }),
    )
}

async fn storage_wal(ctx: &ClientCtx, q: &str) -> (u16, Value) {
    let tablet = tablet_param(q);
    let Some(g) = ctx.edge.local_cp(tablet) else {
        return not_hosted(tablet);
    };
    let Some(segs) = g.wal_segment_sizes().await else {
        return (
            200,
            json!({"tablet": tablet.0, "backend": g.backend_name()}),
        );
    };
    let (durable_seq, rotations) = g.wal_stats().unwrap_or((0, 0));
    let segments: Vec<Value> = segs
        .iter()
        .map(|(seg, bytes)| json!({"segment": seg, "bytes": bytes}))
        .collect();
    (
        200,
        json!({
            "tablet": tablet.0,
            "backend": g.backend_name(),
            "durable_seq": durable_seq,
            "rotations": rotations,
            "segments": segments,
        }),
    )
}

async fn storage_wal_segment(ctx: &ClientCtx, q: &str) -> (u16, Value) {
    let tablet = tablet_param(q);
    let Some(seg) = http::query_param(q, "seg").and_then(|s| s.parse::<u64>().ok()) else {
        return (
            400,
            json!({"error": "missing or invalid `seg` query parameter"}),
        );
    };
    let Some(g) = ctx.edge.local_cp(tablet) else {
        return not_hosted(tablet);
    };
    let Some(records) = g.wal_records(seg).await else {
        return (
            200,
            json!({"tablet": tablet.0, "backend": g.backend_name()}),
        );
    };
    let records: Vec<Value> = records.iter().map(wal_record_json).collect();
    (
        200,
        json!({"tablet": tablet.0, "segment": seg, "records": records}),
    )
}

async fn storage_key(ctx: &ClientCtx, q: &str) -> (u16, Value) {
    let tablet = tablet_param(q);
    let Some(key) = http::query_param(q, "key") else {
        return (400, json!({"error": "missing `key` query parameter"}));
    };
    // Accept the dashboard's `<token-base64>:<remainder>` display form (so clicking a
    // browsed key looks it up faithfully) as well as a raw plain key.
    let key = parse_key_display(&key);
    let Some(g) = ctx.edge.local_cp(tablet) else {
        return not_hosted(tablet);
    };
    let live = g.local_get(&key).await;
    let Some(versions) = g.disk_versions(&key).await else {
        return (
            200,
            json!({
                "tablet": tablet.0,
                "backend": g.backend_name(),
                "key": key_display(&key),
                "live": live.as_deref().map(key_str),
            }),
        );
    };
    let disk: Vec<Value> = versions
        .iter()
        .map(|(version, tombstone)| json!({"version": version, "tombstone": tombstone}))
        .collect();
    (
        200,
        json!({
            "tablet": tablet.0,
            "backend": g.backend_name(),
            "key": key_display(&key),
            "live": live.as_deref().map(key_str),
            "disk_versions": disk,
        }),
    )
}

/// `GET /admin/storage/scan?tablet=&start=&limit=` — the first `limit` live
/// `(key, value)` pairs with `key >= start`, in key order, from this node's local
/// engine (the dashboard "browse keys" view, ADR 0021). `start` defaults to the
/// beginning, `limit` to 50 (capped at 1000). Node-local, like the other storage
/// routes — scrape the leader for its committed state.
async fn storage_scan(ctx: &ClientCtx, q: &str) -> (u16, Value) {
    let tablet = tablet_param(q);
    // Decode the dashboard's `<token-base64>:<remainder>` display form (so paging by
    // pasting the last displayed key works), falling back to a raw plain prefix.
    let start = parse_key_display(&http::query_param(q, "start").unwrap_or_default());
    let limit = http::query_param(q, "limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(50)
        .clamp(1, 1000);
    let Some(g) = ctx.edge.local_cp(tablet) else {
        return not_hosted(tablet);
    };
    let pairs = g.local_scan(&start, limit).await;
    let truncated = pairs.len() == limit;
    let items: Vec<Value> = pairs
        .iter()
        .map(|(key, value)| {
            json!({
                "key": key_display(key),
                "value": key_str(value),
                "value_len": value.len(),
            })
        })
        .collect();
    (
        200,
        json!({
            "tablet": tablet.0,
            "backend": g.backend_name(),
            "count": items.len(),
            "limit": limit,
            "truncated": truncated,
            "items": items,
        }),
    )
}

fn metrics_view(ctx: &ClientCtx) -> Value {
    let (counters, is_leader) = ctx.metrics_json();
    json!({ "counters": counters, "is_leader": is_leader })
}

/// This node's metrics-history ring buffer (ADR 0020), backing the
/// dashboard's sparklines — a real live snapshot each `/admin/metrics` sample,
/// not a cluster-wide aggregate (the same "per-node sink" caveat `/admin/
/// metrics` itself carries).
fn metrics_history_view(ctx: &ClientCtx) -> Value {
    json!({ "samples": ctx.metrics_history() })
}

fn health(ctx: &ClientCtx) -> (u16, Value) {
    let r = &ctx.raft;
    let leader_known = r.leader().is_some();
    let hosts_cp = !ctx.edge.hosted_groups().is_empty();
    let body = json!({
        "ok": leader_known,
        "control_leader_known": leader_known,
        "is_control_leader": r.is_leader(),
        "hosts_cp": hosts_cp,
    });
    // 503 until the control plane has a leader (the readiness signal); 200 once
    // it does (whether this node leads or follows).
    (if leader_known { 200 } else { 503 }, body)
}

// ---- operator actions ---------------------------------------------------

#[derive(Deserialize)]
struct SplitReq {
    tablet: u64,
    split_key: String,
}

#[derive(Deserialize)]
struct TabletReq {
    tablet: u64,
}

#[derive(Deserialize)]
struct ReconfigureReq {
    tablet: u64,
    voters: Vec<NodeId>,
}

#[derive(Deserialize)]
struct DrainReq {
    node: NodeId,
}

async fn action_split(ctx: &ClientCtx, body: &[u8]) -> (u16, Value) {
    let req: SplitReq = match parse_body(body) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let resp = ctx
        .trigger_split(TabletId(req.tablet), req.split_key.into_bytes())
        .await;
    client_response_to_json(resp, json!({"ok": true, "tablet": req.tablet}))
}

async fn action_flush(ctx: &ClientCtx, body: &[u8]) -> (u16, Value) {
    let req: TabletReq = match parse_body(body) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let tablet = TabletId(req.tablet);
    let Some(g) = ctx.edge.local_cp(tablet) else {
        return not_hosted(tablet);
    };
    match g.flush_now().await {
        Some(Ok(())) => (
            200,
            json!({"ok": true, "tablet": req.tablet, "flushed": true}),
        ),
        Some(Err(e)) => (500, json!({"error": e})),
        None => (
            200,
            json!({"ok": true, "tablet": req.tablet, "backend": "memory", "flushed": false}),
        ),
    }
}

async fn action_compact(ctx: &ClientCtx, body: &[u8]) -> (u16, Value) {
    let req: TabletReq = match parse_body(body) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let tablet = TabletId(req.tablet);
    let Some(g) = ctx.edge.local_cp(tablet) else {
        return not_hosted(tablet);
    };
    match g.compact_now().await {
        Some(Ok(())) => (
            200,
            json!({"ok": true, "tablet": req.tablet, "compacted": true}),
        ),
        Some(Err(e)) => (500, json!({"error": e})),
        None => (
            200,
            json!({"ok": true, "tablet": req.tablet, "backend": "memory", "compacted": false}),
        ),
    }
}

fn action_reconfigure(ctx: &ClientCtx, body: &[u8]) -> (u16, Value) {
    let req: ReconfigureReq = match parse_body(body) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let tablet = TabletId(req.tablet);
    // Reconfigure is leader-only — resolve the leader handle for this tablet.
    let Some(leader) = ctx.edge.cp_leader(tablet) else {
        return if ctx.edge.local_cp(tablet).is_some() {
            (
                409,
                json!({"error": "this node does not lead the tablet's CP group; retry on the leader"}),
            )
        } else {
            not_hosted(tablet)
        };
    };
    let desired: std::collections::BTreeSet<NodeId> = req.voters.into_iter().collect();
    match leader.reconfigure_step(&desired) {
        Some(stepped) => (
            200,
            json!({"ok": true, "tablet": req.tablet, "stepped_to": stepped.into_iter().collect::<Vec<_>>()}),
        ),
        None => (
            200,
            json!({"ok": true, "tablet": req.tablet, "stepped": false, "note": "already at target or no single-server step available"}),
        ),
    }
}

fn action_drain(ctx: &ClientCtx, body: &[u8]) -> (u16, Value) {
    let req: DrainReq = match parse_body(body) {
        Ok(r) => r,
        Err(e) => return e,
    };
    match ctx.admin_drain(req.node) {
        Ok(()) => (
            200,
            json!({"ok": true, "node": req.node, "status": "Leaving"}),
        ),
        Err(e) => (409, json!({"error": e})),
    }
}

// ---- data write proxies (ADR 0021 dashboard) ----------------------------

#[derive(Deserialize)]
struct DynamoDataReq {
    /// The DynamoDB operation, e.g. `PutItem` (or the full `DynamoDB_20120810.PutItem`).
    op: String,
    /// The operation's JSON payload (the DynamoDB request body).
    #[serde(default)]
    payload: Value,
}

#[derive(Deserialize)]
struct CqlDataReq {
    /// One or more `;`-separated CQL statements.
    query: String,
    /// Optional keyspace to `USE` before running the statements.
    #[serde(default)]
    keyspace: Option<String>,
}

/// `POST /admin/data/dynamo` — run a DynamoDB operation from the dashboard, by
/// reusing the DynamoDB edge's decode + execute path in-process (ADR 0021). The
/// response is the operation's own JSON (or a DynamoDB error object), with the
/// edge's status code.
async fn action_data_dynamo(ctx: &ClientCtx, body: &[u8]) -> (u16, Value) {
    let req: DynamoDataReq = match parse_body(body) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let target = if req.op.contains('.') {
        req.op.clone()
    } else {
        format!("DynamoDB_20120810.{}", req.op)
    };
    let payload = serde_json::to_vec(&req.payload).unwrap_or_default();
    let (status, json_body) = crate::dynamo::execute(ctx, &target, &payload).await;
    let value = serde_json::from_str::<Value>(&json_body).unwrap_or(Value::String(json_body));
    (status, value)
}

/// `POST /admin/data/cql` — run CQL statements from the dashboard's editor by
/// driving this node's own CQL port as a loopback client (ADR 0021,
/// [`crate::cql_client`]); returns one JSON result per statement. A connection or
/// handshake failure is a `502` (the node's CQL edge is unreachable).
async fn action_data_cql(ctx: &ClientCtx, body: &[u8]) -> (u16, Value) {
    let req: CqlDataReq = match parse_body(body) {
        Ok(r) => r,
        Err(e) => return e,
    };
    match crate::cql_client::run(ctx.admin.cql_addr, req.keyspace.as_deref(), &req.query).await {
        Ok(results) => (200, json!({ "results": results })),
        Err(e) => (502, json!({ "error": e })),
    }
}

#[derive(Deserialize)]
struct DropTableReq {
    table: String,
}

/// `POST /admin/data/drop-table {table}` — drop a table: remove its schema from
/// the replicated catalog (ADR 0021 table management) **and** its tablets from
/// the replicated map, which triggers every replica's GC loop to reclaim the
/// table's data on disk (ADR 0024). Same sink as CQL `DROP TABLE`; idempotent.
/// The DynamoDB wire has no `DeleteTable`, so this is the dashboard's delete
/// primitive.
async fn action_drop_table(ctx: &ClientCtx, body: &[u8]) -> (u16, Value) {
    let req: DropTableReq = match parse_body(body) {
        Ok(r) => r,
        Err(e) => return e,
    };
    match ctx.drop_table(req.table.clone()).await {
        Ok(()) => (200, json!({ "ok": true, "table": req.table })),
        Err(e) => (409, json!({ "error": e })),
    }
}

/// Most keys a single `/admin/data/seed` request will write (the dashboard chunks
/// a larger seed into several requests so it can show progress + let tablets split
/// between chunks).
const SEED_MAX_PER_REQUEST: u64 = 200_000;
/// Cap on a synthetic value's size.
const SEED_MAX_VALUE_BYTES: usize = 1 << 20;
/// Keys committed as **one `Batch` Raft entry** (ADR 0017 — bulk-write batching):
/// a seed chunk of this many keys is proposed as a single consensus round instead
/// of one round per key, the bulk-seed throughput win. `cp_batch_write` further
/// groups a chunk by tablet, so a chunk that straddles a split boundary commits one
/// atomic entry per tablet.
const SEED_BATCH_SIZE: u64 = 500;
/// Cap on a seed batch's **raw entry bytes** (keys + values). `SEED_BATCH_SIZE`
/// alone lets a large-`value_bytes` seed build a 500 × 1 MiB batch, whose
/// cross-process forwarding frame (`ClientRequest::PutBatch`, JSON at ≤ 4 chars
/// per byte) would blow past the client protocol's [`crate::MAX_FRAME_LEN`].
/// Bounding the batch by bytes keeps the largest legitimate frame well under the
/// cap (~4 MiB raw → ~17 MiB JSON); default-sized (64 B) seeds still batch the
/// full 500 keys.
const SEED_BATCH_MAX_BYTES: usize = 4 << 20;
/// Attempts per seeded key, to absorb transient failures while a tablet is
/// **splitting** (writes racing the split point are truncated/tombstoned and
/// re-route to the new child on retry). Each attempt is bounded by the data path's
/// own `CLIENT_TIMEOUT`.
const SEED_WRITE_ATTEMPTS: usize = 4;
/// Backoff between seed write attempts — long enough for a freshly-split child
/// group to elect a leader / the tablet map to settle.
const SEED_RETRY_BACKOFF: Duration = Duration::from_millis(150);

#[derive(Deserialize)]
struct SeedReq {
    /// How many keys to write this request.
    count: u64,
    /// First index (partition keys are `key_prefix + zero-padded index`); the
    /// dashboard advances this per chunk.
    #[serde(default)]
    start: u64,
    /// Partition-key prefix (default `seed:`). Each seeded row is token-prefixed
    /// like any edge write (ADR 0022), so sequential indices still spread evenly
    /// across the table's hash ring.
    #[serde(default)]
    key_prefix: Option<String>,
    /// Synthetic value size in bytes (default 64).
    #[serde(default)]
    value_bytes: Option<usize>,
    /// The table to seed into. **Required** (ADR 0023: every key names a table — no
    /// default, so a seed request without `table` fails to decode). Its tablet is
    /// provisioned up front if it does not exist yet.
    table: String,
}

/// The data-plane key for a synthetic seed row: `partition_token(escape(pk)) ||
/// escape(pk)`, the edges' ADR 0022/0023 layout with no sort key (the DynamoDB
/// `item_key` shape). Seeding must hash like a real write — a raw `pk` key would
/// partition the *raw* keyspace, so sequential seed indices would all land in one
/// tablet's tail (the exact hot-prefix skew the token exists to remove) and split
/// boundaries would sit in raw-key space while edge writes route by token.
fn seed_key(pk: &[u8]) -> Vec<u8> {
    let escaped = escape(pk);
    let mut key = partition_token(&escaped).to_vec();
    key.extend_from_slice(&escaped);
    key
}

/// `POST /admin/data/seed {table, count, start?, key_prefix?, value_bytes?}` —
/// bulk-write synthetic keys to the CP plane to drive sharding tests (ADR 0021).
/// `table` is **required** and must **already exist** (ADR 0023: seeding writes into
/// a table, it does not create one — a non-existent table is a `404`). Each row's
/// partition key is `key_prefix + zero-padded (start..start+count)`, stored under
/// the same token-prefixed layout the wire edges write ([`seed_key`]) with a filler
/// value; writes go through the normal durable `cp_write` path (routed to the
/// leader), with bounded concurrency to amortize WAL group-commit. With
/// `--auto-split` enabled, crossing the key-count threshold splits the tablet —
/// visible live in the Tablets view.
async fn action_data_seed(ctx: &ClientCtx, body: &[u8]) -> (u16, Value) {
    let req: SeedReq = match parse_body(body) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let count = req.count.min(SEED_MAX_PER_REQUEST);
    let prefix = req.key_prefix.unwrap_or_else(|| "seed:".to_string());
    let value_bytes = req.value_bytes.unwrap_or(64).min(SEED_MAX_VALUE_BYTES);
    // Every key names a table (ADR 0023), and seeding **writes into an existing
    // table** — it never creates one. Look the table up (a read of the replicated
    // tablet map); reject if it doesn't exist — the caller must create it first.
    let table = req.table;
    if !ctx.raft.metadata().has_table_tablet(&table) {
        return (
            404,
            serde_json::json!({
                "error": format!("table `{table}` does not exist — create it first")
            }),
        );
    }
    let value = vec![b'x'; value_bytes];

    // ADR 0027: the seeder emulates a client issuing many `PutBatch` requests,
    // but calls `cp_batch_write` directly rather than going through
    // `handle_client` — so without a span here, `cp_forward`'s
    // `otel::current_traceparent()` has no active context to inject when a
    // batch forwards to another node, and the seed is invisible in a trace
    // backend no matter how much data it writes. One root span per request
    // (not per batch) mirrors a `client_request` span's granularity.
    let span = tracing::info_span!("admin_seed", table = %table, count, start = req.start);
    let seed_result: (u64, Option<String>) = async {
        let mut written = 0u64;
        let mut first_err: Option<String> = None;
        let mut i = 0u64;
        // Bound each batch by entry count *and* raw bytes (see `SEED_BATCH_MAX_BYTES`:
        // the forwarded `PutBatch` frame must stay under `MAX_FRAME_LEN`). ~64 B of
        // key overhead per entry (token + escaped pk); at least one entry per batch.
        let per_entry = value_bytes + 64;
        let max_by_bytes = (SEED_BATCH_MAX_BYTES / per_entry).max(1) as u64;
        while i < count {
            let chunk = (count - i).min(SEED_BATCH_SIZE).min(max_by_bytes);
            // Build the chunk's `(key, value)` pairs and commit them as **one Batch
            // entry per tablet** (`cp_batch_write` groups by tablet) — one consensus
            // round for the whole chunk instead of one per key.
            let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..chunk)
                .map(|j| {
                    let pk = format!("{prefix}{:012}", req.start + i + j);
                    (seed_key(pk.as_bytes()), value.clone())
                })
                .collect();
            // One child span per chunk (covering all its retry attempts) — gives a
            // trace backend per-batch visibility into forwarding/retries, the same
            // way a real client's individual `PutBatch` requests would.
            let batch_span =
                tracing::info_span!("admin_seed_batch", start_index = req.start + i, len = chunk);
            // Retry transient failures via `cp_batch_write_patient` rather than a
            // plain retry loop over `cp_batch_write`: a batch racing a tablet
            // **split** may route to the parent and be truncated/tombstoned (the
            // upper range moved to the new child), and a fresh propose against the
            // now-settled tablet map correctly re-routes to the elected child
            // (idempotent — same keys+value, per-key LWW). But a *plain*
            // confirm-timeout on the correct leader does not mean the batch is
            // lost, just slow — `cp_batch_write_patient` polls the
            // already-accepted entry instead of proposing a duplicate one, so a
            // slow/contended commit path doesn't get retry-amplified into
            // something worse (see its doc).
            let last = ctx
                .cp_batch_write_patient(
                    &table,
                    entries.clone(),
                    SEED_WRITE_ATTEMPTS,
                    SEED_RETRY_BACKOFF,
                )
                .instrument(batch_span)
                .await;
            match last {
                Ok(()) => written += chunk,
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
            i += chunk;
        }
        (written, first_err)
    }
    .instrument(span)
    .await;
    let (written, first_err) = seed_result;

    // All writes failing is a server error; a partial failure still reports what
    // landed (so the dashboard can surface it without losing the count).
    let status = if written == 0 && first_err.is_some() {
        500
    } else {
        200
    };
    (
        status,
        json!({
            "written": written,
            "requested": count,
            "start": req.start,
            "key_prefix": prefix,
            "value_bytes": value_bytes,
            "error": first_err,
        }),
    )
}

// ---- helpers ------------------------------------------------------------

/// The `tablet` query parameter, defaulting to the bootstrap tablet (id 1) — the
/// whole-keyspace CP group on a cluster that has not split.
fn tablet_param(q: &str) -> TabletId {
    TabletId(
        http::query_param(q, "tablet")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1),
    )
}

/// Render a *value* (or any opaque byte string) as a lossy UTF-8 string for
/// human-readable debug output. For a data-plane **key**, use [`key_display`]
/// instead — its leading bytes are a binary partition token.
fn key_str(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// The display width of a base64url-rendered partition token: unpadded base64
/// of `n` bytes is `ceil(4n/3)` chars (11 for the 8-byte token).
const TOKEN_B64_LEN: usize = (TOKEN_BYTES * 4).div_ceil(3);

/// Unpadded base64url (RFC 4648 §5, `-`/`_` instead of `+`/`/`, no `=`) of the
/// binary partition token. URL-safe because a displayed key is pasted back into
/// `?key=`/`?start=` query strings, where the standard alphabet's `+` decodes as
/// a space (and `=` padding would percent-encode noisily).
fn token_base64(token: &[u8]) -> String {
    base64url_encode(token)
}

/// Inverse of [`token_base64`]. Strict: the canonical unpadded base64url form
/// only (see [`parse_key_display`], which leans on that strictness).
fn parse_token_base64(s: &str) -> Option<Vec<u8>> {
    base64url_decode(s)
}

/// Render a data-plane **key** for the dashboard's key views. A key written
/// through a wire edge (DynamoDB/CQL) or the bulk seeder is
/// `token || escape(pk) || rk` (ADR 0022/0023): the leading [`TOKEN_BYTES`] bytes
/// are the big-endian Murmur3 **partition token** — binary, not text — so lossy
/// UTF-8 would mangle them into replacement characters. Show that token as
/// unpadded base64url, a `:` separator, then the human-readable
/// `escape(pk) || rk` remainder as a lossy UTF-8 string.
///
/// But a plain-client `Put` stores its key **verbatim**, un-prefixed, so not every
/// key has a token. Distinguish by *content*: only a leading `TOKEN_BYTES`-run that
/// isn't all printable ASCII is a real binary token (a Murmur3 token is
/// overwhelmingly likely to contain a non-printable byte). A fully-printable key —
/// or one shorter than the token width — is shown as text unchanged. Inverse of
/// [`parse_key_display`], so a token-prefixed key shown in the browse view
/// round-trips back through the key inspector.
fn key_display(bytes: &[u8]) -> String {
    let printable = |b: &u8| (0x20..0x7f).contains(b);
    if bytes.len() < TOKEN_BYTES || bytes[..TOKEN_BYTES].iter().all(printable) {
        return String::from_utf8_lossy(bytes).into_owned();
    }
    let (token, rest) = bytes.split_at(TOKEN_BYTES);
    let mut s = token_base64(token);
    s.push(':');
    s.push_str(&String::from_utf8_lossy(rest));
    s
}

/// Inverse of [`key_display`]: turn a `<token-base64>:<remainder>` string (as
/// shown in the dashboard's key views) back into the raw key bytes, so clicking
/// a browsed key sends the *real* key to the inspector. A string not in that
/// form — no [`TOKEN_B64_LEN`]-char, `:`-terminated prefix decoding to exactly
/// [`TOKEN_BYTES`] bytes (e.g. a hand-typed plain key: the decode is strict —
/// URL-safe alphabet only, canonical trailing bits — so an accidental match is
/// rare) — is taken verbatim as raw bytes. The
/// remainder round-trips exactly when the original was valid UTF-8 (the common
/// printable-pk case); bytes that lossy UTF-8 already replaced cannot be
/// recovered, same as before this formatting.
fn parse_key_display(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    if b.len() > TOKEN_B64_LEN && b[TOKEN_B64_LEN] == b':' {
        let token = std::str::from_utf8(&b[..TOKEN_B64_LEN])
            .ok()
            .and_then(parse_token_base64)
            .filter(|t| t.len() == TOKEN_BYTES);
        if let Some(mut key) = token {
            key.extend_from_slice(&b[TOKEN_B64_LEN + 1..]);
            return key;
        }
    }
    b.to_vec()
}

fn wal_record_json(r: &WalRecordView) -> Value {
    match r {
        WalRecordView::Put {
            key,
            version,
            value_len,
        } => {
            json!({"type": "put", "key": key_display(key), "version": version, "value_len": value_len})
        }
        WalRecordView::Delete { key, version } => {
            json!({"type": "delete", "key": key_display(key), "version": version})
        }
        WalRecordView::DeleteRange {
            start,
            end,
            keys,
            version,
        } => json!({
            "type": "delete_range",
            "start": key_display(start),
            "end": key_display(end),
            "keys": keys,
            "version": version,
        }),
        WalRecordView::Batch { version, ops } => {
            json!({"type": "batch", "version": version, "ops": ops})
        }
        WalRecordView::MergeBatch { ops, max_version } => {
            json!({"type": "merge_batch", "ops": ops, "max_version": max_version})
        }
    }
}

fn not_hosted(tablet: TabletId) -> (u16, Value) {
    (
        404,
        json!({
            "error": format!("this node hosts no replica of tablet {}; scrape a node that does", tablet.0)
        }),
    )
}

/// Parse a JSON request body into `T`, or return a `400` error response.
fn parse_body<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, (u16, Value)> {
    serde_json::from_slice(body)
        .map_err(|e| (400, json!({"error": format!("invalid JSON body: {e}")})))
}

/// Map a wire [`ClientResponse`] (from a routed action like split) to an admin
/// JSON response: `PutOk` → `ok`, an error → `409`.
fn client_response_to_json(resp: ClientResponse, ok: Value) -> (u16, Value) {
    match resp {
        ClientResponse::PutOk => (200, ok),
        ClientResponse::Error(e) => (409, json!({"error": e})),
        other => (
            500,
            json!({"error": format!("unexpected response: {other:?}")}),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A token-prefixed key: the binary 8-byte token renders as 11 chars of
    /// unpadded base64url, the printable `escape(pk)` remainder as text, and it
    /// round-trips exactly.
    #[test]
    fn key_display_shows_binary_token_as_base64_and_round_trips() {
        // A token with a guaranteed non-printable byte (0x00), so the heuristic
        // classifies it as binary regardless of the hash — then the readable pk.
        let mut key = vec![0x8a, 0x3f, 0x1c, 0x00, 0x77, 0xd2, 0xb6, 0xe1];
        key.extend_from_slice(b"user#42");
        let shown = key_display(&key);
        let (tok_b64, sep_rest) = shown.split_at(TOKEN_B64_LEN);
        assert_eq!(tok_b64, "ij8cAHfStuE");
        assert_eq!(sep_rest, ":user#42");
        assert_eq!(parse_key_display(&shown), key);
    }

    /// A token whose standard-base64 form contains `+`/`/` renders with the
    /// URL-safe `-`/`_` instead (a displayed key is pasted into `?key=` query
    /// strings, where a raw `+` decodes as a space), with no `=` padding, and
    /// still round-trips.
    #[test]
    fn key_display_token_base64_is_url_safe() {
        // 0xfb 0xff... encodes to base64 starting "+/" in the standard alphabet.
        let mut key = vec![0xfb, 0xff, 0xfe, 0x00, 0x77, 0xd2, 0xb6, 0xe1];
        key.extend_from_slice(b"user#42");
        let shown = key_display(&key);
        assert!(
            !shown.contains('+') && !shown.contains('/') && !shown.contains('='),
            "token must be unpadded URL-safe base64: {shown}"
        );
        assert_eq!(parse_key_display(&shown), key);
    }

    /// A key whose real token happens to be all binary also encodes, exercising
    /// the production `seed_key` layout end to end (the display always reverses).
    #[test]
    fn key_display_round_trips_a_seed_key() {
        let key = seed_key(b"user#42");
        assert_eq!(parse_key_display(&key_display(&key)), key);
    }

    /// A plain-client `Put` stores its key verbatim (no token). A fully-printable
    /// key — or one shorter than the token width — is shown as text unchanged, and
    /// a plain string with no base64-token prefix is taken raw by the inverse.
    #[test]
    fn key_display_leaves_printable_keys_as_text() {
        assert_eq!(key_display(b"admin-key"), "admin-key");
        assert_eq!(key_display(b"ab"), "ab");
        assert_eq!(parse_key_display("admin-key"), b"admin-key".to_vec());
        assert_eq!(parse_key_display("seed:"), b"seed:".to_vec());
        // 11 printable chars then ':' — every char is in the URL-safe alphabet,
        // but the trailing bits are non-canonical (no byte string encodes to
        // "seed-000042"), so the strict decode rejects it and it is NOT
        // mistaken for a token prefix.
        assert_eq!(
            parse_key_display("seed-000042:x"),
            b"seed-000042:x".to_vec()
        );
        // A 12-char prefix (the old padded width) has no ':' at index 11, so it
        // never enters the token path at all.
        assert_eq!(
            parse_key_display("seed-0000042:x"),
            b"seed-0000042:x".to_vec()
        );
    }
}
