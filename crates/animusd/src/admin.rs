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
//! - `GET  /admin/health`              — liveness/readiness
//! - `POST /admin/tablet/split`        — `{tablet, split_key}`
//! - `POST /admin/storage/flush`       — `{tablet}`
//! - `POST /admin/storage/compact`     — `{tablet}`
//! - `POST /admin/raftkv/reconfigure`  — `{tablet, voters}`
//! - `POST /admin/drain`               — `{node}`

use animus_env::NodeId;
use animus_storage::WalRecordView;
use animus_tablet::TabletId;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};

use crate::http;
use crate::{ClientCtx, ClientResponse};

/// This group's Raft state for the `/admin/raftkv` view (ADR 0020). Built by
/// [`crate::CpGroup::raft_view`] over either engine arm.
#[derive(serde::Serialize)]
pub(crate) struct CpRaftView {
    pub(crate) tablet: u64,
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

/// Whether `path` should serve the dashboard SPA (ADR 0021). The root and a couple
/// of `/admin` aliases all return the single-page app.
fn is_ui_path(path: &str) -> bool {
    matches!(
        path,
        "/" | "/admin" | "/admin/" | "/admin/ui" | "/index.html"
    )
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
        ("GET", "/admin/raftkv") => (200, raftkv_view(ctx)),
        ("GET", "/admin/storage/lsm") => storage_lsm(ctx, q).await,
        ("GET", "/admin/storage/wal") => storage_wal(ctx, q).await,
        ("GET", "/admin/storage/wal/segment") => storage_wal_segment(ctx, q).await,
        ("GET", "/admin/storage/key") => storage_key(ctx, q).await,
        ("GET", "/admin/storage/scan") => storage_scan(ctx, q).await,
        ("GET", "/admin/metrics") => (200, metrics_view(ctx)),
        ("GET", "/admin/health") => health(ctx),
        ("POST", "/admin/tablet/split") => action_split(ctx, &request.body).await,
        ("POST", "/admin/storage/flush") => action_flush(ctx, &request.body).await,
        ("POST", "/admin/storage/compact") => action_compact(ctx, &request.body).await,
        ("POST", "/admin/raftkv/reconfigure") => action_reconfigure(ctx, &request.body),
        ("POST", "/admin/drain") => action_drain(ctx, &request.body),
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

fn raftkv_view(ctx: &ClientCtx) -> Value {
    let groups: Vec<CpRaftView> = ctx
        .edge
        .hosted_groups()
        .iter()
        .map(|(t, g)| g.raft_view(*t))
        .collect();
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
                "min_key": s.min_key.as_deref().map(key_str),
                "max_key": s.max_key.as_deref().map(key_str),
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
    let key = key.into_bytes();
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
                "key": key_str(&key),
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
            "key": key_str(&key),
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
    let start = http::query_param(q, "start")
        .unwrap_or_default()
        .into_bytes();
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
                "key": key_str(key),
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

/// Render bytes as a lossy UTF-8 string for human-readable debug output.
fn key_str(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn wal_record_json(r: &WalRecordView) -> Value {
    match r {
        WalRecordView::Put {
            key,
            version,
            value_len,
        } => {
            json!({"type": "put", "key": key_str(key), "version": version, "value_len": value_len})
        }
        WalRecordView::Delete { key, version } => {
            json!({"type": "delete", "key": key_str(key), "version": version})
        }
        WalRecordView::DeleteRange {
            start,
            end,
            keys,
            version,
        } => json!({
            "type": "delete_range",
            "start": key_str(start),
            "end": key_str(end),
            "keys": keys,
            "version": version,
        }),
        WalRecordView::Batch { version, ops } => {
            json!({"type": "batch", "version": version, "ops": ops})
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
