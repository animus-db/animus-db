//! End-to-end test of the admin / debug HTTP-JSON interface over real TCP
//! (`ProdEnv`, ADR 0020), as an operator or the `animus admin` CLI would hit it.
//!
//! Brings up a 3-node cluster **one process per node** (each its own
//! `ClusterEdgeState`, as a real deployment — so each node's admin views are
//! node-local, not the shared in-process `--cluster` registry). Lets it elect a
//! control leader + bootstrap, writes a key through the client API (forwarded to
//! the CP leader), then exercises the admin routes on the dedicated admin port:
//! the read-only views and an operator action (`storage/flush`, forcing the
//! written key out to an SSTable observed via `storage/lsm`). Real time + sockets,
//! so it polls with generous timeouts.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, DynamoAuthConfig, Node, read_frame};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Bring up an `n`-node cluster, one process per node (each its own edge state),
/// retrying the (allocate-fresh-ports + start-all) as a unit. `free_addrs` frees
/// each port before `run_node` rebinds it, so another test binary can steal one in
/// the window (`AddrInUse`); a fresh attempt re-allocates and the started nodes are
/// torn down first (the documented port-TOCTOU mitigation, see the crate guide).
async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, animusd::ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
                advertise_host: None,
                tls: None,
                encryption_key_path: None,
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            match animusd::run_node(&config, i, dir.join(format!("node-{attempt}-{i}"))).await {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            return (nodes, config);
        }
        for node in &nodes {
            node.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up cluster after retries (ports kept getting stolen)");
}

// `bring_up_with_streams_quiesce` lost its last caller — ADR 0061 rung H,
// C-08 PR 6 converted `admin_split_in_place_children_inherit_the_parents_
// own_replicas` (its one caller) to
// `sim_cluster_admin_actions.rs::split_in_place_children_inherit_the_
// parents_own_replicas` — deleted from this file.

// `bring_up_with_fast_ttl` lost its last caller — ADR 0061 rung I, C-09 PR 4
// converted `admin_ttl_reports_reaper_progress_and_ttl_tables` (its one
// caller) to `sim_cluster_admin.rs::
// admin_ttl_reports_reaper_progress_and_ttl_tables` — deleted from this
// file.

/// Like [`bring_up`], but with DynamoDB Streams enabled and a **generous**
/// `stream_retention` (roadmap U-07's `admin_gc_reports_segment_janitor_
/// progress_and_leader_state` needs to prove the segment janitor's own
/// drop-table cascade — `segment_janitor.rs`'s own "table_dropped" rule,
/// which reclaims a dropped table's stream-shard rows immediately,
/// regardless of retention — the same shape `tests/stream_janitor.rs::
/// drop_table_cascade_converges_via_the_janitor` uses a 600s retention for:
/// if this test ever passed only because retention itself elapsed rather
/// than the drop-table rule, a short retention would let it pass for the
/// wrong reason). Seals almost immediately on any pending byte
/// (`seal_bytes: 1`, mirroring `tests/stream_janitor.rs::tiny_seal_knobs`)
/// so a single write reliably produces a sealed shard row for the janitor
/// to later reclaim.
async fn bring_up_with_streams(
    n: usize,
    dir: &std::path::Path,
) -> (Vec<Node>, animusd::ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
                advertise_host: None,
                tls: None,
                encryption_key_path: None,
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            match animusd::run_node_with_streams(
                &config,
                i,
                dir.join(format!("node-{attempt}-{i}")),
                animusd::StorageBackend::default(),
                animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
                animusd::StreamSealKnobs {
                    seal_bytes: 1,
                    seal_age: Duration::from_secs(3600),
                },
                animusd::SegmentStoreConfig::default(),
                Duration::from_secs(600),
            )
            .await
            {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            return (nodes, config);
        }
        for node in &nodes {
            node.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up cluster after retries (ports kept getting stolen)");
}

/// Like [`bring_up_with_streams`], but pins an explicit `fs:PATH`
/// [`animusd::SegmentStoreConfig`] instead of the default `Cluster`
/// variant — `admin_segment_store_reports_shard_placement_and_local_
/// objects` needs one node with the single-shared-directory opt-in to
/// prove `GET /admin/segment-store` reports `shards: null` for it (no
/// per-node replica concept there — see `SegmentStoreHandle::put_sealed`'s
/// own doc).
async fn bring_up_with_fs_segment_store(
    n: usize,
    dir: &std::path::Path,
    segment_store_dir: &std::path::Path,
) -> (Vec<Node>, animusd::ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
                advertise_host: None,
                tls: None,
                encryption_key_path: None,
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            match animusd::run_node_with_streams(
                &config,
                i,
                dir.join(format!("node-{attempt}-{i}")),
                animusd::StorageBackend::default(),
                animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
                animusd::StreamSealKnobs {
                    seal_bytes: 1,
                    seal_age: Duration::from_secs(3600),
                },
                animusd::SegmentStoreConfig::Fs(
                    segment_store_dir.join(format!("attempt-{attempt}")),
                ),
                Duration::from_secs(600),
            )
            .await
            {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            return (nodes, config);
        }
        for node in &nodes {
            node.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up cluster after retries (ports kept getting stolen)");
}

async fn await_bootstrap(nodes: &[Node]) {
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap in 20s");
}

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed JSON)`.
async fn admin(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.0\r\n\
         Host: animus\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    assert!(
        head.to_ascii_lowercase()
            .contains("content-type: application/json"),
        "admin response should be application/json, headers:\n{head}"
    );
    let value: Value = serde_json::from_str(payload)
        .unwrap_or_else(|e| panic!("admin body is not JSON ({e}): {payload}"));
    (status, value)
}

async fn admin_get(addr: SocketAddr, path: &str) -> (u16, Value) {
    admin(addr, "GET", path, None).await
}

/// A put with a bounded retry on ANY error reply (mirrors
/// `split_build.rs::put`): a put is idempotent, and early-cluster-formation/
/// split/election transients all surface as a clean, retryable
/// `ClientResponse::Error` — see `docs/engineering-lessons.md`'s "CP
/// write-forward path has no retry-on-not-the-leader-here" and issue #268's
/// fast-futility entries. A bare one-shot assert on the first write right
/// after bootstrap (or racing a split) is a documented latent flake, not a
/// bug this test should paper over.
async fn put(stream: &mut TcpStream, table: &str, key: Vec<u8>, value: Vec<u8>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        animusd::write_frame(
            stream,
            &ClientRequest::Put {
                key: key.clone(),
                value: value.clone(),
                table: table.to_string(),
            },
        )
        .await
        .expect("send put");
        let reply: ClientResponse = read_frame(stream)
            .await
            .expect("read reply")
            .expect("a reply");
        match reply {
            ClientResponse::PutOk => return,
            ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(150)).await;
            }
            other => panic!("put failed: {other:?}"),
        }
    }
}

// `percent_encode` lost its last caller — ADR 0061 rung H, C-08 PR 6
// converted `admin_seed_writes_synthetic_keys` (its one caller) to
// `sim_cluster_admin_actions.rs::seed_writes_synthetic_keys` (a local copy
// lives there) — deleted from this file.

/// **KEPT `ProdEnv` (ADR 0061 rung H, C-08 PR 6)**: the one action this
/// sweep exercises beyond what's covered elsewhere, `POST
/// /admin/storage/flush`, has the same `flush_now`/`compact_now` gap
/// `admin_storage_compact_action`'s own KEPT reason names below —
/// `MemoryEngine` (`SimCluster`'s only backend) has no LSM/SSTable concept
/// for a forced flush to act on — so this stays whole as this crate's one
/// remaining real-socket admin observer sweep, rather than trimmed to
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admin_interface_surfaces_state_and_actions() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        // Write a key through the client API (forwarded to the CP leader).
        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect");
        put(
            &mut stream,
            "kv",
            b"admin-key".to_vec(),
            b"admin-val".to_vec(),
        )
        .await;

        let admin_addr = nodes[0].admin_addr();

        // ---- /admin/config -------------------------------------------------
        let (s, config_view) = admin_get(admin_addr, "/admin/config").await;
        assert_eq!(s, 200);
        assert_eq!(
            config_view["node_id"], "n0",
            "node 0's one id (ADR 0040 PR1/PR3)"
        );
        assert_eq!(
            config_view["addrs"]["admin"].as_str(),
            Some(admin_addr.to_string().as_str()),
            "config echoes the admin address: {config_view}"
        );
        // U-06 (docs/roadmap.md): a combined-mode node started through plain
        // `run_node` gets the production-default `Cluster` stores, no
        // quiescence (disabled by default), and no `dynamo_auth` section —
        // `auth_enabled` is `Some(false)`, never `null` (combined nodes bind
        // the dynamo listener, so the field applies), and no key ids.
        assert_eq!(
            config_view["backup_store"]["kind"].as_str(),
            Some("cluster"),
            "default backup store: {config_view}"
        );
        assert_eq!(
            config_view["segment_store"]["kind"].as_str(),
            Some("cluster"),
            "default segment store: {config_view}"
        );
        assert!(
            config_view["quiesce_after_ms"].is_null(),
            "quiescence is off by default: {config_view}"
        );
        assert_eq!(
            config_view["auth_enabled"].as_bool(),
            Some(false),
            "no dynamo_auth section: {config_view}"
        );
        assert!(
            config_view["auth_access_key_ids"].is_null(),
            "auth is off: {config_view}"
        );
        // The resolved OTLP endpoint mirrors `animusd::otel::resolved_endpoint`'s
        // own env-var read exactly — asserted against the live process
        // environment rather than hardcoded to `null`, so this doesn't go
        // flaky under a CI runner that happens to export the var.
        let expected_otlp = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .ok()
            .filter(|e| !e.is_empty());
        assert_eq!(
            config_view["otlp_endpoint"].as_str().map(str::to_owned),
            expected_otlp,
            "resolved OTLP endpoint mirrors the process env: {config_view}"
        );

        // ---- /admin/status -------------------------------------------------
        // `members` is populated by `bootstrap()`'s per-id `UpsertMember`
        // proposals landing on the control plane one commit at a time —
        // `await_bootstrap` only requires the registry to be non-empty, and
        // the `put` above only needs enough `Active` members to place one
        // tablet (`provision_tablet`'s own doc: even a fresh metadata read
        // can observe a cluster still mid-bootstrap and mint a smaller
        // initial replica set that self-heals later), so "all 3 data nodes
        // registered" is an eventual property here, not something either
        // already guarantees. Poll to convergence rather than assert in one
        // shot under CI contention (root CLAUDE.md: an eventual property
        // gets a converged-or-timeout poll, never a fixed-deadline assert).
        support::poll_until_or_stalled(
            admin_addr,
            "control-plane member registry never reached all 3 data nodes (/admin/status members)",
            Duration::from_millis(100),
            || async {
                admin_get(admin_addr, "/admin/status").await.1["members"]
                    .as_object()
                    .map(|m| m.len())
                    == Some(3)
            },
        )
        .await;
        let (s, status) = admin_get(admin_addr, "/admin/status").await;
        assert_eq!(s, 200);
        assert_eq!(
            status["members"].as_object().map(|m| m.len()),
            Some(3),
            "status reports 3 members: {status}"
        );

        // ---- /admin/raft ---------------------------------------------------
        // Same eventual member-registry field as `/admin/status` above, read
        // through a different view (`raft_view`'s own `meta.members`, not the
        // Raft *voter* config) — poll it to convergence for the same reason.
        support::poll_until_or_stalled(
            admin_addr,
            "control-plane member registry never reached all 3 data nodes (/admin/raft members)",
            Duration::from_millis(100),
            || async {
                admin_get(admin_addr, "/admin/raft").await.1["members"]
                    .as_array()
                    .map(Vec::len)
                    == Some(3)
            },
        )
        .await;
        let (s, raft) = admin_get(admin_addr, "/admin/raft").await;
        assert_eq!(s, 200);
        assert!(raft["term"].as_u64().unwrap() >= 1, "raft term: {raft}");
        assert!(raft["role"].is_string(), "raft role present: {raft}");
        assert_eq!(
            raft["members"].as_array().map(Vec::len),
            Some(3),
            "raft view lists members: {raft}"
        );

        // ---- /admin/raftkv (one group per node in per-process mode) --------
        // `hosts_cp` flips true only once this node's own tablet-host
        // reconciler has observed the tablet's placement in the now-committed
        // `Metadata` and materialized the CP group locally (`ctx.edge.
        // hosted_groups()`, populated asynchronously by that reconciler, per
        // the crate guide's tablet-lifecycle section) — a real cluster-wide
        // write can succeed as soon as a QUORUM of replicas (not necessarily
        // node 0) has hosted and elected, so node 0 catching up is a genuine
        // eventual property, not something the `put` above guarantees by the
        // time it returns. This was the CI-observed flake (issue: `hosts_cp
        // == true` asserted in one shot right after bootstrap) — poll to
        // convergence instead (root CLAUDE.md: an eventual property gets a
        // converged-or-timeout poll, never a fixed-deadline one-shot assert).
        support::poll_until_or_stalled(
            admin_addr,
            "node 0 never came to host the CP group (/admin/raftkv hosts_cp)",
            Duration::from_millis(100),
            || async { admin_get(admin_addr, "/admin/raftkv").await.1["hosts_cp"] == true },
        )
        .await;
        let (s, raftkv) = admin_get(admin_addr, "/admin/raftkv").await;
        assert_eq!(s, 200);
        assert_eq!(raftkv["hosts_cp"], true, "node 0 hosts the CP group");
        let groups = raftkv["groups"].as_array().expect("groups array");
        assert_eq!(groups.len(), 1, "node-local view: one group: {raftkv}");
        assert_eq!(groups[0]["tablet"], 1, "the bootstrap tablet id");
        assert_eq!(groups[0]["backend"], "lsm", "durable backend");

        // ---- /admin/storage/wal --------------------------------------------
        let (s, wal) = admin_get(admin_addr, "/admin/storage/wal?tablet=1").await;
        assert_eq!(s, 200);
        assert_eq!(wal["backend"], "lsm");
        assert!(
            wal["segments"].as_array().is_some_and(|a| !a.is_empty()),
            "WAL has at least one live segment: {wal}"
        );

        // ---- /admin/storage/control (ADR 0038 PR4) -------------------------
        // A combined node has a `ControlHandle::Local` control role, so its
        // own control-plane system-keyspace engine is available — the exact
        // same physical shared engine `/admin/raftkv`'s hosted tablet above
        // also reports on (`Metadata` just lives at a reserved key prefix
        // within it, ADR 0038), so the backend agrees.
        let (s, ctl_storage) = admin_get(admin_addr, "/admin/storage/control").await;
        assert_eq!(s, 200);
        assert_eq!(
            ctl_storage["available"], true,
            "a combined node has a local control-plane system-keyspace engine: {ctl_storage}"
        );
        assert_eq!(ctl_storage["backend"], "lsm");

        // ---- /admin/metrics ------------------------------------------------
        // Metrics are per-node sinks (a follower's leader-only counters are 0), so
        // every node exposes the full counter set as JSON, and the *control
        // leader's* node reports the election counters non-zero + is_leader 1.
        let (s, metrics) = admin_get(admin_addr, "/admin/metrics").await;
        assert_eq!(s, 200);
        assert!(
            metrics["counters"]["control_elections_started"].is_u64(),
            "metrics JSON exposes the full counter set: {metrics}"
        );
        let leader_idx = nodes
            .iter()
            .position(Node::is_control_leader)
            .expect("a control leader exists after bootstrap");
        let (s, lmetrics) = admin_get(nodes[leader_idx].admin_addr(), "/admin/metrics").await;
        assert_eq!(s, 200);
        assert_eq!(lmetrics["is_leader"], 1, "leader node reports is_leader 1");
        assert!(
            lmetrics["counters"]["control_elections_won"]
                .as_u64()
                .unwrap()
                >= 1,
            "control leader won an election: {lmetrics}"
        );
        // W-09 (ADR 0034 amendment): `request_rates` rides beside
        // `stream_change_rates`, and both auto-split thresholds are
        // reported (as `null` here — this cluster sets neither flag).
        assert!(
            metrics["stream_change_rates"].as_array().is_some(),
            "stream_change_rates is always an array: {metrics}"
        );
        assert!(
            metrics["request_rates"].as_array().is_some(),
            "request_rates is always an array: {metrics}"
        );
        assert!(
            metrics["auto_split_bytes_threshold"].is_null(),
            "no --auto-split-bytes configured in this test cluster: {metrics}"
        );
        assert!(
            metrics["auto_split_ops_rate_threshold"].is_null(),
            "no --auto-split-ops-rate configured in this test cluster: {metrics}"
        );

        // ---- /admin/metrics/history -----------------------------------------
        // The sampler ticks every 10s (METRICS_SAMPLE_INTERVAL); poll rather than
        // sleep a fixed amount, matching this suite's real-time convention.
        let (s, history) = admin_get(admin_addr, "/admin/metrics/history").await;
        assert_eq!(s, 200);
        assert!(
            history["samples"].as_array().is_some(),
            "history is a samples array even before the first tick: {history}"
        );
        let sample = timeout(Duration::from_secs(15), async {
            loop {
                let (_, h) = admin_get(admin_addr, "/admin/metrics/history").await;
                if let Some(first) = h["samples"].as_array().and_then(|a| a.first()) {
                    return first.clone();
                }
                sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("a metrics-history sample appears within one sample interval");
        assert!(sample["ts_ms"].as_u64().is_some_and(|t| t > 0), "{sample}");
        assert!(
            sample["counters"]["control_elections_started"].is_u64(),
            "a sample carries the same counter set as /admin/metrics: {sample}"
        );

        // ---- /admin/health -------------------------------------------------
        let (s, health) = admin_get(admin_addr, "/admin/health").await;
        assert_eq!(s, 200, "health 200 once a leader is known");
        assert_eq!(health["ok"], true);

        // ---- /admin/live (issue #710) — a healthy node reports 200 here too,
        // same as /admin/health, but the two are independent routes.
        let (s, live) = admin_get(admin_addr, "/admin/live").await;
        assert_eq!(s, 200, "live is 200 on a healthy node: {live}");
        assert_eq!(live["ok"], true);

        // ---- unknown route + malformed body --------------------------------
        let (s, _) = admin_get(admin_addr, "/admin/nope").await;
        assert_eq!(s, 404, "unknown admin route is 404");
        let (s, _) = admin(admin_addr, "POST", "/admin/storage/flush", Some("not json")).await;
        assert_eq!(s, 400, "malformed JSON body is 400");

        // ---- action: flush on the CP leader's node, observe the SSTable.
        // The leader has the put applied for certain, so the forced flush has data.
        let mut leader_admin = None;
        for node in &nodes {
            let (_, rk) = admin_get(node.admin_addr(), "/admin/raftkv").await;
            if rk["groups"][0]["is_leader"].as_bool() == Some(true) {
                leader_admin = Some(node.admin_addr());
                break;
            }
        }
        let leader_admin = leader_admin.expect("a CP group leader exists");

        let (s, flushed) = admin(
            leader_admin,
            "POST",
            "/admin/storage/flush",
            Some("{\"tablet\":1}"),
        )
        .await;
        assert_eq!(s, 200, "flush action returns 200: {flushed}");
        assert_eq!(flushed["flushed"], true, "flush ran: {flushed}");

        let (s, lsm) = admin_get(leader_admin, "/admin/storage/lsm?tablet=1").await;
        assert_eq!(s, 200);
        assert_eq!(lsm["backend"], "lsm");
        assert!(
            lsm["sstables"].as_array().is_some_and(|a| !a.is_empty()),
            "the forced flush produced at least one SSTable: {lsm}"
        );

        // ---- /admin/storage/scan (browse keys) — the written key is listed -----
        let (s, scan) = admin_get(leader_admin, "/admin/storage/scan?tablet=1&limit=10").await;
        assert_eq!(s, 200, "scan returns 200: {scan}");
        let items = scan["items"].as_array().expect("scan items array");
        assert!(
            items
                .iter()
                .any(|it| { it["key"] == "admin-key" && it["value"] == "admin-val" }),
            "browse-keys lists the written pair: {scan}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

// `admin_data_write_dynamo` converted → ADR 0061 rung H, C-08 PR 6's
// `sim_cluster_admin_actions.rs::data_write_dynamo` (PutItem/GetItem via
// `/admin/data/dynamo`, issued from a node hosting no replica of the
// table's own tablet) — deleted from this file.

// `admin_table_management_create_and_drop` converted → ADR 0061 rung H,
// C-08 PR 6's `sim_cluster_admin_actions.rs::table_management_create_and_
// drop` (a composite `CreateTable`/`drop-table` via the same proxy, issued
// from a control follower) — deleted from this file.

// `admin_backups_view_reflects_the_catalog` converted → ADR 0061 rung H,
// C-08 PR 5's `sim_cluster_admin.rs::backups_view_reflects_the_catalog`
// (`BeginBackup`/`RecordBackupTabletComplete`/`CompleteBackup` proposed
// directly via `SimCluster::propose_meta`, mirroring this test's own
// harness-level `MetaCommand` idiom) — deleted from this file.

/// The CP data plane's per-tablet Raft group must hold **stable leadership under
/// sustained write load** — the driver-liveness guarantee (ADR 0017). Before engine
/// apply + compaction were moved off the consensus loop, a bulk seed blocked the
/// single driver task for ~180-300ms per batch (LSM merges + compaction), past the
/// 150ms election timeout, so followers repeatedly timed out and campaigned: the CP
/// term climbed ~8-18 per 2000-key seed (a leader-election storm that truncated
/// in-flight writes and collapsed throughput to ~15/s). With apply/compaction on a
/// separate task, the leader keeps heartbeating and the term stays flat.
///
/// This is a **real-time `ProdEnv` liveness assertion** — the class `SimEnv` cannot
/// catch (virtual time never trips the wall-clock election timeout). We seed 2000
/// keys through the CP leader and require the group's term to barely move.
///
/// **KEPT `ProdEnv` (ADR 0061 rung H, C-08 PR 6)**: real-thread
/// election-timing liveness.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn seed_load_does_not_storm_cp_elections() {
    // The CP term may legitimately advance a little (an initial election retry, a
    // stray heartbeat miss under CI load); the storm this guards against moved it by
    // 8-18 in a single seed. A generous bound stays non-flaky while still failing
    // loudly if the storm returns.
    const MAX_TERM_DELTA: u64 = 3;

    timeout(Duration::from_secs(90), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;
        let (s, _ct) = admin(
            nodes[0].admin_addr(),
            "POST",
            "/admin/data/dynamo",
            Some(
                r#"{"op":"CreateTable","payload":{"TableName":"seedt",
                    "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                    "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}}"#,
            ),
        )
        .await;
        assert_eq!(s, 200, "CreateTable seedt");

        // Poll until the bootstrap CP group has a leader; return (node index, term).
        async fn cp_leader(nodes: &[Node]) -> (usize, u64) {
            for _ in 0..100 {
                for (i, node) in nodes.iter().enumerate() {
                    let (_, rk) = admin_get(node.admin_addr(), "/admin/raftkv").await;
                    if let Some(groups) = rk["groups"].as_array() {
                        for g in groups {
                            if g["is_leader"].as_bool() == Some(true) {
                                return (i, g["term"].as_u64().unwrap_or(0));
                            }
                        }
                    }
                }
                sleep(Duration::from_millis(100)).await;
            }
            panic!("CP group never elected a leader");
        }

        let (leader_idx, term_before) = cp_leader(&nodes).await;
        let started = std::time::Instant::now();
        let (s, body) = admin(
            nodes[leader_idx].admin_addr(),
            "POST",
            "/admin/data/seed",
            Some(r#"{"table":"seedt","count":2000,"key_prefix":"seed:","value_bytes":64}"#),
        )
        .await;
        assert_eq!(s, 200, "seed returns 200: {body}");
        assert_eq!(
            body["written"], 2000,
            "seed wrote all requested keys: {body}"
        );

        let (_, term_after) = cp_leader(&nodes).await;
        let delta = term_after.saturating_sub(term_before);
        let rate = 2000.0 / started.elapsed().as_secs_f64();
        eprintln!("seed 2000 keys: {rate:.0}/s, CP term {term_before} -> {term_after} (Δ{delta})");
        assert!(
            delta <= MAX_TERM_DELTA,
            "CP leadership stormed under seed load: term moved {term_before} -> {term_after} \
             (Δ{delta} > {MAX_TERM_DELTA}) — apply/compaction is likely blocking the driver \
             loop past the election timeout again"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("seed-load election-stability test timed out");
}

// `admin_seed_writes_synthetic_keys` converted → ADR 0061 rung H, C-08
// PR 6's `sim_cluster_admin_actions.rs::seed_writes_synthetic_keys`
// (the bulk-seed endpoint's full contract: a 404 on a nonexistent table,
// written-count/raw-scan/DynamoDB-readback checks for both a simple and a
// composite table, and the displayed-key round trip through
// `/admin/storage/key`) — deleted from this file.

/// Regression (2026-08-19): `/admin/raftkv` is **polled** — the Console
/// fetches it from every node on its auto-refresh interval (5s by default) —
/// so its default response must not materialize every hosted tablet's rows.
/// It used to: `key_count`/`byte_size` came from `local_pairs()`, an
/// O(dataset) scan per hosted group per request. Measured on a live
/// 20,000-row cluster, polling this route every 3s inflated a split's own
/// build from 4.5s to 41.8s (~9x) — an observer that materially perturbs
/// what it observes.
///
/// The teeth use the LSM's own `storage_sstable_block_reads` counter as the
/// cost meter rather than wall-clock, which would be flaky under CI
/// contention: a materializing read of flushed data must read SSTable
/// blocks, and a metadata-only estimate must not. Both windows are the same
/// shape and duration, so whatever background work the node's own loops do
/// lands in both and cancels out of the comparison; only the route's own
/// cost differs.
///
/// **KEPT `ProdEnv` (ADR 0061 rung H, C-08 PR 5)**: `SimCluster`'s engine
/// is `MemoryEngine`, not `LsmEngine` — there is no SSTable/block-read
/// counter at all under `SimEnv`, so the cost differential this test
/// measures (`storage_sstable_block_reads` between the cheap and
/// `?exact=1` paths) cannot exist under that fixture.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admin_raftkv_default_does_not_materialize_the_dataset() {
    /// Enough rows that one materializing scan is unmistakable in block
    /// reads, few enough that seeding stays quick.
    const N: usize = 2_000;
    /// Polls per window — mirrors an operator leaving the Tablets tab open.
    const POLLS: usize = 10;

    timeout(Duration::from_secs(120), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;
        let a = nodes[0].admin_addr();

        let (s, ct) = admin(
            a,
            "POST",
            "/admin/data/dynamo",
            Some(
                r#"{"op":"CreateTable","payload":{"TableName":"big",
                    "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                    "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}}"#,
            ),
        )
        .await;
        assert_eq!(s, 200, "CreateTable big: {ct}");
        let (s, seeded) = admin(
            a,
            "POST",
            "/admin/data/seed",
            Some(&format!(
                r#"{{"table":"big","count":{N},"key_prefix":"seed:","value_bytes":64}}"#
            )),
        )
        .await;
        assert_eq!(s, 200, "seed: {seeded}");

        // Flush every hosted group so the rows live in SSTables, not the
        // memtable — otherwise even the exact scan reads no blocks and the
        // meter below cannot tell the two paths apart.
        let (_, rk) = admin_get(a, "/admin/raftkv").await;
        let mut tablets: Vec<u64> = Vec::new();
        for g in rk["groups"].as_array().cloned().unwrap_or_default() {
            let tablet = g["tablet"].as_u64().expect("group has a tablet id");
            tablets.push(tablet);
            let (s, f) = admin(
                a,
                "POST",
                "/admin/storage/flush",
                Some(&format!(r#"{{"tablet":{tablet}}}"#)),
            )
            .await;
            assert_eq!(s, 200, "flush tablet {tablet}: {f}");
        }

        // Issue #587: wait for `big`'s own change-log housekeeping to drain
        // on THIS node before metering. `bring_up`'s nodes run with
        // quiescence disabled (`run_node`'s `quiesce_after: Duration::ZERO`
        // default), so `change_consumer_loop` (`index_drain.rs`) never gets
        // to skip a led group via `is_quiesced()` — it ticks every
        // `INDEX_DRAIN_INTERVAL` regardless. Every one of the `N` seeded
        // writes leaves an ADR 0049 change-log marker record with no
        // stream/GSI/PITR to consume it, so this table's tablet takes the
        // loop's mandatory idle fast path: as long as `KIND_CHANGE` bytes
        // remain, each tick does a real `pending_changes` scan (real
        // SSTable block reads once flushed) and trims a `TRIM_BATCH`-sized
        // slice, in batches, until the backlog is fully drained — only then
        // does the fast path's `bytes == 0` branch stop scanning for good.
        // That scan has nothing to do with the routes under test, but it
        // lands on the exact same node/counter this test meters, and under
        // scheduling pressure a tick can fall inside one metered window and
        // not the other, inflating `cheap` (or `exact`) independent of
        // `/admin/raftkv`'s own cost — the actual cause of this test's
        // one-off failure (issue #587), not a cross-node/process-wide
        // counter (the ADR 0015 metrics sink is per-node, see
        // `docs/engineering-lessons.md`).
        //
        // There is no single already-exposed "drain done" boolean here (the
        // RaftCore-level `quiesced` diagnostic doesn't apply — it never
        // fires at all with quiescence disabled), so the existing knob to
        // poll instead is the exact view's own `key_count`/`byte_size`
        // (`?exact=1`, ADR 0020): once the trim loop's batches stop landing,
        // these stop changing. Require several consecutive identical reads
        // (spaced past one `INDEX_DRAIN_INTERVAL`, so a batch's own
        // busy/idle-throttled cadence can't look "stable" mid-drain) before
        // treating the tablet as settled — a converged-or-timeout poll, not
        // a fixed sleep, per this repo's own testing discipline.
        const STABLE_READS_REQUIRED: usize = 5;
        let settle_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut stable = 0usize;
        let mut last: Option<(u64, u64)> = None;
        loop {
            let (_, rk) = admin_get(a, "/admin/raftkv?exact=1").await;
            let groups = rk["groups"].as_array().cloned().unwrap_or_default();
            let totals = tablets.iter().try_fold((0u64, 0u64), |(kc, bs), t| {
                let g = groups.iter().find(|g| g["tablet"].as_u64() == Some(*t))?;
                Some((kc + g["key_count"].as_u64()?, bs + g["byte_size"].as_u64()?))
            });
            match totals {
                Some(cur) if last == Some(cur) => {
                    stable += 1;
                    if stable >= STABLE_READS_REQUIRED {
                        break;
                    }
                }
                Some(cur) => {
                    last = Some(cur);
                    stable = 1;
                }
                None => {
                    stable = 0;
                    last = None;
                }
            }
            assert!(
                tokio::time::Instant::now() < settle_deadline,
                "big's change-log housekeeping never settled before metering: {groups:?}"
            );
            sleep(Duration::from_millis(250)).await;
        }

        async fn block_reads(a: SocketAddr) -> u64 {
            let (_, m) = admin_get(a, "/admin/metrics").await;
            m["counters"]["storage_sstable_block_reads"]
                .as_u64()
                .expect("the LSM block-read counter is exported")
        }

        let base = block_reads(a).await;
        for _ in 0..POLLS {
            let (s, _) = admin_get(a, "/admin/raftkv").await;
            assert_eq!(s, 200, "default raftkv poll");
        }
        let after_cheap = block_reads(a).await;
        for _ in 0..POLLS {
            let (s, _) = admin_get(a, "/admin/raftkv?exact=1").await;
            assert_eq!(s, 200, "exact raftkv poll");
        }
        let after_exact = block_reads(a).await;

        let cheap = after_cheap - base;
        let exact = after_exact - after_cheap;
        eprintln!(
            "raftkv cost over {N} flushed rows: {POLLS} default polls = {cheap} SSTable block \
             reads, {POLLS} `?exact=1` polls = {exact}"
        );
        assert!(
            exact > cheap * 4 + 50,
            "the default `/admin/raftkv` must not materialize the dataset: {POLLS} default \
             polls read {cheap} SSTable blocks vs {exact} for the same number of `?exact=1` \
             polls over {N} flushed rows — the default is scanning again"
        );

        // The cheap path still answers, and `?exact=1` still answers exactly.
        let (_, cheap_view) = admin_get(a, "/admin/raftkv").await;
        let sum = |v: &Value| -> u64 {
            v["groups"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .iter()
                .filter_map(|g| g["key_count"].as_u64())
                .sum()
        };
        assert!(
            sum(&cheap_view) > 0,
            "the LSM backend has a cheap key-count estimate: {cheap_view}"
        );
        // `raft_view`'s exact path (`CpGroup::local_pairs`) is a pure LOCAL engine
        // read with no consensus barrier or leadership check (`admin.rs`'s own
        // doc: "/admin/raftkv is node-local"), and every poll in this test targets
        // `nodes[0]` specifically regardless of which node leads the "big" table's
        // tablet. If node 0 is a follower here, its exact count is an EVENTUAL
        // property of its own apply loop, not a fact the leader-side seed/flush
        // acks already guarantee — so assert it one-shot only. Converge-poll
        // instead (the repo's own idiom, `docs/engineering-lessons.md`'s Testing
        // section: "Eventual properties get a converged-or-timeout poll, never a
        // fixed-deadline one-shot assert"). Bounded generously (20s) relative to
        // the apply/flush cadence exercised above, not guessed.
        let exact_view = timeout(Duration::from_secs(20), async {
            loop {
                let (_, v) = admin_get(a, "/admin/raftkv?exact=1").await;
                if sum(&v) >= N as u64 {
                    return v;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "`?exact=1` on node {a} never converged to counting every seeded row \
                 within 20s — a lagging follower's own applied state, not a genuine \
                 undercount (the seeding/flush above only guarantees the LEADER's \
                 view, not every follower's)"
            )
        });
        assert!(
            sum(&exact_view) >= N as u64,
            "`?exact=1` counts every seeded row (plus bookkeeping kinds): {exact_view}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

// `admin_raftkv_key_count_is_scoped_per_tablet_after_split` converted
// → ADR 0061 rung H, C-08 PR 5's `sim_cluster_admin.rs::raftkv_key_
// count_is_scoped_per_tablet_after_split` (the fixture's own split +
// `GET /admin/raftkv?exact=1`) — deleted from this file.

// `admin_split_in_place_children_inherit_the_parents_own_replicas`
// converted → ADR 0061 rung H, C-08 PR 6's `sim_cluster_admin_actions.rs::
// split_in_place_children_inherit_the_parents_own_replicas` (a 4-node/RF-3
// `SimCluster`, the identical ADR 0062 rung 4 teeth: both children's
// pre-fork intent replicas must be exactly the parent's own current
// replicas, never a placement-recomputed set that would have recruited
// the deliberately-idle 4th node) — deleted from this file.

/// docs/roadmap.md U-05's lineage panel on the Tablets tab
/// (`dashboard_tablets.js`) reads `GET /admin/system-table?kind=
/// split_lineage`/`?kind=split_placing`, keyed by tablet id off the item's
/// own `id`/`value` shape. This is the real-cluster proof that a
/// COMPLETED in-place split (ADR 0058 Train 2 rung 3 — cutover, not just
/// kickoff) actually populates the `split_lineage` kind the panel's
/// ancestor/child walk depends on, with the exact `{id, value: {parent,
/// ...}}` shape the dashboard's `loadTabletLineage` parses, and that the
/// `split_placing` kind stays an ordinary (if empty) `200` rather than
/// erroring — a 3-node/RF-3 cluster's children inherit exactly the whole
/// cluster, which already satisfies policy, so no directed-Placing entry
/// is expected here (the panel's own "no pending placing" empty state).
/// Rides the identical split recipe
/// `admin_raftkv_key_count_is_scoped_per_tablet_after_split` above already
/// proves end to end for a different surface.
///
/// **KEPT `ProdEnv` (ADR 0061 rung H, C-08 PR 6)**: `ctx.control_storage`
/// is always `None` under `SimCluster`, so `GET /admin/system-table`
/// unconditionally answers `{"available": false}` there regardless of
/// what actually split.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admin_system_table_split_lineage_after_a_real_split() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;
        let admin_addr = nodes[0].admin_addr();

        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect");
        for i in 0..10u32 {
            let key = format!("key{i:02}").into_bytes();
            let value = format!("v{i}").into_bytes();
            put(&mut stream, "kv", key, value).await;
        }

        let (s, split) = admin(
            admin_addr,
            "POST",
            "/admin/tablet/split",
            Some(r#"{"tablet":1,"split_key":"key05"}"#),
        )
        .await;
        assert_eq!(s, 200, "split committed: {split}");

        // Wait for cutover to actually complete — `split_lineage` is written
        // by `CutoverSplit`'s own apply, not by the fork alone, so the
        // parent must be genuinely gone (not merely outnumbered mid-workflow).
        let children: Vec<String> = timeout(Duration::from_secs(15), async {
            loop {
                let (_, status) = admin_get(admin_addr, "/admin/status").await;
                let tablets = status["tablets"].as_object().cloned().unwrap_or_default();
                if !tablets.contains_key("1") && tablets.len() == 2 {
                    return tablets.keys().cloned().collect();
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("split did not cut over to two children");
        assert_eq!(children.len(), 2, "exactly two children after cutover");

        // Both children now carry a `split_lineage` row naming tablet 1 as
        // their parent — the exact shape `loadTabletLineage`
        // (dashboard_tablets.js) walks (`id` a decimal string, `value.parent`
        // a plain JSON number — `TabletId`'s newtype serialization).
        let (s, body) = admin_get(admin_addr, "/admin/system-table?kind=split_lineage").await;
        assert_eq!(s, 200, "system-table split_lineage: {body}");
        assert_eq!(body["available"], Value::Bool(true));
        let items = body["items"].as_array().expect("items array");
        for child in &children {
            let row = items
                .iter()
                .find(|it| it["id"].as_str() == Some(child.as_str()))
                .unwrap_or_else(|| panic!("no split_lineage row for child {child}: {body}"));
            assert_eq!(
                row["value"]["parent"],
                Value::from(1),
                "child {child}'s lineage row names tablet 1 as parent: {row}"
            );
        }

        // `split_placing` stays a normal, available route even with no rows.
        let (s, body) = admin_get(admin_addr, "/admin/system-table?kind=split_placing").await;
        assert_eq!(s, 200, "system-table split_placing: {body}");
        assert_eq!(body["available"], Value::Bool(true));

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// Bring up a single node with a `dynamo_auth` section (ADR 0057), retrying
/// the port-TOCTOU race exactly like [`bring_up`] does — this file's own
/// copy since `bring_up` always builds a config with `dynamo_auth: None`,
/// the same "sibling test modules keep their own fixtures independent"
/// convention `dynamo_sigv4.rs::start_single_node_with_auth` already uses.
async fn bring_up_with_auth(
    dir: &std::path::Path,
    credentials: BTreeMap<String, String>,
) -> (Node, animusd::ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(6);
        let config = animusd::ClusterConfig {
            nodes: vec![animusd::RoleAddrs {
                id: animusd::config::node_id(0),
                role: animusd::config::NodeRole::Both,
                internal: addrs[0],
                client: addrs[1],
                dynamo: addrs[2],
                admin: addrs[3],
                intra: addrs[4],
                console: addrs[5],
                advertise_host: None,
                tls: None,
                encryption_key_path: None,
            }],
            dynamo_auth: Some(DynamoAuthConfig {
                credentials: credentials.clone(),
            }),
            cluster_settings: None,
        };
        match animusd::run_node(&config, 0, dir.join(format!("node-{attempt}"))).await {
            Ok(node) => return (node, config),
            Err(_) => sleep(Duration::from_millis(50)).await,
        }
    }
    panic!("single node (dynamo_auth) failed to start after retries (ports kept getting stolen)");
}

/// U-06 (docs/roadmap.md): `/admin/config` reports `auth_enabled: true` and
/// the configured access key **ids** once a `dynamo_auth` section is
/// present — and, load-bearing, the configured *secret* never appears
/// anywhere in the served JSON, however it's rendered. Asserted against the
/// raw response body text, not just the parsed `auth_access_key_ids` field,
/// so this would catch the secret leaking through some other field too.
///
/// **KEPT `ProdEnv` (ADR 0061 rung H, C-08 PR 5)**: no `SimCluster`
/// constructor knob configures `dynamo_auth` — every node's own
/// `ClientCtx::dynamo_auth` is `None` in that fixture, unconditionally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_config_reports_auth_state_and_never_serves_the_secret() {
    timeout(Duration::from_secs(30), async {
        const ACCESS_KEY_ID: &str = "AKIDEXAMPLE";
        const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let mut credentials = BTreeMap::new();
        credentials.insert(ACCESS_KEY_ID.to_string(), SECRET.to_string());

        let dir = support::panic_safe_tempdir();
        let (node, _config) = bring_up_with_auth(dir.path(), credentials).await;
        let admin_addr = node.admin_addr();

        let (status, config_view) = admin_get(admin_addr, "/admin/config").await;
        assert_eq!(status, 200, "config_view: {config_view}");

        assert_eq!(
            config_view["auth_enabled"].as_bool(),
            Some(true),
            "a dynamo_auth section is configured: {config_view}"
        );
        assert_eq!(
            config_view["auth_access_key_ids"].as_array(),
            Some(&vec![Value::String(ACCESS_KEY_ID.to_string())]),
            "the access key id (never the secret) is reported: {config_view}"
        );

        // The load-bearing assertion: the secret never leaves this node's
        // admin surface, in this field or any other.
        let raw = serde_json::to_string(&config_view).expect("config_view serializes");
        assert!(
            !raw.contains(SECRET),
            "the SigV4 secret access key must never appear in /admin/config: {raw}"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

// --- ADR 0066: the replicated credential catalog's admin CRUD -------------
//
// `admin_credentials_view_never_serves_a_secret` converted → ADR 0061 rung
// H, C-08 PR 5's `sim_cluster_admin.rs::credentials_view_never_serves_a_
// secret` (`PutCredential` then `GET /admin/credentials`, redacted, no
// secret anywhere in either response) — deleted from this file.

// `admin_credentials_put_rotate_revoke_round_trip` converted → ADR 0061
// rung H, C-08 PR 6's `sim_cluster_admin_actions.rs::credentials_put_
// rotate_revoke_round_trip` (the full Put/Rotate/Revoke life cycle,
// including the redacted rotation-grace-window fields and the unknown-id/
// idempotent-revoke error shapes) — deleted from this file.

// `admin_credentials_put_on_a_follower_is_relayed_to_the_leader`
// converted → ADR 0061 rung H, C-08 PR 6's `sim_cluster_admin_actions.rs::
// credentials_put_on_a_follower_is_relayed_to_the_leader` (the identical
// `is_relayable_command` allowlist regression, issued from a control
// follower and converged on every node) — deleted from this file.

// `admin_control_transfer_moves_leadership_to_the_named_node` converted
// → ADR 0061 rung H, C-08 PR 6's `sim_cluster_admin_actions.rs::
// control_transfer_moves_leadership_to_the_named_node` (the identical
// whole-call retry discipline against every retryable 409, converging on
// the named target) — deleted from this file.

// `admin_control_transfer_on_a_follower_is_refused` converted → ADR 0061
// rung H, C-08 PR 6's `sim_cluster_admin_actions.rs::control_transfer_on_
// a_follower_is_refused` — deleted from this file.

/// `POST /admin/storage/compact` (docs/roadmap.md U-05, tablet action
/// family) had no integration coverage anywhere in this crate before this
/// test — `admin_interface_surfaces_state_and_actions` above only exercises
/// its sibling `/admin/storage/flush`. Mirrors that test's own flush
/// sequence: write a key, find the CP group leader, flush it to a real
/// on-disk SSTable (so compaction has real LSM state to act on, not a
/// trivial empty-engine no-op), compact, and confirm the written pair
/// still reads back afterward. Also proves the `not_hosted` refusal shape
/// for a tablet id this node doesn't host.
///
/// **KEPT `ProdEnv` (ADR 0061 rung H, C-08 PR 6)**: `CpGroup::
/// compact_now()` is `None` for `SimCluster`'s `MemoryEngine` backend —
/// no LSM/SSTable concept to compact.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admin_storage_compact_action() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect");
        put(
            &mut stream,
            "kv",
            b"compact-key".to_vec(),
            b"compact-val".to_vec(),
        )
        .await;

        let mut leader_admin = None;
        for node in &nodes {
            let (_, rk) = admin_get(node.admin_addr(), "/admin/raftkv").await;
            if rk["groups"][0]["is_leader"].as_bool() == Some(true) {
                leader_admin = Some(node.admin_addr());
                break;
            }
        }
        let leader_admin = leader_admin.expect("a CP group leader exists");

        let (s, flushed) = admin(
            leader_admin,
            "POST",
            "/admin/storage/flush",
            Some("{\"tablet\":1}"),
        )
        .await;
        assert_eq!(s, 200, "flush before compact: {flushed}");
        assert_eq!(flushed["flushed"], true, "flush ran: {flushed}");

        let (s, compacted) = admin(
            leader_admin,
            "POST",
            "/admin/storage/compact",
            Some("{\"tablet\":1}"),
        )
        .await;
        assert_eq!(s, 200, "compact action returns 200: {compacted}");
        assert_eq!(compacted["compacted"], true, "compact ran: {compacted}");
        assert_eq!(compacted["tablet"], 1);

        // The written pair survives compaction.
        let (s, scan) = admin_get(leader_admin, "/admin/storage/scan?tablet=1&limit=10").await;
        assert_eq!(s, 200);
        let items = scan["items"].as_array().expect("scan items array");
        assert!(
            items
                .iter()
                .any(|it| { it["key"] == "compact-key" && it["value"] == "compact-val" }),
            "the written pair survives compaction: {scan}"
        );

        // An unhosted tablet id is refused (`not_hosted`'s 404 shape).
        let (s, err) = admin(
            leader_admin,
            "POST",
            "/admin/storage/compact",
            Some("{\"tablet\":999}"),
        )
        .await;
        assert_eq!(s, 404, "compacting an unhosted tablet is refused: {err}");

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

// `admin_backup_store_reports_reclaim_progress_and_leader_state`
// converted → ADR 0061 rung H, C-08 PR 5's `sim_cluster_admin.rs::
// backup_store_reports_reclaim_progress_and_leader_state` (a backup
// driven to `Available` via direct `MetaCommand`s, the sim's always-on
// backup janitor reclaiming its seeded objects after `MarkBackup
// Deleted`) — deleted from this file.

// `admin_ttl_reports_reaper_progress_and_ttl_tables` converted → ADR 0061
// rung I, C-09 PR 4's `sim_cluster_admin.rs::admin_ttl_reports_reaper_
// progress_and_ttl_tables` (the always-on TTL reaper is now real under
// `SimEnv`, C-09 PR 2 — the "tables" half's own prior sibling,
// `sim_cluster_admin.rs::ttl_tables_lists_a_ttl_enabled_table`, PR 4 of
// C-08/rung H, is unchanged) — deleted from this file.

// `admin_gc_reports_segment_janitor_progress_and_leader_state`
// converted → ADR 0061 rung H, C-08 PR 5's `sim_cluster_admin.rs::
// gc_reports_segment_janitor_progress_and_leader_state` (a streamed
// table, a seal on the table's own data-plane leader, `POST /admin/
// data/drop-table`, the sim's always-on segment janitor converging
// `orphans_deleted_total >= 1`) — deleted from this file.

/// `GET /admin/segment-store` (ADR 0043 §A7b, roadmap U-07's fourth and
/// last route): a real 3-node cluster with DynamoDB Streams enabled and the
/// default `cluster` segment store — creates a streamed table, writes one
/// item, waits for it to seal into a `stream_shards` catalog row (the
/// row's own `replicas` recorded by `ClusterSegmentStore::put_replicated`
/// at seal time), then polls converged-or-timeout until SOME node's own
/// route shows `local_objects.count >= 1` — proving that node actually
/// holds a physical copy locally, not merely that the catalog row exists.
/// Every node is then asserted to report the identical `shards` array (the
/// replicated catalog is identical everywhere, ADR 0038), and a separate
/// single-node cluster configured with the `fs` opt-in reports
/// `shards: null` (no per-node replica concept for a single shared
/// directory every node already reads).
///
/// **KEPT `ProdEnv` (ADR 0061 rung H, C-08 PR 5)**: `SimCluster`'s shared
/// stream segment store is `SegmentStoreHandle::S3` (one shared object
/// store), never `Cluster`; `AdminInfo.segment_store` is always `None`
/// (JSON `null`) under that fixture, so `/admin/segment-store`'s
/// `is_cluster` shard-placement rendering (gated on a `kind == "cluster"`
/// display label the fixture never sets) would need faking a display
/// string rather than exercising the real `ClusterSegmentStore` per-node
/// replica-placement mechanism this test's own subject is — no primitive
/// for that exists under `SimEnv`.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admin_segment_store_reports_shard_placement_and_local_objects() {
    timeout(Duration::from_secs(90), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up_with_streams(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        let leader_idx = nodes
            .iter()
            .position(Node::is_control_leader)
            .expect("a control leader exists after bootstrap");

        // ---- baseline: every node's own route reports the configured
        //      `cluster` store and an honest (possibly empty) local scan --
        for node in &nodes {
            let (s, v) = admin_get(node.admin_addr(), "/admin/segment-store").await;
            assert_eq!(s, 200, "GET /admin/segment-store: {v}");
            assert_eq!(v["store"]["kind"], "cluster", "the configured cluster store: {v}");
            assert!(
                v["local_objects"]["count"].as_u64().is_some(),
                "local_objects.count is always present (even zero): {v}"
            );
            assert!(v["shards"].is_array(), "shards is an array for the cluster kind: {v}");
        }

        // ---- create a streamed table and write one item through the admin
        //      dynamo proxy on any node (it forwards internally) ------------
        let any_addr = nodes[0].admin_addr();
        let (s, ct) = admin(
            any_addr,
            "POST",
            "/admin/data/dynamo",
            Some(
                r#"{"op":"CreateTable","payload":{"TableName":"t","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"StreamSpecification":{"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"}}}"#,
            ),
        )
        .await;
        assert_eq!(s, 200, "CreateTable: {ct}");
        let (s, put) = admin(
            any_addr,
            "POST",
            "/admin/data/dynamo",
            Some(r#"{"op":"PutItem","payload":{"TableName":"t","Item":{"id":{"S":"p1"}}}}"#),
        )
        .await;
        assert_eq!(s, 200, "PutItem: {put}");

        // ---- wait for the write to seal into a catalog row (seal_bytes: 1
        //      means this should be near-immediate) ---------------------------
        timeout(Duration::from_secs(20), async {
            loop {
                if !nodes[leader_idx].metadata().stream_shards.is_empty() {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("no sealed stream-shard row appeared within 20s");

        // ---- poll converged-or-timeout: SOME node's own route shows it
        //      physically holds at least one local object -------------------
        timeout(Duration::from_secs(20), async {
            loop {
                for node in &nodes {
                    let (s, v) = admin_get(node.admin_addr(), "/admin/segment-store").await;
                    assert_eq!(s, 200, "GET /admin/segment-store: {v}");
                    if v["local_objects"]["count"].as_u64().unwrap_or(0) >= 1 {
                        return;
                    }
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("no node's own /admin/segment-store ever reported local_objects.count >= 1");

        // ---- every node eventually reports the identical shard placement
        //      — the replicated catalog is the same everywhere (ADR 0038),
        //      but each node's own local control Raft applies the
        //      `SealStreamShard` commit independently, so this is a
        //      converged-or-timeout poll, never a one-shot snapshot -------
        timeout(Duration::from_secs(20), async {
            loop {
                let mut views = Vec::with_capacity(nodes.len());
                let mut all_non_empty = true;
                for node in &nodes {
                    let (s, v) = admin_get(node.admin_addr(), "/admin/segment-store").await;
                    assert_eq!(s, 200, "GET /admin/segment-store: {v}");
                    let shards = v["shards"].clone();
                    if shards.as_array().is_none_or(|a| a.is_empty()) {
                        all_non_empty = false;
                    }
                    views.push(shards);
                }
                if all_non_empty && views.windows(2).all(|w| w[0] == w[1]) {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect(
            "every node never converged on the identical, non-empty shard->replica \
             placement within 20s",
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// `GET /admin/segment-store` on a node configured with the single-shared-
/// directory `fs` opt-in reports `shards: null` — there is no per-node
/// replica concept to report when every node already reads the identical
/// directory (see `SegmentStoreHandle::put_sealed`'s own doc for the
/// empty-`replicas`/"ask any node" convention this mirrors).
///
/// **KEPT `ProdEnv` (ADR 0061 rung H, C-08 PR 5)**: `SimCluster`'s shared
/// segment store is `S3`-kind, never `fs`-kind, and this test is
/// genuinely `fs`-kind-specific — the identical store-kind gap the
/// `shard_placement_and_local_objects` test above stays `ProdEnv` for.
#[tokio::test(flavor = "multi_thread")]
async fn admin_segment_store_reports_null_shards_for_the_fs_kind() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let segment_store_dir = dir.path().join("fs-segment-store");
        let (nodes, _config) =
            bring_up_with_fs_segment_store(1, dir.path(), &segment_store_dir).await;
        await_bootstrap(&nodes).await;

        let (s, v) = admin_get(nodes[0].admin_addr(), "/admin/segment-store").await;
        assert_eq!(s, 200, "GET /admin/segment-store: {v}");
        assert_eq!(v["store"]["kind"], "fs", "the configured fs: store: {v}");
        assert!(
            v["shards"].is_null(),
            "the fs kind has no per-node placement: {v}"
        );
        assert!(
            v["local_objects"]["count"].as_u64().is_some(),
            "local_objects.count is still reported for the fs kind: {v}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// Bring up ONE node out of an `n`-voter [`animusd::ClusterConfig`] (index
/// `0`; peers `1..n` are never started) — the cheapest reproducible
/// **genuinely leaderless** node: with fewer than a quorum of voters ever
/// up, `0`'s control Raft can never win an election, so `leader_within`
/// stays `None` forever, same shape as [`bring_up`] otherwise (six ports
/// per config entry, `Both` role, retried against the same port-TOCTOU
/// window). Used by
/// `admin_live_is_200_while_a_genuinely_leaderless_admin_health_is_503`
/// below (issue #710).
async fn bring_up_lone_voter_of(n: usize, dir: &std::path::Path) -> Node {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
                advertise_host: None,
                tls: None,
                encryption_key_path: None,
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };
        match animusd::run_node(&config, 0, dir.join(format!("lone-{attempt}-0"))).await {
            Ok(node) => return node,
            Err(_) => sleep(Duration::from_millis(50)).await,
        }
    }
    panic!("could not bring up lone-voter node after retries (ports kept getting stolen)");
}

/// Issue #710 regression: `/admin/live` must answer `200` on a node whose
/// control plane has NO leader — the exact condition under which
/// `/admin/health` correctly answers `503` (issue #595's readiness
/// hysteresis has long since expired for a node that never had a leader in
/// the first place). A Kubernetes `livenessProbe` pointed at the readiness
/// route would SIGTERM this node even though it is healthy and correctly
/// still trying to join; `/admin/live` must never make that mistake.
///
/// **KEPT `ProdEnv` (ADR 0061 rung H, C-08 PR 5)**: `SimCluster::new`'s
/// own doc: "Settles the control group (drives past its first election)
/// before returning" — there is no constructor for a node that boots
/// without ever completing bootstrap, so a genuinely-leaderless-from-the-
/// start node (this test's own subject) cannot be produced under that
/// fixture; crashing peers afterward would test leadership LOSS after a
/// known leader, a materially different condition.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_live_is_200_while_a_genuinely_leaderless_admin_health_is_503() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        // A 3-voter config with only node 0 ever started: peers 1 and 2
        // never come up, so node 0 can never reach quorum and never elects
        // (or hears of) a leader.
        let node = bring_up_lone_voter_of(3, dir.path()).await;

        // `/admin/health` must settle to 503 (poll rather than a one-shot
        // assert: right after startup the hysteresis grace window from
        // issue #595 may not have expired yet even with no leader ever
        // known).
        let (s, health) = timeout(Duration::from_secs(15), async {
            loop {
                let (s, health) = admin_get(node.admin_addr(), "/admin/health").await;
                if s == 503 {
                    return (s, health);
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("a lone voter never gains a leader; /admin/health must settle to 503");
        assert_eq!(s, 503, "leaderless node's readiness must 503: {health}");
        assert_eq!(health["control_leader_recent"], false, "{health}");
        assert_eq!(health["control_leader_known"], false, "{health}");

        // `/admin/live` answers 200 the whole time regardless — sample it
        // now, with the node in the exact leaderless state just confirmed
        // above.
        let (s, live) = admin_get(node.admin_addr(), "/admin/live").await;
        assert_eq!(
            s, 200,
            "liveness must not gate on control leadership: {live}"
        );
        assert_eq!(live["ok"], true, "{live}");
        assert_eq!(
            live["control_leader_recent"], false,
            "the diagnostic field mirrors reality even though it never gates the status: {live}"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}
