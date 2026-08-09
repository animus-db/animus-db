//! **Online cluster growth** (ADR 0030): a 3-node cluster (declaring only 3 in
//! its config) is created, has tables provisioned + written to, and is then
//! grown to 5 nodes with an **expanded config** — no restart of the original 3.
//! The two new nodes self-register `Down` automatically as part of
//! `start_with` (ADR 0032 PR2 folded `ClientCtx::admin_add_member` into every
//! growth node's own bring-up); this test also still calls
//! `POST /admin/member/add` explicitly for each, now exercising that
//! primitive's idempotent no-op path rather than the only way in. Either way,
//! each new node's own `heartbeat_loop` promotes it to `Active` on first
//! contact (ADR 0012's unmodified detector), and automatic rebalancing (ADR
//! 0029) then spreads the pre-existing tablets' replicas onto them with no
//! further operator action. Reads/writes keep working throughout.
//!
//! The control group genuinely never grows (ADR 0030's documented v1
//! limitation): the two new nodes run a **control-plane-follower-less** control
//! role (`run_node_growth`) that never becomes a real voter of the pre-growth
//! control group and instead mirrors `Metadata` by polling `ClientRequest::Status`
//! — this test is the end-to-end proof that the mirror is enough to make
//! join-host / CP routing / the admin add-member's own commit-confirmation all
//! work on such a node.
//!
//! Real TCP/time — polls with generous timeouts, not deterministic assertions.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use animusd::{
    ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, StorageBackend, read_frame,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

const TABLES: [&str; 3] = ["grow0", "grow1", "grow2"];

/// Bring up the **initial** `n`-node cluster, one process per node, retrying the
/// (allocate-fresh-ports + start-all) as a unit — the documented port-TOCTOU
/// mitigation (another test binary can steal a freed ephemeral port before the
/// real bind).
async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<RoleAddrs> = (0..n)
            .map(|i| RoleAddrs {
                role: animusd::config::NodeRole::Both,
                control: Some(addrs[6 * i]),
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                cql: addrs[6 * i + 3],
                raftkv: Some(addrs[6 * i + 4]),
                admin: addrs[6 * i + 5],
            })
            .collect();
        let config = ClusterConfig { nodes: nodes_cfg };
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
            node.shutdown();
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up the initial cluster after retries");
}

/// Grow `base` by `extra` control-plane-follower-less nodes (ADR 0030), each
/// started via `run_node_growth` from an **expanded** config (`base`'s nodes
/// plus `extra` freshly bound ones) with `original_control_ids` = `base`'s own
/// control group — the pre-growth nodes are never touched. Retries only the new
/// nodes' freshly-allocated ports as a unit (same port-TOCTOU mitigation as
/// `bring_up`; the original nodes' addresses are already bound and fixed).
async fn grow(
    base: &ClusterConfig,
    extra: usize,
    dir: &std::path::Path,
) -> (Vec<Node>, ClusterConfig) {
    let original_control_ids = base.control_ids();
    let base_n = base.nodes.len();
    for attempt in 0..16 {
        let addrs = support::free_addrs(extra * 6);
        let mut nodes_cfg = base.nodes.clone();
        for i in 0..extra {
            nodes_cfg.push(RoleAddrs {
                role: animusd::config::NodeRole::Both,
                control: Some(addrs[6 * i]),
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                cql: addrs[6 * i + 3],
                raftkv: Some(addrs[6 * i + 4]),
                admin: addrs[6 * i + 5],
            });
        }
        let expanded = ClusterConfig { nodes: nodes_cfg };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..extra {
            match animusd::run_node_growth(
                &expanded,
                base_n + i,
                original_control_ids.clone(),
                dir.join(format!("grow-{attempt}-{i}")),
                StorageBackend::default(),
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
            return (nodes, expanded);
        }
        for node in &nodes {
            node.shutdown();
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not grow the cluster after retries");
}

async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|node| !node.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(30), ready)
        .await
        .expect("cluster did not bootstrap within 30s");
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
    let value: Value = serde_json::from_str(payload).expect("admin body is JSON");
    (status, value)
}

/// The full replicated tablet map, `TabletId -> (replicas, epoch)`, from a
/// node's `/admin/status` (`Metadata`, mirrored on a growth node — ADR 0030).
async fn tablet_map(admin_addr: SocketAddr) -> BTreeMap<u64, (Vec<u64>, u64)> {
    let (_s, v) = admin(admin_addr, "GET", "/admin/status", None).await;
    v["tablets"]
        .as_object()
        .expect("tablets is an object")
        .iter()
        .map(|(id, t)| {
            let replicas = t["replicas"]
                .as_array()
                .expect("replicas is an array")
                .iter()
                .filter_map(Value::as_u64)
                .collect();
            let epoch = t["epoch"].as_u64().expect("epoch is a number");
            (
                id.parse().expect("tablet id key is numeric"),
                (replicas, epoch),
            )
        })
        .collect()
}

/// Every member's status, `raftkv_id -> "Active"/"Down"/...`, from
/// `/admin/status`.
async fn member_statuses(admin_addr: SocketAddr) -> BTreeMap<u64, String> {
    let (_s, v) = admin(admin_addr, "GET", "/admin/status", None).await;
    v["members"]
        .as_object()
        .expect("members is an object")
        .iter()
        .map(|(id, m)| {
            (
                id.parse().expect("member id key is numeric"),
                m["status"].as_str().expect("status is a string").to_owned(),
            )
        })
        .collect()
}

fn replica_counts(
    map: &BTreeMap<u64, (Vec<u64>, u64)>,
    raftkv_ids: &[u64],
) -> BTreeMap<u64, usize> {
    let mut counts: BTreeMap<u64, usize> = raftkv_ids.iter().map(|&id| (id, 0)).collect();
    for (replicas, _) in map.values() {
        for &r in replicas {
            *counts.entry(r).or_insert(0) += 1;
        }
    }
    counts
}

fn imbalance(counts: &BTreeMap<u64, usize>) -> usize {
    let max = counts.values().copied().max().unwrap_or(0);
    let min = counts.values().copied().min().unwrap_or(0);
    max - min
}

async fn call(addr: SocketAddr, req: ClientRequest) -> Option<ClientResponse> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    animusd::write_frame(&mut stream, &req).await.ok()?;
    read_frame(&mut stream).await.ok()?
}

/// Try every client address in `clients` (round-robin) until one accepts the
/// write. A production client, on growing a cluster, refreshes its endpoint
/// list to include the new nodes — modeled here by `clients` covering every
/// node started so far, old and new, exactly the shape this test needs to
/// prove "keeps working throughout" without depending on a *stale* original
/// node's own `client_route` forwarding to a node it started before (ADR
/// 0030's documented residual gap — see the ADR).
async fn put(clients: &[SocketAddr], table: &str, key: &[u8], value: &[u8], secs: u64) {
    let w = async {
        loop {
            for &c in clients {
                if let Some(ClientResponse::PutOk) = call(
                    c,
                    ClientRequest::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        table: table.to_string(),
                    },
                )
                .await
                {
                    return;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(secs), w)
        .await
        .unwrap_or_else(|_| panic!("write of {table}/{key:?} never committed"));
}

async fn await_value(clients: &[SocketAddr], table: &str, key: &[u8], want: &[u8], secs: u64) {
    let p = async {
        loop {
            for &c in clients {
                if let Some(ClientResponse::Value(Some(v))) = call(
                    c,
                    ClientRequest::Get {
                        key: key.to_vec(),
                        table: table.to_string(),
                    },
                )
                .await
                {
                    if v == want {
                        return;
                    }
                }
            }
            sleep(Duration::from_millis(150)).await;
        }
    };
    timeout(Duration::from_secs(secs), p)
        .await
        .unwrap_or_else(|_| panic!("key {table}/{key:?} never read back as {want:?}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn cluster_grows_from_three_to_five_and_rebalances() {
    let dir = tempfile::tempdir().unwrap();

    // 1. Bring up a 3-node cluster whose *own* config declares only 3 nodes —
    // no reserved control seats, matching a real "we didn't plan to grow yet"
    // deployment.
    let (mut nodes, base_config) = bring_up(3, dir.path()).await;
    await_bootstrap(&nodes).await;
    let base_clients: Vec<SocketAddr> = base_config.nodes.iter().map(|a| a.client).collect();
    let base_admin: Vec<SocketAddr> = base_config.nodes.iter().map(|a| a.admin).collect();

    // 2. Create tables + write, entirely on the original 3 (every table's
    // tablet lands on the first min(3, RF) Active members — {300,301,302}).
    for table in TABLES {
        put(&base_clients, table, b"k0", b"v0", 30).await;
    }
    let raftkv_ids_3 = base_config.raftkv_ids();
    let before_growth = tablet_map(base_admin[0]).await;
    assert_eq!(before_growth.len(), TABLES.len());
    let before_counts = replica_counts(&before_growth, &raftkv_ids_3);
    for &id in &raftkv_ids_3 {
        assert!(
            before_counts[&id] > 0,
            "every original node should host something pre-growth: {before_counts:?}"
        );
    }

    // 3. Grow: start 2 more nodes from an *expanded* config, with no restart of
    // the original 3 — the online part of "online cluster growth".
    let (growth_nodes, expanded_config) = grow(&base_config, 2, dir.path()).await;
    nodes.extend(growth_nodes);
    let all_raftkv_ids = expanded_config.raftkv_ids(); // [300, 301, 302, 303, 304]
    let new_ids = &all_raftkv_ids[3..]; // [303, 304]
    let all_admin: Vec<SocketAddr> = expanded_config.nodes.iter().map(|a| a.admin).collect();
    // A client list covering every node started so far — see `put`'s doc for
    // why this, not just `base_clients`, is the sound way to assert "still
    // serving" once a tablet's leader can have moved onto a new node.
    let all_clients: Vec<SocketAddr> = expanded_config.nodes.iter().map(|a| a.client).collect();

    // Note: this test used to assert here that a freshly-started growth node
    // is not yet a member (sanity: rules out a vacuous "already Active from
    // somewhere else" pass). ADR 0032 PR2 made every growth node
    // self-register itself `Down` as part of `start_with` (the same
    // `admin_add_member` primitive `POST /admin/member/add` calls below) and
    // its own `heartbeat_loop`/detector promotion chain starts just as
    // immediately, so neither "not a member yet" nor even "not yet Active"
    // is a stable window to assert on any more — by the time `grow()`
    // returns and this test can poll, `303`/`304` may already be fully
    // `Active` (observed live: a real race, not a hypothetical). The
    // meaningful invariant — every new node reaches `Active` promptly with
    // no operator action beyond starting it — is what step 5 already proves.

    // 4. Admin-add each new node (registers `Down`) — called on one of the
    // *original* nodes' admin ports, the simplest reliable operator path (the
    // relay reaches whichever control node currently leads). Now redundant
    // with each node's own automatic self-registration (ADR 0032 PR2), but
    // kept as the regression proving `admin_add_member`'s idempotent-no-op
    // path still works when a member is already registered.
    for &id in new_ids {
        let (status, body) = admin(
            base_admin[0],
            "POST",
            "/admin/member/add",
            Some(&format!("{{\"node\":{id}}}")),
        )
        .await;
        assert_eq!(status, 200, "admin add-member failed for {id}: {body}");
    }

    // 5. Promotion: each new node's own heartbeat_loop reaches the real control
    // group (its raftkv env's peer book already has the original control
    // addresses — the expanded config lists them), so it should flip from
    // `Down` to `Active` within a few heartbeat/detect cycles.
    let promoted = async {
        loop {
            let statuses = member_statuses(all_admin[0]).await;
            if new_ids
                .iter()
                .all(|id| statuses.get(id).map(String::as_str) == Some("Active"))
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), promoted)
        .await
        .unwrap_or_else(|_| panic!("added nodes never promoted to Active"));

    // 6. A phantom that never boots must NOT become Active (ADR 0030 hardening,
    // task 3): register a third, never-started raftkv id and confirm it stays
    // `Down` for well past the detector's own timing constants — it can never
    // heartbeat, so it can never be promoted.
    let phantom_id = all_raftkv_ids[4] + 1000; // an id nothing will ever run
    let (status, body) = admin(
        base_admin[0],
        "POST",
        "/admin/member/add",
        Some(&format!("{{\"node\":{phantom_id}}}")),
    )
    .await;
    assert_eq!(status, 200, "admin add-member failed for phantom: {body}");
    sleep(Duration::from_secs(2)).await;
    let statuses = member_statuses(all_admin[0]).await;
    assert_eq!(
        statuses.get(&phantom_id).map(String::as_str),
        Some("Down"),
        "a never-booted declared node must not drift off Down: {statuses:?}"
    );

    // 7. Rebalancing: poll until every raftkv id's replica count is within 1 of
    // every other's, including the two new nodes.
    let converged = async {
        loop {
            let map = tablet_map(base_admin[0]).await;
            let counts = replica_counts(&map, &all_raftkv_ids);
            if imbalance(&counts) <= 1 {
                return counts;
            }
            sleep(Duration::from_millis(300)).await;
        }
    };
    let converged_counts = timeout(Duration::from_secs(120), converged)
        .await
        .unwrap_or_else(|_| panic!("tablet replicas never spread across all 5 nodes within 120s"));
    for &id in new_ids {
        assert!(
            converged_counts[&id] > 0,
            "node {id} never gained a replica: {converged_counts:?}"
        );
    }

    // 8. Still serving, linearizably, throughout — via a client list that
    // includes the newly grown nodes (see `put`'s doc).
    for table in TABLES {
        put(&all_clients, table, b"k1", b"v1", 30).await;
        await_value(&all_clients, table, b"k1", b"v1", 30).await;
    }

    // 9. ADR 0032 PR1: a client connected only to an ORIGINAL node can read/write
    // a key whose tablet leader now lives on a GROWN node. Before this PR,
    // `client_route` on the original 3 was a static, process-start-only
    // snapshot that never learned a grown node's address (the ADR 0030
    // documented residual gap) — a write landing on an original node for a
    // tablet whose leader rebalancing just moved onto node 303/304 would have
    // no forward target at all. Now every node's `route_sync_loop` overlays
    // `Metadata.node_addrs[*].client` (populated by each node's own
    // `RegisterNodeAddrs` self-registration) onto its `client_route`, so this
    // must work with **`base_clients` only** — no grown node's own address in
    // the client list. By this point in the test (post-rebalance, step 7) at
    // least one table's tablet leader has migrated onto a grown node, so this
    // exercises the forward path, not just the local-serve path. A generous
    // timeout tolerates `route_sync_loop`'s `PEER_SYNC_INTERVAL` (200ms) cadence
    // plus the write's own propose/commit + relay hops.
    for table in TABLES {
        put(&base_clients, table, b"k2", b"v2", 30).await;
        await_value(&base_clients, table, b"k2", b"v2", 30).await;
    }

    // 10. `/admin/peers` on an original node eventually lists the grown nodes'
    // admin addresses too (ADR 0032 PR1 closes the same gap for the dashboard's
    // fan-out seed, `admin.rs::peers_view`'s union of the static `admin_addrs`
    // with the replicated `Metadata.node_addrs[*].admin`).
    let peers_include_grown = async {
        loop {
            let (status, body) = admin(base_admin[0], "GET", "/admin/peers", None).await;
            if status == 200 {
                let listed: Vec<String> = body["admin_addrs"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect()
                    })
                    .unwrap_or_default();
                let all_present = expanded_config
                    .nodes
                    .iter()
                    .all(|n| listed.contains(&n.admin.to_string()));
                if all_present {
                    return;
                }
            }
            sleep(Duration::from_millis(200)).await;
        }
    };
    timeout(Duration::from_secs(20), peers_include_grown)
        .await
        .unwrap_or_else(|_| {
            panic!("an original node's /admin/peers never listed the grown nodes' admin addrs")
        });

    for node in nodes {
        node.shutdown();
    }
}

/// `/admin/raftkv`'s `groups`: `(tablet, hosting node, is_leader)`.
async fn raftkv_groups(admin_addr: SocketAddr) -> Vec<(u64, u64, bool)> {
    let (status, body) = admin(admin_addr, "GET", "/admin/raftkv", None).await;
    if status != 200 {
        return Vec::new();
    }
    body["groups"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|g| {
                    (
                        g["tablet"].as_u64().expect("tablet"),
                        g["node"].as_u64().expect("node"),
                        g["is_leader"].as_bool().expect("is_leader"),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Regression for the dashboard health rollup (`dashboard_core.js`'s
/// `computeHealth()`): 3 -> 5 (growth) -> kill an ORIGINAL core node (index 0
/// — a control-core member, which can never be decommissioned via
/// `/admin/member/remove` and so stays `Down` in the member roster forever
/// unless restarted). Once the placement reconciler auto-replaces every
/// tablet the dead node used to replicate onto a live spare (the existing
/// `failure_auto_replaces_replica_onto_spare` cascade), the dashboard must
/// read the cluster as healthy — a lingering `Down` member must NOT hold the
/// whole cluster "degraded" once its data-loss risk is gone. Reproduces
/// `computeHealth()`/`tabletStatus()`'s logic in Rust over the real
/// `/admin/status` + `/admin/raftkv` fanned out across every surviving node,
/// exactly as the browser's cross-node fan-out does.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn dashboard_health_recovers_after_grown_cluster_loses_an_original_node() {
    let dir = tempfile::tempdir().unwrap();

    let (mut nodes, base_config) = bring_up(3, dir.path()).await;
    await_bootstrap(&nodes).await;
    let base_clients: Vec<SocketAddr> = base_config.nodes.iter().map(|a| a.client).collect();
    let base_admin: Vec<SocketAddr> = base_config.nodes.iter().map(|a| a.admin).collect();
    for table in TABLES {
        put(&base_clients, table, b"k0", b"v0", 30).await;
    }

    let (growth_nodes, expanded_config) = grow(&base_config, 2, dir.path()).await;
    nodes.extend(growth_nodes);
    let all_raftkv_ids = expanded_config.raftkv_ids();
    let new_ids = &all_raftkv_ids[3..];
    let all_admin: Vec<SocketAddr> = expanded_config.nodes.iter().map(|a| a.admin).collect();

    for &id in new_ids {
        let (status, body) = admin(
            base_admin[0],
            "POST",
            "/admin/member/add",
            Some(&format!("{{\"node\":{id}}}")),
        )
        .await;
        assert_eq!(status, 200, "admin add-member failed for {id}: {body}");
    }

    let promoted = async {
        loop {
            let statuses = member_statuses(all_admin[0]).await;
            if new_ids
                .iter()
                .all(|id| statuses.get(id).map(String::as_str) == Some("Active"))
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), promoted)
        .await
        .unwrap_or_else(|_| panic!("added nodes never promoted to Active"));

    let converged = async {
        loop {
            let map = tablet_map(base_admin[0]).await;
            let counts = replica_counts(&map, &all_raftkv_ids);
            if imbalance(&counts) <= 1 {
                return counts;
            }
            sleep(Duration::from_millis(300)).await;
        }
    };
    let converged_counts = timeout(Duration::from_secs(120), converged)
        .await
        .unwrap_or_else(|_| panic!("tablet replicas never spread across all 5 nodes within 120s"));
    println!("post-growth converged counts: {converged_counts:?}");

    // Kill an ORIGINAL core node (index 0) -- it can never be decommissioned
    // (control-core member), so it stays `Down` forever unless restarted;
    // this is a different asymmetry than killing a grown node.
    let kill_idx = 0;
    let killed_id = all_raftkv_ids[kill_idx];
    nodes[kill_idx].shutdown();
    let survivor_idx: Vec<usize> = (0..5).filter(|&i| i != kill_idx).collect();
    let survivor_admin: Vec<SocketAddr> = survivor_idx.iter().map(|&i| all_admin[i]).collect();
    println!("killed node {killed_id} (index {kill_idx})");

    // Wait for every tablet to drop the dead replica and be repaired back to
    // 3 live replicas (RF=3), polling from a survivor.
    let repaired = async {
        loop {
            let map = tablet_map(survivor_admin[0]).await;
            let statuses = member_statuses(survivor_admin[0]).await;
            let all_ok = map
                .values()
                .all(|(replicas, _)| replicas.len() == 3 && !replicas.contains(&killed_id));
            if all_ok {
                return (map, statuses);
            }
            sleep(Duration::from_millis(300)).await;
        }
    };
    let (final_map, final_statuses) = timeout(Duration::from_secs(120), repaired)
        .await
        .unwrap_or_else(|_| panic!("tablets never repaired off the dead node within 120s"));
    println!("post-repair tablet map: {final_map:?}");
    println!("post-repair member statuses: {final_statuses:?}");

    // Give the CP groups a further beat to actually elect/reconfigure on the
    // new replica set (metadata can converge slightly before the raft groups
    // do).
    sleep(Duration::from_secs(3)).await;

    // Now reproduce the dashboard's aggregation: fan out `/admin/raftkv` to
    // every survivor and merge by (tablet, node), exactly like
    // `cpGroupsByTablet()`.
    let mut groups_by_tablet: BTreeMap<u64, Vec<(u64, bool)>> = BTreeMap::new();
    for &addr in &survivor_admin {
        for (tablet, node, is_leader) in raftkv_groups(addr).await {
            let seen = groups_by_tablet.entry(tablet).or_default();
            if !seen.iter().any(|(n, _)| *n == node) {
                seen.push((node, is_leader));
            }
        }
    }
    println!("groups_by_tablet: {groups_by_tablet:?}");

    let down_count = final_statuses.values().filter(|s| *s == "Down").count();
    let mut leaderless = 0usize;
    let mut under_replicated = 0usize;
    for (tablet, (replicas, _epoch)) in &final_map {
        let gs = groups_by_tablet.get(tablet).cloned().unwrap_or_default();
        let has_leader = gs.iter().any(|(_, l)| *l);
        let configured = replicas.len();
        if !has_leader {
            leaderless += 1;
            println!("tablet {tablet} is LEADERLESS: gs={gs:?}");
        } else if configured > 0 && gs.len() < configured {
            under_replicated += 1;
            println!(
                "tablet {tablet} is UNDER-REPLICATED: configured={configured} gs.len()={} gs={gs:?}",
                gs.len()
            );
        }
    }
    println!("down_count={down_count} leaderless={leaderless} under_replicated={under_replicated}");

    // The killed original node is permanently `Down` (never decommissionable),
    // so this is exactly the scenario `computeHealth()` must not gate on
    // `down_count` for.
    assert_eq!(down_count, 1, "the killed original node should read Down");
    assert_eq!(
        leaderless, 0,
        "every tablet should have re-elected a leader by now"
    );
    assert_eq!(
        under_replicated, 0,
        "every tablet should be repaired back to its configured replica count"
    );

    for node in nodes {
        node.shutdown();
    }
}
