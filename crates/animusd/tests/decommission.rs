//! Node decommission (ADR 0032 PR3): drain-complete polling, `RemoveMember`,
//! and address-book/membership pruning — the second half of the seed/join
//! lifecycle whose first half `tests/seed_join.rs` covers (joining).
//!
//! Brings up a 3-node config core, joins a 4th node via `run_node_join` (no
//! expanded config, mirroring `tests/seed_join.rs`), waits for it to gain a
//! real tablet replica via rebalancing, then drives the full operator flow:
//! `POST /admin/drain` → poll `GET /admin/member/drain-status` to convergence
//! → `POST /admin/member/remove` → poll the member's disappearance from
//! `/admin/status` (membership **and** the address book) while the cluster
//! keeps serving reads/writes → stop the removed node's process → rejoin at
//! the same index with a fresh dir (proving id reuse after removal). Also
//! exercises the three refusal shapes: a still-**live** control-plane voter
//! can never be removed this way (ADR 0037 — a *dynamic* check now, not the
//! static original-members list ADR 0030/0032 used), an `Active` member
//! can't be removed before draining, and `/admin/member/remove` is
//! local-control-leader-only (a follower refuses cleanly, mirroring
//! `/admin/drain`).
//!
//! [`decommission_refuses_live_control_voter_then_succeeds_after_control_remove`]
//! (ADR 0037 PR4) drives the combined-node-is-a-control-voter two-phase
//! flow's server-side halves directly: refuse while the target's control id
//! is a live voter, control-remove it, then the same decommission succeeds.
//!
//! Real TCP/time — polls with generous timeouts, not deterministic assertions.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use animusd::{
    ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, StorageBackend, read_frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Several tables, mirroring `tests/seed_join.rs`'s `TABLES`: rebalancing
/// (ADR 0029) only ever proposes a move while it improves the *global*
/// imbalance and stops once `max - min <= 1`, so with just one table/tablet a
/// freshly-joined node may never receive a replica at all. Several
/// independent tablets raise the odds that at least one lands on it.
const TABLES: [&str; 3] = ["decomm0", "decomm1", "decomm2"];

/// Bring up the initial `n`-node config core (port-TOCTOU mitigation) — see
/// `support::bring_up_deadline`.
async fn bring_up(n: usize, dir: &Path) -> (Vec<Node>, ClusterConfig) {
    support::bring_up_deadline(n, dir, support::JOIN_DEADLINE).await
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

/// Join a fresh node with newly-allocated addresses (port-TOCTOU mitigation)
/// — see `support::join_fresh_deadline`.
async fn join_fresh(
    seeds: &[SocketAddr],
    index: usize,
    dir: &Path,
    backend: StorageBackend,
) -> (Node, RoleAddrs, PathBuf) {
    support::join_fresh_deadline(seeds, index, dir, backend, support::JOIN_DEADLINE).await
}

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed JSON)`.
async fn admin(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> (u16, serde_json::Value) {
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
    let value: serde_json::Value = serde_json::from_str(payload).expect("admin body is JSON");
    (status, value)
}

async fn drain_status(
    admin_addr: SocketAddr,
    node: &animus_env::NodeId,
) -> (u16, serde_json::Value) {
    admin(
        admin_addr,
        "GET",
        &format!("/admin/member/drain-status?node={node}"),
        None,
    )
    .await
}

async fn remove_member(
    admin_addr: SocketAddr,
    node: &animus_env::NodeId,
) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"node": node.to_string()}).to_string();
    admin(admin_addr, "POST", "/admin/member/remove", Some(&body)).await
}

/// Every member's status, `raftkv_id -> "Active"/"Down"/...`, from
/// `/admin/status`.
async fn member_statuses(
    admin_addr: SocketAddr,
) -> std::collections::BTreeMap<animus_env::NodeId, String> {
    let (_s, v) = admin(admin_addr, "GET", "/admin/status", None).await;
    v["members"]
        .as_object()
        .expect("members is an object")
        .iter()
        .map(|(id, m)| {
            (
                id.parse().expect("member id key is a valid NodeId"),
                m["status"].as_str().expect("status is a string").to_owned(),
            )
        })
        .collect()
}

/// A table whose tablet currently lists `raftkv_id` as a replica, if any (see
/// `tests/seed_join.rs::table_with_replica`'s doc for why the through-only-
/// this-node checks below must target a table this returns, not an arbitrary
/// one).
async fn table_with_replica(
    admin_addr: SocketAddr,
    raftkv_id: &animus_env::NodeId,
) -> Option<String> {
    let (_s, v) = admin(admin_addr, "GET", "/admin/status", None).await;
    v["tablets"]
        .as_object()
        .expect("tablets is an object")
        .values()
        .find_map(|t| {
            let has_replica = t["replicas"]
                .as_array()
                .expect("replicas is an array")
                .iter()
                .filter_map(|r| r.as_str())
                .any(|r| r == raftkv_id.as_str());
            has_replica
                .then(|| t["table"].as_str().map(str::to_owned))
                .flatten()
        })
}

async fn call(addr: SocketAddr, req: ClientRequest) -> Option<ClientResponse> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    animusd::write_frame(&mut stream, &req).await.ok()?;
    read_frame(&mut stream).await.ok()?
}

/// Try every client address in `clients` (round-robin) until one accepts the
/// write.
async fn put(clients: &[SocketAddr], table: &str, key: &[u8], value: &[u8], secs: u64) {
    let mut last: Option<ClientResponse> = None;
    let w = async {
        loop {
            for &c in clients {
                let resp = call(
                    c,
                    ClientRequest::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        table: table.to_string(),
                    },
                )
                .await;
                if let Some(ClientResponse::PutOk) = &resp {
                    return;
                }
                last = resp;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(secs), w)
        .await
        .unwrap_or_else(|_| {
            panic!("write of {table}/{key:?} never committed; last reply: {last:?}")
        });
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
                        stale: false,
                    },
                )
                .await
                    && v == want
                {
                    return;
                }
            }
            sleep(Duration::from_millis(150)).await;
        }
    };
    timeout(Duration::from_secs(secs), p)
        .await
        .unwrap_or_else(|_| panic!("key {table}/{key:?} never read back as {want:?}"));
}

fn leader_index(nodes: &[Node]) -> usize {
    nodes
        .iter()
        .position(Node::is_control_leader)
        .expect("no control leader among the core nodes")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn decommission_drains_removes_and_allows_id_reuse() {
    let dir = tempfile::tempdir().unwrap();

    // 1. Bring up a 3-node core; write through it.
    let (core_nodes, core_config) = bring_up(3, dir.path()).await;
    await_bootstrap(&core_nodes).await;
    let core_clients: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.client).collect();
    // ADR 0047: `--seed` now names the seed's intra address — a separate
    // list from `core_clients` above, which stays client-flavored for the
    // data-plane `put`/`await_value` calls it feeds.
    let core_intra: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.intra).collect();
    let core_admin: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.admin).collect();
    for table in TABLES {
        put(&core_clients, table, b"k0", b"v0", 30).await;
    }

    // 2. Join a 4th node so it gains a real tablet replica via rebalancing —
    // the data-plane hosted-voters signal, not just a `Metadata` member.
    let join_index = core_config.len();
    let join_raftkv_id = animusd::config::node_id(join_index);
    let (joined, _joined_addrs, _joined_dir) = join_fresh(
        &core_intra,
        join_index,
        dir.path(),
        StorageBackend::default(),
    )
    .await;

    let promoted = async {
        loop {
            if member_statuses(core_admin[0])
                .await
                .get(&join_raftkv_id)
                .map(String::as_str)
                == Some("Active")
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), promoted)
        .await
        .unwrap_or_else(|_| panic!("joined node never promoted to Active"));

    let hosted_table: String = {
        let discover = async {
            loop {
                if let Some(table) = table_with_replica(core_admin[0], &join_raftkv_id).await {
                    return table;
                }
                sleep(Duration::from_millis(300)).await;
            }
        };
        timeout(Duration::from_secs(90), discover)
            .await
            .unwrap_or_else(|_| panic!("joined node never gained a tablet replica"))
    };

    // Sanity: the joined node genuinely serves before decommission starts.
    put(&[joined.client_addr()], &hosted_table, b"jk0", b"jv0", 30).await;
    await_value(&core_clients, &hosted_table, b"jk0", b"jv0", 30).await;

    let leader = leader_index(&core_nodes);
    let follower = (0..core_nodes.len())
        .find(|&i| i != leader)
        .expect("a follower exists in a 3-node core");

    // 3. Refusal: an original control-core member can never be decommissioned
    // this way, regardless of its status.
    {
        let core_raftkv_id = animusd::config::node_id(0);
        let (status, body) = remove_member(core_admin[leader], &core_raftkv_id).await;
        assert_eq!(
            status, 409,
            "removing an original core member should be refused: {body}"
        );
    }

    // 4. Refusal: the joined node is still Active — not drained yet.
    {
        let (status, body) = remove_member(core_admin[leader], &join_raftkv_id).await;
        assert_eq!(
            status, 409,
            "removing an Active member should be refused: {body}"
        );
    }

    // 5. Drain the joined node on the leader's admin port.
    {
        let body = serde_json::json!({"node": join_raftkv_id}).to_string();
        let (status, resp) = admin(core_admin[leader], "POST", "/admin/drain", Some(&body)).await;
        assert_eq!(status, 200, "drain failed: {resp}");
    }

    // 6. Poll drain-status until the reconciler + rebalancer + release-GC have
    // actually moved every tablet off it, and it is no longer Active.
    let drained = async {
        loop {
            let (status, body) = drain_status(core_admin[leader], &join_raftkv_id).await;
            if status == 200 {
                let remaining = body["tablets_remaining"].as_u64().unwrap_or(u64::MAX);
                let node_status = body["status"].as_str().unwrap_or("");
                if remaining == 0 && node_status != "Active" {
                    return;
                }
            }
            sleep(Duration::from_millis(200)).await;
        }
    };
    timeout(Duration::from_secs(60), drained)
        .await
        .unwrap_or_else(|_| panic!("joined node never finished draining"));

    // 7. Refusal: `/admin/member/remove` is local-control-leader-only — a
    // follower refuses cleanly (not relayed), mirroring `/admin/drain`. The
    // member is now fully drained, so this proves the leader check itself,
    // not a leftover "not drained" rejection.
    {
        let (status, body) = remove_member(core_admin[follower], &join_raftkv_id).await;
        assert_eq!(
            status, 409,
            "remove on a follower's admin port should be refused: {body}"
        );
        let msg = body["error"].as_str().unwrap_or_default();
        assert!(
            msg.to_ascii_lowercase().contains("leader"),
            "expected a leader-routing error from a follower, got: {msg}"
        );
    }

    // 8. Remove on the leader.
    {
        let (status, body) = remove_member(core_admin[leader], &join_raftkv_id).await;
        assert_eq!(status, 200, "remove failed: {body}");
    }

    // 9. Poll: member absent AND its address-book entry pruned from
    // `/admin/status`, while the cluster keeps serving.
    //
    // **Idle-progress poll, not a flat deadline.** `/admin/member/remove`
    // returning 200 only means `MetaCommand::RemoveMember` was *accepted* by
    // the local Raft core (`ClientCtx::admin_remove_member`, mirroring every
    // other control-plane admin action's fire-and-return shape) — the actual
    // removal only becomes visible here once `RaftNode`'s **async apply
    // task** (ADR 0038 PR3) merges it into the `Metadata` cache
    // `/admin/status` reads, which is deliberately decoupled from Raft
    // consensus (a slow/contended engine merge must never risk tripping an
    // election) and so carries **no latency bound under contention** — see
    // `ControlHandle::engine_applied_index`'s doc. This test's own
    // instrumented reproduction (`taskset -c 0,1` plus background load,
    // mimicking `cargo test --workspace`-scale contention) caught
    // `/admin/raft`'s `commit_index`/`last_applied` converge across all
    // three core nodes in under a second while the apply task's own
    // `engine_applied_index` sat frozen — and, separately, made zero
    // progress for a full 30s, then a full 60s, before eventually catching
    // up — proving no single fixed deadline is principled here: the apply
    // task can legitimately take arbitrarily long to *make progress* under
    // contention, but once it stops making progress at all for a while with
    // the target command still unapplied, that is no longer contention, it
    // is a real bug. So this polls for **forward progress** of
    // `engine_applied_index` (any single node's own apply-task watermark,
    // read here off `core_admin[0]`) with a generous but bounded idle
    // window, and only fails once that watermark has stopped advancing
    // for `IDLE_STALL_TIMEOUT` with the member still present — plus an
    // outer backstop against genuine deadlock. See
    // `docs/engineering-lessons.md`. This shape is now also factored into
    // `support::poll_until_or_stalled` (added once `cluster_growth.rs`
    // needed the identical pattern at several call sites) — kept hand-rolled
    // here rather than migrated, since this file's version already reads
    // `/admin/status` inline alongside the removal-specific `members_gone`/
    // `addrs_gone` check below.
    const IDLE_STALL_TIMEOUT: Duration = Duration::from_secs(60);
    const OVERALL_BACKSTOP: Duration = Duration::from_secs(300);
    let removed = async {
        let overall_deadline = tokio::time::Instant::now() + OVERALL_BACKSTOP;
        let mut last_progress_at = tokio::time::Instant::now();
        let mut last_engine_applied: Option<u64> = None;
        loop {
            let (status, body) = admin(core_admin[0], "GET", "/admin/status", None).await;
            if status == 200 {
                let key = join_raftkv_id.to_string();
                let members_gone = !body["members"]
                    .as_object()
                    .is_some_and(|m| m.contains_key(&key));
                let addrs_gone = !body["node_addrs"]
                    .as_object()
                    .is_some_and(|m| m.contains_key(&key));
                if members_gone && addrs_gone {
                    return;
                }
            }
            let (raft_status, raft_body) = admin(core_admin[0], "GET", "/admin/raft", None).await;
            if raft_status == 200
                && let Some(engine_applied) = raft_body["engine_applied_index"].as_u64()
            {
                if last_engine_applied != Some(engine_applied) {
                    last_engine_applied = Some(engine_applied);
                    last_progress_at = tokio::time::Instant::now();
                } else if last_progress_at.elapsed() >= IDLE_STALL_TIMEOUT {
                    panic!(
                        "removed node never disappeared from /admin/status, and the \
                         apply task's engine_applied_index has been stuck at \
                         {engine_applied} for {IDLE_STALL_TIMEOUT:?} — this is no longer \
                         contention-driven lag, something is actually stuck"
                    );
                }
            }
            if tokio::time::Instant::now() >= overall_deadline {
                panic!(
                    "removed node never disappeared from /admin/status within the \
                     {OVERALL_BACKSTOP:?} backstop, despite apply-task progress \
                     (last engine_applied_index={last_engine_applied:?})"
                );
            }
            sleep(Duration::from_millis(150)).await;
        }
    };
    // No outer `tokio::time::timeout` here — `removed`'s own two panics above
    // are its bounds (a real stall, or the generous overall backstop); a
    // wrapping fixed timeout would reintroduce exactly the "arbitrary flat
    // deadline" problem this poll was rewritten to avoid.
    removed.await;

    // Cluster still serves reads + writes through the core.
    put(&core_clients, &hosted_table, b"post-remove", b"ok", 30).await;
    await_value(&core_clients, &hosted_table, b"post-remove", b"ok", 30).await;

    // 10. Stop the removed node's process.
    joined.shutdown_graceful().await;
    sleep(Duration::from_millis(200)).await;

    // 11. Rejoin with the SAME index at a FRESH dir — proving id reuse after
    // removal (remove + a fresh process at the same raftkv id is, by design,
    // equivalent to a fresh join).
    let (rejoined, _rejoined_addrs, _rejoined_dir) = join_fresh(
        &core_intra,
        join_index,
        &dir.path().join("rejoin"),
        StorageBackend::default(),
    )
    .await;

    let rejoined_promoted = async {
        loop {
            if member_statuses(core_admin[0])
                .await
                .get(&join_raftkv_id)
                .map(String::as_str)
                == Some("Active")
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), rejoined_promoted)
        .await
        .unwrap_or_else(|_| panic!("rejoined node (same id, fresh dir) never promoted to Active"));

    await_value(&[rejoined.client_addr()], &hosted_table, b"jk0", b"jv0", 30).await;

    rejoined.shutdown_graceful().await;
    for node in core_nodes {
        node.shutdown_graceful().await;
    }
}

/// `/admin/raftkv`'s `groups`: `(tablet, hosting node, is_leader)`.
async fn raftkv_groups(admin_addr: SocketAddr) -> Vec<(u64, animus_env::NodeId, bool)> {
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
                        g["node"]
                            .as_str()
                            .expect("node")
                            .parse()
                            .expect("node id parses"),
                        g["is_leader"].as_bool().expect("is_leader"),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Regression for the dashboard health rollup (`dashboard_core.js`'s
/// `computeHealth()`): 3 -> join 2 (5) -> drain + remove one joined node (4,
/// a real decommission-driven shrink, not a crash). Reproduces
/// `computeHealth()`/`tabletStatus()`'s logic in Rust over the real
/// `/admin/status` + `/admin/raftkv` fanned out across every remaining node —
/// the dashboard must read the shrunk cluster as healthy once rebalancing
/// converges, same as `dashboard_health_recovers_after_grown_cluster_loses_an_original_node`
/// (`tests/cluster_growth.rs`) proves for a bare-crash shrink.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn dashboard_health_recovers_after_decommission_shrink() {
    let dir = tempfile::tempdir().unwrap();

    let (core_nodes, core_config) = bring_up(3, dir.path()).await;
    await_bootstrap(&core_nodes).await;
    let core_clients: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.client).collect();
    // ADR 0047: `--seed` now names the seed's intra address.
    let core_intra: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.intra).collect();
    let core_admin: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.admin).collect();
    for table in TABLES {
        put(&core_clients, table, b"k0", b"v0", 30).await;
    }

    // Join two more nodes (3 -> 5), one at a time.
    let mut joined_nodes = Vec::new();
    let mut joined_ids = Vec::new();
    for i in 0..2 {
        let join_index = core_config.len() + i;
        let join_raftkv_id = animusd::config::node_id(join_index);
        let (node, addrs, _node_dir) = join_fresh(
            &core_intra,
            join_index,
            dir.path(),
            StorageBackend::default(),
        )
        .await;
        let promoted = async {
            loop {
                if member_statuses(core_admin[0])
                    .await
                    .get(&join_raftkv_id)
                    .map(String::as_str)
                    == Some("Active")
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        };
        timeout(Duration::from_secs(20), promoted)
            .await
            .unwrap_or_else(|_| panic!("joined node {join_index} never promoted to Active"));
        joined_nodes.push(node);
        joined_ids.push((join_raftkv_id, addrs));
    }
    println!("joined ids: {joined_ids:?}");

    // Pick the first joined node to drain + remove (5 -> 4).
    let (target_id, _target_addrs) = joined_ids[0].clone();
    let leader = leader_index(&core_nodes);

    {
        let body = serde_json::json!({"node": target_id}).to_string();
        let (status, resp) = admin(core_admin[leader], "POST", "/admin/drain", Some(&body)).await;
        assert_eq!(status, 200, "drain failed: {resp}");
    }
    let drained = async {
        loop {
            let (status, body) = drain_status(core_admin[leader], &target_id).await;
            if status == 200 {
                let remaining = body["tablets_remaining"].as_u64().unwrap_or(u64::MAX);
                let node_status = body["status"].as_str().unwrap_or("");
                if remaining == 0 && node_status != "Active" {
                    return;
                }
            }
            sleep(Duration::from_millis(200)).await;
        }
    };
    timeout(Duration::from_secs(60), drained)
        .await
        .unwrap_or_else(|_| panic!("target node never finished draining"));
    {
        let (status, body) = remove_member(core_admin[leader], &target_id).await;
        assert_eq!(status, 200, "remove failed: {body}");
    }
    let removed = async {
        loop {
            let (status, body) = admin(core_admin[0], "GET", "/admin/status", None).await;
            if status == 200 {
                let key = target_id.to_string();
                let members_gone = !body["members"]
                    .as_object()
                    .is_some_and(|m| m.contains_key(&key));
                if members_gone {
                    return;
                }
            }
            sleep(Duration::from_millis(150)).await;
        }
    };
    timeout(Duration::from_secs(30), removed)
        .await
        .unwrap_or_else(|_| panic!("removed node never disappeared from /admin/status"));

    // Give the survivors' reconcilers a further beat to settle.
    sleep(Duration::from_secs(3)).await;

    joined_nodes[0].shutdown_graceful().await;

    let mut survivor_admin: Vec<SocketAddr> = core_admin.clone();
    survivor_admin.push(joined_ids[1].1.admin);

    let (_status, status_body) = admin(survivor_admin[0], "GET", "/admin/status", None).await;
    // Member ids are `NodeId` strings now (ADR 0040 PR3) — only the *values*
    // (status strings / replica counts) matter below, so the key type just
    // needs to parse without panicking, not carry real `NodeId` semantics.
    let member_statuses: std::collections::BTreeMap<String, String> = status_body["members"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(id, m)| (id.clone(), m["status"].as_str().unwrap().to_owned()))
        .collect();
    let tablets: std::collections::BTreeMap<u64, usize> = status_body["tablets"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(id, t)| (id.parse().unwrap(), t["replicas"].as_array().unwrap().len()))
        .collect();
    println!("member_statuses: {member_statuses:?}");
    println!("tablets: {tablets:?}");

    let mut groups_by_tablet: std::collections::BTreeMap<u64, Vec<(animus_env::NodeId, bool)>> =
        std::collections::BTreeMap::new();
    for &addr in &survivor_admin {
        for (tablet, node, is_leader) in raftkv_groups(addr).await {
            let seen = groups_by_tablet.entry(tablet).or_default();
            if !seen.iter().any(|(n, _)| *n == node) {
                seen.push((node, is_leader));
            }
        }
    }
    println!("groups_by_tablet: {groups_by_tablet:?}");

    let down_count = member_statuses.values().filter(|s| *s == "Down").count();
    let mut leaderless = 0usize;
    let mut under_replicated = 0usize;
    for (tablet, replicas) in &tablets {
        let gs = groups_by_tablet.get(tablet).cloned().unwrap_or_default();
        let has_leader = gs.iter().any(|(_, l)| *l);
        let configured = *replicas;
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

    // A cleanly-decommissioned node is gone from `members` entirely (not
    // lingering `Down`), and every tablet should already be repaired.
    assert_eq!(
        down_count, 0,
        "a decommissioned node should be fully removed, not left Down"
    );
    assert_eq!(
        leaderless, 0,
        "every tablet should have re-elected a leader by now"
    );
    assert_eq!(
        under_replicated, 0,
        "every tablet should be repaired back to its configured replica count"
    );

    joined_nodes[1].shutdown_graceful().await;
    for node in core_nodes {
        node.shutdown_graceful().await;
    }
}

/// **ADR 0037 PR4**: a combined node's control-voter status must gate its
/// data-plane decommission through the LIVE control config, not the static
/// original-members snapshot `admin_remove_member` used to read before this
/// PR (ADR 0030/0032's "an original control-core member can never be
/// decommissioned" rule — see the refusal in
/// `decommission_drains_removes_and_allows_id_reuse` above, which still
/// holds because that test never control-removes anyone).
///
/// Drives the plan §7/§8 two-phase flow's server-side halves directly (the
/// same admin actions `animus admin decommission --force-control-remove`
/// orchestrates client-side, `run_control_remove` + a convergence poll then
/// the ordinary drain → drain-status → remove flow): fully drain an original
/// combined node, confirm `/admin/member/remove` still refuses it (its
/// control id is a *live* voter), control-remove it, poll to convergence,
/// then confirm the *same* `/admin/member/remove` call now succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn decommission_refuses_live_control_voter_then_succeeds_after_control_remove() {
    let dir = tempfile::tempdir().unwrap();

    // 1. Bring up a 3-node combined core (control voters {0,1,2}).
    let (core_nodes, core_config) = bring_up(3, dir.path()).await;
    await_bootstrap(&core_nodes).await;
    let core_clients: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.client).collect();
    // ADR 0047: `--seed` now names the seed's intra address.
    let core_intra: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.intra).collect();
    let core_admin: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.admin).collect();
    for table in TABLES {
        put(&core_clients, table, b"k0", b"v0", 30).await;
    }

    // 2. Join a 4th combined node so there's somewhere to relocate the
    // target's replicas to when it drains — it stays a permanent control
    // non-voter (ADR 0030); this test only needs its data role.
    let join_index = core_config.len();
    let (joined, _joined_addrs, _joined_dir) = join_fresh(
        &core_intra,
        join_index,
        dir.path(),
        StorageBackend::default(),
    )
    .await;
    let join_raftkv_id = animusd::config::node_id(join_index);
    let promoted = async {
        loop {
            if member_statuses(core_admin[0])
                .await
                .get(&join_raftkv_id)
                .map(String::as_str)
                == Some("Active")
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), promoted)
        .await
        .unwrap_or_else(|_| panic!("joined node never promoted to Active"));

    // 3. Target a NON-LEADER original combined node (control id == its
    // index here) — keeps this test focused on the decommission-integration
    // behavior, not the leader-self-removal transfer mechanics already
    // covered by `control_membership_admin.rs`.
    let leader = leader_index(&core_nodes);
    let target = (0..3usize)
        .find(|&i| i != leader)
        .expect("a non-leader exists in a 3-node core");
    let target_control_id = animusd::config::node_id(target);
    let target_raftkv_id = animusd::config::node_id(target);
    let leader_admin = core_admin[leader];

    // 4. Drain the target and poll to convergence — its replicas relocate to
    // the joined 4th node.
    {
        let body = serde_json::json!({"node": target_raftkv_id}).to_string();
        let (status, resp) = admin(leader_admin, "POST", "/admin/drain", Some(&body)).await;
        assert_eq!(status, 200, "drain failed: {resp}");
    }
    let drained = async {
        loop {
            let (status, body) = drain_status(leader_admin, &target_raftkv_id).await;
            if status == 200 {
                let remaining = body["tablets_remaining"].as_u64().unwrap_or(u64::MAX);
                let node_status = body["status"].as_str().unwrap_or("");
                if remaining == 0 && node_status != "Active" {
                    return;
                }
            }
            sleep(Duration::from_millis(200)).await;
        }
    };
    timeout(Duration::from_secs(60), drained)
        .await
        .unwrap_or_else(|_| panic!("target node never finished draining"));

    // 5. Refusal: fully drained, but its control id is STILL a live voter —
    // `/admin/member/remove` refuses (409), naming the control-plane reason.
    {
        let (status, body) = remove_member(leader_admin, &target_raftkv_id).await;
        assert_eq!(
            status, 409,
            "removing a still-live control voter should be refused: {body}"
        );
        let msg = body["error"]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase();
        assert!(
            msg.contains("control"),
            "refusal should name the control-plane reason: {msg}"
        );
    }

    // 6. Control-remove it — the two-phase flow's first step.
    {
        let body = serde_json::json!({"node": target_control_id}).to_string();
        let (status, resp) = admin(
            leader_admin,
            "POST",
            "/admin/control/member/remove",
            Some(&body),
        )
        .await;
        assert_eq!(status, 200, "control/member/remove failed: {resp}");
    }
    let control_removed = async {
        loop {
            let (status, body) = admin(leader_admin, "GET", "/admin/control/members", None).await;
            if status == 200
                && let Some(voters) = body["voters"].as_array()
                && !voters
                    .iter()
                    .any(|v| v.as_str() == Some(target_control_id.as_str()))
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(15), control_removed)
        .await
        .unwrap_or_else(|_| panic!("control voter removal never converged"));

    // 7. The two-phase flow's second half: the SAME `/admin/member/remove`
    // call now succeeds — proving the refusal in step 5 read the LIVE
    // config (ADR 0037), not a static original-members snapshot that would
    // have refused forever (the pre-ADR-0037 behavior).
    {
        let (status, body) = remove_member(leader_admin, &target_raftkv_id).await;
        assert_eq!(
            status, 200,
            "removing the now-control-removed node should succeed: {body}"
        );
    }

    joined.shutdown_graceful().await;
    for node in core_nodes {
        node.shutdown_graceful().await;
    }
}
