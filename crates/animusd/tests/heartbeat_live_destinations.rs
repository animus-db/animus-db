//! ADR 0037 "known deferrals" #1, closed by this PR: `heartbeat_loop`'s
//! destination list (and, discovered while fixing it, the raftkv env's own
//! peer *address book*) used to be a bring-up-time snapshot with no
//! live-overlay refresh — a raftkv node started before a control voter was
//! added at runtime never heartbeated that voter directly, so if it later
//! became leader, this specific already-running node's heartbeats kept
//! missing it (see `crates/animusd/CLAUDE.md`'s "cluster's members are the
//! raftkv ids" gotcha and `docs/engineering-lessons.md`'s ADR 0037 PR4 audit
//! entry for the pre-fix state of the world).
//!
//! [`heartbeat_reaches_a_runtime_added_voter_after_it_becomes_leader`] proves
//! the fix end to end over `ProdEnv` (real threads/time, converged-or-timeout
//! polls throughout, never a fixed sleep-and-hope):
//!
//! 1. A single combined node (id 0) starts life as the control group's sole
//!    voter — the pre-existing node whose `heartbeat_loop_live` was spawned
//!    with a static destination list of exactly `{0}` at bring-up, long
//!    before the runtime-added voter below ever existed.
//! 2. A second, control-only node joins as a quiet non-voter (mirroring ADR
//!    0030's growth shape for the control role) and is added as a genuine
//!    control voter through `POST /admin/control/member/add` — a **runtime**
//!    add, not one present at bring-up, so this exercises the real gap (a
//!    voter added at bring-up would trivially already be in every node's
//!    static list).
//! 3. Node 0 self-removes its own voter slot through `POST
//!    /admin/control/member/remove` — since only two voters exist, this
//!    deterministically arms a leadership transfer to the *only* other live
//!    voter (the runtime-added one), not a coin flip between candidates.
//! 4. Once the runtime-added voter reports itself leader, this polls its own
//!    `GET /admin/raft` view for `believes_alive: true` against node 0's
//!    raftkv id, sustained across several `DETECT_TIMEOUT` windows (500ms
//!    each) — the failure detector's own verdict that node 0's heartbeats are
//!    genuinely and repeatedly arriving at the new leader, not a one-off
//!    fluke. Before this PR, this would have failed: node 0's heartbeat
//!    destination list never named the runtime-added voter at all, and even
//!    if it had, its raftkv env's peer book never learned the runtime-added
//!    voter's control address (`ProdEnv::send` silently drops an
//!    address-less peer) — both halves of the fix are required for this
//!    assertion to ever pass.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_env::nid;
use animusd::config::NodeRole;
use animusd::{ClusterConfig, Node, RoleAddrs, StorageBackend};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// One HTTP/1.0 request to the admin endpoint — same shape as
/// `tests/control_membership_admin.rs::admin`.
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

async fn control_members(admin_addr: SocketAddr) -> (u16, serde_json::Value) {
    admin(admin_addr, "GET", "/admin/control/members", None).await
}

async fn add_control_member(
    admin_addr: SocketAddr,
    node: u64,
    addr: SocketAddr,
) -> (u16, serde_json::Value) {
    let body =
        serde_json::json!({"node": nid(node).to_string(), "addr": addr.to_string()}).to_string();
    admin(admin_addr, "POST", "/admin/control/member/add", Some(&body)).await
}

async fn remove_control_member(admin_addr: SocketAddr, node: u64) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"node": nid(node).to_string()}).to_string();
    admin(
        admin_addr,
        "POST",
        "/admin/control/member/remove",
        Some(&body),
    )
    .await
}

fn voters_of(body: &serde_json::Value) -> Option<Vec<String>> {
    body["voters"].as_array().map(|a| {
        a.iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect()
    })
}

/// Whether `GET /admin/raft` on `admin_addr` currently reports `node` alive.
async fn believes_alive(admin_addr: SocketAddr, node: u64) -> bool {
    let (status, body) = admin(admin_addr, "GET", "/admin/raft", None).await;
    if status != 200 {
        return false;
    }
    let want = nid(node).to_string();
    body["members"]
        .as_array()
        .and_then(|members| {
            members
                .iter()
                .find(|m| m["node"].as_str() == Some(want.as_str()))
                .and_then(|m| m["believes_alive"].as_bool())
        })
        .unwrap_or(false)
}

/// Join a **quiet non-voter** control-only node to an already-running control
/// group described by `config` — same helper shape as
/// `tests/control_membership_admin.rs::join_control_nonvoter`.
async fn join_control_nonvoter(
    config: &ClusterConfig,
    new_control_id: u64,
    dir: &Path,
) -> (Node, RoleAddrs) {
    for attempt in 0..16 {
        let raw = support::free_addrs(5);
        let addrs = RoleAddrs {
            id: nid(new_control_id),
            role: NodeRole::Control,
            internal: raw[0],
            client: raw[1],
            dynamo: raw[2],
            cql: raw[3],
            admin: raw[4],
        };
        let bound = match animusd::Node::bind_control(
            nid(new_control_id),
            addrs.clone(),
            dir.join(format!("grow-{attempt}")),
        )
        .await
        {
            Ok(b) => b,
            Err(_) => {
                sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let mut client_route: std::collections::BTreeMap<animus_env::NodeId, SocketAddr> =
            std::collections::BTreeMap::new();
        for (i, a) in config.nodes.iter().enumerate() {
            client_route.insert(animusd::config::node_id(i), a.client);
        }
        let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();
        let node = bound
            .start_control_with(
                config.peer_book(),
                config.control_ids(),
                client_route,
                admin_addrs,
                StorageBackend::Memory,
            )
            .await
            .expect("open the growth control-only node's system-keyspace engine");
        return (node, addrs);
    }
    panic!("could not bind the growth control-only node after retries");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn heartbeat_reaches_a_runtime_added_voter_after_it_becomes_leader() {
    let dir = tempfile::tempdir().unwrap();

    // Step 1: a single combined node, id 0 — the control group's sole voter
    // at bring-up. Its `heartbeat_loop_live` starts with a static destination
    // list of exactly `{0}` (itself); no other control voter exists yet.
    let (node0, config) = support::start_single_node(dir.path(), StorageBackend::Memory).await;
    let node0_admin = node0.admin_addr();
    let node0_raftkv_id = animusd::config::node_id(0);

    timeout(Duration::from_secs(20), async {
        loop {
            if node0.is_control_leader() && !node0.metadata().members.is_empty() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("single node never became its own control leader / registered its raftkv id");

    // Step 2 + genuine RUNTIME add: bring up a control-only node as a quiet
    // non-voter, then add it as a real control voter through the admin
    // action — this id was never in node 0's `control_ids` at bring-up.
    let new_id = 1u64;
    let (grown, grown_addrs) = join_control_nonvoter(&config, new_id, dir.path()).await;
    let grown_admin = grown.admin_addr();
    let grown_control_addr = grown_addrs.internal;

    // `grown` self-registers its own `NodeAddrs` (relayed, since it starts
    // life a non-voter — `MetaCommand::RegisterNode`'s CAS, ADR 0040 PR4)
    // — its `internal` address is already correct from that very first
    // self-registration (ADR 0040 PR1: one address per node, not a separate
    // control/raftkv pair populated later by `control/member/add`). Wait
    // for the self-registration to land on the real cluster before adding
    // the voter — mirrors the real operator runbook's own "confirm it's up
    // first" step; skipping this wait races two independent proposals for
    // the same id's `node_addrs` entry, and the CAS correctly refuses
    // whichever loses instead of silently overwriting it.
    let self_registered = async {
        loop {
            if node0.metadata().node_addrs.contains_key(&nid(new_id)) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(15), self_registered)
        .await
        .expect("grown node's own self-registration never landed on the real cluster");
    sleep(Duration::from_secs(11)).await;

    let (status, body) = add_control_member(node0_admin, new_id, grown_control_addr).await;
    assert_eq!(status, 200, "control/member/add failed: {body}");

    // Converge on {0,1} on both sides before forcing the transfer, so
    // `peer_sync_loop`/`control_peer_sync_loop` have had a tick to merge the
    // runtime-added voter's address in.
    for &a in &[node0_admin, grown_admin] {
        let converged = async {
            loop {
                let (status, body) = control_members(a).await;
                if status == 200
                    && voters_of(&body) == Some(vec!["n0".to_string(), "n1".to_string()])
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        };
        timeout(Duration::from_secs(30), converged)
            .await
            .unwrap_or_else(|_| panic!("node at {a} never converged to voters {{0,1}}"));
    }

    // Step 3: force a leadership transfer to the runtime-added voter. With
    // exactly two voters, node 0 self-removing its own slot has only one
    // possible transfer target — the runtime-added voter, id 1 — so this is
    // deterministic, not a race between candidates. `admin_remove_control_
    // member` arms the transfer and reports it via an error rather than
    // completing the removal (mirrors `control_membership_admin.rs`'s own
    // self-removal tests) — the voter set stays `{0,1}`; only leadership
    // moves.
    let (status, _body) = remove_control_member(node0_admin, 0).await;
    assert_eq!(
        status, 409,
        "self-removal should report the armed transfer, not silently succeed"
    );

    timeout(Duration::from_secs(15), async {
        loop {
            if grown.is_control_leader() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("leadership never transferred to the runtime-added voter");

    // The config is unaffected by the transfer alone — still both voters.
    let (status, body) = control_members(grown_admin).await;
    assert_eq!(status, 200, "control/members failed: {body}");
    assert_eq!(
        voters_of(&body),
        Some(vec!["n0".to_string(), "n1".to_string()])
    );

    // Step 4: the real proof. Poll the new leader's own `/admin/raft` view
    // for `believes_alive: true` against node 0's raftkv id, and require it
    // to STAY true across several full `DETECT_TIMEOUT` (500ms) windows —
    // not a one-off race, but node 0's heartbeats genuinely and repeatedly
    // reaching the runtime-added leader over both halves of the fix
    // (`heartbeat_loop_live`'s live destination list + `peer_sync_loop`'s
    // control-address merge into the raftkv env's own peer book).
    timeout(Duration::from_secs(20), async {
        loop {
            if believes_alive(grown_admin, 0).await {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the runtime-added leader (id {new_id}) never observed node 0's (raftkv id \
             {node0_raftkv_id}) heartbeats as alive — the live-destination-list and/or \
             address-book fix did not reach it"
        )
    });

    let sustained_deadline = tokio::time::Instant::now() + Duration::from_millis(1_700);
    while tokio::time::Instant::now() < sustained_deadline {
        assert!(
            believes_alive(grown_admin, 0).await,
            "node 0's heartbeats stopped reaching the runtime-added leader partway through \
             the sustained-liveness window"
        );
        sleep(Duration::from_millis(150)).await;
    }

    grown.shutdown();
    node0.shutdown();
}
