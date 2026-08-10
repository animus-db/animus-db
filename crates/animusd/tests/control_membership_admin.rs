//! Control-plane membership-change admin API + CLI surface (ADR 0037 PR3):
//! `POST /admin/control/member/{add,remove}` + `GET /admin/control/members`.
//!
//! Two scenarios, each its own bring-up (real TCP/time, poll with generous
//! timeouts, never a fixed sleep-and-hope):
//!
//! - [`grow_control_group_converges_everywhere`]: a genuine split deployment
//!   (3 control-only + 1 data-only, `support::bring_up_split`) grows a 4th
//!   control-only voter that starts life as a quiet non-voter (`--config`
//!   listing only the original 3 as its own peers/`control_ids` — mirroring
//!   ADR 0030's growth-node shape, just for the control role instead of the
//!   data role) — `POST /admin/control/member/add` on the leader, then poll
//!   `GET /admin/control/members` for convergence on *every* node, including
//!   the data-only node's `ControlHandle::Remote` mirror (already wired by
//!   PR2's `control_voters` wire field — no new plumbing needed there).
//! - [`remove_control_voter_refusals_transfer_and_quorum_warnings`]: a 3-node
//!   combined core exercises every refusal/warning shape in one flow: an
//!   unknown-node remove is an idempotent no-op; a non-leader voter removes
//!   cleanly with no warning; removing the current leader's own slot arms a
//!   leadership transfer and returns the same "retry on the leader" refusal
//!   as any other not-leader case (not a silent success — this call cannot
//!   complete the removal itself once it has stepped down); retrying against
//!   the new leader succeeds; removing down to exactly one voter *proceeds*
//!   but carries a `warning`; removing the last voter is refused outright;
//!   and neither admin action is relayable — a follower's admin port refuses
//!   both, symmetric with `/admin/drain`/`/admin/member/remove`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animusd::config::NodeRole;
use animusd::{ClusterConfig, Node, RoleAddrs};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed JSON)`
/// — the same shape `tests/decommission.rs::admin` uses.
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
    let body = serde_json::json!({"node": node, "addr": addr.to_string()}).to_string();
    admin(admin_addr, "POST", "/admin/control/member/add", Some(&body)).await
}

async fn remove_control_member(admin_addr: SocketAddr, node: u64) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"node": node}).to_string();
    admin(
        admin_addr,
        "POST",
        "/admin/control/member/remove",
        Some(&body),
    )
    .await
}

fn voters_of(body: &serde_json::Value) -> Option<Vec<u64>> {
    body["voters"]
        .as_array()
        .map(|a| a.iter().filter_map(serde_json::Value::as_u64).collect())
}

/// Bring up an `n`-node **combined-mode** core, one process per node — the
/// same shape `tests/decommission.rs::bring_up` uses.
async fn bring_up_combined(n: usize, dir: &Path) -> (Vec<Node>, ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<RoleAddrs> = (0..n)
            .map(|i| RoleAddrs {
                role: NodeRole::Both,
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
            match animusd::run_node(&config, i, dir.join(format!("core-{attempt}-{i}"))).await {
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
    panic!("could not bring up the combined core after retries");
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

fn leader_index(nodes: &[Node]) -> usize {
    nodes
        .iter()
        .position(Node::is_control_leader)
        .expect("no control leader among the core nodes")
}

/// Join a **quiet non-voter** control-only node to an already-running control
/// group described by `config` (ADR 0037's control-role dual of ADR 0030's
/// data-role growth-node shape): `peers`/`control_ids` cover only `config`'s
/// existing entries, deliberately excluding `new_control_id` — this node's own
/// `RaftCore` starts knowing nothing about itself as a voter, exactly like a
/// freshly-`change_membership`-added voter must (see the admin action's own
/// doc), until the leader's `POST /admin/control/member/add` actually adds it.
async fn join_control_nonvoter(
    config: &ClusterConfig,
    new_control_id: u64,
    dir: &Path,
) -> (Node, RoleAddrs) {
    for attempt in 0..16 {
        let raw = support::free_addrs(6);
        let addrs = RoleAddrs {
            role: NodeRole::Control,
            control: Some(raw[0]),
            client: raw[1],
            dynamo: raw[2],
            cql: raw[3],
            raftkv: None,
            admin: raw[5],
        };
        let bound = match animusd::Node::bind_control(
            new_control_id,
            addrs,
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
        let mut client_route: BTreeMap<animus_env::NodeId, SocketAddr> = BTreeMap::new();
        for (i, a) in config.nodes.iter().enumerate() {
            if a.role.has_control() {
                client_route.insert(animusd::config::control_id(i), a.client);
            }
            if a.role.has_data() {
                client_route.insert(animusd::config::raftkv_id(i), a.client);
            }
        }
        let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();
        let node = bound
            .start_control_with(
                config.control_peer_book(),
                config.control_ids(),
                client_route,
                admin_addrs,
            )
            .await;
        return (node, addrs);
    }
    panic!("could not bind the growth control-only node after retries");
}

/// Grow 3 -> 4 through the admin endpoint, end to end, on a genuine split
/// deployment (control-only + data-only), converging everywhere including the
/// data-only node's `ControlHandle::Remote` view.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn grow_control_group_converges_everywhere() {
    let dir = tempfile::tempdir().unwrap();
    let (control_nodes, data_nodes, config) = support::bring_up_split(3, 1, dir.path()).await;
    support::await_leader(&control_nodes).await;

    let control_admin: Vec<SocketAddr> = control_nodes.iter().map(Node::admin_addr).collect();
    let data_admin: Vec<SocketAddr> = data_nodes.iter().map(Node::admin_addr).collect();
    let leader_idx = control_nodes
        .iter()
        .position(Node::is_control_leader)
        .expect("no control leader");

    // Every original voter starts at {0,1,2}.
    for &a in &control_admin {
        let (status, body) = control_members(a).await;
        assert_eq!(status, 200, "control/members failed: {body}");
        assert_eq!(voters_of(&body), Some(vec![0, 1, 2]));
    }

    // Bring up the 4th node as a quiet non-voter (control id 3 — free: control
    // ids in this split config are {0,1,2}, raftkv ids start at 300).
    let new_id = 3u64;
    let (grown, grown_addrs) = join_control_nonvoter(&config, new_id, dir.path()).await;
    let grown_admin = grown.admin_addr();
    let grown_control_addr = grown_addrs
        .control
        .expect("control-only node has a control addr");

    // Add it via the admin endpoint, on the leader.
    let (status, body) =
        add_control_member(control_admin[leader_idx], new_id, grown_control_addr).await;
    assert_eq!(status, 200, "control/member/add failed: {body}");

    // Converges on every node, including the new node's own view and the
    // data-only node's `Remote` mirror.
    let mut all_admin = control_admin.clone();
    all_admin.push(grown_admin);
    all_admin.extend(data_admin.iter().copied());
    for &a in &all_admin {
        let converged = async {
            loop {
                let (status, body) = control_members(a).await;
                if status == 200 && voters_of(&body) == Some(vec![0, 1, 2, 3]) {
                    return;
                }
                sleep(Duration::from_millis(200)).await;
            }
        };
        timeout(Duration::from_secs(30), converged)
            .await
            .unwrap_or_else(|_| panic!("node at {a} never converged to voters {{0,1,2,3}}"));
    }

    grown.shutdown();
    for node in control_nodes.into_iter().chain(data_nodes) {
        node.shutdown();
    }
}

/// Refusal to add a colliding id: an id that already names a live control
/// voter is an idempotent success (not an error); an id that already names a
/// data-plane member is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn add_control_member_collision_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up_combined(3, dir.path()).await;
    await_bootstrap(&nodes).await;
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();
    let leader = leader_index(&nodes);

    // Already a live voter: idempotent success.
    {
        let (status, body) = add_control_member(admin_addrs[leader], 1, admin_addrs[1]).await;
        assert_eq!(
            status, 200,
            "re-adding an existing voter should be a no-op: {body}"
        );
    }

    // Collides with an existing data-plane member (raftkv id 300).
    {
        let (status, body) =
            add_control_member(admin_addrs[leader], 300, admin_addrs[leader]).await;
        assert_eq!(
            status, 409,
            "adding a control voter at an existing member's id should be refused: {body}"
        );
    }

    // At/above the cluster-allocated id range: refused outright.
    {
        let (status, body) =
            add_control_member(admin_addrs[leader], 1_000_000, admin_addrs[leader]).await;
        assert_eq!(
            status, 409,
            "adding a control voter at/above ALLOC_ID_BASE should be refused: {body}"
        );
    }

    for node in nodes {
        node.shutdown();
    }
}

/// Every refusal/warning shape for `/admin/control/member/remove`, plus the
/// not-relayable regression for both mutating actions, on one 3-node combined
/// core.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn remove_control_voter_refusals_transfer_and_quorum_warnings() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up_combined(3, dir.path()).await;
    await_bootstrap(&nodes).await;
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();

    // Idempotent: an id that was never a control voter at all.
    {
        let leader = leader_index(&nodes);
        let (status, body) = remove_control_member(admin_addrs[leader], 999).await;
        assert_eq!(
            status, 200,
            "removing an unknown node should be a no-op: {body}"
        );
        assert!(
            body["warning"].is_null(),
            "an idempotent no-op removal should carry no warning: {body}"
        );
    }

    // Not relayable: both actions refuse cleanly on a follower's admin port.
    {
        let leader = leader_index(&nodes);
        let follower = (0..3).find(|&i| i != leader).expect("a follower exists");
        let (status, body) = remove_control_member(admin_addrs[follower], 999).await;
        assert_eq!(
            status, 409,
            "control/member/remove on a follower should be refused: {body}"
        );
        let msg = body["error"]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase();
        assert!(
            msg.contains("leader"),
            "expected a leader-routing refusal, got: {msg}"
        );

        let (status, body) = add_control_member(admin_addrs[follower], 999, admin_addrs[0]).await;
        assert_eq!(
            status, 409,
            "control/member/add on a follower should be refused: {body}"
        );
        let msg = body["error"]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase();
        assert!(
            msg.contains("leader"),
            "expected a leader-routing refusal, got: {msg}"
        );
    }

    // Remove a non-leader voter: succeeds, no warning (2 of 3 remain, both alive).
    let leader = leader_index(&nodes);
    let non_leader_voter = (0..3u64)
        .find(|&i| i != leader as u64)
        .expect("a follower id exists");
    {
        let (status, body) = remove_control_member(admin_addrs[leader], non_leader_voter).await;
        assert_eq!(status, 200, "removing a non-leader voter failed: {body}");
        assert!(
            body["warning"].is_null(),
            "removing down to 2 healthy voters should carry no warning: {body}"
        );
    }
    let converged = async {
        loop {
            let (status, body) = control_members(admin_addrs[leader]).await;
            if status == 200
                && let Some(v) = voters_of(&body)
                && v.len() == 2
                && !v.contains(&non_leader_voter)
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(15), converged)
        .await
        .expect("removal of the non-leader voter never converged");

    // Remove the current leader's own slot: arms a transfer and returns the
    // familiar "retry on the leader" refusal — never a silent success, since
    // this call cannot itself complete the removal once it has stepped down.
    // The two remaining voters are `{leader, other}`; leadership may bounce
    // between them more than once while the transfer settles (a healthy,
    // if noisy, consequence of a genuine election under real scheduling —
    // not something this test should assume happens in exactly one hop), so
    // this retries the removal against whichever of the two currently
    // reports itself leader until it actually succeeds, bounded overall.
    let leader = leader_index(&nodes);
    let leader_id = leader as u64;
    let other_id: u64 = (0..3u64)
        .find(|&i| i != leader_id && i != non_leader_voter)
        .expect("exactly one other voter remains");
    let mut saw_leader_refusal = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let new_leader_admin: SocketAddr = loop {
        let current_leader_id = if nodes[leader].is_control_leader() {
            leader_id
        } else if nodes[other_id as usize].is_control_leader() {
            other_id
        } else {
            sleep(Duration::from_millis(100)).await;
            if tokio::time::Instant::now() >= deadline {
                panic!("neither remaining voter is ever leader while removing the old leader");
            }
            continue;
        };
        let target_admin = admin_addrs[current_leader_id as usize];
        let (status, body) = remove_control_member(target_admin, leader_id).await;
        if status == 200 {
            let warning = body["warning"].as_str();
            assert!(
                warning.is_some(),
                "removing down to 1 voter should carry a quorum-loss warning: {body}"
            );
            break target_admin;
        }
        let msg = body["error"]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase();
        assert!(
            msg.contains("leader"),
            "expected an idempotent no-op or a leader-routing refusal, got: {msg}"
        );
        saw_leader_refusal = true;
        if tokio::time::Instant::now() >= deadline {
            panic!("removing the old leader's own slot never succeeded within 30s: {body}");
        }
        sleep(Duration::from_millis(100)).await;
    };
    assert!(
        saw_leader_refusal,
        "expected at least one leader-self-removal refusal before the eventual success"
    );

    let converged = async {
        loop {
            let (status, body) = control_members(new_leader_admin).await;
            if status == 200 && voters_of(&body).map(|v| v.len()) == Some(1) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(15), converged)
        .await
        .expect("removal down to 1 voter never converged");

    // Removing the last remaining voter is refused outright.
    {
        let (status, body) = remove_control_member(new_leader_admin, other_id).await;
        assert_eq!(
            status, 409,
            "removing the last remaining voter should be refused: {body}"
        );
    }

    for node in nodes {
        node.shutdown();
    }
}
