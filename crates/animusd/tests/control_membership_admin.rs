//! Control-plane membership-change admin API + CLI surface (ADR 0037 PR3):
//! `POST /admin/control/member/{add,remove}` + `GET /admin/control/members`.
//!
//! **ADR 0061 rung L, C-12 PR 4e trimmed this file to its one genuine
//! real-socket residual.** The other 11 of the original 12 tests here now
//! have deterministic `SimCluster` siblings in `crates/animusd/src/sim_
//! cluster_control_membership_admin.rs` — see that module's own doc for the
//! full classification table, including why `SimCluster::grow`'s own
//! "data-only growth only" scope forced a weakened substitute for one of
//! them (`grow_control_group_converges_everywhere`) rather than a full
//! conversion.
//!
//! [`runtime_added_voter_survives_leadership_change_to_a_different_original_voter`]
//! (ADR 0037 PR4) is the one test that **stays here, real-socket, for good**
//! — not a scenario-design difficulty, but a fact `SimEnv` cannot model at
//! all: it proves `ProdEnv::merge_peer`'s own "known scope limit" (a
//! runtime-added voter's dial address is only ever merged into *whichever
//! node happened to be leader* at add time, until the replicated `NodeAddrs.
//! control` field + `control_peer_sync_loop` catch every other voter up),
//! and `Env::merge_peer` has a no-op default on the trait itself that
//! `SimEnv` never overrides (only `ProdEnv` does) — combined with
//! `SimCluster`'s own fully-seeded-at-construction route table, every
//! `SimEnv` node already knows how to dial every other one regardless of
//! which node added it or when. There is nothing for a `SimCluster`
//! scenario to observe going wrong before the fix, or right after it.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_env::nid;
use animusd::config::NodeRole;
use animusd::{ClusterConfig, MetaCommand, Node, NodeStatus, RoleAddrs};
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
    add_control_member_raw(admin_addr, &nid(node).to_string(), addr).await
}

/// The raw-string-id form of [`add_control_member`] — needed to submit an id
/// this test knows in advance is not `nid`-shaped.
async fn add_control_member_raw(
    admin_addr: SocketAddr,
    node: &str,
    addr: SocketAddr,
) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"node": node, "addr": addr.to_string()}).to_string();
    admin(admin_addr, "POST", "/admin/control/member/add", Some(&body)).await
}

async fn remove_control_member(admin_addr: SocketAddr, node: u64) -> (u16, serde_json::Value) {
    remove_control_member_forced(admin_addr, node, false).await
}

/// `force` (ADR 0037 hardening PR2) bypasses the liveness-aware quorum-loss
/// guard — see `ClientCtx::admin_remove_control_member`'s doc. Never `true`
/// in this trimmed file's own one surviving test, but kept alongside
/// [`remove_control_member`] rather than inlined, mirroring the shape the
/// full suite (and its `SimCluster` sibling) both use.
async fn remove_control_member_forced(
    admin_addr: SocketAddr,
    node: u64,
    force: bool,
) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"node": nid(node).to_string(), "force": force}).to_string();
    admin(
        admin_addr,
        "POST",
        "/admin/control/member/remove",
        Some(&body),
    )
    .await
}

fn voters_of(body: &serde_json::Value) -> Option<Vec<animus_env::NodeId>> {
    body["voters"].as_array().map(|a| {
        a.iter()
            .filter_map(|v| v.as_str()?.parse::<animus_env::NodeId>().ok())
            .collect()
    })
}

/// Bring up an `n`-node **combined-mode** core, one process per node — the
/// same shape `tests/decommission.rs::bring_up` uses.
async fn bring_up_combined(n: usize, dir: &Path) -> (Vec<Node>, ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<RoleAddrs> = (0..n)
            .map(|i| RoleAddrs {
                id: animusd::config::node_id(i),
                role: NodeRole::Both,
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
        let config = ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };
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
            node.shutdown_graceful().await;
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
            id: nid(new_control_id),
            role: NodeRole::Control,
            internal: raw[0],
            client: raw[1],
            dynamo: raw[2],
            admin: raw[3],
            intra: raw[4],
            console: raw[5],
            advertise_host: None,
            tls: None,
            encryption_key_path: None,
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
        let mut client_route: BTreeMap<animus_env::NodeId, String> = BTreeMap::new();
        for (i, a) in config.nodes.iter().enumerate() {
            client_route.insert(animusd::config::node_id(i), a.client.to_string());
        }
        let mut intra_route: BTreeMap<animus_env::NodeId, String> = BTreeMap::new();
        for (i, a) in config.nodes.iter().enumerate() {
            intra_route.insert(animusd::config::node_id(i), a.intra.to_string());
        }
        let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();
        let node = bound
            .start_control_with(
                config.peer_book(),
                config.control_ids(),
                client_route,
                intra_route,
                admin_addrs,
                animusd::StorageBackend::Memory,
                animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
                animusd::SegmentStoreConfig::default(),
                animusd::BackupStoreConfig::default(),
                animusd::DEFAULT_STREAM_RETENTION,
            )
            .await
            .expect("open the growth control-only node's system-keyspace engine");
        return (node, addrs);
    }
    panic!("could not bind the growth control-only node after retries");
}

/// **ADR 0037 PR4 regression**: PR3 shipped `admin_add_control_member` with a
/// known, documented gap (`ProdEnv::merge_peer`'s doc, `admin_add_control_
/// member`'s own doc) — a runtime-added voter's control-Raft address was only
/// ever merged into *whichever node happened to be leader* at the moment of
/// the add, so a *later* leadership change left every other voter (including
/// any future one) permanently unable to reach it: their own control env's
/// peer book simply never learned the address. This test drives exactly that
/// sequence and proves the fix (the replicated `NodeAddrs.control` field +
/// every control-role node's own `control_peer_sync_loop`) closes it.
///
/// **Stays real-socket for good** (ADR 0061 rung L, C-12 PR 4e) — see this
/// file's own top doc comment for why `SimEnv`'s no-op `Env::merge_peer`
/// default, combined with `SimCluster`'s fully-seeded-at-construction route
/// table, makes the regression this test proves structurally unobservable
/// under simulation, not merely hard to trigger there.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn runtime_added_voter_survives_leadership_change_to_a_different_original_voter() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up_combined(3, dir.path()).await;
    await_bootstrap(&nodes).await;
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();

    let adder = leader_index(&nodes);

    // Add a 4th control voter through the current leader (`adder`).
    let new_id = 3u64;
    let (grown, grown_addrs) = join_control_nonvoter(&config, new_id, dir.path()).await;
    let grown_control_addr = grown_addrs.internal;

    // Wait for `grown`'s own one-shot self-registration (`MetaCommand::
    // RegisterNode`'s CAS, ADR 0040 PR4, relayed since it starts life a
    // non-voter) to land on the REAL cluster (checked via an original voter's applied
    // `Metadata` — `grown`'s OWN view stays permanently empty until it is
    // actually added as a voter below: a quiet non-voter receives no real
    // Raft replication at all, by design, so it structurally can never
    // observe its own commit through its own `effective_metadata()`)
    // *and* give its bounded retry loop time to fully exhaust
    // (`SCHEMA_COMMIT_TIMEOUT`, 10s): since a non-voter can never see its
    // own registration confirmed, that loop keeps re-proposing its
    // (unmodified, `control: None`) desired value on every tick until it
    // gives up — racing `control/member/add`'s differing `control: Some`
    // write within that window would let a later retry clobber it back to
    // `None`. Mirrors the real operator runbook's own "confirm it's up
    // first" step (plan §3) — this test exercises the intended sequencing,
    // not the race a too-hasty add would hit.
    let self_registered_on_cluster = async {
        loop {
            if nodes[adder]
                .metadata()
                .node_addrs
                .contains_key(&nid(new_id))
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(15), self_registered_on_cluster)
        .await
        .expect("grown node's own self-registration never landed on the real cluster");
    sleep(Duration::from_secs(11)).await;

    let (status, body) = add_control_member(admin_addrs[adder], new_id, grown_control_addr).await;
    assert_eq!(status, 200, "control/member/add failed: {body}");

    // Converge on {0,1,2,3} everywhere (every original voter + the new node's
    // own view) before forcing the transfer — this also guarantees every
    // original voter's own `control_peer_sync_loop` has had at least one
    // tick to merge in id 3's replicated address, since `RegisterNodeAddrs`
    // commits strictly before the config-change entry that this poll
    // observes.
    let grown_admin = grown.admin_addr();
    for &a in admin_addrs.iter().chain(std::iter::once(&grown_admin)) {
        let converged = async {
            loop {
                let (status, body) = control_members(a).await;
                if status == 200 && voters_of(&body) == Some(vec![nid(0), nid(1), nid(2), nid(3)]) {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        };
        timeout(Duration::from_secs(30), converged)
            .await
            .unwrap_or_else(|_| panic!("node at {a} never converged to voters {{0,1,2,3}}"));
    }

    // Force a leadership transfer away from `adder`: self-remove its own
    // slot. `admin_remove_control_member` arms `transfer_leadership` to the
    // smallest OTHER live voter id in `{0,1,2,3}` — always one of the THREE
    // ORIGINAL voters here (id 3, the just-added one, is the largest id in
    // the set, so it can never be the smallest-other-than-`adder`) — then
    // reports the transfer via an error rather than completing the removal,
    // so the live voter set stays exactly `{0,1,2,3}`; only leadership moves.
    //
    // A single one-shot call only ARMS the attempt: `transfer_leadership`
    // sets a `transfer_deadline` of `now + election_base` (the raw
    // un-randomized 150ms default), and `tick()` silently clears it with no
    // signal if the handoff — up to 4 network round trips, including
    // waiting up to 100ms for the next heartbeat tick — doesn't land in
    // that window; nothing then re-arms it. A one-shot call + pure
    // effect-poll can therefore watch a leader that was never going to
    // change. So this retries the mutating call itself — re-issued against
    // whichever original voter currently reports itself leader (re-arming a
    // fresh attempt) — until a DIFFERENT original voter reports itself
    // leader, bounded overall.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let new_leader_idx: usize = loop {
        if let Some(i) = (0..3).find(|&i| i != adder && nodes[i].is_control_leader()) {
            break i;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("leadership never transferred to a different original voter within 30s");
        }
        let Some(current_leader_idx) = (0..3).find(|&i| nodes[i].is_control_leader()) else {
            // Mid-election among the originals; nobody to (re-)arm a
            // transfer through yet.
            sleep(Duration::from_millis(100)).await;
            continue;
        };
        let (status, body) =
            remove_control_member(admin_addrs[current_leader_idx], adder as u64).await;
        assert_eq!(
            status, 409,
            "self-removal should report the transfer, not silently succeed: {body}"
        );
        let msg = body["error"]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase();
        assert!(
            msg.contains("leader"),
            "expected a leadership-transfer refusal, got: {msg}"
        );
        sleep(Duration::from_millis(100)).await;
    };
    assert_ne!(new_leader_idx, adder, "leadership should have moved");

    // The config is unaffected by the transfer alone — still all 4 voters.
    let (status, body) = control_members(admin_addrs[new_leader_idx]).await;
    assert_eq!(status, 200, "control/members failed: {body}");
    assert_eq!(voters_of(&body), Some(vec![nid(0), nid(1), nid(2), nid(3)]));

    // The real proof: propose something new *through the new (different)
    // leader* and confirm it replicates to the runtime-added voter's own
    // locally-applied `Metadata`. This is only possible if the new leader's
    // own control env actually knows id 3's control address — before this
    // PR, only `adder`'s env ever learned it, so this same sequence would
    // have left id 3 permanently unreachable from the new leader (a
    // silently-dropped `AppendEntries`/`InstallSnapshot`, per
    // `ProdEnv::send`'s doc for a destination with no known peer address).
    let label_key = "adr0037_pr4_regression".to_string();
    assert!(
        nodes[new_leader_idx].propose_meta(MetaCommand::UpsertMember {
            node: nid(12_345),
            labels: BTreeMap::from([(label_key.clone(), "1".to_string())]),
            status: NodeStatus::Down,
        }),
        "the new leader should accept its own proposal"
    );
    let replicated_to_grown = async {
        loop {
            if grown
                .metadata()
                .members
                .get(&nid(12_345))
                .and_then(|m| m.labels.get(&label_key))
                .is_some()
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(15), replicated_to_grown)
        .await
        .expect("the runtime-added voter never saw the new leader's proposal replicate");

    grown.shutdown_graceful().await;
    for node in nodes {
        node.shutdown_graceful().await;
    }
}
