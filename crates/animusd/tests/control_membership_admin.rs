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
//! - [`runtime_added_voter_survives_leadership_change_to_a_different_original_voter`]
//!   (ADR 0037 PR4): closes PR3's known gap where a runtime-added voter's
//!   address was only ever known to whichever node happened to be leader at
//!   `admin_add_control_member` time (`ProdEnv::merge_peer`'s "known scope
//!   limit") — forces a leadership transfer to a *different* ORIGINAL voter
//!   (self-removing the adder, which arms a transfer without actually
//!   completing the removal — see `admin_remove_control_member`'s doc) and
//!   proves the new leader still replicates a fresh proposal to the
//!   runtime-added voter, via the replicated `NodeAddrs.control` field +
//!   `control_peer_sync_loop`, not the ephemeral single-env `merge_peer` call
//!   PR3 shipped with.
//! - [`removing_a_live_voter_while_another_is_already_dead_is_refused_without_force`]
//!   (ADR 0037 hardening PR2, superseding the PR5 §9 test of the same shape
//!   that used to document this as an *accepted* risk): the original
//!   count-only guard (refuse `< 1`, warn `== 1`) had no survivor-liveness
//!   signal, so it let a removal through that stranded the group forever.
//!   The liveness-aware guard closes this — killing one follower for good,
//!   then removing a *different* live voter is now refused outright (3 -> 2,
//!   only 1 of the resulting 2 is reachable, short of the 2-voter majority).
//! - [`removing_a_live_voter_while_another_is_already_dead_succeeds_with_force`]:
//!   the `--force` escape hatch still allows the same operationally-risky
//!   removal the old guard let through unconditionally — proves it succeeds
//!   with `force: true` and still strands the group for any further
//!   membership change (`config_change_in_flight` never clears, since the
//!   dead voter can never ack), i.e. `force` is explicit informed consent to
//!   the exact risk ADR 0037's Consequences section originally documented.
//! - [`removing_the_actually_dead_voter_itself_needs_no_force`]: removing the
//!   dead voter itself is unaffected by the guard (it's excluded from
//!   `remaining` by construction) — no `force` needed.
//! - [`removing_a_voter_when_every_remaining_voter_is_alive_is_never_refused`]:
//!   the negative case — with every voter genuinely alive, the liveness
//!   guard never fires, `force` or not.
//! - [`concurrent_control_add_surfaces_in_flight_as_a_clean_retryable_error`]
//!   (ADR 0037 PR5, §9): the core-level in-flight rejection
//!   (`animus-control`'s `rejects_a_change_while_one_is_in_flight`) surfaced
//!   through the real admin HTTP path — two concurrent `control/member/add`
//!   calls for different ids race at the leader's shared lock; the loser
//!   gets a clean `409` it can retry, not a hang or a silent no-op, and the
//!   retry succeeds once the winner has committed.
//! - [`omitted_node_add_mints_an_id_and_converges_to_a_live_voter`] (ADR 0037
//!   hardening trio's PR3, re-based onto ADR 0040 Decision B/C in PR4):
//!   `POST /admin/control/member/add` with `node` omitted self-mints a fresh
//!   id (`NodeId::mint`) instead of requiring one, and converges to a live
//!   voter exactly like an operator-supplied id does.
//! - [`concurrent_omitted_node_adds_mint_distinct_ids_and_both_become_voters`]:
//!   the omitted-node dual of `concurrent_control_add_surfaces_in_flight_as_
//!   a_clean_retryable_error` — two concurrent omitted-node adds each mint
//!   (a 128-bit mint colliding is astronomically unlikely, and the
//!   registration CAS would catch it structurally even if it happened), but
//!   their `change_membership` calls race like any other concurrent pair;
//!   the loser retries (a fresh omitted-node call, necessarily minting a
//!   second distinct id) and both a winner and a second id eventually become
//!   voters.
//! - [`add_control_member_collision_shapes`] (ADR 0040 Decision C): an id
//!   that already names an existing data-plane member now succeeds
//!   (promotion, not a conflict — see that test's own doc for why this
//!   flipped from a 409 refusal); there is no more reserved numeric range to
//!   refuse manually targeting.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_control::node::CONTROL_PEER_LIVENESS_TIMEOUT;
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

/// `POST /admin/member/add` (ADR 0030 online growth) — registers a plain
/// data-plane member with no control role at all, the collision target
/// `add_control_member_collision_shapes` needs (ADR 0040 PR1: with one
/// identity per node, a combined cluster's own bring-up ids are both control
/// *and* data ids, so there is no id within `{0,1,2}` that is "a data-plane
/// member but not a control voter" — this mints a genuinely distinct one).
async fn add_member(admin_addr: SocketAddr, node: u64) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"node": nid(node).to_string()}).to_string();
    admin(admin_addr, "POST", "/admin/member/add", Some(&body)).await
}

async fn add_control_member(
    admin_addr: SocketAddr,
    node: u64,
    addr: SocketAddr,
) -> (u16, serde_json::Value) {
    add_control_member_raw(admin_addr, &nid(node).to_string(), addr).await
}

/// The raw-string-id form of [`add_control_member`] — needed to submit an id
/// this test knows in advance is not `nid`-shaped (e.g. an allocator-range
/// `"alloc-…"` id, ADR 0040 PR3's reserved mint prefix).
async fn add_control_member_raw(
    admin_addr: SocketAddr,
    node: &str,
    addr: SocketAddr,
) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"node": node, "addr": addr.to_string()}).to_string();
    admin(admin_addr, "POST", "/admin/control/member/add", Some(&body)).await
}

/// The **allocator-minted-id** form (ADR 0037 hardening trio's PR3): `node`
/// omitted entirely (not merely `null`) — exercises `AddControlMemberReq`'s
/// `#[serde(default)]` the same way a real 2-arg `animus admin control-add`
/// call would (the CLI's own JSON body simply never sets the field). Like
/// [`add_control_member`] and `concurrent_control_add_surfaces_in_flight_as_a_
/// clean_retryable_error`'s existing convention, `addr` here is a fake,
/// never-connected-to placeholder (an existing node's own admin address) —
/// these tests prove the admin-plane mint + register + `change_membership`
/// mechanics, not real Raft catch-up (already covered for the
/// operator-supplied path by `grow_control_group_converges_everywhere`).
async fn add_control_member_allocated(
    admin_addr: SocketAddr,
    addr: SocketAddr,
) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"addr": addr.to_string()}).to_string();
    admin(admin_addr, "POST", "/admin/control/member/add", Some(&body)).await
}

/// Whether `id` looks like a [`NodeId::mint`](animus_env::NodeId::mint)
/// output — exactly 22 chars (128 bits of base64url, unpadded). ADR 0040
/// retired the ADR 0036 allocator's reserved-range/prefix convention along
/// with the allocator itself; uniqueness is now enforced structurally by the
/// registration CAS, not a namespace, so this is a shape sanity check only.
fn looks_minted(id: &animus_env::NodeId) -> bool {
    id.as_str().chars().count() == 22
}

async fn remove_control_member(admin_addr: SocketAddr, node: u64) -> (u16, serde_json::Value) {
    remove_control_member_forced(admin_addr, node, false).await
}

/// `force` (ADR 0037 hardening PR2) bypasses the liveness-aware quorum-loss
/// guard — see `ClientCtx::admin_remove_control_member`'s doc.
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
        let mut client_route: BTreeMap<animus_env::NodeId, SocketAddr> = BTreeMap::new();
        for (i, a) in config.nodes.iter().enumerate() {
            client_route.insert(animusd::config::node_id(i), a.client);
        }
        let mut intra_route: BTreeMap<animus_env::NodeId, SocketAddr> = BTreeMap::new();
        for (i, a) in config.nodes.iter().enumerate() {
            intra_route.insert(animusd::config::node_id(i), a.intra);
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
            )
            .await
            .expect("open the growth control-only node's system-keyspace engine");
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
        assert_eq!(voters_of(&body), Some(vec![nid(0), nid(1), nid(2)]));
    }

    // Bring up a 5th node as a quiet non-voter (id 4 — free: `bring_up_split`'s
    // 3 control-only + 1 data-only nodes already claim ids `{0,1,2,3}` under
    // ADR 0040 PR1's one-identity-per-node scheme).
    let new_id = 4u64;
    let (grown, grown_addrs) = join_control_nonvoter(&config, new_id, dir.path()).await;
    let grown_admin = grown.admin_addr();
    let grown_control_addr = grown_addrs.internal;

    // Wait for `grown`'s own one-shot self-registration (`MetaCommand::
    // RegisterNode`'s CAS, ADR 0040 PR4) to land on the real cluster before
    // adding it as a control voter — the same "confirm it's up first"
    // sequencing `runtime_added_voter_survives_leadership_change_to_a_
    // different_original_voter` documents (mirrors the real operator
    // runbook, plan §3): calling `control/member/add` before this node's own
    // registration has landed races two *independent* proposals for the
    // same id's `node_addrs` entry against each other, and the CAS
    // correctly refuses whichever loses that race rather than silently
    // clobbering it (unlike the pre-ADR-0040 blind-overwrite behavior this
    // supersedes) — so the test must sequence around it, not rely on
    // eventual convergence through a rejection.
    timeout(Duration::from_secs(15), async {
        loop {
            if control_nodes[leader_idx]
                .metadata()
                .node_addrs
                .contains_key(&nid(new_id))
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("grown node's own self-registration never landed on the real cluster");

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
                if status == 200 && voters_of(&body) == Some(vec![nid(0), nid(1), nid(2), nid(4)]) {
                    return;
                }
                sleep(Duration::from_millis(200)).await;
            }
        };
        timeout(Duration::from_secs(30), converged)
            .await
            .unwrap_or_else(|_| panic!("node at {a} never converged to voters {{0,1,2,4}}"));
    }

    grown.shutdown_graceful().await;
    for node in control_nodes.into_iter().chain(data_nodes) {
        node.shutdown_graceful().await;
    }
}

/// Collision shapes for `POST /admin/control/member/add` (ADR 0040 Decision
/// C, re-basing the ADR 0037/hardening-trio behavior this test used to
/// cover): an id that already names a live control voter is an idempotent
/// success (not an error, unchanged); an id that already names an existing
/// **data-plane** member is now **also** an idempotent success — promoting an
/// already-registered member to a control voter is the common case, not a
/// conflict, since ADR 0040 PR1 unified the id space (one identity per node,
/// no more separate control-id range for an existing member to collide
/// with). There is no more reserved numeric range to refuse manually
/// targeting — `ALLOC_ID_BASE` and the whole allocator are gone; the only
/// remaining collision shape is a **genuinely different** registration for
/// the same id (proven at the `RegisterNode` CAS level in `animus-control`'s
/// `tests/register_node_cas.rs`, not re-proven here).
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

    // An existing data-plane member (not yet a control voter) — promoting it
    // now succeeds (ADR 0040: no more separate control-id range to collide
    // with; "must already be a registered member OR get registered in the
    // same action" — this exercises the "already a member" half).
    {
        let (status, body) = add_member(admin_addrs[leader], 50).await;
        assert_eq!(
            status, 200,
            "registering the collision member failed: {body}"
        );
        let (status, body) = add_control_member(admin_addrs[leader], 50, admin_addrs[leader]).await;
        assert_eq!(
            status, 200,
            "promoting an existing data-plane member to a control voter should succeed: {body}"
        );
    }

    for node in nodes {
        node.shutdown_graceful().await;
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
                && !v.contains(&nid(non_leader_voter))
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
        node.shutdown_graceful().await;
    }
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
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn runtime_added_voter_survives_leadership_change_to_a_different_original_voter() {
    let dir = tempfile::tempdir().unwrap();
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
    // change. So, like `remove_control_voter_refusals_transfer_and_quorum_
    // warnings`'s own leader-self-removal above, this retries the mutating
    // call itself — re-issued against whichever original voter currently
    // reports itself leader (re-arming a fresh attempt) — until a
    // DIFFERENT original voter reports itself leader, bounded overall.
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

/// Bring up a 3-node combined core, kill one non-leader follower for good
/// (process shut down, still occupying its control-voter slot — nobody has
/// removed it), and wait deliberately past
/// [`CONTROL_PEER_LIVENESS_TIMEOUT`] before returning — there is no admin
/// surface exposing per-voter liveness to poll on directly (this crate's
/// `control_peer_believed_alive` is a leader-internal fact, not a wire
/// field), so aging the dead follower out of the leader's `last_contact` map
/// needs a deliberate real-time wait, not a converged-condition poll (unlike
/// most of this test suite — see `docs/engineering-lessons.md`). The margin
/// (3x the timeout) absorbs real scheduling jitter under a loaded CI
/// machine. Returns `(nodes, admin_addrs, leader_admin, dead_id,
/// live_target_id)`.
async fn bring_up_with_one_dead_follower(
    dir: &Path,
) -> (Vec<Node>, Vec<SocketAddr>, SocketAddr, u64, u64) {
    let (mut nodes, config) = bring_up_combined(3, dir).await;
    await_bootstrap(&nodes).await;
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();

    let leader = leader_index(&nodes);
    let followers: Vec<usize> = (0..3).filter(|&i| i != leader).collect();
    let dead_idx = followers[0];
    let live_target_idx = followers[1];

    let dead_node = nodes.remove(dead_idx);
    dead_node.shutdown_graceful().await;
    // Re-index: `nodes`/positions shifted after the `remove` above.
    let leader_admin = admin_addrs[leader];
    let dead_id = dead_idx as u64;
    let live_target_id = live_target_idx as u64;

    sleep(CONTROL_PEER_LIVENESS_TIMEOUT * 3).await;

    (nodes, admin_addrs, leader_admin, dead_id, live_target_id)
}

/// **ADR 0037 hardening PR2**: supersedes the original PR5 §9 test of this
/// name (`..._can_silently_strand_the_group`), which documented — as an
/// *accepted* risk — that the old count-only guard (refuse `< 1`, warn
/// `== 1`) let a removal through that permanently wedges the group when a
/// *different* survivor is already dead. The liveness-aware guard closes
/// this: a 3-node combined core has one non-leader voter genuinely killed;
/// the leader is then asked to remove the OTHER (live) follower — a plain
/// 3 -> 2 removal that the old count-only guard would have allowed
/// unconditionally. The new guard computes `remaining = {leader,
/// dead_follower}`, sees only 1 of the 2 is reachable (short of the 2-voter
/// majority), and refuses — naming the dead voter and pointing at `--force`.
/// The config is left unchanged (still all 3 original voters).
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn removing_a_live_voter_while_another_is_already_dead_is_refused_without_force() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, _admin_addrs, leader_admin, dead_id, live_target_id) =
        bring_up_with_one_dead_follower(dir.path()).await;

    let (status, body) = remove_control_member(leader_admin, live_target_id).await;
    assert_eq!(
        status, 409,
        "removing a live voter while a different survivor is already dead \
         should be refused by the liveness-aware guard: {body}"
    );
    let msg = body["error"]
        .as_str()
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert!(
        msg.contains(&dead_id.to_string()),
        "the refusal should name the apparently-dead voter ({dead_id}): {msg}"
    );
    assert!(
        msg.contains("force"),
        "the refusal should point the operator at --force: {msg}"
    );

    // Unchanged: the refusal didn't touch the config.
    let (status, body) = control_members(leader_admin).await;
    assert_eq!(status, 200, "control/members failed: {body}");
    assert_eq!(
        voters_of(&body).map(|mut v| {
            v.sort_unstable();
            v
        }),
        Some(vec![nid(0), nid(1), nid(2)]),
        "a refused removal must not change the live voter set: {body}"
    );

    for node in nodes {
        node.shutdown_graceful().await;
    }
}

/// The `--force` sibling of the test above: the same setup (one follower
/// genuinely dead, the leader asked to remove the OTHER live follower), but
/// with `force: true` — the explicit escape hatch still allows the
/// operationally-risky removal the old, unconditional count-only guard used
/// to let through by default. Proves both that it succeeds (with no
/// additional warning — `remaining.len() == 2`, not the down-to-1 case) and
/// that it still leaves the group stranded for any FURTHER membership
/// change: the removal's own config-change log entry can never commit
/// (needs the dead voter's ack), so `RaftCore::config_change_in_flight`
/// stays permanently true and every subsequent add keeps getting the
/// familiar "already in flight"/leader-routing refusal, never eventually
/// succeeding, across a generous polling window. `force` is informed
/// consent to exactly the risk ADR 0037's Consequences section originally
/// documented as unconditionally accepted — it is no longer unconditional.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn removing_a_live_voter_while_another_is_already_dead_succeeds_with_force() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, _admin_addrs, leader_admin, _dead_id, live_target_id) =
        bring_up_with_one_dead_follower(dir.path()).await;

    let (status, body) = remove_control_member_forced(leader_admin, live_target_id, true).await;
    assert_eq!(
        status, 200,
        "removing a live voter while another is already dead should succeed \
         with --force: {body}"
    );
    assert!(
        body["warning"].is_null(),
        "a 3 -> 2 removal (not down to 1) carries no warning even with \
         --force: {body}"
    );

    // The real consequence: the control group is now wedged for any FURTHER
    // membership change, because the removal's own config-change entry can
    // never commit (needs the dead voter's ack) — `config_change_in_flight`
    // stays true forever. Retrying an unrelated add (a 4th voter) must keep
    // failing with the familiar in-flight/leader-routing refusal, never
    // eventually succeeding, across a generous polling window.
    let probe_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut ever_succeeded = false;
    let mut last_body = serde_json::Value::Null;
    while tokio::time::Instant::now() < probe_deadline {
        // The placeholder `addr` never needs to resolve to anything real —
        // the config-change is rejected before any connection is attempted
        // (`config_change_in_flight`), so `leader_admin` itself is a
        // convenient, always-valid `SocketAddr` to reuse here.
        let (status, body) = add_control_member(leader_admin, 90, leader_admin).await;
        if status == 200 {
            ever_succeeded = true;
            last_body = body;
            break;
        }
        last_body = body;
        sleep(Duration::from_millis(150)).await;
    }
    assert!(
        !ever_succeeded,
        "a further control-membership change must never succeed once the group is \
         stranded (one dead survivor out of 2 voters) — but it did: {last_body}"
    );

    for node in nodes {
        node.shutdown_graceful().await;
    }
}

/// The dead voter is excluded from `remaining` by construction, so removing
/// it — rather than a *different* live voter — needs no `--force`: the
/// guard only ever counts the *resulting* voters, and the node being removed
/// was never part of that set to begin with.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn removing_the_actually_dead_voter_itself_needs_no_force() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, _admin_addrs, leader_admin, dead_id, _live_target_id) =
        bring_up_with_one_dead_follower(dir.path()).await;

    let (status, body) = remove_control_member(leader_admin, dead_id).await;
    assert_eq!(
        status, 200,
        "removing the actually-dead voter itself should succeed with no \
         --force needed: {body}"
    );
    assert!(
        body["warning"].is_null(),
        "removing down to 2 voters, both alive (leader + the other live \
         follower), should carry no warning: {body}"
    );

    for node in nodes {
        node.shutdown_graceful().await;
    }
}

/// The negative case: with every voter genuinely alive, the liveness-aware
/// guard never fires — removing a non-leader voter succeeds exactly as it
/// always has, `--force` or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn removing_a_voter_when_every_remaining_voter_is_alive_is_never_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up_combined(3, dir.path()).await;
    await_bootstrap(&nodes).await;
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();

    let leader = leader_index(&nodes);
    let non_leader_voter = (0..3u64)
        .find(|&i| i != leader as u64)
        .expect("a follower id exists");

    let (status, body) = remove_control_member(admin_addrs[leader], non_leader_voter).await;
    assert_eq!(
        status, 200,
        "removing a voter when every remaining voter is alive should never \
         be refused by the liveness guard: {body}"
    );
    assert!(
        body["warning"].is_null(),
        "removing down to 2 healthy voters should carry no warning: {body}"
    );

    for node in nodes {
        node.shutdown_graceful().await;
    }
}

/// **ADR 0037 PR5 (§9 "change already in flight... surfaced through the
/// ADMIN path")**: the core-level rejection of a second concurrent
/// `change_membership` while one is uncommitted is already proven at
/// `animus-control`'s `tests/control_membership.rs::
/// rejects_a_change_while_one_is_in_flight`. This test proves the SAME
/// mechanism surfaces as a clean, retryable HTTP error through the actual
/// admin path (not a silent no-op, not a hang, not a crash): two concurrent
/// `POST /admin/control/member/add` calls for two *different* new ids, fired
/// at the same leader via `tokio::join!`, race at the leader's internal
/// `Mutex<RaftCore>` — whichever wins appends its config-change entry first
/// and (per this codebase's own "adopted locally and immediately" single-
/// server-change semantics) that leader's own config now requires the new
/// entry to commit before another config change is even attempted; the
/// loser's own `change_membership` call observes `config_change_in_flight`
/// and is rejected. A real network round trip (committing the winner's
/// change across a majority) takes far longer than the in-process work of
/// reaching the shared lock, so this race is not a hopeful coin flip — it is
/// the expected, reliable outcome of two requests serialized by one mutex
/// while only one of them has anywhere to go yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn concurrent_control_add_surfaces_in_flight_as_a_clean_retryable_error() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up_combined(3, dir.path()).await;
    await_bootstrap(&nodes).await;
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();
    let leader = leader_index(&nodes);
    let leader_admin = admin_addrs[leader];

    let (r1, r2) = tokio::join!(
        add_control_member(leader_admin, 10, admin_addrs[leader]),
        add_control_member(leader_admin, 11, admin_addrs[leader]),
    );

    let outcomes = [&r1, &r2];
    let successes = outcomes.iter().filter(|(status, _)| *status == 200).count();
    let failures: Vec<&(u16, serde_json::Value)> = outcomes
        .iter()
        .copied()
        .filter(|(status, _)| *status != 200)
        .collect();
    assert_eq!(
        successes, 1,
        "exactly one of two concurrent control-add calls should win: r1={r1:?}, r2={r2:?}"
    );
    assert_eq!(
        failures.len(),
        1,
        "exactly one of two concurrent control-add calls should lose cleanly: \
         r1={r1:?}, r2={r2:?}"
    );
    let (loser_status, loser_body) = failures[0];
    assert_eq!(
        *loser_status, 409,
        "the loser must fail cleanly, not hang/crash"
    );
    let msg = loser_body["error"]
        .as_str()
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert!(
        msg.contains("flight") || msg.contains("leader") || msg.contains("retry"),
        "expected a clear, retryable-sounding error for the loser, got: {msg}"
    );

    // Whichever id lost, retrying it once the winner's change has committed
    // and caught up must succeed — proving the error was genuinely
    // retryable, not a permanent refusal.
    let winner_id: u64 = if r1.0 == 200 { 10 } else { 11 };
    let loser_id: u64 = if winner_id == 10 { 11 } else { 10 };

    let converged = async {
        loop {
            let (status, body) = control_members(leader_admin).await;
            if status == 200
                && let Some(v) = voters_of(&body)
                && v.contains(&nid(winner_id))
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(15), converged)
        .await
        .expect("the winning control-add never converged");

    let retry_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut retried_ok = false;
    let mut last_body = serde_json::Value::Null;
    while tokio::time::Instant::now() < retry_deadline {
        let (status, body) = add_control_member(leader_admin, loser_id, admin_addrs[leader]).await;
        if status == 200 {
            retried_ok = true;
            break;
        }
        last_body = body;
        sleep(Duration::from_millis(150)).await;
    }
    assert!(
        retried_ok,
        "the loser's retry should eventually succeed once the winner committed: {last_body}"
    );

    for node in nodes {
        node.shutdown_graceful().await;
    }
}

/// **ADR 0037 hardening trio's PR3, re-based onto ADR 0040 Decision B/C in
/// PR4**: `POST /admin/control/member/add` with `node` omitted self-mints a
/// fresh id (`NodeId::mint` off the leader's own bound `Env`) instead of
/// requiring an operator-chosen one, then proceeds through the identical
/// address-registration (now `RegisterNode`'s CAS) + `change_membership`
/// tail. Proves: the response carries the minted id (shaped like a real
/// mint — [`looks_minted`]), and it converges to a live voter via the
/// existing `GET /admin/control/members` poll — the same convergence signal
/// `concurrent_control_add_surfaces_in_flight_as_a_clean_retryable_error`
/// already trusts for the operator-supplied path.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn omitted_node_add_mints_an_id_and_converges_to_a_live_voter() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up_combined(3, dir.path()).await;
    await_bootstrap(&nodes).await;
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();
    let leader = leader_index(&nodes);
    let leader_admin = admin_addrs[leader];

    let (status, body) = add_control_member_allocated(leader_admin, admin_addrs[leader]).await;
    assert_eq!(
        status, 200,
        "omitted-node control/member/add failed: {body}"
    );
    let minted: animus_env::NodeId = body["node"]
        .as_str()
        .expect("the response carries the minted `node`")
        .parse()
        .expect("minted node id parses");
    assert!(
        looks_minted(&minted),
        "minted id {minted} should look like a NodeId::mint output"
    );

    let converged = async {
        loop {
            let (status, body) = control_members(leader_admin).await;
            if status == 200 && voters_of(&body).is_some_and(|v| v.contains(&minted)) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(15), converged)
        .await
        .unwrap_or_else(|_| panic!("minted voter {minted} never converged to a live voter"));

    for node in nodes {
        node.shutdown_graceful().await;
    }
}

/// Two **concurrent** omitted-node adds (ADR 0037 hardening trio's PR3): the
/// allocator itself never collides (ADR 0036's own monotonic-plus-presence-
/// check guarantee — minting is not the contended resource here), but the
/// *subsequent* `change_membership` each mint feeds into is a genuinely
/// sequential single-server delta, so exactly one of the two concurrent
/// calls wins outright and the other gets the same clean, retryable
/// "already in flight" 409
/// `concurrent_control_add_surfaces_in_flight_as_a_clean_retryable_error`
/// already proves for the operator-supplied path — handled here the same
/// way a real caller must: retry. A losing call's own minted id (if minting
/// completed before it lost the `change_membership` race) is left as an
/// orphaned, address-less `Down` member — accepted ADR 0036 semantics (see
/// that ADR's "orphaned Down allocations" consequence), not a defect this
/// test chases down: what "both eventually become voters" needs to show is
/// that a *second*, distinct, minted id eventually joins the first as a
/// voter, not that the loser's original specific id is the one that does
/// (retrying the omitted-node path — unlike retrying an operator-supplied
/// id — always mints a *fresh* id, since the nonce is generated fully
/// server-side inside `admin_add_control_member` and never exposed to the
/// caller to replay).
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn concurrent_omitted_node_adds_mint_distinct_ids_and_both_become_voters() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up_combined(3, dir.path()).await;
    await_bootstrap(&nodes).await;
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();
    let leader = leader_index(&nodes);
    let leader_admin = admin_addrs[leader];

    let (r1, r2) = tokio::join!(
        add_control_member_allocated(leader_admin, admin_addrs[leader]),
        add_control_member_allocated(leader_admin, admin_addrs[leader]),
    );
    for (status, body) in [&r1, &r2] {
        assert!(
            *status == 200 || *status == 409,
            "unexpected status for a concurrent omitted-node add: {body}"
        );
    }
    let successes: Vec<animus_env::NodeId> = [&r1, &r2]
        .iter()
        .filter(|(status, _)| *status == 200)
        .map(|(_, body)| {
            body["node"]
                .as_str()
                .expect("a successful omitted-node add carries the minted `node`")
                .parse()
                .expect("minted node id parses")
        })
        .collect();
    assert_eq!(
        successes.len(),
        1,
        "exactly one concurrent omitted-node add should win outright: r1={r1:?}, r2={r2:?}"
    );
    let winner_id = successes[0].clone();
    assert!(
        looks_minted(&winner_id),
        "minted id {winner_id} should look like a NodeId::mint output"
    );

    let winner_converged = async {
        loop {
            let (status, body) = control_members(leader_admin).await;
            if status == 200 && voters_of(&body).is_some_and(|v| v.contains(&winner_id)) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(15), winner_converged)
        .await
        .unwrap_or_else(|_| panic!("winner {winner_id} never converged to a live voter"));

    // Retry the loser: a fresh omitted-node call mints a *second*,
    // necessarily distinct, id (see this test's own doc for why it cannot be
    // the loser's original one) and adds it once the winner's change has
    // cleared `config_change_in_flight`.
    let retry_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut second_id = None;
    let mut last_body = serde_json::Value::Null;
    while tokio::time::Instant::now() < retry_deadline {
        let (status, body) = add_control_member_allocated(leader_admin, admin_addrs[leader]).await;
        if status == 200 {
            second_id = body["node"].as_str().and_then(|s| s.parse().ok());
            break;
        }
        last_body = body;
        sleep(Duration::from_millis(150)).await;
    }
    let second_id: animus_env::NodeId = second_id
        .unwrap_or_else(|| panic!("the retried omitted-node add never succeeded: {last_body}"));
    assert_ne!(
        second_id, winner_id,
        "the retry must mint an id distinct from the winner's"
    );
    assert!(
        looks_minted(&second_id),
        "minted id {second_id} should look like a NodeId::mint output"
    );

    let second_converged = async {
        loop {
            let (status, body) = control_members(leader_admin).await;
            if status == 200 && voters_of(&body).is_some_and(|v| v.contains(&second_id)) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(15), second_converged)
        .await
        .unwrap_or_else(|_| panic!("second minted voter {second_id} never converged"));

    for node in nodes {
        node.shutdown_graceful().await;
    }
}
