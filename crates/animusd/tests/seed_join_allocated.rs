//! **Self-minted member ids** (ADR 0040 Decision B/C): `animusd join --seed
//! ADDR[,ADDR...] --base-port P` and `animusd data --seed ADDR[,ADDR...]
//! --base-port P`, both with no `--id` — this node self-mints its own id
//! (`NodeId::mint`) and claims it via `MetaCommand::RegisterNode`'s
//! registration CAS instead of an operator picking a small index or proposing
//! an explicit `--id`.
//!
//! **C-13 / ADR 0061 rung M PR 4 trimmed this file from five tests to two;
//! PR 5 trims it further, to one.** Tests 1/3/5
//! (`no_node_join_becomes_active_and_gets_a_replica`,
//! `data_only_allocated_join_becomes_active_and_gets_a_replica`,
//! `follower_connected_seed_completes_the_allocate_node_id_round_trip`) were
//! **deleted by PR 4** — every assertion each one made had a `SimCluster`
//! sibling in `crates/animusd/src/sim_cluster_seed_join.rs`:
//! - Test 5 (a self-minted combined join via a deliberately follower-only
//!   seed, asserting only the minted-id shape and real-detector promotion)
//!   is a strict SUBSET of that module's own scenario (a),
//!   `joiner_discovers_claims_and_is_promoted_by_the_real_detector` — (a)
//!   already asserts both, plus the forwarding/balance-driven-replica/peer-
//!   book properties test 5 never checked at all.
//! - Test 3 (the data-only dual, over a 3-control/2-data split deployment)
//!   is a strict SUBSET of that module's own scenario (c),
//!   `data_only_joiner_over_a_split_deployment_gets_a_rebalanced_replica` —
//!   the identical deployment shape, self-minted data-only join, real
//!   replica landing, and bidirectional put/get, already covered there
//!   (built one PR earlier, C-13 PR 3).
//! - Test 1 (the happy path this file's own doc used to point to for
//!   "becomes Active and gets a real tablet replica via rebalancing") is
//!   what scenario (a)'s own PR-4 extension (three tables instead of one,
//!   plus a trailing balance-driven-replica-and-peer-book poll) now proves
//!   for the self-minted combined-join case specifically.
//!
//! **Test 2 (`two_concurrent_allocated_joins_get_distinct_ids`) is deleted
//! by PR 5** — its own real subject (two joiners self-minting CONCURRENTLY
//! against the same seed, proving the mint-retry-on-collision loop and that
//! both end up `Active` with distinct ids) now has TWO `SimCluster` siblings
//! in `sim_cluster_seed_join.rs`: scenario (e),
//! `two_concurrent_self_minted_joiners_get_distinct_ids_and_both_go_active`
//! (the direct conversion — `SimCluster::join_via_seed_concurrently` with
//! `count = 2`, proving exactly what test 2 proved: distinct/minted ids and
//! real-detector promotion for both, from a pinned seed plus a fixed 5-seed
//! `_over_seeds` sibling — deterministic where test 2 could only ever hit
//! the retry-on-collision branch by astronomically unlikely luck), and
//! scenario (f), `forced_mint_collision_retries_and_the_colliding_member_
//! is_untouched` (a NEW, stronger proof test 2 itself never attempted: a
//! DETERMINISTIC forced collision on a self-mint's first attempt, asserting
//! the retry loop actually retries into a fresh mint and that the colliding
//! member's own row is left completely untouched by the rejected attempt —
//! see that scenario's own doc for the exact mechanism).
//!
//! **Test 4
//! (`ephemeral_identity_restart_gets_a_new_id_old_left_down_and_prunable`)
//! is permanent** — untouched by PR 5, and stays real TCP/time: a fresh
//! process on a fresh directory minting a genuinely NEW identity in place of
//! one that silently vanished has no `SimCluster` analogue —
//! `SimCluster::restart` always resumes the SAME node index/id with its
//! retained engine (mirroring a real process restarting on the SAME
//! directory), which is structurally the opposite of what this test needs
//! to prove. See `docs/adr/0061-testability-node-crate-simulator.md`'s
//! "Rung M (post-C-12)" opener amendment, §3, for the full reasoning (the
//! same class of permanent gap `config_node_identity.rs` already carries).
//!
//! Real TCP/time — polls with generous timeouts, not deterministic
//! assertions (a flaky `ProdEnv` test is a real bug, per the root
//! `CLAUDE.md`).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use animusd::{ClusterConfig, Node, NodeStatus, RoleAddrs, StorageBackend};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Bring up the initial `n`-node combined-mode config core (port-TOCTOU
/// mitigation) — see `support::bring_up_deadline`.
async fn bring_up(n: usize, dir: &Path) -> (Vec<Node>, ClusterConfig) {
    support::bring_up_deadline(n, dir, support::JOIN_DEADLINE).await
}

fn leader_index(nodes: &[Node]) -> usize {
    nodes
        .iter()
        .position(Node::is_control_leader)
        .expect("no control leader among the core nodes")
}

/// Join a fresh **combined-mode, cluster-allocated-id** node against `seeds`
/// (ADR 0036), see `support::join_allocated_fresh_deadline`.
async fn join_allocated_fresh(
    seeds: &[SocketAddr],
    dir: &Path,
    label: &str,
    backend: StorageBackend,
) -> (Node, RoleAddrs, PathBuf) {
    support::join_allocated_fresh_deadline(seeds, dir, label, backend, support::JOIN_DEADLINE).await
}

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed
/// JSON)`.
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

/// This node's own id (ADR 0040 PR1: one id per node, was `raftkv_id`), off
/// its own `/admin/config` — there is no direct Rust accessor for it on a
/// bound-and-started [`Node`] (unlike `client_addr()`/`admin_addr()`), so the
/// allocated id a join actually landed on is only observable this way (or by
/// diffing `Metadata.members`).
async fn own_raftkv_id(admin_addr: SocketAddr) -> animus_env::NodeId {
    let (status, body) = admin(admin_addr, "GET", "/admin/config", None).await;
    assert_eq!(status, 200, "GET /admin/config failed: {body}");
    body["node_id"]
        .as_str()
        .expect("node_id present and a string")
        .parse()
        .expect("node_id parses")
}

fn member_status(nodes: &[Node], id: &animus_env::NodeId) -> Option<NodeStatus> {
    nodes
        .iter()
        .find_map(|n| n.metadata().members.get(id).map(|m| m.status))
}

async fn await_active(nodes: &[Node], id: &animus_env::NodeId, secs: u64) {
    timeout(Duration::from_secs(secs), async {
        loop {
            if member_status(nodes, id) == Some(NodeStatus::Active) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("node {id} never promoted to Active"));
}

/// **Ephemeral-identity regression** (ADR 0036): a no-`--node` joined node
/// that goes away and comes back with a fresh nonce (a fresh process/dir,
/// modeled here by calling `join_allocated_fresh` again from scratch) gets a
/// **new** allocated id — the old id's `Member` entry is left `Down`,
/// address-less, forever, exactly as documented, and is prunable via the
/// existing `POST /admin/member/remove` like any other drained, unreferenced
/// member.
///
/// **C-13 PR 4**: kept real-socket, permanently — see this file's own doc.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn ephemeral_identity_restart_gets_a_new_id_old_left_down_and_prunable() {
    let dir = support::panic_safe_tempdir();

    let (core_nodes, core_config) = bring_up(3, dir.path()).await;
    support::await_bootstrap(&core_nodes).await;
    // ADR 0047: `--seed` now names the seed's intra address.
    let core_clients: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.intra).collect();
    let core_admin: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.admin).collect();

    // 1. First join: capture its allocated id, let it become Active.
    let (first, _addrs, _dir1) = join_allocated_fresh(
        &core_clients,
        dir.path(),
        "first",
        StorageBackend::default(),
    )
    .await;
    let old_id = own_raftkv_id(first.admin_addr()).await;
    await_active(&core_nodes, &old_id, 20).await;

    // 2. "Restart": the process goes away without ever decommissioning —
    // exactly the abandoned-join / ephemeral-identity shape this ADR
    // documents, not a graceful drain.
    first.shutdown();

    // 3. A fresh join (fresh nonce, fresh ports/dir) gets a DISTINCT id.
    let (second, _addrs2, _dir2) = join_allocated_fresh(
        &core_clients,
        dir.path(),
        "second",
        StorageBackend::default(),
    )
    .await;
    let new_id = own_raftkv_id(second.admin_addr()).await;
    assert_ne!(
        old_id, new_id,
        "a fresh join attempt after the old process went away must get a new id, \
         never reuse the old one"
    );
    await_active(&core_nodes, &new_id, 20).await;

    // 4. The old id's member entry lingers — the unmodified ADR 0012
    // heartbeat/failure-detector chain marks it `Down` once its heartbeats
    // stop (no new mechanism needed for this).
    //
    // Poll and act through the SAME node (whichever currently leads the
    // control group) — never an arbitrary one (issue #819). `Metadata` is
    // `DRIVER_APPLIED` (ADR 0038, `RaftNode::metadata`'s own doc,
    // `crates/animus-control/src/node.rs`): each node's own async apply
    // task publishes its cache independently, with no guarantee every
    // node — the current leader included — catches up to a given
    // committed transition at the same wall-clock moment. `member_status`
    // resolves to whichever core node is first in `core_nodes` that has an
    // entry for `old_id` (in practice `core_nodes[0]`, since every core
    // node registers one early on), which is not necessarily the leader
    // `admin_remove_member` (`crates/animusd/src/lib.rs`) will actually
    // gate its "not drained" check against — that call reads
    // `self.control.metadata_cached()` on whichever node *receives* the
    // request, i.e. its own, possibly-lagging, apply-task cache. Polling
    // an unrelated node for `Down` and then issuing the removal against
    // "whichever node believes itself leader right now" races that
    // per-node apply skew: `core_nodes[0]` can observe `Down` first while
    // the actual leader's own applied cache is still one commit behind,
    // still reporting `Active` — the flaky `409 "is not drained: status is
    // Active"`. Fixed by polling the leader's own view before ever calling
    // into it, per the standing "poll the node you're about to act
    // through, not the node that made the earlier state change" rule
    // (root `CLAUDE.md`'s engineering-lessons log).
    let leader_idx = leader_index(&core_nodes);
    let leader_admin = core_admin[leader_idx];
    timeout(Duration::from_secs(20), async {
        loop {
            if core_nodes[leader_idx]
                .metadata()
                .members
                .get(&old_id)
                .map(|m| m.status)
                == Some(NodeStatus::Down)
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("old allocated id {old_id} never settled to Down on the leader ({leader_idx}) it will be removed through")
    });

    // 5. Prunable via the existing decommission primitive — no new cleanup
    // mechanism was added for this ADR. Removed through the SAME leader
    // node the poll above just confirmed `Down` on.
    let body = serde_json::json!({"node": old_id.to_string()}).to_string();
    let (status, resp) = admin(leader_admin, "POST", "/admin/member/remove", Some(&body)).await;
    assert_eq!(status, 200, "member/remove failed: {resp}");

    timeout(Duration::from_secs(20), async {
        loop {
            if member_status(&core_nodes, &old_id).is_none() {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("old allocated id {old_id} was never pruned after removal"));

    second.shutdown_graceful().await;
    for node in core_nodes {
        node.shutdown_graceful().await;
    }
}
