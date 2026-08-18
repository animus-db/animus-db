//! **Self-minted member ids** (ADR 0040 Decision B/C): `animusd join --seed
//! ADDR[,ADDR...] --base-port P` and `animusd data --seed ADDR[,ADDR...]
//! --base-port P`, both with no `--id` — this node self-mints its own id
//! (`NodeId::mint`) and claims it via `MetaCommand::RegisterNode`'s
//! registration CAS instead of an operator picking a small index or proposing
//! an explicit `--id`. The sibling of `tests/seed_join.rs`/`tests/
//! data_join.rs` (which cover the explicit-`--id` path, left completely
//! untouched by this change); this file exercises only what's new: no-id
//! discovery + self-minting, concurrent-registration safety, the data-only
//! dual, the ephemeral-identity restart semantics, and the
//! `is_relayable_command` regression for `RegisterNode` through a
//! follower-connected seed.
//!
//! Real TCP/time — polls with generous timeouts, not deterministic
//! assertions (a flaky `ProdEnv` test is a real bug, per the root
//! `CLAUDE.md`).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use animusd::{
    ClientRequest, ClientResponse, ClusterConfig, Node, NodeStatus, RoleAddrs, StorageBackend,
    read_frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

const TABLES: [&str; 3] = ["allocjoin0", "allocjoin1", "allocjoin2"];

/// Bring up the initial `n`-node combined-mode config core (port-TOCTOU
/// mitigation) — see `support::bring_up_deadline`.
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

fn leader_index(nodes: &[Node]) -> usize {
    nodes
        .iter()
        .position(Node::is_control_leader)
        .expect("no control leader among the core nodes")
}

/// Join a fresh **combined-mode, cluster-allocated-id** node against `seeds`
/// (ADR 0036) — the allocated-id counterpart of `tests/seed_join.rs::
/// join_fresh`, see `support::join_allocated_fresh_deadline`.
async fn join_allocated_fresh(
    seeds: &[SocketAddr],
    dir: &Path,
    label: &str,
    backend: StorageBackend,
) -> (Node, RoleAddrs, PathBuf) {
    support::join_allocated_fresh_deadline(seeds, dir, label, backend, support::JOIN_DEADLINE).await
}

/// Join a fresh **data-only, cluster-allocated-id** node against `seeds`
/// (ADR 0036) — the data-only dual of [`join_allocated_fresh`], see
/// `support::join_data_allocated_fresh_deadline`.
async fn join_data_allocated_fresh(
    seeds: &[SocketAddr],
    dir: &Path,
    label: &str,
    backend: StorageBackend,
) -> Node {
    support::join_data_allocated_fresh_deadline(seeds, dir, label, backend, support::JOIN_DEADLINE)
        .await
}

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed
/// JSON)` — mirrors `tests/seed_join.rs::admin` verbatim.
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

/// Whether `id` looks like a [`NodeId::mint`](animus_env::NodeId::mint)
/// output — exactly 22 chars (128 bits of base64url, unpadded). There is no
/// reserved prefix to check anymore (ADR 0040 retired the ADR 0036
/// allocator's `"alloc-"` convention along with the allocator itself):
/// uniqueness is now enforced structurally by the registration CAS, not by a
/// namespace convention, so this is a sanity check on shape, not a
/// disjointness proof.
fn looks_minted(id: &animus_env::NodeId) -> bool {
    id.as_str().chars().count() == 22
}

/// A table whose tablet currently lists `raftkv_id` as a replica, if any —
/// mirrors `tests/seed_join.rs::table_with_replica`'s doc: rebalancing (ADR
/// 0029) only ever proposes a move while it improves the *global* imbalance,
/// so a through-only-the-joined-node check must target a table this actually
/// returns, not an arbitrary one. Reads straight off `Node::metadata()`
/// rather than admin JSON (`Metadata.tablets` already carries `table` +
/// `replicas`).
fn table_with_replica(nodes: &[Node], raftkv_id: &animus_env::NodeId) -> Option<String> {
    nodes.iter().find_map(|n| {
        n.metadata()
            .tablets
            .values()
            .find(|t| t.replicas.contains(raftkv_id))
            .and_then(|t| t.table.clone())
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

async fn await_replica(nodes: &[Node], id: &animus_env::NodeId, secs: u64) -> String {
    timeout(Duration::from_secs(secs), async {
        loop {
            if let Some(table) = table_with_replica(nodes, id) {
                return table;
            }
            sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("node {id} never gained a tablet replica"))
}

/// Happy path (ADR 0036): `join --seed ... --base-port P` with no `--node`
/// comes up, becomes `Active`, gets a real tablet replica via rebalancing,
/// and serves reads/writes both through itself and through the core.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn no_node_join_becomes_active_and_gets_a_replica() {
    let dir = tempfile::tempdir().unwrap();

    let (core_nodes, core_config) = bring_up(3, dir.path()).await;
    await_bootstrap(&core_nodes).await;
    let core_clients: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.client).collect();
    // ADR 0047: `--seed` now names the seed's intra address.
    let core_intra: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.intra).collect();
    for table in TABLES {
        put(&core_clients, table, b"k0", b"v0", 30).await;
    }

    let (joined, _addrs, _node_dir) =
        join_allocated_fresh(&core_intra, dir.path(), "happy", StorageBackend::default()).await;
    let joined_id = own_raftkv_id(joined.admin_addr()).await;
    assert!(
        looks_minted(&joined_id),
        "self-minted id {joined_id} must look like a NodeId::mint output, \
         distinct from any --id-proposed id"
    );

    await_active(&core_nodes, &joined_id, 20).await;
    let hosted_table = await_replica(&core_nodes, &joined_id, 90).await;

    put(&[joined.client_addr()], &hosted_table, b"k1", b"v1", 30).await;
    await_value(&core_clients, &hosted_table, b"k1", b"v1", 30).await;
    put(&core_clients, &hosted_table, b"k2", b"v2", 30).await;
    await_value(&[joined.client_addr()], &hosted_table, b"k2", b"v2", 30).await;

    joined.shutdown_graceful().await;
    for node in core_nodes {
        node.shutdown_graceful().await;
    }
}

/// Two nodes joining **concurrently** with no `--node` (ADR 0036) both
/// succeed with **distinct** allocated ids — no `AlreadyExists` anywhere,
/// unlike the `--node`-indexed path's best-effort collision guard. This is
/// the direct proof that ADR 0032's own documented residual race is closed
/// by construction for the allocated path.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn two_concurrent_allocated_joins_get_distinct_ids() {
    let dir = tempfile::tempdir().unwrap();

    let (core_nodes, core_config) = bring_up(3, dir.path()).await;
    await_bootstrap(&core_nodes).await;
    // ADR 0047: `--seed` now names the seed's intra address.
    let core_clients: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.intra).collect();

    // Both joins race through `join_allocated_fresh`'s port-TOCTOU retry loop
    // concurrently — a genuine race, not a sequential simulation of one.
    let (a, b) = tokio::join!(
        join_allocated_fresh(
            &core_clients,
            dir.path(),
            "racer-a",
            StorageBackend::default()
        ),
        join_allocated_fresh(
            &core_clients,
            dir.path(),
            "racer-b",
            StorageBackend::default()
        ),
    );
    let (node_a, _addrs_a, _dir_a) = a;
    let (node_b, _addrs_b, _dir_b) = b;

    let id_a = own_raftkv_id(node_a.admin_addr()).await;
    let id_b = own_raftkv_id(node_b.admin_addr()).await;
    assert_ne!(
        id_a, id_b,
        "two concurrent join attempts must never be allocated the same id"
    );
    assert!(looks_minted(&id_a) && looks_minted(&id_b));

    await_active(&core_nodes, &id_a, 20).await;
    await_active(&core_nodes, &id_b, 20).await;

    node_a.shutdown_graceful().await;
    node_b.shutdown_graceful().await;
    for node in core_nodes {
        node.shutdown_graceful().await;
    }
}

/// The data-only dual (ADR 0036): `data --seed ... --base-port P` with no
/// `--node` against a genuine split deployment — the control plane mints
/// the raftkv id, this node has no local control role at all, and it still
/// becomes `Active` and gains a real replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn data_only_allocated_join_becomes_active_and_gets_a_replica() {
    let dir = tempfile::tempdir().unwrap();

    let (control_nodes, data_nodes, _config) = support::bring_up_split(3, 2, dir.path()).await;
    support::await_leader(&control_nodes).await;
    let existing_data_raftkv_ids: Vec<animus_env::NodeId> =
        (3..5).map(animusd::config::node_id).collect();
    support::await_data_nodes_active(&control_nodes, &existing_data_raftkv_ids).await;

    let data_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
    for table in TABLES {
        put(&data_clients, table, b"k0", b"v0", 30).await;
    }

    // ADR 0047: `--seed` now names the seed's intra address.
    let mut seeds: Vec<SocketAddr> = control_nodes.iter().map(Node::intra_addr).collect();
    seeds.extend(data_nodes.iter().map(Node::intra_addr));
    let joined =
        join_data_allocated_fresh(&seeds, dir.path(), "data", StorageBackend::Memory).await;
    let joined_id = own_raftkv_id(joined.admin_addr()).await;
    assert!(looks_minted(&joined_id));

    await_active(&control_nodes, &joined_id, 20).await;
    let hosted_table = await_replica(&control_nodes, &joined_id, 90).await;

    put(&[joined.client_addr()], &hosted_table, b"k1", b"v1", 30).await;
    await_value(&data_clients, &hosted_table, b"k1", b"v1", 30).await;
    put(&data_clients, &hosted_table, b"k2", b"v2", 30).await;
    await_value(&[joined.client_addr()], &hosted_table, b"k2", b"v2", 30).await;

    for node in control_nodes
        .iter()
        .chain(data_nodes.iter())
        .chain(std::iter::once(&joined))
    {
        node.shutdown_graceful().await;
    }
}

/// **Ephemeral-identity regression** (ADR 0036): a no-`--node` joined node
/// that goes away and comes back with a fresh nonce (a fresh process/dir,
/// modeled here by calling `join_allocated_fresh` again from scratch) gets a
/// **new** allocated id — the old id's `Member` entry is left `Down`,
/// address-less, forever, exactly as documented, and is prunable via the
/// existing `POST /admin/member/remove` like any other drained, unreferenced
/// member.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn ephemeral_identity_restart_gets_a_new_id_old_left_down_and_prunable() {
    let dir = tempfile::tempdir().unwrap();

    let (core_nodes, core_config) = bring_up(3, dir.path()).await;
    await_bootstrap(&core_nodes).await;
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
    timeout(Duration::from_secs(20), async {
        loop {
            if member_status(&core_nodes, &old_id) == Some(NodeStatus::Down) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("old allocated id {old_id} never settled to Down"));

    // 5. Prunable via the existing decommission primitive — no new cleanup
    // mechanism was added for this ADR.
    let leader_admin = core_admin[leader_index(&core_nodes)];
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

/// **Follower-connected seed** (the `is_relayable_command` regression for
/// `MetaCommand::RegisterNode`, ADR 0040): a joiner whose *only* seed is a
/// non-leader control node still completes the whole mint-and-confirm
/// round trip — proving `RegisterNode` is actually in the relay allowlist
/// (a missed entry would hang this join until `JOIN_DISCOVERY_BUDGET`
/// expires, indistinguishable from "no seed answered").
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn follower_connected_seed_completes_the_allocate_node_id_round_trip() {
    let dir = tempfile::tempdir().unwrap();

    let (core_nodes, core_config) = bring_up(3, dir.path()).await;
    await_bootstrap(&core_nodes).await;
    let follower = (0..core_nodes.len())
        .find(|&i| i != leader_index(&core_nodes))
        .expect("a follower exists in a 3-node core");
    // ADR 0047: `--seed` now names the seed's intra address.
    let follower_intra = core_config.nodes[follower].intra;

    // Contact ONLY the follower's intra address — no leader address
    // anywhere in the seed list.
    let (joined, _addrs, _node_dir) = join_allocated_fresh(
        &[follower_intra],
        dir.path(),
        "follower-seed",
        StorageBackend::default(),
    )
    .await;
    let joined_id = own_raftkv_id(joined.admin_addr()).await;
    assert!(looks_minted(&joined_id));

    await_active(&core_nodes, &joined_id, 20).await;

    joined.shutdown_graceful().await;
    for node in core_nodes {
        node.shutdown_graceful().await;
    }
}
