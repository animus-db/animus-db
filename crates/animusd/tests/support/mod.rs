//! Shared support for the `animusd` integration tests.
//!
//! Each `tests/*.rs` file is its own crate that pulls this module in via `mod
//! support;`, and no single test file uses every helper here — so per-binary
//! dead-code analysis flags whichever ones a given consumer doesn't call.
//! `#![allow(dead_code)]` is the standard fix for a shared multi-consumer test
//! support module (same shape as `tests/common/mod.rs` elsewhere).
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use animusd::config::NodeRole;
use animusd::{ClusterConfig, Node, RoleAddrs, StorageBackend};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// Default wall-clock deadline for the `*_deadline` join/bring-up helpers
/// below — generous enough that a genuinely broken join still fails loudly,
/// while riding out the transient port-TOCTOU-under-`--workspace`-contention
/// window that a fixed attempt count could exhaust (see each helper's doc).
pub const JOIN_DEADLINE: Duration = Duration::from_secs(30);

/// Reserve `count` free loopback ports (bind :0, read addr, release the
/// listener). This is itself the source of the documented port-TOCTOU: the
/// port is free the instant this returns, so another test binary's own probe
/// can steal it before the real bind. Callers that build a **fresh** config
/// per attempt (e.g. [`start_single_node`], or a per-process cluster
/// bring-up helper) ride this out by retrying the whole
/// allocate-fresh-ports-and-start unit; a same-address restart that must
/// reuse a captured config instead retries the rebind itself (see
/// [`restart_same_addrs`]).
pub fn free_addrs(count: usize) -> Vec<SocketAddr> {
    let listeners: Vec<std::net::TcpListener> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
    // listeners dropped here, freeing the ports for the caller to bind.
}

/// A single-node config pinned to fresh ephemeral addresses.
fn single_node_config() -> ClusterConfig {
    let a = free_addrs(6);
    ClusterConfig {
        nodes: vec![RoleAddrs {
            id: animusd::config::node_id(0),
            role: animusd::config::NodeRole::Both,
            internal: a[0],
            client: a[1],
            dynamo: a[2],
            admin: a[3],
            intra: a[4],
            console: a[5],
        }],
    }
}

/// Start a single-node cluster, retrying bring-up against the port-TOCTOU
/// race documented on [`free_addrs`] — each attempt allocates a **fresh**
/// config (new ports), since unlike [`restart_same_addrs`] there is no
/// existing config this helper is bound to reuse.
pub async fn start_single_node(dir: &Path, backend: StorageBackend) -> (Node, ClusterConfig) {
    let mut last_err = None;
    for attempt in 0..10 {
        let config = single_node_config();
        match animusd::run_node_with(&config, 0, dir, backend).await {
            Ok(node) => return (node, config),
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(50 * (attempt + 1))).await;
            }
        }
    }
    panic!("single node failed to start after 10 attempts: {last_err:?}");
}

/// Restart a node on the **same addresses + data dir** (the durability tests'
/// same-address recovery), retrying the rebind briefly. A clean shutdown frees
/// the ports, but another test binary's `free_addrs` probe can bind a just-freed
/// port for a moment (the documented port-TOCTOU) — and unlike a *first*
/// bring-up, a same-address restart cannot re-allocate around the thief (reusing
/// the captured config *is the test*). The probe holds the port only
/// microseconds, so a bounded retry rides it out; a genuinely occupied port
/// still fails when the deadline exhausts.
pub async fn restart_same_addrs(
    config: &ClusterConfig,
    index: usize,
    dir: &Path,
    backend: StorageBackend,
) -> Node {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match animusd::run_node_with(config, index, dir, backend).await {
            Ok(node) => return node,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "restart on the same dir/addresses did not rebind: {e}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Bring up a combined-mode `n`-node core, one process per node, retrying the
/// (allocate-fresh-ports + start-all) as a unit against a wall-clock
/// `deadline` rather than a fixed attempt count — same shape as
/// [`restart_same_addrs`], generalized from a fixed 16-attempt/50ms retry
/// (duplicated near-verbatim across `decommission.rs`, `seed_join.rs`,
/// `seed_join_allocated.rs`, and `cluster_growth.rs`) that could exhaust
/// under `cargo test --workspace`-level port-TOCTOU contention while the
/// churn was still transient.
pub async fn bring_up_deadline(
    n: usize,
    dir: &Path,
    deadline: Duration,
) -> (Vec<Node>, ClusterConfig) {
    let hard_deadline = tokio::time::Instant::now() + deadline;
    let mut attempt: u64 = 0;
    loop {
        let addrs = free_addrs(n * 6);
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
        assert!(
            tokio::time::Instant::now() < hard_deadline,
            "could not bring up the initial {n}-node cluster within {deadline:?}"
        );
        sleep(Duration::from_millis(50)).await;
        attempt += 1;
    }
}

/// Grow `base` by `extra` control-plane-follower-less nodes (ADR 0030) via
/// `run_node_growth`, retrying the new nodes' freshly-allocated ports as a
/// unit against a wall-clock `deadline` — the `grow`-loop counterpart of
/// [`bring_up_deadline`] (`cluster_growth.rs`'s own fixed-16-attempt loop).
/// The original nodes in `base` are never touched.
pub async fn grow_deadline(
    base: &ClusterConfig,
    extra: usize,
    dir: &Path,
    deadline: Duration,
) -> (Vec<Node>, ClusterConfig) {
    let original_control_ids = base.control_ids();
    let base_n = base.nodes.len();
    let hard_deadline = tokio::time::Instant::now() + deadline;
    let mut attempt: u64 = 0;
    loop {
        let addrs = free_addrs(extra * 6);
        let mut nodes_cfg = base.nodes.clone();
        for i in 0..extra {
            nodes_cfg.push(RoleAddrs {
                id: animusd::config::node_id(base_n + i),
                role: NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
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
            node.shutdown_graceful().await;
        }
        assert!(
            tokio::time::Instant::now() < hard_deadline,
            "could not grow the cluster by {extra} node(s) within {deadline:?}"
        );
        sleep(Duration::from_millis(50)).await;
        attempt += 1;
    }
}

/// Join a fresh **combined-mode**, explicit-id node against `seeds` via
/// `run_node_join` (ADR 0040 PR4: `--id` replaces the old `--node I` index —
/// `index` is used only to derive a deterministic, readable test id,
/// `config::node_id(index)`, and the data dir), retrying the allocate-ports-
/// and-join step as a unit against a wall-clock `deadline` rather than a
/// fixed attempt count. Generalized from the identical fixed-16-attempt/50ms
/// helper duplicated in `decommission.rs` and `seed_join.rs`: under `cargo
/// test --workspace`-level port-TOCTOU contention, 16 attempts (0.8s total)
/// could exhaust while the port churn was still transient, surfacing as a
/// spurious "could not join node N" panic rather than a real join bug.
/// Trade-off (same as [`restart_same_addrs`]): a genuinely-broken join now
/// takes up to `deadline` to report instead of failing in under a second.
///
/// Returns the node, the addresses it actually bound, and the data dir it
/// used (a caller that needs to rejoin at the exact same addresses/dir, e.g.
/// `seed_join.rs`'s rejoin test, needs all three).
pub async fn join_fresh_deadline(
    seeds: &[SocketAddr],
    index: usize,
    dir: &Path,
    backend: StorageBackend,
    deadline: Duration,
) -> (Node, RoleAddrs, PathBuf) {
    let hard_deadline = tokio::time::Instant::now() + deadline;
    let mut attempt: u64 = 0;
    loop {
        let raw = free_addrs(6);
        let id = animusd::config::node_id(index);
        let addrs = RoleAddrs {
            id: id.clone(),
            role: NodeRole::Both,
            internal: raw[0],
            client: raw[1],
            dynamo: raw[2],
            admin: raw[3],
            intra: raw[4],
            console: raw[5],
        };
        let node_dir = dir.join(format!("join-{index}-{attempt}"));
        match animusd::run_node_join(
            seeds.to_vec(),
            Some(id),
            addrs.clone(),
            &node_dir,
            backend,
            BTreeMap::new(),
        )
        .await
        {
            Ok(node) => return (node, addrs, node_dir),
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < hard_deadline,
                    "could not join node {index} within {deadline:?}: {e}"
                );
                sleep(Duration::from_millis(50)).await;
                attempt += 1;
            }
        }
    }
}

/// Join a fresh **data-only**, explicit-id node against `seeds` via
/// `run_node_data_join` — the data-only dual of [`join_fresh_deadline`],
/// generalized from `data_join.rs`'s own fixed-16-attempt/50ms helper.
pub async fn join_data_fresh_deadline(
    seeds: &[SocketAddr],
    index: usize,
    dir: &Path,
    backend: StorageBackend,
    deadline: Duration,
) -> Node {
    let hard_deadline = tokio::time::Instant::now() + deadline;
    let mut attempt: u64 = 0;
    loop {
        let raw = free_addrs(6);
        let id = animusd::config::node_id(index);
        let addrs = RoleAddrs {
            id: id.clone(),
            role: NodeRole::Data,
            internal: raw[0],
            client: raw[1],
            dynamo: raw[2],
            admin: raw[3],
            intra: raw[4],
            console: raw[5],
        };
        let node_dir = dir.join(format!("data-join-{index}-{attempt}"));
        match animusd::run_node_data_join(
            seeds.to_vec(),
            Some(id),
            addrs,
            &node_dir,
            backend,
            BTreeMap::new(),
        )
        .await
        {
            Ok(node) => return node,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < hard_deadline,
                    "could not join data node {index} within {deadline:?}: {e}"
                );
                sleep(Duration::from_millis(50)).await;
                attempt += 1;
            }
        }
    }
}

/// Join a fresh **combined-mode, self-minted-id** node against `seeds` (ADR
/// 0040 Decision B/C, `run_node_join` with `id: None`) — the minted-id
/// counterpart of [`join_fresh_deadline`], generalized from
/// `seed_join_allocated.rs`'s own fixed-16-attempt/50ms helper. `label`
/// disambiguates the data dir across concurrent callers sharing one `dir`
/// (unlike the explicit-id helper, there is no id known upfront to name it
/// after).
pub async fn join_allocated_fresh_deadline(
    seeds: &[SocketAddr],
    dir: &Path,
    label: &str,
    backend: StorageBackend,
    deadline: Duration,
) -> (Node, RoleAddrs, PathBuf) {
    let hard_deadline = tokio::time::Instant::now() + deadline;
    let mut attempt: u64 = 0;
    loop {
        let raw = free_addrs(6);
        let addrs = RoleAddrs {
            // Unread placeholder: the real id is self-minted pre-bind
            // (ADR 0040 Decision B) — never derived from `addrs.id`.
            id: animus_env::NodeId::new_unchecked("pending-mint"),
            role: NodeRole::Both,
            internal: raw[0],
            client: raw[1],
            dynamo: raw[2],
            admin: raw[3],
            intra: raw[4],
            console: raw[5],
        };
        let node_dir = dir.join(format!("join-alloc-{label}-{attempt}"));
        match animusd::run_node_join(
            seeds.to_vec(),
            None,
            addrs.clone(),
            &node_dir,
            backend,
            BTreeMap::new(),
        )
        .await
        {
            Ok(node) => return (node, addrs, node_dir),
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < hard_deadline,
                    "could not join (minted id) within {deadline:?}: {e}"
                );
                sleep(Duration::from_millis(50)).await;
                attempt += 1;
            }
        }
    }
}

/// Join a fresh **data-only, self-minted-id** node against `seeds` (ADR 0040
/// Decision B/C) — the data-only dual of [`join_allocated_fresh_deadline`].
pub async fn join_data_allocated_fresh_deadline(
    seeds: &[SocketAddr],
    dir: &Path,
    label: &str,
    backend: StorageBackend,
    deadline: Duration,
) -> Node {
    let hard_deadline = tokio::time::Instant::now() + deadline;
    let mut attempt: u64 = 0;
    loop {
        let raw = free_addrs(6);
        let addrs = RoleAddrs {
            // See `join_allocated_fresh_deadline`'s identical placeholder.
            id: animus_env::NodeId::new_unchecked("pending-mint"),
            role: NodeRole::Data,
            internal: raw[0],
            client: raw[1],
            dynamo: raw[2],
            admin: raw[3],
            intra: raw[4],
            console: raw[5],
        };
        let node_dir = dir.join(format!("data-join-alloc-{label}-{attempt}"));
        match animusd::run_node_data_join(
            seeds.to_vec(),
            None,
            addrs,
            &node_dir,
            backend,
            BTreeMap::new(),
        )
        .await
        {
            Ok(node) => return node,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < hard_deadline,
                    "could not join as a data node (minted id) within {deadline:?}: {e}"
                );
                sleep(Duration::from_millis(50)).await;
                attempt += 1;
            }
        }
    }
}

/// Bring up a genuine split cluster: `control_n` control-only nodes
/// (`animusd control`'s `run_node_control`) plus `data_n` data-only nodes
/// (`animusd data`'s `run_node_data`, `ControlHandle::Remote`) — **no**
/// combined-mode node anywhere, one process (in this test binary) per node,
/// each its own `ClusterEdgeState`. Retries the (allocate-fresh-ports +
/// start-all) as a unit, the same port-TOCTOU mitigation every other bring-up
/// helper in this module uses. Moved here from `tests/data_only.rs` (ADR 0035
/// PR5) so `tests/data_join.rs` can reuse it verbatim instead of duplicating
/// the split-config assembly.
pub async fn bring_up_split(
    control_n: usize,
    data_n: usize,
    dir: &Path,
) -> (Vec<Node>, Vec<Node>, ClusterConfig) {
    let total = control_n + data_n;
    for attempt in 0..16 {
        let addrs = free_addrs(total * 6);
        let nodes_cfg: Vec<RoleAddrs> = (0..total)
            .map(|i| {
                let role = if i < control_n {
                    NodeRole::Control
                } else {
                    NodeRole::Data
                };
                RoleAddrs {
                    id: animusd::config::node_id(i),
                    role,
                    internal: addrs[6 * i],
                    client: addrs[6 * i + 1],
                    dynamo: addrs[6 * i + 2],
                    admin: addrs[6 * i + 3],
                    intra: addrs[6 * i + 4],
                    console: addrs[6 * i + 5],
                }
            })
            .collect();
        let config = ClusterConfig { nodes: nodes_cfg };

        let mut control_nodes = Vec::new();
        let mut data_nodes = Vec::new();
        let mut failed = false;
        for i in 0..control_n {
            match animusd::run_node_control(
                &config,
                i,
                dir.join(format!("a{attempt}-c{i}")),
                animusd::StorageBackend::default(),
            )
            .await
            {
                Ok(n) => control_nodes.push(n),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            for i in control_n..total {
                match animusd::run_node_data(
                    &config,
                    i,
                    dir.join(format!("a{attempt}-d{i}")),
                    StorageBackend::Memory,
                )
                .await
                {
                    Ok(n) => data_nodes.push(n),
                    Err(_) => {
                        failed = true;
                        break;
                    }
                }
            }
        }
        if !failed {
            return (control_nodes, data_nodes, config);
        }
        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up split cluster after retries (ports kept getting stolen)");
}

/// Wait for at least one of `control_nodes` to become the control-plane
/// leader.
pub async fn await_leader(control_nodes: &[Node]) {
    timeout(Duration::from_secs(20), async {
        loop {
            if control_nodes.iter().any(Node::is_control_leader) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("control deployment did not elect a leader in 20s");
}

/// Wait for every data node's raftkv id to become `Active` in the control
/// deployment's own metadata (the unmodified ADR 0012 heartbeat/detector
/// promotion chain — `tests/cluster_growth.rs` is the existing proof this
/// mechanism works unattended; no test-side force here).
pub async fn await_data_nodes_active(
    control_nodes: &[Node],
    data_raftkv_ids: &[animus_env::NodeId],
) {
    timeout(Duration::from_secs(20), async {
        loop {
            if data_raftkv_ids.iter().all(|id| {
                control_nodes.iter().any(|n| {
                    n.metadata().members.get(id).map(|m| m.status)
                        == Some(animusd::NodeStatus::Active)
                })
            }) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("data nodes did not become Active in 20s");
}

/// Idle-stall bound for [`poll_until_or_stalled`] — how long the answering
/// node's own apply-task watermark may sit frozen while its condition is
/// still unmet before that is treated as a real stall rather than
/// contention-driven lag. Matches `decommission.rs`'s original
/// `IDLE_STALL_TIMEOUT` (PR #146) so every caller of this shape, across both
/// files, reads as one convention.
pub const IDLE_STALL_TIMEOUT: Duration = Duration::from_secs(60);

/// Outer wall-clock backstop for [`poll_until_or_stalled`], guarding against
/// a genuine deadlock even while the watermark keeps inching forward.
pub const OVERALL_BACKSTOP: Duration = Duration::from_secs(300);

/// One-shot `GET /admin/raft` on `addr`, returning its `engine_applied_index`
/// field if the request and parse both succeed (`None` on any failure —
/// callers treat that as "no progress signal this tick", not a hard error,
/// since a momentarily-unreachable node is exactly the kind of transient
/// blip this whole poll shape exists to ride out). Deliberately minimal
/// (GET-only, no request body) rather than reusing a test file's own richer
/// `admin()` helper (which also POSTs) — this module has no reason to grow a
/// full HTTP client.
async fn engine_applied_index(addr: SocketAddr) -> Option<u64> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    let request = "GET /admin/raft HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).await.ok()?;
    stream.flush().await.ok()?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.ok()?;
    let text = String::from_utf8(raw).ok()?;
    let (head, payload) = text.split_once("\r\n\r\n")?;
    let status: u16 = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    if status != 200 {
        return None;
    }
    let value: serde_json::Value = serde_json::from_str(payload).ok()?;
    value["engine_applied_index"].as_u64()
}

/// Poll `condition` to convergence for an eventual property that is read
/// through a node's ADR 0038 async apply-task cache (`/admin/status`, a
/// tablet map, member statuses, `/admin/peers`, ...) — which has **no
/// contention-independent latency bound** (see `docs/engineering-lessons.md`'s
/// DRIVER_APPLIED entry): a flat wall-clock deadline around such a wait is
/// either too tight (spurious failure under `cargo test --workspace`-scale
/// contention, or even just several concurrent tests in one binary) or too
/// loose (no diagnostic value when something is genuinely stuck).
///
/// Instead, poll `condition` at `poll_interval`, and alongside it require
/// **forward progress** of `progress_addr`'s own `/admin/raft`
/// `engine_applied_index` — pick a node whose view `condition` itself reads,
/// so "no progress" genuinely means "the thing feeding this condition is
/// stuck", not merely "some other node is stuck". Fails only once that
/// watermark has made no progress for [`IDLE_STALL_TIMEOUT`] with
/// `condition` still unmet, or once the [`OVERALL_BACKSTOP`] wall-clock
/// budget expires — whichever comes first. `what` names the awaited property
/// in both panic messages.
///
/// Factored out of `decommission.rs`'s original hand-rolled "Idle-progress
/// poll, not a flat deadline" block (PR #146) once `cluster_growth.rs`
/// needed the identical shape at several call sites.
pub async fn poll_until_or_stalled<C, Fut>(
    progress_addr: SocketAddr,
    what: &str,
    poll_interval: Duration,
    mut condition: C,
) where
    C: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let overall_deadline = tokio::time::Instant::now() + OVERALL_BACKSTOP;
    let mut last_progress_at = tokio::time::Instant::now();
    let mut last_engine_applied: Option<u64> = None;
    loop {
        if condition().await {
            return;
        }
        if let Some(engine_applied) = engine_applied_index(progress_addr).await {
            if last_engine_applied != Some(engine_applied) {
                last_engine_applied = Some(engine_applied);
                last_progress_at = tokio::time::Instant::now();
            } else if last_progress_at.elapsed() >= IDLE_STALL_TIMEOUT {
                panic!(
                    "{what}: never converged, and the apply task's engine_applied_index has \
                     been stuck at {engine_applied} for {IDLE_STALL_TIMEOUT:?} — this is no \
                     longer contention-driven lag, something is actually stuck"
                );
            }
        }
        if tokio::time::Instant::now() >= overall_deadline {
            panic!(
                "{what}: never converged within the {OVERALL_BACKSTOP:?} backstop, despite \
                 apply-task progress (last engine_applied_index={last_engine_applied:?})"
            );
        }
        sleep(poll_interval).await;
    }
}
