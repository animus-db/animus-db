//! Regression for a `Node::bind`/`bind_control`/`bind_data` identity bug:
//! every `--config FILE --node I` entry point (`run_node_with_streams_
//! quiesce_and_ttl_sweep_interval` and its control-only/data-only/growth
//! siblings, `lib.rs`) used to bind a node under `config::node_id(index)` —
//! the "n{index}" minting convention `ClusterConfig::generate` happens to
//! use — instead of the loaded config's own `RoleAddrs::id`.
//!
//! Every other test in this crate builds its `ClusterConfig` either via
//! `generate` or via a hand-rolled `RoleAddrs { id: animusd::config::
//! node_id(i), .. }` (see `tests/support::bring_up_deadline`,
//! `tests/advertise_host.rs::role_addrs_at`), so `addrs.id ==
//! config::node_id(index)` held everywhere else by pure coincidence and the
//! bug produced no visible symptom. It surfaces the moment a config's own
//! ids diverge from that convention — precisely what a hand-written config,
//! or the Kubernetes operator's generated config (`"{cluster}-{ordinal}"`
//! ids, see `animus-operator::desired::cluster_config::node_id`), does.
//!
//! With the mismatch, every node's own claimed identity (what it embeds as
//! `Envelope::from` on outbound messages, and what `RaftCore` checks against
//! its own genesis voter set) is absent from that same voter set —
//! `RaftCore::is_voter()` (`self.config.contains(&self.id)`) is `false`
//! forever, on every node, so no one ever calls `start_election`/
//! `start_pre_vote` and the control group never elects a leader. This was
//! found and reproduced with a real 3-container `docker network` cluster
//! mirroring the operator's exact deployment shape (identical `0.0.0.0`
//! binds across all three nodes, distinct `advertise_host` FQDNs, ids like
//! `"e2e-0"`) — `/admin/health` 503'd forever, and `/admin/config`'s
//! `node_id` read `"n0"` while `control_ids`/`peers` all read `"e2e-0"`.
//! This test reproduces the same id-divergence shape without needing
//! containers, through the identical `run_node` entry point every
//! `--config/--node` deployment uses.
//!
//! Real TCP/time — polls with generous timeouts, not deterministic
//! assertions (this crate has no `SimEnv`, see the crate's own `CLAUDE.md`).

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animusd::config::NodeRole;
use animusd::{ClusterConfig, Node, RoleAddrs};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// A `RoleAddrs` whose `id` is `"{name}-{i}"` — deliberately NOT
/// `animusd::config::node_id(i)`'s `"n{i}"` convention, mirroring the
/// Kubernetes operator's own id shape (`animus-operator::desired::
/// cluster_config::node_id`).
fn named_role_addrs(name: &str, i: usize, addrs: &[SocketAddr]) -> RoleAddrs {
    RoleAddrs {
        id: format!("{name}-{i}")
            .parse()
            .expect("a simple ASCII cluster-name/index id is a valid NodeId"),
        role: NodeRole::Both,
        internal: addrs[6 * i],
        client: addrs[6 * i + 1],
        dynamo: addrs[6 * i + 2],
        admin: addrs[6 * i + 3],
        intra: addrs[6 * i + 4],
        console: addrs[6 * i + 5],
        advertise_host: None,
    }
}

/// Bring up a combined-mode cluster whose every `RoleAddrs::id` follows the
/// `named_role_addrs` convention above, retrying the whole bind-everything
/// unit against a wall-clock deadline — the same port-TOCTOU mitigation
/// `support::bring_up_deadline` uses, generalized so the id convention can
/// differ from that helper's own `config::node_id`-derived one.
async fn bring_up_named(
    name: &str,
    n: usize,
    dir: &Path,
    deadline: Duration,
) -> (Vec<Node>, ClusterConfig) {
    let hard_deadline = tokio::time::Instant::now() + deadline;
    let mut attempt: u64 = 0;
    loop {
        let addrs = support::free_addrs(n * 6);
        let config = ClusterConfig {
            nodes: (0..n).map(|i| named_role_addrs(name, i, &addrs)).collect(),
            dynamo_auth: None,
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
        assert!(
            tokio::time::Instant::now() < hard_deadline,
            "could not bring up the {n}-node cluster within {deadline:?}"
        );
        sleep(Duration::from_millis(50)).await;
        attempt += 1;
    }
}

/// One HTTP/1.0 GET to the admin endpoint; returns `(status, parsed JSON)`.
async fn admin_get(addr: SocketAddr, path: &str) -> (u16, serde_json::Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let request = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
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
    let value: serde_json::Value = serde_json::from_str(payload)
        .unwrap_or_else(|e| panic!("admin body is not JSON ({e}): {payload}"));
    (status, value)
}

/// Waits for every node to observe a control leader and a non-empty
/// membership — the same convergence proof `advertise_host.rs::
/// await_bootstrap`/`seed_join.rs::await_bootstrap` use. Pre-fix, this
/// never converges: every node's `is_voter()` is permanently false (its own
/// claimed id is absent from its own genesis config), so no node ever
/// campaigns.
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
    timeout(Duration::from_secs(30), ready).await.expect(
        "cluster did not elect a control leader within 30s — likely a node identity/genesis \
         voter-set mismatch (see this file's own module doc)",
    );
}

/// A 3-node cluster whose ids follow the Kubernetes-operator-style
/// `"{cluster}-{ordinal}"` convention (never `"n{i}"`) elects a leader, and
/// every node's own `/admin/config` reports the identity its own config
/// entry actually declared — not the unrelated `"n{i}"` id
/// `config::node_id(index)` used to bind it under.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_cluster_with_operator_style_ids_elects_a_leader() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) =
        bring_up_named("custom-cluster", 3, dir.path(), Duration::from_secs(30)).await;

    await_bootstrap(&nodes).await;

    for (i, node) in nodes.iter().enumerate() {
        let expected_id = format!("custom-cluster-{i}");
        assert_eq!(
            config.nodes[i].id.as_str(),
            expected_id,
            "test setup: config entry {i} should declare the expected id"
        );
        let (status, config_view) = admin_get(node.admin_addr(), "/admin/config").await;
        assert_eq!(status, 200);
        assert_eq!(
            config_view["node_id"], expected_id,
            "node {i} bound under a different id than its own config declared \
             (index-derived \"n{i}\" instead of its real RoleAddrs::id) — this is the \
             exact mismatch that makes RaftCore::is_voter() false forever"
        );
    }

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}
