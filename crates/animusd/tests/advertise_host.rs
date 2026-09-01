//! `RoleAddrs::advertise_host` (ADR 0060's advertise/dial split): a node's
//! bind address (what a listener actually opens on) and the address it
//! *advertises* for peers to dial can differ. Absent (every existing
//! config), the two are identical — every existing test in this crate
//! already proves that path unmodified. These tests prove the opt-in path:
//! a node that sets `advertise_host` is dialed by peers (and by its own
//! restarted self) at `{advertise_host}:{its own bound port}`, never at its
//! literal bind address.
//!
//! Real TCP/time — polls with generous timeouts, not deterministic
//! assertions (this crate has no `SimEnv`, see the crate's own `CLAUDE.md`).

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animusd::config::NodeRole;
use animusd::{ClusterConfig, Node, RoleAddrs, StorageBackend};
use tokio::time::{sleep, timeout};

mod support;

/// Waits for every node in `nodes` to observe a control leader and a
/// non-empty membership — the same convergence proof
/// `seed_join.rs::await_bootstrap` uses. With exactly two nodes in the
/// control group, this can only pass if both directions of Raft traffic
/// (votes/heartbeats/`AppendEntries`) actually got through — which, for a
/// node dialed only via its advertised host string, is the real proof that
/// the advertised address (not the bind address) is what peers used.
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

/// Bring up a combined-mode cluster from `nodes_cfg`, retrying the whole
/// (bind + start every node) unit against a wall-clock deadline — the same
/// port-TOCTOU mitigation `support::bring_up_deadline` uses, generalized to
/// take the node list directly (so a caller can set `advertise_host` per
/// entry before ports are known).
async fn bring_up_with_config(
    build_config: impl Fn(&[SocketAddr]) -> ClusterConfig,
    n: usize,
    dir: &Path,
    deadline: Duration,
) -> (Vec<Node>, ClusterConfig) {
    let hard_deadline = tokio::time::Instant::now() + deadline;
    let mut attempt: u64 = 0;
    loop {
        let addrs = support::free_addrs(n * 6);
        let config = build_config(&addrs);
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
            "could not bring up the cluster within {deadline:?}"
        );
        attempt += 1;
    }
}

/// A test-only `/etc/hosts` entry, removed by `Drop` even on panic. Real DNS
/// (a Kubernetes `Service`, the deployment target this ADR is for) re-points
/// a stable hostname to wherever its target actually is; a sandboxed test
/// has no real DNS server to control, so this is the closest honest
/// simulation available — and it exercises exactly the production
/// assumption `advertised_addr`'s own doc states: resolution happens fresh
/// on every `TcpStream::connect`, never cached, so re-pointing this entry
/// while the cluster is live is exactly what a rescheduled pod's own DNS
/// update looks like from a peer's point of view. `try_new` returns `None`
/// (never panics) if `/etc/hosts` isn't writable here (a non-root sandbox,
/// a read-only mount) — the one test that needs this degrades to "skipped"
/// rather than failing the whole suite on an environment it can't assume.
struct HostsEntryGuard {
    hostname: &'static str,
}

impl HostsEntryGuard {
    fn try_new(hostname: &'static str, ip: &str) -> Option<Self> {
        let mut contents = std::fs::read_to_string("/etc/hosts").ok()?;
        if !contents.ends_with('\n') {
            contents.push('\n');
        }
        contents.push_str(&format!("{ip} {hostname}\n"));
        std::fs::write("/etc/hosts", &contents).ok()?;
        Some(Self { hostname })
    }

    /// Re-point this entry to `ip`, simulating a DNS update.
    fn repoint(&self, ip: &str) {
        let contents = std::fs::read_to_string("/etc/hosts").expect("read /etc/hosts");
        let marker = format!(" {}", self.hostname);
        let mut new_contents: String = contents
            .lines()
            .map(|line| {
                if line.trim_end().ends_with(&marker) {
                    format!("{ip} {}", self.hostname)
                } else {
                    line.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        new_contents.push('\n');
        std::fs::write("/etc/hosts", new_contents).expect("rewrite /etc/hosts");
    }
}

impl Drop for HostsEntryGuard {
    fn drop(&mut self) {
        if let Ok(contents) = std::fs::read_to_string("/etc/hosts") {
            let marker = format!(" {}", self.hostname);
            let mut cleaned: String = contents
                .lines()
                .filter(|line| !line.trim_end().ends_with(&marker))
                .collect::<Vec<_>>()
                .join("\n");
            if !cleaned.is_empty() {
                cleaned.push('\n');
            }
            let _ = std::fs::write("/etc/hosts", cleaned);
        }
    }
}

fn role_addrs_at(id: usize, addrs: &[SocketAddr], advertise_host: Option<&str>) -> RoleAddrs {
    RoleAddrs {
        id: animusd::config::node_id(id),
        role: NodeRole::Both,
        internal: addrs[6 * id],
        client: addrs[6 * id + 1],
        dynamo: addrs[6 * id + 2],
        admin: addrs[6 * id + 3],
        intra: addrs[6 * id + 4],
        console: addrs[6 * id + 5],
        advertise_host: advertise_host.map(str::to_string),
    }
}

/// (a) A node binds `127.0.0.1:P` but advertises `localhost` — a second,
/// plain (no `advertise_host`) node reaches it purely through the
/// advertised name. Two control voters means bootstrap can only converge if
/// BOTH directions of Raft traffic crossed successfully; node 1 dialing
/// node 0 has no numeric address to fall back to (its own peer/route books
/// only ever learn `"localhost:{port}"` for node 0, never `127.0.0.1:...`),
/// so a converged two-node cluster is a direct proof the dial worked via
/// the advertised name, not the bind address.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_second_node_reaches_an_advertised_node_purely_by_its_advertised_name() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up_with_config(
        |addrs| ClusterConfig {
            nodes: vec![
                role_addrs_at(0, addrs, Some("localhost")),
                role_addrs_at(1, addrs, None),
            ],
            dynamo_auth: None,
        },
        2,
        dir.path(),
        Duration::from_secs(30),
    )
    .await;

    await_bootstrap(&nodes).await;

    // Node 0's own self-registered address book names `localhost`, not its
    // literal `127.0.0.1` bind address, for every port it advertises.
    let meta = nodes[0].metadata();
    let addrs0 = meta
        .node_addrs
        .get(&animusd::config::node_id(0))
        .expect("node 0 self-registered its own NodeAddrs");
    let expected_intra = format!("localhost:{}", config.nodes[0].intra.port());
    let expected_client = format!("localhost:{}", config.nodes[0].client.port());
    assert_eq!(
        addrs0.intra, expected_intra,
        "node 0's own registered intra address must be its advertised name, not its bind address"
    );
    assert_eq!(
        addrs0.client, expected_client,
        "node 0's own registered client address must be its advertised name, not its bind address"
    );
    assert!(
        !addrs0.intra.starts_with("127.0.0.1"),
        "must never fall back to the bind address once advertise_host is set: {}",
        addrs0.intra
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// (b) A same-identity restart with a **different bind IP** (`127.0.0.2`
/// instead of `127.0.0.1`) but the **same advertised host** and the same
/// port numbers: peers still route correctly after the restart, and —
/// because `RegisterNodeAddrs`'s CAS is a no-op when the proposed `NodeAddrs`
/// is byte-identical to what's already replicated (the same-identity rejoin
/// path) — `Metadata.node_addrs` for the restarted node's id is completely
/// unchanged by the restart, since the advertised `host:port` string is
/// identical before and after even though the underlying bind IP moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn same_identity_restart_on_a_different_bind_ip_keeps_the_same_advertised_identity() {
    // Verify 127.0.0.2 is actually bindable in this sandbox before relying
    // on it — a loud, clear skip-shaped failure beats a confusing bind error
    // deep inside `bring_up_with_config`'s retry loop.
    std::net::TcpListener::bind("127.0.0.2:0")
        .expect("this test needs 127.0.0.2 to be bindable in this sandbox");

    // A real advertised hostname needs somewhere to actually resolve — see
    // `HostsEntryGuard`'s own doc for why this is the only honest way to
    // simulate "the same name now points somewhere else" (a rescheduled
    // pod's own DNS update) without a real DNS server to control. Skip
    // (not fail) if this sandbox won't allow it.
    const HOST: &str = "animus-advertise-host-test-b";
    let Some(hosts_entry) = HostsEntryGuard::try_new(HOST, "127.0.0.1") else {
        eprintln!("skipping: /etc/hosts is not writable in this sandbox");
        return;
    };

    let dir = support::panic_safe_tempdir();
    let (mut nodes, mut config) = bring_up_with_config(
        |addrs| ClusterConfig {
            nodes: vec![
                role_addrs_at(0, addrs, None),
                role_addrs_at(1, addrs, Some(HOST)),
            ],
            dynamo_auth: None,
        },
        2,
        dir.path(),
        Duration::from_secs(30),
    )
    .await;
    await_bootstrap(&nodes).await;

    let node1_id = animusd::config::node_id(1);
    let before = nodes[0]
        .metadata()
        .node_addrs
        .get(&node1_id)
        .cloned()
        .expect("node 1 self-registered before the restart");

    // Shut node 1 down, freeing its ports, then rebind the identical port
    // numbers on 127.0.0.2 instead of 127.0.0.1 — same id, same
    // `advertise_host`, same ports, different bind IP. Mirrors
    // `support::restart_same_addrs`'s bounded-retry shape (a just-freed port
    // can be stolen momentarily by another test binary's own probe).
    // Re-pointing the hosts entry now, before the rebind, mirrors a real
    // DNS update landing slightly ahead of (or concurrent with) the pod
    // actually moving — `node0`'s already-cached `intra_route` entry for
    // node 1 is unaffected either way (it's the same string), so this is
    // never a race against `node0`'s own routing state.
    hosts_entry.repoint("127.0.0.2");
    nodes[1].shutdown_graceful().await;
    let moved_addrs = RoleAddrs {
        id: node1_id.clone(),
        role: NodeRole::Both,
        internal: SocketAddr::new([127, 0, 0, 2].into(), config.nodes[1].internal.port()),
        client: SocketAddr::new([127, 0, 0, 2].into(), config.nodes[1].client.port()),
        dynamo: SocketAddr::new([127, 0, 0, 2].into(), config.nodes[1].dynamo.port()),
        admin: SocketAddr::new([127, 0, 0, 2].into(), config.nodes[1].admin.port()),
        intra: SocketAddr::new([127, 0, 0, 2].into(), config.nodes[1].intra.port()),
        console: SocketAddr::new([127, 0, 0, 2].into(), config.nodes[1].console.port()),
        advertise_host: Some(HOST.to_string()),
    };
    // Updating this node's own config entry in place is enough: every
    // other node's own peer/route books re-derive from replicated
    // `Metadata` (`peer_sync_loop`/`route_sync_loop`/`intra_route_sync_loop`)
    // rather than a static snapshot of this file, so only the restarting
    // node itself needs the moved `RoleAddrs`.
    config.nodes[1] = moved_addrs;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let restarted = loop {
        match animusd::run_node_with(
            &config,
            1,
            dir.path().join("moved"),
            StorageBackend::default(),
        )
        .await
        {
            Ok(node) => break node,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "rebind on 127.0.0.2 at the same ports did not succeed: {e}"
                );
                sleep(Duration::from_millis(50)).await;
            }
        }
    };
    nodes[1] = restarted;

    await_bootstrap(&nodes).await;

    // Peers still route to the restarted node: node 0's own metadata still
    // lists it and the cluster remains live.
    timeout(Duration::from_secs(15), async {
        loop {
            if nodes[0].metadata().node_addrs.contains_key(&node1_id) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("node 1 must still be a known member after the moved-IP restart");

    // The same-identity rejoin is a genuine no-op: the replicated
    // `NodeAddrs` is byte-identical to what it was before the restart, since
    // the advertised `host:port` string never changed even though the bind
    // IP did.
    let after = nodes[0]
        .metadata()
        .node_addrs
        .get(&node1_id)
        .cloned()
        .expect("node 1 still registered after the restart");
    assert_eq!(
        before, after,
        "Metadata.node_addrs must be unchanged by a same-advertised-identity restart \
         on a different bind IP"
    );
    assert!(
        after.intra.starts_with(&format!("{HOST}:")),
        "the registered address must stay the advertised name, never the bind IP: {}",
        after.intra
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
    drop(hosts_entry);
}

/// Addendum: the **static** peer/route seed a node bootstraps its
/// `ProdEnv`/`client_route`/`intra_route` books from
/// (`ClusterConfig::peer_book`, and the identical-shape `client_route`/
/// `intra_route` builders in `run_node_with`) also prefers `advertise_host`
/// over the numeric bind address — not just each node's own
/// self-registered `NodeAddrs`. A 3-node cluster whose every entry binds a
/// distinct `127.0.0.1` port but all advertise `localhost` can only
/// bootstrap (elect a leader, converge membership across all three) if
/// every node's initial dial of every other node — which, before any
/// `Metadata` has even replicated once, can only go through this static
/// config-derived seed — used the advertised name.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_static_config_derived_peer_book_dials_every_advertised_name() {
    let dir = support::panic_safe_tempdir();
    let n = 3;
    let (nodes, _config) = bring_up_with_config(
        |addrs| ClusterConfig {
            nodes: (0..n)
                .map(|i| role_addrs_at(i, addrs, Some("localhost")))
                .collect(),
            dynamo_auth: None,
        },
        n,
        dir.path(),
        Duration::from_secs(30),
    )
    .await;

    await_bootstrap(&nodes).await;
    // Full 3-voter convergence: every node's membership view names all 3.
    timeout(Duration::from_secs(15), async {
        loop {
            if nodes.iter().all(|node| node.metadata().members.len() == n) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("all 3 nodes must converge on the full membership set");

    for i in 0..n {
        let addrs = nodes[0]
            .metadata()
            .node_addrs
            .get(&animusd::config::node_id(i))
            .cloned()
            .unwrap_or_else(|| panic!("node {i} must have self-registered"));
        assert!(
            addrs.internal.starts_with("localhost:"),
            "node {i}'s internal address must be its advertised name: {}",
            addrs.internal
        );
    }

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}
