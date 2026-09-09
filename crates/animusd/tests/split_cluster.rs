//! Split-deployment scenarios beyond what `data_only.rs` / `control_only.rs` /
//! `data_join.rs` / `watch_metadata.rs` already cover.
//!
//! **Trimmed (ADR 0061 rung L, C-12 PR 4b).** Of this file's original 8
//! tests, 6 converted whole to `SimCluster`
//! (`crates/animusd/src/sim_cluster_split_cluster.rs`'s own classification
//! table has the per-test mapping): `control_leader_failover_under_live_
//! data_traffic`, `split_over_a_split_deployment`, `data_node_failure_is_
//! detected_and_repaired_onto_a_spare`, `decommission_a_data_node_over_
//! split_deployment_via_the_control_leader`, `control_leader_and_data_
//! node_failure_simultaneously_still_converges`, and `decommission_racing_
//! a_tablet_split_converges_with_no_data_loss` are all fully converted.
//! Kept here, both whole:
//!
//! - `full_split_cluster_restart_recovers_metadata_and_data` — a genuine
//!   on-disk `StorageBackend::Lsm` full-outage restart (every control AND
//!   data process stopped, then rebound on the same dir/addresses): real
//!   fsync/on-disk WAL crash recovery `SimCluster` cannot stand in for
//!   (its own `restart` rebuilds a node in-process over `MemoryEngine`,
//!   never replaying an on-disk WAL).
//! - `cluster_control_data_threads_quiesce_after_to_admin_config` — the
//!   real `animusd::start_split_cluster_with_growth` process assembly
//!   (`--cluster-control`/`--cluster-data`'s own CLI-equivalent config
//!   wiring), proving `--quiesce-after`/`--heartbeat-batch`/`--shared-wal`
//!   reach every data-role node that way too: `SimCluster` never goes
//!   through that config-parse/process-boundary path at all.
//!
//! Real TCP/time throughout — every wait is a bounded, converged-or-timeout
//! poll, never a fixed sleep used as an assertion.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use animusd::config::NodeRole;
use animusd::{
    ClientRequest, ClientResponse, ClusterConfig, ColumnType, MetaCommand, Node, RoleAddrs,
    StorageBackend, TableSchema, read_frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;
use support::{await_data_nodes_active, await_leader, free_addrs};

async fn call(addr: SocketAddr, req: ClientRequest) -> Option<ClientResponse> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    animusd::write_frame(&mut stream, &req).await.ok()?;
    read_frame(&mut stream).await.ok()?
}

/// Try every client address in `clients` (round-robin) until one accepts the
/// write (mirrors `tests/data_join.rs`/`tests/decommission.rs`'s `put`).
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
        "{method} {path} HTTP/1.0\r\nHost: animus\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );
    stream.write_all(request.as_bytes()).await.expect("send");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .expect("status line");
    let value: serde_json::Value = serde_json::from_str(payload).unwrap_or(serde_json::Value::Null);
    (status, value)
}

// ---- Full-cluster stop/restart -------------------------------------------

/// Bring up a split cluster with a DURABLE data backend (`Lsm`, unlike
/// `support::bring_up_split`'s always-`Memory` — this test restarts every
/// node and relies on real on-disk durability, not on a survivor's live
/// Raft replication) and return each node's own directory, so a
/// full-outage restart can rebind every node on its own same dir/addresses.
async fn bring_up_split_durable(
    control_n: usize,
    data_n: usize,
    dir: &Path,
) -> (
    Vec<Node>,
    Vec<Node>,
    ClusterConfig,
    Vec<PathBuf>,
    Vec<PathBuf>,
) {
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
                    advertise_host: None,
                    tls: None,
                    encryption_key_path: None,
                }
            })
            .collect();
        let config = ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };
        let control_dirs: Vec<PathBuf> = (0..control_n)
            .map(|i| dir.join(format!("a{attempt}-c{i}")))
            .collect();
        let data_dirs: Vec<PathBuf> = (0..data_n)
            .map(|i| dir.join(format!("a{attempt}-d{i}")))
            .collect();

        let mut control_nodes = Vec::new();
        let mut data_nodes = Vec::new();
        let mut failed = false;
        for (i, d) in control_dirs.iter().enumerate() {
            match animusd::run_node_control(&config, i, d, StorageBackend::Lsm).await {
                Ok(n) => control_nodes.push(n),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            for (idx, d) in data_dirs.iter().enumerate() {
                let i = control_n + idx;
                match animusd::run_node_data(&config, i, d, StorageBackend::Lsm).await {
                    Ok(n) => data_nodes.push(n),
                    Err(_) => {
                        failed = true;
                        break;
                    }
                }
            }
        }
        if !failed {
            return (control_nodes, data_nodes, config, control_dirs, data_dirs);
        }
        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up a durable split cluster after retries (ports kept getting stolen)");
}

/// How long a same-address restart's rebind retries before giving up
/// (`support::restart_same_addrs` uses 5s for a *single* node). Confirmed
/// this is genuine ephemeral-port contention, not a socket-close ordering
/// bug: `mio::net::TcpListener::bind` already sets `SO_REUSEADDR` (checked
/// in the vendored source), which rules out a lingering-`TIME_WAIT`
/// explanation for "Address already in use" here — every failed rebind
/// attempt really did race another process's live bind on that exact port.
/// A full-cluster restart rebinds *every* node in sequence right after
/// tearing every other node down, so it multiplies a single node's usual
/// sub-second exposure to that race by however many nodes must rebind; under
/// heavy `cargo test --workspace`-style CPU/ephemeral-port contention the
/// tail of that race can occasionally stretch well past what a lone
/// restart test ever needs. A generous bound here turns "rare and slow"
/// into "reliably eventually succeeds" without weakening what the retry
/// actually proves (same-address recovery, not a latency bound) — paired
/// with keeping the fleet small (`bring_up_split_durable(3, 1, ..)` below)
/// to also cut the number of rebinds this test needs, not just how long
/// each may take.
const RESTART_REBIND_TIMEOUT: Duration = Duration::from_secs(60);

/// Rebind `run_node_control` on the same address/dir, retrying to ride out
/// the documented port-TOCTOU (the control-only counterpart of
/// `support::restart_same_addrs`, which is combined-mode/data-backend only).
async fn restart_control(config: &ClusterConfig, index: usize, dir: &Path) -> Node {
    let deadline = tokio::time::Instant::now() + RESTART_REBIND_TIMEOUT;
    loop {
        match animusd::run_node_control(config, index, dir, StorageBackend::Lsm).await {
            Ok(n) => return n,
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!(
                        "control node {index} did not rebind on restart: {e}\n{}",
                        listen_holders(Some(config.nodes[index].internal))
                    );
                }
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Diagnostic-only: shell out to `ss` to show which process (if any) is
/// listening on `addr` right now — attached to a rebind-timeout panic so a
/// future flake carries forensic evidence (PID/process name) instead of just
/// "address already in use", distinguishing "another process on this
/// machine is genuinely squatting on this port" from a same-process
/// socket-lifetime bug. Best-effort: `ss` may not exist or may need
/// privileges to show every process, so a failure here never masks the real
/// assertion.
fn listen_holders(addr: Option<SocketAddr>) -> String {
    let Some(addr) = addr else {
        return "listen_holders: no address".into();
    };
    match std::process::Command::new("ss")
        .args(["-ltnp", "-H"])
        .output()
    {
        Ok(out) => {
            let text = String::from_utf8_lossy(&out.stdout);
            let port_suffix = format!(":{}", addr.port());
            let hits: Vec<&str> = text.lines().filter(|l| l.contains(&port_suffix)).collect();
            if hits.is_empty() {
                format!("ss found no listener on {addr} (may lack permission to see it)")
            } else {
                format!("ss listeners on {addr}:\n{}", hits.join("\n"))
            }
        }
        Err(e) => format!("ss unavailable ({e}); no diagnostic for {addr}"),
    }
}

/// The data-only counterpart of [`restart_control`].
async fn restart_data(
    config: &ClusterConfig,
    index: usize,
    dir: &Path,
    backend: StorageBackend,
) -> Node {
    let deadline = tokio::time::Instant::now() + RESTART_REBIND_TIMEOUT;
    loop {
        match animusd::run_node_data(config, index, dir, backend).await {
            Ok(n) => return n,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "data node {index} did not rebind on restart: {e}"
                );
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn full_split_cluster_restart_recovers_metadata_and_data() {
    // Generous outer bound: 4 sequential rebinds can each need up to
    // `RESTART_REBIND_TIMEOUT` under heavy contention (see its doc).
    timeout(Duration::from_secs(300), async {
        let dir = support::panic_safe_tempdir();
        // A single data node is enough to prove "data re-hosts and re-serves
        // after a full outage" — replication/HA across multiple data nodes is
        // already covered elsewhere (`sim_cluster_split_cluster.rs`); keeping
        // the fleet small here directly reduces how many same-address
        // rebinds this test needs (see `RESTART_REBIND_TIMEOUT`'s doc).
        let (control_nodes, data_nodes, config, control_dirs, data_dirs) =
            bring_up_split_durable(3, 1, dir.path()).await;
        await_leader(&control_nodes).await;
        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (3..4).map(animusd::config::node_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        // Schema DDL + data, both meant to survive the full outage.
        let create = MetaCommand::CreateTableSchema {
            table: "restart_t".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        };
        timeout(Duration::from_secs(20), async {
            loop {
                let _ = call(
                    // ADR 0047: `ProposeSchema` is intra-only.
                    data_nodes[0].intra_addr(),
                    ClientRequest::ProposeSchema(create.clone()),
                )
                .await;
                if control_nodes
                    .iter()
                    .all(|n| n.metadata().has_table_schema("restart_t"))
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("schema did not commit before the outage");

        let data_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
        put(&data_clients, "restart_t", b"k0", b"v0", 20).await;
        await_value(&data_clients, "restart_t", b"k0", b"v0", 20).await;

        // Stop EVERYTHING — control trio and data fleet alike.
        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }

        // Restart every node on its own same dir/addresses — control first
        // (the discovery root data nodes mirror), then data.
        let mut restarted_control = Vec::new();
        for (i, d) in control_dirs.iter().enumerate() {
            restarted_control.push(restart_control(&config, i, d).await);
        }
        let mut restarted_data = Vec::new();
        for (idx, d) in data_dirs.iter().enumerate() {
            let i = 3 + idx;
            restarted_data.push(restart_data(&config, i, d, StorageBackend::Lsm).await);
        }

        // Control metadata recovered — a catch-up gate (any node electing),
        // not a specific-node leadership gate: every node in this fresh
        // 3-of-3 restart replays/elects the same as any other cold start.
        await_leader(&restarted_control).await;
        timeout(Duration::from_secs(20), async {
            loop {
                if restarted_control
                    .iter()
                    .all(|n| n.metadata().has_table_schema("restart_t"))
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("control metadata (schema) did not recover after the full restart");

        // Data re-hosts (from durable on-disk state, `Lsm` backend) and the
        // pre-restart write is readable again.
        await_data_nodes_active(&restarted_control, &data_raftkv_ids).await;
        let restarted_clients: Vec<SocketAddr> =
            restarted_data.iter().map(Node::client_addr).collect();
        await_value(&restarted_clients, "restart_t", b"k0", b"v0", 30).await;

        // A fresh write after the restart also works end to end.
        put(&restarted_clients, "restart_t", b"k1", b"v1", 20).await;
        await_value(&restarted_clients, "restart_t", b"k1", b"v1", 20).await;

        for n in restarted_control.iter().chain(restarted_data.iter()) {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("full_split_cluster_restart_recovers_metadata_and_data timed out");
}

// ---- Issue #676: `--cluster-control`/`--cluster-data` threads
// `--quiesce-after`/`--heartbeat-batch`/`--shared-wal` ----------------------

/// The real-`ProdEnv` proof that `--quiesce-after` (and, by the identical
/// wiring, `--heartbeat-batch`/`--shared-wal`) now reaches every data-role
/// node `--cluster-control N --cluster-data M` stands up, not just
/// `--config`/`--node` and `--cluster N` — via
/// [`animusd::start_split_cluster_with_growth`], the exact function
/// `main.rs`'s `run_in_process_split_cluster` calls, with the same
/// non-default value that function's own CLI dispatch would resolve
/// `--quiesce-after 11` to. Observed the same way
/// `tests/admin_endpoint.rs`/`tests/dashboard_endpoint.rs` already prove it
/// for the other two entry points: `GET /admin/config`'s `quiesce_after_ms`
/// field on the DATA-role node (a control-only node has no data plane to
/// quiesce at all — `quiesce_after_ms` is absent there regardless, per
/// `animusd::config::ClusterSettings`'s own applicability table).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_control_data_threads_quiesce_after_to_admin_config() {
    let dir = support::panic_safe_tempdir();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let nodes = loop {
        match animusd::start_split_cluster_with_growth(
            1,
            1,
            dir.path().join("attempt"),
            "127.0.0.1".parse().unwrap(),
            animusd::StorageBackend::Memory,
            None,
            animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
            None,
            None,
            None,
            Duration::from_secs(11),
            animusd::DEFAULT_HEARTBEAT_BATCH,
            animusd::DEFAULT_SHARED_WAL,
        )
        .await
        {
            Ok(nodes) => break nodes,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "could not bring up the split cluster within the deadline: {e}"
                );
                sleep(Duration::from_millis(50)).await;
            }
        }
    };
    // `start_split_cluster_with_growth(1, 1, ..)`: node 0 is control-only,
    // node 1 is data-only (see the function's own `control_n..total` split).
    let (control_node, data_node) = (&nodes[0], &nodes[1]);

    let (status, data_cfg) = admin(data_node.admin_addr(), "GET", "/admin/config", None).await;
    assert_eq!(status, 200, "GET /admin/config (data) failed: {data_cfg}");
    assert_eq!(
        data_cfg["quiesce_after_ms"].as_u64(),
        Some(11_000),
        "quiesce_after_ms did not reflect --quiesce-after 11 threaded through \
         --cluster-control/--cluster-data: {data_cfg}"
    );

    // A control-only node has no data plane to quiesce — the field stays
    // structurally absent there regardless of the flag (see
    // `animusd::config::ClusterSettings`'s own applicability table).
    let (status, control_cfg) =
        admin(control_node.admin_addr(), "GET", "/admin/config", None).await;
    assert_eq!(
        status, 200,
        "GET /admin/config (control) failed: {control_cfg}"
    );
    assert!(
        control_cfg["quiesce_after_ms"].is_null(),
        "a control-only node's /admin/config must never report quiesce_after_ms: {control_cfg}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}
