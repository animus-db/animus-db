//! Issue #676 — the real-`ProdEnv` end-to-end proof that `animusd join` and
//! `animusd data --seed` (the two real growth paths, S-06's own scope cut)
//! now resolve the same per-node data-plane knobs `--config FILE --node I`
//! already does, instead of hardcoding the pre-#676 off/zero values.
//!
//! - `join_defaults_to_shared_wal_matching_a_bare_config_node`: a bare
//!   `animusd join` (no flags — `animusd::run_node_join`'s own unchanged
//!   signature) now writes the SAME shared-WAL layout a bare `--config
//!   FILE --node I` does by default, closing the exact surprise the issue
//!   names — proven the same way `tests/shared_wal_e2e.rs`'s own
//!   layout-mismatch regression does: restarting the joined node's own
//!   directory through the real `--config`/`--node` startup surface with
//!   the flag flipped is refused with `check_wal_layout`'s loud error,
//!   restarting with it matching succeeds.
//! - `join_no_shared_wal_writes_the_per_group_layout`: the explicit
//!   `--no-shared-wal` opt-out (`animusd::run_node_join_with_settings`)
//!   reaches `join` too.
//! - `data_seed_join_threads_quiesce_after_to_admin_config`: `--quiesce-after`
//!   on `animusd data --seed` (`animusd::run_node_data_join_with_settings`)
//!   reaches the joined node's own reconciler, observed via `GET
//!   /admin/config`'s `quiesce_after_ms` field (`admin.rs::config_view`).
//! - `join_threads_encryption_key`: `--encryption-key` on `join` seals the
//!   joined node's own data directory — mirrors
//!   `tests/encryption_at_rest_e2e.rs`'s plaintext-absence proof.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use animusd::{
    BackupStoreConfig, ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs,
    SegmentStoreConfig, StorageBackend, read_frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

async fn call(addr: SocketAddr, req: ClientRequest) -> Option<ClientResponse> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    animusd::write_frame(&mut stream, &req).await.ok()?;
    read_frame(&mut stream).await.ok()?
}

/// Try every client address in `clients` (round-robin) until one accepts the
/// write — mirrors `tests/data_join.rs::put`.
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

/// Every member's status, `raftkv_id -> "Active"/"Down"/...`, from
/// `/admin/status` — mirrors `tests/seed_join.rs::member_statuses`/
/// `tests/data_join.rs::member_statuses`.
async fn member_statuses(
    admin_addr: SocketAddr,
) -> std::collections::BTreeMap<animus_env::NodeId, String> {
    let (_status, v) = admin_get(admin_addr, "/admin/status").await;
    v["members"]
        .as_object()
        .expect("members is an object")
        .iter()
        .map(|(id, m)| {
            (
                id.parse().expect("member id key is a valid NodeId"),
                m["status"].as_str().expect("status is a string").to_owned(),
            )
        })
        .collect()
}

/// A table whose tablet currently lists `raftkv_id` as a replica, if any —
/// mirrors `tests/seed_join.rs::table_with_replica`'s own doc: with RF =
/// min(N,3) and exactly 2 total data nodes here, a freshly-created table's
/// *initial* placement already covers both, but only once the joined node
/// has actually been promoted `Active` — polling this (rather than assuming
/// the very first `put` lands on both) is what actually proves the joined
/// node hosts a real replica, not just a `Metadata` member row.
async fn table_with_replica(
    admin_addr: SocketAddr,
    raftkv_id: &animus_env::NodeId,
) -> Option<String> {
    let (_status, v) = admin_get(admin_addr, "/admin/status").await;
    v["tablets"]
        .as_object()
        .expect("tablets is an object")
        .values()
        .find_map(|t| {
            let has_replica = t["replicas"]
                .as_array()
                .expect("replicas is an array")
                .iter()
                .filter_map(|r| r.as_str())
                .any(|r| r == raftkv_id.as_str());
            has_replica
                .then(|| t["table"].as_str().map(str::to_owned))
                .flatten()
        })
}

/// Wait for `raftkv_id` to be promoted `Active` (the growth-node
/// self-registration + heartbeat/failure-detector promotion, ADR 0032 PR2),
/// then wait for it to gain a REAL tablet replica and write+confirm through
/// its own client address — the "genuinely hosts data, not just a
/// registered-but-inert member" proof every join test in this crate uses
/// before treating a joined node's directory as having real WAL content to
/// check the layout of.
async fn wait_hosted_and_write(
    base_admin: SocketAddr,
    raftkv_id: &animus_env::NodeId,
    clients: &[SocketAddr],
    table: &str,
) {
    let promoted = async {
        loop {
            if member_statuses(base_admin)
                .await
                .get(raftkv_id)
                .map(String::as_str)
                == Some("Active")
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), promoted)
        .await
        .unwrap_or_else(|_| panic!("joined node {raftkv_id} never promoted to Active"));

    let hosted = async {
        loop {
            if table_with_replica(base_admin, raftkv_id).await.is_some() {
                return;
            }
            put(clients, table, b"k0", b"v0", 20).await;
            sleep(Duration::from_millis(200)).await;
        }
    };
    timeout(Duration::from_secs(60), hosted)
        .await
        .unwrap_or_else(|_| panic!("joined node {raftkv_id} never gained a tablet replica"));
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
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .expect("status line");
    let value: serde_json::Value = serde_json::from_str(payload).expect("admin body is JSON");
    (status, value)
}

/// Bring up the initial 1-node combined-mode core to join against
/// (port-TOCTOU mitigation) — see `support::bring_up_deadline`.
async fn bring_up_base(dir: &Path) -> (Vec<Node>, ClusterConfig) {
    support::bring_up_deadline(1, dir, support::JOIN_DEADLINE).await
}

/// A fresh combined-mode `RoleAddrs` (own ports, no config file) for join
/// index `index`.
fn fresh_combined_addrs(index: usize, encryption_key_path: Option<String>) -> RoleAddrs {
    let raw = support::free_addrs(6);
    RoleAddrs {
        id: animusd::config::node_id(index),
        role: animusd::config::NodeRole::Both,
        internal: raw[0],
        client: raw[1],
        dynamo: raw[2],
        admin: raw[3],
        intra: raw[4],
        console: raw[5],
        advertise_host: None,
        tls: None,
        encryption_key_path,
    }
}

/// Join a fresh combined-mode node via a BARE `animusd::run_node_join` call
/// (its own unchanged, defaulted signature — no settings overrides), the
/// exact shape a bare `animusd join` with no flags produces. Retries the
/// allocate-ports-and-join step as a unit against a deadline, the same
/// port-TOCTOU-resilient shape `support::join_fresh_deadline` uses.
async fn join_bare(seeds: &[SocketAddr], index: usize, dir: &Path) -> (Node, RoleAddrs, PathBuf) {
    let deadline = tokio::time::Instant::now() + support::JOIN_DEADLINE;
    let addrs = fresh_combined_addrs(index, None);
    let mut attempt: u64 = 0;
    loop {
        let node_dir = dir.join(format!("join-bare-{index}-{attempt}"));
        match animusd::run_node_join(
            seeds.iter().map(ToString::to_string).collect(),
            Some(addrs.id.clone()),
            addrs.clone(),
            &node_dir,
            StorageBackend::Memory,
            std::collections::BTreeMap::new(),
        )
        .await
        {
            Ok(node) => return (node, addrs, node_dir),
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "could not join node {index} within {:?}: {e}",
                    support::JOIN_DEADLINE
                );
                sleep(Duration::from_millis(50)).await;
                attempt += 1;
            }
        }
    }
}

/// Join a fresh combined-mode node with explicit settings
/// ([`animusd::run_node_join_with_settings`]) — the widened sibling of
/// [`join_bare`], for the tests that exercise a non-default flag
/// explicitly.
#[allow(clippy::too_many_arguments)]
async fn join_with_settings(
    seeds: &[SocketAddr],
    index: usize,
    dir: &Path,
    quiesce_after: Duration,
    heartbeat_batch: bool,
    shared_wal: bool,
    encryption_key_path: Option<String>,
) -> (Node, RoleAddrs, PathBuf) {
    let deadline = tokio::time::Instant::now() + support::JOIN_DEADLINE;
    let addrs = fresh_combined_addrs(index, encryption_key_path);
    let mut attempt: u64 = 0;
    loop {
        let node_dir = dir.join(format!("join-{index}-{attempt}"));
        match animusd::run_node_join_with_settings(
            seeds.iter().map(ToString::to_string).collect(),
            Some(addrs.id.clone()),
            addrs.clone(),
            &node_dir,
            StorageBackend::Memory,
            std::collections::BTreeMap::new(),
            quiesce_after,
            heartbeat_batch,
            shared_wal,
            SegmentStoreConfig::default(),
            BackupStoreConfig::default(),
        )
        .await
        {
            Ok(node) => return (node, addrs, node_dir),
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "could not join node {index} within {:?}: {e}",
                    support::JOIN_DEADLINE
                );
                sleep(Duration::from_millis(50)).await;
                attempt += 1;
            }
        }
    }
}

/// A synthetic single-node `ClusterConfig` pointing at `addrs` — reconstructs
/// the shape a real `--config FILE --node 0` deployment would use to
/// restart exactly the joined node's own directory/addresses, the same
/// technique `tests/shared_wal_e2e.rs`/`tests/encryption_at_rest_e2e.rs`
/// use for their own restart-with-flipped-flag proofs.
fn solo_config(addrs: &RoleAddrs) -> ClusterConfig {
    ClusterConfig {
        nodes: vec![addrs.clone()],
        dynamo_auth: None,
        cluster_settings: None,
    }
}

async fn start_solo_with_shared_wal(
    config: &ClusterConfig,
    dir: &Path,
    shared_wal: bool,
) -> Result<Node, String> {
    animusd::run_node_with_cluster_settings(
        config,
        0,
        dir,
        StorageBackend::Memory,
        animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
        animusd::StreamSealKnobs::default(),
        SegmentStoreConfig::default(),
        animusd::DEFAULT_STREAM_RETENTION,
        Duration::ZERO,
        false,
        None,
        None,
        None,
        BackupStoreConfig::default(),
        None,
        None,
        None,
        None,
        None,
        shared_wal,
    )
    .await
    .map_err(|e| e.to_string())
}

/// **The core issue #676 regression for the WAL half**: a bare `animusd
/// join` (no `--shared-wal`/`--no-shared-wal` at all — `run_node_join`'s own
/// unchanged, defaulted signature) must write the SAME on-by-default shared
/// WAL layout a bare `--config FILE --node I` does, not silently fall back
/// to the pre-#676 hardcoded per-group layout. Proven by restarting the
/// joined node's OWN directory through the real `--config`/`--node`
/// startup surface (`run_node_with_cluster_settings`, the same path
/// `main.rs`'s `run_single` uses) with the flag matching (succeeds) and
/// flipped (refused with `check_wal_layout`'s loud mismatch error) —
/// exactly `tests/shared_wal_e2e.rs::a_restart_with_shared_wal_flipped_
/// refuses_to_start`'s own technique, applied to a JOINED node's directory
/// instead of one started directly.
#[tokio::test(flavor = "multi_thread")]
async fn join_defaults_to_shared_wal_matching_a_bare_config_node() {
    let dir = support::panic_safe_tempdir();
    let (base_nodes, _base_config) = bring_up_base(dir.path()).await;
    let seeds: Vec<SocketAddr> = base_nodes.iter().map(Node::intra_addr).collect();

    let (joined, joined_addrs, joined_dir) = join_bare(&seeds, 1, dir.path()).await;
    let joined_id = animusd::config::node_id(1);

    // Wait for real promotion + a genuine tablet replica landing on the
    // joined node before treating its directory as having real WAL content
    // to check the layout of — see `wait_hosted_and_write`'s own doc.
    let clients = [base_nodes[0].client_addr(), joined.client_addr()];
    wait_hosted_and_write(
        base_nodes[0].admin_addr(),
        &joined_id,
        &clients,
        "wal_reach_t",
    )
    .await;

    joined.shutdown_graceful().await;
    for n in &base_nodes {
        n.shutdown_graceful().await;
    }

    let solo = solo_config(&joined_addrs);

    // Restarting with `shared_wal: true` (matching the default this join
    // should have used) must succeed — no layout mismatch.
    start_solo_with_shared_wal(&solo, &joined_dir, true)
        .await
        .expect(
            "restarting the joined node's own directory with shared_wal=true (the default) \
             must succeed if `join` genuinely defaulted to the shared layout",
        )
        .shutdown_graceful()
        .await;

    // Restarting with the OPPOSITE (`shared_wal: false`) must be refused —
    // proving definitively it's the SHARED layout on disk, not merely "the
    // first restart happened not to fail".
    let err = match start_solo_with_shared_wal(&solo, &joined_dir, false).await {
        Err(e) => e,
        Ok(_) => {
            panic!("restarting with shared_wal=false against a shared-WAL directory must fail")
        }
    };
    assert!(
        err.contains("shared WAL file"),
        "expected a shared-WAL-layout mismatch error, got: {err}"
    );
    assert!(err.contains("--no-shared-wal"), "error text: {err}");
}

/// The explicit `--no-shared-wal` opt-out ([`animusd::
/// run_node_join_with_settings`]) also reaches `join` — the converse
/// direction of the default-posture proof above.
#[tokio::test(flavor = "multi_thread")]
async fn join_no_shared_wal_writes_the_per_group_layout() {
    let dir = support::panic_safe_tempdir();
    let (base_nodes, _base_config) = bring_up_base(dir.path()).await;
    let seeds: Vec<SocketAddr> = base_nodes.iter().map(Node::intra_addr).collect();

    let (joined, joined_addrs, joined_dir) = join_with_settings(
        &seeds,
        1,
        dir.path(),
        Duration::ZERO,
        false,
        // `--no-shared-wal`
        false,
        None,
    )
    .await;
    let joined_id = animusd::config::node_id(1);

    let clients = [base_nodes[0].client_addr(), joined.client_addr()];
    wait_hosted_and_write(
        base_nodes[0].admin_addr(),
        &joined_id,
        &clients,
        "wal_reach_no_shared_t",
    )
    .await;

    joined.shutdown_graceful().await;
    for n in &base_nodes {
        n.shutdown_graceful().await;
    }

    let solo = solo_config(&joined_addrs);

    // Restarting with `shared_wal: false` (matching `--no-shared-wal`) must
    // succeed.
    start_solo_with_shared_wal(&solo, &joined_dir, false)
        .await
        .expect("restarting with shared_wal=false must succeed after --no-shared-wal join")
        .shutdown_graceful()
        .await;

    // Restarting with `shared_wal: true` must be refused — the per-group
    // layout is genuinely on disk.
    let err = match start_solo_with_shared_wal(&solo, &joined_dir, true).await {
        Err(e) => e,
        Ok(_) => {
            panic!("restarting with shared_wal=true against a per-group-WAL directory must fail")
        }
    };
    assert!(
        err.contains("per-group"),
        "expected a per-group-layout mismatch error, got: {err}"
    );
    assert!(err.contains("--no-shared-wal"), "error text: {err}");
}

/// `--quiesce-after` on `animusd data --seed` (`run_node_data_join_with_
/// settings`) reaches the joined node's own reconciler — observed via `GET
/// /admin/config`'s `quiesce_after_ms` field, the same signal
/// `admin_endpoint.rs`/`dashboard_endpoint.rs` already use for the
/// `--config`/`--node` path (roadmap U-06).
#[tokio::test(flavor = "multi_thread")]
async fn data_seed_join_threads_quiesce_after_to_admin_config() {
    let dir = support::panic_safe_tempdir();
    let (base_nodes, _base_config) = bring_up_base(dir.path()).await;
    let seeds: Vec<SocketAddr> = base_nodes.iter().map(Node::intra_addr).collect();

    let raw = support::free_addrs(6);
    let addrs = RoleAddrs {
        id: animusd::config::node_id(1),
        role: animusd::config::NodeRole::Data,
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
    let joined = animusd::run_node_data_join_with_settings(
        seeds.iter().map(ToString::to_string).collect(),
        Some(addrs.id.clone()),
        addrs,
        &dir.path().join("data-seed-quiesce"),
        StorageBackend::Memory,
        std::collections::BTreeMap::new(),
        None,
        // `--quiesce-after 7`
        Duration::from_secs(7),
        animusd::DEFAULT_HEARTBEAT_BATCH,
        animusd::DEFAULT_SHARED_WAL,
        SegmentStoreConfig::default(),
        BackupStoreConfig::default(),
    )
    .await
    .expect("data --seed join failed");

    let (status, cfg) = admin_get(joined.admin_addr(), "/admin/config").await;
    assert_eq!(status, 200, "GET /admin/config failed: {cfg}");
    assert_eq!(
        cfg["quiesce_after_ms"].as_u64(),
        Some(7_000),
        "quiesce_after_ms did not reflect --quiesce-after 7 threaded through data --seed: {cfg}"
    );

    joined.shutdown_graceful().await;
    for n in &base_nodes {
        n.shutdown_graceful().await;
    }
}

/// `--encryption-key` on `join` seals the joined node's own data directory
/// — mirrors `tests/encryption_at_rest_e2e.rs`'s plaintext-absence proof,
/// scoped to just the joined node's own directory (the base cluster stays
/// unencrypted throughout, proving the key is genuinely per-node, not
/// cluster-wide, on this path).
#[tokio::test(flavor = "multi_thread")]
async fn join_threads_encryption_key() {
    const PLAINTEXT_NEEDLE: &[u8] = b"issue-676-plaintext-needle";
    let dir = support::panic_safe_tempdir();
    let (base_nodes, _base_config) = bring_up_base(dir.path()).await;
    let seeds: Vec<SocketAddr> = base_nodes.iter().map(Node::intra_addr).collect();

    let key_path = dir.path().join("key.hex");
    std::fs::write(&key_path, "11".repeat(32)).expect("write key file");
    let key_path = key_path.to_string_lossy().into_owned();

    let (joined, _addrs, joined_dir) = join_with_settings(
        &seeds,
        1,
        dir.path(),
        Duration::ZERO,
        false,
        false,
        Some(key_path),
    )
    .await;
    let joined_id = animusd::config::node_id(1);

    let clients = [base_nodes[0].client_addr(), joined.client_addr()];
    wait_hosted_and_write(
        base_nodes[0].admin_addr(),
        &joined_id,
        &clients,
        "encryption_reach_t",
    )
    .await;
    // The real payload this test is about — written directly through the
    // JOINED node's own client address (not round-robin), on the table
    // `wait_hosted_and_write` already confirmed it replicates, and read
    // back through it too, so this genuinely proves ITS disk holds the item.
    put(
        &[joined.client_addr()],
        "encryption_reach_t",
        b"k1",
        PLAINTEXT_NEEDLE,
        20,
    )
    .await;
    let readback = call(
        joined.client_addr(),
        ClientRequest::Get {
            key: b"k1".to_vec(),
            table: "encryption_reach_t".to_string(),
            stale: false,
        },
    )
    .await;
    assert!(
        matches!(readback, Some(ClientResponse::Value(Some(ref v))) if v == PLAINTEXT_NEEDLE),
        "expected the joined node's own client to read back the written value, got: {readback:?}"
    );

    joined.shutdown_graceful().await;
    for n in &base_nodes {
        n.shutdown_graceful().await;
    }

    assert_plaintext_absent(&joined_dir, PLAINTEXT_NEEDLE);
}

/// Walks every regular file under `dir` (recursively) and asserts none
/// contain `needle` as raw bytes — see
/// `tests/encryption_at_rest_e2e.rs::assert_plaintext_absent`'s identical
/// doc.
fn assert_plaintext_absent(dir: &Path, needle: &[u8]) {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    walk(dir, &mut files);
    assert!(
        !files.is_empty(),
        "expected at least one file under {dir:?} to scan"
    );
    for file in files {
        let Ok(bytes) = std::fs::read(&file) else {
            continue;
        };
        assert!(
            !bytes.windows(needle.len()).any(|w| w == needle),
            "plaintext value leaked into {file:?}"
        );
    }
}
