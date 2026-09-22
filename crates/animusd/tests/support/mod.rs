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

use animusd::config::{NodeRole, TlsSection};
use animusd::{ClusterConfig, Node, RoleAddrs, StorageBackend};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// A [`TempDir`] that survives a panicking unwind instead of removing its
/// directory tree (issue #511).
///
/// **The mechanism this closes**: a prod-liveness test's locals normally
/// drop in the safe order (a `TempDir` declared before its `Vec<Node>`/
/// `Node`s, so the nodes drop first) — but [`Node`]'s own `Drop` impl only
/// latches every *hosted CP group's* `halted` flag (issues #282/#279,
/// `ClusterEdgeState::halt_hosted_cp_groups`); it deliberately does **not**
/// abort this node's tasks or tear down its envs, so the control-plane Raft
/// driver task (and every other background loop) keeps running, detached,
/// past the point its owning `Node` was dropped. **The control-plane WAL
/// has no `halted`-gate at all** (see `animus-control/CLAUDE.md`'s "The WAL
/// `fsync` is raced..." entry) — `persist_wal`'s `env.append(WAL,
/// ..).await.expect("wal append")`/`env.sync(WAL).await.expect("wal
/// sync")` are bare, unconditional `.expect()`s on every node, live or
/// shutting down, with no tolerated-error path whatsoever. So a plain
/// `tempfile::TempDir` dropping right after — synchronously removing the
/// directory tree as part of the SAME panicking unwind — races that still-
/// live, still-polled control-plane driver task: the next time it appends/
/// syncs its WAL against now-vanished files, `persist_wal`'s bare
/// `.expect()` panics unconditionally, on whichever thread is driving it.
/// That panic is indistinguishable from a genuine live durability fault and
/// buries the actual assertion failure that triggered the unwind in the
/// first place (issue #511) — the very thing issue #273 first found for a
/// bring-up helper that owned-and-dropped its own `TempDir` internally,
/// just reached here by a mid-test panic instead of a badly-scoped helper.
///
/// The fix here is deliberately **not** another `halted`-gate (that would
/// mean teaching `animus-control`'s consensus WAL about test-only teardown,
/// widening a production I/O panic's tolerance for a testing concern, and
/// still wouldn't be deterministic — `task.abort()` only *requests*
/// cancellation, docs on [`Node::shutdown`] are explicit that it doesn't
/// wait). Instead: **never let the directory disappear out from under a
/// panicking test at all.** On a clean, non-panicking drop this behaves
/// exactly like `TempDir` (the directory is removed — no leak on the
/// passing-test path); on a *panicking* drop it leaks the directory
/// (`std::mem::forget`) instead of removing it, so any background task
/// still racing it keeps seeing real files and never faults — the original
/// panic (the real assertion failure) is what surfaces, and CI's ephemeral
/// runner bounds the leaked directory's lifetime. This protects **every**
/// background loop uniformly (the control-plane WAL persist above, and any
/// CP-group I/O the `halted` latch already covers or doesn't), and touches
/// no production code: a real, non-test `.expect("wal append")` durability
/// panic is completely unaffected — this only ever changes whether a
/// **test's own `TempDir`** is removed on an unwind it did not cause.
///
/// Construct via [`panic_safe_tempdir`]; call [`path`](Self::path) exactly
/// like `TempDir::path`.
pub struct PanicSafeTempDir(Option<TempDir>);

impl PanicSafeTempDir {
    /// This directory's path — mirrors `TempDir::path`.
    pub fn path(&self) -> &Path {
        self.0
            .as_ref()
            .expect("PanicSafeTempDir: path() called after drop")
            .path()
    }
}

impl Drop for PanicSafeTempDir {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // Leak rather than remove: see the type's own doc for why this
            // is the fix, not a `halted`-style latch.
            if let Some(dir) = self.0.take() {
                std::mem::forget(dir);
            }
        }
        // Not panicking: fall through and let the `Option<TempDir>` drop
        // normally below, removing the directory exactly like a bare
        // `TempDir` would on the ordinary passing-test path.
    }
}

/// Construct a [`PanicSafeTempDir`] — the drop-in replacement for
/// `tempfile::tempdir().unwrap()` every `support::`-fixture-using
/// prod-liveness test should use instead, so a mid-test panic can never
/// cascade into issue #511's WAL panic. See [`PanicSafeTempDir`]'s own doc
/// for the full mechanism.
pub fn panic_safe_tempdir() -> PanicSafeTempDir {
    PanicSafeTempDir(Some(
        tempfile::tempdir().expect("create panic-safe temp dir"),
    ))
}

/// A teardown check that fails the test loudly if any watched node counted a
/// **spawned-task panic** during the test (issue #939).
///
/// **The gap this closes**: `crates/animusd/tests/streams_e2e.rs`'s
/// `cascade_split_walks_the_grandparent_chain_with_closed_shard_shape`
/// reported ok in Run-6 even though a leader's apply task had already
/// panicked (`animus_cp_data::apply_and_compact`'s split-fork seal-marker
/// `.expect(..)` firing on a real `wal group-commit sync failed` under disk
/// pressure) — the replica just quietly stopped applying, and nothing in
/// the test happened to assert against exactly that now-dead replica before
/// the test's own assertions were satisfied some other way. `ProdEnv::spawn`
/// now counts a spawned task's panic on the env itself (see that impl's own
/// doc), but counting it is not the same as *checking* it — this type is
/// the check, so a zombie replica can never again masquerade as a passing
/// run.
///
/// Construct with [`watch_task_panics`] right after a cluster is brought
/// up (or extend an existing guard with [`TaskPanicGuard::watch`] for nodes
/// added later, e.g. after [`grow_deadline`]). On a **non-panicking** drop
/// — so a genuine assertion failure already unwinding never also tries to
/// panic here, which would abort the process rather than report cleanly —
/// it panics naming the total count and the first captured message if any
/// watched node's [`Node::spawned_task_panics`] is nonzero. A clean drop
/// with nothing counted is silent and free: this never changes a passing
/// test's outcome.
///
/// [`assert_no_task_panics`] is the non-`Drop`, explicit-call sibling for a
/// test that prefers to check at a specific point rather than at teardown.
#[must_use = "binding this to `_` drops it immediately, checking nothing — \
              bind it with `let guard = watch_task_panics(..)` and keep it \
              alive for the span you want checked"]
pub struct TaskPanicGuard<'a> {
    nodes: Vec<&'a Node>,
}

impl<'a> TaskPanicGuard<'a> {
    /// Track more nodes (e.g. ones joined/grown into the cluster after this
    /// guard was constructed).
    pub fn extend(&mut self, nodes: &[&'a Node]) {
        self.nodes.extend_from_slice(nodes);
    }

    /// Alias for [`extend`](Self::extend) reading better at a single-node
    /// call site (`guard.watch(&[&new_node])`).
    pub fn watch(&mut self, nodes: &[&'a Node]) {
        self.extend(nodes);
    }
}

impl Drop for TaskPanicGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // Already unwinding for some other (likely more informative)
            // reason — never panic-in-drop on top of it, which would abort
            // the process instead of reporting either failure cleanly.
            return;
        }
        let total: u64 = self.nodes.iter().map(|n| n.spawned_task_panics()).sum();
        if total == 0 {
            return;
        }
        let first = self
            .nodes
            .iter()
            .find_map(|n| n.first_spawned_task_panic())
            .unwrap_or_else(|| "<no message captured>".to_string());
        panic!(
            "a background task spawned through env.spawn_task panicked during \
             this test (issue #939) — {total} panic(s) counted across watched \
             node(s); first: {first}"
        );
    }
}

/// Start watching `nodes` for a spawned-task panic (issue #939) — see
/// [`TaskPanicGuard`]'s own doc for the full mechanism. Keep the returned
/// guard alive (bound to a `let`, never `let _ =`) for the rest of the test.
pub fn watch_task_panics<'a>(nodes: &[&'a Node]) -> TaskPanicGuard<'a> {
    let mut guard = TaskPanicGuard { nodes: Vec::new() };
    guard.extend(nodes);
    guard
}

/// Explicit, non-`Drop` sibling of [`watch_task_panics`]/[`TaskPanicGuard`]
/// for a test that prefers to assert at one specific point instead of at
/// teardown.
pub fn assert_no_task_panics(nodes: &[&Node]) {
    for node in nodes {
        let count = node.spawned_task_panics();
        assert_eq!(
            count,
            0,
            "node counted {count} spawned-task panic(s) (issue #939); first: {}",
            node.first_spawned_task_panic()
                .unwrap_or_else(|| "<no message captured>".to_string())
        );
    }
}

/// Default wall-clock deadline for the `*_deadline` join/bring-up helpers
/// below — generous enough that a genuinely broken join still fails loudly,
/// while riding out the transient port-TOCTOU-under-`--workspace`-contention
/// window that a fixed attempt count could exhaust (see each helper's doc).
pub const JOIN_DEADLINE: Duration = Duration::from_secs(30);

/// Reserve `count` free loopback ports (bind :0, read addr, release the
/// listener). **This is itself a port-TOCTOU by construction**: the port is
/// free the instant this returns, so another process/test's own bind can
/// steal it before the real one happens — see issue #627's own investigation
/// (`docs/lessons/testing/2026-09-20-allocate-test-ports-by-binding-and-
/// holding-never-probe-and-release.md`) for the two structural hazards this
/// created in every bring-up that used to retry around it instead. **Every
/// fresh-cluster bring-up in this module now avoids this window entirely**
/// by binding with ephemeral (`:0`) addresses directly via [`Node::bind`] and
/// holding the resulting listeners open until the node itself starts (see
/// [`bring_up_deadline`]/[`bring_up_deadline_tls`]/[`start_single_node`]) —
/// this function survives only for the callers that genuinely need a
/// not-yet-bound address to hand to a **joiner or growth** path *before* that
/// path itself binds anything ([`grow_deadline`], [`join_fresh_deadline`],
/// [`join_data_fresh_deadline`], [`join_allocated_fresh_deadline`],
/// [`join_data_allocated_fresh_deadline`], [`bring_up_split`]) — those retry
/// their own bind-and-join step as a unit, the same shape every one of these
/// bring-up helpers used before this fix.
pub fn free_addrs(count: usize) -> Vec<SocketAddr> {
    let listeners: Vec<std::net::TcpListener> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
    // listeners dropped here, freeing the ports for the caller to bind.
}

/// `127.0.0.1:0` — an address [`Node::bind`] resolves to a real,
/// OS-assigned ephemeral port at bind time, atomically, with no separate
/// probe-then-release step (contrast [`free_addrs`]'s own doc).
fn ephemeral_loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// A fresh, unbound [`RoleAddrs`] for [`Node::bind`] — every port
/// `127.0.0.1:0`, [`NodeRole::Both`], no advertise host/TLS/encryption key.
/// `id` is [`animusd::config::node_id(index)`].
fn unbound_role_addrs(index: usize) -> RoleAddrs {
    RoleAddrs {
        id: animusd::config::node_id(index),
        role: NodeRole::Both,
        internal: ephemeral_loopback(),
        client: ephemeral_loopback(),
        dynamo: ephemeral_loopback(),
        admin: ephemeral_loopback(),
        intra: ephemeral_loopback(),
        console: ephemeral_loopback(),
        advertise_host: None,
        tls: None,
        encryption_key_path: None,
    }
}

/// The [`RoleAddrs`] entry a [`ClusterConfig`] should carry for an
/// already-[`Node::bind`]-bound `node` — its own resolved addresses, its own
/// id, same role/advertise-host/TLS/encryption-key shape [`unbound_role_addrs`]
/// gave it (a combined-mode bring-up fixture never sets the last three).
fn bound_role_addrs(node: &animusd::BoundNode) -> RoleAddrs {
    RoleAddrs {
        id: node.id().clone(),
        role: NodeRole::Both,
        internal: node.internal_addr(),
        client: node.client_addr(),
        dynamo: node.dynamo_addr(),
        admin: node.admin_addr(),
        intra: node.intra_addr(),
        console: node.console_addr(),
        advertise_host: None,
        tls: None,
        encryption_key_path: None,
    }
}

/// Start a single-node cluster. **Bind-and-hold, not probe-and-release**
/// (issue #627): [`Node::bind`] itself resolves every `127.0.0.1:0` port to
/// a real, OS-assigned address at bind time and holds every listener open
/// until the node starts, so there is no window in which another
/// process/test could steal one — no retry loop is needed or present. A bind
/// failure here can only be genuine local resource exhaustion (too many open
/// fds/ports), never a stolen port, so it panics immediately rather than
/// retrying.
pub async fn start_single_node(dir: &Path, backend: StorageBackend) -> (Node, ClusterConfig) {
    let bound = Node::bind(animusd::config::node_id(0), unbound_role_addrs(0), dir)
        .await
        .unwrap_or_else(|e| panic!("start_single_node: bind failed: {e}"));
    let config = ClusterConfig {
        nodes: vec![bound_role_addrs(&bound)],
        dynamo_auth: None,
        cluster_settings: None,
    };
    let node = animusd::run_bound_node_with(bound, &config, 0, backend)
        .await
        .unwrap_or_else(|e| panic!("start_single_node: start failed: {e}"));
    (node, config)
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

/// Bring up a combined-mode `n`-node core, one process per node — **bind
/// every node first, then start every node** (issue #627), never
/// probe-and-release-then-rebind-one-at-a-time: each of `n` nodes is
/// [`Node::bind`]-bound (holding its six listeners open, OS-assigned ports)
/// before any of them is started, so a bind failure on node *k* can never
/// leave nodes `0..k-1` already running with live background tasks that then
/// need tearing down before a retry — and once every bind succeeds, no later
/// step can ever race a still-open port a not-yet-started node holds, because
/// nothing here ever releases one. **There is no retry loop any more**: a
/// `:0` bind is assigned by the kernel atomically at bind time and held by
/// this cluster's own nodes until they themselves drop it, so no other
/// process or test can ever be named as the culprit in this config — the old
/// partial-start-then-teardown retry (and the survivor-task hazard it
/// created: an untracked per-connection handler from a torn-down partial
/// attempt could still dial out to whatever a *later* attempt or a sibling
/// test had since bound at the very port that attempt's own address named,
/// under the same reused `"n{i}"` node id and with no cluster identity on
/// the raw Raft wire to catch it — see `docs/lessons/testing/
/// 2026-09-20-allocate-test-ports-by-binding-and-holding-never-probe-and-
/// release.md`) is gone entirely, not merely retried around. `deadline` is
/// now a pure **liveness bound on bring-up as a whole** (wrapped in
/// [`tokio::time::timeout`]): a genuinely hung bind or start fails loudly
/// once `deadline` elapses instead of hanging the test forever — it no
/// longer has anything to do with port contention. Each bind failure and
/// each start failure panics immediately, naming the failing node's index
/// and the underlying error (a bind failure here can only be genuine local
/// resource exhaustion, never a stolen port). Data dirs are `core-{i}` — no
/// attempt counter, since there is only ever one attempt.
pub async fn bring_up_deadline(
    n: usize,
    dir: &Path,
    deadline: Duration,
) -> (Vec<Node>, ClusterConfig) {
    timeout(deadline, async move {
        let mut bounds = Vec::with_capacity(n);
        for i in 0..n {
            let bound = Node::bind(
                animusd::config::node_id(i),
                unbound_role_addrs(i),
                dir.join(format!("core-{i}")),
            )
            .await
            .unwrap_or_else(|e| panic!("bring_up_deadline: node {i} failed to bind: {e}"));
            bounds.push(bound);
        }
        let config = ClusterConfig {
            nodes: bounds.iter().map(bound_role_addrs).collect(),
            dynamo_auth: None,
            cluster_settings: None,
        };
        let mut nodes = Vec::with_capacity(n);
        for (i, bound) in bounds.into_iter().enumerate() {
            let node = animusd::run_bound_node(bound, &config, i)
                .await
                .unwrap_or_else(|e| panic!("bring_up_deadline: node {i} failed to start: {e}"));
            nodes.push(node);
        }
        (nodes, config)
    })
    .await
    .unwrap_or_else(|_| {
        panic!("bring_up_deadline: {n}-node cluster did not come up within {deadline:?}")
    })
}

// ---- TLS (ADR 0064, S-01 commit 2) ---------------------------------------
//
// A small, independent copy of `animus-env`'s own `prod::tests::{test_pki,
// write_test_pki}` (real self-signed CA + per-node leaf certs via `rcgen`,
// dev-dependency only) — that helper is `#[cfg(test)]`-private to
// `animus-env`'s own crate, so it cannot be reused across the crate
// boundary; this is the "small copy" the task's own contingency names.
// Every leaf's SAN/CN is `"127.0.0.1"`, matching `animus_env::tls::
// server_name_for`'s derivation for a loopback dial address on any port —
// every fixture in this module binds nodes on `127.0.0.1`.

/// Generate a self-signed test CA plus one leaf certificate per entry in
/// `names` (order preserved), write every PEM to real files under a fresh
/// temp dir, and return `(temp_dir, tls_sections)` — one [`TlsSection`] per
/// name, each already pointing at that leaf's cert/key and the shared CA.
/// The returned [`TempDir`] must outlive every node using these sections
/// (they name real files on disk); callers keep it alive for the fixture's
/// own lifetime.
pub fn tls_pki(names: &[&str]) -> (TempDir, Vec<TlsSection>) {
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};

    let dir = tempfile::tempdir().expect("create tls pki temp dir");

    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "animusd test CA");
    let ca_key = KeyPair::generate().expect("generate ca key");
    let ca_cert = ca_params.self_signed(&ca_key).expect("self-sign ca");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, ca_cert.pem()).expect("write ca.pem");

    let sections = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let mut leaf_params =
                CertificateParams::new(vec![(*name).to_string()]).expect("leaf params");
            leaf_params
                .distinguished_name
                .push(DnType::CommonName, *name);
            let leaf_key = KeyPair::generate().expect("generate leaf key");
            let leaf_cert = leaf_params
                .signed_by(&leaf_key, &ca_cert, &ca_key)
                .expect("sign leaf with ca");

            let cert_path = dir.path().join(format!("node{i}.cert.pem"));
            let key_path = dir.path().join(format!("node{i}.key.pem"));
            std::fs::write(&cert_path, leaf_cert.pem()).expect("write cert pem");
            std::fs::write(&key_path, leaf_key.serialize_pem()).expect("write key pem");

            TlsSection {
                cert_path,
                key_path,
                ca_path: Some(ca_path.clone()),
            }
        })
        .collect();

    (dir, sections)
}

/// Like [`bring_up_deadline`], but every node's [`RoleAddrs::tls`] is set
/// from a freshly generated [`tls_pki`] — mutual TLS on `internal`/`intra`,
/// server-only on `client`/`dynamo`/`admin`/`console` (every port a
/// combined node binds). Returns the PKI's own [`TempDir`] alongside the
/// usual `(nodes, config)` — callers must keep it alive for as long as the
/// returned nodes run (their TLS material was loaded from these files at
/// bind time, but a restart/rebind against the same config would need them
/// again). Bind-and-hold, same as [`bring_up_deadline`] — the port itself is
/// irrelevant to which leaf cert a node presents (every leaf's SAN/CN is
/// `"127.0.0.1"`, [`tls_pki`]'s own doc), so nothing about TLS material
/// selection changes with this fix; only the allocation mechanism does.
pub async fn bring_up_deadline_tls(
    n: usize,
    dir: &Path,
    deadline: Duration,
) -> (Vec<Node>, ClusterConfig, TempDir) {
    let (pki_dir, sections) = tls_pki(&vec!["127.0.0.1"; n]);
    timeout(deadline, async move {
        let mut bounds = Vec::with_capacity(n);
        for (i, section) in sections.iter().enumerate() {
            let addrs = RoleAddrs {
                tls: Some(section.clone()),
                ..unbound_role_addrs(i)
            };
            let bound = Node::bind(
                animusd::config::node_id(i),
                addrs,
                dir.join(format!("tls-core-{i}")),
            )
            .await
            .unwrap_or_else(|e| panic!("bring_up_deadline_tls: node {i} failed to bind: {e}"));
            bounds.push(bound);
        }
        let config = ClusterConfig {
            nodes: bounds
                .iter()
                .enumerate()
                .map(|(i, b)| RoleAddrs {
                    tls: Some(sections[i].clone()),
                    ..bound_role_addrs(b)
                })
                .collect(),
            dynamo_auth: None,
            cluster_settings: None,
        };
        let mut nodes = Vec::with_capacity(n);
        for (i, bound) in bounds.into_iter().enumerate() {
            let node = animusd::run_bound_node(bound, &config, i)
                .await
                .unwrap_or_else(|e| panic!("bring_up_deadline_tls: node {i} failed to start: {e}"));
            nodes.push(node);
        }
        (nodes, config, pki_dir)
    })
    .await
    .unwrap_or_else(|_| {
        panic!("bring_up_deadline_tls: {n}-node TLS cluster did not come up within {deadline:?}")
    })
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
                advertise_host: None,
                tls: None,
                encryption_key_path: None,
            });
        }
        let expanded = ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };
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
///
/// **Issue #406/#450 (Bug A)**: the ports (and therefore the `--id`'s
/// `NodeAddrs`) are picked **once**, before the retry loop, and reused on
/// every attempt — never re-randomized per attempt. `run_node_join`'s
/// `claim_join_identity` durably registers `--id`'s `NodeAddrs` (a
/// `MetaCommand::RegisterNode` CAS, ADR 0040 Decision C) *before* it ever
/// calls `Node::bind`; if that bind then fails (the ordinary port-TOCTOU
/// this retry exists for, issue #278), the old code re-picked brand-new
/// ports for the *same* `--id` on the next attempt, so the retry's own
/// re-registration proposed a **different** `NodeAddrs` for an id that had
/// already durably claimed a different one moments earlier — a genuine CAS
/// collision against itself, surfacing as "node id already claimed by a
/// different registration (different addresses/labels)". Reusing the same
/// `addrs` makes every retry's re-registration land on the *idempotent*
/// `NoOp` path instead (`existing == addrs`), mirroring
/// [`restart_same_addrs`]'s own retry-in-place idiom for the identical
/// reason: a transient port-TOCTOU bind failure is best retried on the same
/// address (the conflict is another test binary's momentary `free_addrs`
/// probe, not a permanently-held port), not papered over by minting a new
/// one that then collides with this attempt's own already-durable claim.
/// See `docs/engineering-lessons.md` for the general lesson.
pub async fn join_fresh_deadline(
    seeds: &[SocketAddr],
    index: usize,
    dir: &Path,
    backend: StorageBackend,
    deadline: Duration,
) -> (Node, RoleAddrs, PathBuf) {
    let hard_deadline = tokio::time::Instant::now() + deadline;
    let mut attempt: u64 = 0;
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
        advertise_host: None,
        tls: None,
        encryption_key_path: None,
    };
    loop {
        let node_dir = dir.join(format!("join-{index}-{attempt}"));
        match animusd::run_node_join(
            seeds.iter().map(ToString::to_string).collect(),
            Some(id.clone()),
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
            advertise_host: None,
            tls: None,
            encryption_key_path: None,
        };
        let node_dir = dir.join(format!("data-join-{index}-{attempt}"));
        match animusd::run_node_data_join(
            seeds.iter().map(ToString::to_string).collect(),
            Some(id),
            addrs,
            &node_dir,
            backend,
            BTreeMap::new(),
            None,
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
            advertise_host: None,
            tls: None,
            encryption_key_path: None,
        };
        let node_dir = dir.join(format!("join-alloc-{label}-{attempt}"));
        match animusd::run_node_join(
            seeds.iter().map(ToString::to_string).collect(),
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
            advertise_host: None,
            tls: None,
            encryption_key_path: None,
        };
        let node_dir = dir.join(format!("data-join-alloc-{label}-{attempt}"));
        match animusd::run_node_data_join(
            seeds.iter().map(ToString::to_string).collect(),
            None,
            addrs,
            &node_dir,
            backend,
            BTreeMap::new(),
            None,
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

/// Bring-up barrier for a **combined-mode** cluster: wait converged-or-timeout
/// until (1) some node believes it is the control-plane leader **and** (2)
/// **every** node observes **every** founding data node's raftkv id as
/// [`NodeStatus::Active`](animusd::NodeStatus) in its own replicated
/// [`metadata()`](Node::metadata).
///
/// **Why "members Active", not merely "members present" — the issue #1028
/// race.** A node's own `RegisterNode` self-registration can land as `Down`
/// (the status a freshly-registered member carries until the failure detector
/// promotes it) *before* `bootstrap`'s idempotent `Active` upsert applies. A
/// barrier that gates only on "some control leader + a non-empty members map"
/// (the shape the local `await_bootstrap` helpers copy-pasted into ~60 test
/// files used) therefore returns while one or more founding nodes are still
/// recorded `Down`. That matters because placement is status-sensitive:
/// `ClientCtx::provision_tablet` (`schema.rs`) seeds the first tablet from the
/// first `MAX_REPLICATION_FACTOR` members whose `status == Active`, **in id
/// order**, so a node still `Down` at the moment the first placement-sensitive
/// write provisions the tablet is silently skipped in favour of a higher-id
/// `Active` peer — and stays skipped until the failure detector promotes it
/// (~100-200ms later, wider under CPU load). A placement-sensitive first write
/// can then land on the wrong node set (e.g. `{n0,n1,n3}`, skipping `n2`),
/// which flakes any test that hard-codes which node is a voter vs. the idle
/// spare (issue #1028).
///
/// Gating on **all members Active** closes the window: once every founding
/// member is promoted, the failure detector has caught up, so a subsequent
/// `provision_tablet` sees the full `Active` set and places deterministically.
///
/// **Read from every node, not just the leader.** The status-sensitive
/// `provision_tablet` read (`metadata_fresh()`) runs on whichever node serves
/// the first write — not necessarily the control leader — and many callers
/// read a specific node's `Status`/metadata immediately after this barrier
/// returns (e.g. `cluster.rs`, which the copied helpers' own `all(|n|
/// members.len() == nodes.len())` variant already protected). Gating on the
/// leader alone would let a lagging follower still show a partial or
/// not-yet-`Active` membership right after the barrier. So this requires the
/// full, all-`Active` membership to be visible on **every** node — a strict
/// strengthening of every copied `await_bootstrap` shape (the leader is one of
/// the nodes checked, and "all Active" implies "all present").
///
/// # Signature
/// Takes `&[Node]` for a combined-mode cluster, where **every** node is a
/// data-role node registered in `members`. Because the started [`Node`] type
/// exposes no accessor for its own membership id, the per-node check gates on
/// the equivalent-for-combined-mode condition: `metadata().members` has
/// `len() == nodes.len()` **and** every member's status is `Active`. In
/// combined mode every node claims exactly one `members` row (control-only
/// nodes never do, but a combined-mode fixture has none), so a full-count,
/// all-`Active` map is precisely "every founding data node is Active".
pub async fn await_bootstrap(nodes: &[Node]) {
    let expected = nodes.len();
    timeout(Duration::from_secs(30), async {
        loop {
            let leader = nodes.iter().any(Node::is_control_leader);
            let all_active = nodes.iter().all(|node| {
                let members = node.metadata().members;
                members.len() == expected
                    && members
                        .values()
                        .all(|m| m.status == animusd::NodeStatus::Active)
            });
            if leader && all_active {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect(
        "cluster did not reach an all-members-Active bootstrap within 30s \
         (issue #1028: gating on members-present rather than members-Active \
         lets a still-Down founding node be skipped by placement)",
    );
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

/// One best-effort `GET /admin/status` against `addr`, returning the raw
/// response body text (or a description of why it failed) — deliberately
/// raw, not parsed `Value`, since this is diagnostic-only (folded into a
/// timeout panic message) and a malformed/absent response is itself
/// diagnostic. Mirrors [`engine_applied_index`]'s own minimal GET-only shape.
async fn admin_status_get(addr: SocketAddr) -> Result<String, String> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    let request = "GET /admin/status HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n";
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| format!("write failed: {e}"))?;
    stream
        .flush()
        .await
        .map_err(|e| format!("flush failed: {e}"))?;
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .map_err(|e| format!("read failed: {e}"))?;
    let text = String::from_utf8(raw).map_err(|e| format!("non-utf8 response: {e}"))?;
    let (_head, payload) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| "response has no body".to_string())?;
    Ok(payload.to_string())
}

/// Best-effort, bounded (~2s per node) snapshot of every `config.nodes[i]`'s
/// own `GET /admin/status` — one line per node naming its index, its admin
/// address, and either the raw JSON body it answered with or the error that
/// kept it from answering. Meant to be folded into a timeout panic message
/// (`cp_cross_process.rs`'s own retry-loop timeouts, issue #627) alongside
/// the last `ClientResponse::Error` text, so a next occurrence pinpoints
/// which node actually answered and what its own leader/term/membership
/// view of the cluster looked like at the moment the write kept failing —
/// none of the existing `ClientResponse::Error` strings name which node
/// answered `call()`'s own connect, only what that node's own routing
/// decision concluded (see the issue's own investigation notes). Never
/// panics — a wedged/unreachable process is exactly the scenario this
/// diagnostic exists to describe, not fail on.
pub async fn cluster_status_snapshot(config: &ClusterConfig) -> String {
    let mut out = String::new();
    for (i, node) in config.nodes.iter().enumerate() {
        let line = match timeout(Duration::from_secs(2), admin_status_get(node.admin)).await {
            Ok(Ok(body)) => format!("node {i} (admin {}): {body}", node.admin),
            Ok(Err(e)) => format!("node {i} (admin {}): {e}", node.admin),
            Err(_) => format!("node {i} (admin {}): timed out after 2s", node.admin),
        };
        out.push_str(&line);
        out.push('\n');
    }
    out
}
