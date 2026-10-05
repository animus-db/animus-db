//! End-to-end regression test: the real `animus admin ...` CLI against a
//! real `animusd` admin listener (`ProdEnv`, real loopback socket).
//!
//! Bug: `http_call` dialed through `maybe_tls_connect`, which always ran the
//! client-protocol handshake preamble. The admin listener is plain HTTP
//! (optionally server-only TLS) and never answers a preamble, so every
//! `animus admin ...` subcommand failed with
//! `client handshake with <addr> failed: TimedOut`. No earlier test drove the
//! CLI binary against a real admin listener (see the lesson
//! `docs/lessons/testing/2026-10-04-cli-admin-path-never-ran-against-real-admin-listener.md`).
//!
//! Real processes + sockets: every wait is a converged-or-deadline poll.

#![allow(
    clippy::disallowed_methods,
    reason = "real-process/real-socket test: wall-clock deadlines are the point (ProdEnv liveness, not SimEnv)"
)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use animusd::config::{NodeRole, TlsSection};
use animusd::{ClusterConfig, Node, RoleAddrs};

/// Overall bound for one CLI call to succeed. A pre-fix run fails each
/// attempt after the CLI's own 10s handshake timeout, so this deadline is
/// what bounds the failing case.
const OVERALL: Duration = Duration::from_secs(40);

fn role_addrs(tls: Option<TlsSection>) -> RoleAddrs {
    let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
    RoleAddrs {
        id: animusd::config::node_id(0),
        role: NodeRole::Both,
        internal: any,
        client: any,
        dynamo: any,
        admin: any,
        intra: any,
        console: any,
        advertise_host: None,
        tls,
        encryption_key_path: None,
        labels: Default::default(),
        overload: None,
    }
}

/// Bind-and-hold a single-node cluster (`Node::bind` resolves `:0` itself, so
/// no port can be stolen) and start it.
async fn start_node(dir: &Path, tls: Option<TlsSection>) -> Node {
    let bound = Node::bind(
        animusd::config::node_id(0),
        role_addrs(tls.clone()),
        dir.join("node-0"),
    )
    .await
    .expect("bind node");
    let config = ClusterConfig {
        version: animusd::config::CLUSTER_CONFIG_VERSION,
        nodes: vec![RoleAddrs {
            id: bound.id().clone(),
            role: NodeRole::Both,
            internal: bound.internal_addr(),
            client: bound.client_addr(),
            dynamo: bound.dynamo_addr(),
            admin: bound.admin_addr(),
            intra: bound.intra_addr(),
            console: bound.console_addr(),
            advertise_host: None,
            tls,
            encryption_key_path: None,
            labels: Default::default(),
            overload: None,
        }],
        dynamo_auth: None,
        cluster_settings: None,
    };
    animusd::run_bound_node(bound, &config, 0)
        .await
        .expect("start node")
}

/// Test PKI: a CA plus one leaf valid for `127.0.0.1`.
fn test_pki(dir: &Path) -> (TlsSection, std::path::PathBuf) {
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "animus-cli test CA");
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_path = dir.join("ca.pem");
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();

    let mut leaf = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
    leaf.distinguished_name
        .push(DnType::CommonName, "127.0.0.1");
    let leaf_key = KeyPair::generate().unwrap();
    let leaf_cert = leaf.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();
    let cert_path = dir.join("node.cert.pem");
    let key_path = dir.join("node.key.pem");
    std::fs::write(&cert_path, leaf_cert.pem()).unwrap();
    std::fs::write(&key_path, leaf_key.serialize_pem()).unwrap();
    (
        TlsSection {
            cert_path,
            key_path,
            ca_path: Some(ca_path.clone()),
        },
        ca_path,
    )
}

/// Path to the `animus` binary under test. CI runs this target from a
/// cargo-nextest archive extracted on another runner (`--workspace-remap`),
/// where the compile-time `CARGO_BIN_EXE_animus` path does not exist; nextest
/// sets `NEXTEST_BIN_EXE_animus` to the remapped path at runtime, so prefer it
/// and fall back to the compile-time path for plain `cargo test`.
fn animus_bin() -> PathBuf {
    std::env::var_os("NEXTEST_BIN_EXE_animus")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_animus")))
}

/// Run `animus [--tls-ca CA] admin <sub> <addr>` until it succeeds with a
/// stdout satisfying `ok`, or `OVERALL` elapses (then panic with the last
/// stdout/stderr).
async fn run_admin_until(sub: &str, addr: SocketAddr, ca: Option<&Path>, ok: fn(&str) -> bool) {
    let sub = sub.to_string();
    let ca = ca.map(Path::to_path_buf);
    let out = tokio::task::spawn_blocking(move || {
        let deadline = Instant::now() + OVERALL;
        loop {
            let mut cmd = Command::new(animus_bin());
            if let Some(ca) = &ca {
                cmd.arg("--tls-ca").arg(ca);
            }
            let o = cmd
                .args(["admin", &sub, &addr.to_string()])
                .output()
                .expect("spawn animus");
            let stdout = String::from_utf8_lossy(&o.stdout).into_owned();
            let stderr = String::from_utf8_lossy(&o.stderr).into_owned();
            if o.status.success() && ok(&stdout) {
                return Ok(stdout);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "`animus admin {sub} {addr}` never succeeded: status {:?}\nstdout: {stdout}\nstderr: {stderr}",
                    o.status
                ));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    })
    .await
    .expect("join");
    if let Err(e) = out {
        panic!("{e}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_admin_subcommands_work_against_real_plain_admin_listener() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_node(dir.path(), None).await;
    let addr = node.admin_addr();
    // `health` and `config` are flat one-shot admin routes (`http_call`).
    run_admin_until("health", addr, None, |s| !s.trim().is_empty()).await;
    run_admin_until("config", addr, None, |s| s.trim_start().starts_with('{')).await;
    node.shutdown_graceful().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_admin_subcommands_work_against_real_tls_admin_listener() {
    let dir = tempfile::tempdir().unwrap();
    let (section, ca) = test_pki(dir.path());
    let node = start_node(dir.path(), Some(section)).await;
    let addr = node.admin_addr();
    run_admin_until("health", addr, Some(&ca), |s| !s.trim().is_empty()).await;
    run_admin_until("config", addr, Some(&ca), |s| {
        s.trim_start().starts_with('{')
    })
    .await;
    node.shutdown_graceful().await;
}
