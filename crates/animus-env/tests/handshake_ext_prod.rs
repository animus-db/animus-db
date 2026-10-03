//! Real-socket `ProdEnv` tests for the handshake `ext` area (ADR 0073 Phase 2,
//! P2-A): the peer's `ext` is stamped on every received `Envelope`, a
//! disjoint version range is refused at the handshake, and the era-on
//! `Network::set_require_peer_ext` hook refuses empty-`ext` (Phase 1) peers —
//! on new connections and on already-open ones. Its own `tests/` target (real
//! sockets and threads, never merged into a SimEnv binary — see CLAUDE.md).
#![cfg(feature = "prod")]
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a real-socket ProdEnv liveness test: real wall-clock timeouts guard it"
)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use animus_env::handshake::encode_ext;
use animus_env::{Env, Metric, Network, NodeId, ProdEnv, nid};

fn unique_tmp_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("animus-hsext-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

async fn bind(id: u64) -> (ProdEnv, SocketAddr) {
    let loop0: SocketAddr = "127.0.0.1:0".parse().unwrap();
    ProdEnv::bind(nid(id), loop0, unique_tmp_dir())
        .await
        .expect("bind")
}

fn peers(of: &[(NodeId, SocketAddr)]) -> std::collections::BTreeMap<NodeId, String> {
    of.iter().map(|(n, a)| (n.clone(), a.to_string())).collect()
}

const T: Duration = Duration::from_secs(10);

/// Polls (never a fixed-deadline one-shot) until `f` holds.
async fn eventually(what: &str, f: impl Fn() -> bool) {
    let deadline = Instant::now() + T;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_ext_is_stamped_on_received_envelopes() {
    let (a, a_addr) = bind(0).await;
    let (b, _) = bind(1).await;
    let b_ext = encode_ext(Some((1, 3)), Some("build-b"));
    a.set_own_ext(encode_ext(Some((2, 5)), None));
    b.set_own_ext(b_ext.clone());
    b.set_peers(peers(&[(nid(0), a_addr)]));
    for i in 0..3u8 {
        b.send(nid(0), vec![i]).await;
    }
    let mut seen = Vec::new();
    for _ in 0..3 {
        let env = tokio::time::timeout(T, a.recv()).await.expect("recv");
        assert_eq!(&*env.peer_ext, &b_ext[..], "ext stamped on every frame");
        seen.push(env.payload[0]);
    }
    // Each send rides its own task, so arrival order is not guaranteed.
    seen.sort_unstable();
    assert_eq!(seen, vec![0, 1, 2]);
    a.shutdown();
    b.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn default_envs_behave_as_before_with_empty_peer_ext() {
    let (a, a_addr) = bind(0).await;
    let (b, _) = bind(1).await;
    assert!(a.own_ext().is_empty());
    b.set_peers(peers(&[(nid(0), a_addr)]));
    b.send(nid(0), b"hi".to_vec()).await;
    let env = tokio::time::timeout(T, a.recv()).await.expect("recv");
    assert_eq!(env.payload, b"hi");
    assert!(env.peer_ext.is_empty());
    a.shutdown();
    b.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn disjoint_ranges_are_refused_and_counted() {
    let (a, a_addr) = bind(0).await;
    let (b, _) = bind(1).await;
    let (c, _) = bind(2).await;
    a.set_own_ext(encode_ext(Some((1, 2)), None));
    b.set_own_ext(encode_ext(Some((3, 4)), None));
    c.set_own_ext(encode_ext(Some((2, 3)), None));
    b.set_peers(peers(&[(nid(0), a_addr)]));
    c.set_peers(peers(&[(nid(0), a_addr)]));
    b.send(nid(0), b"refused".to_vec()).await;
    let b_metrics = b.metrics();
    eventually("a refusal counted", || {
        b_metrics.get(Metric::NetworkHandshakeRefused) >= 1
            || a.metrics().get(Metric::NetworkHandshakeRefused) >= 1
    })
    .await;
    // An overlapping peer is served, and the refused frame never shows up.
    c.send(nid(0), b"ok".to_vec()).await;
    let env = tokio::time::timeout(T, a.recv()).await.expect("recv");
    assert_eq!(env.payload, b"ok");
    assert_eq!(env.from, nid(2));
    assert!(
        tokio::time::timeout(Duration::from_millis(300), a.recv())
            .await
            .is_err(),
        "the disjoint peer's frame must never be delivered"
    );
    a.shutdown();
    b.shutdown();
    c.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn require_flag_refuses_empty_ext_peer_on_new_connection() {
    let (a, a_addr) = bind(0).await;
    let (b, _) = bind(1).await;
    a.set_own_ext(encode_ext(Some((1, 2)), None));
    a.set_require_peer_ext(true);
    b.set_peers(peers(&[(nid(0), a_addr)]));
    b.send(nid(0), b"phase1".to_vec()).await;
    eventually("a's refusal of the Phase 1 peer", || {
        a.metrics().get(Metric::NetworkHandshakeRefused) >= 1
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), a.recv())
            .await
            .is_err()
    );
    // Once the peer advertises an ext (it "upgraded"), it is served.
    b.set_own_ext(encode_ext(Some((1, 1)), None));
    b.send(nid(0), b"upgraded".to_vec()).await;
    let env = tokio::time::timeout(T, a.recv()).await.expect("recv");
    assert_eq!(env.payload, b"upgraded");
    a.shutdown();
    b.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn require_flag_closes_an_already_open_empty_ext_connection() {
    let (a, a_addr) = bind(0).await;
    let (b, _) = bind(1).await;
    b.set_peers(peers(&[(nid(0), a_addr)]));
    b.send(nid(0), b"before".to_vec()).await;
    let env = tokio::time::timeout(T, a.recv()).await.expect("recv");
    assert_eq!(env.payload, b"before");
    a.set_require_peer_ext(true);
    // The pooled connection is closed on its next frame (which is dropped),
    // and every re-dial is then refused at the handshake.
    let a_metrics = a.metrics();
    let deadline = Instant::now() + T;
    while a_metrics.get(Metric::NetworkHandshakeRefused) == 0 {
        assert!(Instant::now() < deadline, "no refusal after flag flip");
        b.send(nid(0), b"after".to_vec()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(300), a.recv())
            .await
            .is_err(),
        "no frame may be delivered after the require flag"
    );
    a.shutdown();
    b.shutdown();
}
