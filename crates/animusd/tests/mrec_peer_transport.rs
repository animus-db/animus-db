//! Cross-cluster MREC transport over **real loopback sockets** (ADR 0075
//! section 4.3, G-01 stage G-d M3): two independent one-node `animusd`
//! clusters ("east" and "west"), each with the other as a configured peer,
//! and `ProdPeerClient` (reached through `animusd::mrec_peer::
//! probe_peer_for_test`) dialling the peer's **intra** port with a real
//! `tokio-rustls` handshake.
//!
//! What this proves: the `MrecApply` frame crosses the mutual-TLS intra port
//! between two clusters with *different* CAs (each side's `ca_path` bundles
//! both), a stranger CA is refused, a plaintext node refuses to dial or
//! accept unless `allow_insecure_peers`, and the receiver handler is reached
//! (a decoded `MrecApplyResponse`, never a transport error).
//!
//! What it does not: applying data. Converting a table to MREC in a real
//! cluster needs the replica-create saga (M4), so the receiver answers
//! `Refused { retryable: true }` ("not enabled yet" / "not an MREC global
//! table"); LWW/skew/routing semantics are covered over `SimWorld`
//! (`sim_world_mrec_tests.rs`). The full real-process two-cluster apply is M6.
//!
//! **Real time/sockets (the `ProdEnv` edge)**, like every `animusd`
//! integration test.

use std::path::Path;
use std::time::Duration;

use animus_node::{MREC_PROTO, MrecApplyRequest, MrecApplyResponse};
use animusd::config::{ClusterSettings, PeerCluster, TlsSection};
use animusd::mrec_peer::probe_peer_for_test;
use animusd::{ClusterConfig, Node, RoleAddrs};

mod support;

struct Side {
    node: Node,
    config: ClusterConfig,
}

fn role(bound: &animusd::BoundNode, tls: Option<TlsSection>) -> RoleAddrs {
    RoleAddrs {
        tls,
        ..support::bound_role_addrs(bound)
    }
}

fn config(
    own: RoleAddrs,
    region: &str,
    peer_region: &str,
    peer_intra: std::net::SocketAddr,
    allow_insecure: bool,
) -> ClusterConfig {
    ClusterConfig {
        version: animusd::config::CLUSTER_CONFIG_VERSION,
        nodes: vec![own],
        dynamo_auth: None,
        cluster_settings: Some(ClusterSettings {
            region: Some(region.into()),
            peers: vec![PeerCluster {
                region: peer_region.into(),
                endpoints: vec![peer_intra.to_string()],
                tls_ca: None,
            }],
            allow_insecure_peers: allow_insecure.then_some(true),
            ..Default::default()
        }),
    }
}

/// Bring up east + west. `tls` is each side's section (`None` = plaintext).
async fn two_clusters(
    dir: &Path,
    tls: [Option<TlsSection>; 2],
    allow_insecure: [bool; 2],
) -> (Side, Side) {
    let [tls_e, tls_w] = tls;
    let east = animusd::Node::bind(
        animusd::config::node_id(0),
        RoleAddrs {
            tls: tls_e.clone(),
            ..support::unbound_role_addrs(0)
        },
        dir.join("east"),
    )
    .await
    .expect("bind east");
    let west = animusd::Node::bind(
        animusd::config::node_id(0),
        RoleAddrs {
            tls: tls_w.clone(),
            ..support::unbound_role_addrs(0)
        },
        dir.join("west"),
    )
    .await
    .expect("bind west");
    let ce = config(
        role(&east, tls_e),
        "east",
        "west",
        west.intra_addr(),
        allow_insecure[0],
    );
    let cw = config(
        role(&west, tls_w),
        "west",
        "east",
        east.intra_addr(),
        allow_insecure[1],
    );
    let ne = animusd::run_bound_node(east, &ce, 0)
        .await
        .expect("start east");
    let nw = animusd::run_bound_node(west, &cw, 0)
        .await
        .expect("start west");
    (
        Side {
            node: ne,
            config: ce,
        },
        Side {
            node: nw,
            config: cw,
        },
    )
}

/// Two PKIs (one per cluster), each cluster's `ca_path` a bundle of both CAs:
/// the documented cross-cluster mutual-TLS shape. `lone_west = true` leaves
/// east's CA out of west's bundle (a stranger).
fn pkis(dir: &Path, lone_west: bool) -> [Option<TlsSection>; 2] {
    let (_d1, mut a) = support::tls_pki(&["127.0.0.1"]);
    let (_d2, mut b) = support::tls_pki(&["127.0.0.1"]);
    // Move the PKI files out of the temp dirs (which drop here) into `dir`.
    let place = |s: &mut TlsSection, tag: &str| {
        for (field, name) in [
            (&mut s.cert_path, "cert"),
            (&mut s.key_path, "key"),
        ] {
            let to = dir.join(format!("{tag}-{name}.pem"));
            std::fs::copy(&*field, &to).expect("copy pem");
            *field = to;
        }
        let ca = s.ca_path.clone().expect("ca");
        let to = dir.join(format!("{tag}-ca.pem"));
        std::fs::copy(&ca, &to).expect("copy ca");
        s.ca_path = Some(to);
    };
    place(&mut a[0], "east");
    place(&mut b[0], "west");
    let ca_a = std::fs::read(a[0].ca_path.as_ref().unwrap()).unwrap();
    let ca_b = std::fs::read(b[0].ca_path.as_ref().unwrap()).unwrap();
    let bundle = |own: &[u8], other: Option<&[u8]>, tag: &str| {
        let mut v = own.to_vec();
        if let Some(o) = other {
            v.push(b'\n');
            v.extend_from_slice(o);
        }
        let p = dir.join(format!("{tag}-bundle.pem"));
        std::fs::write(&p, v).unwrap();
        p
    };
    a[0].ca_path = Some(bundle(&ca_a, Some(&ca_b), "east"));
    b[0].ca_path = Some(bundle(&ca_b, (!lone_west).then_some(&ca_a[..]), "west"));
    [a.pop(), b.pop()]
}

fn request() -> Vec<u8> {
    serde_json::to_vec(&MrecApplyRequest {
        proto: MREC_PROTO,
        from_region: "east".into(),
        table: "t".into(),
        records: vec![],
    })
    .unwrap()
}

fn decode(bytes: &[u8]) -> MrecApplyResponse {
    serde_json::from_slice(bytes).expect("an MrecApplyResponse")
}

const T: Duration = Duration::from_secs(10);

/// The receiver handler was reached over mutual TLS: a decoded, *retryable*
/// refusal about the table/gate (not about TLS or peers).
fn assert_reached(resp: &MrecApplyResponse) {
    match resp {
        MrecApplyResponse::Refused { message, retryable } => {
            assert!(*retryable, "must be retryable: {message}");
            assert!(
                message.contains("MrecReplication gate")
                    || message.contains("not an MREC global table"),
                "unexpected refusal: {message}"
            );
        }
        other => panic!("expected a refusal from the handler, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutual_tls_between_two_cas_carries_an_mrec_frame_to_the_peer_handler() {
    let dir = support::panic_safe_tempdir();
    let (east, west) = two_clusters(dir.path(), pkis(dir.path(), false), [false, false]).await;
    let bytes = probe_peer_for_test(&east.config, 0, 0, request(), T)
        .await
        .expect("a mutual-TLS dial between the two CAs succeeds");
    assert_reached(&decode(&bytes));
    // And the other direction (west -> east, asking as west).
    let mut req: MrecApplyRequest = serde_json::from_slice(&request()).unwrap();
    req.from_region = "west".into();
    let bytes = probe_peer_for_test(&west.config, 0, 0, serde_json::to_vec(&req).unwrap(), T)
        .await
        .expect("west -> east");
    assert_reached(&decode(&bytes));
    east.node.shutdown_graceful().await;
    west.node.shutdown_graceful().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_whose_ca_is_not_in_the_receivers_bundle_is_refused() {
    let dir = support::panic_safe_tempdir();
    let (east, west) = two_clusters(dir.path(), pkis(dir.path(), true), [false, false]).await;
    let r = probe_peer_for_test(&east.config, 0, 0, request(), Duration::from_secs(5)).await;
    assert!(r.is_err(), "a stranger CA must not get an answer: {r:?}");
    east.node.shutdown_graceful().await;
    west.node.shutdown_graceful().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plaintext_peers_are_refused_unless_allow_insecure_peers() {
    // No TLS, no allow_insecure: the client refuses locally, nothing is sent.
    let dir = support::panic_safe_tempdir();
    let (east, west) = two_clusters(dir.path(), [None, None], [false, false]).await;
    let r = probe_peer_for_test(&east.config, 0, 0, request(), T).await;
    let err = r.expect_err("plaintext dial without allow_insecure_peers");
    assert!(err.contains("requires TLS"), "{err}");
    east.node.shutdown_graceful().await;
    west.node.shutdown_graceful().await;

    // Sender opted in, receiver did not: the receiver refuses, non-retryably.
    let dir = support::panic_safe_tempdir();
    let (east, west) = two_clusters(dir.path(), [None, None], [true, false]).await;
    let bytes = probe_peer_for_test(&east.config, 0, 0, request(), T)
        .await
        .expect("frame delivered");
    match decode(&bytes) {
        MrecApplyResponse::Refused { message, retryable } => {
            assert!(!retryable && message.contains("requires TLS"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    east.node.shutdown_graceful().await;
    west.node.shutdown_graceful().await;

    // Both opted in (dev/sim): plaintext flows and the handler is reached.
    let dir = support::panic_safe_tempdir();
    let (east, west) = two_clusters(dir.path(), [None, None], [true, true]).await;
    let bytes = probe_peer_for_test(&east.config, 0, 0, request(), T)
        .await
        .expect("insecure peers allowed");
    assert_reached(&decode(&bytes));
    east.node.shutdown_graceful().await;
    west.node.shutdown_graceful().await;
}
