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
//! The last test (`two_real_clusters_replicate_a_table_both_ways_over_mutual_tls`,
//! G-d M6) is the end-to-end one: it finalizes both clusters to the MREC
//! gate, creates a table over the DynamoDB wire on east, issues
//! `UpdateTable ReplicaUpdates Create west`, and polls until a write on each
//! side is readable on the other, every WAN byte crossing the mutual-TLS intra
//! port. LWW/skew/routing/fault semantics are covered over `SimWorld`
//! (`sim_world_mrec_*`); this proves the real sockets, the TLS peer client and
//! the shipper/saga loops under `ProdEnv`.
//!
//! **Real time/sockets (the `ProdEnv` edge)**, like every `animusd`
//! integration test.

use std::path::Path;
use std::time::Duration;

use animus_node::{MREC_PROTO, MrecApplyRequest, MrecApplyResponse};
use animusd::config::{ClusterSettings, PeerCluster, TlsSection};
use animusd::mrec_peer::{probe_peer_for_test, probe_peer_request_for_test};
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
    let peer_ca = own
        .tls
        .as_ref()
        .and_then(|t| t.peer_ca_path.as_ref())
        .map(|p| p.to_string_lossy().into_owned());
    ClusterConfig {
        version: animusd::config::CLUSTER_CONFIG_VERSION,
        nodes: vec![own],
        dynamo_auth: None,
        cluster_settings: Some(ClusterSettings {
            region: Some(region.into()),
            peers: vec![PeerCluster {
                region: peer_region.into(),
                endpoints: vec![peer_intra.to_string()],
                // The peer's CA, to verify its server certificate (the node's
                // own `ca_path` is its own CA only).
                tls_ca: peer_ca,
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

/// Two PKIs (one per cluster): each `ca_path` is its own CA and each
/// `peer_ca_path` the other's (issue #1253). `lone_west = true` leaves east's
/// CA out of west's trust entirely (a stranger).
fn pkis(dir: &Path, lone_west: bool) -> [Option<TlsSection>; 2] {
    let (_d1, mut a) = support::tls_pki(&["127.0.0.1"]);
    let (_d2, mut b) = support::tls_pki(&["127.0.0.1"]);
    // Move the PKI files out of the temp dirs (which drop here) into `dir`.
    let place = |s: &mut TlsSection, tag: &str| {
        for (field, name) in [(&mut s.cert_path, "cert"), (&mut s.key_path, "key")] {
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
    // Issue #1253: `ca_path` stays each cluster's OWN CA; the other cluster's
    // CA goes in `peer_ca_path`, admitted to the handshake but trusted for
    // MREC replication frames only.
    let peer_file = |ca: &Path, tag: &str| {
        let p = dir.join(format!("{tag}-peer-ca.pem"));
        std::fs::copy(ca, &p).unwrap();
        p
    };
    let ca_a = a[0].ca_path.clone().unwrap();
    let ca_b = b[0].ca_path.clone().unwrap();
    a[0].peer_ca_path = Some(peer_file(&ca_b, "east"));
    b[0].peer_ca_path = (!lone_west).then(|| peer_file(&ca_a, "west"));
    [a.pop(), b.pop()]
}

fn request() -> Vec<u8> {
    serde_json::to_vec(&MrecApplyRequest {
        proto: MREC_PROTO,
        from_region: "east".into(),
        table: "t".into(),
        records: vec![],
        control: None,
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

// ---- Issue #1253: a peer-region certificate is trusted for MREC only -------

fn probe_get() -> animus_node::ClientRequest {
    animus_node::ClientRequest::Get {
        key: b"k".to_vec(),
        table: "t".into(),
        stale: false,
    }
}

fn assert_gate_refused(resp: &animus_node::ClientResponse, what: &str) {
    match resp {
        animus_node::ClientResponse::Error(e) => assert!(
            e.contains("not permitted for a peer-region certificate"),
            "{what}: wrong error: {e}"
        ),
        other => panic!("{what}: expected the peer-region refusal, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_region_certificate_may_send_only_mrec_apply_on_the_intra_port() {
    let dir = support::panic_safe_tempdir();
    let (east, west) = two_clusters(dir.path(), pkis(dir.path(), false), [false, false]).await;
    let east_intra = east.node.intra_addr().to_string();
    // West's certificate chains only to the peer-region CA bundle on east.
    let west_trusts_east_ca = west.config.nodes[0]
        .tls
        .as_ref()
        .and_then(|t| t.peer_ca_path.clone())
        .expect("west peer_ca_path");
    let send = |req: animus_node::ClientRequest| {
        probe_peer_request_for_test(
            &west.config,
            0,
            Some(&west_trusts_east_ca),
            east_intra.clone(),
            req,
            T,
        )
    };

    let forwarded = animus_node::ClientRequest::Forwarded {
        request: Box::new(probe_get()),
        traceparent: None,
    };
    assert_gate_refused(&send(forwarded).await.expect("dial"), "Forwarded");
    assert_gate_refused(&send(probe_get()).await.expect("dial"), "bare Get");

    // The MREC replication frame still reaches the receiver handler.
    let mrec = animus_node::ClientRequest::MrecApply(serde_json::from_slice(&request()).unwrap());
    match send(mrec).await.expect("dial") {
        animus_node::ClientResponse::MrecApply(resp) => assert_reached(&resp),
        other => panic!("MrecApply must reach the handler, got {other:?}"),
    }

    east.node.shutdown_graceful().await;
    west.node.shutdown_graceful().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_own_ca_certificate_is_not_gated_on_the_intra_port() {
    let dir = support::panic_safe_tempdir();
    let (east, west) = two_clusters(dir.path(), pkis(dir.path(), false), [false, false]).await;
    let east_intra = east.node.intra_addr().to_string();
    // East dials its own intra port: the certificate chains to east's own CA.
    let forwarded = animus_node::ClientRequest::Forwarded {
        request: Box::new(probe_get()),
        traceparent: None,
    };
    for (what, req) in [("Forwarded", forwarded), ("bare Get", probe_get())] {
        let resp = probe_peer_request_for_test(&east.config, 0, None, east_intra.clone(), req, T)
            .await
            .expect("dial");
        if let animus_node::ClientResponse::Error(e) = &resp {
            assert!(
                !e.contains("peer-region certificate"),
                "{what}: an own-CA certificate must not be gated: {e}"
            );
        }
    }
    east.node.shutdown_graceful().await;
    west.node.shutdown_graceful().await;
}

// ---- G-d M6: the real-process two-cluster apply ----------------------------

/// One HTTP/1.1 request over a server-only TLS connection (the dynamo and
/// admin ports); `(status, body)`. Same shape as `tls_e2e.rs`'s helper.
async fn tls_http(
    ca_path: &Path,
    addr: std::net::SocketAddr,
    head: &str,
    body: &str,
) -> (u16, String) {
    use rustls_pki_types::pem::PemObject;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let bytes = std::fs::read(ca_path).expect("read ca");
    let mut roots = rustls::RootCertStore::empty();
    for c in rustls_pki_types::CertificateDer::pem_slice_iter(&bytes) {
        roots.add(c.expect("ca cert")).expect("add ca");
    }
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(cfg));
    let tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let name = animus_env::tls::server_name_for(&addr.to_string()).expect("server name");
    let mut stream = connector.connect(name, tcp).await.expect("tls handshake");
    let req = format!(
        "{head}\r\nHost: x\r\nConnection: close\r\nContent-Type: application/x-amz-json-1.0\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf).await; // no close_notify from this edge
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    (status, body.to_owned())
}

struct Wire<'a> {
    side: &'a Side,
}

impl Wire<'_> {
    fn ca(&self) -> std::path::PathBuf {
        self.side.config.nodes[0]
            .tls
            .as_ref()
            .and_then(|t| t.ca_path.clone())
            .expect("tls ca")
    }
    async fn dynamo(&self, op: &str, body: &str) -> (u16, String) {
        tls_http(
            &self.ca(),
            self.side.node.dynamo_addr(),
            &format!("POST / HTTP/1.1\r\nX-Amz-Target: DynamoDB_20120810.{op}"),
            body,
        )
        .await
    }
    async fn admin_post(&self, path: &str, body: &str) -> (u16, String) {
        tls_http(
            &self.ca(),
            self.side.config.nodes[0].admin,
            &format!("POST {path} HTTP/1.1"),
            body,
        )
        .await
    }
    async fn admin_get(&self, path: &str) -> (u16, String) {
        tls_http(
            &self.ca(),
            self.side.config.nodes[0].admin,
            &format!("GET {path} HTTP/1.1"),
            "",
        )
        .await
    }
    async fn get(&self, table: &str, k: &str) -> Option<String> {
        let (st, b) = self
            .dynamo(
                "GetItem",
                &format!(r#"{{"TableName":"{table}","Key":{{"pk":{{"S":"{k}"}}}},"ConsistentRead":true}}"#),
            )
            .await;
        if st != 200 {
            return None;
        }
        let v: serde_json::Value = serde_json::from_str(&b).ok()?;
        v["Item"]["v"]["S"].as_str().map(str::to_owned)
    }
    async fn put(&self, table: &str, k: &str, v: &str) {
        let (st, b) = self
            .dynamo(
                "PutItem",
                &format!(
                    r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"{k}"}},"v":{{"S":"{v}"}}}}}}"#
                ),
            )
            .await;
        assert_eq!(st, 200, "PutItem {k}: {b}");
    }
}

/// Poll `f` until it yields `Some`, or panic with `what` after `secs`.
async fn converge<T, F, Fut>(what: &str, secs: u64, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(v) = f().await {
            return v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn two_real_clusters_replicate_a_table_both_ways_over_mutual_tls() {
    use animus_control::version::Gate;
    let dir = support::panic_safe_tempdir();
    let (east, west) = two_clusters(dir.path(), pkis(dir.path(), false), [false, false]).await;
    let (we, ww) = (Wire { side: &east }, Wire { side: &west });

    // Open the MREC gate on each cluster: wait for the era, then finalize 1 -> 2 -> 3.
    for (w, name) in [(&we, "east"), (&ww, "west")] {
        converge(&format!("{name}: version era"), 60, || async {
            w.side.node.features().era_active().then_some(())
        })
        .await;
        for (from, to) in [(1, 2), (2, 3)] {
            converge(&format!("{name}: finalize to {to}"), 60, || async {
                let (st, _) = w
                    .admin_post(
                        "/admin/cluster-version/finalize",
                        &format!(r#"{{"to":{to},"expected":{from}}}"#),
                    )
                    .await;
                (st == 200).then_some(())
            })
            .await;
        }
        converge(&format!("{name}: MrecReplication gate"), 60, || async {
            w.side
                .node
                .features()
                .is_open(Gate::MrecReplication)
                .then_some(())
        })
        .await;
    }

    let table = "mrec_real";
    let create = format!(
        r#"{{"TableName":"{table}","KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],"AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    converge("east CreateTable", 60, || async {
        (we.dynamo("CreateTable", &create).await.0 == 200).then_some(())
    })
    .await;
    // Pre-existing rows must be copied by the initial scan.
    converge("east pre-existing put", 60, || async {
        let (st, _) = we
            .dynamo(
                "PutItem",
                &format!(
                    r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"pre"}},"v":{{"S":"old"}}}}}}"#
                ),
            )
            .await;
        (st == 200).then_some(())
    })
    .await;

    let (st, body) = we
        .dynamo(
            "UpdateTable",
            &format!(r#"{{"TableName":"{table}","ReplicaUpdates":[{{"Create":{{"RegionName":"west"}}}}]}}"#),
        )
        .await;
    assert_eq!(st, 200, "UpdateTable Create west: {body}");

    // The saga creates the peer table, copies, and both sides go ACTIVE.
    converge("the pre-existing row reached west", 120, || async {
        (ww.get(table, "pre").await.as_deref() == Some("old")).then_some(())
    })
    .await;
    converge("both replicas ACTIVE on east", 120, || async {
        let (_, b) = we
            .dynamo("DescribeTable", &format!(r#"{{"TableName":"{table}"}}"#))
            .await;
        let v: serde_json::Value = serde_json::from_str(&b).ok()?;
        let reps = v["Table"]["Replicas"].as_array()?;
        (reps.len() == 2 && reps.iter().all(|r| r["ReplicaStatus"] == "ACTIVE")).then_some(())
    })
    .await;

    // Steady state, both directions.
    we.put(table, "from-east", "e").await;
    ww.put(table, "from-west", "w").await;
    converge("east's write on west", 60, || async {
        (ww.get(table, "from-east").await.as_deref() == Some("e")).then_some(())
    })
    .await;
    converge("west's write on east", 60, || async {
        (we.get(table, "from-west").await.as_deref() == Some("w")).then_some(())
    })
    .await;

    // The operator view names the MREC table, its replicas and the shipper.
    let (st, body) = we.admin_get("/admin/global-tables").await;
    assert_eq!(st, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).expect("json");
    let t = v["tables"]
        .as_array()
        .and_then(|a| a.iter().find(|t| t["table"] == table))
        .unwrap_or_else(|| panic!("no MREC row: {body}"));
    assert_eq!(t["consistency"], "EVENTUAL", "{body}");
    assert_eq!(t["replica_status"]["west"], "ACTIVE", "{body}");

    east.node.shutdown_graceful().await;
    west.node.shutdown_graceful().await;
}
