//! Emulated-WAN, real-process MREC measurement (ADR 0075 G-d known gap "no
//! WAN latency/bandwidth number"; G-01).
//!
//! Two single-node `animusd` clusters ("east", "west") with mutual TLS and
//! different CAs, exactly as in `mrec_peer_transport.rs`, except that each
//! side's configured **peer endpoint is a [`wan_proxy::WanProxy`]** in front of
//! the other side's intra port. Every cross-region byte (the `MrecApply`
//! frames, the saga's calls, the TLS handshakes) therefore pays a configurable
//! one-way delay (default 40 ms, so an 80 ms RTT) and an optional bandwidth cap.
//!
//! The test creates an MREC table on east, adds the west replica, then runs a
//! paced write workload on both sides at once and asserts:
//! - every acked write becomes readable, with its value, on the other region;
//! - concurrent writes to the same keys from both regions converge to the
//!   same (last-writer-wins) value on both sides;
//! - structural bounds only (no tight latency thresholds): replication lag is
//!   at least half the one-way delay (the proxy really is on the path) and
//!   below a generous ceiling; both proxy directions carried bytes.
//!
//! It **prints** the replication-lag percentiles (ack in region A to
//! visibility in region B, measured by a 20 ms poller, so resolution is about
//! 20 ms plus a TLS request) and the bytes shipped per write (proxy byte
//! counters, TLS framing included, minus the measured idle baseline).
//! `ANIMUS_WAN_ONE_WAY_MS`, `ANIMUS_WAN_JITTER_MS`, `ANIMUS_WAN_MBPS` (megabit/s
//! cap, 0 = none) and `ANIMUS_WAN_WRITES` (per side) change the link and size.
//!
//! **This is an emulated WAN on loopback, not a real WAN**: no loss, no
//! reordering, no congestion control, one host. Run with `--nocapture` to
//! see the numbers.

use std::path::Path;
use std::time::Duration;

use animus_control::version::Gate;
use animusd::config::{ClusterSettings, PeerCluster, TlsSection};
use animusd::{ClusterConfig, Node, RoleAddrs};
use tokio::time::Instant;

mod support;
mod wan_proxy;
use wan_proxy::{WanLink, WanProxy};

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

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn config(
    own: RoleAddrs,
    region: &str,
    peer_region: &str,
    peer_endpoint: std::net::SocketAddr,
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
                endpoints: vec![peer_endpoint.to_string()],
                tls_ca: peer_ca,
            }],
            ..Default::default()
        }),
    }
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

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

fn summarize(label: &str, lags: &mut [Duration]) -> (Duration, Duration) {
    lags.sort();
    let (min, p50, p95, p99, max) = (
        lags[0],
        percentile(lags, 0.50),
        percentile(lags, 0.95),
        percentile(lags, 0.99),
        lags[lags.len() - 1],
    );
    println!(
        "WAN-EMU {label}: n={} lag min={min:?} p50={p50:?} p95={p95:?} p99={p99:?} max={max:?}",
        lags.len()
    );
    (min, max)
}

/// Writes `prefix-0..n` to `from`, paced, sending each ack time to the poller.
async fn write_side(
    from: &Wire<'_>,
    table: &str,
    prefix: &str,
    n: usize,
    pace: Duration,
    tx: tokio::sync::mpsc::UnboundedSender<(String, String, Instant)>,
) {
    for i in 0..n {
        let (k, v) = (format!("{prefix}-{i}"), format!("{prefix}v{i}"));
        from.put(table, &k, &v).await;
        let _ = tx.send((k, v, Instant::now()));
        tokio::time::sleep(pace).await;
    }
}

/// For each acked write (in order) polls `to` until it is visible; returns lags.
async fn watch_side(
    to: &Wire<'_>,
    table: &str,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<(String, String, Instant)>,
) -> Vec<Duration> {
    let mut lags = Vec::new();
    while let Some((k, v, acked)) = rx.recv().await {
        converge(
            &format!("acked write {k} visible on the other region"),
            90,
            || async { (to.get(table, &k).await.as_deref() == Some(v.as_str())).then_some(()) },
        )
        .await;
        lags.push(acked.elapsed());
    }
    lags
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn emulated_wan_two_real_clusters_converge_and_report_lag_and_bytes() {
    let one_way = env_u64("ANIMUS_WAN_ONE_WAY_MS", 40);
    let mbps = env_u64("ANIMUS_WAN_MBPS", 0);
    let writes = env_u64("ANIMUS_WAN_WRITES", 40) as usize;
    let link = WanLink {
        one_way: Duration::from_millis(one_way),
        jitter: Duration::from_millis(env_u64("ANIMUS_WAN_JITTER_MS", 0)),
        seed: 0x5eed_0001,
        bytes_per_sec: (mbps > 0).then(|| mbps * 125_000),
    };
    let dir = support::panic_safe_tempdir();
    let [tls_e, tls_w] = pkis(dir.path(), false);
    let east = Node::bind(
        animusd::config::node_id(0),
        RoleAddrs {
            tls: tls_e.clone(),
            ..support::unbound_role_addrs(0)
        },
        dir.path().join("east"),
    )
    .await
    .expect("bind east");
    let west = Node::bind(
        animusd::config::node_id(0),
        RoleAddrs {
            tls: tls_w.clone(),
            ..support::unbound_role_addrs(0)
        },
        dir.path().join("west"),
    )
    .await
    .expect("bind west");
    // East dials west through `to_west`; west dials east through `to_east`.
    let to_west = WanProxy::start(west.intra_addr(), link).await;
    let to_east = WanProxy::start(east.intra_addr(), link).await;
    let ce = config(role(&east, tls_e), "east", "west", to_west.addr());
    let cw = config(role(&west, tls_w), "west", "east", to_east.addr());
    let east = Side {
        node: animusd::run_bound_node(east, &ce, 0)
            .await
            .expect("start east"),
        config: ce,
    };
    let west = Side {
        node: animusd::run_bound_node(west, &cw, 0)
            .await
            .expect("start west"),
        config: cw,
    };
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

    let table = "mrec_wan";
    let create = format!(
        r#"{{"TableName":"{table}","KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],"AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    converge("east CreateTable", 60, || async {
        (we.dynamo("CreateTable", &create).await.0 == 200).then_some(())
    })
    .await;
    let (st, body) = we
        .dynamo(
            "UpdateTable",
            &format!(r#"{{"TableName":"{table}","ReplicaUpdates":[{{"Create":{{"RegionName":"west"}}}}]}}"#),
        )
        .await;
    assert_eq!(st, 200, "UpdateTable Create west: {body}");
    converge("both replicas ACTIVE on east", 180, || async {
        let (_, b) = we
            .dynamo("DescribeTable", &format!(r#"{{"TableName":"{table}"}}"#))
            .await;
        let v: serde_json::Value = serde_json::from_str(&b).ok()?;
        let reps = v["Table"]["Replicas"].as_array()?;
        (reps.len() == 2 && reps.iter().all(|r| r["ReplicaStatus"] == "ACTIVE")).then_some(())
    })
    .await;
    // The replica table exists on west: wait until a probe write crosses.
    we.put(table, "probe", "p").await;
    converge("probe row reached west", 120, || async {
        (ww.get(table, "probe").await.as_deref() == Some("p")).then_some(())
    })
    .await;

    // Idle baseline: the MREC loops' empty ships, heartbeats and handshakes.
    let total = || to_west.stats().total_bytes() + to_east.stats().total_bytes();
    let idle_from = total();
    let idle_t = Instant::now();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let idle_rate = (total() - idle_from) as f64 / idle_t.elapsed().as_secs_f64();

    // Workload: both regions at once, paced, plus conflicting keys.
    let pace = Duration::from_millis(100);
    let (txe, rxe) = tokio::sync::mpsc::unbounded_channel();
    let (txw, rxw) = tokio::sync::mpsc::unbounded_channel();
    let bytes0 = total();
    let t0 = Instant::now();
    let (_, _, lags_e_to_w, lags_w_to_e) = tokio::join!(
        write_side(&we, table, "e", writes, pace, txe),
        write_side(&ww, table, "w", writes, pace, txw),
        watch_side(&ww, table, rxe),
        watch_side(&we, table, rxw),
    );
    let (mut lags_e_to_w, mut lags_w_to_e) = (lags_e_to_w, lags_w_to_e);
    assert_eq!((lags_e_to_w.len(), lags_w_to_e.len()), (writes, writes));

    // Conflicting writes: both regions write the same keys at once; the
    // survivors must agree (LWW per key), and be one of the two writes.
    let hot = 8;
    for j in 0..hot {
        let k = format!("hot-{j}");
        tokio::join!(
            we.put(table, &k, "from-east"),
            ww.put(table, &k, "from-west")
        );
    }
    for j in 0..hot {
        let k = format!("hot-{j}");
        converge(&format!("{k}: both regions agree"), 90, || async {
            let (a, b) = (we.get(table, &k).await?, ww.get(table, &k).await?);
            (a == b && (a == "from-east" || a == "from-west")).then_some(())
        })
        .await;
    }
    let work_secs = t0.elapsed().as_secs_f64();
    let wan_bytes = total() - bytes0;
    let n_writes = (2 * writes + 2 * hot) as f64;
    let idle_bytes = idle_rate * work_secs;
    let net = (wan_bytes as f64 - idle_bytes).max(0.0);

    println!(
        "WAN-EMU link: one_way={one_way}ms jitter={}ms cap={} (emulated WAN on loopback, not a real WAN)",
        link.jitter.as_millis(),
        if mbps == 0 {
            "none".to_string()
        } else {
            format!("{mbps} Mbit/s")
        }
    );
    let (min_a, max_a) = summarize("east->west", &mut lags_e_to_w);
    let (min_b, max_b) = summarize("west->east", &mut lags_w_to_e);
    println!(
        "WAN-EMU bytes: workload window {work_secs:.1}s, {n_writes} writes ({} per side + {hot} conflicting keys each side); \
         WAN bytes (both directions, TLS framing included) total={wan_bytes} idle-baseline={idle_bytes:.0} net={net:.0} => {:.0} B/write net, {:.0} B/write gross; \
         east-dialled fwd={} back={}, west-dialled fwd={} back={}",
        writes,
        net / n_writes,
        wan_bytes as f64 / n_writes,
        to_west.stats().forward_bytes(),
        to_west.stats().back_bytes(),
        to_east.stats().forward_bytes(),
        to_east.stats().back_bytes(),
    );

    // Structural bounds only.
    let floor = Duration::from_millis(one_way / 2);
    assert!(
        min_a >= floor && min_b >= floor,
        "lag below half the one-way delay: the proxy is not on the path ({min_a:?}/{min_b:?})"
    );
    let ceiling = Duration::from_secs(60);
    assert!(
        max_a < ceiling && max_b < ceiling,
        "lag {max_a:?}/{max_b:?}"
    );
    for (name, p) in [("east-dialled", &to_west), ("west-dialled", &to_east)] {
        assert!(
            p.stats().forward_bytes() > 0 && p.stats().back_bytes() > 0,
            "{name} proxy carried no traffic"
        );
    }

    // The operator view names the MREC table and its ACTIVE west replica.
    let (st, body) = we.admin_get("/admin/global-tables").await;
    assert_eq!(st, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).expect("json");
    let row = v["tables"]
        .as_array()
        .and_then(|a| a.iter().find(|t| t["table"] == table))
        .unwrap_or_else(|| panic!("no MREC row: {body}"));
    assert_eq!(row["replica_status"]["west"], "ACTIVE", "{body}");

    east.node.shutdown_graceful().await;
    west.node.shutdown_graceful().await;
}
