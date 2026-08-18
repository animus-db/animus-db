//! Regression for a placement-policy bug found while root-causing an
//! apparent "livelock" in `cluster_growth.rs` under heavy concurrent load
//! (see `docs/engineering-lessons.md`): `provision_tablet` used to record a
//! tablet's placement policy as `t.replicas.len()` — whatever the *initial*
//! replica set's size happened to be at creation time — rather than the
//! fixed target `MAX_REPLICATION_FACTOR`. A cluster that provisions its
//! first table before every founding member is `Active` yet (structurally
//! guaranteed here by starting a genuinely 2-node cluster, no timing race
//! needed) would get an under-sized *initial* replica set, which is
//! expected and fine — but the *policy* recording that same under-size
//! made it permanent: growing the cluster later never revisited it, since
//! `reconcile_placement` only repairs violations of the recorded policy,
//! and 2-of-2 already satisfied a policy of RF 2.
//!
//! This test provisions a table on a **2-node** cluster (so the tablet's
//! initial replica set is genuinely, unavoidably sized 2 — not a race to
//! win), then grows to 3 nodes and asserts the tablet's replica set
//! **grows to 3**. That convergence is only possible if the policy
//! recorded the *target* RF (3) rather than the *observed* initial size
//! (2) — against the unfixed code this test would hang until its timeout,
//! since `reconcile_placement` would see the 2-replica set as already
//! policy-compliant and never propose a `CasTabletReplicas` to grow it.
//!
//! Real TCP/time — polls with generous timeouts, not deterministic assertions.

use std::net::SocketAddr;
use std::time::Duration;

use animus_env::NodeId;
use animusd::{ClientRequest, ClientResponse, ClusterConfig, Node, read_frame};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, ClusterConfig) {
    support::bring_up_deadline(n, dir, support::JOIN_DEADLINE).await
}

async fn grow(
    base: &ClusterConfig,
    extra: usize,
    dir: &std::path::Path,
) -> (Vec<Node>, ClusterConfig) {
    support::grow_deadline(base, extra, dir, support::JOIN_DEADLINE).await
}

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

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed JSON)`.
async fn admin(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.0\r\n\
         Host: animus\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
    );
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
    let value: Value = serde_json::from_str(payload).expect("admin body is JSON");
    (status, value)
}

/// This table's tablet's replica set, via `/admin/status`. Panics if the
/// table has no tablet yet (callers only use this after `put` has returned).
async fn tablet_replicas(admin_addr: SocketAddr) -> Vec<NodeId> {
    let (_s, v) = admin(admin_addr, "GET", "/admin/status", None).await;
    let tablets = v["tablets"].as_object().expect("tablets is an object");
    assert_eq!(tablets.len(), 1, "expected exactly one tablet: {tablets:?}");
    let (_, t) = tablets.iter().next().expect("one tablet");
    t["replicas"]
        .as_array()
        .expect("replicas is an array")
        .iter()
        .filter_map(|r| r.as_str()?.parse::<NodeId>().ok())
        .collect()
}

async fn call(addr: SocketAddr, req: ClientRequest) -> Option<ClientResponse> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    animusd::write_frame(&mut stream, &req).await.ok()?;
    read_frame(&mut stream).await.ok()?
}

async fn put(clients: &[SocketAddr], table: &str, key: &[u8], value: &[u8], secs: u64) {
    let w = async {
        loop {
            for &c in clients {
                if let Some(ClientResponse::PutOk) = call(
                    c,
                    ClientRequest::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        table: table.to_string(),
                    },
                )
                .await
                {
                    return;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(secs), w)
        .await
        .unwrap_or_else(|_| panic!("write of {table}/{key:?} never committed"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn tablet_provisioned_undersized_on_a_small_cluster_self_heals_after_growth() {
    let dir = tempfile::tempdir().unwrap();

    // A genuinely 2-node cluster: the tablet's *initial* replica set can only
    // ever be sized 2 here — no timing race needed to construct this, it's
    // structural. `MAX_REPLICATION_FACTOR` (3) exceeds what's available.
    let (nodes, base_config) = bring_up(2, dir.path()).await;
    await_bootstrap(&nodes).await;
    let base_clients: Vec<SocketAddr> = base_config.nodes.iter().map(|a| a.client).collect();
    let base_admin: Vec<SocketAddr> = base_config.nodes.iter().map(|a| a.admin).collect();

    const TABLE: &str = "rf_self_heal";
    put(&base_clients, TABLE, b"k0", b"v0", 30).await;

    let initial = tablet_replicas(base_admin[0]).await;
    assert_eq!(
        initial.len(),
        2,
        "tablet should be provisioned with exactly the 2 available members: {initial:?}"
    );

    // Grow to 3 — a normal ADR 0030 growth node, exactly like `cluster_growth.rs`.
    let (growth_nodes, expanded_config) = grow(&base_config, 1, dir.path()).await;
    let mut nodes = nodes;
    nodes.extend(growth_nodes);
    let all_admin: Vec<SocketAddr> = expanded_config.nodes.iter().map(|a| a.admin).collect();

    // The regression: this must converge to 3 replicas. Against the unfixed
    // code (policy RF recorded as `t.replicas.len()` == 2 at creation time),
    // `reconcile_placement` sees the 2-replica set as already
    // policy-compliant and never proposes a `CasTabletReplicas` to grow it —
    // this poll would hang until its own idle-stall/backstop bound fires.
    support::poll_until_or_stalled(
        all_admin[0],
        "tablet never grew from 2 to 3 replicas after the cluster grew to 3 nodes \
         (the RF policy was likely recorded as the initial replica count, not the \
         target MAX_REPLICATION_FACTOR)",
        Duration::from_millis(200),
        || async { tablet_replicas(all_admin[0]).await.len() == 3 },
    )
    .await;

    for node in nodes {
        node.shutdown_graceful().await;
    }
}
