//! Per-process deployment: nodes started independently from a shared config
//! (via `run_node`, the same entry point the `--config --node I` binary uses)
//! form one cluster and serve clients. This mirrors running one `animusd`
//! process per node, but in-process so the test needs no child processes.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, read_frame};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect to node");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// A put with a bounded retry on ANY `ClientResponse::Error` reply: a put is
/// idempotent, and both early-cluster-formation transients and the
/// documented "not the leader here"/futility-retry shapes surface as a
/// clean, retryable error (see `docs/engineering-lessons.md`'s "CP
/// write-forward path has no retry-on-not-the-leader-here" entry).
async fn put_retry(addr: SocketAddr, key: &[u8], value: &[u8]) -> ClientResponse {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let resp = call(
            addr,
            ClientRequest::Put {
                key: key.to_vec(),
                value: value.to_vec(),
                table: "kv".to_string(),
            },
        )
        .await;
        match resp {
            ClientResponse::PutOk => return resp,
            ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(150)).await;
            }
            other => return other,
        }
    }
}

async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not bootstrap in 20s");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn per_process_nodes_form_a_cluster_from_shared_config() {
    let n = 3;
    // Start each node independently, as separate processes would — wrapped in the
    // documented **port-TOCTOU retry** (`free_addrs` releases the probed ports
    // before `run_node` rebinds them, so a concurrent test binary can steal one;
    // re-allocate fresh ports and retry the bring-up as a unit).
    let dir = tempfile::tempdir().unwrap();
    let mut brought_up = None;
    'attempts: for attempt in 0..16 {
        let addrs = support::free_addrs(n * 7);
        let nodes_cfg: Vec<RoleAddrs> = (0..n)
            .map(|i| RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[7 * i],
                client: addrs[7 * i + 1],
                dynamo: addrs[7 * i + 2],
                cql: addrs[7 * i + 3],
                admin: addrs[7 * i + 4],
                intra: addrs[7 * i + 5],
                console: addrs[7 * i + 6],
            })
            .collect();
        let config = ClusterConfig { nodes: nodes_cfg };

        // The config round-trips through JSON exactly as it would on disk between
        // processes.
        let config = ClusterConfig::from_json(&config.to_json()).unwrap();

        let mut nodes = Vec::new();
        for i in 0..n {
            match animusd::run_node(&config, i, dir.path().join(format!("node-{attempt}-{i}")))
                .await
            {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    for node in &nodes {
                        node.shutdown_graceful().await;
                    }
                    sleep(Duration::from_millis(50)).await;
                    continue 'attempts;
                }
            }
        }
        brought_up = Some((nodes, config));
        break;
    }
    let (nodes, config) =
        brought_up.expect("could not bring up cluster after retries (ports kept getting stolen)");

    await_bootstrap(&nodes).await;

    // Clients connect to the configured client addresses (not the bound handle).
    let client0 = config.nodes[0].client;
    let client1 = config.nodes[1].client;
    let client2 = config.nodes[2].client;

    match call(client0, ClientRequest::Status).await {
        ClientResponse::Status { metadata: meta, .. } => {
            assert_eq!(meta.members.len(), 3);
            // ADR 0023: no data tablet until the first write provisions one.
            assert_eq!(meta.tablets.len(), 0);
        }
        other => panic!("unexpected status: {other:?}"),
    }

    let put = put_retry(client0, b"k", b"v1").await;
    assert!(matches!(put, ClientResponse::PutOk), "put failed: {put:?}");

    // Read back and cross-node overwrite, just like the in-process cluster test.
    assert_eq!(
        call(
            client1,
            ClientRequest::Get {
                key: b"k".to_vec(),
                table: "kv".to_string()
            }
        )
        .await,
        ClientResponse::Value(Some(b"v1".to_vec()))
    );
    let put2 = put_retry(client2, b"k", b"v2").await;
    assert!(
        matches!(put2, ClientResponse::PutOk),
        "overwrite failed: {put2:?}"
    );
    assert_eq!(
        call(
            client0,
            ClientRequest::Get {
                key: b"k".to_vec(),
                table: "kv".to_string()
            }
        )
        .await,
        ClientResponse::Value(Some(b"v2".to_vec()))
    );
}
