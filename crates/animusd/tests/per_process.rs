//! Per-process deployment: nodes started independently from a shared config
//! (via `run_node`, the same entry point the `--config --node I` binary uses)
//! form one cluster and serve clients. This mirrors running one `animusd`
//! process per node, but in-process so the test needs no child processes.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, read_frame};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect to node");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
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

/// Reserve `count` free TCP ports on loopback (bind to :0, read the addr, then
/// release). A small reuse race, acceptable for a test.
fn free_addrs(count: usize) -> Vec<SocketAddr> {
    let listeners: Vec<std::net::TcpListener> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
    // listeners dropped here, freeing the ports for the nodes to bind.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn per_process_nodes_form_a_cluster_from_shared_config() {
    let n = 3;
    let addrs = free_addrs(n * 6);
    let nodes_cfg: Vec<RoleAddrs> = (0..n)
        .map(|i| RoleAddrs {
            control: addrs[6 * i],
            client: addrs[6 * i + 1],
            dynamo: addrs[6 * i + 2],
            cql: addrs[6 * i + 3],
            raftkv: addrs[6 * i + 4],
            admin: addrs[6 * i + 5],
        })
        .collect();
    let config = ClusterConfig { nodes: nodes_cfg };

    // The config round-trips through JSON exactly as it would on disk between
    // processes.
    let config = ClusterConfig::from_json(&config.to_json()).unwrap();

    // Start each node independently, as separate processes would.
    let dir = tempfile::tempdir().unwrap();
    let mut nodes = Vec::new();
    for i in 0..n {
        let node = animusd::run_node(&config, i, dir.path().join(format!("node-{i}")))
            .await
            .unwrap_or_else(|e| panic!("node {i} failed to start: {e}"));
        nodes.push(node);
    }

    await_bootstrap(&nodes).await;

    // Clients connect to the configured client addresses (not the bound handle).
    let client0 = config.nodes[0].client;
    let client1 = config.nodes[1].client;
    let client2 = config.nodes[2].client;

    match call(client0, ClientRequest::Status).await {
        ClientResponse::Status(meta) => {
            assert_eq!(meta.members.len(), 3);
            // ADR 0023: no data tablet until the first write provisions one.
            assert_eq!(meta.tablets.len(), 0);
        }
        other => panic!("unexpected status: {other:?}"),
    }

    let put = call(
        client0,
        ClientRequest::Put {
            key: b"k".to_vec(),
            value: b"v1".to_vec(),
            table: Some("kv".to_string()),
        },
    )
    .await;
    assert!(matches!(put, ClientResponse::PutOk), "put failed: {put:?}");

    // Read back and cross-node overwrite, just like the in-process cluster test.
    assert_eq!(
        call(
            client1,
            ClientRequest::Get {
                key: b"k".to_vec(),
                table: Some("kv".to_string())
            }
        )
        .await,
        ClientResponse::Value(Some(b"v1".to_vec()))
    );
    let put2 = call(
        client2,
        ClientRequest::Put {
            key: b"k".to_vec(),
            value: b"v2".to_vec(),
            table: Some("kv".to_string()),
        },
    )
    .await;
    assert!(
        matches!(put2, ClientResponse::PutOk),
        "overwrite failed: {put2:?}"
    );
    assert_eq!(
        call(
            client0,
            ClientRequest::Get {
                key: b"k".to_vec(),
                table: Some("kv".to_string())
            }
        )
        .await,
        ClientResponse::Value(Some(b"v2".to_vec()))
    );
}
