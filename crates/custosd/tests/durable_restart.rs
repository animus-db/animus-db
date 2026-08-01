//! Data-plane durability across a process restart.
//!
//! With the data replica backed by the on-disk `LsmEngine` (the `custosd`
//! default), a value written through the client API survives the node being
//! stopped and re-created on the **same data directory and the same addresses**
//! — the engine recovers its state from disk on reopen. This mirrors a real
//! `custosd` process being stopped and restarted.
//!
//! Each "incarnation" runs in its **own tokio runtime**: the node's protocols
//! are detached background tasks (Raft, the replica serve loop, the listeners'
//! accept loops), and dropping a `Node` does not stop them. Shutting the runtime
//! down between incarnations aborts those tasks and releases the listener ports,
//! standing in for the OS reclaiming a stopped process — so the replacement can
//! rebind the same addresses. Like the other `custosd` tests this uses real
//! TCP/time and is non-deterministic by design (the ProdEnv edge).

use std::net::SocketAddr;
use std::time::Duration;

use custosd::{ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, StorageBackend};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio::time::{sleep, timeout};

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect to node");
    custosd::write_frame(&mut stream, &req).await.expect("send");
    custosd::read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

async fn await_bootstrap(node: &Node) {
    let ready = async {
        loop {
            if node.is_control_leader() && !node.metadata().tablets.is_empty() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("node did not bootstrap in 20s");
}

/// Reserve `count` free TCP ports on loopback (bind to :0, read the addr, then
/// release). The restarted node must rebind these exact addresses, which is the
/// point of the test.
fn fixed_addrs(count: usize) -> Vec<SocketAddr> {
    let listeners: Vec<std::net::TcpListener> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
    // listeners dropped here, freeing the ports for the node to bind.
}

/// A single-node config (R = W = 1) pinned to fixed addresses, so the same
/// config can start, stop, and restart the same node.
fn single_node_config() -> ClusterConfig {
    let a = fixed_addrs(6);
    ClusterConfig {
        nodes: vec![RoleAddrs {
            control: a[0],
            data: a[1],
            coord: a[2],
            client: a[3],
            dynamo: a[4],
            cql: a[5],
        }],
        r: 1,
        w: 1,
    }
}

/// Run one node "incarnation" to completion in its own multi-thread runtime,
/// then shut the runtime down (aborting the node's detached tasks and releasing
/// its listener ports), standing in for an OS process restart.
fn incarnation<F, Fut>(body: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let rt = Runtime::new().expect("build runtime");
    rt.block_on(body());
    // Force-abort the node's background tasks so the ports are free for the next
    // incarnation to rebind (mirrors the OS reclaiming a stopped process).
    rt.shutdown_timeout(Duration::from_secs(5));
}

#[test]
fn data_survives_node_restart_on_disk() {
    let config = single_node_config();
    let client = config.nodes[0].client;
    let dir = TempDir::new().unwrap();
    let node_dir = dir.path().join("node-0");

    // --- First incarnation: write a durable key. ---
    incarnation(|| async {
        let node = custosd::run_node(&config, 0, &node_dir)
            .await
            .expect("first start");
        await_bootstrap(&node).await;

        let put = call(
            client,
            ClientRequest::Put {
                key: b"durable".to_vec(),
                value: b"survives".to_vec(),
            },
        )
        .await;
        assert!(matches!(put, ClientResponse::PutOk), "put failed: {put:?}");

        // A returned PutOk means the on-disk LSM WAL-synced the write before
        // acking, so it is durable. Confirm it reads back while still up.
        assert_eq!(
            call(
                client,
                ClientRequest::Get {
                    key: b"durable".to_vec()
                }
            )
            .await,
            ClientResponse::Value(Some(b"survives".to_vec())),
        );
    });

    // --- Second incarnation: SAME data dir + SAME addresses. ---
    incarnation(|| async {
        let node = custosd::run_node(&config, 0, &node_dir)
            .await
            .expect("restart on the same dir/addresses");
        await_bootstrap(&node).await;

        // The previously-written value survived because the LSM recovered it
        // from disk — the whole point of the on-disk data plane.
        let got = call(
            client,
            ClientRequest::Get {
                key: b"durable".to_vec(),
            },
        )
        .await;
        assert_eq!(
            got,
            ClientResponse::Value(Some(b"survives".to_vec())),
            "durable key did not survive the restart (got {got:?})",
        );

        // A key that was never written stays absent across the restart — the
        // engine recovered exactly what was durably committed, nothing more.
        let absent = call(
            client,
            ClientRequest::Get {
                key: b"never".to_vec(),
            },
        )
        .await;
        assert_eq!(
            absent,
            ClientResponse::Value(None),
            "an unwritten key should be absent after recovery (got {absent:?})",
        );
    });
}

/// The contrast: with the `--ephemeral` in-memory backend, a restart on the same
/// dir/addresses starts empty — proving the durability above comes from the
/// on-disk engine, not from anything else in the stack (the control plane's own
/// WAL recovers metadata either way).
#[test]
fn data_is_lost_on_restart_with_memory_backend() {
    let config = single_node_config();
    let client = config.nodes[0].client;
    let dir = TempDir::new().unwrap();
    let node_dir = dir.path().join("node-0");

    incarnation(|| async {
        let node = custosd::run_node_with(&config, 0, &node_dir, StorageBackend::Memory)
            .await
            .expect("first start (memory)");
        await_bootstrap(&node).await;

        let put = call(
            client,
            ClientRequest::Put {
                key: b"volatile".to_vec(),
                value: b"gone".to_vec(),
            },
        )
        .await;
        assert!(matches!(put, ClientResponse::PutOk), "put failed: {put:?}");
    });

    incarnation(|| async {
        let node = custosd::run_node_with(&config, 0, &node_dir, StorageBackend::Memory)
            .await
            .expect("restart (memory)");
        await_bootstrap(&node).await;

        // The in-memory replica started empty, so the value is gone.
        let got = call(
            client,
            ClientRequest::Get {
                key: b"volatile".to_vec(),
            },
        )
        .await;
        assert_eq!(
            got,
            ClientResponse::Value(None),
            "in-memory backend should lose data across a restart (got {got:?})",
        );
    });
}
