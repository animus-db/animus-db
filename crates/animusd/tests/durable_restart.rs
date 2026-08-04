//! Data-plane durability across a process restart.
//!
//! With the data replica backed by the on-disk `LsmEngine` (the `animusd`
//! default), a value written through the client API survives the node being
//! stopped and re-created on the **same data directory and the same addresses**
//! — the engine recovers its state from disk on reopen. This mirrors a real
//! `animusd` process being stopped and restarted.
//!
//! The incarnations run in the **same tokio runtime**: between them,
//! [`Node::shutdown`] aborts the node's spawned protocols (Raft driver, replica
//! serve loop, the internal accept loops) and its client/dynamo/cql listeners,
//! freeing all six listener ports so the replacement can rebind the same
//! addresses — a clean teardown → rebind → recover cycle. (Before `shutdown`
//! existed, dropping a `Node` left those detached tasks running, so the test had
//! to spin up a fresh runtime per incarnation to abort them.) Like the other
//! `animusd` tests this uses real TCP/time and is non-deterministic by design
//! (the ProdEnv edge).

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, StorageBackend};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect to node");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    animusd::read_frame(&mut stream)
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

/// A single-node config pinned to fixed addresses, so the same config can start,
/// stop, and restart the same node.
fn single_node_config() -> ClusterConfig {
    let a = fixed_addrs(6);
    ClusterConfig {
        nodes: vec![RoleAddrs {
            control: a[0],
            client: a[1],
            dynamo: a[2],
            cql: a[3],
            raftkv: a[4],
            admin: a[5],
        }],
    }
}

/// Start a single node, retrying with **fresh ephemeral ports** on a bind/startup
/// failure. `fixed_addrs` binds `:0`, reads the addr, then drops the listener —
/// under `cargo test --workspace` (many test binaries in parallel) another binder
/// can steal a freed port in that TOCTOU window, so the subsequent `run_node`
/// rebind intermittently fails with `AddrInUse`. Retrying with a brand-new config
/// makes the first bring-up robust. Returns the started `Node` **and** the
/// `ClusterConfig` it actually bound, so the restart can reuse the same addresses
/// (its reuse window is tiny and acceptable).
async fn start_single_node(dir: &Path, backend: StorageBackend) -> (Node, ClusterConfig) {
    let mut last_err = None;
    for attempt in 0..10 {
        let config = single_node_config();
        match animusd::run_node_with(&config, 0, dir, backend).await {
            Ok(node) => return (node, config),
            Err(e) => {
                last_err = Some(e);
                sleep(Duration::from_millis(50 * (attempt + 1))).await;
            }
        }
    }
    panic!("single node failed to start after 10 attempts: {last_err:?}");
}

/// Stop a node cleanly and give the OS a moment to release its now-aborted
/// listeners' ports, so the replacement can rebind the same addresses.
async fn stop(node: Node) {
    node.shutdown();
    drop(node);
    sleep(Duration::from_millis(200)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn data_survives_node_restart_on_disk() {
    let dir = TempDir::new().unwrap();
    let node_dir = dir.path().join("node-0");

    // --- First incarnation: write a durable key, then shut down cleanly. ---
    let (node, config) = start_single_node(&node_dir, StorageBackend::default()).await;
    let client = config.nodes[0].client;
    await_bootstrap(&node).await;

    let put = call(
        client,
        ClientRequest::Put {
            key: b"durable".to_vec(),
            value: b"survives".to_vec(),
            table: None,
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
                key: b"durable".to_vec(),
                table: None,
            }
        )
        .await,
        ClientResponse::Value(Some(b"survives".to_vec())),
    );

    stop(node).await;

    // --- Second incarnation: SAME runtime, SAME data dir + SAME addresses. ---
    // The clean shutdown above freed the ports, so the replacement rebinds them.
    let node = animusd::run_node(&config, 0, &node_dir)
        .await
        .expect("restart on the same dir/addresses after a clean shutdown");
    await_bootstrap(&node).await;

    // The previously-written value survived because the LSM recovered it from
    // disk — the whole point of the on-disk data plane.
    let got = call(
        client,
        ClientRequest::Get {
            key: b"durable".to_vec(),
            table: None,
        },
    )
    .await;
    assert_eq!(
        got,
        ClientResponse::Value(Some(b"survives".to_vec())),
        "durable key did not survive the restart (got {got:?})",
    );

    // A key that was never written stays absent across the restart — the engine
    // recovered exactly what was durably committed, nothing more.
    let absent = call(
        client,
        ClientRequest::Get {
            key: b"never".to_vec(),
            table: None,
        },
    )
    .await;
    assert_eq!(
        absent,
        ClientResponse::Value(None),
        "an unwritten key should be absent after recovery (got {absent:?})",
    );

    stop(node).await;
}

/// The contrast: with the `--ephemeral` in-memory backend, a restart on the same
/// dir/addresses starts empty — proving the durability above comes from the
/// on-disk engine, not from anything else in the stack (the control plane's own
/// WAL recovers metadata either way).
#[tokio::test(flavor = "multi_thread")]
async fn data_is_lost_on_restart_with_memory_backend() {
    let dir = TempDir::new().unwrap();
    let node_dir = dir.path().join("node-0");

    let (node, config) = start_single_node(&node_dir, StorageBackend::Memory).await;
    let client = config.nodes[0].client;
    await_bootstrap(&node).await;

    let put = call(
        client,
        ClientRequest::Put {
            key: b"volatile".to_vec(),
            value: b"gone".to_vec(),
            table: None,
        },
    )
    .await;
    assert!(matches!(put, ClientResponse::PutOk), "put failed: {put:?}");

    stop(node).await;

    let node = animusd::run_node_with(&config, 0, &node_dir, StorageBackend::Memory)
        .await
        .expect("restart (memory)");
    await_bootstrap(&node).await;

    // The in-memory replica started empty, so the value is gone.
    let got = call(
        client,
        ClientRequest::Get {
            key: b"volatile".to_vec(),
            table: None,
        },
    )
    .await;
    assert_eq!(
        got,
        ClientResponse::Value(None),
        "in-memory backend should lose data across a restart (got {got:?})",
    );

    stop(node).await;
}
