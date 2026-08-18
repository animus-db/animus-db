//! Data-plane durability across a process restart.
//!
//! With the data replica backed by the on-disk `LsmEngine` (the `animusd`
//! default), a value written through the client API survives the node being
//! stopped and re-created on the **same data directory and the same addresses**
//! — the engine recovers its state from disk on reopen. This mirrors a real
//! `animusd` process being stopped and restarted.
//!
//! The incarnations run in the **same tokio runtime**: between them,
//! [`Node::shutdown_graceful`] cooperatively drains the node's spawned protocols
//! (Raft driver, replica serve loop, the internal accept loops) and its
//! client/dynamo/cql listeners, freeing all six listener ports so the
//! replacement can rebind the same addresses — a clean teardown → rebind →
//! recover cycle. (Before `shutdown` existed, dropping a `Node` left those
//! detached tasks running, so the test had to spin up a fresh runtime per
//! incarnation to abort them; a bare `shutdown` frees the ports but doesn't wait
//! for the driver tasks to actually stop, so a same-address restart needs the
//! graceful/awaited form — see `animusd/CLAUDE.md`'s `Node::shutdown()` entry.)
//! Like the other
//! `animusd` tests this uses real TCP/time and is non-deterministic by design
//! (the ProdEnv edge).

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, StorageBackend};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

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
            if node.is_control_leader() && !node.metadata().members.is_empty() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("node did not bootstrap in 20s");
}

/// Stop a node cleanly and give the OS a moment to release its now-aborted
/// listeners' ports, so the replacement can rebind the same addresses.
async fn stop(node: Node) {
    node.shutdown_graceful().await;
    drop(node);
    sleep(Duration::from_millis(200)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn data_survives_node_restart_on_disk() {
    let dir = TempDir::new().unwrap();
    let node_dir = dir.path().join("node-0");

    // --- First incarnation: write a durable key, then shut down cleanly. ---
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let client = config.nodes[0].client;
    await_bootstrap(&node).await;

    let put = call(
        client,
        ClientRequest::Put {
            key: b"durable".to_vec(),
            value: b"survives".to_vec(),
            table: "kv".to_string(),
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
                table: "kv".to_string(),
            }
        )
        .await,
        ClientResponse::Value(Some(b"survives".to_vec())),
    );

    stop(node).await;

    // --- Second incarnation: SAME runtime, SAME data dir + SAME addresses. ---
    // The clean shutdown above freed the ports; the retried rebind rides out a
    // concurrent test binary's momentary port probe (see `support`).
    let node = support::restart_same_addrs(&config, 0, &node_dir, StorageBackend::default()).await;
    await_bootstrap(&node).await;

    // The previously-written value survived because the LSM recovered it from
    // disk — the whole point of the on-disk data plane.
    let got = call(
        client,
        ClientRequest::Get {
            key: b"durable".to_vec(),
            table: "kv".to_string(),
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
            table: "kv".to_string(),
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

/// Even with the `--ephemeral` in-memory **engine**, an acked write survives a
/// clean restart on the same data dir: the CP group's Raft WAL (`raftkv.wal`) is
/// fsynced on the real disk *before* the ack, and the restarted sole leader
/// re-advances commit over its recovered log tail on re-election (its election
/// no-op commits immediately in a single-node group), re-applying the tail into
/// the fresh engine. The engine backend only decides whether the *engine image*
/// is durable — durability of acked writes comes from the Raft WAL either way.
///
/// (Historically this test asserted the opposite — that the memory backend loses
/// the value — but that relied on a consensus gap: a restarted single-voter
/// group never re-advanced commit over its recovered WAL tail until the *next*
/// propose, silently violating `RaftCore::recovered`'s re-apply contract and
/// leaving acked-but-unre-applied state invisible. The ReadIndex §6.4
/// current-term-commit gate fix closed that gap.)
///
/// Recovery → election → re-apply is asynchronous after the restart, so the
/// read is a converged-or-timeout poll, not a one-shot assert.
#[tokio::test(flavor = "multi_thread")]
async fn acked_write_survives_memory_backend_restart_via_raft_wal() {
    let dir = TempDir::new().unwrap();
    let node_dir = dir.path().join("node-0");

    let (node, config) = support::start_single_node(&node_dir, StorageBackend::Memory).await;
    let client = config.nodes[0].client;
    await_bootstrap(&node).await;

    let put = call(
        client,
        ClientRequest::Put {
            key: b"acked".to_vec(),
            value: b"survives".to_vec(),
            table: "kv".to_string(),
        },
    )
    .await;
    assert!(matches!(put, ClientResponse::PutOk), "put failed: {put:?}");

    stop(node).await;

    let node = support::restart_same_addrs(&config, 0, &node_dir, StorageBackend::Memory).await;
    await_bootstrap(&node).await;

    // Poll until the recovered group has re-applied its WAL tail (bounded).
    let recovered = async {
        loop {
            let got = call(
                client,
                ClientRequest::Get {
                    key: b"acked".to_vec(),
                    table: "kv".to_string(),
                },
            )
            .await;
            if got == ClientResponse::Value(Some(b"survives".to_vec())) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), recovered)
        .await
        .expect("acked write did not survive the memory-backend restart via the raftkv WAL");

    stop(node).await;
}
