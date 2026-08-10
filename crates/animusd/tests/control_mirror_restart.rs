//! A control-only node's dedicated system-keyspace engine survives a real
//! process restart (`ProdEnv`, real disk I/O, real TCP) — the
//! `animusd`-layer counterpart of `animus-control`'s `SimEnv`-driven
//! `apply_engine.rs` differential oracle, over the actual
//! `Node::bind_control`/`start_control_with` assembly a real `animusd
//! control` process runs.
//!
//! Originally written for ADR 0038 PR2, when this engine was only a
//! shadow-mode dual-write mirror of a separate in-core `Metadata` (so the
//! interesting claim was "the mirror agrees with the in-core copy and both
//! survive"). **Since PR3's cutover, this engine *is* the durable source of
//! truth** (`Metadata: DRIVER_APPLIED` — there is no in-core copy to mirror
//! anymore) — so this test now proves the load-bearing claim directly: real
//! bytes on a real disk are what a restarted node's control-plane state
//! *actually* recovers from, read back here by an entirely separate
//! `LsmEngine` handle opened after the node that wrote them has been shut
//! down, independent of any node's own in-memory state.
//!
//! Like the other `animusd` integration tests this uses real TCP/time and is
//! non-deterministic by design (the `ProdEnv` edge); every wait is a bounded
//! poll, never a fixed sleep.

use std::net::SocketAddr;
use std::time::Duration;

use animus_control::mirror::rebuild_metadata_from_engine;
use animus_env::{NodeId, ProdEnv};
use animus_storage::LsmEngine;
use animusd::{ClientRequest, ClientResponse, MetaCommand, Node, NodeStatus, read_frame};
use tempfile::TempDir;
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

fn free_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}

async fn await_leader(node: &Node) {
    timeout(Duration::from_secs(20), async {
        loop {
            if node.is_control_leader() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("single control node did not elect itself leader in 20s");
}

/// Start a single-node control-only cluster at fixed `addrs` (so a later
/// restart can rebind the exact same addresses), with the ADR 0038 PR2
/// mirror engine attached over the durable `LsmEngine` backend.
async fn start(addrs: animusd::RoleAddrs, dir: &std::path::Path) -> Node {
    let config = animusd::ClusterConfig { nodes: vec![addrs] };
    animusd::run_node_control(&config, 0, dir, animusd::StorageBackend::Lsm)
        .await
        .expect("control-only node starts")
}

fn upsert(node: NodeId, status: NodeStatus) -> MetaCommand {
    MetaCommand::UpsertMember {
        node,
        labels: std::collections::BTreeMap::new(),
        status,
    }
}

/// Propose `command` (an `UpsertMember`) via the client API's generic
/// `ProposeSchema` passthrough (the same wire primitive
/// `control_only.rs::schema_ddl_via_control_node_commits_and_relays` drives —
/// it proposes *any* `MetaCommand`, not just schema ones) and poll until it
/// lands in this node's own applied `Metadata`.
async fn propose_and_await(node: &Node, client_addr: SocketAddr, command: MetaCommand) {
    let resp = call(client_addr, ClientRequest::ProposeSchema(command.clone())).await;
    assert!(
        matches!(resp, ClientResponse::PutOk),
        "ProposeSchema should ack: {resp:?}"
    );
    let MetaCommand::UpsertMember { node: id, .. } = &command else {
        panic!("test only proposes UpsertMember");
    };
    let id = *id;
    timeout(Duration::from_secs(10), async {
        loop {
            if node.metadata().members.contains_key(&id) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("proposed member did not land in 10s");
}

/// Reopen the mirror's on-disk `LsmEngine` directly (a **separate** handle
/// from the one the (now shut-down) node held) and rebuild a `Metadata` from
/// it, to verify durability independent of any node's own in-memory state.
async fn read_mirror_from_disk(dir: &std::path::Path) -> animus_control::Metadata {
    // A fresh, throwaway `ProdEnv` bound at a scratch address but pointed at
    // the SAME control directory the node's own control env used — disk
    // content is keyed by directory, not by which port/id opened it.
    let (env, _addr) = ProdEnv::bind(999_999, free_addr(), dir.join("control"))
        .await
        .expect("bind a scratch env over the same directory");
    let engine: LsmEngine<ProdEnv> = LsmEngine::open(env, animusd::SYSKV_LSM_PREFIX)
        .await
        .expect("reopen the mirror's durable engine");
    rebuild_metadata_from_engine(&engine)
        .await
        .expect("rebuild metadata from the reopened engine")
}

#[tokio::test(flavor = "multi_thread")]
async fn control_only_mirror_engine_survives_a_real_process_restart() {
    let dir = TempDir::new().unwrap();
    let node_dir = dir.path().join("node-0");
    let addrs = animusd::RoleAddrs {
        role: animusd::config::NodeRole::Control,
        control: Some(free_addr()),
        client: free_addr(),
        dynamo: free_addr(),
        cql: free_addr(),
        raftkv: None,
        admin: free_addr(),
    };

    // --- First incarnation: propose a few commands, then shut down cleanly. ---
    let node = start(addrs, &node_dir).await;
    await_leader(&node).await;
    for id in 0..3 {
        propose_and_await(&node, addrs.client, upsert(id, NodeStatus::Down)).await;
    }
    // Let the apply task catch up before shutting down.
    sleep(Duration::from_millis(500)).await;
    node.shutdown();
    sleep(Duration::from_millis(200)).await;

    let mirrored_before = read_mirror_from_disk(&node_dir).await;
    assert_eq!(
        mirrored_before.members.len(),
        3,
        "mirror engine should hold the pre-restart writes on disk"
    );

    // --- Second incarnation: same directory, same addresses. ---
    let node = start(addrs, &node_dir).await;
    await_leader(&node).await;
    // The restarted node recovers its 3 pre-restart members from its own
    // control WAL (independent of the mirror) before we drive anything new
    // through it.
    timeout(Duration::from_secs(10), async {
        loop {
            if node.metadata().members.len() == 3 {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("restarted node did not recover its pre-restart members in 10s");

    for id in 3..6 {
        propose_and_await(&node, addrs.client, upsert(id, NodeStatus::Down)).await;
    }
    let reference_after = node.metadata();
    assert_eq!(reference_after.members.len(), 6);

    // Let the apply task catch up on the post-restart writes, then shut
    // down again and verify the *reopened* engine agrees with the
    // now-6-member in-core `Metadata` — proving both that the mirror
    // survived the restart with its pre-restart content intact AND that it
    // kept mirroring afterward.
    sleep(Duration::from_millis(500)).await;
    node.shutdown();
    sleep(Duration::from_millis(200)).await;

    let mirrored_after = read_mirror_from_disk(&node_dir).await;
    assert_eq!(
        mirrored_after, reference_after,
        "mirror engine should survive the restart with consistent, caught-up content"
    );
}
