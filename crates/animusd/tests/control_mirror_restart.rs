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
use animus_env::{NodeId, ProdEnv, nid};
use animus_storage::LsmEngine;
use animus_tablet::{KeyRange, TabletId};
use animusd::{
    ClientRequest, ClientResponse, ColumnType, MetaCommand, Node, NodeStatus, TableSchema,
    read_frame,
};
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

/// `count` distinct free ephemeral addresses, allocated **simultaneously**.
///
/// Holding every listener until they are all bound is load-bearing twice over.
/// It guarantees the addresses are *distinct* — allocating them one at a time
/// releases each port before probing the next, so the OS is free to hand the
/// same port back and a node would then be configured with (say) `internal ==
/// client`. And it releases them in one instant rather than five, shrinking the
/// documented port-TOCTOU window (see `support::free_addrs`, and the retry in
/// [`start`] that rides out the rest of it).
fn free_addrs(count: usize) -> Vec<SocketAddr> {
    let listeners: Vec<std::net::TcpListener> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
    // listeners dropped here, freeing the ports for the caller to bind.
}

fn free_addr() -> SocketAddr {
    free_addrs(1)[0]
}

/// The five addresses a node's roles bind, allocated as one distinct set.
fn role_addrs(id: NodeId) -> animusd::RoleAddrs {
    let a = free_addrs(6);
    animusd::RoleAddrs {
        id,
        role: animusd::config::NodeRole::Control,
        internal: a[0],
        client: a[1],
        dynamo: a[2],
        admin: a[3],
        intra: a[4],
        console: a[5],
    }
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
    // Bounded rebind retry against the documented port-TOCTOU: another test
    // binary's `free_addrs` probe can hold a just-freed port for microseconds,
    // and this test cannot re-allocate around a thief — both call sites are
    // pinned to the addresses captured up front, because *rebinding the same
    // addresses is what the restart half is testing*. Same shape and reasoning
    // as `support::restart_same_addrs`; this file predates that helper and
    // carries its own control-only bring-up, which is how it missed the
    // mitigation. A genuinely occupied port still fails at the deadline.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match animusd::run_node_control(&config, 0, dir, animusd::StorageBackend::Lsm).await {
            Ok(node) => return node,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "control-only node did not start/rebind within 30s: {e}"
                );
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
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
    let id = id.clone();
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
    // the SAME internal directory the node's own env used (ADR 0040 PR1:
    // one shared env, one directory, not a separate `control` subdir) — disk
    // content is keyed by directory, not by which port/id opened it.
    let (env, _addr) = ProdEnv::bind(nid(999_999), free_addr(), dir.join("internal"))
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
    let addrs = role_addrs(nid(0));

    // --- First incarnation: propose a few commands, then shut down cleanly. ---
    let node = start(addrs.clone(), &node_dir).await;
    await_leader(&node).await;
    for id in 0..3 {
        propose_and_await(&node, addrs.intra, upsert(nid(id), NodeStatus::Down)).await;
    }
    // Let the apply task catch up before shutting down.
    sleep(Duration::from_millis(500)).await;
    node.shutdown_graceful().await;
    sleep(Duration::from_millis(200)).await;

    let mirrored_before = read_mirror_from_disk(&node_dir).await;
    assert_eq!(
        mirrored_before.members.len(),
        3,
        "mirror engine should hold the pre-restart writes on disk"
    );

    // --- Second incarnation: same directory, same addresses. ---
    let node = start(addrs.clone(), &node_dir).await;
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
        propose_and_await(&node, addrs.intra, upsert(nid(id), NodeStatus::Down)).await;
    }
    let reference_after = node.metadata();
    assert_eq!(reference_after.members.len(), 6);

    // Let the apply task catch up on the post-restart writes, then shut
    // down again and verify the *reopened* engine agrees with the
    // now-6-member in-core `Metadata` — proving both that the mirror
    // survived the restart with its pre-restart content intact AND that it
    // kept mirroring afterward.
    sleep(Duration::from_millis(500)).await;
    node.shutdown_graceful().await;
    sleep(Duration::from_millis(200)).await;

    let mirrored_after = read_mirror_from_disk(&node_dir).await;
    assert_eq!(
        mirrored_after, reference_after,
        "mirror engine should survive the restart with consistent, caught-up content"
    );
}

/// ADR 0038 PR4: the sibling of the test above, widened from `members` alone
/// to the **full replicated-metadata shape** the plan's restart matrix calls
/// out explicitly — the schema catalog and the tablet map, not just
/// membership — over the exact same real-`ProdEnv`-restart, dedicated-engine
/// path. A control-only node hosts no CP tablet group itself, but its own
/// control plane still owns the authoritative tablet *map* (placement
/// metadata), so this proposes a `CreateTableSchema` and a `CreateTablet`
/// alongside an `UpsertMember`, then proves all three survive a **hard**
/// restart (`shutdown_and_wait`, no `flush()` — the restart recovers from
/// whatever the real Raft WAL + system-keyspace engine already made durable
/// on their own schedule, not from a clean-teardown flush), both through the
/// restarted node's own `metadata()` and through an entirely separate
/// `LsmEngine` handle reopened over the same directory.
#[tokio::test(flavor = "multi_thread")]
async fn control_only_schema_and_tablet_map_survive_a_hard_restart() {
    let dir = TempDir::new().unwrap();
    let node_dir = dir.path().join("node-0");
    let addrs = role_addrs(nid(0));

    let table = "ctl_meta_t";
    let tablet = TabletId(4242);

    // --- First incarnation: propose membership + schema + a tablet, then a
    // hard (non-graceful) shutdown. ---
    let node = start(addrs.clone(), &node_dir).await;
    await_leader(&node).await;
    propose_and_await(&node, addrs.intra, upsert(nid(7), NodeStatus::Down)).await;

    let create_schema = MetaCommand::CreateTableSchema {
        table: table.to_string(),
        schema: TableSchema::simple("id", ColumnType::String),
    };
    let resp = call(addrs.intra, ClientRequest::ProposeSchema(create_schema)).await;
    assert!(matches!(resp, ClientResponse::PutOk));
    timeout(Duration::from_secs(10), async {
        loop {
            if node.metadata().has_table_schema(table) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("schema did not land in 10s");

    let create_tablet = MetaCommand::CreateTablet {
        tablet,
        table: Some(table.to_string()),
        range: KeyRange::whole(),
        replicas: vec![nid(300)],
    };
    let resp = call(addrs.intra, ClientRequest::ProposeSchema(create_tablet)).await;
    assert!(matches!(resp, ClientResponse::PutOk));
    timeout(Duration::from_secs(10), async {
        loop {
            if node.metadata().tablets.contains_key(&tablet) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("tablet did not land in 10s");

    // Let the apply task's engine merge catch up, then a genuinely **hard**
    // shutdown — `shutdown_and_wait` still hard-`abort()`s every task (no
    // `RaftNode::flush()` call), it only additionally waits for the abort to
    // finish unwinding so the immediate rebind below doesn't race this same
    // process's own not-yet-freed listener ports (the documented
    // `AddrInUse` flake, see `docs/engineering-lessons.md`) — durability
    // itself still comes entirely from whatever the Raft WAL/engine already
    // made durable on their own schedule, exactly what a real `kill`+restart
    // would exercise.
    sleep(Duration::from_millis(500)).await;
    node.shutdown_and_wait().await;

    let mirrored_before = read_mirror_from_disk(&node_dir).await;
    assert!(
        mirrored_before.members.contains_key(&nid(7)),
        "engine should hold the pre-restart member"
    );
    assert!(
        mirrored_before.has_table_schema(table),
        "engine should hold the pre-restart schema"
    );
    assert!(
        mirrored_before.tablets.contains_key(&tablet),
        "engine should hold the pre-restart tablet"
    );

    // --- Second incarnation: same directory, same addresses — the DEDICATED
    // system-keyspace engine path (a control-only node has no separate
    // `raftkv` engine to fall back on). ---
    let node = start(addrs.clone(), &node_dir).await;
    await_leader(&node).await;
    timeout(Duration::from_secs(10), async {
        loop {
            let meta = node.metadata();
            if meta.members.contains_key(&nid(7))
                && meta.has_table_schema(table)
                && meta.tablets.contains_key(&tablet)
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("restarted node did not recover the full pre-restart metadata shape in 10s");

    node.shutdown_and_wait().await;
}
