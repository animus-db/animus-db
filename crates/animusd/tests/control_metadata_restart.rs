//! ADR 0038 PR4's animusd-level crash/restart matrix for the control plane's
//! `Metadata`, rounding out `control_mirror_restart.rs` (the control-only,
//! dedicated-engine path) with the other two legs:
//!
//! - a **combined** node's restart recovers the schema catalog + members +
//!   tablet map via the exact same physical **shared** CP-data engine a
//!   hosted tablet's own data also lives on (`Metadata` at a reserved key
//!   prefix within it, ADR 0038) — `durable_restart.rs` only ever proves
//!   *data*-plane durability through this engine, never the control plane's;
//! - an **`--ephemeral`** control-only node's restart does *not* inherit its
//!   previous incarnation's `Metadata` and re-bootstraps cleanly (no panic,
//!   elects itself leader, serves requests) rather than crashing.
//!
//! Real TCP/time (`ProdEnv`), so every wait is a bounded poll, never a fixed
//! sleep, per this crate's established restart-test discipline.

use std::net::SocketAddr;
use std::time::Duration;

use animus_control::mirror::rebuild_metadata_from_engine;
use animus_env::{ProdEnv, nid};
use animus_storage::LsmEngine;
use animus_tablet::{KeyRange, TabletId};
use animusd::{
    ClientRequest, ClientResponse, ColumnType, MetaCommand, Node, StorageBackend, TableSchema,
    read_frame,
};
use tempfile::TempDir;
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

fn free_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}

async fn await_bootstrap(node: &Node) {
    timeout(Duration::from_secs(20), async {
        loop {
            if node.is_control_leader() && !node.metadata().members.is_empty() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("node did not bootstrap in 20s");
}

async fn await_leader_only(node: &Node) {
    timeout(Duration::from_secs(20), async {
        loop {
            if node.is_control_leader() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("control-only node did not elect itself leader in 20s");
}

/// Start a single-node **control-only** cluster at fixed `addrs` (so a later
/// restart can rebind the exact same addresses).
async fn start_control(addrs: animusd::RoleAddrs, dir: &std::path::Path) -> Node {
    let config = animusd::ClusterConfig { nodes: vec![addrs] };
    animusd::run_node_control(&config, 0, dir, StorageBackend::Memory)
        .await
        .expect("control-only node starts")
}

#[tokio::test(flavor = "multi_thread")]
async fn combined_node_restart_recovers_control_metadata_via_shared_engine() {
    let dir = TempDir::new().unwrap();
    let node_dir = dir.path().join("node-0");

    // --- First incarnation. ---
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    // ADR 0047: `ProposeSchema` is intra-only now — dial the intra port
    // (still named `client` here purely to minimize the diff; every call
    // below is a `ProposeSchema`).
    let client = config.nodes[0].intra;
    await_bootstrap(&node).await;
    // `bootstrap` already registered this single node's own raftkv id as an
    // `Active` member — proving `members` survives needs no extra proposal.
    let member_id = node
        .metadata()
        .members
        .keys()
        .next()
        .cloned()
        .expect("bootstrap registered at least one member");

    let table = "combined_meta_t";
    let tablet = TabletId(4242);
    let resp = call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTableSchema {
            table: table.to_string(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
    )
    .await;
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

    let resp = call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTablet {
            tablet,
            table: Some(table.to_string()),
            range: KeyRange::whole(),
            replicas: vec![member_id.clone()],
        }),
    )
    .await;
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

    // Let the control plane's apply task merge into the shared engine, then
    // a hard (non-graceful) shutdown — the point is to prove durability came
    // from what the engine/WAL already made durable on their own schedule,
    // not from a clean-teardown flush.
    sleep(Duration::from_millis(500)).await;
    node.shutdown_and_wait().await;

    // Independently reopen the SAME shared engine a hosted tablet's own data
    // also lives on (`animusd::LSM_PREFIX`, over the `raftkv` directory) — an
    // entirely separate handle from the one the (now shut-down) node held —
    // and rebuild `Metadata` from it directly, proving the control plane's
    // system-keyspace mirror is real, independent of any node's own
    // in-memory state or of the restart path below.
    let raftkv_dir = node_dir.join("internal");
    let (env, _addr) = ProdEnv::bind(nid(999_999), free_addr(), raftkv_dir)
        .await
        .expect("bind a scratch env over the same raftkv directory");
    let engine: LsmEngine<ProdEnv> = LsmEngine::open(env, animusd::LSM_PREFIX)
        .await
        .expect("reopen the shared engine");
    let rebuilt = rebuild_metadata_from_engine(&engine)
        .await
        .expect("rebuild metadata from the reopened shared engine");
    assert!(
        rebuilt.members.contains_key(&member_id),
        "the shared engine should hold the bootstrap member"
    );
    assert!(
        rebuilt.has_table_schema(table),
        "the shared engine should hold the pre-restart schema"
    );
    assert!(
        rebuilt.tablets.contains_key(&tablet),
        "the shared engine should hold the pre-restart tablet"
    );
    drop(engine); // release the scratch handle before the real restart reopens the same files.

    // --- Second incarnation: same dir + addresses, genuine restart path. ---
    let node = support::restart_same_addrs(&config, 0, &node_dir, StorageBackend::default()).await;
    await_bootstrap(&node).await;
    timeout(Duration::from_secs(10), async {
        loop {
            let meta = node.metadata();
            if meta.members.contains_key(&member_id)
                && meta.has_table_schema(table)
                && meta.tablets.contains_key(&tablet)
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("restarted combined node did not recover the full pre-restart control metadata in 10s");

    node.shutdown_and_wait().await;
}

/// **`--ephemeral` control-only restart**: `StorageBackend::Memory` selects a
/// fresh, volatile system-keyspace engine every process start (ADR 0038) —
/// exactly like `durable_restart.rs`'s data-plane analogue
/// (`acked_write_survives_memory_backend_restart_via_raft_wal`), a real
/// ephemeral instance is a **new, empty on-disk directory** too (a genuinely
/// stateless replacement for a crashed/torn-down pod, not the same disk with
/// only the engine choice flipped — reusing the SAME directory would still
/// recover the control Raft's own on-disk WAL log tail regardless of the
/// engine backend, exactly the already-documented `animusd/CLAUDE.md` gotcha
/// that "`--ephemeral` does NOT make the control/raftkv WALs ephemeral,"
/// which is *why* a genuinely fresh directory is the right way to exercise
/// "does this incarnation carry zero history"). This proves the restarted
/// node does not inherit its predecessor's `Metadata` and re-bootstraps
/// cleanly — no panic, elects itself leader, serves a fresh proposal — with
/// its own address identity unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn ephemeral_control_only_restart_does_not_carry_over_metadata() {
    let dir = TempDir::new().unwrap();
    let addrs = animusd::RoleAddrs {
        id: animusd::config::node_id(0),
        role: animusd::config::NodeRole::Control,
        internal: free_addr(),
        client: free_addr(),
        dynamo: free_addr(),
        cql: free_addr(),
        admin: free_addr(),
        intra: free_addr(),
    };

    // --- First incarnation: propose a schema, then a hard shutdown. ---
    let node = start_control(addrs.clone(), &dir.path().join("incarnation-0")).await;
    await_leader_only(&node).await;
    let table = "ephemeral_ctl_t";
    let resp = call(
        // ADR 0047: `ProposeSchema` is intra-only.
        addrs.intra,
        ClientRequest::ProposeSchema(MetaCommand::CreateTableSchema {
            table: table.to_string(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
    )
    .await;
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
    sleep(Duration::from_millis(300)).await;
    node.shutdown_and_wait().await;

    // --- Second incarnation: SAME addresses (a real deployment keeps a
    // replacement pod's identity/routing stable), but a FRESH directory (a
    // genuinely stateless engine — no prior WAL, no prior system-keyspace
    // engine) and the SAME `--ephemeral` backend choice. ---
    let node = start_control(addrs.clone(), &dir.path().join("incarnation-1")).await;
    await_leader_only(&node).await;

    // It re-bootstrapped cleanly (no panic, genuinely elected itself leader
    // above) with an EMPTY `Metadata` — the previous incarnation's schema is
    // simply gone, never replayed from anywhere.
    assert!(
        node.metadata().members.is_empty(),
        "a fresh ephemeral incarnation should start with no members: {:?}",
        node.metadata().members
    );
    assert!(
        !node.metadata().has_table_schema(table),
        "a fresh ephemeral incarnation must not inherit the previous incarnation's schema"
    );

    // And it's genuinely live, not just quiet: a fresh proposal against it
    // commits normally.
    let table2 = "ephemeral_ctl_t2";
    let resp = call(
        // ADR 0047: `ProposeSchema` is intra-only.
        addrs.intra,
        ClientRequest::ProposeSchema(MetaCommand::CreateTableSchema {
            table: table2.to_string(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
    )
    .await;
    assert!(matches!(resp, ClientResponse::PutOk));
    timeout(Duration::from_secs(10), async {
        loop {
            if node.metadata().has_table_schema(table2) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the re-bootstrapped node did not serve a fresh proposal in 10s");

    node.shutdown_and_wait().await;
}
