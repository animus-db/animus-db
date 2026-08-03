//! Stage 3a (ADR 0017 #3a): the **leaderful CP data plane runs in the assembled
//! node over `ProdEnv`**. A table marked `ReplicationMode::Cp` in the replicated
//! schema catalog has its client reads/writes routed to a per-tablet Raft group
//! (`animus-raftdata`) hosted on the nodes' `raftkv` role, instead of the
//! leaderless AP quorum plane.
//!
//! This is the production assembly of the CP plane whose mechanism is sim-proven
//! in `animus-raftdata` (single-tablet linearizable KV, ReadIndex reads). Here we
//! drive it over real TCP/time through the same client API the CLI uses:
//!
//! 1. bring up a 3-node cluster and bootstrap it;
//! 2. create a table schema and flip it to **CP** (`SetTableMode`), via the
//!    interim `Node::propose_meta` admin hook — and wait for it to replicate;
//! 3. write/read that table's key through the plain client API tagged with the
//!    table name — the node routes it to the CP group leader, and the value
//!    round-trips (written via one node, read back via another — the CP group is
//!    the single source of truth, reached through the shared cluster edge state);
//! 4. an **AP** key (no table / unmarked table) still works on the AP plane.
//!
//! Real TCP/time, so it polls with generous timeouts rather than asserting
//! deterministic timing. CP client routing within a `--cluster N` process is what
//! 3a delivers; cross-process routing + dynamic CP placement/split/reconfigure
//! over `ProdEnv` are Stage 3b.

use std::time::Duration;

use animusd::{
    ClientRequest, ClientResponse, ColumnType, MetaCommand, Node, ReplicationMode, TableSchema,
    bind_cluster, read_frame, start_cluster,
};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

const CP_TABLE: &str = "cp_t";

async fn call(addr: std::net::SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect to node");
    animusd::write_frame(&mut stream, &req)
        .await
        .expect("send request");
    read_frame(&mut stream)
        .await
        .expect("read reply")
        .expect("a reply")
}

async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().tablets.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not bootstrap within 20s");
}

/// Propose a `MetaCommand` on whichever node currently leads the control plane,
/// retrying until accepted (a fresh cluster may still be electing).
async fn propose_on_leader(nodes: &[Node], command: MetaCommand) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if nodes.iter().any(|n| n.propose_meta(command.clone())) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no control leader accepted {command:?} within 20s"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cp_table_reads_and_writes_route_through_the_raft_group() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound, 2, 2).await.unwrap();
    await_bootstrap(&nodes).await;

    // Register a table schema, then flip it to CP. Both are replicated control-plane
    // commands; SetTableMode is rejected unless the schema already exists, so order
    // matters.
    propose_on_leader(
        &nodes,
        MetaCommand::CreateTableSchema {
            table: CP_TABLE.into(),
            schema: TableSchema::simple("id", ColumnType::String),
        },
    )
    .await;
    propose_on_leader(
        &nodes,
        MetaCommand::SetTableMode {
            table: CP_TABLE.into(),
            mode: ReplicationMode::Cp,
        },
    )
    .await;

    // Wait until the CP mode has replicated to every node (so any node's edge
    // routes the table to the CP plane).
    let mode_ready = async {
        loop {
            if nodes
                .iter()
                .all(|n| n.metadata().table_mode(CP_TABLE) == ReplicationMode::Cp)
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), mode_ready)
        .await
        .expect("CP mode did not replicate within 20s");

    // Write the CP key through node 0's client API, tagged with the table name. The
    // node routes it to the CP group leader. Retry until PutOk: the CP group may
    // still be electing its own leader (its election is independent of the control
    // plane's), so `cp_put` returns an error ("no CP group leader available") until
    // it settles.
    let addr0 = nodes[0].client_addr();
    let put_ok = async {
        loop {
            let resp = call(
                addr0,
                ClientRequest::Put {
                    key: b"k".to_vec(),
                    value: b"cp-value".to_vec(),
                    table: Some(CP_TABLE.into()),
                },
            )
            .await;
            match resp {
                ClientResponse::PutOk => return,
                ClientResponse::Error(_) => sleep(Duration::from_millis(100)).await,
                other => panic!("unexpected CP put response: {other:?}"),
            }
        }
    };
    timeout(Duration::from_secs(20), put_ok)
        .await
        .expect("CP write did not succeed within 20s");

    // Read it back through a *different* node's client API: the CP group is the
    // single source of truth (reached via the shared cluster edge state), so the
    // linearizable read observes the committed write.
    let addr2 = nodes[2].client_addr();
    let got = call(
        addr2,
        ClientRequest::Get {
            key: b"k".to_vec(),
            table: Some(CP_TABLE.into()),
        },
    )
    .await;
    assert_eq!(
        got,
        ClientResponse::Value(Some(b"cp-value".to_vec())),
        "CP read must observe the committed CP write"
    );

    // A CP read of an absent key in the same table reads as None (not a phantom).
    let absent = call(
        addr0,
        ClientRequest::Get {
            key: b"absent".to_vec(),
            table: Some(CP_TABLE.into()),
        },
    )
    .await;
    assert_eq!(absent, ClientResponse::Value(None));

    // The AP plane is untouched: a key with no table routes to the leaderless
    // quorum coordinator and round-trips as before.
    let ap_put = call(
        addr0,
        ClientRequest::Put {
            key: b"ap".to_vec(),
            value: b"ap-value".to_vec(),
            table: None,
        },
    )
    .await;
    assert!(
        matches!(ap_put, ClientResponse::PutOk),
        "AP put failed: {ap_put:?}"
    );
    let ap_got = call(
        addr2,
        ClientRequest::Get {
            key: b"ap".to_vec(),
            table: None,
        },
    )
    .await;
    assert_eq!(ap_got, ClientResponse::Value(Some(b"ap-value".to_vec())));

    for n in &nodes {
        n.shutdown();
    }
}
