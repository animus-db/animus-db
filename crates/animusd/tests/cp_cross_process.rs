//! Phase 1 / A1 (v1 plan, ADR 0017 #3b): **cross-process CP routing**. In a
//! one-process-per-node deployment (`run_node` from a shared config), a node that
//! receives a CP-table op but does **not** host the CP group leader **forwards**
//! the request to the leader's node (resolved via a local CP replica's Raft leader
//! hint → the node's client address from the config). Stage 3a only routed CP ops
//! within a single `--cluster N` process (shared edge state); this proves the
//! real multi-process path.
//!
//! Each node here has its **own** `ClusterEdgeState` (as separate processes
//! would), so a CP op on a non-leader node can only succeed via forwarding.
//!
//! Real TCP/time → polls with timeouts.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{
    ClientRequest, ClientResponse, ColumnType, MetaCommand, Node, ReplicationMode, TableSchema,
    read_frame,
};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

const CP_TABLE: &str = "cp_t";

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// Reserve `count` free loopback ports (bind :0, read addr, release).
fn free_addrs(count: usize) -> Vec<SocketAddr> {
    let ls: Vec<std::net::TcpListener> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    ls.iter().map(|l| l.local_addr().unwrap()).collect()
}

async fn await_bootstrap(nodes: &[Node]) {
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap in 20s");
}

async fn propose_on_leader(nodes: &[Node], command: MetaCommand) {
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(|n| n.propose_meta(command.clone())) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no control leader accepted {command:?} in 20s"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cp_op_on_a_non_leader_node_is_forwarded_to_the_leader() {
    let n = 3;
    let addrs = free_addrs(n * 6);
    let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
        .map(|i| animusd::RoleAddrs {
            control: addrs[6 * i],
            client: addrs[6 * i + 1],
            dynamo: addrs[6 * i + 2],
            cql: addrs[6 * i + 3],
            raftkv: addrs[6 * i + 4],
            admin: addrs[6 * i + 5],
        })
        .collect();
    let config = animusd::ClusterConfig { nodes: nodes_cfg };

    // One node per process — each gets its own edge state via `run_node`.
    let dir = tempfile::tempdir().unwrap();
    let mut nodes = Vec::new();
    for i in 0..n {
        nodes.push(
            animusd::run_node(&config, i, dir.path().join(format!("node-{i}")))
                .await
                .unwrap_or_else(|e| panic!("node {i} start: {e}")),
        );
    }
    await_bootstrap(&nodes).await;

    // Mark a table CP and wait for it to replicate to every node.
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
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes
                .iter()
                .all(|n| n.metadata().table_mode(CP_TABLE) == ReplicationMode::Cp)
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("CP mode did not replicate in 20s");

    let client = |i: usize| config.nodes[i].client;

    // Write the CP key via node 0. Whether or not node 0 is the CP leader, this
    // must succeed — locally if it leads, else by forwarding. Retry while the CP
    // group elects its own leader.
    timeout(Duration::from_secs(25), async {
        loop {
            match call(
                client(0),
                ClientRequest::Put {
                    key: b"k".to_vec(),
                    value: b"v-cross".to_vec(),
                    table: CP_TABLE.into(),
                },
            )
            .await
            {
                ClientResponse::PutOk => return,
                ClientResponse::Error(_) => sleep(Duration::from_millis(150)).await,
                other => panic!("unexpected CP put response: {other:?}"),
            }
        }
    })
    .await
    .expect("CP write (possibly forwarded) did not succeed in 25s");

    // Read it back via *every* node. With one CP leader among three nodes, at least
    // two of these reads land on a non-leader and must be served by forwarding.
    for i in 0..n {
        let got = call(
            client(i),
            ClientRequest::Get {
                key: b"k".to_vec(),
                table: CP_TABLE.into(),
            },
        )
        .await;
        assert_eq!(
            got,
            ClientResponse::Value(Some(b"v-cross".to_vec())),
            "CP read via node {i} (forwarded if non-leader) must see the write"
        );
    }

    for node in &nodes {
        node.shutdown();
    }
}
