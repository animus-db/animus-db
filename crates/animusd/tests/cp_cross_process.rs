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

/// Bring up `n` per-process nodes (each via `run_node`, so each has its own edge
/// state), wrapped in the documented **port-TOCTOU retry**: `free_addrs` releases
/// the probed ports before `run_node` rebinds them, so a concurrent test binary can
/// steal one — re-allocate fresh ports and retry the whole bring-up as a unit.
async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, animusd::ClusterConfig) {
    for attempt in 0..16 {
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
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            match animusd::run_node(&config, i, dir.join(format!("node-{attempt}-{i}"))).await {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            return (nodes, config);
        }
        for node in &nodes {
            node.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up cluster after retries (ports kept getting stolen)");
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
    // One node per process — each gets its own edge state via `run_node`.
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
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

/// A **batch write** (`ClientRequest::PutBatch`, ADR 0017 bulk-write batching) must
/// route/forward exactly like a single write: issued from **every** node in turn,
/// so at least two of the three land on a non-leader and must be **forwarded** to
/// the CP leader's node (the `cp_serve_forwarded` `PutBatch` arm). A missing
/// forwarded arm is the classic bimodal per-process failure (works only when the
/// connected node happens to lead) — this fails it deterministically wherever the
/// leader lands. Each batch's keys are then read back to confirm they committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn batch_write_on_a_non_leader_node_is_forwarded() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let client = |i: usize| config.nodes[i].client;

    // A batch issued from each node: node `i` writes keys `bwN-i`. Whether or not
    // node `i` leads the tablet, the batch must succeed — locally or forwarded.
    for i in 0..n {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..5)
            .map(|k| {
                (
                    format!("bw{k}-{i}").into_bytes(),
                    format!("val{k}-{i}").into_bytes(),
                )
            })
            .collect();
        timeout(Duration::from_secs(25), async {
            loop {
                match call(
                    client(i),
                    ClientRequest::PutBatch {
                        entries: entries.clone(),
                        table: CP_TABLE.into(),
                    },
                )
                .await
                {
                    ClientResponse::PutOk => return,
                    ClientResponse::Error(_) => sleep(Duration::from_millis(150)).await,
                    other => panic!("unexpected CP batch response: {other:?}"),
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("CP batch via node {i} did not succeed in 25s"));
    }

    // Every key of every batch reads back (via node 0 — forwarded if it's not the
    // leader), so the whole batch committed on the tablet.
    for i in 0..n {
        for k in 0..5 {
            let got = call(
                client(0),
                ClientRequest::Get {
                    key: format!("bw{k}-{i}").into_bytes(),
                    table: CP_TABLE.into(),
                },
            )
            .await;
            assert_eq!(
                got,
                ClientResponse::Value(Some(format!("val{k}-{i}").into_bytes())),
                "batch key bw{k}-{i} must be present after a (possibly forwarded) batch"
            );
        }
    }

    for node in &nodes {
        node.shutdown();
    }
}

/// ADR 0017 #4 regression — **derived member ids must translate back to base ids on
/// the forward path**. The *first* provisioned table wins the tablet-id race with
/// bootstrap and rides the bootstrap group, whose member ids **are** the base
/// `raftkv` ids — so a missing member→base translation in `cp_forward_target` is
/// invisible on it (the test above). The **second** table's tablet gets *derived*
/// member ids (`base + tablet * STRIDE`); its leader hint must be translated back to
/// a base id before the `client_route` lookup, or a follower node waits out
/// `CLIENT_TIMEOUT` on a healthy group ("no CP group leader reachable" — the
/// admin_data_write flake). Reading via **every** node guarantees at least two
/// forwarded reads, so a broken translation fails deterministically, wherever the
/// leader landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn second_table_with_derived_member_ids_forwards_across_processes() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let client = |i: usize| config.nodes[i].client;

    // Provision two tables via node 0 (the plain client auto-provisions on first
    // write, retrying while each group forms/elects). The first rides the bootstrap
    // group (base ids); the SECOND gets a derived-id group — the one under test.
    for (table, key, value) in [
        ("cp_first", b"a".to_vec(), b"v1".to_vec()),
        ("cp_second", b"b".to_vec(), b"v2".to_vec()),
    ] {
        timeout(Duration::from_secs(25), async {
            loop {
                match call(
                    client(0),
                    ClientRequest::Put {
                        key: key.clone(),
                        value: value.clone(),
                        table: table.into(),
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
        .unwrap_or_else(|_| panic!("CP write to {table} did not succeed in 25s"));
    }

    // Read the second table's key via *every* node: at least two reads land on a
    // non-leader and must forward via the translated leader hint. No retry — the
    // write above proved the group is led, so a miss here is the routing bug.
    for i in 0..n {
        let got = call(
            client(i),
            ClientRequest::Get {
                key: b"b".to_vec(),
                table: "cp_second".into(),
            },
        )
        .await;
        assert_eq!(
            got,
            ClientResponse::Value(Some(b"v2".to_vec())),
            "derived-id CP read via node {i} (forwarded if non-leader) must see the write"
        );
    }

    for node in &nodes {
        node.shutdown();
    }
}
