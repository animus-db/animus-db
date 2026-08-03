//! The **leaderful CP data plane** runs in the assembled node over `ProdEnv`
//! (ADR 0017 #3a / v1 ADR 0019: CP-only). Every client read/write is routed to a
//! per-tablet Raft group (`animus-cp-data`) hosted on the nodes' `raftkv` role —
//! the single, linearizable source of truth.
//!
//! This is the production assembly of the CP plane whose mechanism is sim-proven
//! in `animus-cp-data` (single-tablet linearizable KV, ReadIndex reads). Here we
//! drive it over real TCP/time through the same client API the CLI uses:
//!
//! 1. bring up a 3-node cluster and bootstrap it;
//! 2. write a key through one node's client API — the node routes it to the CP
//!    group leader (in-process: the shared cluster edge state reaches the leader);
//! 3. read it back through a *different* node — the CP group is the single source
//!    of truth, so the linearizable read observes the committed write;
//! 4. an absent key reads as `None` (not a phantom); an untagged key round-trips
//!    the same way (the optional `table` no longer selects a plane — there is only
//!    the CP plane).
//!
//! Real TCP/time, so it polls with generous timeouts rather than asserting
//! deterministic timing. Cross-process CP routing (forwarding to the leader's
//! node) is covered by `cp_cross_process.rs`.

use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, bind_cluster, read_frame, start_cluster};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn reads_and_writes_route_through_the_raft_group() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    // Write a key through node 0's client API, tagged with a table name. The node
    // routes it to the CP group leader. Retry until PutOk: the CP group may still
    // be electing its own leader (independent of the control plane's), so `cp_put`
    // errors until it settles.
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

    // A read of an absent key reads as `None` (not a phantom).
    let absent = call(
        addr0,
        ClientRequest::Get {
            key: b"absent".to_vec(),
            table: Some(CP_TABLE.into()),
        },
    )
    .await;
    assert_eq!(absent, ClientResponse::Value(None));

    // An **untagged** key round-trips the same way (there is only the CP plane; the
    // optional `table` no longer selects a plane).
    let untagged_put = call(
        addr0,
        ClientRequest::Put {
            key: b"u".to_vec(),
            value: b"u-value".to_vec(),
            table: None,
        },
    )
    .await;
    assert!(
        matches!(untagged_put, ClientResponse::PutOk),
        "untagged put failed: {untagged_put:?}"
    );
    let untagged_got = call(
        addr2,
        ClientRequest::Get {
            key: b"u".to_vec(),
            table: None,
        },
    )
    .await;
    assert_eq!(
        untagged_got,
        ClientResponse::Value(Some(b"u-value".to_vec()))
    );

    for n in &nodes {
        n.shutdown();
    }
}

/// Phase 2.3a — **CP member address distribution.** Each CP-group node registers
/// its `raftkv` listen address in the replicated control-plane `Metadata`
/// (`cp_member_addrs`), so a peer-sync loop on every node can reach a
/// runtime-created group member (a split sibling, a joined node). Here we assert
/// the bootstrap members register and the entries replicate cluster-wide: every
/// node sees a parseable address for each of the 3 CP group member ids
/// (`raftkv_id(0..3)` = 300/301/302).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cp_member_addresses_register_and_replicate() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let want: Vec<u64> = (0..3).map(animusd::config::raftkv_id).collect();
    let replicated = async {
        loop {
            // Every node's replicated view has a parseable address for all 3 members.
            let ok = nodes.iter().all(|n| {
                let m = n.metadata();
                want.iter().all(|id| {
                    m.cp_member_addrs
                        .get(id)
                        .and_then(|a| a.parse::<std::net::SocketAddr>().ok())
                        .is_some()
                })
            });
            if ok {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), replicated)
        .await
        .expect("CP member addresses did not register + replicate within 20s");

    for n in &nodes {
        n.shutdown();
    }
}
