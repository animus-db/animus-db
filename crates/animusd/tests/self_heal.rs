//! Observable **self-healing** in the assembled `animusd` node (ADR 0005 + 0012).
//!
//! The autonomous control-plane behaviors are proven deterministically in
//! `animus-control` under `SimEnv`; this test proves they are actually **wired
//! into the running binary** over real `ProdEnv`/TCP:
//!
//! 1. bring up a 4-node cluster (so the bootstrap tablet's replication factor of
//!    3 leaves one **spare** data node), write a key;
//! 2. kill one node that holds a tablet replica (and is not the control leader,
//!    to keep the cluster's Raft quorum intact);
//! 3. assert the cluster **detects** the failure (the dead data member is marked
//!    `Down` in the replicated metadata) and **re-places** the tablet off it onto
//!    the live spare (the dead replica leaves the set, the epoch bumps);
//! 4. assert reads of the previously-written key **still succeed** via the
//!    surviving replicas.
//!
//! Like the other `animusd` tests this is real TCP/time, so it polls with
//! generous timeouts rather than asserting deterministic timing.
//!
//! There is also a concurrency smoke test (mirroring
//! `animus-storage/tests/lsm_concurrent.rs`): many clients hammering the
//! assembled node concurrently complete without deadlock.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::config::data_id;
use animusd::{ClientRequest, ClientResponse, Node, bind_cluster, start_cluster};
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

/// Wait until a leader is elected and every node has the bootstrap tablet.
async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            let leader = nodes.iter().any(Node::is_control_leader);
            let everyone_has_tablet = nodes.iter().all(|n| !n.metadata().tablets.is_empty());
            if leader && everyone_has_tablet {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not elect a leader and bootstrap within 20s");
}

/// The index of the current control leader (panics if none after a short wait).
async fn leader_index(nodes: &[Node]) -> usize {
    let find = async {
        loop {
            if let Some(i) = nodes.iter().position(Node::is_control_leader) {
                return i;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(10), find)
        .await
        .expect("no control leader")
}

/// The replica set of the single bootstrap tablet, as a surviving node sees it.
fn tablet_replicas(node: &Node) -> Vec<u64> {
    let meta = node.metadata();
    let (_, t) = meta.tablets.iter().next().expect("a bootstrap tablet");
    t.replicas.clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_detects_a_dead_node_and_self_heals_placement() {
    let dir = tempfile::tempdir().unwrap();
    // Four nodes: the tablet's RF is capped at 3, so data node index 3 (data id
    // 103) is a spare the leader can move a failed replica onto.
    let bound = bind_cluster(4, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    // R = W = 2 (over the 3-replica tablet) so a read tolerates one missing
    // replica — exactly the condition during/after a re-placement.
    let nodes = start_cluster(bound, 2, 2).await.unwrap();
    await_bootstrap(&nodes).await;

    // Write a key while the cluster is healthy.
    let writer = nodes[0].client_addr();
    let put = call(
        writer,
        ClientRequest::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            table: None,
        },
    )
    .await;
    assert!(matches!(put, ClientResponse::PutOk), "put failed: {put:?}");

    // Choose a victim that (a) holds a tablet replica and (b) is not the control
    // leader, so killing it leaves the 4-node Raft quorum intact and no
    // re-election is needed for the detector to act.
    let leader = leader_index(&nodes).await;
    let initial_replicas = tablet_replicas(&nodes[0]);
    let victim_index = (0..4)
        .find(|&i| i != leader && initial_replicas.contains(&data_id(i)))
        .expect("a replica node that is not the leader");
    let victim_data_id = data_id(victim_index);
    let initial_epoch = nodes[0].metadata().tablets.values().next().unwrap().epoch;

    // A node we keep, to observe the cluster's converged state and serve reads.
    let observer_index = (0..4)
        .find(|&i| i != victim_index && i != leader)
        .expect("a surviving non-leader observer");
    let observer_client = nodes[observer_index].client_addr();

    // --- Kill the victim node (stops its data heartbeat + replica). ---
    nodes[victim_index].shutdown();

    // The leader's failure detector must mark the dead data member `Down`, and the
    // placement reconciler must then move the tablet off it onto the spare,
    // bumping the epoch. Both are autonomous (no operator, no test-driven CAS).
    let healed = async {
        loop {
            let meta = nodes[observer_index].metadata();
            let down = meta
                .members
                .get(&victim_data_id)
                .is_some_and(|m| m.status == animusd::NodeStatus::Down);
            let t = meta.tablets.values().next().unwrap();
            let moved_off = !t.replicas.contains(&victim_data_id);
            let bumped = t.epoch > initial_epoch;
            if down && moved_off && bumped {
                return t.replicas.clone();
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    let new_replicas = timeout(Duration::from_secs(30), healed)
        .await
        .expect("cluster did not detect the failure and re-place the tablet within 30s");

    // The re-placement reused the spare (data id 103) and kept the survivors.
    assert!(
        new_replicas.contains(&data_id(3)),
        "spare not brought in: {new_replicas:?}"
    );
    assert!(
        !new_replicas.contains(&victim_data_id),
        "dead replica still placed: {new_replicas:?}"
    );

    // Reads of the previously-written key still succeed via the survivors.
    let got = call(
        observer_client,
        ClientRequest::Get {
            key: b"k".to_vec(),
            table: None,
        },
    )
    .await;
    assert_eq!(
        got,
        ClientResponse::Value(Some(b"v".to_vec())),
        "read did not survive the node failure (got {got:?})",
    );

    for (i, node) in nodes.iter().enumerate() {
        if i != victim_index {
            node.shutdown();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn assembled_node_handles_concurrent_client_load_without_deadlock() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound, 2, 2).await.unwrap();
    await_bootstrap(&nodes).await;

    let addr = nodes[0].client_addr();

    // Many clients concurrently put-then-get distinct keys. The single coord
    // inbox is serialized per node behind a lock; this asserts that serialization
    // does not deadlock or starve under concurrent load.
    let mut handles = Vec::new();
    for c in 0..16u32 {
        handles.push(tokio::spawn(async move {
            for r in 0..8u32 {
                let key = format!("c{c}-r{r}").into_bytes();
                let value = format!("v{c}-{r}").into_bytes();
                let put = call(
                    addr,
                    ClientRequest::Put {
                        key: key.clone(),
                        value: value.clone(),
                        table: None,
                    },
                )
                .await;
                assert!(matches!(put, ClientResponse::PutOk), "put failed: {put:?}");
                let got = call(addr, ClientRequest::Get { key, table: None }).await;
                assert_eq!(got, ClientResponse::Value(Some(value)));
            }
        }));
    }

    // The whole concurrent workload must finish well within this bound; a
    // deadlock would hang until the timeout fires.
    let all = async {
        for h in handles {
            h.await.expect("client task panicked");
        }
    };
    timeout(Duration::from_secs(30), all)
        .await
        .expect("concurrent client load did not complete in 30s (possible deadlock)");

    for node in &nodes {
        node.shutdown();
    }
}
