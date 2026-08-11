//! ADR 0040 PR1 ("one identity per node"): proves the control-plane
//! `RaftNode` (which rides `PRIMARY_STREAM` = stream 0, ADR 0026) and a
//! second protocol instance's traffic on a *distinct* stream of the very
//! same node id — standing in for a per-tablet `RaftKvNode` group under the
//! merged combined-mode env this PR introduces — multiplex correctly on one
//! shared inbox, including under partition and a leader kill.
//!
//! This is deliberately **not** a raw wire-level unit test (the demux itself
//! is already proven in `animus-env`'s own
//! `prod::tests::prod_env_multiplexed_streams_do_not_cross_talk` and
//! `animus-sim`'s `determinism.rs::multiplexed_streams_are_isolated_and_deterministic`)
//! — it is an integration-level proof that a *real* production consumer (the
//! control `RaftNode`, with its actual election/replication workload) and a
//! second stream's traffic coexist under real fault injection on the same
//! `SimEnv` id, exactly the shape ADR 0040 PR1 relies on to justify merging
//! a combined node's two internal `ProdEnv`s into one.
//!
//! Byte-reproducible from the printed seed (`ANIMUS_SEED=<seed> cargo test
//! -p animus-control --test one_identity_multiplexing`).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::raft::ProposeResult;
use animus_control::{MetaCommand, NodeStatus, RaftNode};
use animus_env::{Clock, EnvExt, Network, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [NodeId; 3] = [nid(0), nid(1), nid(2)];

/// The stand-in "per-tablet" stream this test drives alongside the control
/// plane's own `PRIMARY_STREAM` (0) — any value `>= 1` mirrors a real tablet
/// id (ADR 0026/ADR 0022: tablet ids floor at 1), so it is guaranteed
/// disjoint from the control plane's stream by the same invariant PR1's
/// combined-mode merge relies on.
const TABLET_STREAM: u64 = 7;

/// How often each node pings its peers on [`TABLET_STREAM`].
const PING_INTERVAL: Duration = Duration::from_millis(20);

/// One "tablet-group" ping: a monotonic per-sender sequence number, so the
/// receiver can assert both delivery (it arrived at all) and ordering (it
/// arrived in order) — the two properties a buggy demux could violate.
fn encode_ping(seq: u64) -> Vec<u8> {
    seq.to_be_bytes().to_vec()
}

fn decode_ping(bytes: &[u8]) -> u64 {
    let arr: [u8; 8] = bytes.try_into().expect(
        "a TABLET_STREAM payload must be exactly 8 bytes — anything else means \
                  a foreign frame (e.g. a control-plane RaftMsg) leaked onto this stream",
    );
    u64::from_be_bytes(arr)
}

/// Per-(sender, receiver) tally of every sequence number this node has
/// observed on [`TABLET_STREAM`] from `sender` — `BTreeMap` throughout (no
/// `HashMap` in logic, ADR 0003).
type PingLog = Arc<Mutex<BTreeMap<NodeId, Vec<u64>>>>;

/// Spawn this node's `TABLET_STREAM` sender + receiver tasks on its own env —
/// the same env the control `RaftNode` (started separately, by the caller,
/// on the identical id) already occupies `PRIMARY_STREAM` of. Returns the
/// receiver's shared log for the test to inspect.
fn spawn_tablet_stream_traffic(env: &SimEnv, peers: &[NodeId]) -> PingLog {
    let log: PingLog = Arc::new(Mutex::new(BTreeMap::new()));

    // Receiver: drains TABLET_STREAM forever, decoding every envelope. A
    // decode failure (see `decode_ping`) would mean a foreign frame crossed
    // streams — a real demux bug — so it panics rather than silently
    // dropping, exactly like the analogous wire-level tests.
    {
        let log = log.clone();
        let recv_env = env.clone();
        env.spawn_task(async move {
            loop {
                let envelope = recv_env.recv_stream(TABLET_STREAM).await;
                let seq = decode_ping(&envelope.payload);
                log.lock()
                    .expect("ping log poisoned")
                    .entry(envelope.from)
                    .or_default()
                    .push(seq);
            }
        });
    }

    // Sender: pings every peer on a fixed cadence, stamping an
    // ever-increasing sequence number.
    {
        let send_env = env.clone();
        let peers = peers.to_vec();
        env.spawn_task(async move {
            let mut seq: u64 = 0;
            loop {
                for &peer in &peers {
                    send_env
                        .send_stream(peer, TABLET_STREAM, encode_ping(seq))
                        .await;
                }
                seq += 1;
                send_env.sleep(PING_INTERVAL).await;
            }
        });
    }

    log
}

fn upsert(node: NodeId) -> MetaCommand {
    MetaCommand::UpsertMember {
        node,
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

fn unique_leader(nodes: &[RaftNode<SimEnv>], live: &[usize], seed: u64) -> usize {
    let leaders: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "expected exactly one leader among {live:?}, found {leaders:?} (seed={seed})"
    );
    leaders[0]
}

/// Total pings a node's log has recorded from `sender`.
fn received_from(log: &PingLog, sender: NodeId) -> usize {
    log.lock()
        .expect("ping log poisoned")
        .get(&sender)
        .map_or(0, Vec::len)
}

/// Every recorded sequence-number run is strictly increasing — proves
/// `TABLET_STREAM` delivery is in-order per sender, not just "eventually all
/// arrive" (a demux that interleaved/reordered stream 7 with stream 0
/// traffic could still deliver every byte, just out of order).
fn assert_all_runs_ordered(log: &PingLog, seed: u64) {
    for (&sender, seqs) in log.lock().expect("ping log poisoned").iter() {
        for w in seqs.windows(2) {
            assert!(
                w[0] < w[1],
                "TABLET_STREAM delivery from {sender} went out of order: {seqs:?} (seed={seed})"
            );
        }
    }
}

/// The control plane's own `RaftNode` (stream 0) and a stand-in per-tablet
/// group's traffic (stream 7) coexist on the same three node ids for the
/// whole run — election, steady-state replication, a symmetric partition
/// that isolates one follower, heal, and a leader kill — with neither stream
/// ever starving or corrupting the other.
#[test]
fn control_raft_and_tablet_stream_traffic_multiplex_on_one_node_id() {
    let seed = 0x0040_0001;
    let mut sim = Simulator::new(seed);
    let envs: Vec<SimEnv> = NODES.iter().map(|&id| sim.env(id)).collect();

    // The control-plane RaftNode, exactly as `control_raft.rs` builds it —
    // one per id, riding PRIMARY_STREAM (stream 0) by default.
    let raft_nodes: Vec<RaftNode<SimEnv>> = envs
        .iter()
        .map(|env| RaftNode::start(env.clone(), NODES.to_vec(), MemoryEngine::new()))
        .collect();

    // The stand-in tablet-group traffic on the SAME ids/envs, stream 7.
    let ping_logs: Vec<PingLog> = envs
        .iter()
        .enumerate()
        .map(|(i, env)| {
            let peers: Vec<NodeId> = NODES.iter().copied().filter(|&id| id != NODES[i]).collect();
            spawn_tablet_stream_traffic(env, &peers)
        })
        .collect();

    // ---- Phase 1: elect + steady state -----------------------------------
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&raft_nodes, &[0, 1, 2], seed);

    assert!(matches!(
        raft_nodes[leader].propose(upsert(nid(10))),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));

    let reference = raft_nodes[leader].metadata();
    for (i, n) in raft_nodes.iter().enumerate() {
        assert_eq!(
            n.metadata(),
            reference,
            "control metadata diverged at node {i} before any fault (seed={seed})"
        );
    }
    assert!(reference.members.contains_key(&nid(10)));

    // Every node has heard from both peers on TABLET_STREAM by now, and
    // every run is in order.
    for (i, log) in ping_logs.iter().enumerate() {
        for &peer in &NODES {
            if peer == NODES[i] {
                continue;
            }
            assert!(
                received_from(log, peer) > 0,
                "node {i} never received a TABLET_STREAM ping from {peer} in steady state \
                 (seed={seed})"
            );
        }
    }
    for log in &ping_logs {
        assert_all_runs_ordered(log, seed);
    }

    // ---- Phase 2: partition a follower, both streams feel it -------------
    let follower = (0..3).find(|&i| i != leader).expect("a follower exists");
    let other = (0..3)
        .find(|&i| i != leader && i != follower)
        .expect("a third node exists");
    sim.partition_pair(NODES[follower], NODES[leader]);
    sim.partition_pair(NODES[follower], NODES[other]);

    let pre_partition_counts: Vec<usize> = (0..3)
        .map(|i| received_from(&ping_logs[i], NODES[follower]))
        .collect();
    sim.run_for(Duration::from_secs(2));

    // The partitioned follower's own control view stops advancing (it can
    // still see itself as a stale follower or start a doomed pre-vote, but
    // it must not somehow become — and stay — leader while cut off), and its
    // TABLET_STREAM traffic to/from the other two stalls exactly like its
    // control traffic does: partitioning is a per-*node* fault (ADR 0026:
    // one shared network link per id), not a per-stream one.
    assert!(
        !raft_nodes[follower].is_leader(),
        "a partitioned-away node must not be control leader (seed={seed})"
    );
    for i in 0..3 {
        if i == follower {
            continue;
        }
        let after = received_from(&ping_logs[i], NODES[follower]);
        assert_eq!(
            after, pre_partition_counts[i],
            "node {i} kept receiving TABLET_STREAM pings from the partitioned \
             node {follower} — partition did not isolate stream {TABLET_STREAM} \
             (seed={seed})"
        );
    }

    // ---- Phase 3: heal — both streams recover -----------------------------
    sim.heal(NODES[follower], NODES[leader]);
    sim.heal(NODES[follower], NODES[other]);
    sim.run_for(Duration::from_secs(2));

    for i in 0..3 {
        if i == follower {
            continue;
        }
        assert!(
            received_from(&ping_logs[i], NODES[follower]) > pre_partition_counts[i],
            "node {i} never resumed receiving TABLET_STREAM pings from {follower} after heal \
             (seed={seed})"
        );
    }
    for log in &ping_logs {
        assert_all_runs_ordered(log, seed);
    }

    // ---- Phase 4: kill the leader — control re-elects, tablet stream keeps
    // flowing among the survivors the whole time ---------------------------
    let old_term = raft_nodes[leader].term();
    let survivors: [usize; 2] = {
        let mut s = [0usize; 2];
        let mut idx = 0;
        for i in 0..3 {
            if i != leader {
                s[idx] = i;
                idx += 1;
            }
        }
        s
    };
    let pre_kill_counts: Vec<usize> = survivors
        .iter()
        .map(|&i| {
            let other = survivors.iter().copied().find(|&j| j != i).unwrap();
            received_from(&ping_logs[i], NODES[other])
        })
        .collect();

    sim.crash(NODES[leader]);
    sim.run_for(Duration::from_secs(3));

    let new_leader = unique_leader(&raft_nodes, &survivors, seed);
    assert!(
        raft_nodes[new_leader].term() > old_term,
        "new term should exceed the old one after the leader kill (seed={seed})"
    );
    let a = raft_nodes[survivors[0]].metadata();
    let b = raft_nodes[survivors[1]].metadata();
    assert_eq!(
        a, b,
        "survivor metadata diverged after leader kill (seed={seed})"
    );
    assert!(
        a.members.contains_key(&nid(10)),
        "pre-kill write lost (seed={seed})"
    );

    // The survivors' own TABLET_STREAM traffic to each other never stalled
    // because the *other* stream (0) was busy re-electing — proving the two
    // streams are genuinely independent under real fault injection, not
    // just in the idle steady-state case.
    for (idx, &i) in survivors.iter().enumerate() {
        let other = survivors.iter().copied().find(|&j| j != i).unwrap();
        assert!(
            received_from(&ping_logs[i], NODES[other]) > pre_kill_counts[idx],
            "survivor {i} stopped receiving TABLET_STREAM pings from {other} across the \
             leader kill/re-election (seed={seed})"
        );
    }
    for (i, log) in ping_logs.iter().enumerate() {
        if i == leader {
            continue;
        }
        assert_all_runs_ordered(log, seed);
    }

    // The control plane can still make progress on the new leader.
    assert!(matches!(
        raft_nodes[new_leader].propose(upsert(nid(11))),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));
    assert!(
        raft_nodes[survivors[0]]
            .metadata()
            .members
            .contains_key(&nid(11))
    );
    assert!(
        raft_nodes[survivors[1]]
            .metadata()
            .members
            .contains_key(&nid(11))
    );
}
