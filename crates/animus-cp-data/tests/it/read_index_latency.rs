//! Issue #1197: a linearizable read costs about **one heartbeat round trip**,
//! not a poll quantum.
//!
//! `read_barrier` used to learn that its `ReadProbe` quorum had acked, and that
//! the engine had applied the ReadIndex, by sleeping `READ_POLL` (20 ms) between
//! checks. The probe acks land within a couple of milliseconds, but the barrier
//! only looked again at the next 20 ms boundary, so a `ConsistentRead: true`
//! `GetItem` had a floor of ~21 ms regardless of how fast the network was. The
//! barrier now parks on event-driven wakes (a probe ack, an engine-applied
//! advance) and keeps `READ_POLL` only as a safety net.
//!
//! The network here is 1 ms base delay + up to 2 ms jitter each way, so one
//! probe round trip is at most 6 ms of virtual time. A barrier that still waits
//! on a poll tick cannot finish below 20 ms; the assertions below sit well
//! under that. Depth knob: `ANIMUS_READ_LATENCY_SEEDS` (default 4).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Clock, EnvExt, nid};
use animus_sim::{NetConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_test::corpus::{for_each_seed, seeds_from_env};

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;
/// A read's value and the virtual instant (ns) it completed at.
type Timed = (Option<Vec<u8>>, u64);

/// Worst-case one-way delivery delay configured below (1 ms base + 2 ms jitter).
const ONE_WAY_MAX: Duration = Duration::from_millis(3);
/// A probe round trip plus slack for the apply wake. Far below `READ_POLL`.
const READ_BUDGET: Duration = Duration::from_millis(12);

fn start(seed: u64) -> (Simulator, Vec<KvNode>, usize) {
    let mut sim = Simulator::new(seed);
    let mut cfg = NetConfig::default();
    cfg.base_delay = Duration::from_millis(1);
    cfg.max_jitter = Duration::from_millis(2);
    sim.set_net_config(cfg);
    let nodes: Vec<KvNode> = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));
    let ls: Vec<usize> = (0..3).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    (sim, nodes, ls[0])
}

/// Run one linearizable read to completion and return `(value, virtual time it
/// took)`. The completion instant is stamped by the task itself, so the result
/// is independent of how coarsely the sim is stepped.
fn timed_read(sim: &mut Simulator, node: &KvNode, key: &[u8]) -> (Option<Vec<u8>>, Duration) {
    let slot: Arc<Mutex<Option<Timed>>> = Arc::new(Mutex::new(None));
    let env = node.env().clone();
    let start = env.now().0;
    let (n, s, k) = (node.clone(), Arc::clone(&slot), key.to_vec());
    env.clone().spawn_task(async move {
        let v = n.linearizable_get(&k).await;
        *s.lock().unwrap() = Some((v, env.now().0));
    });
    sim.run_for(Duration::from_millis(200));
    let (v, end) = slot
        .lock()
        .unwrap()
        .clone()
        .expect("linearizable read did not complete");
    (v, Duration::from_nanos(end - start))
}

fn put_and_settle(sim: &mut Simulator, node: &KvNode, key: &[u8], value: &[u8]) {
    assert!(matches!(
        node.put(key.to_vec(), value.to_vec()),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_millis(500));
}

#[test]
fn a_linearizable_read_costs_a_round_trip_not_a_poll_tick() {
    for_each_seed(
        "read_index_latency_steady",
        seeds_from_env("ANIMUS_READ_LATENCY_SEEDS").max(4),
        |seed| {
            let (mut sim, nodes, l) = start(seed);
            put_and_settle(&mut sim, &nodes[l], b"k", b"v");

            // The first read after a quiet period may also have to drive a
            // `ReadCeiling` through the log (one more commit round trip); the
            // second is the pure ReadIndex path. Both must beat the old poll.
            for round in 0..3 {
                let (v, took) = timed_read(&mut sim, &nodes[l], b"k");
                assert_eq!(v.as_deref(), Some(&b"v"[..]), "seed={seed} round={round}");
                let budget = if round == 0 {
                    READ_BUDGET * 2
                } else {
                    READ_BUDGET
                };
                assert!(
                    took <= budget,
                    "linearizable read took {took:?} (> {budget:?}; one-way max {ONE_WAY_MAX:?}) \
                     — it is waiting on a poll tick instead of the probe ack (seed={seed} round={round})"
                );
                sim.run_for(Duration::from_millis(37));
            }
        },
    );
}

#[test]
fn a_linearizable_read_still_confirms_fast_with_one_follower_partitioned() {
    // Fault: one of the two followers is unreachable, so the quorum is the
    // leader plus the single live follower. The barrier must complete on that
    // one ack immediately, not after the dead peer's probe is given up on.
    for_each_seed(
        "read_index_latency_one_follower_down",
        seeds_from_env("ANIMUS_READ_LATENCY_SEEDS").max(4),
        |seed| {
            let (mut sim, nodes, l) = start(seed);
            put_and_settle(&mut sim, &nodes[l], b"k", b"v");
            let dead = (0..3).find(|&i| i != l).unwrap();
            sim.partition_pair(nid(l as u64), nid(dead as u64));
            sim.run_for(Duration::from_millis(300));
            assert!(nodes[l].is_leader(), "leader kept its quorum (seed={seed})");

            for round in 0..3 {
                let (v, took) = timed_read(&mut sim, &nodes[l], b"k");
                assert_eq!(v.as_deref(), Some(&b"v"[..]), "seed={seed} round={round}");
                let budget = if round == 0 {
                    READ_BUDGET * 2
                } else {
                    READ_BUDGET
                };
                assert!(
                    took <= budget,
                    "linearizable read took {took:?} (> {budget:?}) with one follower down \
                     (seed={seed} round={round})"
                );
                sim.run_for(Duration::from_millis(41));
            }
        },
    );
}

#[test]
fn a_read_after_a_fresh_write_waits_for_the_apply_not_a_poll_tick() {
    // The ReadIndex is the commit index of a write that was just acked; the
    // barrier must also wait for the engine to apply it. That wait is released
    // by the applied-watch bump, so it too must finish inside a round trip.
    for_each_seed(
        "read_index_latency_after_write",
        seeds_from_env("ANIMUS_READ_LATENCY_SEEDS").max(4),
        |seed| {
            let (mut sim, nodes, l) = start(seed);
            put_and_settle(&mut sim, &nodes[l], b"warm", b"x");
            let _ = timed_read(&mut sim, &nodes[l], b"warm");
            sim.run_for(Duration::from_millis(13));
            let ProposeResult::Accepted { index, .. } =
                nodes[l].put(b"k2".to_vec(), b"v2".to_vec())
            else {
                panic!("leader refused the write (seed={seed})");
            };
            // Step to the instant the write commits, then read straight away:
            // the read's ReadIndex covers it, so the barrier must also wait for
            // the engine to apply it.
            let mut guard = 0;
            while nodes[l].commit_index() < index {
                sim.run_for(Duration::from_micros(250));
                guard += 1;
                assert!(guard < 2000, "write never committed (seed={seed})");
            }
            let (v, took) = timed_read(&mut sim, &nodes[l], b"k2");
            assert_eq!(v.as_deref(), Some(&b"v2"[..]), "seed={seed}");
            assert!(
                took <= READ_BUDGET,
                "read racing a fresh write took {took:?} (seed={seed})"
            );
        },
    );
}
