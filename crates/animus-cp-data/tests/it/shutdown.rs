//! `RaftKvNode::shutdown` (drop-table GC, ADR 0024): a halted node's driver
//! exits and the node stops participating in its group — no more applies, no
//! more heartbeats — while the surviving replicas re-elect and keep serving.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`, never
//! `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::nid;
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn leader(nodes: &[KvNode], live: &[usize], seed: u64) -> usize {
    let ls: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among {live:?}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

fn put(nodes: &[KvNode], live: &[usize], seed: u64, key: &[u8], value: &[u8]) {
    let l = leader(nodes, live, seed);
    match nodes[l].put(key.to_vec(), value.to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected a put: {other:?} (seed={seed})"),
    }
}

/// A shut-down **follower** stops applying: it acknowledges the exit
/// (`is_stopped`), and a write committed after its halt never reaches its
/// engine while the live majority still applies it.
#[test]
fn halted_follower_stops_applying() {
    let seed = 0xD0D0;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect

    put(&nodes, &[0, 1, 2], seed, b"k1", b"v1");
    sim.run_for(Duration::from_secs(2)); // replicate + apply everywhere

    let l = leader(&nodes, &[0, 1, 2], seed);
    let follower = (0..3).find(|&i| i != l).expect("a follower exists");
    assert!(!nodes[follower].is_stopped());
    nodes[follower].shutdown();
    assert!(
        nodes[follower].is_halted(),
        "shutdown must latch (seed={seed})"
    );
    sim.run_for(Duration::from_secs(2)); // driver observes the flag on its next wake
    assert!(
        nodes[follower].is_stopped(),
        "driver must exit after shutdown (seed={seed})"
    );

    put(&nodes, &[l], seed, b"k2", b"v2");
    sim.run_for(Duration::from_secs(2));

    assert_eq!(
        block_on(nodes[follower].local_get(b"k1")),
        Some(b"v1".to_vec()),
        "pre-halt state stays readable locally (seed={seed})"
    );
    assert_eq!(
        block_on(nodes[follower].local_get(b"k2")),
        None,
        "a halted follower must not apply post-halt writes (seed={seed})"
    );
    assert_eq!(
        block_on(nodes[l].local_get(b"k2")),
        Some(b"v2".to_vec()),
        "the live majority still commits + applies (seed={seed})"
    );
}

/// A shut-down **leader** stops heartbeating, so the survivors re-elect and the
/// group keeps accepting writes — a halted node cannot wedge its group.
#[test]
fn survivors_reelect_after_leader_shutdown() {
    let seed = 0x0FF1;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect

    let old = leader(&nodes, &[0, 1, 2], seed);
    nodes[old].shutdown();
    sim.run_for(Duration::from_secs(5)); // halt observed; survivors time out + re-elect

    assert!(
        nodes[old].is_stopped(),
        "old leader's driver exited (seed={seed})"
    );
    let live: Vec<usize> = (0..3).filter(|&i| i != old).collect();
    let new = leader(&nodes, &live, seed);
    put(&nodes, &[new], seed, b"k", b"v");
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        block_on(nodes[new].local_get(b"k")),
        Some(b"v".to_vec()),
        "the re-elected group must keep serving writes (seed={seed})"
    );
}

/// **The halted-gated `persist_wal` error tolerance** (the animusd issue #278
/// item 1 fix): a `shutdown()` racing a still-pending WAL append/sync must
/// exit quietly, never panic, and never claim durability for a record that
/// never made it to disk.
///
/// `put` followed immediately by `shutdown()` — both synchronous, no `.await`
/// between them — queues a record in the core's own pending-persist buffer
/// and latches `halted` in the same beat, before the driver task is next
/// polled. Combined with a `DiskConfig` fault forcing every subsequent disk
/// op on this node to fail, the driver's very next `persist_wal` pass drains
/// that pending record and hits the injected fault while `halted` is already
/// true — exactly the race `persist_wal`'s halted gate exists to tolerate.
/// Before the fix this was a hard `.expect()` panic indistinguishable from a
/// genuine live durability fault; the assertion below is simply that the
/// driver exits cleanly instead (a panic anywhere in this test fails it).
#[test]
fn a_halted_nodes_pending_write_tolerates_a_wal_fault_with_no_panic() {
    let seed = 0xFA17;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect

    let l = leader(&nodes, &[0, 1, 2], seed);

    // Every append/sync/read/read_at/replace on the leader's node now fails.
    let mut fault = DiskConfig::default();
    fault.set_error_prob(1.0);
    sim.set_disk_config_for(nid(l as u64), fault);

    // Queue a write, then halt in the same synchronous beat — the driver task
    // cannot observe `halted` between these two calls (SimEnv only polls
    // tasks inside `run_for`/`run_until`), so the just-queued record is still
    // sitting in the core's pending-persist buffer when the driver's next
    // `persist_wal` pass finds `halted` already latched.
    match nodes[l].put(b"k".to_vec(), b"v".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected a put: {other:?} (seed={seed})"),
    }
    nodes[l].shutdown();

    // No panic (the halted gate tolerates the injected fault), and the driver
    // still exits cleanly despite never having durably persisted the queued
    // write.
    sim.run_for(Duration::from_secs(2));
    assert!(
        nodes[l].is_stopped(),
        "driver must exit after shutdown despite the injected disk fault (seed={seed})"
    );

    // The surviving majority is unaffected: it re-elects and keeps serving,
    // proving the halted node's tolerated fault stayed local.
    let live: Vec<usize> = (0..3).filter(|&i| i != l).collect();
    sim.run_for(Duration::from_secs(3)); // survivors time out + re-elect
    let new = leader(&nodes, &live, seed);
    put(&nodes, &[new], seed, b"k2", b"v2");
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        block_on(nodes[new].local_get(b"k2")),
        Some(b"v2".to_vec()),
        "the surviving majority must keep serving after the halted node's tolerated fault \
         (seed={seed})"
    );
}

/// **The animusd issues #282/#279 fix's own load-bearing assumption**: bare
/// `Node::shutdown()`/`Drop` (`animusd`, which has no `SimEnv`, so cannot
/// exercise the real race itself) latch `halted` on *every* locally hosted
/// CP group, not just a leading one — because a hard-killed or panic-unwound
/// process has no way to know in advance which of its hosted groups happen to
/// be leading at that instant. This proves the halted-gate mechanism this
/// fix leans on holds for a **follower** too: `persist_wal` runs identically
/// on every replica (a follower persists its own copy of each entry as it
/// arrives via `AppendEntries`), so a follower racing a disk fault right
/// after being halted must tolerate it exactly like a leader racing one after
/// its own local `put` does above — never panic.
///
/// Unlike the leader case above, a follower can't queue a pending write with
/// a direct synchronous call — the write only reaches it over the network,
/// which only progresses inside `run_for`. So this arms the fault and the
/// halt on the follower *before* the leader ever proposes, then proposes and
/// drives the sim: the follower's own halted latch stays set the whole time,
/// so whenever `AppendEntries` for the new entry arrives and its own
/// `persist_wal` pass hits the injected fault, halted is unquestionably
/// already true (a stronger guarantee than a tight race, and the shape a
/// process-level kill actually produces — halted latches once, well before
/// the process's I/O has any chance to fail).
#[test]
fn a_halted_followers_incoming_write_tolerates_a_wal_fault_with_no_panic() {
    let seed = 0xF011;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect

    let l = leader(&nodes, &[0, 1, 2], seed);
    let follower = (0..3).find(|&i| i != l).expect("a follower exists");

    // Every append/sync/read/read_at/replace on the follower's node now
    // fails.
    let mut fault = DiskConfig::default();
    fault.set_error_prob(1.0);
    sim.set_disk_config_for(nid(follower as u64), fault);

    // Halt the follower before it has seen the write at all — mirroring what
    // a bare-killed process's `halt_hosted_cp_groups` does to every hosted
    // group, leader or not, before any of their drivers gets to run again.
    nodes[follower].shutdown();

    // The leader proposes and replicates normally (its own disk is fine); the
    // halted follower's own persist_wal pass for the incoming entry must hit
    // the injected fault and tolerate it rather than panic.
    put(&nodes, &[l], seed, b"k", b"v");
    sim.run_for(Duration::from_secs(2));

    assert!(
        nodes[follower].is_stopped(),
        "the halted follower's driver must exit despite the injected disk fault (seed={seed})"
    );

    // The live majority (leader + the other follower) is unaffected.
    assert_eq!(
        block_on(nodes[l].local_get(b"k")),
        Some(b"v".to_vec()),
        "the live leader must still commit + apply despite the halted follower's tolerated \
         fault (seed={seed})"
    );
}
