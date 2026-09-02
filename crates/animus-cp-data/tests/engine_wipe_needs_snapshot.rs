//! Issue #554: a replica whose Raft log survives a restart but whose local
//! engine does not (wiped and reopened fresh — `host.rs`'s destroy-and-
//! reopen recovery, or any other way the two fall out of sync) must not
//! silently believe it is caught up once its log has been compacted past
//! what the fresh engine holds. `RaftCore::state_machine_behind` +
//! `AppendEntriesResp::needs_snapshot` is the fix: the replica detects the
//! gap at `drive()` start (`applied.rs`'s durable watermark, read back below
//! `RaftCore::snapshot_index`), refuses reads/campaigning, and its next
//! `AppendEntriesResp` — regardless of `next_index`, which its own intact
//! log tail can satisfy on its own — tells its leader to ship a fresh
//! `InstallSnapshot` built at the LEADER's current applied index.
//!
//! Both scenarios here write well past `COMPACT_THRESHOLD` (64) so the
//! group's log is genuinely compacted before the wipe — below that
//! threshold the bug this regresses is invisible (the log tail alone still
//! covers everything, see the issue's own writeup).

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

const NODES: [u64; 3] = [700, 701, 702];
const N: u64 = 90; // past COMPACT_THRESHOLD (64)

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn spawn(sim: &Simulator, id: u64, engine: MemoryEngine) -> KvNode {
    RaftKvNode::start(
        sim.env(nid(id)),
        NODES.iter().copied().map(nid).collect(),
        engine,
    )
}

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| spawn(&sim, id, MemoryEngine::new()))
        .collect();
    (sim, nodes)
}

fn leader_idx(nodes: &[KvNode], seed: u64) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(_, n)| n.is_leader())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(ls.len(), 1, "expected exactly one leader (seed={seed})");
    ls[0]
}

fn write_past_compaction(nodes: &[KvNode], leader: usize, seed: u64) {
    for i in 0..N {
        match nodes[leader].put(
            format!("k{i:04}").into_bytes(),
            format!("v{i}").into_bytes(),
        ) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected put {i}: {other:?} (seed={seed})"),
        }
    }
}

/// Run `fut` to completion by spawning it and driving `sim`, returning
/// `None` if it didn't complete within `budget` — needed for `linearizable_get`
/// (its read-barrier quorum probe is a real network round trip, so a bare
/// `block_on` never completes: nothing else would advance the simulated
/// clock or deliver the probe's messages). Mirrors `snapshot_catchup.rs`'s
/// own `drive` helper.
fn drive<T: Send + 'static>(
    sim: &mut Simulator,
    env: &SimEnv,
    budget: Duration,
    fut: impl Future<Output = T> + Send + 'static,
) -> Option<T> {
    let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let s = Arc::clone(&slot);
    env.clone().spawn_task(async move {
        let v = fut.await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock().unwrap().take()
}

fn assert_every_key_reads_back(node: &KvNode, seed: u64, who: &str) {
    for i in 0..N {
        let key = format!("k{i:04}").into_bytes();
        assert_eq!(
            block_on(node.local_get(&key)),
            Some(format!("v{i}").into_bytes()),
            "{who} missing k{i:04} after recovery (seed={seed})"
        );
    }
}

/// A wiped **follower**: its Raft WAL survives (same simulated node id,
/// `sim.stop` keeps durable disk), but its engine is a brand-new, empty
/// `MemoryEngine` — modelling `host.rs`'s destroy-and-reopen recovery. The
/// group's log has already been compacted past `snapshot_index` by the time
/// this happens, so a fresh engine holds none of the compacted prefix and
/// the only way back is a leader-shipped `InstallSnapshot`.
#[test]
fn a_wiped_follower_past_compaction_requests_and_installs_a_snapshot() {
    let seed = 0x5544_0001;
    let (mut sim, mut nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect
    let l = leader_idx(&nodes, seed);

    write_past_compaction(&nodes, l, seed);
    sim.run_for(Duration::from_secs(3)); // replicate + apply + compact everywhere

    let leader_snapshot_index = nodes[l].snapshot_index();
    assert!(
        leader_snapshot_index > 0,
        "seed={seed}: sanity — the leader's own log must have compacted \
         past the threshold by now (snapshot_index={leader_snapshot_index})"
    );

    let victim = (0..3).find(|&i| i != l).expect("a follower exists");

    // Sanity: genuinely caught up (and durably compacted, same as the
    // leader) before the wipe.
    assert_eq!(
        nodes[victim].engine_applied_index(),
        nodes[l].engine_applied_index(),
        "seed={seed}: victim must be caught up before its engine is wiped"
    );
    assert!(
        nodes[victim].snapshot_index() > 0,
        "seed={seed}: sanity — the victim's own log must be compacted too, \
         so the wipe below actually exercises the gap"
    );

    // "Process exit": durable WAL survives, engine handle is dropped.
    sim.stop(nid(NODES[victim]));
    // "Reopened fresh": same node id, same WAL, a BRAND NEW empty engine —
    // never the retained handle.
    nodes[victim] = spawn(&sim, NODES[victim], MemoryEngine::new());

    sim.run_for(Duration::from_secs(6)); // request + chunked InstallSnapshot + tail replay

    assert_eq!(
        nodes[victim].engine_applied_index(),
        nodes[l].engine_applied_index(),
        "seed={seed}: wiped follower's engine_applied_index must converge \
         to the group's after the snapshot request/install"
    );
    assert_every_key_reads_back(&nodes[victim], seed, "wiped follower");

    // The eventual leader (not necessarily the original — irrelevant here)
    // must still serve a correct linearizable read for a key that only ever
    // lived in the compacted prefix.
    let l2 = leader_idx(&nodes, seed);
    let key = b"k0000".to_vec();
    let leader_env = sim.env(nid(NODES[l2]));
    let leader_handle = nodes[l2].clone();
    let got = drive(&mut sim, &leader_env, Duration::from_secs(2), async move {
        leader_handle.linearizable_get(&key).await
    });
    assert_eq!(
        got,
        Some(Some(b"v0".to_vec())),
        "seed={seed}: linearizable read of a pre-compaction key must be correct \
         (None = the read didn't complete within budget)"
    );
}

/// The wiped node is the group's own **leader** at the moment of the wipe.
/// Per `RaftCore::recovered`, a restart always comes back as a plain
/// `Follower` (leadership is never persisted), so this replica cannot
/// silently keep serving as leader over an incomplete engine — it simply
/// rejoins as an ordinary (temporarily behind) follower, the surviving two
/// replicas elect a new leader among themselves, and the design's
/// `state_machine_behind` campaign gate (`RaftCore::start_pre_vote`/
/// `start_election`) additionally stops the recovering replica from ever
/// WINNING a future election until it has caught up — so it never resumes
/// leading on top of a hollow engine. Documented here as the resolution of
/// the "wiped node is the leader" design fork the issue called out.
#[test]
fn a_wiped_former_leader_past_compaction_steps_aside_and_catches_up() {
    let seed = 0x5544_0002;
    let (mut sim, mut nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect
    let l = leader_idx(&nodes, seed);

    write_past_compaction(&nodes, l, seed);
    sim.run_for(Duration::from_secs(3)); // replicate + apply + compact everywhere

    assert!(
        nodes[l].snapshot_index() > 0,
        "seed={seed}: sanity — the leader's own log must have compacted \
         past the threshold by now"
    );
    let pre_wipe_applied = nodes[l].engine_applied_index();

    // Wipe the LEADER itself.
    sim.stop(nid(NODES[l]));
    nodes[l] = spawn(&sim, NODES[l], MemoryEngine::new());

    sim.run_for(Duration::from_secs(8)); // new election among survivors + catch-up

    // A new leader exists among the group (possibly the recovered node
    // itself, but only once it has genuinely caught back up — see below).
    let l2 = leader_idx(&nodes, seed);

    // The recovered node's engine must have converged — whether or not it
    // is the new leader — and must never have been trusted to serve as
    // leader while still hollow: if it IS the new leader now, its own
    // engine_applied_index must already be caught up to what it held before
    // the wipe (never regressed, never served from an incomplete engine).
    assert_eq!(
        nodes[l].engine_applied_index(),
        nodes[l2].engine_applied_index(),
        "seed={seed}: recovered node must have fully converged"
    );
    assert!(
        nodes[l].engine_applied_index() >= pre_wipe_applied,
        "seed={seed}: recovered node must have caught back up to at least \
         what it held before the wipe"
    );
    assert_every_key_reads_back(&nodes[l], seed, "recovered former leader");

    let key = b"k0000".to_vec();
    let leader_env = sim.env(nid(NODES[l2]));
    let leader_handle = nodes[l2].clone();
    let got = drive(&mut sim, &leader_env, Duration::from_secs(2), async move {
        leader_handle.linearizable_get(&key).await
    });
    assert_eq!(
        got,
        Some(Some(b"v0".to_vec())),
        "seed={seed}: linearizable read of a pre-compaction key must be correct \
         through the post-wipe leader (None = the read didn't complete within budget)"
    );
}
