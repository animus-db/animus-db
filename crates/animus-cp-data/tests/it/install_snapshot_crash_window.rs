//! Issue #1368: a follower that installs a leader snapshot while the leader
//! keeps writing persists the leader's next `AppendEntries` (indexes past the
//! new base) onto its WAL BEFORE the apply task's deferred rewrite records the
//! installed base. Replay is positional, so a process stop in that window
//! recovered `[old tail] ++ [new entries]` -- a gap in which every position
//! holds the wrong index -- and the leader's resend later appended an entry
//! twice (the soak's `assert_ts_monotonic` panic on two equal HLC stamps).
//!
//! Real data-plane path (`RaftKvNode`, SimEnv): a lagging replica (down past
//! the leader's retention cap) heals via a pure `InstallSnapshot` under a
//! continuous writer. The on-disk WAL is replayed after every millisecond of
//! the catch-up (a crash at that instant recovers exactly that file) and must
//! be positionally contiguous; and a process stopped at the first moment the
//! WAL shows post-install appends must restart, rejoin and converge (not
//! halted, no panic). With the `set_wal_records_install` opt-in off (the
//! pre-fix behaviour) both fail.
//!
//! `ANIMUS_SEED=<decimal> cargo test -p animus-cp-data --test it
//! install_snapshot_crash_window::` replays one seed.

use std::time::Duration;

use animus_control::{PersistedState, ProposeResult};
use animus_cp_data::{KvCommand, KvState, RaftKvNode, wal_file};
use animus_env::{Disk, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
/// Past the leader's `COMPACT_RETENTION_CAP_ENTRIES` (4096), so the down
/// replica is excluded from the compaction floor and needs a snapshot.
const WRITES: u64 = 2_300;
const STEPS_MS: u64 = 3_000;

type Node = RaftKvNode<SimEnv, MemoryEngine>;

fn start(sim: &Simulator, id: u64, engine: &MemoryEngine) -> Node {
    Node::start(
        sim.env(nid(id)),
        NODES.iter().copied().map(nid).collect(),
        engine.clone(),
    )
}

fn leader(nodes: &[Node]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    (ls.len() == 1).then(|| ls[0])
}

fn put(n: &Node, i: u64, seed: u64) {
    match n.put(
        format!("k{i:05}").into_bytes(),
        format!("v{i}").into_bytes(),
    ) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected put {i}: {other:?} (seed={seed})"),
    }
}

struct Rig {
    sim: Simulator,
    nodes: Vec<Node>,
    engines: Vec<MemoryEngine>,
    l0: usize,
    lag: usize,
    next: u64,
    seed: u64,
}

impl Rig {
    /// Elect, take one replica down, write past the retention cap, compact,
    /// then heal it (the slow `Disk::replace` keeps the apply task's WAL
    /// rewrite well behind the consensus loop's appends).
    fn healing(seed: u64) -> Self {
        let mut sim = Simulator::new(seed);
        let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
        let nodes: Vec<Node> = NODES
            .iter()
            .map(|&i| start(&sim, i, &engines[i as usize]))
            .collect();
        sim.run_for(Duration::from_secs(2));
        let l0 = leader(&nodes).unwrap_or_else(|| panic!("no leader (seed={seed})"));
        let lag = (0..3).find(|&i| i != l0).unwrap();
        let mut slow = DiskConfig::default();
        slow.set_replace_data_delay(Duration::from_millis(100));
        sim.set_disk_config_for(nid(lag as u64), slow);

        sim.crash(nid(lag as u64));
        for i in 0..2 * WRITES {
            put(&nodes[l0], i, seed);
            if i % 100 == 99 {
                sim.run_for(Duration::from_millis(200));
            }
        }
        sim.run_for(Duration::from_secs(4)); // leader compacts past `lag`
        sim.restart(nid(lag as u64));
        Rig {
            sim,
            nodes,
            engines,
            l0,
            lag,
            next: 2 * WRITES,
            seed,
        }
    }

    /// Advance 1 ms with the leader still writing.
    fn step(&mut self) {
        put(&self.nodes[self.l0], self.next, self.seed);
        self.next += 1;
        self.sim.run_for(Duration::from_millis(1));
    }

    fn replayed(&self) -> PersistedState<KvCommand, KvState> {
        let env = self.sim.env(nid(self.lag as u64));
        let bytes = block_on(env.read(&wal_file(0))).unwrap_or_default();
        PersistedState::replay(
            PersistedState::<KvCommand, KvState>::decode(&bytes).expect("WAL decodes"),
        )
    }
}

#[test]
fn the_wal_of_a_replica_healing_by_install_snapshot_is_always_replayable() {
    let seeds: Vec<u64> = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(|s| vec![s])
        .unwrap_or_else(|| vec![136_800, 136_801]);
    let mut bad = Vec::new();
    for seed in seeds {
        let mut rig = Rig::healing(seed);
        for ms in 0..STEPS_MS {
            rig.step();
            let st = rig.replayed();
            if let Err(e) = st.check_log_contiguous() {
                bad.push(format!("seed={seed} +{ms}ms: {e}"));
                break;
            }
        }
    }
    assert!(
        bad.is_empty(),
        "issue #1368: misaligned WAL replays:\n{}",
        bad.join("\n")
    );
}

/// A process stop at the first moment post-install appends are durable must
/// recover: not halted, and converged with the leader (the pre-fix recovery
/// duplicated an entry and tripped `assert_ts_monotonic` in the apply task).
#[test]
fn a_restart_right_after_the_first_post_install_append_rejoins_and_converges() {
    let seed: u64 = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(136_800);
    let mut rig = Rig::healing(seed);
    let mut stopped = false;
    for _ in 0..STEPS_MS {
        rig.step();
        if rig.replayed().log.len() > 1 {
            stopped = true;
            break;
        }
    }
    assert!(
        stopped,
        "the replica never persisted a post-heal append (seed={seed})"
    );
    rig.sim.stop(nid(rig.lag as u64));
    rig.nodes[rig.lag] = start(&rig.sim, rig.lag as u64, &rig.engines[rig.lag]);
    for _ in 0..20 {
        rig.step();
    }
    rig.sim.run_for(Duration::from_secs(10));
    assert!(
        !rig.nodes[rig.lag].is_halted(),
        "restarted replica halted (seed={seed})"
    );
    assert_eq!(
        rig.nodes[rig.lag].engine_applied_index(),
        rig.nodes[rig.l0].engine_applied_index(),
        "restarted replica must converge (seed={seed})"
    );
}
