//! Regression for GitHub issue #811: a genuine process restart
//! (`Simulator::stop` + a fresh `RaftKvNode::start` on the same
//! engine/disk) of a live, **fully caught-up** voter of a 3-replica
//! `RaftKvNode` group — while the other two replicas stay live — hung
//! forever: one CPU core pinned, `Simulator::run_for`/`run_until` never
//! returning even for a 50ms virtual-time window.
//!
//! # The mechanism
//!
//! The hang is specific to a replica whose only path to being caught up was
//! a pure `InstallSnapshot` (never an ordinary logged `AppendEntries` tail):
//!
//! - `RaftCore::handle_install_snapshot`'s successful-install path
//!   (`animus-control::raft`) durably fixes up the CORE's in-memory
//!   `snapshot_index`/log/`snapshot_dirty` synchronously, on the consensus
//!   loop, the moment the last chunk lands. But nothing ever flushes that
//!   to the WAL FILE at that point: `RaftCore::has_unflushed_wal` (the
//!   consensus loop's own ordinary per-message persist gate) checks only
//!   the pending log-append queue and the current term/vote, never
//!   `snapshot_dirty`; and `apply_and_compact`'s own compaction pass (the
//!   *other* WAL writer, on the apply task) only actually rewrites the WAL
//!   when `behind >= COMPACT_THRESHOLD` or a peer is waiting on a fresh
//!   image (`image_needed`) — and immediately after an install, `behind`
//!   (`engine_applied - snapshot_index`) is `0`, since both are set to the
//!   identical `last_index` in the same step. So the just-installed
//!   snapshot state sits correct in memory but never reaches disk.
//! - A replica caught up ENTIRELY this way (no log entry of its own ever
//!   logged) can sit fully caught-up indefinitely with a WAL file that
//!   still reads back empty.
//! - A later **genuine process restart** (`Simulator::stop` + a fresh
//!   `RaftKvNode::start` on the SAME engine — never `Simulator::crash`/
//!   `Simulator::restart`, which mute/re-arm the SAME still-live in-memory
//!   `RaftCore` and never touch the WAL at all) recovers from that empty
//!   WAL: `drive`'s `fresh_group` check is true, so `RaftCore::recovered`
//!   is skipped entirely and the fresh core keeps `snapshot_index ==
//!   last_applied == 0` — while `engine_applied` is correctly reseeded from
//!   the ENGINE's own durable applied-watermark marker (already caught up,
//!   e.g. `201`, since the engine handle itself is the same one carried
//!   over the restart).
//! - `behind` is then permanently `engine_applied` itself:
//!   `RaftCore::snapshot_upto(ea)` clamps to `ea.min(last_applied)`, and
//!   `last_applied` is ALSO stuck at `0` on this fresh, never-recovered
//!   core, so it can never advance `snapshot_index` past `0`. `behind`
//!   never shrinks, the compaction threshold is crossed on every single
//!   pass, and the apply task spins `did_work = true` forever: `apply_loop`
//!   never reaches its idle `select(ApplyPending, sleep(..))`, so
//!   `SimEnv`'s executor never advances virtual time and `run_for`/
//!   `run_until` never return.
//!
//! **Fixed** in `apply_and_compact` (`lib.rs`): processing a
//! `drain_pending_install` now unconditionally forces the compaction
//! section's WAL-rewrite branch in the SAME pass, regardless of `behind`/
//! `image_needed` — durably recording the just-installed `snapshot_index`
//! (and the, here, empty log) before any later restart can ever race it.
//!
//! # Why the existing corpora never caught this
//!
//! `crates/animus-test/tests/raftkv_linearizable.rs`'s stop/restart cells
//! never chain a genuine `InstallSnapshot` catch-up immediately before a
//! `Simulator::stop`+reconstruct restart of the SAME replica — the
//! ingredient this bug needs is specifically "the replica being restarted
//! has NEVER logged an ordinary entry of its own, only ever installed a
//! snapshot." `crates/animus-cp-data/tests/hlc_differential_skew.rs`
//! (issue #804) needed exactly this shape too and is where this bug was
//! first found — its own regressions deliberately stay restart-free (see
//! that file's module doc) specifically because of this still-open issue.
//!
//! # Reproduction
//!
//! A wall-clock watchdog thread bounds the hang: `SimEnv`'s `run_for`/
//! `run_until`/`run_until_quiescent` all bound *virtual* time or *timeline
//! step count* — neither helps when the busy loop never reaches the
//! timeline at all (an idle-select that resolves instantly, on every poll,
//! with no timer ever registered), which is exactly this bug's shape: a
//! single task's own `Future::poll` never returns control to the executor.
//! Only a real OS-thread wall-clock bound can catch that.
//!
//! Seeded and replayable: `ANIMUS_SEED=<decimal seed> cargo test -p
//! animus-cp-data --test restart_after_install_snapshot`.

use std::sync::mpsc;
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, MemoryEngine};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
/// Real writes, then a failed-CAS burst — each comfortably past `lib.rs`'s
/// own `COMPACT_THRESHOLD` (64) so the sender's compaction genuinely runs
/// and the lagging replica must catch up via a pure `InstallSnapshot`, not
/// an ordinary log replay.
const REAL_WRITES: u64 = 100;
const FAILED_CAS: u64 = 100;
/// Generous — the hang this test guards against pins a real CPU core
/// indefinitely; a healthy run finishes in well under 100ms of wall time.
const WATCHDOG_BUDGET: Duration = Duration::from_secs(20);

type MemNode = RaftKvNode<SimEnv, MemoryEngine>;

fn mem_group(seed: u64) -> (Simulator, Vec<MemNode>, Vec<MemoryEngine>) {
    let sim = Simulator::new(seed);
    let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
    let nodes = NODES
        .iter()
        .zip(engines.iter())
        .map(|(&id, engine)| {
            MemNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                engine.clone(),
            )
        })
        .collect();
    (sim, nodes, engines)
}

fn leader_among<E: animus_env::Env, S: animus_storage::StorageEngine + 'static>(
    nodes: &[RaftKvNode<E, S>],
) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

/// Drives `sim.run_for(dur)` on a dedicated OS thread and bounds it by a
/// real wall-clock budget — see this file's module doc for why neither
/// `run_until_quiescent`'s step cap nor `run_for`'s own virtual-time
/// deadline can catch this specific hang shape (the busy task's own poll
/// never returns, so the executor's step/timeline machinery never gets a
/// chance to intervene). Consumes `sim`/every remaining `RaftKvNode`
/// handle so nothing outside this call can still reach the (possibly
/// still-spinning, on a genuine hang) simulated world.
fn drive_bounded(mut sim: Simulator, dur: Duration, budget: Duration) -> bool {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        sim.run_for(dur);
        let _ = tx.send(());
    });
    rx.recv_timeout(budget).is_ok()
}

/// The full scenario: elect, partition one replica, cross
/// `COMPACT_THRESHOLD` twice (real writes, then a failed-CAS burst so the
/// sender's own compaction truncates its WAL through both), heal the
/// partitioned replica purely via `InstallSnapshot`, then genuinely
/// restart THAT replica (`Simulator::stop` + fresh `RaftKvNode::start` on
/// the same engine) while the other two stay live. Returns whether a
/// following `run_for(50ms)` completed within `WATCHDOG_BUDGET` of real
/// wall time.
fn run_scenario(seed: u64) -> bool {
    let (mut sim, mut nodes, engines) = mem_group(seed);
    sim.run_for(Duration::from_secs(2)); // elect
    let l0 = leader_among(&nodes).unwrap_or_else(|| panic!("no initial leader (seed={seed})"));
    let lagging = (0..3)
        .find(|&i| i != l0)
        .expect("a non-leader replica exists");

    // Partition `lagging`: it must catch up later via a pure
    // `InstallSnapshot` — its own log start will be long gone by then.
    sim.crash(nid(lagging as u64));

    for i in 0..REAL_WRITES {
        match nodes[l0].put(
            format!("k{i:04}").into_bytes(),
            format!("v{i}").into_bytes(),
        ) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected real put {i}: {other:?} (seed={seed})"),
        }
    }
    sim.run_for(Duration::from_secs(2)); // replicate + apply + compact on {l0, third}

    // A failed-CAS burst: committed, applied, ts-bearing, writes NO row —
    // crossing `COMPACT_THRESHOLD` again so the sender's NEXT compaction
    // truncates its WAL straight through these too, leaving `lagging`
    // nothing to catch up on except a fresh snapshot.
    for i in 0..FAILED_CAS {
        match nodes[l0].cas(
            b"never-written".to_vec(),
            Some(format!("bogus-expected-{i}").into_bytes()),
            format!("would-be-v{i}").into_bytes(),
        ) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected failed-cas {i}: {other:?} (seed={seed})"),
        }
    }
    sim.run_for(Duration::from_secs(3)); // apply + compact the failed-cas tail too

    // Heal `lagging` (network-reachability only — its in-memory `RaftCore`
    // never went away): it must catch up via a pure `InstallSnapshot`.
    sim.restart(nid(lagging as u64));
    sim.run_for(Duration::from_secs(4));

    let l0_applied = nodes[l0].engine_applied_index();
    let lag_applied = nodes[lagging].engine_applied_index();
    assert_eq!(
        lag_applied, l0_applied,
        "sanity: `lagging` must be FULLY caught up via InstallSnapshot before \
         the genuine restart below, or this isn't the scenario shape issue \
         #811 needs (seed={seed}, l0_applied={l0_applied}, lag_applied={lag_applied})"
    );

    // The genuine process restart: stop `lagging`'s process, then start a
    // fresh one on the SAME engine handle — modeling a real restart, not a
    // network blip. `l0`/`third` stay live throughout.
    sim.stop(nid(lagging as u64));
    nodes[lagging] = MemNode::start(
        sim.env(nid(lagging as u64)),
        NODES.iter().copied().map(nid).collect(),
        engines[lagging].clone(),
    );
    // Drop every `RaftKvNode` handle before handing `sim` to the watchdog
    // thread: a handle held on this (the test's own) thread while the
    // spawned thread drives `sim` would be a cross-thread aliasing hazard
    // on a genuine hang (the driven thread's task polls still touch the
    // same `Arc<Mutex<..>>`-backed state these handles reference) — and
    // this test needs no further access to any node once the restart
    // itself has been issued.
    drop(nodes);

    drive_bounded(sim, Duration::from_millis(50), WATCHDOG_BUDGET)
}

/// Iterates a handful of seeds so a seed-specific election/timing accident
/// doesn't mask the bug (each run is a small fraction of a second when
/// healthy). Replay a single one with a decimal `ANIMUS_SEED`.
#[test]
fn genuine_restart_of_an_install_snapshot_caught_up_replica_does_not_livelock() {
    let seeds: Vec<u64> = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(|s| vec![s])
        .unwrap_or_else(|| vec![55_300, 55_301, 55_302, 55_303, 55_304]);
    for seed in seeds {
        assert!(
            run_scenario(seed),
            "issue #811 reproduced: a genuine restart of an InstallSnapshot-\
             caught-up replica livelocked the apply task — run_for(50ms) did \
             not return within {WATCHDOG_BUDGET:?} of real wall time \
             (seed={seed})"
        );
    }
}

/// The same scenario over `LsmEngine<SimEnv>` instead of `MemoryEngine` —
/// the root cause (`RaftCore`'s own WAL/snapshot-dirty bookkeeping) is
/// entirely independent of the `StorageEngine` implementation, so this
/// proves the fix isn't accidentally `MemoryEngine`-specific. A single seed
/// is enough: this dimension is about the storage backend, not scheduling
/// timing, which the multi-seed test above already covers.
#[test]
fn genuine_restart_of_an_install_snapshot_caught_up_replica_does_not_livelock_lsm() {
    let seed: u64 = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(55_300);
    type LsmNode = RaftKvNode<SimEnv, LsmEngine<SimEnv>>;

    let mut sim = Simulator::new(seed);
    let prefixes: Vec<String> = NODES.iter().map(|i| format!("db-issue811-n{i}-")).collect();
    let mut nodes: Vec<LsmNode> = NODES
        .iter()
        .zip(prefixes.iter())
        .map(|(&id, prefix)| {
            let env = sim.env(nid(id));
            let engine = block_on(LsmEngine::open(env.clone(), prefix.clone()))
                .expect("open a fresh LsmEngine");
            LsmNode::start(env, NODES.iter().copied().map(nid).collect(), engine)
        })
        .collect();

    sim.run_for(Duration::from_secs(2));
    let l0 = leader_among(&nodes).unwrap_or_else(|| panic!("no initial leader (seed={seed})"));
    let lagging = (0..3).find(|&i| i != l0).expect("a non-leader replica");

    sim.crash(nid(lagging as u64));
    for i in 0..REAL_WRITES {
        match nodes[l0].put(
            format!("k{i:04}").into_bytes(),
            format!("v{i}").into_bytes(),
        ) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected real put {i}: {other:?} (seed={seed})"),
        }
    }
    sim.run_for(Duration::from_secs(2));
    for i in 0..FAILED_CAS {
        match nodes[l0].cas(
            b"never-written".to_vec(),
            Some(format!("bogus-expected-{i}").into_bytes()),
            format!("would-be-v{i}").into_bytes(),
        ) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected failed-cas {i}: {other:?} (seed={seed})"),
        }
    }
    sim.run_for(Duration::from_secs(3));

    sim.restart(nid(lagging as u64));
    sim.run_for(Duration::from_secs(4));

    let l0_applied = nodes[l0].engine_applied_index();
    let lag_applied = nodes[lagging].engine_applied_index();
    assert_eq!(
        lag_applied, l0_applied,
        "sanity: `lagging` must be fully caught up before the genuine \
         restart (seed={seed}, l0_applied={l0_applied}, lag_applied={lag_applied})"
    );

    sim.stop(nid(lagging as u64));
    let env = sim.env(nid(lagging as u64));
    let engine = block_on(LsmEngine::open(env.clone(), prefixes[lagging].clone()))
        .expect("re-open the same LsmEngine prefix after a genuine restart");
    nodes[lagging] = LsmNode::start(env, NODES.iter().copied().map(nid).collect(), engine);
    drop(nodes);

    assert!(
        drive_bounded(sim, Duration::from_millis(50), WATCHDOG_BUDGET),
        "issue #811 reproduced over LsmEngine<SimEnv>: run_for(50ms) did not \
         return within {WATCHDOG_BUDGET:?} of real wall time (seed={seed})"
    );
}
