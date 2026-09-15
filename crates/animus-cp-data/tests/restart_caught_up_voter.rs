//! Regression for GitHub issue #811 (P0): a genuine process restart of a
//! live, fully caught-up voter of an otherwise-active 3-node `RaftKvNode`
//! group pinned one core forever inside `apply_and_compact`.
//!
//! # Root cause
//!
//! `apply_and_compact`'s compaction section used to set `did_work = true`
//! unconditionally on entering `if (threshold_hit || image_needed) &&
//! !halted`, regardless of whether anything inside that block actually
//! changed state. Being *eligible* to attempt compaction is not the same
//! claim as having performed it: `snapshot_upto(ea)` clamps its target to
//! `RaftCore::last_applied()`, and a restart can leave `last_applied` stuck
//! **below** `engine_applied` (`ea`) for as long as no leader re-drives Raft
//! commit past it — in which case `snapshot_upto` legitimately no-ops
//! (`take_snapshot_dirty()` stays `false`) every single call, `behind`
//! never shrinks on its own, and `threshold_hit` never goes false again.
//! `apply_loop`'s own `if !did_work { select(..).await }` is the ONLY place
//! this task ever yields back to the executor — an unconditional `did_work
//! = true` skips it forever, and since nothing else in the fast (no-op)
//! path of `apply_and_compact` ever returns `Poll::Pending` under `SimEnv`
//! (its one `.await`, an uncontended `wal_lock.lock()`, resolves
//! synchronously), the task's `poll()` call itself never returns — a true
//! infinite loop invisible to any timer, seed, or step-count budget.
//!
//! # The restart-time disagreement that triggers it
//!
//! A replica that catches up via a completed `InstallSnapshot` gets its
//! `RaftCore::snapshot_index`/`last_applied` advanced **in-memory**
//! immediately (`handle_install_snapshot`'s `install` closure), and its
//! engine's own durable applied-watermark marker advanced **durably** in
//! the same install (`install_engine_image`'s `merge_batch`) — but the
//! WAL rewrite that would make the CORE's own advance durable only happens
//! later, inside this same `apply_and_compact` compaction block, gated on
//! `threshold_hit`/`image_needed`. Immediately after an install, `behind ==
//! 0`, so that gate never fires — the fact that the base moved sits
//! recorded only as the in-memory `snapshot_dirty` flag, with no
//! independent trigger to flush it. A genuine process restart
//! (`Simulator::stop` + a fresh `RaftKvNode::start` on the same durable
//! engine) right then recovers `RaftCore` from the stale, pre-install WAL
//! (old, low `snapshot_index`/`last_applied`) while `engine_applied` is
//! re-seeded from the engine's own (durably-advanced) watermark marker —
//! so `ea` is far ahead of `core.snapshot_index()`, `behind >=
//! COMPACT_THRESHOLD` is immediately true, and (with the other two
//! replicas fully live but this replica never hearing from them again in
//! this test) nothing ever advances `last_applied` to let `snapshot_upto`
//! actually move the base.
//!
//! # The fix
//!
//! `did_work` now reflects only provable progress: a freshly-built
//! on-demand snapshot image actually installed (`image_needed` was true —
//! a take-once flag, so this alone can never spin), and/or a real
//! WAL-rewriting compaction that actually completed (`bytes.is_some()`).
//! Neither ever holds in the stuck shape above, so `apply_and_compact`
//! returns `false`, `apply_loop` takes its `select(ApplyPending,
//! env.sleep(APPLY_SAFETY_POLL))` branch, and the task genuinely yields —
//! bounded, idle polling instead of a pinned core, until real Raft
//! progress (a leader re-establishing contact) resolves the disagreement
//! the ordinary way.
//!
//! # Why this test drives the scenario on a background thread
//!
//! Pre-fix, the very last `run_for` call below never returns: the spin is a
//! genuine Rust-level infinite loop inside one `Future::poll()` call, with
//! no timer that ever fires and no step count that is ever reached — there
//! is no way to bound it from *inside* `SimEnv`/`Simulator`. Driving the
//! whole scenario on a background thread and bounding the wait with a
//! wall-clock `recv_timeout` on the main test thread turns a reintroduced
//! spin into a clean, fast test failure instead of a hung `cargo test`
//! process (mirroring how the issue itself was diagnosed: an external,
//! wall-clock-bounded observation of an otherwise-unobservable infinite
//! loop).

use std::sync::mpsc;
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::nid;
use animus_sim::{SimEnv, SimStats, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// The current leader among `nodes`, if exactly one reports it.
fn leader_among(nodes: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

/// Run the full issue #811 scenario once, returning the `SimStats` delta
/// across the final, previously-hanging `run_for` call.
fn run_scenario(seed: u64) -> SimStats {
    let sim = Simulator::new(seed);
    let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
    let mut nodes: Vec<KvNode> = NODES
        .iter()
        .zip(engines.iter())
        .map(|(&id, engine)| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                engine.clone(),
            )
        })
        .collect();
    let mut sim = sim;
    sim.run_for(Duration::from_secs(2)); // elect
    let l0 = leader_among(&nodes).unwrap_or_else(|| panic!("no leader elected (seed={seed})"));
    let lagging = (0..3)
        .find(|&i| i != l0)
        .expect("a non-leader replica exists");

    // Partition the to-be-lagging replica so it falls behind the leader's
    // eventually-compacted log and must catch up via a genuine
    // `InstallSnapshot`, never plain `AppendEntries` replay.
    sim.crash(nid(lagging as u64));

    // Comfortably past `COMPACT_THRESHOLD` (64) on the two live replicas,
    // so their own WAL gets truncated well ahead of anything `lagging`
    // still has.
    const WRITES: u64 = 200;
    for i in 0..WRITES {
        match nodes[l0].put(
            format!("k{i:04}").into_bytes(),
            format!("v{i}").into_bytes(),
        ) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected put {i}: {other:?} (seed={seed})"),
        }
    }
    sim.run_for(Duration::from_secs(2)); // replicate + apply + compact on the live pair

    // Heal: `lagging` reconnects with a log start long gone from the
    // leader's own WAL, forcing a real `InstallSnapshot` transfer.
    sim.restart(nid(lagging as u64));
    sim.run_for(Duration::from_secs(2)); // let the transfer land

    // The issue #811 shape: a genuine process restart (`stop` + a fresh
    // `start` on the SAME durable engine) of the now live, fully
    // caught-up voter, while the other two replicas stay fully live.
    sim.stop(nid(lagging as u64));
    nodes[lagging] = RaftKvNode::start(
        sim.env(nid(lagging as u64)),
        NODES.iter().copied().map(nid).collect(),
        engines[lagging].clone(),
    );

    let before = sim.stats();
    // The hang: pre-fix, this call never returns.
    sim.run_for(Duration::from_millis(50));
    let after = sim.stats();

    SimStats {
        task_polls: after.task_polls - before.task_polls,
        timer_fires: after.timer_fires - before.timer_fires,
    }
}

#[test]
fn restarting_a_live_caught_up_voter_does_not_pin_a_core() {
    let seed = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(811_2026_09_15u64);

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let delta = run_scenario(seed);
        // The receiver may already be gone (main thread's `recv_timeout`
        // gave up) — sending is best-effort, this thread is abandoned
        // either way (see the module doc for why the process still exits
        // cleanly: the test binary's own `main` returns regardless).
        let _ = tx.send(delta);
    });

    match rx.recv_timeout(Duration::from_secs(20)) {
        Ok(delta) => {
            // One healthy 50ms tick over a 3-node group does at most a
            // couple hundred task polls; a reintroduced spin would run
            // into the millions long before the 20s wall-clock guard
            // above would ever fire on its own.
            assert!(
                delta.task_polls < 50_000,
                "suspiciously high task_polls ({}) for one 50ms tick after \
                 restarting a caught-up voter — apply_and_compact may be \
                 spinning again (issue #811 regression, seed={seed})",
                delta.task_polls
            );
        }
        Err(_) => panic!(
            "restarting a live, caught-up voter hung for 20s of wall-clock \
             time driving a single 50ms simulated tick — apply_and_compact \
             is pinning a core (issue #811 regression, seed={seed})"
        ),
    }
}
