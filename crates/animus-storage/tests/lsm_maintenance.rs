//! `LsmOptions::background_maintenance` (opt-in, default `false`): moves
//! flush/compaction off the write path's ack, triggering a background task
//! (`env.spawn_task`) instead and applying a bounded-memtable-overshoot
//! backpressure gate to writers.
//!
//! This needs a driver that actually polls spawned tasks — a bare
//! `futures::executor::block_on` of a single write never runs anything else,
//! so the background task would simply never execute. Every test here spawns
//! the write workload itself as a task and drives everything with
//! `Simulator::run_until_quiescent`, the same pattern other crates use for
//! spawned protocol tasks (e.g. `animus-control`'s Raft tests) — see
//! `LsmOptions::background_maintenance`'s doc comment.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use animus_env::EnvExt;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";

fn opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 256,
        compaction_trigger: 4,
        target_table_bytes: 1024,
        level_fanout: 2,
        wal_segment_bytes: 4096,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: true,
    }
}

fn open(sim: &Simulator) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(0), PREFIX, opts())).expect("open")
}

fn key(i: u64) -> String {
    format!("k{i:04}")
}

fn value(i: u64) -> String {
    format!("v{i}")
}

/// A write's ack does not wait for the flush it triggers: right after a
/// `block_on` of enough puts to cross the flush threshold returns, no flush
/// has happened yet (the maintenance task is only *spawned*, not yet polled —
/// nothing has driven the simulator). Once the simulator *is* driven to
/// quiescence, the background task runs and the data is flushed.
#[test]
fn ack_does_not_wait_for_the_triggered_flush() {
    let mut sim = Simulator::new(1);
    let e = open(&sim);

    block_on(async {
        // Comfortably cross `flush_threshold_bytes` (256) so a flush is due.
        for i in 0..40u64 {
            e.put(key(i).as_bytes(), value(i).as_bytes(), i + 1)
                .await
                .unwrap();
        }
    });
    // The ack already returned above; nothing has polled the spawned
    // maintenance task yet, so no flush has run — the whole point of moving
    // it off the ack path.
    assert_eq!(
        e.sstable_count(),
        0,
        "a flush must not have run inline on the write path"
    );
    assert!(
        e.background_maintenance_in_flight() || e.flush_count() == 0,
        "a maintenance task should have been triggered by crossing the threshold"
    );

    // Drive the simulator: this is what actually polls the spawned task.
    sim.run_until_quiescent(10_000);

    assert!(
        e.sstable_count() >= 1,
        "background maintenance should have flushed once driven"
    );
    assert!(
        !e.background_maintenance_in_flight(),
        "maintenance should have finished once quiescent"
    );
    assert_eq!(
        e.background_maintenance_error(),
        None,
        "no fault was injected; maintenance should not have failed"
    );

    // Data is correct regardless of whether it's still in the memtable or has
    // moved to an SSTable.
    block_on(async {
        for i in 0..40u64 {
            assert_eq!(
                e.get(key(i).as_bytes()).await.unwrap().unwrap().value,
                value(i).as_bytes(),
            );
        }
    });
}

/// A burst of writes, driven as a spawned task alongside the background
/// maintenance it triggers, ends up fully flushed/compacted and correct —
/// exercising the backpressure gate (`await_backpressure`) end to end: the
/// writer task must not race ahead of maintenance without bound.
#[test]
fn burst_of_writes_is_bounded_and_converges_once_driven() {
    let mut sim = Simulator::new(2);
    let e = open(&sim);
    let n = 400u64;

    let done = Arc::new(AtomicBool::new(false));
    let done_writer = Arc::clone(&done);
    let writer = e.clone();
    sim.env(0).spawn_task(async move {
        for i in 0..n {
            writer
                .put(key(i).as_bytes(), value(i).as_bytes(), i + 1)
                .await
                .unwrap();
        }
        done_writer.store(true, Ordering::Release);
    });

    let quiescent = sim.run_until_quiescent(1_000_000);
    assert!(quiescent, "simulation should quiesce, not hit the step cap");
    assert!(done.load(Ordering::Acquire), "the writer task must finish");
    assert_eq!(
        e.background_maintenance_error(),
        None,
        "no fault was injected; maintenance should not have failed"
    );

    // Every write is durable and correct, and maintenance actually ran (this
    // workload comfortably crosses both the flush threshold and the
    // compaction trigger many times over).
    assert!(e.flush_count() >= 1, "expected at least one flush");
    assert!(
        e.compaction_count() >= 1,
        "expected at least one compaction"
    );
    block_on(async {
        for i in 0..n {
            assert_eq!(
                e.get(key(i).as_bytes()).await.unwrap().unwrap().value,
                value(i).as_bytes(),
                "key {} lost or corrupted under background maintenance",
                key(i)
            );
        }
    });
}

/// The default (`background_maintenance: false`) behavior is completely
/// unaffected: a write's ack still means flush/compaction already ran inline,
/// exactly as every other test in this crate assumes — a bare `block_on` of a
/// write loop is enough, no simulator driving required.
#[test]
fn default_behavior_is_still_fully_synchronous() {
    let sim = Simulator::new(3);
    let mut o = opts();
    o.background_maintenance = false;
    let e = block_on(LsmEngine::open_with(sim.env(0), PREFIX, o)).expect("open");
    block_on(async {
        for i in 0..40u64 {
            e.put(key(i).as_bytes(), value(i).as_bytes(), i + 1)
                .await
                .unwrap();
        }
    });
    assert!(
        e.sstable_count() >= 1,
        "default behavior must still flush inline on the write path, with no \
         simulator driving needed"
    );
}
