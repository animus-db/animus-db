//! M1 acceptance tests for the deterministic simulator.
//!
//! These exercise the core guarantee (ADR 0003): an entire distributed run —
//! tasks exchanging timed messages, with injected partitions and crashes — is a
//! pure function of its seed, so the recorded history is byte-identical across
//! repeated runs and any failure is replayable from its printed seed.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Clock, Disk, Env, EnvExt, Network};
use animus_sim::Simulator;

const N: u64 = 3;
const MAX_HOPS: u8 = 9;

/// Build a ring scenario: each node forwards a token to its successor after a
/// short think-time; node 0 kicks it off. The hop counter bounds the run.
fn build_ring(sim: &Simulator) {
    for id in 0..N {
        let env = sim.env(id);
        env.clone().spawn_task(async move {
            loop {
                let msg = env.recv().await;
                let hop = msg.payload[0];
                if hop >= MAX_HOPS {
                    continue; // park: token has finished its journey
                }
                env.sleep(Duration::from_micros(100)).await;
                let next = (env.node_id() + 1) % N;
                env.send(next, vec![hop + 1]).await;
            }
        });
    }
    let starter = sim.env(0);
    starter.clone().spawn_task(async move {
        starter.sleep(Duration::from_millis(1)).await;
        starter.send(1, vec![0]).await;
    });
}

fn run_ring(seed: u64, partition_1_2: bool) -> Vec<String> {
    let mut sim = Simulator::new(seed);
    build_ring(&sim);
    if partition_1_2 {
        sim.partition_pair(1, 2);
    }
    // Guard against a scenario that fails to settle; the ring is bounded so
    // this should never trip.
    assert!(
        sim.run_until_quiescent(100_000),
        "ring did not reach quiescence (seed={})",
        sim.seed()
    );
    sim.trace_lines()
}

#[test]
fn trace_is_byte_identical_across_runs() {
    let seed = seed_from_env(0xC057_05DB);
    let first = run_ring(seed, false);
    let second = run_ring(seed, false);

    assert!(
        !first.is_empty(),
        "expected a non-trivial history (seed={seed})"
    );
    assert!(
        first.iter().any(|l| l.contains("DELIVER")),
        "expected message deliveries in the history (seed={seed})"
    );
    assert_eq!(
        first, second,
        "history diverged across identical runs — nondeterminism leaked in (seed={seed})"
    );
}

#[test]
fn partition_is_reproducible_and_changes_history() {
    let seed = seed_from_env(0xBEEF_F00D);
    let partitioned_a = run_ring(seed, true);
    let partitioned_b = run_ring(seed, true);
    let healthy = run_ring(seed, false);

    assert_eq!(
        partitioned_a, partitioned_b,
        "partitioned run was not reproducible from its seed (seed={seed})"
    );
    assert!(
        partitioned_a
            .iter()
            .any(|l| l.contains("DROP") && l.contains("partition")),
        "expected a partition-induced drop in the history (seed={seed})"
    );
    assert_ne!(
        partitioned_a, healthy,
        "partition did not change observable history (seed={seed})"
    );
}

#[test]
fn crash_drops_unsynced_disk_bytes() {
    let seed = seed_from_env(7);
    let mut sim = Simulator::new(seed);

    // Phase 1: sync "aaa", then append un-synced "bbb"; capture the live view.
    let live_view = Arc::new(Mutex::new(Vec::new()));
    {
        let env = sim.env(0);
        let live = Arc::clone(&live_view);
        env.clone().spawn_task(async move {
            env.append("wal", b"aaa").await.unwrap();
            env.sync("wal").await.unwrap();
            env.append("wal", b"bbb").await.unwrap();
            *live.lock().unwrap() = env.read("wal").await.unwrap();
        });
        sim.run();
    }
    assert_eq!(
        &*live_view.lock().unwrap(),
        b"aaabbb",
        "before crash, a read should see synced + un-synced bytes (seed={seed})"
    );

    // Crash: the un-synced "bbb" must be lost; the synced "aaa" survives.
    sim.crash(0);
    sim.restart(0);

    let after = Arc::new(Mutex::new(Vec::new()));
    {
        let env = sim.env(0);
        let out = Arc::clone(&after);
        env.clone().spawn_task(async move {
            *out.lock().unwrap() = env.read("wal").await.unwrap();
        });
        sim.run();
    }
    assert_eq!(
        &*after.lock().unwrap(),
        b"aaa",
        "after crash, only synced bytes should remain (seed={seed})"
    );
}

#[test]
fn disk_random_access_size_remove_and_crash() {
    let seed = seed_from_env(11);
    let mut sim = Simulator::new(seed);

    // Sync 10 durable bytes, then append 3 un-synced bytes.
    let snap = Arc::new(Mutex::new((Vec::new(), Vec::new(), 0u64, 0u64)));
    {
        let env = sim.env(0);
        let out = Arc::clone(&snap);
        env.clone().spawn_task(async move {
            env.append("sst", b"0123456789").await.unwrap();
            env.sync("sst").await.unwrap();
            env.append("sst", b"abc").await.unwrap(); // un-synced tail
            let mid = env.read_at("sst", 3, 4).await.unwrap(); // "3456"
            let tail = env.read_at("sst", 10, 9).await.unwrap(); // "abc" (clamped at EOF)
            let size = env.size("sst").await.unwrap(); // 13
            let past = env.size("missing").await.unwrap(); // 0
            *out.lock().unwrap() = (mid, tail, size, past);
        });
        sim.run();
    }
    let (mid, tail, size, past) = snap.lock().unwrap().clone();
    assert_eq!(mid, b"3456", "read_at offset/len wrong (seed={seed})");
    assert_eq!(
        tail, b"abc",
        "read_at must clamp at EOF over the buffered tail"
    );
    assert_eq!(size, 13, "size must include the un-synced tail");
    assert_eq!(past, 0, "size of a missing file is 0");

    // Crash drops the un-synced tail; random reads see only the durable prefix.
    sim.crash(0);
    sim.restart(0);
    let after = Arc::new(Mutex::new((0u64, Vec::new(), 0u64)));
    {
        let env = sim.env(0);
        let out = Arc::clone(&after);
        env.clone().spawn_task(async move {
            let size = env.size("sst").await.unwrap(); // back to 10
            let tail = env.read_at("sst", 10, 3).await.unwrap(); // empty: tail gone
            env.remove("sst").await.unwrap();
            let after_remove = env.size("sst").await.unwrap(); // 0
            *out.lock().unwrap() = (size, tail, after_remove);
        });
        sim.run();
    }
    let (size, tail, after_remove) = after.lock().unwrap().clone();
    assert_eq!(size, 10, "crash must drop the un-synced tail (seed={seed})");
    assert!(
        tail.is_empty(),
        "the un-synced tail must be gone after crash"
    );
    assert_eq!(after_remove, 0, "remove must delete the file (seed={seed})");
}

/// A node parked on `recv()` that is crashed and then restarted must resume
/// receiving: a message delivered after the restart should wake it and be
/// processed. (Regression: `crash` dropped the parked recv waker, so after
/// `restart` nothing re-polled the task and later deliveries were never woken.)
#[test]
fn restart_resumes_a_parked_recv() {
    let seed = seed_from_env(0x5EED_0FEE);
    let mut sim = Simulator::new(seed);

    // A receiver that records every message payload byte it observes.
    let seen = Arc::new(Mutex::new(Vec::<u8>::new()));
    {
        let env = sim.env(0);
        let out = Arc::clone(&seen);
        env.clone().spawn_task(async move {
            loop {
                let msg = env.recv().await;
                out.lock().unwrap().push(msg.payload[0]);
            }
        });
    }

    // Deliver a first message and let it be processed, so the task is now parked
    // back on `recv()` with an empty inbox (its waker registered).
    {
        let sender = sim.env(1);
        sender.clone().spawn_task(async move {
            sender.send(0, vec![1]).await;
        });
    }
    sim.run_for(Duration::from_millis(10));
    assert_eq!(
        &*seen.lock().unwrap(),
        &[1],
        "receiver should have processed the pre-crash message (seed={seed})"
    );

    // Crash the receiver while it is parked on `recv()`, then restart it.
    sim.crash(0);
    sim.restart(0);

    // Deliver another message after the restart; the re-armed task must wake and
    // process it.
    {
        let sender = sim.env(1);
        sender.clone().spawn_task(async move {
            sender.send(0, vec![2]).await;
        });
    }
    sim.run_for(Duration::from_millis(10));

    assert_eq!(
        &*seen.lock().unwrap(),
        &[1, 2],
        "restarted node never processed the post-restart message — its parked \
         recv was not re-armed (seed={seed})"
    );
}

/// `Disk::list` enumerates only the calling node's files, sorted, and reflects
/// `remove` — the primitive a teardown path uses to find every file of a
/// prefix-named component without knowing the exact set.
#[test]
fn disk_list_is_per_node_and_sorted() {
    let seed = seed_from_env(13);
    let mut sim = Simulator::new(seed);

    let out = Arc::new(Mutex::new((Vec::new(), Vec::new())));
    {
        let env = sim.env(0);
        let other = sim.env(1);
        let snap = Arc::clone(&out);
        env.clone().spawn_task(async move {
            env.append("db-wal", b"w").await.unwrap();
            env.append("db-MANIFEST", b"m").await.unwrap();
            other.append("db-other", b"o").await.unwrap();
            let before = env.list().await.unwrap();
            env.remove("db-wal").await.unwrap();
            let after = env.list().await.unwrap();
            *snap.lock().unwrap() = (before, after);
        });
        sim.run();
    }
    let (before, after) = out.lock().unwrap().clone();
    assert_eq!(
        before,
        vec!["db-MANIFEST".to_string(), "db-wal".to_string()],
        "list must be this node's files only, sorted (seed={seed})"
    );
    assert_eq!(
        after,
        vec!["db-MANIFEST".to_string()],
        "list must reflect remove (seed={seed})"
    );
}

/// Multiplexed `(node, stream)` addressing (ADR 0026): two streams to the same
/// node are isolated from each other (no cross-talk) and the whole run
/// (including the `stream=` field now carried in the trace) stays a byte-
/// identical function of the seed — the same determinism guarantee every prior
/// seam addition (metrics, disk `list`) was held to.
fn run_multiplexed_streams(seed: u64) -> (Vec<u8>, Vec<u8>, Vec<String>) {
    let mut sim = Simulator::new(seed);
    const STREAM_A: u64 = 7;
    const STREAM_B: u64 = 42;

    let seen_a = Arc::new(Mutex::new(Vec::<u8>::new()));
    let seen_b = Arc::new(Mutex::new(Vec::<u8>::new()));
    {
        let env = sim.env(1);
        let out = Arc::clone(&seen_a);
        env.clone().spawn_task(async move {
            for _ in 0..5 {
                let msg = env.recv_stream(STREAM_A).await;
                out.lock().unwrap().push(msg.payload[0]);
            }
        });
    }
    {
        let env = sim.env(1);
        let out = Arc::clone(&seen_b);
        env.clone().spawn_task(async move {
            for _ in 0..5 {
                let msg = env.recv_stream(STREAM_B).await;
                out.lock().unwrap().push(msg.payload[0]);
            }
        });
    }
    // Interleave sends on both streams from the same sender node.
    {
        let sender = sim.env(0);
        sender.clone().spawn_task(async move {
            for i in 0..5u8 {
                sender.send_stream(1, STREAM_A, vec![i]).await;
                sender.send_stream(1, STREAM_B, vec![100 + i]).await;
            }
        });
    }
    assert!(
        sim.run_until_quiescent(100_000),
        "multiplexed-stream scenario did not settle (seed={seed})"
    );

    let a = seen_a.lock().unwrap().clone();
    let b = seen_b.lock().unwrap().clone();
    (a, b, sim.trace_lines())
}

#[test]
fn multiplexed_streams_are_isolated_and_deterministic() {
    let seed = seed_from_env(0x57EA_11ED);
    let (a1, b1, trace1) = run_multiplexed_streams(seed);
    let (a2, b2, trace2) = run_multiplexed_streams(seed);

    // The default `NetConfig` applies random per-message jitter, so delivery
    // order within one stream is not guaranteed — only *membership* is: each
    // stream must see exactly its own payloads, never the other stream's
    // (cross-talk would show up as a wrong value or a wrong count here).
    let mut sorted_a1 = a1.clone();
    sorted_a1.sort_unstable();
    let mut sorted_b1 = b1.clone();
    sorted_b1.sort_unstable();
    assert_eq!(
        sorted_a1,
        vec![0, 1, 2, 3, 4],
        "stream A must see exactly its own payloads — no cross-talk from stream \
         B on the same (from, to) pair (seed={seed})"
    );
    assert_eq!(
        sorted_b1,
        vec![100, 101, 102, 103, 104],
        "stream B must see exactly its own payloads — no cross-talk from stream \
         A on the same (from, to) pair (seed={seed})"
    );
    assert_eq!(
        a1, a2,
        "stream A's observed sequence (order included) must be a pure function \
         of the seed (seed={seed})"
    );
    assert_eq!(
        b1, b2,
        "stream B's observed sequence (order included) must be a pure function \
         of the seed (seed={seed})"
    );
    assert_eq!(
        trace1, trace2,
        "trace (now carrying the stream field) must stay byte-identical across \
         runs of the same seed — multiplexing must not leak nondeterminism \
         (seed={seed})"
    );
    assert!(
        trace1.iter().any(|l| l.contains("stream=7")),
        "trace must record stream A's id (seed={seed})"
    );
    assert!(
        trace1.iter().any(|l| l.contains("stream=42")),
        "trace must record stream B's id (seed={seed})"
    );
}

/// Read a seed from `ANIMUS_SEED` for replay, falling back to `default`. A
/// failing run prints its seed (see the assertion messages) so it can be
/// replayed with `ANIMUS_SEED=<seed> cargo test`.
fn seed_from_env(default: u64) -> u64 {
    match std::env::var("ANIMUS_SEED") {
        Ok(s) => s.parse().unwrap_or(default),
        Err(_) => default,
    }
}
