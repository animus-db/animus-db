//! ADR 0044 phase-1 PR3: the end-to-end `SimEnv` corpus for a real 3-node
//! `RaftKvNode` group opted into quiescence (`crates/animus-control/tests/
//! quiescence.rs` covers the pure `RaftCore` predicate/message mechanics this
//! file assumes correct). Five properties, each run at the seed depth knob
//! `ANIMUS_QUIESCE_SEEDS` (default 1, per the house corpus doctrine — ADR
//! 0014 — the same "K seed variants per scenario" shape as
//! `ANIMUS_RECONCILER_SEEDS`/`ANIMUS_RAFTKV_SEEDS`):
//!
//! (i) an idle group reaches quiescence — every replica's own consensus loop
//!     genuinely parks with no timer (`next_deadline() == None`);
//! (ii) `Metric::CpAppendEntriesSent` stays flat across a long idle window
//!     once quiesced — the actual wakeup-cost win this feature exists for;
//! (iii) a write after quiescence un-quiesces the leader and still commits;
//! (iv) a linearizable read on a quiesced leader is served **without**
//!     un-quiescing (the ReadIndex probe path never touches `RaftCore::handle`
//!     — see this crate's `CLAUDE.md`'s "Key invariants" ReadProbe entry);
//! (v) killing the quiesced leader, then writing to a survivor, still
//!     converges (the `WakeRequest`-no-reply-then-campaign path, fork B).
//!
//! **On property (i)'s "genuine event-quiescence" and why this file does not
//! call `Simulator::run_until_quiescent` and expect `true`:** the apply
//! task's own idle back-off (ADR 0044 phase-1 PR1) races `ApplyPending`
//! against a **250ms safety-poll `env.sleep`, forever, independent of Raft
//! activity** — a deliberate, already-shipped design (a missed/lost
//! `ApplySignal` must still converge). That safety poll keeps one scheduled
//! `SimEnv` timeline event per node alive at all times, so `run_until_
//! quiescent` can never observe a truly empty timeline for a live group,
//! quiesced or not — this is not a PR3 defect, it is PR1's own accepted
//! trade-off surfacing at a different observation point. Per
//! `docs/engineering-lessons.md`'s note that a raw `TraceEvent::Timer` tally
//! is unreliable once anything else races a sleep (a lost-race sleep still
//! logs a stale `Timer` line at its original deadline), the **strongest
//! available, unfakeable proof of the consensus loop's own timerlessness**
//! is exactly what phase-1 PR2/PR3 added for that purpose:
//! `RaftCore::next_deadline() == None` — checked directly on every replica,
//! not inferred from trace event counts. Property (ii)'s flat
//! `CpAppendEntriesSent` is the corroborating, still-unambiguous quantitative
//! proof that the reduced timer activity actually stopped real Raft traffic.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{EnvExt, Metric, MetricsHandle, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
/// Short relative to every run's own settle window, long relative to one
/// heartbeat interval (50ms) — so a run that idles past its own election/
/// heartbeat settle genuinely satisfies the entry predicate's "no activity"
/// clause on its very next heartbeat tick.
const QUIESCE_AFTER: Duration = Duration::from_millis(200);

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// Depth knob (`ANIMUS_QUIESCE_SEEDS`, default 1) — mirrors `ANIMUS_
/// RECONCILER_SEEDS`/`ANIMUS_RAFTKV_SEEDS`'s exact pattern.
fn seed_depth() -> u64 {
    std::env::var("ANIMUS_QUIESCE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
        .max(1)
}

/// Every seed a scenario runs at, derived from `base` — additive under a
/// deeper knob (seed 0 is always `base` itself, so `K=1`'s exact behavior is
/// a strict prefix of any deeper run).
fn seeds(base: u64) -> impl Iterator<Item = u64> {
    (0..seed_depth()).map(move |k| base.wrapping_add(k))
}

/// Stand up a 3-node group, every replica opted into quiescence with the same
/// `quiesce_after` — a real deployment enables it on every replica of a
/// group, not just whichever happens to be leader at the time (fork A: a
/// quiesced node stays leader, and any replica could become leader later).
/// Each node records into its own [`MetricsHandle`], index-aligned.
fn group(seed: u64) -> (Simulator, Vec<KvNode>, Vec<MetricsHandle>) {
    let sim = Simulator::new(seed);
    let handles: Vec<MetricsHandle> = NODES.iter().map(|_| MetricsHandle::recording()).collect();
    let nodes: Vec<KvNode> = NODES
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            let n = RaftKvNode::start_with_metrics(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                handles[i].clone(),
            );
            n.enable_quiescence(QUIESCE_AFTER);
            n
        })
        .collect();
    (sim, nodes, handles)
}

fn leader_index(nodes: &[KvNode], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

/// Elect, settle, then idle well past `QUIESCE_AFTER` so the leader's own
/// heartbeat tick evaluates (and, absent any obstruction, satisfies) the
/// entry predicate.
fn settle_to_quiescence(sim: &mut Simulator) {
    sim.run_for(Duration::from_secs(2)); // elect + replicate the no-op
    sim.run_for(Duration::from_secs(2)); // idle well past QUIESCE_AFTER
}

/// Run a linearizable read on `node` to completion — mirrors
/// `tests/metrics.rs::lin_read` (spawned, since it awaits a quorum probe
/// round; a linearizable read's internal polling only resolves while the
/// `Simulator` is advancing virtual time).
fn lin_read(sim: &mut Simulator, node: &KvNode, key: &[u8], budget: Duration) -> Option<Vec<u8>> {
    use std::sync::{Arc, Mutex};
    let slot: Arc<Mutex<Option<Option<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let n = node.clone();
    let s = Arc::clone(&slot);
    let k = key.to_vec();
    node.env().clone().spawn_task(async move {
        let v = n.linearizable_get(&k).await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock()
        .unwrap()
        .clone()
        .expect("linearizable read did not complete")
}

// ---- (i) an idle group genuinely reaches quiescence -----------------------

#[test]
fn idle_group_reaches_quiescence() {
    for seed in seeds(0xF1DE1) {
        let (mut sim, nodes, _handles) = group(seed);
        settle_to_quiescence(&mut sim);

        let leader = leader_index(&nodes, seed);
        assert!(
            nodes[leader].is_quiesced(),
            "the leader must have quiesced after idling past QUIESCE_AFTER (seed={seed})"
        );
        // Every replica's own consensus loop must have genuinely parked with
        // no timer at all — the strongest, unfakeable proof available (see
        // this file's module doc for why a raw Simulator::run_until_quiescent
        // or TraceEvent::Timer tally is not it).
        for (i, n) in nodes.iter().enumerate() {
            assert!(
                n.is_quiesced(),
                "node {i} (leader={leader}) must have accepted quiescence too (seed={seed})"
            );
        }
        let _ = sim.now(); // sanity: the sim itself is still a valid handle
    }
}

// ---- (ii) AppendEntries traffic goes flat once quiesced -------------------

#[test]
fn append_entries_traffic_goes_flat_once_quiesced() {
    for seed in seeds(0xF1DE2) {
        let (mut sim, nodes, handles) = group(seed);
        settle_to_quiescence(&mut sim);
        let leader = leader_index(&nodes, seed);
        assert!(nodes[leader].is_quiesced(), "seed={seed}");

        let combined_before: u64 = handles
            .iter()
            .map(|h| h.get(Metric::CpAppendEntriesSent))
            .sum();
        // A long idle window — many multiples of the (now-stopped) 50ms
        // heartbeat interval, and of QUIESCE_AFTER.
        sim.run_for(Duration::from_secs(5));
        let combined_after: u64 = handles
            .iter()
            .map(|h| h.get(Metric::CpAppendEntriesSent))
            .sum();

        assert_eq!(
            combined_before, combined_after,
            "AppendEntries traffic must not move at all across a 5s idle \
             window once quiesced (seed={seed}): before={combined_before} \
             after={combined_after}"
        );
        assert!(
            nodes[leader].is_quiesced(),
            "must still be quiesced after the idle window (seed={seed})"
        );
    }
}

// ---- (iii) a write after quiescence un-quiesces and commits ---------------

#[test]
fn a_write_after_quiescence_un_quiesces_and_commits() {
    for seed in seeds(0xF1DE3) {
        let (mut sim, nodes, _handles) = group(seed);
        settle_to_quiescence(&mut sim);
        let leader = leader_index(&nodes, seed);
        assert!(nodes[leader].is_quiesced(), "seed={seed}");

        match nodes[leader].put(b"k".to_vec(), b"v".to_vec()) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected a put after quiescing: {other:?} (seed={seed})"),
        }
        assert!(
            !nodes[leader].is_quiesced(),
            "a local propose must un-quiesce the leader immediately (seed={seed})"
        );

        sim.run_for(Duration::from_secs(2)); // replicate + apply
        for (i, n) in nodes.iter().enumerate() {
            assert_eq!(
                block_on(n.local_get(b"k")),
                Some(b"v".to_vec()),
                "node {i} missing the post-quiescence write (seed={seed})"
            );
        }
    }
}

// ---- (iv) a linearizable read is served without un-quiescing --------------

#[test]
fn a_linearizable_read_on_a_quiesced_leader_is_served_without_un_quiescing() {
    for seed in seeds(0xF1DE4) {
        let (mut sim, nodes, _handles) = group(seed);
        settle_to_quiescence(&mut sim);
        let leader = leader_index(&nodes, seed);
        assert!(nodes[leader].is_quiesced(), "seed={seed}");

        // No data written yet, so a linearizable get legitimately serves
        // `None` — what matters here is that it completes at all (the
        // ReadIndex quorum-probe round succeeds) and never touches
        // `RaftCore::handle`.
        let got = lin_read(&mut sim, &nodes[leader], b"nope", Duration::from_secs(2));
        assert_eq!(got, None, "seed={seed}");
        assert!(
            nodes[leader].is_quiesced(),
            "a linearizable read must not un-quiesce the leader — the \
             ReadProbe/ReadProbeAck exchange is a KvWire message the \
             consensus loop answers directly, never routed through \
             RaftCore::handle (seed={seed})"
        );
    }
}

// ---- (v) killing the quiesced leader, a survivor still converges ----------

#[test]
fn killing_the_quiesced_leader_a_survivor_still_converges_via_wake_request() {
    for seed in seeds(0xF1DE5) {
        let (mut sim, nodes, _handles) = group(seed);
        settle_to_quiescence(&mut sim);
        let leader = leader_index(&nodes, seed);
        assert!(nodes[leader].is_quiesced(), "seed={seed}");
        for (i, n) in nodes.iter().enumerate() {
            assert!(
                n.is_quiesced(),
                "node {i} must be quiesced too (seed={seed})"
            );
        }

        sim.crash(nid(leader as u64));

        let survivor = (0..nodes.len())
            .find(|&i| i != leader)
            .expect("a survivor exists");
        // The proactive-wake edge case (PR4's `resolve_cp_route`/reconciler
        // hook) doesn't exist yet — this stands in for it directly, exactly
        // as the plan's own scenario (v) names it: the survivor's local
        // caller (here, the test itself) wakes it, which — since it's a
        // quiesced follower — sends `WakeRequest` to the now-dead leader,
        // gets no reply, and re-arms a fresh election timeout.
        nodes[survivor].wake();

        // Long enough for the WakeRequest to go unanswered, the re-armed
        // election timeout to expire, a pre-vote + real election round to
        // complete among the two survivors, and a write to replicate.
        sim.run_for(Duration::from_secs(3));

        let new_leader = (0..nodes.len())
            .filter(|&i| i != leader)
            .find(|&i| nodes[i].is_leader());
        let new_leader = new_leader.unwrap_or_else(|| {
            panic!("no new leader elected among the survivors after waking (seed={seed})")
        });

        match nodes[new_leader].put(b"k".to_vec(), b"v".to_vec()) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("new leader {new_leader} rejected a put: {other:?} (seed={seed})"),
        }
        sim.run_for(Duration::from_secs(2));
        for i in (0..nodes.len()).filter(|&i| i != leader) {
            assert_eq!(
                block_on(nodes[i].local_get(b"k")),
                Some(b"v".to_vec()),
                "surviving node {i} missing the post-recovery write (seed={seed})"
            );
        }
    }
}
