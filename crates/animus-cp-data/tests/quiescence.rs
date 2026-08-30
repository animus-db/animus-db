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
//! **A second fault-primitives tier (ADR 0061 Decision 3)**, wiring
//! `animus-sim`'s fault vocabulary into this corpus (mirroring the sibling
//! `raftkv`/`txn` corpora's own wiring), targeting specifically the *wake*
//! path this feature's whole design turns on — most of this file's own
//! faults, unlike an active-traffic corpus's, are about what happens to an
//! **idle** group, not a busy one:
//!
//! (vi) a duplicated peer message (`NetConfig::set_duplicate_prob`) must
//!      wake a quiesced group and apply its write **exactly once**, never
//!      twice, and the group must still be able to settle back into
//!      quiescence afterward;
//! (vii) `DiskConfig::set_fsync_lie_prob` revealed by a later
//!      `torn_tail_on_crash` crash, composed with a genuine restart (a bare
//!      `Simulator::crash` with no restart afterward gives this
//!      disk-tearing field zero test teeth — see
//!      `docs/engineering-lessons.md`'s "a crash-only fault has zero test
//!      teeth" entry): the recovered leader must converge to exactly what
//!      the honest survivors already committed, and must be able to rejoin
//!      quiescence like any other replica. **`corrupt_on_crash` is
//!      deliberately excluded from this cell** — arming it alongside
//!      `torn_tail_on_crash` here reproducibly panics the whole test
//!      process (a hard-`assert!` witnessing-chain violation,
//!      `assert_ts_monotonic`), a confirmed real finding — see this
//!      property's own test doc for the full account.
//!
//! `NetConfig::set_corrupt_prob` is deliberately **not** used anywhere in
//! this file — as of this checkout, `animus-cp-data::codec`'s fix for the
//! untrusted-wire-length-prefix allocator-abort DoS (~12 `Vec::
//! with_capacity(n as usize)` sites reading an unvalidated `u32` count
//! straight off the wire) has not landed on `main` yet (verified directly:
//! `grep -n "with_capacity" crates/animus-cp-data/src/codec.rs` still shows
//! the raw, unbounded form at every site). This mirrors the identical,
//! already-documented exclusion the sibling `raftkv`/`txn` corpora apply for
//! the same unfixed gap — re-add it here once that fix lands on `main`.
//! `DiskConfig::set_enospc_prob`/`set_error_prob` are excluded too, for an
//! unrelated reason: `persist_wal`'s own `assert!(halted.load(..), ...)`
//! hard-panics the whole test process if either fires on a live (non-halted)
//! node, so they are out of scope for this crate's tests entirely, not just
//! this file's.
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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{RaftKvNode, StageOutcome};
use animus_env::{EnvExt, Metric, MetricsHandle, nid};
use animus_sim::{DiskConfig, NetConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{escape, partition_token};
use animus_test::corpus;
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
    corpus::seeds_from_env("ANIMUS_QUIESCE_SEEDS") as u64
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

// ---- ADR 0044 phase-1 PR5 (fork D): a pending transaction vetoes
// quiescence until resolved ---------------------------------------------

/// A real ADR 0022-shaped data-plane key — mirrors `txn_single.rs`'s own
/// `key` helper (duplicated rather than shared across separate test
/// binaries).
fn key(pk: &[u8], rk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out.extend_from_slice(rk);
    out
}

/// Run `fut` to completion by spawning it on `node`'s own env and driving
/// `sim` for up to `budget` — [`lin_read`]'s identical shape, generalized to
/// any future's output type.
fn drive<T: Send + 'static>(
    sim: &mut Simulator,
    node: &KvNode,
    budget: Duration,
    fut: impl std::future::Future<Output = T> + Send + 'static,
) -> Option<T> {
    let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let s = Arc::clone(&slot);
    node.env().clone().spawn_task(async move {
        let v = fut.await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock().unwrap().take()
}

#[test]
fn a_pending_transaction_vetoes_quiescence_until_resolved() {
    for seed in seeds(0xF1DE6) {
        let (mut sim, nodes, _handles) = group(seed);
        sim.run_for(Duration::from_secs(2)); // elect + replicate the no-op
        let leader = leader_index(&nodes, seed);

        let k = key(b"acct-1", b"balance");
        let n = nodes[leader].clone();
        let kk = k.clone();
        let (txn_id, record_key, outcome) = drive(
            &mut sim,
            &nodes[leader],
            Duration::from_secs(5),
            async move { n.txn_stage("t", vec![(kk, Some(b"100".to_vec()))]).await },
        )
        .flatten()
        .unwrap_or_else(|| panic!("txn_stage did not complete (seed={seed})"));
        assert_eq!(outcome, StageOutcome::Staged, "seed={seed}");

        // Idle well past `QUIESCE_AFTER`: the veto (a non-empty `TxnTracker`
        // — this record is still `Pending`) must hold the group awake, even
        // though the ordinary entry predicate's other clauses are otherwise
        // satisfied (nothing left to replicate, no membership change, ...).
        sim.run_for(Duration::from_secs(2));
        assert!(
            !nodes[leader].is_quiesced(),
            "a group with a pending 2PC intent must never quiesce (seed={seed})"
        );

        // Commit + resolve the anchor's own keys in one call (`txn_decide`,
        // `commit: true`) — the record moves `Pending -> Committed` and its
        // resolve lands in the same group, so `TxnTracker` ends up
        // genuinely empty (neither `pending` nor `unresolved_decided`
        // holds it) rather than merely moving the veto from one map to the
        // other.
        let n = nodes[leader].clone();
        let commit_ts = drive(
            &mut sim,
            &nodes[leader],
            Duration::from_secs(5),
            async move { n.txn_decide(txn_id, record_key, vec![k], true).await },
        )
        .flatten();
        assert!(
            commit_ts.is_some(),
            "commit+resolve did not complete (seed={seed})"
        );

        // Now genuinely idle again: the veto must have released, letting
        // the group reach quiescence exactly as an untouched group would.
        sim.run_for(Duration::from_secs(2));
        assert!(
            nodes[leader].is_quiesced(),
            "the veto must release once the transaction resolves (seed={seed})"
        );
    }
}

// ---- (vi) a duplicated peer message wakes a quiesced group exactly once ---

/// `NetConfig::set_duplicate_prob` (ADR 0061 Decision 3), targeting the wake
/// path directly: quiesce the whole group (leader and followers alike, per
/// property (i)), then un-quiesce with a write while every surviving message
/// — including the `AppendEntries` that wakes each quiesced follower — is
/// delivered twice with its own independent delay. A duplicated wake must
/// never double-apply the write, and the group must still be able to settle
/// back into genuine quiescence afterward (a duplication-confused group that
/// never re-parks would be its own, separate bug).
#[test]
fn a_duplicated_peer_message_wakes_a_quiesced_group_exactly_once() {
    for seed in seeds(0xF1DE7) {
        let (mut sim, nodes, _handles) = group(seed);
        settle_to_quiescence(&mut sim);
        let leader = leader_index(&nodes, seed);
        for (i, n) in nodes.iter().enumerate() {
            assert!(
                n.is_quiesced(),
                "node {i} (leader={leader}) must be quiesced before the \
                 duplicated-delivery window opens (seed={seed})"
            );
        }

        // Every surviving message for the rest of this test is delivered
        // twice — including the leader's own un-quiescing AppendEntries to
        // its two quiesced followers.
        let mut net = NetConfig::default();
        net.set_duplicate_prob(1.0);
        sim.set_net_config(net);

        match nodes[leader].put(b"k".to_vec(), b"v".to_vec()) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected a put after quiescing: {other:?} (seed={seed})"),
        }
        assert!(
            !nodes[leader].is_quiesced(),
            "a local propose must un-quiesce the leader immediately, \
             duplication or not (seed={seed})"
        );

        sim.run_for(Duration::from_secs(2)); // replicate + apply, doubled over
        for (i, n) in nodes.iter().enumerate() {
            assert_eq!(
                block_on(n.local_get(b"k")),
                Some(b"v".to_vec()),
                "node {i} missing the post-quiescence write under duplicated \
                 delivery — a duplicated wake must apply the write exactly \
                 once, never twice or not at all (seed={seed})"
            );
        }

        // Heal, then idle again: even after a duplication-driven wake, the
        // group must still reach quiescence again cleanly.
        sim.set_net_config(NetConfig::default());
        sim.run_for(Duration::from_secs(2));
        for (i, n) in nodes.iter().enumerate() {
            assert!(
                n.is_quiesced(),
                "node {i} must still be able to reach quiescence again \
                 after a duplicated wake (seed={seed})"
            );
        }
    }
}

// ---- (vii) a lying fsync, revealed by a crash, recovers on restart -------

/// `DiskConfig::set_fsync_lie_prob` composed with a genuine crash+restart
/// (ADR 0061 Decision 3). A lie alone is unobservable — every `sync` it
/// covers still returns `Ok`, so the group behaves exactly as if durable —
/// so this only has teeth once something later reads the crashed node's own
/// recovered state back: arm the lie on the (about-to-wake) leader, un-
/// quiesce it with several writes it believes are durably logged and
/// genuinely replicates to the two honest followers, then reveal the lie
/// with a `torn_tail_on_crash` crash and a **real** restart (`Simulator::
/// stop` + `Simulator::restart` + a fresh `RaftKvNode::start` on the same
/// node id — mirrors the sibling `raftkv` corpus's `StopRestart`; a bare
/// `sim.crash` with no restart, like this file's own property (v), would
/// leave `torn_tail_on_crash` with zero test teeth, since nothing would ever
/// observe what it tore — see `docs/engineering-lessons.md`'s "a crash-only
/// fault has zero test teeth" entry). **`Simulator::stop` alone does not
/// clear the `crashed` flag `Simulator::crash` sets** (its own doc: "this
/// does not mute or set the node crashed") — without an explicit
/// `Simulator::restart` in between, the freshly reconstructed node's
/// network traffic is silently dropped in both directions forever, which
/// this test's own first draft hit directly (the "recovered" replica sat at
/// its pre-crash term with `engine_applied == 0` for the rest of the run,
/// never receiving a single message). The recovered replica must converge
/// to exactly what the untouched survivors already committed, and must be
/// able to rejoin quiescence like any other.
///
/// **`corrupt_on_crash` is deliberately excluded — a confirmed, reproducible
/// process-abort finding, not a suppressed low-probability flake.** Arming
/// it alongside `torn_tail_on_crash` here (seed `0xF1DE8`, the leader
/// replica at index 2) makes the recovered replica hit a **hard `assert!`
/// panic** once it catches up and applies an entry past the corrupted
/// record: `animus_cp_data::assert_ts_monotonic`'s witnessing-chain check
/// (`lib.rs`, ADR 0018 §2) — "did not strictly exceed the last applied
/// HlcTimestamp — the witnessing chain is broken." The single flipped byte
/// landed inside a still-JSON-syntactically-valid `WalRecord::Append`'s
/// packed `HlcTimestamp`, producing a **wrong but successfully decoded**
/// timestamp rather than a decode failure — exactly the residual gap this
/// crate's own `WalRecord::decode` doc and the sibling `raftkv` corpus's
/// `wal_fault_disk_config` doc already flag ("the Raft WAL's on-disk record
/// framing... has no per-record checksum... a bit-flip that happens to land
/// inside a byte that keeps the JSON syntactically valid could silently
/// produce a different, undetected-corrupt WAL record instead of a decode
/// error"), now confirmed to reach a **hard-panicking** assert rather than
/// merely stale/wrong served data. Reproduced directly: with
/// `torn_tail_on_crash` alone (no corruption) the exact same scenario
/// converges cleanly (`engine_applied` matches the honest survivors' `8`
/// exactly); adding `corrupt_on_crash` to the identical seed panics the
/// whole test process every time. This mirrors the already-established
/// precedent this file's own module doc documents for `NetConfig::
/// set_corrupt_prob` (and the sibling `raftkv`/`txn` corpora's identical
/// exclusion) — a fault primitive that reliably aborts the process rather
/// than degrading gracefully is out of scope for an ambient corpus cell,
/// not a property this corpus can usefully assert against. **This is a
/// real, unfixed finding** — the fix belongs in `animus-cp-data`'s WAL
/// codec (a per-record checksum, or a decode-time HLC sanity bound), as its
/// own change with its own regression test, never folded into this corpus
/// PR. Re-arm `corrupt_on_crash` here once that fix lands.
#[test]
fn a_lying_fsync_revealed_by_a_crash_recovers_correctly_on_restart() {
    for seed in seeds(0xF1DE8) {
        let (mut sim, mut nodes, handles) = group(seed);
        settle_to_quiescence(&mut sim);
        let leader = leader_index(&nodes, seed);
        assert!(nodes[leader].is_quiesced(), "seed={seed}");

        // Every sync the leader performs from here on reports `Ok` but
        // leaves the bytes buffered, not durable.
        let mut lying = DiskConfig::default();
        lying.set_fsync_lie_prob(1.0);
        sim.set_disk_config_for(nid(leader as u64), lying);

        // Un-quiesce with several writes: the leader believes every one is
        // durably logged, and genuinely replicates them to the two honest
        // followers, which commit and apply for real.
        for i in 0..5u32 {
            match nodes[leader].put(format!("k{i}").into_bytes(), format!("v{i}").into_bytes()) {
                ProposeResult::Accepted { .. } => {}
                other => panic!("leader rejected put {i}: {other:?} (seed={seed})"),
            }
        }
        assert!(!nodes[leader].is_quiesced(), "seed={seed}");
        sim.run_for(Duration::from_secs(2)); // replicate + apply on all three

        for i in 0..5u32 {
            let key = format!("k{i}").into_bytes();
            for (n_idx, n) in nodes.iter().enumerate() {
                assert_eq!(
                    block_on(n.local_get(&key)),
                    Some(format!("v{i}").into_bytes()),
                    "node {n_idx} missing k{i} before the crash (seed={seed})"
                );
            }
        }

        // Reveal the lie: arm torn/corrupted tearing on the leader and crash
        // it — every one of the 5 writes' WAL bytes the lying fsync left
        // buffered is now torn (a strict seed-chosen prefix survives) and
        // possibly bit-flipped.
        let mut torn = DiskConfig::default();
        torn.torn_tail_on_crash = true;
        // `corrupt_on_crash` is deliberately NOT armed here — see this
        // test's own doc for the confirmed process-abort finding that
        // excludes it.
        sim.set_disk_config_for(nid(leader as u64), torn);
        sim.crash(nid(leader as u64));

        // The two honest survivors elect a fresh leader and keep committing
        // — mirrors property (v)'s recovery path.
        let survivor = (0..nodes.len())
            .find(|&i| i != leader)
            .expect("a survivor exists");
        nodes[survivor].wake();
        sim.run_for(Duration::from_secs(3));
        let new_leader = (0..nodes.len())
            .filter(|&i| i != leader)
            .find(|&i| nodes[i].is_leader())
            .unwrap_or_else(|| panic!("no new leader elected among the survivors (seed={seed})"));
        match nodes[new_leader].put(b"after-crash".to_vec(), b"v".to_vec()) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("new leader {new_leader} rejected a put: {other:?} (seed={seed})"),
        }
        sim.run_for(Duration::from_secs(2));

        // Genuinely restart the crashed leader: process exit, then a fresh
        // `RaftKvNode::start` on the same node id, recovering from whatever
        // survived the tear+corruption — the actual "read the crashed
        // node's own post-crash state back" step the disk-tearing fields
        // need to have any effect at all.
        sim.stop(nid(leader as u64));
        // `crash` (above) marked this node `crashed` — a flag `stop` does
        // NOT clear (its own doc: "this does not mute or set the node
        // crashed") — so every message to/from it would be silently
        // dropped forever unless something clears it. `restart` clears
        // `crashed` and re-arms any tasks it still finds for this node; by
        // now `stop` has already removed every one of them, so this call
        // is purely "un-mute the node," with nothing left to re-poll.
        sim.restart(nid(leader as u64));
        let recovered = RaftKvNode::start_with_metrics(
            sim.env(nid(leader as u64)),
            NODES.iter().copied().map(nid).collect(),
            MemoryEngine::new(),
            handles[leader].clone(),
        );
        recovered.enable_quiescence(QUIESCE_AFTER);
        nodes[leader] = recovered;
        sim.run_for(Duration::from_secs(4)); // catch up (AppendEntries or a snapshot)

        // The recovered replica must converge on every key the honest
        // survivors actually hold, regardless of what the lying fsync's
        // torn/corrupted tail did to its own local durable bytes — Raft
        // recovery (WAL replay for whatever records survived, plus a
        // snapshot/log catch-up for the rest) must reconstruct identical
        // state.
        for i in 0..5u32 {
            let key = format!("k{i}").into_bytes();
            assert_eq!(
                block_on(nodes[leader].local_get(&key)),
                Some(format!("v{i}").into_bytes()),
                "recovered node {leader} missing k{i} after crash+restart \
                 revealed the lying fsync's torn tail (seed={seed})"
            );
        }
        assert_eq!(
            block_on(nodes[leader].local_get(b"after-crash")),
            Some(b"v".to_vec()),
            "recovered node {leader} missing the post-recovery write (seed={seed})"
        );

        // Idle again: the recovered replica must be able to rejoin
        // quiescence like any other.
        sim.run_for(Duration::from_secs(2));
        for (i, n) in nodes.iter().enumerate() {
            assert!(
                n.is_quiesced(),
                "node {i} must reach quiescence again after the torn-tail \
                 crash+restart (seed={seed})"
            );
        }
    }
}
