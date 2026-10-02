//! Regression: the `Cas` apply-time outcome channel (ADR 0017) is keyed by
//! Raft log index, but an *accepted* (appended-locally) entry is not yet a
//! *committed* one — `ProposeResult::Accepted` means "appended to my own
//! log", never "committed" (see the root `CLAUDE.md`'s durable-before-visible
//! bullet). If the proposer loses leadership before its entry commits,
//! log-matching truncates it, and a completely different `Cas` can commit and
//! apply at the identical index — recording an outcome there for *its own*
//! content, not the original proposer's. Index alone cannot tell "my entry
//! applied" from "a different entry now occupies my old index"; only the pair
//! (index, term) can, by Raft's log-matching property (identical index + term
//! implies identical entry, cluster-wide, for the life of the log) — which is
//! why `CasResults`/`RaftKvNode::cas_result` now carry the entry's own term
//! alongside the outcome, mirroring the fix already shipped for
//! `KindBatchOutcomes` (see `tests/kind_batch_outcome_identity.rs`) and
//! extended here to `Cas`'s sibling channel, `StageOutcomes`
//! (`TxnStage`/`StageOutcome`), which had the identical gap.
//!
//! This reproduces the truncation directly: isolate the leader, let it accept
//! two entries locally that never commit, let the survivors elect a new
//! leader whose election no-op and first real `Cas` occupy the identical two
//! log positions, then heal the partition so the isolated leader's
//! uncommitted tail is truncated and overwritten. The recorded outcome at the
//! colliding index is present — but at the *survivors'* term, never the
//! isolated leader's — so:
//!
//! - an index-only reader (the pre-fix shape) would report a confirmed
//!   outcome for the ORIGINAL proposer's `Cas`, which never actually landed
//!   anywhere — the exact false-ack class this fix closes;
//! - the fixed `cas_result(index, term)` correctly returns `None` when
//!   queried with the ORIGINAL proposer's own accepted term, and the
//!   fixed `compare_and_swap` (which now also checks `is_leader()` every
//!   poll, closing the stale-comment/missing-guard mismatch) never reports a
//!   definitive `Some(_)` for the truncated attempt.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).
//! Replay a specific run with `ANIMUS_SEED=<seed> cargo test -p
//! animus-cp-data --test cas_outcome_identity`.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
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

#[test]
fn a_truncated_cas_index_is_not_falsely_confirmed_by_a_reoccupying_entry() {
    let seed = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x0CA5_0001);
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect

    let old = leader(&nodes, &[0, 1, 2], seed);
    let survivors: Vec<usize> = (0..3).filter(|&i| i != old).collect();

    // Isolate the leader from both survivors. It keeps believing it's the
    // leader (no leader-lease step-down in this core — see
    // `animus-control/CLAUDE.md`'s pre-vote entry), so it keeps *accepting*
    // proposals (appending to its own log) even though it can never again
    // reach a majority to commit them.
    for &s in &survivors {
        sim.partition_pair(nid(old as u64), nid(s as u64));
    }

    // A filler entry the isolated leader accepts locally and will never
    // commit — occupies the log slot the survivors' own election no-op will
    // later claim, aligning the collision index below.
    let filler = nodes[old].cas(b"filler".to_vec(), None, b"f".to_vec());
    assert!(
        matches!(filler, ProposeResult::Accepted { .. }),
        "isolated leader must still accept locally (seed={seed}): {filler:?}"
    );

    // `mine` — the write under test — is the isolated leader's own CAS.
    // Seed the key beforehand isn't necessary: CAS-if-absent (`expected:
    // None`) accepts on an empty key.
    let mine_key = b"mine".to_vec();
    let (accepted_index, accepted_term) =
        match nodes[old].cas(mine_key.clone(), None, b"my-write".to_vec()) {
            ProposeResult::Accepted { index, term } => (index, term),
            other => panic!("isolated leader must still accept locally (seed={seed}): {other:?}"),
        };

    // The survivors, now leaderless, elect a new leader — its election no-op
    // occupies the identical first slot the isolated leader's filler entry
    // did (both logs shared the same length right before the isolation).
    sim.run_for(Duration::from_secs(3));
    let new = leader(&nodes, &survivors, seed);
    assert_ne!(new, old, "the new leader must not be the isolated node");

    // The new leader's own (different) CAS is its first real proposal,
    // landing at the identical index the isolated leader's own write did —
    // the collision this test exists to force.
    let theirs_key = b"theirs".to_vec();
    let (theirs_index, theirs_term) =
        match nodes[new].cas(theirs_key.clone(), None, b"their-write".to_vec()) {
            ProposeResult::Accepted { index, term } => (index, term),
            other => panic!("new leader rejected its own CAS (seed={seed}): {other:?}"),
        };
    assert_eq!(
        theirs_index, accepted_index,
        "the collision this test needs didn't happen — indices differ (seed={seed})"
    );
    assert_ne!(
        theirs_term, accepted_term,
        "the collision needs distinct terms, or it doesn't exercise the bug (seed={seed})"
    );
    sim.run_for(Duration::from_secs(2)); // the survivors commit + apply it

    // Heal: the old leader rejoins, discovers a higher term, steps down, and
    // log-matching truncates its uncommitted tail (both entries) in favor of
    // the new leader's committed ones.
    for &s in &survivors {
        sim.heal(nid(old as u64), nid(s as u64));
    }
    sim.run_for(Duration::from_secs(3));

    // Every replica — including the formerly-isolated one — now has the
    // *new* leader's CAS at `accepted_index`, recorded under the *new*
    // leader's term, never the old one's.
    for (i, node) in nodes.iter().enumerate() {
        // Sanity check: an index-only predicate (the pre-fix shape) sees a
        // recorded outcome at the colliding index — this scenario must
        // actually reach that state, or it doesn't exercise the old bug at
        // all.
        let index_only_result = node.cas_result(accepted_index, theirs_term);
        assert!(
            index_only_result.is_some(),
            "node {i}: sanity check — the scenario must actually reach a \
             recorded outcome at this index (under the reoccupying entry's \
             own term), or it doesn't exercise the old bug at all (seed={seed})"
        );

        // The load-bearing assertion: querying with the ORIGINAL (truncated)
        // proposer's own accepted term must never return the reoccupying
        // entry's outcome as a confirm of the original write — this is the
        // false-ack the fix closes. `None` here means "not mine" / "retry",
        // never a false positive OR a false negative for the original CAS.
        assert_eq!(
            node.cas_result(accepted_index, accepted_term),
            None,
            "node {i}: a term mismatch must never be treated as a confirm of \
             the original CAS — this is the false-ack the fix closes \
             (seed={seed})"
        );

        assert_eq!(
            block_on(node.local_get(&theirs_key)),
            Some(b"their-write".to_vec()),
            "node {i}: the new leader's CAS landed (seed={seed})"
        );
        assert_eq!(
            block_on(node.local_get(&mine_key)),
            None,
            "node {i}: the truncated CAS must never appear anywhere — \
             confirming it would have been the silent lost write this fix \
             prevents (seed={seed})"
        );
    }

    // `compare_and_swap`'s own end-to-end polling loop must agree: replaying
    // the identical scenario through the public async entry point (rather
    // than the low-level `cas`/`cas_result` pair above) must never report a
    // definitive `Some(_)` for an attempt that was truncated out from under
    // it — it must poll until `is_leader()` goes false (this fixed loop's
    // own guard, mirroring `wait_applied`/`wait_stage_outcome`) or the
    // timeout elapses, then give up with `None`.
    let (mut sim2, nodes2) = group(seed);
    sim2.run_for(Duration::from_secs(2));
    let old2 = leader(&nodes2, &[0, 1, 2], seed);
    let survivors2: Vec<usize> = (0..3).filter(|&i| i != old2).collect();
    for &s in &survivors2 {
        sim2.partition_pair(nid(old2 as u64), nid(s as u64));
    }
    let _ = nodes2[old2].cas(b"filler".to_vec(), None, b"f".to_vec());
    let mut mine_future =
        Box::pin(nodes2[old2].compare_and_swap(b"mine".to_vec(), None, b"my-write".to_vec()));
    poll_once(&mut mine_future);
    sim2.run_for(Duration::from_secs(3));
    let new2 = leader(&nodes2, &survivors2, seed);
    assert_ne!(new2, old2, "the new leader must not be the isolated node");
    let _ = nodes2[new2].cas(b"theirs".to_vec(), None, b"their-write".to_vec());
    sim2.run_for(Duration::from_secs(2));
    for &s in &survivors2 {
        sim2.heal(nid(old2 as u64), nid(s as u64));
    }
    // Drive well past `compare_and_swap`'s own `CAS_TIMEOUT` so the future is
    // guaranteed to have reached a terminal poll either via the `is_leader()`
    // guard or the deadline.
    sim2.run_for(Duration::from_secs(8));
    match poll_result(&mut mine_future) {
        None => panic!(
            "compare_and_swap must reach a terminal (Ready) state well past \
             its own CAS_TIMEOUT (seed={seed})"
        ),
        Some(outcome) => assert_eq!(
            outcome, None,
            "compare_and_swap must never report a definitive outcome for an \
             attempt truncated by a leadership change (seed={seed}): {outcome:?}"
        ),
    }
}

/// Poll a future once on a no-op waker, discarding the result; used to issue
/// a `compare_and_swap`'s proposal eagerly before driving virtual time (the
/// same technique `tests/cas.rs` uses).
fn poll_once<F: std::future::Future + Unpin>(f: &mut F) {
    use std::task::Context;
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let _ = std::pin::Pin::new(f).poll(&mut cx);
}

/// Poll a future once and return its output if it is `Ready` (else `None`).
fn poll_result<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    use std::task::{Context, Poll};
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    match std::pin::Pin::new(f).poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}
