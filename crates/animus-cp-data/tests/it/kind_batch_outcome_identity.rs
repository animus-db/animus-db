//! Regression: the `KindBatch` apply-time outcome channel (ADR 0041 §3) is
//! keyed by Raft log index, but an *accepted* (appended-locally) entry is not
//! yet a *committed* one — `ProposeResult::Accepted` means "appended to my
//! own log", never "committed" (see the root `CLAUDE.md`'s durable-before-
//! visible bullet). If the proposer loses leadership before its entry
//! commits, log-matching truncates it, and a completely different `KindBatch`
//! can commit and apply at the identical index — recording `Applied` there
//! for *its own* content, not the original proposer's. Index alone cannot
//! tell "my entry applied" from "a different entry now occupies my old
//! index"; only the pair (index, term) can, by Raft's log-matching property
//! (identical index + term implies identical entry, cluster-wide, for the
//! life of the log) — which is why `KindBatchOutcomes`/
//! `RaftKvNode::kind_batch_outcome` carry the entry's own term alongside
//! the outcome (see `KindBatchOutcome`'s doc in `animus_cp_data`).
//!
//! This reproduces the truncation directly: isolate the leader, let it accept
//! two entries locally that never commit, let the survivors elect a new
//! leader whose election no-op and first real `KindBatch` occupy the
//! identical two log positions, then heal the partition so the isolated
//! leader's uncommitted tail is truncated and overwritten. The recorded
//! outcome at the colliding index is `Applied` — but at the *survivors'*
//! term, never the isolated leader's — so a confirm predicate that checks
//! `term == accepted_term` (mirroring `animusd::poll_probe`'s fix) correctly
//! rejects it, and the isolated leader's own write is confirmed to never
//! land anywhere.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{KIND_BASE, KindBatchOutcome, RaftKvNode};
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
fn a_truncated_entrys_index_is_not_falsely_confirmed_by_a_reoccupying_entry() {
    let seed = 0x0334_0001;
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

    // Two entries the isolated leader accepts locally and will never commit.
    // The first just occupies a log slot; the second — `mine` — is the write
    // under test.
    let filler = nodes[old].put_kind_batch(
        vec![(KIND_BASE, b"filler".to_vec(), Some(b"f".to_vec()))],
        Vec::new(),
    );
    assert!(
        matches!(filler, ProposeResult::Accepted { .. }),
        "isolated leader must still accept locally (seed={seed}): {filler:?}"
    );

    let mine_key = b"mine".to_vec();
    let mine_val = b"my-write".to_vec();
    let (accepted_index, accepted_term) = match nodes[old].put_kind_batch(
        vec![(KIND_BASE, mine_key.clone(), Some(mine_val.clone()))],
        Vec::new(),
    ) {
        ProposeResult::Accepted { index, term } => (index, term),
        other => panic!("isolated leader must still accept locally (seed={seed}): {other:?}"),
    };

    // The survivors, now leaderless, elect a new leader — its election no-op
    // occupies the identical first slot the isolated leader's filler entry
    // did (both logs shared the same length right before the isolation).
    sim.run_for(Duration::from_secs(3));
    let new = leader(&nodes, &survivors, seed);
    assert_ne!(new, old, "the new leader must not be the isolated node");

    // The new leader's own (different) write is its first real proposal,
    // landing at the identical index the isolated leader's own write did —
    // the collision this test exists to force.
    let theirs_key = b"theirs".to_vec();
    let theirs_val = b"their-write".to_vec();
    let (theirs_index, theirs_term) = match nodes[new].put_kind_batch(
        vec![(KIND_BASE, theirs_key.clone(), Some(theirs_val.clone()))],
        Vec::new(),
    ) {
        ProposeResult::Accepted { index, term } => (index, term),
        other => panic!("new leader rejected its own write (seed={seed}): {other:?}"),
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
    // *new* leader's write at `accepted_index`, recorded under the *new*
    // leader's term, never the old one's.
    for (i, node) in nodes.iter().enumerate() {
        let recorded = node.kind_batch_outcome(accepted_index);
        assert_eq!(
            recorded,
            Some((theirs_term, KindBatchOutcome::Applied)),
            "node {i}: the reoccupying entry's own recorded outcome (seed={seed})"
        );

        // The load-bearing assertion: an index-alone confirm predicate would
        // have called this a confirm of the ORIGINAL proposer's write
        // (`Applied`, regardless of which term recorded it — the bug this
        // suite exists to catch). The fix's predicate — term must also
        // match `accepted_term` — correctly rejects it.
        let (recorded_term, outcome) = recorded.expect("checked above");
        let an_index_only_predicate_would_confirm = outcome == KindBatchOutcome::Applied;
        let the_fixed_predicate_confirms =
            recorded_term == accepted_term && outcome == KindBatchOutcome::Applied;
        assert!(
            an_index_only_predicate_would_confirm,
            "node {i}: sanity check — the scenario must actually reach an \
             `Applied` outcome at this index, or it doesn't exercise the old \
             bug at all (seed={seed})"
        );
        assert!(
            !the_fixed_predicate_confirms,
            "node {i}: a term mismatch must never be treated as a confirm of \
             the original write — this is the false-ack the fix closes \
             (seed={seed})"
        );

        assert_eq!(
            block_on(node.local_get(&theirs_key)),
            Some(theirs_val.clone()),
            "node {i}: the new leader's write landed (seed={seed})"
        );
        assert_eq!(
            block_on(node.local_get(&mine_key)),
            None,
            "node {i}: the truncated write must never appear anywhere — \
             confirming it would have been the silent lost write this fix \
             prevents (seed={seed})"
        );
    }
}
