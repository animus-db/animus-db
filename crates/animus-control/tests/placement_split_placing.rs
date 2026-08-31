//! ADR 0062 §2's reconcile-loop third phase: **directed-Placing
//! convergence** (rung 3 of the ADR's own sequencing — the mechanism this
//! file exercises is `Metadata::split_placing_reconcile`/
//! `PlacementView::split_placing_reconcile`, wired into `node.rs`'s
//! `reconcile_loop` after repair and rebalance, unconditionally every tick).
//!
//! Mirrors `placement_reconcile.rs`/`placement_rebalance.rs`'s own
//! through-Raft, `SimEnv`-driven style: real `MetaCommand`s proposed through
//! a real `RaftNode` cluster, converged-or-timeout polling, no shortcuts
//! into `Metadata` internals — except the one test (explicitly marked) that
//! proves a safety property only reachable by driving `Metadata::apply`
//! directly, since the live driver's own synchronous per-tick body makes
//! the equivalent same-tick interleaving structurally unreachable through
//! the public `RaftNode` API (see that test's own doc for why).
//!
//! Every registered data member heartbeats (ADR 0030 phantom-member
//! hardening, the same `register`-spawns-a-heartbeat idiom
//! `placement_rebalance.rs` already documents) so a candidate used across
//! several reconcile ticks never gets flipped back to `Down` by the
//! failure detector mid-test.
//!
//! `MarkSplitPlacingDone` is never proposed here except as inert test setup
//! for the "never touches a `done` entry" case — wiring its real completion
//! loop is ADR 0062 §3, a later rung.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::node::heartbeat_loop;
use animus_control::raft::ProposeResult;
use animus_control::{ApplyOutcome, MetaCommand, Metadata, NodeStatus, RaftNode};
use animus_env::{EnvExt, NodeId, nid};
use animus_placement::PlacementPolicy;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{Epoch, KeyRange, TabletId};

const CONTROL: [u64; 3] = [0, 1, 2];

/// A split key strictly inside `KeyRange::whole()`, reused verbatim from
/// `meta.rs`'s own in-place split fixtures.
fn split_key() -> Vec<u8> {
    0x8000_0000_0000_0000u64.to_be_bytes().to_vec()
}

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = CONTROL
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                CONTROL.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn leader_among(nodes: &[RaftNode<SimEnv>], live: &[usize]) -> usize {
    let leaders: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "expected one leader among {live:?}, got {leaders:?}"
    );
    leaders[0]
}

/// Register a data member as `Active` and start it heartbeating (ADR 0030
/// phantom-member hardening — see this file's doc for why this matters
/// across a multi-tick test).
fn register(sim: &Simulator, node: &RaftNode<SimEnv>, id: u64) {
    assert!(matches!(
        node.propose(MetaCommand::UpsertMember {
            node: nid(id),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        }),
        ProposeResult::Accepted { .. }
    ));
    let env = sim.env(nid(id));
    env.spawn_task(heartbeat_loop(
        env.clone(),
        CONTROL.iter().copied().map(nid).collect(),
    ));
}

/// Provision an RF-3-policied parent tablet at `TabletId(1)` on `initial`,
/// begin an in-place split into `TabletId(2)`/`TabletId(3)` (both forked
/// verbatim onto the parent's own current replicas, per ADR 0062 §1), and
/// cut over — landing whatever `split_placing` obligation (if any)
/// `CutoverSplit`'s own apply decides. Returns the leader index used.
fn split_fixture(sim: &mut Simulator, nodes: &[RaftNode<SimEnv>], leader: usize, initial: &[u64]) {
    let replicas: Vec<_> = initial.iter().copied().map(nid).collect();
    assert!(matches!(
        nodes[leader].propose(MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("t".to_owned()),
            range: KeyRange::whole(),
            replicas: replicas.clone(),
        }),
        ProposeResult::Accepted { .. }
    ));
    assert!(matches!(
        nodes[leader].propose(MetaCommand::SetTabletPolicy {
            tablet: TabletId(1),
            policy: Some(PlacementPolicy::simple("p", 3)),
        }),
        ProposeResult::Accepted { .. }
    ));
    assert!(matches!(
        nodes[leader].propose(MetaCommand::BeginSplitInPlace {
            parent: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            split_key: split_key(),
            children: [
                (TabletId(2), replicas.clone()),
                (TabletId(3), replicas.clone()),
            ],
        }),
        ProposeResult::Accepted { .. }
    ));
    assert!(matches!(
        nodes[leader].propose(MetaCommand::CutoverSplit {
            parent: TabletId(1),
            expected_epoch: Epoch::INITIAL.next(),
            cutover_wall_ms: 1,
        }),
        ProposeResult::Accepted { .. }
    ));
    // Let the cutover commit and apply (well under one `RECONCILE_INTERVAL`
    // tick, so nothing has moved yet).
    sim.run_for(Duration::from_millis(300));
}

/// Poll (converged-or-timeout, the repo-wide idiom) until every id in
/// `children` has exactly `want` as its replica set, or give up.
fn wait_converged(
    sim: &mut Simulator,
    nodes: &[RaftNode<SimEnv>],
    leader: usize,
    children: &[TabletId],
    want: &[NodeId],
) -> bool {
    for _ in 0..60 {
        sim.run_for(Duration::from_millis(500));
        let meta = nodes[leader].metadata();
        if children.iter().all(|c| meta.tablets[c].replicas == want) {
            return true;
        }
    }
    false
}

/// **Test 1**: an un-done `split_placing` entry with `Some(target)`
/// differing from the child's current replicas gets a `CasTabletReplicas`
/// proposed by the third reconcile phase, and `Metadata`'s replicas
/// converge to that target — `target` itself (the diagnostic `CutoverSplit`
/// wrote) is left untouched, and `done` stays `false` (nothing in this rung
/// ever proposes `MarkSplitPlacingDone`).
#[test]
fn split_placing_phase_converges_a_differing_target() {
    let seed = 0x5717_0001u64;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = leader_among(&nodes, &[0, 1, 2]);

    // Four active candidates ("n10".."n13", same digit width so lexical
    // order == numeric order); RF3 over them prefers the three lowest ids
    // — [n10, n11, n12] — which the parent's own current (and thus both
    // children's fork-inherited) replicas ([n11, n12, n13]) are not.
    for id in [10, 11, 12, 13] {
        register(&sim, &nodes[leader], id);
    }
    sim.run_for(Duration::from_secs(1));

    split_fixture(&mut sim, &nodes, leader, &[11, 12, 13]);

    let want = vec![nid(10), nid(11), nid(12)];
    let meta = nodes[leader].metadata();
    for child in [TabletId(2), TabletId(3)] {
        let entry = &meta.split_placing[&child];
        assert_eq!(entry.target, Some(want.clone()), "child {child:?}");
        assert!(!entry.done, "child {child:?}");
        assert_eq!(
            meta.tablets[&child].replicas,
            vec![nid(11), nid(12), nid(13)],
            "child {child:?} moved before any reconcile tick ran"
        );
    }

    assert!(
        wait_converged(&mut sim, &nodes, leader, &[TabletId(2), TabletId(3)], &want),
        "split-placing phase never converged both children (seed={seed})"
    );

    let meta = nodes[leader].metadata();
    for child in [TabletId(2), TabletId(3)] {
        assert_eq!(meta.tablets[&child].replicas, want);
        // `target` is a diagnostic snapshot of what `CutoverSplit` decided —
        // never rewritten by the reconcile loop (ADR 0062 §2).
        assert_eq!(meta.split_placing[&child].target, Some(want.clone()));
        assert!(!meta.split_placing[&child].done);
    }
}

/// **Test 2**: a `target: None` (unsatisfiable-at-cutover) entry stays
/// completely inert while candidates remain insufficient, then converges
/// once enough `Active` members exist — `target` itself is left `None`
/// forever (a diagnostic snapshot of the cutover instant, never rewritten),
/// even once the live replicas have actually converged.
#[test]
fn split_placing_phase_stays_inert_then_converges_once_candidates_recover() {
    let seed = 0x5717_0002u64;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = leader_among(&nodes, &[0, 1, 2]);

    // No active candidates at all yet: RF3 is unsatisfiable at cutover.
    // Sacrificial parent replica ids (101/102/103), disjoint from every id
    // this file uses elsewhere or ever registers as a member.
    split_fixture(&mut sim, &nodes, leader, &[101, 102, 103]);

    let meta = nodes[leader].metadata();
    for child in [TabletId(2), TabletId(3)] {
        assert_eq!(meta.split_placing[&child].target, None, "child {child:?}");
        assert!(!meta.split_placing[&child].done, "child {child:?}");
        assert_eq!(
            meta.tablets[&child].replicas,
            vec![nid(101), nid(102), nid(103)]
        );
    }

    // Several reconcile ticks pass with candidates still insufficient:
    // nothing moves.
    sim.run_for(Duration::from_secs(3));
    let meta = nodes[leader].metadata();
    for child in [TabletId(2), TabletId(3)] {
        assert_eq!(
            meta.tablets[&child].replicas,
            vec![nid(101), nid(102), nid(103)],
            "child {child:?} moved with no active candidates at all"
        );
        assert!(!meta.split_placing[&child].done);
    }

    // Candidates recover: exactly 3 new `Active` members register.
    for id in [120, 121, 122] {
        register(&sim, &nodes[leader], id);
    }

    let want = vec![nid(120), nid(121), nid(122)];
    assert!(
        wait_converged(&mut sim, &nodes, leader, &[TabletId(2), TabletId(3)], &want),
        "did not converge once candidates recovered (seed={seed})"
    );

    let meta = nodes[leader].metadata();
    for child in [TabletId(2), TabletId(3)] {
        assert_eq!(meta.tablets[&child].replicas, want);
        // `target` stays exactly what `CutoverSplit` wrote (`None`) —
        // diagnostic only, never updated once the live replicas converge.
        assert_eq!(meta.split_placing[&child].target, None);
        assert!(!meta.split_placing[&child].done);
    }
}

/// **Test 3**: a `done: true` entry is never touched by the phase, even
/// when its `target` still differs from the tablet's actual replicas — the
/// per-entry skip is unconditional, not merely "already converged so there
/// happens to be nothing to propose."
#[test]
fn split_placing_phase_never_touches_a_done_entry() {
    let seed = 0x5717_0003u64;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = leader_among(&nodes, &[0, 1, 2]);

    for id in [10, 11, 12, 13] {
        register(&sim, &nodes[leader], id);
    }
    sim.run_for(Duration::from_secs(1));

    split_fixture(&mut sim, &nodes, leader, &[11, 12, 13]);

    let meta = nodes[leader].metadata();
    let frozen_epoch = meta.tablets[&TabletId(2)].epoch;
    let frozen_replicas = meta.tablets[&TabletId(2)].replicas.clone();
    assert_eq!(frozen_replicas, vec![nid(11), nid(12), nid(13)]);

    // Mark child 2 done directly (test-only use of the pre-existing rung-2
    // command — its own real, automatic completion loop is ADR 0062 §3, not
    // this rung) WITHOUT its replicas ever having actually converged, so a
    // later "the phase left this alone" observation can only be explained
    // by the `done` skip itself, never by "there was nothing to do."
    assert!(matches!(
        nodes[leader].propose(MetaCommand::MarkSplitPlacingDone {
            tablet: TabletId(2),
            expected_epoch: frozen_epoch,
        }),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_millis(300));
    assert!(nodes[leader].metadata().split_placing[&TabletId(2)].done);

    // Child 3 (still un-done) converges normally in the meantime.
    let want = vec![nid(10), nid(11), nid(12)];
    assert!(
        wait_converged(&mut sim, &nodes, leader, &[TabletId(3)], &want),
        "the un-done sibling never converged (seed={seed})"
    );

    // Child 2 (done) is frozen: same replicas, same epoch, still done.
    let meta = nodes[leader].metadata();
    assert_eq!(
        meta.tablets[&TabletId(2)].replicas,
        frozen_replicas,
        "a done split_placing entry's tablet was moved"
    );
    assert_eq!(
        meta.tablets[&TabletId(2)].epoch,
        frozen_epoch,
        "a done split_placing entry's tablet epoch churned"
    );
    assert!(meta.split_placing[&TabletId(2)].done);
}

/// **Test 4a** (epoch churn, safety half): a `CasTabletReplicas` this phase
/// computed becomes stale the instant a *different*, concurrent proposal
/// (an ordinary rebalance move, or another control-plane leader after a
/// failover) commits against the same tablet at the same epoch first.
/// Applying the now-stale command afterward must fail harmlessly —
/// rejected, never a panic — leaving the tablet exactly as the concurrent
/// write left it and `split_placing` completely untouched (`CasTabletReplicas`
/// never writes it), so the next tick's always-fresh recomputation can
/// retry cleanly rather than getting stuck on a poisoned entry.
///
/// **Proven directly against `Metadata::apply`, not the live driver — and
/// deliberately so.** `reconcile_loop`'s whole per-tick body, from its
/// `PlacementView` clone through every phase's own `propose` calls, is
/// synchronous with no `.await` point in between (`node.rs`): under
/// `SimEnv`'s single-threaded cooperative executor, nothing can interleave
/// mid-tick, so the exact "observe, then get raced before proposing" window
/// this test's name describes is structurally unreachable through the
/// public `RaftNode` API within one tick. The reachable live-driver analog
/// — a concurrent write landing between cutover and the phase's first
/// chance to react, proving it does not get wedged by that churn — is
/// `split_placing_phase_converges_despite_a_concurrent_replicas_bump`
/// (below), this test's liveness-side companion.
#[test]
fn split_placing_epoch_churn_rejects_the_stale_cas_harmlessly() {
    let mut m = Metadata::default();
    for n in [1u64, 2, 3, 4] {
        assert_eq!(
            m.apply(&MetaCommand::UpsertMember {
                node: nid(n),
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            }),
            ApplyOutcome::Applied
        );
    }
    assert_eq!(
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("users".to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(2), nid(3), nid(4)],
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        m.apply(&MetaCommand::SetTabletPolicy {
            tablet: TabletId(1),
            policy: Some(PlacementPolicy::simple("users", 3)),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        m.apply(&MetaCommand::BeginSplitInPlace {
            parent: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            split_key: split_key(),
            children: [
                (TabletId(2), vec![nid(2), nid(3), nid(4)]),
                (TabletId(3), vec![nid(2), nid(3), nid(4)]),
            ],
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        m.apply(&MetaCommand::CutoverSplit {
            parent: TabletId(1),
            expected_epoch: Epoch::INITIAL.next(),
            cutover_wall_ms: 1,
        }),
        ApplyOutcome::Applied
    );

    // Child 2's directed-Placing target is [n1, n2, n3] (the same
    // 4-candidates-RF3 scenario `meta.rs`'s own rung-2 unit tests use),
    // differing from its fork-inherited [n2, n3, n4].
    let child = TabletId(2);
    let entry_before = m.split_placing[&child].clone();
    assert_eq!(entry_before.target, Some(vec![nid(1), nid(2), nid(3)]));
    let stale_epoch = m.tablets[&child].epoch;

    // What this tick's third phase would compute and propose.
    let stale_command = m
        .split_placing_reconcile()
        .into_iter()
        .find(|c| matches!(c, MetaCommand::CasTabletReplicas { tablet, .. } if *tablet == child))
        .expect("the phase should propose a move for this child");
    assert!(matches!(
        &stale_command,
        MetaCommand::CasTabletReplicas { expected_epoch, replicas, .. }
            if *expected_epoch == stale_epoch && *replicas == vec![nid(1), nid(2), nid(3)]
    ));

    // A CONCURRENT proposer (an ordinary rebalance move, or another leader
    // post-failover) commits FIRST against the same tablet, same epoch.
    let concurrent_replicas = vec![nid(2), nid(3)];
    assert_eq!(
        m.apply(&MetaCommand::CasTabletReplicas {
            tablet: child,
            expected_epoch: stale_epoch,
            replicas: concurrent_replicas.clone(),
        }),
        ApplyOutcome::Applied
    );

    // The phase's own (now-stale) command is rejected: no panic, no
    // wrong-epoch write.
    assert_eq!(
        m.apply(&stale_command),
        ApplyOutcome::Rejected("epoch mismatch")
    );
    assert_eq!(m.tablets[&child].replicas, concurrent_replicas);
    // `CasTabletReplicas` never touches `split_placing` either way.
    assert_eq!(m.split_placing[&child], entry_before);

    // Next tick: a fresh recomputation reacts to the NOW-current state
    // (not the stale one) and keeps retrying rather than getting stuck.
    let retried_command = m
        .split_placing_reconcile()
        .into_iter()
        .find(|c| matches!(c, MetaCommand::CasTabletReplicas { tablet, .. } if *tablet == child))
        .expect("the phase must keep retrying, not give up after one rejection");
    assert!(matches!(
        retried_command,
        MetaCommand::CasTabletReplicas { expected_epoch, replicas, .. }
            if expected_epoch == m.tablets[&child].epoch
                && replicas == vec![nid(1), nid(2), nid(3)]
    ));
}

/// **Test 4b** (epoch churn, liveness companion): a concurrent write to a
/// split-placing child's replicas — landing between cutover and the
/// reconcile loop's first real chance to react — never wedges convergence.
/// The phase's always-fresh recomputation just reacts to whatever state the
/// tablet is actually in on its next tick and still drives both children to
/// the same target.
#[test]
fn split_placing_phase_converges_despite_a_concurrent_replicas_bump() {
    let seed = 0x5717_0004u64;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = leader_among(&nodes, &[0, 1, 2]);

    for id in [10, 11, 12, 13] {
        register(&sim, &nodes[leader], id);
    }
    sim.run_for(Duration::from_secs(1));

    split_fixture(&mut sim, &nodes, leader, &[11, 12, 13]);

    // A concurrent write (an unrelated admin/repair CAS, say) moves child
    // 2's replicas before the reconcile loop's phase has necessarily had a
    // tick to react.
    let epoch_now = nodes[leader].metadata().tablets[&TabletId(2)].epoch;
    assert!(matches!(
        nodes[leader].propose(MetaCommand::CasTabletReplicas {
            tablet: TabletId(2),
            expected_epoch: epoch_now,
            replicas: vec![nid(11), nid(13)],
        }),
        ProposeResult::Accepted { .. }
    ));

    // Both children still converge to the identical target — the bump was
    // absorbed, not a permanent wedge.
    let want = vec![nid(10), nid(11), nid(12)];
    assert!(
        wait_converged(&mut sim, &nodes, leader, &[TabletId(2), TabletId(3)], &want),
        "did not converge despite a concurrent replicas bump (seed={seed})"
    );
}

/// **Test 5**: the directed-Placing phase is leader-gated exactly like
/// repair/rebalance (the same `if !core.lock().is_leader() { continue; }`
/// check in `reconcile_loop` — a non-leader never even attempts to
/// propose). Proven two ways: a follower directly attempting the identical
/// command the phase would compute is refused `NotLeader`, and the final
/// converged state shows exactly the single epoch hop a lone leader driving
/// convergence would produce — no redundant churn from a second actor
/// racing in.
#[test]
fn split_placing_phase_is_leader_only() {
    let seed = 0x5717_0005u64;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = leader_among(&nodes, &[0, 1, 2]);
    let follower = (0..3).find(|&i| i != leader).expect("a follower exists");
    assert!(!nodes[follower].is_leader());

    for id in [10, 11, 12, 13] {
        register(&sim, &nodes[leader], id);
    }
    sim.run_for(Duration::from_secs(1));

    split_fixture(&mut sim, &nodes, leader, &[11, 12, 13]);

    let meta = nodes[leader].metadata();
    let entry = meta.split_placing[&TabletId(2)].clone();
    let epoch = meta.tablets[&TabletId(2)].epoch;
    let want = vec![nid(10), nid(11), nid(12)];
    assert_eq!(entry.target, Some(want.clone()));

    // Even the exact command the phase would compute is refused from a
    // non-leader.
    assert!(!nodes[follower].is_leader());
    assert!(matches!(
        nodes[follower].propose(MetaCommand::CasTabletReplicas {
            tablet: TabletId(2),
            expected_epoch: epoch,
            replicas: entry.target.expect("target present"),
        }),
        ProposeResult::NotLeader { .. }
    ));

    // The real, leader-driven phase still converges normally.
    assert!(
        wait_converged(&mut sim, &nodes, leader, &[TabletId(2)], &want),
        "leader-driven convergence never happened (seed={seed})"
    );

    // Exactly one CAS hop — a second, fighting actor would have produced
    // extra epoch churn (its own rejected attempt bumps nothing, but a
    // *successful* stray proposal would have).
    let meta = nodes[leader].metadata();
    assert_eq!(
        meta.tablets[&TabletId(2)].epoch,
        epoch.next(),
        "expected exactly one CAS hop from a single leader-driven move"
    );

    // Every replica — leader and followers alike — converges to the
    // identical final state.
    for node in &nodes {
        assert_eq!(
            node.metadata().tablets[&TabletId(2)].replicas,
            want,
            "a replica diverged from the leader-driven convergence"
        );
    }
}
