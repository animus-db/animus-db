//! Issue #513 investigation regression: `RaftKvNode::reconfigure_step`
//! (ADR 0058 Train 1) driven toward a desired replica set that differs from
//! the group's CURRENT voter set by **two** of three replicas (the shape
//! reported to make live Raft membership "oscillate indefinitely" under a
//! real cluster — filed during ADR 0062 rung 6, see
//! `docs/engineering-lessons.md` and ADR 0062's amendment).
//!
//! **This corpus does NOT reproduce that oscillation.** Re-investigating
//! #513 (both here under `SimEnv`, across many harness shapes, and
//! end-to-end under a real multi-threaded `ProdEnv` cluster —
//! `crates/animusd/tests/split_placing_two_replica_diff_e2e.rs`, which
//! mirrors the ORIGINAL rung-6 reproduction recipe exactly: grow a 3-node
//! cluster by two lower-sorting-id nodes, so a fresh `select_replicas`
//! prefers both new nodes over two of the parent's three, then drive a real
//! in-place split's directed-Placing target through it) could not
//! substantiate a genuine defect in `reconfigure_step`'s own step-selection
//! logic or in its production caller (`host::Reconciler`'s `HostAction::
//! Reconfigure`, one call per node per tick, leader-gated). Every run —
//! dozens of `SimEnv` seeds across five harness fidelities (direct
//! `reconfigure_step` polling, `spawn_reconfigure_loop` with a shared
//! target, `spawn_reconfigure_loop` with each group member independently
//! polling its OWN control-plane replica, the real `host::Reconciler`
//! driven uniformly, and 25+ real `ProdEnv` end-to-end runs including
//! several where leadership genuinely transfers mid-sequence via
//! `reconfigure_step`'s own step 6 self-removal case) — converges cleanly
//! and monotonically: 3 voters -> learner add -> 4 voters -> learner add ->
//! the genuinely over-replicated 5-voter intermediate the issue names ->
//! remove -> 4 -> remove -> 3, settling on the desired set with no reversion
//! observed. See the engineering-lessons.md entry filed alongside this test
//! for the likely explanation (a convergence check that includes an
//! ALREADY-REMOVED replica's own frozen `config()`, which stops updating
//! the instant that replica is excluded from the group and can look like a
//! "revert" if compared naively against a live, still-converging replica —
//! the exact mistake this investigation made repeatedly while building its
//! own repro harnesses before catching it).
//!
//! This file exists as a permanent regression: if #513's oscillation is
//! ever genuinely reintroduced (or was real and merely didn't trigger under
//! these conditions), this corpus is the first thing that should catch it.
//! `ANIMUS_RECONFIGURE_DIFF_SEEDS` (default the seed range below, 60 seeds)
//! scales depth the same way this repo's other corpora do.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_control::node::heartbeat_loop;
use animus_control::{MetaCommand, NodeStatus, ProposeResult, RaftNode};
use animus_cp_data::RaftKvNode;
use animus_env::{Clock, EnvExt, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId};

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const CONTROL: [u64; 3] = [100, 101, 102];
const TABLET: TabletId = TabletId(1);
const RECONFIGURE_INTERVAL: Duration = Duration::from_millis(200);
/// How many seeds this corpus sweeps — a fixed range (unlike the
/// `ANIMUS_*_SEEDS`-scaled corpora elsewhere in this repo), since this file
/// is a single focused regression rather than a full fault-injection corpus
/// with a frozen scenario list.
const SEED_COUNT: u64 = 60;

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

fn control_leader(nodes: &[RaftNode<SimEnv>]) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one control leader, got {ls:?}");
    ls[0]
}

/// Two of a tablet's three replicas replaced in one directed CAS (mirroring
/// `Metadata::split_placing_reconcile`'s one-shot recompute), driven by the
/// REAL production caller shape: every group member — including the two
/// about-to-be-removed originals and the two newcomers — runs its own
/// `spawn_reconfigure_loop`, polling `desired`/`down` from its OWN control
/// plane replica (a DIFFERENT one per group member, so each has a genuinely
/// independent, real-Raft-replicated view — the closest `SimEnv` gets to
/// separate `animusd` processes each mirroring `Metadata` on their own
/// schedule).
#[test]
fn reconfigure_step_converges_on_a_two_of_three_replica_diff() {
    for seed in 0..SEED_COUNT {
        run(seed);
    }
}

fn run(seed: u64) {
    let mut sim = Simulator::new(seed);

    let control: Vec<RaftNode<SimEnv>> = CONTROL
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                CONTROL.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();

    let data_ids: [u64; 5] = [0, 1, 2, 3, 4];
    for &id in &data_ids {
        let env = sim.env(nid(id));
        env.spawn_task(heartbeat_loop(
            env.clone(),
            CONTROL.iter().copied().map(nid).collect(),
        ));
    }

    // Data group: initial voters {0,1,2}; 3,4 pre-started as quiet
    // non-voters (the "pre-start a to-be-added node knowing only the
    // CURRENT voters" gotcha — `animus-cp-data/CLAUDE.md`).
    let initial: [u64; 3] = [0, 1, 2];
    let mut group: BTreeMap<u64, KvNode> = BTreeMap::new();
    for &id in &initial {
        group.insert(
            id,
            RaftKvNode::start(
                sim.env(nid(id)),
                initial.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            ),
        );
    }
    for &id in &[3u64, 4] {
        group.insert(
            id,
            RaftKvNode::start(
                sim.env(nid(id)),
                initial.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            ),
        );
    }

    // Each group node polls a DIFFERENT control replica (id % 3) — genuinely
    // independent local mirrors of `Metadata`, exactly like production's
    // per-node tablet-host reconciler each reading its own control-plane
    // handle.
    for (i, &id) in data_ids.iter().enumerate() {
        let ctrl = control[i % 3].clone();
        let ctrl_down = control[i % 3].clone();
        group[&id].spawn_reconfigure_loop(
            RECONFIGURE_INTERVAL,
            move || {
                ctrl.metadata()
                    .tablets
                    .get(&TABLET)
                    .map(|t| t.replicas.iter().cloned().collect())
            },
            move || {
                ctrl_down
                    .metadata()
                    .members
                    .iter()
                    .filter(|(_, m)| m.status == NodeStatus::Down)
                    .map(|(id, _)| id.clone())
                    .collect()
            },
        );
    }

    sim.run_for(Duration::from_secs(2));
    let cl = control_leader(&control);

    for &id in &data_ids {
        assert!(matches!(
            control[cl].propose(MetaCommand::UpsertMember {
                node: nid(id),
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            }),
            ProposeResult::Accepted { .. }
        ));
    }
    assert!(matches!(
        control[cl].propose(MetaCommand::CreateTablet {
            tablet: TABLET,
            table: None,
            range: KeyRange::whole(),
            replicas: initial.iter().copied().map(nid).collect(),
        }),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    // Directed placing: jump straight to a target replacing TWO of the
    // three current replicas via one CAS — the exact shape
    // `Metadata::split_placing_reconcile`'s one-shot fresh recompute
    // produces (and what a single-replica-diff move never exercises).
    let desired = set(&[2, 3, 4]);
    let epoch = control[cl].metadata().tablets[&TABLET].epoch;
    assert!(matches!(
        control[cl].propose(MetaCommand::CasTabletReplicas {
            tablet: TABLET,
            expected_epoch: epoch,
            replicas: desired.iter().cloned().collect(),
        }),
        ProposeResult::Accepted { .. }
    ));

    // A paced continuous writer so catch-up/commit_index are genuine moving
    // targets throughout convergence, not a quiescent group.
    {
        let writers: Vec<KvNode> = data_ids.iter().map(|id| group[id].clone()).collect();
        let env = sim.env(nid(0));
        env.clone().spawn_task(async move {
            let mut i: u64 = 0;
            loop {
                env.sleep(Duration::from_millis(20)).await;
                if let Some(w) = writers.iter().find(|n| n.is_leader()) {
                    let _ = w.put(format!("k{i}").into_bytes(), vec![i as u8; 32]);
                    i += 1;
                }
            }
        });
    }

    let budget = Duration::from_secs(60);
    let step = Duration::from_millis(200);
    let mut elapsed = Duration::ZERO;
    let mut converged = false;
    let mut max_voters_seen = 0usize;
    while elapsed < budget {
        sim.run_for(step);
        elapsed += step;
        // Only the members of `desired` can ever reflect FULL convergence —
        // a removed voter stops receiving `AppendEntries` the instant it's
        // excluded, so its own `config()` freezes at whatever it last
        // observed. Checking it here would be exactly the false-oscillation
        // trap this file's own module doc names.
        max_voters_seen = max_voters_seen.max(group[&2].config().len());
        if [2u64, 3, 4]
            .iter()
            .all(|id| group[id].config() == desired && group[id].learners().is_empty())
        {
            converged = true;
            break;
        }
    }
    assert!(
        converged,
        "seed={seed}: two-of-three replica diff never converged to {desired:?} within {budget:?} \
         (final configs: n2={:?} n3={:?} n4={:?})",
        group[&2].config(),
        group[&3].config(),
        group[&4].config(),
    );
    // The move really did pass through the over-replicated intermediate the
    // issue names — otherwise this test would just be proving the
    // single-diff case again under a different harness.
    assert!(
        max_voters_seen >= 5,
        "seed={seed}: expected to observe the transient 5-voter intermediate state \
         (add-both-learners-before-removing-either), only ever saw up to {max_voters_seen}"
    );
}
