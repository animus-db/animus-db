//! Issue #596 regression: `RaftKvNode::voter_history` must record the exact
//! add-before-remove sequence a two-of-three-replica-diff Placing target
//! drives `reconfigure_step` through — **from a durable in-process record**,
//! never from an external poll.
//!
//! `crates/animus-cp-data/tests/reconfigure_multi_replica_diff.rs` proved the
//! move converges and, via an external 200ms `SimEnv`-time poll of
//! `config().len()`, that it passes through the transient 5-voter
//! intermediate. That style of assertion is exactly what
//! `crates/animusd/tests/split_placing_two_replica_diff_e2e.rs` found
//! flaky under a REAL cluster: the intermediate's own duration is an
//! implementation timing artifact (how fast consecutive reconciler ticks
//! happen to remove the two extras), not a property the poll interval
//! controls — see `docs/engineering-lessons.md`'s matching entry. This file
//! reuses that same harness shape (mirrored, not imported — different
//! crate-test binaries can't share a `mod`) but asserts on
//! `RaftKvNode::voter_history()`, the mechanism issue #596 adds specifically
//! so this kind of assertion never has to race a sampling interval again.

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
/// A single focused regression, not a full fault-injection corpus — a
/// smaller fixed sweep than `reconfigure_multi_replica_diff.rs`'s 60 is
/// plenty to prove the history mechanism itself records the sequence
/// faithfully across seeds; the broader convergence property is that file's
/// job, not this one's.
const SEED_COUNT: u64 = 30;

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

fn control_leader(nodes: &[RaftNode<SimEnv>]) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one control leader, got {ls:?}");
    ls[0]
}

/// Two of a tablet's three replicas replaced in one directed CAS (mirroring
/// `Metadata::split_placing_reconcile`'s one-shot recompute) — see
/// `reconfigure_multi_replica_diff.rs`'s own doc for the full production
/// shape this mirrors. Node `2` is the one replica present in BOTH the
/// initial ({0,1,2}) and the desired ({2,3,4}) sets, so its own
/// `voter_history()` is the one record that can prove the exact
/// 3→4→5→4→3 add-before-remove sequence end to end.
#[test]
fn voter_history_records_the_exact_add_before_remove_sequence() {
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
    // three current replicas via one CAS.
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
    while elapsed < budget {
        sim.run_for(step);
        elapsed += step;
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

    // Node 2 is present in both the initial and the desired set — its own
    // `voter_history()` is the one record that saw every step of the whole
    // sequence from a fixed vantage point, start to finish.
    let history = group[&2].voter_history();
    let counts: Vec<usize> = history.iter().map(|(_, v)| v.len()).collect();
    assert!(
        counts.iter().all(|&c| c >= 3),
        "seed={seed}: voter_history dropped below the 3-voter floor at some point: {counts:?} \
         (full history: {history:?})"
    );
    assert!(
        counts.contains(&5),
        "seed={seed}: voter_history never recorded the transient over-replicated 5-voter \
         intermediate: {counts:?} (full history: {history:?})"
    );
    assert_eq!(
        counts,
        vec![3, 4, 5, 4, 3],
        "seed={seed}: voter_history did not record the exact add-before-remove sequence \
         (full history: {history:?})"
    );
    // Every recorded transition changes membership by exactly one node —
    // `reconfigure_step`'s own "one single-server step per call" discipline
    // (ADR 0031/ADR 0058 Train 1), visible end to end from this one replica's
    // vantage point.
    for pair in history.windows(2) {
        let (_, before) = &pair[0];
        let (_, after) = &pair[1];
        let added = after.difference(before).count();
        let removed = before.difference(after).count();
        assert_eq!(
            added + removed,
            1,
            "seed={seed}: a recorded voter_history transition changed more than one member \
             at once: {before:?} -> {after:?} (full history: {history:?})"
        );
    }
    // The very first entry is this replica's own genuine starting
    // configuration, never a synthesized/empty one.
    assert_eq!(
        history.first().map(|(_, v)| v.clone()),
        Some(set(&[0, 1, 2])),
        "seed={seed}: voter_history's first entry was not the group's real initial config \
         (full history: {history:?})"
    );
}
