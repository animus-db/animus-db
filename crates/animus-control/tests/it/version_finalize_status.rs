//! ADR 0073 decision 6, enforced at **apply** (issue #1168): a
//! `FinalizeClusterVersion` is rejected while any `members` row is `Down`,
//! `Leaving` or a never-activated `Joining`, whoever proposed it and
//! whatever the proposer's own pre-check saw.
//!
//! - `finalize_apply_blocks_on_each_member_status`: the pure per-status
//!   matrix against `Metadata::apply` (a recorded range containing the target
//!   does not help; `Active` and an *activated* `Joining` member do not
//!   block; a removal clears the block).
//! - `a_member_flipped_down_between_precheck_and_apply_blocks_the_finalize`:
//!   the SimEnv race the issue names. The Finalize is proposed right behind
//!   the `Down` flip (so the proposer's applied view still showed the member
//!   `Active`); the committed Finalize must still be rejected on every
//!   replica, and the identical command succeeds once the failure detector has
//!   promoted the member back (positive control, so the rejection is the
//!   status check and not something else).
//!
//! Replay one seed with `ANIMUS_SEED=<seed>`.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::raft::ProposeResult;
use animus_control::version::VersionRange;
use animus_control::{ApplyOutcome, MetaCommand, Metadata, NodeStatus};
use animus_env::nid;

use super::version_world::{World, seeds};

const BLOCKED: ApplyOutcome =
    ApplyOutcome::Rejected("blocked: a member is Down, Leaving, or a never-activated Joining");

fn upsert(n: u64, status: NodeStatus) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(n),
        labels: BTreeMap::new(),
        status,
    }
}

fn report(n: u64, min: u32, max: u32) -> MetaCommand {
    MetaCommand::ReportNodeVersion {
        node: nid(n),
        range: VersionRange::new(min, max),
        build: "t".into(),
    }
}

const FINALIZE: MetaCommand = MetaCommand::FinalizeClusterVersion {
    expected: 1,
    target: 2,
};

/// Two members, both reporting `[1,2]` (era on, version 1), the second one
/// then driven through `steps`.
fn metadata_with_second_member(steps: &[NodeStatus]) -> Metadata {
    let mut m = Metadata::default();
    for cmd in [
        upsert(1, NodeStatus::Active),
        upsert(2, NodeStatus::Active),
        report(1, 1, 2),
        report(2, 1, 2),
    ] {
        assert_eq!(m.apply(&cmd), ApplyOutcome::Applied, "{cmd:?}");
    }
    for s in steps {
        m.apply(&upsert(2, *s));
    }
    m
}

#[test]
fn finalize_apply_blocks_on_each_member_status() {
    // (status history of member 2, whether Finalize may apply)
    let cases: [(&[NodeStatus], bool); 5] = [
        (&[], true),
        (&[NodeStatus::Down], false),
        (&[NodeStatus::Leaving], false),
        // Never activated: it is registered `Joining` and never `Active`.
        (&[NodeStatus::Joining], true),
        // Activated at some point, `Joining` again afterwards.
        (&[NodeStatus::Down, NodeStatus::Active], true),
    ];
    for (steps, ok) in cases {
        let mut m = metadata_with_second_member(steps);
        let got = m.apply(&FINALIZE);
        if ok {
            assert_eq!(got, ApplyOutcome::Applied, "{steps:?}");
            assert_eq!(m.cluster_version(), 2);
        } else {
            assert_eq!(got, BLOCKED, "{steps:?}");
            assert_eq!(
                m.cluster_version(),
                1,
                "{steps:?}: a rejected finalize moved"
            );
        }
    }

    // A never-activated Joining member: member 2 is first recorded `Joining`
    // (so `has_activated` is false) and has reported a range containing the
    // target. The recorded range must not buy it past the status check.
    let mut m = Metadata::default();
    for cmd in [
        upsert(1, NodeStatus::Active),
        upsert(2, NodeStatus::Joining),
        report(1, 1, 2),
        report(2, 1, 2),
    ] {
        assert_eq!(m.apply(&cmd), ApplyOutcome::Applied, "{cmd:?}");
    }
    assert!(!m.members[&nid(2)].has_activated);
    assert_eq!(m.apply(&FINALIZE), BLOCKED);
    // Activate it: the block clears.
    m.apply(&upsert(2, NodeStatus::Active));
    assert_eq!(m.apply(&FINALIZE), ApplyOutcome::Applied);

    // Every blocking status, then a removal: the removed row stops blocking
    // (Down and Leaving rows may be removed once nothing references them).
    for blocking in [NodeStatus::Down, NodeStatus::Leaving] {
        let mut m = metadata_with_second_member(&[blocking]);
        assert_eq!(m.apply(&FINALIZE), BLOCKED, "{blocking:?}");
        assert_eq!(
            m.apply(&MetaCommand::RemoveMember { node: nid(2) }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.apply(&FINALIZE), ApplyOutcome::Applied, "{blocking:?}");
    }
}

fn run_race(seed: u64) {
    const VOTERS: [u64; 3] = [0, 1, 2];
    const MEMBERS: [u64; 2] = [10, 11];
    const V12: (u32, u32) = (1, 2);
    let mut w = World::new(seed, &VOTERS, &[], &MEMBERS);
    w.bootstrap();
    let ids: Vec<u64> = VOTERS.iter().chain(&MEMBERS).copied().collect();
    for id in &ids {
        w.flip_range(*id, V12, false);
    }
    let mut check = |_: &World| {};
    w.poll(
        Duration::from_secs(40),
        "era on with [1,2] records everywhere",
        &mut check,
        &|w| {
            w.nodes.values().all(|n| {
                let m = n.metadata();
                m.versioning_active() && m.node_versions.len() == ids.len()
            })
        },
    );

    // The race: the Down flip and the Finalize go into the log back to back
    // from the same leader, so the proposer's applied view (what the admin
    // pre-check reads) still has member 10 `Active` when the Finalize is
    // proposed.
    let leader = w.leader().expect("leader");
    assert_eq!(
        w.nodes[&leader].metadata().members[&nid(10)].status,
        NodeStatus::Active,
        "seed={seed}: precondition: the pre-check would pass"
    );
    for cmd in [upsert(10, NodeStatus::Down), FINALIZE] {
        assert!(
            matches!(
                w.nodes[&leader].propose(cmd.clone()),
                ProposeResult::Accepted { .. }
            ),
            "seed={seed}: {cmd:?} not accepted by the leader"
        );
    }
    // Long enough for both entries to commit and apply on every replica.
    w.run(Duration::from_secs(1), &mut check);
    // The Finalize entry sat directly behind the Down flip in the log, so it
    // applied while member 10 was Down: rejected, on every replica (member 10
    // may well have been promoted back since; that does not re-propose it).
    for n in w.nodes.values() {
        assert_eq!(
            n.metadata().cluster_version(),
            1,
            "seed={seed}: a Finalize applied over a Down member"
        );
    }

    // Positive control: once 10 is Active again the same command applies.
    w.poll(
        Duration::from_secs(30),
        "member 10 promoted back to Active",
        &mut check,
        &|w| {
            w.leader_meta()
                .is_some_and(|m| m.members[&nid(10)].status == NodeStatus::Active)
        },
    );
    w.propose_confirmed(&FINALIZE, &|m| m.cluster_version() == 2, "Finalize 1 -> 2");
}

#[test]
fn a_member_flipped_down_between_precheck_and_apply_blocks_the_finalize() {
    for seed in seeds("version_finalize_status", 4) {
        run_race(seed);
    }
}
