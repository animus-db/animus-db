//! ADR 0073 Phase 2 (P2-B): the control node's gate plumbing.
//!
//! - the node's `ClusterFeatures` follows the applied `Metadata` (era off at
//!   first, on once the era starts, cluster version after a finalize), on every
//!   node, and the whole era start / upkeep / finalize path (which proposes
//!   `ReportNodeVersion` while `Gate::Era` is still closed, via the named
//!   `propose_era_start` exemption) never trips a gate violation;
//! - a closed-gate proposal through `RaftNode::propose` is refused, never appended.

use std::time::Duration;

use animus_control::MetaCommand;
use animus_control::raft::ProposeResult;
use animus_control::version::{Gate, GateSurface, VersionRange};
use animus_env::nid;

use super::version_world::{World, era, seeds};

const VOTERS: [u64; 3] = [0, 1, 2];
const MEMBERS: [u64; 1] = [10];
const V12: (u32, u32) = (1, 2);

fn violations(w: &World) -> u64 {
    w.nodes
        .values()
        .flat_map(|n| {
            let f = n.features();
            GateSurface::ALL.iter().map(move |&s| f.violations(s))
        })
        .sum()
}

#[test]
fn features_follow_the_applied_metadata_and_the_era_path_trips_no_violation() {
    for seed in seeds("gate_enforcement", 3) {
        let mut w = World::new(seed, &VOTERS, &[], &MEMBERS);
        w.bootstrap();
        let mut check = |_: &World| {};
        for n in w.nodes.values() {
            assert!(n.features().is_open(Gate::Base));
            assert!(!n.features().is_open(Gate::Era), "seed={seed}: era is off");
        }
        for id in VOTERS.iter().chain(&MEMBERS) {
            w.flip_range(*id, V12, false);
        }
        w.poll(
            Duration::from_secs(40),
            "era on everywhere",
            &mut check,
            &|w| w.nodes.values().all(|n| era(&n.metadata())),
        );
        w.run(Duration::from_secs(2), &mut check);
        for (id, n) in &w.nodes {
            assert!(
                n.features().is_open(Gate::Era),
                "seed={seed}: node {id} features lag the applied era"
            );
        }
        // Finalize through the ordinary gate check (era open) 1 -> 2.
        w.propose_confirmed(
            &MetaCommand::FinalizeClusterVersion {
                expected: 1,
                target: 2,
            },
            &|m| m.cluster_version() == 2,
            "finalize",
        );
        w.run(Duration::from_secs(1), &mut check);
        for n in w.nodes.values() {
            assert_eq!(n.features().cluster_version(), 2, "seed={seed}");
        }
        assert_eq!(violations(&w), 0, "seed={seed}: gate violations counted");
    }
}

/// Pre-era, an era-only command is refused at `RaftNode::propose`. In a debug
/// build the `debug_assert!` in `ClusterFeatures::check` fails first (the point
/// of the assert), so the refusal itself is checked only without assertions.
#[test]
fn a_closed_gate_proposal_is_never_appended() {
    let seed = 7;
    let mut w = World::new(seed, &VOTERS, &[], &MEMBERS);
    w.bootstrap();
    let leader = w.leader().expect("leader");
    let cmd = MetaCommand::ReportNodeVersion {
        node: nid(10),
        range: VersionRange::new(1, 1),
        build: "t".into(),
    };
    if cfg!(debug_assertions) {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            w.nodes[&leader].propose(cmd.clone())
        }));
        assert!(r.is_err(), "a closed gate must debug_assert");
    } else {
        assert!(matches!(
            w.nodes[&leader].propose(cmd),
            ProposeResult::NotLeader { leader: None }
        ));
    }
    assert_eq!(
        w.nodes[&leader]
            .features()
            .violations(GateSurface::MetaCommand),
        1
    );
    w.sim.run_for(Duration::from_secs(2));
    assert!(
        !era(&w.nodes[&leader].metadata()),
        "refused proposal started the era"
    );
}
