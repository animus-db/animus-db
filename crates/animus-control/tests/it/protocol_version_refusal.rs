//! ADR 0073 Phase 0, workstream D — `SimEnv`'s per-node network-protocol
//! handshake model, end to end over a real 3-node control-plane `RaftCore`
//! cluster (mirrors `control_raft.rs`'s harness idioms: `Simulator::run_for`,
//! never `run()`, since Raft's heartbeats are perpetual).
//!
//! Scenario: a 3-node control group where node 2's declared network-protocol
//! version is bumped by one (the other two stay at this build's default).
//! Every message to or from node 2 is refused at delivery time
//! (`SimEnv::fire_event`'s `Event::Deliver` arm, via `animus_env::handshake
//! ::check_peer`) — the same check `ProdEnv`'s real per-connection handshake
//! performs, just at message granularity since `SimEnv` has no real
//! connections. This asserts:
//!
//! (a) the v1 majority (nodes 0 and 1) still elects a leader and commits
//!     proposals — quorum-of-3 is still reachable from two mutually-
//!     reachable voters;
//! (b) the odd node (2) never receives or applies anything the majority
//!     commits, and never becomes leader itself;
//! (c) the refusal is observable: `Simulator::protocol_refusals` is nonzero
//!     for node 2, and a trace `Drop` with the named `"protocol-refused"`
//!     reason exists;
//! (d) resetting the odd node back to the default protocol lets it converge
//!     to the majority's committed state (converged-or-timeout poll, never a
//!     fixed-deadline one-shot) — proof the refusal, not something else, was
//!     the only thing holding it back.
//!
//! Every run is a pure function of its seed; `ANIMUS_SEED=<decimal>` (no `0x`
//! prefix, matching every other seeded test in this crate) replays a single
//! one.

use std::time::Duration;

use animus_control::raft::ProposeResult;
use animus_control::{MetaCommand, NodeStatus, RaftNode};
use animus_env::handshake::{NETWORK_PROTOCOL, ProtocolSpec};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const CONTROL: [u64; 3] = [0, 1, 2];
/// The odd node whose protocol version is mismatched for most of each run.
const ODD: u64 = 2;

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    // Set the mismatch before any node exchanges a single message: not load-
    // bearing for correctness (the check is re-evaluated on every delivery,
    // so setting it later would refuse just as surely), but it keeps this
    // scenario's very first messages already subject to the refusal, rather
    // than the odd node briefly and confusingly participating pre-mismatch.
    sim.set_network_protocol_for(
        nid(ODD),
        ProtocolSpec {
            name: NETWORK_PROTOCOL.name,
            magic: NETWORK_PROTOCOL.magic,
            version: NETWORK_PROTOCOL.version + 1,
        },
    );
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

/// Index of the unique leader among `live` nodes, asserting there is exactly
/// one (panics with the seed on divergence, per repo convention).
fn unique_leader(nodes: &[RaftNode<SimEnv>], live: &[usize], seed: u64) -> usize {
    let leaders: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "expected exactly one leader among {live:?}, found {leaders:?} (seed={seed})"
    );
    leaders[0]
}

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: [("region".to_string(), "eu-west".to_string())]
            .into_iter()
            .collect(),
        status: NodeStatus::Active,
    }
}

#[test]
fn odd_protocol_version_is_refused_but_the_majority_still_works() {
    let seeds: Vec<u64> = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(|s| vec![s])
        .unwrap_or_else(|| vec![0x0073_0001, 0x0073_0002, 0x0073_0003, 0x0073_0004]);
    for seed in seeds {
        run(seed);
    }
}

fn run(seed: u64) {
    let (mut sim, nodes) = cluster(seed);

    // (a) The v1 majority (nodes 0 and 1) elects a leader and commits a
    // proposal, entirely without node 2's participation.
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, &[0, 1], seed);
    assert!(
        matches!(
            nodes[leader].propose(upsert(10)),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}: the v1 majority must still commit proposals"
    );
    sim.run_for(Duration::from_secs(1));
    let committed = nodes[leader].metadata();
    assert!(
        committed.members.contains_key(&nid(10)),
        "seed={seed}: the committed member must be visible on the leader"
    );
    for &live in &[0usize, 1] {
        assert!(
            nodes[live].metadata().members.contains_key(&nid(10)),
            "seed={seed}: node {live} (majority) never saw the committed member"
        );
    }

    // (b) The odd node never receives or applies anything, and never
    // becomes leader.
    let odd = ODD as usize;
    assert!(
        !nodes[odd].is_leader(),
        "seed={seed}: the odd node must never win an election it can't be heard in"
    );
    assert!(
        !nodes[odd].metadata().members.contains_key(&nid(10)),
        "seed={seed}: the odd node must never see a commit it was refused"
    );

    // (c) The refusal is observable: a nonzero counter, and a named trace
    // reason.
    assert!(
        sim.protocol_refusals(&nid(ODD)) > 0,
        "seed={seed}: the odd node's refusal count must be nonzero"
    );
    assert!(
        sim.trace_lines()
            .iter()
            .any(|l| l.contains("protocol-refused")),
        "seed={seed}: a protocol-refused drop must appear in the trace"
    );

    // (d) Resetting the odd node back to the default protocol lets it
    // converge to the majority's committed state — proof the mismatch, and
    // nothing else, was holding it back. Converged-or-timeout poll, never a
    // fixed-deadline one-shot.
    sim.set_network_protocol_for(nid(ODD), NETWORK_PROTOCOL);
    let mut converged = false;
    for _ in 0..50 {
        sim.run_for(Duration::from_millis(100));
        if nodes[odd].metadata().members.contains_key(&nid(10)) {
            converged = true;
            break;
        }
    }
    assert!(
        converged,
        "seed={seed}: the odd node never converged after its protocol was reset to default"
    );
}
