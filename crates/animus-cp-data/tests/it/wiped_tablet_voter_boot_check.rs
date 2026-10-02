//! Regression for issue #900 (P0 Raft safety): the identical wiped-voter
//! double-vote hazard issue #667 closed for the control plane
//! (`animus-control::node::drive`) also existed, unfixed, in the CP data
//! plane's own tablet-group driver (`animus-cp-data::drive`) — a voter whose
//! whole disk is wiped (ephemeral storage, or a rescheduled pod on an
//! `EmptyDir` volume) and restarted into an already-established tablet
//! group used to come back as an ordinary, fully-eligible `RaftCore::new()`
//! follower with **no** gating at all: eligible to vote/campaign
//! immediately, with no memory of any vote it durably granted before the
//! wipe. `RaftKvNode` reuses `animus-control`'s generic, sync
//! `RaftCore<C, S>` unchanged (ADR 0016/0017), so the disambiguation
//! mechanism itself (`RaftCore::begin_cluster_check`/
//! `handle_cluster_probe`/`handle_cluster_probe_resp`, the
//! `ClusterProbe`/`ClusterProbeResp` wire messages, and the vote/campaign
//! gating) is inherited for free — proven safe generically by
//! `animus-control`'s own `wiped_voter_double_vote_safety.rs`. The bug this
//! file regresses is narrower and purely cp-data's own: **the driver never
//! called `begin_cluster_check` in the first place**, so the inherited
//! mechanism never engaged for a tablet group no matter how the peers
//! answered.
//!
//! This is therefore a `SimEnv`/`RaftKvNode`-driven, end-to-end regression
//! of the actual boot wiring (not a hand-crafted `RaftCore`-level poke of
//! the kind `wiped_voter_double_vote_safety.rs` uses for the control
//! plane) — going through the real driver is the only way to prove *this*
//! bug, since the generic core-level mechanism was never in question.
//!
//! # How the red-then-green proof was obtained
//!
//! Pre-fix, `RaftKvNode` had no `cluster_check_pending`/`refused_as_voter`
//! accessors at all (this file's own new API, added by the fix) — a literal
//! `git checkout main -- lib.rs` alongside this file would not compile,
//! exactly the reason `wiped_voter_double_vote_safety.rs`'s own doc gives
//! for why that isn't how a *gate* gets proven red: it only proves API
//! surface. Instead, the gate itself was disabled in place, mirroring that
//! file's methodology exactly: `drive`'s new `else if !campaign_immediately`
//! arm was changed to `else if false && !campaign_immediately` (never
//! calling `begin_cluster_check`, exactly today's pre-fix behavior) and this
//! exact test was re-run:
//!
//! ```text
//! thread 'a_wiped_tablet_voter_is_refused_while_the_rest_of_the_group_keeps_serving' panicked at
//! crates/animus-cp-data/tests/wiped_tablet_voter_boot_check.rs:154:5:
//! seed=90020260915: the wiped voter must engage the issue #900/#667 boot-time
//! cluster check immediately on restart (cluster_check_pending() was never
//! observed true) -- the exact hazard this test exists to catch
//! ```
//!
//! Restoring the real `else if !campaign_immediately { begin_cluster_check
//! ... }` arm turns it green. No seed search was needed; the fixed seed
//! below reproduces both the pass and (with the gate disabled) the failure
//! deterministically on every run.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// The unique leader among `live`, panicking with the seed if there isn't
/// exactly one — mirrors `restart_caught_up_voter.rs`'s own `leader_among`,
/// generalized to a caller-supplied live set.
fn unique_leader(nodes: &[KvNode], live: &[usize], seed: u64, when: &str) -> usize {
    let leaders: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "seed={seed}: expected exactly one leader among {live:?} {when}, found {leaders:?}"
    );
    leaders[0]
}

/// The core regression: a plain 3-replica tablet group, no growth/split
/// involved — one voter's disk (and engine — a real `storage.ephemeral:
/// true` wipe loses both, `wiped_voter_rejoin.rs`'s own precedent) is wiped
/// and restarted fresh on the same node id and the same static 3-node
/// bootstrap config. Expect:
///
/// 1. The restarted replica's own issue #900/#667 boot-time cluster check
///    engages **immediately** on restart (`cluster_check_pending()`), and
///    resolves to a permanent refusal (`refused_as_voter()`) once its two
///    peers answer — both have real committed history and neither is
///    itself a genuinely fresh peer, and the original leader (whichever
///    node that turned out to be) genuinely received this identity's real
///    granted vote before the wipe, so it recognizes the restart as
///    established.
/// 2. The refused replica never becomes leader.
/// 3. The rest of the group is completely unaffected: the untouched pair
///    (the original leader plus the other original follower) already forms
///    a majority of the group's static 3-voter config on its own, so
///    service never so much as blips — a write proposed on the original
///    leader before the wipe, and another proposed after, both commit.
#[test]
fn a_wiped_tablet_voter_is_refused_while_the_rest_of_the_group_keeps_serving() {
    let seed: u64 = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(90020260915u64); // issue 900, fixed 2026-09-15

    let sim = Simulator::new(seed);
    let mut nodes: Vec<KvNode> = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    let mut sim = sim;
    sim.run_for(Duration::from_secs(2)); // elect

    let leader = unique_leader(&nodes, &[0, 1, 2], seed, "before the wipe");
    assert!(
        matches!(
            nodes[leader].put(b"k0".to_vec(), b"v0".to_vec()),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}: a write should commit before the wipe"
    );
    sim.run_for(Duration::from_secs(1)); // let it commit + apply, real history for every replica

    // The victim: any voter that is not the current leader.
    let victim = (0..3usize)
        .find(|&i| i != leader)
        .expect("a non-leader voter exists in a 3-node group");
    assert_ne!(
        victim, leader,
        "seed={seed}: victim must not be the current leader"
    );

    // The StatefulSet-roll shape `wiped_voter_rejoin.rs` models for the
    // control plane: delete the process, wipe the ephemeral volume (WAL
    // *and* engine both gone — a fresh `MemoryEngine`, not the old one),
    // recreate it fresh on the SAME node id with the SAME static bootstrap
    // config.
    sim.stop(nid(victim as u64));
    sim.wipe_disk(nid(victim as u64));
    nodes[victim] = RaftKvNode::start(
        sim.env(nid(victim as u64)),
        NODES.iter().copied().map(nid).collect(),
        MemoryEngine::new(),
    );

    // Let the freshly-spawned driver task get its first poll (`start`
    // schedules `drive` as a task; nothing guarantees it has run yet the
    // instant `start` returns) — a tiny step, well under the default
    // `NetConfig` base delay, so no peer has had a real chance to answer.
    sim.run_for(Duration::from_millis(1));

    // Assertion 1: the boot-time check must engage essentially IMMEDIATELY
    // on restart, before any peer has had a chance to answer — this is the
    // exact fact issue #900 says the driver never establishes today.
    assert!(
        nodes[victim].cluster_check_pending(),
        "seed={seed}: the wiped voter must engage the issue #900/#667 boot-time cluster \
         check immediately on restart (cluster_check_pending() was never observed true) -- \
         the exact hazard this test exists to catch"
    );
    // And, per the mechanism's own contract, a still-checking replica is
    // never (falsely) reported as leading.
    assert!(
        !nodes[victim].is_leader(),
        "seed={seed}: a replica still resolving its own boot-time cluster check must never \
         report itself as leader"
    );

    // Give the probe/response round trip room to complete: both surviving
    // peers must answer before a refusal verdict is reached (the
    // wait-for-every-peer discipline, ADR 0009's first 2026-09-15
    // amendment) -- comfortably more than the couple of milliseconds the
    // default `NetConfig` needs for two single-hop round trips.
    sim.run_for(Duration::from_secs(2));

    // Assertion 2: resolved, and refused -- both surviving peers have real
    // committed history (the original election + the write above), neither
    // is itself genuinely fresh, and the node that was leader when the
    // victim cast its real vote genuinely received that grant
    // (`ever_heard_from_prober`), so the aggregate verdict is
    // "established, wiped, refuse."
    assert!(
        !nodes[victim].cluster_check_pending(),
        "seed={seed}: the boot-time cluster check should have resolved by now"
    );
    assert!(
        nodes[victim].refused_as_voter(),
        "seed={seed}: a voter that was genuinely an established member of this tablet's \
         Raft group, restarted from a wiped disk, must be refused -- exactly the hazard \
         issue #900 exists to close"
    );

    // Assertion 3: the rest of the group is completely unaffected -- the
    // ORIGINAL leader plus the ORIGINAL other follower already form a
    // majority of the group's static 3-voter config on their own, so
    // service never blips, before OR after the wipe.
    let survivor = (0..3usize)
        .find(|&i| i != leader && i != victim)
        .expect("a third replica exists in a 3-node group");
    assert!(
        nodes[leader].is_leader(),
        "seed={seed}: the original leader should still be leading -- it never needed the \
         (now-refused) victim's vote to hold its majority"
    );
    assert!(
        matches!(
            nodes[leader].put(b"k1".to_vec(), b"v1".to_vec()),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}: a write must still commit after the victim's refusal"
    );
    sim.run_for(Duration::from_secs(1));
    assert!(
        nodes[survivor].commit_index() >= 2,
        "seed={seed}: the untouched survivor should have caught up to both commits"
    );

    // Defense in depth, mirroring `wiped_voter_double_vote_safety.rs`'s own
    // "not even a re-ask from the original candidate grants" check: even
    // after the original leader eventually goes away, the refused voter
    // must never become part of a new majority. Crash the leader; with the
    // victim permanently refused, only the lone survivor remains able to
    // vote at all, so the group can never re-elect -- the conservative,
    // safety-over-liveness outcome this mechanism deliberately accepts
    // (operational recovery is the documented learner/rejoin path, ADR
    // 0032/0058, never a bare restart back into the voter set).
    sim.crash(nid(leader as u64));
    sim.run_for(Duration::from_secs(10));
    let leaders_after: Vec<usize> = [victim, survivor]
        .into_iter()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert!(
        leaders_after.is_empty(),
        "seed={seed}: with the original leader gone and the wiped voter permanently refused, \
         the lone remaining voter cannot reach a majority and must not elect a new leader -- \
         found leader(s) {leaders_after:?} (the refused voter must have granted a vote it \
         never should have)"
    );
    assert!(
        !nodes[victim].is_leader(),
        "seed={seed}: the refused voter itself must never become leader"
    );
}
