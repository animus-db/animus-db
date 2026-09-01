//! Regression for the third (final) learner-starvation defect in issues
//! #532/#537: unbounded `InstallSnapshot` chunk **resend frequency**
//! (`animus-control/src/raft.rs`'s `SnapshotResend`/`SNAPSHOT_ACK_RESEND_CAP`
//! gate, ADR 0009's third 2026-09-01 amendment). `learner_catchup_under_load.rs`
//! proves the fix doesn't regress convergence; this test proves the fix's OWN
//! claim — that message VOLUME is bounded — directly, since volume is
//! otherwise invisible under `SimEnv` (free of virtual time, so a flood
//! costs nothing the convergence test's own timing assertions would notice).
//!
//! **Message volume is countable under `SimEnv` even though it's free**: this
//! is deliberately a `Metric::CpSnapshotShips` read (ADR 0015's
//! deterministic, additive metrics seam, threaded via
//! `RaftKvNode::start_with_metrics`), not a virtual-time or real-time
//! measurement — sending an `InstallSnapshot` chunk costs `SimEnv` nothing in
//! either dimension, so a test that only watched timing (like
//! `learner_catchup_under_load.rs`) could regress the flood back to
//! thousands of redundant sends per real chunk without ever going red. The
//! two tests are deliberately complementary: one proves convergence, the
//! other proves the mechanism that convergence doesn't by itself prove
//! anything about efficiency.
//!
//! **The denominator is `RaftKvNode::snapshot_chunk_advances`, not periodic
//! polling of `snapshot_offset`.** An earlier draft of this test polled the
//! leader's own view of the peer's acked offset once per write round and
//! counted distinct values seen — a plausible-looking approach that turned
//! out to badly UNDERcount: a genuine chunk advance can happen well inside a
//! single millisecond once a transfer is flowing, so any external poll
//! coarser than that silently drops most of the real denominator and
//! inflates the measured ratio regardless of how effective the fix actually
//! is (found investigating a spurious-looking red result on already-fixed
//! code). `snapshot_chunk_advances` is instead an exact lifetime count
//! `RaftCore` itself keeps, bumped the one place it can never miss a
//! transition: inside `snapshot_chunk_for`, exactly when a chunk is built
//! for an offset never attempted before (see that field's own doc) — the
//! `#[cfg(test)]`-accessor precedent this crate already uses for exposing
//! pure internal facts a test needs, applied here as a plain always-built
//! accessor since the test lives in a different crate.
//!
//! **Workload shape matters for reproducing the flood.** A tight synchronous
//! burst of proposes (`learner_catchup_under_load.rs`'s own `BURST_LEN`
//! shape) coalesces `replicate_now`'s wake-on-propose to at most one call per
//! burst — `RaftCore::propose`'s wake is a single `AtomicBool`
//! (`ProposeSignal`), not a per-propose counter, and nothing yields the
//! `SimEnv` executor between the ten synchronous puts in one burst (found
//! investigating this very fix — see the ADR amendment). To reproduce the
//! field's own per-write flood (each write genuinely getting its own
//! scheduler turn, unlike a synchronous test loop), this test issues ONE
//! propose per `Simulator::run_for` call instead — the shape that lets
//! `replicate_now` actually fire close to once per propose, which is what
//! the field diagnosis (96,451 sends / 196 offset transitions) was
//! describing.
//!
//! Red on the pre-this-fix code (temporarily reverting both
//! `SnapshotResend::Capped` gates back to an unconditional resend — see the
//! fix's own final report for the quoted counts): several hundred
//! `InstallSnapshot` sends per genuine chunk advance, matching the field's
//! own ~492-per-transition ratio in magnitude. Green with the fix: a
//! meaningfully smaller, bounded ratio — see `MAX_SENDS_PER_OFFSET`'s own
//! doc for the honest, measured target and why it isn't smaller still.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Metric, MetricsHandle, NodeId, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn leader_among(nodes: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

/// Proposes per round and the (small) virtual gap after each — one propose,
/// not a batch, is the whole point (see the module doc): it is what lets
/// `replicate_now`'s wake-on-propose actually fire close to once per write,
/// exercising the exact call site issues #532/#537 diagnosed.
const ROUNDS: u64 = 1200;
const ROUND_GAP: Duration = Duration::from_millis(1);

/// Sends-per-genuine-advance must stay under this constant. This is a
/// measured, honest target, not a round number picked in advance: at this
/// seed/workload the fixed mechanism lands around 80-90 sends per genuine
/// chunk advance (`SNAPSHOT_ACK_RESEND_CAP`'s own bounded retry budget for
/// the ack-driven cascade, `snapshot_chunk_for`'s own doc, dominates the
/// residual — wake-on-propose's own `Capped(0)` gate is responsible for
/// only a small fraction of it once measured this precisely), against
/// several HUNDRED on the unfixed mechanism at the identical seed/workload
/// (confirmed directly, matching the field's own ~492-per-transition ratio
/// in order of magnitude — see the fix's own final report for the exact
/// quoted counts). 150 gives the fixed mechanism a comfortable margin while
/// still failing outright against the unfixed one by more than 2x.
const MAX_SENDS_PER_OFFSET: u64 = 150;

#[test]
fn install_snapshot_resends_stay_bounded_per_chunk_issues_532_537() {
    let seed = 0x5324_0002;
    let mut sim = Simulator::new(seed);
    let ids = [0u64, 1, 2];
    let handles: Vec<MetricsHandle> = ids.iter().map(|_| MetricsHandle::recording()).collect();
    let nodes: Vec<KvNode> = ids
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            RaftKvNode::start_with_metrics(
                sim.env(nid(id)),
                ids.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                handles[i].clone(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l = leader_among(&nodes).expect("an initial leader");

    // Warm the log up before the learner ever joins, exactly like
    // `learner_catchup_under_load.rs`, so the leader's log is already
    // compacted past the point a fresh learner can catch up on ordinary
    // `AppendEntries` alone — forcing the chunked `InstallSnapshot` path.
    for b in 0..8u64 {
        for i in 0..10u64 {
            let key = format!("warm-{b}-{i}").into_bytes();
            assert!(
                matches!(
                    nodes[l].put(key, b"v".to_vec()),
                    ProposeResult::Accepted { .. }
                ),
                "seed={seed}: warm-up write {b}-{i} must be locally accepted"
            );
        }
        sim.run_for(Duration::from_millis(10));
    }

    // The learner's own disk carries a modest round-trip cost — the same
    // shape `learner_catchup_under_load.rs` uses to make the pathology
    // observable under `SimEnv` at all (see that test's own module doc for
    // why: `SimEnv` charges zero virtual time for network/disk by default).
    let mut learner_disk = DiskConfig::default();
    learner_disk.set_sync_delay(Duration::from_millis(10));
    sim.set_disk_config_for(nid(3), learner_disk);

    let voters: Vec<NodeId> = ids.iter().copied().map(nid).collect();
    let learner = nid(3);
    let _node3 = RaftKvNode::start(sim.env(learner.clone()), voters, MemoryEngine::new());

    assert!(
        matches!(
            nodes[l].add_learner(learner.clone()),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}: add_learner must be accepted by the leader"
    );

    // Sustained, one-propose-per-scheduler-turn writer (module doc: this
    // shape, not a batched burst, is what exercises wake-on-propose's real
    // call frequency).
    for round in 0..ROUNDS {
        let key = format!("k-{round}").into_bytes();
        let _ = nodes[l].put(key, b"v".to_vec());
        sim.run_for(ROUND_GAP);
    }

    let total_ships: u64 = handles.iter().map(|h| h.get(Metric::CpSnapshotShips)).sum();
    // `snapshot_chunk_advances` (not periodic polling of `snapshot_offset` —
    // an earlier draft of this test tried that and found it a broken
    // measurement: a genuine advance can happen well inside a single
    // millisecond, so a coarser external poll silently UNDERcounts distinct
    // offsets and inflates the very ratio this test means to bound,
    // regardless of how small `sends_per_offset` genuinely is) is the
    // EXACT lifetime count of chunks built for an offset never attempted
    // before, summed across every compaction restart the sustained writer
    // provoked — see that accessor's own doc.
    let real_advances = nodes[l].snapshot_chunk_advances(&learner);
    // At least one genuine advance must have happened — otherwise the
    // transfer never got off the ground at all in this run, which would
    // make the ratio below meaningless (divide-by-zero territory) rather
    // than a genuine pass. `learner_caught_up`'s own convergence is
    // `learner_catchup_under_load.rs`'s job, not this test's; this test
    // only needs the transfer to have made SOME progress to measure
    // send-per-offset efficiency honestly.
    assert!(
        real_advances > 0,
        "seed={seed}: the transfer to the learner made no observable progress at all \
         ({total_ships} InstallSnapshot chunks sent, 0 genuine advances) — \
         cannot assess resend efficiency"
    );
    let sends_per_offset = total_ships / real_advances;
    assert!(
        sends_per_offset <= MAX_SENDS_PER_OFFSET,
        "seed={seed}: {total_ships} InstallSnapshot sends against only \
         {real_advances} genuine chunk advances ({sends_per_offset} sends/offset, \
         bound {MAX_SENDS_PER_OFFSET}) — issues #532/#537's chunk-resend flood",
    );
}
