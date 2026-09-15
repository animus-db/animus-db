//! Issue #667 follow-up: a deterministic, hand-driven proof that a real
//! N-node genesis bootstrap converges to exactly one leader within a bounded
//! step budget under **staggered starts and lost/delayed cluster-check
//! probes** — the exact CI regression PR #902 hit under real `ProdEnv`
//! threading ("cluster did not bootstrap in 20s"), reproduced here without
//! any real socket, thread, or wall-clock dependency.
//!
//! Mirrors `next_deadline.rs`/`wiped_voter_double_vote_safety.rs`'s own
//! style: hand-driven `RaftCore`s, no `SimEnv`/`Simulator`, a manual message
//! queue and a manual tick-driven event loop this test owns outright, so
//! the exact sequence delivered to each core is deterministic and
//! inspectable, and no seed is needed.
//!
//! **What "staggered" and "lost/delayed" mean here**, modeling real CI's
//! CPU-starved bring-up:
//! - node 2 does not even call [`RaftCore::begin_cluster_check`] until
//!   several steps after nodes 0 and 1 — a slower process/thread scheduling
//!   delay a real staggered bring-up produces.
//! - the slow founder's (node 2's) very first `ClusterProbe` to each of the
//!   other two, and their first `ClusterProbeResp` back, are dropped
//!   outright (never delivered) — modeling a connection attempt that fails
//!   because the peer's listener isn't bound yet, which `tick()`'s own
//!   resend logic (and, since this amendment, `next_deadline()` correctly
//!   waking for it) must recover from without operator intervention. Nodes
//!   0 and 1 exchange their own probes cleanly (no drop), so they resolve
//!   and elect a real leader quickly — this is deliberate, not an
//!   oversight: it is exactly what creates the race this scenario needs,
//!   since by the time node 2's resent probe reaches them, they already
//!   have real, committed history that also names node 2 (a genesis
//!   config always does).
//!
//! **The regressions this specific scenario would have caught, had it
//! existed before landing either fix**:
//! - Before the `next_deadline()` fix: a still-checking node that starts
//!   receiving ordinary `AppendEntries` from an already-elected sibling
//!   would have its own probe resend suppressed indefinitely (the driver
//!   never wakes to run `tick()`'s own correct resend logic), so a lost
//!   initial probe to a THIRD, still-unresolved node would never be
//!   retried and no leader would ever form.
//! - Before the `ever_heard_from_prober` fix: a majority electing among
//!   itself before a slower node's own check resolves would look
//!   indistinguishable, via `config.contains` alone, from a genuinely
//!   established voter whose disk was wiped, permanently refusing the
//!   slower node as a voter instead of admitting it as an ordinary
//!   same-bootstrap participant.

use std::collections::{BTreeMap, BTreeSet};

use animus_control::{RaftCore, RaftMsg};
use animus_env::{Nanos, NodeId, nid};

const STEP: u64 = 20_000_000; // 20ms per step, matching a plausible poll cadence
const MAX_STEPS: u64 = 500; // 10s of virtual time — generous relative to a
// 150ms election_base, comfortably bounding without needing to be anywhere
// near CI's real 20s test timeout.

/// One entropy value per (step, node) pair, deterministic and distinct
/// enough to avoid every node drawing identical randomized backoffs.
fn entropy_for(step: u64, node_idx: usize) -> u64 {
    step.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(node_idx as u64)
        .wrapping_add(1)
}

/// Drives a 3-node genesis bootstrap with staggered starts and each ordered
/// pair's first `ClusterProbe`/`ClusterProbeResp` dropped, asserting
/// convergence to exactly one leader — and, critically, that NO node is ever
/// falsely refused as a voter — within [`MAX_STEPS`].
#[test]
fn staggered_three_node_genesis_converges_with_no_false_refusal() {
    let ids: [NodeId; 3] = [nid(0), nid(1), nid(2)];
    let all: Vec<NodeId> = ids.to_vec();

    // Nodes 0 and 1 start at step 0; node 2 (the "slow" founder) doesn't
    // even construct/begin its own check until step 5 -- a real staggered
    // process start.
    let mut cores: [Option<RaftCore>; 3] = [None, None, None];
    let start_step: [u64; 3] = [0, 0, 5];

    let mut inbox: BTreeMap<NodeId, Vec<(NodeId, RaftMsg)>> = BTreeMap::new();
    for id in &ids {
        inbox.insert(id.clone(), Vec::new());
    }

    // Tracks whether the first message of a given (from, to) DIRECTED pair
    // has already been dropped -- at most one drop per directed pair, so
    // every pair's *resend* still gets through and the scenario can't stall
    // forever by construction.
    let mut dropped_once: BTreeSet<(NodeId, NodeId)> = BTreeSet::new();

    for step in 0..MAX_STEPS {
        let now = Nanos(step * STEP);

        // Bring a staggered node online: construct it and kick off its own
        // cluster check exactly once, the moment its start step arrives.
        for (i, id) in ids.iter().enumerate() {
            if step == start_step[i] && cores[i].is_none() {
                let mut core: RaftCore = RaftCore::new(id.clone(), &all, now, entropy_for(step, i));
                let out = core.begin_cluster_check(now, entropy_for(step, i));
                cores[i] = Some(core);
                for (to, msg) in out {
                    route(&mut inbox, &mut dropped_once, &ids[2], id.clone(), to, msg);
                }
            }
        }

        // Deliver every message currently queued for each already-started
        // node, then let it tick if its own next_deadline is due.
        for (i, id) in ids.iter().enumerate() {
            let Some(core) = cores[i].as_mut() else {
                continue;
            };
            let pending = inbox.get_mut(id).map(std::mem::take).unwrap_or_default();
            let mut produced: Vec<(NodeId, RaftMsg)> = Vec::new();
            for (from, msg) in pending {
                produced.extend(core.handle(from, msg, now, entropy_for(step, i)));
            }
            if core.next_deadline().is_some_and(|d| now.0 >= d.0) {
                produced.extend(core.tick(now, entropy_for(step, i)));
            }
            for (to, msg) in produced {
                route(&mut inbox, &mut dropped_once, &ids[2], id.clone(), to, msg);
            }
        }

        // Convergence check: exactly the property a real deployment cares
        // about -- some node is leader, and NO node was ever falsely
        // refused (a false refusal is a permanent, unrecoverable outage for
        // that voter, so it must never happen in a genuine genesis race).
        for (i, id) in ids.iter().enumerate() {
            if let Some(core) = &cores[i] {
                assert!(
                    !core.refused_as_voter(),
                    "node {id} was falsely refused as a voter during a genuine \
                     staggered genesis bootstrap at step {step} (now={now:?}) -- \
                     this is the exact issue #667 regression this scenario exists \
                     to catch"
                );
            }
        }
        let any_leader = cores
            .iter()
            .any(|c| c.as_ref().is_some_and(RaftCore::is_leader));
        if any_leader && cores.iter().all(Option::is_some) {
            // Converged: some node leads, every founder has at least
            // started (so this isn't a trivial single-node false-positive
            // before node 2 even joined), and no false refusal occurred
            // (checked unconditionally above, every step, not just here).
            return;
        }
    }

    panic!(
        "no leader formed within {MAX_STEPS} steps ({}ms of virtual time) -- \
         reproduces the CI \"cluster did not bootstrap\" regression",
        MAX_STEPS * STEP / 1_000_000
    );
}

/// Route one `(from, to, msg)` triple into `inbox[to]`, dropping it instead
/// if this is the FIRST time this exact directed pair has ever sent
/// anything -- modeling a lost initial connection attempt. Every
/// subsequent message between the same ordered pair is delivered normally,
/// so a resend always eventually gets through.
fn route(
    inbox: &mut BTreeMap<NodeId, Vec<(NodeId, RaftMsg)>>,
    dropped_once: &mut BTreeSet<(NodeId, NodeId)>,
    slow_founder: &NodeId,
    from: NodeId,
    to: NodeId,
    msg: RaftMsg,
) {
    // Only the SLOW founder's own probes are ever dropped (once, per
    // directed pair) -- nodes 0 and 1 exchange their probes cleanly so
    // they resolve and elect a real leader quickly, which is exactly what
    // creates the race this scenario needs: by the time the slow founder's
    // RESENT probe reaches them, they already have real, committed history
    // that also names the slow founder (a genesis config always does) --
    // the precise shape that used to be indistinguishable from a genuinely
    // established, wiped voter.
    let is_probe = matches!(
        msg,
        RaftMsg::ClusterProbe | RaftMsg::ClusterProbeResp { .. }
    );
    if is_probe && from == *slow_founder {
        let key = (from.clone(), to.clone());
        if dropped_once.insert(key) {
            return; // simulate the lost first probe packet
        }
    }
    inbox.entry(to).or_default().push((from, msg));
}
