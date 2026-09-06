//! ADR 0044 phase 2 (C-02 PR 1) — a baseline, PRE-BATCHER measurement of the
//! per-node heartbeat send cost `docs/design/heartbeat-send-sites.md` maps.
//! No production code changes ride with this file; PR 3 of the C-02 stack
//! flips this test's own assertion (see the doc comment on the one test
//! below) once a per-node-pair batcher lands.
//!
//! **Why this hosts G *independent* `Simulator` worlds instead of G groups
//! co-hosted in one `Simulator`.** The realistic production shape — several
//! tablet groups sharing one physical node's env, each on its own
//! `(node, stream = tablet_id)` address (ADR 0026) — is `RaftKvNode::
//! start_hosted` (`crates/animus-cp-data/src/lib.rs`, see
//! `tests/stream_addressing.rs` for the exact pattern). But `start_hosted`
//! records into `env.metrics()`, which is `MetricsHandle::noop()` under
//! `SimEnv` by design (`animus_env::Env::metrics`'s own doc: an env that
//! doesn't record metrics returns the shared no-op handle) — there is no
//! production constructor that both takes an explicit `stream` (for
//! co-hosting) AND an injectable `MetricsHandle` (for `SimEnv`
//! observability). Adding one would be exactly the kind of small,
//! additive-only production-code change `RaftKvNode::start_with_metrics`
//! itself already is (see that constructor's own doc: "a sim test threads a
//! recording handle in here to read counters back without editing
//! `animus-sim`") — but this PR's scope is documentation plus, at most, a
//! measurement test, with **no** production behaviour change, so that
//! constructor is left for PR 2 to add if it turns out to be needed there
//! too (see the map's own §7 note on this).
//!
//! Instead: every group in this file is hosted via `start_with_metrics`
//! (which forces `PRIMARY_STREAM`, so it must live in its own `Simulator`
//! world to avoid stream collision with any other group on the same node
//! ids), but every group — across every `Simulator` — records into the
//! SAME three `MetricsHandle`s, index-aligned by node id 0/1/2.
//! `MetricsHandle` is a plain shared counter sink (`Clone`, backed by
//! atomics — see `animus_env::metrics`), so summing across it is exactly
//! what a real node's own single `env.metrics()` sink would show if it
//! hosted every one of these groups itself: this models "one physical
//! node's shared metrics aggregate every hosted group's own traffic" with
//! zero `src/` changes, using only pre-existing test infrastructure
//! (`start_with_metrics`, `MetricsHandle::recording`, `Simulator`) in the
//! same combination `tests/quiescence.rs` already established for a single
//! group.

use std::time::Duration;

use animus_cp_data::RaftKvNode;
use animus_env::{Metric, MetricsHandle, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

/// `animus_control::raft::RaftCore`'s own default heartbeat cadence
/// (`crates/animus-control/src/raft.rs:854`), reused by every
/// `animus-cp-data::RaftKvNode` group (ADR 0016 — the sync core is shared
/// unchanged between the control plane and the per-tablet data plane).
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

/// Elect a leader and replicate its own no-op — comfortably past the
/// randomized election timeout (`election_base` = 150ms,
/// `crates/animus-control/src/raft.rs:853`) with margin.
const SETTLE: Duration = Duration::from_secs(1);

/// Many multiples of [`HEARTBEAT_INTERVAL`] (~100 ticks) — long enough that
/// each group's own election-time `become_leader` broadcast is a small
/// fraction of the total, so the measured ratio between a 1-group and a
/// 5-group run tracks the steady-state heartbeat rate, not startup noise.
const IDLE_WINDOW: Duration = Duration::from_secs(5);

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// Stand up one independent 3-node group in its own `Simulator` world,
/// recording into the caller's per-node-id `handles` (shared across every
/// group this test's own `run_groups` stands up, so their traffic
/// aggregates exactly as if co-hosted on the same three physical nodes).
/// Quiescence (ADR 0048) is deliberately never enabled here — this
/// baseline is about the ACTIVE, always-ticking cost C-02 exists to
/// amortize (quiescence already zeroes the idle case; see the map's own
/// §4 "what remains" paragraph), so every group in this file keeps
/// ticking its Raft heartbeat for the whole run, exactly like a real
/// actively-written-to tablet that never goes idle long enough to
/// quiesce.
fn spawn_group(seed: u64, handles: &[MetricsHandle; 3]) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes: Vec<KvNode> = NODES
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            RaftKvNode::start_with_metrics(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                handles[i].clone(),
            )
        })
        .collect();
    (sim, nodes)
}

fn total_append_entries_sent(handles: &[MetricsHandle; 3]) -> u64 {
    handles
        .iter()
        .map(|h| h.get(Metric::CpAppendEntriesSent))
        .sum()
}

/// Run `count` independent groups, each for `SETTLE + IDLE_WINDOW` of its
/// own virtual time, and return the combined `Metric::CpAppendEntriesSent`
/// total across every group's replicas — the per-node-aggregate metric a
/// real node's own shared `env.metrics()` sink would show (see this file's
/// own module doc for why `count` independent `Simulator` worlds stand in
/// for `count` groups co-hosted on one node's env).
fn run_groups(count: u64, seed_base: u64) -> u64 {
    let handles: [MetricsHandle; 3] = std::array::from_fn(|_| MetricsHandle::recording());
    let mut groups: Vec<(Simulator, Vec<KvNode>)> = Vec::new();
    for g in 0..count {
        let (mut sim, nodes) = spawn_group(seed_base.wrapping_add(g), &handles);
        sim.run_for(SETTLE);
        groups.push((sim, nodes));
    }
    for (sim, _nodes) in groups.iter_mut() {
        sim.run_for(IDLE_WINDOW);
    }
    total_append_entries_sent(&handles)
}

/// **Baseline, pre-batcher measurement (C-02 PR 1).** `Metric::
/// CpAppendEntriesSent`, summed per node across every hosted group's
/// replicas, scales with the number of actively-ticking hosted groups —
/// the exact per-group cost ADR 0044 phase 2's batcher exists to remove
/// (see `docs/design/heartbeat-send-sites.md` §4/§7). 5 co-located groups
/// send roughly 5x the `AppendEntries` traffic 1 group does, for the
/// SAME 3 physical node ids and the SAME `--cluster 3`/RF-3 node-pair
/// count — proportional to `G × P`, not flat.
///
/// **PR 3 of the C-02 stack flips this assertion** once a per-node-pair
/// heartbeat batcher lands behind a flag: the same workload's *physical
/// frame count* should go flat as `G` grows (a new counter PR 2 adds, per
/// the map's own §5 open questions), while this metric — the *logical*
/// per-group heartbeat count — should keep scaling with `G` exactly as it
/// does today, proving the batcher preserves every group's own semantics
/// (§3 of the map) while amortizing only the wire framing.
#[test]
fn append_entries_sent_scales_with_hosted_group_count_not_node_pairs() {
    let low = run_groups(1, 0xC02_0001);
    let high = run_groups(5, 0xC02_0002);

    // Generous bounds absorb election-settle jitter and independent-
    // `Simulator`-world timing drift (each group's own heartbeat_deadline
    // is independently phased — see the map's §1b) — the point is "scales
    // with G," not an exact multiple.
    let ratio = high as f64 / low as f64;
    assert!(
        (4.0..=6.0).contains(&ratio),
        "expected ~5x AppendEntries traffic for 5x the actively-hosted \
         groups (today's un-amortized per-group heartbeat cost): \
         low={low} high={high} ratio={ratio:.2} \
         (heartbeat_interval={HEARTBEAT_INTERVAL:?})"
    );
    assert!(
        low > 0,
        "sanity: a single active group must have sent some heartbeats \
         over a {IDLE_WINDOW:?} idle window"
    );
}
