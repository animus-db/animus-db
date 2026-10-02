//! ADR 0044 phase 2 — the per-node heartbeat send cost baseline
//! `docs/design/heartbeat-send-sites.md` maps.
//!
//! **C-02 PR 1** measured the PRE-batcher cost here: `G` independently
//! hosted groups sharing no physical env send `G`x the `AppendEntries`
//! traffic. **C-02 PR 3 (this cutover) flips the measurement itself**, not
//! just its assertion: heartbeat batching is now the DEFAULT (`--heartbeat-
//! batch` defaults on; `--no-heartbeat-batch` is the opt-out), so the
//! baseline this file pins is what a freshly-started node actually does —
//! several groups sharing ONE physical node's env, batching on, physical
//! frames flat while the logical per-group count keeps scaling with `G`.
//! This is the identical shape `tests/heartbeat_batch_corpus.rs`'s own cell
//! (a) (`scenario_frames_scale_with_peers_not_groups`) already proved as an
//! opt-in capability in PR 2 — this file now pins the SAME amortization as
//! today's DEFAULT behavior, not merely something available behind a flag.
//! `heartbeat_batch_corpus.rs` keeps the batching-OFF control cell
//! (`scenario_batching_off_frames_scale_with_groups_like_the_old_default`)
//! as the explicit opt-out proof — see that file's own module doc.
//!
//! **Why co-hosted in one `Simulator`, not `G` independent worlds (as PR 1
//! originally had it).** Batching only has anything to amortize when
//! several groups share one physical node's own `env` (and thus one
//! `HeartbeatBatcher`) — `RaftKvNode::start_with_metrics` (PR 1's own
//! constructor) forces `PRIMARY_STREAM` and takes no batcher at all, so it
//! structurally cannot host more than one group per node id and cannot
//! measure a physical frame count. This file now uses `start_hosted_
//! campaigning_with_batcher`/`start_hosted_with_batcher` instead (ADR 0026
//! `(node, stream = tablet_id)` addressing — the real production shape —
//! plus PR 2's batcher hook), mirroring `heartbeat_batch_corpus.rs`'s own
//! `hosted_group_fixed_leader` helper: every group's leader is forced onto
//! the SAME physical node (deterministic campaign) so leadership does not
//! spread across the other two physical nodes as the group count grows —
//! see that helper's own doc for why that confound would otherwise swamp
//! the measurement (more leader nodes ⇒ more distinct destination frames,
//! independent of batching).

use std::time::Duration;

use animus_cp_data::heartbeat_batch::{DEFAULT_HEARTBEAT_BATCH_INTERVAL, HeartbeatBatcher};
use animus_cp_data::{RaftKvNode, StorageScope};
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

/// One `MetricsHandle` per physical node id, index-aligned with [`NODES`] —
/// every group hosted on a given node id (across every stream) shares the
/// same handle, exactly as a real node's one `env.metrics()` sink would.
fn metrics_handles() -> [MetricsHandle; 3] {
    std::array::from_fn(|_| MetricsHandle::recording())
}

/// One [`HeartbeatBatcher`] per physical node id, index-aligned with
/// [`NODES`] and sharing [`metrics_handles`]'s own handles — the production
/// shape as of this cutover: every group hosted on a node gets the SAME
/// per-node batcher (`host::Reconciler::enable_heartbeat_batching`'s own
/// wiring, now the default).
fn batchers(sim: &Simulator, handles: &[MetricsHandle; 3]) -> [HeartbeatBatcher<SimEnv>; 3] {
    std::array::from_fn(|i| {
        HeartbeatBatcher::new(
            sim.env(nid(NODES[i])),
            DEFAULT_HEARTBEAT_BATCH_INTERVAL,
            handles[i].clone(),
        )
    })
}

/// Host one 3-replica group on `stream`, over the same three physical node
/// ids every other group in a run also uses, with replica `leader_idx`
/// campaigning immediately at bootstrap so every group hosted this way
/// lands on the SAME physical leader node (deterministic first leader —
/// see `crates/animus-cp-data/CLAUDE.md`'s matching entry) rather than a
/// randomized election that would spread leadership across all 3 physical
/// nodes as the group count grows.
fn hosted_group_fixed_leader(
    sim: &Simulator,
    stream: u64,
    batchers: &[HeartbeatBatcher<SimEnv>; 3],
    leader_idx: usize,
) -> Vec<KvNode> {
    NODES
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            let env = sim.env(nid(id));
            let all_nodes = NODES.iter().copied().map(nid).collect();
            let engine = MemoryEngine::new();
            let scope = StorageScope::whole();
            let batcher = Some(batchers[i].clone());
            if leader_idx == i {
                RaftKvNode::start_hosted_campaigning_with_batcher(
                    env, all_nodes, engine, scope, stream, batcher,
                )
            } else {
                RaftKvNode::start_hosted_with_batcher(
                    env, all_nodes, engine, scope, stream, batcher,
                )
            }
        })
        .collect()
}

/// Host `count` groups sharing the same three node ids in ONE `Simulator`,
/// batching on (today's default), every group's leader forced to the SAME
/// physical node (index 0), for a settle + idle window. Returns that one
/// leader node's own (`CpHeartbeatFramesSent`, `CpAppendEntriesSent`) —
/// reading only `handles[0]` (never summed across all three) is what
/// isolates "one node leading many groups" from "how many of the 3
/// physical nodes lead something," the actual amortization claim (§4 of
/// the design doc: flat **per node pair**, i.e. per leader).
fn run_batched_groups(count: u64, seed: u64) -> (u64, u64) {
    let sim = Simulator::new(seed);
    let handles = metrics_handles();
    let batchers = batchers(&sim, &handles);
    // Host every group before advancing time at all, so every group gets
    // the IDENTICAL total (settle + idle) ticking window regardless of
    // `count` — hosting them one-at-a-time with a settle run_for between
    // each would let earlier groups accumulate extra ticks while later
    // groups are still being added, biasing the ratio upward with `count`
    // for reasons that have nothing to do with batching.
    let groups: Vec<Vec<KvNode>> = (0..count)
        .map(|g| hosted_group_fixed_leader(&sim, 100 + g, &batchers, 0))
        .collect();
    let mut sim = sim;
    sim.run_for(SETTLE);
    for (g, nodes) in groups.iter().enumerate() {
        assert!(
            nodes[0].is_leader(),
            "group {g}'s forced leader (physical node index 0) failed to \
             win leadership by settle (seed={seed})"
        );
    }
    sim.run_for(IDLE_WINDOW);
    drop(groups);
    (
        handles[0].get(Metric::CpHeartbeatFramesSent),
        handles[0].get(Metric::CpAppendEntriesSent),
    )
}

/// **Post-cutover default-behavior baseline (C-02 PR 3).** With heartbeat
/// batching on by default, `Metric::CpHeartbeatFramesSent` — the physical
/// wire-frame count the ONE node leading every group actually sends per
/// destination — stays flat as the hosted group count grows 5x (amortized
/// across the SAME 3-node-pair destination set), while `Metric::
/// CpAppendEntriesSent` — the logical per-group heartbeat count — keeps
/// scaling ~5x with the group count exactly as PR 1's own pre-batcher
/// baseline measured, proving the batcher preserves every group's own
/// semantics (§3 of the map) while amortizing only the wire framing, and
/// that this amortization is what a freshly-started node gets with no
/// flag at all.
///
/// Measured (this test, `cargo test -p animus-cp-data --test
/// heartbeat_cost`): `frames1=238`, `frames5=238` (ratio 1.00, flat)
/// against `logical1=238`, `logical5=1190` (ratio 5.00, still scaling with
/// `G`) — identical to `heartbeat_batch_corpus.rs`'s own cell (a)
/// measurement, since this is now the same shape measuring the same
/// default-on behavior.
#[test]
fn heartbeat_frames_scale_with_peers_not_groups_by_default() {
    let (frames1, logical1) = run_batched_groups(1, 0xC02_0001);
    let (frames5, logical5) = run_batched_groups(5, 0xC02_0002);

    assert!(
        logical1 > 0 && logical5 > 0,
        "sanity: some heartbeat traffic must have been sent over a \
         {IDLE_WINDOW:?} idle window (heartbeat_interval={HEARTBEAT_INTERVAL:?})"
    );

    // Generous bounds absorb election-settle jitter — the point is "scales
    // with G," not an exact multiple.
    let logical_ratio = logical5 as f64 / logical1 as f64;
    assert!(
        (4.0..=6.0).contains(&logical_ratio),
        "expected ~5x logical AppendEntries traffic (`CpAppendEntriesSent`) \
         for 5x the actively-hosted groups — batching preserves the LOGICAL \
         per-group count, only the wire framing is amortized: logical1={logical1} \
         logical5={logical5} ratio={logical_ratio:.2}"
    );

    let frame_ratio = frames5 as f64 / frames1 as f64;
    assert!(
        frame_ratio <= 1.5,
        "expected physical frame count (`CpHeartbeatFramesSent`) sent by \
         the ONE node leading every group to stay ~flat (ratio ≈ 1) as the \
         hosted group count grows 5x — this is now the DEFAULT behavior \
         (C-02 PR 3 cutover), not merely an opt-in capability: frames1={frames1} \
         frames5={frames5} ratio={frame_ratio:.2}"
    );
    assert!(
        frames1 > 0,
        "sanity: at least one physical frame must have been sent for a \
         single actively-ticking group"
    );
}
