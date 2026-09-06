//! ADR 0044 phase 2 (C-02 PR 2) — `SimEnv` fault-injection corpus for the
//! per-node [`HeartbeatBatcher`]. `crates/animus-cp-data/tests/
//! heartbeat_cost.rs` (C-02 PR 1) already proves the PRE-batcher baseline
//! (traffic scales with hosted-group count); this file proves the batcher
//! itself: amortization actually happens, every per-group invariant
//! `docs/design/heartbeat-send-sites.md` §3 promises still holds, an
//! unknown/released group in a received frame is dropped rather than
//! delivered or panicking, a genuine partition still elects correctly
//! despite losing a whole batched frame at once, a lossy-but-connected link
//! does not spuriously elect, and leader kill still converges.
//!
//! **Depth knob**: `ANIMUS_HEARTBEAT_SEEDS` (default 1), the house
//! `corpus::seed_expand` convention (see the root `CLAUDE.md`'s knob table
//! and `animus_test::corpus`'s own doc). Every seed is deterministic
//! (`SimEnv`, ADR 0003) — a scenario's own seed is a stable hash of its
//! name (`corpus::name_seed`), so `ANIMUS_SEED=<seed>` replays any failure
//! exactly (the seed printed in every assertion message is the scenario's
//! OWN seed, not a fresh draw).
//!
//! **Why co-hosted in one `Simulator`, unlike `heartbeat_cost.rs`'s
//! independent-`Simulator`-worlds shape.** Batching is a genuinely per-node
//! mechanism — it only has anything to amortize when several groups share
//! one physical node's own `env` (and thus one `HeartbeatBatcher`). Every
//! scenario below follows `tests/stream_addressing.rs`'s own established
//! pattern instead: several `RaftKvNode` groups, each on its own `stream`
//! (ADR 0026), sharing the SAME three physical `NodeId`s in ONE
//! `Simulator`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::heartbeat_batch::{DEFAULT_HEARTBEAT_BATCH_INTERVAL, HeartbeatBatcher};
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_env::{EnvExt, Metric, MetricsHandle, nid};
use animus_sim::{NetConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_test::corpus::{self, SeedVariant};

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// Elect + replicate the no-op — comfortably past `election_base` (150ms).
const SETTLE: Duration = Duration::from_secs(1);

/// One `MetricsHandle` per physical node id, index-aligned with [`NODES`] —
/// every group hosted on a given node id (across every stream) shares the
/// same handle, exactly as a real node's one `env.metrics()` sink would.
fn metrics_handles() -> [MetricsHandle; 3] {
    std::array::from_fn(|_| MetricsHandle::recording())
}

/// One [`HeartbeatBatcher`] per physical node id, index-aligned with
/// [`NODES`] and sharing [`metrics_handles`]'s own handles — construct once
/// per `Simulator`, pass a clone into every group hosted on that node id.
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
/// ids every other group in a scenario also uses. `batchers` attaches the
/// per-node batcher to each replica when `Some` (mirrors
/// `host::Reconciler::enable_heartbeat_batching`'s own "every group gets
/// the SAME per-node batcher" wiring); `None` is the batching-OFF control.
fn hosted_group(
    sim: &Simulator,
    stream: u64,
    batchers: Option<&[HeartbeatBatcher<SimEnv>; 3]>,
) -> Vec<KvNode> {
    hosted_group_inner(sim, stream, batchers, None)
}

/// Like [`hosted_group`], but replica `leader_idx` campaigns immediately at
/// bootstrap (`RaftKvNode::start_hosted_campaigning_with_batcher`, the same
/// deterministic-first-leader mechanism the in-place split fork uses — see
/// `crates/animus-cp-data/CLAUDE.md`'s "Deterministic first leader" entry)
/// instead of waiting out a randomized election timeout, so every group a
/// caller hosts this way lands on the SAME physical leader node. Used only
/// by cell (a): comparing physical frame counts across a growing group
/// count is only meaningful measured against a FIXED leader — with natural
/// random election, more groups means more of the 3 physical nodes end up
/// leading *something*, which would scale the frame count with the number
/// of distinct leader nodes (up to 3) rather than isolating the "one node
/// leading many groups" amortization this cell exists to prove.
fn hosted_group_fixed_leader(
    sim: &Simulator,
    stream: u64,
    batchers: &[HeartbeatBatcher<SimEnv>; 3],
    leader_idx: usize,
) -> Vec<KvNode> {
    hosted_group_inner(sim, stream, Some(batchers), Some(leader_idx))
}

fn hosted_group_inner(
    sim: &Simulator,
    stream: u64,
    batchers: Option<&[HeartbeatBatcher<SimEnv>; 3]>,
    leader_idx: Option<usize>,
) -> Vec<KvNode> {
    NODES
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            let env = sim.env(nid(id));
            let all_nodes = NODES.iter().copied().map(nid).collect();
            let engine = MemoryEngine::new();
            let scope = StorageScope::whole();
            let batcher = batchers.map(|b| b[i].clone());
            if leader_idx == Some(i) {
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

fn leader_index(nodes: &[KvNode], seed: u64, label: &str) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(
        ls.len(),
        1,
        "expected exactly one leader in {label}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

/// Run a linearizable read on `node` to completion — mirrors
/// `tests/quiescence.rs::lin_read`: spawned, since it awaits a quorum
/// ReadIndex probe round that only resolves while `Simulator` advances time.
fn lin_read(sim: &mut Simulator, node: &KvNode, key: &[u8], budget: Duration) -> Option<Vec<u8>> {
    let slot: Arc<Mutex<Option<Option<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let n = node.clone();
    let s = Arc::clone(&slot);
    let k = key.to_vec();
    node.env().clone().spawn_task(async move {
        let v = n.linearizable_get(&k).await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock()
        .unwrap()
        .clone()
        .expect("linearizable read did not complete (seed budget too small?)")
}

// ---------------------------------------------------------------------------
// The frozen corpus: named scenarios, each a fixed script.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Scenario {
    name: String,
    seed: u64,
    run: fn(u64),
}

impl SeedVariant for Scenario {
    fn scenario_name(&self) -> &str {
        &self.name
    }
    fn reseeded(&self, name: String, seed: u64) -> Self {
        Scenario {
            name,
            seed,
            run: self.run,
        }
    }
}

/// Depth knob (`ANIMUS_HEARTBEAT_SEEDS`, default 1) — mirrors every other
/// corpus's `ANIMUS_<X>_SEEDS` convention (root `CLAUDE.md`'s knob table).
fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_HEARTBEAT_SEEDS")
}

macro_rules! scenario {
    ($name:expr, $f:ident) => {
        Scenario {
            name: $name.to_string(),
            seed: corpus::name_seed($name),
            run: $f,
        }
    };
}

fn scenario_cells() -> Vec<Scenario> {
    vec![
        scenario!(
            "frames_scale_with_peers_not_groups",
            scenario_frames_scale_with_peers_not_groups
        ),
        scenario!(
            "batching_off_frames_scale_with_groups_like_the_old_default",
            scenario_batching_off_frames_scale_with_groups_like_the_old_default
        ),
        scenario!(
            "per_group_invariants_hold_under_batching",
            scenario_per_group_invariants_hold_under_batching
        ),
        scenario!(
            "real_partition_still_elects_despite_losing_a_whole_frame",
            scenario_real_partition_still_elects
        ),
        scenario!(
            "lossy_but_connected_link_causes_no_spurious_election",
            scenario_lossy_link_no_spurious_election
        ),
        scenario!(
            "unknown_group_in_a_frame_is_dropped_and_counted",
            scenario_unknown_group_dropped_and_counted
        ),
        scenario!(
            "leader_kill_converges_with_sibling_groups_unaffected",
            scenario_leader_kill_converges
        ),
        scenario!(
            "follower_kill_keeps_quorum_and_leader_stable",
            scenario_follower_kill_converges
        ),
    ]
}

fn corpus() -> Vec<Scenario> {
    corpus::seed_expand(scenario_cells(), seeds_per_cell())
}

#[test]
fn heartbeat_batch_corpus_runs_every_scenario() {
    for s in corpus() {
        (s.run)(s.seed);
    }
}

// ---------------------------------------------------------------------------
// Cell (a): physical frames scale with destination node pairs, not with
// the number of co-hosted groups — while the LOGICAL heartbeat count keeps
// scaling with the group count exactly as `heartbeat_cost.rs`'s (PR 1)
// baseline already proved for the flag-off path.
// ---------------------------------------------------------------------------

/// Host `count` groups sharing the same three node ids, batching on, every
/// group's leader forced to the SAME physical node (index 0 —
/// `hosted_group_fixed_leader`, the deterministic-campaign mechanism, so
/// leadership does not spread across the other two physical nodes as
/// `count` grows — see that function's own doc for why that confound would
/// otherwise swamp the measurement). Returns (that one leader node's own
/// `CpHeartbeatFramesSent`, that one leader node's own
/// `CpAppendEntriesSent`) after a settle + idle window — reading only
/// `handles[0]` (never summed across all three) is what isolates "one node
/// leading many groups" from "how many of the 3 physical nodes lead
/// something," which is the actual amortization claim (§4 of the design
/// doc: flat **per node pair**, i.e. per leader).
fn run_batched_groups(count: u64, seed: u64) -> (u64, u64) {
    let mut sim = Simulator::new(seed);
    let handles = metrics_handles();
    let batchers = batchers(&sim, &handles);
    // Host every group before advancing time at all, so every group gets
    // the IDENTICAL total (settle + idle) ticking window regardless of
    // `count` — hosting them one-at-a-time with a settle run_for between
    // each (as `heartbeat_cost.rs`'s independent-`Simulator`-worlds shape
    // effectively does per world) would let earlier groups accumulate
    // extra ticks while later groups are still being added, biasing the
    // ratio upward with `count` for reasons that have nothing to do with
    // batching.
    let groups: Vec<Vec<KvNode>> = (0..count)
        .map(|g| hosted_group_fixed_leader(&sim, 100 + g, &batchers, 0))
        .collect();
    sim.run_for(SETTLE);
    for (g, nodes) in groups.iter().enumerate() {
        assert!(
            nodes[0].is_leader(),
            "group {g}'s forced leader (physical node index 0) failed to \
             win leadership by settle (seed={seed})"
        );
    }
    sim.run_for(Duration::from_secs(5));
    drop(groups);
    (
        handles[0].get(Metric::CpHeartbeatFramesSent),
        handles[0].get(Metric::CpAppendEntriesSent),
    )
}

fn scenario_frames_scale_with_peers_not_groups(seed: u64) {
    let (frames1, logical1) = run_batched_groups(1, seed);
    let (frames5, logical5) = run_batched_groups(5, seed.wrapping_add(1));

    assert!(
        logical1 > 0 && logical5 > 0,
        "sanity: some heartbeat traffic must have been sent (seed={seed})"
    );
    let logical_ratio = logical5 as f64 / logical1 as f64;
    assert!(
        (4.0..=6.0).contains(&logical_ratio),
        "logical heartbeat count (`CpAppendEntriesSent`) must still scale \
         ~5x with 5x the hosted groups (batching preserves the LOGICAL \
         count — only the wire framing is amortized): logical1={logical1} \
         logical5={logical5} ratio={logical_ratio:.2} (seed={seed})"
    );

    let frame_ratio = frames5 as f64 / frames1 as f64;
    assert!(
        frame_ratio <= 1.5,
        "physical frame count (`CpHeartbeatFramesSent`) sent by the ONE \
         node leading every group must stay ~flat (ratio ≈ 1) as the \
         hosted group count grows 5x (amortized across the SAME \
         3-node-pair destination set), not scale with it: frames1={frames1} \
         frames5={frames5} ratio={frame_ratio:.2} (seed={seed})"
    );
    assert!(
        frames1 > 0,
        "sanity: at least one physical frame must have been sent for a \
         single actively-ticking group (seed={seed})"
    );
}

// ---------------------------------------------------------------------------
// Explicit opt-out cell (C-02 PR 3, ADR 0044 phase-2 cutover): with the
// batcher OFF (`--no-heartbeat-batch`), the logical heartbeat count still
// scales with the hosted-group count exactly as it did before this crate
// had a batcher at all (C-02 PR 1's own pre-batcher baseline,
// `heartbeat_cost.rs`, moved here now that that file's own baseline measures
// the DEFAULT (batching-on) behavior instead) — proving the opt-out flag
// genuinely restores the pre-cutover byte-for-byte behavior, not merely
// "some traffic reduction." Independent `Simulator` worlds, not co-hosted
// (see `spawn_unbatched_group`'s own doc for why). No batcher is attached
// at all, so there is no physical-frame count to measure separately here:
// `Metric::CpHeartbeatFramesSent` is only ever incremented by
// `HeartbeatBatcher::register`, never by the plain unbatched send path — a
// `--no-heartbeat-batch` node reports a flat zero on that counter forever,
// which `docs/design/heartbeat-send-sites.md`'s own closing note documents
// as the expected read, not a bug.
// ---------------------------------------------------------------------------

/// Stand up one independent 3-node group in its OWN `Simulator` world,
/// recording into the caller's per-node-id `handles` — `heartbeat_cost.rs`'s
/// (C-02 PR 1) own pre-cutover harness shape, reused verbatim here rather
/// than `hosted_group`'s co-hosted-with-injectable-batcher shape: with no
/// batcher at all there is nothing to co-host for (unbatched groups share
/// no destination-frame amortization to prove either way), and
/// `RaftKvNode::start_hosted_with_batcher(.., None)` — the co-hosted
/// constructor — calls `env.metrics()` internally, which is `SimEnv`'s
/// no-op handle unless a caller injects one; `start_with_metrics` is the
/// one constructor that both forces a real recording handle AND needs no
/// batcher, at the cost of forcing `PRIMARY_STREAM` (so it must live in its
/// own `Simulator` world to avoid a stream collision with any sibling
/// group on the same node ids — identical to PR 1's own reasoning).
fn spawn_unbatched_group(seed: u64, handles: &[MetricsHandle; 3]) -> (Simulator, Vec<KvNode>) {
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

/// Run `count` independent groups (batcher OFF), each for `SETTLE + 5s` of
/// its own virtual time, and return the combined `Metric::
/// CpAppendEntriesSent` total across every group's replicas — the
/// per-node-aggregate metric a real `--no-heartbeat-batch` node's own
/// shared `env.metrics()` sink would show.
fn run_unbatched_groups(count: u64, seed_base: u64) -> u64 {
    let handles = metrics_handles();
    let mut groups: Vec<(Simulator, Vec<KvNode>)> = Vec::new();
    for g in 0..count {
        let (mut sim, nodes) = spawn_unbatched_group(seed_base.wrapping_add(g), &handles);
        sim.run_for(SETTLE);
        groups.push((sim, nodes));
    }
    for (sim, _nodes) in groups.iter_mut() {
        sim.run_for(Duration::from_secs(5));
    }
    handles
        .iter()
        .map(|h| h.get(Metric::CpAppendEntriesSent))
        .sum()
}

fn scenario_batching_off_frames_scale_with_groups_like_the_old_default(seed: u64) {
    let low = run_unbatched_groups(1, seed);
    let high = run_unbatched_groups(5, seed.wrapping_add(1));

    let ratio = high as f64 / low as f64;
    assert!(
        (4.0..=6.0).contains(&ratio),
        "with the batcher off (`--no-heartbeat-batch`), AppendEntries \
         traffic must still scale ~5x with 5x the hosted groups — this is \
         the pre-cutover default behavior, and the opt-out flag must \
         restore it byte-for-byte: low={low} high={high} ratio={ratio:.2} \
         (seed={seed})"
    );
    assert!(
        low > 0,
        "sanity: a single active group must have sent some heartbeats \
         (seed={seed})"
    );
}

// ---------------------------------------------------------------------------
// Cell (b): every per-group invariant `heartbeat-send-sites.md` §3
// promises still holds under batching — election timers, term, commit
// index, and ReadIndex confirmation, all per group, independently.
// ---------------------------------------------------------------------------

fn scenario_per_group_invariants_hold_under_batching(seed: u64) {
    const GROUPS: u64 = 4;
    let mut sim = Simulator::new(seed);
    let handles = metrics_handles();
    let batchers = batchers(&sim, &handles);
    let mut groups: Vec<Vec<KvNode>> = Vec::new();
    for g in 0..GROUPS {
        let nodes = hosted_group(&sim, 200 + g, Some(&batchers));
        sim.run_for(SETTLE);
        groups.push(nodes);
    }

    // Snapshot each group's leader + term right after settle.
    let mut leaders: Vec<usize> = Vec::new();
    let mut terms_after_settle: Vec<u64> = Vec::new();
    for (g, nodes) in groups.iter().enumerate() {
        let l = leader_index(nodes, seed, &format!("group {g} post-settle"));
        leaders.push(l);
        terms_after_settle.push(nodes[l].term());
    }

    // A long idle window under batching, no faults at all.
    sim.run_for(Duration::from_secs(10));

    // (i) No spurious elections: every group's own leader and term are
    // unchanged — batching must not coalesce the SEMANTICS (per-group
    // election timers), only the transport.
    for (g, nodes) in groups.iter().enumerate() {
        let l = leader_index(nodes, seed, &format!("group {g} post-idle"));
        assert_eq!(
            l, leaders[g],
            "group {g} changed leader over a fault-free idle window under \
             batching (seed={seed})"
        );
        assert_eq!(
            nodes[l].term(),
            terms_after_settle[g],
            "group {g} bumped its term over a fault-free idle window under \
             batching — a spurious election (seed={seed})"
        );
    }

    // (ii) Commit index still advances per group, and (iii) ReadIndex
    // confirmation still resolves — one write + one linearizable read per
    // group, each group's own leader.
    for (g, nodes) in groups.iter().enumerate() {
        let l = leaders[g];
        let key = format!("k{g}").into_bytes();
        let value = format!("v{g}").into_bytes();
        match nodes[l].put(key.clone(), value.clone()) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("group {g} leader rejected a put: {other:?} (seed={seed})"),
        }
        sim.run_for(Duration::from_secs(2));
        for (i, n) in nodes.iter().enumerate() {
            assert_eq!(
                futures::executor::block_on(n.local_get(&key)),
                Some(value.clone()),
                "group {g} replica {i} never applied its own committed write \
                 under batching (seed={seed})"
            );
        }
        let read = lin_read(&mut sim, &nodes[l], &key, Duration::from_secs(2));
        assert_eq!(
            read,
            Some(value),
            "group {g}'s ReadIndex-confirmed linearizable read did not see \
             its own just-committed write under batching (seed={seed})"
        );
    }
}

// ---------------------------------------------------------------------------
// Cell (c1): a genuine partition — which drops the WHOLE batched frame
// (every co-hosted group's heartbeat at once, since partition blocks a
// node pair regardless of stream) — still lets each affected group elect
// correctly, and a sibling group NOT sharing the partitioned pair stays
// stable throughout.
// ---------------------------------------------------------------------------

fn scenario_real_partition_still_elects(seed: u64) {
    let mut sim = Simulator::new(seed);
    let handles = metrics_handles();
    let batchers = batchers(&sim, &handles);
    // Leaders forced to DIFFERENT physical nodes (0 and 1) — see
    // `hosted_group_fixed_leader`'s own doc. Without this, group A's and
    // group B's leaders are independently, randomly elected across the
    // same 3 physical nodes and can coincide, in which case isolating
    // group A's leader node ALSO isolates group B's leader (the partition
    // is at the node-pair level, per this scenario's own point) and group
    // B would legitimately re-elect too — a real, valid property, but not
    // the "sibling sharing no partitioned pair is untouched" one this cell
    // exists to prove. Forcing distinct leader nodes makes that property
    // deterministic instead of a per-seed coin flip.
    let group_a = hosted_group_fixed_leader(&sim, 300, &batchers, 0);
    let group_b = hosted_group_fixed_leader(&sim, 301, &batchers, 1);
    sim.run_for(SETTLE);

    let la = leader_index(&group_a, seed, "group A pre-partition");
    let lb = leader_index(&group_b, seed, "group B pre-partition");
    assert_eq!(
        NODES[la], NODES[0],
        "group A's forced leader (physical node index 0) failed to win \
         leadership by settle (seed={seed})"
    );
    assert_eq!(
        NODES[lb], NODES[1],
        "group B's forced leader (physical node index 1) failed to win \
         leadership by settle (seed={seed})"
    );
    let term_a_before = group_a[la].term();
    let term_b_before = group_b[lb].term();

    // Fully isolate group A's leader node from BOTH its followers, in
    // both directions — this blocks every stream between those node ids,
    // including the reserved heartbeat-batch stream group B also rides if
    // it shares the same node pair. Group B is deliberately NOT touched.
    let leader_id = nid(NODES[la]);
    for (i, &id) in NODES.iter().enumerate() {
        if i != la {
            sim.partition(leader_id.clone(), nid(id));
            sim.partition(nid(id), leader_id.clone());
        }
    }

    // Comfortably past the election timeout (`election_base` 150ms) many
    // times over, so a real partition is unambiguous.
    sim.run_for(Duration::from_secs(3));

    // Group A: the surviving two replicas must have elected a NEW leader
    // at a HIGHER term (the isolated old leader keeps believing itself
    // leader — a halted node's own view is frozen/stale, not proof of
    // anything about the rest of the group).
    let survivors: Vec<usize> = (0..group_a.len()).filter(|&i| i != la).collect();
    let new_leaders: Vec<usize> = survivors
        .iter()
        .copied()
        .filter(|&i| group_a[i].is_leader())
        .collect();
    assert_eq!(
        new_leaders.len(),
        1,
        "group A's two surviving replicas must elect exactly one new \
         leader after their old leader is partitioned away: survivors' \
         leader flags = {:?} (seed={seed})",
        survivors
            .iter()
            .map(|&i| group_a[i].is_leader())
            .collect::<Vec<_>>()
    );
    assert!(
        group_a[new_leaders[0]].term() > term_a_before,
        "group A's new leader must be at a strictly higher term than the \
         old (now-partitioned) leader's (seed={seed})"
    );

    // Group B, which shares no partitioned pair, must be completely
    // unaffected — same leader, same term.
    let lb_after = leader_index(&group_b, seed, "group B post-partition");
    assert_eq!(
        lb_after, lb,
        "group B's leader changed even though none of its own traffic was \
         partitioned (seed={seed})"
    );
    assert_eq!(
        group_b[lb_after].term(),
        term_b_before,
        "group B bumped its term even though none of its own traffic was \
         partitioned (seed={seed})"
    );
}

// ---------------------------------------------------------------------------
// Cell (c2): a lossy-but-connected link — independent per-frame drops at a
// modest probability — must not spuriously elect, at a rate the unbatched
// path already tolerates. **The bound this proves**: batching does not
// change any ONE group's own per-tick delivery probability — exactly one
// send still carries that group's own logical heartbeat per tick, whether
// batched with siblings or not, so the SAME `p` that keeps the unbatched
// path election-free keeps the batched path election-free too. What
// batching changes is CORRELATION (when a frame is lost, every co-hosted
// group loses its heartbeat on that same tick together) — not any single
// group's own miss rate — so this cell asserts the property that actually
// follows from that: no election fires for ANY of several co-hosted groups
// sharing the lossy link, at `p` well below what several consecutive
// misses would need to cross the election timeout.
// ---------------------------------------------------------------------------

fn scenario_lossy_link_no_spurious_election(seed: u64) {
    const GROUPS: u64 = 3;
    let mut sim = Simulator::new(seed);
    let handles = metrics_handles();
    let batchers = batchers(&sim, &handles);
    let mut groups: Vec<Vec<KvNode>> = Vec::new();
    for g in 0..GROUPS {
        let nodes = hosted_group(&sim, 400 + g, Some(&batchers));
        sim.run_for(SETTLE);
        groups.push(nodes);
    }

    let mut leaders = Vec::new();
    let mut terms_before = Vec::new();
    for (g, nodes) in groups.iter().enumerate() {
        let l = leader_index(nodes, seed, &format!("group {g} pre-loss"));
        leaders.push(l);
        terms_before.push(nodes[l].term());
    }

    // A modest, independent-per-message 5% drop probability on the link
    // from every group's own leader node to one fixed follower node — low
    // enough that several consecutive misses (what an election actually
    // needs) stays vanishingly unlikely over this run's tick count.
    let mut cfg = NetConfig::default();
    cfg.set_drop_prob(0.05);
    for (g, nodes) in groups.iter().enumerate() {
        let l = leaders[g];
        let follower = (0..nodes.len()).find(|&i| i != l).expect("has a follower");
        sim.set_link_net_config(nid(NODES[l]), nid(NODES[follower]), cfg.clone());
    }

    sim.run_for(Duration::from_secs(10));

    for (g, nodes) in groups.iter().enumerate() {
        let l = leader_index(nodes, seed, &format!("group {g} post-loss"));
        assert_eq!(
            l, leaders[g],
            "group {g} changed leader under a 5% lossy-but-connected link \
             — batching must not make an ordinary tolerable loss rate \
             spuriously elect (seed={seed})"
        );
        assert_eq!(
            nodes[l].term(),
            terms_before[g],
            "group {g} bumped its term under a 5% lossy-but-connected link \
             (seed={seed})"
        );
    }
}

// ---------------------------------------------------------------------------
// Cell (d): a frame naming a group this node no longer hosts is dropped
// and counted, never delivered into a stale inbox and never a panic — the
// exact lifecycle race a group release/teardown can produce in production.
// ---------------------------------------------------------------------------

fn scenario_unknown_group_dropped_and_counted(seed: u64) {
    let mut sim = Simulator::new(seed);
    let handles = metrics_handles();
    let batchers = batchers(&sim, &handles);
    let nodes = hosted_group(&sim, 500, Some(&batchers));
    sim.run_for(SETTLE);

    let l = leader_index(&nodes, seed, "pre-shutdown");
    let follower = (0..nodes.len()).find(|&i| i != l).expect("has a follower");
    let follower_handle = handles[follower].clone();
    let before = follower_handle.get(Metric::CpHeartbeatDemuxDropped);

    // Release just this one follower's own group handle — its node's
    // batcher (still running, still serving any OTHER group that node
    // hosts) unregisters this stream at teardown (`drive`'s halted exit),
    // so the leader's very next flush toward this follower's node names a
    // group id nobody there hosts any more.
    nodes[follower].shutdown();
    let stopped = (0..50).any(|_| {
        sim.run_for(Duration::from_millis(50));
        nodes[follower].is_stopped()
    });
    assert!(stopped, "follower did not stop within budget (seed={seed})");

    // A few more heartbeat intervals so at least one post-teardown frame
    // from the (still-leading) leader reaches the now-unhosting follower
    // node.
    sim.run_for(DEFAULT_HEARTBEAT_BATCH_INTERVAL * 5);

    let after = follower_handle.get(Metric::CpHeartbeatDemuxDropped);
    assert!(
        after > before,
        "a batched heartbeat frame naming a just-released group must be \
         dropped and counted (`CpHeartbeatDemuxDropped`) on the receiving \
         node: before={before} after={after} (seed={seed})"
    );
}

// ---------------------------------------------------------------------------
// Cell (e): leader kill (a whole-node crash) still converges under
// batching, and a sibling group sharing one of the two surviving node ids
// is completely unaffected — no cascading spurious election from the
// crashed node's own batcher tasks disappearing.
// ---------------------------------------------------------------------------

fn scenario_leader_kill_converges(seed: u64) {
    let mut sim = Simulator::new(seed);
    let handles = metrics_handles();
    let batchers = batchers(&sim, &handles);
    let group_a = hosted_group(&sim, 600, Some(&batchers));
    sim.run_for(SETTLE);
    let group_b = hosted_group(&sim, 601, Some(&batchers));
    sim.run_for(SETTLE);

    let la = leader_index(&group_a, seed, "group A pre-kill");
    let lb = leader_index(&group_b, seed, "group B pre-kill");
    let term_b_before = group_b[lb].term();

    sim.crash(nid(NODES[la]));
    sim.run_for(Duration::from_secs(3));

    let survivors: Vec<usize> = (0..group_a.len()).filter(|&i| i != la).collect();
    let new_leaders: Vec<usize> = survivors
        .iter()
        .copied()
        .filter(|&i| group_a[i].is_leader())
        .collect();
    assert_eq!(
        new_leaders.len(),
        1,
        "group A's surviving replicas must elect exactly one new leader \
         after their leader's node crashes (seed={seed})"
    );
    let new_leader = new_leaders[0];

    // The group still serves under its new leader.
    match group_a[new_leader].put(b"after-kill".to_vec(), b"ok".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!(
            "group A's new leader rejected a write after leader kill: \
             {other:?} (seed={seed})"
        ),
    }
    sim.run_for(Duration::from_secs(2));
    for &i in &survivors {
        assert_eq!(
            futures::executor::block_on(group_a[i].local_get(b"after-kill")),
            Some(b"ok".to_vec()),
            "group A survivor {i} never applied the post-recovery write \
             (seed={seed})"
        );
    }

    // Group B is unaffected by the crash's own batcher teardown — same
    // leader, same term, unless B's leader happened to be hosted on the
    // crashed node too (in which case it independently must have
    // recovered exactly like group A did, checked the same way instead).
    if group_b[lb].is_leader() {
        assert_eq!(
            group_b[lb].term(),
            term_b_before,
            "group B bumped its term from a sibling group's leader crash \
             it shares no faulted node with (seed={seed})"
        );
    } else {
        // `group B`'s leader happened to sit on the crashed node too —
        // it must independently recover the identical way group A did.
        let survivors_b: Vec<usize> = (0..group_b.len()).filter(|&i| i != lb).collect();
        let new_b: Vec<usize> = survivors_b
            .iter()
            .copied()
            .filter(|&i| group_b[i].is_leader())
            .collect();
        assert_eq!(
            new_b.len(),
            1,
            "group B's surviving replicas must also elect exactly one new \
             leader if its own leader shared the crashed node (seed={seed})"
        );
    }
}

// ---------------------------------------------------------------------------
// Cell (e), sibling half: a FOLLOWER's node crashes (not the leader's) — a
// 3-replica group keeps quorum (2 of 3) with no election at all needed, so
// this proves the batcher's own per-node teardown (the crashed node's flush
// and demux tasks simply stop existing) does not perturb a still-healthy
// leader/quorum, and a sibling group sharing the crashed node id still
// converges once ITS OWN leader (if it was hosted there) re-elects.
// ---------------------------------------------------------------------------

fn scenario_follower_kill_converges(seed: u64) {
    let mut sim = Simulator::new(seed);
    let handles = metrics_handles();
    let batchers = batchers(&sim, &handles);
    let group_a = hosted_group(&sim, 700, Some(&batchers));
    sim.run_for(SETTLE);
    let group_b = hosted_group(&sim, 701, Some(&batchers));
    sim.run_for(SETTLE);

    let la = leader_index(&group_a, seed, "group A pre-kill");
    let lb = leader_index(&group_b, seed, "group B pre-kill");
    let term_a_before = group_a[la].term();
    let term_b_before = group_b[lb].term();

    // Kill a follower of group A specifically (never the leader).
    let victim = (0..group_a.len())
        .find(|&i| i != la)
        .expect("a 3-replica group has a follower");
    sim.crash(nid(NODES[victim]));
    sim.run_for(Duration::from_secs(3));

    // Group A: quorum (2 of 3) survives with NO election — same leader,
    // same term, and it still serves writes.
    assert!(
        group_a[la].is_leader(),
        "group A's leader must still be leader after a FOLLOWER (not the \
         leader) crashes — quorum (2 of 3) survives with no election \
         needed (seed={seed})"
    );
    assert_eq!(
        group_a[la].term(),
        term_a_before,
        "group A must not bump its term over a follower-only crash \
         (seed={seed})"
    );
    match group_a[la].put(b"after-follower-kill".to_vec(), b"ok".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!(
            "group A's leader rejected a write after a follower crash: \
             {other:?} (seed={seed})"
        ),
    }
    sim.run_for(Duration::from_secs(2));
    for (i, n) in group_a.iter().enumerate() {
        if i == victim {
            continue;
        }
        assert_eq!(
            futures::executor::block_on(n.local_get(b"after-follower-kill")),
            Some(b"ok".to_vec()),
            "group A survivor {i} never applied the post-crash write \
             (seed={seed})"
        );
    }

    // Group B: unaffected unless it happened to host ITS OWN leader on the
    // crashed node id, in which case it independently re-elects exactly
    // like `scenario_leader_kill_converges` proves.
    if NODES[lb] != NODES[victim] {
        assert_eq!(
            group_b[lb].term(),
            term_b_before,
            "group B bumped its term from a sibling group's follower crash \
             on a node id it doesn't lead from (seed={seed})"
        );
        assert!(
            group_b[lb].is_leader(),
            "group B's leader must be unaffected by a follower crash on a \
             node id it doesn't lead from (seed={seed})"
        );
    } else {
        let survivors_b: Vec<usize> = (0..group_b.len()).filter(|&i| i != lb).collect();
        let new_b: Vec<usize> = survivors_b
            .iter()
            .copied()
            .filter(|&i| group_b[i].is_leader())
            .collect();
        assert_eq!(
            new_b.len(),
            1,
            "group B's surviving replicas must elect exactly one new \
             leader if ITS OWN leader happened to sit on the crashed node \
             (seed={seed})"
        );
    }
}
