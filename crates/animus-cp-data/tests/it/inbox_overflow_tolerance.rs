//! ADR 0026's 2026-09-28 inbox-cap amendment: proves loss via overflow is
//! tolerated end to end by a real, live Raft group — the actual property the
//! per-stream `InboxCap` (`crates/animus-env/src/lib.rs`) rests on: every
//! consumer of a `recv_stream`-fed inbox in this codebase tolerates a
//! dropped frame (Raft resends; a request/response class times out and
//! retries — see `docs/adr/0026-multiplexed-node-stream-addressing.md`'s
//! "Per-stream inbox cap" amendment's consumer-classification table).
//!
//! **Scenario**: reproduces the real production shape the amendment's own
//! live measurement found (`docs/lessons/code-patterns/2026-09-28-an-
//! unbounded-per-stream-queue-is-invisible-until-it-has-its-own-
//! observability.md`) — a leader starts replicating to a node whose own
//! consumer (`RaftKvNode::drive`'s `recv_stream` loop) has not started yet.
//! Here: a 3-node tablet group elects a leader and commits writes with only
//! 2 of 3 `RaftKvNode`s actually running (a live majority); the third
//! node's own `SimEnv` inbox still receives every heartbeat/`AppendEntries`
//! frame the leader addresses to it (nothing partitions or crashes it — it
//! simply has no driver polling `recv_stream` yet), which — under a tiny
//! configured [`InboxCap`] — overflows and evicts the earliest frames
//! before the third node's `RaftKvNode` is ever constructed. Once it
//! starts, ordinary Raft catch-up (the leader detects it is behind and
//! backfills/resends) must converge it to the leader's committed log
//! regardless of what was silently dropped by the cap — proving the loss
//! is tolerated, not merely "usually recovered by luck".
//!
//! Deterministic and seed-reproducible (ADR 0003): replay a specific run
//! with `ANIMUS_SEED=<seed> cargo test -p animus-cp-data --test
//! inbox_overflow_tolerance`.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_env::{EnvExt, InboxCap, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
/// This scenario's tablet-like stream id — deliberately **not**
/// `PRIMARY_STREAM` (real per-tablet groups never use it, ADR 0040
/// Decision A: `stream = tablet_id`, and `PRIMARY_STREAM`/the reserved
/// high-id block are exempt from `InboxCap` entirely, `animus_env::
/// is_reserved_stream`) — this is exactly the class of stream the cap
/// exists to bound.
const STREAM: u64 = 42;
/// Small enough that a few seconds of ordinary heartbeat/`AppendEntries`
/// traffic to a never-yet-consumed stream overflows it many times over,
/// without needing a fault-injected loss/delay knob at all — the leak this
/// reproduces is a **liveness gap** (no consumer yet), not a lossy network.
const CAP_FRAMES: usize = 8;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn leader(nodes: &[(u64, KvNode)], seed: u64) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(_, (_, n))| n.is_leader())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among the live nodes, got {ls:?} (seed={seed})"
    );
    ls[0]
}

fn put(nodes: &[(u64, KvNode)], seed: u64, key: &[u8], value: &[u8]) {
    let l = leader(nodes, seed);
    match nodes[l].1.put(key.to_vec(), value.to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected a put: {other:?} (seed={seed})"),
    }
}

/// Run a linearizable read on the current leader to completion (spawned,
/// since it awaits a read-barrier probe round), driving the sim up to
/// `budget`. Mirrors `tests/read_index.rs`'s own `lin_read` helper.
fn lin_read(
    sim: &mut Simulator,
    nodes: &[(u64, KvNode)],
    seed: u64,
    key: &[u8],
    budget: Duration,
) -> Option<Vec<u8>> {
    let l = leader(nodes, seed);
    let node = nodes[l].1.clone();
    let slot: std::sync::Arc<std::sync::Mutex<Option<Option<Vec<u8>>>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let out = std::sync::Arc::clone(&slot);
    let k = key.to_vec();
    node.env().clone().spawn_task(async move {
        let v = node.linearizable_get(&k).await;
        *out.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock().unwrap().clone().unwrap_or(None)
}

fn scenario(seed: u64) {
    let mut sim = Simulator::new(seed);
    sim.set_inbox_cap(InboxCap {
        max_bytes: usize::MAX,
        max_frames: CAP_FRAMES,
    });

    // Start only nodes 0 and 1 — a live majority of the 3-node group.
    // Node 2's own `SimEnv` inbox is nonetheless a real destination the
    // moment `sim.env(nid(2))` is even implicitly addressed by a `Deliver`
    // (no explicit registration needed — `fire_event`'s `Deliver` arm
    // creates the entry lazily), exactly like the production incident: the
    // control/data plane already knows node 2 is a configured replica
    // (`all_nodes` below) long before this test ever constructs its
    // `RaftKvNode`.
    let all_nodes: Vec<_> = NODES.iter().copied().map(nid).collect();
    let mut nodes: Vec<(u64, KvNode)> = vec![0, 1]
        .into_iter()
        .map(|id| {
            (
                id,
                RaftKvNode::start_hosted(
                    sim.env(nid(id)),
                    all_nodes.clone(),
                    MemoryEngine::new(),
                    StorageScope::whole(),
                    STREAM,
                ),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2)); // elect, with node 2 absent

    // Commit a steady stream of writes while node 2's inbox silently
    // accumulates (and, under the tiny cap, overflows) every frame the
    // leader addresses to it — heartbeats and real `AppendEntries` alike.
    for i in 0..40u32 {
        put(
            &nodes,
            seed,
            format!("k{i}").as_bytes(),
            format!("v{i}").as_bytes(),
        );
        sim.run_for(Duration::from_millis(100));
    }

    assert_eq!(
        sim.inbox_len(nid(2), STREAM),
        CAP_FRAMES,
        "node 2's never-yet-consumed inbox must be pinned at the cap, not \
         grown without bound, before its own RaftKvNode ever starts \
         (seed={seed})"
    );
    let overflow_drops = sim
        .trace_lines()
        .iter()
        .filter(|l| l.contains("inbox-overflow"))
        .count();
    assert!(
        overflow_drops > 0,
        "this scenario must actually exercise the cap — no frame was \
         evicted, so nothing here proves overflow-tolerance (seed={seed})"
    );

    // Now start node 2's own driver — the reconciler finally catching up to
    // reality in the real incident. Whatever the cap silently dropped is
    // gone for good; ordinary Raft catch-up (the leader notices node 2 is
    // behind and backfills/resends via its own retry cadence, or falls back
    // to InstallSnapshot if the log has moved on) must still converge it.
    nodes.push((
        2,
        RaftKvNode::start_hosted(
            sim.env(nid(2)),
            all_nodes.clone(),
            MemoryEngine::new(),
            StorageScope::whole(),
            STREAM,
        ),
    ));
    sim.run_for(Duration::from_secs(15)); // generous catch-up budget

    // Convergence: every node's own engine reflects every committed write —
    // the loss the cap introduced was tolerated, not silently accepted as a
    // permanent divergence.
    for i in 0..40u32 {
        let key = format!("k{i}");
        let want = format!("v{i}").into_bytes();
        for (id, n) in &nodes {
            assert_eq!(
                block_on(n.local_get(key.as_bytes())),
                Some(want.clone()),
                "node {id} missing {key} after catch-up (seed={seed})"
            );
        }
    }

    // Linearizability: a fresh write after convergence is visible through a
    // real ReadIndex read-barrier round on whichever node currently leads —
    // never a stale/local peek — proving the group is not merely
    // "eventually consistent by luck" after tolerating the overflow.
    put(&nodes, seed, b"final", b"value");
    let got = lin_read(&mut sim, &nodes, seed, b"final", Duration::from_secs(3));
    assert_eq!(
        got,
        Some(b"value".to_vec()),
        "a linearizable read after catch-up must observe the latest write (seed={seed})"
    );
}

#[test]
fn a_never_hosted_streams_overflow_is_tolerated_and_the_group_converges() {
    let seed = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x1AB0_0F10);
    scenario(seed);
}
