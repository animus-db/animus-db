//! Real-thread `ProdEnv` regression for a genuine HLC/MVCC-timestamp race
//! (found via `animusd/tests/self_heal.rs` panicking under concurrent client
//! load — see `docs/engineering-lessons.md` and `RaftKvNode::propose_ordered`'s
//! own doc for the full mechanism).
//!
//! **Why this test exists at all, and why it can only be `ProdEnv`:** the bug
//! was never a missing witness point (all four points in the Key Invariants
//! doc are correct) — it was that minting a proposal's `ts`
//! (`mint_pushed`/`next_ceiling_candidate`) and appending that proposal to the
//! Raft log (`core.propose(..)`) used to be two separate, unsynchronized
//! steps with **no `.await` between them** at all. Under `SimEnv`'s
//! single-threaded cooperative scheduler, two sequential non-yielding
//! function calls can never be preempted mid-way by another task — so this
//! race is **not expressible in `SimEnv`** no matter how a scenario is
//! scripted; it only exists under genuine OS-thread parallelism
//! (`ProdEnv`'s multi-threaded tokio runtime), where two concurrent
//! proposers' calls can physically interleave between any two instructions.
//! This is the house "`SimEnv` proves logic/ordering, not real-thread
//! liveness" lesson in its purest form. Every other regression in this
//! crate's suite drives `SimEnv`; this one deliberately can't.
//!
//! The fix (`RaftKvNode::propose_ordered`) makes "compute this proposal's
//! `ts`" and "append it to the Raft log" one atomic step under the group's
//! own `core` lock, and additionally floors every ts-producing path on
//! `last_proposed_ts` (this leader's own last-*logged*, not just
//! last-*applied*, timestamp) so a write can't land below an
//! already-logged-but-not-yet-applied `ReadCeiling`. This test hammers a real
//! 3-node group directly (bypassing the whole `animusd` assembly, so the
//! failure mode is isolated to this crate's own invariant) with many
//! concurrent put+linearizable-get pairs — exercising both the write-vs-write
//! mint/propose race and the write-vs-ceiling race (every `linearizable_get`
//! proposes a `ReadCeiling` via `ensure_ceiling_above`) at once. Against the
//! unfixed code this reliably panics (`assert_ts_monotonic`) within a few
//! seconds; confirmed by temporarily reverting the fix and re-running.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Env, NodeId, ProdEnv, nid};
use animus_storage::MemoryEngine;
use tokio::time::{sleep, timeout};

type KvNode = RaftKvNode<ProdEnv, MemoryEngine>;

fn unique_tmp_dir() -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("animus-cp-ts-mono-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

async fn leader_of(nodes: &[KvNode]) -> usize {
    for _ in 0..200 {
        if let Some(i) = nodes.iter().position(RaftKvNode::is_leader) {
            return i;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader elected within 10s");
}

/// Put `key`/`value` on `leader`, wait for it to actually engine-apply (not
/// just commit), then read it back via a linearizable get and assert it
/// matches — the read is what also exercises the `ReadCeiling`/write race,
/// since every `linearizable_get` proposes a fresh ceiling when needed
/// (`ensure_ceiling_above`).
async fn put_then_confirm(leader: &KvNode, key: Vec<u8>, value: Vec<u8>) {
    let index = match leader.put(key.clone(), value.clone()) {
        ProposeResult::Accepted { index } => index,
        other => panic!("put was not accepted by the leader: {other:?}"),
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while leader.engine_applied_index() < index {
        assert!(
            tokio::time::Instant::now() < deadline,
            "put at index {index} never engine-applied within 10s — the apply \
             task most likely panicked (assert_ts_monotonic), see stderr"
        );
        sleep(Duration::from_millis(5)).await;
    }
    let got = leader.linearizable_get(&key).await;
    assert_eq!(
        got.as_deref(),
        Some(value.as_slice()),
        "linearizable_get for {key:?} did not return what was just put"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writes_and_reads_never_violate_ts_monotonicity() {
    let group: Vec<NodeId> = vec![nid(0), nid(1), nid(2)];
    let dirs: Vec<_> = (0..3).map(|_| unique_tmp_dir()).collect();
    let loop0 = || "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let mut envs = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        let (env, _addr) = ProdEnv::bind(nid(i as u64), loop0(), dir)
            .await
            .expect("bind");
        envs.push(env);
    }
    let book: BTreeMap<NodeId, SocketAddr> =
        envs.iter().map(|e| (e.node_id(), e.local_addr())).collect();
    for e in &envs {
        e.set_peers(book.clone());
    }

    let nodes: Vec<KvNode> = envs
        .into_iter()
        .map(|env| RaftKvNode::start(env, group.clone(), MemoryEngine::new()))
        .collect();

    let leader_idx = leader_of(&nodes).await;
    let leader = nodes[leader_idx].clone();

    // Many concurrent client tasks, each its own real OS-scheduled tokio task
    // (the parallelism this race needs), each doing several put+get round
    // trips against distinct keys. High enough concurrency that, against the
    // unfixed code, the mint/propose interleaving window reliably gets hit.
    let all = async {
        let mut handles = Vec::new();
        for c in 0..24u32 {
            let leader = leader.clone();
            handles.push(tokio::spawn(async move {
                for r in 0..10u32 {
                    let key = format!("k{c}-{r}").into_bytes();
                    let value = format!("v{c}-{r}").into_bytes();
                    put_then_confirm(&leader, key, value).await;
                }
            }));
        }
        for h in handles {
            h.await.expect(
                "client task panicked — most likely assert_ts_monotonic \
                 (raftkv apply: HLC ts did not strictly exceed the last applied)",
            );
        }
    };
    timeout(Duration::from_secs(60), all)
        .await
        .expect("concurrent load did not complete within 60s (possible deadlock)");

    for node in &nodes {
        node.shutdown();
    }
}
