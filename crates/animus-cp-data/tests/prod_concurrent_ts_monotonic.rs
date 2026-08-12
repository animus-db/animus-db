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
//! steps with **no `.await` between them** at all. `SimEnv`'s single-threaded
//! executor only ever yields control at an `.await` point — mint-then-propose
//! was one uninterrupted *synchronous* stretch of code, so two tasks racing
//! to run it could never actually interleave under `SimEnv`, no matter how a
//! scenario is scripted; only genuine OS-thread parallelism (`ProdEnv`'s
//! multi-threaded tokio runtime) can preempt one task's synchronous code
//! between any two instructions to let another run. This is the house
//! "`SimEnv` proves logic/ordering, not real-thread liveness" lesson made
//! concrete. Every other regression in this crate's suite drives `SimEnv`;
//! this one deliberately can't.
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
use animus_tablet::{escape, partition_token};
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

/// Start a real 3-node group over `ProdEnv` (bound loopback sockets, a
/// temp dir per node) and return it once a leader has been elected.
/// Shared by every test in this file — each hammers the *same* real,
/// bound group with concurrent client tasks (the genuine OS-thread
/// parallelism this file's races need).
async fn start_group() -> (Vec<KvNode>, usize) {
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
    (nodes, leader_idx)
}

/// A real ADR 0022-shaped data-plane key: `partition_token(pk) ||
/// escape(pk) || rk` — `RaftKvNode::txn_write`'s anchor-token disjointness
/// proof (`txn.rs`) assumes every key leads with the 8-byte token.
fn concurrent_key(client: u32, round: u32) -> Vec<u8> {
    let pk = format!("acct{client}");
    let mut out = partition_token(pk.as_bytes()).to_vec();
    out.extend_from_slice(&escape(pk.as_bytes()));
    out.extend_from_slice(format!("-{round}").as_bytes());
    out
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
    let (nodes, leader_idx) = start_group().await;
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

/// `txn_write` + a linearizable_get, waiting on the same "commit != apply"
/// confirm-by-index discipline `put_then_confirm` does (via `txn_write`'s
/// own internal `wait_applied`, so this just needs to await it) — the txn
/// analogue of `put_then_confirm`, extending this file's coverage (ADR
/// 0018 §2/PR3) to `TxnStage`/`TxnCommit`/`TxnResolve`, each of which mints
/// and proposes its own `ts` through the identical `propose_ordered`/
/// `propose_ordered_aux` critical section a plain `put` does.
async fn txn_write_then_confirm(leader: &KvNode, key: Vec<u8>, value: Vec<u8>) {
    leader
        .txn_write(vec![(key.clone(), Some(value.clone()))])
        .await
        .expect("txn_write did not complete (leader stepped down?)");
    let got = leader.linearizable_get(&key).await;
    assert_eq!(
        got.as_deref(),
        Some(value.as_slice()),
        "linearizable_get for {key:?} did not return what txn_write just committed"
    );
}

/// The txn-command extension of
/// `concurrent_writes_and_reads_never_violate_ts_monotonicity`: many
/// concurrent client tasks each hammering `txn_write` (stage + commit +
/// resolve — three proposals per call, each through `propose_ordered`/
/// `propose_ordered_aux`) racing `linearizable_get`'s own `ReadCeiling`
/// proposals, on a real multi-threaded `ProdEnv` group. Every `KvCommand`
/// this PR added mints its `ts` through the exact same critical section a
/// plain `put` does, so this is the same race
/// `propose_ordered`/`assert_ts_monotonic` already guard against — this
/// test is the regression that the new commands didn't reopen it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_txn_writes_and_reads_never_violate_ts_monotonicity() {
    let (nodes, leader_idx) = start_group().await;
    let leader = nodes[leader_idx].clone();

    let all = async {
        let mut handles = Vec::new();
        for c in 0..24u32 {
            let leader = leader.clone();
            handles.push(tokio::spawn(async move {
                for r in 0..10u32 {
                    let key = concurrent_key(c, r);
                    let value = format!("v{c}-{r}").into_bytes();
                    txn_write_then_confirm(&leader, key, value).await;
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
