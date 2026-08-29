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
//!
//! **Harness note (issue #278 item 6): the load-generation helpers below must
//! be election-tolerant, independently of the property above.** 24 concurrent
//! client tasks hammering a real 3-node group is itself enough CPU contention
//! on a busy CI runner to blow the ADR 0017 driver's election timeout (the
//! same starved-consensus-loop shape `propose_ordered` was built around), so
//! a mid-test election is expected, not exceptional. The original harness got
//! this wrong two ways: (1) it resolved the leader **once** at test start and
//! every client task kept hammering that one handle for the rest of the run,
//! so a deposed leader's `put`/`txn_write` calls degrade to `NotLeader` (or a
//! panic) forever after; (2) it treated `put() -> Accepted{index}` plus
//! `engine_applied_index() >= index` as proof the write **committed** — but
//! `Accepted` only ever means "appended to the leader's own log," never
//! "committed" (see the root `CLAUDE.md`'s standing lesson on this). After an
//! election, the deposed leader's uncommitted entry is truncated and the new
//! leader's own entries re-occupy that same index, so the index-advance wait
//! passed while the write never actually landed — and the subsequent
//! `linearizable_get` correctly returned `None`, which is the
//! "did not return what was just put (left: None)" failure this item fixes.
//! `put_then_confirm`/`txn_write_then_confirm` now take the whole `nodes`
//! slice, re-resolve the current leader on every propose attempt and every
//! confirm read, and confirm a write only by reading the expected value back
//! (never by index-advance alone) — retrying the whole put/txn_write on a
//! stale/absent confirmation, since retrying an already-landed write at the
//! same key/value is idempotent. All of it is a bounded
//! converged-or-timeout loop, never a fixed one-shot wait.

// ADR 0003 / ADR 0061 Decision 4 (rung B5): a real-thread ProdEnv regression
// (see the module doc above) — the race under test is structurally
// unreachable under SimEnv's cooperative single-thread scheduler, so real
// time/threads/spawn are the point here, not a determinism hole.
#![allow(
    clippy::disallowed_methods,
    reason = "real-thread ProdEnv regression (the race is unreachable under SimEnv's cooperative scheduler, see module doc); ADR 0061 Decision 4"
)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Env, NodeId, ProdEnv, nid};
use animus_storage::MemoryEngine;
use animus_tablet::{escape, partition_token};
use tokio::time::{Instant, sleep, timeout};

type KvNode = RaftKvNode<ProdEnv, MemoryEngine>;

/// Per-operation deadline for `put_then_confirm`/`txn_write_then_confirm` —
/// generous enough to absorb several elections' worth of retries under heavy
/// contention, but still a bounded converged-or-timeout budget rather than an
/// unbounded retry loop.
const OP_DEADLINE: Duration = Duration::from_secs(20);
/// How long a single propose attempt is given to confirm (by reading the
/// value back) before falling back to re-resolving the leader and retrying
/// the whole propose — covers the case where the entry we just appended gets
/// truncated by an election before it commits.
const CONFIRM_WINDOW: Duration = Duration::from_secs(3);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

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

/// Re-resolve whichever node currently believes it's the leader, bounded by
/// `deadline` — never cached across calls, since a mid-test election can
/// depose any previously-resolved leader at any time (see the module doc).
async fn current_leader(nodes: &[KvNode], deadline: Instant) -> KvNode {
    loop {
        if let Some(n) = nodes.iter().find(|n| n.is_leader()) {
            return n.clone();
        }
        assert!(
            Instant::now() < deadline,
            "no leader found before the op deadline (repeated elections?)"
        );
        sleep(POLL_INTERVAL).await;
    }
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
    let book: BTreeMap<NodeId, String> = envs
        .iter()
        .map(|e| (e.node_id(), e.local_addr().to_string()))
        .collect();
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

/// Put `key`/`value` through the group's current leader and confirm the
/// write actually **committed** by reading it back — never by index-advance
/// alone, since `Accepted { index }` only means "appended to the leader's
/// own log," not "committed" (see the module doc). Election-tolerant: both
/// the propose and the confirm read re-resolve the current leader on every
/// attempt, and a stale/absent confirmation retries the whole put (sound,
/// since re-putting the same key/value is idempotent).
async fn put_then_confirm(nodes: &[KvNode], key: Vec<u8>, value: Vec<u8>) {
    let deadline = Instant::now() + OP_DEADLINE;
    loop {
        let leader = current_leader(nodes, deadline).await;
        if matches!(
            leader.put(key.clone(), value.clone()),
            ProposeResult::Accepted { .. }
        ) {
            let confirm_deadline = std::cmp::min(deadline, Instant::now() + CONFIRM_WINDOW);
            loop {
                let reader = current_leader(nodes, deadline).await;
                if reader.linearizable_get(&key).await.as_deref() == Some(value.as_slice()) {
                    return;
                }
                if Instant::now() >= confirm_deadline {
                    break; // fall through to re-resolve the leader and retry the put
                }
                sleep(POLL_INTERVAL).await;
            }
        }
        assert!(
            Instant::now() < deadline,
            "put_then_confirm for {key:?} did not converge within {OP_DEADLINE:?} \
             (repeated leader churn, or the apply task panicked — assert_ts_monotonic, see stderr)"
        );
    }
}

/// `txn_write` + a confirm-by-read-back, the txn analogue of
/// `put_then_confirm` (ADR 0018 §2/PR3), extending this file's coverage to
/// `TxnStage`/`TxnCommit`/`TxnResolve`, each of which mints and proposes its
/// own `ts` through the identical `propose_ordered`/`propose_ordered_aux`
/// critical section a plain `put` does. `txn_write` already returns `None`
/// (rather than panicking) if the leader stepped down mid-transaction, so
/// that case falls through to the same re-resolve-and-retry loop
/// `put_then_confirm` uses — the whole write is idempotent to retry (same
/// key/value), so a truncated attempt is always safe to redo against
/// whichever node is leader now.
async fn txn_write_then_confirm(nodes: &[KvNode], key: Vec<u8>, value: Vec<u8>) {
    let deadline = Instant::now() + OP_DEADLINE;
    loop {
        let leader = current_leader(nodes, deadline).await;
        let committed = leader
            .txn_write("t", vec![(key.clone(), Some(value.clone()))])
            .await
            .is_some();
        if committed {
            let confirm_deadline = std::cmp::min(deadline, Instant::now() + CONFIRM_WINDOW);
            loop {
                let reader = current_leader(nodes, deadline).await;
                if reader.linearizable_get(&key).await.as_deref() == Some(value.as_slice()) {
                    return;
                }
                if Instant::now() >= confirm_deadline {
                    break; // fall through to re-resolve the leader and retry txn_write
                }
                sleep(POLL_INTERVAL).await;
            }
        }
        assert!(
            Instant::now() < deadline,
            "txn_write_then_confirm for {key:?} did not converge within {OP_DEADLINE:?} \
             (repeated leader churn, or the apply task panicked — assert_ts_monotonic, see stderr)"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writes_and_reads_never_violate_ts_monotonicity() {
    let (nodes, _leader_idx) = start_group().await;

    // Many concurrent client tasks, each its own real OS-scheduled tokio task
    // (the parallelism this race needs), each doing several put+get round
    // trips against distinct keys. High enough concurrency that, against the
    // unfixed code, the mint/propose interleaving window reliably gets hit —
    // and, on a contended runner, high enough to reliably trigger a mid-test
    // election too, which is exactly what `put_then_confirm` must tolerate.
    let all = async {
        let mut handles = Vec::new();
        for c in 0..24u32 {
            let nodes = nodes.clone();
            handles.push(tokio::spawn(async move {
                for r in 0..10u32 {
                    let key = format!("k{c}-{r}").into_bytes();
                    let value = format!("v{c}-{r}").into_bytes();
                    put_then_confirm(&nodes, key, value).await;
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
    let (nodes, _leader_idx) = start_group().await;

    let all = async {
        let mut handles = Vec::new();
        for c in 0..24u32 {
            let nodes = nodes.clone();
            handles.push(tokio::spawn(async move {
                for r in 0..10u32 {
                    let key = concurrent_key(c, r);
                    let value = format!("v{c}-{r}").into_bytes();
                    txn_write_then_confirm(&nodes, key, value).await;
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
