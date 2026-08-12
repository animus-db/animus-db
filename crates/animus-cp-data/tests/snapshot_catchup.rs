//! Stage A.2 (ADR 0017): a lagging follower catches up via a **streaming
//! `InstallSnapshot`** carrying the leader's **engine image**. After the leader
//! compacts (snapshots the engine + truncates the Raft log prefix), a replica
//! that missed the writes can no longer be caught up by `AppendEntries` (the log
//! is gone), so the leader ships the engine image; the follower writes it into
//! its own engine and then replays the log tail on top.
//!
//! `snapshot_catchup_carries_txn_records_and_intents` (ADR 0018 §2/PR3)
//! extends this to the txn record/intent machinery: since a txn record and
//! its intents are ordinary in-scope logical keys (unlike the engine-global
//! seal/ceiling markers), they ship through `engine_image` exactly like any
//! other data — no special-casing needed, and this test is the proof.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{escape, partition_token};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn leader(nodes: &[KvNode], live: &[usize], seed: u64) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(i, n)| live.contains(i) && n.is_leader())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among {live:?}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

#[test]
fn lagging_follower_catches_up_via_snapshot() {
    let seed = 0x5A0;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect
    let l = leader(&nodes, &[0, 1, 2], seed);
    let lagging = (0..3).find(|&i| i != l).expect("a follower exists");

    // Crash the lagging follower (so it stays at its old term — no rejoin churn).
    // The surviving two are still a majority.
    sim.crash(nid(lagging as u64));

    // Write well past the compaction threshold (64) so the leader snapshots and
    // truncates the log prefix the crashed follower would have needed.
    const N: u64 = 150;
    for i in 0..N {
        match nodes[l].put(
            format!("k{i:03}").into_bytes(),
            format!("v{i}").into_bytes(),
        ) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected put {i}: {other:?} (seed={seed})"),
        }
    }
    sim.run_for(Duration::from_secs(3)); // replicate + apply + compact on {l, third}

    // Restart the lagging follower. Its log is far behind the leader's compacted
    // base, so the leader must catch it up with an InstallSnapshot (engine image),
    // then replay the post-snapshot log tail on top.
    sim.restart(nid(lagging as u64));
    sim.run_for(Duration::from_secs(6));

    // The recovered follower's engine converged to every write (sample the range).
    for i in [0u64, 1, 64, 100, N - 1] {
        let key = format!("k{i:03}").into_bytes();
        assert_eq!(
            block_on(nodes[lagging].local_get(&key)),
            Some(format!("v{i}").into_bytes()),
            "follower {lagging} missing k{i:03} after snapshot catch-up (seed={seed})"
        );
    }
}

/// Run `fut` to completion by spawning it and driving `sim`, returning
/// `None` if it didn't complete within `budget` — needed for `txn_stage`
/// (its `wait_applied` poll waits on `env.sleep`, so a bare `block_on`
/// would hang: nothing else would ever advance the simulated clock).
fn drive<T: Send + 'static>(
    sim: &mut Simulator,
    env: &SimEnv,
    budget: Duration,
    fut: impl Future<Output = T> + Send + 'static,
) -> Option<T> {
    let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let s = Arc::clone(&slot);
    env.clone().spawn_task(async move {
        let v = fut.await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock().unwrap().take()
}

/// ADR 0018 §2/PR3: a txn record and its intents are ordinary in-scope
/// logical keys (unlike the engine-global seal/ceiling markers) — they must
/// ship through `engine_image`/`InstallSnapshot` exactly like any other
/// data, with no special-casing. Stages (but never decides) a transaction
/// before the compacting write burst, then confirms a snapshot-caught-up
/// follower's raw engine holds the identical still-`Pending` intent
/// envelope and record bytes the leader has — and that resolving the
/// transaction afterward converges normally on that same follower.
#[test]
fn snapshot_catchup_carries_txn_records_and_intents() {
    let seed = 0x7C57;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect
    let l = leader(&nodes, &[0, 1, 2], seed);
    let lagging = (0..3).find(|&i| i != l).expect("a follower exists");

    sim.crash(nid(lagging as u64));

    // A real ADR 0022-shaped key: `partition_token(pk) || escape(pk) || rk`
    // — `txn_stage`'s anchor-token disjointness proof (`txn.rs`) assumes
    // every key leads with the 8-byte token.
    let staged_key = {
        let mut out = partition_token(b"acct").to_vec();
        out.extend_from_slice(&escape(b"acct"));
        out.extend_from_slice(b"balance");
        out
    };
    let n = nodes[l].clone();
    let kk = staged_key.clone();
    let (txn_id, record_key) = drive(
        &mut sim,
        nodes[l].env(),
        Duration::from_secs(5),
        async move {
            n.txn_stage("t", vec![(kk, Some(b"staged-value".to_vec()))])
                .await
        },
    )
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage did not complete (seed={seed})"));

    // Past the compaction threshold (64), so the leader snapshots +
    // truncates the log prefix the crashed follower would have needed —
    // same shape as `lagging_follower_catches_up_via_snapshot`.
    const N: u64 = 150;
    for i in 0..N {
        match nodes[l].put(
            format!("k{i:03}").into_bytes(),
            format!("v{i}").into_bytes(),
        ) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected put {i}: {other:?} (seed={seed})"),
        }
    }
    sim.run_for(Duration::from_secs(3)); // replicate + apply + compact on {l, third}

    sim.restart(nid(lagging as u64));
    sim.run_for(Duration::from_secs(6));

    // The staged key is still covered by a `Pending` intent everywhere —
    // including the just-caught-up follower, via its `InstallSnapshot`
    // image, not the (already-truncated) log tail. `local_get` reports a
    // `Pending` intent as absent (its documented, non-blocking-peek
    // contract), so confirm the *raw* stored bytes instead: tag `1`
    // (`Envelope::Intent`, `txn.rs`), never a bare/undecorated value.
    let raw = block_on(nodes[lagging].storage().get(&staged_key))
        .expect("engine read ok")
        .unwrap_or_else(|| panic!("follower {lagging} missing the staged intent (seed={seed})"));
    assert_eq!(
        raw.value.first().copied(),
        Some(1u8),
        "follower {lagging}'s snapshot-caught-up copy of the staged key must still be \
         an intent envelope (tag 1), not a bare value or absent (seed={seed})"
    );

    // Resolving the transaction now converges normally on every replica,
    // including the one that only ever learned of it via the snapshot.
    let n = nodes[l].clone();
    let kk = staged_key.clone();
    let decided = drive(
        &mut sim,
        nodes[l].env(),
        Duration::from_secs(5),
        async move { n.txn_decide(txn_id, record_key, vec![kk], true).await },
    )
    .flatten();
    assert!(
        decided.is_some(),
        "commit txn_decide must complete (seed={seed})"
    );
    sim.run_for(Duration::from_secs(3));

    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&staged_key)),
            Some(b"staged-value".to_vec()),
            "node {i}: the transaction must resolve to its committed value everywhere, \
             including the snapshot-caught-up follower (seed={seed})"
        );
    }
}
