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
//!
//! `snapshot_catchup_reseeds_hot_change_max` (issue #859 follow-up) extends
//! this again to `RaftKvNode::hot_change_max`: unlike a fresh
//! `start_inner` boot, an `InstallSnapshot` replaces an ALREADY-RUNNING
//! replica's engine content wholesale, so this cache needs its own
//! re-seed at that same point — this test is the proof that it gets one.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{KIND_BASE, RaftKvNode, StorageScope, hlc};
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{KeyRange, escape, partition_token};
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
    let (txn_id, record_key, _outcome) = drive(
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
    let raw = block_on(
        nodes[lagging]
            .storage()
            .get(&nodes[lagging].physical_key(animus_cp_data::KIND_BASE, &staged_key)),
    )
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

/// A real ADR 0022-shaped logical key: `partition_token(pk) || escape(pk) ||
/// rk` (mirrors `tests/kind_batch.rs`'s identical helper).
fn logical(pk: &[u8], rk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out.extend_from_slice(rk);
    out
}

/// Like [`group`], but scoped to a real `KeyRange` rather than
/// `StorageScope::whole()`. `pending_changes`/`hot_change_max`'s own boot
/// scan reads as empty for `StorageScope::whole()` ("only
/// `StorageScope::whole()`; no real tablet" — that method's own doc), so a
/// test that needs genuine `KIND_CHANGE` records must use this instead,
/// mirroring `tests/kind_batch.rs`'s own `group`.
fn group_scoped(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start_scoped(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                StorageScope::new(KeyRange::whole()),
            )
        })
        .collect();
    (sim, nodes)
}

/// Drive `sim` in small ticks, repeatedly (re-)arming a transfer to `target`
/// on whoever currently leads, until `target` itself reports leadership or
/// the budget runs out — mirrors `tests/hlc_differential_skew.rs`'s
/// identical `force_leadership` helper (a single `transfer_leadership` call
/// only *arms* the handoff; it still needs ticks of virtual time for
/// `TimeoutNow` to actually land).
fn force_leadership(sim: &mut Simulator, nodes: &[KvNode], target: usize, seed: u64) {
    for _ in 0..200 {
        if nodes[target].is_leader() {
            return;
        }
        let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
        if ls.len() == 1 && ls[0] != target {
            nodes[ls[0]].transfer_leadership(nid(target as u64));
        }
        sim.run_for(Duration::from_millis(50));
    }
    panic!("could not force leadership onto node {target} (seed={seed})");
}

/// The `(HlcTimestamp, ordinal)` a `KIND_CHANGE` record's own logical key
/// suffix encodes (`prefix || hlc::pack(ts) || ordinal`,
/// `materialize_derived`'s doc, issue #852) — the identical decode
/// `animus-cp-data`'s own `decode_change_suffix`/`animusd::index_drain::
/// record_hlc_ordinal` perform, reproduced here by hand (rather than
/// exported) so this test's own ground truth is independent of
/// `hot_change_max`'s own machinery, not just a re-invocation of it.
fn decode_change_suffix(key: &[u8]) -> Option<(hlc::HlcTimestamp, u32)> {
    let n = key.len().checked_sub(12)?;
    let ts = hlc::unpack(u64::from_be_bytes(key[n..n + 8].try_into().ok()?));
    let ordinal = u32::from_be_bytes(key[n + 8..].try_into().ok()?);
    Some((ts, ordinal))
}

/// Issue #859 follow-up: `hot_change_max` must be re-seeded when
/// `InstallSnapshot` replaces an already-running replica's engine content
/// wholesale, not just at a fresh `start_inner` boot — otherwise a replica
/// that caught up this way keeps a cache reflecting only what IT locally
/// materialized before falling behind (too low), and if it later becomes
/// this group's leader, `GetShardIterator{LATEST}` would return a too-low
/// position and re-deliver records that already existed.
///
/// Writes real `KIND_CHANGE` records on the leader, past the compaction
/// threshold so the lagging follower must catch up via a genuine
/// engine-image install (not a log replay), then confirms the caught-up
/// follower's own `hot_change_max()` matches the leader's own true
/// maximum (found by directly decoding `pending_changes()`, independent of
/// `hot_change_max` itself — the ground truth). Then forces leadership
/// onto that follower and re-confirms: the shape that actually matters for
/// `GetShardIterator{LATEST}`, since only a group's LEADER ever serves it.
#[test]
fn snapshot_catchup_reseeds_hot_change_max() {
    let seed = 0x8590;
    let (mut sim, nodes) = group_scoped(seed);
    sim.run_for(Duration::from_secs(2)); // elect
    let l = leader(&nodes, &[0, 1, 2], seed);
    let lagging = (0..3).find(|&i| i != l).expect("a follower exists");

    sim.crash(nid(lagging as u64));

    // Past the compaction threshold (64), so the leader snapshots +
    // truncates the log prefix the crashed follower would have needed —
    // same shape as `lagging_follower_catches_up_via_snapshot`.
    const N: u64 = 150;
    for i in 0..N {
        let base = logical(format!("k{i:03}").as_bytes(), b"");
        match nodes[l].put_kind_batch(
            vec![(KIND_BASE, base.clone(), Some(format!("v{i}").into_bytes()))],
            vec![(base, format!("rec{i}").into_bytes())],
        ) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected kind batch {i}: {other:?} (seed={seed})"),
        }
        sim.run_for(Duration::from_millis(20));
    }
    sim.run_for(Duration::from_secs(3)); // replicate + apply + compact on {l, third}

    // Ground truth, taken from the leader BEFORE the follower's restart —
    // every one of `N` writes minted exactly one change record, so this is
    // never empty.
    let expected_max = block_on(nodes[l].pending_changes_key_order())
        .iter()
        .filter_map(|(k, _)| decode_change_suffix(k))
        .max()
        .unwrap_or_else(|| panic!("leader has no KIND_CHANGE records (seed={seed})"));
    assert_eq!(
        block_on(nodes[l].hot_change_max()),
        Some(expected_max),
        "leader's own hot_change_max must already match the ground truth (seed={seed})"
    );

    // Restart the lagging follower — its log is far behind the leader's
    // compacted base, so the leader must catch it up with an
    // InstallSnapshot (engine image), then replay the post-snapshot log
    // tail on top.
    sim.restart(nid(lagging as u64));
    sim.run_for(Duration::from_secs(6));

    assert_eq!(
        block_on(nodes[lagging].hot_change_max()),
        Some(expected_max),
        "follower {lagging}'s hot_change_max must be re-seeded by the InstallSnapshot, \
         not left at whatever it locally materialized before falling behind (seed={seed})"
    );

    // The shape that actually matters: only a LEADER's own cache is ever
    // read by `GetShardIterator{LATEST}`.
    force_leadership(&mut sim, &nodes, lagging, seed);
    assert_eq!(
        block_on(nodes[lagging].hot_change_max()),
        Some(expected_max),
        "the now-leading, snapshot-caught-up follower {lagging} must still report the \
         true max, not a too-low value that would re-deliver already-existing records \
         through GetShardIterator{{LATEST}} (seed={seed})"
    );
}
