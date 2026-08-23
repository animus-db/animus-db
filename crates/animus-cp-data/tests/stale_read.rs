//! ADR 0055: **eventually-consistent reads** — the `ConsistentRead: false`
//! read primitives (`stale_read_ready`/`stale_get_served`/`stale_scan`),
//! proven at the level the DynamoDB edge cannot: deterministically, on a
//! named follower, with a real unresolved transaction intent in the way.
//!
//! Three properties, each the reason a piece of the design exists:
//!
//! 1. **A follower serves.** No ReadIndex barrier, no leadership — the whole
//!    point. Contrast `read_index.rs`'s
//!    `linearizable_scan_returns_sorted_live_range`, whose follower arm
//!    asserts the opposite for the strong read.
//! 2. **An intent reads back one version, never as absent** (point read and
//!    scan alike). ADR 0055 §3: an eventual read may be stale, but it may
//!    never fabricate a deletion of an item that exists — which is exactly
//!    what `local_get`'s raw peek would report here.
//! 3. **The freshness gate refuses a replica that has heard from no leader.**
//!    ADR 0055 §2: an engine that is not yet *any* state of this tablet must
//!    not answer, or a whole populated tablet reads as empty.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()`. The stale reads themselves never `await` a sleep — that is
//! their defining property — so `block_on` is correct for them, and using it
//! deliberately doubles as a *proof* of it: a stale read that ever grew an
//! internal wait would hang this file rather than silently costing what a
//! strong read costs.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{escape, partition_token};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
const ELECT: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_secs(2);

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

fn leader(nodes: &[KvNode], seed: u64) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(_, n)| n.is_leader())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

/// A real ADR 0022-shaped data-plane key: `partition_token(pk) ||
/// escape(pk) || rk` — the layout the txn machinery's anchor-token
/// disjointness proof assumes, so a staged intent needs it.
fn key(pk: &[u8], rk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out.extend_from_slice(rk);
    out
}

/// Spawn `fut` on `env` and drive `sim` until it completes (or `budget`
/// elapses) — needed for anything awaiting `env.sleep`, which here is only
/// the transaction staging, never a stale read.
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

fn put(nodes: &[KvNode], l: usize, key: &[u8], value: &[u8]) {
    assert!(matches!(
        nodes[l].put(key.to_vec(), value.to_vec()),
        ProposeResult::Accepted { .. }
    ));
}

/// Every **follower** of the group, by index.
fn followers(l: usize) -> Vec<usize> {
    (0..NODES.len()).filter(|&i| i != l).collect()
}

#[test]
fn a_follower_serves_an_eventual_read_with_no_barrier_and_no_leadership() {
    let seed = 0x5741_1E01;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);

    for (k, v) in [(b"k1", b"v1"), (b"k2", b"v2"), (b"k3", b"v3")] {
        put(&nodes, l, k, v);
    }
    sim.run_for(SETTLE);

    for f in followers(l) {
        assert!(
            nodes[f].stale_read_ready(),
            "follower {f} should be caught up and leader-aware after settling (seed={seed})"
        );
        // The point read. `block_on` (not `drive`) is deliberate: a stale
        // read must never await a timer — see this file's own doc.
        assert_eq!(
            block_on(nodes[f].stale_get_served(b"k2")),
            Some(Some(b"v2".to_vec())),
            "follower {f} must serve an eventual read of a committed key (seed={seed})"
        );
        // A genuinely absent key is `Some(None)` — served, and absent —
        // never the outer `None`, which means "not served at all".
        assert_eq!(
            block_on(nodes[f].stale_get_served(b"nope")),
            Some(None),
            "follower {f}: an absent key is a served absence (seed={seed})"
        );
        // The scan, over the same half-open range semantics `local_scan`
        // and `linearizable_scan` use.
        assert_eq!(
            block_on(nodes[f].stale_scan(b"k1", Some(b"k3"), None)),
            vec![
                (b"k1".to_vec(), b"v1".to_vec()),
                (b"k2".to_vec(), b"v2".to_vec()),
            ],
            "follower {f} must serve an eventual scan (seed={seed})"
        );
        // Descending, with the limit keeping the *highest* rows.
        assert_eq!(
            block_on(nodes[f].stale_scan_rev(b"k0", Some(b"k9"), Some(2))),
            vec![
                (b"k3".to_vec(), b"v3".to_vec()),
                (b"k2".to_vec(), b"v2".to_vec()),
            ],
            "follower {f} must serve a descending eventual page (seed={seed})"
        );
    }
}

#[test]
fn an_unresolved_intent_reads_back_one_version_never_as_absent() {
    let seed = 0x5741_1E02;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);

    let k = key(b"acct-9", b"balance");
    put(&nodes, l, &k, b"committed");
    sim.run_for(SETTLE);

    // Stage an overwrite and deliberately never decide it: every replica now
    // holds an `Envelope::Intent` over `k` with the committed value one MVCC
    // version below it.
    let n = nodes[l].clone();
    let kk = k.clone();
    let staged = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        n.txn_stage("t", vec![(kk, Some(b"staged".to_vec()))]).await
    })
    .flatten();
    assert!(staged.is_some(), "txn_stage must complete (seed={seed})");
    sim.run_for(SETTLE);

    for (i, n) in nodes.iter().enumerate() {
        // The baseline this exists to beat: the raw peek reports the
        // intent-covered key as ABSENT, which for a client-visible read
        // would be a fabricated deletion of an item that exists.
        assert_eq!(
            block_on(n.local_get(&k)),
            None,
            "node {i}: `local_get`'s raw peek still reports a pending intent as \
             absent — the behavior ADR 0055 §3 must not inherit (seed={seed})"
        );
        // The eventual read falls back one version instead: stale, and
        // genuinely committed.
        assert_eq!(
            block_on(n.stale_get_served(&k)),
            Some(Some(b"committed".to_vec())),
            "node {i}: an eventual read of an intent-covered key must return the \
             last committed value, never the staged one and never absent (seed={seed})"
        );
        // Same rule row-by-row inside a scan: the row must not vanish from
        // the page, and the staged bytes must not appear in it.
        assert_eq!(
            block_on(n.stale_scan(&partition_token(b"acct-9"), None, None)),
            vec![(k.clone(), b"committed".to_vec())],
            "node {i}: an eventual scan must keep the intent-covered row at its \
             last committed value (seed={seed})"
        );
    }
}

#[test]
fn a_replica_that_has_heard_from_no_leader_refuses_to_serve() {
    let seed = 0x5741_1E03;
    let sim = Simulator::new(seed);
    // Started, never driven: no election has happened, so no node has a
    // leader and every engine is empty. This is the shape of a freshly
    // placed voter (ADR 0029 rebalance) before its first `AppendEntries` —
    // the one case where serving locally would report a populated tablet as
    // entirely absent.
    let nodes: Vec<KvNode> = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            !n.stale_read_ready(),
            "node {i} must refuse an eventual read before it knows any leader \
             (seed={seed})"
        );
    }

    // Once the group elects, every node passes — the gate is a startup
    // guard, not a permanent restriction.
    let mut sim = sim;
    sim.run_for(ELECT);
    sim.run_for(SETTLE);
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.stale_read_ready(),
            "node {i} must serve eventual reads once the group has a leader and \
             it is caught up (seed={seed})"
        );
    }
}
