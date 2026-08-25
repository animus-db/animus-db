//! The **in-place split fork** (ADR 0058 Train 2 rung 3, Stage 3):
//! `KvCommand::SplitTablet` mints two children's identities atomically into
//! the parent's own log — reusing `Freeze`'s whole-range seal discipline
//! verbatim for the ordering fence, plus a durable fork-specific marker
//! (`split.rs`) the host reconciler discovers via `pending_split()`. These
//! cells prove the mint's own apply-path mechanics (fence + durable
//! payload + idempotency + restart survival); the reconciler's
//! materialization of the two children (clone/trim/bootstrap) is covered by
//! `tests/inplace_split_reconciler.rs`.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()`.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{KIND_BASE, RaftKvNode};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{SplitChild, TabletId};
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

fn leader(nodes: &[KvNode], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

fn test_children() -> [SplitChild; 2] {
    [
        SplitChild {
            id: TabletId(2),
            replicas: vec![nid(10), nid(11), nid(12)],
        },
        SplitChild {
            id: TabletId(3),
            replicas: vec![nid(13), nid(14), nid(15)],
        },
    ]
}

/// Propose the fork on the leader and drive until every replica has applied
/// it (`is_frozen()` on all three, `pending_split()` answering the same
/// payload everywhere).
fn fork_and_settle(
    sim: &mut Simulator,
    nodes: &[KvNode],
    l: usize,
    split_key: &[u8],
    children: &[SplitChild; 2],
    seed: u64,
) {
    match nodes[l].propose_split_tablet(split_key.to_vec(), children.clone()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("split-tablet not accepted: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_frozen(),
            "replica {i} did not latch frozen after the fork applied (seed={seed})"
        );
        let pending = block_on(n.pending_split()).unwrap_or_else(|| {
            panic!("replica {i} does not see the fork payload (seed={seed})")
        });
        assert_eq!(pending.split_key, split_key, "seed={seed}");
        assert_eq!(pending.children, *children, "seed={seed}");
        // Every replica captures the SAME bootstrap voter set (the parent's
        // own config+learners at the fork position — see `split.rs`'s
        // module doc) — here, a plain 3-voter group with no learners added,
        // so it's exactly the group's own membership.
        assert_eq!(
            pending.bootstrap_voters,
            NODES.iter().copied().map(nid).collect(),
            "replica {i} captured the wrong bootstrap voter set (seed={seed})"
        );
    }
}

/// After the fork applies: every later-ordered mutating command is a
/// deterministic no-op on every replica (the SAME whole-range seal
/// `Freeze` uses), while a linearizable read of the pre-fork state still
/// serves — mirroring `freeze.rs`'s own regression, proving the two
/// workflows share the identical apply-time backstop.
#[test]
fn split_tablet_rejects_every_later_mutation_and_reads_keep_serving() {
    let seed = 0x5713_0001;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);
    let children = test_children();

    match nodes[l].put(b"pre".to_vec(), b"v1".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("pre-fork put rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    fork_and_settle(&mut sim, &nodes, l, b"m", &children, seed);

    match nodes[l].put(b"post".to_vec(), b"v2".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("post-fork put not appended: {other:?} (seed={seed})"),
    }
    match nodes[l].cas(b"cpost".to_vec(), None, b"cv".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("post-fork cas not appended: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"post")),
            None,
            "replica {i} applied a post-fork put (seed={seed})"
        );
        assert_eq!(
            block_on(n.local_get(b"cpost")),
            None,
            "replica {i} applied a post-fork cas (seed={seed})"
        );
    }
    assert_eq!(
        block_on(nodes[l].local_get(b"pre")),
        Some(b"v1".to_vec()),
        "a forked leader must keep serving reads (seed={seed})"
    );
}

/// Consumer bookkeeping (a cursor row) still applies on a forked group, the
/// same USER-data-only contract `Freeze` gives — a forked parent's
/// pre-cutover backlog can still drain if the deployment ever needs it.
#[test]
fn consumer_bookkeeping_still_applies_after_a_fork() {
    let seed = 0x5713_0002;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);
    fork_and_settle(&mut sim, &nodes, l, b"m", &test_children(), seed);

    match nodes[l].put_kind_batch_conditioned(
        vec![(
            animus_cp_data::KIND_CURSOR,
            b"cursor-row".to_vec(),
            Some(b"wm".to_vec()),
        )],
        Vec::new(),
        Vec::new(),
    ) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("cursor write not appended: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    assert_eq!(
        block_on(nodes[l].local_get_kind(animus_cp_data::KIND_CURSOR, b"cursor-row")),
        Some(b"wm".to_vec()),
        "a forked group must accept consumer-bookkeeping writes (seed={seed})"
    );
}

/// A duplicate `SplitTablet` (a crashed leader/reconciler re-proposing
/// without checking first) applies as a no-op — the marker is written once,
/// `pending_split()` still answers the original payload.
#[test]
fn split_tablet_is_idempotent() {
    let seed = 0x5713_0003;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);
    let children = test_children();

    fork_and_settle(&mut sim, &nodes, l, b"m", &children, seed);
    let first = block_on(nodes[l].pending_split()).expect("forked");

    // A duplicate re-propose: accepted at the log level (a fresh entry), but
    // applies as a no-op against the already-forked marker/seal.
    fork_and_settle(&mut sim, &nodes, l, b"m", &children, seed);
    let second = block_on(nodes[l].pending_split()).expect("still forked");

    assert_eq!(
        first.split_key, second.split_key,
        "a duplicate fork must not rewrite the marker's payload (seed={seed})"
    );
    assert_eq!(first.children, second.children, "seed={seed}");
    assert_eq!(
        first.ts, second.ts,
        "the marker's own ts must stay the FIRST fork's (seed={seed})"
    );
}

/// The fork survives a genuine process restart via its engine-durable
/// marker (mirrors `freeze.rs::freeze_survives_restart_via_the_durable_marker`):
/// the restarted group refuses new mutations from its first post-recovery
/// propose, and `pending_split()` still answers the original children.
#[test]
fn split_tablet_survives_restart_via_the_durable_marker() {
    let seed = 0x5713_0004;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let id = nid(0);
    let children = test_children();

    let node: KvNode = RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], engine.clone());
    sim.run_for(Duration::from_secs(2)); // elect (single voter)

    match node.put(b"pre".to_vec(), b"v1".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("pre-fork put rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    match node.propose_split_tablet(b"m".to_vec(), children.clone()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("split-tablet not accepted: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    assert!(node.is_frozen(), "not forked before restart (seed={seed})");

    sim.stop(id.clone());
    let restarted: KvNode =
        RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], engine.clone());
    sim.run_for(Duration::from_secs(2)); // recovery + re-election

    assert!(
        restarted.is_frozen(),
        "the restarted group must re-latch frozen from the durable seal marker (seed={seed})"
    );
    let pending = block_on(restarted.pending_split())
        .expect("the restarted group must re-derive the fork payload from its own durable marker");
    assert_eq!(pending.split_key, b"m");
    assert_eq!(pending.children, children);

    match restarted.put(b"post".to_vec(), b"v2".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("post-restart put not appended: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        block_on(restarted.local_get(b"post")),
        None,
        "a restarted forked group applied a mutation (seed={seed})"
    );
    assert_eq!(
        block_on(restarted.local_get(b"pre")),
        Some(b"v1".to_vec()),
        "a restarted forked group must keep serving reads (seed={seed})"
    );
}

/// `pending_split()` answers `None` on an ordinary, never-forked group —
/// the common case every reconciler tick must cheaply skip past.
#[test]
fn pending_split_is_none_on_an_ordinary_group() {
    let seed = 0x5713_0005;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    for n in &nodes {
        assert_eq!(block_on(n.pending_split()), None, "seed={seed}");
    }
    assert!(!nodes[leader(&nodes, seed)].is_frozen());
}

/// A copy-based freeze and an in-place fork are mutually exclusive per
/// tablet in production (selected by the operator's split-mode flag), but
/// the mechanism itself must not silently misreport one as the other: a
/// plain `Freeze`d group has NO fork payload — `pending_split()` stays
/// `None` even though `is_frozen()` is `true`.
#[test]
fn a_plain_freeze_carries_no_fork_payload() {
    let seed = 0x5713_0006;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);
    match nodes[l].propose_freeze() {
        ProposeResult::Accepted { .. } => {}
        other => panic!("freeze not accepted: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    assert!(nodes[l].is_frozen(), "seed={seed}");
    assert_eq!(
        block_on(nodes[l].pending_split()),
        None,
        "a plain Freeze must not be misread as a fork (seed={seed})"
    );
}

/// Sanity: `KIND_BASE` writes pre-fork are ordinary base rows (guards the
/// fixture itself, not the mechanism — a regression here would mean the
/// harness stopped exercising `KIND_BASE`, silently narrowing every other
/// cell in this file).
#[test]
fn pre_fork_base_write_lands_before_the_fork() {
    let seed = 0x5713_0007;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);
    match nodes[l].put_kind_batch_conditioned(
        vec![(KIND_BASE, b"row".to_vec(), Some(b"v".to_vec()))],
        Vec::new(),
        Vec::new(),
    ) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("pre-fork kind batch rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        block_on(nodes[l].local_get_kind(KIND_BASE, b"row")),
        Some(b"v".to_vec()),
        "seed={seed}"
    );
}
