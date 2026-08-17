//! The split-cutover **freeze** (ADR 0050 Train B rung 5, stage 3):
//! `KvCommand::Freeze` is the terminal, whole-range descendant of the range
//! seal — after it applies, the parent group rejects every later-ordered
//! mutating command (its apply pushes a whole-range entry into the same
//! sealed set every fence+seal gate consults, and persists the identical
//! durable marker so a restarted/compacted group stays frozen), while
//! linearizable reads keep serving (the frozen state IS current until
//! cutover). Teeth note: `Freeze` is a NEW command, so these cells are
//! structural specifications, not red-on-old-code regressions — the
//! behavioral red→green teeth for rung 5 live in `animusd`'s
//! `tests/split_build.rs` e2e, which fails without the endgame wiring.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()`.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{KIND_BASE, RaftKvNode};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::KeyRange;
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

/// Propose the freeze on the leader and drive until every replica has
/// applied it (`is_frozen()` on all three).
fn freeze_and_settle(sim: &mut Simulator, nodes: &[KvNode], l: usize, seed: u64) {
    match nodes[l].propose_freeze() {
        ProposeResult::Accepted { .. } => {}
        other => panic!("freeze not accepted: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_frozen(),
            "replica {i} did not latch frozen after the Freeze applied (seed={seed})"
        );
    }
}

/// After `Freeze` applies: every later-ordered mutating command — plain put,
/// kind batch, CAS, seed batch, txn stage — is a deterministic no-op on
/// every replica, while a linearizable read of the pre-freeze state still
/// serves. The one whole-range seal covers them all (zero per-command
/// gating).
#[test]
fn freeze_rejects_every_later_mutation_and_reads_keep_serving() {
    let seed = 0xF0_0001;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);

    match nodes[l].put(b"pre".to_vec(), b"v1".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("pre-freeze put rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    freeze_and_settle(&mut sim, &nodes, l, seed);

    // Plain put: no-op everywhere (the whole-range seal, not the fence —
    // the fence passes, since the key is inside the declared range).
    match nodes[l].put_fenced(b"post".to_vec(), b"v2".to_vec(), KeyRange::whole()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("post-freeze put not appended: {other:?} (seed={seed})"),
    }
    // Kind batch (base + a change record): no-op.
    match nodes[l].put_kind_batch_fenced(
        vec![(KIND_BASE, b"kpost".to_vec(), Some(b"kv".to_vec()))],
        vec![(b"kpost".to_vec(), b"rec".to_vec())],
        Vec::new(),
        KeyRange::whole(),
    ) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("post-freeze kind batch not appended: {other:?} (seed={seed})"),
    }
    // CAS (expected absent, would otherwise succeed): decided false.
    match nodes[l].cas(b"cpost".to_vec(), None, b"cv".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("post-freeze cas not appended: {other:?} (seed={seed})"),
    }
    // Seed batch (never legitimately directed at a parent): dropped whole.
    match nodes[l].propose_seed_batch(
        vec![(KIND_BASE, b"spost".to_vec(), Some(b"sv".to_vec()), 42)],
        KeyRange::whole(),
    ) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("post-freeze seed batch not appended: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    // (A frozen `TxnStage`/`TxnResolve` no-ops through the IDENTICAL
    // `is_sealed` gate the cells above exercise — their apply arms AND the
    // whole-range seal share one check (`lib.rs`'s stage/resolve fence
    // buckets), so a separate async-staged cell here would only re-prove
    // the same gate while deadlocking `block_on` against the undriven sim
    // (the crate's documented gotcha); the transactional behavior across a
    // real freeze→cutover is `animusd`'s `split_build.rs` e2e's job.)

    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"post")),
            None,
            "replica {i} applied a post-freeze put (seed={seed})"
        );
        assert_eq!(
            block_on(n.local_get_kind(KIND_BASE, b"kpost")),
            None,
            "replica {i} applied a post-freeze kind batch (seed={seed})"
        );
        assert_eq!(
            block_on(n.local_get(b"cpost")),
            None,
            "replica {i} applied a post-freeze cas (seed={seed})"
        );
        assert_eq!(
            block_on(n.local_get_kind(KIND_BASE, b"spost")),
            None,
            "replica {i} applied a post-freeze seed batch (seed={seed})"
        );
    }

    // Reads keep serving the frozen (current) state (`local_get`: the
    // leader's applied engine view — a `block_on(linearizable_get)` would
    // deadlock against the undriven sim, the crate's documented gotcha;
    // the real linearizable read path over a frozen parent is exercised by
    // `animusd`'s e2e).
    assert_eq!(
        block_on(nodes[l].local_get(b"pre")),
        Some(b"v1".to_vec()),
        "a frozen leader must keep serving reads (seed={seed})"
    );
}

/// The freeze's contract is USER data (base/LSI): a pure consumer-
/// bookkeeping batch — a cursor row, a footprint, a change-log-only entry
/// (the backfill seeder's shape) — still applies on a frozen group, so the
/// GSI drain/backfill seeder can finish draining the frozen parent and
/// release the rung-5 cutover vetoes (without this the vetoes deadlock
/// against the freeze — caught red by `backfill_seeder`'s revived
/// split-during-backfill e2e).
#[test]
fn consumer_bookkeeping_still_applies_on_a_frozen_group() {
    let seed = 0xF0_0004;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);
    freeze_and_settle(&mut sim, &nodes, l, seed);

    // A cursor-kind write (the GSI drain's own bookkeeping shape).
    match nodes[l].put_kind_batch_fenced(
        vec![(
            animus_cp_data::KIND_CURSOR,
            b"cursor-row".to_vec(),
            Some(b"wm".to_vec()),
        )],
        Vec::new(),
        Vec::new(),
        KeyRange::whole(),
    ) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("cursor write not appended: {other:?} (seed={seed})"),
    }
    // A change-log-only batch (the backfill seeder's synthetic record).
    match nodes[l].put_kind_batch_fenced(
        Vec::new(),
        vec![(
            b"\x00\x00\x00\x00\x00\x00\x00\x02pk".to_vec(),
            b"rec".to_vec(),
        )],
        Vec::new(),
        KeyRange::whole(),
    ) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("record-only write not appended: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    assert_eq!(
        block_on(nodes[l].local_get_kind(animus_cp_data::KIND_CURSOR, b"cursor-row")),
        Some(b"wm".to_vec()),
        "a frozen group must accept consumer-bookkeeping writes (seed={seed})"
    );
    let changes = block_on(nodes[l].pending_changes());
    assert_eq!(
        changes.len(),
        1,
        "the change-log-only batch must land its record (seed={seed})"
    );
}

/// A duplicate `Freeze` applies as a no-op — the driver may re-propose
/// after a crash/re-lead without checking first, and the group stays
/// frozen and serving.
#[test]
fn freeze_is_idempotent() {
    let seed = 0xF0_0002;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);

    match nodes[l].put(b"pre".to_vec(), b"v1".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("pre-freeze put rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    freeze_and_settle(&mut sim, &nodes, l, seed);
    // Again — the duplicate must append and apply harmlessly.
    freeze_and_settle(&mut sim, &nodes, l, seed);

    assert_eq!(
        block_on(nodes[l].local_get(b"pre")),
        Some(b"v1".to_vec()),
        "a doubly-frozen leader must still serve (seed={seed})"
    );
}

/// The freeze survives a genuine process restart via its engine-durable
/// whole-range seal marker (the `sealed`-set rebuild re-latches
/// `is_frozen()`), even though the WAL tail may replay: the restarted
/// group refuses new mutations from its first post-recovery propose.
#[test]
fn freeze_survives_restart_via_the_durable_marker() {
    let seed = 0xF0_0003;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let id = nid(0);

    let node: KvNode = RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], engine.clone());
    sim.run_for(Duration::from_secs(2)); // elect (single voter)

    match node.put(b"pre".to_vec(), b"v1".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("pre-freeze put rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    match node.propose_freeze() {
        ProposeResult::Accepted { .. } => {}
        other => panic!("freeze not accepted: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    assert!(node.is_frozen(), "not frozen before restart (seed={seed})");

    // Genuine restart: same WAL (Env disk), same engine handle (stands in
    // for a durable engine surviving a crash) — `witnessing.rs`'s pattern.
    sim.stop(id.clone());
    let restarted: KvNode =
        RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], engine.clone());
    sim.run_for(Duration::from_secs(2)); // recovery + re-election

    assert!(
        restarted.is_frozen(),
        "the restarted group must re-latch frozen from the durable marker (seed={seed})"
    );
    match restarted.put_fenced(b"post".to_vec(), b"v2".to_vec(), KeyRange::whole()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("post-restart put not appended: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        block_on(restarted.local_get(b"post")),
        None,
        "a restarted frozen group applied a mutation (seed={seed})"
    );
    assert_eq!(
        block_on(restarted.local_get(b"pre")),
        Some(b"v1".to_vec()),
        "a restarted frozen group must keep serving reads (seed={seed})"
    );
}
