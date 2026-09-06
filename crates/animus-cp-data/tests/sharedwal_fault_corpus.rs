//! C-05 PR 2 (ADR 0028) — `SimEnv` fault-injection corpus for `SharedWal`
//! wired into the CP-data persist path (`persist_wal`'s append coalescing,
//! `apply_and_compact`'s compaction rewrite, `host::Reconciler::forget`'s
//! teardown reclaim). Companion to `animus-control::shared_wal`'s own unit
//! tests (which prove the coordinator's queue/cache mechanics in isolation)
//! and `benches/wal_fsync_bench.rs` (which measured the coalescing win on
//! real disk, C-05 PR 1) — this corpus proves the SAME mechanism wired into
//! real `RaftKvNode` groups, under `SimEnv` fault injection.
//!
//! **Depth knob**: `ANIMUS_SHAREDWAL_SEEDS` (default 1), the house
//! `corpus::seed_expand` convention. Every seed is deterministic (`SimEnv`,
//! ADR 0003); a scenario's own seed is a stable hash of its name
//! (`corpus::name_seed`), so `ANIMUS_SEED=<seed>` replays any failure
//! exactly.
//!
//! **Harness shape**: every scenario hosts one or more single-voter
//! `RaftKvNode` groups on ONE physical node id (the shape that has anything
//! to coalesce/GC/isolate — sharing a WAL across nodes isn't this
//! mechanism's concern, cross-node replication already has its own
//! corpora), each on its own `stream` (= its `TabletId`), sharing one
//! `Arc<SharedWal<KvCommand, KvState>>` built via `SharedWal::open`. A
//! single-voter group is deliberately used throughout (`all_nodes =
//! [node]`, `start_hosted_campaigning_with_batcher_and_shared_wal` for an
//! instant, deterministic leader) — cross-tablet coalescing/GC/isolation is
//! what this corpus tests, not cross-replica replication, which the
//! existing `raftkv_linearizable`/`reconciler_corpus` corpora already
//! cover thoroughly with the (still-default-off) shared WAL untouched.
//!
//! **Scope note** (recorded honestly rather than silently short of the
//! design doc's full cell list): this corpus does not re-run the
//! `raftkv_linearizable` `LeaderKill`/`FollowerKill` harness with the flag
//! on (cell (e) of the design) — that harness's own fixture builds
//! per-group `RaftKvNode`s with no shared-WAL construction seam at all, and
//! wiring one in is a larger, separately-scoped harness change. What IS
//! covered here (cells (a)-(d)) exercises the identical `persist_wal`/
//! `apply_and_compact`/`host::Reconciler::forget` code paths that harness's
//! own groups would call if it were extended — the fault matrix below (a
//! genuine `Simulator::crash` with `torn_tail_on_crash`/`corrupt_on_crash`
//! armed, mid-round) is the same class of fault that harness's own
//! `Nemesis::LeaderKill`/`FollowerKill` cells inject, just via `crash`
//! directly rather than through that harness's specific nemesis vocabulary.

use std::sync::Arc;
use std::time::Duration;

use animus_control::{ProposeResult, SharedWal};
use animus_cp_data::{KvCommand, KvState, RaftKvNode, SHARED_WAL, StorageScope};
use animus_env::nid;
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::TabletId;
use animus_test::corpus::{self, SeedVariant};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;
type Wal = SharedWal<KvCommand, KvState>;

/// The one physical node id every scenario hosts on — this mechanism is
/// entirely about several tablets sharing one node's own persist path.
const NODE: u64 = 0;

/// Comfortably past a single-voter group's own instant campaign-and-commit
/// (no real election round trip needed) and past the apply task's
/// `APPLY_SAFETY_POLL`, so a settle window always converges regardless of
/// seed-derived timing.
const SETTLE: Duration = Duration::from_secs(2);

fn host(sim: &Simulator, stream: u64, shared: &Arc<Wal>) -> KvNode {
    RaftKvNode::start_hosted_campaigning_with_batcher_and_shared_wal(
        sim.env(nid(NODE)),
        vec![nid(NODE)],
        MemoryEngine::new(),
        StorageScope::whole(),
        stream,
        None,
        Some(Arc::clone(shared)),
    )
}

fn open_shared(sim: &Simulator) -> Arc<Wal> {
    block_on(Wal::open(&sim.env(nid(NODE)), SHARED_WAL)).expect("shared WAL opens")
}

fn put(node: &KvNode, seed: u64, key: &[u8], value: &[u8]) {
    match node.put(key.to_vec(), value.to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected a put: {other:?} (seed={seed})"),
    }
}

fn get(node: &KvNode, key: &[u8]) -> Option<Vec<u8>> {
    block_on(node.local_get(key))
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

/// Depth knob (`ANIMUS_SHAREDWAL_SEEDS`, default 1) — mirrors every other
/// corpus's `ANIMUS_<X>_SEEDS` convention (root `CLAUDE.md`'s knob table).
fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_SHAREDWAL_SEEDS")
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
            "cell_a_bursty_writes_across_groups_coalesce_fsyncs",
            scenario_a_coalescing
        ),
        scenario!(
            "cell_b_crash_mid_round_preserves_durable_prefix_no_cross_contamination",
            scenario_b_crash_mid_round
        ),
        scenario!(
            "cell_c_forget_reclaims_a_torn_down_tablet_without_disturbing_siblings",
            scenario_c_forget_gc
        ),
        scenario!(
            "cell_d_a_quiet_groups_single_write_survives_a_churning_siblings_compaction",
            scenario_d_quiet_survives_sibling_compaction
        ),
    ]
}

fn corpus() -> Vec<Scenario> {
    corpus::seed_expand(scenario_cells(), seeds_per_cell())
}

#[test]
fn sharedwal_fault_corpus_names_are_unique() {
    let cells = scenario_cells();
    let names: std::collections::BTreeSet<&str> = cells.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names.len(), cells.len(), "duplicate scenario name");
}

#[test]
fn sharedwal_fault_corpus_runs_every_scenario() {
    for s in corpus() {
        (s.run)(s.seed);
    }
}

// ---------------------------------------------------------------------------
// Cell (a): a burst of one write to each of K co-hosted groups coalesces
// into far fewer than K physical `SharedWal` writes.
// ---------------------------------------------------------------------------

const CELL_A_GROUPS: u64 = 16;

fn scenario_a_coalescing(seed: u64) {
    let mut sim = Simulator::new(seed);
    let shared = open_shared(&sim);
    let nodes: Vec<KvNode> = (1..=CELL_A_GROUPS)
        .map(|stream| host(&sim, stream, &shared))
        .collect();
    // Let every group's own bootstrap (single-voter self-election, which
    // itself persists hard state) settle before measuring — the burst below
    // must be measured against a clean baseline, not conflated with K
    // bootstrap writes.
    sim.run_for(SETTLE);
    for n in &nodes {
        assert!(
            n.is_leader(),
            "every single-voter group must self-lead (seed={seed})"
        );
    }
    let baseline = shared.physical_write_count();

    // A small injected `append`/`sync` latency is what makes concurrent
    // physical writes genuinely OVERLAP under `SimEnv`'s single-threaded
    // cooperative scheduler — with zero latency, one group's own driver
    // task runs its whole `Poll::Ready`-only path to completion before the
    // scheduler ever gets to the next one, so nothing is ever queued behind
    // an in-flight leader (see `animus-control::shared_wal`'s own
    // `overlapping_appends_are_coalesced_into_one_physical_write` test,
    // which needs the identical forced-yield treatment for the same
    // reason). This models the real disk latency `wal_fsync_bench.rs`
    // measured naturally, under real concurrent OS threads.
    let mut cfg = DiskConfig::default();
    cfg.set_sync_delay(Duration::from_millis(20));
    sim.set_disk_config_for(nid(NODE), cfg);

    // The burst: one put to every group, issued back-to-back with no
    // intervening virtual time — mirrors `wal_fsync_bench.rs`'s own "one
    // write to each of K active groups" workload shape.
    for (i, n) in nodes.iter().enumerate() {
        put(n, seed, format!("k{i}").as_bytes(), b"v");
    }
    sim.run_for(SETTLE);

    let physical_writes = shared.physical_write_count() - baseline;
    println!(
        "cell (a) seed={seed}: {CELL_A_GROUPS} groups, burst of {CELL_A_GROUPS} writes \
         coalesced into {physical_writes} physical SharedWal write(s)"
    );
    assert!(
        physical_writes >= 1,
        "the burst must have produced at least one physical write (seed={seed})"
    );
    assert!(
        physical_writes < CELL_A_GROUPS,
        "a burst across {CELL_A_GROUPS} co-hosted groups must coalesce into fewer than \
         {CELL_A_GROUPS} physical writes (got {physical_writes}) — the whole point of \
         routing every group's persist_wal through one SharedWal (seed={seed})"
    );
    // And every write actually landed and applied, regardless of how many
    // physical rounds it took.
    sim.run_for(SETTLE);
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            get(n, format!("k{i}").as_bytes()),
            Some(b"v".to_vec()),
            "group {i}'s own burst write must have landed (seed={seed})"
        );
    }
}

// ---------------------------------------------------------------------------
// Cell (b): a crash mid-round (torn tail + corrupted trailing byte) recovers
// every group's own durable-before-crash prefix, never a torn or foreign
// record, and loses at most the not-yet-synced tail — never more.
// ---------------------------------------------------------------------------

fn scenario_b_crash_mid_round(seed: u64) {
    let t_a = 1u64;
    let t_b = 2u64;
    let node = nid(NODE);

    // Round 1: both groups write a baseline key each, fully settled and
    // durable BEFORE any fault is armed. Everything below runs on the SAME
    // `Simulator` — its disk is the one simulated world this whole
    // scenario's crash/recovery is about; a second `Simulator::new(seed)`
    // would be a brand-new, empty disk sharing nothing but the seed.
    let mut sim = Simulator::new(seed);
    let shared = open_shared(&sim);
    let a = host(&sim, t_a, &shared);
    let b = host(&sim, t_b, &shared);
    sim.run_for(SETTLE);
    put(&a, seed, b"baseline-a", b"1");
    put(&b, seed, b"baseline-b", b"1");
    sim.run_for(SETTLE);
    assert_eq!(get(&a, b"baseline-a"), Some(b"1".to_vec()));
    assert_eq!(get(&b, b"baseline-b"), Some(b"1".to_vec()));

    // Now arm a slow, tearable disk and crash mid-round: issue a SECOND
    // write to each group, then crash almost immediately — well before
    // `sync_delay` elapses, so the round's append/sync is still buffered
    // (unsynced) at the crash instant.
    let mut cfg = DiskConfig::default();
    cfg.set_sync_delay(Duration::from_millis(500));
    cfg.torn_tail_on_crash = true;
    cfg.corrupt_on_crash = true;
    sim.set_disk_config_for(node.clone(), cfg);
    put(&a, seed, b"racy-a", b"2");
    put(&b, seed, b"racy-b", b"2");
    sim.run_for(Duration::from_millis(20));
    sim.crash(node.clone());
    sim.stop(node.clone());
    sim.restart(node.clone()); // clears the crashed flag before rebuilding
    drop(a);
    drop(b);

    // Fresh reconstruction, on the SAME simulated disk, reading purely off
    // whatever the crash left behind (possibly torn/corrupted) with fresh
    // engines and a fresh `SharedWal::open` re-demux.
    let shared = open_shared(&sim);
    let a2 = host(&sim, t_a, &shared);
    let b2 = host(&sim, t_b, &shared);
    sim.run_for(SETTLE);

    // Durable-before-crash prefix: both baseline writes MUST have survived,
    // for BOTH groups, regardless of which group's round-2 write (if
    // either) happened to be the one torn.
    assert_eq!(
        get(&a2, b"baseline-a"),
        Some(b"1".to_vec()),
        "group A's durable-before-crash baseline write must survive a crash mid a LATER \
         round (seed={seed})"
    );
    assert_eq!(
        get(&b2, b"baseline-b"),
        Some(b"1".to_vec()),
        "group B's durable-before-crash baseline write must survive a crash mid a LATER \
         round (seed={seed})"
    );

    // No cross-contamination: neither group's own engine ever shows the
    // OTHER group's key, whatever the racy round's outcome.
    assert_eq!(
        get(&a2, b"baseline-b"),
        None,
        "group A must never see group B's key (seed={seed})"
    );
    assert_eq!(
        get(&a2, b"racy-b"),
        None,
        "group A must never see group B's racy key (seed={seed})"
    );
    assert_eq!(
        get(&b2, b"baseline-a"),
        None,
        "group B must never see group A's key (seed={seed})"
    );
    assert_eq!(
        get(&b2, b"racy-a"),
        None,
        "group B must never see group A's racy key (seed={seed})"
    );

    // The racy round-2 write, per group, is either fully present or fully
    // absent — never a torn/partial value (this WAL's records are whole
    // JSON lines, so "torn" can only ever mean "the whole line is gone,"
    // never a half-written value slipping through — the checksum framing,
    // issue #495, guarantees this).
    let a_racy = get(&a2, b"racy-a");
    assert!(
        a_racy == Some(b"2".to_vec()) || a_racy.is_none(),
        "group A's racy write must be whole-or-absent, never torn (got {a_racy:?}, seed={seed})"
    );
    let b_racy = get(&b2, b"racy-b");
    assert!(
        b_racy == Some(b"2".to_vec()) || b_racy.is_none(),
        "group B's racy write must be whole-or-absent, never torn (got {b_racy:?}, seed={seed})"
    );
    println!(
        "cell (b) seed={seed}: racy round survived as a={:?} b={:?} (either outcome is \
         correct — only wholeness/isolation/the durable baseline are asserted)",
        a_racy, b_racy
    );
}

// ---------------------------------------------------------------------------
// Cell (c): `forget` (the teardown-time GC primitive `host::Reconciler`
// calls) reclaims exactly the torn-down tablet's own bytes and never
// disturbs a sibling's.
// ---------------------------------------------------------------------------

fn scenario_c_forget_gc(seed: u64) {
    let t_gone = 1u64;
    let t_stays = 2u64;
    let sim = Simulator::new(seed);
    let shared = open_shared(&sim);
    let gone = host(&sim, t_gone, &shared);
    let stays = host(&sim, t_stays, &shared);
    let mut sim = sim;
    sim.run_for(SETTLE);
    put(&gone, seed, b"k", b"gone");
    put(&stays, seed, b"k", b"stays");
    sim.run_for(SETTLE);
    assert_eq!(get(&gone, b"k"), Some(b"gone".to_vec()));
    assert_eq!(get(&stays, b"k"), Some(b"stays".to_vec()));

    // Teardown, mirroring `host::Reconciler::erase_tablet_files`'s
    // shared-WAL branch exactly: shut the driver down, then forget the
    // tablet from the coordinator.
    gone.shutdown();
    sim.run_for(Duration::from_millis(200));
    block_on(shared.forget(&sim.env(nid(NODE)), SHARED_WAL, TabletId(t_gone)))
        .expect("forget succeeds");

    // Reopen from scratch: the physical file must no longer carry ANY trace
    // of the forgotten tablet, while the sibling's own data is untouched —
    // proven by rebuilding the sibling fresh off the reopened file.
    let reopened = open_shared(&sim);
    let recovered_gone = block_on(reopened.recovered_state(TabletId(t_gone)));
    assert!(
        recovered_gone.is_empty(),
        "a forgotten tablet must leave no trace in the physical file (seed={seed})"
    );
    let stays2 = host(&sim, t_stays, &reopened);
    sim.run_for(SETTLE);
    assert_eq!(
        get(&stays2, b"k"),
        Some(b"stays".to_vec()),
        "a sibling tablet's own data must survive another tablet's forget/GC untouched \
         (seed={seed})"
    );
}

// ---------------------------------------------------------------------------
// Cell (d): a quiet group's one-time write survives a churning sibling's
// own repeated real compaction (crossing `COMPACT_THRESHOLD`) — "no group's
// own GC waits on, or is disturbed by, another's."
// ---------------------------------------------------------------------------

fn scenario_d_quiet_survives_sibling_compaction(seed: u64) {
    let t_quiet = 1u64;
    let t_churn = 2u64;
    let sim = Simulator::new(seed);
    let shared = open_shared(&sim);
    let quiet = host(&sim, t_quiet, &shared);
    let churn = host(&sim, t_churn, &shared);
    let mut sim = sim;
    sim.run_for(SETTLE);

    put(&quiet, seed, b"only-write", b"still-here");
    sim.run_for(SETTLE);
    assert_eq!(get(&quiet, b"only-write"), Some(b"still-here".to_vec()));

    let before = shared.physical_write_count();
    // Churn well past `COMPACT_THRESHOLD` (64) so the churner's own apply
    // task triggers at least one REAL compaction rewrite while `quiet`'s
    // own task never runs again.
    for i in 0..120u32 {
        put(&churn, seed, format!("churn-{i}").as_bytes(), b"x");
        // Give the apply task room to actually merge + compact between
        // bursts rather than piling the whole 120 into one round.
        if i % 20 == 19 {
            sim.run_for(Duration::from_millis(300));
        }
    }
    sim.run_for(SETTLE);
    let after = shared.physical_write_count();
    assert!(
        after > before,
        "the churner must have produced real physical SharedWal activity (seed={seed})"
    );

    assert_eq!(
        get(&quiet, b"only-write"),
        Some(b"still-here".to_vec()),
        "an unrelated churning sibling's own repeated compaction/appends must never touch \
         a quiet group's own already-durable write (seed={seed})"
    );
    for i in 0..120u32 {
        assert_eq!(
            get(&churn, format!("churn-{i}").as_bytes()),
            Some(b"x".to_vec()),
            "the churner's own write {i} must have landed (seed={seed})"
        );
    }

    // Deliberately NOT restarted with a fresh (wiped) engine here, unlike
    // cells (b)/(c): the churner has genuinely compacted past
    // `COMPACT_THRESHOLD`, and a single-VOTER group (no peer to source a
    // catch-up `InstallSnapshot` from) has no way to recover a compacted
    // prefix behind a wiped engine — a pre-existing, documented structural
    // limitation of single-voter groups (`host.rs`'s own "Engine-loss
    // recovery" doc), orthogonal to this PR's own WAL mechanism and already
    // proven at the WAL layer alone by cell (b)'s restart (which never
    // crosses a compaction). Restarting with a wiped engine here would
    // conflate that unrelated limitation with what this cell is actually
    // testing; the property this cell exists to prove (a quiet group's
    // write survives an unrelated sibling's real compaction) is already
    // fully established by the assertions above, with no restart needed.
}
