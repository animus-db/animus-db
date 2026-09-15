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
        scenario!(
            "cell_e_a_tolerated_halted_failure_leaves_no_phantom_group_tails_entry",
            scenario_e_tolerated_failure_leaves_no_phantom_entry
        ),
        scenario!(
            "cell_f_a_sync_only_failure_never_lets_a_siblings_later_success_launder_it",
            scenario_f_sync_only_failure_leaves_no_buffered_phantom
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

// ---------------------------------------------------------------------------
// Cell (e) (issue #838): a tolerated (halted-gated) LIVE write failure for
// one tablet must never leave a phantom `group_tails` entry a SIBLING
// tablet's own later, perfectly healthy compaction could durably write out.
//
// A DIFFERENT fault shape from cells (b)-(d) above: those use
// `Simulator::crash` (+ `torn_tail_on_crash`/`corrupt_on_crash`) — a genuine
// process crash, which the per-record CRC32 + torn-tail tolerance (issue
// #495) already handles at the file-decode layer. This cell never crashes
// anything: `persist_wal`'s own `halted`-gated tolerance
// (`crates/animus-cp-data/src/lib.rs`) is what's under test — the process
// keeps running, `shutdown()` races an in-flight `SharedWal::append_tagged`
// round, and the round's own physical write fails. Before the fix, the
// `group_tails` mutation `append_tagged` makes at ENQUEUE time (well before
// the physical write is even attempted) survived that tolerated failure
// unrolled-back; this cell proves it no longer does, by making a HEALTHY
// sibling's own ordinary `compact_group` rewrite (which re-serializes every
// OTHER tablet's cached tail verbatim) run right afterward and checking
// what actually lands in the reopened file.
// ---------------------------------------------------------------------------

fn scenario_e_tolerated_failure_leaves_no_phantom_entry(seed: u64) {
    let t_a = 1u64;
    let t_b = 2u64;
    let node = nid(NODE);

    let sim = Simulator::new(seed);
    let shared = open_shared(&sim);
    let a = host(&sim, t_a, &shared);
    let b = host(&sim, t_b, &shared);
    let mut sim = sim;
    sim.run_for(SETTLE);

    // A's own last CONFIRMED-durable write, laid down before any fault is
    // armed — this is the state a correct recovery must land on.
    put(&a, seed, b"a-confirmed", b"1");
    sim.run_for(SETTLE);
    assert_eq!(get(&a, b"a-confirmed"), Some(b"1".to_vec()));

    // Shut A down — mirrors the issue's own trigger exactly:
    // `RaftKvNode::shutdown()` (ordinary graceful teardown, e.g. the
    // reconciler moving this tablet off, or a whole-node shutdown) sets
    // `halted` on a process that keeps running, no crash involved.
    a.shutdown();
    assert!(
        a.is_halted(),
        "A must be halted before its doomed round below (seed={seed})"
    );

    // Arm the disk to fail every subsequent op, then submit A's own doomed
    // round DIRECTLY against `SharedWal` — exactly the call `persist_wal`
    // itself would have made had A's driver still been running (`shared.
    // append_tagged(env, wal, tablet, &records)`, `crates/animus-cp-data/
    // src/lib.rs`). Calling it here, on A's now-halted node's own `Env`
    // handle, reproduces the physical outcome precisely — `append_tagged`
    // mutates `group_tails[t_a]` SYNCHRONOUSLY, inside the same critical
    // section that enqueues the op (issue #838's own root cause), well
    // before `env.append` ever runs — while keeping the fault shape clean:
    // `error_prob(1.0)` fails `env.append` INSTANTLY, before it ever
    // buffers a single byte, so there is no leftover unsynced buffer for a
    // LATER, unrelated caller's own successful `sync` to durable-ize out
    // from under this test — a real but separate hazard this cell
    // deliberately does not conflate with the one under test here (see
    // `DiskConfig::set_sync_delay`'s own doc on `append`/`sync` ordering).
    // `persist_wal`'s own `assert!(halted, ..)` is exactly this same
    // tolerated-iff-halted gate; asserted directly above rather than
    // re-driven through the full consensus-loop/select! machinery, which
    // buys nothing further once `SharedWal`'s own contract is what's under
    // test.
    let mut failing = DiskConfig::default();
    failing.set_error_prob(1.0);
    sim.set_disk_config_for(node.clone(), failing);
    let phantom_record = animus_control::WalRecord::Append(animus_control::LogEntry {
        index: 3,
        term: 1,
        command: KvCommand::Put {
            key: b"a-phantom".to_vec(),
            value: b"2".to_vec(),
            ts: animus_cp_data::hlc::HlcTimestamp {
                wall_ms: 4_000,
                logical: 0,
            },
        },
        config: None,
        learners: None,
    });
    let doomed = block_on(shared.append_tagged(
        &sim.env(node.clone()),
        SHARED_WAL,
        TabletId(t_a),
        &[phantom_record],
    ));
    assert!(
        doomed.is_err(),
        "A's own doomed round must actually fail against the armed disk (seed={seed})"
    );

    // Heal the disk before B's own healthy compaction runs — B must never
    // see an injected fault of its own; only A's round ever failed.
    sim.set_disk_config_for(node.clone(), DiskConfig::default());

    // Tablet B, healthy throughout, churns well past `COMPACT_THRESHOLD`
    // (64) so its own apply task triggers at least one REAL
    // `SharedWal::compact_group` rewrite — the whole-file rewrite that
    // (pre-fix) durably writes A's phantom entry to disk as a side effect
    // of B's own perfectly ordinary, successful compaction.
    let before = shared.physical_write_count();
    for i in 0..120u32 {
        put(&b, seed, format!("b-{i}").as_bytes(), b"x");
        if i % 20 == 19 {
            sim.run_for(Duration::from_millis(300));
        }
    }
    sim.run_for(SETTLE);
    assert!(
        shared.physical_write_count() > before,
        "tablet B's own churn must have produced real physical SharedWal activity, \
         including at least one compaction rewrite (seed={seed})"
    );
    // B's own tail, as this coordinator's in-memory cache holds it right
    // now — the baseline the post-reopen check below must reproduce
    // EXACTLY, byte for byte, regardless of A's own unrelated failure.
    let live_recovered_b = block_on(shared.recovered_state(TabletId(t_b)));
    for i in 0..120u32 {
        assert_eq!(
            get(&b, format!("b-{i}").as_bytes()),
            Some(b"x".to_vec()),
            "tablet B's own churn must land live, before any restart (seed={seed})"
        );
    }

    // Tear both drivers down (A is already halted; B gets a clean graceful
    // stop) and reopen the shared file completely from scratch — this
    // proves the PHYSICAL FILE, not any in-memory cache, holds only what
    // was actually confirmed durable.
    a.shutdown();
    b.shutdown();
    sim.run_for(Duration::from_millis(200));
    drop(a);
    drop(b);

    let reopened = open_shared(&sim);
    let recovered_a = block_on(reopened.recovered_state(TabletId(t_a)));
    // 2, not 1: a single-voter group's own leader-election no-op (index 1)
    // plus the one CONFIRMED-durable `Put` (index 2) — never a third entry
    // for the phantom write whose own physical round failed while halted.
    assert_eq!(
        recovered_a.log.len(),
        2,
        "tablet A's recovered log must contain exactly its leader-election no-op plus its \
         one CONFIRMED-durable entry, never the phantom write whose own physical round \
         failed while halted, even though a sibling's later, healthy compaction rewrote \
         the whole shared file in between (seed={seed}, recovered={:?})",
        recovered_a.log
    );

    // B's own tail must reopen BYTE-FOR-BYTE identical to what it was
    // right before the restart — proving A's rollback (and the sibling
    // compaction rewrite it ran alongside) touched nothing of B's own
    // record set. Checked directly against the recovered `PersistedState`
    // rather than by rehosting a THIRD `RaftKvNode` for B and reading back
    // through a fresh engine: B has genuinely compacted past
    // `COMPACT_THRESHOLD` here, and a single-voter group (no peer to source
    // a catch-up `InstallSnapshot` from) has no way to recover a compacted
    // prefix behind a freshly-constructed engine — the same pre-existing,
    // documented structural limitation cell (d) above deliberately avoids
    // for the identical reason (see that cell's own closing comment). That
    // limitation is orthogonal to this cell's own property (A's tolerated
    // failure must never disturb B's own record set) and already fully
    // established by the byte-identical comparison below, with no
    // engine-backed reconstruction needed.
    let recovered_b = block_on(reopened.recovered_state(TabletId(t_b)));
    assert_eq!(
        format!("{:?}", recovered_b.log),
        format!("{:?}", live_recovered_b.log),
        "tablet B's own recovered log must reopen byte-for-byte identical to what it was \
         right before the restart — a sibling's tolerated (halted-gated) failure and its \
         own rollback must never disturb an unrelated tablet's record set (seed={seed})"
    );
    assert!(
        recovered_b
            .log
            .iter()
            .any(|e| matches!(&e.command, KvCommand::Put { key, .. } if key == b"b-119")),
        "tablet B's own LAST write must still be present in the reopened log \
         (seed={seed}, recovered_b={:?})",
        recovered_b.log
    );

    // A's own restart-and-replay: unlike B, A never crossed
    // `COMPACT_THRESHOLD` (its whole confirmed history is two log entries),
    // so rehosting it fresh and reading back through the engine is exactly
    // what a real restart does, with no such limitation to avoid.
    let a2 = host(&sim, t_a, &reopened);
    sim.run_for(SETTLE);
    assert_eq!(get(&a2, b"a-confirmed"), Some(b"1".to_vec()));
    assert_eq!(
        get(&a2, b"a-phantom"),
        None,
        "a write whose own physical round failed while halted must never resurrect after \
         a restart, even though a sibling tablet's own healthy compaction ran in between \
         (seed={seed})"
    );
}

// ---------------------------------------------------------------------------
// Cell (f): issue #883 — a tolerated failure whose own `env.append` already
// buffered real bytes before its own `env.sync` fails must never leave those
// bytes for a LATER, unrelated caller's own successful `sync` to durably
// launder. Distinct from cell (e): that cell deliberately fails `env.append`
// itself (`DiskConfig::set_error_prob(1.0)`, checked before a single byte is
// ever buffered) to isolate issue #838's `group_tails` mechanism in
// isolation, per the engineering-lessons entry that names this exact,
// deliberately-not-covered hazard. This cell exercises the complementary
// shape: let `env.append` succeed and genuinely buffer bytes, and fail only
// the FOLLOWING `env.sync` — the shape a real fsync failure (or an ordinary
// teardown race landing between the two calls) actually takes on a real
// filesystem, where a successful `write()` is not retroactively un-written
// by a failed `fsync()`.
// ---------------------------------------------------------------------------

fn scenario_f_sync_only_failure_leaves_no_buffered_phantom(seed: u64) {
    let t_a = 1u64;
    let t_b = 2u64;
    let node = nid(NODE);

    let sim = Simulator::new(seed);
    let shared = open_shared(&sim);
    let a = host(&sim, t_a, &shared);
    let b = host(&sim, t_b, &shared);
    let mut sim = sim;
    sim.run_for(SETTLE);

    // A's own last CONFIRMED-durable write, laid down before any fault is
    // armed — the state a correct recovery must land on, and (crucially for
    // this cell) what leaves the shared file's buffered region genuinely
    // EMPTY going into the doomed round below: every `flush` call only ever
    // returns having fully synced or having already failed and been
    // repaired, so a clean prior write is what makes "buffered is empty
    // right before the doomed round's own `append`" true.
    put(&a, seed, b"a-confirmed", b"1");
    sim.run_for(SETTLE);
    assert_eq!(get(&a, b"a-confirmed"), Some(b"1".to_vec()));

    // Shut A down — the same ordinary, non-crashing teardown trigger cell
    // (e) uses (`RaftKvNode::shutdown()`, e.g. the reconciler moving this
    // tablet off, or a whole-node shutdown): a process that keeps running,
    // no crash involved.
    a.shutdown();
    assert!(
        a.is_halted(),
        "A must be halted before its doomed round below (seed={seed})"
    );

    // Arm `DiskConfig::set_sync_error_prob(1.0)` — unlike `set_error_prob`
    // (which cell (e) uses, and which fires uniformly on EVERY disk op),
    // this fails ONLY the `sync` op: `env.append` below genuinely succeeds
    // and buffers `phantom_record`'s bytes, exactly the shape a real fsync
    // failure takes (a successful `write()` is not retroactively un-written
    // by a failed `fsync()`) — and, crucially, it leaves a coordinator-level
    // repair's own immediate follow-up `read`/`replace` (run with no
    // scheduling boundary before it, so a test cannot "heal" the disk in
    // between) free to succeed, exactly like the real, non-crashing
    // teardown race this whole tolerated-failure path exists for: the
    // failing `sync` is a local, one-off hazard, not a fully dead disk.
    let mut failing = DiskConfig::default();
    failing.set_sync_error_prob(1.0);
    sim.set_disk_config_for(node.clone(), failing);

    let phantom_record = animus_control::WalRecord::Append(animus_control::LogEntry {
        index: 3,
        term: 1,
        command: KvCommand::Put {
            key: b"a-phantom-sync".to_vec(),
            value: b"2".to_vec(),
            ts: animus_cp_data::hlc::HlcTimestamp {
                wall_ms: 4_000,
                logical: 0,
            },
        },
        config: None,
        learners: None,
    });
    let doomed = block_on(shared.append_tagged(
        &sim.env(node.clone()),
        SHARED_WAL,
        TabletId(t_a),
        &[phantom_record],
    ));
    assert!(
        doomed.is_err(),
        "A's own doomed round must actually fail once its sync is armed to fail (seed={seed})"
    );

    // Heal the disk before B's own healthy round runs — B must never see an
    // injected fault of its own; only A's round's `sync` ever failed.
    sim.set_disk_config_for(node.clone(), DiskConfig::default());

    // Tablet B, healthy throughout, does one perfectly ORDINARY round — no
    // need to cross `COMPACT_THRESHOLD` here at all, since this hazard lives
    // at the raw `env.append`/`env.sync` level, not `group_tails`: a plain
    // `append_tagged` call is exactly what extends the shared file's
    // buffered region (with A's leftover phantom bytes still in it, absent
    // the fix) and then durably commits the whole thing via its own
    // successful `sync`.
    put(&b, seed, b"b-ordinary", b"x");
    sim.run_for(SETTLE);
    assert_eq!(
        get(&b, b"b-ordinary"),
        Some(b"x".to_vec()),
        "tablet B's own ordinary round must land live, before any restart (seed={seed})"
    );

    // Tear both drivers down and reopen the shared file completely from
    // scratch — this proves the PHYSICAL FILE, not any in-memory cache,
    // holds only what was actually confirmed durable.
    a.shutdown();
    b.shutdown();
    sim.run_for(Duration::from_millis(200));
    drop(a);
    drop(b);

    let reopened = open_shared(&sim);
    let recovered_a = block_on(reopened.recovered_state(TabletId(t_a)));
    // 2, not 3: a single-voter group's own leader-election no-op (index 1)
    // plus the one CONFIRMED-durable `Put` (index 2) — never a third entry
    // for the phantom write whose own `sync` failed while halted, even
    // though B's own later, healthy round physically extended the SAME file
    // (and successfully synced it) right afterward.
    assert_eq!(
        recovered_a.log.len(),
        2,
        "tablet A's recovered log must contain exactly its leader-election no-op plus its \
         one CONFIRMED-durable entry, never the phantom write whose own `sync` failed while \
         halted, even though a sibling's later, healthy ordinary round physically extended \
         and re-synced the SAME shared file right afterward (seed={seed}, recovered={:?})",
        recovered_a.log
    );
    assert!(
        recovered_a
            .log
            .iter()
            .all(|e| !matches!(&e.command, KvCommand::Put { key, .. } if key == b"a-phantom-sync")),
        "the phantom write must never appear in tablet A's recovered log, laundered in by \
         tablet B's own successful sync (seed={seed}, recovered={:?})",
        recovered_a.log
    );

    // B's own write must have landed durably, completely unaffected by A's
    // unrelated tolerated failure and its own repair.
    let recovered_b = block_on(reopened.recovered_state(TabletId(t_b)));
    assert!(
        recovered_b
            .log
            .iter()
            .any(|e| matches!(&e.command, KvCommand::Put { key, .. } if key == b"b-ordinary")),
        "tablet B's own write must still be present in the reopened log (seed={seed}, \
         recovered_b={:?})",
        recovered_b.log
    );

    // A's own restart-and-replay, through a real rehosted `RaftKvNode`.
    let a2 = host(&sim, t_a, &reopened);
    sim.run_for(SETTLE);
    assert_eq!(get(&a2, b"a-confirmed"), Some(b"1".to_vec()));
    assert_eq!(
        get(&a2, b"a-phantom-sync"),
        None,
        "a write whose own `sync` failed while halted must never resurrect after a restart, \
         even though a sibling tablet's own healthy ordinary round physically extended and \
         re-synced the SAME shared file right afterward (seed={seed})"
    );
}
