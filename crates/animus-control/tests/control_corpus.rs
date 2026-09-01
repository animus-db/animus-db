//! A fault-injected, seed-reproducible **corpus for the control plane's own
//! machinery** — the ADR 0038 async apply task (including its real
//! crash-recovery path), the replicated schema catalog's exclusivity
//! guarantee (ADR 0013), the tablet-id/`RegisterNode` allocator-shaped
//! counters, and a genuine multi-chunk `InstallSnapshot` transfer under a
//! mid-transfer fault.
//!
//! `learner_corpus.rs` already covers the learner/membership-class fault
//! vocabulary at seed depth (ADR 0058 Train 1); this corpus is the sibling
//! that exercises everything else about `Metadata`/`RaftNode` under real
//! fault injection instead of the ~30 fixed-single-seed acceptance tests
//! this crate otherwise has. **Built as a 3-PR stacked series: PR① the
//! harness architecture + a baseline + a schema-catalog-race workload; PR②
//! an `AllocatorRace` workload (invariant #4), a `RegisterCas` workload
//! lifting `register_node_cas.rs`'s fixed-single-seed CAS proof into this
//! corpus's fault matrix, and a fuller fault vocabulary
//! (`Duplicate`/`FsyncLie`/`TornTail`); PR③ (this one, final) a
//! `StopRestart` nemesis (a REAL process restart, not a muted-and-resumed
//! crash), invariant #5 (apply-task liveness), a `SustainedChurn` workload
//! for a crash-timing sweep under real load, and two bespoke
//! chunked-snapshot-under-fault tests exercising a genuine multi-chunk
//! `InstallSnapshot` transfer composed with a mid-transfer fault** — see the
//! "Nemesis set" and "Chunked-snapshot-under-fault" sections below.
//!
//! **Harness shape**, deliberately mirroring
//! `crates/animus-test/tests/raftkv_linearizable.rs` (the flagship corpus in
//! this repo — read it first if this file is unfamiliar): a declarative
//! [`Scenario`] (name + seed + replica count + [`Workload`] + a scheduled
//! [`Nemesis`] list + an optional outage `window`), a [`Group`] that owns the
//! live `RaftNode` set and knows how to `apply`/`heal_all` each nemesis, and
//! a `run_scenario`/`assert_scenario_ok` pair the tests drive. **One
//! adaptation**: unlike raftkv's single generic client loop (every scenario
//! drives the identical single-key list-append workload), this plane's
//! interesting scenarios need genuinely different workload *shapes*
//! (concurrent schema proposers vs. plain no-contention churn) — so
//! `Scenario` carries a `workload: Workload` field selecting which
//! `spawn_*_workload` function the runner drives, the same pattern
//! `crates/animus-test/tests/txn_serializable.rs`'s own `Workload` struct
//! uses for its several read/write/rmw shapes.
//!
//! **What "correctness" means here — and why there's no `check_cycles`.**
//! Unlike the per-tablet KV plane (`animus-cp-data`, `raftkv_linearizable.rs`),
//! this plane has no client-visible read/write history to build an Elle
//! dependency graph over: a single Raft log total-orders every `MetaCommand`,
//! so the interesting property is **convergence + safety invariants**, not
//! serializability. Checks, asserted on every scenario (`assert_scenario_ok`):
//!
//! 1. **Convergence** — `nodes[i].metadata() == nodes[j].metadata()` for
//!    every pair of replicas, via a converged-or-timeout poll (mirroring
//!    raftkv's `CONVERGENCE_POLL_STEP`/`CONVERGENCE_BUDGET` exactly).
//! 2. **Durability** — an effect a proposer's own retry loop actually
//!    *confirmed* (read back, byte-identical, after proposing — never merely
//!    `ProposeResult::Accepted`, which only means "appended to the leader's
//!    log," see `ProposeResult`'s own doc) must still be present in the
//!    final converged state. Mirrors raftkv's ok/info confirm discipline,
//!    minus the `info`-recording machinery this plane doesn't need (a
//!    proposer that never confirms simply contributes nothing to this
//!    check, rather than needing an explicit indeterminate outcome
//!    recorded). Covers `SchemaRace`/`PlainChurn` effects, and (PR②)
//!    `AllocatorRace`'s confirmed tablet ids and `RegisterCas`'s confirmed
//!    registrations too.
//! 3. **Schema-catalog exclusivity** (a *safety* property, checked
//!    unconditionally on every scenario, fault or not) — for every table
//!    name two or more racers proposed, `MetaCommand::CreateTableSchema`'s
//!    apply-time semantics (`meta.rs`: rejects outright if a schema for the
//!    name already exists — **not** idempotent-on-identical the way
//!    `RegisterNode`'s CAS is; first-committer-wins, full stop) mean at most
//!    one racing schema can ever take effect. So on every replica: the
//!    table's final schema (if present) is byte-identical to exactly one of
//!    the racing proposals — never a hybrid — and it is never absent if any
//!    racer's proposal was ever durably confirmed by that racer's own retry
//!    loop.
//! 4. **Allocator injectivity** (PR②, a *safety* property, checked
//!    unconditionally) — `AllocatorRace`'s `check_allocator_injectivity`:
//!    every `TabletId` observed in any replica's tablet map at any sampled
//!    point in the run has a stable identity (never two different
//!    `CreateTablet`/`BeginSplitInPlace` calls assigned the same id), and every id
//!    a proposer's own confirm loop reported as applied is pairwise
//!    distinct. Sampled at every convergence poll AND every fault-schedule
//!    step (`Shared::sample_tablets`), not just the final state, so a
//!    transient double-assignment a later poll happens to "correct" is
//!    still caught.
//! 5. **`RegisterNode` CAS integrity** (PR②, a *safety* property, checked
//!    unconditionally) — `RegisterCas`'s `check_register_cas_integrity`,
//!    mirroring check 3's shape over `Metadata::node_addrs` instead of
//!    `Metadata::schemas`: for a node id two or more DIFFERING address books
//!    were ever attempted for (this workload's own deterministic
//!    differing-re-registration collision), the final address book (if
//!    present) on every replica must byte-match exactly one attempt, never
//!    a hybrid, and must be present if any attempt was ever confirmed.
//! 6. **Apply-task liveness / no-permanent-stall** (PR③, a *safety*
//!    property, checked unconditionally on EVERY scenario — cheap: two
//!    atomic reads per node) — `poll_apply_task_caught_up`: after
//!    convergence, `engine_applied_index()` must catch up to
//!    `commit_index()` on every live replica within the same
//!    converged-or-timeout budget check 1 uses. This is deliberately
//!    **not** the same property as check 1: a uniformly-stalled apply task
//!    (every replica stuck at the same stale-but-consistent `Metadata`)
//!    still looks "converged" to check 1, which only ever compares
//!    replicas against each other, never against the group's own
//!    `commit_index`. Especially relevant after `StopRestart` — see that
//!    nemesis's own doc. **No separate double-apply probe** — see the
//!    "Nemesis set" section's `StopRestart` entry for why checks 3/4 above
//!    already catch a double-apply if one ever happened.
//!
//! **Nemesis set**: `LeaderKill` (`sim.crash` the current leader),
//! `FollowerKill` (`sim.crash` a non-leader), `PartitionLeader` (isolate the
//! leader from the rest), `SplitBrain` (full-mesh partition, no majority
//! anywhere), `Lossy` (`NetConfig::set_drop_prob`), (PR②) `Duplicate`
//! (`NetConfig::set_duplicate_prob`), `FsyncLie`
//! (`DiskConfig::set_fsync_lie_prob`), `TornTail`
//! (`DiskConfig::torn_tail_on_crash`, composed with a crash), and (PR③)
//! `StopRestart` — a REAL process restart: `sim.stop` (tasks + volatile
//! state gone; durable engine survives) on a non-leader victim, then, at
//! `heal_all` time, a FRESH `RaftNode::start` reopening the SAME retained
//! `MemoryEngine` handle (`Group::engines`, mirroring `tests/restart.rs`'s
//! idiomatic pattern exactly). This is categorically different from
//! `LeaderKill`/`FollowerKill`'s `sim.crash` — a crash mutes the SAME
//! still-live tasks (`RaftCore`, the driver loop, the apply task all stay
//! in memory) and `sim.restart` merely re-arms them, never touching
//! `meta_apply_loop`'s restart-recovery path (`rebuild_metadata_from_
//! engine` + reseeding `engine_applied` from the engine's own `syskv::
//! applied_index_key()`, ADR 0038) at all. **Gotcha** (`docs/engineering-
//! lessons.md`): `Simulator::stop` does NOT clear a `crashed` flag a prior
//! `Simulator::crash` on the same node set — composing the two on one id
//! needs `crash; stop; restart` (restart clears the mute) *before*
//! reconstructing the fresh node, or its network traffic is silently
//! dropped forever; `Group::stop_node` defensively does this even though no
//! cell in this file currently composes the two. **`CorruptOnCrash` is also
//! deliberately NOT a `Nemesis` variant** — a hard process abort (which is
//! what this composition used to cause in the CP data plane, issue #495,
//! now fixed by a per-record CRC32 checksum on the shared WAL codec,
//! `animus-control::persist::WalRecord`) cannot be an ordinary scenario
//! assertion; the one cell exercising the identical composition here is a
//! dedicated `#[ignore]`d test
//! (`control_corrupt_on_crash_may_hard_panic_issue_495`), never part of the
//! asserted `corpus_cells()` set — see that test's own doc for why this
//! plane's own sweep found no reproduction even before the fix
//! (`Metadata::apply` has no invariant as strict as cp-data's
//! `assert_ts_monotonic` for a wrong-but-decodable field to trip). `heal_all`
//! resets **both** `NetConfig` and `DiskConfig` to default — required for
//! `FsyncLie`/`TornTail`, which
//! are armed globally with no auto-expiry (PR① never used `DiskConfig` at
//! all, so this reset is new here).
//!
//! **Workloads**: [`Workload::SchemaRace`] — 2-3 concurrent proposers each
//! racing `MetaCommand::CreateTableSchema`, either for the SAME table name
//! with distinct schemas (`same_table: true`, the exclusivity teeth) or for
//! distinct names each (`same_table: false`, a lower-contention baseline
//! where every racer should win its own name); [`Workload::PlainChurn`] —
//! trivial no-contention `UpsertMember` proposals, the non-vacuity floor
//! every corpus in this repo needs (mirroring `control_raft.rs`'s own
//! baseline style); (PR②) [`Workload::AllocatorRace`] — several proposers
//! racing `CreateTablet`/`BeginSplitInPlace` against ONE shared table/tablet,
//! hammering `Metadata::next_tablet_id`/`next_free_tablet_id()`; and (PR②)
//! [`Workload::RegisterCas`] — several proposers each claiming a distinct
//! node id then attempting one deterministic differing-re-registration
//! collision against their own claim, lifting `register_node_cas.rs`'s
//! fixed-single-seed CAS proof into this corpus's fault matrix; and (PR③)
//! [`Workload::SustainedChurn`] — like `PlainChurn` but `SUSTAINED_CHURN_
//! ROUNDS` (50, not `CHURN_ROUNDS`'s 3) per proposer, driving the committed
//! log well past `SNAPSHOT_THRESHOLD` so a swept `StopRestart` has real
//! in-flight apply-task/compaction state to interrupt at every point in the
//! sweep (generalizes `wal_compaction.rs`'s single-fixed-instant `crash_
//! during_sustained_compaction_recovers_to_the_uninterrupted_reference_
//! state` across an early/mid/late window — that test's own module doc
//! calls out the single-instant limitation this workload/nemesis pairing
//! closes). Every proposer retries against whichever node currently reports
//! itself leader, exactly the `propose`/`NotLeader`-hint retry idiom
//! `register_node_cas.rs` already uses in this crate.
//!
//! **Chunked-snapshot-under-fault** (PR③, two bespoke tests near the bottom
//! of this file, NOT part of `corpus_cells()`/`Workload`/`Nemesis` at all):
//! `chunked_snapshot_source_crash_mid_transfer_3` and `chunked_snapshot_
//! receiver_stop_restart_3` grow a REAL `Metadata` image (never a
//! hand-supplied synthetic blob, unlike `install_snapshot.rs`'s own
//! chunk-mechanics tests) through the actual `meta_apply_and_compact`/
//! `syskv_image` path until it forces a genuine multi-chunk
//! `InstallSnapshot` transfer, then inject a fault — a source-leader crash,
//! or a receiver `StopRestart` — while chunks are demonstrably still in
//! flight (`wait_for_snapshot_transfer_in_flight`, a condition-based poll,
//! not a duration guess — see that function's own doc for why: an
//! exploratory run found the whole multi-chunk transfer completes within
//! roughly 3ms of virtual time once shipping starts, far too narrow a
//! window for this harness's usual `Vec<(Duration, Nemesis)>` fault
//! schedule to land inside reliably). No existing fixed test in this crate
//! composes a mid-transfer fault with a real multi-chunk transfer.
//!
//! **Depth knob**: `ANIMUS_CONTROL_SEEDS` (default 1 = the frozen cells,
//! byte-identical run-to-run), wired via `animus_test::corpus`'s
//! `seeds_from_env`/`seed_expand` exactly like every other corpus in this
//! repo — covers `corpus_cells()`; the two chunked-snapshot tests are
//! fixed-single-seed regressions in the same style as `install_snapshot.rs`'s
//! own tests, not seed-expanded (see those tests' own doc for why: their
//! setup targets one specific follower rather than a generic `Nemesis`-
//! selected victim, which doesn't fit `Scenario`'s shape without a
//! disproportionate harness change for two cells).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::{
    ColumnType, MetaCommand, Metadata, NodeAddrs, NodeStatus, RaftNode, TableSchema,
};
use animus_env::{Clock, EnvExt, NodeId, nid};
use animus_sim::{DiskConfig, NetConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId};
use animus_test::corpus::{self, SeedVariant};

/// A control-group node under `SimEnv`.
type Node = RaftNode<SimEnv>;
/// The live replica set. Interior mutability so a fault-injecting `Group`
/// method and concurrently-running client tasks can both hold a handle;
/// clients clone the `Arc` out and never hold the lock across an `.await`
/// (mirroring `raftkv_linearizable.rs`'s identical `Nodes` shape — this PR
/// never *replaces* a slot the way that corpus's `StopRestart` does, but
/// keeping the same shape now is what makes PR③'s `StopRestart` a
/// non-invasive addition later).
type Nodes = Arc<Mutex<Vec<Arc<Node>>>>;

/// Control-group replica node ids. A scenario uses a prefix (3 or 5
/// replicas), exactly `raftkv_linearizable.rs`'s convention.
const GROUP_IDS: [u64; 5] = [0, 1, 2, 3, 4];
/// Per-proposer driver env ids — disjoint from the group, **never faulted**,
/// so a proposer task always makes progress (it routes its own proposals to
/// whichever group node currently leads, tolerating crashes/partitions of
/// the group itself).
const CLIENT_IDS: [u64; 5] = [100, 101, 102, 103, 104];

/// How long a single proposer keeps retrying before giving up on ever
/// confirming its own effect. Generous, mirroring raftkv's `OP_BUDGET`
/// reasoning: a proposal racing a `SplitBrain`/`PartitionLeader` fault must
/// be allowed to ride out an election plus catch-up without being
/// misclassified as permanently lost.
const OP_BUDGET: Duration = Duration::from_secs(15);
/// Poll granularity while a proposer waits for its own effect to land.
const POLL: Duration = Duration::from_millis(100);
/// Settle time before the workload starts (let the group elect a leader).
const SETTLE: Duration = Duration::from_millis(800);
/// Post-heal drain: run the workload tail to completion (every proposer's own
/// `OP_BUDGET` clock keeps ticking through this) before snapshotting final
/// state for the checks.
const DRAIN: Duration = Duration::from_secs(25);
/// Converged-or-timeout poll step + budget for cross-replica agreement —
/// mirrors raftkv's identical constants and poll-loop shape.
const CONVERGENCE_POLL_STEP: Duration = Duration::from_secs(2);
const CONVERGENCE_BUDGET: Duration = Duration::from_secs(120);
/// Rounds a `PlainChurn` proposer runs — small, since the point is a
/// non-vacuity floor, not a stress test.
const CHURN_ROUNDS: u64 = 3;
/// Rounds a `SustainedChurn` proposer runs (PR③) — with 3 proposers this
/// drives ~150 committed entries, comfortably past `SNAPSHOT_THRESHOLD`
/// (64) so compaction/apply-task activity is genuinely in flight across the
/// whole `early`/`mid`/`late` sweep window, not just at one instant.
const SUSTAINED_CHURN_ROUNDS: u64 = 50;

// ---------------------------------------------------------------------------
// Declarative scenario model.
// ---------------------------------------------------------------------------

/// A fault the nemesis injects at a scheduled virtual time, resolved against
/// the live group at run time. Subset of the eventual vocabulary — see this
/// file's top doc for what's deferred to PR②/③ and why.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Nemesis {
    /// Crash the current leader. Survivors must elect a new leader and keep
    /// serving; every confirmed effect must survive. Restarted by
    /// `heal_all`.
    LeaderKill,
    /// Crash a follower (a non-leader replica). Quorum is unaffected; the
    /// crashed node must catch up after restart.
    FollowerKill,
    /// Partition the current leader away from every other replica: the
    /// majority elects a fresh leader, the isolated old leader can neither
    /// commit nor confirm a proposal. Healed by `heal_all`.
    PartitionLeader,
    /// Partition every replica from every other (full-mesh islands): **no**
    /// side has a majority, so commits stall entirely until heal.
    SplitBrain,
    /// Inject lossy links (independent per-message drop) for the rest of the
    /// run.
    Lossy,
    /// Inject wire-message duplication (`NetConfig::set_duplicate_prob`) for
    /// the rest of the run — a delivered message is redelivered a second
    /// time. Tests idempotency of `RegisterNode`'s CAS and
    /// `CreateTableSchema`'s reject-on-repeat apply-time logic under a
    /// literally duplicated wire message, distinct from a proposer's own
    /// duplicated *proposal* (already covered by every racing workload's
    /// blind retry loop).
    Duplicate,
    /// Arm **fsync-acked-but-lost** (`DiskConfig::set_fsync_lie_prob`,
    /// global) for the rest of the run: a `sync` call still returns `Ok`,
    /// but the bytes it claims to have made durable stay buffered — only a
    /// later crash (this PR composes it with `LeaderKill`/`FollowerKill` in
    /// the same fault schedule) reveals the lie by losing them. **Must be
    /// paired with resetting `DiskConfig` in `heal_all`** — see that
    /// method's own doc for why a fired `FsyncLie` would otherwise keep
    /// lying past its intended window.
    FsyncLie,
    /// Arm `DiskConfig::torn_tail_on_crash` (global) for the rest of the
    /// run: the **next** `Simulator::crash` on any node keeps only a
    /// seed-chosen strict prefix of that node's un-synced buffered WAL
    /// bytes instead of dropping the whole tail atomically — modelling a
    /// write torn mid-record by a power loss. Has no effect by itself; a
    /// scenario using this schedules a `LeaderKill`/`FollowerKill` after it.
    /// Deliberately **not** paired with `corrupt_on_crash` here — see the
    /// dedicated, `#[ignore]`d `control_corrupt_on_crash_may_hard_panic_
    /// issue_495` test near the bottom of this file for why that
    /// composition is a separate, deliberately-unasserted case (issue
    /// #495).
    TornTail,
    /// PR③: a **real process restart** — `sim.stop` (tasks + volatile state
    /// gone; durable engine survives) followed, at `heal_all` time, by a
    /// FRESH `RaftNode::start` reopening the SAME retained `MemoryEngine`
    /// handle (`Group::engines`) — never merely `sim.crash`+`sim.restart`
    /// (which re-arms the SAME still-live tasks and never touches
    /// `meta_apply_loop`'s restart-recovery path at all: `rebuild_metadata_
    /// from_engine` + reseeding `engine_applied` from the engine's own
    /// `syskv::applied_index_key()`, ADR 0038). This is what actually
    /// exercises the apply-task crash-recovery contract `LeaderKill`/
    /// `FollowerKill` cannot: those crash the *process* only figuratively
    /// (the driver loop, apply task, and `RaftCore` all stay in memory,
    /// merely muted). Picks a **non-leader** victim (mirrors `FollowerKill`'s
    /// selection — restarting the leader would also force a re-election,
    /// confounding what this nemesis is meant to isolate). Defensively
    /// clears any stale `crashed` mute before stopping
    /// (`Group::stop_node`) — **gotcha** (`docs/engineering-lessons.md`):
    /// `Simulator::stop` does NOT clear a `crashed` flag a prior
    /// `Simulator::crash` on the same node set, so a cell that ever composed
    /// this with an earlier crash on the same id would silently blackhole
    /// the reconstructed node's traffic without this. Healed/reconstructed
    /// by `heal_all`, same as every other nemesis here.
    StopRestart,
}

/// Which workload shape a scenario drives — this plane's own reason for a
/// `Workload` field (see this file's top doc): its interesting scenarios
/// need genuinely different client behavior, not just different parameters
/// of one shared loop.
#[derive(Clone, Debug)]
enum Workload {
    /// `proposers` concurrent racers, each attempting
    /// `MetaCommand::CreateTableSchema`. `same_table: true` races them all
    /// against the identical table name with distinct schemas (the
    /// exclusivity teeth); `same_table: false` gives each racer its own
    /// name (a lower-contention baseline — every racer should win).
    SchemaRace { proposers: usize, same_table: bool },
    /// `proposers` concurrent, non-contending `UpsertMember` proposers — the
    /// non-vacuity floor.
    PlainChurn { proposers: usize },
    /// `proposers` concurrent racers hammering `Metadata::next_tablet_id`/
    /// `next_free_tablet_id()` (`crates/animus-control/src/meta.rs`) — the
    /// genuinely allocator-shaped counter `CreateTablet`/`BeginSplitInPlace` race.
    /// Two phases per scenario, both against ONE shared table/tablet (the
    /// "same parent range/table" this workload's own doc promises): first
    /// every racer proposes `CreateTablet` for the identical table name
    /// (only one can ever win — ADR 0023's one-tablet-per-table rule — so a
    /// loser must recompute a fresh candidate id and retry against a
    /// *different* table name-shaped collision, i.e. an id another racer's
    /// `CreateTablet` already claimed); then, once any racer observes the
    /// shared tablet exists, every racer repeatedly proposes `BeginSplitInPlace`
    /// against it with a freshly-recomputed split key and freshly-recomputed
    /// child ids (again racing the same counter) until the parent leaves
    /// `Active` (someone won) or the budget expires. See invariant #4
    /// (allocator injectivity) and `check_allocator_injectivity` below.
    AllocatorRace { proposers: usize },
    /// `registrants` concurrent clients, each claiming its OWN distinct new
    /// node id via `MetaCommand::RegisterNode` (lifting
    /// `tests/register_node_cas.rs`'s fixed-single-seed CAS proof — distinct
    /// concurrent registrations, leader-kill-mid-registration retry,
    /// follower relay, differing-re-registration rejection — into this
    /// corpus's seed-depth fault matrix, since the underlying apply logic is
    /// already proven correct at a single seed and only needs fault-matrix
    /// depth, not new logic). After a client's own claim is durably
    /// confirmed, it makes exactly ONE follow-up "collision" attempt: the
    /// SAME node id with a DIFFERENT address book — a deterministic,
    /// guaranteed-second `RegisterNode` for an already-claimed id, proving
    /// the CAS rejects it outright rather than overwriting.
    RegisterCas { registrants: usize },
    /// PR③: like `PlainChurn`, but `SUSTAINED_CHURN_ROUNDS` (not `CHURN_ROUNDS`)
    /// per proposer — enough non-contending `UpsertMember`s to reliably drive
    /// the committed log well past `SNAPSHOT_THRESHOLD` (64), so a
    /// `StopRestart` landing mid-run has real in-flight apply-task/compaction
    /// state to interrupt. `PlainChurn`'s own `CHURN_ROUNDS = 3` is far too
    /// light for this (mirrors `wal_compaction.rs`'s `crash_during_sustained_
    /// compaction_recovers_to_the_uninterrupted_reference_state`'s load shape,
    /// generalized across a swept crash instant instead of that file's one
    /// fixed instant — see that test's own module doc, which calls out the
    /// single-fixed-instant gap this workload/nemesis pairing closes).
    SustainedChurn { proposers: usize },
}

/// A seed-reproducible scenario: a named group size + workload + an explicit
/// fault schedule (virtual time → nemesis) + an optional outage window.
/// `heal_all` is always applied by the runner at the end, so it is never
/// scheduled here (mirrors raftkv's `Scenario` exactly, minus the engine-tier
/// concern that harness carries and this one doesn't need yet).
#[derive(Clone, Debug)]
struct Scenario {
    name: String,
    seed: u64,
    replicas: usize,
    workload: Workload,
    faults: Vec<(Duration, Nemesis)>,
    /// How long the runner keeps the *last* fault open before healing. Every
    /// cell in this PR uses `ZERO` (no deepened tier yet — see the top doc);
    /// carried as a field now so PR②/③ can add windowed cells without
    /// reshaping `Scenario`.
    window: Duration,
}

impl SeedVariant for Scenario {
    fn scenario_name(&self) -> &str {
        &self.name
    }
    fn reseeded(&self, name: String, seed: u64) -> Self {
        Scenario {
            name,
            seed,
            ..self.clone()
        }
    }
}

/// Fault timing relative to the workload's life: early / mid / late —
/// identical convention to raftkv's `CORPUS_TIMINGS`.
const CORPUS_TIMINGS: [(&str, Duration); 3] = [
    ("early", Duration::from_millis(700)),
    ("mid", Duration::from_millis(2200)),
    ("late", Duration::from_millis(3800)),
];

fn schema_race_scenario(
    name: &str,
    replicas: usize,
    proposers: usize,
    same_table: bool,
    faults: Vec<(Duration, Nemesis)>,
) -> Scenario {
    Scenario {
        seed: corpus::name_seed(name),
        name: name.to_string(),
        replicas,
        workload: Workload::SchemaRace {
            proposers,
            same_table,
        },
        faults,
        window: Duration::ZERO,
    }
}

fn plain_churn_scenario(
    name: &str,
    replicas: usize,
    proposers: usize,
    faults: Vec<(Duration, Nemesis)>,
) -> Scenario {
    Scenario {
        seed: corpus::name_seed(name),
        name: name.to_string(),
        replicas,
        workload: Workload::PlainChurn { proposers },
        faults,
        window: Duration::ZERO,
    }
}

fn sustained_churn_scenario(
    name: &str,
    replicas: usize,
    proposers: usize,
    faults: Vec<(Duration, Nemesis)>,
) -> Scenario {
    Scenario {
        seed: corpus::name_seed(name),
        name: name.to_string(),
        replicas,
        workload: Workload::SustainedChurn { proposers },
        faults,
        window: Duration::ZERO,
    }
}

fn allocator_race_scenario(
    name: &str,
    replicas: usize,
    proposers: usize,
    faults: Vec<(Duration, Nemesis)>,
) -> Scenario {
    Scenario {
        seed: corpus::name_seed(name),
        name: name.to_string(),
        replicas,
        workload: Workload::AllocatorRace { proposers },
        faults,
        window: Duration::ZERO,
    }
}

fn register_cas_scenario(
    name: &str,
    replicas: usize,
    registrants: usize,
    faults: Vec<(Duration, Nemesis)>,
) -> Scenario {
    Scenario {
        seed: corpus::name_seed(name),
        name: name.to_string(),
        replicas,
        workload: Workload::RegisterCas { registrants },
        faults,
        window: Duration::ZERO,
    }
}

/// The structural cells of this corpus. Every `Nemesis` variant and every
/// `Workload` variant appears at least once (checked by
/// `control_corpus_covers_the_fault_matrix`, below) — some fault/workload
/// combinations get only a single spot-check cell; PR③ is expected to keep
/// deepening these into fuller early/mid/late/5-replica grids the way
/// raftkv's corpus does for its own fault set.
fn corpus_cells() -> Vec<Scenario> {
    let mut out = Vec::new();

    // --- Non-vacuity floor: no-fault PlainChurn baselines, both shapes. ---
    out.push(plain_churn_scenario("baseline_3", 3, 3, vec![]));
    out.push(plain_churn_scenario("baseline_5", 5, 3, vec![]));

    // --- Fault-free schema race: exclusivity must hold even absent any
    //     fault — not previously proven at any seed depth. ---
    out.push(schema_race_scenario(
        "schema_race_baseline_3",
        3,
        2,
        true,
        vec![],
    ));

    // --- LeaderKill x early/mid/late, same-table race, 3 replicas. ---
    for (tname, at) in CORPUS_TIMINGS {
        let name = format!("schema_race_leader_kill_{tname}_3");
        out.push(schema_race_scenario(
            &name,
            3,
            2,
            true,
            vec![(at, Nemesis::LeaderKill)],
        ));
    }

    // --- PartitionLeader mid-race. ---
    out.push(schema_race_scenario(
        "schema_race_partition_mid_3",
        3,
        2,
        true,
        vec![(Duration::from_millis(2200), Nemesis::PartitionLeader)],
    ));

    // --- FollowerKill / Lossy spot-checks, so the coverage guard has a real
    //     scenario for each of this PR's remaining nemesis variants. ---
    out.push(schema_race_scenario(
        "schema_race_follower_kill_mid_3",
        3,
        2,
        true,
        vec![(Duration::from_millis(2200), Nemesis::FollowerKill)],
    ));
    out.push(schema_race_scenario(
        "schema_race_lossy_mid_3",
        3,
        2,
        true,
        vec![(Duration::from_millis(2200), Nemesis::Lossy)],
    ));

    // --- Distinct-name race under a full split brain, 5 replicas. ---
    out.push(schema_race_scenario(
        "schema_race_distinct_names_split_brain_5",
        5,
        3,
        false,
        vec![(Duration::from_millis(2200), Nemesis::SplitBrain)],
    ));

    // --- PR② additions below: AllocatorRace + RegisterCas workloads, and
    //     the fuller fault vocabulary (Duplicate/FsyncLie/TornTail). ---

    // --- AllocatorRace: fault-free baseline. ---
    out.push(allocator_race_scenario(
        "allocator_race_baseline_3",
        3,
        3,
        vec![],
    ));

    // --- AllocatorRace under a mid-race leader kill (phase-agnostic — may
    //     land during either the CreateTablet or the BeginSplitInPlace
    //     phase). ---
    out.push(allocator_race_scenario(
        "allocator_race_leader_kill_mid_3",
        3,
        3,
        vec![(Duration::from_millis(2200), Nemesis::LeaderKill)],
    ));

    // --- AllocatorRace with a LATER leader kill, timed to land once the
    //     shared tablet is already created and racers are contending
    //     specifically over `BeginSplitInPlace`'s child-id allocation. ---
    out.push(allocator_race_scenario(
        "allocator_race_split_leader_kill_3",
        3,
        3,
        vec![(Duration::from_millis(3800), Nemesis::LeaderKill)],
    ));

    // --- AllocatorRace under a full partition, 5 replicas. ---
    out.push(allocator_race_scenario(
        "allocator_race_partition_5",
        5,
        4,
        vec![(Duration::from_millis(2200), Nemesis::PartitionLeader)],
    ));

    // --- AllocatorRace crossed with FsyncLie (new nemesis) + LeaderKill:
    //     the leader's own un-synced tail is lied-about-durable, then lost
    //     on crash — survivors must still keep every confirmed id unique
    //     and durable. ---
    out.push(allocator_race_scenario(
        "allocator_race_fsync_lie_leader_kill_3",
        3,
        3,
        vec![
            (Duration::from_millis(700), Nemesis::FsyncLie),
            (Duration::from_millis(2200), Nemesis::LeaderKill),
        ],
    ));

    // --- RegisterCas: fault-free baseline. ---
    out.push(register_cas_scenario(
        "register_cas_baseline_3",
        3,
        3,
        vec![],
    ));

    // --- RegisterCas under a mid-registration leader kill — the seed-depth
    //     generalization of `register_node_cas.rs`'s
    //     `leader_killed_mid_registration_identical_retry_converges`. ---
    out.push(register_cas_scenario(
        "register_cas_leader_kill_mid_3",
        3,
        3,
        vec![(Duration::from_millis(2200), Nemesis::LeaderKill)],
    ));

    // --- RegisterCas crossed with the new Duplicate nemesis: a duplicated
    //     wire message must never double-claim or corrupt a registration. ---
    out.push(register_cas_scenario(
        "register_cas_duplicate_mid_3",
        3,
        3,
        vec![(Duration::from_millis(2200), Nemesis::Duplicate)],
    ));

    // --- RegisterCas under a lossy network AND a leader partition, 5
    //     replicas — the CAS's collision-rejection path must hold even when
    //     both the collision retry and the original claim are racing a
    //     degraded, partitioned network. ---
    out.push(register_cas_scenario(
        "register_cas_lossy_partition_5",
        5,
        4,
        vec![
            (Duration::from_millis(700), Nemesis::Lossy),
            (Duration::from_millis(2200), Nemesis::PartitionLeader),
        ],
    ));

    // --- TornTail composed with a crash (LeaderKill): the crashed node's
    //     un-synced WAL tail is torn (a seed-chosen strict prefix kept, the
    //     rest lost) on restart. Only the *surviving* replicas' convergence
    //     is asserted here (the existing convergence check already proves
    //     it) — checking the torn-tailed node's own recovered state needs
    //     `StopRestart`, deferred to PR③ (see this file's top doc). Reuses
    //     `PlainChurn` (a plain workload is enough teeth for a WAL-tear
    //     regression; nothing about the tear is workload-specific). ---
    out.push(plain_churn_scenario(
        "torn_tail_leader_kill_3",
        3,
        3,
        vec![
            (Duration::from_millis(1500), Nemesis::TornTail),
            (Duration::from_millis(2200), Nemesis::LeaderKill),
        ],
    ));

    // --- PR③ additions below: StopRestart (a real process restart —
    //     `sim.stop` + a fresh `RaftNode::start` reopening the SAME retained
    //     engine handle) and invariant #5 (apply-task liveness). ---

    // --- SchemaRace under a mid-race StopRestart: proves the apply task
    //     correctly rebuilds `shadow: Metadata` from `mirror::
    //     rebuild_metadata_from_engine` and reseeds its watermark from the
    //     engine's own `syskv::applied_index_key()` after a REAL restart
    //     (ADR 0038), not merely a muted/re-armed `sim.crash`. ---
    out.push(schema_race_scenario(
        "schema_race_stop_restart_mid_3",
        3,
        2,
        true,
        vec![(Duration::from_millis(2200), Nemesis::StopRestart)],
    ));

    // --- AllocatorRace under a mid-race StopRestart: the same real-restart
    //     recovery proof, over the allocator-shaped counter instead of the
    //     schema catalog. ---
    out.push(allocator_race_scenario(
        "allocator_race_stop_restart_mid_3",
        3,
        3,
        vec![(Duration::from_millis(2200), Nemesis::StopRestart)],
    ));

    // --- SustainedChurn x early/mid/late StopRestart: a crash-timing sweep
    //     under genuinely sustained load (SUSTAINED_CHURN_ROUNDS, well past
    //     SNAPSHOT_THRESHOLD), generalizing `wal_compaction.rs`'s single
    //     fixed-instant `crash_during_sustained_compaction_recovers_to_the_
    //     uninterrupted_reference_state` across the swept window that test's
    //     own module doc calls out as a gap. ---
    for (tname, at) in CORPUS_TIMINGS {
        let name = format!("apply_task_stop_restart_under_load_{tname}_3");
        out.push(sustained_churn_scenario(
            &name,
            3,
            3,
            vec![(at, Nemesis::StopRestart)],
        ));
    }

    out
}

/// Seeds per structural cell (`ANIMUS_CONTROL_SEEDS`, default 1) — `K=1` is
/// byte-identical to the committed frozen set.
fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_CONTROL_SEEDS")
}

/// The corpus the headline test runs: the frozen cells, seed-expanded by the
/// depth knob.
fn corpus() -> Vec<Scenario> {
    corpus::seed_expand(corpus_cells(), seeds_per_cell())
}

fn lossy(p: f64) -> NetConfig {
    let mut cfg = NetConfig::default();
    cfg.set_drop_prob(p);
    cfg
}

/// A duplicated-message net config — house convention `0.1`-`0.3` range (see
/// `Nemesis::Duplicate`'s own doc).
fn duplicate(p: f64) -> NetConfig {
    let mut cfg = NetConfig::default();
    cfg.set_duplicate_prob(p);
    cfg
}

// ---------------------------------------------------------------------------
// The running group.
// ---------------------------------------------------------------------------

/// Everything a proposer's confirm loop reports back, and everything the
/// exclusivity check needs about the full field of racing attempts.
struct Shared {
    /// Every `CreateTableSchema` attempt any racer ever made, recorded once
    /// at attempt time regardless of outcome — invariant #3 (exclusivity) is
    /// checked against this full candidate set, not just what one
    /// proposer's own confirm loop happened to observe win.
    schema_attempts: Mutex<Vec<(String, TableSchema)>>,
    /// Attempts a proposer's own confirm loop actually observed committed:
    /// its own proposed schema, byte-identical, durably visible on a read
    /// after proposing. Invariant #2 (durability) requires every one of
    /// these to still be present in the final converged state.
    confirmed_schemas: Mutex<Vec<(String, TableSchema)>>,
    /// `PlainChurn`: member ids a proposer's own confirm loop saw land — the
    /// same durability obligation, over `Metadata::members` instead of
    /// `Metadata::schemas`.
    confirmed_members: Mutex<BTreeSet<NodeId>>,
    /// `AllocatorRace`: every `TabletId` a proposer's own confirm loop
    /// actually observed committed (either the shared table's `CreateTablet`
    /// or a `BeginSplitInPlace`'s own child id) — the durability obligation over
    /// `Metadata::tablets`, and the set invariant #4's "no two calls were
    /// ever assigned the same id" check is over (see
    /// `check_allocator_injectivity`).
    confirmed_tablet_ids: Mutex<Vec<TabletId>>,
    /// `AllocatorRace`: a running fingerprint (`table`, `range.start`,
    /// `range.end`) of every `TabletId` observed in ANY replica's tablet map
    /// at ANY sampled point in the run (`Shared::sample_tablets`, called at
    /// every convergence poll and fault-schedule step — see this file's top
    /// doc). A later sample disagreeing with an earlier one for the same id
    /// is a transient double-assignment, caught even if a later poll
    /// "corrects" it back to one identity. Violations are recorded directly
    /// into `injectivity_violations` at sample time, not re-derived at check
    /// time, precisely so a transient mismatch a later sample overwrites is
    /// never silently lost.
    tablet_fingerprints: Mutex<BTreeMap<TabletId, TabletFingerprint>>,
    /// Violations `sample_tablets` has ever recorded — see
    /// `tablet_fingerprints`'s own doc for why this must accumulate rather
    /// than being derived fresh from the final state alone.
    injectivity_violations: Mutex<Vec<String>>,
    /// `RegisterCas`: every `RegisterNode` attempt any client ever made
    /// (its own original claim, and its own deterministic differing-
    /// re-registration collision attempt against that same id), recorded at
    /// attempt time regardless of outcome — mirrors `schema_attempts`'
    /// role for `check_register_cas_integrity`.
    register_attempts: Mutex<Vec<(NodeId, NodeAddrs)>>,
    /// `RegisterCas`: `(node, addrs)` pairs whose ORIGINAL claim a client's
    /// own confirm loop actually observed committed — its own proposed
    /// address book, byte-identical, durably visible on a read after
    /// proposing. Invariant #2 (durability) requires each to still be
    /// present, byte-identical, in `Metadata::node_addrs` in the final
    /// converged state.
    confirmed_registrations: Mutex<Vec<(NodeId, NodeAddrs)>>,
}

/// `(table, range.start, range.end)` — a tablet's identity for the
/// allocator-injectivity sampler; cheap to clone/compare, and exactly the
/// fields two different `CreateTablet`/`BeginSplitInPlace` calls minting the same
/// id could plausibly disagree on.
type TabletFingerprint = (Option<String>, Vec<u8>, Option<Vec<u8>>);

impl Shared {
    fn new() -> Self {
        Shared {
            schema_attempts: Mutex::new(Vec::new()),
            confirmed_schemas: Mutex::new(Vec::new()),
            confirmed_members: Mutex::new(BTreeSet::new()),
            confirmed_tablet_ids: Mutex::new(Vec::new()),
            tablet_fingerprints: Mutex::new(BTreeMap::new()),
            injectivity_violations: Mutex::new(Vec::new()),
            register_attempts: Mutex::new(Vec::new()),
            confirmed_registrations: Mutex::new(Vec::new()),
        }
    }

    fn record_schema_attempt(&self, table: &str, schema: &TableSchema) {
        self.schema_attempts
            .lock()
            .unwrap()
            .push((table.to_string(), schema.clone()));
    }

    fn confirm_schema(&self, table: &str, schema: &TableSchema) {
        self.confirmed_schemas
            .lock()
            .unwrap()
            .push((table.to_string(), schema.clone()));
    }

    fn confirm_member(&self, node: NodeId) {
        self.confirmed_members.lock().unwrap().insert(node);
    }

    fn confirm_tablet_id(&self, id: TabletId) {
        self.confirmed_tablet_ids.lock().unwrap().push(id);
    }

    fn record_register_attempt(&self, node: NodeId, addrs: &NodeAddrs) {
        self.register_attempts
            .lock()
            .unwrap()
            .push((node, addrs.clone()));
    }

    fn confirm_registration(&self, node: NodeId, addrs: &NodeAddrs) {
        self.confirmed_registrations
            .lock()
            .unwrap()
            .push((node, addrs.clone()));
    }

    fn confirmed_count(&self) -> usize {
        self.confirmed_schemas.lock().unwrap().len()
            + self.confirmed_members.lock().unwrap().len()
            + self.confirmed_tablet_ids.lock().unwrap().len()
            + self.confirmed_registrations.lock().unwrap().len()
    }

    /// Sample every replica's current tablet map into `tablet_fingerprints`,
    /// recording a violation the instant a `TabletId` is observed with a
    /// fingerprint that disagrees with an earlier sample. Called at every
    /// convergence-poll iteration and fault-schedule step (see this file's
    /// top doc + `run_scenario`) so a transient double-assignment is caught
    /// even if a later poll happens to "correct" back to one identity.
    ///
    /// Also fingerprints `AllocatorRace`'s in-place-split racers' `left`/
    /// `right` ids EVEN THOUGH `BeginSplitInPlace` mints no tablet-map row
    /// for them at all (the fork is recorded as an intent on the PARENT;
    /// this workload never proposes the `CutoverSplit` that would
    /// materialize them) — without this, those ids would never enter
    /// `tablet_fingerprints` and invariant #4 (allocator injectivity) would
    /// have no teeth over the split phase of the race at all. Derives the
    /// SAME `(table, range.start, range.end)` shape a real materialized
    /// child would carry, computed from the parent's own (untouched) range
    /// and the intent's own split key — exactly what `CutoverSplit`'s
    /// in-place branch itself computes when it eventually mints the row.
    fn sample_tablets(&self, metas: &[Metadata]) {
        let mut fp = self.tablet_fingerprints.lock().unwrap();
        let mut violations = self.injectivity_violations.lock().unwrap();
        let mut record = |ri: usize, id: TabletId, fingerprint: TabletFingerprint| match fp.get(&id)
        {
            None => {
                fp.insert(id, fingerprint);
            }
            Some(existing) if *existing == fingerprint => {}
            Some(existing) => violations.push(format!(
                "tablet id {id:?} observed with two different identities (replica \
                 {ri}): {existing:?} then {fingerprint:?} — a transient \
                 double-assignment"
            )),
        };
        for (ri, m) in metas.iter().enumerate() {
            for (id, t) in &m.tablets {
                record(
                    ri,
                    *id,
                    (t.table.clone(), t.range.start.clone(), t.range.end.clone()),
                );
                if let Some(intent) = &t.inplace_split
                    && let Some((left, right)) = t.range.split_at(&intent.split_key)
                {
                    record(
                        ri,
                        intent.children[0].id,
                        (t.table.clone(), left.start, left.end),
                    );
                    record(
                        ri,
                        intent.children[1].id,
                        (t.table.clone(), right.start, right.end),
                    );
                }
            }
        }
    }
}

/// Index + handle of a group node that currently believes it leads (lowest
/// index if more than one — possible transiently under a partition). `None`
/// if none does. Clones the `Arc` out so no lock is held across an `.await`.
fn leader_slot(nodes: &Nodes) -> Option<(usize, Arc<Node>)> {
    let guard = nodes.lock().unwrap();
    guard
        .iter()
        .position(|n| n.is_leader())
        .map(|i| (i, Arc::clone(&guard[i])))
}

struct Group {
    sim: Simulator,
    nodes: Nodes,
    replicas: usize,
    shared: Arc<Shared>,
    /// Group ids crashed and not yet restarted.
    crashed: BTreeSet<u64>,
    /// PR③: each node's own durable `StorageEngine` handle, index-aligned
    /// with `GROUP_IDS[..replicas]`/`nodes` — kept alive **outside** the
    /// node itself (mirroring `tests/restart.rs`'s idiomatic pattern
    /// exactly) so `Nemesis::StopRestart`/`stop_node` can construct a FRESH
    /// `RaftNode::start` that reopens the SAME handle a stopped node used,
    /// rather than a fresh, empty one. `MemoryEngine` clones share state, so
    /// re-cloning this handle at reconstruction time is what actually models
    /// "the durable disk survives a process restart".
    engines: Vec<MemoryEngine>,
    /// Group ids `sim.stop`ped and not yet reconstructed — disjoint in
    /// practice from `crashed` (this file never composes the two on the same
    /// id), but tracked separately since the two need different `heal_all`
    /// treatment: `crashed` just needs `sim.restart`, `stopped` needs a
    /// brand-new `RaftNode`.
    stopped: BTreeSet<u64>,
}

impl Group {
    fn start(seed: u64, replicas: usize) -> Group {
        assert!((3..=5).contains(&replicas));
        let sim = Simulator::new(seed);
        let ids: Vec<u64> = GROUP_IDS[..replicas].to_vec();
        let engines: Vec<MemoryEngine> = ids.iter().map(|_| MemoryEngine::new()).collect();
        let nodes: Vec<Arc<Node>> = ids
            .iter()
            .zip(engines.iter())
            .map(|(&id, engine)| {
                Arc::new(RaftNode::start(
                    sim.env(nid(id)),
                    ids.iter().copied().map(nid).collect(),
                    engine.clone(),
                ))
            })
            .collect();
        Group {
            sim,
            nodes: Arc::new(Mutex::new(nodes)),
            replicas,
            shared: Arc::new(Shared::new()),
            crashed: BTreeSet::new(),
            engines,
            stopped: BTreeSet::new(),
        }
    }

    fn spawn_workload(&mut self, workload: &Workload) {
        match *workload {
            Workload::SchemaRace {
                proposers,
                same_table,
            } => self.spawn_schema_race_workload(proposers, same_table),
            Workload::PlainChurn { proposers } => self.spawn_plain_churn_workload(proposers),
            Workload::AllocatorRace { proposers } => self.spawn_allocator_race_workload(proposers),
            Workload::RegisterCas { registrants } => self.spawn_register_cas_workload(registrants),
            Workload::SustainedChurn { proposers } => {
                self.spawn_sustained_churn_workload(proposers)
            }
        }
    }

    /// `proposers` concurrent racers, each on its own never-faulted driver
    /// env (mirroring raftkv's `CLIENT_IDS` discipline), each racing
    /// `MetaCommand::CreateTableSchema` for either the SAME table name (with
    /// distinct schemas — a different `partition_key` name per proposer, so
    /// two racers' proposals are always structurally distinguishable) or
    /// distinct names each.
    fn spawn_schema_race_workload(&mut self, proposers: usize, same_table: bool) {
        for (p, &client_id) in CLIENT_IDS.iter().enumerate().take(proposers) {
            let env = self.sim.env(nid(client_id));
            let nodes = Arc::clone(&self.nodes);
            let shared = Arc::clone(&self.shared);
            let table = if same_table {
                "ks.race".to_string()
            } else {
                format!("ks.race_{p}")
            };
            let schema = TableSchema::simple(format!("pk_{p}"), ColumnType::String);
            env.clone().spawn_task(async move {
                schema_race_client(env, nodes, shared, table, schema).await;
            });
        }
    }

    /// `proposers` concurrent, non-contending `UpsertMember` proposers —
    /// each proposes `CHURN_ROUNDS` distinct member ids of its own (never
    /// colliding with another proposer's), confirming each before moving on.
    fn spawn_plain_churn_workload(&mut self, proposers: usize) {
        for (p, &client_id) in CLIENT_IDS.iter().enumerate().take(proposers) {
            let env = self.sim.env(nid(client_id));
            let nodes = Arc::clone(&self.nodes);
            let shared = Arc::clone(&self.shared);
            let base = 900 + (p as u64) * 10;
            env.clone().spawn_task(async move {
                plain_churn_client(env, nodes, shared, base, CHURN_ROUNDS).await;
            });
        }
    }

    /// PR③: `proposers` concurrent, non-contending `UpsertMember` proposers,
    /// `SUSTAINED_CHURN_ROUNDS` each — see [`Workload::SustainedChurn`]'s own
    /// doc. Uses a member-id range (`3000+`) disjoint from `PlainChurn`'s
    /// (`900+`) — harmless in practice since only one workload ever runs per
    /// scenario, but keeping the ranges disjoint costs nothing and rules out
    /// a collision if a future PR ever combines workloads in one scenario.
    fn spawn_sustained_churn_workload(&mut self, proposers: usize) {
        for (p, &client_id) in CLIENT_IDS.iter().enumerate().take(proposers) {
            let env = self.sim.env(nid(client_id));
            let nodes = Arc::clone(&self.nodes);
            let shared = Arc::clone(&self.shared);
            let base = 3000 + (p as u64) * 100;
            env.clone().spawn_task(async move {
                plain_churn_client(env, nodes, shared, base, SUSTAINED_CHURN_ROUNDS).await;
            });
        }
    }

    /// `proposers` concurrent racers, all hammering ONE shared table/tablet
    /// — see [`Workload::AllocatorRace`]'s own doc for the two-phase shape
    /// each `allocator_race_client` drives.
    fn spawn_allocator_race_workload(&mut self, proposers: usize) {
        let group_ids: Vec<NodeId> = GROUP_IDS[..self.replicas]
            .iter()
            .copied()
            .map(nid)
            .collect();
        for (p, &client_id) in CLIENT_IDS.iter().enumerate().take(proposers) {
            let env = self.sim.env(nid(client_id));
            let nodes = Arc::clone(&self.nodes);
            let shared = Arc::clone(&self.shared);
            let replicas = group_ids.clone();
            env.clone().spawn_task(async move {
                allocator_race_client(env, nodes, shared, p, replicas).await;
            });
        }
    }

    /// `registrants` concurrent clients, each on its own never-faulted
    /// driver env, each claiming a distinct node id then attempting one
    /// deterministic differing-re-registration collision against its own
    /// claim — see [`Workload::RegisterCas`]'s own doc.
    fn spawn_register_cas_workload(&mut self, registrants: usize) {
        for (r, &client_id) in CLIENT_IDS.iter().enumerate().take(registrants) {
            let env = self.sim.env(nid(client_id));
            let nodes = Arc::clone(&self.nodes);
            let shared = Arc::clone(&self.shared);
            // Distinct from every `PlainChurn`/`AllocatorRace` id range this
            // file mints (900+ / tablet ids), and distinct per registrant.
            let target = nid(950 + r as u64);
            let suffix = 40 + r as u16;
            env.clone().spawn_task(async move {
                register_cas_client(env, nodes, shared, target, suffix).await;
            });
        }
    }

    fn apply(&mut self, nem: Nemesis) {
        let ids: Vec<u64> = GROUP_IDS[..self.replicas].to_vec();
        match nem {
            Nemesis::LeaderKill => {
                if let Some((li, _)) = leader_slot(&self.nodes) {
                    self.sim.crash(nid(ids[li]));
                    self.crashed.insert(ids[li]);
                }
            }
            Nemesis::FollowerKill => {
                let leader = leader_slot(&self.nodes).map(|(i, _)| i);
                let victim = (0..self.replicas)
                    .find(|&i| Some(i) != leader && !self.crashed.contains(&ids[i]));
                if let Some(i) = victim {
                    self.sim.crash(nid(ids[i]));
                    self.crashed.insert(ids[i]);
                }
            }
            Nemesis::PartitionLeader => {
                if let Some((li, _)) = leader_slot(&self.nodes) {
                    for j in 0..self.replicas {
                        if j != li {
                            self.sim.partition_pair(nid(ids[li]), nid(ids[j]));
                        }
                    }
                }
            }
            Nemesis::SplitBrain => {
                for i in 0..self.replicas {
                    for j in (i + 1)..self.replicas {
                        self.sim.partition_pair(nid(ids[i]), nid(ids[j]));
                    }
                }
            }
            Nemesis::Lossy => {
                self.sim.set_net_config(lossy(0.1));
            }
            Nemesis::Duplicate => {
                self.sim.set_net_config(duplicate(0.2));
            }
            Nemesis::FsyncLie => {
                let mut cfg = DiskConfig::default();
                cfg.set_fsync_lie_prob(0.2);
                self.sim.set_disk_config(cfg);
            }
            Nemesis::TornTail => {
                let mut cfg = DiskConfig::default();
                cfg.torn_tail_on_crash = true;
                self.sim.set_disk_config(cfg);
            }
            Nemesis::StopRestart => {
                let leader = leader_slot(&self.nodes).map(|(i, _)| i);
                let victim = (0..self.replicas).find(|&i| {
                    Some(i) != leader
                        && !self.crashed.contains(&ids[i])
                        && !self.stopped.contains(&ids[i])
                });
                if let Some(i) = victim {
                    self.stop_node(ids[i]);
                }
            }
        }
    }

    /// `sim.stop` a specific node id (a real process exit: tasks + volatile
    /// state gone, durable engine kept) and remember it for `heal_all` to
    /// reconstruct. Shared by `Nemesis::StopRestart`'s own auto-selected
    /// victim above and the chunked-snapshot fault-injection tests near the
    /// bottom of this file, which need to target a SPECIFIC node (the one
    /// mid-transfer) rather than whichever victim `apply`'s own leader-aware
    /// selection would pick.
    ///
    /// Defensively clears any stale `crashed` mute first — **gotcha**
    /// (`docs/engineering-lessons.md`): `Simulator::stop` does NOT clear a
    /// `crashed` flag a prior `Simulator::crash` on the same node set, so a
    /// cell that ever composed this with an earlier crash on the same id
    /// would otherwise silently blackhole the reconstructed node's traffic
    /// forever. No cell in this file currently does that, but the guard is
    /// free and rules the whole hazard class out by construction.
    fn stop_node(&mut self, id: u64) {
        self.sim.restart(nid(id));
        self.sim.stop(nid(id));
        self.stopped.insert(id);
    }

    /// Heal every partition, restart every crashed node, restore default
    /// links **and default disk behavior**. The `DiskConfig` reset is
    /// required, not cosmetic: `FsyncLie`/`TornTail` are armed globally
    /// (`Simulator::set_disk_config`, no per-scenario auto-expiry), so
    /// without resetting it here a fired fault keeps lying/tearing past its
    /// intended window — including into a LATER scenario in the same
    /// process if a future PR ever reused a `Simulator` across scenarios
    /// (this file doesn't, but the discipline is what stops that from
    /// becoming a footgun later). PR① never used `DiskConfig` at all, so
    /// this reset is new in PR②.
    fn heal_all(&mut self) {
        let ids: Vec<u64> = GROUP_IDS[..self.replicas].to_vec();
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                self.sim.heal(nid(ids[i]), nid(ids[j]));
            }
        }
        let crashed: Vec<u64> = self.crashed.iter().copied().collect();
        for v in crashed {
            self.sim.restart(nid(v));
        }
        self.crashed.clear();

        // PR③: a `StopRestart` victim needs a FRESH `RaftNode` reopening the
        // SAME retained engine handle — `sim.restart` alone (as used for
        // `crashed` above) would be a no-op here, since `sim.stop` removed
        // the node's tasks entirely instead of merely muting them (see
        // `Nemesis::StopRestart`'s own doc).
        if !self.stopped.is_empty() {
            let all_ids: Vec<NodeId> = ids.iter().copied().map(nid).collect();
            let stopped: Vec<u64> = self.stopped.iter().copied().collect();
            let mut guard = self.nodes.lock().unwrap();
            for v in stopped {
                let idx = ids
                    .iter()
                    .position(|&x| x == v)
                    .expect("stopped id is a member of this group");
                guard[idx] = Arc::new(RaftNode::start(
                    self.sim.env(nid(v)),
                    all_ids.clone(),
                    self.engines[idx].clone(),
                ));
            }
            drop(guard);
            self.stopped.clear();
        }

        self.sim.set_net_config(NetConfig::default());
        self.sim.set_disk_config(DiskConfig::default());
    }
}

/// One schema-race proposer: repeatedly (re)proposes its own `(table,
/// schema)` pair against whichever node currently leads, until either its
/// own schema is durably visible (it won) or a DIFFERENT schema is already
/// durably visible for `table` (it lost — `CreateTableSchema` rejects
/// outright on an existing name, so there is nothing left to retry). Never
/// asserts anything itself: outcomes feed `Shared`, checked once by the
/// runner after the whole scenario settles (the same "indeterminate outcomes
/// are data, not an in-task assertion" discipline raftkv's `run_write`/
/// `run_read` follow).
async fn schema_race_client(
    env: SimEnv,
    nodes: Nodes,
    shared: Arc<Shared>,
    table: String,
    schema: TableSchema,
) {
    shared.record_schema_attempt(&table, &schema);
    let deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
    while env.now().0 < deadline {
        if let Some((_, node)) = leader_slot(&nodes) {
            node.propose(MetaCommand::CreateTableSchema {
                table: table.clone(),
                schema: schema.clone(),
            });
        }
        env.sleep(POLL).await;
        if let Some((_, node)) = leader_slot(&nodes) {
            let meta = node.metadata();
            match meta.table_schema(&table) {
                Some(existing) if *existing == schema => {
                    shared.confirm_schema(&table, &schema);
                    return;
                }
                Some(_) => return, // lost the race: a different schema already won
                None => {}
            }
        }
    }
}

/// One `PlainChurn`/`SustainedChurn` proposer: `rounds` distinct
/// `UpsertMember` proposals, each retried against the current leader and
/// confirmed via a subsequent read before moving to the next. `rounds` is
/// `CHURN_ROUNDS` for `PlainChurn` and `SUSTAINED_CHURN_ROUNDS` for
/// `SustainedChurn` (PR③) — same client logic, just how much of it runs.
async fn plain_churn_client(
    env: SimEnv,
    nodes: Nodes,
    shared: Arc<Shared>,
    base: u64,
    rounds: u64,
) {
    for i in 0..rounds {
        let member = nid(base + i);
        let cmd = MetaCommand::UpsertMember {
            node: member.clone(),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        };
        let deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
        let mut confirmed = false;
        while env.now().0 < deadline && !confirmed {
            if let Some((_, node)) = leader_slot(&nodes) {
                node.propose(cmd.clone());
            }
            env.sleep(POLL).await;
            if let Some((_, node)) = leader_slot(&nodes)
                && node.metadata().members.contains_key(&member)
            {
                confirmed = true;
            }
        }
        if confirmed {
            shared.confirm_member(member);
        }
    }
}

/// The one shared table name every `AllocatorRace` client races
/// `CreateTablet` against — see [`Workload::AllocatorRace`]'s own doc.
const ALLOCATOR_RACE_TABLE: &str = "ks.alloc_race";

/// One `AllocatorRace` proposer. Phase 1: repeatedly recomputes a candidate
/// tablet id from the current leader's own `next_free_tablet_id()` and
/// proposes `CreateTablet` for the ONE shared table (`ALLOCATOR_RACE_TABLE`)
/// — every racer's proposal is byte-identical except for the candidate id,
/// so whichever one commits establishes the shared parent for everyone
/// (there is no "whose literal call won" ambiguity to resolve, unlike phase
/// 2 below: content besides the id is identical by construction). A racer
/// only calls `confirm_tablet_id` when the tablet that landed carries its
/// OWN most recently proposed id. Phase 2 starts once ANY racer observes the
/// shared table has a tablet (own win or not): every racer repeatedly
/// recomputes a fresh `(left, right)` child id pair from the current
/// `next_free_tablet_id()` and proposes `BeginSplitInPlace` (ADR 0058 Train
/// 2 rung 3 — same monotonic-allocator floor and epoch-CAS discipline
/// `BeginSplit` used before the copy-split deletion stack's layer 1) against
/// the shared parent, with `p`'s own fixed, structurally distinct split key
/// (so a content check — not mere id presence — can tell a genuine own-win
/// apart from a same-id coincidence with a different racer's proposal, the
/// exact "confirm by content, not presence" discipline `schema_race_client`'s
/// own doc and `docs/engineering-lessons.md` already establish). Since
/// `BeginSplitInPlace` mints no tablet-map row for the children at all
/// (unlike `BeginSplit`'s `Building` rows), the content check reads the
/// PARENT's own recorded `inplace_split` intent instead of a child tablet's
/// `range` — see `check_allocator_injectivity`/`sample_tablets`'s own doc
/// for how the injectivity sampler was extended to still fingerprint these
/// never-materialized child ids. Stops the instant the parent leaves
/// `Active` (someone won; nothing left to retry) or the budget expires.
async fn allocator_race_client(
    env: SimEnv,
    nodes: Nodes,
    shared: Arc<Shared>,
    p: usize,
    replicas: Vec<NodeId>,
) {
    let table = ALLOCATOR_RACE_TABLE.to_string();
    let split_key = format!("k{p}").into_bytes();

    // --- Phase 1: race CreateTablet for the shared table. ---
    let phase1_deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
    let mut parent: Option<TabletId> = None;
    while env.now().0 < phase1_deadline && parent.is_none() {
        let Some((_, node)) = leader_slot(&nodes) else {
            env.sleep(POLL).await;
            continue;
        };
        let candidate = node.metadata().next_free_tablet_id();
        node.propose(MetaCommand::CreateTablet {
            tablet: candidate,
            table: Some(table.clone()),
            range: KeyRange::whole(),
            replicas: replicas.clone(),
        });
        env.sleep(POLL).await;
        if let Some((_, node)) = leader_slot(&nodes) {
            let meta = node.metadata();
            if let Some((&id, _)) = meta.tablets_for_table(&table).next() {
                if id == candidate {
                    shared.confirm_tablet_id(id);
                }
                parent = Some(id);
            }
        }
    }
    let Some(parent) = parent else {
        return; // the shared table never got a tablet within budget
    };

    // --- Phase 2: race BeginSplitInPlace against the shared parent. ---
    let phase2_deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
    while env.now().0 < phase2_deadline {
        let Some((_, node)) = leader_slot(&nodes) else {
            env.sleep(POLL).await;
            continue;
        };
        let meta = node.metadata();
        let Some(source) = meta.tablets.get(&parent) else {
            return; // the parent tablet is gone — nothing left to race
        };
        if source.state != animus_tablet::TabletState::Active {
            return; // some racer already won the split
        }
        let left = meta.next_free_tablet_id();
        let right = TabletId(left.0 + 1);
        let source_replicas = source.replicas.clone();
        node.propose(MetaCommand::BeginSplitInPlace {
            parent,
            expected_epoch: source.epoch,
            split_key: split_key.clone(),
            children: [(left, source_replicas.clone()), (right, source_replicas)],
        });
        env.sleep(POLL).await;
        if let Some((_, node)) = leader_slot(&nodes) {
            let meta = node.metadata();
            // Confirm by CONTENT, not presence: `BeginSplitInPlace` mints no
            // tablet-map row for `left`/`right` at all (unlike `BeginSplit`'s
            // `Building` rows), so the win check reads the PARENT's own
            // recorded intent instead — it must carry exactly THIS racer's
            // split key. A different racer whose proposal happened to
            // compute the identical (left, right) id pair from an equally
            // stale read (a real possibility early in the race, before
            // either has committed) used a DIFFERENT split key, so the
            // intent that actually landed would disagree with ours here.
            if meta
                .tablets
                .get(&parent)
                .and_then(|t| t.inplace_split.as_ref())
                .is_some_and(|intent| intent.split_key == split_key)
            {
                shared.confirm_tablet_id(left);
                shared.confirm_tablet_id(right);
                return;
            }
        }
    }
}

/// Deterministic address book for a `RegisterCas` client's claim, keyed off
/// `suffix` exactly like `register_node_cas.rs`'s own `register()` helper
/// (this workload's fixed-single-seed ancestor).
fn register_cas_addrs(suffix: u16) -> NodeAddrs {
    NodeAddrs {
        internal: format!("127.0.0.1:{}", 9300 + suffix),
        client: format!("127.0.0.1:{}", 9000 + suffix),
        admin: format!("127.0.0.1:{}", 9500 + suffix),
        intra: format!("127.0.0.1:{}", 9600 + suffix),
        role: "combined".to_string(),
    }
}

/// One `RegisterCas` client: claims `target` with its own address book,
/// retried against the current leader until durably confirmed (its own
/// address book, byte-identical, observed on a read after proposing — never
/// merely `ProposeResult::Accepted`). Once confirmed, makes exactly ONE
/// deterministic follow-up collision attempt: the SAME `target` id with a
/// DIFFERENT address book — the seed-depth generalization of
/// `register_node_cas.rs`'s
/// `a_different_registration_for_an_already_claimed_id_is_rejected`. That
/// second attempt can never confirm (the CAS must reject it), so it is only
/// retried a bounded number of rounds — enough for the rejection to settle
/// on whatever leader is currently reachable before the scenario's drain
/// window closes, not an indefinite wait for an outcome that will never
/// come.
async fn register_cas_client(
    env: SimEnv,
    nodes: Nodes,
    shared: Arc<Shared>,
    target: NodeId,
    suffix: u16,
) {
    let addrs = register_cas_addrs(suffix);
    shared.record_register_attempt(target.clone(), &addrs);
    let cmd = MetaCommand::RegisterNode {
        node: target.clone(),
        addrs: addrs.clone(),
        labels: BTreeMap::new(),
    };
    let deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
    let mut confirmed = false;
    while env.now().0 < deadline && !confirmed {
        if let Some((_, node)) = leader_slot(&nodes) {
            node.propose(cmd.clone());
        }
        env.sleep(POLL).await;
        if let Some((_, node)) = leader_slot(&nodes)
            && node.metadata().node_addrs.get(&target) == Some(&addrs)
        {
            confirmed = true;
        }
    }
    if !confirmed {
        return; // never landed — nothing to collide against
    }
    shared.confirm_registration(target.clone(), &addrs);

    // The deterministic collision: same id, a DIFFERENT address book.
    let colliding_addrs = register_cas_addrs(suffix + 1000);
    shared.record_register_attempt(target.clone(), &colliding_addrs);
    let collide_cmd = MetaCommand::RegisterNode {
        node: target.clone(),
        addrs: colliding_addrs,
        labels: BTreeMap::new(),
    };
    for _ in 0..5 {
        if let Some((_, node)) = leader_slot(&nodes) {
            node.propose(collide_cmd.clone());
        }
        env.sleep(POLL).await;
    }
}

// ---------------------------------------------------------------------------
// Checks. No `check_cycles` here (see this file's top doc for why) — plain,
// self-contained verdicts over `Metadata` equality/presence instead of
// `animus_test`'s Elle/list-append `CheckReport` machinery, which doesn't
// apply to this plane's command-log model.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Verdict {
    ok: bool,
    violations: Vec<String>,
}

fn verdict(violations: Vec<String>) -> Verdict {
    let ok = violations.is_empty();
    Verdict { ok, violations }
}

/// Invariant #1 (convergence): every replica's own applied-state cache
/// agrees with replica 0's.
fn check_convergence_meta(metas: &[Metadata]) -> Verdict {
    let mut violations = Vec::new();
    for (i, m) in metas.iter().enumerate().skip(1) {
        if m != &metas[0] {
            violations.push(format!("replica {i} metadata diverged from replica 0"));
        }
    }
    verdict(violations)
}

/// Invariant #2 (durability): every effect a proposer's own confirm loop
/// actually observed committed must still be present in `reference` (the
/// converged final state).
fn check_durability_meta(shared: &Shared, reference: &Metadata) -> Verdict {
    let mut violations = Vec::new();
    for (table, schema) in shared.confirmed_schemas.lock().unwrap().iter() {
        match reference.table_schema(table) {
            Some(existing) if existing == schema => {}
            Some(other) => violations.push(format!(
                "confirmed schema for {table} lost: final state holds a DIFFERENT schema \
                 ({other:?} != {schema:?})"
            )),
            None => violations.push(format!(
                "confirmed schema for {table} lost: absent from final state"
            )),
        }
    }
    for member in shared.confirmed_members.lock().unwrap().iter() {
        if !reference.members.contains_key(member) {
            violations.push(format!("confirmed member {member:?} lost from final state"));
        }
    }
    for id in shared.confirmed_tablet_ids.lock().unwrap().iter() {
        // Phase 1's shared-parent id always gets its own materialized
        // tablet-map row. Phase 2's split `left`/`right` ids never do under
        // `BeginSplitInPlace` — this workload only ever proposes
        // `BeginSplitInPlace`, never the `CutoverSplit` that would
        // materialize them — so "still present" for one of THOSE ids means
        // its own parent still carries an `inplace_split` intent naming it
        // (which, once won, is permanent for the life of this scenario: no
        // other `BeginSplitInPlace` can land on a non-`Active` parent, and
        // nothing here ever cuts over).
        let still_present = reference.tablets.contains_key(id)
            || reference.tablets.values().any(|t| {
                t.inplace_split
                    .as_ref()
                    .is_some_and(|intent| intent.children.iter().any(|c| c.id == *id))
            });
        if !still_present {
            violations.push(format!(
                "confirmed tablet id {id:?} (AllocatorRace) lost from final state"
            ));
        }
    }
    for (node, addrs) in shared.confirmed_registrations.lock().unwrap().iter() {
        match reference.node_addrs.get(node) {
            Some(existing) if existing == addrs => {}
            Some(other) => violations.push(format!(
                "confirmed registration for {node:?} lost: final state holds a DIFFERENT \
                 address book ({other:?} != {addrs:?})"
            )),
            None => violations.push(format!(
                "confirmed registration for {node:?} lost: absent from final state"
            )),
        }
    }
    verdict(violations)
}

/// Invariant #3 (schema-catalog exclusivity, checked unconditionally — see
/// this file's top doc for the full argument). Groups every attempted
/// `CreateTableSchema` by table name; a name only one proposer ever
/// attempted has nothing to check here (durability above already covers
/// it). For a name **two or more** proposers raced: on every replica, the
/// table's schema (if present) must byte-match exactly one of the racing
/// attempts, and must be present if any attempt was ever durably confirmed.
fn check_schema_exclusivity(shared: &Shared, metas: &[Metadata]) -> Verdict {
    let attempts = shared.schema_attempts.lock().unwrap();
    let confirmed_tables: BTreeSet<String> = shared
        .confirmed_schemas
        .lock()
        .unwrap()
        .iter()
        .map(|(t, _)| t.clone())
        .collect();
    let mut by_table: BTreeMap<&str, Vec<&TableSchema>> = BTreeMap::new();
    for (table, schema) in attempts.iter() {
        by_table.entry(table.as_str()).or_default().push(schema);
    }

    let mut violations = Vec::new();
    for (table, schemas) in &by_table {
        if schemas.len() < 2 {
            continue; // no race on this table name
        }
        for (i, meta) in metas.iter().enumerate() {
            match meta.table_schema(table) {
                None => {
                    if confirmed_tables.contains(*table) {
                        violations.push(format!(
                            "table {table} raced by {} proposers but ABSENT on replica {i}, \
                             though a racing schema was durably confirmed",
                            schemas.len()
                        ));
                    }
                }
                Some(winner) => {
                    if !schemas.contains(&winner) {
                        violations.push(format!(
                            "table {table} on replica {i} holds a schema matching NONE of the \
                             {} racing proposals (a hybrid/corrupted result): {winner:?}",
                            schemas.len()
                        ));
                    }
                }
            }
        }
    }
    verdict(violations)
}

/// Invariant #4 (allocator injectivity — `AllocatorRace`): every violation
/// `Shared::sample_tablets` ever recorded while the scenario ran — a
/// `TabletId` observed with two disagreeing identities (table/range) at
/// different sample points, catching a transient double-assignment even if
/// a later poll "corrected" it back — see `sample_tablets`'s own doc.
///
/// **Deliberately not** a second check over raw `confirmed_tablet_ids`
/// pairwise distinctness: phase 1's `CreateTablet` proposals are
/// content-identical across every racer except for the candidate id itself
/// (same shared table, range, replicas — see `allocator_race_client`'s own
/// doc), so it is entirely legitimate for TWO OR MORE racers who happened
/// to read the same stale `next_free_tablet_id()` to each correctly
/// recognize "the tablet that now exists carries my own candidate id" —
/// that is one real assignment multiply (and correctly) confirmed, not a
/// double-assignment. `sample_tablets`' fingerprint comparison is the
/// content-aware check that actually distinguishes a benign shared
/// confirmation from a genuine same-id-different-content collision, so it
/// alone is invariant #4's teeth.
fn check_allocator_injectivity(shared: &Shared) -> Verdict {
    verdict(shared.injectivity_violations.lock().unwrap().clone())
}

/// `RegisterCas` integrity, mirroring `check_schema_exclusivity`'s shape
/// over `Metadata::node_addrs`/`RegisterNode` instead of
/// `Metadata::schemas`/`CreateTableSchema` — the CAS is
/// idempotent-on-identical rather than first-committer-wins-outright, but
/// the "never a hybrid, never absent after a confirmed win" shape is
/// identical. Groups every attempted registration by node id; an id only
/// one attempt ever targeted has nothing to check here (durability above
/// already covers it). For an id with **two or more** DIFFERING attempted
/// address books (this workload's own deterministic collision, but written
/// generally): on every replica, the id's address book (if present) must
/// byte-match exactly one of the attempts, and must be present if any
/// attempt for it was ever durably confirmed.
fn check_register_cas_integrity(shared: &Shared, metas: &[Metadata]) -> Verdict {
    let attempts = shared.register_attempts.lock().unwrap();
    let confirmed_nodes: BTreeSet<NodeId> = shared
        .confirmed_registrations
        .lock()
        .unwrap()
        .iter()
        .map(|(n, _)| n.clone())
        .collect();
    let mut by_node: BTreeMap<NodeId, Vec<&NodeAddrs>> = BTreeMap::new();
    for (node, addrs) in attempts.iter() {
        let bucket = by_node.entry(node.clone()).or_default();
        if !bucket.iter().any(|a| **a == *addrs) {
            bucket.push(addrs);
        }
    }

    let mut violations = Vec::new();
    for (node, addr_books) in &by_node {
        if addr_books.len() < 2 {
            continue; // no genuine collision attempted for this id
        }
        for (i, meta) in metas.iter().enumerate() {
            match meta.node_addrs.get(node) {
                None => {
                    if confirmed_nodes.contains(node) {
                        violations.push(format!(
                            "node {node:?} raced by {} differing registrations but ABSENT on \
                             replica {i}, though one was durably confirmed",
                            addr_books.len()
                        ));
                    }
                }
                Some(winner) => {
                    if !addr_books.contains(&winner) {
                        violations.push(format!(
                            "node {node:?} on replica {i} holds an address book matching NONE \
                             of the {} attempted registrations (a hybrid/corrupted result): \
                             {winner:?}",
                            addr_books.len()
                        ));
                    }
                }
            }
        }
    }
    verdict(violations)
}

/// Invariant #5 (PR③, apply-task liveness / no-permanent-stall — ADR 0038).
/// Checked on **every** scenario (cheap: two atomic reads per node, no
/// locking beyond the existing `nodes` mutex) rather than only
/// `StopRestart`-using ones: after (would-be) convergence,
/// [`RaftNode::engine_applied_index`] must catch up to
/// [`RaftNode::commit_index`] on every live replica — the async apply task
/// (`meta_apply_loop`/`meta_apply_and_compact`) must never permanently stall
/// behind consensus. This is a genuinely **separate** property from
/// `check_convergence_meta`: a uniformly-stalled apply task (every replica
/// stuck at the same stale-but-consistent `Metadata`) still looks
/// "converged" to that check, since it only compares replicas against each
/// other, never against the group's own `commit_index`. Especially relevant
/// post-`StopRestart`: a freshly reconstructed node's apply task reseeds its
/// watermark from the engine's own `syskv::applied_index_key()`, not
/// `core.last_applied()` (`meta_apply_loop`'s own doc) — a bug in that
/// reseed (e.g. losing the watermark, or re-deriving writes for an index the
/// engine already durably reflects) would show up here as a permanent gap,
/// not as a `Metadata` mismatch.
///
/// A converged-or-timeout poll of its own, mirroring `check_convergence_meta`'s
/// shape exactly (same constants), since a node whose apply task is merely
/// still catching up — not stalled — needs the same grace ordinary
/// cross-replica convergence gets. `sim` is an owned `Simulator` clone (see
/// `animus-sim`'s own doc: cloning hands out another handle to the SAME
/// shared world, not a fork) so this can advance virtual time without
/// needing `&mut Group`.
fn poll_apply_task_caught_up(nodes: &Nodes, mut sim: Simulator) -> Verdict {
    let check = || -> Verdict {
        let mut violations = Vec::new();
        for (i, n) in nodes.lock().unwrap().iter().enumerate() {
            let applied = n.engine_applied_index();
            let commit = n.commit_index();
            if applied < commit {
                violations.push(format!(
                    "replica {i} apply task behind consensus: engine_applied_index={applied} \
                     < commit_index={commit}"
                ));
            }
        }
        verdict(violations)
    };
    let mut v = check();
    let poll_deadline = sim.now().0 + CONVERGENCE_BUDGET.as_nanos() as u64;
    while !v.ok && sim.now().0 < poll_deadline {
        sim.run_for(CONVERGENCE_POLL_STEP);
        v = check();
    }
    v
}

// ---------------------------------------------------------------------------
// The scenario runner + result.
// ---------------------------------------------------------------------------

struct ScenarioResult {
    convergence: Verdict,
    durability: Verdict,
    exclusivity: Verdict,
    allocator_injectivity: Verdict,
    register_cas_integrity: Verdict,
    apply_task_progress: Verdict,
    final_metas: Vec<Metadata>,
    schema_attempts: Vec<(String, TableSchema)>,
    confirmed_count: usize,
}

fn read_all_metadata(nodes: &Nodes) -> Vec<Metadata> {
    nodes.lock().unwrap().iter().map(|n| n.metadata()).collect()
}

/// Converged-or-timeout poll for cross-replica `Metadata` agreement — the
/// same shape/constants `run_scenario`'s own inline loop uses, factored out
/// here so the bespoke chunked-snapshot tests (PR③, near the bottom of this
/// file — they don't go through `run_scenario`/`Workload` at all, since their
/// setup needs to target one SPECIFIC follower rather than a generic
/// `Nemesis`-selected victim) can reuse it instead of re-deriving the poll
/// loop. Doesn't assert; the caller checks `check_convergence_meta` on the
/// result, exactly like `run_scenario` does.
fn converge_or_timeout(group: &Group) -> Vec<Metadata> {
    let mut sim = group.sim.clone();
    let mut metas = read_all_metadata(&group.nodes);
    let mut convergence = check_convergence_meta(&metas);
    let poll_deadline = sim.now().0 + CONVERGENCE_BUDGET.as_nanos() as u64;
    while !convergence.ok && sim.now().0 < poll_deadline {
        sim.run_for(CONVERGENCE_POLL_STEP);
        metas = read_all_metadata(&group.nodes);
        convergence = check_convergence_meta(&metas);
    }
    metas
}

fn run_scenario(scenario: &Scenario) -> ScenarioResult {
    let mut group = Group::start(scenario.seed, scenario.replicas);

    // Let the group elect a leader, then start the concurrent workload.
    group.sim.run_for(SETTLE);
    group.spawn_workload(&scenario.workload);
    group
        .shared
        .sample_tablets(&read_all_metadata(&group.nodes));

    // Walk the fault schedule in virtual-time order, sampling the tablet map
    // at every step (invariant #4 wants samples throughout the run, not
    // just at the end — see `Shared::sample_tablets`'s own doc — and every
    // scenario already pays for a metadata read here regardless of
    // workload, so this is free for non-`AllocatorRace` scenarios).
    let mut faults = scenario.faults.clone();
    faults.sort_by_key(|(at, _)| *at);
    let base = group.sim.now().0;
    for (at, nem) in faults {
        let target = base + at.as_nanos() as u64;
        if target > group.sim.now().0 {
            group.sim.run_until(animus_env::Nanos(target));
        }
        group
            .shared
            .sample_tablets(&read_all_metadata(&group.nodes));
        group.apply(nem);
    }

    // Hold the last fault open for the scenario's outage window (zero for
    // every cell in this PR — see `Scenario::window`'s own doc).
    if !scenario.window.is_zero() {
        group.sim.run_for(scenario.window);
    }

    // End healthy so the workload tail + final reads can make a quorum.
    group.heal_all();
    group.sim.run_for(DRAIN);

    // Converged-or-timeout poll for cross-replica agreement: a lagging
    // replica may still be catching up at the fixed drain, so re-read in
    // bounded increments and stop early once convergence holds. Each poll
    // also feeds `sample_tablets`.
    let mut metas = read_all_metadata(&group.nodes);
    group.shared.sample_tablets(&metas);
    let mut convergence = check_convergence_meta(&metas);
    let poll_deadline = group.sim.now().0 + CONVERGENCE_BUDGET.as_nanos() as u64;
    while !convergence.ok && group.sim.now().0 < poll_deadline {
        group.sim.run_for(CONVERGENCE_POLL_STEP);
        metas = read_all_metadata(&group.nodes);
        group.shared.sample_tablets(&metas);
        convergence = check_convergence_meta(&metas);
    }

    let durability = check_durability_meta(&group.shared, &metas[0]);
    let exclusivity = check_schema_exclusivity(&group.shared, &metas);
    let allocator_injectivity = check_allocator_injectivity(&group.shared);
    let register_cas_integrity = check_register_cas_integrity(&group.shared, &metas);
    let apply_task_progress = poll_apply_task_caught_up(&group.nodes, group.sim.clone());
    let schema_attempts = group.shared.schema_attempts.lock().unwrap().clone();
    let confirmed_count = group.shared.confirmed_count();

    ScenarioResult {
        convergence,
        durability,
        exclusivity,
        allocator_injectivity,
        register_cas_integrity,
        apply_task_progress,
        final_metas: metas,
        schema_attempts,
        confirmed_count,
    }
}

/// Assert all checks on one scenario result, labelling the scenario in the
/// failure message. Exclusivity/convergence/allocator-injectivity/
/// register-CAS-integrity are **safety** properties (hard assert at any
/// depth); durability is already behind the converged-or-timeout poll, so a
/// failure there means the budget was genuinely exhausted.
fn assert_scenario_ok(s: &Scenario, r: &ScenarioResult) {
    assert!(
        r.convergence.ok,
        "scenario {} did not converge: {:?} (seed={})",
        s.name, r.convergence.violations, s.seed
    );
    assert!(
        r.durability.ok,
        "scenario {} lost a confirmed effect: {:?} (seed={})",
        s.name, r.durability.violations, s.seed
    );
    assert!(
        r.exclusivity.ok,
        "scenario {} violated schema-catalog exclusivity: {:?} (seed={})",
        s.name, r.exclusivity.violations, s.seed
    );
    assert!(
        r.allocator_injectivity.ok,
        "scenario {} violated allocator injectivity: {:?} (seed={})",
        s.name, r.allocator_injectivity.violations, s.seed
    );
    assert!(
        r.register_cas_integrity.ok,
        "scenario {} violated RegisterNode CAS integrity: {:?} (seed={})",
        s.name, r.register_cas_integrity.violations, s.seed
    );
    assert!(
        r.apply_task_progress.ok,
        "scenario {} apply task stalled behind consensus: {:?} (seed={})",
        s.name, r.apply_task_progress.violations, s.seed
    );
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[test]
fn control_plain_churn_baseline_converges() {
    let scenario = corpus_cells()
        .into_iter()
        .find(|s| s.name == "baseline_3")
        .expect("baseline_3 exists");
    let r = run_scenario(&scenario);
    assert_scenario_ok(&scenario, &r);
    assert!(
        r.confirmed_count > 0,
        "no confirmed churn — vacuous run (seed={})",
        scenario.seed
    );
}

#[test]
fn control_schema_race_baseline_holds_exclusivity() {
    let scenario = corpus_cells()
        .into_iter()
        .find(|s| s.name == "schema_race_baseline_3")
        .expect("schema_race_baseline_3 exists");
    let r = run_scenario(&scenario);
    assert_scenario_ok(&scenario, &r);
    assert!(
        r.confirmed_count > 0,
        "no confirmed schema — vacuous run (seed={})",
        scenario.seed
    );
    // Teeth: this must actually have been a race (>= 2 attempts on the one
    // shared table name), or the exclusivity check above was vacuous.
    let attempts_on_shared = r
        .schema_attempts
        .iter()
        .filter(|(t, _)| t == "ks.race")
        .count();
    assert!(
        attempts_on_shared >= 2,
        "workload did not actually race the same table name (seed={})",
        scenario.seed
    );
}

#[test]
fn control_corpus_is_convergent_and_durable() {
    let scenarios = corpus();
    let mut total_confirmed = 0usize;
    let mut faulted_with_confirms = 0usize;
    let mut faulted_total = 0usize;
    for s in &scenarios {
        let r = run_scenario(s);
        assert_scenario_ok(s, &r);
        total_confirmed += r.confirmed_count;
        if !s.faults.is_empty() {
            faulted_total += 1;
            if r.confirmed_count > 0 {
                faulted_with_confirms += 1;
            }
        }
    }
    // Non-vacuity guards: the corpus as a whole did real, fault-tolerant
    // work — mirrors raftkv's identical guard shape.
    assert!(
        total_confirmed > 0,
        "corpus too vacuous: no confirmed effects across {} scenarios",
        scenarios.len()
    );
    assert!(
        faulted_with_confirms >= faulted_total / 2,
        "too few faulted scenarios kept making confirmed progress \
         ({faulted_with_confirms}/{faulted_total}) — faults may be downing the group entirely"
    );
}

/// Coverage guard, mirroring `raftkv_corpus_covers_the_fault_matrix`: the
/// generator must keep exercising every fault class this PR's `Nemesis`
/// vocabulary defines, both workload shapes, and both group sizes —
/// otherwise a dimension silently stopped being tested. Structural only (no
/// scenario runs).
#[test]
fn control_corpus_covers_the_fault_matrix() {
    let cells = corpus_cells();

    let mut seen_faults: BTreeSet<Nemesis> = BTreeSet::new();
    let mut seen_workloads: BTreeSet<&str> = BTreeSet::new();
    let mut seen_shapes: BTreeSet<usize> = BTreeSet::new();
    let mut baselines = 0usize;
    for s in &cells {
        seen_shapes.insert(s.replicas);
        if s.faults.is_empty() {
            baselines += 1;
        }
        for (_, f) in &s.faults {
            seen_faults.insert(*f);
        }
        seen_workloads.insert(match s.workload {
            Workload::SchemaRace { .. } => "schema_race",
            Workload::PlainChurn { .. } => "plain_churn",
            Workload::AllocatorRace { .. } => "allocator_race",
            Workload::RegisterCas { .. } => "register_cas",
            Workload::SustainedChurn { .. } => "sustained_churn",
        });
    }

    // Every `Nemesis` variant must appear in some asserted cell — EXCEPT
    // there is no `Nemesis::CorruptOnCrash` to enumerate here at all (see
    // this file's top doc for why: issue #495, the composition is
    // deliberately confined to the dedicated `#[ignore]`d
    // `control_corrupt_on_crash_may_hard_panic_issue_495` test below, which
    // this guard must never require to be part of the asserted set).
    for f in [
        Nemesis::LeaderKill,
        Nemesis::FollowerKill,
        Nemesis::PartitionLeader,
        Nemesis::SplitBrain,
        Nemesis::Lossy,
        Nemesis::Duplicate,
        Nemesis::FsyncLie,
        Nemesis::TornTail,
        Nemesis::StopRestart,
    ] {
        assert!(
            seen_faults.contains(&f),
            "fault {f:?} is not covered by any corpus scenario"
        );
    }
    for w in [
        "schema_race",
        "plain_churn",
        "allocator_race",
        "register_cas",
        "sustained_churn",
    ] {
        assert!(
            seen_workloads.contains(w),
            "workload {w} is not covered by any corpus scenario"
        );
    }
    assert!(
        seen_shapes.contains(&3) && seen_shapes.contains(&5),
        "both 3- and 5-replica shapes must be covered: {seen_shapes:?}"
    );
    assert!(baselines >= 2, "expected >= 2 no-fault baselines");

    let names: BTreeSet<&str> = cells.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names.len(), cells.len(), "corpus names must be unique");
    let seeds: BTreeSet<u64> = cells.iter().map(|s| s.seed).collect();
    assert_eq!(seeds.len(), cells.len(), "corpus seeds must be unique");
}

/// Seed-depth lever (`ANIMUS_CONTROL_SEEDS`): expanding the cells by `k`
/// yields exactly `k×` scenarios, names/seeds stay unique, and **variant 0
/// preserves the canonical (frozen) name+seed**. Structural only.
#[test]
fn control_seed_expansion_is_additive_and_unique() {
    let base = corpus_cells();
    let k = 3;
    let expanded = corpus::seed_expand(base.clone(), k);
    assert_eq!(expanded.len(), base.len() * k);

    let names: BTreeSet<&str> = expanded.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names.len(), expanded.len(), "expanded names must be unique");
    let seeds: BTreeSet<u64> = expanded.iter().map(|s| s.seed).collect();
    assert_eq!(seeds.len(), expanded.len(), "expanded seeds must be unique");

    for b in &base {
        let kept = expanded
            .iter()
            .find(|s| s.name == b.name)
            .unwrap_or_else(|| panic!("base scenario {} missing after expansion", b.name));
        assert_eq!(kept.seed, b.seed, "seed moved for {}", b.name);
    }
    // k == 1 is the identity.
    assert_eq!(corpus::seed_expand(base.clone(), 1).len(), base.len());
}

#[test]
fn control_run_is_deterministic() {
    // Same scenario twice → byte-identical final metadata and attempt log
    // (ADR 0003).
    let scenario = schema_race_scenario(
        "determinism_check",
        3,
        2,
        true,
        vec![(Duration::from_millis(2200), Nemesis::LeaderKill)],
    );
    let a = run_scenario(&scenario);
    let b = run_scenario(&scenario);
    assert_eq!(
        a.final_metas, b.final_metas,
        "final metadata not reproducible for seed {}",
        scenario.seed
    );
    assert_eq!(
        a.schema_attempts, b.schema_attempts,
        "schema-attempt log not reproducible for seed {}",
        scenario.seed
    );
}

#[test]
fn control_allocator_race_baseline_is_injective() {
    let scenario = corpus_cells()
        .into_iter()
        .find(|s| s.name == "allocator_race_baseline_3")
        .expect("allocator_race_baseline_3 exists");
    let r = run_scenario(&scenario);
    assert_scenario_ok(&scenario, &r);
    assert!(
        r.confirmed_count > 0,
        "no confirmed tablet id — vacuous run (seed={})",
        scenario.seed
    );
}

#[test]
fn control_register_cas_baseline_holds_integrity() {
    let scenario = corpus_cells()
        .into_iter()
        .find(|s| s.name == "register_cas_baseline_3")
        .expect("register_cas_baseline_3 exists");
    let r = run_scenario(&scenario);
    assert_scenario_ok(&scenario, &r);
    assert!(
        r.confirmed_count > 0,
        "no confirmed registration — vacuous run (seed={})",
        scenario.seed
    );
}

/// **Deliberately `#[ignore]`d — exercises the same shared-WAL-codec
/// composition that used to trigger issue #495**, `animus-cp-data`'s
/// confirmed, reproducible hard-panic when `DiskConfig::torn_tail_on_crash`
/// was composed with `corrupt_on_crash`: the shared codec
/// (`animus-control::persist::WalRecord`) used to have no per-record
/// checksum, so a corrupted-but-still-JSON-valid record decoded
/// successfully with a wrong value instead of failing gracefully like a
/// torn/unparseable record does. #495 confirmed the panic via
/// `animus-cp-data/tests/quiescence.rs`'s `assert_ts_monotonic` — a
/// downstream invariant over HLC timestamps that plane's `KvState` machine
/// has and this one does not. **Fixed**: every WAL line now carries a
/// per-record CRC32 checksum (`animus-control::persist`); a mismatch is
/// dropped exactly like a torn tail, never decoded into a value.
///
/// **This test's own result, run against this plane** (default single seed
/// below, plus an 80-combination sweep — seeds × `PlainChurn`/
/// `AllocatorRace` × `LeaderKill`/`FollowerKill` × with/without an
/// `FsyncLie`-accumulated un-synced buffer before the crash — done during
/// this test's own development and not committed as code): **no panic
/// reproduced anywhere in this plane, even before the fix.** The
/// corruption fired (`DiskCorrupt`/`DiskTear` trace events were emitted,
/// confirmed by inspection during development) but `Metadata::apply`/the
/// recovery path have no invariant as strict as `assert_ts_monotonic` for a
/// wrong-but-decodable numeric field to trip — this plane's commands carry
/// no HLC timestamp at all, and its CAS/epoch checks reject a mismatch
/// rather than asserting on one. That negative result stays useful in its
/// own right (see #495 and this session's PR② report), and this test stays
/// as a standing regression probe: with the codec now fixed, a corrupted
/// record is simply dropped (along with anything recorded after it) rather
/// than silently mis-decoded, so this cell should stay clean regardless —
/// but it remains the one place that would notice a future `MetaCommand`
/// field or replay-path invariant becoming strict enough to care about a
/// dropped tail-of-log the way `assert_ts_monotonic` cares about a
/// wrong-valued one.
///
/// Deliberately NOT a `Nemesis` variant (see `Nemesis`'s own doc: adding
/// `CorruptOnCrash` there would put this composition one cell away from
/// running in the normal, asserted suite) and NOT in `corpus_cells()` — a
/// hard process abort cannot be an ordinary scenario assertion, so this
/// test drives a `Group` directly instead of going through
/// `run_scenario`/`assert_scenario_ok`.
///
/// Run explicitly: `cargo test -p animus-control --test control_corpus
/// control_corrupt_on_crash_may_hard_panic_issue_495 -- --ignored`.
#[test]
#[ignore = "standing regression probe for the WAL-corruption composition tracked by issue #495 (fixed by a per-record checksum) — see this test's own doc"]
fn control_corrupt_on_crash_may_hard_panic_issue_495() {
    let seed = corpus::name_seed("corrupt_on_crash_issue_495");
    let mut group = Group::start(seed, 3);
    group.sim.run_for(SETTLE);
    group.spawn_workload(&Workload::PlainChurn { proposers: 3 });
    group.sim.run_for(Duration::from_millis(1500));

    // Arm BOTH torn-tail and corrupt-on-crash — the exact composition #495
    // isolated as the trigger (torn-tail alone converges cleanly).
    let mut cfg = DiskConfig::default();
    cfg.torn_tail_on_crash = true;
    cfg.corrupt_on_crash = true;
    group.sim.set_disk_config(cfg);

    group.apply(Nemesis::LeaderKill);
    group.sim.run_for(Duration::from_millis(500));
    // Restarting replays the crashed node's (possibly corrupted) WAL — with
    // the checksum fix, a corrupted record is now dropped at decode time
    // (never applied), so this line runs clean regardless (see this test's
    // own doc above for why it always ran clean in this plane anyway).
    group.heal_all();
    group.sim.run_for(DRAIN);

    let _ = read_all_metadata(&group.nodes);
}

// ---------------------------------------------------------------------------
// Chunked-snapshot-under-fault (PR③).
//
// `install_snapshot.rs` already has a genuine multi-chunk transfer test
// (`follower_catches_up_via_multi_chunk_snapshot`, using a synthetic image)
// and a re-ship-after-catch-up regression, but neither composes a mid-
// transfer fault. These two tests do, over a REAL `Metadata` image grown
// through the actual `meta_apply_and_compact`/`syskv_image` path (never a
// hand-supplied synthetic blob) — driving real `RaftNode`s under `Simulator`,
// mirroring `partitioned_follower_catches_up_via_install_snapshot`'s own
// end-to-end setup rather than `install_snapshot.rs`'s hand-driven
// `RaftCore`-level tests.
//
// **Timing.** This harness's usual fault schedule
// (`Scenario::faults: Vec<(Duration, Nemesis)>`) is far too coarse for this:
// an exploratory run (this PR's own development, not committed as code)
// found the WHOLE multi-chunk transfer — first chunk received through fully
// reassembled — completes within roughly 3ms of virtual time once the
// leader starts shipping (no artificial per-chunk delay in this plane's
// replication path), while a fixed `Duration` schedule targets a fault to
// land within an early/mid/late window measured in *seconds*. So instead of
// guessing a duration, `wait_for_snapshot_transfer_in_flight` **polls** at a
// fine (200µs) step until the receiving follower has demonstrably started
// (`snapshot_index() > 0`, the core has the base index from at least one
// received chunk) but not finished (`members` not yet fully reassembled) —
// a condition-based wait, not a duration guess, so it lands inside the real
// window regardless of exactly how many microseconds of virtual time that
// window occupies for a given seed. Both cells assert they actually caught
// the window (not raced past it) before injecting their fault, so a future
// change that made the transfer instantaneous (0 virtual time) would fail
// loudly here instead of silently degrading into a fault-free no-op cell.
// ---------------------------------------------------------------------------

/// Enough real `UpsertMember` commits to force a genuine multi-chunk
/// `InstallSnapshot` transfer through the REAL `meta_apply_and_compact`/
/// `syskv_image` engine-scan path — this PR's own exploratory run measured
/// a resulting several-hundred-KB syskv image at this count, many multiples
/// of `SNAPSHOT_CHUNK_BYTES` (1024 bytes).
const CHUNKED_SNAPSHOT_MEMBERS: u64 = 400;

/// Growth-load command for the chunked-snapshot tests below — a distinct,
/// high node-id range so it can never collide with any other workload's own
/// member-id range in this file (not load-bearing here, since these tests
/// never run concurrently with another workload, but costs nothing and
/// matches this file's existing convention of disjoint ranges per
/// workload).
fn chunk_growth_member(i: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(50_000 + i),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

/// Shared setup for both chunked-snapshot cells: elect, isolate ONE follower
/// from both other replicas (before any compaction touches it — mirrors
/// `partitioned_follower_catches_up_via_install_snapshot`'s own setup, one
/// specific follower rather than a generic `Nemesis`-selected victim, since
/// these tests need to know exactly which replica will receive the transfer),
/// grow real state on the healthy majority (leader + the third replica) past
/// several multiples of `SNAPSHOT_CHUNK_BYTES`, then heal the isolation so
/// the leader starts shipping `InstallSnapshot` chunks. Returns the running
/// `Group` plus the leader/follower/third-replica indices at the moment of
/// return (the leader index is which replica *started* the transfer — a
/// caller injecting `LeaderKill`-shaped source crash below is what may
/// change who leads next).
fn setup_chunked_snapshot_transfer(seed: u64) -> (Group, usize, usize, usize) {
    let mut group = Group::start(seed, 3);
    group.sim.run_for(SETTLE);
    let ids: Vec<u64> = GROUP_IDS[..3].to_vec();
    let (li, _) = leader_slot(&group.nodes).expect("group elected a leader");
    let leader_id = nid(ids[li]);
    let follower_idx = (0..3).find(|&i| i != li).expect("a follower exists");
    let follower_id = nid(ids[follower_idx]);
    let other_idx = (0..3)
        .find(|&i| i != li && i != follower_idx)
        .expect("a third replica exists");
    let other_id = nid(ids[other_idx]);

    group
        .sim
        .partition_pair(follower_id.clone(), leader_id.clone());
    group
        .sim
        .partition_pair(follower_id.clone(), other_id.clone());

    for i in 0..CHUNKED_SNAPSHOT_MEMBERS {
        if let Some((_, leader)) = leader_slot(&group.nodes) {
            leader.propose(chunk_growth_member(i));
        }
    }
    // Let the healthy majority (leader + third replica) commit, apply, and
    // compact — the isolated follower learns nothing.
    group.sim.run_for(Duration::from_secs(3));

    group.sim.heal(follower_id.clone(), leader_id);
    group.sim.heal(follower_id, other_id);

    (group, li, follower_idx, other_idx)
}

/// Poll in fine (`STEP`) increments of virtual time until the receiving
/// follower has demonstrably started but not finished its `InstallSnapshot`
/// transfer — see this section's own top doc for why a fixed-duration
/// schedule can't reliably land inside this window. Returns `true` if it
/// caught the window; `false` if the transfer either never started or had
/// already fully completed by `max_wait` (a caller must treat `false` as a
/// failure to exercise the fault window, not silently proceed).
fn wait_for_snapshot_transfer_in_flight(
    group: &Group,
    follower_idx: usize,
    target_members: usize,
    max_wait: Duration,
) -> bool {
    const STEP: Duration = Duration::from_micros(200);
    let mut sim = group.sim.clone();
    let deadline = sim.now().0 + max_wait.as_nanos() as u64;
    while sim.now().0 < deadline {
        let (started, done) = {
            let nodes = group.nodes.lock().unwrap();
            let f = &nodes[follower_idx];
            (
                f.snapshot_index() > 0,
                f.metadata().members.len() >= target_members,
            )
        };
        if started && !done {
            return true;
        }
        if done {
            return false; // raced past the window before this poll caught it
        }
        sim.run_for(STEP);
    }
    false
}

/// Isolate a follower before compaction, grow real `Metadata` past
/// `SNAPSHOT_CHUNK_BYTES`'s multi-chunk threshold, heal, and — while chunks
/// are demonstrably still in flight to the follower — crash the SOURCE
/// leader (`Nemesis::LeaderKill`'s own `sim.crash`, muted-and-resumable, not
/// `StopRestart`: what matters here is that the group re-elects and a
/// DIFFERENT node becomes leader mid-transfer, not that the old leader's
/// engine specifically survives a real process exit — `chunked_snapshot_
/// receiver_stop_restart_3` below is the cell that exercises a genuine
/// engine-reopening restart). Proves the "lazy, on-demand, dropped-when-idle"
/// snapshot-blob contract (`node.rs`'s `meta_apply_and_compact` doc) survives
/// a leadership change mid-transfer: the fresh leader must rebuild its own
/// image on demand and resume shipping correctly rather than leaving the
/// follower stuck on a base it can never complete. No existing fixed test in
/// this crate covers this composition.
#[test]
fn chunked_snapshot_source_crash_mid_transfer_3() {
    let seed = corpus::name_seed("chunked_snapshot_source_crash_mid_transfer_3");
    let (mut group, _li, follower_idx, _other_idx) = setup_chunked_snapshot_transfer(seed);

    let caught = wait_for_snapshot_transfer_in_flight(
        &group,
        follower_idx,
        CHUNKED_SNAPSHOT_MEMBERS as usize,
        Duration::from_secs(2),
    );
    assert!(
        caught,
        "never caught the InstallSnapshot transfer in flight — the fault \
         window was missed entirely (seed={seed})"
    );

    // Crash the CURRENT source leader mid-transfer (`Nemesis::LeaderKill`'s
    // own selection re-derives the leader fresh, so this is correct even if
    // it's no longer `ids[li]` for some other reason by this point).
    group.apply(Nemesis::LeaderKill);

    group.sim.run_for(Duration::from_secs(4)); // re-elect + fresh leader resumes shipping
    group.heal_all();
    group.sim.run_for(DRAIN);

    let metas = converge_or_timeout(&group);
    let convergence = check_convergence_meta(&metas);
    assert!(
        convergence.ok,
        "did not converge after a source-leader crash mid-InstallSnapshot-transfer: {:?} \
         (seed={seed})",
        convergence.violations
    );
    assert_eq!(
        metas[0].members.len(),
        CHUNKED_SNAPSHOT_MEMBERS as usize,
        "converged state lost members across the mid-transfer leader crash (seed={seed})"
    );
    let apply_progress = poll_apply_task_caught_up(&group.nodes, group.sim.clone());
    assert!(
        apply_progress.ok,
        "apply task stalled after a mid-transfer source-leader crash: {:?} (seed={seed})",
        apply_progress.violations
    );
}

/// Same setup as the source-crash cell above, but `StopRestart`s the
/// RECEIVING follower itself mid-chunk-transfer instead of the source
/// leader: a genuine `sim.stop` (tasks + volatile state, including
/// whatever partial chunks `RaftCore` had assembled in memory, all gone) +
/// fresh `RaftNode::start` reopening the SAME retained engine handle.
/// Proves a partially-received image is safely discarded on a real restart
/// — the reconstructed node starts from a clean `meta_apply_loop` rebuild
/// (`rebuild_metadata_from_engine` + the engine's own `syskv::
/// applied_index_key()` watermark) and simply re-requests the snapshot from
/// scratch, rather than the partial in-flight chunk state ever leaking into
/// `shadow`/`cache`.
#[test]
fn chunked_snapshot_receiver_stop_restart_3() {
    let seed = corpus::name_seed("chunked_snapshot_receiver_stop_restart_3");
    let (mut group, _li, follower_idx, _other_idx) = setup_chunked_snapshot_transfer(seed);
    let ids: Vec<u64> = GROUP_IDS[..3].to_vec();

    let caught = wait_for_snapshot_transfer_in_flight(
        &group,
        follower_idx,
        CHUNKED_SNAPSHOT_MEMBERS as usize,
        Duration::from_secs(2),
    );
    assert!(
        caught,
        "never caught the InstallSnapshot transfer in flight — the fault \
         window was missed entirely (seed={seed})"
    );

    // A real process restart of the RECEIVER, mid-chunk-transfer.
    group.stop_node(ids[follower_idx]);

    group.sim.run_for(Duration::from_secs(4)); // the fresh node re-requests + re-catches-up
    group.heal_all();
    group.sim.run_for(DRAIN);

    let metas = converge_or_timeout(&group);
    let convergence = check_convergence_meta(&metas);
    assert!(
        convergence.ok,
        "did not converge after a receiver StopRestart mid-InstallSnapshot-transfer: {:?} \
         (seed={seed})",
        convergence.violations
    );
    assert_eq!(
        metas[0].members.len(),
        CHUNKED_SNAPSHOT_MEMBERS as usize,
        "converged state lost members across the mid-transfer receiver restart (seed={seed})"
    );
    let apply_progress = poll_apply_task_caught_up(&group.nodes, group.sim.clone());
    assert!(
        apply_progress.ok,
        "apply task stalled after a mid-transfer receiver StopRestart: {:?} (seed={seed})",
        apply_progress.violations
    );
}
