//! ADR 0018 §4 / Follow-up step 5 (PR6): the **multi-tablet Elle
//! serializability corpus** for the CP-plane's cross-tablet 2PC transaction
//! protocol (`animus-cp-data`'s `txn_stage_anchor`/`txn_stage_participant`/
//! `txn_commit_at_least`/`txn_resolve`/recovery-push primitives).
//!
//! **What this proves, and what it deliberately does not.** This corpus
//! drives an **in-test coordinator** directly over raw `RaftKvNode` handles
//! (mirroring `animus-cp-data/tests/txn_multi.rs`'s and `txn_recovery.rs`'s
//! own harness style) — it proves the **protocol** (2PC staging/commit/
//! resolve + the recovery "push" that drives an in-doubt transaction to a
//! decision) is serializable under deterministic fault injection, at depth.
//! It does **not** exercise `animusd`'s wire-level coordinator
//! (`ClientCtx::cp_txn`/`txn_recover`) — that is `animusd/tests/cp_txn.rs`'s
//! job (a real multi-process `ProdEnv` cluster). The two are complementary:
//! this file is the deep, seed-reproducible safety net for the protocol
//! itself; `cp_txn.rs` is the "does the real wire plumbing forward this
//! correctly" acceptance test. See `animus-cp-data/CLAUDE.md`'s "Key
//! invariants" section and ADR 0018's PR4/PR5 amendments for the protocol
//! this coordinator reimplements.
//!
//! **Why a multi-tablet workload has real teeth for `check_cycles`.** The
//! single-tablet `raftkv_linearizable.rs` corpus (ADR 0017) can only ever
//! observe a single Raft group's own total order — a cycle there could only
//! mean a forked/stale read from a deposed leader. Here, every transaction
//! spans **two or three independent tablet Raft groups** with independent
//! leaders, independent `Hlc` clocks, and independent commit pipelines: a
//! genuine cross-tablet ordering violation (one anchor commits transactions
//! in an order its participants' applied values contradict) is now
//! *possible* in principle, so `check_cycles` finding zero cycles across the
//! whole corpus is a real, non-vacuous claim. The teeth-proof test at the
//! bottom of this file demonstrates this directly: with a scoped, deliberate
//! break of the coordinator's own refresh check, `check_cycles` catches a
//! genuine G2 (write-skew) cycle.
//!
//! **Topology.** Three independent tablet Raft groups (`t0`/`t1`/`t2`), 3
//! replicas each, one `MemoryEngine` shared **within** each group's own
//! replicas (mirroring `txn_multi.rs`'s harness convention — Raft consensus
//! is still exercised faithfully per replica; only the final storage engine
//! is a shared test shortcut, exactly as `txn_multi.rs`/`txn_recovery.rs`
//! already establish). Clients live on dedicated, never-faulted driver env
//! ids (the raftkv-corpus convention) and route to whichever node in a group
//! currently leads.
//!
//! **Keyspace.** Nine keys (3 groups × 3 clients: a key's global id is its
//! owning group times 3 plus its owning client) — **single-writer-per-key
//! throughout**, including the read-modify-write shape, exactly like the
//! raftkv/Accord corpora (see `run_rmw_txn`'s doc for why this is load-
//! bearing here too: the storage layer's plain `TxnStage` merge does not
//! arbitrate two transactions racing to stage the *same* key — an earlier
//! draft of this corpus used separate multi-writer "shared" keys for the
//! rmw shape and a live run immediately found a spurious `check_cycles`
//! cycle from exactly that race; see the ADR 0018 PR6 amendment). A client
//! owns **exactly one key per group**, so any multi-key op over a client's
//! own keys is *automatically* cross-tablet.
//!
//! **Three transaction shapes** (ADR 0018 §4's workload requirement):
//! 1. **Write-only, multi-key.** A client appends to 2–3 of its own owned
//!    keys (spanning that many distinct groups) through the full 2PC
//!    protocol — never a begin-time read (the house lesson from the
//!    Accord/raftkv corpora): each client keeps its own authoritative
//!    in-memory list per owned key and only commits a candidate list into
//!    that cache once the transaction's outcome is `Committed`/
//!    `Indeterminate` — **never on a confirmed `Aborted`**, so a
//!    provably-rolled-back append can never silently "leak" into a later
//!    write's encoded prefix (see the module-level comment on
//!    `run_write_only_txn` for the full argument).
//! 2. **Read-only, multi-key, one snapshot.** 2–3 keys across distinct
//!    groups are read via `RaftKvNode::read_at` at one coordinator-minted
//!    timestamp — the MVCC snapshot-read building block ADR 0018 §2/PR2b
//!    lands, used here exactly as its own doc prescribes: a `ts` not yet
//!    covered by a group's committed read ceiling is refused, so the
//!    coordinator bootstraps coverage with an ordinary `linearizable_get`
//!    first and retries. A separate single-key point-read shape
//!    (`run_point_read`) additionally exercises the foreign-intent
//!    read-path push (ADR 0018 §2/PR4 §3, lifted per PR5 §4).
//! 3. **Read-modify-write.** A client appends to 2 of its own owned keys —
//!    **conditioned on** a precondition read of a *different* client's own
//!    key (in the one group this transaction doesn't itself write to),
//!    rechecked by value right before the commit decision; a mismatch
//!    aborts the whole transaction (mirrors `cp_txn`'s own documented
//!    condition-read design, ADR 0018 §2/PR4 §6). This is the shape that
//!    makes genuine G2/write-skew observable: the commit depends on a read
//!    of something a *different*, concurrently-running transaction writes.
//!
//! **The coordinator + recovery push.** `run_txn` reimplements
//! `animusd::ClientCtx::cp_txn`'s protocol directly over `RaftKvNode`
//! (stage the anchor, stage every participant concurrently, decide, and —
//! per ADR 0018 §2/PR5 §6 — resolve **asynchronously**, never inline on the
//! happy path). `push`/`recovery_resolve` reimplement
//! `ClientCtx::txn_recover`'s push protocol (adapted from
//! `animus-cp-data/tests/txn_recovery.rs`'s own helpers of the same name,
//! made async-native since this corpus drives one shared `Simulator` via
//! spawned tasks + `run_for`, never the synchronous `drive` bridge those
//! unit tests use). A per-scenario `resolver_loop` mirrors
//! `animusd::txn_resolver_loop` (poll every group's `pending_txns()`/
//! `unresolved_decided()` on an interval and push/resolve), and the
//! read-only shape's `read_at` — like a real reader hitting a foreign
//! intent — lifts a still-`Pending` status via the same `push` on demand.
//! Both are how a **coordinator-abandon** scenario (the client task stops
//! after prepare, or after a confirmed-but-unresolved commit) still
//! converges within the drain window.
//!
//! **Clock skew beyond the uncertainty interval is a LIVENESS knob here,
//! never a correctness one** (ADR 0018 §2's Decision section): the
//! `clock_skew_beyond_uncertainty` cell deliberately skews a whole group's
//! clocks past `HLC_MAX_OFFSET` (500ms) — some `read_at` calls may time out
//! (recorded `info`, never `fail`) while the skew is in effect, but
//! `check_cycles` must stay green throughout, exactly like
//! `clock_skew_within_uncertainty`. See `assert_scenario_ok`'s doc.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{FastRead, RaftKvNode, StorageScope, TxnDecisionStatus, TxnId, TxnOutcome};
use animus_env::{Clock, EnvExt, Rng, nid};
use animus_sim::{NetConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::KeyRange;
use animus_test::history::{Key, Mop, Process};
use animus_test::{History, Recorder, check_convergence, check_cycles, check_durability};

type Node = RaftKvNode<SimEnv, MemoryEngine>;

// ---------------------------------------------------------------------------
// Topology: three independent tablet groups, 3 replicas each.
// ---------------------------------------------------------------------------

const NUM_GROUPS: usize = 3;
const TABLES: [&str; NUM_GROUPS] = ["t0", "t1", "t2"];
const GROUP_IDS: [[u64; 3]; NUM_GROUPS] = [[0, 1, 2], [10, 11, 12], [20, 21, 22]];
const CLIENT_IDS: [u64; 3] = [200, 201, 202];
const RESOLVER_ID: u64 = 250;
/// A dedicated env id for `force_resolve_all_owned_keys`'s driver task —
/// distinct from `RESOLVER_ID` (which stays busy on its own perpetual
/// loop) and every `CLIENT_IDS` id (whose own client_loop tasks have
/// already finished by the time this runs).
const FORCE_RESOLVE_ID: u64 = 251;
/// Bounded budget for one `force_resolve_all_owned_keys` pass.
const FORCE_RESOLVE_BUDGET: Duration = Duration::from_secs(8);
const NUM_CLIENTS: usize = 3;

/// Timing/budget constants. Mirrors `raftkv_linearizable.rs`'s scale, widened
/// where the transaction protocol needs more round trips per op (stage x N +
/// commit, vs. a single put/get).
const ELECT: Duration = Duration::from_millis(800);
const OP_BUDGET: Duration = Duration::from_secs(6);
const POLL: Duration = Duration::from_millis(100);
/// Bounded attempts `run_txn`'s stage helpers give a stage blocked by
/// another transaction's unresolved intent (ADR 0018 §2/PR6, task #16)
/// before reporting the whole transaction `Aborted` — mirrors `animusd`'s
/// `ClientCtx::txn_prepare_pushing`.
const STAGE_PUSH_ATTEMPTS: u32 = 3;
/// Backoff between `STAGE_PUSH_ATTEMPTS` — room for the blocking
/// transaction to clear (its own coordinator, or `resolver_loop`'s passive
/// sweep past `RECOVERY_GRACE`).
const STAGE_PUSH_BACKOFF: Duration = Duration::from_millis(250);
/// Resolver sweep interval — mirrors `animusd::TXN_RESOLVER_INTERVAL` (1s).
const RESOLVER_INTERVAL: Duration = Duration::from_secs(1);
/// Drain window: long enough to cover `animus_cp_data::RECOVERY_GRACE` (5s)
/// plus several resolver ticks, so an abandoned coordinator's transaction has
/// genuinely converged before the final reads are taken.
const DRAIN: Duration = Duration::from_secs(14);
const CONVERGENCE_POLL_STEP: Duration = Duration::from_secs(2);
const CONVERGENCE_BUDGET: Duration = Duration::from_secs(30);
/// A comfortable margin below `HLC_MAX_OFFSET` (500ms, `animus-cp-data`'s
/// private constant) and comfortably beyond it, for the two clock-skew cells.
const SKEW_WITHIN_NANOS: i64 = 200_000_000; // +200ms
const SKEW_BEYOND_NANOS: i64 = 1_500_000_000; // +1.5s

fn group_index_of_table(table: &str) -> usize {
    TABLES
        .iter()
        .position(|t| *t == table)
        .unwrap_or_else(|| panic!("unknown table {table}"))
}

/// Global key ids `0..9`: single-writer-per-key throughout (`id % NUM_CLIENTS`
/// owns `id`, `id / NUM_CLIENTS` is the owning group) — including the
/// read-modify-write shape, which never writes a key it doesn't itself own
/// (see `run_rmw_txn`'s doc for why: the storage layer's plain `TxnStage`
/// merge does not arbitrate two transactions racing to stage the same key,
/// so genuine multi-writer contention on one key is out of scope here,
/// exactly like the raftkv/Accord corpora's own single-writer-per-key
/// doctrine).
fn group_of_key(key: Key) -> usize {
    (key / NUM_CLIENTS as u64) as usize
}

fn table_of_key(key: Key) -> &'static str {
    TABLES[group_of_key(key)]
}

fn owned_key(client: usize, group: usize) -> Key {
    (group * NUM_CLIENTS + client) as u64
}

/// An 8-byte partition token (ADR 0022) — every real data-plane key leads
/// with one; `txn_stage_anchor`'s anchor-key assert requires it.
fn key_bytes(key: Key) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}

fn encode_list(list: &[u64]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(list.len() * 8);
    for v in list {
        bytes.extend_from_slice(&v.to_be_bytes());
    }
    bytes
}

fn decode_list(bytes: &[u8]) -> Vec<u64> {
    bytes
        .chunks_exact(8)
        .map(|c| u64::from_be_bytes(c.try_into().unwrap()))
        .collect()
}

fn lossy(p: f64) -> NetConfig {
    let mut cfg = NetConfig::default();
    cfg.set_drop_prob(p);
    cfg
}

struct Topology {
    /// `nodes[g]` is group `g`'s (`TABLES[g]`) fixed replica set. No node
    /// object is ever replaced (no `StopRestart`-style fault in this
    /// corpus) — a crash mutes the underlying `SimEnv` node, exactly like
    /// `raftkv_linearizable.rs`'s `LeaderKill`/`FollowerKill`.
    nodes: Vec<Vec<Node>>,
}

impl Topology {
    fn start(sim: &Simulator) -> Topology {
        let mut nodes = Vec::with_capacity(NUM_GROUPS);
        for g in 0..NUM_GROUPS {
            let engine = MemoryEngine::new();
            let ids = GROUP_IDS[g];
            let scope = StorageScope::new(TABLES[g].as_bytes().to_vec(), KeyRange::whole());
            let group_nodes: Vec<Node> = ids
                .iter()
                .map(|&id| {
                    RaftKvNode::start_scoped(
                        sim.env(nid(id)),
                        ids.iter().copied().map(nid).collect(),
                        engine.clone(),
                        scope.clone(),
                    )
                })
                .collect();
            nodes.push(group_nodes);
        }
        Topology { nodes }
    }

    fn for_table(&self, table: &str) -> &[Node] {
        &self.nodes[group_index_of_table(table)]
    }

    fn for_group(&self, g: usize) -> &[Node] {
        &self.nodes[g]
    }
}

/// The group's current leader, or `None` if no replica currently believes
/// itself one. **Deliberately not `nodes.iter().find(|n| n.is_leader())`**:
/// a crashed replica keeps reporting `is_leader() == true` forever from its
/// last-known, frozen-at-crash state (it never learns it lost the term —
/// it's muted from the network, not gracefully shut down; see
/// `raftkv_linearizable.rs`'s own `leader_among` helper for the identical,
/// already-documented gotcha), so a bare `.find()` can return the *first*
/// array-order match — which, whenever the crashed replica's own index
/// happens to precede the genuine new leader's, is the stale, isolated
/// replica instead. Picking the **highest `term()`** among every
/// `is_leader() == true` replica is robust to this without needing to
/// thread a separate "which ids are currently crashed" set through every
/// call site: any real election strictly increments the term, so a frozen
/// replica's stale term can never out-rank a genuinely-elected new leader's.
/// Found live by the ADR 0018 multi-tablet transaction corpus's
/// `anchor_leader_kill_early` scenario (seed 3924719889167511385) — see the
/// module doc / the ADR 0018 PR6 amendment for the full account.
fn leader_of(nodes: &[Node]) -> Option<&Node> {
    nodes
        .iter()
        .filter(|n| n.is_leader())
        .max_by_key(|n| n.term())
}

fn leader_index(nodes: &[Node]) -> Option<usize> {
    nodes.iter().position(|n| n.is_leader())
}

// ---------------------------------------------------------------------------
// The declarative scenario model (adapts `raftkv_linearizable.rs`'s
// `Nemesis`/`Scenario`/`Scenario::window` shape to the cross-tablet workload).
// ---------------------------------------------------------------------------

/// Faults applied to the *topology* (network/process). Coordinator abandonment
/// is deliberately **not** here — it's a workload behavior (the coordinating
/// client's own choice to stop mid-protocol), modeled as a `Workload`
/// probability instead; see `Workload::abandon_prepare_pct`/`abandon_commit_pct`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Nemesis {
    /// Kill the current leader of group 1 or 2. Every write-only and
    /// read-only op always touches group 0 (`client_loop`'s key selection
    /// always includes it), so group 0 is the anchor for the large
    /// majority of transactions — an rmw op is the one exception, when
    /// group 0 happens to be its *watched* (read-only, unwritten) group.
    ParticipantLeaderKill { group: usize },
    /// Kill the current leader of group 0 — the anchor for most, though
    /// not strictly all, transactions (see `ParticipantLeaderKill`'s doc).
    AnchorLeaderKill,
    /// Partition a group's leader away from its own peers during the
    /// prepare window, healed at the end of the scenario's outage window.
    PartitionDuringPrepare { group: usize },
    /// Independent per-message drop for the rest of the run.
    Lossy,
    /// Skew every replica of the anchor group's clock **within**
    /// `HLC_MAX_OFFSET` — serializability must hold (uncertainty-interval
    /// read restarts absorb it, ADR 0018 §2/PR4 §4).
    ClockSkewWithin,
    /// Skew every replica of the anchor group's clock **beyond**
    /// `HLC_MAX_OFFSET` — serializability must *still* hold; only liveness
    /// (some `read_at`/prepare round trips may time out) may degrade. See
    /// the module doc.
    ClockSkewBeyond,
}

/// The transaction-shape mix + coordinator-abandon probabilities a scenario
/// drives. Percentages are of 100; `write_pct + read_pct` is the read/write
/// split, the remainder is read-modify-write.
#[derive(Clone, Debug)]
struct Workload {
    rounds: u64,
    write_pct: u64,
    read_pct: u64,
    /// Chance (of 100) that a write-only/rmw transaction's coordinator
    /// abandons right after a successful prepare (before deciding) —
    /// models a coordinator crash mid-2PC. Recorded `info`.
    abandon_prepare_pct: u64,
    /// Chance (of 100) that the coordinator abandons immediately after a
    /// **single, non-retried** commit attempt confirms, without ever
    /// following up or resolving — models a coordinator crash right after
    /// the durable commit point. Recorded `info`; convergence depends
    /// entirely on the resolver loop / on-demand foreign-intent reads.
    abandon_commit_pct: u64,
}

impl Workload {
    fn default_mix() -> Workload {
        Workload {
            rounds: 5,
            write_pct: 45,
            read_pct: 25,
            abandon_prepare_pct: 0,
            abandon_commit_pct: 0,
        }
    }
    fn rmw_heavy() -> Workload {
        Workload {
            write_pct: 25,
            read_pct: 20,
            ..Workload::default_mix()
        }
    }
    fn read_heavy() -> Workload {
        Workload {
            write_pct: 25,
            read_pct: 55,
            ..Workload::default_mix()
        }
    }
    fn with_abandon(prepare_pct: u64, commit_pct: u64) -> Workload {
        Workload {
            abandon_prepare_pct: prepare_pct,
            abandon_commit_pct: commit_pct,
            ..Workload::default_mix()
        }
    }
}

#[derive(Clone, Debug)]
struct Scenario {
    name: String,
    seed: u64,
    workload: Workload,
    faults: Vec<(Duration, Nemesis)>,
    /// Outage window held before healing — zero for scenarios with no
    /// timed fault.
    window: Duration,
}

/// FNV-1a name→seed map (repo convention — see `raftkv_linearizable.rs`'s
/// identically-named function; each corpus file defines its own, no
/// nondeterministic `std::hash`).
fn name_seed(name: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

fn cell(
    name: &str,
    workload: Workload,
    faults: Vec<(Duration, Nemesis)>,
    window: Duration,
) -> Scenario {
    Scenario {
        seed: name_seed(name),
        name: name.to_string(),
        workload,
        faults,
        window,
    }
}

const EARLY: Duration = Duration::from_millis(600);
const MID: Duration = Duration::from_millis(1800);
const LATE: Duration = Duration::from_millis(3200);
const TIMINGS: [(&str, Duration); 3] = [("early", EARLY), ("mid", MID), ("late", LATE)];
/// Real outage window every faulted cell rides out before healing.
const WINDOW: Duration = Duration::from_millis(2000);

/// The frozen, name-seeded scenario corpus (ADR 0014's doctrine, ported to
/// the cross-tablet transaction protocol). ~25 cells: baselines, coordinator
/// abandonment (both flavors), the fault matrix (participant/anchor leader
/// kill, partition-during-prepare, each at 3 timings), lossy links, clock
/// skew within/beyond uncertainty, and 6 compound cells crossing
/// abandonment/faults/workload mix.
#[allow(clippy::vec_init_then_push)] // built incrementally across several loops below
fn corpus_cells() -> Vec<Scenario> {
    let mut out = Vec::new();

    out.push(cell(
        "baseline",
        Workload::default_mix(),
        vec![],
        Duration::ZERO,
    ));
    out.push(cell(
        "baseline_rmw_heavy",
        Workload::rmw_heavy(),
        vec![],
        Duration::ZERO,
    ));
    out.push(cell(
        "baseline_read_heavy",
        Workload::read_heavy(),
        vec![],
        Duration::ZERO,
    ));

    out.push(cell(
        "coordinator_abandon_prepare",
        Workload::with_abandon(30, 0),
        vec![],
        Duration::ZERO,
    ));
    out.push(cell(
        "coordinator_abandon_commit_unresolved",
        Workload::with_abandon(0, 30),
        vec![],
        Duration::ZERO,
    ));

    for (tname, at) in TIMINGS {
        out.push(cell(
            &format!("participant_leader_kill_{tname}"),
            Workload::default_mix(),
            vec![(at, Nemesis::ParticipantLeaderKill { group: 1 })],
            WINDOW,
        ));
    }
    for (tname, at) in TIMINGS {
        out.push(cell(
            &format!("anchor_leader_kill_{tname}"),
            Workload::default_mix(),
            vec![(at, Nemesis::AnchorLeaderKill)],
            WINDOW,
        ));
    }
    for (tname, at) in TIMINGS {
        out.push(cell(
            &format!("partition_during_prepare_{tname}"),
            Workload::default_mix(),
            vec![(at, Nemesis::PartitionDuringPrepare { group: 2 })],
            WINDOW,
        ));
    }

    out.push(cell(
        "lossy_links",
        Workload::default_mix(),
        vec![(EARLY, Nemesis::Lossy)],
        WINDOW,
    ));
    out.push(cell(
        "clock_skew_within_uncertainty",
        Workload::default_mix(),
        vec![(EARLY, Nemesis::ClockSkewWithin)],
        WINDOW,
    ));
    out.push(cell(
        "clock_skew_beyond_uncertainty",
        Workload::default_mix(),
        vec![(EARLY, Nemesis::ClockSkewBeyond)],
        WINDOW,
    ));

    // Compound cells.
    out.push(cell(
        "compound_skew_beyond_and_participant_kill",
        Workload::default_mix(),
        vec![
            (EARLY, Nemesis::ClockSkewBeyond),
            (MID, Nemesis::ParticipantLeaderKill { group: 1 }),
        ],
        WINDOW,
    ));
    out.push(cell(
        "compound_lossy_and_anchor_kill",
        Workload::default_mix(),
        vec![(EARLY, Nemesis::Lossy), (MID, Nemesis::AnchorLeaderKill)],
        WINDOW,
    ));
    out.push(cell(
        "compound_abandon_prepare_and_partition",
        Workload::with_abandon(25, 0),
        vec![(MID, Nemesis::PartitionDuringPrepare { group: 1 })],
        WINDOW,
    ));
    out.push(cell(
        "compound_abandon_commit_and_anchor_kill",
        Workload::with_abandon(0, 25),
        vec![(MID, Nemesis::AnchorLeaderKill)],
        WINDOW,
    ));
    out.push(cell(
        "participant_leader_kill_mid_group2",
        Workload::default_mix(),
        vec![(MID, Nemesis::ParticipantLeaderKill { group: 2 })],
        WINDOW,
    ));
    out.push(cell(
        "anchor_leader_kill_mid_rmw_heavy",
        Workload::rmw_heavy(),
        vec![(MID, Nemesis::AnchorLeaderKill)],
        WINDOW,
    ));
    out.push(cell(
        "lossy_read_heavy",
        Workload::read_heavy(),
        vec![(EARLY, Nemesis::Lossy)],
        WINDOW,
    ));
    out.push(cell(
        "partition_during_prepare_rmw_heavy_mid",
        Workload::rmw_heavy(),
        vec![(MID, Nemesis::PartitionDuringPrepare { group: 1 })],
        WINDOW,
    ));

    out
}

/// Depth knob (`ANIMUS_TXN_SEEDS`, default 1) — mirrors `seed_expand` in
/// `raftkv_linearizable.rs`/`support::seed_expand`: variant 0 keeps the
/// cell's canonical frozen name+seed, `k=1` is the identity.
fn seeds_per_cell() -> usize {
    std::env::var("ANIMUS_TXN_SEEDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1)
}

fn seed_expand(cells: Vec<Scenario>, k: usize) -> Vec<Scenario> {
    if k <= 1 {
        return cells;
    }
    let mut out = Vec::with_capacity(cells.len() * k);
    for cell in cells {
        for i in 0..k {
            if i == 0 {
                out.push(cell.clone());
            } else {
                let name = format!("{}_s{i:02}", cell.name);
                out.push(Scenario {
                    seed: name_seed(&name),
                    name,
                    workload: cell.workload.clone(),
                    faults: cell.faults.clone(),
                    window: cell.window,
                });
            }
        }
    }
    out
}

fn corpus() -> Vec<Scenario> {
    seed_expand(corpus_cells(), seeds_per_cell())
}

// ---------------------------------------------------------------------------
// Fault application.
// ---------------------------------------------------------------------------

fn kill_leader(sim: &Simulator, nodes: &[Node], ids: &[u64; 3], crashed: &mut BTreeSet<u64>) {
    if let Some(i) = leader_index(nodes) {
        sim.crash(nid(ids[i]));
        crashed.insert(ids[i]);
    }
}

fn partition_leader(sim: &Simulator, nodes: &[Node], ids: &[u64; 3]) {
    if let Some(li) = leader_index(nodes) {
        for j in 0..ids.len() {
            if j != li {
                sim.partition_pair(nid(ids[li]), nid(ids[j]));
            }
        }
    }
}

fn apply_nemesis(sim: &Simulator, topo: &Topology, nem: Nemesis, crashed: &mut BTreeSet<u64>) {
    match nem {
        Nemesis::AnchorLeaderKill => kill_leader(sim, topo.for_group(0), &GROUP_IDS[0], crashed),
        Nemesis::ParticipantLeaderKill { group } => {
            kill_leader(sim, topo.for_group(group), &GROUP_IDS[group], crashed)
        }
        Nemesis::PartitionDuringPrepare { group } => {
            partition_leader(sim, topo.for_group(group), &GROUP_IDS[group])
        }
        Nemesis::Lossy => sim.set_net_config(lossy(0.1)),
        Nemesis::ClockSkewWithin => {
            for &id in &GROUP_IDS[0] {
                sim.set_clock_skew_for(nid(id), SKEW_WITHIN_NANOS);
            }
        }
        Nemesis::ClockSkewBeyond => {
            for &id in &GROUP_IDS[0] {
                sim.set_clock_skew_for(nid(id), SKEW_BEYOND_NANOS);
            }
        }
    }
}

fn heal_all(sim: &Simulator, crashed: &mut BTreeSet<u64>) {
    let all_ids: Vec<u64> = GROUP_IDS.iter().flatten().copied().collect();
    for i in 0..all_ids.len() {
        for j in (i + 1)..all_ids.len() {
            sim.heal(nid(all_ids[i]), nid(all_ids[j]));
        }
    }
    for &id in crashed.iter() {
        sim.restart(nid(id));
    }
    crashed.clear();
    sim.set_net_config(NetConfig::default());
    for &id in &all_ids {
        sim.set_clock_skew_for(nid(id), 0);
    }
}

// ---------------------------------------------------------------------------
// Shared state + a generic bounded leader-retry helper.
// ---------------------------------------------------------------------------

struct Shared {
    rec: Mutex<Recorder>,
    next_value: Mutex<u64>,
}

impl Shared {
    fn fresh_value(&self) -> u64 {
        let mut v = self.next_value.lock().unwrap();
        *v += 1;
        *v
    }
}

/// Retry `f` against a group's **current** leader (re-resolved every
/// attempt, since a fault may depose it mid-op) until it returns `Some`, or
/// `budget` elapses. Mirrors the routing/retry a real forwarding coordinator
/// performs, without this test needing its own wire layer.
async fn with_leader_retry<T, F, Fut>(
    env: &SimEnv,
    nodes: &[Node],
    budget: Duration,
    mut f: F,
) -> Option<T>
where
    F: FnMut(Node) -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = env.now().0 + budget.as_nanos() as u64;
    loop {
        if let Some(node) = leader_of(nodes).cloned()
            && let Some(v) = f(node).await
        {
            return Some(v);
        }
        if env.now().0 >= deadline {
            return None;
        }
        env.sleep(POLL).await;
    }
}

/// Wait (bounded, retried) for **some** leader to exist, then call `f`
/// **exactly once** — never used for a decide-class call (`txn_commit_at_
/// least`/`txn_abort`/`txn_abort_orphan`). Those mint a **fresh** HLC
/// timestamp on every call (`mint_at_least`/`mint_pushed`), so retrying the
/// *decide itself* after an unconfirmed attempt can genuinely propose a
/// **second**, differently-timestamped `TxnCommit`/`TxnAbort` for the same
/// `txn_id` — and if the first, unconfirmed attempt's entry *also* lands
/// (it may: `wait_applied` failing only means *this caller* couldn't
/// confirm it, not that the entry was discarded), the apply path's own
/// "two different commit timestamps for one record" hard assert fires —
/// a real regression this corpus caught live under leader-kill fault
/// injection during its own development (see the ADR 0018 PR6 amendment).
/// The system's own contract is that **at most one decide attempt** is
/// ever made per transaction per decider; finding *a* leader to propose to
/// is safe to retry (no propose happens without one), but the decide call
/// itself must be single-shot, falling back to re-reading the record's
/// actual status (`query_final_status`) rather than re-proposing.
async fn decide_once<T, F, Fut>(env: &SimEnv, nodes: &[Node], budget: Duration, f: F) -> Option<T>
where
    F: FnOnce(Node) -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = env.now().0 + budget.as_nanos() as u64;
    loop {
        if let Some(node) = leader_of(nodes).cloned() {
            return f(node).await;
        }
        if env.now().0 >= deadline {
            return None;
        }
        env.sleep(POLL).await;
    }
}

/// A single-key `linearizable_get_served` bounded retry, absent treated as an
/// empty list (so a shared key's very first read needs no seeding step).
async fn read_served_with_retry(
    env: &SimEnv,
    nodes: &[Node],
    key: &[u8],
    budget: Duration,
) -> Option<Vec<u8>> {
    with_leader_retry(env, nodes, budget, |node| {
        let key = key.to_vec();
        async move { node.linearizable_get_served(&key).await }
    })
    .await
    .map(|v| v.unwrap_or_default())
}

// ---------------------------------------------------------------------------
// The coordinator: reimplements `animusd::ClientCtx::cp_txn` directly over
// `RaftKvNode` (ADR 0018 §2/PR4, §2/PR5 §6's async-resolve revision).
// ---------------------------------------------------------------------------

/// `(table, key, value)` — `value: None` is a staged delete.
type TxnWrite = (&'static str, Vec<u8>, Option<Vec<u8>>);
/// `(table, key, expected_bytes)` — a value-precondition to re-check
/// right before the commit decision.
type TxnPrecondition = (&'static str, Vec<u8>, Vec<u8>);

#[derive(Clone, Debug)]
enum TxnRunOutcome {
    Committed,
    Aborted,
    Indeterminate,
}

fn to_outcome(status: &TxnDecisionStatus) -> Option<TxnOutcome> {
    match status {
        TxnDecisionStatus::Committed { commit_ts } => Some(TxnOutcome::Committed {
            commit_ts: *commit_ts,
        }),
        TxnDecisionStatus::Aborted => Some(TxnOutcome::Aborted),
        TxnDecisionStatus::Pending => None,
    }
}

/// Bounded retry for "what did this record actually decide" — treats
/// `Pending` as "keep polling", per the house rule (`animusd::
/// ClientCtx::txn_decide_anchor`'s doc): never assume your own proposal won,
/// always re-read and report the real, final status.
async fn query_final_status(
    env: &SimEnv,
    nodes: &[Node],
    record_key: Vec<u8>,
    budget: Duration,
) -> Option<TxnDecisionStatus> {
    with_leader_retry(env, nodes, budget, |node| {
        let record_key = record_key.clone();
        async move {
            match node.txn_status_local(&record_key).await {
                Some(TxnDecisionStatus::Pending) | None => None,
                Some(other) => Some(other),
            }
        }
    })
    .await
}

async fn recovery_resolve(
    env: &SimEnv,
    topo: &Topology,
    record_key: Vec<u8>,
    txn_id: TxnId,
    intent_spans: &[(String, KeyRange)],
    outcome: &TxnOutcome,
) {
    for (table, span) in intent_spans {
        let nodes = topo.for_table(table);
        if let Some(leader) = leader_of(nodes).cloned() {
            let _ = leader
                .txn_resolve(
                    txn_id.clone(),
                    record_key.clone(),
                    vec![span.start.clone()],
                    outcome.clone(),
                )
                .await;
        }
    }
    let _ = env; // reserved for future timing use; keeps the signature uniform
}

/// The recovery "push" (ADR 0018 §2/PR5 §3), adapted from
/// `animus-cp-data/tests/txn_recovery.rs`'s helper of the same name — async
/// native (no `drive` bridge) since this corpus drives everything through
/// one shared `Simulator`. Best-effort and single-attempt per call (like
/// `animusd::txn_resolver_loop`'s own per-tick sweep): a caller that needs
/// it to actually land calls it repeatedly (the resolver loop's next tick,
/// or a reader's own retry loop).
async fn push(
    env: &SimEnv,
    topo: &Topology,
    record_nodes: &[Node],
    record_key: Vec<u8>,
    txn_id: TxnId,
) {
    let Some(leader) = leader_of(record_nodes).cloned() else {
        return;
    };
    let Some(view) = leader.txn_record_view(&record_key).await else {
        // This corpus injects no seal/fence-miss faults, so the anchor's own
        // stage never silently no-ops — the record-absent/orphan branch
        // (ADR 0018 §2/PR5 §2b) is out of this corpus's scope; it is
        // already covered by `animus-cp-data/tests/txn_recovery.rs` and the
        // in-crate `pr5_orphan_and_resurrection_tests`.
        return;
    };
    if !matches!(view.status, TxnDecisionStatus::Pending) {
        if let Some(outcome) = to_outcome(&view.status) {
            recovery_resolve(env, topo, record_key, txn_id, &view.intent_spans, &outcome).await;
        }
        return;
    }

    let now_ms = env.now().0 / 1_000_000;
    if now_ms < view.created_ts.wall_ms + animus_cp_data::RECOVERY_GRACE.as_millis() as u64 {
        return; // decline: a still-live coordinator gets room to finish.
    }

    let mut all_staged = true;
    for (table, span) in &view.intent_spans {
        let nodes = topo.for_table(table);
        let Some(participant) = leader_of(nodes).cloned() else {
            all_staged = false;
            continue;
        };
        match participant.txn_verify_staged(span, &txn_id).await {
            Some(true) => {}
            _ => all_staged = false,
        }
    }

    let Some(anchor) = leader_of(record_nodes).cloned() else {
        return;
    };
    if all_staged {
        let _ = anchor
            .txn_commit_at_least(txn_id.clone(), record_key.clone(), view.created_ts)
            .await;
    } else {
        let _ = anchor.txn_abort(txn_id.clone(), record_key.clone()).await;
    }

    let Some(anchor2) = leader_of(record_nodes).cloned() else {
        return;
    };
    if let Some(final_view) = anchor2.txn_record_view(&record_key).await
        && let Some(outcome) = to_outcome(&final_view.status)
    {
        recovery_resolve(
            env,
            topo,
            record_key,
            txn_id,
            &final_view.intent_spans,
            &outcome,
        )
        .await;
    }
}

/// One `linearizable_get_served_fast` attempt with foreign-intent resolution
/// (ADR 0018 §2/PR4 §3, lifted per PR5 §4): a still-`Pending` or unreachable
/// status calls `push` before giving up, mirroring `animusd::
/// ClientCtx::cp_get_local_resolving`.
/// One resolution attempt. `None` means "not resolvable yet, caller
/// retries" (mirrors `FastRead`/`ResolveStep`'s own contract); `Some(v)`
/// means resolved, `v` itself distinguishing present (`Some(bytes)`) from
/// genuinely absent (`None`) — never conflated, unlike a bare `Option<Vec<u8>>`
/// would.
async fn read_resolving_once(
    env: &SimEnv,
    topo: &Topology,
    table: &str,
    key: &[u8],
) -> Option<Option<Vec<u8>>> {
    let nodes = topo.for_table(table);
    let node = leader_of(nodes)?.clone();
    match node.linearizable_get_served_fast(key).await? {
        FastRead::Value(v) => Some(v),
        // The local-`Pending` case now carries the same `IntentInfo`
        // payload `Foreign` does (torn-pair-fix stack's ADR 0018 §2
        // amendment), but `read_resolving_once` deliberately keeps its
        // pre-existing behavior here (immediate give-up, no push attempt)
        // — it backs `run_point_read`'s own single-key retry discipline,
        // untouched by that amendment. See `snapshot_read_uniform` below
        // for the fixed design's own reader, which *does* merge this arm
        // with `Foreign`'s.
        FastRead::Pending(_) => None,
        FastRead::Foreign(info) => {
            let record_nodes = topo.for_table(&info.record_table);
            let status = match leader_of(record_nodes) {
                Some(leader) => leader.txn_status_local(&info.record_key).await,
                None => None,
            };
            match status {
                Some(TxnDecisionStatus::Pending) | None => {
                    push(
                        env,
                        topo,
                        record_nodes,
                        info.record_key.clone(),
                        info.txn_id.clone(),
                    )
                    .await;
                    None
                }
                Some(decided) => {
                    node.resolve_intent_given_status(key, None, &info.txn_id, decided)
                        .await
                }
            }
        }
    }
}

/// Best-effort abort, then re-read the record's *actual* final decision
/// **before resolving anything** and resolve every key confirmed staged
/// anywhere (`staged`, `(table, key)` pairs — the anchor's own key(s)
/// included) using *that* outcome — never assume the abort proposal itself
/// won (a concurrent recovery commit, however unlikely in this controlled
/// workload, is handled the same uniform way) and never resolve with a
/// stale, hardcoded outcome. **This ordering is load-bearing, not
/// stylistic**: resolving with a hardcoded `Aborted` before checking what
/// actually happened would be a genuine torn-resolve bug — restoring
/// pre-intent values for a transaction a concurrent recovery decider had
/// already, legitimately, committed (mirrors `animusd::ClientCtx::cp_txn`/
/// `txn_recover`'s own discipline: every real resolve call is fed from a
/// post-decision status re-read, never a caller's own candidate/proposed
/// value — see the ADR 0018 PR6 amendment's torn-resolve audit).
async fn abort_and_report(
    env: &SimEnv,
    topo: &Topology,
    anchor_nodes: &[Node],
    txn_id: TxnId,
    record_key: Vec<u8>,
    staged: &[(&'static str, Vec<u8>)],
) -> TxnRunOutcome {
    let _ = decide_once(env, anchor_nodes, OP_BUDGET, |node| {
        let txn_id = txn_id.clone();
        let record_key = record_key.clone();
        async move { node.txn_abort(txn_id, record_key).await }
    })
    .await;

    let final_status = query_final_status(env, anchor_nodes, record_key.clone(), OP_BUDGET).await;
    let Some(outcome) = final_status.as_ref().and_then(to_outcome) else {
        // Couldn't determine the actual decision within budget — resolving
        // now would risk using a guessed outcome. Leave every intent
        // unresolved; the resolver loop / on-demand foreign-intent reads
        // will finish it once the decision is known.
        return TxnRunOutcome::Indeterminate;
    };

    for (table, key) in staged {
        let nodes = topo.for_table(table);
        let _ = with_leader_retry(env, nodes, OP_BUDGET, |node| {
            let txn_id = txn_id.clone();
            let record_key = record_key.clone();
            let key = key.clone();
            let outcome = outcome.clone();
            async move {
                node.txn_resolve(txn_id, record_key, vec![key], outcome)
                    .await
            }
        })
        .await;
    }

    match final_status {
        Some(TxnDecisionStatus::Aborted) => TxnRunOutcome::Aborted,
        Some(TxnDecisionStatus::Committed { .. }) => TxnRunOutcome::Committed,
        _ => TxnRunOutcome::Indeterminate,
    }
}

/// After a stage attempt returns `Some(..)`, verify `key` actually holds
/// *this* transaction's own intent (`RaftKvNode::txn_verify_staged`, the
/// same primitive a recovery push already uses) — see
/// `stage_anchor_pushing`/`stage_participant_pushing`'s own doc for why
/// `Some` alone is not enough (ADR 0018 §2/PR6, task #16).
async fn stage_landed(env: &SimEnv, nodes: &[Node], key: &[u8], txn_id: &TxnId) -> bool {
    let mut end = key.to_vec();
    end.push(0);
    let span = KeyRange::new(key.to_vec(), Some(end));
    with_leader_retry(env, nodes, OP_BUDGET, |node| {
        let span = span.clone();
        let txn_id = txn_id.clone();
        async move { node.txn_verify_staged(&span, &txn_id).await }
    })
    .await
    .unwrap_or(false)
}

/// Stage the anchor, retrying (bounded `STAGE_PUSH_ATTEMPTS`) if the target
/// key was blocked by another transaction's still-unresolved intent — the
/// apply-time writer-push-intents guard `KvCommand::TxnStage`'s doc
/// describes (ADR 0018 §2/PR6, task #16). **A stage call returning
/// `Some(..)` only means its entry applied, never that it genuinely wrote
/// an intent** — the entry can have no-op'd internally exactly like a
/// fence/seal miss, so every caller here re-verifies via `stage_landed`
/// before trusting it. Without this, a blocked anchor stage would look
/// identical to a genuine one, and the transaction would go on to commit
/// without the anchor's own write ever having happened.
async fn stage_anchor_pushing(
    env: &SimEnv,
    nodes: &[Node],
    table: &'static str,
    key: Vec<u8>,
    val: Option<Vec<u8>>,
    participant_spans: Vec<(String, KeyRange)>,
) -> Option<(TxnId, Vec<u8>)> {
    for attempt in 0..STAGE_PUSH_ATTEMPTS {
        let staged = with_leader_retry(env, nodes, OP_BUDGET, |node| {
            let key = key.clone();
            let val = val.clone();
            let participant_spans = participant_spans.clone();
            async move {
                node.txn_stage_anchor(table, vec![(key, val)], participant_spans, Vec::new())
                    .await
            }
        })
        .await;
        if let Some((txn_id, _, _outcome)) = &staged
            && stage_landed(env, nodes, &key, txn_id).await
        {
            return staged.map(|(txn_id, record_key, _outcome)| (txn_id, record_key));
        }
        if attempt + 1 < STAGE_PUSH_ATTEMPTS {
            env.sleep(STAGE_PUSH_BACKOFF).await;
        }
    }
    None
}

/// The participant dual of `stage_anchor_pushing` — same verify-then-retry
/// discipline, same reason (ADR 0018 §2/PR6, task #16).
#[allow(clippy::too_many_arguments)]
async fn stage_participant_pushing(
    env: &SimEnv,
    nodes: &[Node],
    txn_id: TxnId,
    record_key: Vec<u8>,
    record_table: String,
    key: Vec<u8>,
    val: Option<Vec<u8>>,
) -> Option<HlcTimestamp> {
    for attempt in 0..STAGE_PUSH_ATTEMPTS {
        let ts = with_leader_retry(env, nodes, OP_BUDGET, |node| {
            let txn_id = txn_id.clone();
            let record_key = record_key.clone();
            let record_table = record_table.clone();
            let key = key.clone();
            let val = val.clone();
            async move {
                node.txn_stage_participant(
                    txn_id,
                    record_key,
                    record_table,
                    vec![(key, val)],
                    Vec::new(),
                )
                .await
            }
        })
        .await;
        if ts.is_some() && stage_landed(env, nodes, &key, &txn_id).await {
            return ts.map(|(ts, _outcome)| ts);
        }
        if attempt + 1 < STAGE_PUSH_ATTEMPTS {
            env.sleep(STAGE_PUSH_BACKOFF).await;
        }
    }
    None
}

/// The full cross-tablet 2PC coordinator (ADR 0018 §2/PR4, generalized here
/// to `N` writes across up to 3 groups; ADR 0018 §2/PR5 §6's async-resolve
/// revision — the happy path never resolves inline, matching production).
/// `writes[0]` is the anchor. `precondition`, if present, is re-checked by
/// **value** right before the commit decision (mirrors `cp_txn`'s own
/// documented deviation, ADR 0018 §2/PR4 §6: refresh by value, not by HLC
/// timestamp) — a mismatch aborts the whole transaction.
async fn run_txn(
    env: &SimEnv,
    topo: &Topology,
    writes: Vec<TxnWrite>,
    precondition: Option<Vec<TxnPrecondition>>,
    abandon_after_prepare: bool,
    abandon_after_commit: bool,
) -> TxnRunOutcome {
    assert!(!writes.is_empty(), "run_txn: writes must be non-empty");
    let (anchor_table, anchor_key, anchor_val) = writes[0].clone();
    let participants: Vec<TxnWrite> = writes[1..].to_vec();
    let participant_spans: Vec<(String, KeyRange)> = participants
        .iter()
        .map(|(t, k, _)| {
            let mut end = k.clone();
            end.push(0);
            (t.to_string(), KeyRange::new(k.clone(), Some(end)))
        })
        .collect();

    let anchor_nodes = topo.for_table(anchor_table);
    let staged_anchor = stage_anchor_pushing(
        env,
        anchor_nodes,
        anchor_table,
        anchor_key.clone(),
        anchor_val,
        participant_spans,
    )
    .await;
    // The anchor's stage creates the record; nothing is ever created
    // anywhere else until it confirms, so a failure here is safe to report
    // as a definite abort (see the module doc's discussion in `run_txn`'s
    // own design notes / the final report).
    let Some((txn_id, record_key)) = staged_anchor else {
        return TxnRunOutcome::Aborted;
    };

    if abandon_after_prepare {
        return TxnRunOutcome::Indeterminate;
    }

    // `record_table` on a participant's own stage names the **anchor's**
    // table (`anchor_table`, captured above) — never the participant's own
    // table (`table`, this iteration's own tablet) — so that any reader
    // hitting this participant's intent routes its `TxnStatus` query to
    // where the record *actually* lives. Passing the participant's own
    // table here instead (a real bug this corpus's own development caught,
    // see the module doc / ADR 0018 PR6 amendment) means every foreign-
    // intent resolution attempt looks for the record in the *participant's*
    // scope — where it can never be, since it lives on the anchor — so
    // `txn_record_view`/`txn_status_local` always come back empty and the
    // intent never resolves, on demand or otherwise.
    let stage_futs = participants.iter().map(|(table, key, val)| {
        let nodes = topo.for_table(table);
        let txn_id = txn_id.clone();
        let record_key = record_key.clone();
        let anchor_table_owned = anchor_table.to_string();
        let key = key.clone();
        let val = val.clone();
        async move {
            stage_participant_pushing(env, nodes, txn_id, record_key, anchor_table_owned, key, val)
                .await
        }
    });
    let stage_results: Vec<Option<HlcTimestamp>> = futures::future::join_all(stage_futs).await;

    let mut staged_keys: Vec<(&'static str, Vec<u8>)> = vec![(anchor_table, anchor_key.clone())];
    let mut candidate = txn_id.ts;
    let mut all_staged = true;
    for ((table, key, _), ts) in participants.iter().zip(stage_results.iter()) {
        match ts {
            Some(t) => {
                candidate = candidate.max(*t);
                staged_keys.push((table, key.clone()));
            }
            None => all_staged = false,
        }
    }
    if !all_staged {
        return abort_and_report(env, topo, anchor_nodes, txn_id, record_key, &staged_keys).await;
    }

    if let Some(preconditions) = &precondition {
        for (table, key, expected) in preconditions {
            let nodes = topo.for_table(table);
            let current = with_leader_retry(env, nodes, OP_BUDGET, |node| {
                let key = key.clone();
                async move { node.linearizable_get_served(&key).await }
            })
            .await
            .map(|v| v.unwrap_or_default());
            if current.as_ref() != Some(expected) {
                return abort_and_report(env, topo, anchor_nodes, txn_id, record_key, &staged_keys)
                    .await;
            }
        }
    }

    if abandon_after_commit {
        // A single, non-retried commit attempt — then true abandonment: no
        // follow-up, no resolve, no status query. Convergence depends
        // entirely on the resolver loop / on-demand foreign-intent reads.
        let _ = leader_of(anchor_nodes)
            .cloned()
            .expect("anchor has a leader")
            .txn_commit_at_least(txn_id, record_key, candidate)
            .await;
        return TxnRunOutcome::Indeterminate;
    }

    // The commit *attempt* — its return value is deliberately not treated
    // as the outcome (see the comment below).
    let _ = decide_once(env, anchor_nodes, OP_BUDGET, |node| {
        let txn_id = txn_id.clone();
        let record_key = record_key.clone();
        async move {
            node.txn_commit_at_least(txn_id, record_key, candidate)
                .await
        }
    })
    .await;

    // ADR 0018 §2/PR5 §6: resolve is asynchronous/best-effort even on the
    // happy path — deliberately not awaited here. The resolver loop and
    // on-demand foreign-intent reads both converge it (see the module doc).
    //
    // **`commit_ts.is_some()` does NOT mean this call's own commit decided
    // the record — it only means this call's own `TxnCommit` *entry*
    // applied.** Once the duelling-decider fix (ADR 0018 §2/PR6) makes a
    // same-outcome-different-ts commit a legal no-op instead of an assert,
    // an entry can apply as a no-op against an *already-`Aborted`* record
    // (a racing recovery push that decided abort first, exactly the
    // window `RECOVERY_GRACE` < `CLIENT_TIMEOUT` makes reachable) and
    // `txn_commit_at_least` still returns `Some(ts)` — the same footgun
    // `abort_and_report` above was fixed for. Always re-read the actual
    // decided status before reporting, never assume a `Some` return means
    // "my own decision won" (mirrors `animusd::ClientCtx::
    // txn_decide_anchor`'s discipline — see `RaftKvNode::
    // txn_commit_at_least`'s own doc for the same warning). Found live by
    // this corpus's `anchor_leader_kill_mid` scenario (seed
    // 8961263187725107424): the coordinator falsely ack'd a transaction
    // that a racing recovery abort had already, correctly, decided —
    // `[14, 19]` (the pre-intent value the abort's own resolve correctly
    // restored) looked like "lost data" only because this bug recorded
    // the abort as an `ok` append.
    match query_final_status(env, anchor_nodes, record_key, OP_BUDGET).await {
        Some(TxnDecisionStatus::Committed { .. }) => TxnRunOutcome::Committed,
        Some(TxnDecisionStatus::Aborted) => TxnRunOutcome::Aborted,
        _ => TxnRunOutcome::Indeterminate,
    }
}

/// Mirrors `animusd::txn_resolver_loop`: every `RESOLVER_INTERVAL`, for each
/// group this "node" currently leads, push every locally-anchored pending
/// transaction and resolve every locally-decided-but-unresolved one.
/// One resolver sweep: for every group this loop can currently reach, push
/// every locally-anchored pending transaction and resolve every locally-
/// decided-but-unresolved one. Factored out of `resolver_loop` so
/// `force_resolve_all_owned_keys` can also run it directly — the per-key
/// `force_durably_resolve_key` pass only ever catches a *foreign* intent
/// (a participant's own key, per that function's doc); a transaction whose
/// *anchor* key is still only locally `Pending` (this same group holds
/// both the intent and the record) needs this group-level sweep instead,
/// since `FastRead::Pending` carries no `txn_id`/`record_key` a per-key
/// caller could act on directly.
async fn resolver_tick(env: &SimEnv, topo: &Topology) {
    for nodes in &topo.nodes {
        let Some(leader) = leader_of(nodes).cloned() else {
            continue;
        };
        for (txn_id, (record_key, _created_ts)) in leader.pending_txns() {
            push(env, topo, nodes, record_key, txn_id).await;
        }
        for (txn_id, (record_key, outcome)) in leader.unresolved_decided() {
            if let Some(view) = leader.txn_record_view(&record_key).await {
                recovery_resolve(env, topo, record_key, txn_id, &view.intent_spans, &outcome).await;
            }
        }
    }
}

async fn resolver_loop(env: SimEnv, topo: Arc<Topology>) {
    loop {
        env.sleep(RESOLVER_INTERVAL).await;
        resolver_tick(&env, &topo).await;
    }
}

// ---------------------------------------------------------------------------
// The three transaction shapes.
// ---------------------------------------------------------------------------

/// Write-only, multi-key list-append over 2–3 of this client's own owned
/// keys (spanning that many distinct groups — cross-tablet by construction).
/// **Never a begin-time read**: `my_lists` is this client's own authoritative
/// per-key list; a candidate (list-with-the-new-value-appended) is computed
/// from it, and only ever written back into `my_lists` once the outcome is
/// `Committed` or `Indeterminate` — **never** on a confirmed `Aborted`, so a
/// provably-rolled-back value can never "leak" into a later write's encoded
/// prefix (which, unlike the raftkv/Accord corpora's plain-KV workload,
/// *would* otherwise silently re-introduce a value the checker was told
/// definitely did not happen).
#[allow(clippy::too_many_arguments)]
async fn run_write_only_txn(
    env: &SimEnv,
    topo: &Topology,
    shared: &Arc<Shared>,
    proc: Process,
    keys: &[Key],
    my_lists: &mut BTreeMap<Key, Vec<u64>>,
    abandon_prepare: bool,
    abandon_commit: bool,
) {
    let mut mops = Vec::with_capacity(keys.len());
    let mut candidates: BTreeMap<Key, Vec<u64>> = BTreeMap::new();
    let mut writes = Vec::with_capacity(keys.len());
    for &k in keys {
        let v = shared.fresh_value();
        let mut list = my_lists.get(&k).cloned().unwrap_or_default();
        list.push(v);
        writes.push((table_of_key(k), key_bytes(k), Some(encode_list(&list))));
        candidates.insert(k, list);
        mops.push(Mop::Append { key: k, value: v });
    }
    shared
        .rec
        .lock()
        .unwrap()
        .invoke(proc, env.now().0, mops.clone());

    let outcome = run_txn(env, topo, writes, None, abandon_prepare, abandon_commit).await;
    if !matches!(outcome, TxnRunOutcome::Aborted) {
        for (k, l) in candidates {
            my_lists.insert(k, l);
        }
    }
    let mut rec = shared.rec.lock().unwrap();
    match outcome {
        TxnRunOutcome::Committed => rec.ok(proc, env.now().0, mops),
        TxnRunOutcome::Aborted => rec.fail(proc, env.now().0, mops),
        TxnRunOutcome::Indeterminate => rec.info(proc, env.now().0, mops),
    }
}

/// A multi-key "read every key as of the same moment" primitive built from
/// repeated **concurrent** rounds of latest reads (`linearizable_get_served`,
/// `futures::future::join_all` — never a sequential per-key loop, which lets
/// a slow key observe a much later moment than a fast one), accepted once
/// two consecutive rounds agree byte-for-byte on every key. `None` = gave up
/// (a key never served, or the rounds never agreed) within `OP_BUDGET`.
///
/// Replaces two earlier, abandoned designs for this same problem — found
/// live, in order, while developing this corpus:
/// 1. **A single coordinator-minted `read_at` snapshot ts**: undermined by
///    `RaftKvNode::mint_pushed`'s write-conflict floor
///    (`raise_low_water(committed_ceiling())`), which stamps a write
///    *above* whatever ceiling a **prior** future-padded read already
///    pushed that group's committed ceiling to — and since `Hlc::mint` is
///    monotonic, that elevated write becomes a **permanent** new floor for
///    every future mint on that group, so no fixed or dynamically-sampled
///    margin closes it (the group's clock only ever ratchets further ahead
///    of wall-clock, never back). Found by `participant_leader_kill_early`
///    (seed 2743871795844702347), then again by plain `baseline` (seed
///    17545084747175723362, no fault injection at all).
/// 2. **Force-resolve once, then read every key sequentially**: a slow
///    key's own resolve/read can itself take real sim time, so a
///    transaction touching an *earlier*, already-read key can still land
///    before a *later* key in the same list is read. Found by
///    `baseline_read_heavy` (seed 7528627231693481155): reading key0 then
///    key3, both written by the same two transactions, saw *neither*
///    append on key0 (read first, before either transaction had committed)
///    but *both* on key3 (read ~700ms of sim time later, by which point
///    both had).
///
/// A single **concurrent** round (this function, minus the two-round
/// check) narrows that window to one round trip but doesn't eliminate it —
/// group-to-group ReadIndex latency still varies. Found by
/// `compound_abandon_prepare_and_partition` (seed 6530153742069050081): a
/// transaction's own participant stage landed on one group microseconds
/// after that group's own concurrent read observed it absent, while the
/// anchor's group (resolved via its local record, no physical resolve
/// needed at all) had already observed the same transaction committed.
///
/// **Two-round agreement is what actually closes it**: if nothing changed
/// between two independent concurrent rounds, no transaction was in flight
/// touching any involved key during that whole window, so the read is
/// genuinely consistent — not just "probably fine," provably so from the
/// two observations themselves.
async fn quiescent_multi_read(
    env: &SimEnv,
    topo: &Topology,
    keys: &[Key],
) -> Option<Vec<Option<Vec<u8>>>> {
    // Proactively resolve any still-foreign intent whose transaction has
    // already decided (mirrors the final-state check's own use of this) so
    // a merely-undurable resolve doesn't masquerade as "not yet committed".
    let force_futs = keys
        .iter()
        .map(|&k| force_durably_resolve_key(env, topo, table_of_key(k), key_bytes(k)));
    futures::future::join_all(force_futs).await;

    let deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
    let mut previous: Option<Vec<Option<Vec<u8>>>> = None;
    loop {
        let read_futs = keys.iter().map(|&k| {
            let nodes = topo.for_table(table_of_key(k));
            async move {
                with_leader_retry(env, nodes, OP_BUDGET, |node| {
                    let key = key_bytes(k);
                    async move { node.linearizable_get_served(&key).await }
                })
                .await
            }
        });
        let round: Vec<Option<Option<Vec<u8>>>> = futures::future::join_all(read_futs).await;
        if round.iter().any(Option::is_none) {
            return None;
        }
        let round: Vec<Option<Vec<u8>>> = round.into_iter().map(Option::unwrap).collect();
        if previous.as_ref() == Some(&round) {
            return Some(round);
        }
        previous = Some(round);
        if env.now().0 >= deadline {
            return None;
        }
        env.sleep(POLL).await;
    }
}

/// Read-only, multi-key: 2–3 keys across distinct groups, each read at
/// **latest** (`linearizable_get_served`, ReadIndex) **concurrently**
/// (`futures::future::join_all`, never a sequential per-key loop), retried
/// as a whole until two consecutive concurrent rounds agree
/// (`quiescent_multi_read`, below) — this is what gives a *multi-key*
/// read-only op the joint consistency Elle needs to treat its several
/// `Mop::Read`s as one atomic transaction: unlike `run_point_read` below
/// (deliberately single-key), bundling **independent, non-synchronized**
/// per-key reads into one multi-mop op here would be a torn-read false
/// positive (each key observed at its own unrelated instant, with no joint
/// atomicity claim the mechanism could back) — the exact hazard
/// `raftkv_linearizable.rs`'s module doc calls out; caught during this
/// file's own development via `txn_corpus_is_serializable` flagging a
/// spurious cycle before this comment (and the split with
/// `run_point_read`) existed. See `quiescent_multi_read`'s own doc for the
/// two-round-agreement design and the two abandoned single-round designs
/// it replaced. The whole op is recorded `info` if any key can't be
/// resolved, or the rounds never agree, within budget — never a
/// partial/half-observed `ok`.
async fn run_read_only_txn(
    env: &SimEnv,
    topo: &Topology,
    shared: &Arc<Shared>,
    proc: Process,
    keys: &[Key],
) {
    let invoke_mops: Vec<Mop> = keys
        .iter()
        .map(|&k| Mop::Read {
            key: k,
            observed: None,
        })
        .collect();
    shared
        .rec
        .lock()
        .unwrap()
        .invoke(proc, env.now().0, invoke_mops);

    let ok_values = quiescent_multi_read(env, topo, keys).await;
    let ok = ok_values.is_some();
    let observed: Vec<(Key, Vec<u64>)> = ok_values
        .into_iter()
        .flatten()
        .zip(keys.iter())
        .map(|(v, &k)| (k, decode_list(&v.unwrap_or_default())))
        .collect();

    let mut rec = shared.rec.lock().unwrap();
    if ok {
        let mops = observed
            .into_iter()
            .map(|(k, l)| Mop::Read {
                key: k,
                observed: Some(l),
            })
            .collect();
        rec.ok(proc, env.now().0, mops);
    } else {
        let mops = keys
            .iter()
            .map(|&k| Mop::Read {
                key: k,
                observed: None,
            })
            .collect();
        rec.info(proc, env.now().0, mops);
    }
}

/// A single-key point read via `read_resolving_once` — the exact
/// `linearizable_get_served_fast` → cross-tablet `TxnStatus` →
/// `resolve_intent_given_status` round trip `animusd::
/// ClientCtx::cp_get_local_resolving` performs over the network (ADR 0018
/// §2/PR4 §3, lifted per PR5 §4), including a recovery `push` on a
/// still-`Pending`/unreachable foreign intent. Deliberately **single-key**
/// (see `run_read_only_txn`'s doc for why a multi-key op needs the snapshot
/// mechanism instead) — recorded as its own one-`Mop::Read` Elle op, which
/// needs no cross-key atomicity claim at all.
async fn run_point_read(
    env: &SimEnv,
    topo: &Topology,
    shared: &Arc<Shared>,
    proc: Process,
    key: Key,
) {
    shared.rec.lock().unwrap().invoke(
        proc,
        env.now().0,
        vec![Mop::Read {
            key,
            observed: None,
        }],
    );

    let deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
    let observed = loop {
        if let Some(v) = read_resolving_once(env, topo, table_of_key(key), &key_bytes(key)).await {
            break Some(v);
        }
        if env.now().0 >= deadline {
            break None;
        }
        env.sleep(POLL).await;
    };

    let mut rec = shared.rec.lock().unwrap();
    match observed {
        Some(v) => rec.ok(
            proc,
            env.now().0,
            vec![Mop::Read {
                key,
                observed: Some(decode_list(&v.unwrap_or_default())),
            }],
        ),
        None => rec.info(
            proc,
            env.now().0,
            vec![Mop::Read {
                key,
                observed: None,
            }],
        ),
    }
}

/// Read-modify-write: append to 2 of this client's own owned keys (in
/// `write_groups`, single-writer, exactly like the write-only shape) —
/// **conditioned on** a precondition read of a *different* client's owned
/// key (`watch_group`/`watch_owner`, in the one group this transaction does
/// not itself write to), rechecked by value right before the commit
/// decision; a mismatch aborts the whole transaction. This is the shape
/// with genuine G2/write-skew teeth: this transaction's commit depends on a
/// read of something a *different*, concurrently-running transaction
/// writes — a real cross-transaction read-write anti-dependency — without
/// ever having two transactions race to stage the same key (see the doc
/// below for why that combination is out of scope).
#[allow(clippy::too_many_arguments)]
async fn run_rmw_txn(
    env: &SimEnv,
    topo: &Topology,
    shared: &Arc<Shared>,
    proc: Process,
    my_lists: &mut BTreeMap<Key, Vec<u64>>,
    write_groups: [usize; 2],
    watch_group: usize,
    watch_owner: usize,
    abandon_prepare: bool,
    abandon_commit: bool,
) {
    // The watched key: a DIFFERENT client's own owned key, in the one
    // group this transaction does not itself write to. **Never** written
    // here — only read as a precondition and rechecked right before the
    // commit decision (mirrors `animusd::ClientCtx::cp_txn`'s own
    // documented condition-read design, ADR 0018 §2/PR4 §6). This is what
    // gives the shape genuine G2/write-skew teeth — the commit depends on
    // a read of something a *different* transaction concurrently writes —
    // without ever staging two transactions' intents on the same key
    // (which the storage layer's plain, unconditional `TxnStage` merge
    // does not arbitrate; see this file's own module doc / the ADR 0018
    // PR6 amendment for the harness bug this design replaced).
    let watch_key = owned_key(watch_owner, watch_group);
    let watch_table = table_of_key(watch_key);
    let watch_bytes = key_bytes(watch_key);
    let precondition_val =
        match read_served_with_retry(env, topo.for_table(watch_table), &watch_bytes, OP_BUDGET)
            .await
        {
            Some(v) => v,
            // Couldn't even establish the precondition this round — skip
            // silently (no history entry: we never actually attempted an op).
            None => return,
        };

    let mut mops = Vec::with_capacity(2);
    let mut writes = Vec::with_capacity(2);
    let mut candidates: BTreeMap<Key, Vec<u64>> = BTreeMap::new();
    for &g in &write_groups {
        let k = owned_key(proc as usize, g);
        let value = shared.fresh_value();
        let mut list = my_lists.get(&k).cloned().unwrap_or_default();
        list.push(value);
        writes.push((table_of_key(k), key_bytes(k), Some(encode_list(&list))));
        candidates.insert(k, list);
        mops.push(Mop::Append { key: k, value });
    }
    shared
        .rec
        .lock()
        .unwrap()
        .invoke(proc, env.now().0, mops.clone());

    let preconditions = vec![(watch_table, watch_bytes, precondition_val)];
    let outcome = run_txn(
        env,
        topo,
        writes,
        Some(preconditions),
        abandon_prepare,
        abandon_commit,
    )
    .await;
    if !matches!(outcome, TxnRunOutcome::Aborted) {
        for (k, l) in candidates {
            my_lists.insert(k, l);
        }
    }
    let mut rec = shared.rec.lock().unwrap();
    match outcome {
        TxnRunOutcome::Committed => rec.ok(proc, env.now().0, mops),
        TxnRunOutcome::Aborted => rec.fail(proc, env.now().0, mops),
        TxnRunOutcome::Indeterminate => rec.info(proc, env.now().0, mops),
    }
}

// ---------------------------------------------------------------------------
// The client driver loop.
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn client_loop(
    env: SimEnv,
    topo: Arc<Topology>,
    shared: Arc<Shared>,
    proc: Process,
    workload: Workload,
) {
    let mut my_lists: BTreeMap<Key, Vec<u64>> = BTreeMap::new();
    for _round in 0..workload.rounds {
        let roll = env.gen_below(100);
        let abandon_prepare = env.gen_below(100) < workload.abandon_prepare_pct;
        let abandon_commit = !abandon_prepare && env.gen_below(100) < workload.abandon_commit_pct;
        if roll < workload.write_pct {
            // 2 or 3 of this client's own owned keys (one per group already
            // by construction), ordered ascending by group so group 0 (the
            // client's group-0 key) is always the anchor when included.
            let n = 2 + (env.gen_below(2) as usize);
            let keys: Vec<Key> = (0..n).map(|g| owned_key(proc as usize, g)).collect();
            run_write_only_txn(
                &env,
                &topo,
                &shared,
                proc,
                &keys,
                &mut my_lists,
                abandon_prepare,
                abandon_commit,
            )
            .await;
        } else if roll < workload.write_pct + workload.read_pct {
            // Mostly the multi-key snapshot-read shape (b); a fraction are
            // single-key point reads instead, exercising the foreign-intent
            // read-path push (`run_point_read`'s doc) — deliberately never
            // bundled together (see `run_read_only_txn`'s doc for why).
            if env.gen_below(4) == 0 {
                let g = env.gen_below(NUM_GROUPS as u64) as usize;
                let key = owned_key(env.gen_below(NUM_CLIENTS as u64) as usize, g);
                run_point_read(&env, &topo, &shared, proc, key).await;
            } else {
                let n = 2 + (env.gen_below(2) as usize);
                let mut groups: Vec<usize> = (0..NUM_GROUPS).collect();
                groups.truncate(n);
                let keys: Vec<Key> = groups
                    .iter()
                    .map(|&g| owned_key(env.gen_below(NUM_CLIENTS as u64) as usize, g))
                    .collect();
                run_read_only_txn(&env, &topo, &shared, proc, &keys).await;
            }
        } else {
            // Two of this client's own groups to write to; the remaining
            // (third) group hosts the watched precondition key, owned by a
            // different client (see `run_rmw_txn`'s doc).
            let watch_group = env.gen_below(NUM_GROUPS as u64) as usize;
            let write_groups: Vec<usize> = (0..NUM_GROUPS).filter(|&g| g != watch_group).collect();
            let mut watch_owner = env.gen_below(NUM_CLIENTS as u64) as usize;
            while watch_owner == proc as usize {
                watch_owner = env.gen_below(NUM_CLIENTS as u64) as usize;
            }
            run_rmw_txn(
                &env,
                &topo,
                &shared,
                proc,
                &mut my_lists,
                [write_groups[0], write_groups[1]],
                watch_group,
                watch_owner,
                abandon_prepare,
                abandon_commit,
            )
            .await;
        }
        env.sleep(POLL).await;
    }
}

// ---------------------------------------------------------------------------
// The scenario runner.
// ---------------------------------------------------------------------------

struct ScenarioResult {
    cycles: animus_test::CheckReport,
    durability: animus_test::CheckReport,
    convergence: animus_test::CheckReport,
    history: History,
    ok_txns: usize,
    fail_txns: usize,
    info_txns: usize,
    cross_tablet_ok_txns: usize,
}

fn final_state(topo: &Topology, replica: usize) -> BTreeMap<Key, Vec<u64>> {
    use futures::executor::block_on;
    let mut map = BTreeMap::new();
    for g in 0..NUM_GROUPS {
        let node = &topo.nodes[g][replica];
        for c in 0..NUM_CLIENTS {
            let k = owned_key(c, g);
            let list = block_on(node.local_get(&key_bytes(k)))
                .map(|b| decode_list(&b))
                .unwrap_or_default();
            map.insert(k, list);
        }
    }
    map
}

/// Issue one resolving read of every owned key (`run_scenario`'s own doc,
/// right before its call sites, explains why this is necessary) and drive
/// `sim` until the pass completes or `FORCE_RESOLVE_BUDGET` elapses.
/// Best-effort: a read that can't resolve within budget is simply left for
/// the next call (the convergence-poll loop calls this every iteration).
/// **Durably** resolve `key`, unlike `read_resolving_once`/`resolve_intent_
/// given_status` (both read-side only — they compute the right answer for
/// *one read*, per `animusd::ClientCtx::cp_get_local_resolving`'s identical,
/// documented design, but never rewrite the key's own stored envelope).
/// `TxnTracker::unresolved_decided`'s own doc names exactly this gap: an
/// anchor stops tracking a transaction once its *own* keys resolve, even if
/// a participant's own intent (a different tablet) never got a proactive
/// resolve fan-out — accepted in production because *any* read still gets
/// the correct value; but a key nobody ever reads again then never
/// durably settles. This test-only helper closes that gap for the corpus's
/// own final-state check (which deliberately reads raw, unresolving
/// `local_get` per replica — see `final_state`'s doc): if `key` still holds
/// a decided, foreign intent, propose an actual `txn_resolve` for it
/// directly (mirroring `recovery_resolve`, scoped to just this one key).
async fn force_durably_resolve_key(
    env: &SimEnv,
    topo: &Topology,
    table: &'static str,
    key: Vec<u8>,
) {
    let nodes = topo.for_table(table);
    let Some(node) = leader_of(nodes).cloned() else {
        return;
    };
    let Some(FastRead::Foreign(info)) = node.linearizable_get_served_fast(&key).await else {
        return;
    };
    let record_nodes = topo.for_table(&info.record_table);
    let mut status = match leader_of(record_nodes) {
        Some(leader) => leader.txn_status_local(&info.record_key).await,
        None => None,
    };
    if !matches!(
        status,
        Some(TxnDecisionStatus::Committed { .. } | TxnDecisionStatus::Aborted)
    ) {
        push(
            env,
            topo,
            record_nodes,
            info.record_key.clone(),
            info.txn_id.clone(),
        )
        .await;
        status = match leader_of(record_nodes) {
            Some(leader) => leader.txn_status_local(&info.record_key).await,
            None => None,
        };
    }
    if let Some(status) = status
        && let Some(outcome) = to_outcome(&status)
    {
        let mut end = key.clone();
        end.push(0);
        let span = KeyRange::new(key, Some(end));
        recovery_resolve(
            env,
            topo,
            info.record_key,
            info.txn_id,
            &[(table.to_string(), span)],
            &outcome,
        )
        .await;
    }
}

fn force_resolve_all_owned_keys(sim: &mut Simulator, topo: &Arc<Topology>) {
    let env = sim.env(nid(FORCE_RESOLVE_ID));
    let topo = Arc::clone(topo);
    let done: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let done2 = Arc::clone(&done);
    let env2 = env.clone();
    env.spawn_task(async move {
        // An extra, immediate resolver sweep (mirrors `resolver_loop`'s own
        // periodic one) — catches a transaction whose *anchor* key is still
        // only locally `Pending` (see `resolver_tick`'s doc for why the
        // per-key pass below can't).
        resolver_tick(&env2, &topo).await;
        for g in 0..NUM_GROUPS {
            for c in 0..NUM_CLIENTS {
                let k = owned_key(c, g);
                force_durably_resolve_key(&env2, &topo, table_of_key(k), key_bytes(k)).await;
            }
        }
        *done2.lock().unwrap() = true;
    });
    let deadline = sim.now().0 + FORCE_RESOLVE_BUDGET.as_nanos() as u64;
    while !*done.lock().unwrap() && sim.now().0 < deadline {
        sim.run_for(Duration::from_millis(200));
    }
}

fn run_scenario(scenario: &Scenario) -> ScenarioResult {
    let mut sim = Simulator::new(scenario.seed);
    let topo = Arc::new(Topology::start(&sim));
    sim.run_for(ELECT);

    let shared = Arc::new(Shared {
        rec: Mutex::new(Recorder::new(scenario.seed)),
        next_value: Mutex::new(0),
    });

    for (c, &client_id) in CLIENT_IDS.iter().enumerate().take(NUM_CLIENTS) {
        let env = sim.env(nid(client_id));
        let topo = Arc::clone(&topo);
        let shared = Arc::clone(&shared);
        let workload = scenario.workload.clone();
        env.clone().spawn_task(async move {
            client_loop(env, topo, shared, c as Process, workload).await;
        });
    }

    let resolver_env = sim.env(nid(RESOLVER_ID));
    {
        let topo = Arc::clone(&topo);
        resolver_env.clone().spawn_task(async move {
            resolver_loop(resolver_env, topo).await;
        });
    }

    let mut faults = scenario.faults.clone();
    faults.sort_by_key(|(at, _)| *at);
    let base = sim.now().0;
    let mut crashed: BTreeSet<u64> = BTreeSet::new();
    for (at, nem) in faults {
        let target = base + at.as_nanos() as u64;
        if target > sim.now().0 {
            sim.run_until(animus_env::Nanos(target));
        }
        apply_nemesis(&sim, &topo, nem, &mut crashed);
    }

    if !scenario.window.is_zero() {
        sim.run_for(scenario.window);
    }
    heal_all(&sim, &mut crashed);
    sim.run_for(DRAIN);

    let history = shared.rec.lock().unwrap().history().clone();
    let cycles = check_cycles(&history);

    // Force a resolving read of every owned key before taking the final,
    // raw cross-replica snapshot below. This is load-bearing, not
    // belt-and-suspenders: `TxnTracker::unresolved_decided`'s own doc
    // documents a **deliberate, accepted** approximation — an anchor only
    // ever observes a resolve landing on *itself*, so it can stop tracking
    // a transaction once its own keys resolve, even if a participant's own
    // intent (a different tablet entirely) never got a proactive resolve
    // fan-out. The system's own answer is "a straggling remote intent is
    // still resolved on demand the moment any reader hits it" — but
    // `final_state`'s raw, per-replica `local_get` is deliberately *not*
    // such a reader (it never resolves anything, by design, so it stays a
    // meaningful cross-replica comparison) — so nothing in this corpus's
    // own workload would otherwise ever trigger that on-demand path for a
    // key nobody reads again after its last write. Found live by the ADR
    // 0018 multi-tablet transaction corpus's `anchor_leader_kill_early`
    // scenario (seed 3924719889167511385): a participant's own committed
    // append looked like a lost write purely because nothing had ever
    // resolved it, not because it was actually lost.
    force_resolve_all_owned_keys(&mut sim, &topo);

    let last = 2usize; // 3rd replica of each group
    let mut a = final_state(&topo, 0);
    let mut b = final_state(&topo, last);
    let mut durability = check_durability(&history, &a);
    let mut convergence = check_convergence(scenario.seed, &a, &b);
    let poll_deadline = sim.now().0 + CONVERGENCE_BUDGET.as_nanos() as u64;
    while !(convergence.ok && durability.ok) && sim.now().0 < poll_deadline {
        sim.run_for(CONVERGENCE_POLL_STEP);
        force_resolve_all_owned_keys(&mut sim, &topo);
        a = final_state(&topo, 0);
        b = final_state(&topo, last);
        durability = check_durability(&history, &a);
        convergence = check_convergence(scenario.seed, &a, &b);
    }

    let mut ok_txns = 0usize;
    let mut fail_txns = 0usize;
    let mut info_txns = 0usize;
    let mut cross_tablet_ok_txns = 0usize;
    for e in &history.entries {
        match e.outcome {
            animus_test::history::Outcome::Ok => {
                ok_txns += 1;
                let groups: BTreeSet<usize> = e
                    .mops
                    .iter()
                    .map(|m| match m {
                        Mop::Append { key, .. } => group_of_key(*key),
                        Mop::Read { key, .. } => group_of_key(*key),
                    })
                    .collect();
                if groups.len() >= 2 {
                    cross_tablet_ok_txns += 1;
                }
            }
            animus_test::history::Outcome::Fail => fail_txns += 1,
            animus_test::history::Outcome::Info => info_txns += 1,
            animus_test::history::Outcome::Invoke => {}
        }
    }

    ScenarioResult {
        cycles,
        durability,
        convergence,
        history,
        ok_txns,
        fail_txns,
        info_txns,
        cross_tablet_ok_txns,
    }
}

/// Asserts the three checks on one scenario result. Serializability is a
/// **safety** property (a hard assert at any depth, including under
/// beyond-uncertainty clock skew — see the module doc); durability +
/// convergence sit behind the converged-or-timeout poll already, so a
/// failure here means the budget was genuinely exhausted.
fn assert_scenario_ok(s: &Scenario, r: &ScenarioResult) {
    assert!(
        r.cycles.ok,
        "scenario {} not serializable: {:?} (seed={})",
        s.name, r.cycles.violations, s.seed
    );
    assert!(
        r.durability.ok,
        "scenario {} lost an acked append: {:?} (seed={})",
        s.name, r.durability.violations, s.seed
    );
    assert!(
        r.convergence.ok,
        "scenario {} did not converge: {:?} (seed={})",
        s.name, r.convergence.violations, s.seed
    );
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[test]
fn txn_baseline_is_serializable() {
    let scenario = cell("baseline", Workload::default_mix(), vec![], Duration::ZERO);
    let r = run_scenario(&scenario);
    assert_scenario_ok(&scenario, &r);
    assert!(r.ok_txns > 0, "no committed transactions — vacuous run");
    assert!(
        r.cross_tablet_ok_txns * 2 >= r.ok_txns,
        "fewer than half the committed transactions were cross-tablet (seed={})",
        scenario.seed
    );
}

#[test]
fn txn_corpus_is_serializable() {
    let scenarios = corpus();
    let mut total_ok = 0usize;
    let mut total_fail = 0usize;
    let mut total_info = 0usize;
    let mut total_cross_tablet_ok = 0usize;
    for s in &scenarios {
        let r = run_scenario(s);
        // Serializability is a SAFETY property — must hold on every
        // scenario, including under beyond-uncertainty clock skew.
        assert_scenario_ok(s, &r);
        total_ok += r.ok_txns;
        total_fail += r.fail_txns;
        total_info += r.info_txns;
        total_cross_tablet_ok += r.cross_tablet_ok_txns;
    }
    // Non-vacuity / teeth guards.
    assert!(
        total_ok > scenarios.len(),
        "corpus too vacuous: only {total_ok} committed transactions across {} scenarios",
        scenarios.len()
    );
    assert!(
        total_cross_tablet_ok * 2 >= total_ok,
        "fewer than half of all committed transactions were cross-tablet: {total_cross_tablet_ok}/{total_ok}"
    );
    assert!(
        total_fail > 0,
        "no aborted transaction observed anywhere in the corpus — the rmw \
         contention/refresh-check shape has no teeth"
    );
    assert!(
        total_info > 0,
        "no indeterminate transaction observed anywhere in the corpus — \
         faults/abandonment aren't genuinely exercising recovery"
    );
}

/// Coverage guard (mirrors `raftkv_corpus_covers_the_fault_matrix`): every
/// fault class, both abandonment flavors, and a real outage window must
/// stay represented. Structural only (no scenario runs).
#[test]
fn txn_corpus_covers_the_fault_matrix() {
    let cells = corpus_cells();

    let mut seen_faults: BTreeSet<Nemesis> = BTreeSet::new();
    let mut windowed = 0usize;
    let mut baselines = 0usize;
    let mut abandon_prepare = 0usize;
    let mut abandon_commit = 0usize;
    let mut compound = 0usize;
    for s in &cells {
        if s.faults.is_empty() {
            baselines += 1;
        }
        if s.faults.len() > 1 {
            compound += 1;
        }
        if !s.window.is_zero() {
            windowed += 1;
        }
        if s.workload.abandon_prepare_pct > 0 {
            abandon_prepare += 1;
        }
        if s.workload.abandon_commit_pct > 0 {
            abandon_commit += 1;
        }
        for (_, f) in &s.faults {
            seen_faults.insert(*f);
        }
    }

    for f in [
        Nemesis::AnchorLeaderKill,
        Nemesis::ParticipantLeaderKill { group: 1 },
        Nemesis::ParticipantLeaderKill { group: 2 },
        Nemesis::PartitionDuringPrepare { group: 1 },
        Nemesis::PartitionDuringPrepare { group: 2 },
        Nemesis::Lossy,
        Nemesis::ClockSkewWithin,
        Nemesis::ClockSkewBeyond,
    ] {
        assert!(
            seen_faults.contains(&f),
            "fault {f:?} is not covered by any corpus scenario"
        );
    }
    assert!(
        baselines >= 3,
        "expected >= 3 no-fault baselines, found {baselines}"
    );
    assert!(
        abandon_prepare >= 2,
        "expected >= 2 coordinator-abandon-prepare cells, found {abandon_prepare}"
    );
    assert!(
        abandon_commit >= 2,
        "expected >= 2 coordinator-abandon-commit cells, found {abandon_commit}"
    );
    assert!(
        compound >= 2,
        "expected >= 2 compound (multi-fault) scenarios, found {compound}"
    );
    assert!(
        windowed >= 10,
        "expected >= 10 scenarios with a real outage window, found {windowed}"
    );
    assert!(
        cells.len() >= 20,
        "corpus shrank unexpectedly to {} cells",
        cells.len()
    );
    assert!(
        cells.len() <= 30,
        "corpus grew past the documented ~20-30 cell budget: {} cells",
        cells.len()
    );

    let names: BTreeSet<&str> = cells.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names.len(), cells.len(), "corpus names must be unique");
    let seeds: BTreeSet<u64> = cells.iter().map(|s| s.seed).collect();
    assert_eq!(seeds.len(), cells.len(), "corpus seeds must be unique");

    // Frozen-name discipline: a sample of the original cells stay present,
    // seeded by their own names.
    for legacy in [
        "baseline",
        "coordinator_abandon_prepare",
        "anchor_leader_kill_mid",
        "participant_leader_kill_early",
        "clock_skew_beyond_uncertainty",
    ] {
        let c = cells
            .iter()
            .find(|s| s.name == legacy)
            .unwrap_or_else(|| panic!("frozen cell {legacy} disappeared from the corpus"));
        assert_eq!(c.seed, name_seed(legacy), "frozen seed moved for {legacy}");
    }
}

/// Seed-depth lever (`ANIMUS_TXN_SEEDS`): expanding by `k` yields exactly
/// `k×` scenarios, names/seeds stay unique, and variant 0 preserves the
/// canonical frozen name+seed.
#[test]
fn txn_seed_expansion_is_additive_and_unique() {
    let base = corpus_cells();
    let k = 3;
    let expanded = seed_expand(base.clone(), k);
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
    assert_eq!(seed_expand(base.clone(), 1).len(), base.len());
}

#[test]
fn txn_run_is_deterministic() {
    let scenario = cell(
        "anchor_leader_kill_determinism_check",
        Workload::default_mix(),
        vec![(MID, Nemesis::AnchorLeaderKill)],
        WINDOW,
    );
    let a = run_scenario(&scenario);
    let b = run_scenario(&scenario);
    assert_eq!(
        serde_json::to_string(&a.history).unwrap(),
        serde_json::to_string(&b.history).unwrap(),
        "history not reproducible for seed {}",
        scenario.seed
    );
}

// The teeth-proof exercise (deliberately breaking `run_txn`'s precondition
// refresh, confirming `check_cycles` flags a genuine G2 cycle on a live
// corpus run, then reverting) was performed during development — see the
// ADR 0018 PR6 amendment for the scenario/seed/violation evidence. The
// negative control that keeps proving the checker itself can reject a
// non-serializable history stays `negative_control.rs` (same checkers) —
// deliberately not duplicated here.

// ---------------------------------------------------------------------------
// Tight-pair torn-snapshot regression (torn-pair-fix stack, PR2 of the ADR
// 0018 §2 amendment) — see `animusd::dynamo::run_transact_get`'s doc (and
// the ADR amendment itself) for the production bug this proves closed at
// the protocol level. `animusd/tests/dynamo_txn.rs::
// transact_get_items_never_observes_a_torn_pair_under_concurrent_writes` is
// the real wire-level acceptance test; this is its SimEnv-driven,
// seed-reproducible analog over the raw 2PC primitives — deliberately its
// own small scenario, following this file's naming/depth conventions
// (`name_seed`, `ANIMUS_TXN_SEEDS`/`seeds_per_cell`) rather than the
// `Scenario`/`run_scenario`/Elle-history machinery above, because the
// property under test (a numeric sum invariant across one contended key
// pair, sampled by a round-agreement reader) is not a `Mop::Append`/`Read`
// serializability claim `check_cycles` is built to check.
// ---------------------------------------------------------------------------

/// Back-to-back sum-to-zero transaction count — mirrors `animusd/tests/
/// dynamo_txn.rs`'s own `1..=15` step count, rounded up for a bit more
/// contention window.
const TIGHT_PAIR_STEPS: i64 = 20;

fn encode_i64(v: i64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

/// `0` for anything that isn't exactly 8 bytes — covers "never written yet"
/// (before the writer's first transaction commits).
fn decode_i64(bytes: &[u8]) -> i64 {
    <[u8; 8]>::try_from(bytes)
        .map(i64::from_be_bytes)
        .unwrap_or(0)
}

/// One non-blocking, single-shot resolution attempt — the uniform design
/// `animusd::ClientCtx::cp_get_local_snapshot` gives every key of a
/// `TransactGetItems` round (ADR 0018 §2 amendment): a `Pending`/`Foreign`
/// intent (both carry the identical `IntentInfo` payload since that same
/// amendment) gets exactly one status query plus, if still pending, one
/// push attempt — **never** a further per-key wait. `None` means "did not
/// sample cleanly this instant" (a routing failure, or the intent stayed
/// undecided even after the push) — only the caller's own ROUND loop may
/// retry on that, never this function. Deliberately a separate function
/// from `read_resolving_once` above: that one backs `run_point_read`'s own,
/// intentionally different, single-key retry discipline (immediate give-up
/// on a local `Pending`, unchanged by this amendment).
async fn snapshot_read_uniform(
    env: &SimEnv,
    topo: &Topology,
    table: &str,
    key: &[u8],
) -> Option<Option<Vec<u8>>> {
    let nodes = topo.for_table(table);
    let node = leader_of(nodes)?.clone();
    match node.linearizable_get_served_fast(key).await? {
        FastRead::Value(v) => Some(v),
        FastRead::Pending(info) | FastRead::Foreign(info) => {
            let record_nodes = topo.for_table(&info.record_table);
            let status = match leader_of(record_nodes) {
                Some(leader) => leader.txn_status_local(&info.record_key).await,
                None => None,
            };
            match status {
                Some(TxnDecisionStatus::Pending) | None => {
                    push(
                        env,
                        topo,
                        record_nodes,
                        info.record_key.clone(),
                        info.txn_id.clone(),
                    )
                    .await;
                    // Uniform, single-shot: never re-query after the push —
                    // a still-pending outcome is the round loop's to retry,
                    // exactly the asymmetry fix this scenario proves.
                    None
                }
                Some(decided) => {
                    node.resolve_intent_given_status(key, None, &info.txn_id, decided)
                        .await
                }
            }
        }
    }
}

/// Round-agreement reader over a fixed key pair, uniform-single-shot per
/// key (see `snapshot_read_uniform`) — the SimEnv analog of
/// `animusd::dynamo::quiescent_multi_get`'s fixed design: both keys are read
/// **concurrently** (`futures::future::join_all`, never sequential), and a
/// round where *either* key is unresolved is discarded outright (`previous`
/// resets to `None`, never compared) — the exact discipline that closes the
/// torn-pair bug.
async fn quiescent_pair_read_uniform(
    env: &SimEnv,
    topo: &Topology,
    pair: &[(&'static str, Vec<u8>); 2],
) -> Option<[Vec<u8>; 2]> {
    let deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
    let mut previous: Option<[Vec<u8>; 2]> = None;
    loop {
        let futs = pair
            .iter()
            .map(|(table, key)| snapshot_read_uniform(env, topo, table, key));
        let round: Vec<Option<Option<Vec<u8>>>> = futures::future::join_all(futs).await;
        let sampled: Option<[Vec<u8>; 2]> = if round.iter().all(Option::is_some) {
            let mut it = round.into_iter().map(|r| r.unwrap().unwrap_or_default());
            Some([it.next().unwrap(), it.next().unwrap()])
        } else {
            None
        };
        if let (Some(r), Some(p)) = (&sampled, &previous)
            && r == p
        {
            return sampled;
        }
        previous = sampled;
        if env.now().0 >= deadline {
            return None;
        }
        env.sleep(POLL).await;
    }
}

/// The writer half of the tight-pair scenario: `TIGHT_PAIR_STEPS`
/// back-to-back sum-to-zero 2-key cross-tablet transactions on a FIXED key
/// pair — step `i` writes `key_a = i`, `key_b = -i`, mirroring `animusd/
/// tests/dynamo_txn.rs::transact_get_items_never_observes_a_torn_pair_under_concurrent_writes`'s
/// exact invariant, back-to-back with no pacing (the tight-loop shape that
/// reproduced the production bug).
async fn tight_pair_writer(
    env: SimEnv,
    topo: Arc<Topology>,
    table_a: &'static str,
    bytes_a: Vec<u8>,
    table_b: &'static str,
    bytes_b: Vec<u8>,
    done: Arc<Mutex<bool>>,
) {
    for step in 1..=TIGHT_PAIR_STEPS {
        let writes = vec![
            (table_a, bytes_a.clone(), Some(encode_i64(step))),
            (table_b, bytes_b.clone(), Some(encode_i64(-step))),
        ];
        let _ = run_txn(&env, &topo, writes, None, false, false).await;
    }
    *done.lock().unwrap() = true;
}

/// The reader half: polls the same fixed pair via
/// `quiescent_pair_read_uniform` until the writer finishes, recording every
/// completed round's decoded `(a, b)` pair — asserted against in the test
/// itself (kept out of this function so a `#[test]` failure message names
/// the seed directly, not a panic from inside a spawned task).
#[allow(clippy::too_many_arguments)]
async fn tight_pair_reader(
    env: SimEnv,
    topo: Arc<Topology>,
    table_a: &'static str,
    bytes_a: Vec<u8>,
    table_b: &'static str,
    bytes_b: Vec<u8>,
    done: Arc<Mutex<bool>>,
    rounds: Arc<Mutex<Vec<(i64, i64)>>>,
) {
    loop {
        let pair = [(table_a, bytes_a.clone()), (table_b, bytes_b.clone())];
        if let Some([av, bv]) = quiescent_pair_read_uniform(&env, &topo, &pair).await {
            rounds
                .lock()
                .unwrap()
                .push((decode_i64(&av), decode_i64(&bv)));
        }
        if *done.lock().unwrap() {
            return;
        }
        env.sleep(Duration::from_millis(5)).await;
    }
}

/// One seed's run of the tight-pair scenario (no fault injection — the
/// property under test is purely about read/write timing under a tight
/// writer, the exact shape that reproduced the production bug).
fn run_tight_pair_scenario(seed: u64) {
    let mut sim = Simulator::new(seed);
    let topo = Arc::new(Topology::start(&sim));
    sim.run_for(ELECT);

    let key_a = owned_key(0, 0);
    let key_b = owned_key(0, 1);
    let table_a = table_of_key(key_a);
    let table_b = table_of_key(key_b);
    let bytes_a = key_bytes(key_a);
    let bytes_b = key_bytes(key_b);

    let resolver_env = sim.env(nid(RESOLVER_ID));
    {
        let topo = Arc::clone(&topo);
        resolver_env.clone().spawn_task(async move {
            resolver_loop(resolver_env, topo).await;
        });
    }

    let done = Arc::new(Mutex::new(false));
    let rounds: Arc<Mutex<Vec<(i64, i64)>>> = Arc::new(Mutex::new(Vec::new()));

    let writer_env = sim.env(nid(CLIENT_IDS[0]));
    {
        let topo = Arc::clone(&topo);
        let done = Arc::clone(&done);
        let bytes_a = bytes_a.clone();
        let bytes_b = bytes_b.clone();
        writer_env.clone().spawn_task(async move {
            tight_pair_writer(writer_env, topo, table_a, bytes_a, table_b, bytes_b, done).await;
        });
    }

    let reader_env = sim.env(nid(CLIENT_IDS[1]));
    {
        let topo = Arc::clone(&topo);
        let done = Arc::clone(&done);
        let rounds = Arc::clone(&rounds);
        reader_env.clone().spawn_task(async move {
            tight_pair_reader(
                reader_env, topo, table_a, bytes_a, table_b, bytes_b, done, rounds,
            )
            .await;
        });
    }

    sim.run_for(DRAIN);

    let rounds = rounds.lock().unwrap();
    assert!(
        !rounds.is_empty(),
        "seed {seed}: reader never completed a single quiesced round"
    );
    let torn: Vec<&(i64, i64)> = rounds.iter().filter(|(a, b)| a + b != 0).collect();
    assert!(
        torn.is_empty(),
        "seed {seed}: observed {} torn pair(s) out of {} round(s) (e.g. {:?}) — a quiesced \
         round must never see a+b != 0",
        torn.len(),
        rounds.len(),
        torn.first()
    );
}

/// Depth via `ANIMUS_TXN_SEEDS` (default 1), following `seed_expand`'s own
/// variant-0-keeps-the-canonical-seed convention: variant 0 is the frozen
/// `tight_pair_never_torn` seed; `ANIMUS_TXN_SEEDS=K` runs `K` independent
/// seed variants.
#[test]
fn tight_pair_transactions_never_observe_a_torn_snapshot() {
    for variant in 0..seeds_per_cell() {
        let name = if variant == 0 {
            "tight_pair_never_torn".to_string()
        } else {
            format!("tight_pair_never_torn_s{variant:02}")
        };
        run_tight_pair_scenario(name_seed(&name));
    }
}
