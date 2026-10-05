//! Upgrade-restart harness, **tier 1: workload-shaped restarts** (ADR 0073
//! Phase 1, workstream P1-D step 2).
//!
//! Tier 0 (`upgrade_restart_tier0.rs`) seeds a disk from a checked-in fixture
//! and opens the real reader. Tier 1 runs a **live workload** at the current
//! format versions on `SimEnv` disks, stops the node(s) — cleanly, by a crash,
//! or by a crash with a torn/corrupted un-synced tail — runs each stopped
//! node's disk through the per-format **transcode table**
//! (`animus_test::upgrade::transcode`, the *identity* while every format is
//! v1), restarts fresh nodes on the current code, and keeps going. One
//! `Recorder` spans both phases, so `check_cycles` sees the combined history
//! and every acknowledged write — from before *and* after the restart — must
//! be durable on every replica.
//!
//! **Cells.** kind {`Data`: 3-replica `RaftKvNode` over `LsmEngine<SimEnv>`
//! (`raftkv.wal` + LSM WAL/manifest/SSTs), `Control`: 3-node
//! `animus_control::RaftNode` over `LsmEngine` (`raft.wal`/CWL1 + LSM),
//! `SharedWal`: one node, four tablets on one `SharedWal` (SWL1, whole-node
//! only)} x scope {`Leader`, `Follower`, `WholeGroup`} x stop {`Clean`,
//! `Crash`, `TornTail`} x `back` in `transcode::supported_back()` (only `0`
//! today, so the matrix grows with the table and never names a version).
//! `ANIMUS_UPGRADE_RESTART_SEEDS=K` runs K seeds per cell (`corpus::seed_expand`);
//! `ANIMUS_SEED=<seed>` replays one seed across every cell (optionally narrowed
//! by `ANIMUS_UPGRADE_RESTART_CELL=<substring>`). Every failure prints the cell
//! name and `seed=`.
//!
//! **Seeded, never the simulator RNG.** The mid-transcode-window crash
//! (`stop_after_files`), the mixed-version files (`keep_current_fraction`), the
//! crash instant's jitter and the catch-up partition window/peer are all drawn
//! from `splitmix64(cell seed, tag)`, so varying them never perturbs the
//! simulator's own draw order.
//!
//! **The oracle** (list-append over a per-replica final state): `check_cycles`
//! over the combined history; a **post-restart probe** (converged-or-timeout)
//! that every phase-1 ack survived *before* any phase-2 write can mask a loss
//! by rewriting the whole list; `check_durability` per replica and
//! `check_convergence` after the final converged-or-timeout poll; and
//! **non-vacuity**: acks in both phases. Engines are opened strictly
//! (`.expect`): the raftkv corpus's destroy-and-reopen fallback would mask a
//! bad transcode as a clean wipe, so it is deliberately not copied.
//!
//! **Negative controls** (on by default, fixed seed, `WholeGroup` stop) prove
//! the harness has teeth: a transcoder that drops the final record of each
//! node's WALs must surface as a lost acknowledged write; one that truncates an
//! SSTable must fail loudly at the strict engine open. An identity run of the
//! same cell must pass.
//!
//! Control-plane note (issue #495): `torn_tail_on_crash` + `corrupt_on_crash`
//! together are what #495 isolated; the per-record CRC fix means a corrupted
//! record is now dropped like a torn tail. The `Control` `TornTail` cells arm
//! both, the faults are NOT narrowed; a failure there is a real finding.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::{
    MetaCommand, Metadata, NodeStatus, ProposeResult, RaftNode, SharedWal as SharedWalCoordinator,
};
use animus_cp_data::{KvCommand, KvState, RaftKvNode, SHARED_WAL, StorageScope};
use animus_env::{Clock, Disk, EnvExt, NodeId, Rng, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions};
use animus_test::corpus::{self, SeedVariant, name_seed};
use animus_test::history::{Key, Mop, Process};
use animus_test::upgrade::transcode::{self, TranscodeOpts, TranscodeReport};
use animus_test::{
    CheckReport, History, Recorder, check_convergence, check_cycles, check_durability,
};
use futures::executor::block_on;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const GROUP_IDS: [u64; 3] = [0, 1, 2];
const CLIENT_IDS: [u64; 3] = [100, 101, 102];
const CLIENTS: usize = 3;
const KEYSPACE: u64 = 6;
const READ_PCT: u64 = 25;
/// Phase-1 rounds for a clean stop (the workload runs to completion) and for a
/// crash stop (never reached: the crash lands mid-workload).
const ROUNDS_CLEAN: u64 = 8;
const ROUNDS_CRASH: u64 = 40;
const ROUNDS_PHASE2: u64 = 6;

const OP_BUDGET: Duration = Duration::from_secs(9);
const POLL: Duration = Duration::from_millis(100);
const SETTLE: Duration = Duration::from_millis(800);
/// Latency armed on the to-be-crashed disks so an un-synced tail exists at the
/// crash instant (a zero-latency sync leaves nothing buffered between steps).
const SYNC_DELAY: Duration = Duration::from_millis(15);
const STEP: Duration = Duration::from_millis(500);
const WORKLOAD_BUDGET: Duration = Duration::from_secs(180);
const CONVERGENCE_POLL_STEP: Duration = Duration::from_secs(2);
const CONVERGENCE_BUDGET: Duration = Duration::from_secs(120);

/// Members the control plane's workload mints live at `(key + 1) * MEMBER_BASE
/// + value`, well clear of the group's own `n0..n2`.
const MEMBER_BASE: u64 = 1_000_000;

/// Tablets on the `SharedWal` kind's single node.
const SHARED_TABLETS: u64 = 4;
const SHARED_ROUNDS_CLEAN: u64 = 66;
const SHARED_ROUNDS_PHASE2: u64 = 5;

// ---------------------------------------------------------------------------
// Cells
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Data,
    Control,
    SharedWal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scope {
    Leader,
    Follower,
    WholeGroup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    /// Workload drained and synced, then the process exits.
    Clean,
    /// Mid-workload `Simulator::crash` with an un-synced tail (whole buffer
    /// dropped).
    Crash,
    /// As `Crash`, with `torn_tail_on_crash` + `corrupt_on_crash` armed.
    TornTail,
}

#[derive(Clone, Debug)]
struct Cell {
    name: String,
    seed: u64,
    kind: Kind,
    scope: Scope,
    stop: Stop,
    back: u32,
}

impl SeedVariant for Cell {
    fn scenario_name(&self) -> &str {
        &self.name
    }
    fn reseeded(&self, name: String, seed: u64) -> Self {
        Cell {
            name,
            seed,
            ..self.clone()
        }
    }
}

fn cell(kind: Kind, scope: Scope, stop: Stop, back: u32) -> Cell {
    let name = format!(
        "{}_{}_{}_b{back}",
        format!("{kind:?}").to_lowercase(),
        format!("{scope:?}").to_lowercase(),
        format!("{stop:?}").to_lowercase(),
    );
    Cell {
        seed: name_seed(&format!("upgrade_restart/{name}")),
        name,
        kind,
        scope,
        stop,
        back,
    }
}

/// The frozen cell list of one kind. `SharedWal` is one node, so whole-node only.
fn cells_of(kind: Kind) -> Vec<Cell> {
    let scopes: &[Scope] = if kind == Kind::SharedWal {
        &[Scope::WholeGroup]
    } else {
        &[Scope::Leader, Scope::Follower, Scope::WholeGroup]
    };
    let mut out = Vec::new();
    for &back in transcode::supported_back() {
        for &scope in scopes {
            for stop in [Stop::Clean, Stop::Crash, Stop::TornTail] {
                out.push(cell(kind, scope, stop, back));
            }
        }
    }
    out
}

fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_UPGRADE_RESTART_SEEDS")
}

/// The cells of `kind` after the depth knob, `ANIMUS_SEED` replay and the
/// optional `ANIMUS_UPGRADE_RESTART_CELL` name filter.
fn corpus_of(kind: Kind) -> Vec<Cell> {
    let filter = std::env::var("ANIMUS_UPGRADE_RESTART_CELL").ok();
    let cells = if let Some(seed) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        cells_of(kind)
            .into_iter()
            .map(|c| {
                let name = c.name.clone();
                c.reseeded(name, seed)
            })
            .collect()
    } else {
        corpus::seed_expand(cells_of(kind), seeds_per_cell())
    };
    cells
        .into_iter()
        .filter(|c| filter.as_ref().is_none_or(|f| c.name.contains(f.as_str())))
        .collect()
}

// ---------------------------------------------------------------------------
// Seeded parameters (never the simulator RNG)
// ---------------------------------------------------------------------------

fn splitmix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;
    x
}

fn mix(seed: u64, tag: &str) -> u64 {
    splitmix64(seed ^ name_seed(tag))
}

/// Per-victim transcode options: a seeded mix of "transcode everything",
/// "leave some files at the current version" (mixed-version files) and "stop
/// after N files" (a crash inside the transcode window).
fn transcode_opts(seed: u64, victim: u64) -> TranscodeOpts {
    let tag = format!("transcode/{victim}");
    let r = mix(seed, &tag);
    let keep = [0u32, 0, 250, 600, 1000][(r % 5) as usize];
    let stop_after = if (r >> 8).is_multiple_of(4) {
        Some(((r >> 16) % 8) as usize)
    } else {
        None
    };
    TranscodeOpts {
        keep_current_fraction_permille: keep,
        stop_after_files: stop_after,
        seed: mix(seed, &format!("{tag}/files")),
        // The engine-row pass (ADR 0073 P1-D): these workloads write no
        // transaction intent, so it rewrites nothing here, but it walks every
        // engine file on the disk and restarts on the result, so a row pass
        // that damaged an engine is caught by every cell. The cells that DO
        // carry intents are `upgrade_restart_txn_envelope`.
        row_back: transcode::supported_row_back().last().copied().unwrap_or(0),
    }
}

// ---------------------------------------------------------------------------
// The transcoder hook (the negative controls swap it)
// ---------------------------------------------------------------------------

/// Transcode one stopped node's disk. The production hook is
/// [`transcode::transcode_disk`] itself.
type Transcoder = fn(&SimEnv, u32, &TranscodeOpts) -> io::Result<TranscodeReport>;

/// Control (a): drops the final record of every node's line-framed WAL
/// (`raftkv.wal*`, `raft.wal`) and of the newest WAL segment of every LSM
/// engine on the disk (length+crc framed; one engine per `lsm/` or `tN/` prefix).
fn drop_final_wal_record(
    env: &SimEnv,
    _back: u32,
    _opts: &TranscodeOpts,
) -> io::Result<TranscodeReport> {
    let mut files = block_on(env.list())?;
    files.sort();
    // Newest segment per engine prefix (files are sorted, so the last wins).
    let mut newest_lsm_wal: BTreeMap<String, String> = BTreeMap::new();
    for f in &files {
        let bytes = block_on(env.read(f))?;
        match transcode::classify(f, &bytes).map(|e| e.name) {
            Some("control-wal") | Some("shared-wal") => {
                // Cut at the start of the last line.
                let body = bytes.strip_suffix(b"\n").unwrap_or(&bytes);
                let cut = body.iter().rposition(|&b| b == b'\n').map_or(0, |p| p + 1);
                block_on(env.replace(f, &bytes[..cut]))?;
            }
            Some("lsm-wal") => {
                let prefix = f.rsplit_once("wal-").map_or("", |(p, _)| p).to_string();
                newest_lsm_wal.insert(prefix, f.clone());
            }
            _ => {}
        }
    }
    for f in newest_lsm_wal.values() {
        let bytes = block_on(env.read(f))?;
        // 5-byte file header (`LWL1` + version), then `len(be32) crc(be32) payload`.
        let mut pos = 5usize.min(bytes.len());
        let mut starts: Vec<usize> = Vec::new();
        while pos + 8 <= bytes.len() {
            let len = u32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            if pos + 8 + len > bytes.len() {
                break;
            }
            starts.push(pos);
            pos += 8 + len;
        }
        let last_start = starts.last().copied().unwrap_or(5.min(bytes.len()));
        block_on(env.replace(f, &bytes[..last_start]))?;
    }
    Ok(TranscodeReport::default())
}

/// Control (e): truncates the largest SSTable (else the manifest) to half.
fn truncate_sstable(
    env: &SimEnv,
    _back: u32,
    _opts: &TranscodeOpts,
) -> io::Result<TranscodeReport> {
    let mut files = block_on(env.list())?;
    files.sort();
    let mut target: Option<(usize, String)> = None;
    let mut manifest: Option<String> = None;
    for f in &files {
        let bytes = block_on(env.read(f))?;
        match transcode::classify(f, &bytes).map(|e| e.name) {
            Some("lsm-sstable") if target.as_ref().is_none_or(|(n, _)| bytes.len() > *n) => {
                target = Some((bytes.len(), f.clone()));
            }
            Some("lsm-manifest") => manifest = Some(f.clone()),
            _ => {}
        }
    }
    if let Some(f) = target.map(|(_, f)| f).or(manifest) {
        let bytes = block_on(env.read(&f))?;
        block_on(env.replace(&f, &bytes[..bytes.len() / 2]))?;
    }
    Ok(TranscodeReport::default())
}

/// Control (c): removes every storage-engine file (LSM WAL, manifest, SSTables).
fn wipe_engine_files(
    env: &SimEnv,
    _back: u32,
    _opts: &TranscodeOpts,
) -> io::Result<TranscodeReport> {
    remove_where(env, |name| name.starts_with("lsm-"))
}

/// Control (b): the consensus log keeps only its first half (header line plus
/// the first half of the records) AND the engine is gone: every entry in the
/// second half of the history exists nowhere.
fn halve_log_and_wipe_engine(
    env: &SimEnv,
    back: u32,
    opts: &TranscodeOpts,
) -> io::Result<TranscodeReport> {
    let mut files = block_on(env.list())?;
    files.sort();
    for f in &files {
        let bytes = block_on(env.read(f))?;
        if matches!(
            transcode::classify(f, &bytes).map(|e| e.name),
            Some("control-wal") | Some("shared-wal")
        ) {
            let lines: Vec<&[u8]> = bytes.split_inclusive(|&b| b == b'\n').collect();
            let keep = 1 + (lines.len().saturating_sub(1)) / 2;
            let kept: Vec<u8> = lines[..keep.min(lines.len())].concat();
            block_on(env.replace(f, &kept))?;
        }
    }
    wipe_engine_files(env, back, opts)
}

/// Control (d): removes every durable file on the node.
fn wipe_everything(env: &SimEnv, _back: u32, _opts: &TranscodeOpts) -> io::Result<TranscodeReport> {
    remove_where(env, |_| true)
}

fn remove_where(env: &SimEnv, pick: impl Fn(&str) -> bool) -> io::Result<TranscodeReport> {
    let mut files = block_on(env.list())?;
    files.sort();
    for f in &files {
        let bytes = block_on(env.read(f))?;
        let name = transcode::classify(f, &bytes).map_or("unclassified", |e| e.name);
        if pick(name) {
            block_on(env.remove(f))?;
        }
    }
    Ok(TranscodeReport::default())
}

// ---------------------------------------------------------------------------
// Verdict
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct CellVerdict {
    cell: String,
    seed: u64,
    ok: bool,
    violations: Vec<String>,
    /// A panic out of the run (strict engine open, transcoder error, ...).
    panic: Option<String>,
    acks_before: usize,
    acks_after: usize,
    /// The combined history, serialised (for the determinism comparison).
    history: String,
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|m| (*m).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string())
}

/// Real wall-clock bound on one cell. Every in-sim wait is already bounded in
/// *virtual* time (`WORKLOAD_BUDGET`, `CONVERGENCE_BUDGET`, the per-op budget),
/// but a livelock with no `.await` yield point (or a zero-time event loop, such
/// as the `wal_lock` starvation this harness found) never advances virtual
/// time, so only an OS-thread bound turns it into a failure that names the
/// cell and seed. This is a *diagnostic* bound on a hang, never a timeout that
/// decides a verdict: a healthy cell finishes in seconds.
const CELL_WATCHDOG: Duration = Duration::from_secs(300);

/// Run one cell on its own thread under [`CELL_WATCHDOG`]. A panic out of the
/// run itself becomes a verdict with `panic: Some(..)` (see [`run_cell_inner`]);
/// a hang **panics here** with the cell name and seed.
fn run_cell(cell: &Cell, tc: Transcoder) -> CellVerdict {
    let (tx, rx) = std::sync::mpsc::channel();
    let c = cell.clone();
    std::thread::Builder::new()
        .name(format!("upgrade_restart/{}", cell.name))
        .spawn(move || {
            let _ = tx.send(run_cell_inner(&c, tc));
        })
        .expect("spawn the cell thread");
    match rx.recv_timeout(CELL_WATCHDOG) {
        Ok(v) => v,
        Err(e) => panic!(
            "upgrade_restart cell={} HUNG or died (seed={}): no verdict within {:?} ({e}) \
             -- replay with ANIMUS_SEED={} ANIMUS_UPGRADE_RESTART_CELL={}",
            cell.name, cell.seed, CELL_WATCHDOG, cell.seed, cell.name
        ),
    }
}

/// Run one cell. Never panics: a panic out of the run becomes a verdict with
/// `panic: Some(..)`, so a negative control can assert on it.
fn run_cell_inner(cell: &Cell, tc: Transcoder) -> CellVerdict {
    eprintln!("upgrade_restart cell={} seed={}", cell.name, cell.seed);
    let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match cell.kind {
        Kind::Data | Kind::Control => run_group_cell(cell, tc),
        Kind::SharedWal => run_sharedwal_cell(cell, tc),
    }));
    match run {
        Ok(v) => v,
        Err(p) => {
            let msg = panic_message(p);
            CellVerdict {
                cell: cell.name.clone(),
                seed: cell.seed,
                ok: false,
                violations: vec![format!("panic: {msg}")],
                panic: Some(msg),
                acks_before: 0,
                acks_after: 0,
                history: String::new(),
            }
        }
    }
}

fn assert_cell_ok(v: &CellVerdict) {
    assert!(
        v.ok,
        "upgrade_restart cell={} FAILED (seed={}): {:?}",
        v.cell, v.seed, v.violations
    );
}

// ---------------------------------------------------------------------------
// Shared list-append helpers
// ---------------------------------------------------------------------------

type State = BTreeMap<Key, Vec<u64>>;

fn encode_list(list: &[u64]) -> Vec<u8> {
    list.iter().flat_map(|v| v.to_be_bytes()).collect()
}

fn decode_list(bytes: &[u8]) -> Vec<u64> {
    bytes
        .chunks_exact(8)
        .map(|c| u64::from_be_bytes(c.try_into().unwrap()))
        .collect()
}

fn key_bytes(key: Key) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}

fn combine(seed: u64, reports: impl Iterator<Item = CheckReport>) -> CheckReport {
    let violations: Vec<String> = reports.flat_map(|r| r.violations).collect();
    CheckReport {
        ok: violations.is_empty(),
        violations,
        seed,
    }
}

fn check_states(seed: u64, history: &History, states: &[State]) -> (CheckReport, CheckReport) {
    let durability = combine(seed, states.iter().map(|s| check_durability(history, s)));
    let convergence = combine(
        seed,
        states[1..]
            .iter()
            .map(|s| check_convergence(seed, &states[0], s)),
    );
    (durability, convergence)
}

fn ok_appends(history: &History) -> usize {
    history
        .ok_entries()
        .flat_map(|e| &e.mops)
        .filter(|m| matches!(m, Mop::Append { .. }))
        .count()
}

/// Converged-or-timeout poll (never a fixed-deadline one-shot): run the
/// simulator in steps until every replica holds every ack and all agree, or the
/// budget is spent. Returns the last durability and convergence reports.
fn converge_or_timeout(
    sim: &mut Simulator,
    seed: u64,
    history: impl Fn() -> History,
    states: impl Fn() -> Vec<State>,
) -> (CheckReport, CheckReport) {
    let deadline = sim.now().0 + CONVERGENCE_BUDGET.as_nanos() as u64;
    loop {
        let h = history();
        let (d, c) = check_states(seed, &h, &states());
        if (d.ok && c.ok) || sim.now().0 >= deadline {
            return (d, c);
        }
        sim.run_for(CONVERGENCE_POLL_STEP);
    }
}

// ---------------------------------------------------------------------------
// Group cells (Data + Control)
// ---------------------------------------------------------------------------

type DataNode = RaftKvNode<SimEnv, LsmEngine<SimEnv>>;
type CtlNode = RaftNode<SimEnv>;

/// Tiny thresholds so a short workload produces real WAL rotation, flushed
/// SSTables, compaction and a manifest.
fn lsm_opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 256,
        compaction_trigger: 3,
        target_table_bytes: 1024,
        level_fanout: 2,
        wal_segment_bytes: 256,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

/// Open the node's LSM engine **strictly**. No destroy-and-reopen fallback: a
/// wipe-and-rejoin would mask exactly the defect a bad transcode causes.
fn open_engine(sim: &Simulator, id: u64) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(nid(id)), "lsm/", lsm_opts()))
        .expect("strict open of the LSM engine after restart (no destroy-and-reopen)")
}

#[derive(Clone)]
enum AnyNode {
    Data(Arc<DataNode>),
    Ctl(Arc<CtlNode>),
}

fn member_id(key: Key, value: u64) -> NodeId {
    nid((key + 1) * MEMBER_BASE + value)
}

fn parse_member(id: &NodeId) -> Option<(Key, u64)> {
    let n: u64 = id.as_str().strip_prefix('n')?.parse().ok()?;
    (n >= MEMBER_BASE).then(|| (n / MEMBER_BASE - 1, n % MEMBER_BASE))
}

/// Per-key lists of the control plane's minted members, ascending by value
/// (== commit order: a key's writer never proposes value N+1 before N committed).
fn ctl_lists(meta: &Metadata) -> State {
    let mut out: State = (0..KEYSPACE).map(|k| (k, Vec::new())).collect();
    for id in meta.members.keys() {
        if let Some((key, value)) = parse_member(id)
            && let Some(list) = out.get_mut(&key)
        {
            list.push(value);
        }
    }
    for list in out.values_mut() {
        list.sort_unstable();
    }
    out
}

impl AnyNode {
    fn is_leader(&self) -> bool {
        match self {
            Self::Data(n) => n.is_leader(),
            Self::Ctl(n) => n.is_leader(),
        }
    }

    /// Best-effort, idempotent proposal of "append `value` to `key`'s list".
    fn propose_append(&self, key: Key, value: u64, list: &[u64]) -> bool {
        match self {
            Self::Data(n) => matches!(
                n.put(key_bytes(key), encode_list(list)),
                ProposeResult::Accepted { .. }
            ),
            Self::Ctl(n) => matches!(
                n.propose(MetaCommand::UpsertMember {
                    node: member_id(key, value),
                    labels: BTreeMap::new(),
                    status: NodeStatus::Active,
                }),
                ProposeResult::Accepted { .. }
            ),
        }
    }

    /// A leader-served read of `key` (`None`: could not confirm).
    async fn read(&self, key: Key) -> Option<Vec<u64>> {
        match self {
            Self::Data(n) => n
                .linearizable_get(&key_bytes(key))
                .await
                .map(|b| decode_list(&b)),
            Self::Ctl(n) => {
                if !n.is_leader() {
                    return None;
                }
                ctl_lists(&n.metadata()).remove(&key)
            }
        }
    }

    /// This replica's own applied state (no quorum, no leadership).
    fn state(&self) -> State {
        match self {
            Self::Data(n) => (0..KEYSPACE)
                .map(|k| {
                    let list = block_on(n.local_get(&key_bytes(k)))
                        .map(|b| decode_list(&b))
                        .unwrap_or_default();
                    (k, list)
                })
                .collect(),
            Self::Ctl(n) => ctl_lists(&n.metadata()),
        }
    }
}

type Nodes = Arc<Mutex<Vec<AnyNode>>>;

fn start_node(kind: Kind, sim: &Simulator, id: u64) -> AnyNode {
    let all: Vec<NodeId> = GROUP_IDS.iter().copied().map(nid).collect();
    let engine = open_engine(sim, id);
    let env = sim.env(nid(id));
    match kind {
        Kind::Data => AnyNode::Data(Arc::new(RaftKvNode::start(env, all, engine))),
        Kind::Control => AnyNode::Ctl(Arc::new(RaftNode::start(env, all, engine))),
        Kind::SharedWal => unreachable!("SharedWal has its own runner"),
    }
}

fn leader_slot(nodes: &Nodes) -> Option<(usize, AnyNode)> {
    let guard = nodes.lock().unwrap();
    guard
        .iter()
        .position(AnyNode::is_leader)
        .map(|i| (i, guard[i].clone()))
}

struct Shared {
    rec: Mutex<Recorder>,
    next_value: Mutex<u64>,
    /// Each key's authoritative list (single writer per key), carried across
    /// phases: a phase-2 writer extends what phase 1 wrote.
    lists: Mutex<BTreeMap<Key, Vec<u64>>>,
    /// Keys with at least one acknowledged append (the only ones worth reading:
    /// a `None` read of a never-written key is indistinguishable from a
    /// deposed leader that cannot confirm).
    written: Mutex<BTreeSet<Key>>,
    done: Mutex<usize>,
    /// The control plane's writes retry until confirmed (their list order is the
    /// commit order only if a key's values commit in proposal order).
    retry_forever: bool,
}

impl Shared {
    fn new(seed: u64, retry_forever: bool) -> Self {
        Shared {
            rec: Mutex::new(Recorder::new(seed)),
            next_value: Mutex::new(0),
            lists: Mutex::new(BTreeMap::new()),
            written: Mutex::new(BTreeSet::new()),
            done: Mutex::new(0),
            retry_forever,
        }
    }
    fn fresh_value(&self) -> u64 {
        let mut v = self.next_value.lock().unwrap();
        *v += 1;
        *v
    }
    fn history(&self) -> History {
        self.rec.lock().unwrap().history().clone()
    }
    fn done(&self) -> usize {
        *self.done.lock().unwrap()
    }
    fn reset_done(&self) {
        *self.done.lock().unwrap() = 0;
    }
}

#[allow(clippy::too_many_arguments)]
async fn client_loop(
    env: SimEnv,
    nodes: Nodes,
    shared: Arc<Shared>,
    c: usize,
    phase: u64,
    rounds: u64,
) {
    let proc: Process = phase * 100 + c as u64;
    let owned: Vec<Key> = (0..KEYSPACE)
        .filter(|&k| k % CLIENTS as u64 == c as u64)
        .collect();
    for _ in 0..rounds {
        if env.gen_below(100) < READ_PCT {
            let keys: Vec<Key> = shared.written.lock().unwrap().iter().copied().collect();
            if !keys.is_empty() {
                let key = keys[env.gen_below(keys.len() as u64) as usize];
                run_read(&env, &nodes, &shared, proc, key).await;
            }
        } else if !owned.is_empty() {
            let key = owned[env.gen_below(owned.len() as u64) as usize];
            run_write(&env, &nodes, &shared, proc, key).await;
        }
        env.sleep(POLL).await;
    }
    *shared.done.lock().unwrap() += 1;
}

async fn run_write(env: &SimEnv, nodes: &Nodes, shared: &Arc<Shared>, proc: Process, key: Key) {
    let value = shared.fresh_value();
    let list = {
        let mut lists = shared.lists.lock().unwrap();
        let l = lists.entry(key).or_default();
        l.push(value);
        l.clone()
    };
    let mops = vec![Mop::Append { key, value }];
    shared
        .rec
        .lock()
        .unwrap()
        .invoke(proc, env.now().0, mops.clone());

    let deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
    let mut proposed_on: Option<usize> = None;
    let mut committed = false;
    let mut iter = 0u64;
    while shared.retry_forever || env.now().0 < deadline {
        if let Some((li, node)) = leader_slot(nodes)
            && (proposed_on != Some(li) || iter.is_multiple_of(20))
            && node.propose_append(key, value, &list)
        {
            proposed_on = Some(li);
        }
        iter += 1;
        env.sleep(POLL).await;
        if let Some((_, node)) = leader_slot(nodes)
            && let Some(got) = node.read(key).await
            && got.contains(&value)
        {
            committed = true;
            break;
        }
    }
    let mut rec = shared.rec.lock().unwrap();
    if committed {
        rec.ok(proc, env.now().0, mops);
        shared.written.lock().unwrap().insert(key);
    } else {
        rec.info(proc, env.now().0, mops);
    }
}

async fn run_read(env: &SimEnv, nodes: &Nodes, shared: &Arc<Shared>, proc: Process, key: Key) {
    let invoke = vec![Mop::Read {
        key,
        observed: None,
    }];
    shared.rec.lock().unwrap().invoke(proc, env.now().0, invoke);
    let deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
    let mut observed = None;
    while env.now().0 < deadline {
        if let Some((_, node)) = leader_slot(nodes)
            && let Some(list) = node.read(key).await
        {
            observed = Some(list);
            break;
        }
        env.sleep(POLL).await;
    }
    let mops = vec![Mop::Read { key, observed }];
    let mut rec = shared.rec.lock().unwrap();
    if mops[0]
        == (Mop::Read {
            key,
            observed: None,
        })
    {
        rec.info(proc, env.now().0, mops);
    } else {
        rec.ok(proc, env.now().0, mops);
    }
}

fn spawn_clients(sim: &Simulator, nodes: &Nodes, shared: &Arc<Shared>, phase: u64, rounds: u64) {
    shared.reset_done();
    for (c, &client_id) in CLIENT_IDS.iter().enumerate() {
        let env = sim.env(nid(client_id));
        let (nodes, shared) = (Arc::clone(nodes), Arc::clone(shared));
        env.clone().spawn_task(async move {
            client_loop(env, nodes, shared, c, phase, rounds).await;
        });
    }
}

/// Step the simulator until all clients finished their rounds or the budget is
/// spent; returns whether they finished.
fn run_until_clients_done(sim: &mut Simulator, shared: &Shared) -> bool {
    let deadline = sim.now().0 + WORKLOAD_BUDGET.as_nanos() as u64;
    while shared.done() < CLIENTS && sim.now().0 < deadline {
        sim.run_for(STEP);
    }
    shared.done() >= CLIENTS
}

/// The ADR 0038 apply frontier of every control node, `(commit_index,
/// engine_applied_index)`. Core `last_applied` only means "handed to the apply
/// task"; the driver-applied frontier is `engine_applied_index`, and it is what
/// `metadata()` is gated on, so it is what a restart-liveness check must read
/// (docs/lessons/testing: core-last-applied-is-not-the-driver-applied-apply-frontier).
fn ctl_frontiers(nodes: &Nodes) -> Vec<(u64, u64)> {
    let snap: Vec<AnyNode> = nodes.lock().unwrap().clone();
    snap.iter()
        .filter_map(|n| match n {
            AnyNode::Ctl(c) => Some((c.commit_index(), c.engine_applied_index())),
            AnyNode::Data(_) => None,
        })
        .collect()
}

/// Converged-or-timeout: every control node's apply frontier has caught up to
/// its commit index (and is non-zero: the apply task really ran). Empty for a
/// `Data` group. Returns the violations at the end of the budget.
fn frontier_violations(sim: &mut Simulator, nodes: &Nodes) -> Vec<String> {
    let deadline = sim.now().0 + CONVERGENCE_BUDGET.as_nanos() as u64;
    loop {
        let f = ctl_frontiers(nodes);
        let caught_up = f
            .iter()
            .all(|&(commit, applied)| applied >= commit && applied > 0);
        if caught_up {
            return Vec::new();
        }
        if sim.now().0 >= deadline {
            return vec![format!(
                "apply frontier never caught up to commit: (commit, engine_applied) per node = {f:?}"
            )];
        }
        sim.run_for(CONVERGENCE_POLL_STEP);
    }
}

fn snapshot_states(nodes: &Nodes) -> Vec<State> {
    let snap: Vec<AnyNode> = nodes.lock().unwrap().clone();
    snap.iter().map(AnyNode::state).collect()
}

fn run_group_cell(cell: &Cell, tc: Transcoder) -> CellVerdict {
    let seed = cell.seed;
    let kind = cell.kind;
    let mut violations: Vec<String> = Vec::new();
    let mut sim = Simulator::new(seed);
    let shared = Arc::new(Shared::new(seed, kind == Kind::Control));
    let nodes: Nodes = Arc::new(Mutex::new(
        GROUP_IDS
            .iter()
            .map(|&id| start_node(kind, &sim, id))
            .collect(),
    ));
    sim.run_for(SETTLE);

    // Crash stops: a slow sync on every disk so an un-synced tail exists at the
    // crash instant (set only after the engines are open: a `block_on` open
    // under a sync delay would wait on a timer the simulator never fires).
    if cell.stop != Stop::Clean {
        let mut cfg = DiskConfig::default();
        cfg.set_sync_delay(SYNC_DELAY);
        if cell.stop == Stop::TornTail {
            cfg.torn_tail_on_crash = true;
            cfg.corrupt_on_crash = true;
        }
        for id in GROUP_IDS {
            sim.set_disk_config_for(nid(id), cfg.clone());
        }
    }

    // ---- phase 1 ----
    let rounds = if cell.stop == Stop::Clean {
        ROUNDS_CLEAN
    } else {
        ROUNDS_CRASH
    };
    spawn_clients(&sim, &nodes, &shared, 0, rounds);
    if cell.stop == Stop::Clean {
        if !run_until_clients_done(&mut sim, &shared) {
            violations.push("phase-1 workload did not finish within its budget".into());
        }
        sim.run_for(Duration::from_millis(1500));
    } else {
        sim.run_for(Duration::from_millis(600 + mix(seed, "crash_delay") % 1400));
        let deadline = sim.now().0 + Duration::from_secs(60).as_nanos() as u64;
        while ok_appends(&shared.history()) < 2 && sim.now().0 < deadline {
            sim.run_for(POLL);
        }
        sim.run_for(Duration::from_millis(mix(seed, "crash_jitter") % 500));
    }

    // ---- the restart: atomic in virtual time ----
    for &client_id in &CLIENT_IDS {
        sim.stop(nid(client_id));
    }
    let acks_before = ok_appends(&shared.history());
    let leader = leader_slot(&nodes).map(|(i, _)| i);
    let victims: Vec<usize> = match cell.scope {
        Scope::Leader => vec![leader.unwrap_or(0)],
        Scope::Follower => vec![
            (0..GROUP_IDS.len())
                .find(|&i| Some(i) != leader)
                .unwrap_or(1),
        ],
        Scope::WholeGroup => (0..GROUP_IDS.len()).collect(),
    };
    for &i in &victims {
        let node = nid(GROUP_IDS[i]);
        if cell.stop != Stop::Clean {
            sim.crash(node.clone());
        }
        sim.stop(node.clone());
        if cell.stop != Stop::Clean {
            sim.restart(node.clone()); // clears the crashed flag
        }
        // The recovery path runs on a healthy disk (see the sync-delay note).
        sim.set_disk_config_for(node, DiskConfig::default());
    }
    for &i in &victims {
        let id = GROUP_IDS[i];
        let env = sim.env(nid(id));
        let report = tc(&env, cell.back, &transcode_opts(seed, id)).unwrap_or_else(|e| {
            panic!("transcode of node {id} failed: {e}");
        });
        eprintln!(
            "  node {id}: transcoded={} kept={} unrecognised={} not_reached={}",
            report.transcoded.len(),
            report.kept.len(),
            report.unrecognised.len(),
            report.not_reached.len()
        );
    }
    for &i in &victims {
        let fresh = start_node(kind, &sim, GROUP_IDS[i]);
        nodes.lock().unwrap()[i] = fresh;
    }

    // ---- catch-up under a seeded partition of the restarted node from a peer ----
    let r = victims[0];
    let others: Vec<usize> = (0..GROUP_IDS.len()).filter(|&j| j != r).collect();
    let peer = others[(mix(seed, "partition_peer") % others.len() as u64) as usize];
    sim.partition_pair(nid(GROUP_IDS[r]), nid(GROUP_IDS[peer]));
    sim.run_for(Duration::from_millis(
        300 + mix(seed, "partition_window") % 2200,
    ));
    sim.heal(nid(GROUP_IDS[r]), nid(GROUP_IDS[peer]));

    // ---- probe: every phase-1 ack survived, before any phase-2 write can
    // rewrite a lost list ----
    let probe_history = shared.history();
    let (d, c) = converge_or_timeout(
        &mut sim,
        seed,
        || probe_history.clone(),
        || snapshot_states(&nodes),
    );
    violations.extend(
        d.violations
            .into_iter()
            .map(|v| format!("post-restart probe: {v}")),
    );
    violations.extend(
        c.violations
            .into_iter()
            .map(|v| format!("post-restart probe: {v}")),
    );

    // ---- phase 2 ----
    spawn_clients(&sim, &nodes, &shared, 1, ROUNDS_PHASE2);
    if !run_until_clients_done(&mut sim, &shared) {
        violations.push("phase-2 workload did not finish within its budget".into());
    }
    let (d, c) = converge_or_timeout(
        &mut sim,
        seed,
        || shared.history(),
        || snapshot_states(&nodes),
    );
    let history = shared.history();
    let cycles = check_cycles(&history);
    violations.extend(
        cycles
            .violations
            .into_iter()
            .map(|v| format!("cycles: {v}")),
    );
    violations.extend(d.violations.into_iter().map(|v| format!("durability: {v}")));
    violations.extend(
        c.violations
            .into_iter()
            .map(|v| format!("convergence: {v}")),
    );
    if kind == Kind::Control {
        violations.extend(
            frontier_violations(&mut sim, &nodes)
                .into_iter()
                .map(|v| format!("control: {v}")),
        );
    }

    let acks_total = ok_appends(&history);
    let acks_after = acks_total - acks_before;
    if acks_before == 0 {
        violations.push("vacuous: no acknowledged write before the stop".into());
    }
    if acks_after == 0 {
        violations.push("vacuous: no acknowledged write after the restart".into());
    }
    finish(cell, violations, acks_before, acks_after, &history)
}

fn finish(
    cell: &Cell,
    violations: Vec<String>,
    acks_before: usize,
    acks_after: usize,
    history: &History,
) -> CellVerdict {
    CellVerdict {
        cell: cell.name.clone(),
        seed: cell.seed,
        ok: violations.is_empty(),
        violations,
        panic: None,
        acks_before,
        acks_after,
        history: serde_json::to_string(history).expect("history serialises"),
    }
}

// ---------------------------------------------------------------------------
// SharedWal cell: one node, several tablets on one SWL1 file
// ---------------------------------------------------------------------------

type KvNode = RaftKvNode<SimEnv, LsmEngine<SimEnv>>;
type Wal = SharedWalCoordinator<KvCommand, KvState>;

const SW_NODE: u64 = 0;

fn sw_open(sim: &Simulator) -> Arc<Wal> {
    block_on(Wal::open(&sim.env(nid(SW_NODE)), SHARED_WAL))
        .expect("strict open of the shared WAL after restart")
}

fn sw_hosts(sim: &Simulator, shared: &Arc<Wal>) -> Vec<KvNode> {
    (1..=SHARED_TABLETS)
        .map(|stream| {
            RaftKvNode::start_hosted_campaigning_with_batcher_and_shared_wal(
                sim.env(nid(SW_NODE)),
                vec![nid(SW_NODE)],
                block_on(LsmEngine::open_with(
                    sim.env(nid(SW_NODE)),
                    &format!("t{stream}/"),
                    lsm_opts(),
                ))
                .expect("strict open of the tablet's LSM engine after restart"),
                StorageScope::whole(),
                stream,
                None,
                Some(Arc::clone(shared)),
            )
        })
        .collect()
}

fn sw_state(hosts: &[KvNode]) -> State {
    hosts
        .iter()
        .enumerate()
        .map(|(t, n)| {
            let list = block_on(n.local_get(&key_bytes(t as u64)))
                .map(|b| decode_list(&b))
                .unwrap_or_default();
            (t as u64, list)
        })
        .collect()
}

/// One sequential round: append a fresh value to every tablet's list, waiting
/// for each to become visible (applied after the commit's fsync) before
/// recording it `ok`, then read it back.
fn sw_round(
    sim: &mut Simulator,
    hosts: &[KvNode],
    rec: &mut Recorder,
    lists: &mut BTreeMap<Key, Vec<u64>>,
    next: &mut u64,
) {
    for (t, node) in hosts.iter().enumerate() {
        let key = t as u64;
        *next += 1;
        let value = *next;
        let list = lists.entry(key).or_default();
        list.push(value);
        let mops = vec![Mop::Append { key, value }];
        rec.invoke(0, sim.now().0, mops.clone());
        let _ = node.put(key_bytes(key), encode_list(list));
        let mut seen = None;
        for _ in 0..100 {
            sim.run_for(Duration::from_millis(50));
            if let Some(b) = block_on(node.local_get(&key_bytes(key)))
                && decode_list(&b).contains(&value)
            {
                seen = Some(decode_list(&b));
                break;
            }
        }
        match seen {
            Some(observed) => {
                rec.ok(0, sim.now().0, mops);
                let read = vec![Mop::Read {
                    key,
                    observed: Some(observed),
                }];
                rec.invoke(1, sim.now().0, read.clone());
                rec.ok(1, sim.now().0, read);
            }
            None => rec.info(0, sim.now().0, mops),
        }
    }
}

fn run_sharedwal_cell(cell: &Cell, tc: Transcoder) -> CellVerdict {
    let seed = cell.seed;
    let mut violations: Vec<String> = Vec::new();
    let mut sim = Simulator::new(seed);
    let node = nid(SW_NODE);
    let mut rec = Recorder::new(seed);
    let mut lists: BTreeMap<Key, Vec<u64>> = BTreeMap::new();
    let mut next = 0u64;

    let shared = sw_open(&sim);
    let hosts = sw_hosts(&sim, &shared);
    sim.run_for(Duration::from_secs(2));

    if cell.stop != Stop::Clean {
        let mut cfg = DiskConfig::default();
        cfg.set_sync_delay(SYNC_DELAY);
        if cell.stop == Stop::TornTail {
            cfg.torn_tail_on_crash = true;
            cfg.corrupt_on_crash = true;
        }
        sim.set_disk_config_for(node.clone(), cfg);
    }

    // ---- phase 1 ----
    let rounds = if cell.stop == Stop::Clean {
        SHARED_ROUNDS_CLEAN
    } else {
        8 + mix(seed, "crash_round") % 56
    };
    for _ in 0..rounds {
        sw_round(&mut sim, &hosts, &mut rec, &mut lists, &mut next);
    }
    if cell.stop == Stop::Clean {
        sim.run_for(Duration::from_secs(1));
    } else {
        // A round in flight at the crash: issued, never waited for.
        for (t, n) in hosts.iter().enumerate() {
            let key = t as u64;
            next += 1;
            let list = lists.entry(key).or_default();
            list.push(next);
            rec.invoke(0, sim.now().0, vec![Mop::Append { key, value: next }]);
            let _ = n.put(key_bytes(key), encode_list(list));
        }
        sim.run_for(Duration::from_millis(mix(seed, "crash_jitter") % 60));
    }

    // ---- the restart ----
    let acks_before = ok_appends(rec.history());
    if cell.stop != Stop::Clean {
        sim.crash(node.clone());
    }
    sim.stop(node.clone());
    if cell.stop != Stop::Clean {
        sim.restart(node.clone());
    }
    sim.set_disk_config_for(node.clone(), DiskConfig::default());
    drop(hosts);
    drop(shared);
    let report = tc(
        &sim.env(node.clone()),
        cell.back,
        &transcode_opts(seed, SW_NODE),
    )
    .unwrap_or_else(|e| panic!("transcode failed: {e}"));
    eprintln!(
        "  node {SW_NODE}: transcoded={} kept={} unrecognised={} not_reached={}",
        report.transcoded.len(),
        report.kept.len(),
        report.unrecognised.len(),
        report.not_reached.len()
    );
    let shared = sw_open(&sim);
    let hosts = sw_hosts(&sim, &shared);
    sim.run_for(Duration::from_secs(2));

    // ---- probe, then phase 2 ----
    let probe_history = rec.history().clone();
    let (d, c) = converge_or_timeout(
        &mut sim,
        seed,
        || probe_history.clone(),
        || vec![sw_state(&hosts)],
    );
    violations.extend(
        d.violations
            .into_iter()
            .map(|v| format!("post-restart probe: {v}")),
    );
    violations.extend(
        c.violations
            .into_iter()
            .map(|v| format!("post-restart probe: {v}")),
    );
    for _ in 0..SHARED_ROUNDS_PHASE2 {
        sw_round(&mut sim, &hosts, &mut rec, &mut lists, &mut next);
    }

    let history = rec.history().clone();
    let (d, c) = converge_or_timeout(
        &mut sim,
        seed,
        || history.clone(),
        || vec![sw_state(&hosts)],
    );
    let cycles = check_cycles(&history);
    violations.extend(
        cycles
            .violations
            .into_iter()
            .map(|v| format!("cycles: {v}")),
    );
    violations.extend(d.violations.into_iter().map(|v| format!("durability: {v}")));
    violations.extend(
        c.violations
            .into_iter()
            .map(|v| format!("convergence: {v}")),
    );
    let acks_after = ok_appends(&history) - acks_before;
    if acks_before == 0 {
        violations.push("vacuous: no acknowledged write before the stop".into());
    }
    if acks_after == 0 {
        violations.push("vacuous: no acknowledged write after the restart".into());
    }
    finish(cell, violations, acks_before, acks_after, &history)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn run_corpus(kind: Kind) {
    let mut failures = Vec::new();
    for c in corpus_of(kind) {
        let v = run_cell(&c, transcode::transcode_disk);
        eprintln!(
            "  -> ok={} acks_before={} acks_after={}",
            v.ok, v.acks_before, v.acks_after
        );
        if !v.ok {
            failures.push(format!(
                "cell={} seed={}: {:?}",
                v.cell, v.seed, v.violations
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "upgrade_restart {kind:?} corpus: {} cell(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn upgrade_restart_data_corpus() {
    run_corpus(Kind::Data);
}

#[test]
fn upgrade_restart_control_corpus() {
    run_corpus(Kind::Control);
}

#[test]
fn upgrade_restart_sharedwal_corpus() {
    run_corpus(Kind::SharedWal);
}

#[test]
fn upgrade_restart_cell_names_and_seeds_are_unique() {
    let all: Vec<Cell> = [Kind::Data, Kind::Control, Kind::SharedWal]
        .into_iter()
        .flat_map(cells_of)
        .collect();
    let names: BTreeSet<&str> = all.iter().map(|c| c.name.as_str()).collect();
    let seeds: BTreeSet<u64> = all.iter().map(|c| c.seed).collect();
    assert_eq!(names.len(), all.len(), "duplicate cell name");
    assert_eq!(seeds.len(), all.len(), "duplicate cell seed");
    // Every kind covers every stop class at every supported back.
    for kind in [Kind::Data, Kind::Control] {
        assert_eq!(
            cells_of(kind).len(),
            transcode::supported_back().len() * 9,
            "{kind:?}"
        );
    }
    assert_eq!(
        cells_of(Kind::SharedWal).len(),
        transcode::supported_back().len() * 3
    );
}

#[test]
fn upgrade_restart_run_is_deterministic() {
    for kind in [Kind::Data, Kind::Control, Kind::SharedWal] {
        let c = cell(kind, Scope::WholeGroup, Stop::Crash, 0);
        let a = run_cell(&c, transcode::transcode_disk);
        let b = run_cell(&c, transcode::transcode_disk);
        assert_cell_ok(&a);
        assert_eq!(
            (a.acks_before, a.acks_after, &a.history),
            (b.acks_before, b.acks_after, &b.history),
            "same cell, same seed must replay identically (cell={} seed={})",
            c.name,
            c.seed
        );
    }
}

// ---- negative controls: the harness must have teeth ----

fn control_cell(kind: Kind, tag: &str) -> Cell {
    let mut c = cell(kind, Scope::WholeGroup, Stop::Clean, 0);
    c.seed = name_seed(&format!("upgrade_restart/negative_control/{tag}"));
    c
}

/// Positive control: the identity transcode passes the very cells the negative
/// controls break.
#[test]
fn upgrade_restart_identity_control_passes() {
    for kind in [Kind::Data, Kind::Control, Kind::SharedWal] {
        let v = run_cell(&control_cell(kind, "identity"), transcode::transcode_disk);
        assert_cell_ok(&v);
    }
}

fn has_violation(v: &CellVerdict, needle: &str) -> bool {
    v.violations.iter().any(|m| m.contains(needle))
}

/// (a) A `Data` replica group that loses the final record of every WAL loses an
/// acknowledged write, and the oracle says so (the post-restart probe, before any
/// phase-2 write could rewrite the list).
///
/// `Control` and `SharedWal` are asserted **benign** for this same corruption,
/// and that is by design, not a blind spot: neither keeps its last write only in
/// the WAL tail. The control plane mirrors every applied entry into its LSM
/// engine (ADR 0038) *and* keeps the whole uncompacted `raft.wal` (two
/// independent copies), and `SharedWal`'s per-tablet engines hold everything the
/// SWL1 tail lost once flushed. Their teeth are controls (b)-(d).
#[test]
fn upgrade_restart_negative_control_dropped_wal_record() {
    let c = control_cell(Kind::Data, "drop_wal");
    let v = run_cell(&c, drop_final_wal_record);
    assert!(
        v.panic.is_none(),
        "a dropped final record is a tolerated tail and must open cleanly (cell={} seed={}): {:?}",
        c.name,
        c.seed,
        v.panic
    );
    assert!(
        has_violation(&v, "post-restart probe: lost acknowledged append"),
        "expected a probe durability violation (cell={} seed={}), got ok={} {:?}",
        c.name,
        c.seed,
        v.ok,
        v.violations
    );
    for kind in [Kind::Control, Kind::SharedWal] {
        let c = control_cell(kind, "drop_wal");
        let v = run_cell(&c, drop_final_wal_record);
        assert!(
            v.ok,
            "{kind:?}: a lost log tail is covered by the engine/log redundancy (cell={} seed={}): {:?}",
            c.name, c.seed, v.violations
        );
    }
}

/// (b) Half the consensus log gone *and* the engine gone: the second half of the
/// history exists nowhere, for every kind. Must surface as a lost acknowledged
/// append (this is the corruption `Control` must catch, since either copy alone
/// masks the loss).
#[test]
fn upgrade_restart_negative_control_half_the_history_lost() {
    for kind in [Kind::Data, Kind::Control, Kind::SharedWal] {
        let c = control_cell(kind, "halve");
        let v = run_cell(&c, halve_log_and_wipe_engine);
        assert!(
            v.panic.is_none(),
            "{kind:?}: a shortened log must open cleanly (cell={} seed={}): {:?}",
            c.name,
            c.seed,
            v.panic
        );
        assert!(
            has_violation(&v, "lost acknowledged append"),
            "{kind:?}: expected a lost-acknowledged-append violation (cell={} seed={}), got ok={} {:?}",
            c.name,
            c.seed,
            v.ok,
            v.violations
        );
    }
}

/// (c) `SharedWal`: the SWL1 tail is GC'd once every tablet's engine has flushed
/// past it, so the per-tablet engines are the only holder of older history;
/// removing them loses acknowledged writes the intact SWL1 file cannot restore.
#[test]
fn upgrade_restart_negative_control_sharedwal_engines_wiped() {
    let c = control_cell(Kind::SharedWal, "wipe_engine");
    let v = run_cell(&c, wipe_engine_files);
    assert!(
        v.panic.is_none(),
        "cell={} seed={}: {:?}",
        c.name,
        c.seed,
        v.panic
    );
    assert!(
        has_violation(&v, "lost acknowledged append"),
        "expected a lost-acknowledged-append violation (cell={} seed={}), got ok={} {:?}",
        c.name,
        c.seed,
        v.ok,
        v.violations
    );
}

/// (d) Everything gone on every node: total loss, for every kind.
#[test]
fn upgrade_restart_negative_control_total_loss() {
    for kind in [Kind::Data, Kind::Control, Kind::SharedWal] {
        let c = control_cell(kind, "wipe_all");
        let v = run_cell(&c, wipe_everything);
        assert!(
            v.panic.is_none(),
            "{kind:?} cell={} seed={}: {:?}",
            c.name,
            c.seed,
            v.panic
        );
        assert!(
            has_violation(&v, "lost acknowledged append"),
            "{kind:?}: expected a lost-acknowledged-append violation (cell={} seed={}), got ok={} {:?}",
            c.name,
            c.seed,
            v.ok,
            v.violations
        );
    }
}

/// (e) A truncated SSTable must fail loudly at the strict engine open, never
/// silently serve short data, for every kind.
#[test]
fn upgrade_restart_negative_control_truncated_sstable_fails_open() {
    for kind in [Kind::Data, Kind::Control, Kind::SharedWal] {
        let c = control_cell(kind, "truncate_sst");
        let v = run_cell(&c, truncate_sstable);
        assert!(
            !v.ok,
            "{kind:?}: truncated sstable went unnoticed (cell={} seed={})",
            c.name, c.seed
        );
        let msg = v.panic.clone().unwrap_or_default();
        assert!(
            msg.contains("strict open of the"),
            "{kind:?}: expected the strict engine open to fail (cell={} seed={}), got {:?}",
            c.name,
            c.seed,
            v
        );
    }
}
