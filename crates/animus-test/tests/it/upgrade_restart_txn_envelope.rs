//! Upgrade-restart harness, **tier 1b: an intent unresolved across the
//! upgrade** (ADR 0073 Phase 1, `txn-envelope` v2; ADR 0018 §2's 2026-10-04
//! amendment).
//!
//! The generic tier-1 corpus (`upgrade_restart_corpus`) writes no transaction,
//! so it never has an intent to carry across a restart. This one does. A
//! 3-replica `RaftKvNode` group over `LsmEngine<SimEnv>` commits values, stages
//! a transaction and **leaves it unresolved** (so the v2 intents, each carrying
//! the committed value it shadows, sit in the engine's SSTable *and* WAL), then
//! stops the node(s), runs each stopped disk through the harness's transcode
//! with `TranscodeOpts::row_back` (`transcode::ROW_TABLE`'s `txn-envelope`
//! v2 -> v1: every stored intent loses its `prior`), restarts on current code
//! over strict engine opens, and only then resolves the transaction.
//!
//! What that proves, per cell:
//!
//! * the transcode **really rewrote** the stored envelopes (the rewritten-value
//!   count is asserted non-zero, and the raw row is read back and its tag
//!   byte checked: `2` before, `1` after) -- not an identity;
//! * the restarted group decodes the v1 intents through
//!   `txn::legacy::v1` (`IntentPrior::Unknown`) and serves the **old lookback**
//!   for them: an eventually-consistent read of each shadowed key, taken
//!   *before* the transaction is resolved, still returns the last committed
//!   value on every replica;
//! * commit and abort resolve correctly over v1 intents (a commit installs the
//!   staged value or delete; an abort restores the committed value, or absence
//!   for a key that had none), on every replica, converged-or-timeout;
//! * every write acknowledged before the stop survives, and new transactions
//!   staged after the restart (v2 intents beside the resolved v1 history)
//!   commit normally.
//!
//! The cells cross {commit, abort} x {leader only, one follower, whole group}
//! x {clean stop, crash with an un-synced tail, torn/corrupt tail} x
//! `row_back` {0: the identity control, 1: every file rewritten, 1 with a
//! seeded fraction of files left at v2: one engine holding both versions}.
//!
//! **The documented residual gap** (ADR 0018's amendment, "what this does not
//! fix"): a v1 intent that is still unresolved across the upgrade has no carried
//! prior, so its abort still depends on the engine retaining the history one
//! MVCC version below it. These cells therefore keep the engine from
//! compacting between the stage and the resolution (default LSM thresholds,
//! one explicit flush), and
//! [`v1_intent_under_compaction_is_the_documented_residual_gap`] pins the gap
//! itself as a control: the same abort under a compaction burst keeps the value
//! at `row_back = 0` and loses it at `row_back = 1`. If that control ever
//! stops failing the way it asserts, the gap was closed and the ADR, the crate
//! guides and this control need updating together.
//!
//! Seed-reproducible: `ANIMUS_SEED=<seed> ANIMUS_UPGRADE_RESTART_CELL=<cell
//! name substring> cargo test -p animus-test --test it
//! upgrade_restart_txn_envelope:: -- --nocapture`. Depth:
//! `ANIMUS_UPGRADE_RESTART_SEEDS=K` (the knob the other upgrade-restart tiers
//! share). Fault timing is drawn from `splitmix64(cell seed, tag)`, never the
//! simulator RNG.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{KIND_BASE, RaftKvNode, TxnId};
use animus_env::{EnvExt, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use animus_tablet::{escape, partition_token};
use animus_test::corpus::{self, SeedVariant, name_seed};
use animus_test::upgrade::transcode::{self, TranscodeOpts};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
const ELECT: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_secs(2);
const POLL: Duration = Duration::from_millis(250);
const CONVERGE_BUDGET: Duration = Duration::from_secs(60);
/// Latency armed on the to-be-crashed disks so an un-synced tail exists at the
/// crash instant.
const SYNC_DELAY: Duration = Duration::from_millis(15);
const PREFIX: &str = "tablet/";
const CELL_WATCHDOG: Duration = Duration::from_secs(120);

type Node = RaftKvNode<SimEnv, LsmEngine<SimEnv>>;

// ---------------------------------------------------------------------------
// Cells
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Resolve {
    Commit,
    Abort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scope {
    Leader,
    Follower,
    WholeGroup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    Clean,
    Crash,
    TornTail,
}

/// How the row transcode treats the stopped disks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rows {
    /// `row_back = 0`: the identity control (intents stay v2).
    Current,
    /// `row_back = 1`, every file: intents become v1.
    OneBack,
    /// `row_back = 1`, a seeded fraction of files left at v2.
    OneBackMixed,
}

#[derive(Clone, Debug)]
struct Cell {
    name: String,
    seed: u64,
    resolve: Resolve,
    scope: Scope,
    stop: Stop,
    rows: Rows,
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

fn cells() -> Vec<Cell> {
    let mut out = Vec::new();
    for resolve in [Resolve::Commit, Resolve::Abort] {
        for scope in [Scope::Leader, Scope::Follower, Scope::WholeGroup] {
            for stop in [Stop::Clean, Stop::Crash, Stop::TornTail] {
                for rows in [Rows::Current, Rows::OneBack, Rows::OneBackMixed] {
                    let name = format!(
                        "{}_{}_{}_{}",
                        format!("{resolve:?}").to_lowercase(),
                        format!("{scope:?}").to_lowercase(),
                        format!("{stop:?}").to_lowercase(),
                        match rows {
                            Rows::Current => "rb0",
                            Rows::OneBack => "rb1",
                            Rows::OneBackMixed => "rb1mixed",
                        },
                    );
                    out.push(Cell {
                        seed: name_seed(&format!("upgrade_restart_txn/{name}")),
                        name,
                        resolve,
                        scope,
                        stop,
                        rows,
                    });
                }
            }
        }
    }
    out
}

fn corpus_cells() -> Vec<Cell> {
    let filter = std::env::var("ANIMUS_UPGRADE_RESTART_CELL").ok();
    let expanded = if let Some(seed) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        cells()
            .into_iter()
            .map(|c| {
                let name = c.name.clone();
                c.reseeded(name, seed)
            })
            .collect()
    } else {
        corpus::seed_expand(
            cells(),
            corpus::seeds_from_env("ANIMUS_UPGRADE_RESTART_SEEDS"),
        )
    };
    expanded
        .into_iter()
        .filter(|c: &Cell| filter.as_ref().is_none_or(|f| c.name.contains(f.as_str())))
        .collect()
}

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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A real ADR 0022-shaped data-plane key.
fn key(pk: &[u8], rk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out.extend_from_slice(rk);
    out
}

/// Spawn `fut` on `env` and drive `sim` for `budget`.
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

/// No compaction between the stage and the resolution (see the module doc's
/// residual gap): default thresholds, no background maintenance.
fn quiet_opts() -> LsmOptions {
    LsmOptions {
        background_maintenance: false,
        ..LsmOptions::default()
    }
}

/// Tiny thresholds + the production-default GC grace: the compaction pressure
/// of `animus-cp-data`'s `txn_abort_restore_history` and the txn corpus'
/// `lsm_compaction_*` cells.
fn compacting_opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 256,
        compaction_trigger: 2,
        target_table_bytes: 1024,
        level_fanout: 2,
        wal_segment_bytes: 512,
        background_maintenance: false,
        ..LsmOptions::default()
    }
}

/// Open the node's engine **strictly**: no destroy-and-reopen fallback, which
/// would hide a bad transcode as a clean wipe.
fn open_engine(sim: &Simulator, id: u64, opts: LsmOptions) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(nid(id)), PREFIX, opts))
        .expect("strict open of the LSM engine (no destroy-and-reopen)")
}

fn start(sim: &Simulator, id: u64, engine: LsmEngine<SimEnv>) -> Node {
    RaftKvNode::start(
        sim.env(nid(id)),
        NODES.iter().copied().map(nid).collect(),
        engine,
    )
}

fn live_leader(nodes: &[Option<Node>]) -> Option<usize> {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(_, n)| n.as_ref().is_some_and(Node::is_leader))
        .map(|(i, _)| i)
        .collect();
    (ls.len() == 1).then(|| ls[0])
}

fn wait_leader(sim: &mut Simulator, nodes: &[Option<Node>]) -> Option<usize> {
    let deadline = sim.now().0 + Duration::from_secs(30).as_nanos() as u64;
    while sim.now().0 < deadline {
        if let Some(l) = live_leader(nodes) {
            return Some(l);
        }
        sim.run_for(POLL);
    }
    None
}

/// The raw envelope tag byte of `k`'s latest row on a **stopped** node's disk
/// (`0` committed, `1` v1 intent, `2` v2 intent), read through a throwaway
/// engine open.
fn raw_tag(sim: &Simulator, id: u64, k: &[u8]) -> Option<u8> {
    let engine = block_on(LsmEngine::open_with(sim.env(nid(id)), PREFIX, quiet_opts()))
        .expect("raw peek open");
    let mut physical = vec![KIND_BASE];
    physical.extend_from_slice(k);
    block_on(engine.get(&physical))
        .expect("raw peek get")
        .map(|vv| vv.value[0])
}

#[derive(Debug, Default)]
struct Verdict {
    violations: Vec<String>,
    /// Envelope values the row transcode rewrote, summed over victims.
    rewritten: usize,
}

struct Keys {
    /// Committed value, intent puts a new one.
    put: Vec<u8>,
    /// Committed value, intent deletes it.
    del: Vec<u8>,
    /// No committed value, intent puts one.
    fresh: Vec<u8>,
    /// Written (and acked) after the stage, WAL-only.
    filler: Vec<u8>,
}

fn keys() -> Keys {
    Keys {
        put: key(b"list-put", b"v"),
        del: key(b"list-del", b"v"),
        fresh: key(b"list-fresh", b"v"),
        filler: key(b"list-filler", b"v"),
    }
}

/// Stage `writes` on `leader` and return the transaction's handle.
fn stage(
    sim: &mut Simulator,
    leader: &Node,
    writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
) -> Option<(TxnId, Vec<u8>)> {
    let n = leader.clone();
    drive(sim, leader.env(), SETTLE, async move {
        n.txn_stage("t", writes).await
    })
    .flatten()
    .map(|(id, record_key, _)| (id, record_key))
}

fn decide(
    sim: &mut Simulator,
    leader: &Node,
    txn: (TxnId, Vec<u8>),
    keys: Vec<Vec<u8>>,
    commit: bool,
) -> bool {
    let n = leader.clone();
    drive(sim, leader.env(), SETTLE, async move {
        n.txn_decide(txn.0, txn.1, keys, commit).await
    })
    .flatten()
    .is_some()
}

/// Converged-or-timeout: every live replica's resolved read of each
/// `(key, expected)` agrees.
fn converge(
    sim: &mut Simulator,
    nodes: &[Option<Node>],
    expect: &[(Vec<u8>, Option<Vec<u8>>)],
) -> Vec<String> {
    let deadline = sim.now().0 + CONVERGE_BUDGET.as_nanos() as u64;
    loop {
        let mut wrong = Vec::new();
        for (i, n) in nodes.iter().enumerate() {
            let Some(n) = n else { continue };
            for (k, want) in expect {
                let got = block_on(n.local_get(k));
                if &got != want {
                    wrong.push(format!(
                        "node {i}: key {:02x?}: got {:?}, want {:?}",
                        &k[k.len().saturating_sub(4)..],
                        got.as_deref().map(String::from_utf8_lossy),
                        want.as_deref().map(String::from_utf8_lossy),
                    ));
                }
            }
        }
        if wrong.is_empty() || sim.now().0 >= deadline {
            return wrong;
        }
        sim.run_for(POLL);
    }
}

// ---------------------------------------------------------------------------
// One cell
// ---------------------------------------------------------------------------

fn run_cell_inner(cell: &Cell) -> Verdict {
    let seed = cell.seed;
    eprintln!("upgrade_restart_txn cell={} seed={seed}", cell.name);
    let mut v = Verdict::default();
    let mut sim = Simulator::new(seed);
    let k = keys();

    let engines: Vec<LsmEngine<SimEnv>> = NODES
        .iter()
        .map(|&id| open_engine(&sim, id, quiet_opts()))
        .collect();
    let mut nodes: Vec<Option<Node>> = NODES
        .iter()
        .zip(&engines)
        .map(|(&id, e)| Some(start(&sim, id, e.clone())))
        .collect();
    sim.run_for(ELECT);
    let Some(l) = wait_leader(&mut sim, &nodes) else {
        v.violations.push("no leader before the workload".into());
        return v;
    };
    let leader = nodes[l].clone().unwrap();

    // ---- phase 1: committed values, then a staged-and-unresolved txn ----
    for (kk, val) in [(&k.put, "committed-put"), (&k.del, "committed-del")] {
        if !matches!(
            leader.put(kk.clone(), val.as_bytes().to_vec()),
            ProposeResult::Accepted { .. }
        ) {
            v.violations.push("leader rejected a seed write".into());
            return v;
        }
    }
    sim.run_for(SETTLE);
    let Some(txn) = stage(
        &mut sim,
        &leader,
        vec![
            (k.put.clone(), Some(b"staged-put".to_vec())),
            (k.del.clone(), None),
            (k.fresh.clone(), Some(b"staged-fresh".to_vec())),
        ],
    ) else {
        v.violations.push("txn_stage did not complete".into());
        return v;
    };
    sim.run_for(SETTLE);

    // The intents are in every replica's engine. Flush them into an SSTable
    // (the WAL's active segment still holds them too: both carriers), while
    // the disks are still healthy (a `block_on` flush under a sync delay would
    // wait on a timer the simulator never fires).
    for e in &engines {
        block_on(e.flush_now()).expect("flush");
    }

    // Crash stops: a slow sync so an un-synced tail exists at the crash.
    if cell.stop != Stop::Clean {
        let mut cfg = DiskConfig::default();
        cfg.set_sync_delay(SYNC_DELAY);
        if cell.stop == Stop::TornTail {
            cfg.torn_tail_on_crash = true;
            cfg.corrupt_on_crash = true;
        }
        for id in NODES {
            sim.set_disk_config_for(nid(id), cfg.clone());
        }
    }
    // An acknowledged write after the flush: WAL-only, and must survive.
    if !matches!(
        leader.put(k.filler.clone(), b"filler-acked".to_vec()),
        ProposeResult::Accepted { .. }
    ) {
        v.violations.push("leader rejected the filler write".into());
        return v;
    }
    sim.run_for(SETTLE);
    // ...and one that is NOT acknowledged before the crash (never asserted).
    let _ = leader.put(key(b"list-unacked", b"v"), b"unacked".to_vec());

    // ---- the stop ----
    let victims: Vec<usize> = match cell.scope {
        Scope::Leader => vec![l],
        Scope::Follower => vec![(0..NODES.len()).find(|&i| i != l).unwrap()],
        Scope::WholeGroup => (0..NODES.len()).collect(),
    };
    for &i in &victims {
        let node = nid(NODES[i]);
        if cell.stop != Stop::Clean {
            sim.crash(node.clone());
        }
        sim.stop(node.clone());
        if cell.stop != Stop::Clean {
            sim.restart(node.clone());
        }
        sim.set_disk_config_for(node, DiskConfig::default());
        nodes[i] = None;
    }
    drop(leader);

    // ---- the transcode ----
    let row_back = u32::from(cell.rows != Rows::Current);
    for &i in &victims {
        let id = NODES[i];
        let keep = match cell.rows {
            Rows::OneBackMixed => [250u32, 600][(mix(seed, &format!("keep/{id}")) % 2) as usize],
            _ => 0,
        };
        let opts = TranscodeOpts {
            keep_current_fraction_permille: keep,
            seed: mix(seed, &format!("files/{id}")),
            row_back,
            ..TranscodeOpts::default()
        };
        let report = transcode::transcode_disk(&sim.env(nid(id)), 0, &opts)
            .unwrap_or_else(|e| panic!("transcode of node {id} failed: {e}"));
        v.rewritten += report.rows.values_rewritten;
        eprintln!(
            "  node {id}: row transcode rewrote {} of {} values over {:?} (skipped {:?})",
            report.rows.values_rewritten,
            report.rows.values_seen,
            report.rows.files_rewritten,
            report.rows.files_skipped
        );
        // The raw rows prove what the pass did.
        for kk in [&k.put, &k.del, &k.fresh] {
            let tag = raw_tag(&sim, id, kk);
            let ok = matches!(
                (cell.rows, tag),
                (Rows::Current, Some(2))
                    | (Rows::OneBack, Some(1))
                    | (Rows::OneBackMixed, Some(1 | 2))
            );
            if !ok {
                v.violations.push(format!(
                    "node {id}: a stored intent has envelope tag {tag:?} after the row \
                     transcode ({:?})",
                    cell.rows
                ));
            }
        }
        if cell.rows == Rows::OneBack && report.rows.values_rewritten == 0 {
            v.violations
                .push(format!("node {id}: the row transcode rewrote nothing"));
        }
    }

    // ---- the restart: strict opens, current code ----
    for &i in &victims {
        let id = NODES[i];
        let engine = open_engine(&sim, id, quiet_opts());
        nodes[i] = Some(start(&sim, id, engine));
    }
    sim.run_for(ELECT);
    let Some(l) = wait_leader(&mut sim, &nodes) else {
        v.violations.push("no leader after the restart".into());
        return v;
    };
    let leader = nodes[l].clone().unwrap();

    // ---- before resolving: the shadowed values are still served (the v1
    // path's lookback; the v2 path's carried prior in the control cells) ----
    let pending_expect = [
        (k.put.clone(), Some(b"committed-put".to_vec())),
        (k.del.clone(), Some(b"committed-del".to_vec())),
        (k.fresh.clone(), None),
    ];
    for (i, n) in nodes.iter().enumerate() {
        let n = n.as_ref().unwrap();
        for (kk, want) in &pending_expect {
            let got = block_on(n.stale_get_served(kk));
            if got != Some(want.clone()) {
                v.violations.push(format!(
                    "node {i}: an eventual read under the pending intent must serve the last \
                     committed value ({want:?}), got {got:?}"
                ));
            }
        }
    }

    // ---- resolve over whatever shape the intents are in ----
    let write_keys = vec![k.put.clone(), k.del.clone(), k.fresh.clone()];
    let commit = cell.resolve == Resolve::Commit;
    if !decide(&mut sim, &leader, txn, write_keys, commit) {
        v.violations.push("txn_decide did not complete".into());
        return v;
    }
    sim.run_for(SETTLE);

    let mut expect: Vec<(Vec<u8>, Option<Vec<u8>>)> = if commit {
        vec![
            (k.put.clone(), Some(b"staged-put".to_vec())),
            (k.del.clone(), None),
            (k.fresh.clone(), Some(b"staged-fresh".to_vec())),
        ]
    } else {
        vec![
            (k.put.clone(), Some(b"committed-put".to_vec())),
            (k.del.clone(), Some(b"committed-del".to_vec())),
            (k.fresh.clone(), None),
        ]
    };
    expect.push((k.filler.clone(), Some(b"filler-acked".to_vec())));

    // ---- a new transaction after the restart: v2 intents beside the history ----
    let fresh2 = key(b"list-after", b"v");
    let Some(txn2) = stage(
        &mut sim,
        &leader,
        vec![
            (k.put.clone(), Some(b"after-restart".to_vec())),
            (fresh2.clone(), Some(b"after-fresh".to_vec())),
        ],
    ) else {
        v.violations
            .push("the post-restart txn_stage did not complete".into());
        return v;
    };
    if !decide(
        &mut sim,
        &leader,
        txn2,
        vec![k.put.clone(), fresh2.clone()],
        true,
    ) {
        v.violations
            .push("the post-restart commit did not complete".into());
        return v;
    }
    sim.run_for(SETTLE);
    expect[0] = (k.put.clone(), Some(b"after-restart".to_vec()));
    expect.push((fresh2, Some(b"after-fresh".to_vec())));

    v.violations.extend(converge(&mut sim, &nodes, &expect));
    v
}

/// Run one cell on its own thread under [`CELL_WATCHDOG`] (a livelock with no
/// `.await` yield never advances virtual time; only an OS bound names it).
fn run_cell(cell: &Cell) -> Verdict {
    let (tx, rx) = std::sync::mpsc::channel();
    let c = cell.clone();
    std::thread::Builder::new()
        .name(format!("upgrade_restart_txn/{}", cell.name))
        .spawn(move || {
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_cell_inner(&c)));
            let _ = tx.send(r.map_err(|p| {
                p.downcast_ref::<&str>()
                    .map(|m| (*m).to_string())
                    .or_else(|| p.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "<non-string panic>".into())
            }));
        })
        .expect("spawn the cell thread");
    match rx.recv_timeout(CELL_WATCHDOG) {
        Ok(Ok(v)) => v,
        Ok(Err(msg)) => Verdict {
            violations: vec![format!("panic: {msg}")],
            rewritten: 0,
        },
        Err(e) => panic!(
            "upgrade_restart_txn cell={} HUNG (seed={}): {e} -- replay with ANIMUS_SEED={} \
             ANIMUS_UPGRADE_RESTART_CELL={}",
            cell.name, cell.seed, cell.seed, cell.name
        ),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn upgrade_restart_txn_envelope_corpus() {
    let mut failures = Vec::new();
    let (mut cells_run, mut rewritten) = (0usize, 0usize);
    for c in corpus_cells() {
        let v = run_cell(&c);
        cells_run += 1;
        rewritten += v.rewritten;
        if !v.violations.is_empty() {
            failures.push(format!(
                "cell={} seed={}: {:?}",
                c.name, c.seed, v.violations
            ));
        }
    }
    eprintln!("upgrade_restart_txn: {cells_run} cells, {rewritten} envelopes rewritten");
    assert!(
        failures.is_empty(),
        "upgrade_restart_txn corpus: {} cell(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn upgrade_restart_txn_cell_names_and_seeds_are_unique() {
    let all = corpus::seed_expand(cells(), 3);
    let mut names: Vec<_> = all.iter().map(|c| c.name.clone()).collect();
    names.sort();
    let n = names.len();
    names.dedup();
    assert_eq!(n, names.len(), "duplicate cell name");
    assert_eq!(cells().len(), 2 * 3 * 3 * 3);
}

/// The residual gap, pinned as a control (module doc): the same abort under a
/// compaction burst between the stage and the resolution keeps the committed
/// value when the intent is v2 (it carries its prior) and loses it when the
/// intent was down-converted to v1 (the old one-MVCC-version lookback finds
/// nothing once compaction GC'd the history under the intent).
#[test]
fn v1_intent_under_compaction_is_the_documented_residual_gap() {
    let mut outcome = [None, None];
    for (slot, row_back) in [(0usize, 0u32), (1, 1)] {
        let seed = 0x1205_0001;
        let mut sim = Simulator::new(seed);
        let k = keys();
        // Compacting engines from the start, so the burst below really GCs.
        let engines: Vec<_> = NODES
            .iter()
            .map(|&id| open_engine(&sim, id, compacting_opts()))
            .collect();
        let mut nodes: Vec<Option<Node>> = NODES
            .iter()
            .zip(&engines)
            .map(|(&id, e)| Some(start(&sim, id, e.clone())))
            .collect();
        sim.run_for(ELECT);
        let l = wait_leader(&mut sim, &nodes).expect("leader");
        let leader = nodes[l].clone().unwrap();
        assert!(matches!(
            leader.put(k.put.clone(), b"committed-put".to_vec()),
            ProposeResult::Accepted { .. }
        ));
        sim.run_for(SETTLE);
        let txn = stage(
            &mut sim,
            &leader,
            vec![(k.put.clone(), Some(b"staged-put".to_vec()))],
        )
        .expect("stage");
        sim.run_for(SETTLE);

        // Stop the whole group, down-convert, restart (compacting engines).
        for i in 0..NODES.len() {
            sim.stop(nid(NODES[i]));
            nodes[i] = None;
        }
        drop(leader);
        drop(engines);
        let mut rewritten = 0;
        for &id in &NODES {
            let opts = TranscodeOpts {
                row_back,
                ..TranscodeOpts::default()
            };
            rewritten += transcode::transcode_disk(&sim.env(nid(id)), 0, &opts)
                .unwrap()
                .rows
                .values_rewritten;
        }
        assert_eq!(rewritten > 0, row_back == 1, "row_back={row_back}");
        for (i, &id) in NODES.iter().enumerate() {
            nodes[i] = Some(start(&sim, id, open_engine(&sim, id, compacting_opts())));
        }
        sim.run_for(ELECT);
        let l = wait_leader(&mut sim, &nodes).expect("leader after restart");
        let leader = nodes[l].clone().unwrap();

        // The compaction burst: unrelated writes spread over many ms of HLC.
        for i in 0..80u64 {
            let fk = key(format!("filler-{i:04}").as_bytes(), b"r");
            assert!(matches!(
                leader.put(fk, vec![b'x'; 48]),
                ProposeResult::Accepted { .. }
            ));
            sim.run_for(Duration::from_millis(5));
        }
        sim.run_for(SETTLE);

        assert!(decide(&mut sim, &leader, txn, vec![k.put.clone()], false));
        sim.run_for(SETTLE);
        outcome[slot] = Some(block_on(nodes[l].as_ref().unwrap().local_get(&k.put)));
    }
    assert_eq!(
        outcome[0],
        Some(Some(b"committed-put".to_vec())),
        "a v2 intent restores its carried prior even after compaction GC"
    );
    assert_eq!(
        outcome[1],
        Some(None),
        "documented residual gap (ADR 0018 2026-10-04 amendment): a v1 intent unresolved \
         across the upgrade still depends on MVCC history, which compaction GC removed. \
         If this now restores the value the gap is closed: update the ADR, the crate \
         guides and this control together."
    );
}
