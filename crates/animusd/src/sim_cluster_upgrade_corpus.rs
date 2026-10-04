//! `sim_cluster_upgrade_corpus` — ADR 0073 Phase 1, workstream P1-D, **tier 2**:
//! a whole-cluster upgrade restart over `SimCluster` with real
//! `LsmEngine<SimEnv>` engines and the DynamoDB wire on top.
//!
//! Tier 1 (`animus-test/tests/upgrade_restart_corpus.rs`) restarts one Raft
//! group at a time over `LsmEngine`. This tier restarts a **whole cluster**:
//! the control `Metadata` (control WAL `CWL1` + the system-keyspace LSM mirror,
//! ADR 0038), the data-only node's `Metadata` mirror, every per-tablet
//! `LsmEngine` and the tablet-host reconciler, and the DynamoDB wire clients.
//!
//! # One scenario
//!
//! ```text
//! SimCluster::new_with_lsm_engines  (roles [Both, Both, Both, Data], RF 3)
//!   -> wire CreateTable tbl0 (stream enabled) and tbl1, a metadata-only
//!      backup catalog row, three aux items on tbl1
//!   -> phase 1: three wire clients list_append / ConsistentRead tbl0
//!   -> (clean: drain; crash/torn tail: stop mid-workload)
//!   -> optional stream seal, clean-only pre-stop snapshot of every key
//!   -> whole-cluster stop (Simulator::stop on every node, after a
//!      Simulator::crash on the crash cells), transcode_disk on EVERY node's
//!      disk (identity today; seeded mid-transcode stop and mixed-version
//!      fraction), SimCluster::restart on every node (strict LSM opens)
//!   -> optional seeded partition of one restarted node from one peer
//!   -> oracle probes, phase 2 clients, final oracle
//! ```
//!
//! # Oracle
//!
//! `check_cycles` over the combined history (phase 1, post-restart probe
//! reads, phase 2); a post-restart wire probe that every phase-1 acknowledged
//! append is still readable, in order, *before* any phase-2 write can mask a
//! loss (clean cells: exact equality with the pre-stop snapshot); the control
//! apply frontier (`engine_applied_index` reaching `commit_index`, ADR 0038);
//! every node's `Metadata` (mirror included) agreeing on the tables and the
//! backup catalog row; every tablet re-hosted on its full replica set; the
//! stream still serving every acknowledged append through
//! `DescribeStream`/`GetShardIterator`/`GetRecords`; tbl1's aux items;
//! `check_durability` and `check_convergence` per live replica after a
//! converged-or-timeout poll; non-vacuity (acks in both phases).
//!
//! # Strict opens
//!
//! `SimEngineBackend::Lsm` opens every engine strictly (panic, never the
//! reconciler's destroy-and-reopen fallback, issue #554), so a bad transcode
//! or a recovery bug surfaces as `strict open of ...` rather than a silent
//! clean wipe that Raft repopulates.
//!
//! # What is in the loop and what is not (precisely)
//!
//! In: control `Metadata`, the mirror (`NodeRole::Data` node 3), tablet
//! hosting + rebalancing, streams (enable, writes, optional seal into the
//! shared `SimSegmentStore`, `GetRecords` over sealed and open shards after the
//! restart), the backup *catalog* (`BeginBackup`/`RecordBackupTabletComplete`/
//! `CompleteBackup` proposed on the control leader, read back after the
//! restart).
//!
//! Not in, and not faked: a backup whose data objects are captured before the
//! upgrade and **restored** after it. `animusd::backup_capture::
//! backup_capture_loop` and `backup_restore::backup_restore_loop` take the
//! concrete `ClientCtx<ProdEnv, AnimusdRelayClient>`, `SimCluster` never spawns
//! either (it spawns only the backup *janitor*), and the restore path also
//! needs the `BackupStoreHandle`'s real chunk objects. The sealed stream
//! segments and the backup store live in `SimSegmentStore`s that are shared
//! across the cluster and are not part of any node's disk, so they are never
//! transcoded; stream *cursors* (the per-tablet `KIND_CURSOR` rows) live in
//! the tablet engines and are transcoded with them, but no cursor consumer
//! loop (`change_consumer_loop`) runs under `SimCluster`, so nothing advances
//! one here.
//!
//! # Determinism, replay, knobs
//!
//! Fault timing is drawn from `splitmix64(cell seed, tag)`, never the
//! simulator RNG. Depth: `ANIMUS_UPGRADE_RESTART_SEEDS=K` (shared with tier 1,
//! ADR 0073; `ANIMUS_UPGRADE_SEEDS` is the separate Phase 2 mixed-version knob, see
//! `sim_cluster_mixed_version_corpus`). Replay:
//! `ANIMUS_SEED=<seed> ANIMUS_UPGRADE_RESTART_CELL=<cell substring> cargo test
//! -p animusd --lib sim_cluster_upgrade_corpus -- --nocapture`. Each cell runs
//! on its own OS thread under a wall-clock watchdog ([`CELL_WATCHDOG`]).

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::BackupStatus;
use animus_env::{Clock, Env, EnvExt, Rng, nid};
use animus_sim::{DiskConfig, SimEnv};
use animus_test::corpus::{self, SeedVariant};
use animus_test::history::{History, Key, Mop, Process};
use animus_test::upgrade::transcode::{self, TranscodeOpts, TranscodeReport};
use animus_test::{CheckReport, Recorder, check_convergence, check_cycles, check_durability};
use futures::executor::block_on;
use serde_json::{Value, json};

use super::sim_cluster::{SimCluster, SimClusterHandle};
use super::*;
use crate::config::NodeRole;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Control-prefixed role mix: nodes 0..3 are control voters + data, node 3 is
/// a data-only node reaching the control plane through its mirror.
const ROLES: [NodeRole; 4] = [
    NodeRole::Both,
    NodeRole::Both,
    NodeRole::Both,
    NodeRole::Data,
];
pub(super) const NODES: u64 = 4;
const REPLICATION: usize = 3;
pub(super) const CLIENTS: u64 = 3;
pub(super) const KEYSPACE: u64 = 6;
pub(super) const READ_PCT: u64 = 30;
pub(super) const PARTITIONS: u64 = 2;
const ROUNDS_CLEAN: u64 = 10;
const ROUNDS_CRASH: u64 = 40;
const ROUNDS_PHASE2: u64 = 6;
pub(super) const POLL: Duration = Duration::from_millis(80);
pub(super) const SETTLE: Duration = Duration::from_millis(300);
pub(super) const DRAIN: Duration = Duration::from_secs(3);
/// A slow sync on every disk of a crash cell, so an un-synced tail exists at
/// the crash instant (set only after bring-up: a `block_on` open under a sync
/// delay would wait on a timer the simulator never fires).
const SYNC_DELAY: Duration = Duration::from_millis(15);
pub(super) const WORKLOAD_BUDGET: Duration = Duration::from_secs(180);
pub(super) const CONVERGENCE_STEP: Duration = Duration::from_secs(1);
pub(super) const CONVERGENCE_BUDGET: Duration = Duration::from_secs(90);

/// Real wall-clock bound on one cell: a diagnostic bound on a hang (a livelock
/// that never advances virtual time can never hit an in-sim budget), never a
/// timeout that decides a verdict. See `animus-test/CLAUDE.md`.
pub(super) const CELL_WATCHDOG: Duration = Duration::from_secs(300);

pub(super) const TBL: &str = "tbl0";
pub(super) const AUX: &str = "tbl1";
const BACKUP_ID: &str = "upgrade-backup-0";

// ---------------------------------------------------------------------------
// Cells
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    /// Workload drained and synced, then `Simulator::stop` on every node.
    Clean,
    /// Mid-workload `Simulator::crash` (un-synced tail dropped), then stop.
    Crash,
    /// As `Crash`, with `torn_tail_on_crash` + `corrupt_on_crash` armed
    /// together (issue #495's combination; do not narrow).
    TornTail,
}

#[derive(Clone, Debug)]
struct Cell {
    name: String,
    seed: u64,
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

fn cell(stop: Stop, back: u32) -> Cell {
    let name = format!("cluster_{}_b{back}", format!("{stop:?}").to_lowercase());
    Cell {
        seed: corpus::name_seed(&format!("sim_cluster_upgrade/{name}")),
        name,
        stop,
        back,
    }
}

fn cells() -> Vec<Cell> {
    let mut out = Vec::new();
    for &back in transcode::supported_back() {
        for stop in [Stop::Clean, Stop::Crash, Stop::TornTail] {
            out.push(cell(stop, back));
        }
    }
    out
}

fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_UPGRADE_RESTART_SEEDS")
}

/// The cells after the depth knob, `ANIMUS_SEED` replay and the optional
/// `ANIMUS_UPGRADE_RESTART_CELL` name filter.
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
        corpus::seed_expand(cells(), seeds_per_cell())
    };
    expanded
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
    splitmix64(seed ^ corpus::name_seed(tag))
}

/// Per-node transcode options: a seeded mix of "transcode everything",
/// "leave some files at the current version" (mixed-version files) and "stop
/// after N files" (a crash inside the transcode window).
fn transcode_opts(seed: u64, node: u64) -> TranscodeOpts {
    let tag = format!("transcode/{node}");
    let r = mix(seed, &tag);
    let keep = [0u32, 0, 250, 600, 1000][(r % 5) as usize];
    let stop_after = if (r >> 8).is_multiple_of(4) {
        Some(((r >> 16) % 12) as usize)
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

/// Control (b): every consensus log (control and per-tablet `CWL1`) keeps only
/// its first half and every LSM engine is wiped, on every node: whatever sits
/// in the second half of the history exists nowhere.
fn halve_logs_and_wipe_engines(
    env: &SimEnv,
    _back: u32,
    _opts: &TranscodeOpts,
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
            let keep = 1 + lines.len().saturating_sub(1) / 2;
            let kept: Vec<u8> = lines[..keep.min(lines.len())].concat();
            block_on(env.replace(f, &kept))?;
        }
    }
    remove_where(env, |name| name.starts_with("lsm-"))
}

/// Control (d): removes every durable file on every node.
fn wipe_everything(env: &SimEnv, _back: u32, _opts: &TranscodeOpts) -> io::Result<TranscodeReport> {
    remove_where(env, |_| true)
}

/// Control (e): truncates the largest SSTable, else the manifest, else the
/// newest LSM WAL segment's header, of every node to half.
fn truncate_an_lsm_file(
    env: &SimEnv,
    _back: u32,
    _opts: &TranscodeOpts,
) -> io::Result<TranscodeReport> {
    let mut files = block_on(env.list())?;
    files.sort();
    let mut sst: Option<(usize, String)> = None;
    let mut manifest: Option<String> = None;
    for f in &files {
        let bytes = block_on(env.read(f))?;
        match transcode::classify(f, &bytes).map(|e| e.name) {
            Some("lsm-sstable") if sst.as_ref().is_none_or(|(n, _)| bytes.len() > *n) => {
                sst = Some((bytes.len(), f.clone()));
            }
            Some("lsm-manifest") => manifest = Some(f.clone()),
            _ => {}
        }
    }
    if let Some(f) = sst.map(|(_, f)| f).or(manifest) {
        let bytes = block_on(env.read(&f))?;
        block_on(env.replace(&f, &bytes[..bytes.len() / 2]))?;
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
    /// `transcoded/kept/unrecognised/not_reached` file counts, summed.
    transcode_summary: String,
    /// Virtual seconds the whole cell advanced (for the runtime table).
    history: String,
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|m| (*m).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string())
}

/// Run one cell on its own thread under [`CELL_WATCHDOG`]. A panic out of the
/// run itself becomes a verdict with `panic: Some(..)`; a hang **panics here**
/// naming the cell and seed.
fn run_cell(cell: &Cell, tc: Transcoder) -> CellVerdict {
    let (tx, rx) = std::sync::mpsc::channel();
    let c = cell.clone();
    std::thread::Builder::new()
        .name(format!("sim_cluster_upgrade/{}", cell.name))
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            let _ = tx.send(run_cell_inner(&c, tc));
        })
        .expect("spawn the cell thread");
    match rx.recv_timeout(CELL_WATCHDOG) {
        Ok(v) => v,
        Err(e) => panic!(
            "sim_cluster_upgrade cell={} HUNG or died (seed={}): no verdict within {:?} ({e}) \
             -- replay with ANIMUS_SEED={} ANIMUS_UPGRADE_RESTART_CELL={}",
            cell.name, cell.seed, CELL_WATCHDOG, cell.seed, cell.name
        ),
    }
}

fn run_cell_inner(cell: &Cell, tc: Transcoder) -> CellVerdict {
    eprintln!("sim_cluster_upgrade cell={} seed={}", cell.name, cell.seed);
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_scenario(cell, tc))) {
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
                transcode_summary: String::new(),
                history: String::new(),
            }
        }
    }
}

fn assert_cell_ok(v: &CellVerdict) {
    assert!(
        v.ok,
        "sim_cluster_upgrade cell={} FAILED (seed={}): {:?}",
        v.cell, v.seed, v.violations
    );
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

pub(super) fn pk_sk(key: Key) -> (String, String) {
    (format!("part-{}", key % PARTITIONS), format!("item-{key}"))
}

pub(super) fn decode_items_attr(item: &Value) -> Vec<u64> {
    item.get("items")
        .and_then(|v| v.get("L"))
        .and_then(Value::as_array)
        .map(|elems| {
            elems
                .iter()
                .filter_map(|e| e.get("N")?.as_str()?.parse::<u64>().ok())
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn decode_engine_items(bytes: &[u8]) -> Vec<u64> {
    let Ok(Some(item)) = animus_item::decode_stored_item(bytes) else {
        return Vec::new();
    };
    match item.get("items") {
        Some(animus_item::AttributeValue::L(list)) => list
            .iter()
            .filter_map(|v| match v {
                animus_item::AttributeValue::N(s) => s.parse::<u64>().ok(),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

pub(super) fn create_table_body(table: &str, stream: bool) -> String {
    let mut body = json!({
        "TableName": table,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
    });
    if stream {
        body["StreamSpecification"] =
            json!({"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"});
    }
    body.to_string()
}

pub(super) fn get_body(key: Key) -> String {
    let (pk, sk) = pk_sk(key);
    json!({
        "ConsistentRead": true,
        "TableName": TBL,
        "Key": {"pk": {"S": pk}, "sk": {"S": sk}},
    })
    .to_string()
}

/// A consistent wire read of `key` from `node`: `Some(list)` on a 200.
pub(super) fn wire_read(cluster: &mut SimCluster, node: u64, key: Key) -> Option<Vec<u64>> {
    let (status, body) =
        cluster.dynamo_fast(node, "DynamoDB_20120810.GetItem", get_body(key).as_bytes());
    if status != 200 {
        return None;
    }
    let v: Value = serde_json::from_str(&body).ok()?;
    Some(v.get("Item").map(decode_items_attr).unwrap_or_default())
}

// ---------------------------------------------------------------------------
// The workload
// ---------------------------------------------------------------------------

pub(super) struct Shared {
    pub(super) rec: Mutex<Recorder>,
    pub(super) next_value: Mutex<u64>,
    pub(super) done: Mutex<usize>,
}

impl Shared {
    pub(super) fn fresh_value(&self) -> u64 {
        let mut v = self.next_value.lock().expect("next_value poisoned");
        *v += 1;
        *v
    }
    pub(super) fn history(&self) -> History {
        self.rec
            .lock()
            .expect("recorder poisoned")
            .history()
            .clone()
    }
}

pub(super) async fn run_write(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    key: Key,
    node: u64,
) {
    let value = shared.fresh_value();
    let (pk, sk) = pk_sk(key);
    let mops = vec![Mop::Append { key, value }];
    shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .invoke(proc, env.now().0, mops.clone());
    let body = json!({
        "TableName": TBL,
        "Key": {"pk": {"S": pk}, "sk": {"S": sk}},
        "UpdateExpression": "SET items = list_append(if_not_exists(items, :empty), :v)",
        "ExpressionAttributeValues": {
            ":empty": {"L": []},
            ":v": {"L": [{"N": value.to_string()}]},
        },
    })
    .to_string();
    let (status, _) = handle
        .dynamo(node, "DynamoDB_20120810.UpdateItem", body.as_bytes())
        .await;
    let mut rec = shared.rec.lock().expect("recorder poisoned");
    if status == 200 {
        rec.ok(proc, env.now().0, mops);
    } else {
        // Indeterminate: the write may have applied and only the reply lost.
        rec.info(proc, env.now().0, mops);
    }
}

pub(super) async fn run_read(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    key: Key,
    node: u64,
) {
    let read = |observed| vec![Mop::Read { key, observed }];
    shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .invoke(proc, env.now().0, read(None));
    let (status, body) = handle
        .dynamo(node, "DynamoDB_20120810.GetItem", get_body(key).as_bytes())
        .await;
    let mut rec = shared.rec.lock().expect("recorder poisoned");
    if status != 200 {
        rec.info(proc, env.now().0, read(None));
        return;
    }
    let list = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|v| v.get("Item").map(decode_items_attr))
        .unwrap_or_default();
    rec.ok(proc, env.now().0, read(Some(list)));
}

pub(super) async fn client_loop(
    env: SimEnv,
    handle: SimClusterHandle,
    shared: Arc<Shared>,
    proc: Process,
    client: u64,
    rounds: u64,
) {
    let owned: Vec<Key> = (0..KEYSPACE).filter(|k| k % CLIENTS == client).collect();
    for _ in 0..rounds {
        let node = env.gen_below(NODES);
        if env.gen_below(100) < READ_PCT {
            let key = env.gen_below(KEYSPACE);
            run_read(&env, &handle, &shared, proc, key, node).await;
        } else if !owned.is_empty() {
            let key = owned[env.gen_below(owned.len() as u64) as usize];
            run_write(&env, &handle, &shared, proc, key, node).await;
        }
        env.sleep(POLL).await;
    }
    *shared.done.lock().expect("done poisoned") += 1;
}

/// Spawn the clients of `phase`; returns the node ids of their envs so the
/// caller can stop them. Phase `p` uses client env indices `p*10 + c`.
pub(super) fn spawn_clients(
    cluster: &SimCluster,
    shared: &Arc<Shared>,
    phase: u64,
    rounds: u64,
) -> Vec<animus_env::NodeId> {
    *shared.done.lock().expect("done poisoned") = 0;
    let handle = cluster.handle();
    let mut ids = Vec::new();
    for c in 0..CLIENTS {
        let env = cluster.client_env(phase * 10 + c);
        ids.push(env.node_id());
        let (handle, shared) = (handle.clone(), Arc::clone(shared));
        let proc: Process = phase * 100 + c;
        env.clone().spawn_task(async move {
            client_loop(env, handle, shared, proc, c, rounds).await;
        });
    }
    ids
}

pub(super) fn run_until_clients_done(cluster: &mut SimCluster, shared: &Shared) -> bool {
    let mut waited = Duration::ZERO;
    while *shared.done.lock().expect("done poisoned") < CLIENTS as usize {
        if waited >= WORKLOAD_BUDGET {
            return false;
        }
        cluster.run_for(POLL);
        waited += POLL;
    }
    true
}

pub(super) fn ok_appends(h: &History) -> usize {
    h.ok_entries()
        .flat_map(|e| &e.mops)
        .filter(|m| matches!(m, Mop::Append { .. }))
        .count()
}

/// Every acknowledged append, per key, in acknowledgement order. Under the
/// single-writer-per-key discipline and a sequential per-client loop, ack
/// order is commit order.
pub(super) fn acked_by_key(h: &History) -> BTreeMap<Key, Vec<u64>> {
    let mut out: BTreeMap<Key, Vec<u64>> = BTreeMap::new();
    for e in h.ok_entries() {
        for m in &e.mops {
            if let Mop::Append { key, value } = m {
                out.entry(*key).or_default().push(*value);
            }
        }
    }
    out
}

pub(super) fn is_subsequence(needle: &[u64], hay: &[u64]) -> bool {
    let mut it = hay.iter();
    needle.iter().all(|n| it.any(|h| h == n))
}

pub(super) fn combine(seed: u64, reports: impl Iterator<Item = CheckReport>) -> CheckReport {
    let mut violations = Vec::new();
    for r in reports {
        violations.extend(r.violations);
    }
    CheckReport {
        ok: violations.is_empty(),
        violations,
        seed,
    }
}

// ---------------------------------------------------------------------------
// Cluster inspection
// ---------------------------------------------------------------------------

pub(super) fn tablet_of(cluster: &SimCluster, node: u64, table: &str) -> Option<TabletId> {
    cluster
        .metadata(node)
        .tablets_for_table(table)
        .next()
        .map(|(id, _)| *id)
}

pub(super) fn live_replicas(cluster: &SimCluster, tablet: TabletId) -> Vec<u64> {
    (0..NODES)
        .filter(|&n| cluster.hosted_tablets(n).contains(&tablet))
        .collect()
}

pub(super) fn final_state(
    handle: &SimClusterHandle,
    tablet: TabletId,
    node: u64,
) -> BTreeMap<Key, Vec<u64>> {
    (0..KEYSPACE)
        .map(|key| {
            let (pk, sk) = pk_sk(key);
            let list = block_on(handle.local_value(node, tablet, &pk, &sk))
                .map(|b| decode_engine_items(&b))
                .unwrap_or_default();
            (key, list)
        })
        .collect()
}

/// Poll `cond` (advancing virtual time) until it holds or `CONVERGENCE_BUDGET`
/// elapses. Returns whether it held. Eventual properties are never a one-shot
/// fixed-deadline assert (root `CLAUDE.md`).
pub(super) fn converge(
    cluster: &mut SimCluster,
    mut cond: impl FnMut(&mut SimCluster) -> bool,
) -> bool {
    let mut waited = Duration::ZERO;
    loop {
        if cond(cluster) {
            return true;
        }
        if waited >= CONVERGENCE_BUDGET {
            return false;
        }
        cluster.run_for(CONVERGENCE_STEP);
        waited += CONVERGENCE_STEP;
    }
}

fn accepted(r: ProposeResult) -> bool {
    matches!(r, ProposeResult::Accepted { .. })
}

/// A metadata-only backup catalog row (the capture/restore drivers are not
/// drivable under `SimCluster`, see the module doc) — the exact shape of
/// `sim_cluster_backup_janitor.rs::complete_a_backup`.
fn complete_a_backup(cluster: &mut SimCluster, table: &str, tablet: TabletId) {
    assert!(
        accepted(cluster.propose_meta(MetaCommand::BeginBackup {
            backup_id: BACKUP_ID.to_owned(),
            table: table.to_owned(),
            created_wall_ms: 1_000,
            backup_name: "upgrade".to_owned(),
            pitr_base: false,
        })),
        "BeginBackup rejected"
    );
    assert!(
        accepted(
            cluster.propose_meta(MetaCommand::RecordBackupTabletComplete {
                backup_id: BACKUP_ID.to_owned(),
                tablet,
                cut_version: 10,
                bytes: 100,
                chunk_count: 1,
            })
        ),
        "RecordBackupTabletComplete rejected"
    );
    assert!(
        accepted(cluster.propose_meta(MetaCommand::CompleteBackup {
            backup_id: BACKUP_ID.to_owned(),
        })),
        "CompleteBackup rejected"
    );
    cluster.run_for(Duration::from_millis(300));
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

fn stream_json(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or(Value::Null)
}

/// Every `(key, value)` pair the stream's `NewImage`s carry, over every shard
/// (sealed and open) from `TRIM_HORIZON`. `None` if the stream API never
/// answered.
fn stream_pairs(cluster: &mut SimCluster, node: u64) -> Option<BTreeSet<(Key, u64)>> {
    let (status, body) = cluster.dynamo_streams(
        node,
        "DynamoDBStreams_20120810.ListStreams",
        json!({"TableName": TBL}).to_string().as_bytes(),
    );
    if status != 200 {
        return None;
    }
    let arn = stream_json(&body)["Streams"]
        .as_array()?
        .iter()
        .find_map(|s| s["StreamArn"].as_str().map(str::to_owned))?;
    let (status, body) = cluster.dynamo_streams(
        node,
        "DynamoDBStreams_20120810.DescribeStream",
        json!({"StreamArn": arn}).to_string().as_bytes(),
    );
    if status != 200 {
        return None;
    }
    let shard_ids: Vec<String> = stream_json(&body)["StreamDescription"]["Shards"]
        .as_array()?
        .iter()
        .filter_map(|s| s["ShardId"].as_str().map(str::to_owned))
        .collect();
    let mut pairs = BTreeSet::new();
    for shard in shard_ids {
        let (status, body) = cluster.dynamo_streams(
            node,
            "DynamoDBStreams_20120810.GetShardIterator",
            json!({"StreamArn": arn, "ShardId": shard, "ShardIteratorType": "TRIM_HORIZON"})
                .to_string()
                .as_bytes(),
        );
        if status != 200 {
            return None;
        }
        let mut iterator = stream_json(&body)["ShardIterator"].as_str()?.to_owned();
        for _ in 0..200 {
            let (status, body) = cluster.dynamo_streams(
                node,
                "DynamoDBStreams_20120810.GetRecords",
                json!({"ShardIterator": iterator, "Limit": 1000})
                    .to_string()
                    .as_bytes(),
            );
            if status != 200 {
                return None;
            }
            let v = stream_json(&body);
            let records = v["Records"].as_array().cloned().unwrap_or_default();
            for r in &records {
                let d = &r["dynamodb"];
                let Some(sk) = d["Keys"]["sk"]["S"].as_str() else {
                    continue;
                };
                let Some(key) = sk.strip_prefix("item-").and_then(|k| k.parse::<Key>().ok()) else {
                    continue;
                };
                for value in decode_items_attr(&d["NewImage"]) {
                    pairs.insert((key, value));
                }
            }
            match v["NextShardIterator"].as_str() {
                Some(next) if !records.is_empty() => iterator = next.to_owned(),
                _ => break,
            }
        }
    }
    Some(pairs)
}

// ---------------------------------------------------------------------------
// The scenario
// ---------------------------------------------------------------------------

fn run_scenario(cell: &Cell, tc: Transcoder) -> CellVerdict {
    let seed = cell.seed;
    let mut violations: Vec<String> = Vec::new();
    let mut cluster = SimCluster::new_with_lsm_engines(
        seed,
        &ROLES,
        REPLICATION,
        Some(Duration::from_secs(DEFAULT_QUIESCE_AFTER_SECS)),
    );
    let sim = cluster.simulator();

    // ---- setup: tables over the wire, a backup catalog row, aux items ----
    for (table, stream) in [(TBL, true), (AUX, false)] {
        let (status, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.CreateTable",
            create_table_body(table, stream).as_bytes(),
        );
        assert_eq!(
            status, 200,
            "seed={seed}: CreateTable {table} failed: {body}"
        );
    }
    let tbl_tablet = tablet_of(&cluster, 0, TBL).expect("tbl0 has a tablet");
    let aux_tablet = tablet_of(&cluster, 0, AUX).expect("tbl1 has a tablet");
    complete_a_backup(&mut cluster, TBL, tbl_tablet);
    for i in 0..3u64 {
        let body = json!({
            "TableName": AUX,
            "Item": {"pk": {"S": "aux"}, "sk": {"S": format!("a{i}")}, "v": {"N": i.to_string()}},
        })
        .to_string();
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(status, 200, "seed={seed}: aux put failed: {resp}");
    }
    cluster.run_for(SETTLE);

    // Crash stops: a slow sync on every disk so an un-synced tail exists at
    // the crash instant. Set only now, after the `block_on` engine opens.
    if cell.stop != Stop::Clean {
        let mut cfg = DiskConfig::default();
        cfg.set_sync_delay(SYNC_DELAY);
        if cell.stop == Stop::TornTail {
            cfg.torn_tail_on_crash = true;
            cfg.corrupt_on_crash = true;
        }
        for n in 0..NODES {
            sim.set_disk_config_for(nid(n), cfg.clone());
        }
    }

    // ---- phase 1 ----
    let shared = Arc::new(Shared {
        rec: Mutex::new(Recorder::new(seed)),
        next_value: Mutex::new(0),
        done: Mutex::new(0),
    });
    let rounds = if cell.stop == Stop::Clean {
        ROUNDS_CLEAN
    } else {
        ROUNDS_CRASH
    };
    let client_ids = spawn_clients(&cluster, &shared, 0, rounds);
    let mut pre_snapshot: Option<BTreeMap<Key, Vec<u64>>> = None;
    if cell.stop == Stop::Clean {
        if !run_until_clients_done(&mut cluster, &shared) {
            violations.push("phase-1 workload did not finish within its budget".into());
        }
        cluster.run_for(DRAIN);
    } else {
        cluster.run_for(Duration::from_millis(600 + mix(seed, "crash_delay") % 1400));
        let mut waited = Duration::ZERO;
        while ok_appends(&shared.history()) < 2 && waited < Duration::from_secs(60) {
            cluster.run_for(POLL);
            waited += POLL;
        }
        cluster.run_for(Duration::from_millis(mix(seed, "crash_jitter") % 500));
    }
    for id in &client_ids {
        sim.stop(id.clone());
    }
    // Stream sealing before the stop (seeded), so some cells carry sealed
    // shards in the shared segment store and some only an open tail.
    let sealed = mix(seed, "seal").is_multiple_of(2);
    if sealed {
        for n in 0..NODES {
            cluster.drive_stream_seal(n);
        }
    }
    if cell.stop == Stop::Clean {
        let mut snap = BTreeMap::new();
        for key in 0..KEYSPACE {
            match wire_read(&mut cluster, key % 3, key) {
                Some(list) => {
                    snap.insert(key, list);
                }
                None => violations.push(format!("pre-stop snapshot read of key {key} failed")),
            }
        }
        pre_snapshot = Some(snap);
    }
    let history_before = shared.history();
    let acked = acked_by_key(&history_before);
    let acks_before = ok_appends(&history_before);

    // ---- the upgrade: whole-cluster stop, transcode every disk, restart ----
    for n in 0..NODES {
        let id = nid(n);
        if cell.stop != Stop::Clean {
            sim.crash(id.clone());
        }
        sim.stop(id.clone());
        if cell.stop != Stop::Clean {
            sim.restart(id.clone()); // clears the crashed flag
        }
        // The recovery path runs on a healthy disk (see SYNC_DELAY).
        sim.set_disk_config_for(id, DiskConfig::default());
    }
    let mut summary = [0usize; 4];
    for n in 0..NODES {
        let env = sim.env(nid(n));
        let report = tc(&env, cell.back, &transcode_opts(seed, n))
            .unwrap_or_else(|e| panic!("transcode of node {n} failed: {e}"));
        summary[0] += report.transcoded.len();
        summary[1] += report.kept.len();
        summary[2] += report.unrecognised.len();
        summary[3] += report.not_reached.len();
    }
    let transcode_summary = format!(
        "transcoded={} kept={} unrecognised={} not_reached={}",
        summary[0], summary[1], summary[2], summary[3]
    );
    eprintln!("  {transcode_summary}");
    for n in 0..NODES {
        cluster.restart(n);
    }

    // ---- catch-up under a seeded partition of one node from one peer ----
    if mix(seed, "partition").is_multiple_of(2) {
        let r = mix(seed, "partition_node") % NODES;
        let peer = (r + 1 + mix(seed, "partition_peer") % (NODES - 1)) % NODES;
        cluster.partition(r, peer);
        cluster.run_for(Duration::from_millis(
            300 + mix(seed, "partition_window") % 2200,
        ));
        cluster.heal_all();
    }

    // ---- oracle, before any phase-2 write can mask a loss ----
    let control_nodes = [0u64, 1, 2];
    if !converge(&mut cluster, |c| {
        control_nodes.iter().all(|&n| {
            let (commit, applied) = c.control_raft_indices(n);
            commit > 0 && applied >= commit
        })
    }) {
        let idx: Vec<_> = control_nodes
            .iter()
            .map(|&n| (n, cluster.control_raft_indices(n)))
            .collect();
        violations.push(format!(
            "control apply frontier never reached commit_index after the restart: {idx:?}"
        ));
    }

    let tables_agree = |c: &mut SimCluster| {
        let want: Vec<BTreeSet<TabletId>> = [TBL, AUX]
            .iter()
            .map(|t| {
                c.metadata(0)
                    .tablets_for_table(t)
                    .map(|(id, _)| *id)
                    .collect()
            })
            .collect();
        want.iter().all(|s| !s.is_empty())
            && (0..NODES).all(|n| {
                [TBL, AUX].iter().zip(&want).all(|(t, w)| {
                    let got: BTreeSet<TabletId> = c
                        .metadata(n)
                        .tablets_for_table(t)
                        .map(|(id, _)| *id)
                        .collect();
                    &got == w
                })
            })
    };
    if !converge(&mut cluster, tables_agree) {
        violations.push(
            "post-restart: nodes (the data-only mirror included) disagree on the table/tablet map"
                .into(),
        );
    }
    let backup_ok = |c: &mut SimCluster| {
        (0..NODES).all(|n| {
            c.metadata(n)
                .backups
                .get(BACKUP_ID)
                .is_some_and(|row| row.status == BackupStatus::Available)
        })
    };
    if !converge(&mut cluster, backup_ok) {
        violations.push(
            "post-restart: the pre-upgrade backup catalog row is not Available on every node"
                .into(),
        );
    }
    let hosted = |c: &mut SimCluster| {
        [tbl_tablet, aux_tablet]
            .iter()
            .all(|&t| live_replicas(c, t).len() == REPLICATION)
    };
    if !converge(&mut cluster, hosted) {
        violations.push(format!(
            "post-restart: tablets not re-hosted on {REPLICATION} replicas (tbl0 on {:?}, tbl1 on {:?})",
            live_replicas(&cluster, tbl_tablet),
            live_replicas(&cluster, aux_tablet)
        ));
    }

    // The wire probe: every phase-1 acknowledged append reads back, in order.
    let mut probe_reads: BTreeMap<Key, Vec<u64>> = BTreeMap::new();
    for key in 0..KEYSPACE {
        let mut got = None;
        let _ = converge(&mut cluster, |c| {
            got = wire_read(c, key % NODES, key);
            got.is_some()
        });
        match got {
            Some(list) => {
                shared.rec.lock().expect("recorder poisoned").ok(
                    900,
                    sim.now().0,
                    vec![Mop::Read {
                        key,
                        observed: Some(list.clone()),
                    }],
                );
                probe_reads.insert(key, list);
            }
            None => violations.push(format!(
                "post-restart probe: key {key} unreadable after the upgrade \
                 (lost acknowledged append: {:?})",
                acked.get(&key).cloned().unwrap_or_default()
            )),
        }
    }
    for (key, want) in &acked {
        if let Some(got) = probe_reads.get(key)
            && !is_subsequence(want, got)
        {
            violations.push(format!(
                "post-restart probe: lost acknowledged append on key {key}: acked {want:?}, read {got:?}"
            ));
        }
    }
    if let Some(pre) = &pre_snapshot {
        for (key, want) in pre {
            if let Some(got) = probe_reads.get(key)
                && got != want
            {
                violations.push(format!(
                    "post-restart probe: key {key} reads back differently after a clean \
                     upgrade: before {want:?}, after {got:?}"
                ));
            }
        }
    }
    // tbl1's aux items.
    for i in 0..3u64 {
        let body = json!({
            "ConsistentRead": true, "TableName": AUX,
            "Key": {"pk": {"S": "aux"}, "sk": {"S": format!("a{i}")}},
        })
        .to_string();
        let mut ok = false;
        let _ = converge(&mut cluster, |c| {
            let (status, resp) =
                c.dynamo_fast(i % NODES, "DynamoDB_20120810.GetItem", body.as_bytes());
            ok = status == 200
                && stream_json(&resp)["Item"]["v"]["N"].as_str() == Some(&i.to_string());
            ok
        });
        if !ok {
            violations.push(format!(
                "post-restart probe: aux item a{i} on {AUX} lost (lost acknowledged write)"
            ));
        }
    }
    // The stream still serves every acknowledged append.
    let mut pairs = None;
    let _ = converge(&mut cluster, |c| {
        pairs = stream_pairs(c, mix(seed, "stream_node") % NODES);
        pairs.as_ref().is_some_and(|p| {
            acked
                .iter()
                .all(|(k, vs)| vs.iter().all(|v| p.contains(&(*k, *v))))
        })
    });
    match pairs {
        None => violations.push("post-restart: the DynamoDB Streams API never answered".into()),
        Some(p) => {
            for (k, vs) in &acked {
                for v in vs {
                    if !p.contains(&(*k, *v)) {
                        violations.push(format!(
                            "post-restart: stream lost acknowledged append key {k} value {v} (sealed={sealed})"
                        ));
                    }
                }
            }
        }
    }

    // ---- phase 2: the cluster still accepts writes ----
    let _ids = spawn_clients(&cluster, &shared, 1, ROUNDS_PHASE2);
    if !run_until_clients_done(&mut cluster, &shared) {
        violations.push("phase-2 workload did not finish within its budget".into());
    }
    cluster.run_for(DRAIN);

    let history = shared.history();
    let cycles = check_cycles(&history);
    violations.extend(
        cycles
            .violations
            .into_iter()
            .map(|v| format!("cycles: {v}")),
    );

    let handle = cluster.handle();
    let states = |c: &SimCluster| -> Vec<BTreeMap<Key, Vec<u64>>> {
        live_replicas(c, tbl_tablet)
            .iter()
            .map(|&n| final_state(&handle, tbl_tablet, n))
            .collect()
    };
    let verdict = |c: &SimCluster| {
        let st = states(c);
        let durability = combine(seed, st.iter().map(|s| check_durability(&history, s)));
        let convergence = combine(
            seed,
            st.iter()
                .skip(1)
                .map(|s| check_convergence(seed, &st[0], s)),
        );
        (st.len(), durability, convergence)
    };
    let mut last = verdict(&cluster);
    let _ = converge(&mut cluster, |c| {
        last = verdict(c);
        last.0 > 0 && last.1.ok && last.2.ok
    });
    if last.0 == 0 {
        violations.push("no live replica of tbl0 after the upgrade".into());
    }
    violations.extend(
        last.1
            .violations
            .into_iter()
            .map(|v| format!("durability: {v}")),
    );
    violations.extend(
        last.2
            .violations
            .into_iter()
            .map(|v| format!("convergence: {v}")),
    );

    let acks_total = ok_appends(&history);
    let acks_after = acks_total.saturating_sub(acks_before);
    if acks_before == 0 {
        violations.push("vacuous: no acknowledged write before the stop".into());
    }
    if acks_after == 0 {
        violations.push("vacuous: no acknowledged write after the restart".into());
    }
    CellVerdict {
        cell: cell.name.clone(),
        seed,
        ok: violations.is_empty(),
        violations,
        panic: None,
        acks_before,
        acks_after,
        transcode_summary,
        history: format!("{history:?}"),
    }
}

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

fn has_violation(v: &CellVerdict, needle: &str) -> bool {
    v.violations.iter().any(|s| s.contains(needle))
}

#[test]
fn sim_cluster_upgrade_corpus_is_consistent() {
    let cells = corpus_cells();
    assert!(!cells.is_empty(), "no cell selected by the filters");
    for c in &cells {
        let v = run_cell(c, transcode::transcode_disk);
        eprintln!(
            "  {} ok={} acks before/after={}/{} ({})",
            v.cell, v.ok, v.acks_before, v.acks_after, v.transcode_summary
        );
        assert_cell_ok(&v);
    }
}

#[test]
fn sim_cluster_upgrade_cell_names_and_seeds_are_unique() {
    let cells = corpus::seed_expand(cells(), 3);
    let names: BTreeSet<_> = cells.iter().map(|c| c.name.clone()).collect();
    let seeds: BTreeSet<_> = cells.iter().map(|c| c.seed).collect();
    assert_eq!(names.len(), cells.len(), "duplicate cell name");
    assert_eq!(seeds.len(), cells.len(), "duplicate cell seed");
}

#[test]
fn sim_cluster_upgrade_run_is_deterministic() {
    let c = cell(Stop::Crash, 0);
    let a = run_cell(&c, transcode::transcode_disk);
    let b = run_cell(&c, transcode::transcode_disk);
    assert_eq!(a.history, b.history, "same seed, different history");
    assert_eq!(a.violations, b.violations);
}

fn control_cell(tag: &str, stop: Stop) -> Cell {
    let mut c = cell(stop, 0);
    c.name = format!("{}_{tag}", c.name);
    c.seed = corpus::name_seed(&format!("sim_cluster_upgrade/control/{tag}"));
    c
}

/// The identity run of the control cell must pass, or its negative controls
/// prove nothing.
#[test]
fn sim_cluster_upgrade_identity_control_passes() {
    for stop in [Stop::Clean, Stop::Crash] {
        let c = control_cell("identity", stop);
        assert_cell_ok(&run_cell(&c, transcode::transcode_disk));
    }
}

#[test]
fn sim_cluster_upgrade_negative_control_logs_halved_and_engines_wiped() {
    let c = control_cell("halve", Stop::Clean);
    assert_cell_ok(&run_cell(&c, transcode::transcode_disk));
    let v = run_cell(&c, halve_logs_and_wipe_engines);
    eprintln!("halve+wipe: {:?}", v.violations);
    assert!(
        !v.ok,
        "a halved log with wiped engines on every node must be caught"
    );
    assert!(
        has_violation(&v, "lost acknowledged append"),
        "expected a lost acknowledged append, got {:?}",
        v.violations
    );
}

#[test]
fn sim_cluster_upgrade_negative_control_total_loss() {
    let c = control_cell("wipe", Stop::Clean);
    let v = run_cell(&c, wipe_everything);
    eprintln!("wipe everything: {:?}", v.violations);
    assert!(!v.ok, "wiping every disk must be caught");
    assert!(
        has_violation(&v, "unreadable after the upgrade")
            || has_violation(&v, "lost acknowledged append"),
        "expected unreadable keys / lost acknowledged appends, got {:?}",
        v.violations
    );
}

#[test]
fn sim_cluster_upgrade_negative_control_truncated_lsm_file_fails_the_strict_open() {
    let c = control_cell("truncate", Stop::Clean);
    let v = run_cell(&c, truncate_an_lsm_file);
    eprintln!("truncate: {:?}", v.violations);
    assert!(!v.ok, "a truncated LSM file on every node must be caught");
    assert!(
        v.panic
            .as_deref()
            .is_some_and(|p| p.contains("strict open of the")),
        "expected the strict open to fail, got {:?}",
        v.violations
    );
}
