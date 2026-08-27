//! On-demand backup **capture** fault-injection corpus (ADR 0059 §4/§5/§6,
//! Train 1 PR③).
//!
//! ## What this proves, and against which layer
//!
//! The capture driver (`animusd::backup_capture`) and the completion
//! aggregator (`animusd::backup_completion`) both live in `animusd` — a
//! crate with no `SimEnv` binding of its own (see `crates/animusd/
//! CLAUDE.md`). This file follows the exact precedent `backfill_fault_
//! corpus.rs`/`stream_lineage_corpus.rs` set for the identical layering
//! problem (ADR 0042/0043's sealer/consumer and ADR 0045's backfill
//! seeder/aggregator, also `animusd`-only): a **self-contained
//! reimplementation**, directly over `animus-cp-data`'s `RaftKvNode` and a
//! bare `animus-control::Metadata` (mutated with plain `.apply()` calls —
//! no live control Raft), mirroring the production functions' exact
//! algorithms rather than importing them (they are private to `animusd`).
//!
//! **The §6 split-re-planning DECISION itself is real production code,
//! never reimplemented**: [`animus_control::Metadata::
//! backup_capture_target`]/`live_split_descendants`/
//! `backup_ready_to_complete`/`backup_manifest_tablet_progress` are called
//! directly — only the capture driver's own scan/chunk/cursor mechanics
//! and the aggregator's own manifest-assembly/stuck-timeout mechanics are
//! mirrored here (see [`backup_capture_tick`]/[`backup_completion_tick`]'s
//! own docs for the exact correspondence to `animusd::backup_capture::
//! backup_capture_tick`/`animusd::backup_completion::
//! backup_completion_tick`).
//!
//! ## Verification, without restore (Train 2's own concern)
//!
//! A completed backup's manifest + every referenced data object are
//! decoded directly ([`read_all_captured_rows`]) and diffed against an
//! independently-tracked model of the source table's committed state at
//! capture-pin time — never through a restore path, which doesn't exist
//! yet. [`assert_backup_matches_model`] is the one shared assertion every
//! scenario's own happy ending calls, checking: every decoded row is
//! exactly a value this test itself wrote (never a raw intent envelope —
//! §5's "committed values only" rule, checked by never having written an
//! envelope-tagged byte string into the model to begin with, and directly
//! by exercising a genuinely staged-but-unresolved intent in
//! [`single_tablet_backup_converges_under_concurrent_writes`]); no key is
//! ever double-counted across two different reporting tablets (the §6
//! correctness argument: a split's children partition the parent's range,
//! so the union of what they each captured must equal — not exceed — the
//! model); and a crash never yields a torn `Available` (`assert_backup_
//! matches_model` itself requires the manifest + every referenced chunk to
//! decode cleanly and match, so calling it after driving to `Available` at
//! any of this corpus's kill points *is* that assertion — a scenario that
//! reached `Available` with a missing/corrupt object would fail right
//! there instead of silently passing).
//!
//! ## Corpus doctrine (ADR 0014)
//!
//! Frozen, named scenario cells (one `#[test]` each), a depth knob
//! (`ANIMUS_BACKUP_SEEDS`, default 1 — variant 0 always keeps the cell's
//! own canonical, name-derived seed, matching every other corpus's
//! `seed_expand` convention). See `crates/animus-test/CLAUDE.md` for the
//! full knob table.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_control::{
    ApplyOutcome, BackupStatus, ColumnType, MetaCommand, Metadata, ProposeResult, TableSchema,
};
use animus_cp_data::backup::{
    self as backup_codec, BackupManifestObject, BackupManifestTabletEntry,
};
use animus_cp_data::cursor;
use animus_cp_data::{
    KIND_BASE, KIND_CURSOR, KIND_LSI, RaftKvNode, SeedRow, StorageScope, TxnWrite,
};
use animus_env::{Clock, EnvExt, SegmentStore, nid};
use animus_sim::{SegmentFaultConfig, SimEnv, SimSegmentStore, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId, escape, partition_token};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const NODES: [u64; 3] = [70, 71, 72];
const TABLE: &str = "widgets";
/// A deliberately small per-chunk row cap (unlike production's `CHUNK_ROWS
/// == 200`) so a modest row count still needs several chunks/ticks — the
/// multi-tick interleaving surface is exactly where a fault has room to
/// land mid-capture.
const CHUNK_ROWS: usize = 3;

// --- corpus boilerplate (identical convention to every sibling corpus) ----

fn name_seed(name: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h | 1
}

fn seeds_per_cell() -> usize {
    std::env::var("ANIMUS_BACKUP_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&k: &usize| k > 0)
        .unwrap_or(1)
}

fn cell_seed(name: &str, variant: usize) -> u64 {
    if variant == 0 {
        name_seed(name)
    } else {
        name_seed(&format!("{name}_s{variant}"))
    }
}

fn for_each_seed(name: &str, mut body: impl FnMut(u64)) {
    for variant in 0..seeds_per_cell() {
        body(cell_seed(name, variant));
    }
}

// --- tablet-group harness (mirrors backfill_fault_corpus.rs/stream_lineage_corpus.rs) --

struct Group {
    id: TabletId,
    range: KeyRange,
    nodes: Vec<KvNode>,
}

fn engines() -> BTreeMap<u64, MemoryEngine> {
    NODES.iter().map(|&n| (n, MemoryEngine::new())).collect()
}

fn start_group(
    sim: &Simulator,
    engines: &BTreeMap<u64, MemoryEngine>,
    id: TabletId,
    range: KeyRange,
) -> Group {
    let ids: Vec<_> = NODES.iter().copied().map(nid).collect();
    let nodes = NODES
        .iter()
        .map(|&n| {
            RaftKvNode::start_hosted(
                sim.env(nid(n)),
                ids.clone(),
                engines[&n].clone(),
                StorageScope::new(range.clone()),
                id.0,
            )
        })
        .collect();
    Group { id, range, nodes }
}

fn elect(sim: &mut Simulator, group: &Group, live: &[usize], seed: u64) -> usize {
    for _ in 0..200 {
        let ls: Vec<usize> = live
            .iter()
            .copied()
            .filter(|&i| group.nodes[i].is_leader())
            .collect();
        if ls.len() == 1 {
            return ls[0];
        }
        sim.run_for(Duration::from_millis(20));
    }
    panic!(
        "no unique leader elected for tablet {:?} among {live:?} (seed={seed})",
        group.id
    );
}

fn confirm(sim: &mut Simulator, node: &KvNode, index: u64, seed: u64) {
    for _ in 0..300 {
        if node.engine_applied_index() >= index {
            return;
        }
        sim.run_for(Duration::from_millis(10));
    }
    panic!("write index {index} never applied (seed={seed})");
}

fn propose_confirmed(sim: &mut Simulator, node: &KvNode, seed: u64, result: ProposeResult) -> u64 {
    match result {
        ProposeResult::Accepted { index, .. } => {
            confirm(sim, node, index, seed);
            index
        }
        other => panic!("[seed={seed}] proposal rejected: {other:?}"),
    }
}

fn logical(pk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out
}

fn write_base_row(sim: &mut Simulator, node: &KvNode, pk: &[u8], value: &[u8]) {
    let result = node.put_kind_batch(
        vec![(KIND_BASE, logical(pk), Some(value.to_vec()))],
        Vec::new(),
    );
    propose_confirmed(sim, node, 0, result);
}

// --- capture driver mirror (ADR 0059 §4/§5/§6) -----------------------------

/// The row kinds a backup ever captures — mirrors `animusd::backup_capture
/// ::CAPTURE_KINDS` exactly (`KIND_FOOTPRINT` omitted here: this corpus
/// never writes footprint rows, and the driver's own per-kind loop logic
/// is identical regardless of which kinds are listed).
const CAPTURE_KINDS: [u8; 2] = [KIND_BASE, KIND_LSI];

/// Mirrors `animusd::backup_capture::CaptureCursor` field-for-field — see
/// that type's own doc for why each field is exactly what write-once
/// safety across a crash/leader-change needs.
#[derive(Clone, Debug, PartialEq, Eq)]
struct CaptureCursor {
    cut_version: u64,
    phase: usize,
    next_key: Vec<u8>,
    next_chunk: u64,
    bytes_so_far: u64,
}

impl CaptureCursor {
    fn fresh(cut_version: u64) -> Self {
        CaptureCursor {
            cut_version,
            phase: 0,
            next_key: Vec::new(),
            next_chunk: 0,
            bytes_so_far: 0,
        }
    }

    fn done(&self) -> bool {
        self.phase >= CAPTURE_KINDS.len()
    }
}

fn backup_cursor_tag(backup_id: &str) -> String {
    format!("backup:{backup_id}")
}

const CURSOR_CODEC_VERSION: u8 = 1;

/// Mirrors `animusd::backup_capture::encode_capture_cursor` byte-for-byte.
fn encode_capture_cursor(c: &CaptureCursor) -> Vec<u8> {
    let mut out = Vec::with_capacity(30 + c.next_key.len());
    out.push(CURSOR_CODEC_VERSION);
    out.push(u8::try_from(c.phase).expect("phase fits a byte"));
    out.extend_from_slice(&c.cut_version.to_be_bytes());
    out.extend_from_slice(&c.next_chunk.to_be_bytes());
    out.extend_from_slice(&c.bytes_so_far.to_be_bytes());
    out.extend_from_slice(&u32::try_from(c.next_key.len()).unwrap().to_be_bytes());
    out.extend_from_slice(&c.next_key);
    out
}

fn decode_capture_cursor(bytes: &[u8]) -> Option<CaptureCursor> {
    const HEADER: usize = 1 + 1 + 8 + 8 + 8 + 4;
    if bytes.len() < HEADER || bytes[0] != CURSOR_CODEC_VERSION {
        return None;
    }
    let phase = bytes[1] as usize;
    let cut_version = u64::from_be_bytes(bytes[2..10].try_into().ok()?);
    let next_chunk = u64::from_be_bytes(bytes[10..18].try_into().ok()?);
    let bytes_so_far = u64::from_be_bytes(bytes[18..26].try_into().ok()?);
    let key_len = u32::from_be_bytes(bytes[26..30].try_into().ok()?) as usize;
    if bytes.len() != HEADER + key_len {
        return None;
    }
    Some(CaptureCursor {
        cut_version,
        phase,
        next_key: bytes[HEADER..].to_vec(),
        next_chunk,
        bytes_so_far,
    })
}

/// One capture step for one `(backup_id, tablet)` pair — mirrors
/// `animusd::backup_capture::backup_capture_tick`'s exact algorithm
/// (object identity, cursor persistence, and write-once discipline
/// against a store fault, all identical — see that function's own doc for
/// the full correctness argument). Returns `true` once this tablet's own
/// [`CaptureCursor`] has reached [`CaptureCursor::done`] (the caller then
/// proposes `RecordBackupTabletComplete`, mirroring `animusd::
/// backup_capture::report_capture_complete`).
fn backup_capture_tick(
    sim: &mut Simulator,
    node: &KvNode,
    range: &KeyRange,
    store: &SimSegmentStore,
    backup_id: &str,
    tablet: TabletId,
    seed: u64,
) -> bool {
    let tag = backup_cursor_tag(backup_id);
    let cursor_key_bytes = cursor::cursor_key(&range.start, &tag);
    let mut cur = block_on(node.local_get_kind(KIND_CURSOR, &cursor_key_bytes))
        .and_then(|b| decode_capture_cursor(&b))
        .unwrap_or_else(|| CaptureCursor::fresh(node.engine_latest_version()));

    if cur.done() {
        return true;
    }

    let kind = CAPTURE_KINDS[cur.phase];
    let (rows, next) =
        block_on(node.local_scan_kind_snapshot(kind, &cur.next_key, cur.cut_version, CHUNK_ROWS));

    if rows.is_empty() {
        cur.phase += 1;
        cur.next_key = Vec::new();
        let result = node.put_kind_batch_conditioned(
            vec![(
                KIND_CURSOR,
                cursor_key_bytes,
                Some(encode_capture_cursor(&cur)),
            )],
            Vec::new(),
            Vec::new(),
        );
        propose_confirmed(sim, node, seed, result);
        return cur.done();
    }

    let seed_rows: Vec<SeedRow> = rows
        .iter()
        .map(|(k, v, ver)| (kind, k.clone(), Some(v.clone()), *ver))
        .collect();
    let object_bytes = backup_codec::encode_data_chunk(&seed_rows);
    let object_id = backup_codec::backup_data_object_id(backup_id, tablet.0, cur.next_chunk);

    // Write-once, fault-tolerant `put` — mirrors the production driver's
    // own discipline exactly: `SimSegmentStore`'s ack-lost fault (ADR 0043
    // §A7) still lands the object for real and only fails the CALLER's own
    // ack, so treat it identically to `Ok` for cursor-advance purposes (the
    // object is really there, `get` would find it). A genuine failure
    // (`SimSegmentStore`'s unavailability window) leaves the cursor
    // untouched, so the NEXT tick re-derives and re-`put`s the identical
    // bytes at the identical id.
    if block_on(store.put(&object_id, &object_bytes)).is_err()
        && block_on(store.get(&object_id)).ok().flatten().as_deref()
            != Some(object_bytes.as_slice())
    {
        return false;
    }
    cur.next_chunk += 1;
    cur.bytes_so_far += object_bytes.len() as u64;
    match next {
        Some(key) => cur.next_key = key,
        None => {
            cur.phase += 1;
            cur.next_key = Vec::new();
        }
    }
    let result = node.put_kind_batch_conditioned(
        vec![(
            KIND_CURSOR,
            cursor_key_bytes,
            Some(encode_capture_cursor(&cur)),
        )],
        Vec::new(),
        Vec::new(),
    );
    propose_confirmed(sim, node, seed, result);
    cur.done()
}

/// Drives one tablet's own capture to completion (re-electing a leader
/// every tick, tolerating a leadership change mid-sweep) and proposes
/// `RecordBackupTabletComplete` against `meta` once done — the whole
/// per-tablet workflow [`backup_capture_tick`]'s caller in `animusd` runs
/// tick-by-tick; this collapses it to one call for scenarios that don't
/// need to interleave a fault mid-sweep.
fn drive_tablet_capture_to_reported(
    sim: &mut Simulator,
    meta: &mut Metadata,
    group: &Group,
    live: &[usize],
    store: &SimSegmentStore,
    backup_id: &str,
    seed: u64,
) {
    for _ in 0..10_000 {
        let leader = elect(sim, group, live, seed);
        let cursor_key_bytes =
            cursor::cursor_key(&group.range.start, &backup_cursor_tag(backup_id));
        let done = backup_capture_tick(
            sim,
            &group.nodes[leader],
            &group.range,
            store,
            backup_id,
            group.id,
            seed,
        );
        if done {
            let cur = block_on(group.nodes[leader].local_get_kind(KIND_CURSOR, &cursor_key_bytes))
                .and_then(|b| decode_capture_cursor(&b))
                .expect("a Done cursor must be durably present");
            let outcome = meta.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: backup_id.to_owned(),
                tablet: group.id,
                cut_version: cur.cut_version,
                bytes: cur.bytes_so_far,
            });
            assert!(
                matches!(outcome, ApplyOutcome::Applied | ApplyOutcome::NoOp),
                "[seed={seed}] RecordBackupTabletComplete rejected: {outcome:?}"
            );
            return;
        }
        sim.run_for(Duration::from_millis(10));
    }
    panic!(
        "[seed={seed}] tablet {:?} capture never reached Done",
        group.id
    );
}

// --- completion aggregator mirror (ADR 0059 §3/§4) -------------------------

/// One aggregator tick — mirrors `animusd::backup_completion::
/// backup_completion_tick`'s exact decision: assemble + durably `put` the
/// manifest object (via the REAL `Metadata::backup_manifest_tablet_
/// progress`/`backup_ready_to_complete`, never reimplemented) before
/// proposing `CompleteBackup` (durable-before-visible), or propose
/// `FailBackup` once `stuck` records no progress for `timeout_nanos` of
/// **virtual** (`Clock`) time — env-time, deterministic, unlike
/// production's own real-clock `tokio::time::Instant` (which this crate
/// cannot reach at all — the whole reason this corpus exists).
fn backup_completion_tick(
    env: &SimEnv,
    meta: &mut Metadata,
    store: &SimSegmentStore,
    backup_id: &str,
    stuck: &mut BTreeMap<String, (u64, usize)>,
    timeout_nanos: u64,
) -> bool {
    let Some(row) = meta.backup(backup_id) else {
        return true;
    };
    if !matches!(row.status, BackupStatus::Creating) {
        return true;
    }
    if meta.backup_ready_to_complete(backup_id) {
        let tablet_progress: Vec<BackupManifestTabletEntry> = meta
            .backup_manifest_tablet_progress(backup_id)
            .into_iter()
            .filter_map(|(tablet, progress)| {
                progress.map(|p| BackupManifestTabletEntry {
                    tablet,
                    progress: p,
                })
            })
            .collect();
        let object = BackupManifestObject {
            manifest: row.manifest.clone(),
            tablet_progress,
        };
        let bytes = backup_codec::encode_manifest_object(&object);
        let object_id = backup_codec::backup_manifest_object_id(backup_id);
        if block_on(store.put(&object_id, &bytes)).is_err()
            && block_on(store.get(&object_id)).ok().flatten().as_deref() != Some(bytes.as_slice())
        {
            return false; // manifest put genuinely failed — retry next tick
        }
        let outcome = meta.apply(&MetaCommand::CompleteBackup {
            backup_id: backup_id.to_owned(),
        });
        assert!(
            matches!(outcome, ApplyOutcome::Applied),
            "CompleteBackup rejected: {outcome:?}"
        );
        return true;
    }
    let reported = meta
        .backup_manifest_tablet_progress(backup_id)
        .into_iter()
        .filter(|(_, p)| p.is_some())
        .count();
    let now = env.now().0;
    let (since, last_reported) = stuck.entry(backup_id.to_owned()).or_insert((now, reported));
    if reported > *last_reported {
        *last_reported = reported;
        *since = now;
        return false;
    }
    if now.saturating_sub(*since) >= timeout_nanos {
        let outcome = meta.apply(&MetaCommand::FailBackup {
            backup_id: backup_id.to_owned(),
            reason: "stuck-Creating timeout (corpus)".to_owned(),
        });
        assert!(
            matches!(outcome, ApplyOutcome::Applied),
            "FailBackup rejected: {outcome:?}"
        );
        return true;
    }
    false
}

fn drive_completion_to_available(
    sim: &mut Simulator,
    env: &SimEnv,
    meta: &mut Metadata,
    store: &SimSegmentStore,
    backup_id: &str,
    seed: u64,
) {
    let mut stuck = BTreeMap::new();
    for _ in 0..2_000 {
        if backup_completion_tick(env, meta, store, backup_id, &mut stuck, u64::MAX) {
            assert_eq!(
                meta.backup(backup_id).map(|r| r.status.clone()),
                Some(BackupStatus::Available),
                "[seed={seed}] the aggregator reached a terminal state other than Available"
            );
            return;
        }
        sim.run_for(Duration::from_millis(5));
    }
    panic!("[seed={seed}] backup {backup_id} never reached Available");
}

// --- bare-Metadata catalog helpers -----------------------------------------

fn base_meta() -> Metadata {
    let mut m = Metadata::default();
    assert_eq!(
        m.apply(&MetaCommand::CreateTableSchema {
            table: TABLE.into(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
        ApplyOutcome::Applied
    );
    m
}

fn create_tablet(meta: &mut Metadata, id: TabletId, range: KeyRange) {
    assert_eq!(
        meta.apply(&MetaCommand::CreateTablet {
            tablet: id,
            table: Some(TABLE.into()),
            range,
            replicas: NODES.iter().copied().map(nid).collect(),
        }),
        ApplyOutcome::Applied
    );
}

fn begin_backup(meta: &mut Metadata, backup_id: &str, wall_ms: u64) {
    assert_eq!(
        meta.apply(&MetaCommand::BeginBackup {
            backup_id: backup_id.to_owned(),
            table: TABLE.into(),
            created_wall_ms: wall_ms,
            backup_name: "backup".to_string(),
        }),
        ApplyOutcome::Applied
    );
}

fn split_tablet(meta: &mut Metadata, source: TabletId, split_key: Vec<u8>, new_id: TabletId) {
    let expected_epoch = meta.tablets[&source].epoch;
    let replicas = meta.tablets[&source].replicas.clone();
    assert_eq!(
        meta.apply(&MetaCommand::BeginSplit {
            parent: source,
            expected_epoch,
            split_key,
            children: [
                (new_id, replicas.clone()),
                (TabletId(new_id.0 + 1), replicas),
            ],
        }),
        ApplyOutcome::Applied
    );
    let bumped = meta.tablets[&source].epoch;
    assert_eq!(
        meta.apply(&MetaCommand::CutoverSplit {
            parent: source,
            expected_epoch: bumped,
            cutover_wall_ms: 1_000,
        }),
        ApplyOutcome::Applied
    );
}

// --- verification (ADR 0059 §5, "committed values only") ------------------

/// Every row this backup's own data objects hold, decoded directly —
/// `(tablet, key, value, version)`, across every `tablet` in
/// `reporting_tablets`. Enumerates each tablet's chunks by simple sequential
/// probing (`chunk = 0, 1, 2, ...` until a `get` misses) — sufficient for
/// this corpus's own direct-decode verification (Train 2's actual restore
/// algorithm is free to do this differently; `SegmentStore::list()` is
/// deliberately never relied on here either, mirroring the ADR's own
/// "the catalog, never a store listing, is the sole authority" rule).
fn read_all_captured_rows(
    store: &SimSegmentStore,
    backup_id: &str,
    reporting_tablets: &[TabletId],
) -> Vec<(TabletId, Vec<u8>, Vec<u8>, u64)> {
    let mut out = Vec::new();
    for &tablet in reporting_tablets {
        let mut chunk = 0u64;
        loop {
            let id = backup_codec::backup_data_object_id(backup_id, tablet.0, chunk);
            let Some(bytes) = block_on(store.get(&id)).expect("store get ok") else {
                break;
            };
            for (_, key, value, version) in
                backup_codec::decode_data_chunk(&bytes).expect("chunk decodes")
            {
                let value = value.expect("capture never writes a tombstone");
                out.push((tablet, key, value, version));
            }
            chunk += 1;
        }
    }
    out
}

/// The one shared "is this backup correct" assertion every scenario's own
/// happy ending calls (see the module doc's "Verification" section for the
/// full property list): the manifest decodes; its `tablet_progress` names
/// exactly the tablets that reported (real production tablets — never a
/// retired, split-superseded id, ADR 0059 §6); every decoded row is
/// present in `model` with the identical value; no key is ever decoded
/// twice (the split-descendant double-count hazard `Metadata::
/// backup_manifest_tablet_progress`'s own doc names); and no decoded key
/// or value is ever one this test staged as a pending transaction intent
/// (`forbidden_values`) — ADR 0059 §5's "committed values only, never a
/// raw envelope" rule, checked directly rather than merely by construction.
fn assert_backup_matches_model(
    store: &SimSegmentStore,
    backup_id: &str,
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    forbidden_values: &[Vec<u8>],
    seed: u64,
) {
    let manifest_bytes = block_on(store.get(&backup_codec::backup_manifest_object_id(backup_id)))
        .expect("store get ok")
        .unwrap_or_else(|| panic!("[seed={seed}] no manifest object for {backup_id}"));
    let manifest = backup_codec::decode_manifest_object(&manifest_bytes).expect("manifest decodes");

    let reporting_tablets: Vec<TabletId> =
        manifest.tablet_progress.iter().map(|e| e.tablet).collect();
    let rows = read_all_captured_rows(store, backup_id, &reporting_tablets);

    let mut seen: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for (tablet, key, value, _version) in &rows {
        assert!(
            seen.insert(key.clone(), value.clone()).is_none(),
            "[seed={seed}] key {key:?} captured more than once (tablet {:?} is a \
             duplicate reporter) — the §6 double-count hazard",
            tablet.0
        );
        for forbidden in forbidden_values {
            assert_ne!(
                value, forbidden,
                "[seed={seed}] a captured value byte-matches a value only ever staged \
                 inside a pending transaction intent — a raw envelope or its staged \
                 content leaked into the backup"
            );
        }
    }
    assert_eq!(
        &seen, model,
        "[seed={seed}] decoded backup content does not match the model — {backup_id}"
    );
}

// --- cell 1: single_tablet_backup_converges_under_concurrent_writes --------

fn scenario_single_tablet_backup_converges_under_concurrent_writes(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    create_tablet(&mut meta, TabletId(1), KeyRange::whole());
    let group = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));

    let mut model = BTreeMap::new();
    let leader = elect(&mut sim, &group, &live, seed);
    for i in 0..10 {
        let pk = format!("pre{i:03}");
        let value = format!("v{i}").into_bytes();
        write_base_row(&mut sim, &group.nodes[leader], pk.as_bytes(), &value);
        model.insert(logical(pk.as_bytes()), value);
    }

    // A pending, never-resolved transaction intent — its staged value must
    // never surface anywhere in the backup (ADR 0059 §5).
    let staged_key = logical(b"pending-intent");
    let staged_value = b"never-committed".to_vec();
    let write = TxnWrite {
        key: staged_key,
        value: Some(staged_value.clone()),
        kind_writes: Vec::new(),
        change_log: None,
        stage_marker: None,
    };
    let n = group.nodes[leader].clone();
    let env = n.env().clone();
    let slot: std::sync::Arc<std::sync::Mutex<Option<_>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let s = std::sync::Arc::clone(&slot);
    env.spawn_task(async move {
        let r = n
            .txn_stage_anchor("t", vec![write], Vec::new(), Vec::new())
            .await;
        *s.lock().unwrap() = Some(r);
    });
    sim.run_for(Duration::from_millis(300));
    assert!(
        slot.lock().unwrap().take().flatten().is_some(),
        "[seed={seed}] the intent must stage"
    );

    begin_backup(&mut meta, "backup-1", 1_000);

    // Pin the capture (first tick) BEFORE any post-pin write lands.
    let leader = elect(&mut sim, &group, &live, seed);
    backup_capture_tick(
        &mut sim,
        &group.nodes[leader],
        &group.range,
        &store,
        "backup-1",
        group.id,
        seed,
    );

    // Concurrent writes AFTER the pin: must never appear in the backup.
    for i in 0..5 {
        let leader = elect(&mut sim, &group, &live, seed);
        write_base_row(
            &mut sim,
            &group.nodes[leader],
            format!("post{i:03}").as_bytes(),
            b"late",
        );
    }

    drive_tablet_capture_to_reported(&mut sim, &mut meta, &group, &live, &store, "backup-1", seed);
    let env = sim.env(nid(NODES[0]));
    drive_completion_to_available(&mut sim, &env, &mut meta, &store, "backup-1", seed);

    assert_backup_matches_model(&store, "backup-1", &model, &[staged_value], seed);
}

#[test]
fn single_tablet_backup_converges_under_concurrent_writes() {
    for_each_seed(
        "single_tablet_backup_converges_under_concurrent_writes",
        scenario_single_tablet_backup_converges_under_concurrent_writes,
    );
}

// --- cell 2: leader_kill_mid_capture ----------------------------------------

fn scenario_leader_kill_mid_capture(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    create_tablet(&mut meta, TabletId(1), KeyRange::whole());
    let group = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let mut live = vec![0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));

    let mut model = BTreeMap::new();
    let leader = elect(&mut sim, &group, &live, seed);
    for i in 0..24 {
        let pk = format!("p{i:03}");
        let value = format!("v{i}").into_bytes();
        write_base_row(&mut sim, &group.nodes[leader], pk.as_bytes(), &value);
        model.insert(logical(pk.as_bytes()), value);
    }
    begin_backup(&mut meta, "backup-1", 1_000);

    // A couple of ticks under the first leader, then kill it mid-sweep.
    let leader = elect(&mut sim, &group, &live, seed);
    backup_capture_tick(
        &mut sim,
        &group.nodes[leader],
        &group.range,
        &store,
        "backup-1",
        group.id,
        seed,
    );
    let leader = elect(&mut sim, &group, &live, seed);
    backup_capture_tick(
        &mut sim,
        &group.nodes[leader],
        &group.range,
        &store,
        "backup-1",
        group.id,
        seed,
    );
    let cursor_key_bytes = cursor::cursor_key(&group.range.start, &backup_cursor_tag("backup-1"));
    let cursor_before =
        block_on(group.nodes[leader].local_get_kind(KIND_CURSOR, &cursor_key_bytes));
    assert!(
        cursor_before.is_some(),
        "[seed={seed}] a cursor must exist by now"
    );

    let dying = leader;
    sim.crash(nid(NODES[dying]));
    live.retain(|&i| i != dying);
    let new_leader = elect(&mut sim, &group, &live, seed);
    let mut cursor_after = None;
    for _ in 0..200 {
        cursor_after =
            block_on(group.nodes[new_leader].local_get_kind(KIND_CURSOR, &cursor_key_bytes));
        if cursor_after == cursor_before {
            break;
        }
        sim.run_for(Duration::from_millis(10));
    }
    assert_eq!(
        cursor_after, cursor_before,
        "[seed={seed}] the newly-elected leader must converge to the identical \
         durably-committed cursor"
    );

    drive_tablet_capture_to_reported(&mut sim, &mut meta, &group, &live, &store, "backup-1", seed);
    let env = sim.env(nid(NODES[0]));
    drive_completion_to_available(&mut sim, &env, &mut meta, &store, "backup-1", seed);
    assert_backup_matches_model(&store, "backup-1", &model, &[], seed);
}

#[test]
fn leader_kill_mid_capture() {
    for_each_seed("leader_kill_mid_capture", scenario_leader_kill_mid_capture);
}

// --- cell 3: capture_driver_node_crash_restart ------------------------------

/// A **true process restart** (`sim.stop` + a fresh `RaftKvNode::start_hosted`
/// on the same id and the same durable `MemoryEngine`, mirroring
/// `raftkv_linearizable.rs`'s own `StopRestart` nemesis) of the very node
/// that was mid-capture — as opposed to [`leader_kill_mid_capture`]'s
/// `sim.crash` (a live-but-muted process, failing over to a *different*
/// replica). The restarted node's own engine — and with it, the tablet's
/// `KIND_CURSOR` row and every chunk `put` so far — survives; the driver
/// resumes from the durable cursor exactly as if nothing happened.
fn scenario_capture_driver_node_crash_restart(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    create_tablet(&mut meta, TabletId(1), KeyRange::whole());
    let mut group = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let live = vec![0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));

    let mut model = BTreeMap::new();
    let leader = elect(&mut sim, &group, &live, seed);
    for i in 0..18 {
        let pk = format!("p{i:03}");
        let value = format!("v{i}").into_bytes();
        write_base_row(&mut sim, &group.nodes[leader], pk.as_bytes(), &value);
        model.insert(logical(pk.as_bytes()), value);
    }
    begin_backup(&mut meta, "backup-1", 1_000);

    let leader = elect(&mut sim, &group, &live, seed);
    backup_capture_tick(
        &mut sim,
        &group.nodes[leader],
        &group.range,
        &store,
        "backup-1",
        group.id,
        seed,
    );

    // Process exit + fresh start on the SAME id and durable engine.
    let restarted_id = NODES[leader];
    sim.stop(nid(restarted_id));
    let ids: Vec<_> = NODES.iter().copied().map(nid).collect();
    let fresh: KvNode = RaftKvNode::start_hosted(
        sim.env(nid(restarted_id)),
        ids,
        engines[&restarted_id].clone(),
        StorageScope::new(group.range.clone()),
        group.id.0,
    );
    group.nodes[leader] = fresh;
    sim.run_for(Duration::from_secs(2));

    drive_tablet_capture_to_reported(&mut sim, &mut meta, &group, &live, &store, "backup-1", seed);
    let env = sim.env(nid(NODES[0]));
    drive_completion_to_available(&mut sim, &env, &mut meta, &store, "backup-1", seed);
    assert_backup_matches_model(&store, "backup-1", &model, &[], seed);
}

#[test]
fn capture_driver_node_crash_restart() {
    for_each_seed(
        "capture_driver_node_crash_restart",
        scenario_capture_driver_node_crash_restart,
    );
}

// --- cell 4: split_races_capture_and_replans_onto_descendants (ADR 0059 §6) -

/// The named §6 scenario: a split cuts over **while capture is still in
/// flight** on the parent. The parent's own (never completed) cursor/chunks
/// are simply abandoned; each child restarts its own capture from scratch
/// over its own narrower range (`SplitPolicy::RestartFromScratch`, real
/// `Metadata::backup_capture_target`/`live_split_descendants` decide who
/// the live targets are) — the manifest's own `tablet_progress` ends up
/// naming the two children, never the retired parent, and the union of
/// what they each captured is exactly the parent's original row set with
/// no row ever double-counted (each child only ever holds its own
/// half-range's rows, copied here directly rather than through a real
/// `SeedBatch` — the split-build driver's own mechanics are out of this
/// corpus's scope, proven elsewhere, ADR 0050).
fn scenario_split_races_capture_and_replans_onto_descendants(seed: u64) {
    let mut sim = Simulator::new(seed);
    let parent_engines = engines();
    let mut meta = base_meta();
    create_tablet(&mut meta, TabletId(1), KeyRange::whole());
    let parent = start_group(&sim, &parent_engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));

    // Rows straddling the split key `"m..."` — half fall left, half right.
    let mut model = BTreeMap::new();
    let leader = elect(&mut sim, &parent, &live, seed);
    for pk in ["a001", "a002", "a003", "z001", "z002", "z003"] {
        let value = format!("v-{pk}").into_bytes();
        write_base_row(&mut sim, &parent.nodes[leader], pk.as_bytes(), &value);
        model.insert(logical(pk.as_bytes()), value);
    }
    begin_backup(&mut meta, "backup-1", 1_000);

    // The parent makes SOME progress before the split lands.
    let leader = elect(&mut sim, &parent, &live, seed);
    backup_capture_tick(
        &mut sim,
        &parent.nodes[leader],
        &parent.range,
        &store,
        "backup-1",
        parent.id,
        seed,
    );
    assert!(
        !meta
            .backup_manifest_tablet_progress("backup-1")
            .iter()
            .any(|(_, p)| p.is_some()),
        "[seed={seed}] the parent must not have finished before the split"
    );

    let split_key = b"m".to_vec();
    split_tablet(&mut meta, TabletId(1), split_key.clone(), TabletId(2));
    let (left_id, right_id) = (TabletId(2), TabletId(3));
    assert!(!meta.tablets.contains_key(&TabletId(1)));
    assert!(meta.backup_capture_target("backup-1", left_id));
    assert!(meta.backup_capture_target("backup-1", right_id));
    assert!(!meta.backup_capture_target("backup-1", TabletId(1)));

    // Materialize each child's own copied share directly (the split-build
    // driver's own SeedBatch mechanics are out of scope here) — every row
    // whose logical key falls in the child's own range.
    let left_range = KeyRange::new(Vec::new(), Some(split_key.clone()));
    let right_range = KeyRange::new(split_key, None);
    let left_engines = engines();
    let right_engines = engines();
    let left = start_group(&sim, &left_engines, left_id, left_range.clone());
    let right = start_group(&sim, &right_engines, right_id, right_range.clone());
    sim.run_for(Duration::from_secs(2));
    let ll = elect(&mut sim, &left, &live, seed);
    let rl = elect(&mut sim, &right, &live, seed);
    for (key, value) in &model {
        if left_range.contains(key) {
            let result = left.nodes[ll].put_kind_batch(
                vec![(KIND_BASE, key.clone(), Some(value.clone()))],
                Vec::new(),
            );
            propose_confirmed(&mut sim, &left.nodes[ll], seed, result);
        } else {
            assert!(
                right_range.contains(key),
                "[seed={seed}] every key falls in exactly one child's range"
            );
            let result = right.nodes[rl].put_kind_batch(
                vec![(KIND_BASE, key.clone(), Some(value.clone()))],
                Vec::new(),
            );
            propose_confirmed(&mut sim, &right.nodes[rl], seed, result);
        }
    }

    drive_tablet_capture_to_reported(&mut sim, &mut meta, &left, &live, &store, "backup-1", seed);
    drive_tablet_capture_to_reported(&mut sim, &mut meta, &right, &live, &store, "backup-1", seed);
    assert!(meta.backup_ready_to_complete("backup-1"));

    let env = sim.env(nid(NODES[0]));
    drive_completion_to_available(&mut sim, &env, &mut meta, &store, "backup-1", seed);

    let final_tablets: BTreeSet<u64> = meta
        .backup_manifest_tablet_progress("backup-1")
        .into_iter()
        .map(|(t, _)| t.0)
        .collect();
    assert_eq!(
        final_tablets,
        BTreeSet::from([left_id.0, right_id.0]),
        "[seed={seed}] the manifest's authoritative tablet list must be the two \
         children, never the retired parent"
    );
    assert_backup_matches_model(&store, "backup-1", &model, &[], seed);
}

#[test]
fn split_races_capture_and_replans_onto_descendants() {
    for_each_seed(
        "split_races_capture_and_replans_onto_descendants",
        scenario_split_races_capture_and_replans_onto_descendants,
    );
}

// --- cell 5: store_faults_ack_lost_puts_still_converge ----------------------

fn scenario_store_faults_ack_lost_puts_still_converge(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    create_tablet(&mut meta, TabletId(1), KeyRange::whole());
    let group = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut fault = SegmentFaultConfig::default();
    fault.set_put_ack_lost_prob(0.5);
    store.set_fault_config(fault);

    let mut model = BTreeMap::new();
    let leader = elect(&mut sim, &group, &live, seed);
    for i in 0..20 {
        let pk = format!("p{i:03}");
        let value = format!("v{i}").into_bytes();
        write_base_row(&mut sim, &group.nodes[leader], pk.as_bytes(), &value);
        model.insert(logical(pk.as_bytes()), value);
    }
    begin_backup(&mut meta, "backup-1", 1_000);

    drive_tablet_capture_to_reported(&mut sim, &mut meta, &group, &live, &store, "backup-1", seed);
    let env = sim.env(nid(NODES[0]));
    drive_completion_to_available(&mut sim, &env, &mut meta, &store, "backup-1", seed);
    assert_backup_matches_model(&store, "backup-1", &model, &[], seed);
}

#[test]
fn store_faults_ack_lost_puts_still_converge() {
    for_each_seed(
        "store_faults_ack_lost_puts_still_converge",
        scenario_store_faults_ack_lost_puts_still_converge,
    );
}

// --- cell 6: a_wedged_capture_fails_after_the_stuck_timeout -----------------

/// The aggregator's own stuck-`Creating` mark phase (ADR 0059 §3): a backup
/// whose pinned tablet never reports (modeling a permanently unreachable
/// tablet) is failed once no progress has been observed for the
/// configured window — proven with a deterministic, seed-reproducible
/// virtual clock (never real wall time, which this corpus has no access to
/// anyway), and proven NOT to fire early.
fn scenario_a_wedged_capture_fails_after_the_stuck_timeout(seed: u64) {
    let mut sim = Simulator::new(seed);
    let mut meta = base_meta();
    create_tablet(&mut meta, TabletId(1), KeyRange::whole());
    sim.run_for(Duration::from_secs(1));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    begin_backup(&mut meta, "backup-1", 1_000);

    const TIMEOUT_NANOS: u64 = 5_000_000_000; // 5s virtual
    let env = sim.env(nid(NODES[0]));
    let mut stuck = BTreeMap::new();

    // Nothing ever reports — tick the aggregator a few times well before
    // the timeout and confirm it stays `Creating`.
    for _ in 0..3 {
        sim.run_for(Duration::from_secs(1));
        let done = backup_completion_tick(
            &env,
            &mut meta,
            &store,
            "backup-1",
            &mut stuck,
            TIMEOUT_NANOS,
        );
        assert!(
            !done,
            "[seed={seed}] must not fail before the timeout elapses"
        );
    }
    assert_eq!(
        meta.backup("backup-1").map(|r| r.status.clone()),
        Some(BackupStatus::Creating)
    );

    // Advance well past the timeout.
    sim.run_for(Duration::from_secs(10));
    let mut failed = false;
    for _ in 0..10 {
        if backup_completion_tick(
            &env,
            &mut meta,
            &store,
            "backup-1",
            &mut stuck,
            TIMEOUT_NANOS,
        ) {
            failed = true;
            break;
        }
        sim.run_for(Duration::from_secs(1));
    }
    assert!(
        failed,
        "[seed={seed}] a wedged capture must eventually fail"
    );
    assert!(
        matches!(
            meta.backup("backup-1").map(|r| r.status.clone()),
            Some(BackupStatus::Failed { .. })
        ),
        "[seed={seed}] expected Failed, got {:?}",
        meta.backup("backup-1").map(|r| r.status.clone())
    );
}

#[test]
fn a_wedged_capture_fails_after_the_stuck_timeout() {
    for_each_seed(
        "a_wedged_capture_fails_after_the_stuck_timeout",
        scenario_a_wedged_capture_fails_after_the_stuck_timeout,
    );
}
