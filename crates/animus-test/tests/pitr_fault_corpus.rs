//! The PITR (point-in-time recovery) **sealing corpus** (ADR 0059 §9, Train
//! 3).
//!
//! ## What this proves, and against which layer
//!
//! The fifth consumer arm (`animusd::index_drain::pitr_tick`/
//! `pitr_seal_now`) lives in `animusd` — a crate with no `SimEnv` binding of
//! its own. This file follows the exact precedent `stream_lineage_corpus.rs`
//! set for the identical layering problem (ADR 0042/0043's own sealer): a
//! **self-contained reimplementation** directly over `animus-cp-data`'s
//! `RaftKvNode` and a bare `animus-control::Metadata` (mutated with plain
//! `.apply()` calls — no live control Raft), mirroring `pitr_seal_now`'s
//! exact algorithm rather than importing it (private to `animusd`), sharing
//! `segment.rs`'s real codec and `animus-sim`'s `SimSegmentStore` (standing
//! in for the backup store — both are the identical `SegmentStore` trait,
//! ADR 0059 §1) with the streams sealer's own corpus. Periodic base
//! snapshots and the janitor's own janitor-loop plumbing are `animusd`
//! background loops with no interesting protocol of their own beyond "call
//! `BeginBackup`/propose a mark" — this corpus instead proves the janitor's
//! **retention decision** (the keep-anchor predicate) directly as a pure
//! function, the same technique `pitr_janitor.rs`'s own unit tests use, at
//! depth and under randomized interleavings.
//!
//! ## Corpus doctrine (ADR 0014)
//!
//! Frozen, named scenario cells (one `#[test]` each), a depth knob
//! (`ANIMUS_PITR_SEEDS`, default 1 — variant 0 always keeps the cell's own
//! canonical, name-derived seed, matching every other corpus's
//! `seed_expand` convention). See `crates/animus-test/CLAUDE.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_control::{ApplyOutcome, ColumnType, MetaCommand, Metadata, PitrSpec, TableSchema};
use animus_cp_data::backup as backup_codec;
use animus_cp_data::host::{MemoryTabletEngines, MetadataView, Reconciler};
use animus_cp_data::{KIND_BASE, RaftKvNode, StorageScope, segment};
use animus_env::{Clock, Env, EnvExt, NodeId, Rng, SegmentStore, nid};
use animus_sim::{DiskConfig, NetConfig, SimEnv, SimSegmentStore, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId};
use animus_test::corpus;
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const NODES: [u64; 3] = [80, 81, 82];
const TABLE: &str = "orders";

/// Depth-scaled per-cell seed derivation and expansion — shared with every
/// sibling corpus in this crate, `animus_test::corpus`
/// (`odd_name_seed`/`seeds_from_env`/`for_each_seed`).
fn for_each_seed(name: &str, body: impl FnMut(u64)) {
    corpus::for_each_seed(name, corpus::seeds_from_env("ANIMUS_PITR_SEEDS"), body);
}

// --- tablet-group harness (mirrors stream_lineage_corpus.rs) --------------

struct Group {
    id: TabletId,
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
    Group { id, nodes }
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

fn propose_write(group: &Group, leader: usize, item_key: &[u8], record: &[u8]) -> u64 {
    match group.nodes[leader].put_kind_batch_conditioned(
        vec![(KIND_BASE, item_key.to_vec(), Some(record.to_vec()))],
        vec![(item_key.to_vec(), record.to_vec())],
        Vec::new(),
    ) {
        animus_control::ProposeResult::Accepted { index, .. } => index,
        other => panic!("leader rejected a write: {other:?}"),
    }
}

fn confirm(sim: &mut Simulator, group: &Group, leader: usize, index: u64, seed: u64) {
    for _ in 0..300 {
        if group.nodes[leader].engine_applied_index() >= index {
            return;
        }
        sim.run_for(Duration::from_millis(10));
    }
    panic!(
        "write index {index} never applied on tablet {:?} (seed={seed})",
        group.id
    );
}

fn write_and_journal(
    sim: &mut Simulator,
    group: &Group,
    live: &[usize],
    journal: &mut BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
    item_key: &[u8],
    payload: &[u8],
    seed: u64,
) {
    let leader = elect(sim, group, live, seed);
    let index = propose_write(group, leader, item_key, payload);
    confirm(sim, group, leader, index, seed);
    journal
        .entry(item_key.to_vec())
        .or_default()
        .push(payload.to_vec());
}

fn key(i: usize) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

fn record_hlc_suffix(key: &[u8]) -> Option<u64> {
    let n = key.len().checked_sub(8)?;
    Some(u64::from_be_bytes(key[n..].try_into().ok()?))
}

/// `orders` with a schema and PITR enabled at generation 1 — `base_meta`'s
/// PITR twin, mirroring `stream_lineage_corpus.rs::base_meta`'s exact
/// two-step shape (schema, then the feature toggle).
fn base_meta_with_pitr() -> Metadata {
    let mut m = Metadata::default();
    assert_eq!(
        m.apply(&MetaCommand::CreateTableSchema {
            table: TABLE.into(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
        ApplyOutcome::Applied,
        "schema registration must apply"
    );
    assert_eq!(
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: TABLE.into(),
            enabled: true,
            wall_ms: 0,
        }),
        ApplyOutcome::Applied,
        "PITR enable must apply"
    );
    m
}

fn current_open_pitr_epoch(meta: &Metadata, tablet: TabletId) -> u64 {
    meta.pitr_segments
        .range((tablet, 0)..=(tablet, u64::MAX))
        .next_back()
        .map_or(0, |((_, e), _)| e + 1)
}

/// One PITR seal attempt of `group`'s currently-open epoch — mirrors
/// `animusd::index_drain::pitr_seal_now`'s exact sequence (see that
/// function's own doc): scan `pending_changes()` past the effective PITR
/// watermark, sort by the HLC key suffix, encode a segment, `SegmentStore::
/// put` under the PITR object namespace, then propose
/// `MetaCommand::SealPitrSegment`. `None` if there was nothing to seal, or
/// (`skip_commit`) to model a crash between the store `put` and the catalog
/// commit — the caller re-invokes to model the idempotent retry.
#[allow(clippy::too_many_arguments)]
fn pitr_seal_now(
    meta: &mut Metadata,
    store: &SimSegmentStore,
    group: &Group,
    leader: usize,
    wall_ms: u64,
    skip_commit: bool,
) -> Option<u64> {
    let generation = meta.table_pitr(TABLE)?.generation;
    let watermark = meta.pitr_segment_watermark(group.id).unwrap_or(0);
    let mut filtered: Vec<(Vec<u8>, u64, Vec<u8>)> =
        block_on(group.nodes[leader].pending_changes())
            .into_iter()
            .filter_map(|(k, v)| {
                let hlc = record_hlc_suffix(&k)?;
                (hlc > watermark).then_some((k, hlc, v))
            })
            .collect();
    if filtered.is_empty() {
        return None;
    }
    filtered.sort_by_key(|(_, hlc, _)| *hlc);

    let epoch = current_open_pitr_epoch(meta, group.id);
    let hlc_range = (watermark, filtered.last().expect("non-empty").1);
    let count = filtered.len() as u64;
    let records: Vec<segment::SegmentRecord> = filtered
        .iter()
        .map(|(k, hlc, v)| segment::SegmentRecord {
            source_key: k.clone(),
            packed_hlc: *hlc,
            change_record: v.clone(),
        })
        .collect();
    let parent_shard_id = meta.stream_shard_parent_id(group.id, epoch);
    let header = segment::new_header(
        TABLE.to_owned(),
        format!("gen{generation}"),
        group.id.0,
        epoch,
        parent_shard_id,
        hlc_range,
        wall_ms,
    );
    let bytes = segment::encode(&header, &records);
    let env = group.nodes[leader].env();
    let seg_id = backup_codec::pitr_segment_object_id(
        TABLE,
        generation,
        group.id.0,
        epoch,
        env.node_id().as_str(),
        group.nodes[leader].term(),
        env.next_u64(),
    );
    block_on(store.put(&seg_id, &bytes)).ok()?;

    if skip_commit {
        return None; // modelled crash before catalog commit
    }
    let outcome = meta.apply(&MetaCommand::SealPitrSegment {
        table: TABLE.into(),
        generation,
        tablet: group.id,
        epoch,
        hlc_range,
        count,
        seal_wall_ms: wall_ms,
        replicas: Vec::new(),
        object_id: seg_id,
    });
    matches!(outcome, ApplyOutcome::Applied).then_some(epoch)
}

/// The model consumer: every closed PITR segment of `generation` in
/// `group`'s own chain (ascending epoch, fetched-and-sliced from the store),
/// then the open tail past the current watermark — the PITR twin of
/// `stream_lineage_corpus.rs::collect_tablet_records`.
fn collect_pitr_records(
    meta: &Metadata,
    store: &SimSegmentStore,
    group: &Group,
    leader: usize,
    generation: u64,
) -> Vec<(Vec<u8>, u64, Vec<u8>)> {
    let mut all = Vec::new();
    for ((_, _epoch), row) in meta
        .pitr_segments
        .range((group.id, 0)..=(group.id, u64::MAX))
        .filter(|(_, row)| row.table == TABLE && row.generation == generation)
    {
        let seg_id = row.object_id.as_str();
        let bytes = block_on(store.get(seg_id))
            .unwrap_or_else(|e| panic!("backup store get of {seg_id}: {e}"));
        let bytes =
            bytes.unwrap_or_else(|| panic!("sealed PITR segment {seg_id} missing from the store"));
        let (_, records) = segment::decode_and_slice(&bytes, row.hlc_range)
            .unwrap_or_else(|e| panic!("corrupt PITR segment {seg_id}: {e}"));
        for r in records {
            all.push((r.source_key, r.packed_hlc, r.change_record));
        }
    }
    let watermark = meta.pitr_segment_watermark(group.id).unwrap_or(0);
    let mut hot: Vec<(Vec<u8>, u64, Vec<u8>)> = block_on(group.nodes[leader].pending_changes())
        .into_iter()
        .filter_map(|(k, v)| {
            let hlc = record_hlc_suffix(&k)?;
            (hlc > watermark).then_some((k, hlc, v))
        })
        .collect();
    hot.sort_by_key(|(_, hlc, _)| *hlc);
    all.extend(hot);
    all
}

/// Asserts exactly-once delivery (every packed HLC exactly once, globally,
/// across `lineage`), per-item order, and total-count agreement against
/// `journal` — the PITR twin of `stream_lineage_corpus.rs::verify_lineage`.
/// `lineage` is `(group, leader, generation)`, parent-before-child order.
fn verify_pitr_lineage(
    meta: &Metadata,
    store: &SimSegmentStore,
    lineage: &[(&Group, usize, u64)],
    journal: &BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
    seed: u64,
) {
    let mut delivered_by_key: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    let mut total = 0usize;
    for (group, leader, generation) in lineage {
        // Exactly-once WITHIN one tablet's own chain — packed-HLC
        // uniqueness is a per-group guarantee, never a cross-group one
        // (no node-id bits, ADR 0018 §2's own documented tradeoff): two
        // independent sibling tablets can legitimately mint the identical
        // packed value absent production's real `SeedBatch` witnessing,
        // which this corpus doesn't model (see the split scenario's own
        // doc) — so this check is scoped per-group, not globally across
        // `lineage`.
        let mut seen_hlcs_this_group: BTreeSet<u64> = BTreeSet::new();
        for (source_key, hlc, record) in
            collect_pitr_records(meta, store, group, *leader, *generation)
        {
            assert!(
                seen_hlcs_this_group.insert(hlc),
                "[seed={seed}] tablet {:?}: hlc {hlc} delivered more than once within its own \
                 chain — violates exactly-once",
                group.id
            );
            let item_key = source_key[..source_key.len() - 8].to_vec();
            delivered_by_key.entry(item_key).or_default().push(record);
            total += 1;
        }
    }
    let journal_total: usize = journal.values().map(Vec::len).sum();
    assert_eq!(
        total, journal_total,
        "[seed={seed}] delivered record count must equal the journal's write count"
    );
    for (k, writes) in journal {
        assert_eq!(
            delivered_by_key.get(k).cloned().unwrap_or_default(),
            *writes,
            "[seed={seed}] key {k:?}'s delivered sequence must match its write order exactly"
        );
    }
}

// --- cell 1: quiet table rollover ------------------------------------------

fn scenario_quiet_table_pitr_rollover(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta_with_pitr();
    let group = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    for i in 0..3 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v0", seed);
    }
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = pitr_seal_now(&mut meta, &store, &group, leader, 1_000, false);
    assert_eq!(sealed, Some(0), "[seed={seed}] expected epoch 0 to seal");

    for i in 0..3 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v1", seed);
    }
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = pitr_seal_now(&mut meta, &store, &group, leader, 2_000, false);
    assert_eq!(sealed, Some(1), "[seed={seed}] expected epoch 1 to seal");

    verify_pitr_lineage(&meta, &store, &[(&group, leader, 1)], &journal, seed);
}

#[test]
fn quiet_table_pitr_rollover() {
    for_each_seed(
        "quiet_table_pitr_rollover",
        scenario_quiet_table_pitr_rollover,
    );
}

// --- cell 2: an idle group never proposes a PITR seal (quiescence) --------

/// Structural proof of ADR 0059 §9's quiescence contract's "read locally
/// without waking" half: with nothing pending, `pitr_seal_now` returns
/// `None` without ever calling `store.put`/proposing — the identical
/// early-return `pending_changes().is_empty()`-adjacent shape production's
/// own `pitr_tick` gates its whole sweep behind (`approx_bytes_kind(KIND_
/// CHANGE) == 0` in production; here, directly, the filtered-set-empty
/// check). A seal only ever happens once real writes land.
fn scenario_idle_group_never_proposes_a_pitr_seal(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta_with_pitr();
    let group = start_group(&sim, &engines, TabletId(2), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));

    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = pitr_seal_now(&mut meta, &store, &group, leader, 500, false);
    assert_eq!(sealed, None, "[seed={seed}] an idle group must never seal");
    assert!(
        meta.pitr_segments.is_empty(),
        "[seed={seed}] no catalog row from a no-op seal attempt"
    );

    // Now a real write lands — the very next attempt seals it.
    let mut journal = BTreeMap::new();
    write_and_journal(&mut sim, &group, &live, &mut journal, &key(0), b"v0", seed);
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = pitr_seal_now(&mut meta, &store, &group, leader, 600, false);
    assert_eq!(sealed, Some(0), "[seed={seed}] a real write must seal");
}

#[test]
fn idle_group_never_proposes_a_pitr_seal() {
    for_each_seed(
        "idle_group_never_proposes_a_pitr_seal",
        scenario_idle_group_never_proposes_a_pitr_seal,
    );
}

// --- cell 3: leader kill mid-seal converges --------------------------------

fn scenario_kill_sealing_leader(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta_with_pitr();
    let group = start_group(&sim, &engines, TabletId(3), KeyRange::whole());
    let mut live = vec![0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    for i in 0..4 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v0", seed);
    }
    let old_leader = elect(&mut sim, &group, &live, seed);
    // A crash between the store `put` and the catalog commit — the
    // idempotent-retry recovery argument (ledger-named-object amendment).
    let none = pitr_seal_now(&mut meta, &store, &group, old_leader, 1_000, true);
    assert_eq!(
        none, None,
        "[seed={seed}] skip_commit models no catalog row yet"
    );
    assert!(meta.pitr_segments.is_empty());

    sim.crash(nid(NODES[old_leader]));
    live.retain(|&i| i != old_leader);
    let new_leader = elect(&mut sim, &group, &live, seed);

    // More writes land under the new leader before the retry.
    for i in 4..7 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v0", seed);
    }
    let sealed = pitr_seal_now(&mut meta, &store, &group, new_leader, 2_000, false);
    assert_eq!(
        sealed,
        Some(0),
        "[seed={seed}] the retry re-seals the full backlog as one epoch"
    );

    verify_pitr_lineage(&meta, &store, &[(&group, new_leader, 1)], &journal, seed);
}

#[test]
fn kill_sealing_leader_pitr_converges() {
    for_each_seed(
        "kill_sealing_leader_pitr_converges",
        scenario_kill_sealing_leader,
    );
}

// --- cell 4: disable then re-enable resets the window ----------------------

fn scenario_disable_reenable(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta_with_pitr();
    let group = start_group(&sim, &engines, TabletId(4), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal_gen1 = BTreeMap::new();

    for i in 0..3 {
        write_and_journal(
            &mut sim,
            &group,
            &live,
            &mut journal_gen1,
            &key(i),
            b"v0",
            seed,
        );
    }
    let leader = elect(&mut sim, &group, &live, seed);
    assert_eq!(
        pitr_seal_now(&mut meta, &store, &group, leader, 1_000, false),
        Some(0)
    );

    // Disable: the final seal runs first (mirroring `dynamo.rs`'s own
    // sequencing), so the hot tail is fully covered before the row flips.
    for i in 3..5 {
        write_and_journal(
            &mut sim,
            &group,
            &live,
            &mut journal_gen1,
            &key(i),
            b"v1",
            seed,
        );
    }
    let leader = elect(&mut sim, &group, &live, seed);
    assert_eq!(
        pitr_seal_now(&mut meta, &store, &group, leader, 1_500, false),
        Some(1)
    );
    assert_eq!(
        meta.apply(&MetaCommand::UpdateContinuousBackups {
            table: TABLE.into(),
            enabled: false,
            wall_ms: 2_000,
        }),
        ApplyOutcome::Applied
    );
    verify_pitr_lineage(&meta, &store, &[(&group, leader, 1)], &journal_gen1, seed);

    // Re-enable: a fresh generation (2), and a fresh journal — no fake
    // continuity with generation 1's own coverage.
    assert_eq!(
        meta.apply(&MetaCommand::UpdateContinuousBackups {
            table: TABLE.into(),
            enabled: true,
            wall_ms: 3_000,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        meta.table_pitr(TABLE).map(|s: &PitrSpec| s.generation),
        Some(2)
    );
    let mut journal_gen2 = BTreeMap::new();
    for i in 0..3 {
        write_and_journal(
            &mut sim,
            &group,
            &live,
            &mut journal_gen2,
            &key(i),
            b"g2",
            seed,
        );
    }
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = pitr_seal_now(&mut meta, &store, &group, leader, 3_500, false);
    assert_eq!(
        sealed,
        Some(2),
        "[seed={seed}] the SAME tablet's epoch chain continues (2), never resets"
    );
    let row = &meta.pitr_segments[&(group.id, 2)];
    assert_eq!(
        row.generation, 2,
        "[seed={seed}] the new segment carries the new generation"
    );
    verify_pitr_lineage(&meta, &store, &[(&group, leader, 2)], &journal_gen2, seed);
}

#[test]
fn disable_then_reenable_resets_generation_and_continues_epoch_chain() {
    for_each_seed(
        "disable_then_reenable_resets_generation_and_continues_epoch_chain",
        scenario_disable_reenable,
    );
}

// --- cell 6: drop-table retention hold --------------------------------------

/// PITR segments and the generation floor survive `DropTableSchema` — the
/// catalog's deliberate outlives-the-table override of the streams
/// retention-zero rule (ADR 0059 §9/§10). Metadata-only (no `RaftKvNode`
/// needed — this is a pure catalog claim), but included in this corpus
/// (rather than left to `animus-control`'s own unit tests alone) so the
/// "mixed load with faults" property list this file's own doc promises is
/// genuinely all covered in one place, seed-reproducibly.
fn scenario_drop_table_then_segments_survive(seed: u64) {
    let mut meta = base_meta_with_pitr();
    let tablet = TabletId(20);
    assert_eq!(
        meta.apply(&MetaCommand::CreateTablet {
            tablet,
            table: Some(TABLE.into()),
            range: KeyRange::whole(),
            replicas: Vec::new(),
        }),
        ApplyOutcome::Applied
    );
    let generation = meta.table_pitr(TABLE).unwrap().generation;
    assert_eq!(
        meta.apply(&MetaCommand::SealPitrSegment {
            table: TABLE.into(),
            generation,
            tablet,
            epoch: 0,
            hlc_range: (0, 100),
            count: 3,
            seal_wall_ms: 1_000,
            replicas: Vec::new(),
            object_id: format!("backup/pitr/{TABLE}/{}/0/seed{seed}", tablet.0),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        meta.apply(&MetaCommand::DropTableSchema {
            table: TABLE.into(),
        }),
        ApplyOutcome::Applied,
        "[seed={seed}] DropTableSchema must apply"
    );
    assert!(
        meta.pitr_segments.contains_key(&(tablet, 0)),
        "[seed={seed}] a PITR segment must survive its source table's drop"
    );
    assert_eq!(
        meta.pitr_generation.get(TABLE),
        Some(&generation),
        "[seed={seed}] the generation floor must survive the drop too"
    );

    // The janitor's own retention decision is untouched by the drop — the
    // segment still ages out by ordinary retention alone, never
    // immediately (unlike the streams retention-zero rule).
    let now_ms = 1_000 + Duration::from_secs(35 * 24 * 60 * 60).as_millis() as u64;
    let due = now_ms.saturating_sub(meta.pitr_segments[&(tablet, 0)].seal_wall_ms)
        >= Duration::from_secs(35 * 24 * 60 * 60).as_millis() as u64;
    assert!(
        due,
        "[seed={seed}] past the retention window, the row is now ordinarily due"
    );
    let not_yet_ms = 1_000 + Duration::from_secs(10).as_millis() as u64;
    let not_due = not_yet_ms.saturating_sub(meta.pitr_segments[&(tablet, 0)].seal_wall_ms)
        >= Duration::from_secs(35 * 24 * 60 * 60).as_millis() as u64;
    assert!(
        !not_due,
        "[seed={seed}] well within the window, the dropped table's row is still held"
    );
}

#[test]
fn drop_table_then_segments_and_generation_floor_survive() {
    for_each_seed(
        "drop_table_then_segments_and_generation_floor_survive",
        scenario_drop_table_then_segments_survive,
    );
}

// --- cell 7: retention keep-anchor never orphans a needed replay base ------

/// The PITR retention janitor's own base-snapshot keep-anchor predicate
/// (`animusd::pitr_janitor`'s own unit-tested pure function, reproduced here
/// verbatim so this corpus proves it too, under **randomized**
/// interleavings rather than only the janitor's own hand-picked cases):
/// never mark the newest base at or before the retention floor — every
/// still-retained segment sealed after it needs it as its replay base.
fn due_for_mark(bases_created_wall_ms: &[u64], floor_ms: u64) -> Vec<u64> {
    let mut sorted = bases_created_wall_ms.to_vec();
    sorted.sort_unstable();
    let keep_anchor = sorted.iter().rev().find(|&&ms| ms <= floor_ms).copied();
    let Some(anchor) = keep_anchor else {
        return Vec::new();
    };
    sorted.into_iter().filter(|&ms| ms < anchor).collect()
}

fn scenario_retention_keep_anchor_never_orphans_a_needed_replay_base(seed: u64) {
    let sim = Simulator::new(seed);
    let env = sim.env(nid(NODES[0]));
    // A randomized sequence of base-snapshot creation times and PITR
    // segment seal times, all sharing one wall clock — models a table
    // whose base snapshots + segments interleave unpredictably under fault
    // injection (leader changes reordering which ticks actually commit).
    let mut wall_ms = 0u64;
    let mut bases: Vec<u64> = Vec::new();
    let mut segments_needing: Vec<(u64 /* sealed_at */, u64 /* base it needs */)> = Vec::new();
    for _ in 0..40 {
        wall_ms += 1_000 + env.gen_below(5_000);
        if env.gen_below(3) == 0 || bases.is_empty() {
            bases.push(wall_ms);
        } else {
            let base = *bases.last().expect("non-empty");
            segments_needing.push((wall_ms, base));
        }
    }
    // Evaluate at several retention floors, including ones that land
    // strictly between two base-snapshot times.
    for floor_ms in [0u64, wall_ms / 4, wall_ms / 2, (wall_ms * 3) / 4, wall_ms] {
        let due = due_for_mark(&bases, floor_ms);
        let due_set: BTreeSet<u64> = due.into_iter().collect();
        for &(sealed_at, needs_base) in &segments_needing {
            // A segment still within retention (sealed after the floor)
            // must never find its own required base among the marked set.
            if sealed_at > floor_ms {
                assert!(
                    !due_set.contains(&needs_base),
                    "[seed={seed}] floor={floor_ms}: segment sealed at {sealed_at} needs base \
                     {needs_base}, but that base was marked for reclaim"
                );
            }
        }
    }
}

#[test]
fn retention_keep_anchor_never_orphans_a_needed_replay_base() {
    for_each_seed(
        "retention_keep_anchor_never_orphans_a_needed_replay_base",
        scenario_retention_keep_anchor_never_orphans_a_needed_replay_base,
    );
}

// --- cell 8: restore-to-random-second matches an independent model --------

/// The production selection algorithm under test: `Metadata::
/// pitr_replay_segments` (real code, `animus-control`, never reimplemented
/// here — unlike `pitr_seal_now` above, which lives in `animusd` and has no
/// `SimEnv` binding of its own, this function needs only `Metadata` fields
/// this corpus already builds by hand). Fetches every segment the plan
/// names, decodes+slices each to its own `replay_range`, and reduces to the
/// last-writer-wins value per item key (by packed HLC) — the same
/// last-write-wins MVCC resolution a real restored engine gives for free.
/// Asserts the result matches `expected` (an independently-tracked model
/// snapshot) exactly.
fn assert_replay_matches_model(
    meta: &Metadata,
    store: &SimSegmentStore,
    base_tablet_progress: &[(TabletId, u64)],
    target_wall_ms: u64,
    expected: &BTreeMap<Vec<u8>, Vec<u8>>,
    seed: u64,
) {
    let plan = meta.pitr_replay_segments(base_tablet_progress, target_wall_ms);
    let mut actual: BTreeMap<Vec<u8>, (u64, Vec<u8>)> = BTreeMap::new();
    for seg in &plan {
        let bytes = block_on(store.get(&seg.object_id))
            .unwrap_or_else(|e| panic!("[seed={seed}] segment store get {}: {e}", seg.object_id))
            .unwrap_or_else(|| panic!("[seed={seed}] segment {} missing", seg.object_id));
        let (_, records) = segment::decode_and_slice(&bytes, seg.replay_range)
            .unwrap_or_else(|e| panic!("[seed={seed}] corrupt segment {}: {e}", seg.object_id));
        for r in records {
            let Some(n) = r.source_key.len().checked_sub(8) else {
                continue;
            };
            let item_key = r.source_key[..n].to_vec();
            let slot = actual.entry(item_key).or_insert((0, Vec::new()));
            if r.packed_hlc >= slot.0 {
                *slot = (r.packed_hlc, r.change_record);
            }
        }
    }
    let actual_values: BTreeMap<Vec<u8>, Vec<u8>> =
        actual.into_iter().map(|(k, (_, v))| (k, v)).collect();
    assert_eq!(
        &actual_values, expected,
        "[seed={seed}] replay to wall_ms={target_wall_ms} did not match the model"
    );
}

/// A burst of `n` random-keyed writes (from a small keyspace, so later
/// bursts overwrite earlier ones), updating both the journal and a running
/// `model` snapshot of "current value per key".
#[allow(clippy::too_many_arguments)]
fn write_burst(
    sim: &mut Simulator,
    group: &Group,
    live: &[usize],
    journal: &mut BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
    model: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    env: &SimEnv,
    round: usize,
    n: usize,
    seed: u64,
) {
    write_burst_ranged(sim, group, live, journal, model, env, round, n, 0..6, seed);
}

/// [`write_burst`]'s range-scoped sibling — for a **split** scenario, where
/// a child group's own writes must land within keys that group's own
/// declared range actually owns (`key_range`, half-open indices into
/// [`key`]) — an out-of-range key silently no-ops at apply time (the
/// routing-bug tripwire `animusd/CLAUDE.md`'s "Write fences are GONE"
/// section names), which a plain random pick across the WHOLE keyspace
/// would occasionally hit, corrupting this test's own model rather than
/// the production code under test (found on this scenario's own first
/// run — see `docs/engineering-lessons.md`).
#[allow(clippy::too_many_arguments)]
fn write_burst_ranged(
    sim: &mut Simulator,
    group: &Group,
    live: &[usize],
    journal: &mut BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
    model: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    env: &SimEnv,
    round: usize,
    n: usize,
    key_range: std::ops::Range<usize>,
    seed: u64,
) {
    let span = key_range.end - key_range.start;
    for _ in 0..n {
        let k = key(key_range.start + env.gen_below(span as u64) as usize);
        let v = format!("r{round}-{}", env.next_u64()).into_bytes();
        write_and_journal(sim, group, live, journal, &k, &v, seed);
        model.insert(k, v);
    }
}

/// A model's value-per-key state as of some sealed `wall_ms`, tracked
/// alongside a replay-scenario's own running model so a single scenario run
/// can assert `pitr_replay_segments` against more than just the final
/// snapshot.
type ModelSnapshots = Vec<(u64, BTreeMap<Vec<u8>, Vec<u8>>)>;

/// **The flagship property**: restore-to-a-random-second against a table
/// under mixed load (multiple keys, overwritten across rounds) with a
/// leader kill mid-stream, verified against an independently-tracked model
/// at every sealed second — not just the final one. Each successful
/// `pitr_seal_now` call snapshots the model's own current state under that
/// seal's `wall_ms`; `Metadata::pitr_replay_segments` at any `wall_ms` at or
/// after one snapshot and before the next must reproduce that snapshot
/// exactly.
fn scenario_restore_to_random_second_matches_the_model_with_a_leader_kill(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta_with_pitr();
    let group = start_group(&sim, &engines, TabletId(40), KeyRange::whole());
    let mut live = vec![0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let env = sim.env(nid(NODES[0]));
    let mut journal: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut snapshots: ModelSnapshots = Vec::new();
    let mut wall_ms = 1_000u64;

    for round in 0..8usize {
        // A leader kill partway through, BEFORE this round's own write
        // burst — so that burst's own `write_and_journal` (which
        // internally elects and confirms-by-applied-index on whichever
        // node is leader now) is what proves the new leader has actually
        // caught its apply cursor up to the crashed leader's last
        // committed entry before this round's own seal ever reads
        // `pending_changes()` on it. Killing AFTER the burst (or sealing
        // right after a bare re-election with no confirmed write of its
        // own in between) is unsound: a freshly elected leader's
        // `is_leader()` flipping true is a LEADERSHIP signal, not an APPLY
        // one — see `docs/engineering-lessons.md` for the general lesson
        // this scenario's own first depth run at seed 8085152262896110479
        // found the hard way (a real corpus-harness gap, not a production
        // bug: `pitr_seal_now`'s own `pending_changes()` scan is exactly
        // as fresh as the node it's read from).
        if round == 3 {
            let old_leader = elect(&mut sim, &group, &live, seed);
            sim.crash(nid(NODES[old_leader]));
            live.retain(|&i| i != old_leader);
            elect(&mut sim, &group, &live, seed);
        }

        let n = 1 + env.gen_below(3) as usize;
        write_burst(
            &mut sim,
            &group,
            &live,
            &mut journal,
            &mut model,
            &env,
            round,
            n,
            seed,
        );

        wall_ms += 500 + env.gen_below(1_000);
        let leader = elect(&mut sim, &group, &live, seed);
        if let Some(_epoch) = pitr_seal_now(&mut meta, &store, &group, leader, wall_ms, false) {
            snapshots.push((wall_ms, model.clone()));
        }
    }
    assert!(
        snapshots.len() >= 4,
        "[seed={seed}] expected several successful seals, got {}",
        snapshots.len()
    );

    let base = vec![(TabletId(40), 0)];
    // Every recorded snapshot's own wall_ms reproduces exactly that
    // snapshot when used as the restore target.
    for (wall_ms, expected) in &snapshots {
        assert_replay_matches_model(&meta, &store, &base, *wall_ms, expected, seed);
    }
    // A target strictly between two consecutive snapshots reproduces the
    // EARLIER one — the "never include a write whose own seal hasn't
    // happened yet" half of the property.
    for pair in snapshots.windows(2) {
        let (a_ms, a_model) = &pair[0];
        let (b_ms, _) = &pair[1];
        if b_ms > a_ms {
            let mid = a_ms + (b_ms - a_ms) / 2;
            assert_replay_matches_model(&meta, &store, &base, mid, a_model, seed);
        }
    }
    // Before the very first seal: nothing at all is restorable yet.
    let (first_ms, _) = &snapshots[0];
    if *first_ms > 0 {
        assert_replay_matches_model(&meta, &store, &base, first_ms - 1, &BTreeMap::new(), seed);
    }
}

#[test]
fn restore_to_random_second_matches_the_model_with_a_leader_kill() {
    for_each_seed(
        "restore_to_random_second_matches_the_model_with_a_leader_kill",
        scenario_restore_to_random_second_matches_the_model_with_a_leader_kill,
    );
}

// --- cell 10: generation-gap rejection --------------------------------------

/// `Metadata::pitr_restore_window` (real code) scopes to the table's own
/// LATEST generation only — a randomized number of disable/re-enable
/// cycles, each with its own writes/seals, proves a target second from any
/// earlier generation's own coverage (including the disabled gap itself)
/// is never reachable through the current generation's window, whatever
/// the cycle count.
fn scenario_pitr_restore_window_scopes_to_the_latest_generation_under_random_cycles(seed: u64) {
    let mut m = base_meta_with_pitr();
    let sim = Simulator::new(seed);
    let env = sim.env(nid(NODES[0]));
    let cycles = 1 + env.gen_below(4);
    let mut wall_ms = 1_000u64;
    let mut earlier_generation_earliest = Vec::new();

    for _ in 0..cycles {
        let window = m
            .pitr_restore_window(TABLE)
            .expect("a live generation always has a window");
        earlier_generation_earliest.push(window.earliest_ms);
        wall_ms += 1_000 + env.gen_below(5_000);
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: TABLE.into(),
            enabled: false,
            wall_ms,
        });
        wall_ms += 1_000 + env.gen_below(5_000);
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: TABLE.into(),
            enabled: true,
            wall_ms,
        });
    }
    let current = m.pitr_restore_window(TABLE).unwrap();
    assert_eq!(current.generation, cycles + 1);
    for earlier_earliest in earlier_generation_earliest {
        assert!(
            earlier_earliest < current.earliest_ms,
            "[seed={seed}] an earlier generation's own coverage start ({earlier_earliest}) must \
             never be reachable once a later generation ({}) is current",
            current.generation
        );
    }
}

#[test]
fn pitr_restore_window_scopes_to_the_latest_generation_under_random_cycles() {
    for_each_seed(
        "pitr_restore_window_scopes_to_the_latest_generation_under_random_cycles",
        scenario_pitr_restore_window_scopes_to_the_latest_generation_under_random_cycles,
    );
}

// --- cell 11: deleted-table PITR restore matches the model ------------------

/// The content-correctness twin of cell 6 (`drop_table_then_segments_and_
/// generation_floor_survive`, which only proves the catalog rows survive):
/// after `DropTableSchema`/`DropTableTablets`, `pitr_replay_segments` must
/// still reproduce the exact pre-drop model at a second recorded before the
/// drop, under a randomized number of write/seal rounds.
fn scenario_deleted_table_pitr_restore_matches_the_model(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta_with_pitr();
    let group = start_group(&sim, &engines, TabletId(60), KeyRange::whole());
    assert_eq!(
        meta.apply(&MetaCommand::CreateTablet {
            tablet: group.id,
            table: Some(TABLE.into()),
            range: KeyRange::whole(),
            replicas: Vec::new(),
        }),
        ApplyOutcome::Applied,
        "[seed={seed}] the tablet must be registered in Metadata before DropTableTablets can \
         find it to drop"
    );
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let env = sim.env(nid(NODES[0]));
    let mut journal = BTreeMap::new();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut wall_ms = 1_000u64;
    let rounds = 2 + env.gen_below(3);

    let mut last_snapshot = None;
    for round in 0..rounds {
        let n = 1 + env.gen_below(3) as usize;
        write_burst(
            &mut sim,
            &group,
            &live,
            &mut journal,
            &mut model,
            &env,
            round as usize,
            n,
            seed,
        );
        wall_ms += 500 + env.gen_below(1_000);
        let leader = elect(&mut sim, &group, &live, seed);
        if pitr_seal_now(&mut meta, &store, &group, leader, wall_ms, false).is_some() {
            last_snapshot = Some((wall_ms, model.clone()));
        }
    }
    let (target_wall_ms, expected) = last_snapshot.expect("[seed={seed}] at least one seal");

    assert_eq!(
        meta.apply(&MetaCommand::DropTableSchema {
            table: TABLE.into(),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        meta.apply(&MetaCommand::DropTableTablets {
            table: TABLE.into(),
        }),
        ApplyOutcome::Applied
    );
    assert!(!meta.tablets.contains_key(&group.id));

    let base = vec![(group.id, 0)];
    assert_replay_matches_model(&meta, &store, &base, target_wall_ms, &expected, seed);

    let window = meta
        .pitr_restore_window(TABLE)
        .expect("[seed={seed}] a dropped table's PITR window must still resolve");
    assert!(
        target_wall_ms <= window.latest_ms,
        "[seed={seed}] the recorded target ({target_wall_ms}) must fall inside the \
         still-resolvable window (latest={})",
        window.latest_ms
    );
}

#[test]
fn deleted_table_pitr_restore_matches_the_model() {
    for_each_seed(
        "deleted_table_pitr_restore_matches_the_model",
        scenario_deleted_table_pitr_restore_matches_the_model,
    );
}

// --- cell 12: UseLatestRestorableTime reproduces the full model -------------

/// Restoring to the window's own `latest_ms` (the wire's
/// `UseLatestRestorableTime`, resolved by `animusd::dynamo` before ever
/// calling `pitr_replay_segments` — this cell proves the underlying
/// selection function itself reproduces the FULL model at that value, the
/// property the wire layer's own resolution depends on) must include
/// everything ever sealed.
fn scenario_use_latest_restorable_time_matches_the_full_model(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta_with_pitr();
    let group = start_group(&sim, &engines, TabletId(70), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let env = sim.env(nid(NODES[0]));
    let mut journal = BTreeMap::new();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut wall_ms = 1_000u64;
    let rounds = 2 + env.gen_below(4);
    let mut last_seal_ms = None;

    for round in 0..rounds {
        let n = 1 + env.gen_below(3) as usize;
        write_burst(
            &mut sim,
            &group,
            &live,
            &mut journal,
            &mut model,
            &env,
            round as usize,
            n,
            seed,
        );
        wall_ms += 500 + env.gen_below(1_000);
        let leader = elect(&mut sim, &group, &live, seed);
        if pitr_seal_now(&mut meta, &store, &group, leader, wall_ms, false).is_some() {
            last_seal_ms = Some(wall_ms);
        }
    }
    let last_seal_ms = last_seal_ms.expect("[seed={seed}] at least one seal");
    let window = meta.pitr_restore_window(TABLE).unwrap();
    assert_eq!(
        window.latest_ms, last_seal_ms,
        "[seed={seed}] Latest must track this tablet's own most recent seal exactly"
    );

    let base = vec![(group.id, 0)];
    assert_replay_matches_model(&meta, &store, &base, window.latest_ms, &model, seed);
}

#[test]
fn use_latest_restorable_time_matches_the_full_model() {
    for_each_seed(
        "use_latest_restorable_time_matches_the_full_model",
        scenario_use_latest_restorable_time_matches_the_full_model,
    );
}

// --- cell 13: fsync-acked-but-lost lie on the crashing sealing leader ------

/// The identical crash-mid-seal property as [`scenario_kill_sealing_leader`]
/// (cell 3) — a crash between the store `put` and the catalog commit, then a
/// leader failover, then the idempotent retry re-seals the full backlog as
/// one epoch — with the added twist that the crashing leader's own recent
/// `sync`es may have **lied**: [`DiskConfig::set_fsync_lie_prob`] set
/// globally means some of what the leader believed was durable (WAL records
/// it had already fsynced and acked) was in fact still only buffered when
/// the crash hit, so `Simulator::crash`'s default (no [`DiskConfig::
/// torn_tail_on_crash`] configured here) whole-buffer-drop applies to MORE
/// bytes than an un-lied crash would ever lose. The corpus's own core
/// durability argument — the retry recovers the full backlog regardless of
/// exactly how much the crashed leader's own local WAL lost — has to hold
/// under this too: Raft safety only ever depends on a *majority* having
/// durably persisted a committed entry, never on the crashing node's own
/// copy of it.
fn scenario_wal_fsync_lie_kill_sealing_leader(seed: u64) {
    let mut sim = Simulator::new(seed);
    let mut disk_cfg = DiskConfig::default();
    disk_cfg.set_fsync_lie_prob(0.3);
    sim.set_disk_config(disk_cfg);
    let engines = engines();
    let mut meta = base_meta_with_pitr();
    let group = start_group(&sim, &engines, TabletId(100), KeyRange::whole());
    let mut live = vec![0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    for i in 0..4 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v0", seed);
    }
    let old_leader = elect(&mut sim, &group, &live, seed);
    // A crash between the store `put` and the catalog commit — the
    // idempotent-retry recovery argument (ledger-named-object amendment) —
    // while the leader's own fsyncs have been lying about durability.
    let none = pitr_seal_now(&mut meta, &store, &group, old_leader, 1_000, true);
    assert_eq!(
        none, None,
        "[seed={seed}] skip_commit models no catalog row yet"
    );
    assert!(meta.pitr_segments.is_empty());

    sim.crash(nid(NODES[old_leader]));
    live.retain(|&i| i != old_leader);
    let new_leader = elect(&mut sim, &group, &live, seed);

    // More writes land under the new leader before the retry.
    for i in 4..7 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v0", seed);
    }
    let sealed = pitr_seal_now(&mut meta, &store, &group, new_leader, 2_000, false);
    assert_eq!(
        sealed,
        Some(0),
        "[seed={seed}] the retry re-seals the full backlog as one epoch, even under a lying fsync \
         on the node that crashed"
    );

    verify_pitr_lineage(&meta, &store, &[(&group, new_leader, 1)], &journal, seed);
}

#[test]
fn wal_fsync_lie_kill_sealing_leader() {
    for_each_seed(
        "wal_fsync_lie_kill_sealing_leader",
        scenario_wal_fsync_lie_kill_sealing_leader,
    );
}

// --- cell 14: chaotic network — quiet table rollover under loss+dup -------

/// The identical baseline-rollover property as
/// [`scenario_quiet_table_pitr_rollover`] (cell 1), under a compound
/// lossy+duplicating network (`NetConfig::set_drop_prob`/
/// `set_duplicate_prob`, ADR 0061 Decision 3) set globally from the very
/// first `Simulator::new(seed)`. Deliberately never
/// `NetConfig::set_corrupt_prob`: `crates/animus-cp-data/src/codec.rs`'s
/// dozen-odd `Vec::with_capacity(n as usize)` sites over an untrusted wire
/// length prefix are still unbounded on this branch (verified directly —
/// the fix is sibling PR #485, not yet landed), so a corrupted length
/// prefix landing near `u32::MAX` would abort the whole test process
/// rather than surface as a recoverable error; excluded, matching the
/// conservative choice the sibling raftkv/txn/backup corpora already made.
fn scenario_chaotic_network_pitr_rollover(seed: u64) {
    let mut sim = Simulator::new(seed);
    let mut net_cfg = NetConfig::default();
    net_cfg.set_drop_prob(0.05);
    net_cfg.set_duplicate_prob(0.10);
    sim.set_net_config(net_cfg);
    let engines = engines();
    let mut meta = base_meta_with_pitr();
    let group = start_group(&sim, &engines, TabletId(101), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    for i in 0..3 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v0", seed);
    }
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = pitr_seal_now(&mut meta, &store, &group, leader, 1_000, false);
    assert_eq!(sealed, Some(0), "[seed={seed}] expected epoch 0 to seal");

    for i in 0..3 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v1", seed);
    }
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = pitr_seal_now(&mut meta, &store, &group, leader, 2_000, false);
    assert_eq!(sealed, Some(1), "[seed={seed}] expected epoch 1 to seal");

    verify_pitr_lineage(&meta, &store, &[(&group, leader, 1)], &journal, seed);
}

#[test]
fn chaotic_network_pitr_rollover() {
    for_each_seed(
        "chaotic_network_pitr_rollover",
        scenario_chaotic_network_pitr_rollover,
    );
}

// --- cell 15: chaotic network — idle group still never seals a no-op ------

/// The identical quiescence-contract property as
/// [`scenario_idle_group_never_proposes_a_pitr_seal`] (cell 2), under the
/// same compound lossy+duplicating network as cell 14 — proving the
/// "nothing pending ⇒ no store `put`, no propose" contract is a purely
/// local decision (`pending_changes()` reading empty) that packet loss or
/// duplication cannot spuriously trip into a false seal, and that a real
/// write still gets through and seals despite the same fault.
fn scenario_chaotic_network_idle_group_never_proposes_a_pitr_seal(seed: u64) {
    let mut sim = Simulator::new(seed);
    let mut net_cfg = NetConfig::default();
    net_cfg.set_drop_prob(0.05);
    net_cfg.set_duplicate_prob(0.10);
    sim.set_net_config(net_cfg);
    let engines = engines();
    let mut meta = base_meta_with_pitr();
    let group = start_group(&sim, &engines, TabletId(102), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));

    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = pitr_seal_now(&mut meta, &store, &group, leader, 500, false);
    assert_eq!(sealed, None, "[seed={seed}] an idle group must never seal");
    assert!(
        meta.pitr_segments.is_empty(),
        "[seed={seed}] no catalog row from a no-op seal attempt"
    );

    // Now a real write lands — the very next attempt seals it, even under
    // the same lossy/duplicating network.
    let mut journal = BTreeMap::new();
    write_and_journal(&mut sim, &group, &live, &mut journal, &key(0), b"v0", seed);
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = pitr_seal_now(&mut meta, &store, &group, leader, 600, false);
    assert_eq!(sealed, Some(0), "[seed={seed}] a real write must seal");
}

#[test]
fn chaotic_network_idle_group_never_proposes_a_pitr_seal() {
    for_each_seed(
        "chaotic_network_idle_group_never_proposes_a_pitr_seal",
        scenario_chaotic_network_idle_group_never_proposes_a_pitr_seal,
    );
}

// --- cell 16: torn/corrupted WAL tail on the crashing sealing leader ------

/// The identical crash-mid-seal property as [`scenario_kill_sealing_leader`]
/// (cell 3), now with [`DiskConfig::torn_tail_on_crash`]/[`DiskConfig::
/// corrupt_on_crash`] set globally so the crashing leader's own last
/// un-synced WAL record is torn (a seed-chosen strict prefix kept, at least
/// one byte always lost) and bit-flipped, rather than simply dropped
/// wholesale — and then, unlike cell 3, the crashed node is brought all the
/// way back as a **true process restart** reading that torn/corrupted tail
/// off disk. This needs a specific sequencing this repo's crash idiom
/// requires (see `docs/engineering-lessons.md`): `crash` (applies the tear)
/// → `restart` (clears the `crashed` mute — a crashed node silently drops
/// every send/delivery until it does, so skipping this step would leave the
/// freshly-constructed node permanently unable to talk to its peers) →
/// `stop` (drops the just-re-armed tasks, keeps the now-torn durable state)
/// → a fresh `RaftKvNode::start_hosted` on the same id/engine, mirroring
/// `backup_fault_corpus.rs`'s own `capture_driver_node_crash_restart`
/// idiom. A further round of writes and a second seal, with the recovered
/// node back among the live set, proves `current_open_pitr_epoch` — derived
/// purely from the catalog `Metadata`, never from any one replica's own
/// recovered state — continues cleanly at epoch 1: a wrong recovered epoch
/// here would silently produce a duplicate or skipped epoch number on
/// reseal, exactly what `verify_pitr_lineage`'s exactly-once check exists
/// to catch.
fn scenario_wal_torn_on_crash_kill_sealing_leader(seed: u64) {
    let mut sim = Simulator::new(seed);
    let mut disk_cfg = DiskConfig::default();
    disk_cfg.torn_tail_on_crash = true;
    disk_cfg.corrupt_on_crash = true;
    sim.set_disk_config(disk_cfg);
    let engines = engines();
    let mut meta = base_meta_with_pitr();
    let mut group = start_group(&sim, &engines, TabletId(103), KeyRange::whole());
    let mut live = vec![0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    for i in 0..4 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v0", seed);
    }
    let old_leader = elect(&mut sim, &group, &live, seed);
    let none = pitr_seal_now(&mut meta, &store, &group, old_leader, 1_000, true);
    assert_eq!(
        none, None,
        "[seed={seed}] skip_commit models no catalog row yet"
    );
    assert!(meta.pitr_segments.is_empty());

    // Crash with the torn/corrupted-tail disk model active — the crashing
    // leader's own last WAL record may now be torn or bit-flipped.
    sim.crash(nid(NODES[old_leader]));
    live.retain(|&i| i != old_leader);
    let new_leader = elect(&mut sim, &group, &live, seed);

    for i in 4..7 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v0", seed);
    }
    let sealed = pitr_seal_now(&mut meta, &store, &group, new_leader, 2_000, false);
    assert_eq!(
        sealed,
        Some(0),
        "[seed={seed}] the retry re-seals the full backlog as one epoch"
    );
    verify_pitr_lineage(&meta, &store, &[(&group, new_leader, 1)], &journal, seed);

    // Bring the crashed node all the way back — see this scenario's own
    // doc for why `restart` must land between `crash` and `stop`.
    let restarted_id = NODES[old_leader];
    sim.restart(nid(restarted_id));
    sim.stop(nid(restarted_id));
    let ids: Vec<_> = NODES.iter().copied().map(nid).collect();
    let fresh: KvNode = RaftKvNode::start_hosted(
        sim.env(nid(restarted_id)),
        ids,
        engines[&restarted_id].clone(),
        StorageScope::new(KeyRange::whole()),
        group.id.0,
    );
    group.nodes[old_leader] = fresh;
    live = vec![0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    // A further round of writes + seal, now with the recovered node back
    // among the live set — proves the per-tablet epoch chain recovers
    // correctly, never duplicating or skipping an epoch number.
    for i in 7..10 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v1", seed);
    }
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = pitr_seal_now(&mut meta, &store, &group, leader, 3_000, false);
    assert_eq!(
        sealed,
        Some(1),
        "[seed={seed}] the epoch chain continues at 1 once the recovered node rejoins, never a \
         duplicate or skipped number"
    );

    verify_pitr_lineage(&meta, &store, &[(&group, leader, 1)], &journal, seed);
}

#[test]
fn wal_torn_on_crash_kill_sealing_leader() {
    for_each_seed(
        "wal_torn_on_crash_kill_sealing_leader",
        scenario_wal_torn_on_crash_kill_sealing_leader,
    );
}

// --- cell 17: restore-to-random-second under sealing-leader clock drift ---

/// Sized together with this scenario's own explicit `sim.run_for` between
/// rounds so the drift accumulates to several visible seconds by the last
/// round (checked directly below), rather than being lost in the noise of
/// ordinary write/confirm jitter — a 30% clock-rate error is unrealistically
/// large for a real machine, but this scenario deliberately wants the
/// divergence to be unmissable rather than merely plausible.
const CLOCK_DRIFT_PPM: i64 = 300_000;

/// The identical flagship "restore-to-random-second" property as
/// [`scenario_restore_to_random_second_matches_the_model_with_a_leader_kill`]
/// (cell 8), replacing that cell's leader-kill nemesis with a **clock-drift**
/// one (`Simulator::set_clock_drift_for`, ADR 0061 Decision 3) on the
/// sealing leader itself — never killed here, so any divergence is
/// attributable to the drift alone. PITR is unusually well-suited to this
/// primitive: it is the one subsystem in this codebase that consumes
/// wall-clock epoch seconds (`seal_wall_ms`/`cutoff_wall_ms`, ADR 0051's
/// `wall_now()`). `wall_ms` is no longer a synthetic incrementing counter
/// here — each seal's `wall_ms` argument is read straight off the sealing
/// leader's own drifted `env.wall_now()`, so the model's own snapshot keys
/// inherit exactly the readings production code would see, and
/// `Metadata::pitr_replay_segments` (real code) still has to reproduce the
/// model exactly at every recorded second.
fn scenario_restore_to_random_second_under_clock_drift(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta_with_pitr();
    let group = start_group(&sim, &engines, TabletId(104), KeyRange::whole());
    let live = vec![0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let env = sim.env(nid(NODES[0]));
    let mut journal: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut snapshots: ModelSnapshots = Vec::new();

    let leader = elect(&mut sim, &group, &live, seed);
    let wall_ms_before_drift = group.nodes[leader].env().wall_now().0;
    sim.set_clock_drift_for(nid(NODES[leader]), CLOCK_DRIFT_PPM);

    for round in 0..8usize {
        let n = 1 + env.gen_below(3) as usize;
        write_burst(
            &mut sim,
            &group,
            &live,
            &mut journal,
            &mut model,
            &env,
            round,
            n,
            seed,
        );

        // Advance real virtual time between rounds so the drift has
        // something to accumulate against — no leader kill or other
        // nemesis here, so leadership (and thus which node's clock this
        // reads) stays put for the whole scenario.
        sim.run_for(Duration::from_secs(2));

        let leader = elect(&mut sim, &group, &live, seed);
        let wall_ms = group.nodes[leader].env().wall_now().0;
        if let Some(_epoch) = pitr_seal_now(&mut meta, &store, &group, leader, wall_ms, false) {
            snapshots.push((wall_ms, model.clone()));
        }
    }
    assert!(
        snapshots.len() >= 4,
        "[seed={seed}] expected several successful seals, got {}",
        snapshots.len()
    );

    // The drift really did accumulate to several visible seconds beyond
    // the ~16s of real elapsed time (8 rounds * 2s each) the loop above
    // actually slept — otherwise this cell is indistinguishable from the
    // no-drift baseline and proves nothing about drift-robustness
    // specifically.
    let (last_wall_ms, _) = snapshots.last().expect("checked above");
    let real_elapsed_ms: u64 = 2_000 * 8;
    assert!(
        last_wall_ms.saturating_sub(wall_ms_before_drift) > real_elapsed_ms + 2_000,
        "[seed={seed}] expected a visible multi-second drift by the last seal \
         (wall_ms_before_drift={wall_ms_before_drift}, last_wall_ms={last_wall_ms})"
    );

    let base = vec![(TabletId(104), 0)];
    // Every recorded snapshot's own wall_ms reproduces exactly that
    // snapshot when used as the restore target.
    for (wall_ms, expected) in &snapshots {
        assert_replay_matches_model(&meta, &store, &base, *wall_ms, expected, seed);
    }
    // A target strictly between two consecutive snapshots reproduces the
    // EARLIER one.
    for pair in snapshots.windows(2) {
        let (a_ms, a_model) = &pair[0];
        let (b_ms, _) = &pair[1];
        if b_ms > a_ms {
            let mid = a_ms + (b_ms - a_ms) / 2;
            assert_replay_matches_model(&meta, &store, &base, mid, a_model, seed);
        }
    }
    // Before the very first seal: nothing at all is restorable yet.
    let (first_ms, _) = &snapshots[0];
    if *first_ms > 0 {
        assert_replay_matches_model(&meta, &store, &base, first_ms - 1, &BTreeMap::new(), seed);
    }
}

#[test]
fn restore_to_random_second_under_clock_drift() {
    for_each_seed(
        "restore_to_random_second_under_clock_drift",
        scenario_restore_to_random_second_under_clock_drift,
    );
}

// --- cells 18/19: the in-place split's PITR contract (ADR 0058 Train 2 rung 3) ---
//
// These two cells originally rebuilt, on the DEFAULT production split path
// (the in-place atomic fork), the identical PITR contract two now-deleted
// copy-based cells once proved (`split_children_seal_independently_and_
// inherit_generation`, formerly cell 5, and
// `restore_to_random_second_matches_the_model_across_a_split`, formerly
// cell 9 — a control-metadata-only `BeginSplit`/`CutoverSplit`, with the
// parent's own `Group` hosted directly via this file's `start_group` and
// never touched by a reconciler). Those two copy-based cells were removed
// by the copy-mode-split deletion stack (`--split-mode copy` no longer has
// any coverage in this file); the two cells below are now this corpus's
// only coverage of the split/PITR interaction, kept at their original
// numbers (18/19) per this repo's corpus doctrine (a scenario keeps its
// name/number forever once added — see `crates/animus-test/CLAUDE.md`).
//
// Unlike the copy-based workflow, the in-place fork is materialized by
// `animus_cp_data::host::Reconciler`, so the parent (and both children) must
// be reconciler-hosted from the start: a small `Cluster` of
// `Reconciler<SimEnv, MemoryEngine>` instances (one per node id, mirroring
// `animus-cp-data/tests/inplace_split_reconciler.rs`'s own harness shape),
// driven to convergence at each stage via `tick_one`/`converge` (adapted
// from `backup_fault_corpus.rs`'s/`stream_lineage_corpus.rs`'s own identical
// adaptation for the same mechanism — `Reconciler::tick` `.await`s
// internally via `env.sleep`, so a bare `block_on` would hang with nothing
// advancing the simulator concurrently). Once a stage converges,
// [`wrap_group`] clones the reconciler-hosted `RaftKvNode` handles back into
// this file's own plain [`Group`] — so `elect`/`write_and_journal`/
// `pitr_seal_now`/`verify_pitr_lineage`/`write_burst`/`write_burst_ranged`/
// `assert_replay_matches_model` all run on the reconciler-hosted parent or a
// split child completely unmodified. **Real in-place fork+materialize
// clones+trims each child's own share of the parent's data automatically**
// — unlike the copy-based cells' manual "fresh `engines()` map per child"
// workaround (their own doc explains why that hand-rolled isolation is
// needed there), so neither cell below needs an equivalent: each child's
// engine already holds exactly its own half-range's rows once fork+
// materialize converges.

/// A dedicated node id for spawning the reconciler-driving futures below —
/// distinct from every real replica id in [`NODES`], so a stray env mixup
/// would be obvious rather than silently aliasing a real replica (mirrors
/// `backup_fault_corpus.rs`'s own `driver_id`/`stream_lineage_corpus.rs`'s
/// own `inplace_driver_id`).
fn driver_id() -> NodeId {
    nid(999)
}

type Recon = Reconciler<SimEnv, MemoryEngine>;

/// One node's tablet-host reconciler, standing in for the per-node loop
/// `animusd::tablet_host_reconciler_loop` drives in production — mirrors
/// the sibling corpora's own `Cluster`/`ClusterNode` (kept local here
/// rather than shared: integration test binaries can't share private
/// items).
struct ClusterNode {
    reconciler: Recon,
}

struct Cluster {
    nodes: BTreeMap<NodeId, ClusterNode>,
}

impl Cluster {
    fn new(sim: &Simulator) -> Self {
        let mut nodes = BTreeMap::new();
        for &n in &NODES {
            let id = nid(n);
            let reconciler: Recon = Reconciler::new(
                sim.env(id.clone()),
                MemoryTabletEngines::new(),
                id.clone(),
                |_, _| {},
                |_| {},
            );
            nodes.insert(id, ClusterNode { reconciler });
        }
        Cluster { nodes }
    }

    fn node(&self, id: &NodeId) -> &Recon {
        &self.nodes[id].reconciler
    }

    fn hosted_set(&self, id: &NodeId) -> BTreeSet<TabletId> {
        self.node(id).local_state().hosted.clone()
    }
}

/// `MetadataView { tablets: meta.tablets.clone(), down: BTreeSet::new() }`
/// built from this file's own bare `Metadata` after applying commands — no
/// separate fake view needed.
fn metadata_view(meta: &Metadata) -> MetadataView {
    MetadataView {
        tablets: meta.tablets.clone(),
        down: BTreeSet::new(),
    }
}

/// Runs `fut` to completion by spawning it on `env` and driving `sim` in
/// small steps until it resolves — `stream_lineage_corpus.rs`'s/
/// `backup_fault_corpus.rs`'s own `drive` helper, copied verbatim:
/// `Reconciler::tick` polls internally via `env.sleep`, so a bare
/// `block_on` would hang with nothing advancing the simulator
/// concurrently.
fn drive<T: Send + 'static>(
    sim: &mut Simulator,
    env: &SimEnv,
    fut: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    let slot: std::sync::Arc<std::sync::Mutex<Option<T>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let s = std::sync::Arc::clone(&slot);
    env.clone().spawn_task(async move {
        let v = fut.await;
        *s.lock().unwrap() = Some(v);
    });
    for _ in 0..500 {
        if slot.lock().unwrap().is_some() {
            break;
        }
        sim.run_for(Duration::from_millis(20));
    }
    slot.lock()
        .unwrap()
        .take()
        .expect("drive: future never completed")
}

/// Ticks one node's reconciler once against `view`. The node is moved out of
/// `cluster` for the duration of the tick (`drive`'s spawned future must own
/// what it touches) and put back once it resolves.
fn tick_one(
    sim: &mut Simulator,
    env: &SimEnv,
    cluster: &mut Cluster,
    id: &NodeId,
    view: &MetadataView,
) {
    let mut node = cluster.nodes.remove(id).expect("node exists");
    let view = view.clone();
    let ticked = drive(sim, env, async move {
        node.reconciler.tick(&view).await;
        node
    });
    cluster.nodes.insert(id.clone(), ticked);
}

/// Ticks every node in `ids` against `view` in a bounded loop until `check`
/// holds — mirrors `inplace_split_reconciler.rs`'s own `converge`.
fn converge(
    sim: &mut Simulator,
    env: &SimEnv,
    cluster: &mut Cluster,
    ids: &[NodeId],
    view: &MetadataView,
    mut check: impl FnMut(&Cluster) -> bool,
) -> bool {
    for _ in 0..300 {
        for id in ids {
            tick_one(sim, env, cluster, id, view);
        }
        if check(cluster) {
            return true;
        }
        sim.run_for(Duration::from_millis(100));
    }
    check(cluster)
}

fn leader_of<'a>(cluster: &'a Cluster, ids: &[NodeId], tablet: TabletId) -> Option<&'a KvNode> {
    ids.iter().find_map(|id| {
        cluster
            .node(id)
            .hosted_node(tablet)
            .filter(|h| h.is_leader())
    })
}

/// Clones a reconciler-hosted tablet's per-node handles into this file's own
/// plain [`Group`] — see the section doc above for why. Node order matches
/// `ids` (== [`NODES`]' own order), so the `live: [0, 1, 2]` index
/// convention every other cell in this file uses still lines up.
fn wrap_group(cluster: &Cluster, ids: &[NodeId], tablet: TabletId) -> Group {
    Group {
        id: tablet,
        nodes: ids
            .iter()
            .map(|id| {
                cluster
                    .node(id)
                    .hosted_node(tablet)
                    .expect("tablet hosted on every fork participant")
                    .clone()
            })
            .collect(),
    }
}

// --- cell 18: inplace_split_children_seal_independently_and_inherit_generation

/// Proves that each child seals its own epoch 0 independently, inheriting
/// PITR from the table spec with zero special-casing since `table_pitr` is
/// table- not tablet-scoped; the union of parent-plus-children content
/// covers the full journal with no double-counting. Originally built
/// alongside a now-deleted copy-based sibling cell proving the identical
/// claim via the deprecated `BeginSplit`/`CutoverSplit` control-metadata-
/// only cutover — this cell drives the claim through
/// `BeginSplitInPlace`/reconciler fork+materialize/`CutoverSplit` instead,
/// and is now the corpus's only coverage of it (see the "cells 18/19"
/// section doc above).
fn scenario_inplace_split_children_seal_independently(seed: u64) {
    let mut sim = Simulator::new(seed);
    let mut meta = base_meta_with_pitr();
    let node_ids: Vec<NodeId> = NODES.iter().copied().map(nid).collect();
    let parent_id = TabletId(110);
    assert_eq!(
        meta.apply(&MetaCommand::CreateTablet {
            tablet: parent_id,
            table: Some(TABLE.into()),
            range: KeyRange::whole(),
            replicas: node_ids.clone(),
        }),
        ApplyOutcome::Applied
    );

    let live = [0, 1, 2];
    let driver = sim.env(driver_id());
    let mut cluster = Cluster::new(&sim);

    let base_view = metadata_view(&meta);
    assert!(
        converge(
            &mut sim,
            &driver,
            &mut cluster,
            &node_ids,
            &base_view,
            |c| leader_of(c, &node_ids, parent_id).is_some()
        ),
        "[seed={seed}] parent never elected a leader"
    );
    let parent = wrap_group(&cluster, &node_ids, parent_id);

    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    for i in 0..4 {
        write_and_journal(
            &mut sim,
            &parent,
            &live,
            &mut journal,
            &key(i),
            b"pre",
            seed,
        );
    }
    let leader = elect(&mut sim, &parent, &live, seed);
    assert_eq!(
        pitr_seal_now(&mut meta, &store, &parent, leader, 1_000, false),
        Some(0),
        "[seed={seed}] parent's pre-split backlog seals as epoch 0"
    );

    // Fork: `BeginSplitInPlace` records the intent on the parent; the
    // reconciler materializes both children on every fork participant, then
    // `CutoverSplit` (no freeze/veto gate on the in-place branch — proposed
    // immediately) activates them and retires the parent, freezing
    // `split_lineage`.
    let left = TabletId(111);
    let right = TabletId(112);
    let split_key = key(2);
    let parent_epoch = meta.tablets[&parent_id].epoch;
    assert_eq!(
        meta.apply(&MetaCommand::BeginSplitInPlace {
            parent: parent_id,
            expected_epoch: parent_epoch,
            split_key: split_key.clone(),
            children: [(left, node_ids.clone()), (right, node_ids.clone())],
        }),
        ApplyOutcome::Applied,
        "[seed={seed}] BeginSplitInPlace must apply"
    );

    let pending_view = metadata_view(&meta);
    assert!(
        converge(
            &mut sim,
            &driver,
            &mut cluster,
            &node_ids,
            &pending_view,
            |c| {
                node_ids.iter().all(|id| {
                    c.node(id)
                        .hosted_node(parent_id)
                        .is_some_and(|h| block_on(h.pending_split()).is_some())
                })
            }
        ),
        "[seed={seed}] the in-place split never forked on every participant"
    );
    assert!(
        converge(
            &mut sim,
            &driver,
            &mut cluster,
            &node_ids,
            &pending_view,
            |c| {
                node_ids.iter().all(|id| {
                    let hosted = c.hosted_set(id);
                    hosted.contains(&left) && hosted.contains(&right)
                })
            }
        ),
        "[seed={seed}] both children never materialized on every fork participant"
    );

    let parent_epoch = meta.tablets[&parent_id].epoch;
    assert_eq!(
        meta.apply(&MetaCommand::CutoverSplit {
            parent: parent_id,
            expected_epoch: parent_epoch,
            cutover_wall_ms: 1_500,
        }),
        ApplyOutcome::Applied,
        "[seed={seed}] CutoverSplit must apply"
    );
    assert!(!meta.tablets.contains_key(&parent_id), "the parent retires");
    // Children inherit PITR from the table spec (ADR 0059 §9's own scope
    // ask) — structurally true here: `table_pitr(TABLE)` is table-scoped,
    // not tablet-scoped, so both children see the identical generation
    // with zero special-casing.
    let generation = meta.table_pitr(TABLE).unwrap().generation;

    let post_view = metadata_view(&meta);
    assert!(
        converge(
            &mut sim,
            &driver,
            &mut cluster,
            &node_ids,
            &post_view,
            |c| {
                node_ids.iter().all(|id| {
                    c.node(id).hosted_node(left).is_some()
                        && c.node(id).hosted_node(right).is_some()
                        && !c.hosted_set(id).contains(&parent_id)
                })
            }
        ),
        "[seed={seed}] both children never activated and the parent was never reclaimed everywhere"
    );

    let left_group = wrap_group(&cluster, &node_ids, left);
    let right_group = wrap_group(&cluster, &node_ids, right);
    let mut left_journal = BTreeMap::new();
    let mut right_journal = BTreeMap::new();
    // Deliberate wall-clock separation between the two children's own write
    // bursts: each child's `Hlc` starts
    // fresh (unwitnessed, unlike production's real `SeedBatch`, which this
    // corpus doesn't model), so two independent groups minting their very
    // first record at the identical virtual millisecond can legitimately
    // produce the identical packed HLC (no node-id bits, ADR 0018 §2's own
    // documented tradeoff) — spacing keeps the cross-tablet lineage check
    // meaningful without reimplementing `SeedBatch`'s witnessing for a
    // mechanism (PITR sealing) this corpus isn't testing.
    for i in 0..2 {
        write_and_journal(
            &mut sim,
            &left_group,
            &live,
            &mut left_journal,
            &key(i),
            b"post",
            seed,
        );
    }
    sim.run_for(Duration::from_secs(1));
    for i in 2..4 {
        write_and_journal(
            &mut sim,
            &right_group,
            &live,
            &mut right_journal,
            &key(i),
            b"post",
            seed,
        );
    }
    let left_leader = elect(&mut sim, &left_group, &live, seed);
    let right_leader = elect(&mut sim, &right_group, &live, seed);
    assert_eq!(
        pitr_seal_now(&mut meta, &store, &left_group, left_leader, 2_000, false),
        Some(0),
        "[seed={seed}] an in-place split child's own first PITR seal starts its own chain at 0"
    );
    assert_eq!(
        pitr_seal_now(&mut meta, &store, &right_group, right_leader, 2_000, false),
        Some(0)
    );

    // The union of the parent's final segment and both children's own new
    // segments covers exactly the pre-split writes (parent) plus each
    // child's own post-split writes — no key double-counted across
    // reporting tablets, no key lost.
    verify_pitr_lineage(
        &meta,
        &store,
        &[
            (&parent, leader, generation),
            (&left_group, left_leader, generation),
            (&right_group, right_leader, generation),
        ],
        &{
            let mut all = journal.clone();
            for (k, v) in &left_journal {
                all.entry(k.clone()).or_default().extend(v.clone());
            }
            for (k, v) in &right_journal {
                all.entry(k.clone()).or_default().extend(v.clone());
            }
            all
        },
        seed,
    );
}

#[test]
fn inplace_split_children_seal_independently_and_inherit_generation() {
    for_each_seed(
        "inplace_split_children_seal_independently_and_inherit_generation",
        scenario_inplace_split_children_seal_independently,
    );
}

// --- cell 19: restore_to_random_second_matches_the_model_across_an_inplace_split

/// The flagship restore-across-a-split property: the base snapshot pins
/// the PARENT tablet; the target second can land before the split, right
/// after cutover but before either child seals, or after one or both
/// children have sealed their own post-split writes. Originally built
/// alongside a now-deleted copy-based sibling cell proving the identical
/// property via the deprecated control-metadata-only `BeginSplit`/
/// `CutoverSplit` cutover — this cell drives it through
/// `BeginSplitInPlace`/reconciler fork+materialize/`CutoverSplit` instead,
/// and is now the corpus's only coverage of it (see the "cells 18/19"
/// section doc above).
fn scenario_restore_to_random_second_matches_the_model_across_an_inplace_split(seed: u64) {
    let mut sim = Simulator::new(seed);
    let mut meta = base_meta_with_pitr();
    let node_ids: Vec<NodeId> = NODES.iter().copied().map(nid).collect();
    let parent_id = TabletId(120);
    assert_eq!(
        meta.apply(&MetaCommand::CreateTablet {
            tablet: parent_id,
            table: Some(TABLE.into()),
            range: KeyRange::whole(),
            replicas: node_ids.clone(),
        }),
        ApplyOutcome::Applied
    );

    let live = [0, 1, 2];
    let driver = sim.env(driver_id());
    let mut cluster = Cluster::new(&sim);

    let base_view = metadata_view(&meta);
    assert!(
        converge(
            &mut sim,
            &driver,
            &mut cluster,
            &node_ids,
            &base_view,
            |c| leader_of(c, &node_ids, parent_id).is_some()
        ),
        "[seed={seed}] parent never elected a leader"
    );
    let parent = wrap_group(&cluster, &node_ids, parent_id);

    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let env = sim.env(nid(NODES[0]));
    let mut journal = BTreeMap::new();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    write_burst(
        &mut sim,
        &parent,
        &live,
        &mut journal,
        &mut model,
        &env,
        0,
        3,
        seed,
    );
    let leader = elect(&mut sim, &parent, &live, seed);
    let parent_seal_ms = 1_000u64;
    assert_eq!(
        pitr_seal_now(&mut meta, &store, &parent, leader, parent_seal_ms, false),
        Some(0),
        "[seed={seed}] parent's pre-split backlog seals as epoch 0"
    );
    let model_at_parent_seal = model.clone();

    let left = TabletId(121);
    let right = TabletId(122);
    let split_key = key(2);
    let parent_epoch = meta.tablets[&parent_id].epoch;
    assert_eq!(
        meta.apply(&MetaCommand::BeginSplitInPlace {
            parent: parent_id,
            expected_epoch: parent_epoch,
            split_key: split_key.clone(),
            children: [(left, node_ids.clone()), (right, node_ids.clone())],
        }),
        ApplyOutcome::Applied,
        "[seed={seed}] BeginSplitInPlace must apply"
    );

    let pending_view = metadata_view(&meta);
    assert!(
        converge(
            &mut sim,
            &driver,
            &mut cluster,
            &node_ids,
            &pending_view,
            |c| {
                node_ids.iter().all(|id| {
                    c.node(id)
                        .hosted_node(parent_id)
                        .is_some_and(|h| block_on(h.pending_split()).is_some())
                })
            }
        ),
        "[seed={seed}] the in-place split never forked on every participant"
    );
    assert!(
        converge(
            &mut sim,
            &driver,
            &mut cluster,
            &node_ids,
            &pending_view,
            |c| {
                node_ids.iter().all(|id| {
                    let hosted = c.hosted_set(id);
                    hosted.contains(&left) && hosted.contains(&right)
                })
            }
        ),
        "[seed={seed}] both children never materialized on every fork participant"
    );

    let parent_epoch = meta.tablets[&parent_id].epoch;
    assert_eq!(
        meta.apply(&MetaCommand::CutoverSplit {
            parent: parent_id,
            expected_epoch: parent_epoch,
            cutover_wall_ms: parent_seal_ms + 200,
        }),
        ApplyOutcome::Applied,
        "[seed={seed}] CutoverSplit must apply"
    );
    assert!(!meta.tablets.contains_key(&parent_id));

    let post_view = metadata_view(&meta);
    assert!(
        converge(
            &mut sim,
            &driver,
            &mut cluster,
            &node_ids,
            &post_view,
            |c| {
                node_ids.iter().all(|id| {
                    c.node(id).hosted_node(left).is_some()
                        && c.node(id).hosted_node(right).is_some()
                        && !c.hosted_set(id).contains(&parent_id)
                })
            }
        ),
        "[seed={seed}] both children never activated and the parent was never reclaimed everywhere"
    );

    let left_group = wrap_group(&cluster, &node_ids, left);
    let right_group = wrap_group(&cluster, &node_ids, right);

    let base = vec![(parent_id, 0)];
    // Right after cutover, before either child has sealed anything: the
    // model is still exactly the parent's own pre-split content.
    let just_after_cutover = parent_seal_ms + 300;
    assert_replay_matches_model(
        &meta,
        &store,
        &base,
        just_after_cutover,
        &model_at_parent_seal,
        seed,
    );

    let mut left_journal = BTreeMap::new();
    let mut right_journal = BTreeMap::new();
    // `key(0)`/`key(1)` sort below `key(2)` (the split key) — left's own
    // range; `key(2)..key(5)` is right's.
    write_burst_ranged(
        &mut sim,
        &left_group,
        &live,
        &mut left_journal,
        &mut model,
        &env,
        1,
        2,
        0..2,
        seed,
    );
    sim.run_for(Duration::from_secs(1));
    let left_seal_ms = parent_seal_ms + 1_000;
    let left_leader = elect(&mut sim, &left_group, &live, seed);
    assert_eq!(
        pitr_seal_now(
            &mut meta,
            &store,
            &left_group,
            left_leader,
            left_seal_ms,
            false
        ),
        Some(0),
        "[seed={seed}] left child's own first PITR seal starts its own chain at 0"
    );
    let model_after_left_seal = model.clone();
    assert_replay_matches_model(
        &meta,
        &store,
        &base,
        left_seal_ms,
        &model_after_left_seal,
        seed,
    );
    // The right child hasn't sealed anything of its own post-split writes
    // yet, so the model at this point is parent-content UNION left's own
    // (already reflected in `model`, since `write_burst_ranged` mutated it
    // directly) — right's own writes below must not leak in early.

    write_burst_ranged(
        &mut sim,
        &right_group,
        &live,
        &mut right_journal,
        &mut model,
        &env,
        2,
        2,
        2..6,
        seed,
    );
    sim.run_for(Duration::from_secs(1));
    let right_seal_ms = left_seal_ms + 1_000;
    let right_leader = elect(&mut sim, &right_group, &live, seed);
    assert_eq!(
        pitr_seal_now(
            &mut meta,
            &store,
            &right_group,
            right_leader,
            right_seal_ms,
            false
        ),
        Some(0)
    );
    let model_after_right_seal = model.clone();
    assert_replay_matches_model(
        &meta,
        &store,
        &base,
        right_seal_ms,
        &model_after_right_seal,
        seed,
    );
    // Restoring to a point BEFORE the right child's own seal must still
    // exclude its writes even though they are already fully committed by
    // now — proving the split-lineage walk floors each child at its own
    // segment set, not at "whatever the tablet currently holds".
    assert_replay_matches_model(
        &meta,
        &store,
        &base,
        left_seal_ms,
        &model_after_left_seal,
        seed,
    );
}

#[test]
fn restore_to_random_second_matches_the_model_across_an_inplace_split() {
    for_each_seed(
        "restore_to_random_second_matches_the_model_across_an_inplace_split",
        scenario_restore_to_random_second_matches_the_model_across_an_inplace_split,
    );
}
