//! The DynamoDB Streams **lineage-walk corpus** (ADR 0042/0043 round-3 PR8,
//! testing-plan deliverable D3) plus the **durability kill-point** scenario
//! (D9).
//!
//! ## What this proves, and against which layer
//!
//! ADR 0042/0043's whole subsystem lives in `animusd` (`index_drain.rs`'s
//! seal arm, `dynamo_streams.rs`'s read path) — a crate `animus-test` does
//! not, and by house convention should not, depend on (every existing corpus
//! here is self-contained, reimplementing the protocol directly over the
//! lower-layer primitives — see `txn_serializable.rs`'s coordinator for the
//! precedent). This file follows the identical discipline: it reimplements
//! the **sealer** (`seal_now`, mirroring `index_drain::seal_now`'s exact
//! sequence: scan `pending_changes()` past the effective watermark, sort by
//! the HLC key suffix, encode a segment, `SegmentStore::put`, then propose
//! `MetaCommand::SealStreamShard`) and a **model consumer**
//! (`collect_tablet_records`/`verify_lineage`, mirroring
//! `DescribeStream`/`GetShardIterator(TRIM_HORIZON)`/`GetRecords`'s exact
//! decision: a catalog row ⇒ fetch-and-slice from the store, no row ⇒ a
//! bounded hot scan past the watermark) directly against `animus-cp-data`'s
//! `RaftKvNode`/`segment`/`animus-control`'s bare `Metadata` (mutated with
//! plain `.apply()` calls, no live control Raft needed — the same
//! hand-scripted-catalog technique `animus-cp-data/tests/reconciler_corpus.rs`
//! uses for `MetadataView`) and `animus-sim`'s `SimSegmentStore`.
//!
//! **The delta from the real wire API, documented rather than hidden**: this
//! consumer is driven **once, to convergence, after a scenario's write/seal/
//! fault schedule finishes** — not as a live poll interleaved with ongoing
//! writes the way a real Lambda/KCL consumer would run. It still walks the
//! identical decision tree (parent-before-child, `TRIM_HORIZON` per shard,
//! `GetRecords` until a closed shard's content is exhausted or an open
//! shard's watermark is reached) and is driven against the same lower-layer
//! primitives the real `dynamo_streams.rs` calls, so it is a faithful proof of
//! the *shard-chain reconstruction* contract (exactly-once, per-item order,
//! chain continuity, segment-content fidelity) — just not of the HTTP wire
//! format or of "what does an in-flight poll see mid-stream," which the
//! `ProdEnv` e2e (`animusd/tests/streams_e2e.rs`) and the existing
//! `animusd/tests/dynamo_streams.rs` cover instead.
//!
//! **Why no `animus_test::{History, Recorder, check_cycles}`**: the property
//! under test — "did every shard in a tablet's lineage deliver every write
//! exactly once, in the right order, with the right bytes" — is a shard-chain
//! reconstruction claim, not a serializability claim over concurrent
//! transactions; a bespoke write-journal-vs-delivered-stream diff
//! (`verify_lineage`) states it far more directly than coercing it into an
//! Elle list-append history would.
//!
//! ## Corpus doctrine (ADR 0014)
//!
//! Frozen, named scenario cells (one `#[test]` each), a depth knob
//! (`ANIMUS_STREAM_SEEDS`, default 1 — variant 0 always keeps the cell's own
//! canonical, name-derived seed, matching every other corpus's `seed_expand`
//! convention), and `corpus-deep.yml` nightly wiring at depth. See
//! `crates/animus-test/CLAUDE.md` for the full knob table.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_control::{
    ApplyOutcome, ColumnType, MetaCommand, Metadata, StreamSpec, StreamViewType, TableSchema,
};
use animus_cp_data::{KIND_BASE, RaftKvNode, StorageScope, segment};
use animus_env::{Nanos, SegmentStore, nid};
use animus_sim::{SimEnv, SimSegmentStore, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const NODES: [u64; 3] = [40, 41, 42];
const TABLE: &str = "orders";
const LABEL: &str = "L1";

/// A stable, name-derived seed — every scenario is reproducible and
/// attributable by name, the same convention every other corpus here uses.
fn name_seed(name: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h | 1
}

fn seeds_per_cell() -> usize {
    std::env::var("ANIMUS_STREAM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&k: &usize| k > 0)
        .unwrap_or(1)
}

/// Depth expansion: variant 0 is the frozen canonical seed; `1..K` derive a
/// fresh, distinct name so growing depth never perturbs the base regression.
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

// --- tablet-group harness ----------------------------------------------

/// A tablet's 3-replica Raft group, sharing each node's own per-node engine
/// with every other tablet that node hosts (ADR 0028 — the same "one shared
/// engine per node" discipline `narrow_scope.rs`/`cross_group_lww.rs` use).
struct Group {
    id: TabletId,
    range: KeyRange,
    nodes: Vec<KvNode>,
}

fn start_group(
    sim: &Simulator,
    engines: &BTreeMap<u64, MemoryEngine>,
    id: TabletId,
    range: KeyRange,
) -> Group {
    let ids: Vec<_> = NODES.iter().copied().map(nid).collect();
    // `start_hosted` with `stream = id.0` (ADR 0026 Stage B), never
    // `start_scoped` (which pins every group to `PRIMARY_STREAM`) — several
    // scenarios here run more than one tablet group on the very same 3 node
    // ids at once (the split cells), and two groups sharing a node id's
    // inbox on the same stream cross-talk their Raft traffic, corrupting
    // both (found by this corpus: `combined_chaos` initially livelocked
    // leader election on the parent group the instant a sibling group
    // started on the same node ids — see the engineering-lessons entry).
    let nodes = NODES
        .iter()
        .map(|&n| {
            RaftKvNode::start_hosted(
                sim.env(nid(n)),
                ids.clone(),
                engines[&n].clone(),
                StorageScope::new(TABLE.as_bytes().to_vec(), range.clone()),
                id.0,
            )
        })
        .collect();
    Group { id, range, nodes }
}

/// Waits for exactly one leader among `live` node indices, bounded — panics
/// naming the seed on failure (a real bug, not a slow-but-eventual test).
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
    match group.nodes[leader].put_kind_batch_fenced(
        vec![(KIND_BASE, item_key.to_vec(), Some(record.to_vec()))],
        Some((item_key.to_vec(), record.to_vec())),
        group.range.clone(),
    ) {
        animus_control::ProposeResult::Accepted { index } => index,
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

/// Writes `item_key -> payload` through `group`'s current leader (electing
/// one among `live` first) and confirms it applied; records it into
/// `journal` in write order (per key). Returns the (`item_key`, `payload`)
/// pair's own packed HLC isn't needed by the caller — the journal alone is
/// enough for `verify_lineage`.
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

// --- the sealer (mirrors `animusd::index_drain::seal_now`) --------------

fn current_open_epoch(meta: &Metadata, tablet: TabletId) -> u64 {
    meta.stream_shards
        .range((tablet, 0)..=(tablet, u64::MAX))
        .next_back()
        .map_or(0, |((_, e), _)| e + 1)
}

/// Recovers a change record's packed HLC from its key's trailing 8 bytes —
/// the same suffix `animusd::dynamo_streams::record_hlc_suffix` recovers from
/// the real `StreamHotRead`/segment-record shape.
fn record_hlc_suffix(key: &[u8]) -> Option<u64> {
    let n = key.len().checked_sub(8)?;
    Some(u64::from_be_bytes(key[n..].try_into().ok()?))
}

/// One seal attempt of `group`'s currently-open epoch (ADR 0043 §A3's
/// sequence, steps 1-3): `None` if there was nothing past the watermark to
/// seal, or if the store `put` failed (the store-outage case: the hot scope
/// simply keeps growing — nothing here forces a crash). `skip_commit` models
/// a crash **between** the `put` (step 2) and the catalog commit (step 3) —
/// the caller re-invokes `seal_now` to model the idempotent retry, which
/// recomputes the identical epoch/id and safely overwrites.
#[allow(clippy::too_many_arguments)]
fn seal_now(
    meta: &mut Metadata,
    store: &SimSegmentStore,
    group: &Group,
    leader: usize,
    wall_ms: u64,
    skip_commit: bool,
) -> Option<u64> {
    let watermark = meta.effective_stream_shard_watermark(group.id).unwrap_or(0);
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

    let epoch = current_open_epoch(meta, group.id);
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
    let shard_id = segment::shard_id(group.id.0, epoch);
    let parent_shard_id = meta.stream_shard_parent_id(group.id, epoch);
    let header = segment::SegmentHeader {
        table: TABLE.into(),
        label: LABEL.into(),
        shard_id,
        tablet: group.id.0,
        epoch,
        parent_shard_id,
        hlc_range,
        count,
        seal_wall_ms: wall_ms,
    };
    let bytes = segment::encode(&header, &records);
    let seg_id = segment::segment_id(TABLE, LABEL, group.id.0, epoch);
    block_on(store.put(&seg_id, &bytes)).ok()?;

    if skip_commit {
        return None; // modelled crash before catalog commit
    }
    let outcome = meta.apply(&MetaCommand::SealStreamShard {
        table: TABLE.into(),
        label: LABEL.into(),
        tablet: group.id,
        epoch,
        view_type: StreamViewType::NewAndOldImages,
        hlc_range,
        count,
        seal_wall_ms: wall_ms,
        replicas: Vec::new(),
    });
    matches!(outcome, ApplyOutcome::Applied).then_some(epoch)
}

// --- the model consumer (mirrors `DescribeStream`/`GetShardIterator`/
// `GetRecords`) -----------------------------------------------------------

/// `TRIM_HORIZON..` `GetRecords` of every shard in `group`'s own lineage:
/// every closed shard (ascending epoch, fetched-and-sliced from the store —
/// the sealed serve path) then the open shard's hot tail past the effective
/// watermark (the open serve path, stopping once "caught up" — the same
/// `hlc > position` bound `StreamHotRead` uses).
fn collect_tablet_records(
    meta: &Metadata,
    store: &SimSegmentStore,
    group: &Group,
    leader: usize,
) -> Vec<(Vec<u8>, u64, Vec<u8>)> {
    let mut all = Vec::new();
    for (epoch, row) in meta.stream_shard_chain(TABLE, LABEL, group.id) {
        let seg_id = segment::segment_id(TABLE, LABEL, group.id.0, epoch);
        let bytes = block_on(store.get(&seg_id))
            .unwrap_or_else(|e| panic!("segment store get of {seg_id}: {e}"));
        let bytes = bytes.unwrap_or_else(|| {
            panic!(
                "sealed shard {seg_id} missing from the store (catalog row committed, object gone)"
            )
        });
        let (_, records) = segment::decode_and_slice(&bytes, row.hlc_range)
            .unwrap_or_else(|e| panic!("corrupt segment {seg_id}: {e}"));
        for r in records {
            all.push((r.source_key, r.packed_hlc, r.change_record));
        }
    }
    let watermark = meta.effective_stream_shard_watermark(group.id).unwrap_or(0);
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

/// Walks `lineage` (caller-supplied **parent-before-child** order — the
/// lineage discipline itself, ADR 0042 §2/ADR 0043 §A4) and asserts:
/// exactly-once (every packed HLC delivered by exactly one shard, globally),
/// per-item order (each key's delivered payload sequence matches its
/// journal's write order, byte for byte), and total-count agreement (no
/// record vanished or was invented).
fn verify_lineage(
    meta: &Metadata,
    store: &SimSegmentStore,
    lineage: &[(&Group, usize)],
    journal: &BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
    seed: u64,
) {
    let mut delivered_by_key: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    let mut seen_hlcs: BTreeSet<u64> = BTreeSet::new();
    let mut total = 0usize;
    for (group, leader) in lineage {
        for (source_key, hlc, record) in collect_tablet_records(meta, store, group, *leader) {
            assert!(
                seen_hlcs.insert(hlc),
                "[seed={seed}] hlc {hlc} delivered more than once — violates exactly-once"
            );
            let item_key = source_key[..source_key.len() - 8].to_vec();
            delivered_by_key.entry(item_key).or_default().push(record);
            total += 1;
        }
    }
    let expected_total: usize = journal.values().map(Vec::len).sum();
    assert_eq!(
        total, expected_total,
        "[seed={seed}] delivered record count {total} != journal count {expected_total}"
    );
    for (key, expected) in journal {
        let got = delivered_by_key.get(key).cloned().unwrap_or_default();
        assert_eq!(
            &got, expected,
            "[seed={seed}] item {key:?} delivered out of order or with wrong content"
        );
    }
}

fn engines() -> BTreeMap<u64, MemoryEngine> {
    NODES.iter().map(|&n| (n, MemoryEngine::new())).collect()
}

/// A bare `Metadata` (no live control Raft — every mutation below is a plain
/// `.apply()` call, mirroring `dynamo_streams::tests::base_meta`/`enable`)
/// with `TABLE`'s schema registered and its stream enabled at `LABEL` — the
/// minimum `SealStreamShard`'s own apply-time label validation (ADR 0043
/// §A8) requires before it will accept a first seal.
fn base_meta() -> Metadata {
    let mut m = Metadata::default();
    let outcome = m.apply(&MetaCommand::CreateTableSchema {
        table: TABLE.into(),
        schema: TableSchema::simple("id", ColumnType::String),
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "schema registration must apply"
    );
    let outcome = m.apply(&MetaCommand::SetTableStream {
        table: TABLE.into(),
        spec: Some(StreamSpec {
            view_type: StreamViewType::NewAndOldImages,
            label: LABEL.into(),
        }),
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "enabling the stream must apply"
    );
    m
}

/// Registers `id` in the tablet map so `MetaCommand::SplitTablet` has a row
/// to CAS against (`SealStreamShard` itself needs no tablet-map entry — only
/// the table's schema/stream, which `base_meta` already provides).
fn create_tablet(meta: &mut Metadata, id: TabletId, range: KeyRange) {
    let outcome = meta.apply(&MetaCommand::CreateTablet {
        tablet: id,
        table: Some(TABLE.into()),
        range,
        replicas: NODES.iter().copied().map(nid).collect(),
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "tablet registration must apply"
    );
}

fn key(i: usize) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

// --- cell 1: quiet_table_rollover ---------------------------------------

fn scenario_quiet_table_rollover(seed: u64) {
    let sim = Simulator::new(seed);
    let mut sim = sim;
    let engines = engines();
    let mut meta = base_meta();
    let group = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    for i in 0..3 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v0", seed);
    }
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = seal_now(&mut meta, &store, &group, leader, 1_000, false);
    assert_eq!(sealed, Some(0), "[seed={seed}] expected epoch 0 to seal");

    for i in 0..3 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v1", seed);
    }
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = seal_now(&mut meta, &store, &group, leader, 2_000, false);
    assert_eq!(sealed, Some(1), "[seed={seed}] expected epoch 1 to seal");

    verify_lineage(&meta, &store, &[(&group, leader)], &journal, seed);
}

#[test]
fn quiet_table_rollover() {
    for_each_seed("quiet_table_rollover", scenario_quiet_table_rollover);
}

// --- cell 2: hot_table_size_seals ----------------------------------------

fn scenario_hot_table_size_seals(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    let group = start_group(&sim, &engines, TabletId(2), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    for round in 0..6u64 {
        for i in 0..8 {
            let payload = format!("round{round}").into_bytes();
            write_and_journal(
                &mut sim,
                &group,
                &live,
                &mut journal,
                &key(i),
                &payload,
                seed,
            );
        }
        let leader = elect(&mut sim, &group, &live, seed);
        seal_now(
            &mut meta,
            &store,
            &group,
            leader,
            1_000 + round * 100,
            false,
        );
    }
    let leader = elect(&mut sim, &group, &live, seed);
    verify_lineage(&meta, &store, &[(&group, leader)], &journal, seed);
    assert!(
        meta.stream_shard_chain(TABLE, LABEL, group.id).count() >= 4,
        "[seed={seed}] expected several closed shards from repeated size-seals"
    );
}

#[test]
fn hot_table_size_seals() {
    for_each_seed("hot_table_size_seals", scenario_hot_table_size_seals);
}

// --- cell 3: split_mid_stream --------------------------------------------

const BOUNDARY: &[u8] = b"k0500";

fn scenario_split_mid_stream(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    create_tablet(&mut meta, TabletId(3), KeyRange::whole());
    let parent = start_group(&sim, &engines, TabletId(3), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    // A handful of left-side keys, a handful of right-side keys, and one
    // "straddling" right-side key (`shared_key`) written once before the
    // split and once after — the case that exercises per-item order across
    // a lineage boundary.
    for i in 0..4 {
        write_and_journal(&mut sim, &parent, &live, &mut journal, &key(i), b"L", seed); // left
    }
    for i in 600..604 {
        write_and_journal(&mut sim, &parent, &live, &mut journal, &key(i), b"R", seed); // right
    }
    let shared_key = key(700);
    write_and_journal(
        &mut sim,
        &parent,
        &live,
        &mut journal,
        &shared_key,
        b"before",
        seed,
    );

    // Seal the parent's epoch 0 before splitting, so the split boundary
    // lands cleanly on a sealed watermark (ADR 0043 §A4's own "closes at its
    // last sealed position").
    let leader = elect(&mut sim, &parent, &live, seed);
    let sealed = seal_now(&mut meta, &store, &parent, leader, 1_000, false);
    assert_eq!(
        sealed,
        Some(0),
        "[seed={seed}] parent must seal epoch 0 before the split"
    );

    // The split itself: narrow the parent's live scope, record split
    // provenance in the catalog, and start a fresh sibling group over the
    // SAME per-node engines (ADR 0028 shared storage — the sibling's own
    // `KIND_CHANGE` scope, once widened to its range, transparently exposes
    // whatever right-range records already physically exist).
    let parent_epoch = meta
        .tablets
        .get(&parent.id)
        .map_or(animus_tablet::Epoch::INITIAL, |t| t.epoch);
    let sibling_id = TabletId(4);
    for n in &parent.nodes {
        n.narrow_scope(KeyRange::new(Vec::new(), Some(BOUNDARY.to_vec())));
    }
    let outcome = meta.apply(&MetaCommand::SplitTablet {
        tablet: parent.id,
        expected_epoch: parent_epoch,
        split_key: BOUNDARY.to_vec(),
        new_id: sibling_id,
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "[seed={seed}] split must apply"
    );
    let sibling = start_group(
        &sim,
        &engines,
        sibling_id,
        KeyRange::new(BOUNDARY.to_vec(), None),
    );
    sim.run_for(Duration::from_secs(2));

    // Continued writes on both sides, including the second write for the
    // straddling key — which must now land on the sibling.
    for i in 4..8 {
        write_and_journal(
            &mut sim,
            &parent,
            &[0, 1, 2],
            &mut journal,
            &key(i),
            b"L2",
            seed,
        );
    }
    for i in 610..614 {
        write_and_journal(
            &mut sim,
            &sibling,
            &[0, 1, 2],
            &mut journal,
            &key(i),
            b"R2",
            seed,
        );
    }
    write_and_journal(
        &mut sim,
        &sibling,
        &[0, 1, 2],
        &mut journal,
        &shared_key,
        b"after",
        seed,
    );

    // Seal the sibling's own epoch 0 FIRST, and check its lineage link right
    // here — `stream_shard_parent_id` is derived, not stored (ADR 0043
    // §A8), from "the parent tablet's own CURRENT last sealed shard," so
    // this assertion must run before the parent (which, post-split, is
    // simply the *left child* continuing under the same tablet id — not a
    // separate entity — and is free to go on sealing its own later epochs)
    // seals again and moves what "current last" means.
    let sibling_leader = elect(&mut sim, &sibling, &[0, 1, 2], seed);
    seal_now(&mut meta, &store, &sibling, sibling_leader, 2_100, false);
    let sibling_epoch0_parent = meta.stream_shard_parent_id(sibling.id, 0);
    let parent_last_shard = segment::shard_id(parent.id.0, 0);
    assert_eq!(
        sibling_epoch0_parent,
        Some(parent_last_shard),
        "[seed={seed}] sibling's epoch-0 ParentShardId must name the parent's last sealed shard"
    );

    let parent_leader = elect(&mut sim, &parent, &[0, 1, 2], seed);
    seal_now(&mut meta, &store, &parent, parent_leader, 2_000, false);

    // Parent-before-child: the lineage discipline itself.
    verify_lineage(
        &meta,
        &store,
        &[(&parent, parent_leader), (&sibling, sibling_leader)],
        &journal,
        seed,
    );
}

#[test]
fn split_mid_stream() {
    for_each_seed("split_mid_stream", scenario_split_mid_stream);
}

// --- cell 4: kill_sealing_leader ------------------------------------------

fn scenario_kill_sealing_leader(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    let group = start_group(&sim, &engines, TabletId(5), KeyRange::whole());
    let mut live = vec![0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    for i in 0..5 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v0", seed);
    }

    // Kill whichever replica currently leads mid-stream (before any seal has
    // happened) and elect among the survivors.
    let dying = elect(&mut sim, &group, &live, seed);
    sim.crash(nid(NODES[dying]));
    live.retain(|&i| i != dying);
    let new_leader = elect(&mut sim, &group, &live, seed);

    for i in 5..10 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v1", seed);
    }
    let sealed = seal_now(&mut meta, &store, &group, new_leader, 1_000, false);
    assert_eq!(
        sealed,
        Some(0),
        "[seed={seed}] the post-election leader must still seal correctly from committed state"
    );

    verify_lineage(&meta, &store, &[(&group, new_leader)], &journal, seed);
}

#[test]
fn kill_sealing_leader() {
    for_each_seed("kill_sealing_leader", scenario_kill_sealing_leader);
}

// --- cell 5: store_outage_then_heal --------------------------------------

fn scenario_store_outage_then_heal(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    let group = start_group(&sim, &engines, TabletId(6), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    for i in 0..4 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"a", seed);
    }

    // The store goes unavailable: a seal attempt must fail cleanly, leaving
    // every written record recoverable from hot Raft state — the durability
    // invariant (ADR 0042 §9) never licenses trim off anything but a
    // committed catalog row, and no catalog row can commit here at all.
    store.set_unavailable_until(Nanos(u64::MAX));
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = seal_now(&mut meta, &store, &group, leader, 1_000, false);
    assert_eq!(
        sealed, None,
        "[seed={seed}] a seal must not commit while the store is unavailable"
    );
    assert!(
        meta.stream_shard_chain(TABLE, LABEL, group.id)
            .next()
            .is_none(),
        "[seed={seed}] no catalog row may exist while the store never acked a put"
    );
    // Every write is still recoverable straight from the hot log.
    let hot = block_on(group.nodes[leader].pending_changes());
    assert_eq!(
        hot.len(),
        4,
        "[seed={seed}] the hot scope must keep growing, never lose a write, during an outage"
    );

    // Heal: the very next seal attempt (same code path, no special recovery)
    // now succeeds.
    store.clear_unavailable();
    for i in 4..8 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"b", seed);
    }
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = seal_now(&mut meta, &store, &group, leader, 2_000, false);
    assert_eq!(
        sealed,
        Some(0),
        "[seed={seed}] sealing must succeed once the store heals"
    );

    verify_lineage(&meta, &store, &[(&group, leader)], &journal, seed);
}

#[test]
fn store_outage_then_heal() {
    for_each_seed("store_outage_then_heal", scenario_store_outage_then_heal);
}

// --- cell 6: disable_grace_drain ------------------------------------------

fn scenario_disable_grace_drain(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    let group = start_group(&sim, &engines, TabletId(7), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    for i in 0..5 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"v", seed);
    }

    // F12-b's disable-triggered final seal: every currently-open tablet's
    // hot tail must be sealed before the write gate closes, so every record
    // reaches the readable (segment) tier.
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = seal_now(&mut meta, &store, &group, leader, 1_000, false);
    assert_eq!(sealed, Some(0), "[seed={seed}] the final seal must commit");
    assert!(
        block_on(group.nodes[leader].pending_changes())
            .into_iter()
            .filter_map(|(k, _)| record_hlc_suffix(&k))
            .all(|hlc| hlc <= meta.effective_stream_shard_watermark(group.id).unwrap_or(0)),
        "[seed={seed}] nothing should remain above the watermark after a final seal"
    );

    // The label is still listed/readable during the grace window even
    // though the schema entry is gone — F12-b's own catalog-row-based
    // resolution (this corpus never proposes `SetTableStream`, since the
    // schema catalog isn't part of what this file re-derives; the grace
    // check is purely "the label still has live catalog rows," the exact
    // half `stream_labels_with_rows` answers).
    assert!(
        meta.stream_labels_with_rows(TABLE).contains(LABEL),
        "[seed={seed}] a disabled-but-unreaped label must still have live catalog rows"
    );

    verify_lineage(&meta, &store, &[(&group, leader)], &journal, seed);

    // A re-enable mints a fresh label; both coexist in the catalog's own
    // label set until the old one is reaped (a later PR7 concern, not
    // re-tested here — see `animusd/tests/stream_janitor.rs`).
    const LABEL2: &str = "L2";
    // A genuine disable → re-enable cycle (ADR 0042 §11): disable clears the
    // schema's stream spec (the label's remaining catalog rows are what
    // keeps it in the F12-b grace window, asserted above); re-enable mints
    // the fresh label.
    let outcome = meta.apply(&MetaCommand::SetTableStream {
        table: TABLE.into(),
        spec: None,
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "[seed={seed}] disable must apply"
    );
    let outcome = meta.apply(&MetaCommand::SetTableStream {
        table: TABLE.into(),
        spec: Some(StreamSpec {
            view_type: StreamViewType::NewAndOldImages,
            label: LABEL2.into(),
        }),
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "[seed={seed}] re-enable must apply"
    );
    let watermark = meta.effective_stream_shard_watermark(group.id).unwrap_or(0);
    let outcome = meta.apply(&MetaCommand::SealStreamShard {
        table: TABLE.into(),
        label: LABEL2.into(),
        tablet: group.id,
        epoch: current_open_epoch(&meta, group.id),
        view_type: StreamViewType::NewAndOldImages,
        hlc_range: (watermark, watermark),
        count: 0,
        seal_wall_ms: 3_000,
        replicas: Vec::new(),
    });
    // An empty shard is never sealed in production (`seal_now` never calls
    // `SealStreamShard` for an empty scan) — this hand-built row exists only
    // to prove the catalog can hold two coexisting labels' rows at once; a
    // real re-enable's own first genuine seal is what `stream_janitor.rs`'s
    // `disable_grace_lifecycle_end_to_end_with_reenable_coexistence` proves
    // through the real wire path.
    assert_eq!(outcome, ApplyOutcome::Applied);
    let labels = meta.stream_labels_with_rows(TABLE);
    assert!(
        labels.contains(LABEL) && labels.contains(LABEL2),
        "[seed={seed}] both labels must coexist"
    );
}

#[test]
fn disable_grace_drain() {
    for_each_seed("disable_grace_drain", scenario_disable_grace_drain);
}

// --- cell 7: combined_chaos -----------------------------------------------

fn scenario_combined_chaos(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    create_tablet(&mut meta, TabletId(8), KeyRange::whole());
    let parent = start_group(&sim, &engines, TabletId(8), KeyRange::whole());
    let mut live = vec![0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    for i in 0..3 {
        write_and_journal(&mut sim, &parent, &live, &mut journal, &key(i), b"a", seed);
    }
    let leader = elect(&mut sim, &parent, &live, seed);
    let sealed = seal_now(&mut meta, &store, &parent, leader, 1_000, false);
    assert_eq!(sealed, Some(0));

    // Leader kill mid-stream.
    let dying = elect(&mut sim, &parent, &live, seed);
    sim.crash(nid(NODES[dying]));
    live.retain(|&i| i != dying);
    for i in 3..6 {
        write_and_journal(&mut sim, &parent, &live, &mut journal, &key(i), b"b", seed);
    }

    // Split.
    let parent_epoch = meta
        .tablets
        .get(&parent.id)
        .map_or(animus_tablet::Epoch::INITIAL, |t| t.epoch);
    let sibling_id = TabletId(9);
    for &i in &live {
        parent.nodes[i].narrow_scope(KeyRange::new(Vec::new(), Some(BOUNDARY.to_vec())));
    }
    meta.apply(&MetaCommand::SplitTablet {
        tablet: parent.id,
        expected_epoch: parent_epoch,
        split_key: BOUNDARY.to_vec(),
        new_id: sibling_id,
    });
    let sibling = start_group(
        &sim,
        &engines,
        sibling_id,
        KeyRange::new(BOUNDARY.to_vec(), None),
    );
    sim.run_for(Duration::from_secs(2));
    let sibling_live = [0, 1, 2];

    for i in 600..604 {
        write_and_journal(
            &mut sim,
            &sibling,
            &sibling_live,
            &mut journal,
            &key(i),
            b"c",
            seed,
        );
    }

    // Store outage during the sibling's own first seal attempt, then heal.
    store.set_unavailable_until(Nanos(u64::MAX));
    let sibling_leader = elect(&mut sim, &sibling, &sibling_live, seed);
    let sealed = seal_now(&mut meta, &store, &sibling, sibling_leader, 2_000, false);
    assert_eq!(
        sealed, None,
        "[seed={seed}] the outage must block the sibling's seal"
    );
    store.clear_unavailable();
    for i in 604..607 {
        write_and_journal(
            &mut sim,
            &sibling,
            &sibling_live,
            &mut journal,
            &key(i),
            b"d",
            seed,
        );
    }
    let sibling_leader = elect(&mut sim, &sibling, &sibling_live, seed);
    let sealed = seal_now(&mut meta, &store, &sibling, sibling_leader, 3_000, false);
    assert_eq!(
        sealed,
        Some(0),
        "[seed={seed}] sealing must succeed once healed"
    );

    let parent_leader = elect(&mut sim, &parent, &live, seed);
    seal_now(&mut meta, &store, &parent, parent_leader, 4_000, false);

    verify_lineage(
        &meta,
        &store,
        &[(&parent, parent_leader), (&sibling, sibling_leader)],
        &journal,
        seed,
    );
}

#[test]
fn combined_chaos() {
    for_each_seed("combined_chaos", scenario_combined_chaos);
}

// --- D9: the durability-invariant kill-point scenario ---------------------

/// ADR 0042 §9's own statement, checked directly at a handful of interesting
/// points across a scripted seal lifecycle: **at every instant, every
/// acknowledged write is recoverable from hot Raft state or from a
/// catalog-committed segment — never from neither.** This corpus never
/// implements retention (that lifecycle belongs to
/// `animusd::segment_janitor`, proven end to end over `ProdEnv` in
/// `animusd/tests/stream_janitor.rs`), so nothing here is ever expected to
/// answer `TrimmedDataAccess` — every write, at every kill point, must
/// remain recoverable through to the end of the scenario.
fn assert_every_write_recoverable(
    meta: &Metadata,
    store: &SimSegmentStore,
    group: &Group,
    leader: usize,
    journal: &BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
    seed: u64,
    at: &str,
) {
    let delivered = collect_tablet_records(meta, store, group, leader);
    let mut by_key: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    for (source_key, _, record) in delivered {
        let item_key = source_key[..source_key.len() - 8].to_vec();
        by_key.entry(item_key).or_default().push(record);
    }
    for (key, expected) in journal {
        let got = by_key.get(key).cloned().unwrap_or_default();
        assert_eq!(
            &got, expected,
            "[seed={seed}, kill point={at}] item {key:?} unrecoverable — durability invariant violated"
        );
    }
}

fn scenario_durability_kill_points(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    let group = start_group(&sim, &engines, TabletId(10), KeyRange::whole());
    let mut live = vec![0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    for i in 0..3 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"1", seed);
    }
    let leader = elect(&mut sim, &group, &live, seed);
    assert_every_write_recoverable(
        &meta,
        &store,
        &group,
        leader,
        &journal,
        seed,
        "before any seal",
    );

    // Kill point: put succeeded, catalog commit deliberately skipped
    // (modeling a crash between ADR 0043 §A3 steps 2 and 3).
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = seal_now(&mut meta, &store, &group, leader, 1_000, true);
    assert_eq!(
        sealed, None,
        "[seed={seed}] the modeled crash must skip the catalog commit"
    );
    assert_every_write_recoverable(
        &meta,
        &store,
        &group,
        leader,
        &journal,
        seed,
        "put succeeded, catalog commit skipped",
    );

    // Recovery: re-run the identical seal — the retried put overwrites the
    // same deterministic id; the catalog now genuinely commits.
    let leader = elect(&mut sim, &group, &live, seed);
    let sealed = seal_now(&mut meta, &store, &group, leader, 1_100, false);
    assert_eq!(
        sealed,
        Some(0),
        "[seed={seed}] the retried seal must commit"
    );
    assert_every_write_recoverable(
        &meta,
        &store,
        &group,
        leader,
        &journal,
        seed,
        "after the retried seal committed",
    );

    for i in 3..6 {
        write_and_journal(&mut sim, &group, &live, &mut journal, &key(i), b"2", seed);
    }
    assert_every_write_recoverable(
        &meta,
        &store,
        &group,
        leader,
        &journal,
        seed,
        "hot tail after a committed seal",
    );

    // Kill point: leader kill right after the committed seal, before the
    // new hot writes above are themselves ever sealed.
    let dying = elect(&mut sim, &group, &live, seed);
    sim.crash(nid(NODES[dying]));
    live.retain(|&i| i != dying);
    let leader = elect(&mut sim, &group, &live, seed);
    // The durability invariant is about eventual recoverability from
    // *applied* state, not instantaneous availability the instant a replica
    // dies — a survivor that committed but hadn't yet locally applied at
    // the exact crash instant (the driver's own consensus/apply-task split,
    // ADR 0017) needs a moment to catch up, same as any other apply-lag
    // wait elsewhere in this codebase.
    sim.run_for(Duration::from_millis(500));
    assert_every_write_recoverable(
        &meta,
        &store,
        &group,
        leader,
        &journal,
        seed,
        "after a post-seal leader kill",
    );
}

#[test]
fn durability_invariant_holds_at_every_kill_point() {
    for_each_seed(
        "durability_invariant_holds_at_every_kill_point",
        scenario_durability_kill_points,
    );
}
