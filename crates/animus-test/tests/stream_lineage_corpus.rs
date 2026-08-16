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
use animus_env::{Env, Nanos, Rng, SegmentStore, nid};
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
    // Split-seal range-fence amendment (ADR 0043 §A3/§A4/§A6, 2026-08-15):
    // mirrors `index_drain::seal_now`'s own fence exactly, same reason —
    // `pending_changes()` is bounded only by this group's *physical* scope,
    // which a caller (a scripted scenario here; the reconciler in
    // production) can leave wider than `meta`'s own declared range for a
    // while after a split. A record outside that declared range already
    // belongs to a sibling tablet and must be left for its own seal.
    let declared_range = meta.tablets.get(&group.id).map(|t| t.range.clone());
    let mut filtered: Vec<(Vec<u8>, u64, Vec<u8>)> =
        block_on(group.nodes[leader].pending_changes())
            .into_iter()
            .filter_map(|(k, v)| {
                let hlc = record_hlc_suffix(&k)?;
                if hlc <= watermark {
                    return None;
                }
                if declared_range.as_ref().is_some_and(|r| !r.contains(&k)) {
                    return None;
                }
                Some((k, hlc, v))
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
    // Ledger-named-object amendment: a fresh, attempt-unique id every call —
    // mirrors `index_drain::seal_now`'s own scheme exactly (proposer node id
    // + this group's own current term + a fresh RNG draw), never the bare
    // deterministic `segment_id`.
    let env = group.nodes[leader].env();
    let seg_id = segment::segment_object_id(
        TABLE,
        LABEL,
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
    // Split-seal range-fence CAS (2026-08-15, ADR 0043 §A3/§A4): fetched
    // fresh from `meta` at this exact call — mirrors `index_drain::
    // seal_now`'s own `ctx.effective_metadata()` re-fetch on every call.
    // Deliberately NOT `group.range`, a static field this harness never
    // updates after `start_group`: using it here would silently desync from
    // whatever the calling scenario has since done to `meta` (a
    // `SplitTablet` apply), which is exactly the staleness this CAS exists
    // to catch, not to reintroduce via the test's own scripting shortcut.
    let expected_range = meta
        .tablets
        .get(&group.id)
        .map_or_else(KeyRange::whole, |t| t.range.clone());
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
        object_id: seg_id,
        expected_range,
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
    for (_epoch, row) in meta.stream_shard_chain(TABLE, LABEL, group.id) {
        // Ledger-named-object amendment: resolve from the row, never
        // recompute `segment_id` — mirrors `dynamo_streams::get_records_
        // sealed`'s own fix.
        let seg_id = row.object_id.as_str();
        let bytes = block_on(store.get(seg_id))
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

// --- cell 3b: split_then_parent_seals_first (PR1 frozen-basis bugfix) ----

/// PR1 bugfix regression (ADR 0042 §8/ADR 0043 §A4/§A6) — the **inverse** of
/// `scenario_split_mid_stream`'s deliberate ordering above. That scenario
/// seals the parent *before* splitting ("so the split boundary lands
/// cleanly on a sealed watermark") and seals the *sibling* first
/// afterward, with its own comment naming exactly why: `stream_shard_
/// parent_id`/`effective_stream_shard_watermark` used to be derived live
/// from the parent's *current* chain, so letting the parent seal again
/// after the split would retroactively move what "the parent's last sealed
/// shard" means out from under an already-answered child. **A test comment
/// acknowledging a derivation's time-dependency is a signal to fix the
/// derivation, not to order the test around it** — this cell is that fix's
/// own regression test, driving the ordering the old comment had to avoid.
///
/// Setup: a non-empty, still-**unsealed** backlog on both sides of the
/// future split boundary — the parent never seals before splitting at all.
/// After the split, the **parent** seals its own narrowed scope again
/// first (its `hlc_range.1` necessarily lands above every pre-split HLC,
/// since the shared engine's HLC/version space — ADR 0018 §2's amendment —
/// only ever advances, never resets per tablet), and only *then* does the
/// **sibling** seal its own epoch 0.
///
/// Before the PR1 fix, `effective_stream_shard_watermark(sibling)` walked
/// to the parent's live chain and picked up that inflated post-split
/// watermark, so the sibling's first seal silently filtered its entire
/// inherited backlog out (`hlc <= watermark`) — and since the *hot-tail*
/// read path uses the identical effective watermark, the backlog was never
/// delivered via the open shard either: a total, silent loss.
/// `verify_lineage`'s write-journal diff catches exactly this (delivered
/// count < journal count). After the fix (the frozen `stream_split_basis`,
/// `None` here since the parent had never sealed anything before the
/// split), the sibling's watermark never moves regardless of what the
/// parent seals afterward, and every record is delivered exactly once.
fn scenario_split_then_parent_seals_first(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    create_tablet(&mut meta, TabletId(11), KeyRange::whole());
    let parent = start_group(&sim, &engines, TabletId(11), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    // Left- and right-side backlog, still unsealed at split time — unlike
    // `scenario_split_mid_stream`, the parent never seals before splitting.
    for i in 0..4 {
        write_and_journal(&mut sim, &parent, &live, &mut journal, &key(i), b"L", seed);
    }
    for i in 600..604 {
        write_and_journal(&mut sim, &parent, &live, &mut journal, &key(i), b"R", seed);
    }

    let parent_epoch = meta
        .tablets
        .get(&parent.id)
        .map_or(animus_tablet::Epoch::INITIAL, |t| t.epoch);
    let sibling_id = TabletId(12);
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

    // More left-side writes on the parent's own narrowed scope, then seal
    // the PARENT first — its `hlc_range.1` lands strictly above every
    // pre-split HLC.
    for i in 4..8 {
        write_and_journal(&mut sim, &parent, &live, &mut journal, &key(i), b"L2", seed);
    }
    let parent_leader = elect(&mut sim, &parent, &live, seed);
    let sealed = seal_now(&mut meta, &store, &parent, parent_leader, 2_000, false);
    assert_eq!(
        sealed,
        Some(0),
        "[seed={seed}] the parent's own first post-split seal must apply"
    );

    // A couple more right-side writes on the sibling, THEN the sibling
    // seals its own epoch 0 — last, and only now. Its inherited pre-split
    // backlog (600..603) must still be in it.
    for i in 610..612 {
        write_and_journal(
            &mut sim,
            &sibling,
            &live,
            &mut journal,
            &key(i),
            b"R2",
            seed,
        );
    }
    let sibling_leader = elect(&mut sim, &sibling, &live, seed);
    // Deliberately not asserted against `Some(0)` here: under the
    // unfixed code this call can legitimately return `None` (nothing left
    // past the — wrongly inflated — watermark to seal), which is itself
    // part of the loss this cell exists to catch. `verify_lineage` below is
    // the real assertion either way.
    let _ = seal_now(&mut meta, &store, &sibling, sibling_leader, 2_100, false);

    // Parent-before-child: the lineage discipline itself. This is where an
    // unfixed `effective_stream_shard_watermark` shows up as a diff: the
    // 600..603 backlog present in `journal` but never delivered by either
    // shard.
    verify_lineage(
        &meta,
        &store,
        &[(&parent, parent_leader), (&sibling, sibling_leader)],
        &journal,
        seed,
    );
}

#[test]
fn split_then_parent_seals_first() {
    for_each_seed(
        "split_then_parent_seals_first",
        scenario_split_then_parent_seals_first,
    );
}

// --- cell 3c: split_then_parent_reseals_before_scope_narrows (DUPLICATION,
// INVESTIGATION ONLY — proves the mechanism, not yet a fix) ----------------

/// Reproduces a DISTINCT bug from `scenario_split_then_parent_seals_first`'s
/// PR1 loss fix above — the **mirror-image DUPLICATION direction**, found
/// 2026-08-15 by the D8 e2e diagnostic
/// (`animusd/tests/streams_e2e.rs::auto_split_mid_stream_with_live_consumer_across_every_node`).
///
/// Every existing split cell in this file (`scenario_split_mid_stream`,
/// `scenario_split_then_parent_seals_first`) calls `n.narrow_scope(..)` on
/// the parent's own nodes **synchronously with, in fact strictly before,**
/// applying `MetaCommand::SplitTablet` to `meta` — modelling the reconciler's
/// local scope-narrow action as if it always lands atomically with the
/// control-plane split commit. **In production it does not**: `SplitTablet`
/// commits only to the control Raft (`Metadata`); each node's own
/// `RaftKvNode::narrow_scope` is a *separate*, purely local, un-replicated
/// action (`animus_cp_data::host::HostAction::NarrowScope`, `host.rs`'s
/// `plan()`), applied only when that node's own tablet-host reconciler next
/// notices the metadata change (event-driven watch or a 500ms fallback
/// tick). Nothing synchronizes that against the parent's own seal arm's next
/// tick (`animusd::index_drain::seal_tick`, every 200ms) — this cell drives
/// exactly that window.
///
/// Setup mirrors `scenario_split_then_parent_seals_first` (unsealed backlog
/// on both sides of the future boundary, parent never seals before
/// splitting) but after applying `SplitTablet` — freezing
/// `stream_split_basis` for the sibling, per PR1/#216 — the parent's own
/// nodes' live `StorageScope` is left WIDE across its own next seal.
/// `RaftKvNode::pending_changes` (mirrored here by this file's own
/// `seal_now`/`Group::nodes[..]`) is a raw scan bounded by that live scope
/// alone, not by anything metadata-aware (`animus-cp-data/src/lib.rs`'s
/// `pending_changes`/`StorageScope::physical_bounds`) — so the parent's seal
/// physically picks up the right-side backlog that, per the split just
/// committed, already belongs to the sibling's range. Only *afterward* does
/// this cell narrow the parent's scope (modelling the reconciler catching
/// up) and start the sibling.
///
/// The sibling's own first seal reads its frozen `stream_split_basis`
/// watermark — frozen at the instant of the split, strictly BEFORE the
/// parent's racing seal above ever ran, so it has no way to know that seal
/// already covered part of the backlog the sibling physically inherited
/// (ADR 0028: no data movement, same shared engine). Its own
/// `pending_changes` scan finds those same physical records still present
/// (sealing never deletes — only `trim_janitor` does, gated on the
/// **sibling's own** watermark, which hasn't advanced yet) and re-seals them
/// into its own epoch 0: the same packed HLC delivered by both the parent's
/// epoch **and** the sibling's epoch 0. `verify_lineage`'s `seen_hlcs` set
/// catches this directly ("delivered more than once — violates
/// exactly-once").
///
/// **This cell is expected to FAIL on current `main` (post-#216) — that is
/// the point.** See `docs/engineering-lessons.md`'s and the
/// `split-seal-duplication-bug` investigation memory for the root-cause
/// writeup; no fix has landed yet, so this stays a `#[should_panic]`-free
/// regression-in-waiting rather than a `#[test]` the gates run green today.
/// A fix should make this pass by construction, not by loosening the
/// assertion.
#[allow(clippy::too_many_lines)]
fn scenario_split_then_parent_reseals_before_scope_narrows(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    create_tablet(&mut meta, TabletId(21), KeyRange::whole());
    let parent = start_group(&sim, &engines, TabletId(21), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    // Left- and right-side backlog, still unsealed at split time — like
    // `scenario_split_then_parent_seals_first`, the parent never seals
    // before splitting.
    for i in 0..4 {
        write_and_journal(&mut sim, &parent, &live, &mut journal, &key(i), b"L", seed);
    }
    for i in 600..604 {
        write_and_journal(&mut sim, &parent, &live, &mut journal, &key(i), b"R", seed);
    }

    // The control-plane split commits — freezing the sibling's
    // `stream_split_basis` from the parent's pre-mutation state (`None`,
    // since the parent has never sealed) — but the parent's own nodes' live
    // scope is deliberately left WIDE here: the reconciler hasn't caught up
    // yet, the real-world window this cell exists to prove.
    let parent_epoch = meta
        .tablets
        .get(&parent.id)
        .map_or(animus_tablet::Epoch::INITIAL, |t| t.epoch);
    let sibling_id = TabletId(22);
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

    // The parent's seal arm races the (not-yet-run) reconciler: it seals
    // NOW, before its own scope has narrowed, so `pending_changes()` still
    // returns the right-side (600..603) backlog too — records that, per the
    // split metadata already committed above, belong to the sibling.
    let parent_leader = elect(&mut sim, &parent, &live, seed);
    let sealed = seal_now(&mut meta, &store, &parent, parent_leader, 2_000, false);
    assert_eq!(
        sealed,
        Some(0),
        "[seed={seed}] the parent's racing seal (still on its wide scope) must apply"
    );

    // Only NOW does the reconciler catch up: narrow the parent's own nodes
    // and start the sibling with its correctly narrow scope from birth —
    // exactly like every other split cell does, just too late to matter.
    for n in &parent.nodes {
        n.narrow_scope(KeyRange::new(Vec::new(), Some(BOUNDARY.to_vec())));
    }
    let sibling = start_group(
        &sim,
        &engines,
        sibling_id,
        KeyRange::new(BOUNDARY.to_vec(), None),
    );
    sim.run_for(Duration::from_secs(2));

    // The sibling seals its own epoch 0 — its frozen watermark predates the
    // parent's racing seal above, so it can't know that seal already
    // covered part of what it's about to re-discover on the shared engine.
    let sibling_leader = elect(&mut sim, &sibling, &live, seed);
    let _ = seal_now(&mut meta, &store, &sibling, sibling_leader, 2_100, false);

    // Parent-before-child, exactly as every other cell in this file checks
    // it: this is where the duplication surfaces as a `seen_hlcs` collision
    // ("delivered more than once"), not as a count mismatch — the total
    // count would actually look inflated (over, not under), the D8 e2e
    // symptom's own signature.
    verify_lineage(
        &meta,
        &store,
        &[(&parent, parent_leader), (&sibling, sibling_leader)],
        &journal,
        seed,
    );
}

#[test]
fn split_then_parent_reseals_before_scope_narrows() {
    for_each_seed(
        "split_then_parent_reseals_before_scope_narrows",
        scenario_split_then_parent_reseals_before_scope_narrows,
    );
}

// --- cell 3d: split_then_parent_seals_against_stale_cached_metadata (the
// SECOND staleness layer, ADR 0043 §A3/§A4 CAS amendment) -----------------

/// The metadata-CACHE-staleness sibling of `scenario_split_then_parent_
/// reseals_before_scope_narrows` above: that cell modelled the PHYSICAL
/// scope lagging a split (`RaftKvNode::narrow_scope`'s own local, un-
/// replicated lag); this one models the DIFFERENT staleness layer found
/// while verifying that cell's own fix in production (the D8 e2e
/// diagnostic, 2026-08-15) — the sealer's own `Metadata` READ (`ctx.
/// effective_metadata()`, backed by the ADR 0038 async apply-task cache)
/// can ITSELF be stale relative to the true, already-committed
/// `SplitTablet` on the SAME node, independent of whether the physical
/// scope has narrowed. A metadata-range fence computed against a stale
/// read (Fork A, `in_declared_range`) cannot see past this: the fence and
/// the tablet's own physical scope can both agree with each other while
/// BOTH being stale relative to the SAME just-committed split. Only an
/// apply-time check against the TRUE, sequentially-applied state (never a
/// cache) closes it — this cell proves exactly that check
/// (`Metadata::apply`'s `SealStreamShard` `expected_range` CAS).
///
/// Script: clone `meta` into `stale_view` BEFORE the split (the sealer's
/// own cached snapshot); apply `SplitTablet` only to the AUTHORITATIVE
/// `meta`; compute the parent's seal candidates/watermark/`expected_range`
/// entirely from `stale_view` (still wide) but PROPOSE the resulting
/// command against the AUTHORITATIVE `meta` — exactly the sequence a real
/// node's seal arm produces when its own metadata cache hasn't caught up
/// to a split the control Raft has already committed. Without the CAS,
/// this proposal applies, duplicating whatever the sibling later seals;
/// with it, `Metadata::apply` sees the TRUE post-split range and rejects
/// it outright, regardless of what `stale_view` believed — proven by
/// temporarily short-circuiting the CAS check (red) and restoring it
/// (green), the same "teeth proof" technique `negative_control.rs`-style
/// corpora use elsewhere in this repo.
fn scenario_split_then_parent_seals_against_stale_cached_metadata(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    create_tablet(&mut meta, TabletId(31), KeyRange::whole());
    let parent = start_group(&sim, &engines, TabletId(31), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    // Left- and right-side backlog, unsealed at split time.
    for i in 0..4 {
        write_and_journal(&mut sim, &parent, &live, &mut journal, &key(i), b"L", seed);
    }
    for i in 600..604 {
        write_and_journal(&mut sim, &parent, &live, &mut journal, &key(i), b"R", seed);
    }

    // The sealer's own cached snapshot, taken BEFORE the split — stands in
    // for a node whose ADR 0038 apply-task cache hasn't caught up to the
    // (about to commit) split yet.
    let stale_view = meta.clone();

    // The control-plane split commits, on the AUTHORITATIVE view only.
    let parent_epoch = meta
        .tablets
        .get(&parent.id)
        .map_or(animus_tablet::Epoch::INITIAL, |t| t.epoch);
    let sibling_id = TabletId(32);
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

    // The parent's seal arm runs entirely against its STALE cached view:
    // watermark, candidate records (the group's own physical scope is ALSO
    // still wide — this scenario never calls `narrow_scope` at all,
    // modelling the same node not having caught up on either axis), and
    // `expected_range` all come from `stale_view`, never the authoritative
    // `meta`.
    let parent_leader = elect(&mut sim, &parent, &live, seed);
    let stale_watermark = stale_view
        .effective_stream_shard_watermark(parent.id)
        .unwrap_or(0);
    let mut records: Vec<(Vec<u8>, u64, Vec<u8>)> =
        block_on(parent.nodes[parent_leader].pending_changes())
            .into_iter()
            .filter_map(|(k, v)| {
                let hlc = record_hlc_suffix(&k)?;
                (hlc > stale_watermark).then_some((k, hlc, v))
            })
            .collect();
    records.sort_by_key(|(_, hlc, _)| *hlc);
    assert!(
        !records.is_empty(),
        "[seed={seed}] the parent's stale-view scan must still see the full pre-split backlog"
    );
    let stale_epoch = current_open_epoch(&stale_view, parent.id);
    let stale_hlc_range = (stale_watermark, records.last().expect("checked above").1);
    let stale_count = records.len() as u64;
    let seg_records: Vec<segment::SegmentRecord> = records
        .iter()
        .map(|(k, hlc, v)| segment::SegmentRecord {
            source_key: k.clone(),
            packed_hlc: *hlc,
            change_record: v.clone(),
        })
        .collect();
    let header = segment::SegmentHeader {
        table: TABLE.into(),
        label: LABEL.into(),
        shard_id: segment::shard_id(parent.id.0, stale_epoch),
        tablet: parent.id.0,
        epoch: stale_epoch,
        parent_shard_id: stale_view.stream_shard_parent_id(parent.id, stale_epoch),
        hlc_range: stale_hlc_range,
        count: stale_count,
        seal_wall_ms: 2_000,
    };
    let bytes = segment::encode(&header, &seg_records);
    let env = parent.nodes[parent_leader].env();
    let seg_id = segment::segment_object_id(
        TABLE,
        LABEL,
        parent.id.0,
        stale_epoch,
        env.node_id().as_str(),
        parent.nodes[parent_leader].term(),
        env.next_u64(),
    );
    block_on(store.put(&seg_id, &bytes)).expect("segment store put succeeds regardless");

    // The proposal itself, built ENTIRELY from the stale view — including
    // its own `expected_range` (still `whole()`, the pre-split range) —
    // but applied against the AUTHORITATIVE `meta`, which has already
    // committed the split. This is the exact sequence a real racing node
    // produces.
    let stale_range = stale_view
        .tablets
        .get(&parent.id)
        .map_or_else(KeyRange::whole, |t| t.range.clone());
    let outcome = meta.apply(&MetaCommand::SealStreamShard {
        table: TABLE.into(),
        label: LABEL.into(),
        tablet: parent.id,
        epoch: stale_epoch,
        view_type: StreamViewType::NewAndOldImages,
        hlc_range: stale_hlc_range,
        count: stale_count,
        seal_wall_ms: 2_000,
        replicas: Vec::new(),
        object_id: seg_id,
        expected_range: stale_range,
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Rejected(
            "declared range stale — a split raced this seal, retry with the current range"
        ),
        "[seed={seed}] the parent's stale-metadata-view seal must be rejected by the range CAS"
    );
    assert!(
        !meta.stream_shards.contains_key(&(parent.id, stale_epoch)),
        "[seed={seed}] a rejected seal must never land in the catalog"
    );

    // The reconciler catches up: narrow the parent's own PHYSICAL scope now
    // (mirroring `narrow_scope`'s real, separate, un-replicated timing).
    // This scenario is deliberately isolating the metadata-CACHE-staleness
    // layer/CAS above from the DIFFERENT, already-known, accepted hot_read
    // open-tail residual (ADR 0043's own doc amendment) — without this, the
    // parent's own still-wide-physical-scope hot tail would independently
    // re-surface the right-side backlog via `collect_tablet_records`'
    // unbounded watermark-only read below, which is that OTHER residual,
    // not the one this cell exists to prove.
    for n in &parent.nodes {
        n.narrow_scope(KeyRange::new(Vec::new(), Some(BOUNDARY.to_vec())));
    }

    // Only NOW does the sibling get hosted, with the authoritative
    // (already-split) metadata — its own seal reads fresh, correctly-
    // scoped state throughout, and seals its own epoch 0 exactly once.
    let sibling = start_group(
        &sim,
        &engines,
        sibling_id,
        KeyRange::new(BOUNDARY.to_vec(), None),
    );
    sim.run_for(Duration::from_secs(2));
    let sibling_leader = elect(&mut sim, &sibling, &live, seed);
    let sealed = seal_now(&mut meta, &store, &sibling, sibling_leader, 2_100, false);
    assert_eq!(
        sealed,
        Some(0),
        "[seed={seed}] the sibling's own first seal must apply"
    );

    // The parent gets a fresh chance to seal its OWN left-side backlog,
    // this time with the shared `seal_now` helper's own fresh (and
    // correctly-scoped) read — proving the rejection above was a genuine
    // retry-next-tick, never a permanent wedge.
    let parent_leader2 = elect(&mut sim, &parent, &live, seed);
    let resealed = seal_now(&mut meta, &store, &parent, parent_leader2, 2_200, false);
    assert_eq!(
        resealed,
        Some(0),
        "[seed={seed}] the parent's own retry, now correctly scoped, must apply"
    );

    // Exactly-once end to end: the left-side backlog sealed exactly once
    // (the parent's correctly-scoped retry, never the rejected stale
    // attempt), the right-side backlog exactly once (the sibling) — no
    // duplication survives.
    verify_lineage(
        &meta,
        &store,
        &[(&parent, parent_leader2), (&sibling, sibling_leader)],
        &journal,
        seed,
    );
}

#[test]
fn split_then_parent_seals_against_stale_cached_metadata() {
    for_each_seed(
        "split_then_parent_seals_against_stale_cached_metadata",
        scenario_split_then_parent_seals_against_stale_cached_metadata,
    );
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
        object_id: format!("{TABLE}/{LABEL2}/{}/test", group.id.0),
        // No `CreateTablet` in this scenario — permissive (absent tablet).
        expected_range: KeyRange::whole(),
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

// --- regression: dueling seals for the same epoch (ledger-named-object
// amendment, ADR 0042 §10/ADR 0043 §A3, 2026-08-15) ----------------------
//
// **Root-cause repro, not a frozen corpus cell** — ported from
// `investigate/stream-seal-loss` (seed 849844469346351525, "delivered 5 !=
// journal 10" on unmodified main), which found the byte-seal record-loss
// bug this amendment fixes. Deliberately NOT wired into
// `for_each_seed`/the frozen scenario set — it is a hand-scripted proof of
// one exact interleaving, not a fault-injection cell a seed sweep discovers
// on its own (this harness's own `seal_now` helper reads and applies
// against the *same* `&mut Metadata` reference every call, so no seed of
// *that* harness can express "two attempts computed from two different
// metadata snapshots").
//
// **The mechanism**: two independent seal attempts for the tablet's SAME
// open epoch — exactly what a brief dual-leadership window would produce
// (a re-election triggered by write-burst backpressure on the very node
// driving the seal; see `animus-cp-data/CLAUDE.md`'s "leader-election
// storm" driver-liveness entry for why heavy write load is exactly what
// induces this). `Metadata::apply`'s `SealStreamShard` arm always correctly
// protected the **catalog**: a second proposal for an already-recorded
// `(tablet, epoch)` whose content differs from what's already committed is
// rejected outright as `ApplyOutcome::NoOp` — first-committer-wins.
//
// **What used to protect nothing, before this amendment**: the *segment
// object* physically stored at the deterministic `segment_id(table, label,
// tablet, epoch)` — `SegmentStore::put`/`put_sealed` was an unconditional,
// un-ordered overwrite, so whichever attempt's `put` landed chronologically
// LAST won the physical bytes, independent of which attempt's *proposal*
// won the catalog's first-committer-wins rule. When the later-landing `put`
// carried a SMALLER range than the catalog's own committed one, the gap was
// silently, permanently lost. **The fix**: every attempt now writes at its
// own unique id ([`segment::segment_object_id`]), so the two attempts'
// physical writes can never collide at all — the store enforces write-once
// per id, and a losing attempt's own object simply becomes a harmless,
// uncataloged orphan rather than physically overwriting the winner.
//
// Below: attempt "slow" computes its own record set from a metadata/
// pending-changes snapshot taken BEFORE a second batch of writes lands
// (mirroring a leader whose own `put_sealed` — a real K-way replicated
// write — takes long enough for a second, independent leader to fully
// seal, catalog-commit, and move on before the first's `put` physically
// arrives) and mints its OWN unique object id right then, exactly as a real
// `seal_now` call would. Attempt "fast" runs the ordinary, complete
// `seal_now` sequence against the live metadata (sees both batches), wins
// the catalog with its OWN unique id. "Slow"'s own (late) `put` now lands
// at its own distinct id — never touching the winner's object — and its
// own catalog proposal is (correctly) rejected as a `NoOp` (its `object_id`
// alone already differs from what's committed, on top of the differing
// `hlc_range`). `verify_lineage` (the same decision tree
// `DescribeStream`/`GetShardIterator(TRIM_HORIZON)`/`GetRecords` walks in
// production) now finds **zero** loss — all 10 journaled records delivered
// — proving the fix: this is the identical interleaving that produced
// "delivered 5 != journal 10" before the amendment.
fn scenario_dueling_seals_orphan_hot_range(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    let group = start_group(&sim, &engines, TabletId(20), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    // Batch 1: what the "slow" (about-to-lose-leadership) attempt sees.
    for i in 0..5 {
        write_and_journal(
            &mut sim,
            &group,
            &live,
            &mut journal,
            &key(i),
            b"batch1",
            seed,
        );
    }
    let slow_leader = elect(&mut sim, &group, &live, seed);

    // The slow attempt's own snapshot, taken now — mirrors
    // `index_drain::seal_now`'s own sequence up through its `pending_
    // changes()` read (`animusd/src/index_drain.rs:944-960`), before its
    // (real, K-way replicated, hence slow) `put_sealed` call.
    let watermark_slow = meta.effective_stream_shard_watermark(group.id).unwrap_or(0);
    let mut slow_records: Vec<(Vec<u8>, u64, Vec<u8>)> =
        block_on(group.nodes[slow_leader].pending_changes())
            .into_iter()
            .filter_map(|(k, v)| {
                let hlc = record_hlc_suffix(&k)?;
                (hlc > watermark_slow).then_some((k, hlc, v))
            })
            .collect();
    slow_records.sort_by_key(|(_, hlc, _)| *hlc);
    assert_eq!(
        slow_records.len(),
        5,
        "[seed={seed}] the slow attempt's own snapshot must see exactly batch 1"
    );
    // The slow attempt mints its OWN unique object id right now, exactly as
    // a real `seal_now` call would (ledger-named-object amendment) —
    // captured before the fast attempt's later election, so a term read
    // later would no longer reflect this moment.
    let slow_epoch = 0u64;
    let slow_env = group.nodes[slow_leader].env();
    let slow_term = group.nodes[slow_leader].term();
    let slow_seg_id = segment::segment_object_id(
        TABLE,
        LABEL,
        group.id.0,
        slow_epoch,
        slow_env.node_id().as_str(),
        slow_term,
        slow_env.next_u64(),
    );

    // Batch 2 lands while the slow attempt's `put_sealed` is modeled as
    // still in flight (its own snapshot above is already taken and won't
    // change) — a second leader takes over and drives a genuinely
    // independent, complete seal.
    for i in 5..10 {
        write_and_journal(
            &mut sim,
            &group,
            &live,
            &mut journal,
            &key(i),
            b"batch2",
            seed,
        );
    }

    // The "fast" attempt: an ordinary, complete `seal_now` against live
    // metadata — sees both batches, and is the only proposal that has
    // landed when the catalog resolves this epoch, so it wins outright.
    let fast_leader = elect(&mut sim, &group, &live, seed);
    let sealed_fast = seal_now(&mut meta, &store, &group, fast_leader, 1_000, false);
    assert_eq!(
        sealed_fast,
        Some(0),
        "[seed={seed}] the fast attempt seals epoch 0 covering both batches"
    );
    let fast_watermark = meta
        .effective_stream_shard_watermark(group.id)
        .expect("fast attempt just committed a watermark");
    let fast_seg_id = meta.stream_shards[&(group.id, 0)].object_id.clone();

    // The slow attempt's `put_sealed` finally lands — using the bytes it
    // computed BEFORE batch 2 ever existed — landing at its OWN unique id
    // (minted above), never the winner's (the ledger-named-object
    // amendment: no two attempts ever share a storage key, so there is
    // nothing for this late `put` to overwrite).
    let epoch = slow_epoch;
    let hlc_range_slow = (watermark_slow, slow_records.last().unwrap().1);
    let count = slow_records.len() as u64;
    let seg_records: Vec<segment::SegmentRecord> = slow_records
        .iter()
        .map(|(k, hlc, v)| segment::SegmentRecord {
            source_key: k.clone(),
            packed_hlc: *hlc,
            change_record: v.clone(),
        })
        .collect();
    let header = segment::SegmentHeader {
        table: TABLE.into(),
        label: LABEL.into(),
        shard_id: segment::shard_id(group.id.0, epoch),
        tablet: group.id.0,
        epoch,
        parent_shard_id: meta.stream_shard_parent_id(group.id, epoch),
        hlc_range: hlc_range_slow,
        count,
        seal_wall_ms: 900, // the slow attempt started first, in wall-clock terms
    };
    let bytes = segment::encode(&header, &seg_records);
    block_on(store.put(&slow_seg_id, &bytes))
        .expect("the slow attempt's late put still succeeds, at its own unique id");
    assert_ne!(
        slow_seg_id, fast_seg_id,
        "[seed={seed}] the two independently-computed attempts must never share a storage id"
    );

    // The slow attempt's own catalog proposal, now that it finally gets
    // around to it: its OWN `object_id` alone already differs from what's
    // committed (on top of the differing `hlc_range`), so `Metadata::
    // apply`'s first-committer-wins rule correctly rejects it as a `NoOp`
    // — the catalog itself is proven intact here, exactly as before the
    // amendment.
    let slow_outcome = meta.apply(&MetaCommand::SealStreamShard {
        table: TABLE.into(),
        label: LABEL.into(),
        tablet: group.id,
        epoch,
        view_type: StreamViewType::NewAndOldImages,
        hlc_range: hlc_range_slow,
        count,
        seal_wall_ms: 900,
        replicas: Vec::new(),
        object_id: slow_seg_id,
        // No `CreateTablet` in this scenario at all — the range CAS reads
        // as permissive (absent tablet); `whole()` is never actually
        // checked, same as this scenario's other unaffected assertions.
        expected_range: KeyRange::whole(),
    });
    assert_eq!(
        slow_outcome,
        ApplyOutcome::NoOp,
        "[seed={seed}] the catalog must reject the slow attempt's differing content"
    );
    assert_eq!(
        meta.effective_stream_shard_watermark(group.id),
        Some(fast_watermark),
        "[seed={seed}] the catalog's committed watermark must be untouched by the rejected proposal"
    );

    // The fix: `verify_lineage` now finds ZERO loss. Before the
    // ledger-named-object amendment, the slow attempt's late `put` landed
    // at the SAME deterministic id the fast attempt had just won,
    // physically overwriting it with a smaller record set — batch 2's 5
    // records ended up claimed by the catalog's own committed range but
    // absent from the (now too-small) segment object, and excluded from the
    // open tail by the advanced watermark: "delivered 5 != journal 10" on
    // unmodified main. With unique per-attempt ids there is nothing left
    // for the slow attempt's write to overwrite — its object is a harmless,
    // permanent orphan the segment janitor's own sweep reclaims — and every
    // one of the 10 journaled records is delivered.
    verify_lineage(&meta, &store, &[(&group, fast_leader)], &journal, seed);
}

#[test]
fn dueling_seals_orphan_hot_range() {
    for_each_seed(
        "dueling_seals_orphan_hot_range",
        scenario_dueling_seals_orphan_hot_range,
    );
}
