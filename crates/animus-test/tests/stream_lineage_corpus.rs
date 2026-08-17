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
use animus_cp_data::{KIND_BASE, RaftKvNode, StorageScope, TxnOutcome, TxnWrite, segment};
use animus_env::{Env, EnvExt, Nanos, Rng, SegmentStore, nid};
use animus_sim::{SimEnv, SimSegmentStore, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId, TabletState};
use futures::executor::block_on;
use std::sync::{Arc, Mutex};

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

/// A tablet's 3-replica Raft group. Since ADR 0050 rung 1/2 an engine is
/// **private to one tablet** (keys are `kind || logical`, no table/tablet
/// prefix) — so every scenario passes each group its OWN fresh `engines()`
/// map; two groups sharing one engine would collide byte-identically now
/// (the pre-pivot "one shared engine per node" comment this replaces
/// described ADR 0028's world).
struct Group {
    id: TabletId,
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
                StorageScope::new(range.clone()),
                id.0,
            )
        })
        .collect();
    Group { id, nodes }
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
    match group.nodes[leader].put_kind_batch_conditioned(
        vec![(KIND_BASE, item_key.to_vec(), Some(record.to_vec()))],
        vec![(item_key.to_vec(), record.to_vec())],
        Vec::new(),
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

/// Runs `fut` to completion by spawning it on `env` and driving `sim` in
/// small steps until it resolves or `attempts` steps elapse — the same
/// spawn-and-poll idiom `animus-cp-data/tests/txn_multi.rs`'s/
/// `txn_serializable.rs`'s own `drive` helpers use for exactly the same
/// reason: `RaftKvNode`'s async txn methods poll internally via
/// `env.sleep`, so a bare `block_on` would hang forever with nothing
/// advancing the simulator concurrently.
fn drive<T: Send + 'static>(
    sim: &mut Simulator,
    env: &SimEnv,
    fut: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let s = Arc::clone(&slot);
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

/// As [`write_and_journal`], but through the full cross-tablet 2PC path
/// (`txn_stage_anchor` -> `txn_commit_at_least` -> `txn_resolve`, ADR 0018
/// §2) instead of a direct `KindBatch` — ADR 0046 A1/U3, `TxnStage`
/// kind-writes stack PR3 corpus extension: a transactional write's change
/// record must reach the shard lineage exactly once and in order, exactly
/// like a plain `KindBatch` write's (`materialize_derived` is the ONE
/// shared helper both paths use — see `animus-cp-data/tests/
/// txn_kind_writes.rs`'s own byte-identical-helper proof at the primitive
/// level; this proves it end to end through a sealed/open shard lineage
/// walk too). Single-tablet, single-participant (this corpus's tablet
/// groups are independent of `txn_serializable.rs`'s own multi-tablet
/// coordinator) — `item_key` is both the base write and (via `change_log`)
/// the journaled record, mirroring `propose_write`'s identical convention.
fn write_txn_and_journal(
    sim: &mut Simulator,
    group: &Group,
    live: &[usize],
    journal: &mut BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
    item_key: &[u8],
    payload: &[u8],
    seed: u64,
) {
    let leader = elect(sim, group, live, seed);
    let node = group.nodes[leader].clone();
    let env = node.env().clone();
    let write = TxnWrite {
        key: item_key.to_vec(),
        value: Some(payload.to_vec()),
        kind_writes: Vec::new(),
        change_log: Some((item_key.to_vec(), payload.to_vec())),
        stage_marker: None,
    };
    let n = node.clone();
    let (txn_id, record_key, outcome) = drive(sim, &env, async move {
        n.txn_stage_anchor(TABLE, vec![write], Vec::new(), Vec::new())
            .await
    })
    .unwrap_or_else(|| panic!("txn stage failed (seed={seed})"));
    assert_eq!(
        outcome,
        animus_cp_data::StageOutcome::Staged,
        "[seed={seed}] txn stage did not land"
    );

    let n = node.clone();
    let (txn_id_c, record_key_c) = (txn_id.clone(), record_key.clone());
    let commit_ts = drive(sim, &env, async move {
        n.txn_commit_at_least(txn_id_c, record_key_c, txn_id.ts)
            .await
    })
    .unwrap_or_else(|| panic!("txn commit failed (seed={seed})"));

    let n = node.clone();
    let item_key_owned = item_key.to_vec();
    drive(sim, &env, async move {
        n.txn_resolve(
            txn_id,
            record_key,
            vec![item_key_owned],
            TxnOutcome::Committed { commit_ts },
        )
        .await
    });

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
    let watermark = meta.stream_shard_watermark(group.id).unwrap_or(0);
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
    let watermark = meta.stream_shard_watermark(group.id).unwrap_or(0);
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

fn key(i: usize) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

/// An 8-byte-minimum key for the transactional write path only —
/// `RaftKvNode::txn_stage_anchor`'s own anchor-key assert requires a full
/// ADR 0022 partition token (`key`'s own 5-byte `"k0000"` shape is too
/// short); every other cell here never goes through `TxnStage` at all, so
/// `key`'s shorter form stays untouched.
fn txn_key(i: usize) -> Vec<u8> {
    format!("txnkey{i:04}").into_bytes()
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

// TOMBSTONE (ADR 0050 Train B rung 2): five split cells died here —
// `split_mid_stream`, `split_then_parent_seals_first` (the #216
// frozen-basis loss regression), `split_then_parent_reseals_before_scope_
// narrows` + `split_then_parent_seals_against_stale_cached_metadata` (the
// #220 duplication regressions, physical-scope-lag and metadata-cache-lag
// layers), and `combined_chaos`. All modeled the ZERO-COPY split's
// in-place lineage: `narrow_scope` on the parent's own nodes + a sibling
// group inheriting the same physical rows — `narrow_scope` no longer
// exists (immutable ranges), sibling tablets no longer share engines, and
// the machinery those cells regression-tested (`stream_split_basis`
// inheritance, `in_declared_range`, the `SealStreamShard` range-CAS) is
// production-unreachable while the old split is disabled and is deleted
// outright in the Train B sweep. The copy-based split's own lineage cells
// (children born with EMPTY change logs, parent's final seal at freeze,
// `split_lineage` frozen at cutover) replace them in the cutover rungs.
// Pre-pivot cells retrievable from git history.

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
            .all(|hlc| hlc <= meta.stream_shard_watermark(group.id).unwrap_or(0)),
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
    let watermark = meta.stream_shard_watermark(group.id).unwrap_or(0);
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
    let watermark_slow = meta.stream_shard_watermark(group.id).unwrap_or(0);
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
        .stream_shard_watermark(group.id)
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
    });
    assert_eq!(
        slow_outcome,
        ApplyOutcome::NoOp,
        "[seed={seed}] the catalog must reject the slow attempt's differing content"
    );
    assert_eq!(
        meta.stream_shard_watermark(group.id),
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

// --- cell 12: transactional_writes_exactly_once_and_ordered -------------
// ADR 0046 A1/U3 (`TxnStage` kind-writes stack, PR3 corpus extension): a
// transactionally-committed write's change record must survive the exact
// same exactly-once/per-item-order/lineage-continuity guarantees as a
// plain `KindBatch` write's — proven under a leader-kill fault injection
// (mirroring `kill_sealing_leader`'s shape) so the claim holds across a
// leadership change mid-stream, not just in the quiet case.

fn scenario_transactional_writes_exactly_once_and_ordered(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta();
    let group = start_group(&sim, &engines, TabletId(30), KeyRange::whole());
    let mut live = vec![0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    // Batch 1: transactional writes, quiet.
    for i in 0..5 {
        write_txn_and_journal(
            &mut sim,
            &group,
            &live,
            &mut journal,
            &txn_key(i),
            b"v0",
            seed,
        );
    }

    // Kill whichever replica currently leads mid-stream (before any seal)
    // and elect among the survivors — the identical fault `kill_sealing_
    // leader` injects, but for the transactional write path instead of
    // `write_and_journal`'s direct `KindBatch`.
    let dying = elect(&mut sim, &group, &live, seed);
    sim.crash(nid(NODES[dying]));
    live.retain(|&i| i != dying);
    let new_leader = elect(&mut sim, &group, &live, seed);

    // Batch 2: more transactional writes, through the post-election leader.
    for i in 5..10 {
        write_txn_and_journal(
            &mut sim,
            &group,
            &live,
            &mut journal,
            &txn_key(i),
            b"v1",
            seed,
        );
    }
    let sealed = seal_now(&mut meta, &store, &group, new_leader, 1_000, false);
    assert_eq!(
        sealed,
        Some(0),
        "[seed={seed}] the post-election leader must still seal correctly from committed \
         transactional writes"
    );

    verify_lineage(&meta, &store, &[(&group, new_leader)], &journal, seed);
}

#[test]
fn transactional_writes_exactly_once_and_ordered() {
    for_each_seed(
        "transactional_writes_exactly_once_and_ordered",
        scenario_transactional_writes_exactly_once_and_ordered,
    );
}

// --- cells 9/10: the copy-based split's lineage (ADR 0050 Train B rung 6) --
//
// The successors the rung-2 tombstone above promises: the ZERO-COPY split
// cells died with `narrow_scope`/the shared engine; these model the
// COPY-BASED workflow's lineage contract instead — children born with
// EMPTY change logs, the parent's final seal capturing its entire backlog
// before cutover, and `split_lineage` frozen at `CutoverSplit` apply.
//
// **Fidelity boundary, stated honestly** (the corpus-green-≠-port lesson):
// the animusd split *driver*'s own sequencing/idempotence is NOT under
// test here — `animusd/tests/split_build.rs`/`freeze.rs` and the cascade
// e2e are that gate. What these cells prove at the primitive level, under
// seed-varied write interleavings, is that the pieces the driver composes
// (the `BeginSplit`/`CutoverSplit` apply arms, the seal path, the frozen
// `split_lineage` parent-id derivation, and per-tablet private engines)
// preserve exactly-once + per-item order across a split — including under
// a seal-attempt crash and a store outage (cell 10). The parent's freeze
// is modeled by simply issuing no further writes to it (the `Freeze`
// command's own refusal mechanics are `animus-cp-data/tests/freeze.rs`'s
// subject, not lineage's).

/// An exactly-8-byte item key for the split cells: `BeginSplit`'s apply arm
/// keeps the F11 token-alignment seatbelt for a streamed table (a split key
/// must sit on its own `TOKEN_BYTES` boundary — ADR 0042 §14), so both the
/// split key and the keys it partitions use a full-token shape here, unlike
/// `key`'s shorter 5-byte form the non-split cells keep.
fn key8(i: usize) -> Vec<u8> {
    format!("sk{i:06}").into_bytes()
}

/// One completed copy-based split of `parent_group` at `split_key`,
/// applied to `meta` the way the driver's endgame does: `BeginSplit` →
/// (writes already stopped) → the parent's FINAL seal (whole backlog) →
/// `CutoverSplit`. Returns the two child ids. Panics (naming the seed) if
/// any step's apply is rejected.
fn complete_copy_split(
    meta: &mut Metadata,
    store: &SimSegmentStore,
    sim: &mut Simulator,
    parent_group: &Group,
    split_key: &[u8],
    wall_ms: u64,
    seed: u64,
) -> (TabletId, TabletId) {
    let left = meta.next_free_tablet_id();
    let right = TabletId(left.0 + 1);
    let replicas: Vec<_> = NODES.iter().copied().map(nid).collect();
    let epoch = meta.tablets[&parent_group.id].epoch;
    let outcome = meta.apply(&MetaCommand::BeginSplit {
        parent: parent_group.id,
        expected_epoch: epoch,
        split_key: split_key.to_vec(),
        children: [(left, replicas.clone()), (right, replicas)],
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "[seed={seed}] BeginSplit must apply"
    );

    // The parent's final seal: everything still pending, one shard. A
    // quiet parent (no unsealed backlog) legitimately seals nothing —
    // the driver's own endgame has the same branch.
    let leader = elect(sim, parent_group, &[0, 1, 2], seed);
    let _ = seal_now(meta, store, parent_group, leader, wall_ms, false);
    let watermark = meta.stream_shard_watermark(parent_group.id).unwrap_or(0);
    let still_pending = block_on(parent_group.nodes[leader].pending_changes())
        .into_iter()
        .filter_map(|(k, _)| record_hlc_suffix(&k))
        .any(|hlc| hlc > watermark);
    assert!(
        !still_pending,
        "[seed={seed}] the final seal must leave nothing past the watermark"
    );

    let epoch = meta.tablets[&parent_group.id].epoch;
    let outcome = meta.apply(&MetaCommand::CutoverSplit {
        parent: parent_group.id,
        expected_epoch: epoch,
        cutover_wall_ms: wall_ms + 1,
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "[seed={seed}] CutoverSplit must apply"
    );
    (left, right)
}

// --- cell 9: copy_split_children_born_empty ------------------------------

/// The copy-based split's core lineage contract (ADR 0050 / ADR 0043 §A4 as
/// rewritten): sealed history + an unsealed backlog on the parent → the
/// parent's final seal captures the whole backlog → cutover freezes
/// `split_lineage` → both children start with **empty** change logs, seal
/// their own epoch 0, and the full walk (parent chain, then each child's)
/// delivers every journaled record exactly once, in per-item order.
fn scenario_copy_split_children_born_empty(seed: u64) {
    let mut sim = Simulator::new(seed);
    let mut meta = base_meta();
    let parent_engines = engines();
    let parent = start_group(&sim, &parent_engines, TabletId(7), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let replicas: Vec<_> = NODES.iter().copied().map(nid).collect();
    let outcome = meta.apply(&MetaCommand::CreateTablet {
        tablet: TabletId(7),
        table: Some(TABLE.into()),
        range: KeyRange::whole(),
        replicas,
    });
    assert_eq!(outcome, ApplyOutcome::Applied, "[seed={seed}] CreateTablet");

    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();

    // Sealed history: a seed-varied batch, routine-sealed as epoch 0.
    let pre = 2 + (seed % 3) as usize;
    for i in 0..pre {
        write_and_journal(
            &mut sim,
            &parent,
            &live,
            &mut journal,
            &key8(i),
            b"v0",
            seed,
        );
    }
    let leader = elect(&mut sim, &parent, &live, seed);
    let sealed = seal_now(&mut meta, &store, &parent, leader, 1_000, false);
    assert_eq!(sealed, Some(0), "[seed={seed}] routine seal of epoch 0");

    // Unsealed backlog, one write transactional (the stage-marker path
    // rides the same log; its resolve-materialized record must arrive
    // exactly once through the FINAL seal, not the routine one).
    for i in 0..pre {
        write_and_journal(
            &mut sim,
            &parent,
            &live,
            &mut journal,
            &key8(i),
            b"v1",
            seed,
        );
    }
    write_txn_and_journal(
        &mut sim,
        &parent,
        &live,
        &mut journal,
        &txn_key(0),
        b"t1",
        seed,
    );

    let (left_id, right_id) = complete_copy_split(
        &mut meta,
        &store,
        &mut sim,
        &parent,
        b"sk000002",
        2_000,
        seed,
    );

    // The lineage is frozen at cutover: both children's epoch-0 parent is
    // the parent's FINAL sealed shard, the parent is gone from the map,
    // and the children are Active.
    let final_epoch = meta
        .stream_shards
        .range((TabletId(7), 0)..=(TabletId(7), u64::MAX))
        .next_back()
        .map(|((_, e), _)| *e)
        .expect("parent must have sealed shards");
    let expected_parent = segment::shard_id(7, final_epoch);
    for child in [left_id, right_id] {
        assert_eq!(
            meta.stream_shard_parent_id(child, 0).as_deref(),
            Some(expected_parent.as_str()),
            "[seed={seed}] child {child:?} epoch-0 lineage must be the parent's final shard"
        );
        assert_eq!(
            meta.tablets[&child].state,
            TabletState::Active,
            "[seed={seed}] cutover must activate the children"
        );
    }
    assert!(
        !meta.tablets.contains_key(&TabletId(7)),
        "[seed={seed}] cutover must remove the parent"
    );

    // Children: PRIVATE empty engines (per-tablet storage), empty change
    // logs — nothing inherited, the tombstone's own named property.
    let left_engines = engines();
    let right_engines = engines();
    let left = start_group(
        &sim,
        &left_engines,
        left_id,
        KeyRange::new(Vec::new(), Some(b"sk000002".to_vec())),
    );
    let right = start_group(
        &sim,
        &right_engines,
        right_id,
        KeyRange::new(b"sk000002".to_vec(), None),
    );
    sim.run_for(Duration::from_secs(2));
    for (child, name) in [(&left, "left"), (&right, "right")] {
        let l = elect(&mut sim, child, &live, seed);
        assert!(
            block_on(child.nodes[l].pending_changes()).is_empty(),
            "[seed={seed}] the {name} child must be born with an EMPTY change log"
        );
    }

    // Post-cutover writes route by range; each child seals its own epoch 0.
    write_and_journal(&mut sim, &left, &live, &mut journal, &key8(0), b"v2", seed);
    write_and_journal(&mut sim, &right, &live, &mut journal, &key8(3), b"v2", seed);
    let ll = elect(&mut sim, &left, &live, seed);
    let rl = elect(&mut sim, &right, &live, seed);
    assert_eq!(
        seal_now(&mut meta, &store, &left, ll, 3_000, false),
        Some(0),
        "[seed={seed}] the left child's first seal must be its own epoch 0"
    );
    assert_eq!(
        seal_now(&mut meta, &store, &right, rl, 3_000, false),
        Some(0),
        "[seed={seed}] the right child's first seal must be its own epoch 0"
    );

    verify_lineage(
        &meta,
        &store,
        &[(&parent, leader), (&left, ll), (&right, rl)],
        &journal,
        seed,
    );
}

#[test]
fn copy_split_children_born_empty() {
    for_each_seed(
        "copy_split_children_born_empty",
        scenario_copy_split_children_born_empty,
    );
}

// --- cell 10: copy_split_endgame_survives_seal_crash_and_outage ----------

/// The driver endgame's fault surface at the primitive level: the parent's
/// final seal first "crashes" between the segment `put` and the catalog
/// commit (`skip_commit` — the D9 kill point), then retries into a store
/// outage, then heals and lands. Cutover only after the successful seal;
/// exactly-once must hold across the whole ordeal (the retried seal
/// recomputes the identical epoch; the orphaned first `put` is invisible
/// to the walk — reaped in production by the segment janitor's sub-case
/// (a), which needs no tablet liveness).
fn scenario_copy_split_endgame_survives_seal_faults(seed: u64) {
    let mut sim = Simulator::new(seed);
    let mut meta = base_meta();
    let parent_engines = engines();
    let parent = start_group(&sim, &parent_engines, TabletId(9), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));
    let replicas: Vec<_> = NODES.iter().copied().map(nid).collect();
    let outcome = meta.apply(&MetaCommand::CreateTablet {
        tablet: TabletId(9),
        table: Some(TABLE.into()),
        range: KeyRange::whole(),
        replicas: replicas.clone(),
    });
    assert_eq!(outcome, ApplyOutcome::Applied, "[seed={seed}] CreateTablet");

    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut journal = BTreeMap::new();
    let n = 2 + (seed % 4) as usize;
    for i in 0..n {
        write_and_journal(&mut sim, &parent, &live, &mut journal, &key8(i), b"w", seed);
    }

    // BeginSplit lands; the final seal's first attempt crashes after the
    // `put` (no catalog row), the second hits an outage, the third lands.
    let left_id = meta.next_free_tablet_id();
    let right_id = TabletId(left_id.0 + 1);
    let epoch = meta.tablets[&TabletId(9)].epoch;
    let outcome = meta.apply(&MetaCommand::BeginSplit {
        parent: TabletId(9),
        expected_epoch: epoch,
        split_key: b"sk000001".to_vec(),
        children: [(left_id, replicas.clone()), (right_id, replicas)],
    });
    assert_eq!(outcome, ApplyOutcome::Applied, "[seed={seed}] BeginSplit");

    let leader = elect(&mut sim, &parent, &live, seed);
    assert_eq!(
        seal_now(&mut meta, &store, &parent, leader, 1_000, true),
        None,
        "[seed={seed}] the crashed attempt must commit nothing"
    );
    store.set_unavailable_until(Nanos(u64::MAX));
    assert_eq!(
        seal_now(&mut meta, &store, &parent, leader, 1_100, false),
        None,
        "[seed={seed}] the outage attempt must commit nothing"
    );
    store.clear_unavailable();
    assert_eq!(
        seal_now(&mut meta, &store, &parent, leader, 1_200, false),
        Some(0),
        "[seed={seed}] the healed retry must land the identical epoch"
    );

    let epoch = meta.tablets[&TabletId(9)].epoch;
    let outcome = meta.apply(&MetaCommand::CutoverSplit {
        parent: TabletId(9),
        expected_epoch: epoch,
        cutover_wall_ms: 1_300,
    });
    assert_eq!(outcome, ApplyOutcome::Applied, "[seed={seed}] CutoverSplit");
    assert_eq!(
        meta.stream_shard_parent_id(left_id, 0).as_deref(),
        Some(segment::shard_id(9, 0).as_str()),
        "[seed={seed}] the frozen lineage must name the retried seal's shard"
    );

    verify_lineage(&meta, &store, &[(&parent, leader)], &journal, seed);
}

#[test]
fn copy_split_endgame_survives_seal_faults() {
    for_each_seed(
        "copy_split_endgame_survives_seal_faults",
        scenario_copy_split_endgame_survives_seal_faults,
    );
}
