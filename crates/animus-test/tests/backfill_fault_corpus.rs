//! The secondary-index **backfill fault-injection corpus** (ADR 0045 §2/§3,
//! PR4 of the `UpdateTable` GSI-backfill stack).
//!
//! ## What this proves, and against which layer
//!
//! The backfill seeder (`animusd::index_drain::backfill_seed_tick`) and the
//! completion aggregator (`animusd::index_backfill::index_backfill_tick`)
//! both live in `animusd` — a crate with no `SimEnv` binding of its own (see
//! `crates/animusd/CLAUDE.md`: "this crate has no `SimEnv` — it is the
//! assembly/wire layer over the two sim-tested crates below it"), so it
//! cannot host a seed-reproducible corpus directly. This file follows the
//! exact precedent `stream_lineage_corpus.rs` set for the identical layering
//! problem (ADR 0042/0043's sealer/consumer, also `animusd`-only): a
//! **self-contained reimplementation**, directly over `animus-cp-data`'s
//! `RaftKvNode` and a bare `animus-control::Metadata` (mutated with plain
//! `.apply()` calls — no live control Raft needed, the same hand-scripted-
//! catalog technique `reconciler_corpus.rs`/`stream_lineage_corpus.rs` both
//! use), mirroring the production functions' exact algorithms rather than
//! importing them (they are private to `animusd`).
//!
//! **Deliberately narrower scope than a full GSI-materialization proof**:
//! [`backfill_seed_tick`] mirrors the real seeder's *coverage* mechanism
//! (scan `KIND_BASE` forward from a raw-bytes cursor, enumerate distinct
//! partitions via the "bump the last byte" trick, seed one change-log-only
//! dirty marker per newly-discovered partition, advance the cursor, report
//! completion) — but this file never reimplements `reconcile_partition`'s
//! GSI-row diffing (cross-table routing, `IndexFootprint` diffing, item
//! projection). That is deliberate, not a shortcut: ADR 0045 §2's own
//! argument is that the seeder's *only* novel contribution is coverage —
//! "backfill is the GSI drain applied to every pre-existing key... no new
//! correctness mechanism" (`index_drain.rs`'s module doc) — and
//! `reconcile_partition`'s own correctness (idempotent, content-blind,
//! re-derived every pass) is already proven independently. What this corpus
//! adds under fault injection is exactly the seeder's own claim: **every
//! partition that ever held a row gets at least one dirty marker** — proven
//! directly by scanning `KIND_BASE` and `KIND_CHANGE` and diffing the two
//! partition sets ([`assert_full_coverage`]) — plus the completion
//! aggregator's convergence property, mirrored from `animusd::
//! index_backfill::index_backfill_tick`'s exact decision
//! ([`maybe_flip_active`]). The full production stack (real seeder + real
//! `reconcile_partition` + real DynamoDB wire, proving *exact GSI content*
//! across a real split) is the deterministic `ProdEnv` acceptance test
//! `animusd/tests/backfill_seeder.rs::
//! split_during_backfill_converges_with_correct_final_gsi` — complementary,
//! not overlapping: that test proves content correctness once; this corpus
//! proves the coverage/convergence *mechanism* under fault injection, at
//! depth.
//!
//! ## Corpus doctrine (ADR 0014)
//!
//! Frozen, named scenario cells (one `#[test]` each), a depth knob
//! (`ANIMUS_BACKFILL_SEEDS`, default 1 — variant 0 always keeps the cell's
//! own canonical, name-derived seed, matching every other corpus's
//! `seed_expand` convention). See `crates/animus-test/CLAUDE.md` for the
//! full knob table.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::schema::{IndexDef, IndexKind, IndexProjection};
use animus_control::{
    ApplyOutcome, ColumnType, IndexStatus, MetaCommand, Metadata, ProposeResult, TableSchema,
};
use animus_cp_data::cursor;
use animus_cp_data::{KIND_BASE, KIND_CURSOR, RaftKvNode};
use animus_dynamo::index as dynamo_index;
use animus_dynamo::{AttributeValue, ChangeRecord, storage_key};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TOKEN_BYTES, TabletId, partition_token};
use futures::executor::block_on;
use std::collections::BTreeMap;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const NODES: [u64; 3] = [60, 61, 62];
const TABLE: &str = "orders";
/// A deliberately small per-tick batch (unlike the production
/// `BACKFILL_SEED_BATCH == 256`) so a modest partition count still needs
/// several ticks — the multi-tick interleaving surface is exactly where a
/// fault (split, leader kill, a concurrent live write) has room to land
/// mid-sweep.
const SEED_BATCH: usize = 3;

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
    std::env::var("ANIMUS_BACKFILL_SEEDS")
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

// --- tablet-group harness (mirrors stream_lineage_corpus.rs) --------------

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
    // `start_hosted` with `stream = id.0`, never `start_scoped` (which pins
    // every group to `PRIMARY_STREAM`) — the split cells here run two tablet
    // groups on the same 3 node ids at once, and sharing a node id's inbox
    // on one stream cross-talks their Raft traffic (the exact livelock
    // `stream_lineage_corpus.rs` found and documented).
    let nodes = NODES
        .iter()
        .map(|&n| {
            RaftKvNode::start_hosted(
                sim.env(nid(n)),
                ids.clone(),
                engines[&n].clone(),
                animus_cp_data::StorageScope::new(TABLE.as_bytes().to_vec(), range.clone()),
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

/// Propose-then-confirm-applied, panicking with the seed on rejection — the
/// one choke point every write in this file goes through.
fn propose_confirmed(sim: &mut Simulator, node: &KvNode, seed: u64, result: ProposeResult) -> u64 {
    match result {
        ProposeResult::Accepted { index } => {
            confirm(sim, node, index, seed);
            index
        }
        other => panic!("[seed={seed}] proposal rejected: {other:?}"),
    }
}

// --- DynamoDB key-shape + partition-boundary mirrors ----------------------

/// The data-plane key `animusd::dynamo::item_key` computes for a simple
/// (partition-key-only) item: `partition_token(escape(pk)) || escape(pk)` —
/// duplicated here per every sibling corpus/test's own "keep your own copy"
/// convention (see `dynamo_txn.rs::item_key`), since it is the only way to
/// predict a DynamoDB item's tablet placement from outside the edge.
fn base_key(pk: &str) -> Vec<u8> {
    let escaped = storage_key(&AttributeValue::S(pk.to_owned()), None);
    let token = partition_token(&escaped);
    let mut key = token.to_vec();
    key.extend_from_slice(&escaped);
    key
}

/// Mirrors `animusd::index_drain`'s private `base_partition_prefix_end`
/// verbatim (not reusable across the crate boundary): the length of
/// `token || escape(pk)` at the front of a raw `KIND_BASE` key, given only
/// that the key's first `TOKEN_BYTES` are the ADR 0022 token and
/// `animus_dynamo::escape`'s encoding starts right after (every literal
/// `0x00` byte in `pk` doubled to `0x00 0x01`, the whole segment terminated
/// `0x00 0x00`). `None` on a malformed/truncated key.
fn base_partition_prefix_end(key: &[u8]) -> Option<usize> {
    let mut i = TOKEN_BYTES;
    while i < key.len() {
        if key[i] != 0x00 {
            i += 1;
            continue;
        }
        match key.get(i + 1)? {
            0x01 => i += 2,
            0x00 => return Some(i + 2),
            _ => return None,
        }
    }
    None
}

/// Mirrors `animusd::index_drain::backfill_tag`.
fn backfill_tag(index_name: &str) -> String {
    format!("backfill:{index_name}")
}

/// `n` candidate item ids, sorted by their *actual* data-plane key (never by
/// id string), split at the median into `(boundary, left_ids, right_ids)` —
/// the same key-prediction technique `dynamo_txn.rs::create_table_pre_split`
/// uses, adapted here to also hand back which side each candidate landed on.
fn split_candidates(n: usize) -> (Vec<u8>, Vec<String>, Vec<String>) {
    let mut candidates: Vec<(String, Vec<u8>)> = (0..n)
        .map(|i| {
            let id = format!("p{i:04}");
            let key = base_key(&id);
            (id, key)
        })
        .collect();
    candidates.sort_by(|a, b| a.1.cmp(&b.1));
    let mid = candidates.len() / 2;
    let boundary = candidates[mid].1.clone();
    let left = candidates[..mid].iter().map(|(id, _)| id.clone()).collect();
    let right = candidates[mid..].iter().map(|(id, _)| id.clone()).collect();
    (boundary, left, right)
}

// --- write helpers ----------------------------------------------------------

/// A row that predates the index's own declaration: base-scope only, no
/// change-log entry — exactly the rows the seeder exists to cover.
fn write_pre_existing_row(
    sim: &mut Simulator,
    node: &KvNode,
    range: &KeyRange,
    pk: &str,
    seed: u64,
) {
    let key = base_key(pk);
    let result = node.put_kind_batch_fenced(
        vec![(KIND_BASE, key, Some(b"v".to_vec()))],
        Vec::new(),
        Vec::new(),
        range.clone(),
    );
    propose_confirmed(sim, node, seed, result);
}

/// A **live** write during `Creating` (base row + change record together, in
/// one `KindBatch` — exactly what `dynamo.rs::index_aware_write` commits):
/// unconditionally leaves a dirty marker regardless of the seeder's own
/// progress, per ADR 0045 §2's "no write after `Creating` can ever be
/// missed" argument.
fn write_base_row_live(sim: &mut Simulator, node: &KvNode, range: &KeyRange, pk: &str, seed: u64) {
    let key = base_key(pk);
    let record = ChangeRecord {
        base_sk: Vec::new(),
        old_image: None,
        new_image: None,
        seeded: false,
        marker: false,
    }
    .encode();
    let result = node.put_kind_batch_fenced(
        vec![(KIND_BASE, key.clone(), Some(b"v".to_vec()))],
        vec![(key, record)],
        Vec::new(),
        range.clone(),
    );
    propose_confirmed(sim, node, seed, result);
}

// --- the backfill seeder mirror (ADR 0045 §2) ------------------------------

/// One backfill-seeder tick, mirroring `animusd::index_drain::
/// backfill_seed_tick`'s exact algorithm against a real `RaftKvNode`
/// directly (`node` must be `range`'s current leader). Returns `(partitions
/// seeded this tick, whether the sweep reached the end of `range`)`.
fn backfill_seed_tick(
    sim: &mut Simulator,
    node: &KvNode,
    range: &KeyRange,
    tag: &str,
    batch_limit: usize,
    seed: u64,
) -> (usize, bool) {
    let cursor_key_bytes = cursor::cursor_key(&range.start, tag);
    let mut last_seeded: Option<Vec<u8>> =
        block_on(node.local_get_kind(KIND_CURSOR, &cursor_key_bytes))
            .map(|bytes| cursor::decode_backfill_cursor(&bytes));
    let mut scan_start: Vec<u8> = match &last_seeded {
        Some(prefix) => dynamo_index::range_end(prefix),
        None => Vec::new(),
    };
    let mut seeded = 0usize;
    let mut reached_end = false;
    while seeded < batch_limit {
        let Some((key, _)) = block_on(node.local_scan(&scan_start, None, Some(1)))
            .into_iter()
            .next()
        else {
            reached_end = true;
            break;
        };
        let Some(prefix_len) = base_partition_prefix_end(&key) else {
            scan_start = {
                let mut next = key;
                next.push(0x00);
                next
            };
            continue;
        };
        let prefix = key[..prefix_len].to_vec();
        let base_sk = key[prefix_len..].to_vec();
        let record = ChangeRecord {
            base_sk,
            old_image: None,
            new_image: None,
            seeded: true,
            marker: false,
        }
        .encode();
        let result = node.put_kind_batch_fenced(
            Vec::new(),
            vec![(prefix.clone(), record)],
            Vec::new(),
            range.clone(),
        );
        propose_confirmed(sim, node, seed, result);
        scan_start = dynamo_index::range_end(&prefix);
        last_seeded = Some(prefix);
        seeded += 1;
    }
    if seeded > 0 {
        let prefix = last_seeded.expect("seeded > 0 implies last_seeded was set above");
        let cursor_val = cursor::encode_backfill_cursor(&prefix);
        // `KeyRange::whole()`, NEVER `range` — mirrors the real fix to the
        // exact bug this corpus found (see `advance_backfill_cursor`'s doc
        // in `animusd::index_drain`): `cursor::cursor_key` truncates
        // `range.start` to a bare token, which sorts *below* a non-token-
        // aligned split child's own `range.start` — fencing this write to
        // `range` itself rejects it as "outside this group's live range"
        // on every real split, forever. A cursor row's identity is already
        // fully captured by its own token (disjoint from base data by row
        // kind) and needs no range-fencing at all.
        let result = node.put_kind_batch_fenced(
            vec![(KIND_CURSOR, cursor_key_bytes, Some(cursor_val))],
            Vec::new(),
            Vec::new(),
            KeyRange::whole(),
        );
        propose_confirmed(sim, node, seed, result);
    }
    (seeded, reached_end)
}

/// Drives [`backfill_seed_tick`] to completion (re-electing a leader every
/// tick, tolerating a leadership change mid-sweep), returning the number of
/// ticks it took.
fn drive_sweep_to_completion(
    sim: &mut Simulator,
    group: &Group,
    live: &[usize],
    range: &KeyRange,
    tag: &str,
    seed: u64,
) -> usize {
    for tick in 1..=10_000 {
        let leader = elect(sim, group, live, seed);
        let (_, reached_end) =
            backfill_seed_tick(sim, &group.nodes[leader], range, tag, SEED_BATCH, seed);
        if reached_end {
            return tick;
        }
        sim.run_for(Duration::from_millis(10));
    }
    panic!("[seed={seed}] backfill sweep never reached its end after 10,000 ticks");
}

// --- coverage / aggregator assertions --------------------------------------

/// Every distinct partition physically present in `node`'s own live
/// `KIND_BASE` scope.
fn base_partitions_present(node: &KvNode) -> BTreeSet<Vec<u8>> {
    block_on(node.local_scan(&[], None, None))
        .into_iter()
        .filter_map(|(k, _)| base_partition_prefix_end(&k).map(|n| k[..n].to_vec()))
        .collect()
}

/// Every distinct partition with at least one dirty marker in `node`'s own
/// live `KIND_CHANGE` scope — a record's key is `footprint_key || hlc`
/// (`animusd::index_drain::drain_tablet`'s own derivation), so the partition
/// is its key minus the fixed 8-byte HLC suffix.
fn partitions_with_change_marker(node: &KvNode) -> BTreeSet<Vec<u8>> {
    block_on(node.pending_changes())
        .into_iter()
        .filter_map(|(k, _)| k.len().checked_sub(8).map(|n| k[..n].to_vec()))
        .collect()
}

/// Every raw `KIND_CHANGE` row currently on `node`, decoded, paired with its
/// own partition prefix (`key` minus the trailing 8-byte HLC — the same
/// slicing [`partitions_with_change_marker`] uses). Unlike that function
/// this keeps every row rather than deduplicating into a set and actually
/// decodes each one's content, since the streamed-mid-backfill flag cell
/// below (ADR 0045 follow-up "E1") needs to classify *every* dirty marker a
/// partition got, not just whether it got at least one.
fn decoded_change_records(node: &KvNode) -> Vec<(Vec<u8>, ChangeRecord)> {
    block_on(node.pending_changes())
        .into_iter()
        .filter_map(|(k, v)| {
            let prefix_len = k.len().checked_sub(8)?;
            let record = ChangeRecord::decode(&v)?;
            Some((k[..prefix_len].to_vec(), record))
        })
        .collect()
}

/// ADR 0045 §2's own correctness claim, checked directly: every base
/// partition this tablet currently holds has at least one dirty marker —
/// "every partition that ever held a row gets at least one dirty marker
/// after `Creating` commits... nothing here loses coverage" — the seeder's
/// only job. Never checks GSI *content* (out of this corpus's scope; see
/// the module doc).
fn assert_full_coverage(node: &KvNode, seed: u64, at: &str) {
    let base = base_partitions_present(node);
    let covered = partitions_with_change_marker(node);
    for p in &base {
        assert!(
            covered.contains(p),
            "[seed={seed}, at={at}] partition {p:?} has a base row but no dirty marker — \
             a live write to it would silently never reach the GSI"
        );
    }
}

// --- bare-Metadata catalog helpers (mirrors index_backfill.rs's own tests) -

fn base_meta(table: &str) -> Metadata {
    let mut m = Metadata::default();
    let outcome = m.apply(&MetaCommand::CreateTableSchema {
        table: table.into(),
        schema: TableSchema::simple("id", ColumnType::String),
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "schema registration must apply"
    );
    m
}

fn create_tablet(meta: &mut Metadata, id: TabletId, range: KeyRange, table: &str) {
    let outcome = meta.apply(&MetaCommand::CreateTablet {
        tablet: id,
        table: Some(table.into()),
        range,
        replicas: NODES.iter().copied().map(nid).collect(),
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "tablet registration must apply"
    );
}

fn create_index(meta: &mut Metadata, table: &str, name: &str, hash_attr: &str) {
    let outcome = meta.apply(&MetaCommand::CreateTableIndex {
        table: table.into(),
        index: IndexDef {
            name: name.into(),
            kind: IndexKind::Global,
            hash_attribute: hash_attr.into(),
            sort_attribute: None,
            projection: IndexProjection::All,
            status: IndexStatus::Creating,
        },
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "index declaration must apply"
    );
}

fn mark_backfilled(meta: &mut Metadata, table: &str, index: &str, tablet: TabletId) {
    let outcome = meta.apply(&MetaCommand::MarkIndexBackfilled {
        table: table.into(),
        index: index.into(),
        tablet,
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "MarkIndexBackfilled must apply (rejection means a tablet not scoped to the table, \
         which none of these scenarios should trigger)"
    );
}

/// Mirrors `animusd::index_backfill::index_backfill_tick`'s exact
/// per-(table,index) decision (see that module's own doc): "if the table
/// currently has at least one tablet, and every tablet currently in
/// `tablets_for_table` has reported, propose `SetIndexStatus{Active}`."
/// Returns whether this call actually flipped it.
fn maybe_flip_active(meta: &mut Metadata, table: &str, index: &str) -> bool {
    let is_creating = meta
        .table_indexes(table)
        .iter()
        .any(|i| i.name == index && i.status == IndexStatus::Creating);
    if !is_creating {
        return false;
    }
    let mut has_any = false;
    let all_reported = meta.tablets_for_table(table).all(|(&tablet, _)| {
        has_any = true;
        meta.index_backfill
            .contains_key(&(tablet, index.to_owned()))
    });
    if !has_any || !all_reported {
        return false;
    }
    let outcome = meta.apply(&MetaCommand::SetIndexStatus {
        table: table.into(),
        index: index.into(),
        status: IndexStatus::Active,
    });
    matches!(outcome, ApplyOutcome::Applied)
}

fn index_status(meta: &Metadata, table: &str, index: &str) -> Option<IndexStatus> {
    meta.table_indexes(table)
        .iter()
        .find(|i| i.name == index)
        .map(|i| i.status)
}

// --- cell 1: single_tablet_backfill_converges ------------------------------

fn scenario_single_tablet_backfill_converges(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta(TABLE);
    create_tablet(&mut meta, TabletId(1), KeyRange::whole(), TABLE);
    create_index(&mut meta, TABLE, "by-email", "email");
    let group = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    let leader = elect(&mut sim, &group, &live, seed);
    for i in 0..20 {
        write_pre_existing_row(
            &mut sim,
            &group.nodes[leader],
            &group.range,
            &format!("p{i:03}"),
            seed,
        );
    }

    let tag = backfill_tag("by-email");
    let ticks = drive_sweep_to_completion(&mut sim, &group, &live, &group.range, &tag, seed);
    assert!(
        ticks > 1,
        "[seed={seed}] 20 partitions at batch {SEED_BATCH} must take >1 tick"
    );
    mark_backfilled(&mut meta, TABLE, "by-email", group.id);

    assert!(
        maybe_flip_active(&mut meta, TABLE, "by-email"),
        "[seed={seed}] must flip once the only tablet reports"
    );
    assert_eq!(
        index_status(&meta, TABLE, "by-email"),
        Some(IndexStatus::Active)
    );

    let leader = elect(&mut sim, &group, &live, seed);
    assert_full_coverage(&group.nodes[leader], seed, "single tablet, no faults");
}

#[test]
fn single_tablet_backfill_converges() {
    for_each_seed(
        "single_tablet_backfill_converges",
        scenario_single_tablet_backfill_converges,
    );
}

// --- cell 2: concurrent_split_during_backfill --------------------------------

fn scenario_concurrent_split_during_backfill(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta(TABLE);
    create_tablet(&mut meta, TabletId(1), KeyRange::whole(), TABLE);
    create_index(&mut meta, TABLE, "by-email", "email");
    let parent = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    let (boundary, left_ids, right_ids) = split_candidates(24);
    let leader = elect(&mut sim, &parent, &live, seed);
    for id in left_ids.iter().chain(right_ids.iter()) {
        write_pre_existing_row(&mut sim, &parent.nodes[leader], &parent.range, id, seed);
    }

    let tag = backfill_tag("by-email");
    // Exactly one tick before the split: since `SEED_BATCH == 3` and the 24
    // candidates are sorted by their real data-plane key with the split
    // boundary at the median (index 12), the 3 smallest-keyed partitions
    // seeded here are guaranteed to fall strictly below the boundary — the
    // left child's cursor is therefore guaranteed non-empty at split time,
    // making the "resume" claim below unconditionally checkable rather than
    // seed-dependent.
    let leader = elect(&mut sim, &parent, &live, seed);
    let (seeded, reached_end) = backfill_seed_tick(
        &mut sim,
        &parent.nodes[leader],
        &parent.range,
        &tag,
        SEED_BATCH,
        seed,
    );
    assert_eq!(
        seeded, SEED_BATCH,
        "[seed={seed}] pre-split tick must seed a full batch"
    );
    assert!(
        !reached_end,
        "[seed={seed}] one tick over 24 partitions at batch 3 must not reach the end"
    );

    let left_range = KeyRange::new(Vec::new(), Some(boundary.clone()));
    let right_range = KeyRange::new(boundary.clone(), None);
    let left_cursor_key = cursor::cursor_key(&left_range.start, &tag);
    let right_cursor_key = cursor::cursor_key(&right_range.start, &tag);

    let leader = elect(&mut sim, &parent, &live, seed);
    let left_cursor_before_split =
        block_on(parent.nodes[leader].local_get_kind(KIND_CURSOR, &left_cursor_key));
    assert!(
        left_cursor_before_split.is_some(),
        "[seed={seed}] the left range's cursor must already exist pre-split (the parent's own progress)"
    );

    // The split itself (ADR 0028/0044): narrow the parent's own live scope on
    // every replica, record split provenance in the catalog, and start a
    // fresh sibling group over the SAME per-node engines (shared storage —
    // the sibling's own KIND_BASE/KIND_CHANGE/KIND_CURSOR scopes, once
    // widened to its range, transparently expose whatever right-range
    // records already physically exist).
    let parent_epoch = meta
        .tablets
        .get(&parent.id)
        .map_or(animus_tablet::Epoch::INITIAL, |t| t.epoch);
    let sibling_id = TabletId(2);
    for n in &parent.nodes {
        n.narrow_scope(left_range.clone());
    }
    let outcome = meta.apply(&MetaCommand::SplitTablet {
        tablet: parent.id,
        expected_epoch: parent_epoch,
        split_key: boundary.clone(),
        new_id: sibling_id,
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "[seed={seed}] split must apply"
    );
    let sibling = start_group(&sim, &engines, sibling_id, right_range.clone());
    sim.run_for(Duration::from_secs(2));

    // Fork A, checked directly: the left child's cursor survives the split
    // byte-for-byte unchanged; the right child's own cursor key reads empty.
    let leader = elect(&mut sim, &parent, &live, seed);
    let left_cursor_after_split =
        block_on(parent.nodes[leader].local_get_kind(KIND_CURSOR, &left_cursor_key));
    assert_eq!(
        left_cursor_after_split, left_cursor_before_split,
        "[seed={seed}] the left child's cursor must be UNCHANGED by the split (Fork A: resume)"
    );
    let sibling_leader = elect(&mut sim, &sibling, &[0, 1, 2], seed);
    let right_cursor_at_birth =
        block_on(sibling.nodes[sibling_leader].local_get_kind(KIND_CURSOR, &right_cursor_key));
    assert!(
        right_cursor_at_birth.is_none(),
        "[seed={seed}] the right child's cursor must read empty immediately post-split (Fork A: restart from scratch)"
    );

    // Both children must independently converge; neither may flip the index
    // alone.
    drive_sweep_to_completion(&mut sim, &parent, &live, &left_range, &tag, seed);
    mark_backfilled(&mut meta, TABLE, "by-email", parent.id);
    assert!(
        !maybe_flip_active(&mut meta, TABLE, "by-email"),
        "[seed={seed}] must not flip while the right child has not reported"
    );

    drive_sweep_to_completion(&mut sim, &sibling, &[0, 1, 2], &right_range, &tag, seed);
    mark_backfilled(&mut meta, TABLE, "by-email", sibling.id);
    assert!(
        maybe_flip_active(&mut meta, TABLE, "by-email"),
        "[seed={seed}] must flip once both children have reported"
    );
    assert_eq!(
        index_status(&meta, TABLE, "by-email"),
        Some(IndexStatus::Active)
    );

    let leader = elect(&mut sim, &parent, &live, seed);
    assert_full_coverage(&parent.nodes[leader], seed, "left child after split");
    let sibling_leader = elect(&mut sim, &sibling, &[0, 1, 2], seed);
    assert_full_coverage(
        &sibling.nodes[sibling_leader],
        seed,
        "right child after split",
    );
}

#[test]
fn concurrent_split_during_backfill() {
    for_each_seed(
        "concurrent_split_during_backfill",
        scenario_concurrent_split_during_backfill,
    );
}

// --- cell 3: split_after_tablet_already_reported_done ------------------------

/// The named "split of a tablet that already reported done" dimension: a
/// single tablet completes its **entire** sweep and reports
/// `MarkIndexBackfilled` while it is still the table's only tablet — then
/// splits. The right child inherits (physically, via shared storage) rows
/// that were *already* covered pre-split, but its own tablet id has never
/// reported — the aggregator's fresh-tablet-map-read discipline
/// (`animusd::index_backfill::index_backfill_tick`'s own doc: "a tablet that
/// appears after some others have already reported must still block the
/// flip until it reports too") must still hold, and the real seeder mirror,
/// not a hand-driven `MarkIndexBackfilled`, must be the one to produce the
/// child's own report (restarting from scratch and harmlessly re-covering
/// already-dirty partitions, per idempotence).
fn scenario_split_after_tablet_already_reported_done(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta(TABLE);
    create_tablet(&mut meta, TabletId(1), KeyRange::whole(), TABLE);
    create_index(&mut meta, TABLE, "by-email", "email");
    let parent = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    let (boundary, left_ids, right_ids) = split_candidates(16);
    let leader = elect(&mut sim, &parent, &live, seed);
    for id in left_ids.iter().chain(right_ids.iter()) {
        write_pre_existing_row(&mut sim, &parent.nodes[leader], &parent.range, id, seed);
    }

    let tag = backfill_tag("by-email");
    drive_sweep_to_completion(&mut sim, &parent, &live, &parent.range, &tag, seed);
    mark_backfilled(&mut meta, TABLE, "by-email", parent.id);
    assert!(
        maybe_flip_active(&mut meta, TABLE, "by-email"),
        "[seed={seed}] the only tablet has fully reported — must flip"
    );
    assert_eq!(
        index_status(&meta, TABLE, "by-email"),
        Some(IndexStatus::Active)
    );

    // Re-open the index for a fresh backfill pass would be a different
    // scenario (drop+recreate) — here the point is purely the split-after-
    // done aggregation hazard, so force the index back to `Creating` the
    // same way `SetIndexStatus` always would (this file's `create_index`
    // helper's own path), simulating "the table now has a *second*,
    // concurrently-declared index" would work too, but re-using the same
    // index name keeps the scenario's assertions about `by-email`
    // unambiguous.
    let outcome = meta.apply(&MetaCommand::SetIndexStatus {
        table: TABLE.into(),
        index: "by-email".into(),
        status: IndexStatus::Creating,
    });
    assert_eq!(outcome, ApplyOutcome::Applied);

    let left_range = KeyRange::new(Vec::new(), Some(boundary.clone()));
    let right_range = KeyRange::new(boundary.clone(), None);
    let parent_epoch = meta
        .tablets
        .get(&parent.id)
        .map_or(animus_tablet::Epoch::INITIAL, |t| t.epoch);
    let sibling_id = TabletId(2);
    for n in &parent.nodes {
        n.narrow_scope(left_range.clone());
    }
    let outcome = meta.apply(&MetaCommand::SplitTablet {
        tablet: parent.id,
        expected_epoch: parent_epoch,
        split_key: boundary.clone(),
        new_id: sibling_id,
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "[seed={seed}] split must apply"
    );
    let sibling = start_group(&sim, &engines, sibling_id, right_range.clone());
    sim.run_for(Duration::from_secs(2));

    // The sibling has physically-inherited, already-covered base rows but
    // has never itself reported — must block the flip even though the
    // parent (still the same tablet id, already marked done from before the
    // reopen) technically still has a stale `index_backfill` row.
    assert!(
        !maybe_flip_active(&mut meta, TABLE, "by-email"),
        "[seed={seed}] a freshly-appeared child that has never reported must block the flip"
    );

    // The real seeder mirror, run against the sibling, produces its own
    // report — restarting from scratch (its cursor reads empty) and
    // harmlessly re-marking rows the parent's earlier pass already covered.
    drive_sweep_to_completion(&mut sim, &sibling, &[0, 1, 2], &right_range, &tag, seed);
    mark_backfilled(&mut meta, TABLE, "by-email", sibling.id);
    assert!(
        maybe_flip_active(&mut meta, TABLE, "by-email"),
        "[seed={seed}] must flip once the child reports too"
    );

    let sibling_leader = elect(&mut sim, &sibling, &[0, 1, 2], seed);
    assert_full_coverage(
        &sibling.nodes[sibling_leader],
        seed,
        "child of an already-done parent",
    );
}

#[test]
fn split_after_tablet_already_reported_done() {
    for_each_seed(
        "split_after_tablet_already_reported_done",
        scenario_split_after_tablet_already_reported_done,
    );
}

// --- cell 4: live_writes_race_the_sweep --------------------------------------

fn scenario_live_writes_race_the_sweep(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta(TABLE);
    create_tablet(&mut meta, TabletId(1), KeyRange::whole(), TABLE);
    create_index(&mut meta, TABLE, "by-email", "email");
    let group = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    let leader = elect(&mut sim, &group, &live, seed);
    for i in 0..12 {
        write_pre_existing_row(
            &mut sim,
            &group.nodes[leader],
            &group.range,
            &format!("pre{i:03}"),
            seed,
        );
    }

    let tag = backfill_tag("by-email");
    // Interleave sweep ticks with live writes into brand-new partitions
    // (never touched by the seeder's forward scan yet, at these small
    // batch/partition sizes) — the "no record lost" argument's live-write
    // half: a write after `Creating` already leaves its own dirty marker,
    // unconditional on the seeder's own progress.
    for round in 0..4 {
        let leader = elect(&mut sim, &group, &live, seed);
        backfill_seed_tick(
            &mut sim,
            &group.nodes[leader],
            &group.range,
            &tag,
            SEED_BATCH,
            seed,
        );
        let leader = elect(&mut sim, &group, &live, seed);
        write_base_row_live(
            &mut sim,
            &group.nodes[leader],
            &group.range,
            &format!("live{round:03}"),
            seed,
        );
    }

    let ticks = drive_sweep_to_completion(&mut sim, &group, &live, &group.range, &tag, seed);
    assert!(ticks >= 1);
    mark_backfilled(&mut meta, TABLE, "by-email", group.id);
    assert!(maybe_flip_active(&mut meta, TABLE, "by-email"));

    let leader = elect(&mut sim, &group, &live, seed);
    assert_full_coverage(&group.nodes[leader], seed, "live writes racing the sweep");
    let base = base_partitions_present(&group.nodes[leader]);
    assert_eq!(
        base.len(),
        16,
        "[seed={seed}] 12 pre-existing + 4 live == 16 total partitions"
    );
}

#[test]
fn live_writes_race_the_sweep() {
    for_each_seed(
        "live_writes_race_the_sweep",
        scenario_live_writes_race_the_sweep,
    );
}

// --- cell 5: leader_kill_mid_sweep --------------------------------------------

fn scenario_leader_kill_mid_sweep(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta(TABLE);
    create_tablet(&mut meta, TabletId(1), KeyRange::whole(), TABLE);
    create_index(&mut meta, TABLE, "by-email", "email");
    let group = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let mut live = vec![0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    let leader = elect(&mut sim, &group, &live, seed);
    for i in 0..24 {
        write_pre_existing_row(
            &mut sim,
            &group.nodes[leader],
            &group.range,
            &format!("p{i:03}"),
            seed,
        );
    }

    let tag = backfill_tag("by-email");
    // A couple of ticks under the first leader, then kill it mid-sweep and
    // elect among the survivors — the cursor row lives in the tablet's own
    // replicated `KIND_CURSOR` scope, so it must survive intact.
    let leader = elect(&mut sim, &group, &live, seed);
    backfill_seed_tick(
        &mut sim,
        &group.nodes[leader],
        &group.range,
        &tag,
        SEED_BATCH,
        seed,
    );
    let leader = elect(&mut sim, &group, &live, seed);
    let (seeded_before_kill, _) = backfill_seed_tick(
        &mut sim,
        &group.nodes[leader],
        &group.range,
        &tag,
        SEED_BATCH,
        seed,
    );
    assert_eq!(seeded_before_kill, SEED_BATCH);

    let cursor_key_bytes = cursor::cursor_key(&group.range.start, &tag);
    let cursor_before_kill =
        block_on(group.nodes[leader].local_get_kind(KIND_CURSOR, &cursor_key_bytes));
    assert!(cursor_before_kill.is_some());

    // Kill the exact leader just written through (not a fresh `elect()` —
    // which could, in principle, race a spurious re-election and pick a
    // different node than the one that holds the freshest state).
    let dying = leader;
    sim.crash(nid(NODES[dying]));
    live.retain(|&i| i != dying);
    let new_leader = elect(&mut sim, &group, &live, seed);
    // `local_get_kind` is a non-linearizable **local** read (this crate's
    // apply task can lag the consensus loop by design, per this crate's own
    // CLAUDE.md) — a replica can win an election the instant its *log*
    // catches up without having *applied* every entry yet, so the freshly
    // elected leader's own local read can briefly lag before converging.
    // Poll rather than assert a single immediate read (house style: an
    // eventual property gets a converged-or-timeout poll).
    let mut cursor_after_kill = None;
    for _ in 0..100 {
        cursor_after_kill =
            block_on(group.nodes[new_leader].local_get_kind(KIND_CURSOR, &cursor_key_bytes));
        if cursor_after_kill == cursor_before_kill {
            break;
        }
        sim.run_for(Duration::from_millis(10));
    }
    assert_eq!(
        cursor_after_kill, cursor_before_kill,
        "[seed={seed}] the newly-elected leader must converge to the identical durably-committed cursor"
    );

    let ticks = drive_sweep_to_completion(&mut sim, &group, &live, &group.range, &tag, seed);
    assert!(ticks > 0);
    mark_backfilled(&mut meta, TABLE, "by-email", group.id);
    assert!(maybe_flip_active(&mut meta, TABLE, "by-email"));

    let leader = elect(&mut sim, &group, &live, seed);
    assert_full_coverage(&group.nodes[leader], seed, "post leader-kill");
}

#[test]
fn leader_kill_mid_sweep() {
    for_each_seed("leader_kill_mid_sweep", scenario_leader_kill_mid_sweep);
}

// --- cell 6: two_indexes_creating_independently ------------------------------

fn scenario_two_indexes_creating_independently(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta(TABLE);
    create_tablet(&mut meta, TabletId(1), KeyRange::whole(), TABLE);
    create_index(&mut meta, TABLE, "by-a", "a");
    create_index(&mut meta, TABLE, "by-b", "b");
    let group = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    let leader = elect(&mut sim, &group, &live, seed);
    for i in 0..18 {
        write_pre_existing_row(
            &mut sim,
            &group.nodes[leader],
            &group.range,
            &format!("p{i:03}"),
            seed,
        );
    }

    let tag_a = backfill_tag("by-a");
    let tag_b = backfill_tag("by-b");
    // Interleaved ticks, one tag per round, each maintaining its own
    // per-index cursor row (ADR 0045 §2's "per-index cursor, not one shared
    // scan").
    let mut reached_a = false;
    let mut reached_b = false;
    for _ in 0..30 {
        if !reached_a {
            let leader = elect(&mut sim, &group, &live, seed);
            let (_, done) = backfill_seed_tick(
                &mut sim,
                &group.nodes[leader],
                &group.range,
                &tag_a,
                SEED_BATCH,
                seed,
            );
            reached_a = done;
        }
        if !reached_b {
            let leader = elect(&mut sim, &group, &live, seed);
            let (_, done) = backfill_seed_tick(
                &mut sim,
                &group.nodes[leader],
                &group.range,
                &tag_b,
                SEED_BATCH,
                seed,
            );
            reached_b = done;
        }
        if reached_a && reached_b {
            break;
        }
        sim.run_for(Duration::from_millis(10));
    }
    assert!(
        reached_a && reached_b,
        "[seed={seed}] both independent sweeps must converge"
    );

    mark_backfilled(&mut meta, TABLE, "by-a", group.id);
    mark_backfilled(&mut meta, TABLE, "by-b", group.id);
    assert!(maybe_flip_active(&mut meta, TABLE, "by-a"));
    assert!(maybe_flip_active(&mut meta, TABLE, "by-b"));
    assert_eq!(
        index_status(&meta, TABLE, "by-a"),
        Some(IndexStatus::Active)
    );
    assert_eq!(
        index_status(&meta, TABLE, "by-b"),
        Some(IndexStatus::Active)
    );

    let leader = elect(&mut sim, &group, &live, seed);
    assert_full_coverage(
        &group.nodes[leader],
        seed,
        "two independently-creating indexes",
    );
}

#[test]
fn two_indexes_creating_independently() {
    for_each_seed(
        "two_indexes_creating_independently",
        scenario_two_indexes_creating_independently,
    );
}

// --- cell 7: drop_table_mid_backfill ------------------------------------------

/// The named "drop-table mid-backfill" dimension (ADR 0045 §4's own prune
/// rule): once a table's tablets are dropped (`MetaCommand::
/// DropTableTablets`, the first step of the drop-table cascade), every
/// `index_backfill` row for those tablets must be pruned, and the
/// completion aggregator must never flip an index whose table now has zero
/// tablets — "a table with zero tablets never flips... it simply waits" —
/// distinguishing a genuinely-vacuous "every tablet reported" (true over an
/// empty set) from real completion.
fn scenario_drop_table_mid_backfill(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta(TABLE);
    create_tablet(&mut meta, TabletId(1), KeyRange::whole(), TABLE);
    create_index(&mut meta, TABLE, "by-email", "email");
    let group = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    let leader = elect(&mut sim, &group, &live, seed);
    for i in 0..10 {
        write_pre_existing_row(
            &mut sim,
            &group.nodes[leader],
            &group.range,
            &format!("p{i:03}"),
            seed,
        );
    }

    let tag = backfill_tag("by-email");
    drive_sweep_to_completion(&mut sim, &group, &live, &group.range, &tag, seed);
    mark_backfilled(&mut meta, TABLE, "by-email", group.id);
    assert!(
        meta.index_backfill
            .contains_key(&(group.id, "by-email".to_owned())),
        "[seed={seed}] the report must be visible before the drop"
    );

    let outcome = meta.apply(&MetaCommand::DropTableTablets {
        table: TABLE.into(),
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "[seed={seed}] drop must apply"
    );

    assert!(
        !meta
            .index_backfill
            .contains_key(&(group.id, "by-email".to_owned())),
        "[seed={seed}] the dropped tablet's report must be pruned, not linger forever"
    );
    assert_eq!(
        meta.tablets_for_table(TABLE).count(),
        0,
        "[seed={seed}] the table must have zero tablets after the drop"
    );
    assert!(
        !maybe_flip_active(&mut meta, TABLE, "by-email"),
        "[seed={seed}] a table with zero tablets must never flip (a vacuous \"every tablet \
         reported\" over an empty set is not real completion)"
    );
    assert_eq!(
        index_status(&meta, TABLE, "by-email"),
        Some(IndexStatus::Creating),
        "[seed={seed}] the index's own status is untouched by the drop itself"
    );
}

#[test]
fn drop_table_mid_backfill() {
    for_each_seed("drop_table_mid_backfill", scenario_drop_table_mid_backfill);
}

// --- cell 6: streamed_mid_backfill_seed_flag_never_misclassified -----------

/// ADR 0045 follow-up "E1" (closed by this PR's `ChangeRecord::seeded` flag
/// together with `animusd::dynamo_streams`'s filter): the flag itself must
/// never be misclassified under adversarial interleaving of seeder ticks and live
/// writes racing the sweep — a live write's own dirty marker must always
/// decode `seeded: false` (else the Streams read path would wrongly *drop a
/// real event*, the opposite-direction bug the filter must not introduce),
/// and every pre-existing partition's own marker the seeder produces must
/// always decode `seeded: true` and image-less (else the fix would have
/// nothing distinguishable to filter, i.e. the original phantom-event bug).
///
/// This corpus cannot reach the real Streams read path directly (no
/// `SimEnv` binding of `animusd::dynamo_streams` — see the module doc's
/// "what this proves, and against which layer"); the real wire-level
/// filtering behavior is proven by `animusd/tests/
/// stream_backfill_seed_filter.rs`'s `ProdEnv` integration test instead.
/// What this cell adds is fault-injection depth *on the flag's own
/// correctness* — mirrors `live_writes_race_the_sweep`'s exact interleaving
/// shape, so a future change to the seeder/live-write paths that mislabels
/// either one under a similar interleaving has a seed-reproducible corpus
/// cell to catch it, at `ANIMUS_BACKFILL_SEEDS` depth.
fn scenario_streamed_mid_backfill_seed_flag_never_misclassified(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let mut meta = base_meta(TABLE);
    create_tablet(&mut meta, TabletId(1), KeyRange::whole(), TABLE);
    create_index(&mut meta, TABLE, "by-email", "email");
    let group = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    let pre_ids: Vec<String> = (0..12).map(|i| format!("pre{i:03}")).collect();
    let leader = elect(&mut sim, &group, &live, seed);
    for id in &pre_ids {
        write_pre_existing_row(&mut sim, &group.nodes[leader], &group.range, id, seed);
    }

    let tag = backfill_tag("by-email");
    let live_ids: Vec<String> = (0..4).map(|i| format!("live{i:03}")).collect();
    // Same interleaving as `live_writes_race_the_sweep`: one sweep tick, then
    // one concurrent live write, repeated — the sweep may or may not reach a
    // given live partition before its own live write lands.
    for id in &live_ids {
        let leader = elect(&mut sim, &group, &live, seed);
        backfill_seed_tick(
            &mut sim,
            &group.nodes[leader],
            &group.range,
            &tag,
            SEED_BATCH,
            seed,
        );
        let leader = elect(&mut sim, &group, &live, seed);
        write_base_row_live(&mut sim, &group.nodes[leader], &group.range, id, seed);
    }
    drive_sweep_to_completion(&mut sim, &group, &live, &group.range, &tag, seed);
    mark_backfilled(&mut meta, TABLE, "by-email", group.id);
    assert!(maybe_flip_active(&mut meta, TABLE, "by-email"));

    let leader = elect(&mut sim, &group, &live, seed);
    let records = decoded_change_records(&group.nodes[leader]);

    let pre_prefixes: BTreeSet<Vec<u8>> = pre_ids.iter().map(|id| base_key(id)).collect();
    let live_prefixes: BTreeSet<Vec<u8>> = live_ids.iter().map(|id| base_key(id)).collect();

    // Every pre-existing partition: exactly one marker (the seeder visits
    // each partition at most once, ever, per the cursor's own monotonicity),
    // always `seeded: true`, always image-less — the seeder's own
    // dirty-marker shape (ADR 0045 §2).
    for prefix in &pre_prefixes {
        let mine: Vec<&ChangeRecord> = records
            .iter()
            .filter(|(p, _)| p == prefix)
            .map(|(_, r)| r)
            .collect();
        assert_eq!(
            mine.len(),
            1,
            "[seed={seed}] pre-existing partition {prefix:?} got {} markers, want exactly 1",
            mine.len()
        );
        assert!(
            mine[0].seeded,
            "[seed={seed}] a pre-existing partition's own seeder marker decoded seeded=false \
             — this PR's Streams filter would wrongly let it through as a phantom event"
        );
        assert!(
            mine[0].old_image.is_none() && mine[0].new_image.is_none(),
            "[seed={seed}] the seeder's own marker unexpectedly carries an image"
        );
    }

    // Every live-written partition: at least one marker decodes
    // `seeded: false` — its own real write must never be misclassified as a
    // seed marker (the opposite-direction bug: silently dropping a genuine
    // event from the stream). An *additional*, redundant `seeded: true`
    // marker from the sweep passing over the same partition later is
    // harmless and allowed (the GSI drain re-derives content regardless),
    // never required.
    for prefix in &live_prefixes {
        let mine: Vec<&ChangeRecord> = records
            .iter()
            .filter(|(p, _)| p == prefix)
            .map(|(_, r)| r)
            .collect();
        assert!(
            !mine.is_empty(),
            "[seed={seed}] a live-written partition {prefix:?} has no marker at all"
        );
        assert!(
            mine.iter().any(|r| !r.seeded),
            "[seed={seed}] a live write's own marker was misclassified seeded=true for \
             partition {prefix:?} — the Streams filter would wrongly drop a real event"
        );
    }

    let leader = elect(&mut sim, &group, &live, seed);
    assert_full_coverage(
        &group.nodes[leader],
        seed,
        "streamed mid-backfill, flag classification",
    );
}

#[test]
fn streamed_mid_backfill_seed_flag_never_misclassified() {
    for_each_seed(
        "streamed_mid_backfill_seed_flag_never_misclassified",
        scenario_streamed_mid_backfill_seed_flag_never_misclassified,
    );
}
