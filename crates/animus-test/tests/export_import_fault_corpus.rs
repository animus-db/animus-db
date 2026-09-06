//! S3 export/import fault-injection corpus (ADR 0068 §9 "PR (3)'s planned
//! `SimEnv` corpus", the PR 2 as-built amendment's residual #5 — S-05 PR 3).
//!
//! ## What this proves, and against which layer
//!
//! The export job (`animusd::dynamo::run_export_job`/`run_export_job_inner`)
//! and the import driver (`animusd::import::import_tick`/`import_loop`) both
//! live in `animusd` — a crate with no `SimEnv` binding of its own (see
//! `crates/animusd/CLAUDE.md`). This file follows the exact precedent
//! `backup_fault_corpus.rs`/`pitr_fault_corpus.rs` set for the identical
//! layering problem: a **self-contained reimplementation**, directly over
//! `animus-cp-data`'s `RaftKvNode`, a bare `animus-control::Metadata`
//! (mutated with plain `.apply()` calls — no live control Raft), and
//! `animus-sim`'s `SimSegmentStore` standing in for the customer's own S3
//! bucket (the identical `SegmentStore` trait a real `S3SegmentStore`
//! implements, ADR 0068 §2).
//!
//! **Every DECISION function that lives in `animus-control`/`animus-item` is
//! called for real, never reimplemented**: all six of `BeginExport`/
//! `CompleteExport`/`FailExport`/`BeginImport`/`CompleteImport`/`FailImport`
//! are `Metadata::apply` calls against the real state machine (their own
//! apply arms in `crates/animus-control/src/meta.rs` decide every
//! acceptance/rejection/idempotency rule — this file never reimplements
//! them), and every derived write an imported item produces goes through
//! the real, pure `animus_item::derive_kind_writes` — the identical core
//! `animusd::dynamo::kind_writes_for_item` wraps in production. Every seeded
//! value is also re-wrapped through the real `animus_cp_data::backup::
//! encode_restored_value` before merging (see [`import_tick_mirror`]'s own
//! call site) — the identical corrupt-engine-value hazard
//! `backup_fault_corpus.rs`'s own restore-tick mirror closes; skipping it
//! here made this file's own very first depth-1 run panic immediately with
//! a "corrupt engine value" decode error, caught before it ever shipped.
//! Only the export job's own scan/encode/chunk mechanics
//! ([`run_export_job_mirror`]) and the import driver's own resolve/decode/
//! seed mechanics ([`import_tick_mirror`]) are mirrored — see each
//! function's own doc for its exact correspondence to the production
//! function it stands in for.
//!
//! ## Verification
//!
//! A completed export's `manifest-summary.json` + `manifest-files.json` +
//! every referenced `data/*.json.gz` object are decoded directly
//! ([`read_all_exported_items`]) through the REAL
//! `animus_dynamo::wire::decode_item` decoder and diffed against an
//! independently-tracked model of the source table's committed state at
//! each tablet's own pin point — exact set equality, no key ever decoded
//! twice across tablets (checked directly, a hard `assert!` inside the
//! read itself). A completed import's destination tablet is read back the
//! same way, through the real `animus_item::decode_stored_item` this
//! crate's own writers used to write the source in the first place.
//!
//! ## Corpus doctrine (ADR 0014)
//!
//! Frozen, named scenario cells (one `#[test]` each), a depth knob
//! (`ANIMUS_EXPORT_IMPORT_SEEDS`, default 1 — variant 0 always keeps the
//! cell's own canonical, name-derived seed, matching every other corpus's
//! `seed_expand`/`for_each_seed` convention). See `crates/animus-test/
//! CLAUDE.md` for the full knob table.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_control::{
    ApplyOutcome, ColumnType, ExportFormat, ExportStatus, ExportType, ImportStatus,
    InputCompressionType, InputFormat, MetaCommand, Metadata, ProposeResult, TableSchema,
};
use animus_cp_data::backup as backup_codec;
use animus_cp_data::{KIND_BASE, KIND_LSI, RaftKvNode, SeedRow, StorageScope, TxnWrite};
use animus_dynamo::wire as dynamo_wire;
use animus_env::{Clock, EnvExt, Nanos, SegmentStore, nid};
use animus_item::{
    AttributeValue, Item, WriteSchema, decode_stored_item, derive_kind_writes, encode_stored_item,
    storage_key,
};
use animus_sim::{SegmentFaultConfig, SimEnv, SimSegmentStore, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId, TabletState, partition_token};
use animus_test::corpus;
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const NODES: [u64; 3] = [80, 81, 82];
const SRC_TABLE: &str = "src_items";

// --- corpus boilerplate (identical convention to every sibling corpus) ----

fn for_each_seed(name: &str, body: impl FnMut(u64)) {
    corpus::for_each_seed(
        name,
        corpus::seeds_from_env("ANIMUS_EXPORT_IMPORT_SEEDS"),
        body,
    );
}

// --- tablet-group harness (mirrors backup_fault_corpus.rs/pitr_fault_corpus.rs) --

struct Group {
    id: TabletId,
    #[allow(dead_code)] // kept for symmetry with sibling corpora's own `Group`
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

/// A best-effort, bounded confirm — `false` (never a panic) once the
/// deadline passes, mirroring `animusd::import::propose_local`'s own
/// "retry next tick" contract (every seeded row is safe to re-derive and
/// re-propose, so a timed-out confirm here is never a correctness problem,
/// only a liveness one this corpus never actually needs to exercise at the
/// Raft-propose layer).
fn try_confirm(sim: &mut Simulator, node: &KvNode, index: u64) -> bool {
    for _ in 0..300 {
        if node.engine_applied_index() >= index {
            return true;
        }
        sim.run_for(Duration::from_millis(10));
    }
    false
}

/// The base-table logical key for one item — mirrors `animusd::dynamo::
/// item_key(pk, sk)` byte-for-byte (`token(escape(pk)) || escape(pk) ||
/// sk`).
fn item_key(pk: &AttributeValue, sk: Option<&AttributeValue>) -> Vec<u8> {
    let mut key = partition_token(&storage_key(pk, None)).to_vec();
    key.extend_from_slice(&storage_key(pk, sk));
    key
}

/// Writes one committed base row directly (no LSI/change-log — the source
/// table this corpus exports from is a plain, unindexed/unstreamed table,
/// so a real write there would derive nothing more than this anyway).
fn write_item(sim: &mut Simulator, node: &KvNode, pk: &str, seed: u64) -> Item {
    let mut item = Item::new();
    item.insert("id".to_owned(), AttributeValue::S(pk.to_owned()));
    item.insert("val".to_owned(), AttributeValue::S(format!("v-{pk}")));
    let key = item_key(&AttributeValue::S(pk.to_owned()), None);
    let result = node.put_kind_batch(
        vec![(KIND_BASE, key, Some(encode_stored_item(&item)))],
        Vec::new(),
    );
    propose_confirmed(sim, node, seed, result);
    item
}

/// Deterministically split `total_each_side` distinct partition-key strings
/// per side of `boundary` (a token-space cut point) — the fixture every
/// multi-tablet export scenario uses to guarantee its writes actually land
/// on two DIFFERENT tablets, since token assignment is a real Murmur3 hash
/// this corpus doesn't get to choose directly.
fn split_ids(total_each_side: usize, boundary: &[u8]) -> (Vec<String>, Vec<String>) {
    let mut left = Vec::new();
    let mut right = Vec::new();
    let mut i = 0u64;
    while left.len() < total_each_side || right.len() < total_each_side {
        let pk = format!("item{i:05}");
        let token = partition_token(&storage_key(&AttributeValue::S(pk.clone()), None));
        if token.as_slice() < boundary {
            if left.len() < total_each_side {
                left.push(pk);
            }
        } else if right.len() < total_each_side {
            right.push(pk);
        }
        i += 1;
        assert!(
            i < 1_000_000,
            "failed to find {total_each_side} ids on both sides of the boundary"
        );
    }
    (left, right)
}

/// Every item in `items`, keyed by its own `"id"` attribute — the model
/// shape every verification helper in this file compares against.
fn items_by_pk(items: impl IntoIterator<Item = Item>) -> BTreeMap<String, Item> {
    let mut out = BTreeMap::new();
    for item in items {
        let pk = match item.get("id") {
            Some(AttributeValue::S(s)) => s.clone(),
            other => panic!("unexpected pk shape: {other:?}"),
        };
        out.insert(pk, item);
    }
    out
}

fn base_meta() -> Metadata {
    let mut m = Metadata::default();
    assert_eq!(
        m.apply(&MetaCommand::CreateTableSchema {
            table: SRC_TABLE.to_owned(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
        ApplyOutcome::Applied
    );
    m
}

// ============================================================================
// Export job mirror (ADR 0068 §1/§4)
// ============================================================================

/// DynamoDB's own export-manifest root segment — mirrors `animusd::dynamo::
/// EXPORT_MANIFEST_ROOT`.
const EXPORT_MANIFEST_ROOT: &str = "AWSDynamoDB";

/// A deliberately tiny per-file row cap (unlike production's
/// `EXPORT_CHUNK_ROWS == 1000`) so a modest item count still spans several
/// data files — mirrors `backup_fault_corpus.rs`'s own `CHUNK_ROWS`
/// rationale.
const EXPORT_CHUNK_ROWS: usize = 3;

/// The bare export id (the random hex suffix) from its own ARN — mirrors
/// `animusd::dynamo::export_id_suffix`.
fn export_id_suffix(export_id: &str) -> &str {
    export_id.rsplit('/').next().unwrap_or(export_id)
}

fn gzip_bytes(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(bytes)
        .expect("an in-memory gzip write never fails");
    encoder
        .finish()
        .expect("an in-memory gzip finish never fails")
}

fn gunzip_bytes(bytes: &[u8]) -> Result<String, std::io::Error> {
    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(bytes);
    let mut out = String::new();
    decoder.read_to_string(&mut out)?;
    Ok(out)
}

struct ExportDataFile {
    item_count: u64,
    data_file_s3_key: String,
}

/// Mirrors `animusd::dynamo::run_export_job_inner`'s exact algorithm and,
/// specifically, its durable-before-visible object-write ORDER: the
/// `_started` marker first, then every `data/NNNN.json.gz` file, then
/// `manifest-files.json`, then `manifest-summary.json` **last** — a reader
/// (or this file's own [`read_all_exported_items`]) that finds the summary
/// present can trust every earlier object is already fully written.
///
/// **The one structural difference from production**: `run_export_job_
/// inner` sweeps the whole table via ONE coordinator-level `cp_scan` (which
/// itself fans out across tablets in token order under the hood); this
/// mirror, run directly over `RaftKvNode` handles with no such coordinator,
/// sweeps `groups` one at a time. Each tablet's own committed state is
/// pinned at ITS OWN `engine_latest_version()` the instant this function
/// reaches it (never re-pinned mid-sweep) — the same per-request "current
/// committed state, read through intent resolution" contract `cp_scan`
/// gives production's job (`local_scan_kind_snapshot`'s own snapshot-read +
/// intent-resolution discipline, `animus-cp-data/CLAUDE.md`). `cut_versions`
/// lets a scenario pre-pin a group's cut point itself (so it can write MORE
/// data to that same group afterward and prove none of it leaks in) instead
/// of letting this function derive it fresh.
///
/// `stop_after_groups`, present only in this corpus, stands in for "the
/// node running this export job crashed": `Some(k)` makes this function
/// return `Err` before ever reaching group index `k` — in particular,
/// before `manifest-summary.json` is ever written — pinning ADR 0068 §9's
/// residual #1 (no crash-resumability, no janitor to reclaim or resume a
/// wedged export).
#[allow(clippy::too_many_arguments)] // mirrors run_export_job_inner's own full request shape plus this corpus's two test-only knobs
fn run_export_job_mirror(
    sim: &mut Simulator,
    groups: &[Group],
    live: &[usize],
    store: &SimSegmentStore,
    export_id: &str,
    stop_after_groups: Option<usize>,
    cut_versions: Option<&[u64]>,
    seed: u64,
) -> Result<(u64, u64, String), String> {
    let root = format!("{EXPORT_MANIFEST_ROOT}/{}", export_id_suffix(export_id));
    block_on(store.put(&format!("{root}/_started"), b""))
        .map_err(|e| format!("writing _started marker: {e}"))?;

    let mut item_count: u64 = 0;
    let mut billed_size_bytes: u64 = 0;
    let mut data_files: Vec<ExportDataFile> = Vec::new();
    let mut file_index: u64 = 0;

    for (gi, group) in groups.iter().enumerate() {
        if let Some(stop) = stop_after_groups
            && gi >= stop
        {
            return Err(format!(
                "[seed={seed}] simulated crash of the export job's own node before reaching \
                 tablet {gi}"
            ));
        }
        let leader = elect(sim, group, live, seed);
        let cut_version = match cut_versions {
            Some(v) => v[gi],
            None => group.nodes[leader].engine_latest_version(),
        };
        let mut next_key: Vec<u8> = Vec::new();
        loop {
            let (rows, next) = block_on(group.nodes[leader].local_scan_kind_snapshot(
                KIND_BASE,
                &next_key,
                cut_version,
                EXPORT_CHUNK_ROWS,
            ));
            let mut lines = String::new();
            let mut chunk_items: u64 = 0;
            for (_, value, _version) in &rows {
                let Some(item) =
                    decode_stored_item(value).map_err(|e| format!("decoding stored item: {e}"))?
                else {
                    continue; // a DynamoDB tombstone value — never exported
                };
                let line = serde_json::json!({ "Item": dynamo_wire::encode_item(&item) });
                lines.push_str(&serde_json::to_string(&line).expect("json serializes"));
                lines.push('\n');
                chunk_items += 1;
            }
            if chunk_items > 0 {
                let gz = gzip_bytes(lines.as_bytes());
                let data_key = format!("{root}/data/{file_index:04}.json.gz");
                block_on(store.put(&data_key, &gz))
                    .map_err(|e| format!("writing data file: {e}"))?;
                data_files.push(ExportDataFile {
                    item_count: chunk_items,
                    data_file_s3_key: data_key,
                });
                item_count += chunk_items;
                billed_size_bytes += lines.len() as u64;
                file_index += 1;
            }
            match next {
                Some(k) => next_key = k,
                None => break,
            }
        }
    }

    let mut files_lines = String::new();
    for f in &data_files {
        let line = serde_json::json!({
            "itemCount": f.item_count,
            "dataFileS3Key": f.data_file_s3_key,
        });
        files_lines.push_str(&serde_json::to_string(&line).expect("json serializes"));
        files_lines.push('\n');
    }
    let files_key = format!("{root}/manifest-files.json");
    block_on(store.put(&files_key, files_lines.as_bytes()))
        .map_err(|e| format!("writing manifest-files.json: {e}"))?;

    // `manifest-summary.json` is written LAST — see this function's own doc.
    let summary = serde_json::json!({
        "version": "2020-06-30",
        "exportArn": export_id,
        "manifestFilesS3Key": files_key,
        "itemCount": item_count,
        "billedSizeBytes": billed_size_bytes,
        "outputFormat": "DYNAMODB_JSON",
    });
    let summary_key = format!("{root}/manifest-summary.json");
    block_on(
        store.put(
            &summary_key,
            serde_json::to_string(&summary)
                .expect("json serializes")
                .as_bytes(),
        ),
    )
    .map_err(|e| format!("writing manifest-summary.json: {e}"))?;

    Ok((item_count, billed_size_bytes, summary_key))
}

fn begin_export(meta: &mut Metadata, export_id: &str, table: &str, s3_bucket: &str) {
    assert_eq!(
        meta.apply(&MetaCommand::BeginExport {
            export_id: export_id.to_owned(),
            table: table.to_owned(),
            table_arn: dynamo_wire::table_arn(table),
            s3_bucket: s3_bucket.to_owned(),
            s3_prefix: None,
            format: ExportFormat::DynamoDbJson,
            export_type: ExportType::Full,
            export_time_ms: None,
            client_token: None,
            created_wall_ms: 1_000,
        }),
        ApplyOutcome::Applied
    );
}

/// Runs [`run_export_job_mirror`] to completion and proposes `CompleteExport`/
/// `FailExport` accordingly — mirrors `animusd::dynamo::run_export_job`'s own
/// `Ok`/`Err` dispatch exactly.
#[allow(clippy::too_many_arguments)] // mirrors run_export_job_mirror's own shape
fn run_export_to_completion(
    sim: &mut Simulator,
    meta: &mut Metadata,
    groups: &[Group],
    live: &[usize],
    store: &SimSegmentStore,
    export_id: &str,
    cut_versions: Option<&[u64]>,
    seed: u64,
) -> Result<(), String> {
    match run_export_job_mirror(
        sim,
        groups,
        live,
        store,
        export_id,
        None,
        cut_versions,
        seed,
    ) {
        Ok((item_count, billed_size_bytes, export_manifest)) => {
            let outcome = meta.apply(&MetaCommand::CompleteExport {
                export_id: export_id.to_owned(),
                item_count,
                billed_size_bytes,
                export_manifest,
                completed_wall_ms: 2_000,
            });
            assert_eq!(
                outcome,
                ApplyOutcome::Applied,
                "[seed={seed}] CompleteExport rejected: {outcome:?}"
            );
            Ok(())
        }
        Err(reason) => {
            let outcome = meta.apply(&MetaCommand::FailExport {
                export_id: export_id.to_owned(),
                reason: reason.clone(),
                completed_wall_ms: 2_000,
            });
            assert!(
                matches!(outcome, ApplyOutcome::Applied),
                "[seed={seed}] FailExport rejected: {outcome:?}"
            );
            Err(reason)
        }
    }
}

/// Every item a completed export's own objects hold, decoded through the
/// REAL `animus_dynamo::wire::decode_item` — the "committed values only, no
/// key ever decoded twice" verification every happy-path scenario shares.
fn read_all_exported_items(
    store: &SimSegmentStore,
    export_id: &str,
    seed: u64,
) -> BTreeMap<String, Item> {
    let root = format!("{EXPORT_MANIFEST_ROOT}/{}", export_id_suffix(export_id));
    let summary_bytes = block_on(store.get(&format!("{root}/manifest-summary.json")))
        .expect("store get ok")
        .unwrap_or_else(|| panic!("[seed={seed}] no manifest-summary.json for {export_id}"));
    let summary: serde_json::Value =
        serde_json::from_slice(&summary_bytes).expect("summary decodes");
    let files_key = summary["manifestFilesS3Key"]
        .as_str()
        .expect("manifestFilesS3Key present")
        .to_owned();
    let files_bytes = block_on(store.get(&files_key))
        .expect("store get ok")
        .expect("manifest-files.json present");
    let files_text = String::from_utf8(files_bytes).expect("utf8");

    let mut out = BTreeMap::new();
    for line in files_text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let entry: serde_json::Value =
            serde_json::from_str(line).expect("manifest-files.json line decodes");
        let data_key = entry["dataFileS3Key"]
            .as_str()
            .expect("dataFileS3Key present");
        let gz = block_on(store.get(data_key))
            .expect("store get ok")
            .unwrap_or_else(|| panic!("[seed={seed}] data file {data_key} missing"));
        let text = gunzip_bytes(&gz).expect("gunzips");
        for item_line in text.lines() {
            if item_line.trim().is_empty() {
                continue;
            }
            let parsed: serde_json::Value =
                serde_json::from_str(item_line).expect("item line decodes");
            let item_obj = parsed
                .get("Item")
                .and_then(serde_json::Value::as_object)
                .expect("Item object present");
            let item = dynamo_wire::decode_item(item_obj).expect("item decodes");
            let pk = match item.get("id") {
                Some(AttributeValue::S(s)) => s.clone(),
                other => panic!("[seed={seed}] unexpected pk shape: {other:?}"),
            };
            assert!(
                out.insert(pk.clone(), item).is_none(),
                "[seed={seed}] item `{pk}` decoded more than once across data files — the \
                 §1 double-count hazard"
            );
        }
    }
    out
}

/// `manifest-files.json` names exactly the data objects the store actually
/// holds under this export's own `data/` prefix — no fewer (a dangling
/// reference), no more (an orphan object nothing describes).
fn assert_manifest_files_match_store(store: &SimSegmentStore, export_id: &str, seed: u64) {
    let root = format!("{EXPORT_MANIFEST_ROOT}/{}", export_id_suffix(export_id));
    let summary_bytes = block_on(store.get(&format!("{root}/manifest-summary.json")))
        .expect("store get ok")
        .expect("manifest-summary.json present");
    let summary: serde_json::Value =
        serde_json::from_slice(&summary_bytes).expect("summary decodes");
    let files_key = summary["manifestFilesS3Key"]
        .as_str()
        .expect("manifestFilesS3Key present");
    let files_text = String::from_utf8(
        block_on(store.get(files_key))
            .expect("store get ok")
            .expect("manifest-files.json present"),
    )
    .expect("utf8");
    let mut listed: BTreeSet<String> = BTreeSet::new();
    for line in files_text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let entry: serde_json::Value = serde_json::from_str(line).expect("line decodes");
        listed.insert(entry["dataFileS3Key"].as_str().unwrap().to_owned());
    }
    let actual: BTreeSet<String> = block_on(store.list(&format!("{root}/data/")))
        .expect("store list ok")
        .into_iter()
        .collect();
    assert_eq!(
        listed, actual,
        "[seed={seed}] manifest-files.json does not list exactly the data objects present"
    );
}

// ============================================================================
// Import driver mirror (ADR 0068 §6, S-05 PR 2)
// ============================================================================

/// Mirrors `animusd::import::MAX_MALFORMED_ITEMS` verbatim (10,000) — the
/// production cap past which an import fails immediately rather than
/// retrying forever.
const MAX_MALFORMED_ITEMS: u64 = 10_000;

/// Mirrors `animusd::import::IMPORT_SEED_VERSION` — the fixed version every
/// row this driver seeds carries, making a re-swept row idempotent under
/// `SeedBatch`'s merge-at-carried-version semantics.
const IMPORT_SEED_VERSION: u64 = 1;

/// Mirrors `animusd::import::IMPORT_SEED_BATCH_ROWS`, at a much smaller
/// scale so a modest item count still exercises more than one propose.
const IMPORT_SEED_BATCH_ROWS: usize = 4;

/// Mirrors `animusd::import::IMPORT_STUCK_TIMEOUT` (600s) — expressed as
/// virtual nanoseconds, since this corpus's own stuck-timeout cell drives it
/// against `env.now()`, never a real clock.
const IMPORT_STUCK_TIMEOUT_NANOS: u64 = 600 * 1_000_000_000;

fn begin_import(
    meta: &mut Metadata,
    import_id: &str,
    target_table: &str,
    tablet: TabletId,
    s3_bucket: &str,
    compression: InputCompressionType,
) {
    assert_eq!(
        meta.apply(&MetaCommand::CreateTableSchema {
            table: target_table.to_owned(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        meta.apply(&MetaCommand::BeginImport {
            import_id: import_id.to_owned(),
            target_table: target_table.to_owned(),
            target_table_arn: dynamo_wire::table_arn(target_table),
            table_id: "test-table-id".to_owned(),
            s3_bucket: s3_bucket.to_owned(),
            s3_prefix: None,
            input_format: InputFormat::DynamoDbJson,
            input_compression: compression,
            base_schema: Box::new(TableSchema::simple("id", ColumnType::String)),
            key_types: vec![("id".to_owned(), "S".to_owned())],
            gsi_defs: Vec::new(),
            throughput: None,
            tablet,
            replicas: NODES.iter().copied().map(nid).collect(),
            client_token: None,
            created_wall_ms: 1_000,
        }),
        ApplyOutcome::Applied
    );
}

/// `manifest-summary.json`'s own bytes, plus whether it was found via the
/// "direct" probe — mirrors `animusd::import::ResolvedManifest`/
/// `resolve_manifest`'s exact two-shape resolution (see that function's own
/// doc): a direct hit at the store's own root, or the indirect
/// `list("AWSDynamoDB")` fallback this corpus's own export mirror's flat,
/// unscoped bucket naturally exercises (export and import share one
/// `SimSegmentStore` "bucket" with no per-request prefix-scoping, so every
/// object key really does live at its full `AWSDynamoDB/<id>/...` path).
struct ResolvedManifestMirror {
    bytes: Vec<u8>,
    direct: bool,
}

fn resolve_manifest_mirror(
    store: &SimSegmentStore,
) -> std::io::Result<Option<ResolvedManifestMirror>> {
    if let Some(bytes) = block_on(store.get("manifest-summary.json"))? {
        return Ok(Some(ResolvedManifestMirror {
            bytes,
            direct: true,
        }));
    }
    let mut candidates: Vec<String> = block_on(store.list("AWSDynamoDB"))?
        .into_iter()
        .filter(|id| id.ends_with("/manifest-summary.json"))
        .collect();
    candidates.sort();
    let Some(key) = candidates.into_iter().next() else {
        return Ok(None);
    };
    let Some(bytes) = block_on(store.get(&key))? else {
        return Ok(None); // deleted between list and get — retry next tick
    };
    Ok(Some(ResolvedManifestMirror {
        bytes,
        direct: false,
    }))
}

/// Mirrors `animusd::import::rebase_recorded_key` — see that function's own
/// doc for why the direct-probe shape needs this rewrite and the indirect
/// one doesn't.
fn rebase_recorded_key_mirror(recorded: &str, direct: bool) -> String {
    if !direct {
        return recorded.to_owned();
    }
    let mut parts = recorded.splitn(3, '/');
    let (Some(_root), Some(_id), Some(rest)) = (parts.next(), parts.next(), parts.next()) else {
        return recorded.to_owned();
    };
    rest.to_owned()
}

/// Whether `value` (a decoded key attribute) matches its own declared
/// `AttributeType` — mirrors `animusd::import::key_type_matches` exactly.
fn key_type_matches(value: &AttributeValue, name: &str, key_types: &[(String, String)]) -> bool {
    let actual = match value {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        _ => return false,
    };
    key_types
        .iter()
        .find(|(n, _)| n == name)
        .is_none_or(|(_, declared)| declared == actual)
}

/// One derived kind write: `(kind, physical key, value — `None` is a
/// tombstone)`, [`derive_kind_writes`]'s own per-entry shape.
type DerivedKindWrite = (u8, Vec<u8>, Option<Vec<u8>>);

/// Decode one `{"Item": {...}}` line, validate its key attributes, and
/// derive its `KIND_BASE`/`KIND_LSI` writes — mirrors `animusd::import::
/// decode_and_derive_item` exactly, EXCEPT for the schema-slice
/// construction: production's own `kind_writes_for_item` builds a
/// `WriteSchema` from live `Metadata` (`write_schema_for`), which this
/// corpus's own target tables never need (no LSIs/streams anywhere in this
/// file), so the slice is built directly rather than through a `Metadata`
/// lookup — the actual derivation this stands in for
/// ([`derive_kind_writes`]) is called unmodified either way, which is the
/// real decision logic under test.
fn decode_and_derive_item_mirror(
    base_schema: &TableSchema,
    key_types: &[(String, String)],
    line: &str,
) -> Result<Vec<DerivedKindWrite>, String> {
    let parsed: serde_json::Value =
        serde_json::from_str(line).map_err(|e| format!("line is not valid JSON: {e}"))?;
    let item_obj = parsed
        .get("Item")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "line has no `Item` object".to_owned())?;
    let item = dynamo_wire::decode_item(item_obj)
        .map_err(|e| format!("item does not decode as DynamoDB JSON: {e:?}"))?;

    let pk_name = base_schema.partition_key.as_str();
    let pk = item
        .get(pk_name)
        .cloned()
        .ok_or_else(|| format!("item is missing its own partition key attribute `{pk_name}`"))?;
    if !key_type_matches(&pk, pk_name, key_types) {
        return Err(format!(
            "item's partition key attribute `{pk_name}` has the wrong declared type"
        ));
    }
    let sk = match base_schema.clustering_keys.first() {
        Some(sk_name) => {
            let sk = item
                .get(sk_name.as_str())
                .cloned()
                .ok_or_else(|| format!("item is missing its own sort key attribute `{sk_name}`"))?;
            if !key_type_matches(&sk, sk_name, key_types) {
                return Err(format!(
                    "item's sort key attribute `{sk_name}` has the wrong declared type"
                ));
            }
            Some(sk)
        }
        None => None,
    };

    let token_prefix = partition_token(&storage_key(&pk, None));
    let schema = WriteSchema {
        key: animus_item::TableSchema {
            partition_key: pk_name.to_owned(),
            sort_key: base_schema.clustering_keys.first().cloned(),
        },
        lsis: Vec::new(),
        change_records_carry_images: false,
    };
    let base_value = encode_stored_item(&item);
    let derived = derive_kind_writes(
        &schema,
        &pk,
        sk.as_ref(),
        &token_prefix,
        base_value,
        None,
        Some(&item),
        false,
        KIND_BASE,
        KIND_LSI,
    );
    Ok(derived.writes)
}

/// Propose `rows` as this import's own destination tablet's `SeedBatch` on
/// `leader`, confirming by applied index — mirrors `animusd::import::
/// propose_local`'s shape (`backup_fault_corpus.rs`'s own `restore_tick`
/// uses the identical primitive/shape for the analogous restore driver).
fn propose_seed_rows(sim: &mut Simulator, leader: &KvNode, rows: Vec<SeedRow>) -> bool {
    if rows.is_empty() {
        return true;
    }
    match leader.propose_seed_batch(rows) {
        ProposeResult::Accepted { index, .. } => try_confirm(sim, leader, index),
        _ => false,
    }
}

/// What one [`import_tick_mirror`] call accomplished — mirrors `animusd::
/// import::ImportTickOutcome` exactly.
enum ImportTickOutcome {
    Completed {
        processed: u64,
        imported: u64,
        errors: u64,
        bytes: u64,
    },
    Failed {
        processed: u64,
        imported: u64,
        errors: u64,
        bytes: u64,
        reason: String,
    },
    NoProgress,
}

/// One import step for `leader` against the customer bucket `store` —
/// mirrors `animusd::import::import_tick`'s exact algorithm: resolve the
/// manifest (either prefix shape), read `manifest-files.json`, stream each
/// data file (gunzip when `Gzip`), decode + derive + seed every item in
/// bounded batches, and stop at the first unrecoverable I/O fault
/// (`NoProgress`, safe to retry next tick — every read here is
/// idempotent/re-derivable) or once too many items are malformed
/// (`Failed`, immediately terminal — content a retry can never fix).
fn import_tick_mirror(
    sim: &mut Simulator,
    leader: &KvNode,
    store: &SimSegmentStore,
    base_schema: &TableSchema,
    key_types: &[(String, String)],
    compression: InputCompressionType,
) -> ImportTickOutcome {
    let manifest = match resolve_manifest_mirror(store) {
        Ok(Some(m)) => m,
        Ok(None) | Err(_) => return ImportTickOutcome::NoProgress,
    };
    let summary: serde_json::Value = match serde_json::from_slice(&manifest.bytes) {
        Ok(v) => v,
        Err(_) => return ImportTickOutcome::NoProgress,
    };
    let Some(files_key_raw) = summary
        .get("manifestFilesS3Key")
        .and_then(serde_json::Value::as_str)
    else {
        return ImportTickOutcome::NoProgress;
    };
    let files_key = rebase_recorded_key_mirror(files_key_raw, manifest.direct);
    let files_bytes = match block_on(store.get(&files_key)) {
        Ok(Some(b)) => b,
        Ok(None) | Err(_) => return ImportTickOutcome::NoProgress,
    };
    let Ok(files_text) = String::from_utf8(files_bytes) else {
        return ImportTickOutcome::NoProgress;
    };

    let mut processed = 0u64;
    let mut imported = 0u64;
    let mut errors = 0u64;
    let mut bytes = 0u64;
    let mut pending: Vec<SeedRow> = Vec::new();

    for line in files_text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => return ImportTickOutcome::NoProgress,
        };
        let Some(data_key_raw) = entry
            .get("dataFileS3Key")
            .and_then(serde_json::Value::as_str)
        else {
            return ImportTickOutcome::NoProgress;
        };
        let data_key = rebase_recorded_key_mirror(data_key_raw, manifest.direct);
        let raw = match block_on(store.get(&data_key)) {
            Ok(Some(b)) => b,
            Ok(None) | Err(_) => return ImportTickOutcome::NoProgress,
        };
        bytes += raw.len() as u64;
        let text = match compression {
            InputCompressionType::Gzip => match gunzip_bytes(&raw) {
                Ok(t) => t,
                Err(_) => return ImportTickOutcome::NoProgress,
            },
            InputCompressionType::None => match String::from_utf8(raw) {
                Ok(t) => t,
                Err(_) => return ImportTickOutcome::NoProgress,
            },
            InputCompressionType::Zstd => {
                unreachable!("ZSTD is rejected at wire-decode time — never reaches a committed row")
            }
        };
        for item_line in text.lines() {
            let item_line = item_line.trim();
            if item_line.is_empty() {
                continue;
            }
            processed += 1;
            match decode_and_derive_item_mirror(base_schema, key_types, item_line) {
                Ok(writes) => {
                    imported += 1;
                    for (kind, key, value) in writes {
                        // Every seeded value is re-wrapped in the value
                        // envelope `SeedBatch`'s merge expects — mirrors
                        // `animusd::import::import_tick`'s own
                        // `backup_codec::encode_restored_value` call
                        // exactly (the identical corrupt-engine-value
                        // hazard `backup_fault_corpus.rs`'s restore-tick
                        // mirror closes the same way).
                        pending.push((
                            kind,
                            key,
                            value.map(|v| backup_codec::encode_restored_value(&v)),
                            IMPORT_SEED_VERSION,
                        ));
                    }
                    if pending.len() >= IMPORT_SEED_BATCH_ROWS
                        && !propose_seed_rows(sim, leader, std::mem::take(&mut pending))
                    {
                        return ImportTickOutcome::NoProgress;
                    }
                }
                Err(_) => {
                    errors += 1;
                    if errors > MAX_MALFORMED_ITEMS {
                        return ImportTickOutcome::Failed {
                            processed,
                            imported,
                            errors,
                            bytes,
                            reason: format!(
                                "too many malformed items ({errors} skipped, cap is \
                                 {MAX_MALFORMED_ITEMS})"
                            ),
                        };
                    }
                }
            }
        }
    }
    if !pending.is_empty() && !propose_seed_rows(sim, leader, pending) {
        return ImportTickOutcome::NoProgress;
    }
    ImportTickOutcome::Completed {
        processed,
        imported,
        errors,
        bytes,
    }
}

fn complete_import_mirror(
    meta: &mut Metadata,
    import_id: &str,
    processed: u64,
    imported: u64,
    errors: u64,
    bytes: u64,
    seed: u64,
) {
    let outcome = meta.apply(&MetaCommand::CompleteImport {
        import_id: import_id.to_owned(),
        processed_item_count: processed,
        imported_item_count: imported,
        error_count: errors,
        processed_size_bytes: bytes,
        completed_wall_ms: 3_000,
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "[seed={seed}] CompleteImport rejected: {outcome:?}"
    );
}

/// Mirrors `animusd::import::fail_import_and_cleanup`: propose `FailImport`,
/// then drop the half-created target table through the ordinary
/// `DropTableSchema`/`DropTableTablets` path — real DynamoDB's own "a failed
/// `ImportTable` rolls back the table it was creating" contract.
#[allow(clippy::too_many_arguments)] // mirrors FailImport's own field list
fn fail_import_and_cleanup_mirror(
    meta: &mut Metadata,
    import_id: &str,
    reason: &str,
    processed: u64,
    imported: u64,
    errors: u64,
    bytes: u64,
    seed: u64,
) {
    let outcome = meta.apply(&MetaCommand::FailImport {
        import_id: import_id.to_owned(),
        reason: reason.to_owned(),
        processed_item_count: processed,
        imported_item_count: imported,
        error_count: errors,
        processed_size_bytes: bytes,
        completed_wall_ms: 3_000,
    });
    assert!(
        matches!(outcome, ApplyOutcome::Applied),
        "[seed={seed}] FailImport rejected: {outcome:?}"
    );
    let target_table = meta
        .import(import_id)
        .expect("row present")
        .target_table
        .clone();
    let outcome = meta.apply(&MetaCommand::DropTableSchema {
        table: target_table.clone(),
    });
    assert!(
        matches!(outcome, ApplyOutcome::Applied | ApplyOutcome::NoOp),
        "[seed={seed}] DropTableSchema rejected: {outcome:?}"
    );
    let outcome = meta.apply(&MetaCommand::DropTableTablets {
        table: target_table,
    });
    assert!(
        matches!(outcome, ApplyOutcome::Applied | ApplyOutcome::NoOp),
        "[seed={seed}] DropTableTablets rejected: {outcome:?}"
    );
}

/// Drives one import to a terminal state (re-electing a leader every tick,
/// tolerating a leadership change mid-sweep) — the whole per-import
/// workflow [`import_tick_mirror`]'s caller in `animusd::import::
/// import_loop` runs tick-by-tick; this collapses it to one call for
/// scenarios that don't need to interleave a fault mid-sweep.
#[allow(clippy::too_many_arguments)]
fn drive_import_until_done(
    sim: &mut Simulator,
    meta: &mut Metadata,
    group: &Group,
    live: &[usize],
    store: &SimSegmentStore,
    import_id: &str,
    base_schema: &TableSchema,
    key_types: &[(String, String)],
    compression: InputCompressionType,
    seed: u64,
) {
    for _ in 0..2_000 {
        let leader = elect(sim, group, live, seed);
        match import_tick_mirror(
            sim,
            &group.nodes[leader],
            store,
            base_schema,
            key_types,
            compression,
        ) {
            ImportTickOutcome::Completed {
                processed,
                imported,
                errors,
                bytes,
            } => {
                complete_import_mirror(meta, import_id, processed, imported, errors, bytes, seed);
                return;
            }
            ImportTickOutcome::Failed {
                processed,
                imported,
                errors,
                bytes,
                reason,
            } => {
                fail_import_and_cleanup_mirror(
                    meta, import_id, &reason, processed, imported, errors, bytes, seed,
                );
                return;
            }
            ImportTickOutcome::NoProgress => {}
        }
        sim.run_for(Duration::from_millis(20));
    }
    panic!("[seed={seed}] import {import_id} never reached a terminal state");
}

/// Every `KIND_BASE` row `node` currently holds, decoded — the read-side
/// counterpart to [`write_item`], through the identical
/// `local_scan_kind_snapshot` + `decode_stored_item` primitives every
/// sibling corpus's own "read back what was written" helper uses.
fn read_all_base_items(node: &KvNode) -> BTreeMap<Vec<u8>, Item> {
    let mut out = BTreeMap::new();
    let mut next_key = Vec::new();
    let ceiling = node.engine_latest_version();
    loop {
        let (rows, next) =
            block_on(node.local_scan_kind_snapshot(KIND_BASE, &next_key, ceiling, 1000));
        let got_any = !rows.is_empty();
        for (k, v, _version) in rows {
            if let Some(item) = decode_stored_item(&v).expect("decodes") {
                out.insert(k, item);
            }
        }
        match next {
            Some(k) => next_key = k,
            None => break,
        }
        if !got_any {
            break;
        }
    }
    out
}

// ============================================================================
// Export scenarios
// ============================================================================

// --- cell 1: export_happy_path_across_two_tablets --------------------------

fn scenario_export_happy_path_across_two_tablets(seed: u64) {
    let mut sim = Simulator::new(seed);
    let boundary = vec![0x80, 0, 0, 0, 0, 0, 0, 0];
    let (left_ids, right_ids) = split_ids(12, &boundary);
    let left_engines = engines();
    let right_engines = engines();
    let left = start_group(
        &sim,
        &left_engines,
        TabletId(1),
        KeyRange::new(Vec::new(), Some(boundary.clone())),
    );
    let right = start_group(
        &sim,
        &right_engines,
        TabletId(2),
        KeyRange::new(boundary, None),
    );
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    let mut model = BTreeMap::new();
    for pk in &left_ids {
        let leader = elect(&mut sim, &left, &live, seed);
        let item = write_item(&mut sim, &left.nodes[leader], pk, seed);
        model.insert(pk.clone(), item);
    }
    for pk in &right_ids {
        let leader = elect(&mut sim, &right, &live, seed);
        let item = write_item(&mut sim, &right.nodes[leader], pk, seed);
        model.insert(pk.clone(), item);
    }

    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut meta = base_meta();
    let export_id = dynamo_wire::export_arn(SRC_TABLE, "e0000000000001");
    begin_export(&mut meta, &export_id, SRC_TABLE, "customer-bucket");

    let groups = [left, right];
    run_export_to_completion(
        &mut sim, &mut meta, &groups, &live, &store, &export_id, None, seed,
    )
    .expect("export should succeed");

    assert_eq!(
        meta.export(&export_id).map(|r| r.status.clone()),
        Some(ExportStatus::Completed)
    );
    let row = meta.export(&export_id).expect("row present");
    assert_eq!(
        row.item_count,
        model.len() as u64,
        "[seed={seed}] item_count mismatch"
    );

    let exported = read_all_exported_items(&store, &export_id, seed);
    assert_eq!(
        exported, model,
        "[seed={seed}] exported content does not match the model"
    );
    assert_manifest_files_match_store(&store, &export_id, seed);
}

#[test]
fn export_happy_path_across_two_tablets() {
    for_each_seed(
        "export_happy_path_across_two_tablets",
        scenario_export_happy_path_across_two_tablets,
    );
}

// --- cell 2: export_under_concurrent_writes ---------------------------------

fn scenario_export_under_concurrent_writes(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let group = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    let mut model = BTreeMap::new();
    let leader = elect(&mut sim, &group, &live, seed);
    for i in 0..8 {
        let pk = format!("pre{i:03}");
        let item = write_item(&mut sim, &group.nodes[leader], &pk, seed);
        model.insert(pk, item);
    }

    // A pending, never-resolved transaction intent — its staged value must
    // never surface in the export (ADR 0068's implicit "committed values
    // only," the identical rule ADR 0059 §5 states explicitly for backups;
    // `local_scan_kind_snapshot`'s own intent resolution gives this for
    // free — checked directly below, not merely by construction).
    let staged_key = item_key(&AttributeValue::S("pending-intent".to_owned()), None);
    let write = TxnWrite {
        key: staged_key,
        value: Some(b"never-committed".to_vec()),
        kind_writes: Vec::new(),
        change_log: None,
        stage_marker: None,
        pending: None,
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

    // Pin the export the instant it reaches this group.
    let cut_version = group.nodes[leader].engine_latest_version();

    // Concurrent writes AFTER the pin: must never appear in the export.
    for i in 0..4 {
        let leader = elect(&mut sim, &group, &live, seed);
        write_item(&mut sim, &group.nodes[leader], &format!("post{i:03}"), seed);
    }

    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut meta = base_meta();
    let export_id = dynamo_wire::export_arn(SRC_TABLE, "e0000000000002");
    begin_export(&mut meta, &export_id, SRC_TABLE, "customer-bucket");

    let groups = [group];
    run_export_to_completion(
        &mut sim,
        &mut meta,
        &groups,
        &live,
        &store,
        &export_id,
        Some(&[cut_version]),
        seed,
    )
    .expect("export should succeed");

    let exported = read_all_exported_items(&store, &export_id, seed);
    assert_eq!(
        exported, model,
        "[seed={seed}] exported content does not match the pinned model — a concurrent write \
         or the pending intent leaked in"
    );
}

#[test]
fn export_under_concurrent_writes() {
    for_each_seed(
        "export_under_concurrent_writes",
        scenario_export_under_concurrent_writes,
    );
}

// --- cell 3: export_with_bucket_faults_converges_or_fails_cleanly ----------

fn scenario_export_with_bucket_faults_converges_or_fails_cleanly(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines = engines();
    let group = start_group(&sim, &engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    let mut model = BTreeMap::new();
    let leader = elect(&mut sim, &group, &live, seed);
    for i in 0..10 {
        let pk = format!("f{i:03}");
        let item = write_item(&mut sim, &group.nodes[leader], &pk, seed);
        model.insert(pk, item);
    }

    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut fault = SegmentFaultConfig::default();
    fault.set_put_ack_lost_prob(0.35);
    store.set_fault_config(fault);

    let mut meta = base_meta();
    let export_id = dynamo_wire::export_arn(SRC_TABLE, "e0000000000003");
    begin_export(&mut meta, &export_id, SRC_TABLE, "customer-bucket");

    let groups = [group];
    // Unlike the on-demand backup capture driver, production's export job
    // has NO retry of its own (`run_export_job_inner` propagates the FIRST
    // store error straight to `FailExport`) — this mirror deliberately does
    // the same, so this cell proves the real, documented consequence: under
    // bucket faults the job either converges to a fully correct COMPLETED
    // (the fault never fired) or cleanly FAILED with its counts frozen at
    // their never-set defaults — never a torn COMPLETED.
    match run_export_to_completion(
        &mut sim, &mut meta, &groups, &live, &store, &export_id, None, seed,
    ) {
        Ok(()) => {
            assert_eq!(
                meta.export(&export_id).map(|r| r.status.clone()),
                Some(ExportStatus::Completed)
            );
            let exported = read_all_exported_items(&store, &export_id, seed);
            assert_eq!(
                exported, model,
                "[seed={seed}] a completed export under bucket faults must still match the \
                 model exactly"
            );
        }
        Err(_) => {
            let row = meta.export(&export_id).expect("row present");
            assert!(
                matches!(row.status, ExportStatus::Failed { .. }),
                "[seed={seed}] expected Failed, got {:?}",
                row.status
            );
            assert_eq!(
                row.item_count, 0,
                "[seed={seed}] a failed export's item_count must stay at its never-set default"
            );
            assert_eq!(
                row.billed_size_bytes, 0,
                "[seed={seed}] a failed export's billed_size_bytes must stay at its \
                 never-set default"
            );
            assert!(
                row.export_manifest.is_none(),
                "[seed={seed}] a failed export must never record a manifest key"
            );
            let root = format!("{EXPORT_MANIFEST_ROOT}/{}", export_id_suffix(&export_id));
            assert!(
                block_on(store.get(&format!("{root}/manifest-summary.json")))
                    .expect("store get ok")
                    .is_none(),
                "[seed={seed}] a failed export must never leave a manifest-summary.json behind \
                 — never a torn COMPLETED"
            );
        }
    }
}

#[test]
fn export_with_bucket_faults_converges_or_fails_cleanly() {
    for_each_seed(
        "export_with_bucket_faults_converges_or_fails_cleanly",
        scenario_export_with_bucket_faults_converges_or_fails_cleanly,
    );
}

// --- cell 4: export_leader_kill_mid_job_leaves_row_in_progress -------------

/// Pins ADR 0068 §9's residual #1 (no crash-resumability): if the node
/// running an export job crashes mid-export, the row is left permanently
/// `InProgress` and no `manifest-summary.json` ever appears — an
/// unfinished export must never be mistaken for a finished one. Do NOT
/// "fix" this residual in a future change to this cell without updating
/// the ADR first.
fn scenario_export_leader_kill_mid_job_leaves_row_in_progress(seed: u64) {
    let mut sim = Simulator::new(seed);
    let boundary = vec![0x80, 0, 0, 0, 0, 0, 0, 0];
    let (left_ids, right_ids) = split_ids(4, &boundary);
    let left_engines = engines();
    let right_engines = engines();
    let left = start_group(
        &sim,
        &left_engines,
        TabletId(1),
        KeyRange::new(Vec::new(), Some(boundary.clone())),
    );
    let right = start_group(
        &sim,
        &right_engines,
        TabletId(2),
        KeyRange::new(boundary, None),
    );
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    for pk in &left_ids {
        let leader = elect(&mut sim, &left, &live, seed);
        write_item(&mut sim, &left.nodes[leader], pk, seed);
    }
    for pk in &right_ids {
        let leader = elect(&mut sim, &right, &live, seed);
        write_item(&mut sim, &right.nodes[leader], pk, seed);
    }

    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut meta = base_meta();
    let export_id = dynamo_wire::export_arn(SRC_TABLE, "e0000000000004");
    begin_export(&mut meta, &export_id, SRC_TABLE, "customer-bucket");

    let groups = [left, right];
    // The node running this export job crashes after writing the FIRST
    // tablet's own data but before ever reaching the second — neither
    // `CompleteExport` nor `FailExport` is ever proposed, because nothing
    // observes the crash (production's real job has no supervisor either).
    let result = run_export_job_mirror(
        &mut sim,
        &groups,
        &live,
        &store,
        &export_id,
        Some(1),
        None,
        seed,
    );
    assert!(
        result.is_err(),
        "[seed={seed}] a simulated crash must not report success"
    );

    assert_eq!(
        meta.export(&export_id).map(|r| r.status.clone()),
        Some(ExportStatus::InProgress),
        "[seed={seed}] a crashed export must be left InProgress forever (ADR 0068 residual #1)"
    );
    let root = format!("{EXPORT_MANIFEST_ROOT}/{}", export_id_suffix(&export_id));
    assert!(
        block_on(store.get(&format!("{root}/manifest-summary.json")))
            .expect("store get ok")
            .is_none(),
        "[seed={seed}] an unfinished export must never be mistaken for a finished one"
    );
    // The `_started` marker and the first tablet's own data DID land,
    // proving this is a genuine partial job, not a no-op.
    assert!(
        block_on(store.get(&format!("{root}/_started")))
            .expect("store get ok")
            .is_some()
    );
    assert!(
        !block_on(store.list(&format!("{root}/data/")))
            .expect("store list ok")
            .is_empty(),
        "[seed={seed}] the crash must land after real partial progress, not before any"
    );
}

#[test]
fn export_leader_kill_mid_job_leaves_row_in_progress() {
    for_each_seed(
        "export_leader_kill_mid_job_leaves_row_in_progress",
        scenario_export_leader_kill_mid_job_leaves_row_in_progress,
    );
}

// ============================================================================
// Import scenarios
// ============================================================================

// --- cell 5: import_happy_path_round_trip -----------------------------------

fn scenario_import_happy_path_round_trip(seed: u64) {
    let mut sim = Simulator::new(seed);
    let src_engines = engines();
    let src = start_group(&sim, &src_engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    let mut model = BTreeMap::new();
    let leader = elect(&mut sim, &src, &live, seed);
    for i in 0..10 {
        let pk = format!("r{i:03}");
        let item = write_item(&mut sim, &src.nodes[leader], &pk, seed);
        model.insert(pk, item);
    }

    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut meta = base_meta();
    let export_id = dynamo_wire::export_arn(SRC_TABLE, "e0000000000005");
    begin_export(&mut meta, &export_id, SRC_TABLE, "customer-bucket");
    let src_groups = [src];
    run_export_to_completion(
        &mut sim,
        &mut meta,
        &src_groups,
        &live,
        &store,
        &export_id,
        None,
        seed,
    )
    .expect("export should succeed");

    const TARGET_TABLE: &str = "imported_items";
    let dest_engines = engines();
    let dest = start_group(&sim, &dest_engines, TabletId(2), KeyRange::whole());
    sim.run_for(Duration::from_secs(1));

    let import_id = dynamo_wire::import_arn(TARGET_TABLE, "i0000000000001");
    begin_import(
        &mut meta,
        &import_id,
        TARGET_TABLE,
        TabletId(2),
        "customer-bucket",
        InputCompressionType::Gzip,
    );
    assert_eq!(
        meta.tablets[&TabletId(2)].state,
        TabletState::Building,
        "[seed={seed}] an in-progress import's destination tablet must be Building — unroutable"
    );

    let base_schema = TableSchema::simple("id", ColumnType::String);
    let key_types = vec![("id".to_owned(), "S".to_owned())];
    drive_import_until_done(
        &mut sim,
        &mut meta,
        &dest,
        &live,
        &store,
        &import_id,
        &base_schema,
        &key_types,
        InputCompressionType::Gzip,
        seed,
    );

    let row = meta.import(&import_id).expect("row present");
    assert_eq!(row.status, ImportStatus::Completed);
    assert_eq!(row.processed_item_count, model.len() as u64);
    assert_eq!(row.imported_item_count, model.len() as u64);
    assert_eq!(row.error_count, 0);
    assert_eq!(
        meta.tablets[&TabletId(2)].state,
        TabletState::Active,
        "[seed={seed}] a completed import's destination tablet must activate"
    );

    let dest_leader = elect(&mut sim, &dest, &live, seed);
    let imported = items_by_pk(read_all_base_items(&dest.nodes[dest_leader]).into_values());
    assert_eq!(
        imported, model,
        "[seed={seed}] imported table content does not match the source model"
    );
}

#[test]
fn import_happy_path_round_trip() {
    for_each_seed(
        "import_happy_path_round_trip",
        scenario_import_happy_path_round_trip,
    );
}

// --- cell 6: import_of_hand_written_none_compressed_layout -----------------

fn scenario_import_of_hand_written_none_compressed_layout(seed: u64) {
    let mut sim = Simulator::new(seed);
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let root = "AWSDynamoDB/hand01";
    let mut lines = String::new();
    let mut model = BTreeMap::new();
    for i in 0..5 {
        let pk = format!("h{i:03}");
        let mut item = Item::new();
        item.insert("id".to_owned(), AttributeValue::S(pk.clone()));
        item.insert("val".to_owned(), AttributeValue::S(format!("v-{pk}")));
        let line = serde_json::json!({ "Item": dynamo_wire::encode_item(&item) });
        lines.push_str(&serde_json::to_string(&line).unwrap());
        lines.push('\n');
        model.insert(pk, item);
    }
    block_on(store.put(&format!("{root}/data/0000.json"), lines.as_bytes())).unwrap();
    let files_line = serde_json::json!({
        "itemCount": model.len(),
        "dataFileS3Key": format!("{root}/data/0000.json"),
    });
    block_on(store.put(
        &format!("{root}/manifest-files.json"),
        format!("{}\n", serde_json::to_string(&files_line).unwrap()).as_bytes(),
    ))
    .unwrap();
    let summary = serde_json::json!({
        "version": "2020-06-30",
        "manifestFilesS3Key": format!("{root}/manifest-files.json"),
        "itemCount": model.len(),
    });
    block_on(store.put(
        &format!("{root}/manifest-summary.json"),
        serde_json::to_string(&summary).unwrap().as_bytes(),
    ))
    .unwrap();

    let dest_engines = engines();
    let dest = start_group(&sim, &dest_engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(1));

    let mut meta = base_meta();
    const TARGET_TABLE: &str = "imported_none";
    let import_id = dynamo_wire::import_arn(TARGET_TABLE, "i0000000000002");
    begin_import(
        &mut meta,
        &import_id,
        TARGET_TABLE,
        TabletId(1),
        "customer-bucket",
        InputCompressionType::None,
    );

    let base_schema = TableSchema::simple("id", ColumnType::String);
    let key_types = vec![("id".to_owned(), "S".to_owned())];
    drive_import_until_done(
        &mut sim,
        &mut meta,
        &dest,
        &live,
        &store,
        &import_id,
        &base_schema,
        &key_types,
        InputCompressionType::None,
        seed,
    );

    let row = meta.import(&import_id).expect("row present");
    assert_eq!(row.status, ImportStatus::Completed);
    assert_eq!(row.imported_item_count, model.len() as u64);
    assert_eq!(row.error_count, 0);

    let dest_leader = elect(&mut sim, &dest, &live, seed);
    let imported = items_by_pk(read_all_base_items(&dest.nodes[dest_leader]).into_values());
    assert_eq!(
        imported, model,
        "[seed={seed}] a hand-written NONE-compressed export layout must import correctly"
    );
}

#[test]
fn import_of_hand_written_none_compressed_layout() {
    for_each_seed(
        "import_of_hand_written_none_compressed_layout",
        scenario_import_of_hand_written_none_compressed_layout,
    );
}

// --- cell 7: import_with_malformed_items_counted_in_error_count ------------

fn scenario_import_with_malformed_items_counted_in_error_count(seed: u64) {
    let mut sim = Simulator::new(seed);
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let root = "AWSDynamoDB/malformed01";
    let mut lines = String::new();
    let mut model = BTreeMap::new();
    for i in 0..6 {
        let pk = format!("m{i:03}");
        let mut item = Item::new();
        item.insert("id".to_owned(), AttributeValue::S(pk.clone()));
        let line = serde_json::json!({ "Item": dynamo_wire::encode_item(&item) });
        lines.push_str(&serde_json::to_string(&line).unwrap());
        lines.push('\n');
        model.insert(pk, item);
    }
    // Two malformed items: one missing its own partition key attribute, one
    // whose partition key attribute is the wrong declared type (`N`
    // instead of the declared `S`) — both must be skipped and counted,
    // never proposed as a row.
    lines.push_str(&serde_json::json!({"Item": {"val": {"S": "no-id"}}}).to_string());
    lines.push('\n');
    lines.push_str(&serde_json::json!({"Item": {"id": {"N": "42"}}}).to_string());
    lines.push('\n');
    let gz = gzip_bytes(lines.as_bytes());
    block_on(store.put(&format!("{root}/data/0000.json.gz"), &gz)).unwrap();
    let files_line = serde_json::json!({
        "itemCount": model.len() + 2,
        "dataFileS3Key": format!("{root}/data/0000.json.gz"),
    });
    block_on(store.put(
        &format!("{root}/manifest-files.json"),
        format!("{}\n", serde_json::to_string(&files_line).unwrap()).as_bytes(),
    ))
    .unwrap();
    let summary = serde_json::json!({
        "version": "2020-06-30",
        "manifestFilesS3Key": format!("{root}/manifest-files.json"),
        "itemCount": model.len(),
    });
    block_on(store.put(
        &format!("{root}/manifest-summary.json"),
        serde_json::to_string(&summary).unwrap().as_bytes(),
    ))
    .unwrap();

    let dest_engines = engines();
    let dest = start_group(&sim, &dest_engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(1));

    let mut meta = base_meta();
    const TARGET_TABLE: &str = "imported_malformed";
    let import_id = dynamo_wire::import_arn(TARGET_TABLE, "i0000000000003");
    begin_import(
        &mut meta,
        &import_id,
        TARGET_TABLE,
        TabletId(1),
        "customer-bucket",
        InputCompressionType::Gzip,
    );

    let base_schema = TableSchema::simple("id", ColumnType::String);
    let key_types = vec![("id".to_owned(), "S".to_owned())];
    drive_import_until_done(
        &mut sim,
        &mut meta,
        &dest,
        &live,
        &store,
        &import_id,
        &base_schema,
        &key_types,
        InputCompressionType::Gzip,
        seed,
    );

    let row = meta.import(&import_id).expect("row present");
    assert_eq!(row.status, ImportStatus::Completed);
    assert_eq!(row.processed_item_count, model.len() as u64 + 2);
    assert_eq!(row.imported_item_count, model.len() as u64);
    assert_eq!(row.error_count, 2);

    let dest_leader = elect(&mut sim, &dest, &live, seed);
    let imported = items_by_pk(read_all_base_items(&dest.nodes[dest_leader]).into_values());
    assert_eq!(
        imported, model,
        "[seed={seed}] only the well-formed items must be imported"
    );
}

#[test]
fn import_with_malformed_items_counted_in_error_count() {
    for_each_seed(
        "import_with_malformed_items_counted_in_error_count",
        scenario_import_with_malformed_items_counted_in_error_count,
    );
}

// --- cell 8: import_with_bucket_faults_converges_to_the_same_content ------

fn run_single_tablet_import_scenario(seed: u64, with_fault: bool) -> BTreeMap<String, Item> {
    let mut sim = Simulator::new(seed);
    let src_engines = engines();
    let src = start_group(&sim, &src_engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(2));

    let mut model = BTreeMap::new();
    let leader = elect(&mut sim, &src, &live, seed);
    for i in 0..8 {
        let pk = format!("b{i:03}");
        let item = write_item(&mut sim, &src.nodes[leader], &pk, seed);
        model.insert(pk, item);
    }

    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let mut meta = base_meta();
    let export_id = dynamo_wire::export_arn(SRC_TABLE, "e0000000000006");
    begin_export(&mut meta, &export_id, SRC_TABLE, "customer-bucket");
    let src_groups = [src];
    run_export_to_completion(
        &mut sim,
        &mut meta,
        &src_groups,
        &live,
        &store,
        &export_id,
        None,
        seed,
    )
    .expect("export should succeed");

    const TARGET_TABLE: &str = "imported_faulty";
    let dest_engines = engines();
    let dest = start_group(&sim, &dest_engines, TabletId(2), KeyRange::whole());
    sim.run_for(Duration::from_secs(1));

    if with_fault {
        // Transiently unavailable for a few ticks — every store read this
        // tick reports `NoProgress`, mirroring `import_tick`'s own uniform
        // "any store fault is retryable" treatment (unlike export's own
        // no-retry design, see cell 3's own doc).
        let until = sim
            .env(nid(NODES[0]))
            .now()
            .saturating_add(Duration::from_millis(500));
        store.set_unavailable_until(until);
    }

    let import_id = dynamo_wire::import_arn(TARGET_TABLE, "i0000000000004");
    begin_import(
        &mut meta,
        &import_id,
        TARGET_TABLE,
        TabletId(2),
        "customer-bucket",
        InputCompressionType::Gzip,
    );

    let base_schema = TableSchema::simple("id", ColumnType::String);
    let key_types = vec![("id".to_owned(), "S".to_owned())];
    drive_import_until_done(
        &mut sim,
        &mut meta,
        &dest,
        &live,
        &store,
        &import_id,
        &base_schema,
        &key_types,
        InputCompressionType::Gzip,
        seed,
    );

    let row = meta.import(&import_id).expect("row present");
    assert_eq!(row.status, ImportStatus::Completed);
    assert_eq!(row.imported_item_count, model.len() as u64);

    let dest_leader = elect(&mut sim, &dest, &live, seed);
    items_by_pk(read_all_base_items(&dest.nodes[dest_leader]).into_values())
}

fn scenario_import_with_bucket_faults_converges_to_the_same_content(seed: u64) {
    let with_fault = run_single_tablet_import_scenario(seed, true);
    let without_fault = run_single_tablet_import_scenario(seed, false);
    assert_eq!(
        with_fault, without_fault,
        "[seed={seed}] the identical seed with faults cleared must converge to the same final \
         table contents"
    );
}

#[test]
fn import_with_bucket_faults_converges_to_the_same_content() {
    for_each_seed(
        "import_with_bucket_faults_converges_to_the_same_content",
        scenario_import_with_bucket_faults_converges_to_the_same_content,
    );
}

// --- cell 9: import_failure_rolls_back_the_target_table --------------------

fn scenario_import_failure_rolls_back_the_target_table(seed: u64) {
    let mut sim = Simulator::new(seed);
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    let root = "AWSDynamoDB/overrun01";
    let mut lines = String::new();
    // Every item malformed (missing its own partition key) — past
    // `MAX_MALFORMED_ITEMS`, the import must fail immediately rather than
    // retry forever (content this driver can never re-derive correctly by
    // retrying, unlike an I/O fault).
    let overrun_count = MAX_MALFORMED_ITEMS + 5;
    for _ in 0..overrun_count {
        lines.push_str(&serde_json::json!({"Item": {"val": {"S": "no-id"}}}).to_string());
        lines.push('\n');
    }
    let gz = gzip_bytes(lines.as_bytes());
    block_on(store.put(&format!("{root}/data/0000.json.gz"), &gz)).unwrap();
    let files_line = serde_json::json!({
        "itemCount": overrun_count,
        "dataFileS3Key": format!("{root}/data/0000.json.gz"),
    });
    block_on(store.put(
        &format!("{root}/manifest-files.json"),
        format!("{}\n", serde_json::to_string(&files_line).unwrap()).as_bytes(),
    ))
    .unwrap();
    let summary = serde_json::json!({
        "version": "2020-06-30",
        "manifestFilesS3Key": format!("{root}/manifest-files.json"),
    });
    block_on(store.put(
        &format!("{root}/manifest-summary.json"),
        serde_json::to_string(&summary).unwrap().as_bytes(),
    ))
    .unwrap();

    let dest_engines = engines();
    let dest = start_group(&sim, &dest_engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(1));

    let mut meta = base_meta();
    const TARGET_TABLE: &str = "imported_overrun";
    let import_id = dynamo_wire::import_arn(TARGET_TABLE, "i0000000000005");
    begin_import(
        &mut meta,
        &import_id,
        TARGET_TABLE,
        TabletId(1),
        "customer-bucket",
        InputCompressionType::Gzip,
    );
    assert!(meta.has_table_schema(TARGET_TABLE));

    let base_schema = TableSchema::simple("id", ColumnType::String);
    let key_types = vec![("id".to_owned(), "S".to_owned())];
    drive_import_until_done(
        &mut sim,
        &mut meta,
        &dest,
        &live,
        &store,
        &import_id,
        &base_schema,
        &key_types,
        InputCompressionType::Gzip,
        seed,
    );

    let row = meta.import(&import_id).expect("row present");
    assert!(
        matches!(row.status, ImportStatus::Failed { .. }),
        "[seed={seed}] expected Failed, got {:?}",
        row.status
    );
    assert!(
        !meta.has_table_schema(TARGET_TABLE),
        "[seed={seed}] a failed import must roll back its own half-created target table's \
         schema"
    );
    assert!(
        !meta.tablets.contains_key(&TabletId(1)),
        "[seed={seed}] a failed import must roll back its own half-created target table's \
         tablet"
    );
}

#[test]
fn import_failure_rolls_back_the_target_table() {
    for_each_seed(
        "import_failure_rolls_back_the_target_table",
        scenario_import_failure_rolls_back_the_target_table,
    );
}

// --- cell 10: import_leader_kill_mid_job_stuck_timeout_fails ---------------

/// Pins ADR 0068 §6/§9's residual #3 (no crash-resumability, `import_loop`'s
/// own bounded stuck timeout is the only backstop): a wedged import (its
/// customer bucket permanently unreachable, modeling the node running its
/// tablet leader having died with nobody ever picking it back up) stays
/// `InProgress` until virtual time crosses `IMPORT_STUCK_TIMEOUT`, and does
/// NOT fail early. Driven entirely by `env.now()` — never a real clock.
fn scenario_import_leader_kill_mid_job_stuck_timeout_fails(seed: u64) {
    let mut sim = Simulator::new(seed);
    let dest_engines = engines();
    let dest = start_group(&sim, &dest_engines, TabletId(1), KeyRange::whole());
    let live = [0, 1, 2];
    sim.run_for(Duration::from_secs(1));

    // A bucket permanently unavailable — every tick makes NO progress,
    // exactly the shape `import_loop`'s own in-memory `ImportProgress::
    // last_progress` tracker exists to bound.
    let store = SimSegmentStore::new(sim.env(nid(NODES[0])));
    store.set_unavailable_until(Nanos(u64::MAX));

    let mut meta = base_meta();
    const TARGET_TABLE: &str = "imported_stuck";
    let import_id = dynamo_wire::import_arn(TARGET_TABLE, "i0000000000006");
    begin_import(
        &mut meta,
        &import_id,
        TARGET_TABLE,
        TabletId(1),
        "customer-bucket",
        InputCompressionType::Gzip,
    );

    let base_schema = TableSchema::simple("id", ColumnType::String);
    let key_types = vec![("id".to_owned(), "S".to_owned())];

    let env = sim.env(nid(NODES[0]));
    let started = env.now();
    // Tick a few times well before the timeout — must stay InProgress.
    for _ in 0..3 {
        sim.run_for(Duration::from_secs(30));
        let leader = elect(&mut sim, &dest, &live, seed);
        let outcome = import_tick_mirror(
            &mut sim,
            &dest.nodes[leader],
            &store,
            &base_schema,
            &key_types,
            InputCompressionType::Gzip,
        );
        assert!(
            matches!(outcome, ImportTickOutcome::NoProgress),
            "[seed={seed}] a permanently unavailable bucket must never report progress"
        );
        assert!(
            env.now().duration_since(started) < Duration::from_nanos(IMPORT_STUCK_TIMEOUT_NANOS),
            "[seed={seed}] must not yet have crossed the stuck timeout"
        );
    }
    assert_eq!(
        meta.import(&import_id).map(|r| r.status.clone()),
        Some(ImportStatus::InProgress)
    );

    // Advance well past the stuck timeout.
    sim.run_for(Duration::from_secs(600));
    assert!(env.now().duration_since(started) >= Duration::from_nanos(IMPORT_STUCK_TIMEOUT_NANOS));
    fail_import_and_cleanup_mirror(
        &mut meta,
        &import_id,
        "import made no progress in time",
        0,
        0,
        0,
        0,
        seed,
    );

    let row = meta.import(&import_id).expect("row present");
    assert!(matches!(row.status, ImportStatus::Failed { .. }));
    assert!(!meta.has_table_schema(TARGET_TABLE));
}

#[test]
fn import_leader_kill_mid_job_stuck_timeout_fails() {
    for_each_seed(
        "import_leader_kill_mid_job_stuck_timeout_fails",
        scenario_import_leader_kill_mid_job_stuck_timeout_fails,
    );
}

// ============================================================================
// Catalog invariants (bare Metadata property test, no tablets needed)
// ============================================================================

/// A small, deterministic PRNG (splitmix64) — this cell's own interleaving
/// choices must be reproducible from `seed` like every other corpus cell,
/// but need no connection to the `Simulator`'s own RNG stream (no tablets
/// are hosted at all in this cell).
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn pick(rng: &mut u64, v: &[String]) -> Option<usize> {
    if v.is_empty() {
        return None;
    }
    Some((splitmix64(rng) % v.len() as u64) as usize)
}

fn assert_export_pagination_covers_everything(all: &[dynamo_wire::ExportSummary], seed: u64) {
    let mut collected = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let (page, next) = dynamo_wire::paginate_export_summaries(all, token.as_deref(), Some(3));
        assert!(
            page.len() <= 3,
            "[seed={seed}] a page exceeded its own max_results"
        );
        collected.extend(page.iter().map(|s| s.export_arn.clone()));
        match next {
            Some(t) => token = Some(t),
            None => break,
        }
        assert!(
            collected.len() <= all.len(),
            "[seed={seed}] ListExports pagination looped forever"
        );
    }
    let mut expected: Vec<String> = all.iter().map(|s| s.export_arn.clone()).collect();
    expected.sort();
    assert_eq!(
        collected, expected,
        "[seed={seed}] paginating ListExports end to end did not reproduce the full catalog \
         exactly once each"
    );
}

fn assert_import_pagination_covers_everything(all: &[dynamo_wire::ImportSummary], seed: u64) {
    let mut collected = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let (page, next) = dynamo_wire::paginate_import_summaries(all, token.as_deref(), Some(3));
        assert!(
            page.len() <= 3,
            "[seed={seed}] a page exceeded its own max_results"
        );
        collected.extend(page.iter().map(|s| s.import_arn.clone()));
        match next {
            Some(t) => token = Some(t),
            None => break,
        }
        assert!(
            collected.len() <= all.len(),
            "[seed={seed}] ListImports pagination looped forever"
        );
    }
    let mut expected: Vec<String> = all.iter().map(|s| s.import_arn.clone()).collect();
    expected.sort();
    assert_eq!(
        collected, expected,
        "[seed={seed}] paginating ListImports end to end did not reproduce the full catalog \
         exactly once each"
    );
}

fn scenario_catalog_invariants_random_interleavings(seed: u64) {
    let mut rng = seed ^ 0xC0FFEE;
    let mut meta = base_meta();

    let mut seen_export_ids: BTreeSet<String> = BTreeSet::new();
    let mut seen_import_ids: BTreeSet<String> = BTreeSet::new();
    let mut import_table_arns: BTreeMap<String, String> = BTreeMap::new();
    let mut in_progress_exports: Vec<String> = Vec::new();
    let mut in_progress_imports: Vec<String> = Vec::new();
    let mut terminal_exports: BTreeSet<String> = BTreeSet::new();
    let mut terminal_imports: BTreeSet<String> = BTreeSet::new();
    // The exact counts/reason each row froze at, the instant its own ONE
    // terminal command committed — re-checked at the very end against the
    // live catalog, so nothing else (in particular, no other command this
    // loop happens to interleave) silently drifted them afterward.
    let mut frozen_export_counts: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    let mut frozen_import_counts: BTreeMap<String, (u64, u64, u64, u64)> = BTreeMap::new();
    let mut next_target = 0u64;

    for step in 0..200u64 {
        match splitmix64(&mut rng) % 6 {
            0 => {
                let id = format!("{}/export/e{step:06}", dynamo_wire::table_arn(SRC_TABLE));
                assert!(
                    seen_export_ids.insert(id.clone()),
                    "[seed={seed}] export id `{id}` reused"
                );
                begin_export(&mut meta, &id, SRC_TABLE, "bucket");
                in_progress_exports.push(id);
            }
            1 => {
                if let Some(pos) = pick(&mut rng, &in_progress_exports) {
                    let id = in_progress_exports.remove(pos);
                    let outcome = meta.apply(&MetaCommand::CompleteExport {
                        export_id: id.clone(),
                        item_count: 3,
                        billed_size_bytes: 30,
                        export_manifest: "m".to_owned(),
                        completed_wall_ms: 10,
                    });
                    assert_eq!(
                        outcome,
                        ApplyOutcome::Applied,
                        "[seed={seed}] CompleteExport rejected: {outcome:?}"
                    );
                    frozen_export_counts.insert(id.clone(), (3, 30));
                    terminal_exports.insert(id);
                }
            }
            2 => {
                if let Some(pos) = pick(&mut rng, &in_progress_exports) {
                    let id = in_progress_exports.remove(pos);
                    let outcome = meta.apply(&MetaCommand::FailExport {
                        export_id: id.clone(),
                        reason: "test".to_owned(),
                        completed_wall_ms: 10,
                    });
                    assert_eq!(
                        outcome,
                        ApplyOutcome::Applied,
                        "[seed={seed}] FailExport rejected: {outcome:?}"
                    );
                    frozen_export_counts.insert(id.clone(), (0, 0));
                    terminal_exports.insert(id);
                }
            }
            3 => {
                next_target += 1;
                let target = format!("catalog_target_{next_target}");
                let id = dynamo_wire::import_arn(&target, &format!("i{step:06}"));
                assert!(
                    seen_import_ids.insert(id.clone()),
                    "[seed={seed}] import id `{id}` reused"
                );
                begin_import(
                    &mut meta,
                    &id,
                    &target,
                    TabletId(1000 + next_target),
                    "bucket",
                    InputCompressionType::Gzip,
                );
                import_table_arns.insert(id.clone(), dynamo_wire::table_arn(&target));
                in_progress_imports.push(id);
            }
            4 => {
                if let Some(pos) = pick(&mut rng, &in_progress_imports) {
                    let id = in_progress_imports.remove(pos);
                    complete_import_mirror(&mut meta, &id, 3, 3, 0, 30, seed);
                    frozen_import_counts.insert(id.clone(), (3, 3, 0, 30));
                    terminal_imports.insert(id);
                }
            }
            _ => {
                if let Some(pos) = pick(&mut rng, &in_progress_imports) {
                    let id = in_progress_imports.remove(pos);
                    let outcome = meta.apply(&MetaCommand::FailImport {
                        import_id: id.clone(),
                        reason: "test".to_owned(),
                        processed_item_count: 1,
                        imported_item_count: 0,
                        error_count: 1,
                        processed_size_bytes: 5,
                        completed_wall_ms: 10,
                    });
                    assert_eq!(
                        outcome,
                        ApplyOutcome::Applied,
                        "[seed={seed}] FailImport rejected: {outcome:?}"
                    );
                    frozen_import_counts.insert(id.clone(), (1, 0, 1, 5));
                    terminal_imports.insert(id);
                }
            }
        }

        // A terminal export never re-accepts `CompleteExport` — real state-
        // machine safety (`Metadata::apply`'s own `ExportStatus::Completed
        // => Rejected` / `InProgress`-only guard), not merely this test's
        // own driver discipline.
        for id in &terminal_exports {
            let outcome = meta.apply(&MetaCommand::CompleteExport {
                export_id: id.clone(),
                item_count: 999,
                billed_size_bytes: 999,
                export_manifest: "different".to_owned(),
                completed_wall_ms: 999,
            });
            assert!(
                matches!(outcome, ApplyOutcome::Rejected(_)),
                "[seed={seed}] a terminal export accepted a second CompleteExport: {outcome:?}"
            );
        }
        for id in &terminal_imports {
            let outcome = meta.apply(&MetaCommand::CompleteImport {
                import_id: id.clone(),
                processed_item_count: 999,
                imported_item_count: 999,
                error_count: 999,
                processed_size_bytes: 999,
                completed_wall_ms: 999,
            });
            assert!(
                matches!(outcome, ApplyOutcome::Rejected(_)),
                "[seed={seed}] a terminal import accepted a second CompleteImport: {outcome:?}"
            );
        }
    }

    // Ids never reused: every one we ever minted names exactly one row.
    assert_eq!(meta.exports.len(), seen_export_ids.len());
    assert_eq!(meta.imports.len(), seen_import_ids.len());
    for id in &seen_export_ids {
        assert!(
            meta.export(id).is_some(),
            "[seed={seed}] export `{id}` vanished"
        );
    }
    for id in &seen_import_ids {
        assert!(
            meta.import(id).is_some(),
            "[seed={seed}] import `{id}` vanished"
        );
    }

    // A row's frozen counts never drift once this driver's own ONE terminal
    // command decided them.
    for (id, (item_count, billed_size_bytes)) in &frozen_export_counts {
        let row = meta.export(id).expect("row present");
        assert_eq!(
            &(row.item_count, row.billed_size_bytes),
            &(*item_count, *billed_size_bytes),
            "[seed={seed}] export `{id}`'s frozen counts changed after its own terminal command"
        );
    }
    for (id, (processed, imported, errors, bytes)) in &frozen_import_counts {
        let row = meta.import(id).expect("row present");
        assert_eq!(
            &(
                row.processed_item_count,
                row.imported_item_count,
                row.error_count,
                row.processed_size_bytes
            ),
            &(*processed, *imported, *errors, *bytes),
            "[seed={seed}] import `{id}`'s frozen counts changed after its own terminal command"
        );
    }

    // `ListExports`/`ListImports` pagination, via the REAL production
    // pagination functions, reproduces the full catalog exactly.
    let all_export_summaries: Vec<dynamo_wire::ExportSummary> = meta
        .exports
        .iter()
        .map(|(id, row)| dynamo_wire::ExportSummary {
            export_arn: id.clone(),
            status: match &row.status {
                ExportStatus::InProgress => "IN_PROGRESS",
                ExportStatus::Completed => "COMPLETED",
                ExportStatus::Failed { .. } => "FAILED",
            },
        })
        .collect();
    assert_export_pagination_covers_everything(&all_export_summaries, seed);

    let all_import_summaries: Vec<dynamo_wire::ImportSummary> = meta
        .imports
        .iter()
        .map(|(id, row)| dynamo_wire::ImportSummary {
            import_arn: id.clone(),
            status: match &row.status {
                ImportStatus::InProgress => "IN_PROGRESS",
                ImportStatus::Completed => "COMPLETED",
                ImportStatus::Failed { .. } => "FAILED",
            },
            table_arn: row.target_table_arn.clone(),
            start_wall_ms: row.created_wall_ms,
            end_wall_ms: row.completed_wall_ms,
        })
        .collect();
    assert_import_pagination_covers_everything(&all_import_summaries, seed);

    // `TableArn` filtering: each import's own target table is unique
    // (`next_target` mints a fresh name every time), so its own
    // `TableArn`-filtered listing must show exactly its own row.
    for id in &seen_import_ids {
        let table_arn = &import_table_arns[id];
        let filtered: Vec<&dynamo_wire::ImportSummary> = all_import_summaries
            .iter()
            .filter(|s| &s.table_arn == table_arn)
            .collect();
        assert_eq!(
            filtered.len(),
            1,
            "[seed={seed}] import `{id}`'s own target table's filtered listing must show \
             exactly one row"
        );
        assert_eq!(filtered[0].import_arn, *id);
    }
}

#[test]
fn catalog_invariants_random_interleavings() {
    for_each_seed(
        "catalog_invariants_random_interleavings",
        scenario_catalog_invariants_random_interleavings,
    );
}
