//! `ImportTable`'s **import driver** (ADR 0068 §6, S-05 PR 2): a per-tablet,
//! leader-side, event-driven loop that seeds an `InProgress` import's single
//! `Building` destination tablet from a customer-owned S3 bucket's DynamoDB
//! JSON export layout, then activates it. See
//! `crate::dynamo::create_import` for the wire op that mints an `InProgress`
//! [`animus_control::ImportRow`] via `MetaCommand::BeginImport`, and
//! `docs/adr/0068-s3-export-import.md`'s PR 2 as-built note for the design
//! decision this module implements.
//!
//! ## Shape: `backup_restore.rs`'s twin, sourcing from a customer bucket
//!
//! Structurally identical to [`crate::backup_restore`]'s own restore
//! driver — same "run everywhere, self-gate per tablet on `group.is_leader
//! ()`" discovery, same "no durable cursor, re-sweep the whole thing every
//! tick" resumability discipline (safe here for the identical reason: every
//! seeded row merges at a **fixed, constant version**
//! ([`IMPORT_SEED_VERSION`]) via `KvCommand::SeedBatch`'s own
//! merge-at-carried-version semantics, so re-seeding an already-applied row
//! with the same version is a verified no-op — `animus-storage`'s `merge`
//! applies only when the new version is *strictly* greater than what's
//! already stored, so a bare `<=` on a repeat call is silently skipped, not
//! merely idempotent by luck), same bounded-liveness stuck-timeout
//! ([`IMPORT_STUCK_TIMEOUT`]) that fails a wedged import. The one structural
//! difference: this driver's source is an arbitrary **customer** bucket
//! (built per-import via [`crate::ExportStoreFactory`] — the identical seam
//! `ExportTableToPointInTime`'s job driver uses, ADR 0068 §2's own doc; kept
//! as one shared seam rather than a second one for the mirror-image data
//! flow), not this cluster's own backup store — so every read here can fail
//! for reasons a backup restore's own trusted store never has to consider
//! (a wrong bucket, a missing object, a malformed customer-supplied file);
//! every such fault is treated as *retryable* (`NoProgress`, the stuck-
//! timeout eventually gives up) **except** too many malformed items, which
//! is content the import can never re-derive correctly by retrying and so
//! fails immediately (see [`MAX_MALFORMED_ITEMS`]'s own doc).
//!
//! ## Item -> row derivation: the identical primitive PITR replay uses
//!
//! Each decoded DynamoDB-JSON item is turned into `KIND_BASE`/`KIND_LSI`
//! writes via [`crate::dynamo::kind_writes_for_item`] — the same pure
//! function a live write's own leader-side evaluation, and PITR segment
//! replay (`crate::backup_restore::replay_pitr_segments`), already use — so
//! an imported row's LSI bookkeeping is derived identically rather than a
//! third, independently-maintained copy of that logic. `KIND_CHANGE`
//! (the derived change-log half) is discarded, the identical PITR-replay
//! convention (an imported table's own change log starts empty). **GSIs are
//! never derived here** — `ImportRow::gsi_defs` is declared only once this
//! driver's own [`complete_import`] fires, after which the ordinary
//! backfill seeder rebuilds every GSI fresh from the table's now-fully-
//! seeded `KIND_BASE` content, the identical [`crate::backup_restore`]
//! precedent (see that module's doc for why seeding a footprint/GSI row
//! here would only ever be redundant).
//!
//! ## Object resolution: two prefix shapes, like real DynamoDB
//!
//! `S3KeyPrefix` may name either the level directly ABOVE the export's own
//! `AWSDynamoDB/<id>/` folder (this driver lists `AWSDynamoDB/` and resolves
//! the one `manifest-summary.json` it finds under it) or the export folder
//! ITSELF (`manifest-summary.json` sits at the resolved store's own root) —
//! see [`resolve_manifest`]'s own doc for the exact probe order and
//! [`rebase_recorded_key`]'s doc for why a manifest's own recorded
//! `manifestFilesS3Key`/`dataFileS3Key` need a path rewrite in the second
//! case (this adapter's own export writes those fields relative to
//! whatever store the export job itself was built over, ADR 0068 §1 — never
//! a real bucket-absolute key — so this driver's own resolution must invert
//! whichever of the two shapes actually resolved the manifest object).

use std::collections::BTreeMap;
use std::io::Read;
use std::time::{Duration, Instant};

use animus_control::{ImportId, ImportRow, ImportStatus, MetaCommand, Metadata, ProposeResult};
use animus_cp_data::backup as backup_codec;
use animus_dynamo::AttributeValue;
use animus_env::{Clock, SegmentStore};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::{ClientCtx, CpGroup};

/// This loop's tick cadence — matches [`crate::backup_restore::
/// RESTORE_TICK_INTERVAL`] and every other per-tablet consumer loop in this
/// crate.
pub(crate) const IMPORT_TICK_INTERVAL: Duration = Duration::from_millis(200);

/// How long an `InProgress` import may go with no observed forward progress
/// before this driver gives up and proposes `FailImport` — mirrors
/// [`crate::backup_restore::RESTORE_STUCK_TIMEOUT`]'s own bound and
/// rationale (a genuinely unreachable customer bucket, or one that never
/// finishes writing its own export, must not leave an import `InProgress`
/// forever).
pub(crate) const IMPORT_STUCK_TIMEOUT: Duration = Duration::from_secs(600);

/// How long [`propose_local`]'s own confirm wait allows before giving up for
/// this tick — the next tick simply retries (every proposal here is
/// idempotent or safe to repeat).
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);

/// How many `SeedRow`s [`import_tick`] batches into one `SeedBatch` propose
/// — bounded so one Raft entry never grows unboundedly for a very large
/// import, mirroring the on-demand backup capture driver's own chunk-size
/// discipline (`backup_capture.rs`'s `CHUNK_ROWS`) at a comparable order of
/// magnitude.
const IMPORT_SEED_BATCH_ROWS: usize = 500;

/// The fixed version every row this driver seeds carries (see this module's
/// own doc for why a constant is correct here, unlike backup restore's own
/// captured-at-real-version rows) — never `0`, which `animus-storage`'s
/// engines otherwise treat as "nothing written yet."
const IMPORT_SEED_VERSION: u64 = 1;

/// How many malformed (key-schema-invalid, or undecodable) items one import
/// tolerates before it gives up and fails outright, rather than retrying
/// forever — ADR 0068 §6's own documented cap, matching real DynamoDB's
/// behavior of surfacing a bounded `ErrorCount` and eventually failing a
/// sufficiently corrupt export rather than importing nothing at all. A
/// malformed item is content the export itself got wrong; retrying the
/// identical bytes can never fix it, so (unlike every I/O fault in this
/// module, which is retried) crossing this cap is immediately terminal.
const MAX_MALFORMED_ITEMS: u64 = 10_000;

/// This driver's own per-import liveness tracking — deliberately **in-memory
/// only** (see the module doc's "no durable cursor" note): reset whenever
/// this node starts observing an import fresh (first tick, or a leader
/// change that hands the tablet to this node), advanced on any observed
/// forward progress.
struct ImportProgress {
    last_progress: Instant,
}

/// Every led tablet's own import-seeding step, once per
/// [`IMPORT_TICK_INTERVAL`] tick.
pub(crate) async fn import_loop(ctx: ClientCtx) {
    let tracking: Mutex<BTreeMap<ImportId, ImportProgress>> = Mutex::new(BTreeMap::new());
    loop {
        tokio::time::sleep(IMPORT_TICK_INTERVAL).await;
        let meta = ctx.effective_metadata();
        if meta.imports.is_empty() {
            continue;
        }
        let in_progress: Vec<(ImportId, ImportRow)> = meta
            .imports
            .iter()
            .filter(|(_, row)| matches!(row.status, ImportStatus::InProgress))
            .map(|(id, row)| (id.clone(), row.clone()))
            .collect();
        if in_progress.is_empty() {
            continue;
        }
        let hosted = ctx.edge.hosted_groups();
        for (import_id, row) in in_progress {
            let Some(group) = hosted
                .iter()
                .find(|(t, _)| *t == row.tablet)
                .map(|(_, g)| g.clone())
            else {
                tracing::debug!(import_id, tablet = ?row.tablet, "import: tablet not hosted here yet");
                continue; // not (or not yet) hosted here
            };
            if !group.is_leader() {
                tracing::debug!(import_id, tablet = ?row.tablet, "import: hosted here but not leader yet");
                continue;
            }
            tracing::debug!(import_id, tablet = ?row.tablet, "import: ticking as leader");
            let now = Instant::now();
            let stuck = {
                let mut guard = tracking.lock().await;
                let entry = guard
                    .entry(import_id.clone())
                    .or_insert_with(|| ImportProgress { last_progress: now });
                now.duration_since(entry.last_progress) > IMPORT_STUCK_TIMEOUT
            };
            if stuck {
                fail_import_and_cleanup(
                    &ctx,
                    &import_id,
                    &row,
                    "import made no progress in time",
                    0,
                    0,
                    0,
                    0,
                )
                .await;
                tracking.lock().await.remove(&import_id);
                continue;
            }
            match import_tick(&ctx, &group, &import_id, &row).await {
                ImportTickOutcome::Completed | ImportTickOutcome::Failed => {
                    tracking.lock().await.remove(&import_id);
                }
                ImportTickOutcome::NoProgress => {}
            }
        }
    }
}

/// What one [`import_tick`] call accomplished.
enum ImportTickOutcome {
    /// Every data file was read and seeded (or skipped, up to the malformed
    /// cap); `CompleteImport` was proposed.
    Completed,
    /// Too many malformed items — `FailImport` was proposed and the
    /// half-created target table was dropped.
    Failed,
    /// A store-read fault or corrupt manifest — nothing committed this
    /// tick, safe to retry next tick.
    NoProgress,
}

/// One import step for `(import_id, row)`, seeding **this node's own leader
/// handle** of `group` — the whole customer bucket, in one call (see the
/// module doc's "no durable cursor" discussion for why re-sweeping the
/// whole thing on every retry is safe here).
async fn import_tick(
    ctx: &ClientCtx,
    group: &CpGroup,
    import_id: &str,
    row: &ImportRow,
) -> ImportTickOutcome {
    let build_store = ctx
        .export_store_factory
        .lock()
        .expect("export store factory lock")
        .clone();
    let store = match build_store(&row.s3_bucket, row.s3_prefix.as_deref()) {
        Ok(s) => s,
        Err(err) => {
            tracing::debug!(import_id, %err, "import: building customer S3 store failed, retrying");
            return ImportTickOutcome::NoProgress;
        }
    };

    let manifest = match resolve_manifest(store.as_ref()).await {
        Ok(Some(m)) => m,
        Ok(None) => {
            tracing::debug!(
                import_id,
                "import: manifest-summary.json not found, retrying"
            );
            return ImportTickOutcome::NoProgress;
        }
        Err(err) => {
            tracing::debug!(import_id, %err, "import: manifest read failed, retrying");
            return ImportTickOutcome::NoProgress;
        }
    };
    let summary: Value = match serde_json::from_slice(&manifest.bytes) {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(import_id, %err, "import: manifest-summary.json is corrupt");
            return ImportTickOutcome::NoProgress;
        }
    };
    let Some(files_key_raw) = summary.get("manifestFilesS3Key").and_then(Value::as_str) else {
        tracing::warn!(
            import_id,
            "import: manifest-summary.json missing manifestFilesS3Key"
        );
        return ImportTickOutcome::NoProgress;
    };
    let files_key = rebase_recorded_key(files_key_raw, manifest.direct);
    let files_bytes = match store.get(&files_key).await {
        Ok(Some(b)) => b,
        Ok(None) => {
            tracing::debug!(
                import_id,
                files_key,
                "import: manifest-files.json not found, retrying"
            );
            return ImportTickOutcome::NoProgress;
        }
        Err(err) => {
            tracing::debug!(import_id, %err, "import: manifest-files.json read failed, retrying");
            return ImportTickOutcome::NoProgress;
        }
    };
    let files_text = match String::from_utf8(files_bytes) {
        Ok(t) => t,
        Err(_) => {
            tracing::warn!(import_id, "import: manifest-files.json is not utf8");
            return ImportTickOutcome::NoProgress;
        }
    };

    let meta = ctx.effective_metadata();
    let mut processed_item_count: u64 = 0;
    let mut imported_item_count: u64 = 0;
    let mut error_count: u64 = 0;
    let mut processed_size_bytes: u64 = 0;
    let mut pending: Vec<animus_cp_data::SeedRow> = Vec::new();

    for line in files_text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(import_id, %err, "import: manifest-files.json line is corrupt");
                return ImportTickOutcome::NoProgress;
            }
        };
        let Some(data_key_raw) = entry.get("dataFileS3Key").and_then(Value::as_str) else {
            tracing::warn!(
                import_id,
                "import: manifest-files.json entry missing dataFileS3Key"
            );
            return ImportTickOutcome::NoProgress;
        };
        let data_key = rebase_recorded_key(data_key_raw, manifest.direct);
        let bytes = match store.get(&data_key).await {
            Ok(Some(b)) => b,
            Ok(None) => {
                tracing::debug!(import_id, data_key, "import: data file not found, retrying");
                return ImportTickOutcome::NoProgress;
            }
            Err(err) => {
                tracing::debug!(import_id, %err, "import: data file read failed, retrying");
                return ImportTickOutcome::NoProgress;
            }
        };
        processed_size_bytes += bytes.len() as u64;
        let text = match row.input_compression {
            animus_control::InputCompressionType::Gzip => match gunzip_bytes(&bytes) {
                Ok(t) => t,
                Err(err) => {
                    tracing::warn!(import_id, data_key, %err, "import: data file is not valid gzip");
                    return ImportTickOutcome::NoProgress;
                }
            },
            animus_control::InputCompressionType::None => match String::from_utf8(bytes) {
                Ok(t) => t,
                Err(_) => {
                    tracing::warn!(import_id, data_key, "import: data file is not utf8");
                    return ImportTickOutcome::NoProgress;
                }
            },
            // Rejected at wire-decode time (ADR 0068 §6) — never reaches a
            // committed `ImportRow`.
            animus_control::InputCompressionType::Zstd => {
                tracing::warn!(import_id, "import: ZSTD reached the driver — this is a bug");
                return ImportTickOutcome::NoProgress;
            }
        };

        for item_line in text.lines() {
            let item_line = item_line.trim();
            if item_line.is_empty() {
                continue;
            }
            processed_item_count += 1;
            match decode_and_derive_item(&meta, row, item_line) {
                Ok(writes) => {
                    imported_item_count += 1;
                    for (kind, key, value) in writes {
                        pending.push((
                            kind,
                            key,
                            value.map(|v| backup_codec::encode_restored_value(&v)),
                            IMPORT_SEED_VERSION,
                        ));
                    }
                    if pending.len() >= IMPORT_SEED_BATCH_ROWS
                        && !propose_local(group, std::mem::take(&mut pending)).await
                    {
                        return ImportTickOutcome::NoProgress;
                    }
                }
                Err(reason) => {
                    error_count += 1;
                    tracing::debug!(import_id, %reason, "import: skipping a malformed item");
                    if error_count > MAX_MALFORMED_ITEMS {
                        fail_import_and_cleanup(
                            ctx,
                            import_id,
                            row,
                            &format!(
                                "too many malformed items ({error_count} skipped, cap is \
                                 {MAX_MALFORMED_ITEMS})"
                            ),
                            processed_item_count,
                            imported_item_count,
                            error_count,
                            processed_size_bytes,
                        )
                        .await;
                        return ImportTickOutcome::Failed;
                    }
                }
            }
        }
    }
    if !pending.is_empty() && !propose_local(group, pending).await {
        return ImportTickOutcome::NoProgress;
    }

    complete_import(
        ctx,
        import_id,
        processed_item_count,
        imported_item_count,
        error_count,
        processed_size_bytes,
    )
    .await;
    ImportTickOutcome::Completed
}

/// `manifest-summary.json`'s own bytes, plus whether it was found via the
/// "direct" probe (see [`resolve_manifest`]'s own doc).
struct ResolvedManifest {
    bytes: Vec<u8>,
    direct: bool,
}

/// Resolve `manifest-summary.json` under `store` — two shapes, matching real
/// DynamoDB's own `ImportTable` `S3KeyPrefix` contract:
///
/// 1. **Direct**: `S3KeyPrefix` already names the export's own root folder
///    (`[..]/AWSDynamoDB/<export-id>/`) — `manifest-summary.json` sits at
///    `store`'s own root. Tried first (a cheap single `get`).
/// 2. **Indirect**: `S3KeyPrefix` names the level ABOVE that (the same
///    prefix an `ExportTableToPointInTime` call itself was given) —
///    `store.list("AWSDynamoDB")` finds the one `<export-id>/manifest-
///    summary.json` under it. The first match in sorted order wins if more
///    than one export ever shares a prefix (this adapter names no way to
///    disambiguate further, matching real DynamoDB's own "the prefix must
///    resolve to exactly one export" contract loosely — never encountered
///    in practice, since an operator names a fresh prefix per export).
async fn resolve_manifest(store: &dyn SegmentStore) -> std::io::Result<Option<ResolvedManifest>> {
    if let Some(bytes) = store.get("manifest-summary.json").await? {
        return Ok(Some(ResolvedManifest {
            bytes,
            direct: true,
        }));
    }
    let mut candidates: Vec<String> = store
        .list("AWSDynamoDB")
        .await?
        .into_iter()
        .filter(|id| id.ends_with("/manifest-summary.json"))
        .collect();
    candidates.sort();
    let Some(key) = candidates.into_iter().next() else {
        return Ok(None);
    };
    let Some(bytes) = store.get(&key).await? else {
        return Ok(None); // deleted between list and get — retry next tick
    };
    Ok(Some(ResolvedManifest {
        bytes,
        direct: false,
    }))
}

/// Rewrite a manifest's own recorded `manifestFilesS3Key`/`dataFileS3Key`
/// (always `AWSDynamoDB/<export-id>/<rest>`, this adapter's own export
/// layout — ADR 0068 §1) into a key relative to whichever base actually
/// resolved `manifest-summary.json`.
///
/// **Why this rewrite is needed, not merely defensive**: this adapter's own
/// export job (`crate::dynamo::run_export_job_inner`) writes every recorded
/// key relative to whatever [`crate::ExportStoreFactory`]-built store IT was
/// given — i.e. relative to the export call's own `S3Prefix`, never a
/// bucket-absolute key. When THIS import's own `S3KeyPrefix` matches that
/// same level (`direct: false`, [`resolve_manifest`]'s indirect case), those
/// recorded keys already resolve correctly against this import's identically-
/// scoped store, unchanged. But when this import's own `S3KeyPrefix` instead
/// points directly at the export's root folder (`direct: true`), the
/// recorded key's own leading `AWSDynamoDB/<export-id>/` segment is now
/// redundant — this import's store is already scoped one level deeper — so
/// it must be stripped before use, or every subsequent read would look one
/// level too deep and never find the object.
fn rebase_recorded_key(recorded: &str, direct: bool) -> String {
    if !direct {
        return recorded.to_owned();
    }
    // `recorded` is always `"AWSDynamoDB/<export-id>/<rest>"` — this
    // adapter's own export never emits anything else — so splitting on the
    // first two `/`s recovers `<rest>` (`"manifest-files.json"` or
    // `"data/0000.json.gz"`). Falls back to the key verbatim on a shape this
    // adapter never actually produces (real AWS tooling, say), rather than
    // panicking.
    let mut parts = recorded.splitn(3, '/');
    let (Some(_root), Some(_id), Some(rest)) = (parts.next(), parts.next(), parts.next()) else {
        return recorded.to_owned();
    };
    rest.to_owned()
}

/// gunzip `bytes` into a UTF-8 string, or an error naming what failed —
/// never leaked verbatim to a client (the caller only ever counts/logs this),
/// mirroring [`crate::dynamo::gzip_bytes`]'s own inverse.
fn gunzip_bytes(bytes: &[u8]) -> Result<String, std::io::Error> {
    let mut decoder = flate2::read::GzDecoder::new(bytes);
    let mut out = String::new();
    decoder.read_to_string(&mut out)?;
    Ok(out)
}

/// One derived kind write: `(kind, physical key, value — `None` is a
/// tombstone)`, [`crate::dynamo::kind_writes_for_item`]'s own per-entry
/// shape.
type DerivedKindWrite = (u8, Vec<u8>, Option<Vec<u8>>);

/// Decode one `{"Item": {...}}` line, validate its key attributes against
/// `row.base_schema`/`row.key_types`, and derive its `KIND_BASE`/`KIND_LSI`
/// writes (see the module doc). `Err` names the reason a malformed item was
/// skipped, for the debug log [`import_tick`] emits — never surfaced to a
/// client (only the numeric `ErrorCount` is).
fn decode_and_derive_item(
    meta: &Metadata,
    row: &ImportRow,
    line: &str,
) -> Result<Vec<DerivedKindWrite>, String> {
    let parsed: Value =
        serde_json::from_str(line).map_err(|e| format!("line is not valid JSON: {e}"))?;
    let item_obj = parsed
        .get("Item")
        .and_then(Value::as_object)
        .ok_or_else(|| "line has no `Item` object".to_owned())?;
    let item = animus_dynamo::wire::decode_item(item_obj)
        .map_err(|e| format!("item does not decode as DynamoDB JSON: {e}"))?;

    let pk_name = row.base_schema.partition_key.as_str();
    let pk = item
        .get(pk_name)
        .cloned()
        .ok_or_else(|| format!("item is missing its own partition key attribute `{pk_name}`"))?;
    if !key_type_matches(&pk, pk_name, &row.key_types) {
        return Err(format!(
            "item's partition key attribute `{pk_name}` has the wrong declared type"
        ));
    }
    let sk = match row.base_schema.clustering_keys.first() {
        Some(sk_name) => {
            let sk = item
                .get(sk_name.as_str())
                .cloned()
                .ok_or_else(|| format!("item is missing its own sort key attribute `{sk_name}`"))?;
            if !key_type_matches(&sk, sk_name, &row.key_types) {
                return Err(format!(
                    "item's sort key attribute `{sk_name}` has the wrong declared type"
                ));
            }
            Some(sk)
        }
        None => None,
    };

    let base_key = crate::dynamo::item_key(&pk, sk.as_ref());
    let base_value = animus_dynamo::wire::encode_stored_item(&item);
    let (writes, _change_log) = crate::dynamo::kind_writes_for_item(
        meta,
        &row.target_table,
        &pk,
        sk.as_ref(),
        &base_key,
        base_value,
        None,
        Some(&item),
        false,
    );
    Ok(writes)
}

/// Whether `value` (a decoded key attribute) matches its own declared
/// `AttributeType` from `TableCreationParameters.AttributeDefinitions` — the
/// real DynamoDB key-schema validation this adapter can perform without a
/// second decode pass. A key attribute absent from `key_types` (an unusual
/// but not-rejected `AttributeDefinitions` omission) is accepted on this
/// axis alone (nothing declared to contradict), matching this adapter's own
/// "unknown, defaulted" fallback elsewhere (`animus_dynamo::wire::
/// attribute_definitions`'s own doc). A key attribute is always exactly one
/// of `S`/`N`/`B` in real DynamoDB — any other decoded shape is rejected
/// outright regardless of what's declared.
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

/// Propose `rows` as this import's own destination tablet's `SeedBatch` on a
/// **known-leader** local handle, confirming by applied index — the
/// identical shape [`crate::backup_restore::propose_local`] uses.
async fn propose_local(group: &CpGroup, rows: Vec<animus_cp_data::SeedRow>) -> bool {
    if rows.is_empty() {
        return true;
    }
    let rows_len = rows.len();
    let index = match group.propose_seed_batch(rows) {
        ProposeResult::Accepted { index, .. } => index,
        other => {
            tracing::debug!(?other, "import: seed batch not accepted");
            return false;
        }
    };
    tracing::debug!(
        index,
        rows_len,
        "import: seed batch accepted, awaiting confirm"
    );
    let deadline = tokio::time::Instant::now() + CONFIRM_TIMEOUT;
    while tokio::time::Instant::now() < deadline {
        if group.engine_applied_index() >= index {
            tracing::debug!(index, "import: seed batch confirmed");
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tracing::warn!(
        index,
        applied = group.engine_applied_index(),
        elapsed_ms = CONFIRM_TIMEOUT.as_millis() as u64,
        "import: seed batch confirm timed out, will re-propose next tick"
    );
    false
}

/// Propose `MetaCommand::CompleteImport`, relayed via
/// [`ClientCtx::propose_schema`] (on the `is_relayable_command` allowlist)
/// since this tablet's own leader need not be the control-plane leader.
/// Then (mirroring [`crate::backup_restore::complete_restore`]'s identical
/// GSI-declare-after-activation ordering) declares every one of this
/// import's resolved GSI definitions via `MetaCommand::CreateTableIndex` —
/// fire-and-forget, not commit-waited: the backfill seeder + completion
/// aggregator (ADR 0045, unmodified) converge each one to `Active` entirely
/// on their own.
async fn complete_import(
    ctx: &ClientCtx,
    import_id: &str,
    processed_item_count: u64,
    imported_item_count: u64,
    error_count: u64,
    processed_size_bytes: u64,
) {
    let completed_wall_ms = ctx.env.wall_now().0;
    let accepted = ctx
        .propose_schema(&MetaCommand::CompleteImport {
            import_id: import_id.to_owned(),
            processed_item_count,
            imported_item_count,
            error_count,
            processed_size_bytes,
            completed_wall_ms,
        })
        .await;
    tracing::debug!(
        import_id,
        accepted,
        "import: complete_import propose_schema result"
    );
    // Read fresh: this import's own row (for its target table + GSI plan)
    // may have been mirrored by a different node's own apply task by now.
    let meta = ctx.effective_metadata();
    let Some(row) = meta.import(import_id) else {
        return; // lost the race to observe our own just-proposed commit; the next external caller (DescribeImport) will see it fine
    };
    for def in &row.gsi_defs {
        let _ = ctx
            .propose_schema(&MetaCommand::CreateTableIndex {
                table: row.target_table.clone(),
                index: def.clone(),
            })
            .await;
    }
}

/// Propose `MetaCommand::FailImport`, then — **unlike**
/// [`crate::backup_restore::fail_restore`], which deliberately leaves a
/// failed restore's target table in place for a caller to `DeleteTable`
/// manually — drop the half-created target table through the ordinary
/// `ClientCtx::drop_table` path, matching real DynamoDB's own "a failed
/// `ImportTable` rolls back the table it was creating" contract (see
/// `ImportStatus::Failed`'s own doc). Best-effort: a drop failure here (no
/// control-plane leader reachable right now) is logged, not retried by this
/// call — the table is `Building`-only (never served) and state-agnostic
/// droppable, so a later admin/operator `DeleteTable` (or this same driver
/// naturally retrying on its own next relevant tick, since nothing here
/// prevents a stray future call) still cleans it up.
#[allow(clippy::too_many_arguments)] // the frozen counts, mirroring FailImport's own field list
async fn fail_import_and_cleanup(
    ctx: &ClientCtx,
    import_id: &str,
    row: &ImportRow,
    reason: &str,
    processed_item_count: u64,
    imported_item_count: u64,
    error_count: u64,
    processed_size_bytes: u64,
) {
    let completed_wall_ms = ctx.env.wall_now().0;
    let _ = ctx
        .propose_schema(&MetaCommand::FailImport {
            import_id: import_id.to_owned(),
            reason: reason.to_owned(),
            processed_item_count,
            imported_item_count,
            error_count,
            processed_size_bytes,
            completed_wall_ms,
        })
        .await;
    if let Err(err) = ctx.drop_table(row.target_table.clone()).await {
        tracing::warn!(
            import_id,
            table = row.target_table.as_str(),
            %err,
            "import: failed to drop the half-created target table after a failed import — a \
             later DeleteTable will still clean it up"
        );
    }
}
