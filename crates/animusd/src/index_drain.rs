//! The **GSI drain** (ADR 0041 §4): the background loop that materializes a
//! table's global secondary indexes from the change log its writes leave behind.
//!
//! A local secondary index is written atomically with its base row (it shares
//! the base partition key, so it shares the tablet). A **global** one cannot —
//! it hashes by its own key, so its rows live in a different table's tablets
//! entirely. Rather than pay a cross-tablet 2PC on every write, an indexed write
//! commits a **change-log record** alongside the base row, and this loop applies
//! the index effects afterwards. That is DynamoDB's own contract: a GSI is
//! eventually consistent, and a GSI being unavailable never fails a base write.
//!
//! ## Derivative, not delta-based
//!
//! A change record is treated purely as *a signal that a key is dirty*. The
//! drain never replays the record's images; it recomputes what the item's index
//! rows **should** be from the base row's *current* value, and compares that
//! against the **footprint** — the record of where that item's rows currently
//! are. Three hard failure modes disappear as a result:
//!
//! - **Idempotent**: a crash anywhere re-runs the whole reconciliation harmlessly.
//! - **Self-superseding**: several records for one item collapse into one
//!   reconciliation toward the current value, so there are never stale deltas to
//!   order against each other.
//! - **Orphan-free**: a stale row is, by construction, one the footprint names
//!   and the recomputation did not produce. There is no separate class of orphan
//!   needing its own sweeper.
//!
//! ## Consuming is trimming (for now)
//!
//! The drain deletes the records it has reconciled, in the same entry that
//! writes the updated footprint. That doubles as the log trim ADR 0041 requires
//! to bound growth. A separate cursor — letting records outlive the drain's own
//! consumption — is what DynamoDB Streams will need, and belongs with the
//! retention window in its own ADR; adding one now would be machinery with no
//! second reader to justify it.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_control::Metadata;
use animus_control::schema::{IndexDef, IndexKind};
use animus_cp_data::{KIND_CHANGE, KIND_FOOTPRINT};
use animus_dynamo::wire;
use animus_dynamo::{
    AttributeValue, FootprintEntry, IndexFootprint, Item, index as dynamo_index, index_table_name,
    is_index_table_name, storage_key,
};
use animus_tablet::partition_token;

use crate::{ClientCtx, CpGroup};

/// How often each node sweeps the tablet groups it leads for pending change
/// records. A plain fixed interval, matching `txn_resolver_loop`'s own shape —
/// this is background convergence work, not a latency-sensitive path.
const INDEX_DRAIN_INTERVAL: Duration = Duration::from_millis(200);

/// The **GSI drain background task** (ADR 0041 §4), one per node.
///
/// On every tick, for each tablet group this node currently **leads**, applies
/// any pending change records to that table's global secondary indexes. Errors
/// are logged and swallowed: this is best-effort convergence, and the next tick
/// retries from the same durable records (nothing is consumed until its effects
/// have landed).
pub(crate) async fn index_drain_loop(ctx: ClientCtx) {
    loop {
        tokio::time::sleep(INDEX_DRAIN_INTERVAL).await;
        let meta = ctx.effective_metadata();
        for (tablet, group) in ctx.edge.hosted_groups() {
            if !group.is_leader() {
                continue;
            }
            let Some(table) = meta.tablets.get(&tablet).and_then(|t| t.table.clone()) else {
                continue; // legacy whole-keyspace tablet, or a stale view
            };
            // A hidden index table holds index rows; it has no indexes of its
            // own, and must never recurse into maintaining any.
            if is_index_table_name(&table) {
                continue;
            }
            let gsis: Vec<IndexDef> = meta
                .table_indexes(&table)
                .iter()
                .filter(|i| i.kind == IndexKind::Global)
                .cloned()
                .collect();
            if gsis.is_empty() {
                continue;
            }
            if let Err(e) = drain_tablet(&ctx, &meta, &table, &group, &gsis).await {
                tracing::debug!(tablet = tablet.0, table, error = %e, "index drain: tick failed");
            }
        }
    }
}

/// Reconcile every dirty item of one tablet, then consume its change records.
async fn drain_tablet(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    group: &CpGroup,
    gsis: &[IndexDef],
) -> Result<(), String> {
    let records = group.pending_changes().await;
    if records.is_empty() {
        return Ok(());
    }
    // A record's key is `footprint_key || hlc`, so the partition it belongs to
    // is its key minus that fixed-width suffix — no parsing needed. Several
    // records for one partition collapse into a single reconciliation, which is
    // the point of being derivative.
    let mut by_partition: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    for (key, _) in &records {
        let Some(fp_key) = key.len().checked_sub(HLC_BYTES).map(|n| key[..n].to_vec()) else {
            continue; // malformed; leave it rather than mis-attribute it
        };
        by_partition.entry(fp_key).or_default().push(key.clone());
    }

    for (fp_key, consumed) in by_partition {
        reconcile_partition(ctx, meta, table, group, gsis, &fp_key, consumed).await?;
    }
    Ok(())
}

/// The packed HLC suffix every change-record key ends with (see
/// `KvCommand::KindBatch`'s `change_log`).
const HLC_BYTES: usize = 8;

/// Bring one partition's GSI rows in line with its base rows' *current* values,
/// then atomically record the new footprint and drop the records consumed.
async fn reconcile_partition(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    group: &CpGroup,
    gsis: &[IndexDef],
    fp_key: &[u8],
    consumed: Vec<Vec<u8>>,
) -> Result<(), String> {
    let base = crate::dynamo::schema_for(meta, table);
    let previous = group
        .local_get_kind(KIND_FOOTPRINT, fp_key)
        .await
        .and_then(|bytes| IndexFootprint::decode(&bytes))
        .unwrap_or_default();

    // Every live base row of this partition — the authority the recomputation
    // derives from. A partition's base rows are contiguous, so this is one
    // bounded range scan (a tombstoned row simply doesn't come back, and its
    // index rows therefore fall out as stale below).
    let mut end = fp_key.to_vec();
    *end.last_mut().expect("a footprint key is non-empty") += 1;
    let rows = group.local_scan_bounded(fp_key, &end).await;

    let mut desired = IndexFootprint::default();
    let mut writes: Vec<(String, Vec<u8>, Vec<u8>)> = Vec::new(); // (index table, key, value)
    for (base_key, value) in &rows {
        let Ok(Some(item)) = wire::decode_stored_item(value) else {
            continue; // a tombstone (or corrupt row) contributes no index rows
        };
        let base_sk = base_key[fp_key.len().min(base_key.len())..].to_vec();
        let Some(pk) = item.get(&base.partition_key) else {
            continue;
        };
        let sk = base.sort_key.as_ref().and_then(|n| item.get(n));

        let mut entries: Vec<FootprintEntry> = Vec::new();
        for idx in gsis {
            let Some(ihash) = item.get(&idx.hash_attribute) else {
                continue; // an item missing the index key simply isn't indexed
            };
            let isort = idx.sort_attribute.as_ref().and_then(|n| item.get(n));
            // A composite GSI whose sort attribute the item lacks is likewise
            // not indexed — DynamoDB's own rule, and it keeps the row's key
            // unambiguous (see `parse_gsi_row_key`'s `composite` flag).
            if idx.sort_attribute.is_some() && isort.is_none() {
                continue;
            }
            let key = gsi_row_key(ihash, isort, pk, sk);
            entries.push(FootprintEntry {
                index: idx.name.clone(),
                key: key.clone(),
            });
            writes.push((
                index_table_name(table, &idx.name),
                key,
                wire::encode_stored_item(&projected(&item, &base, idx)),
            ));
        }
        entries.sort();
        desired.set_item(base_sk, entries);
    }

    // Stale rows are exactly those the footprint names and the recomputation
    // did not produce — no orphan hunt, by construction.
    let live: BTreeSet<(&str, &[u8])> = desired
        .items
        .iter()
        .flat_map(|i| &i.entries)
        .map(|e| (e.index.as_str(), e.key.as_slice()))
        .collect();
    let stale: Vec<(String, Vec<u8>)> = previous
        .items
        .iter()
        .flat_map(|i| &i.entries)
        .filter(|e| !live.contains(&(e.index.as_str(), e.key.as_slice())))
        .map(|e| (index_table_name(table, &e.index), e.key.clone()))
        .collect();

    // Order matters for a crash: write the new rows first, then remove the
    // stale ones, and only then record the new footprint. A crash mid-way
    // leaves the footprint still naming the old rows, so the next tick redoes
    // the whole reconciliation — over-covering, never under-covering.
    for (index_table, key, value) in writes {
        ctx.cp_write(&index_table, key, value).await?;
    }
    for (index_table, key) in stale {
        ctx.cp_write(&index_table, key, wire::encode_tombstone())
            .await?;
    }

    // One entry: the new footprint plus the records it accounts for. Consuming
    // the records *is* the log trim (see the module doc).
    let mut batch: Vec<(u8, Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    batch.push((
        KIND_FOOTPRINT,
        fp_key.to_vec(),
        (!desired.is_empty()).then(|| desired.encode()),
    ));
    for key in consumed {
        batch.push((KIND_CHANGE, key, None));
    }
    ctx.cp_kind_write_raw(table, batch).await
}

/// The full data-plane key of one GSI row: the ADR 0022 token over the **index
/// hash value** (not the base partition key — that difference is exactly why a
/// GSI lives in its own tablets and is maintained here rather than inline).
fn gsi_row_key(
    ihash: &AttributeValue,
    isort: Option<&AttributeValue>,
    base_pk: &AttributeValue,
    base_sk: Option<&AttributeValue>,
) -> Vec<u8> {
    let mut key = partition_token(&storage_key(ihash, None)).to_vec();
    key.extend_from_slice(&dynamo_index::gsi_row_key(ihash, isort, base_pk, base_sk));
    key
}

/// The attributes a GSI row carries, per its declared projection.
fn projected(item: &Item, base: &animus_dynamo::TableSchema, idx: &IndexDef) -> Item {
    crate::dynamo::projected_item(item, base, idx)
}
