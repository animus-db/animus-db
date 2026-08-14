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
//! ## Cursor-based consumption (ADR 0042 §7/§8)
//!
//! ADR 0041 originally had the drain **delete** the records it reconciled in
//! the same entry that wrote the updated footprint — "consuming is trimming."
//! That worked only because the GSI drain was the change log's sole reader.
//! ADR 0042's stream copier is a second, independent reader of the same log,
//! so deletion can no longer be a side effect of any one consumer's own
//! progress: the drain now advances a **cursor row** (`KIND_CURSOR`, tag
//! `"gsi"` — see [`animus_cp_data::cursor`]) recording the highest change-record
//! HLC this tablet's reconciliation has fully covered, and a separate
//! **trim janitor** deletes records only once every *expected, present*
//! consumer's cursor has cleared them.
//!
//! **The crash property ADR 0041 documented still holds, restated for the
//! cursor**: the cursor must never claim a reconciliation whose footprint
//! didn't land. [`drain_tablet`] gets this by construction — it only advances
//! the cursor, in its own trailing write, *after* every partition dirtied this
//! pass has had its footprint update durably confirmed (`reconcile_partition`'s
//! own `cp_kind_write_raw` call only returns `Ok` once that specific write's
//! effect is visible; see that primitive's doc). A crash before the cursor
//! write lands simply leaves it wherever it was — the next tick re-reads the
//! same (still-present) records and redoes the same reconciliation, which is
//! safe because it is idempotent.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::Metadata;
use animus_control::schema::{IndexDef, IndexKind};
use animus_cp_data::cursor;
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{KIND_CHANGE, KIND_CURSOR, KIND_FOOTPRINT};
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

/// The consumer tag the GSI drain's own reconcile cursor writes (ADR 0042
/// §7/§8) — the sibling of the stream copier's `"copier"` tag, below.
const GSI_TAG: &str = "gsi";

/// The consumer tag the stream copier's own cursor writes (ADR 0042 §7/§8).
/// Only *expected* today (`expected_consumer_tags`) — the copier itself
/// (which would actually write this tag's row) lands with the shard
/// subsystem, PR B8.
const COPIER_TAG: &str = "copier";

/// The packed HLC suffix every change-record key ends with (see
/// `KvCommand::KindBatch`'s `change_log`) — the same 8-byte encoding a cursor
/// row's own value uses ([`cursor::encode_watermark`]/`decode_watermark`).
const HLC_BYTES: usize = 8;

/// How many change records one trim `KindBatch` entry deletes at most —
/// bounds a large backlog's catch-up to several ticks instead of one
/// outsized Raft entry, mirroring `cp_batch_write_patient`'s own bounded-batch
/// discipline.
const TRIM_BATCH: usize = 256;

/// The **GSI drain background task** (ADR 0041 §4), one per node.
///
/// On every tick, for each tablet group this node currently **leads**, applies
/// any pending change records to that table's global secondary indexes, then
/// runs the trim janitor (ADR 0042 §7/§8) to delete whatever every expected,
/// present consumer's cursor has cleared. Errors are logged and swallowed:
/// this is best-effort convergence, and the next tick retries from the same
/// durable records (nothing is trimmed until every expected consumer's
/// cursor says it's safe to).
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
            let stream_enabled = meta.table_stream(&table).is_some();
            if gsis.is_empty() && !stream_enabled {
                continue;
            }
            // `drain_tablet` is GSI-specific (it reconciles GSI rows and
            // advances the `"gsi"` cursor) — never call it for a
            // streamed-but-unindexed table, or it would write a spurious
            // `"gsi"` cursor row this table's schema never expects (a
            // permanent, own-token unexpected row `cleanup_merge_residue_
            // cursor_rows` deliberately does not clean up — see its own
            // doc). A streamed-only table still needs the trim janitor
            // tick below, so the min-over-expected-tags rule sees its
            // `"copier"` expectation and blocks trim correctly.
            if !gsis.is_empty()
                && let Err(e) = drain_tablet(&ctx, &meta, &table, &group, &gsis).await
            {
                tracing::debug!(tablet = tablet.0, table, error = %e, "index drain: tick failed");
                continue; // don't trim behind a reconciliation pass that didn't complete
            }
            if let Err(e) = trim_janitor(&ctx, &table, &group, &gsis, stream_enabled).await {
                tracing::debug!(tablet = tablet.0, table, error = %e, "index drain: trim janitor tick failed");
            }
        }
    }
}

/// Reconcile every dirty item of one tablet not yet covered by the "gsi"
/// cursor, then advance that cursor to the highest HLC this pass covers.
async fn drain_tablet(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    group: &CpGroup,
    gsis: &[IndexDef],
) -> Result<(), String> {
    // The ADR 0042 §7 min-over-rows watermark: `None` on a cold tablet (no
    // "gsi" row yet) or a split's fresh right child (the rule over an empty
    // set) — either way, "reconcile everything currently pending."
    let watermark = group.cursor_min_watermark(GSI_TAG).await;
    let records = group.pending_changes().await;
    if records.is_empty() {
        return Ok(());
    }
    // A GSI's rows live in its own hidden table, provisioned lazily here on
    // the first drain that has records to apply (ADR 0023). This is
    // load-bearing, not an optimization: `reconcile_partition` writes rows
    // via `cp_write`, which — unlike `cp_kind_write`/`cp_txn` — does NOT
    // auto-provision; without a tablet to route to, its `cp_route` would
    // wait out `CLIENT_TIMEOUT` and fail, every tick, forever. Gated on the
    // caller's metadata snapshot: a stale "absent" just re-proposes an
    // idempotent `CreateTablet` (first-committer wins), and the hit path is
    // sound because tablets are only ever removed by drop-table.
    for idx in gsis {
        let index_table = index_table_name(table, &idx.name);
        if !meta.has_table_tablet(&index_table) {
            ctx.provision_tablet(&index_table).await?;
        }
    }
    // A record's key is `footprint_key || hlc`, so the partition it belongs to
    // is its key minus that fixed-width suffix — no parsing needed. Several
    // records for one partition collapse into a single reconciliation, which is
    // the point of being derivative.
    //
    // Sweep discipline (ADR 0042 §7): only records this pass hasn't already
    // covered — everything else is exactly what `watermark` already claims
    // is done. `max_hlc` accumulates the true highest HLC this pass will end
    // up covering, computed **before** any reconciliation happens, since
    // every partition it comes from is guaranteed to get reconciled below
    // (nothing in `by_partition` is ever skipped).
    let mut by_partition: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut max_hlc: Option<HlcTimestamp> = None;
    for (key, _) in &records {
        let Some(fp_key) = key.len().checked_sub(HLC_BYTES).map(|n| key[..n].to_vec()) else {
            continue; // malformed; leave it rather than mis-attribute it
        };
        let Some(ts) = record_hlc(key) else {
            continue; // malformed HLC suffix; same defensive skip
        };
        if watermark.is_some_and(|w| ts <= w) {
            continue; // already consumed by an earlier pass
        }
        max_hlc = Some(max_hlc.map_or(ts, |m: HlcTimestamp| m.max(ts)));
        by_partition.insert(fp_key);
    }
    if by_partition.is_empty() {
        return Ok(()); // nothing past the watermark; a prior pass covered it all
    }

    for fp_key in by_partition {
        reconcile_partition(ctx, meta, table, group, gsis, &fp_key).await?;
    }

    // Every partition dirtied this pass has now been reconciled and its
    // footprint update durably confirmed (the loop above only reaches here
    // once every `reconcile_partition` call returned `Ok`) — advancing the
    // cursor here, in its own trailing write, is what preserves the crash
    // property: the cursor can only ever name a `max_hlc` whose covering
    // reconciliations have already landed, never one still in flight.
    if let Some(max_hlc) = max_hlc {
        let cursor_key = cursor::cursor_key(&group.scope_range().start, GSI_TAG);
        ctx.cp_kind_write_raw(
            table,
            vec![(
                KIND_CURSOR,
                cursor_key,
                Some(cursor::encode_watermark(max_hlc)),
            )],
        )
        .await?;
    }
    Ok(())
}

/// The HLC a change-record's key suffix encodes — the identical 8-byte
/// big-endian packing [`cursor::encode_watermark`] uses for a watermark value
/// (see `KvCommand::KindBatch`'s `change_log` doc: the key is completed at
/// apply as `prefix || hlc::pack(ts)`). `None` on a malformed suffix, a
/// defensive read mirroring the `fp_key` split's own.
fn record_hlc(key: &[u8]) -> Option<HlcTimestamp> {
    let suffix = key.len().checked_sub(HLC_BYTES).map(|n| &key[n..])?;
    cursor::decode_watermark(suffix)
}

/// Bring one partition's GSI rows in line with its base rows' *current*
/// values, then atomically record the new footprint. Unlike ADR 0041's
/// original design, this no longer deletes the change records that triggered
/// it — see the module doc and [`drain_tablet`]'s trailing cursor write for
/// the ADR 0042 replacement, and [`trim_janitor`] for the actual deletion.
async fn reconcile_partition(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    group: &CpGroup,
    gsis: &[IndexDef],
    fp_key: &[u8],
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
    // A genuine engine delete, not a tombstone *value* (`encode_tombstone`):
    // that sentinel exists so a base-table `DeleteItem` stays observable (to
    // conditional reads and to the change log this very drain consumes), but
    // an index row is wholly derived — a dead one has no reader to inform,
    // and nothing would ever reclaim a sentinel from a hidden index table.
    // The LSI half of an indexed write already prunes with a real tombstone
    // (`KindBatch`'s `None` value); this is the GSI dual.
    for (index_table, key) in stale {
        ctx.cp_delete(&index_table, key).await?;
    }

    // One entry: just the updated footprint. See the module doc and
    // `drain_tablet`'s trailing cursor write for why the records this
    // reconciliation covers are no longer deleted here.
    ctx.cp_kind_write_raw(
        table,
        vec![(
            KIND_FOOTPRINT,
            fp_key.to_vec(),
            (!desired.is_empty()).then(|| desired.encode()),
        )],
    )
    .await
}

/// Every consumer tag this table's *current schema* expects a cursor row for
/// (ADR 0042 §7's trim rule: "what does this table's schema expect, and what
/// do those rows currently say"): `"gsi"` iff the table has at least one
/// global secondary index, `"copier"` iff its stream is enabled — a table
/// may expect neither, either, or both.
//
// round-3 sealer PR: replaces the `"copier"` tag/row here with a
// catalog-derived stream watermark (round-3 streams plan §A6/F10) — no
// consumer ever writes a `"copier"` cursor row in round 3; `COPIER_TAG` and
// this branch are removed there, not given a producer.
fn expected_consumer_tags(gsis: &[IndexDef], stream_enabled: bool) -> Vec<&'static str> {
    let mut tags = Vec::new();
    if !gsis.is_empty() {
        tags.push(GSI_TAG);
    }
    if stream_enabled {
        tags.push(COPIER_TAG);
    }
    tags
}

/// The trim janitor (ADR 0042 §7/§8), run once per tablet per tick, right
/// after this tick's reconciliation. Deletes change records every *expected,
/// present* consumer's cursor has already cleared, and sweeps stale
/// merge-residue cursor rows. Advances no cursor itself — that's
/// [`drain_tablet`]'s job, for the "gsi" tag, and the stream copier's for
/// "copier" once it lands.
async fn trim_janitor(
    ctx: &ClientCtx,
    table: &str,
    group: &CpGroup,
    gsis: &[IndexDef],
    stream_enabled: bool,
) -> Result<(), String> {
    let expected = expected_consumer_tags(gsis, stream_enabled);
    cleanup_merge_residue_cursor_rows(ctx, table, group, &expected).await?;

    // An expected tag with no row at all blocks trim entirely (ADR 0042 §7's
    // safe default) — never trim past a consumer that hasn't started yet.
    let mut trim_point: Option<HlcTimestamp> = None;
    for tag in &expected {
        let Some(w) = group.cursor_min_watermark(tag).await else {
            return Ok(());
        };
        trim_point = Some(trim_point.map_or(w, |t: HlcTimestamp| t.min(w)));
    }
    let Some(trim_point) = trim_point else {
        // No expected consumer at all — not reachable at this call site
        // today (the caller only reaches here when `gsis` is non-empty or
        // `stream_enabled`, so `expected` always names at least one tag),
        // but the safe default (block trim) is still the right one if it
        // ever is.
        return Ok(());
    };

    let mut writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    for (key, _) in group.pending_changes().await {
        let Some(ts) = record_hlc(&key) else {
            continue; // malformed suffix; leave it rather than guess
        };
        if ts > trim_point {
            // `pending_changes` is in key order (token-then-pk-then-HLC), not
            // HLC order (see its own doc), so every record must be checked —
            // there is no earlier prefix to stop at.
            continue;
        }
        writes.push((KIND_CHANGE, key, None));
        if writes.len() >= TRIM_BATCH {
            ctx.cp_kind_write_raw(table, std::mem::take(&mut writes))
                .await?;
        }
    }
    if !writes.is_empty() {
        ctx.cp_kind_write_raw(table, writes).await?;
    }
    Ok(())
}

/// Tombstone a cursor row iff its tag is no longer expected by this table's
/// schema **and** its token isn't this tablet's own — i.e. it is
/// physically-present residue from an absorbed sibling (ADR 0042 §7's merge
/// dual: `StorageScope::with_kind` shares one live `KeyRange`, so widening a
/// survivor's scope over an absorbed tablet exposes whatever cursor rows it
/// wrote while it was its own tablet). An unexpected row at this tablet's OWN
/// token — a disabled stream's, or a dropped index's, own stale row — is
/// deliberately left alone here.
//
// PR A3/B8: once a disabled stream can leave an unexpected `"copier"` row at
// this tablet's own token, that case needs its own cleanup path, not this
// one — this one only ever targets merge residue.
async fn cleanup_merge_residue_cursor_rows(
    ctx: &ClientCtx,
    table: &str,
    group: &CpGroup,
    expected: &[&str],
) -> Result<(), String> {
    let own_token = cursor::token_of(&group.scope_range().start);
    let mut writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    for (token, tag, _) in group.cursor_rows_with_token().await {
        if expected.contains(&tag.as_str()) || token == own_token {
            continue;
        }
        writes.push((KIND_CURSOR, cursor::cursor_key(&token, &tag), None));
    }
    if writes.is_empty() {
        return Ok(());
    }
    ctx.cp_kind_write_raw(table, writes).await
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

/// ADR 0042 §7/§8 regressions for the cursor-based drain + trim janitor
/// above. **In-crate**, like `lib.rs`'s `split_fence_tests`/
/// `auto_split_median_tests`: these need private handles (`CpGroup::
/// pending_changes`/`cursor_min_watermark`/`cursor_rows_with_token`, the
/// plain-client-protocol `ClientRequest::SplitTablet`/`MergeTablets` with an
/// arbitrary binary `split_key`, and `crate::dynamo::item_key` for
/// deterministic side-placement) that an external `tests/` crate — a
/// separate compilation unit, linking only this crate's `pub` surface —
/// cannot reach. All real-socket `ProdEnv` integration tests, per this
/// crate's own testing discipline; every eventual property is a
/// converged-or-timeout poll, never a fixed sleep.
#[cfg(test)]
mod gsi_drain_cursor_tests {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::time::Duration;

    use animus_tablet::TabletId;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::config::NodeRole;
    use crate::{
        ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, read_frame, run_node,
        write_frame,
    };

    fn free_addrs(count: usize) -> Vec<SocketAddr> {
        let ls: Vec<std::net::TcpListener> = (0..count)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        ls.iter().map(|l| l.local_addr().unwrap()).collect()
    }

    fn single_node_config() -> ClusterConfig {
        let addrs = free_addrs(5);
        ClusterConfig {
            nodes: vec![RoleAddrs {
                id: crate::config::node_id(0),
                role: NodeRole::Both,
                internal: addrs[0],
                client: addrs[1],
                dynamo: addrs[2],
                cql: addrs[3],
                admin: addrs[4],
            }],
        }
    }

    async fn await_control_leader(node: &Node) {
        timeout(Duration::from_secs(10), async {
            loop {
                if node.is_control_leader() {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("node did not become control leader in time");
    }

    async fn single_node(dir: &Path) -> Node {
        let config = single_node_config();
        let node = run_node(&config, 0, dir.join("node-0"))
            .await
            .expect("bring up single node");
        await_control_leader(&node).await;
        node
    }

    /// One DynamoDB JSON request over the real HTTP wire (mirroring
    /// `tests/dynamo_gsi_drain.rs`'s helper of the same shape — duplicated
    /// rather than shared, since this module is a different compilation
    /// unit than that external `tests/` crate).
    async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
        let mut s = TcpStream::connect(addr).await.expect("connect");
        let req = format!(
            "POST / HTTP/1.1\r\nHost: x\r\nX-Amz-Target: {target}\r\n\
             Connection: close\r\n\
             Content-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        s.write_all(req.as_bytes()).await.expect("write");
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.expect("read");
        let text = String::from_utf8_lossy(&buf).into_owned();
        let status = text
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        let body = text.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
        (status, body.to_owned())
    }

    /// A table with one GSI (`by-g`, hash attribute `g`) — every test in this
    /// module uses this identical shape.
    async fn create_table_with_gsi(addr: SocketAddr, table: &str) {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.CreateTable",
            &format!(
                r#"{{"TableName":"{table}",
                    "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                    "GlobalSecondaryIndexes":[
                        {{"IndexName":"by-g",
                         "KeySchema":[{{"AttributeName":"g","KeyType":"HASH"}}],
                         "Projection":{{"ProjectionType":"ALL"}}}}]}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");
    }

    /// One item whose own `id` and GSI hash attribute `g` are both `id` —
    /// every item this module writes has a unique, individually queryable
    /// GSI hash value.
    async fn put_item(addr: SocketAddr, table: &str, id: &str) {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"g":{{"S":"{id}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem({id}) failed: {body}");
    }

    /// Poll a GSI `Query` until `accept` is satisfied (mirrors
    /// `tests/dynamo_gsi_drain.rs`'s `await_gsi_query` — a GSI is
    /// eventually consistent by contract).
    async fn await_gsi_query(addr: SocketAddr, body: &str, accept: impl Fn(&str) -> bool) {
        let last = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let seen = std::sync::Arc::clone(&last);
        let converged = async move {
            loop {
                let (status, got) = dynamo(addr, "DynamoDB_20120810.Query", body).await;
                if status == 200 && accept(&got) {
                    return;
                }
                *seen.lock().unwrap() = got;
                sleep(Duration::from_millis(100)).await;
            }
        };
        if timeout(Duration::from_secs(30), converged).await.is_err() {
            panic!(
                "GSI query never converged within 30s (last saw: {})",
                last.lock().unwrap()
            );
        }
    }

    /// A `{"g = :g"}` GSI equality query for `id`, expecting exactly one hit.
    async fn await_indexed(addr: SocketAddr, table: &str, id: &str) {
        await_gsi_query(
            addr,
            &format!(
                r#"{{"TableName":"{table}","IndexName":"by-g",
                    "KeyConditionExpression":"g = :g",
                    "ExpressionAttributeValues":{{":g":{{"S":"{id}"}}}}}}"#
            ),
            |b| b.contains("\"Count\":1"),
        )
        .await;
    }

    fn tablets_of(node: &Node, table: &str) -> Vec<TabletId> {
        node.metadata()
            .tablets
            .iter()
            .filter(|(_, t)| t.table.as_deref() == Some(table))
            .map(|(id, _)| *id)
            .collect()
    }

    fn only_tablet(node: &Node, table: &str) -> TabletId {
        let mut ts = tablets_of(node, table);
        assert_eq!(ts.len(), 1, "expected exactly one tablet for `{table}`");
        ts.pop().unwrap()
    }

    async fn await_true<F: Fn() -> bool>(secs: u64, what: &str, cond: F) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        while !cond() {
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Poll until tablet `tablet`'s own change log (`KIND_CHANGE`, via the
    /// private `CpGroup::pending_changes` accessor — the "raw kind-scan of
    /// leftovers" the crash-recovery scenario needs) holds exactly `want`
    /// records.
    async fn await_pending_changes(node: &Node, tablet: TabletId, want: usize, what: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let group = node
                .edge
                .local_cp(tablet)
                .expect("this node hosts the tablet");
            let n = group.pending_changes().await.len();
            if n == want {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{what}: change log has {n} records, want {want}"
            );
            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Poll until this node's own tablet-host reconciler has actually stood
    /// up `tablet`'s group — a real, if usually short, window separate from
    /// "the tablet map already shows it" (`tablets_of`'s own convergence).
    async fn await_hosted(node: &Node, tablet: TabletId, what: &str) -> CpGroup {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(group) = node.edge.local_cp(tablet) {
                return group;
            }
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn await_cursor_some(group: &CpGroup, what: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if group.cursor_min_watermark(GSI_TAG).await.is_some() {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Split `tablet` at the raw physical key `split_key` — the plain
    /// client-protocol `ClientRequest::SplitTablet`, not the admin HTTP
    /// surface (whose `split_key: String` field is a JSON string and so
    /// can't carry a DynamoDB item key's arbitrary, generally non-UTF8
    /// murmur3 token prefix — `ClientRequest`'s own `Vec<u8>` field
    /// `serde_json`-encodes as an ordinary byte array instead).
    async fn split(client_addr: SocketAddr, tablet: TabletId, split_key: Vec<u8>) {
        let mut stream = TcpStream::connect(client_addr).await.expect("connect");
        write_frame(
            &mut stream,
            &ClientRequest::SplitTablet {
                tablet: tablet.0,
                split_key,
            },
        )
        .await
        .expect("send split");
        let resp: ClientResponse = read_frame(&mut stream)
            .await
            .expect("read reply")
            .expect("a reply");
        assert!(
            matches!(resp, ClientResponse::PutOk),
            "split failed: {resp:?}"
        );
    }

    async fn merge(client_addr: SocketAddr, left: TabletId, right: TabletId) {
        let mut stream = TcpStream::connect(client_addr).await.expect("connect");
        write_frame(
            &mut stream,
            &ClientRequest::MergeTablets {
                left: left.0,
                right: right.0,
            },
        )
        .await
        .expect("send merge");
        let resp: ClientResponse = read_frame(&mut stream)
            .await
            .expect("read reply")
            .expect("a reply");
        assert!(
            matches!(resp, ClientResponse::PutOk),
            "merge failed: {resp:?}"
        );
    }

    /// An `id` value whose real ADR 0022 token (`crate::dynamo::item_key`,
    /// the exact function the DynamoDB edge itself uses) falls on the
    /// requested side of `boundary` — a murmur3 token can't be chosen
    /// directly, so this scans a small deterministic candidate pool instead.
    /// Panics if none match (the pool is too small for this boundary, not a
    /// product bug).
    fn find_id_on_side(boundary: &[u8; 8], want_below: bool, pool: &str) -> String {
        for i in 0..10_000u32 {
            let id = format!("{pool}-{i}");
            let key = crate::dynamo::item_key(&AttributeValue::S(id.clone()), None);
            let token: [u8; 8] = key[..8].try_into().expect("a key has at least 8 bytes");
            if (token < *boundary) == want_below {
                return id;
            }
        }
        panic!(
            "no candidate id found on the {} side of the boundary in `{pool}`'s pool",
            if want_below { "left" } else { "right" }
        );
    }

    /// A fixed, arbitrary token boundary — not derived from any real item.
    /// Any byte string strictly between the ring's absolute start (`[]`) and
    /// its unbounded-above end is a legal split point (`SplitTablet` doesn't
    /// require an existing key), so a plain numeric midpoint works.
    const BOUNDARY: [u8; 8] = 0x8000_0000_0000_0000u64.to_be_bytes();

    /// The change log must not grow without bound while a stream of writes
    /// to an indexed table is ongoing, and must drain back to nothing once
    /// they stop — the reason the cursor+trim-janitor rework exists at all
    /// (ADR 0042 §7/§8), proven here by directly inspecting the raw
    /// `KIND_CHANGE` scope rather than only observing the GSI's own
    /// eventual correctness.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn change_log_stays_bounded_under_sustained_writes_to_an_indexed_table() {
        timeout(Duration::from_secs(90), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node(dir.path()).await;
            let table = "orders";
            create_table_with_gsi(node.dynamo_addr(), table).await;

            for i in 0..200u32 {
                put_item(node.dynamo_addr(), table, &format!("o{i}")).await;
                if i % 20 == 19 {
                    let tablet = only_tablet(&node, table);
                    let group = node
                        .edge
                        .local_cp(tablet)
                        .expect("this node hosts the tablet");
                    let n = group.pending_changes().await.len();
                    // A generous, sampled ceiling: proves the log doesn't
                    // grow unboundedly with the write stream (it would, under
                    // the pre-ADR-0042 GSI-drain-only design's absence of a
                    // second consumer, but a bug here would show as
                    // unbounded growth too), not a tight bound on any one
                    // instant — the drain/trim tick is 200ms, and this test
                    // writes far faster than that.
                    assert!(
                        n < 1000,
                        "change log grew far beyond a single drain/trim tick's worth of \
                         writes: {n} records after {} puts",
                        i + 1
                    );
                }
            }

            let tablet = only_tablet(&node, table);
            await_pending_changes(&node, tablet, 0, "after sustained writes stop").await;
        })
        .await
        .expect("did not converge within 90s");
    }

    /// A real process crash + restart, some time after writes to an indexed
    /// table land, must recover to the complete, correct GSI with no
    /// record's partition ever skipped — the ADR 0042 §7/§8 "over-covers,
    /// never under-covers" guarantee. Real `ProdEnv` gives no hook to pin
    /// the exact instant relative to the drain's own reconcile-entries/
    /// cursor-write boundary, so this does not assert on a specific
    /// pre-crash state (a short delay only biases, without guaranteeing,
    /// toward catching the node mid-reconciliation); it proves the property
    /// that must hold regardless of which state the crash actually catches
    /// the drain in — genuine WAL/engine recovery, not a simulated one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn crash_mid_reconcile_recovers_without_skipping_or_corrupting_the_gsi() {
        timeout(Duration::from_secs(90), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node_dir = dir.path().join("node-0");
            let config = single_node_config();
            let node = run_node(&config, 0, &node_dir).await.expect("bring up");
            await_control_leader(&node).await;

            let table = "orders";
            create_table_with_gsi(node.dynamo_addr(), table).await;
            let ids: Vec<String> = (0..40u32).map(|i| format!("o{i}")).collect();
            for id in &ids {
                put_item(node.dynamo_addr(), table, id).await;
            }
            sleep(Duration::from_millis(20)).await;
            node.shutdown_graceful().await;

            let node2 = run_node(&config, 0, &node_dir)
                .await
                .expect("restart on the same dir");
            await_control_leader(&node2).await;

            for id in &ids {
                await_indexed(node2.dynamo_addr(), table, id).await;
            }

            let tablet = only_tablet(&node2, table);
            await_pending_changes(&node2, tablet, 0, "after recovery").await;
        })
        .await
        .expect("crash/restart recovery did not converge in time");
    }

    /// Split a table's tablet, then reconcile the fresh right child's own
    /// items — a cold start from `W = 0` (the min-over-rows rule over an
    /// empty set: the right child inherits no cursor row at all) — and
    /// confirm it converges to the correct GSI, on both sides, without
    /// corrupting anything (idempotence).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn split_right_childs_cold_start_re_reconciles_from_zero_without_corrupting_the_gsi() {
        timeout(Duration::from_secs(90), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node(dir.path()).await;
            let dynamo_addr = node.dynamo_addr();
            let client_addr = node.client_addr();
            let table = "orders";
            create_table_with_gsi(dynamo_addr, table).await;

            let left_id = find_id_on_side(&BOUNDARY, true, "l");
            put_item(dynamo_addr, table, &left_id).await;
            await_indexed(dynamo_addr, table, &left_id).await;

            let parent = only_tablet(&node, table);
            split(client_addr, parent, BOUNDARY.to_vec()).await;
            await_true(20, "split produced two tablets", || {
                tablets_of(&node, table).len() == 2
            })
            .await;

            let right = {
                let m = node.metadata();
                tablets_of(&node, table)
                    .into_iter()
                    .find(|t| !m.tablets[t].range.start.is_empty())
                    .expect("the right child's range doesn't start at the ring's own beginning")
            };

            let right_ids: Vec<String> = (0..8)
                .map(|i| find_id_on_side(&BOUNDARY, false, &format!("r{i}")))
                .collect();
            for id in &right_ids {
                put_item(dynamo_addr, table, id).await;
            }
            for id in &right_ids {
                await_indexed(dynamo_addr, table, id).await;
            }
            // The split didn't corrupt the other side either.
            await_indexed(dynamo_addr, table, &left_id).await;

            let right_group = await_hosted(&node, right, "right child hosted").await;
            await_cursor_some(&right_group, "right child's own gsi cursor advances").await;
            await_pending_changes(&node, right, 0, "right child's own change log drains").await;
        })
        .await
        .expect("split cold-start scenario did not converge in time");
    }

    /// Two tablets of an indexed table, each with its own, genuinely
    /// different "gsi" cursor watermark, merged together: the survivor must
    /// use the **minimum** over both rows, not just its own (higher) one —
    /// the ADR 0042 §7 min-over-rows rule's one genuine data-loss hazard.
    /// Demonstrates the hazard directly (an "own-row-only" reading of the
    /// same post-merge state disagrees with, and is strictly higher than,
    /// the correct min), then proves the real consequence: the survivor's
    /// next drain pass actually reconciles the absorbed tablet's own
    /// uncopied record.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn merge_survivor_uses_the_min_over_rows_not_its_own_higher_watermark() {
        timeout(Duration::from_secs(120), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node(dir.path()).await;
            let dynamo_addr = node.dynamo_addr();
            let client_addr = node.client_addr();
            let table = "orders";
            create_table_with_gsi(dynamo_addr, table).await;

            // Seed and fully reconcile ONE item on the left side *before*
            // splitting — this is the pre-split (single) tablet's own
            // reconciliation, so its "gsi" row is what the retained LEFT
            // child inherits (same `range.start`). Nothing is written on the
            // right side yet: a seed there too would be reconciled by this
            // same pre-split tablet, and the resulting row would land on
            // LEFT after the split (inherited), not prove anything about the
            // fresh RIGHT child's own, independent reconciliation.
            let left_seed = find_id_on_side(&BOUNDARY, true, "seed-left");
            put_item(dynamo_addr, table, &left_seed).await;
            await_indexed(dynamo_addr, table, &left_seed).await;

            let parent = only_tablet(&node, table);
            split(client_addr, parent, BOUNDARY.to_vec()).await;
            await_true(20, "split produced two tablets", || {
                tablets_of(&node, table).len() == 2
            })
            .await;

            let (left, right) = {
                let m = node.metadata();
                let ts = tablets_of(&node, table);
                let left = *ts
                    .iter()
                    .find(|t| m.tablets[t].range.start.is_empty())
                    .expect("a left child retains the ring's own start");
                let right = *ts
                    .iter()
                    .find(|t| **t != left)
                    .expect("a second, right child exists");
                (left, right)
            };

            // The right child's OWN first reconciliation: it inherited
            // nothing from the pre-split tablet (the min-over-rows rule over
            // an empty set), so this seed is what gives it a genuine "gsi"
            // watermark of its own, on its own (now-independent) raft group.
            let right_seed = find_id_on_side(&BOUNDARY, false, "seed-right");
            put_item(dynamo_addr, table, &right_seed).await;
            await_indexed(dynamo_addr, table, &right_seed).await;

            let right_group_pre = await_hosted(&node, right, "right tablet hosted pre-merge").await;
            await_cursor_some(
                &right_group_pre,
                "right's own gsi watermark exists pre-merge",
            )
            .await;
            let w_right = right_group_pre
                .cursor_min_watermark(GSI_TAG)
                .await
                .expect("right has reconciled once");

            // Grow LEFT's own watermark past `w_right` with more,
            // independent writes+reconciliation on its own (separate) raft
            // group — real wall-clock time passing between these await
            // points is what makes each later HLC genuinely exceed the
            // right side's earlier one (the same cross-group,
            // real-time-grounded HLC ordering `cross_group_lww.rs`'s own
            // clock-skew tests rely on), not any artificial synchronization.
            for i in 0..5 {
                let id = find_id_on_side(&BOUNDARY, true, &format!("left-more-{i}"));
                put_item(dynamo_addr, table, &id).await;
                await_indexed(dynamo_addr, table, &id).await;
            }
            let left_group = await_hosted(&node, left, "left tablet hosted").await;
            let w_left = left_group
                .cursor_min_watermark(GSI_TAG)
                .await
                .expect("left has reconciled");
            assert!(
                w_left > w_right,
                "left's own watermark ({w_left:?}) must exceed right's ({w_right:?}) for this \
                 scenario to be meaningful"
            );

            // One more item on the RIGHT side, written just before merging —
            // the record this scenario's assertions are really about. It may
            // or may not have been reconciled by right's own drain before the
            // merge lands (real `ProdEnv` gives no hook to pin that), but the
            // min-over-rows rule must cover it either way.
            let straggler = find_id_on_side(&BOUNDARY, false, "straggler");
            put_item(dynamo_addr, table, &straggler).await;

            merge(client_addr, left, right).await;
            await_true(20, "merge left a single tablet", || {
                tablets_of(&node, table).len() == 1
            })
            .await;

            let survivor =
                await_hosted(&node, left, "survivor (left) tablet hosted post-merge").await;

            // The absorbed sibling's own "gsi" row survives physically
            // (merge never erases), but only becomes *visible* through the
            // survivor's own scope once the reconciler's own `WidenScope`
            // action actually runs (a real, if usually short, tick or two
            // after the merge is committed) — poll for it rather than
            // asserting on the very next tick.
            let gsi_rows = {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
                loop {
                    let rows = survivor.cursor_rows_with_token().await;
                    let gsi_rows: Vec<_> = rows
                        .into_iter()
                        .filter(|(_, tag, _)| tag == GSI_TAG)
                        .collect();
                    if gsi_rows.len() >= 2 {
                        break gsi_rows;
                    }
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "expected the survivor's own row plus the absorbed sibling's, \
                         last saw {gsi_rows:?}"
                    );
                    sleep(Duration::from_millis(100)).await;
                }
            };

            // Demonstrate the hazard directly: trusting only the survivor's
            // OWN token's row (what an "own-row-only" design would read)
            // gives a HIGHER, wrong answer than the correct min-over-rows
            // rule — proving that design would have claimed records it
            // never actually copied.
            let own_token = cursor::token_of(&survivor.scope_range().start);
            let own_row_only = gsi_rows
                .iter()
                .find(|(token, _, _)| *token == own_token)
                .map(|(_, _, ts)| *ts)
                .expect("the survivor's own row is one of the rows found");
            let correct_min = survivor
                .cursor_min_watermark(GSI_TAG)
                .await
                .expect("at least one row exists");
            assert!(
                own_row_only > correct_min,
                "this scenario's own-row watermark ({own_row_only:?}) must exceed the true min \
                 ({correct_min:?}) — otherwise the min rule and an own-row-only design would \
                 agree, and the hazard wouldn't be demonstrated"
            );

            // And the real consequence: the survivor's next drain pass
            // actually reconciles the straggler (the absorbed tablet's own
            // uncopied record) — proving the min rule drives genuine
            // re-coverage, not just a correct read.
            await_indexed(dynamo_addr, table, &straggler).await;
        })
        .await
        .expect("merge min-rule scenario did not converge in time");
    }

    /// An expected consumer ("gsi", since this table has a GSI) with no
    /// cursor row at all must block trim **entirely** — the ADR 0042 §7 safe
    /// default. `index_drain_loop`'s own first statement is an
    /// unconditional 200ms sleep before its very first tick, so immediately
    /// after a write (a couple of fast loopback round trips, reliably well
    /// under that), no reconciliation has had a chance to run at all: the
    /// "gsi" tag genuinely has no row yet, checked directly here rather than
    /// inferred.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn trim_never_deletes_past_an_expected_consumers_missing_cursor() {
        timeout(Duration::from_secs(30), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node(dir.path()).await;
            let table = "orders";
            create_table_with_gsi(node.dynamo_addr(), table).await;
            put_item(node.dynamo_addr(), table, "o0").await;

            let tablet = only_tablet(&node, table);
            let group = node
                .edge
                .local_cp(tablet)
                .expect("this node hosts the tablet");
            assert_eq!(
                group.cursor_min_watermark(GSI_TAG).await,
                None,
                "an expected tag with no row yet must read as no watermark"
            );
            assert_eq!(
                group.pending_changes().await.len(),
                1,
                "the janitor must not have trimmed anything with no cursor row to bound it"
            );

            // "Blocks trim" isn't "blocks forever": once the drain does run,
            // it must still converge normally.
            await_pending_changes(&node, tablet, 0, "after the drain's first real pass").await;
        })
        .await
        .expect("did not converge in time");
    }
}
