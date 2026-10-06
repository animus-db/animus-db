//! The MREC **shipper** (ADR 0075 section 4.2, G-01 stage G-d M4): the
//! per-tablet, leader-side half of cross-cluster replication.
//!
//! # State-based, derivative shipping (plan fork F2)
//!
//! A change record is only a *dirty-key signal*. The shipper reads the key's
//! **current** committed base row, drops it unless the row's stamp
//! (`MrecVersion::region_id`) says **this** region originated it (an unversioned
//! live row counts as ours, shipped under a fixed initial-copy stamp), and
//! ships `(pk, sk, item | tombstone, ver)` to the peer, whose last-writer-wins
//! apply (M2) makes the delivery idempotent and order-free. One path therefore
//! serves four situations: steady state (dirty keys from the log), the initial
//! copy of a new replica, a resync after a peer fell past the retention cap, and
//! a split child (which starts without a cursor). The price is that the
//! receiver's stream sees *coalesced* states (a key written twice between two
//! ticks ships once); AWS Streams make no cross-item guarantee either.
//!
//! # Split lineage (plan risk R1): unfiltered scan, deliberately
//!
//! `trim_split_child` drops a child's whole CHANGE and CURSOR scopes, so a split
//! child has no cursor and starts a **full scan of its own current rows** (its
//! range only), then resumes the log. That is always correct (every row of the
//! child ships; deletes ship as tombstones; the peer's last-writer-wins apply
//! makes re-delivery of rows it already holds a no-op) and was chosen over the
//! plan's inherited-floor filter (`ver.wall_ms >= parent's shipped wall_ms -
//! skew - margin`, kept in a cursor row exempt from the trim). The cost: a split
//! of a large MREC tablet re-sends its rows once (the peer answers `Superseded`
//! for each). The floor would need a durable cursor-exemption (an ADR 0073
//! format change) and a skew-safety argument for modest savings; revisit only if
//! split-time WAN volume shows up in practice. Covered end to end by
//! `sim_world_mrec_e2e_tests::a_split_of_the_source_tablet_keeps_shipping_every_row`.
//!
//! Loop prevention is state-based too: a row stored with a foreign `region_id`
//! is never shipped, so no origin flag exists on a change record.
//!
//! # Cursor rows (replicated with the tablet, `KIND_CURSOR`)
//!
//! - `mrec:<region>`: the watermark convention (packed HLC). Every change
//!   record at or below it has been delivered to `<region>`. Absent = this
//!   tablet has never shipped to that peer (a fresh replica, a split child, or a
//!   forced resync): the shipper starts a scan.
//! - `mrecscan:<region>`: present while a scan is in progress. Value: `0x00`
//!   then the last scanned base key (empty = not started). A scan fixes the
//!   log watermark at the tablet's newest change record *when it started*, so
//!   when the scan finishes the log mode resumes there and every write made
//!   during the scan is shipped (twice at worst).
//!
//! A cursor advances **only after the peer acknowledged the batch** (`Applied`
//! or `Superseded` for every record; `Retry` holds it). The trim janitor holds
//! the hot change log for the `mrec:<region>` term of every shippable replica,
//! and past `MrecConfig::max_backlog` drops a *log-mode* cursor (clearing it, so
//! the shipper resyncs by scan) rather than letting an unreachable peer pin the
//! log forever.
//!
//! # Pacing
//!
//! One in-flight batch per `(tablet, peer)` (the tick awaits the ack), at most
//! [`MREC_BATCH_ROWS`] rows / [`MREC_BATCH_BYTES`] bytes per frame, exponential
//! backoff with jitter through `env.sleep`-free bookkeeping (a retry-after
//! instant checked at the next tick). Nothing here can fail or stall a local
//! write: the shipper only reads and writes its own cursor rows.

use std::time::Duration;

use animus_control::Metadata;
use animus_control::schema::{MrecReplica, MrecReplicaStatus};
use animus_control::version::Gate;
use animus_cp_data::cursor;
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{KIND_CURSOR, ShipGet};
use animus_dynamo::{AttributeValue, ChangeRecord, Item};
use animus_env::{Env, Metric};
use animus_item::MrecVersion;
use animus_node::host::RelayClient;
use animus_node::{MREC_PROTO, MrecAnswer, MrecApplyRequest, MrecApplyResponse, MrecRecord};
use animus_tablet::TabletId;

use crate::dynamo;
use crate::index_drain::{CHANGE_KEY_SUFFIX_BYTES, record_hlc_ordinal};
use crate::mrec_peer::{HealthKey, PeerClient, PeerError};
use crate::{ClientCtx, CpGroup, MetaCommand};

/// Most rows one frame carries.
pub(crate) const MREC_BATCH_ROWS: usize = 200;
/// Most payload bytes (approximate, serialized rows) one frame carries.
pub(crate) const MREC_BATCH_BYTES: usize = 1 << 20;
/// How long the shipper waits for a peer's answer to one frame.
pub(crate) const MREC_ACK_TIMEOUT: Duration = Duration::from_secs(2);
const BACKOFF_MIN_MS: u64 = 100;
const BACKOFF_MAX_MS: u64 = 5_000;
/// The wall part of the stamp an **unversioned** live row is shipped under (a
/// row from before the table became MREC): below every real stamp, so any real
/// write on either side beats it, and identical on every re-send.
pub(crate) const INITIAL_COPY_WALL_MS: u64 = 1;
/// How long a cursor write waits to apply.
const CURSOR_TIMEOUT: Duration = Duration::from_secs(5);

/// The `mrec:<region>` log-watermark cursor tag.
#[must_use]
pub(crate) fn cursor_tag(region: &str) -> String {
    format!("mrec:{region}")
}

/// The `mrecscan:<region>` scan-position cursor tag.
#[must_use]
pub(crate) fn scan_tag(region: &str) -> String {
    format!("mrecscan:{region}")
}

/// Whether a replica is currently shipped to.
#[must_use]
pub(crate) fn is_shippable(r: &MrecReplica) -> bool {
    !r.local
        && matches!(
            r.status,
            MrecReplicaStatus::Creating | MrecReplicaStatus::Active
        )
}

/// What one tick did for one `(tablet, peer)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShipOutcome {
    /// Nothing to do (caught up, or the table is not shipping from here).
    Idle,
    /// A batch was delivered and the cursor advanced.
    Shipped,
    /// Waiting (backoff, a pending intent, a peer that has not caught up).
    Waiting,
}

fn now_ms<E: Env>(env: &E) -> u64 {
    env.now().0 / 1_000_000
}

/// Classify one stored base row for shipping: `None` = never ship it (a row
/// another region originated, an unversioned tombstone, a keyless tombstone).
fn row_to_record(
    schema: &animus_dynamo::TableSchema,
    local_id: u32,
    value: &[u8],
) -> Option<MrecRecord> {
    let (item, ver) = animus_item::decode_stored_item_versioned(value).ok()?;
    match (item, ver) {
        (Some(item), Some(v)) => {
            if v.region_id != local_id {
                return None;
            }
            record_from_item(schema, item, v)
        }
        (Some(item), None) => {
            // Pre-conversion row: ours by definition.
            let v = MrecVersion {
                wall_ms: INITIAL_COPY_WALL_MS,
                logical: 0,
                region_id: local_id,
            };
            record_from_item(schema, item, v)
        }
        (None, Some(v)) => {
            if v.region_id != local_id {
                return None;
            }
            let (pk, sk) = animus_item::decode_tombstone_key(value)?;
            Some(MrecRecord {
                pk,
                sk,
                item: None,
                ver: v,
            })
        }
        (None, None) => None,
    }
}

fn record_from_item(
    schema: &animus_dynamo::TableSchema,
    item: Item,
    ver: MrecVersion,
) -> Option<MrecRecord> {
    let pk = item.get(&schema.partition_key)?.clone();
    let sk: Option<AttributeValue> = match &schema.sort_key {
        Some(name) => Some(item.get(name)?.clone()),
        None => None,
    };
    Some(MrecRecord {
        pk,
        sk,
        item: Some(item),
        ver,
    })
}

fn approx_bytes(r: &MrecRecord) -> usize {
    serde_json::to_vec(r).map_or(256, |v| v.len())
}

/// One shipping pass over `table`'s tablet `tablet` (which `group` hosts and
/// this node leads): for every shippable peer replica, deliver at most one
/// batch. Never fails the caller; every problem lands in the health memo.
pub(crate) async fn mrec_ship_tick<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    meta: &Metadata,
    table: &str,
    tablet: TabletId,
    group: &CpGroup<E>,
    client: &dyn PeerClient,
) -> ShipOutcome {
    let Some(spec) = meta.table_global(table).filter(|g| g.is_mrec()) else {
        return ShipOutcome::Idle;
    };
    if !ctx.edge.version().features.is_open(Gate::MrecReplication) {
        return ShipOutcome::Idle;
    }
    let Some(local) = spec.replicas.iter().find(|r| r.local) else {
        return ShipOutcome::Idle;
    };
    let mut outcome = ShipOutcome::Idle;
    for replica in spec.replicas.iter().filter(|r| is_shippable(r)) {
        let one = ship_one(
            ctx,
            meta,
            table,
            tablet,
            group,
            client,
            local.region_id,
            replica,
        )
        .await;
        if one != ShipOutcome::Idle && outcome != ShipOutcome::Waiting {
            outcome = one;
        }
    }
    outcome
}

#[allow(clippy::too_many_arguments)]
async fn ship_one<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    meta: &Metadata,
    table: &str,
    tablet: TabletId,
    group: &CpGroup<E>,
    client: &dyn PeerClient,
    local_id: u32,
    replica: &MrecReplica,
) -> ShipOutcome {
    let cfg = &ctx.mrec;
    let region = &replica.region;
    let key: HealthKey = (table.to_owned(), tablet.0, region.clone());
    let now_ns = ctx.env.now().0;
    {
        let h = cfg.health.lock().expect("mrec health poisoned");
        if h.get(&key).is_some_and(|h| h.retry_after > now_ns) {
            return ShipOutcome::Waiting;
        }
    }
    let Some(peer_idx) = cfg.peer_index(region) else {
        fail(
            ctx,
            &key,
            format!("region `{region}` is not a configured peer of this cluster"),
            true,
        );
        return ShipOutcome::Waiting;
    };
    let schema = dynamo::schema_for(meta, table);
    let start = group.scope_range().start;
    let cursor_key = cursor::cursor_key(&start, &cursor_tag(region));
    let scan_key = cursor::cursor_key(&start, &scan_tag(region));
    let watermark = group
        .local_get_kind(KIND_CURSOR, &cursor_key)
        .await
        .and_then(|b| cursor::decode_watermark(&b));
    let scan = group.local_get_kind(KIND_CURSOR, &scan_key).await;

    let Some(watermark) = watermark else {
        // No cursor: first copy, a split child, or a forced resync.
        let h0 = group
            .hot_change_max()
            .await
            .map_or_else(HlcTimestamp::zero, |(ts, _)| ts);
        let writes = vec![
            (cursor_key, Some(cursor::encode_watermark(h0))),
            (scan_key, Some(vec![0u8])),
        ];
        if let Err(e) = write_cursors(group, writes).await {
            fail(ctx, &key, format!("cursor start: {e}"), false);
            return ShipOutcome::Waiting;
        }
        if replica.status == MrecReplicaStatus::Active {
            ctx.env.metrics().incr(Metric::MrecResyncTotal);
        }
        let mut h = cfg.health.lock().expect("mrec health poisoned");
        let e = h.entry(key).or_default();
        e.scanning = true;
        e.needs_resync = replica.status == MrecReplicaStatus::Active;
        e.caught_up = false;
        return ShipOutcome::Waiting;
    };

    match scan {
        Some(bytes) => {
            scan_step(
                ctx,
                table,
                tablet,
                group,
                client,
                &schema,
                local_id,
                replica,
                peer_idx,
                &key,
                &scan_key,
                &bytes,
                spec_copied(meta, table, region, tablet),
            )
            .await
        }
        None => {
            log_step(
                ctx,
                table,
                tablet,
                group,
                client,
                &schema,
                local_id,
                peer_idx,
                &key,
                &cursor_key,
                watermark,
            )
            .await
        }
    }
}

fn spec_copied(meta: &Metadata, table: &str, region: &str, tablet: TabletId) -> bool {
    meta.table_global(table).is_some_and(|g| {
        g.replicas
            .iter()
            .any(|r| r.region == region && r.copied.contains(&tablet.0))
    })
}

/// Record a failed attempt: bump the failure count, schedule the next try with
/// exponential backoff and jitter, remember the message.
fn fail<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    key: &HealthKey,
    message: String,
    operator_error: bool,
) {
    ctx.env.metrics().incr(Metric::MrecShipErrorsTotal);
    let mut h = ctx.mrec.health.lock().expect("mrec health poisoned");
    let e = h.entry(key.clone()).or_default();
    e.failures = e.failures.saturating_add(1);
    let exp = BACKOFF_MIN_MS
        .saturating_mul(1u64 << e.failures.min(6))
        .min(BACKOFF_MAX_MS);
    let jitter = ctx.env.gen_below(exp / 2 + 1);
    let wait_ms = if operator_error {
        BACKOFF_MAX_MS
    } else {
        exp / 2 + jitter
    };
    e.retry_after = ctx.env.now().0.saturating_add(wait_ms * 1_000_000);
    e.last_error = Some(message);
    e.operator_error = operator_error;
    e.caught_up = false;
}

fn succeed<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    key: &HealthKey,
    rows: u64,
    backlog: u64,
    lag_ms: u64,
    scanning: bool,
) {
    ctx.env
        .metrics()
        .incr_by(Metric::MrecShippedRowsTotal, rows);
    ctx.env.metrics().set(Metric::MrecPendingRecords, backlog);
    ctx.env.metrics().set(Metric::MrecReplicationLagMs, lag_ms);
    let mut h = ctx.mrec.health.lock().expect("mrec health poisoned");
    let e = h.entry(key.clone()).or_default();
    e.failures = 0;
    e.retry_after = 0;
    e.last_error = None;
    e.operator_error = false;
    e.backlog = backlog;
    e.lag_ms = lag_ms;
    e.scanning = scanning;
    if !scanning {
        e.needs_resync = false;
    }
    e.shipped_rows += rows;
    if rows > 0 {
        e.last_ack_wall_ms = Some(ctx.env.wall_now().0);
    }
}

/// Send one batch; `Ok(())` once every record is `Applied`/`Superseded`/
/// `Rejected` (a rejected record is permanent: counted and surfaced, never
/// retried), `Err(message, operator_error)` otherwise.
async fn send_batch<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    client: &dyn PeerClient,
    peer_idx: usize,
    table: &str,
    records: Vec<MrecRecord>,
) -> Result<(), (String, bool)> {
    let Some(from_region) = ctx.mrec.region.clone() else {
        return Err(("no cluster_settings.region configured".into(), true));
    };
    let n = records.len();
    let payload = serde_json::to_vec(&MrecApplyRequest {
        proto: MREC_PROTO,
        from_region,
        table: table.to_owned(),
        records,
        control: None,
    })
    .map_err(|e| (e.to_string(), true))?;
    let bytes = client
        .call(peer_idx, payload, MREC_ACK_TIMEOUT)
        .await
        .map_err(|e| match e {
            PeerError::Timeout => ("peer did not answer in time".to_owned(), false),
            PeerError::Transport(m) => (format!("peer transport: {m}"), false),
            PeerError::Refused(m) => (m, true),
        })?;
    match crate::mrec_peer::decode_response(&bytes)
        .map_err(|e| (format!("undecodable peer reply: {e:?}"), false))?
    {
        MrecApplyResponse::Refused { message, retryable } => Err((message, !retryable)),
        MrecApplyResponse::Done => Err(("unexpected control reply".into(), true)),
        MrecApplyResponse::Answers(answers) => {
            if answers.len() != n {
                return Err(("peer answered a different record count".into(), true));
            }
            let mut retry = 0usize;
            let mut rejected: Option<String> = None;
            for a in answers {
                match a {
                    MrecAnswer::Applied | MrecAnswer::Superseded => {}
                    MrecAnswer::Retry => retry += 1,
                    MrecAnswer::Rejected { message } => rejected = Some(message),
                }
            }
            if retry > 0 {
                return Err((format!("{retry} record(s) answered Retry"), false));
            }
            if let Some(m) = rejected {
                // Permanent for that record; surfaced, but the cursor moves.
                let mut h = ctx.mrec.health.lock().expect("mrec health poisoned");
                let e = h.entry((table.to_owned(), 0, String::new())).or_default();
                e.last_error = Some(format!("a record was rejected: {m}"));
            }
            Ok(())
        }
    }
}

/// Scan-mode step: ship the next window of base rows, then advance (or finish)
/// the scan.
#[allow(clippy::too_many_arguments)]
async fn scan_step<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    table: &str,
    tablet: TabletId,
    group: &CpGroup<E>,
    client: &dyn PeerClient,
    schema: &animus_dynamo::TableSchema,
    local_id: u32,
    replica: &MrecReplica,
    peer_idx: usize,
    key: &HealthKey,
    scan_key: &[u8],
    scan_bytes: &[u8],
    already_copied: bool,
) -> ShipOutcome {
    let last = scan_bytes.get(1..).unwrap_or(&[]);
    let from: Vec<u8> = if last.is_empty() {
        Vec::new()
    } else {
        let mut k = last.to_vec();
        k.push(0x00);
        k
    };
    let (rows, blocked) = group.local_scan_for_ship(&from, MREC_BATCH_ROWS).await;
    let mut records = Vec::new();
    let mut bytes = 0usize;
    let mut last_scanned: Option<Vec<u8>> = None;
    let mut taken = 0usize;
    for (k, v) in &rows {
        if let Some(r) = row_to_record(schema, local_id, v) {
            let b = approx_bytes(&r);
            if !records.is_empty() && bytes + b > MREC_BATCH_BYTES {
                break;
            }
            bytes += b;
            records.push(r);
        }
        last_scanned = Some(k.clone());
        taken += 1;
    }
    let reached_end = blocked.is_none() && taken == rows.len() && rows.len() < MREC_BATCH_ROWS;
    if !records.is_empty() {
        let n = records.len() as u64;
        if let Err((m, op)) = send_batch(ctx, client, peer_idx, table, records).await {
            fail(ctx, key, m, op);
            return ShipOutcome::Waiting;
        }
        succeed(ctx, key, n, 0, 0, true);
    }
    let mut writes: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    if reached_end {
        writes.push((scan_key.to_vec(), None));
    } else if let Some(l) = last_scanned {
        let mut v = vec![0u8];
        v.extend_from_slice(&l);
        writes.push((scan_key.to_vec(), Some(v)));
    }
    if !writes.is_empty()
        && let Err(e) = write_cursors(group, writes).await
    {
        fail(ctx, key, format!("scan cursor: {e}"), false);
        return ShipOutcome::Waiting;
    }
    if reached_end {
        succeed(ctx, key, 0, 0, 0, false);
        if !already_copied
            && matches!(
                replica.status,
                MrecReplicaStatus::Creating | MrecReplicaStatus::Active
            )
        {
            ctx.propose_schema(&MetaCommand::MarkMrecCopied {
                table: table.to_owned(),
                region: replica.region.clone(),
                tablet: tablet.0,
            })
            .await;
        }
        return ShipOutcome::Shipped;
    }
    if blocked.is_some() && rows.is_empty() {
        // Held in front of a pending intent: check again shortly.
        let mut h = ctx.mrec.health.lock().expect("mrec health poisoned");
        let e = h.entry(key.clone()).or_default();
        e.scanning = true;
        e.retry_after = ctx.env.now().0.saturating_add(200 * 1_000_000);
        return ShipOutcome::Waiting;
    }
    ShipOutcome::Shipped
}

/// Log-mode step: ship the current rows of the keys dirtied since the
/// watermark, then advance it.
#[allow(clippy::too_many_arguments)]
async fn log_step<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    table: &str,
    _tablet: TabletId,
    group: &CpGroup<E>,
    client: &dyn PeerClient,
    schema: &animus_dynamo::TableSchema,
    local_id: u32,
    peer_idx: usize,
    key: &HealthKey,
    cursor_key: &[u8],
    watermark: HlcTimestamp,
) -> ShipOutcome {
    // (hlc, ordinal, base key) of every record past the watermark.
    let mut dirty: Vec<(HlcTimestamp, u32, Vec<u8>)> = Vec::new();
    for (k, v) in group.pending_changes_key_order().await {
        let Some((ts, ord)) = record_hlc_ordinal(&k) else {
            continue;
        };
        if ts <= watermark {
            continue;
        }
        let Some(rec) = ChangeRecord::decode(&v) else {
            continue;
        };
        let Some(fp) = k.len().checked_sub(CHANGE_KEY_SUFFIX_BYTES) else {
            continue;
        };
        let mut base = k[..fp].to_vec();
        base.extend_from_slice(&rec.base_sk);
        dirty.push((ts, ord, if rec.marker { Vec::new() } else { base }));
    }
    if dirty.is_empty() {
        succeed(ctx, key, 0, 0, 0, false);
        let mut h = ctx.mrec.health.lock().expect("mrec health poisoned");
        h.entry(key.clone()).or_default().caught_up = true;
        return ShipOutcome::Idle;
    }
    dirty.sort();
    let backlog = dirty
        .iter()
        .map(|d| d.2.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .len() as u64;
    let lag_ms = now_ms(&ctx.env).saturating_sub(dirty[0].0.wall_ms);
    // The longest prefix with at most MREC_BATCH_ROWS distinct keys, never
    // splitting one HLC (a batch commit's records share it).
    let mut distinct = std::collections::BTreeSet::new();
    let mut cut = 0usize;
    for (i, (ts, _, base)) in dirty.iter().enumerate() {
        if !base.is_empty()
            && !distinct.contains(base)
            && distinct.len() >= MREC_BATCH_ROWS
            && dirty[cut.saturating_sub(1)].0 != *ts
        {
            break;
        }
        if !base.is_empty() {
            distinct.insert(base.clone());
        }
        cut = i + 1;
    }
    let new_wm = dirty[cut - 1].0;
    let mut records = Vec::new();
    let mut bytes = 0usize;
    for base in &distinct {
        match group.local_get_for_ship(base).await {
            ShipGet::Pending => {
                // A transaction holds the key: wait for it (the resolve leaves
                // a record), never skip the key.
                let mut h = ctx.mrec.health.lock().expect("mrec health poisoned");
                let e = h.entry(key.clone()).or_default();
                e.backlog = backlog;
                e.lag_ms = lag_ms;
                e.retry_after = ctx.env.now().0.saturating_add(200 * 1_000_000);
                return ShipOutcome::Waiting;
            }
            ShipGet::Value(None) => {}
            ShipGet::Value(Some(v)) => {
                if let Some(r) = row_to_record(schema, local_id, &v) {
                    bytes += approx_bytes(&r);
                    records.push(r);
                }
            }
        }
        if bytes >= MREC_BATCH_BYTES {
            // Over the byte budget: ship what we have and leave the rest for
            // the next tick (the watermark stays put: re-shipping is idempotent).
            break;
        }
    }
    let over_budget = bytes >= MREC_BATCH_BYTES;
    let n = records.len() as u64;
    if !records.is_empty()
        && let Err((m, op)) = send_batch(ctx, client, peer_idx, table, records).await
    {
        fail(ctx, key, m, op);
        return ShipOutcome::Waiting;
    }
    if over_budget {
        succeed(ctx, key, n, backlog, lag_ms, false);
        return ShipOutcome::Shipped;
    }
    if let Err(e) = write_cursors(
        group,
        vec![(cursor_key.to_vec(), Some(cursor::encode_watermark(new_wm)))],
    )
    .await
    {
        fail(ctx, key, format!("cursor advance: {e}"), false);
        return ShipOutcome::Waiting;
    }
    let remaining = backlog.saturating_sub(distinct.len() as u64);
    succeed(ctx, key, n, remaining, lag_ms, false);
    ShipOutcome::Shipped
}

/// Durably write (or delete) cursor rows on `group`'s tablet: a direct local
/// propose + wait-for-apply, unfenced for the reason documented on
/// `index_drain::advance_backfill_cursor`.
pub(crate) async fn write_cursors<E: Env>(
    group: &CpGroup<E>,
    writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
) -> Result<(), String> {
    let batch = writes
        .into_iter()
        .map(|(k, v)| (KIND_CURSOR, k, v))
        .collect();
    let index = match group.put_kind_batch(batch, Vec::new()) {
        animus_control::ProposeResult::Accepted { index, .. } => index,
        other => return Err(format!("not accepted: {other:?}")),
    };
    let deadline = group.env().now().saturating_add(CURSOR_TIMEOUT);
    while group.env().now() < deadline {
        if group.engine_applied_index() >= index {
            return Ok(());
        }
        group.env().sleep(Duration::from_millis(10)).await;
    }
    Err("did not apply in time".into())
}

/// Delete both of a peer's cursor rows on `group`'s tablet (replica removal, or
/// the janitor forcing a resync).
pub(crate) async fn clear_peer_cursors<E: Env>(
    group: &CpGroup<E>,
    region: &str,
) -> Result<(), String> {
    let start = group.scope_range().start;
    write_cursors(
        group,
        vec![
            (cursor::cursor_key(&start, &cursor_tag(region)), None),
            (cursor::cursor_key(&start, &scan_tag(region)), None),
        ],
    )
    .await
}

/// Run one tick for every led tablet of `table` this node hosts. The shared
/// body of the prod loop and the SimWorld tests.
pub(crate) async fn mrec_ship_table<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    table: &str,
    client: &dyn PeerClient,
) -> ShipOutcome {
    let meta = ctx.effective_metadata();
    let mut outcome = ShipOutcome::Idle;
    for (tablet, group) in ctx.edge.hosted_groups() {
        let ours = meta
            .tablets
            .get(&tablet)
            .is_some_and(|t| t.table.as_deref() == Some(table));
        if !ours || !group.is_leader() {
            continue;
        }
        if let Some(spec) = meta.table_global(table) {
            for r in spec
                .replicas
                .iter()
                .filter(|r| !r.local && r.status == MrecReplicaStatus::Deleting)
            {
                crate::mrec_saga::drop_deleting_cursors(&group, &r.region).await;
            }
        }
        let one = mrec_ship_tick(ctx, &meta, table, tablet, &group, client).await;
        if one != ShipOutcome::Idle && outcome != ShipOutcome::Waiting {
            outcome = one;
        }
    }
    outcome
}

/// The trim janitor's MREC term for one tablet: the minimum packed `mrec:<region>`
/// watermark over its shippable replicas (`None` = no MREC hold). A peer whose
/// cursor is older than `MrecConfig::max_backlog` is dropped instead of held:
/// both cursor rows are cleared, so the shipper resyncs it by scan rather than
/// letting an unreachable peer pin the log forever.
pub(crate) async fn trim_term<E: Env>(
    env: &E,
    cfg: &crate::mrec_peer::MrecConfig,
    meta: &Metadata,
    table: &str,
    group: &CpGroup<E>,
) -> Option<u64> {
    let spec = meta.table_global(table).filter(|g| g.is_mrec())?;
    let start = group.scope_range().start;
    let mut term: Option<u64> = None;
    let cap_ms = u64::try_from(cfg.max_backlog.as_millis()).unwrap_or(u64::MAX);
    for r in spec.replicas.iter().filter(|r| is_shippable(r)) {
        let Some(w) = group
            .local_get_kind(
                KIND_CURSOR,
                &cursor::cursor_key(&start, &cursor_tag(&r.region)),
            )
            .await
            .and_then(|b| cursor::decode_watermark(&b))
        else {
            continue; // no cursor: a scan will start, the log is not needed
        };
        if now_ms(env).saturating_sub(w.wall_ms) > cap_ms {
            if clear_peer_cursors(group, &r.region).await.is_ok() {
                env.metrics().incr(Metric::MrecResyncTotal);
            }
            continue;
        }
        let p = animus_cp_data::hlc::pack(w);
        term = Some(term.map_or(p, |t| t.min(p)));
    }
    term
}

/// How often the prod loop ticks.
const MREC_LOOP_INTERVAL: Duration = Duration::from_millis(200);

/// The per-node MREC shipper loop (ProdEnv): for every MREC table, ship the
/// tablets this node leads. Inert until a table is MREC and the gate is open.
#[allow(
    clippy::disallowed_methods,
    reason = "a concrete ClientCtx (E = ProdEnv) process-boundary background loop, same class as index_drain::change_consumer_loop; SimWorld drives mrec_ship_table directly"
)]
pub(crate) async fn mrec_ship_loop(ctx: ClientCtx) {
    let mut client: Option<crate::mrec_peer::ProdPeerClient> = None;
    loop {
        tokio::time::sleep(MREC_LOOP_INTERVAL).await;
        let meta = ctx.effective_metadata();
        let tables: Vec<String> = meta
            .schemas
            .iter()
            .filter(|(_, s)| s.global.as_ref().is_some_and(|g| g.is_mrec()))
            .map(|(n, _)| n.clone())
            .collect();
        if tables.is_empty() {
            continue;
        }
        if client.is_none() {
            match crate::mrec_peer::ProdPeerClient::new(
                ctx.mrec.clone(),
                ctx.tls.clone(),
                ctx.mrec.node_tls.as_ref(),
                ctx.edge.version().features.clone(),
            ) {
                Ok(c) => client = Some(c),
                Err(e) => {
                    tracing::warn!(error = %e, "mrec shipper: cannot build the peer client");
                    continue;
                }
            }
        }
        let Some(c) = client.as_ref() else { continue };
        for t in &tables {
            let _ = crate::mrec_saga::mrec_saga_table(&ctx, t, c).await;
            let _ = mrec_ship_table(&ctx, t, c).await;
        }
    }
}
