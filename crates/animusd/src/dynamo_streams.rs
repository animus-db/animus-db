//! DynamoDB **Streams** (ADR 0042 §3/§5/§6/§7/§11, ADR 0043 §A7b, PR6): the
//! consumer-facing read API — `ListStreams`/`DescribeStream`/
//! `GetShardIterator`/`GetRecords` — dispatched on the **same** listener as
//! the DynamoDB_20120810 edge, under `X-Amz-Target:
//! DynamoDBStreams_20120810.*` (`dynamo.rs`'s `dispatch`, the decided
//! same-listener F-fork). Business logic only: every JSON shape and the
//! iterator-token/shard-id codecs live in `animus_dynamo::streams_wire`
//! (pure); this module is the read path's only impure layer — the one that
//! reads `Metadata`, the tablet-host routing, and the segment store.
//!
//! ## Label resolution (F12-b, ADR 0042 §4/§11)
//!
//! [`resolve_label`] is the one function every operation below funnels
//! through: a label is valid iff it names the table's **current** enabled
//! stream, or the table's catalog still holds at least one row for it (a
//! `DISABLED`-but-unreaped stream's grace window). Neither ⇒
//! `ResourceNotFoundException`.
//!
//! ## The two `GetRecords` serve paths
//!
//! A **sealed** shard (a catalog row exists for `(tablet, epoch)`) is served
//! by **any** node: fetch the segment object via the row's own recorded
//! `replicas` (`SegmentStoreHandle::get_sealed`), decode-and-slice to the
//! row's committed `hlc_range` (the superset-slice rule, ADR 0042 §10 —
//! `animus_cp_data::segment::decode_and_slice`), then filter/paginate. An
//! **open** shard (no catalog row — the tablet's current hot tail) is
//! served by the tablet's own leader, forwarded via the internal
//! `ClientRequest::StreamHotRead` (`ClientCtx::read_stream_hot_records`,
//! `index_drain::hot_read`) — **no `ReadIndex` barrier** (F8). Which path a
//! shard id resolves to is decided **fresh at every `GetRecords` call**
//! (never cached from `GetShardIterator` mint time), which is exactly what
//! makes an open-shard iterator survive a seal that happens between polls
//! (ADR 0042 §2's "sealing never invalidates an open-shard iterator").

use animus_control::{Metadata, StreamShardRow, StreamViewType};
use animus_cp_data::{hlc, segment};
use animus_dynamo::ChangeRecord;
use animus_dynamo::streams_wire::{
    self, ShardDescriptor, ShardIteratorType, StreamDescription, StreamSummary, StreamsOperation,
};
use animus_dynamo::wire::WireError;
use animus_tablet::TabletId;

use crate::ClientCtx;
use crate::dynamo::{internal, schema_for};

/// `ListStreams`' default page size (real DynamoDB's own default).
const DEFAULT_LIST_LIMIT: usize = 100;
/// `DescribeStream`'s default shard-page size.
const DEFAULT_DESCRIBE_LIMIT: usize = 100;
/// `GetRecords`' default (and maximum) page size — real DynamoDB's own
/// contract for this API (`1..=1000`, default `1000`).
const DEFAULT_GET_RECORDS_LIMIT: usize = 1000;
const MAX_GET_RECORDS_LIMIT: usize = 1000;

/// Decode + run a DynamoDB Streams operation from its `X-Amz-Target` value
/// and JSON body, returning `(http status, json body)` — the Streams
/// service's own [`crate::dynamo::execute`] sibling.
pub(crate) async fn execute(ctx: &ClientCtx, target: &str, body: &[u8]) -> (u16, String) {
    match streams_wire::decode_request(target, body) {
        Ok(op) => match run_operation(ctx, op).await {
            Ok(body) => (200, body),
            Err(err) => (error_status(&err), err.to_json()),
        },
        Err(err) => (error_status(&err), err.to_json()),
    }
}

fn error_status(err: &WireError) -> u16 {
    match err.code {
        "UnknownOperationException" => 400,
        "InternalServerError" => 500,
        _ => 400,
    }
}

async fn run_operation(ctx: &ClientCtx, op: StreamsOperation) -> Result<String, WireError> {
    match op {
        StreamsOperation::ListStreams {
            table_name,
            limit,
            exclusive_start_stream_arn,
        } => list_streams(
            ctx,
            table_name.as_deref(),
            limit,
            exclusive_start_stream_arn.as_deref(),
        ),
        StreamsOperation::DescribeStream {
            stream_arn,
            limit,
            exclusive_start_shard_id,
        } => describe_stream(ctx, &stream_arn, limit, exclusive_start_shard_id.as_deref()),
        StreamsOperation::GetShardIterator {
            stream_arn,
            shard_id,
            shard_iterator_type,
            sequence_number,
        } => {
            get_shard_iterator(
                ctx,
                &stream_arn,
                &shard_id,
                shard_iterator_type,
                sequence_number.as_deref(),
            )
            .await
        }
        StreamsOperation::GetRecords {
            shard_iterator,
            limit,
        } => get_records(ctx, &shard_iterator, limit).await,
    }
}

fn not_found(what: &str) -> WireError {
    WireError {
        code: "ResourceNotFoundException",
        message: format!("{what} not found"),
    }
}

fn trimmed_data_access(what: &str) -> WireError {
    WireError {
        code: "TrimmedDataAccessException",
        message: format!("{what} has been trimmed and is no longer accessible"),
    }
}

/// F12-b label resolution (ADR 0042 §4/§11): `Ok(true)` iff `label` is
/// `table`'s *current* enabled stream, `Ok(false)` iff it's a
/// disabled-but-unreaped one (the grace window — the catalog still holds at
/// least one row for it), `Err(ResourceNotFoundException)` otherwise.
fn resolve_label(meta: &Metadata, table: &str, label: &str) -> Result<bool, WireError> {
    if meta.table_stream(table).is_some_and(|s| s.label == label) {
        return Ok(true);
    }
    if meta.stream_labels_with_rows(table).contains(label) {
        return Ok(false);
    }
    Err(not_found(&format!("stream `{label}` of table `{table}`")))
}

/// A tablet's own current open shard's epoch (ADR 0042 §2) — mirrors
/// `index_drain::seal_now`'s identical computation: this tablet's own chain
/// length, regardless of label.
fn current_open_epoch(meta: &Metadata, tablet: TabletId) -> u64 {
    meta.stream_shards
        .range((tablet, 0)..=(tablet, u64::MAX))
        .next_back()
        .map_or(0, |((_, e), _)| e + 1)
}

/// Recover a change-log record's own packed HLC from its key's trailing 8
/// bytes (`token || escape(pk) || hlc::pack(ts)`, ADR 0041 §4/ADR 0042 §5 —
/// `animus-dynamo`'s `index::change_record_key`) — the same suffix
/// `ClientResponse::Pairs` (the `StreamHotRead` reply shape) carries, since
/// that response is deliberately the plain `(key, value)` list every other
/// kind-scan reply already is, with no separate out-of-band HLC field.
fn record_hlc_suffix(key: &[u8]) -> Option<u64> {
    let n = key.len().checked_sub(8)?;
    Some(u64::from_be_bytes(key[n..].try_into().ok()?))
}

// --- ListStreams -------------------------------------------------------

/// `ListStreams` (ADR 0042 §3): a pure function of the replicated catalog +
/// schema (F7) — enumerates the table's *current* enabled labels plus every
/// `DISABLED`-but-unreaped label with at least one catalog row (F12-b).
fn list_streams(
    ctx: &ClientCtx,
    table_name: Option<&str>,
    limit: Option<usize>,
    exclusive_start_stream_arn: Option<&str>,
) -> Result<String, WireError> {
    let meta = ctx.effective_metadata();
    let mut pairs: std::collections::BTreeSet<(String, String)> = std::collections::BTreeSet::new();
    for (table, schema) in meta.schemas.iter() {
        if let Some(spec) = &schema.stream {
            pairs.insert((table.clone(), spec.label.clone()));
        }
    }
    for row in meta.stream_shards.values() {
        pairs.insert((row.table.clone(), row.label.clone()));
    }
    let all: Vec<(String, String)> = pairs
        .into_iter()
        .filter(|(t, _)| table_name.is_none_or(|n| t == n))
        .collect();

    let start_idx = match exclusive_start_stream_arn {
        None => 0,
        Some(arn) => all
            .iter()
            .position(|(t, l)| animus_dynamo::wire::stream_arn(t, l) == arn)
            .map_or(0, |i| i + 1),
    };
    let limit = limit.unwrap_or(DEFAULT_LIST_LIMIT).max(1);
    let page: Vec<(String, String)> = all.iter().skip(start_idx).take(limit).cloned().collect();
    let has_more = start_idx + page.len() < all.len();
    let last_evaluated = has_more
        .then(|| {
            page.last()
                .map(|(t, l)| animus_dynamo::wire::stream_arn(t, l))
        })
        .flatten();

    let streams: Vec<StreamSummary> = page
        .into_iter()
        .map(|(table_name, stream_label)| StreamSummary {
            table_name,
            stream_label,
        })
        .collect();
    Ok(streams_wire::list_streams_response(
        &streams,
        last_evaluated.as_deref(),
    ))
}

// --- DescribeStream ------------------------------------------------------

/// `DescribeStream` (ADR 0042 §3): a pure function of `Metadata` (F7) —
/// sealed shards from the catalog, plus the one open shard per live tablet
/// while the label is currently enabled.
fn describe_stream(
    ctx: &ClientCtx,
    stream_arn: &str,
    limit: Option<usize>,
    exclusive_start_shard_id: Option<&str>,
) -> Result<String, WireError> {
    let (table, label) = streams_wire::parse_stream_arn(stream_arn)
        .ok_or_else(|| WireError::validation(format!("malformed StreamArn `{stream_arn}`")))?;
    let meta = ctx.effective_metadata();
    let enabled = resolve_label(&meta, &table, &label)?;
    let view_type = meta
        .stream_view_type(&table, &label)
        .unwrap_or(StreamViewType::NewAndOldImages);
    let dyn_schema = schema_for(&meta, &table);

    let mut shard_entries: Vec<(u64, u64, ShardDescriptor)> = Vec::new();
    for (tablet, epoch, row) in meta.stream_shard_rows_for_label(&table, &label) {
        if row.expired {
            continue; // reaped by the retention sweep (a later PR) — no longer a shard.
        }
        shard_entries.push((
            tablet.0,
            epoch,
            ShardDescriptor {
                shard_id: segment::shard_id(tablet.0, epoch),
                parent_shard_id: meta.stream_shard_parent_id(tablet, epoch),
                starting_sequence_number: row.hlc_range.0,
                ending_sequence_number: Some(row.hlc_range.1),
            },
        ));
    }
    if enabled {
        for (&tablet, _) in meta.tablets_for_table(&table) {
            let epoch = current_open_epoch(&meta, tablet);
            let starting = meta.effective_stream_shard_watermark(tablet).unwrap_or(0);
            shard_entries.push((
                tablet.0,
                epoch,
                ShardDescriptor {
                    shard_id: segment::shard_id(tablet.0, epoch),
                    parent_shard_id: meta.stream_shard_parent_id(tablet, epoch),
                    starting_sequence_number: starting,
                    ending_sequence_number: None,
                },
            ));
        }
    }
    shard_entries.sort_by_key(|(tablet, epoch, _)| (*tablet, *epoch));

    let start_idx = match exclusive_start_shard_id {
        None => 0,
        Some(id) => shard_entries
            .iter()
            .position(|(_, _, s)| s.shard_id == id)
            .map_or(0, |i| i + 1),
    };
    let limit = limit.unwrap_or(DEFAULT_DESCRIBE_LIMIT).max(1);
    let page: Vec<ShardDescriptor> = shard_entries
        .into_iter()
        .skip(start_idx)
        .take(limit)
        .map(|(_, _, s)| s)
        .collect();
    // Recompute against the un-truncated total, using the same start point,
    // to know whether this page left shards unlisted.
    let total = {
        // Cheap re-derivation (same filters, no catalog re-read) rather than
        // keeping two divergent copies of `shard_entries` around.
        let mut n = meta
            .stream_shard_rows_for_label(&table, &label)
            .filter(|(_, _, row)| !row.expired)
            .count();
        if enabled {
            n += meta.tablets_for_table(&table).count();
        }
        n
    };
    let has_more = start_idx + page.len() < total;
    let last_evaluated_shard_id = has_more
        .then(|| page.last().map(|s| s.shard_id.clone()))
        .flatten();

    let desc = StreamDescription {
        table_name: table,
        stream_label: label,
        enabled,
        view_type,
        partition_key: dyn_schema.partition_key,
        sort_key: dyn_schema.sort_key,
        shards: page,
        last_evaluated_shard_id,
    };
    Ok(streams_wire::describe_stream_response(&desc))
}

// --- GetShardIterator ------------------------------------------------------

/// `GetShardIterator` (ADR 0042 §5/§6): mints a stateless position token —
/// no barrier, no store/leader round trip except `LATEST` on a genuinely
/// open shard (which needs one hot read to find the current max).
async fn get_shard_iterator(
    ctx: &ClientCtx,
    stream_arn: &str,
    shard_id: &str,
    iterator_type: ShardIteratorType,
    sequence_number: Option<&str>,
) -> Result<String, WireError> {
    let (table, label) = streams_wire::parse_stream_arn(stream_arn)
        .ok_or_else(|| WireError::validation(format!("malformed StreamArn `{stream_arn}`")))?;
    let (tablet_raw, epoch) = streams_wire::parse_shard_id(shard_id)
        .ok_or_else(|| WireError::validation(format!("malformed ShardId `{shard_id}`")))?;
    let tablet = TabletId(tablet_raw);
    let meta = ctx.effective_metadata();
    let enabled = resolve_label(&meta, &table, &label)?;

    let position = if let Some(row) = meta.stream_shards.get(&(tablet, epoch)) {
        // SEALED shard: any node answers, purely from the catalog row.
        match iterator_type {
            ShardIteratorType::TrimHorizon => row.hlc_range.0,
            ShardIteratorType::Latest => row.hlc_range.1, // the immediate-null path
            ShardIteratorType::AtSequenceNumber => parse_seq(sequence_number)?.saturating_sub(1),
            ShardIteratorType::AfterSequenceNumber => parse_seq(sequence_number)?,
        }
    } else {
        // Must be the label's currently-enabled table's genuine open shard.
        if !enabled
            || current_open_epoch(&meta, tablet) != epoch
            || !meta.tablets.contains_key(&tablet)
        {
            return Err(trimmed_data_access(&format!("shard `{shard_id}`")));
        }
        match iterator_type {
            ShardIteratorType::TrimHorizon => {
                meta.effective_stream_shard_watermark(tablet).unwrap_or(0)
            }
            ShardIteratorType::AtSequenceNumber => parse_seq(sequence_number)?.saturating_sub(1),
            ShardIteratorType::AfterSequenceNumber => parse_seq(sequence_number)?,
            ShardIteratorType::Latest => {
                // The tablet's own leader: one hot read from the effective
                // watermark, taking the max HLC actually present — "current
                // max + a not-yet-existent tick" per ADR 0042 §5, expressed
                // via this crate's exclusive-lower-bound convention as
                // "position = current max" (nothing new yet is > that).
                let watermark = meta.effective_stream_shard_watermark(tablet).unwrap_or(0);
                let hot = ctx
                    .read_stream_hot_records(tablet, watermark, usize::MAX)
                    .await
                    .map_err(|e| internal(&e))?;
                hot.last()
                    .and_then(|(key, _)| record_hlc_suffix(key))
                    .unwrap_or(watermark)
            }
        }
    };
    let token = streams_wire::encode_iterator(&label, shard_id, position);
    Ok(streams_wire::get_shard_iterator_response(&token))
}

fn parse_seq(sequence_number: Option<&str>) -> Result<u64, WireError> {
    let s = sequence_number.ok_or_else(|| WireError::validation("missing `SequenceNumber`"))?;
    streams_wire::parse_sequence_number(s)
}

// --- GetRecords ------------------------------------------------------------

/// `GetRecords` (ADR 0042 §7/§9/§10): resolves the shard id against the
/// catalog **fresh at serve time** — see the module doc's "The two
/// `GetRecords` serve paths" for the sealed/open split this enables.
async fn get_records(
    ctx: &ClientCtx,
    shard_iterator: &str,
    limit: Option<usize>,
) -> Result<String, WireError> {
    let (label, shard_id, position) = streams_wire::decode_iterator(shard_iterator)?;
    let (tablet_raw, epoch) = streams_wire::parse_shard_id(&shard_id).ok_or_else(|| {
        WireError::validation(format!("malformed ShardId in iterator `{shard_id}`"))
    })?;
    let tablet = TabletId(tablet_raw);
    let limit = limit
        .unwrap_or(DEFAULT_GET_RECORDS_LIMIT)
        .clamp(1, MAX_GET_RECORDS_LIMIT);
    let meta = ctx.effective_metadata();

    if let Some(row) = meta.stream_shards.get(&(tablet, epoch)).cloned() {
        resolve_label(&meta, &row.table, &label)?;
        return get_records_sealed(ctx, &meta, &shard_id, &row, position, limit).await;
    }

    let table = meta
        .tablets
        .get(&tablet)
        .and_then(|t| t.table.clone())
        .ok_or_else(|| trimmed_data_access(&format!("shard `{shard_id}`")))?;
    let enabled = resolve_label(&meta, &table, &label)?;
    if !enabled || current_open_epoch(&meta, tablet) != epoch {
        return Err(trimmed_data_access(&format!("shard `{shard_id}`")));
    }
    get_records_open(
        ctx, &meta, &table, &label, tablet, &shard_id, position, limit,
    )
    .await
}

/// Whether a decoded change record must never surface on the Streams read
/// path: the ADR 0045 §2 backfill seeder's synthetic dirty marker (`seeded`,
/// follow-up "E1" — real DynamoDB emits **no** stream event for a GSI
/// backfill's own coverage sweep over pre-existing data), or an ADR 0049 §1
/// image-less marker record (`marker` — written before this table had a
/// stream at all; a stream begins at enable, never retroactively, so a
/// marker-era record sealed into a later shard is history the stream never
/// promised). Both `GetRecords` serve branches below call this one shared
/// predicate (`ChangeRecord::consumer_hidden`) rather than each growing its
/// own copy of the same check (this codebase's own "one function, not two
/// that happen to agree today" discipline).
fn consumer_hidden(record: &ChangeRecord) -> bool {
    record.consumer_hidden()
}

/// The **sealed**-shard `GetRecords` path (ADR 0042 §9/§10, ADR 0043 §A7b):
/// any node fetches via the row's own recorded `replicas`, slices to the
/// committed `hlc_range` (never trusting the raw object), filters/pages,
/// and nulls `NextShardIterator` only once the sliced content is truly
/// exhausted.
#[allow(clippy::too_many_arguments)]
async fn get_records_sealed(
    ctx: &ClientCtx,
    meta: &Metadata,
    shard_id: &str,
    row: &StreamShardRow,
    position: u64,
    limit: usize,
) -> Result<String, WireError> {
    // Ledger-named-object amendment (ADR 0042 §10/ADR 0043 §A3): resolve
    // the id from the row itself, never recompute `segment_id` — the
    // row's `object_id` is the only id this shard's winning bytes ever
    // actually lived at.
    let seg_id = row.object_id.as_str();
    let bytes = ctx
        .data()
        .segment_store
        .get_sealed(&row.replicas, seg_id)
        .await
        .map_err(|e| internal(&format!("segment store get of {seg_id:?}: {e}")))?;
    let Some(bytes) = bytes else {
        return Err(trimmed_data_access(&format!("shard `{shard_id}`")));
    };
    let (_, records) = segment::decode_and_slice(&bytes, row.hlc_range)
        .map_err(|e| internal(&format!("corrupt segment {seg_id:?}: {e}")))?;
    let page: Vec<_> = records
        .into_iter()
        .filter(|r| r.packed_hlc > position)
        .take(limit)
        .collect();

    let exhausted = match page.last() {
        Some(last) => last.packed_hlc >= row.hlc_range.1,
        None => position >= row.hlc_range.1,
    };
    let next_iterator = (!exhausted).then(|| {
        let next_position = page.last().map_or(position, |r| r.packed_hlc);
        streams_wire::encode_iterator(&row.label, shard_id, next_position)
    });

    let dyn_schema = schema_for(meta, &row.table);
    let json_records: Vec<_> = page
        .iter()
        .filter_map(|r| {
            let record = ChangeRecord::decode(&r.change_record)?;
            if consumer_hidden(&record) {
                return None;
            }
            Some(streams_wire::stream_record_json(
                shard_id,
                r.packed_hlc,
                &record,
                row.view_type,
                &dyn_schema.partition_key,
                dyn_schema.sort_key.as_deref(),
                hlc::unpack(r.packed_hlc).wall_ms,
            ))
        })
        .collect();
    Ok(streams_wire::get_records_response(
        json_records,
        next_iterator.as_deref(),
    ))
}

/// The **open**-shard `GetRecords` path (ADR 0042 §7/§8): forwards to the
/// tablet's own leader (`ClientCtx::read_stream_hot_records`, no `ReadIndex`
/// barrier) and never nulls the iterator — an empty poll returns the
/// **same** position (F4/§7: "not there yet, poll again").
#[allow(clippy::too_many_arguments)]
async fn get_records_open(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    label: &str,
    tablet: TabletId,
    shard_id: &str,
    position: u64,
    limit: usize,
) -> Result<String, WireError> {
    let pairs = ctx
        .read_stream_hot_records(tablet, position, limit)
        .await
        .map_err(|e| internal(&e))?;
    let next_position = pairs
        .last()
        .and_then(|(key, _)| record_hlc_suffix(key))
        .unwrap_or(position);
    let next_iterator = streams_wire::encode_iterator(label, shard_id, next_position);

    let dyn_schema = schema_for(meta, table);
    let view_type = meta
        .stream_view_type(table, label)
        .unwrap_or(StreamViewType::NewAndOldImages);
    let json_records: Vec<_> = pairs
        .iter()
        .filter_map(|(key, value)| {
            let packed = record_hlc_suffix(key)?;
            let record = ChangeRecord::decode(value)?;
            if consumer_hidden(&record) {
                return None;
            }
            Some(streams_wire::stream_record_json(
                shard_id,
                packed,
                &record,
                view_type,
                &dyn_schema.partition_key,
                dyn_schema.sort_key.as_deref(),
                hlc::unpack(packed).wall_ms,
            ))
        })
        .collect();
    Ok(streams_wire::get_records_response(
        json_records,
        Some(&next_iterator),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_control::{ApplyOutcome, MetaCommand, StreamSpec};
    use animus_tablet::KeyRange;

    fn base_meta() -> Metadata {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "t".into(),
                schema: animus_control::TableSchema::simple(
                    "id",
                    animus_control::ColumnType::String
                ),
            }),
            ApplyOutcome::Applied
        );
        m
    }

    fn enable(m: &mut Metadata, label: &str) {
        let _ = m.apply(&MetaCommand::SetTableStream {
            table: "t".into(),
            spec: Some(StreamSpec {
                view_type: StreamViewType::NewAndOldImages,
                label: label.into(),
            }),
        });
    }

    fn disable(m: &mut Metadata) {
        let _ = m.apply(&MetaCommand::SetTableStream {
            table: "t".into(),
            spec: None,
        });
    }

    fn seal(m: &mut Metadata, label: &str, tablet: TabletId, epoch: u64, end: u64) {
        let _ = m.apply(&MetaCommand::SealStreamShard {
            table: "t".into(),
            label: label.into(),
            tablet,
            epoch,
            view_type: StreamViewType::NewAndOldImages,
            hlc_range: (end.saturating_sub(100), end),
            count: 1,
            seal_wall_ms: 0,
            replicas: Vec::new(),
            object_id: format!("t/{label}/{}/{epoch}/test", tablet.0),
            expected_range: KeyRange::whole(),
        });
    }

    #[test]
    fn resolve_label_accepts_the_current_enabled_label() {
        let mut m = base_meta();
        enable(&mut m, "L1");
        assert_eq!(resolve_label(&m, "t", "L1"), Ok(true));
    }

    #[test]
    fn resolve_label_accepts_a_disabled_but_unreaped_label() {
        let mut m = base_meta();
        enable(&mut m, "L1");
        let _ = m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("t".into()),
            range: KeyRange::whole(),
            replicas: Vec::new(),
        });
        seal(&mut m, "L1", TabletId(1), 0, 100);
        disable(&mut m);
        // F12-b: the label's still-live catalog row licenses the grace
        // window even though the schema no longer names it.
        assert_eq!(resolve_label(&m, "t", "L1"), Ok(false));
    }

    #[test]
    fn resolve_label_rejects_a_label_with_no_current_or_catalog_claim() {
        let m = base_meta();
        let err = resolve_label(&m, "t", "never-existed").unwrap_err();
        assert_eq!(err.code, "ResourceNotFoundException");
    }

    #[test]
    fn resolve_label_rejects_once_the_hand_built_expired_state_has_no_rows_left() {
        // Not a real retention sweep (PR7's own job) — just proves the
        // label-resolution branch that a genuine expiry will eventually
        // trigger: once every catalog row for a label is gone AND it is no
        // longer the current schema label, resolution must fail exactly
        // like it never existed, not silently pass.
        let mut m = base_meta();
        enable(&mut m, "L1");
        let _ = m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("t".into()),
            range: KeyRange::whole(),
            replicas: Vec::new(),
        });
        seal(&mut m, "L1", TabletId(1), 0, 100);
        disable(&mut m);
        assert_eq!(resolve_label(&m, "t", "L1"), Ok(false)); // still in grace
        let _ = m.apply(&MetaCommand::ExpireStreamShards {
            rows: vec![(TabletId(1), 0)],
            remove: true,
        });
        let err = resolve_label(&m, "t", "L1").unwrap_err();
        assert_eq!(err.code, "ResourceNotFoundException");
    }

    #[test]
    fn current_open_epoch_counts_the_chain_length() {
        let mut m = base_meta();
        enable(&mut m, "L1");
        assert_eq!(current_open_epoch(&m, TabletId(1)), 0);
        seal(&mut m, "L1", TabletId(1), 0, 100);
        assert_eq!(current_open_epoch(&m, TabletId(1)), 1);
        seal(&mut m, "L1", TabletId(1), 1, 200);
        assert_eq!(current_open_epoch(&m, TabletId(1)), 2);
    }

    #[test]
    fn record_hlc_suffix_recovers_the_trailing_big_endian_hlc() {
        let mut key = vec![1, 2, 3];
        key.extend_from_slice(&42u64.to_be_bytes());
        assert_eq!(record_hlc_suffix(&key), Some(42));
        assert_eq!(
            record_hlc_suffix(&[1, 2, 3]),
            None,
            "too short to hold an HLC suffix"
        );
    }
}
