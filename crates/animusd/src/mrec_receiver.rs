//! The MREC **receiver** (ADR 0075 section 4, G-01 stage G-d M3): what a data
//! node does with an [`MrecApplyRequest`] a peer cluster's shipper sent it.
//!
//! # Routing (plan Q3)
//!
//! Any data node of the receiving cluster accepts the frame. It groups the
//! records by **its own** tablet layout (the two clusters' layouts are
//! independent: a table splits differently on each side), and for each
//! destination tablet proposes **one `KindEvalBatch` of `Replicate` ops**
//! through the ordinary leader path (`ClientCtx::cp_kind_write_batch`: local
//! leader or hinted forward with retry/election wait, one hop). It answers
//! only after every group has committed, one verdict per record, in order:
//!
//! - `Applied`: the record won last-writer-wins and is committed;
//! - `Superseded`: the stored stamp is equal or newer (a loss, or an
//!   idempotent re-delivery); terminal, nothing was written;
//! - `Retry`: not applied *yet* and safe to resend (a stamp too far ahead of
//!   local time, a live transaction intent on the key, throttling/overload,
//!   leadership churn, an ambiguous confirm);
//! - `Rejected`: malformed for good (key attributes disagree with the schema,
//!   a stamp the sender does not own).
//!
//! # Why a lost or ambiguous confirm is `Retry`, and confirm is own-entry
//!
//! A replicate is classed `ProbeIdentity::RequiresOwnEntry` (see
//! `dynamo::kind_write_is_idempotent`): value equality would not prove *this*
//! entry won. When the leader-local per-item result is lost the whole group is
//! an ambiguous-confirm error, which maps to `Retry` for its records: resending
//! is safe because applying a replicate is idempotent *as state* (LWW, equal
//! stamp is a no-op). `ConditionFailed` on a replicate can only mean a foreign
//! transaction intent sits on the key (a replicate carries no condition), which
//! is also `Retry`.
//!
//! # The skew bound
//!
//! A record whose stamp's wall part exceeds `env.wall_now() +
//! mrec_max_clock_skew_ms` is answered `Retry` and counted
//! (`mrec_skew_rejected_total`); the shipper resends until local time catches
//! up, so a fast-clocked region cannot plant a far-future stamp that wins every
//! conflict forever. The check is made once, here, at the node that accepts the
//! frame and proposes the batch, and the verdict rides the entry as a pure
//! value: apply never consults a clock (it stays a pure function of the entry
//! and engine state).
//!
//! # Overload
//!
//! At most [`MREC_MAX_INFLIGHT_APPLIES`] batches run concurrently per node;
//! beyond that every record is answered `Retry` (no ack, no cursor move on the
//! sender), so a peer flood cannot starve this cluster's own traffic.
//!
//! The handler is generic over `E: Env`/`R: RelayClient` so `SimWorld` drives
//! the very same code through its `PeerBridge`.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use animus_control::mrec_region_id;
use animus_control::version::Gate;
use animus_dynamo::AttributeValue;
use animus_env::{Env, Metric};
use animus_node::host::RelayClient;
use animus_node::{
    KindWriteBatchItem, KindWriteOp, MREC_PROTO, MrecAnswer, MrecApplyRequest, MrecApplyResponse,
    MrecRecord,
};
use animus_tablet::TabletId;

use crate::dynamo::{self, KindWriteOutcome};
use crate::mrec_peer::{INSECURE_PEER_REFUSAL, MREC_MAX_INFLIGHT_APPLIES, MrecConfig};
use crate::{ClientCtx, topology};

fn refused(message: impl Into<String>, retryable: bool) -> MrecApplyResponse {
    MrecApplyResponse::Refused {
        message: message.into(),
        retryable,
    }
}

/// Releases one in-flight slot on drop (also when the handler future is
/// cancelled, e.g. the peer hung up).
struct InflightGuard<'a>(&'a std::sync::atomic::AtomicUsize);

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Whether `value` is the partition/sort key attribute `name` of `item`.
fn key_matches(item: &animus_dynamo::Item, name: &str, value: Option<&AttributeValue>) -> bool {
    match value {
        Some(v) => item.get(name) == Some(v),
        None => !item.contains_key(name),
    }
}

/// Validate one record against the table's key schema and the sender's
/// identity; `Err` is the `Rejected` message.
fn validate_record(
    schema: &animus_dynamo::TableSchema,
    sender_id: u32,
    rec: &MrecRecord,
) -> Result<(), String> {
    if rec.ver == animus_item::MrecVersion::ZERO {
        return Err("a replicated record needs a non-zero stamp".into());
    }
    if rec.ver.region_id != sender_id {
        return Err(format!(
            "stamp region id {} is not the sender's own ({sender_id}); only a region's own \
             originations are replicated",
            rec.ver.region_id
        ));
    }
    if rec.sk.is_some() != schema.sort_key.is_some() {
        return Err("sort key presence does not match the table's key schema".into());
    }
    if let Some(item) = &rec.item
        && !(key_matches(item, &schema.partition_key, Some(&rec.pk))
            && match &schema.sort_key {
                Some(sk) => key_matches(item, sk, rec.sk.as_ref()),
                None => true,
            })
    {
        return Err("item key attributes disagree with the record's key".into());
    }
    Ok(())
}

/// Serve one [`MrecApplyRequest`]; see the module doc.
pub(crate) async fn handle_mrec_apply<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    cfg: &MrecConfig,
    req: MrecApplyRequest,
) -> MrecApplyResponse {
    if req.proto != MREC_PROTO {
        return refused(
            format!(
                "unsupported MREC protocol {} (this node speaks {MREC_PROTO})",
                req.proto
            ),
            false,
        );
    }
    if !cfg.transport_allowed(ctx.tls.is_some()) {
        return refused(INSECURE_PEER_REFUSAL, false);
    }
    if !ctx.edge.version().features.is_open(Gate::MrecReplication) {
        return refused(
            "MREC replication is not enabled on this cluster yet (cluster version below the \
             MrecReplication gate)",
            true,
        );
    }
    if cfg.region.is_none() {
        return refused(
            "this cluster has no cluster_settings.region configured",
            false,
        );
    }
    if !cfg.is_peer(&req.from_region) {
        return refused(
            format!("region `{}` is not a configured peer", req.from_region),
            false,
        );
    }
    // Shed, never queue (see the module doc's overload section).
    let inflight = cfg.inflight.fetch_add(1, Ordering::AcqRel) + 1;
    let _guard = InflightGuard(&cfg.inflight);
    if inflight > MREC_MAX_INFLIGHT_APPLIES {
        return MrecApplyResponse::Answers(vec![MrecAnswer::Retry; req.records.len()]);
    }

    let meta = ctx.effective_metadata();
    let Some(spec) = meta.table_global(&req.table).filter(|g| g.is_mrec()) else {
        // The table may simply not have reached this node's metadata view yet
        // (the replica-create saga, M4): retryable.
        return refused(
            format!("table `{}` is not an MREC global table here", req.table),
            true,
        );
    };
    if !spec
        .replicas
        .iter()
        .any(|r| !r.local && r.region == req.from_region)
    {
        return refused(
            format!(
                "table `{}` is not replicated with region `{}` here",
                req.table, req.from_region
            ),
            true,
        );
    }

    let schema = dynamo::schema_for(&meta, &req.table);
    let sender_id = mrec_region_id(&req.from_region);
    let now_ms = ctx.env.wall_now().0;
    let skew_limit = now_ms.saturating_add(cfg.max_clock_skew_ms);

    let mut answers: Vec<Option<MrecAnswer>> = vec![None; req.records.len()];
    // Destination tablet -> (record index, item), in record order.
    let mut groups: BTreeMap<TabletId, Vec<(usize, KindWriteBatchItem)>> = BTreeMap::new();
    for (i, rec) in req.records.iter().enumerate() {
        if let Err(message) = validate_record(&schema, sender_id, rec) {
            answers[i] = Some(MrecAnswer::Rejected { message });
            continue;
        }
        if rec.ver.wall_ms > skew_limit {
            ctx.env.metrics().incr(Metric::MrecSkewRejectedTotal);
            answers[i] = Some(MrecAnswer::Retry);
            continue;
        }
        let base_key = dynamo::item_key(&rec.pk, rec.sk.as_ref());
        let Some(tablet) = topology::tablet_for_key(meta.tablets_for_table(&req.table), &base_key)
        else {
            answers[i] = Some(MrecAnswer::Retry);
            continue;
        };
        groups.entry(tablet).or_default().push((
            i,
            KindWriteBatchItem {
                pk: rec.pk.clone(),
                sk: rec.sk.clone(),
                op: KindWriteOp::Replicate {
                    item: rec.item.clone(),
                    ver: rec.ver,
                },
                condition: None,
            },
        ));
    }

    // One `KindEvalBatch` per destination tablet, through the normal leader
    // path (hinted forward / election wait inside `cp_kind_write_batch`).
    // Sequential: deterministic, and the sender acks nothing until all commit.
    for (_tablet, group) in groups {
        let (indexes, items): (Vec<usize>, Vec<KindWriteBatchItem>) = group.into_iter().unzip();
        let results = ctx.cp_kind_write_batch(&meta, &req.table, items).await;
        for (i, result) in indexes.into_iter().zip(results) {
            answers[i] = Some(match result {
                Ok(KindWriteOutcome::Ok { .. }) => MrecAnswer::Applied,
                Ok(KindWriteOutcome::Superseded) => MrecAnswer::Superseded,
                // A replicate carries no condition: this is "a foreign
                // transaction intent holds the key" (M2), safe to resend.
                Ok(KindWriteOutcome::ConditionFailed) => MrecAnswer::Retry,
                // A deterministic evaluation refusal is final; everything else
                // (throttling, unavailability, churn, an ambiguous confirm) is
                // retried — a replicate is idempotent as state.
                Err(e) if e.code == "ValidationException" => {
                    MrecAnswer::Rejected { message: e.message }
                }
                Err(_) => MrecAnswer::Retry,
            });
        }
    }

    MrecApplyResponse::Answers(
        answers
            .into_iter()
            .map(|a| a.unwrap_or(MrecAnswer::Retry))
            .collect(),
    )
}
