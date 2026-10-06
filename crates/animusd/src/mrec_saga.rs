//! The MREC **replica lifecycle** (ADR 0075 sections 4.3/5.1, G-01 stage G-d
//! M4b): the `UpdateTable` `ReplicaUpdates` wire edge, the create/delete saga
//! the owning cluster drives, and the peer-side handler of the
//! [`MrecControl`] messages it sends.
//!
//! # The wire edge ([`update_table_mrec`])
//!
//! Synchronous and purely local: it validates, forces the stream to
//! `NEW_AND_OLD_IMAGES` (V13), converts the table (`ConvertTableToMrec`) and
//! records each new peer as a `Creating` replica (`AddMrecReplica`); a
//! `Delete` flips the replica to `Deleting`. Nothing here talks to a peer: the
//! saga does, so the call returns without waiting on a remote cluster.
//!
//! # The saga ([`mrec_saga_table`])
//!
//! Convergent and idempotent, run beside the shipper (`mrec_ship_loop`) on
//! every node but acting only on the node that **leads the table's
//! lowest-id active tablet** (a deterministic single driver; a failover just
//! moves it, and every step is safe to repeat):
//!
//! - `Creating`: until the first tablet's copy is recorded, send the peer
//!   [`MrecControl::CreateReplica`] (it creates, or adopts, its table and
//!   replica set) and tell every other replica [`MrecControl::AddPeer`]. The
//!   shipper's scan mode does the copy and proposes `MarkMrecCopied` per
//!   tablet; once every active tablet is recorded the saga sets the replica
//!   `Active`. A non-retryable peer refusal sets `CreationFailed`.
//! - `Deleting`: the tablet leaders drop the peer's cursors (the shipper
//!   tick), the saga notifies the peer ([`MrecControl::Leave`], best effort:
//!   it gives up after [`LEAVE_ATTEMPTS`] tries) and, after a short grace for
//!   the cursor drop, removes the replica (`RemoveMrecReplica`).
//!
//! # The peer side ([`handle_control`])
//!
//! `CreateReplica` creates the table through the ordinary `CreateTable` wire
//! path (same key schema and indexes, a NEW_AND_OLD_IMAGES stream, the TTL
//! attribute), or **adopts** an existing table of identical key schema and
//! indexes (its rows then merge by last-writer-wins: the peer's own shipper
//! scans them out). It converts, and adds the sender (and any third region
//! named) as `Active` replicas: the peer's shipper starts a scan for each, so
//! the full mesh is seeded by the same mechanism as the initial copy. A
//! table of a different shape is refused for good, which the owner shows as
//! `CREATION_FAILED`.

use std::time::Duration;

use animus_control::schema::{GlobalTableSpec, MrecReplicaStatus, StreamViewType, mrec_region_id};
use animus_control::version::Gate;
use animus_control::{MetaCommand, Metadata};
use animus_dynamo::global::{GlobalTableUpdate, MrecRequest};
use animus_dynamo::wire::WireError;
use animus_env::Env;
use animus_node::host::RelayClient;
use animus_node::{MREC_PROTO, MrecApplyRequest, MrecApplyResponse, MrecControl};
use serde_json::{Map, Value, json};

use crate::ClientCtx;
use crate::dynamo::{
    SCHEMA_COMMIT_TIMEOUT, SCHEMA_POLL_INTERVAL, describe_table_wrapped, enable_stream, internal,
    table_status,
};
use crate::mrec_peer::{HealthKey, PeerClient, PeerError};
use crate::mrec_shipper::clear_peer_cursors;

/// How long one control message waits for the peer (a `CreateTable` on the
/// far side is included).
const CONTROL_TIMEOUT: Duration = Duration::from_secs(20);
/// `Leave` attempts before the saga stops waiting for the peer.
const LEAVE_ATTEMPTS: u32 = 3;
/// After a `Deleting` replica is first seen, how long the tablet leaders get
/// to drop its cursors before the replica row is removed.
const DELETE_GRACE: Duration = Duration::from_secs(2);
/// The health-map tablet slot the saga's own per-(table, region) memo uses.
const SAGA_SLOT: u64 = u64::MAX;

fn gate_open<E: Env, R: RelayClient>(ctx: &ClientCtx<E, R>, gate: Gate) -> bool {
    let features = &ctx.edge.version().features;
    if !features.is_open(gate) {
        features.update(&ctx.effective_metadata());
    }
    features.is_open(gate)
}

/// Whether the MREC gate is open, re-reading this node's applied view first
/// (the emit-site check, as in `global_tables`).
pub(crate) fn mrec_gate_open<E: Env, R: RelayClient>(ctx: &ClientCtx<E, R>) -> bool {
    gate_open(ctx, Gate::MrecReplication)
}

/// Propose `cmd` until `done` holds on a fresh view (or time out).
async fn commit<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    cmd: MetaCommand,
    done: impl Fn(&Metadata) -> bool,
) -> Result<Metadata, WireError> {
    let deadline = ctx.env.now().saturating_add(SCHEMA_COMMIT_TIMEOUT);
    loop {
        ctx.propose_schema(&cmd).await;
        let fresh = ctx.metadata_fresh().await;
        if done(&fresh) {
            return Ok(fresh);
        }
        if ctx.env.now() >= deadline {
            return Err(internal(
                "a multi-Region replica change did not commit to the control plane in time (no \
                 leader reachable, or the cluster refused it)",
            ));
        }
        ctx.env.sleep(SCHEMA_POLL_INTERVAL).await;
    }
}

fn mrec_spec<'a>(meta: &'a Metadata, table: &str) -> Option<&'a GlobalTableSpec> {
    meta.table_global(table).filter(|g| g.is_mrec())
}

/// Force the stream (V13): enable `NEW_AND_OLD_IMAGES` if absent, reject any
/// other view type.
async fn ensure_stream<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    meta: &Metadata,
    table: &str,
) -> Result<(), WireError> {
    match meta.table_stream(table) {
        Some(s) if s.view_type == StreamViewType::NewAndOldImages => Ok(()),
        Some(s) => Err(WireError::validation(format!(
            "UpdateTable: table `{table}` has a stream with view type {:?}; a multi-Region \
             eventually consistent table requires StreamViewType NEW_AND_OLD_IMAGES (disable the \
             stream first)",
            s.view_type
        ))),
        None => enable_stream(ctx, table, StreamViewType::NewAndOldImages)
            .await
            .map(|_| ()),
    }
}

/// `UpdateTable` `ReplicaUpdates` with `MultiRegionConsistency` absent or
/// `EVENTUAL`, gate open: see the module doc.
///
/// # Errors
/// The named `ValidationException`s of ADR 0075 section 5.1.
pub(crate) async fn update_table_mrec<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    meta: &Metadata,
    table: &str,
    update: &GlobalTableUpdate,
) -> Result<String, WireError> {
    let request: MrecRequest = update.validate_mrec()?;
    let cfg = &ctx.mrec;
    let Some(local) = cfg.region.clone() else {
        return Err(WireError::validation(
            "UpdateTable: this cluster has no cluster_settings.region configured, so it cannot \
             take part in a multi-Region eventually consistent table",
        ));
    };
    if table_status(meta, table) != "ACTIVE" {
        return Err(WireError::validation(format!(
            "UpdateTable: table `{table}` is not ACTIVE (a table that is still being restored or \
             imported cannot become a multi-Region table)"
        )));
    }
    let existing = mrec_spec(meta, table);
    if meta.table_global(table).is_some() && existing.is_none() {
        return Err(WireError::validation(format!(
            "UpdateTable: table `{table}` is already a global table of another kind"
        )));
    }
    if request.deletes.is_empty() {
        let known = existing.map_or(1, |s| s.replicas.len());
        if known + request.creates.len() > GlobalTableSpec::MREC_MAX_REPLICAS {
            return Err(WireError::validation(format!(
                "UpdateTable: a multi-Region eventually consistent table has at most {} \
                 replicas (this request would make {})",
                GlobalTableSpec::MREC_MAX_REPLICAS,
                known + request.creates.len()
            )));
        }
        for region in &request.creates {
            if *region == local {
                return Err(WireError::validation(format!(
                    "UpdateTable: Region `{region}` is this cluster's own Region and cannot be \
                     named in ReplicaUpdates Create"
                )));
            }
            if !cfg.is_peer(region) {
                let peers: Vec<&str> = cfg.peers.iter().map(|p| p.region.as_str()).collect();
                return Err(WireError::validation(format!(
                    "UpdateTable: Region `{region}` is not a configured peer of this cluster \
                     (configured peers: {})",
                    peers.join(", ")
                )));
            }
            if existing.is_some_and(|s| s.replicas.iter().any(|r| r.region == *region)) {
                return Err(WireError::validation(format!(
                    "UpdateTable: table `{table}` already has a replica in Region `{region}`"
                )));
            }
        }
        ensure_stream(ctx, meta, table).await?;
        if existing.is_none() {
            let (t, l) = (table.to_owned(), local.clone());
            commit(
                ctx,
                MetaCommand::ConvertTableToMrec {
                    table: table.to_owned(),
                    local_region: local.clone(),
                    region_id: mrec_region_id(&local),
                },
                move |m| {
                    mrec_spec(m, &t)
                        .is_some_and(|s| s.replicas.iter().any(|r| r.local && r.region == l))
                },
            )
            .await?;
        }
        for region in &request.creates {
            let (t, r) = (table.to_owned(), region.clone());
            commit(
                ctx,
                MetaCommand::AddMrecReplica {
                    table: table.to_owned(),
                    region: region.clone(),
                    region_id: mrec_region_id(region),
                },
                move |m| mrec_spec(m, &t).is_some_and(|s| s.replicas.iter().any(|x| x.region == r)),
            )
            .await?;
        }
    } else {
        let Some(spec) = existing else {
            return Err(WireError::validation(format!(
                "UpdateTable: table `{table}` is not a multi-Region eventually consistent table, \
                 so it has no replica to delete"
            )));
        };
        for region in &request.deletes {
            let Some(replica) = spec.replicas.iter().find(|r| r.region == *region) else {
                return Err(WireError::validation(format!(
                    "UpdateTable: Region `{region}` is not a replica of table `{table}`"
                )));
            };
            if replica.local {
                return Err(WireError::validation(format!(
                    "UpdateTable: Region `{region}` is this cluster's own Region; delete this \
                     table's replicas from the other side instead"
                )));
            }
        }
        for region in &request.deletes {
            let (t, r) = (table.to_owned(), region.clone());
            commit(
                ctx,
                MetaCommand::SetMrecReplicaStatus {
                    table: table.to_owned(),
                    region: region.clone(),
                    status: MrecReplicaStatus::Deleting,
                },
                move |m| {
                    mrec_spec(m, &t).is_none_or(|s| {
                        s.replicas
                            .iter()
                            .find(|x| x.region == r)
                            .is_none_or(|x| x.status == MrecReplicaStatus::Deleting)
                    })
                },
            )
            .await?;
        }
    }
    let fresh = ctx.metadata_fresh().await;
    describe_table_wrapped(&fresh, table, "TableDescription")
}

// ---------------------------------------------------------------------------
// The saga (owner side)

/// The `CreateTable` body a peer needs to hold this table's twin: key schema,
/// attribute definitions, indexes (name, keys, projection), on-demand billing
/// (capacity is per region) and the forced stream.
fn create_table_body(meta: &Metadata, table: &str) -> Option<String> {
    let described = describe_table_wrapped(meta, table, "Table").ok()?;
    let v: Value = serde_json::from_str(&described).ok()?;
    let t = v.get("Table")?;
    let mut body = Map::new();
    body.insert("TableName".into(), json!(table));
    body.insert(
        "AttributeDefinitions".into(),
        t.get("AttributeDefinitions")?.clone(),
    );
    body.insert("KeySchema".into(), t.get("KeySchema")?.clone());
    for key in ["GlobalSecondaryIndexes", "LocalSecondaryIndexes"] {
        if let Some(list) = t.get(key).and_then(Value::as_array) {
            let slim: Vec<Value> = list
                .iter()
                .map(|i| {
                    json!({
                        "IndexName": i.get("IndexName"),
                        "KeySchema": i.get("KeySchema"),
                        "Projection": i.get("Projection"),
                    })
                })
                .collect();
            if !slim.is_empty() {
                body.insert(key.into(), Value::Array(slim));
            }
        }
    }
    body.insert("BillingMode".into(), json!("PAY_PER_REQUEST"));
    body.insert(
        "StreamSpecification".into(),
        json!({"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}),
    );
    serde_json::to_string(&Value::Object(body)).ok()
}

/// The parts of a `CreateTable` body that must agree for a peer to adopt an
/// existing table: key schema, the attribute types the keys use, and every
/// index's name, keys and projection.
fn shape_of(body: &Value) -> Value {
    let mut attrs: Vec<Value> = body
        .get("AttributeDefinitions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    attrs.sort_by_key(|a| a.to_string());
    let mut indexes: Vec<Value> = ["GlobalSecondaryIndexes", "LocalSecondaryIndexes"]
        .iter()
        .filter_map(|k| body.get(*k).and_then(Value::as_array))
        .flatten()
        .map(|i| {
            json!({
                "n": i.get("IndexName"),
                "k": i.get("KeySchema"),
                "p": i.get("Projection"),
            })
        })
        .collect();
    indexes.sort_by_key(|i| i.to_string());
    json!({"k": body.get("KeySchema"), "a": attrs, "i": indexes})
}

async fn send_control<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    client: &dyn PeerClient,
    region: &str,
    table: &str,
    control: MrecControl,
) -> Result<MrecApplyResponse, String> {
    let (Some(from_region), Some(idx)) = (ctx.mrec.region.clone(), ctx.mrec.peer_index(region))
    else {
        return Err(format!("region `{region}` is not a configured peer"));
    };
    let payload = serde_json::to_vec(&MrecApplyRequest {
        proto: MREC_PROTO,
        from_region,
        table: table.to_owned(),
        records: Vec::new(),
        control: Some(control),
    })
    .map_err(|e| e.to_string())?;
    let bytes = client
        .call(idx, payload, CONTROL_TIMEOUT)
        .await
        .map_err(|e| match e {
            PeerError::Timeout => "peer did not answer in time".to_owned(),
            PeerError::Transport(m) | PeerError::Refused(m) => m,
        })?;
    crate::mrec_peer::decode_response(&bytes).map_err(|e| format!("undecodable peer reply: {e:?}"))
}

/// One saga pass for `table` on this node; see the module doc. Returns
/// `true` when it did something (a message sent or a command proposed).
pub(crate) async fn mrec_saga_table<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    table: &str,
    client: &dyn PeerClient,
) -> bool {
    if !mrec_gate_open(ctx) {
        return false;
    }
    let meta = ctx.effective_metadata();
    let Some(spec) = mrec_spec(&meta, table) else {
        return false;
    };
    // The single driver: the leader of the table's lowest-id active tablet.
    let active: Vec<u64> = meta
        .tablets_for_table(table)
        .filter(|(_, t)| t.state == animus_tablet::TabletState::Active)
        .map(|(id, _)| id.0)
        .collect();
    let Some(first) = active.iter().min().copied() else {
        return false;
    };
    let leads_first = ctx
        .edge
        .hosted_groups()
        .iter()
        .any(|(t, g)| t.0 == first && g.is_leader());
    if !leads_first {
        return false;
    }
    let mut did = push_ttl(ctx, client, &meta, spec, table).await;
    if spec.replicas.iter().all(|r| {
        r.local
            || r.status == MrecReplicaStatus::Active
            || r.status == MrecReplicaStatus::CreationFailed
    }) {
        return did;
    }
    for replica in spec.replicas.iter().filter(|r| !r.local) {
        match replica.status {
            MrecReplicaStatus::Creating => {
                if replica.copied.is_empty() {
                    did |= step_creating(ctx, client, &meta, spec, table, &replica.region).await;
                }
                if active.iter().all(|t| replica.copied.contains(t)) {
                    ctx.propose_schema(&MetaCommand::SetMrecReplicaStatus {
                        table: table.to_owned(),
                        region: replica.region.clone(),
                        status: MrecReplicaStatus::Active,
                    })
                    .await;
                    did = true;
                }
            }
            MrecReplicaStatus::Deleting => {
                did |= step_deleting(ctx, client, table, &replica.region).await;
            }
            MrecReplicaStatus::Active | MrecReplicaStatus::CreationFailed => {}
        }
    }
    did
}

async fn step_creating<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    client: &dyn PeerClient,
    meta: &Metadata,
    spec: &GlobalTableSpec,
    table: &str,
    region: &str,
) -> bool {
    let Some(create_table) = create_table_body(meta, table) else {
        return false;
    };
    let others: Vec<String> = spec
        .replicas
        .iter()
        .filter(|r| !r.local && r.region != region && r.status != MrecReplicaStatus::Deleting)
        .map(|r| r.region.clone())
        .collect();
    let ttl_attribute = meta.table_ttl(table).map(|t| t.attribute_name.clone());
    let reply = send_control(
        ctx,
        client,
        region,
        table,
        MrecControl::CreateReplica {
            create_table,
            ttl_attribute,
            peers: others.clone(),
        },
    )
    .await;
    match reply {
        Ok(MrecApplyResponse::Done) => {
            for other in others {
                let _ = send_control(
                    ctx,
                    client,
                    &other,
                    table,
                    MrecControl::AddPeer {
                        region: region.to_owned(),
                    },
                )
                .await;
            }
        }
        Ok(MrecApplyResponse::Refused {
            message,
            retryable: false,
        }) => {
            tracing::warn!(table, region, %message, "mrec replica creation refused by the peer");
            ctx.propose_schema(&MetaCommand::SetMrecReplicaStatus {
                table: table.to_owned(),
                region: region.to_owned(),
                status: MrecReplicaStatus::CreationFailed,
            })
            .await;
        }
        // Retryable refusal, transport trouble, an unexpected reply: try again
        // on the next pass.
        Ok(_) | Err(_) => {}
    }
    true
}

async fn step_deleting<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    client: &dyn PeerClient,
    table: &str,
    region: &str,
) -> bool {
    let key: HealthKey = (table.to_owned(), SAGA_SLOT, region.to_owned());
    let now = ctx.env.now().0;
    let (notified, ready_at) = {
        let mut h = ctx.mrec.health.lock().expect("mrec health poisoned");
        let e = h.entry(key.clone()).or_default();
        if e.retry_after == 0 {
            e.retry_after = now + u64::try_from(DELETE_GRACE.as_nanos()).unwrap_or(u64::MAX);
        }
        (e.failures >= LEAVE_ATTEMPTS || e.caught_up, e.retry_after)
    };
    if !notified {
        let ok = matches!(
            send_control(ctx, client, region, table, MrecControl::Leave).await,
            Ok(MrecApplyResponse::Done | MrecApplyResponse::Refused { .. })
        );
        let mut h = ctx.mrec.health.lock().expect("mrec health poisoned");
        let e = h.entry(key.clone()).or_default();
        if ok {
            e.caught_up = true;
        } else {
            e.failures += 1;
        }
        return true;
    }
    if now < ready_at {
        return false;
    }
    ctx.propose_schema(&MetaCommand::RemoveMrecReplica {
        table: table.to_owned(),
        region: region.to_owned(),
    })
    .await;
    ctx.mrec
        .health
        .lock()
        .expect("mrec health poisoned")
        .remove(&key);
    true
}

/// A `Deleting` replica's cursors on one tablet this node leads: drop them
/// (the shipper tick calls this; idempotent).
pub(crate) async fn drop_deleting_cursors<E: Env>(group: &crate::CpGroup<E>, region: &str) {
    let start = group.scope_range().start;
    let mut present = false;
    for tag in [
        crate::mrec_shipper::cursor_tag(region),
        crate::mrec_shipper::scan_tag(region),
    ] {
        present |= group
            .local_get_kind(
                animus_cp_data::KIND_CURSOR,
                &animus_cp_data::cursor::cursor_key(&start, &tag),
            )
            .await
            .is_some();
    }
    if present {
        let _ = clear_peer_cursors(group, region).await;
    }
}

// ---------------------------------------------------------------------------
// The peer side

fn refused(message: impl Into<String>, retryable: bool) -> MrecApplyResponse {
    MrecApplyResponse::Refused {
        message: message.into(),
        retryable,
    }
}

/// Serve one [`MrecControl`] message from `from` (a configured peer, checked
/// by the caller). Idempotent.
pub(crate) async fn handle_control<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    from: &str,
    table: &str,
    control: MrecControl,
) -> MrecApplyResponse {
    match control {
        MrecControl::CreateReplica {
            create_table,
            ttl_attribute,
            peers,
        } => create_replica(ctx, from, table, &create_table, ttl_attribute, &peers).await,
        MrecControl::AddPeer { region } => add_peer(ctx, table, &region).await,
        MrecControl::Leave => leave(ctx, from, table).await,
        MrecControl::SetTtl { attribute } => set_ttl(ctx, from, table, attribute).await,
    }
}

/// Peer side of [`MrecControl::SetTtl`]: mirror the sender's TTL setting
/// (idempotent: already equal => nothing proposed).
async fn set_ttl<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    from: &str,
    table: &str,
    attribute: Option<String>,
) -> MrecApplyResponse {
    let r = set_ttl_inner(ctx, table, attribute.clone()).await;
    if r == MrecApplyResponse::Done {
        // The sender has this setting: never echo it back to it (an echo
        // arriving after a newer local change would revert that change).
        let key: HealthKey = (table.to_owned(), SAGA_SLOT, from.to_owned());
        let mut h = ctx.mrec.health.lock().expect("mrec health poisoned");
        h.entry(key).or_default().ttl_pushed = Some(attribute);
    }
    r
}

async fn set_ttl_inner<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    table: &str,
    attribute: Option<String>,
) -> MrecApplyResponse {
    let meta = ctx.metadata_fresh().await;
    if !meta.has_table_schema(table) {
        return refused(format!("table `{table}` does not exist here"), true);
    }
    let current = meta.table_ttl(table).map(|t| t.attribute_name.clone());
    if current == attribute {
        return MrecApplyResponse::Done;
    }
    let name = attribute.clone().or(current).unwrap_or_default();
    let body = json!({
        "TableName": table,
        "TimeToLiveSpecification": {"Enabled": attribute.is_some(), "AttributeName": name},
    });
    let _ = crate::dynamo::execute_item_op_as(
        ctx,
        &crate::authz::Principal::unrestricted(),
        "DynamoDB_20120810.UpdateTimeToLive",
        body.to_string().as_bytes(),
    )
    .await;
    let now = ctx.metadata_fresh().await;
    if now.table_ttl(table).map(|t| t.attribute_name.clone()) == attribute {
        MrecApplyResponse::Done
    } else {
        refused("the TTL change did not commit yet", true)
    }
}

/// Sender side: push this table's TTL setting to every `Active` peer when it
/// differs from what this node last pushed. First sight is a baseline (a
/// table with a TTL pushes it, one without records "none" and sends nothing),
/// so a restarted or newly-elected driver never reverts a peer's newer change
/// with its own stale `None`. Driver-local state: a TTL change made while
/// the driver role moves is not re-sent until the next change (documented).
async fn push_ttl<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    client: &dyn PeerClient,
    meta: &Metadata,
    spec: &GlobalTableSpec,
    table: &str,
) -> bool {
    let current = meta.table_ttl(table).map(|t| t.attribute_name.clone());
    let mut did = false;
    for r in spec
        .replicas
        .iter()
        .filter(|r| !r.local && r.status == MrecReplicaStatus::Active)
    {
        let key: HealthKey = (table.to_owned(), SAGA_SLOT, r.region.clone());
        let last = {
            let h = ctx.mrec.health.lock().expect("mrec health poisoned");
            h.get(&key).and_then(|e| e.ttl_pushed.clone())
        };
        let push = match &last {
            None => current.is_some(),
            Some(l) => *l != current,
        };
        let mut done = !push;
        if push {
            done = matches!(
                send_control(
                    ctx,
                    client,
                    &r.region,
                    table,
                    MrecControl::SetTtl {
                        attribute: current.clone()
                    }
                )
                .await,
                Ok(MrecApplyResponse::Done)
            );
            did = true;
        }
        if done {
            let mut h = ctx.mrec.health.lock().expect("mrec health poisoned");
            h.entry(key).or_default().ttl_pushed = Some(current.clone());
        }
    }
    did
}

async fn add_replica_active<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    table: &str,
    region: &str,
) -> Result<(), WireError> {
    let (t, r) = (table.to_owned(), region.to_owned());
    commit(
        ctx,
        MetaCommand::AddMrecReplica {
            table: table.to_owned(),
            region: region.to_owned(),
            region_id: mrec_region_id(region),
        },
        move |m| mrec_spec(m, &t).is_some_and(|s| s.replicas.iter().any(|x| x.region == r)),
    )
    .await?;
    // The initial copy to it is this side's own shipper scan; nothing waits on
    // it, so the replica serves from the start.
    let (t, r) = (table.to_owned(), region.to_owned());
    commit(
        ctx,
        MetaCommand::SetMrecReplicaStatus {
            table: table.to_owned(),
            region: region.to_owned(),
            status: MrecReplicaStatus::Active,
        },
        move |m| {
            mrec_spec(m, &t).is_none_or(|s| {
                s.replicas
                    .iter()
                    .find(|x| x.region == r)
                    .is_none_or(|x| x.status != MrecReplicaStatus::Creating)
            })
        },
    )
    .await?;
    Ok(())
}

async fn create_replica<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    from: &str,
    table: &str,
    create_table: &str,
    ttl_attribute: Option<String>,
    peers: &[String],
) -> MrecApplyResponse {
    let Some(local) = ctx.mrec.region.clone() else {
        return refused(
            "this cluster has no cluster_settings.region configured",
            false,
        );
    };
    let Ok(wanted) = serde_json::from_str::<Value>(create_table) else {
        return refused("malformed CreateTable body", false);
    };
    for p in peers {
        if *p != local && !ctx.mrec.is_peer(p) {
            return refused(
                format!("region `{p}` is not a configured peer of this cluster"),
                false,
            );
        }
    }
    let meta = ctx.metadata_fresh().await;
    if let Some(g) = meta.table_global(table) {
        if !g.is_mrec() {
            return refused(
                format!("table `{table}` here is a multi-Region strongly consistent table"),
                false,
            );
        }
        if g.replicas
            .iter()
            .any(|r| r.region == from && r.status == MrecReplicaStatus::Deleting)
        {
            return refused(
                format!("the replica for `{from}` is being removed here"),
                true,
            );
        }
    }
    if meta.has_table_schema(table) {
        // Adopt only an identical shape.
        let mine =
            create_table_body(&meta, table).and_then(|b| serde_json::from_str::<Value>(&b).ok());
        if mine.is_none_or(|m| shape_of(&m) != shape_of(&wanted)) {
            return refused(
                format!(
                    "table `{table}` already exists here with a different key schema or indexes"
                ),
                false,
            );
        }
        if table_status(&meta, table) != "ACTIVE" {
            return refused(format!("table `{table}` here is not ACTIVE yet"), true);
        }
    } else {
        let (s, r) = crate::dynamo::execute_item_op_as(
            ctx,
            &crate::authz::Principal::unrestricted(),
            "DynamoDB_20120810.CreateTable",
            create_table.as_bytes(),
        )
        .await;
        if s != 200 && !ctx.metadata_fresh().await.has_table_schema(table) {
            return refused(
                format!("creating table `{table}` failed: {r}"),
                s < 500 && s != 400,
            );
        }
    }
    let meta = ctx.metadata_fresh().await;
    if let Err(e) = ensure_stream(ctx, &meta, table).await {
        return refused(e.message, false);
    }
    if let Some(attr) = ttl_attribute
        && meta.table_ttl(table).is_none()
    {
        let body = json!({
            "TableName": table,
            "TimeToLiveSpecification": {"Enabled": true, "AttributeName": attr},
        });
        let _ = crate::dynamo::execute_item_op_as(
            ctx,
            &crate::authz::Principal::unrestricted(),
            "DynamoDB_20120810.UpdateTimeToLive",
            body.to_string().as_bytes(),
        )
        .await;
    }
    if mrec_spec(&meta, table).is_none() {
        let (t, l) = (table.to_owned(), local.clone());
        if let Err(e) = commit(
            ctx,
            MetaCommand::ConvertTableToMrec {
                table: table.to_owned(),
                local_region: local.clone(),
                region_id: mrec_region_id(&local),
            },
            move |m| {
                mrec_spec(m, &t)
                    .is_some_and(|s| s.replicas.iter().any(|r| r.local && r.region == l))
            },
        )
        .await
        {
            return refused(e.message, true);
        }
    }
    let wanted_regions = std::iter::once(from).chain(peers.iter().map(String::as_str));
    for region in wanted_regions {
        if region == local {
            continue;
        }
        let have = ctx
            .metadata_fresh()
            .await
            .table_global(table)
            .is_some_and(|g| g.replicas.iter().any(|r| r.region == region));
        if !have && let Err(e) = add_replica_active(ctx, table, region).await {
            return refused(e.message, true);
        }
    }
    MrecApplyResponse::Done
}

async fn add_peer<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    table: &str,
    region: &str,
) -> MrecApplyResponse {
    if ctx.mrec.region.as_deref() == Some(region) {
        return MrecApplyResponse::Done;
    }
    if !ctx.mrec.is_peer(region) {
        return refused(
            format!("region `{region}` is not a configured peer of this cluster"),
            false,
        );
    }
    let meta = ctx.metadata_fresh().await;
    let Some(spec) = mrec_spec(&meta, table) else {
        return refused(format!("table `{table}` is not an MREC table here"), true);
    };
    if spec.replicas.iter().any(|r| r.region == region) {
        return MrecApplyResponse::Done;
    }
    match add_replica_active(ctx, table, region).await {
        Ok(()) => MrecApplyResponse::Done,
        Err(e) => refused(e.message, true),
    }
}

async fn leave<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    from: &str,
    table: &str,
) -> MrecApplyResponse {
    let meta = ctx.metadata_fresh().await;
    let Some(spec) = mrec_spec(&meta, table) else {
        return MrecApplyResponse::Done;
    };
    let Some(replica) = spec.replicas.iter().find(|r| r.region == from && !r.local) else {
        return MrecApplyResponse::Done;
    };
    if replica.status == MrecReplicaStatus::Deleting {
        return MrecApplyResponse::Done;
    }
    let (t, r) = (table.to_owned(), from.to_owned());
    match commit(
        ctx,
        MetaCommand::SetMrecReplicaStatus {
            table: table.to_owned(),
            region: from.to_owned(),
            status: MrecReplicaStatus::Deleting,
        },
        move |m| {
            mrec_spec(m, &t).is_none_or(|s| {
                s.replicas
                    .iter()
                    .find(|x| x.region == r)
                    .is_none_or(|x| x.status == MrecReplicaStatus::Deleting)
            })
        },
    )
    .await
    {
        Ok(_) => MrecApplyResponse::Done,
        Err(e) => refused(e.message, true),
    }
}
