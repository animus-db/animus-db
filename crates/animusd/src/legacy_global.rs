//! The **legacy (version 2017.11.29) global tables** operations over the MREC
//! machinery (ADR 0075's 2026-10-21 amendment, `Gate::LegacyGlobalTables`,
//! cluster version 9): `CreateGlobalTable`, `UpdateGlobalTable`,
//! `DescribeGlobalTable`, `ListGlobalTables`, `DescribeGlobalTableSettings` and
//! `UpdateGlobalTableSettings`.
//!
//! # Design
//!
//! A legacy global table **is** an MREC table (same saga, shipper, receiver,
//! last-writer-wins, per-replica read overrides) whose replicated spec carries
//! a [`LegacyGlobal`](animus_control::LegacyGlobal) marker. What differs is the
//! control flow, because AWS 2017.11.29 never creates the replica tables: the
//! operator creates an identically named, identically keyed, **empty** table
//! with a `NEW_AND_OLD_IMAGES` stream in every Region first, and
//! `CreateGlobalTable` links them.
//!
//! - **Edge, synchronous** ([`create_global_table`], [`update_global_table`]):
//!   validates the request and this cluster's own table, then asks each new
//!   Region's cluster to *verify* its table with a read-only
//!   [`MrecControl::LegacyReplica`] (`verify_only`). Only if every Region
//!   agrees does it commit anything, so a missing, non-empty, mis-shaped or
//!   wrongly streamed table fails the call (nothing changes), as in AWS.
//! - **Saga, asynchronous** (`mrec_saga::step_creating`): the verified
//!   replicas are recorded `Creating`; the saga then sends the same message
//!   without `verify_only`, so the peer re-verifies (its table may have
//!   changed since) and joins the replica set. A peer that now refuses for
//!   good shows `CREATION_FAILED`. The initial copy is the shipper's scan
//!   (empty tables, so it completes at once).
//! - **Removal**: `UpdateGlobalTable` `Delete` marks the replica `Deleting`;
//!   the saga ([`notify_removal`]) tells the departing Region and every other
//!   replica with [`MrecControl::LegacyRemovePeer`], then the ordinary
//!   `Leave` + grace + `RemoveMrecReplica` path runs. A leaving Region
//!   dismantles its own view of the group; when a table is left with one
//!   Region the legacy spec dissolves and it is a regular table again
//!   (`MetaCommand::RemoveMrecReplica`).
//! - **Settings** ([`update_settings`]): the table's billing mode and write
//!   capacity are one replicated, last-writer-wins record
//!   (`MetaCommand::SetLegacyGlobalSettings`) every cluster applies to its own
//!   table; per-Region read capacity is the existing per-replica override
//!   ([`mrec_saga::set_overrides`]). The saga pushes the record
//!   ([`push_settings`]) like it pushes overrides.
//!
//! The global table name is the table name. The version marker is exclusive:
//! the 2019.11.21 `ReplicaUpdates` refuse a legacy table
//! (`mrec_saga::update_table_mrec`), and the legacy operations answer
//! `GlobalTableNotFoundException` for a table that is not a legacy global
//! table (including a 2019.11.21 one).

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use animus_control::schema::{GlobalBilling, GlobalTableSpec, MrecReplicaStatus, mrec_region_id};
use animus_control::version::Gate;
use animus_control::{IndexKind, MetaCommand, Metadata};
use animus_dynamo::global::{ReadChange, RegionStatus, ReplicaSettings};
use animus_dynamo::legacy_global::{
    self as lg, BillingMode, GlobalTableView, IndexSettingsView, LegacyGlobalOp,
    ReplicaSettingsView, SettingsUpdate,
};
use animus_dynamo::wire::{self, WireError};
use animus_env::Env;
use animus_node::host::RelayClient;
use animus_node::{MrecApplyResponse, MrecControl};

use crate::ClientCtx;
use crate::dynamo::{internal, table_status};
use crate::mrec_peer::{PeerClient, SagaKey};
use crate::mrec_saga::{
    add_replica_active, commit, create_table_body, gate_open, mrec_spec, refused, send_control,
    shape_of,
};

/// How long the edge waits for a peer's read-only verification. Short: the
/// call fails cleanly (nothing committed) and is simply retried.
const VERIFY_TIMEOUT: Duration = Duration::from_secs(5);
/// `LegacyRemovePeer` passes that may fail before the saga stops waiting for
/// an unreachable Region (the ordinary `Leave` path gives up the same way).
const REMOVE_ATTEMPTS: u32 = 6;

/// Whether `Gate::LegacyGlobalTables` is open (the emit-site check).
pub(crate) fn legacy_gate_open<E: Env, R: RelayClient>(ctx: &ClientCtx<E, R>) -> bool {
    gate_open(ctx, Gate::LegacyGlobalTables)
}

fn legacy_spec<'a>(meta: &'a Metadata, name: &str) -> Option<&'a GlobalTableSpec> {
    meta.table_global(name).filter(|g| g.is_legacy())
}

fn status_of(s: MrecReplicaStatus) -> RegionStatus {
    match s {
        MrecReplicaStatus::Creating => RegionStatus::Creating,
        MrecReplicaStatus::Active => RegionStatus::Active,
        MrecReplicaStatus::Deleting => RegionStatus::Deleting,
        MrecReplicaStatus::CreationFailed => RegionStatus::CreationFailed,
    }
}

/// The `DescribeGlobalTable` view of a legacy spec: replicas sorted by Region
/// name so every cluster reports the identical list.
fn global_view(name: &str, spec: &GlobalTableSpec) -> GlobalTableView {
    let mut replicas: Vec<(String, RegionStatus)> = spec
        .replicas
        .iter()
        .map(|r| (r.region.clone(), status_of(r.status)))
        .collect();
    replicas.sort_by(|a, b| a.0.cmp(&b.0));
    let local = spec
        .replicas
        .iter()
        .find(|r| r.local)
        .map_or(String::new(), |r| r.region.clone());
    GlobalTableView {
        name: name.to_owned(),
        created_ms: spec.legacy.as_ref().map_or(0, |l| l.created_ms),
        status: lg::global_table_status(&replicas, &local),
        replicas,
    }
}

/// The `DescribeGlobalTable` body of `name` as `meta` has it.
fn describe(meta: &Metadata, name: &str) -> Result<String, WireError> {
    let spec = legacy_spec(meta, name).ok_or_else(|| lg::global_table_not_found(name))?;
    Ok(lg::global_table_body(&global_view(name, spec)))
}

/// Run one legacy operation.
///
/// # Errors
/// The exception types the service model lists per operation, plus
/// `ValidationException` for a malformed or unsupported request;
/// `legacy_global_table_operation_error` while the gate is closed.
pub(crate) async fn execute<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    op: LegacyGlobalOp,
) -> Result<String, WireError> {
    if !legacy_gate_open(ctx) {
        return Err(animus_dynamo::global::legacy_global_table_operation_error(
            op.operation(),
        ));
    }
    match op {
        LegacyGlobalOp::Create { name, regions } => create_global_table(ctx, &name, &regions).await,
        LegacyGlobalOp::Update {
            name,
            creates,
            deletes,
        } => update_global_table(ctx, &name, &creates, &deletes).await,
        LegacyGlobalOp::Describe { name } => describe(&ctx.metadata_fresh().await, &name),
        LegacyGlobalOp::List {
            exclusive_start,
            limit,
            region,
        } => Ok(list(
            &ctx.metadata_fresh().await,
            exclusive_start.as_deref(),
            limit,
            region.as_deref(),
        )),
        LegacyGlobalOp::DescribeSettings { name } => {
            settings_body(&ctx.metadata_fresh().await, &name)
        }
        LegacyGlobalOp::UpdateSettings(update) => update_settings(ctx, &update).await,
    }
}

fn list(
    meta: &Metadata,
    exclusive_start: Option<&str>,
    limit: usize,
    region: Option<&str>,
) -> String {
    // `schemas` iterates in ascending name order.
    let all: Vec<(String, Vec<String>)> = meta
        .schemas
        .iter()
        .filter_map(|(name, s)| {
            let g = s.global.as_ref().filter(|g| g.is_legacy())?;
            let mut regions: Vec<String> = g.replicas.iter().map(|r| r.region.clone()).collect();
            regions.sort();
            region
                .is_none_or(|want| regions.iter().any(|r| r == want))
                .then(|| (name.clone(), regions))
        })
        .collect();
    let names: Vec<String> = all.iter().map(|(n, _)| n.clone()).collect();
    let (page, last) = wire::paginate_table_names(&names, exclusive_start, Some(limit));
    let entries: Vec<(String, Vec<String>)> =
        all.into_iter().filter(|(n, _)| page.contains(n)).collect();
    lg::list_body(&entries, last.as_deref())
}

fn local_region<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    op: &str,
) -> Result<String, WireError> {
    ctx.mrec.region.clone().ok_or_else(|| {
        WireError::validation(format!(
            "{op}: this cluster has no cluster_settings.region configured, so it cannot take \
             part in a global table"
        ))
    })
}

/// The preconditions on this cluster's own table of a Region joining a legacy
/// global table that the *caller* can check locally.
async fn check_local_table<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    meta: &Metadata,
    op: &str,
    name: &str,
) -> Result<(), WireError> {
    if !meta.has_table_schema(name) {
        return Err(lg::table_not_found(name, ctx.mrec.region.as_deref()));
    }
    if table_status(meta, name) != "ACTIVE" {
        return Err(WireError::validation(format!(
            "{op}: table `{name}` is not ACTIVE"
        )));
    }
    check_stream(meta, op, name)?;
    if crate::global_tables::table_has_items(ctx, name).await? {
        return Err(WireError::validation(format!(
            "{op}: table `{name}` must be empty (a legacy global table's replicas cannot contain \
             any data)"
        )));
    }
    Ok(())
}

fn check_stream(meta: &Metadata, op: &str, name: &str) -> Result<(), WireError> {
    match meta.table_stream(name) {
        Some(s) if s.view_type == animus_control::StreamViewType::NewAndOldImages => Ok(()),
        Some(s) => Err(WireError::validation(format!(
            "{op}: table `{name}` has a stream with view type {:?}; a global table's replicas \
             require StreamViewType NEW_AND_OLD_IMAGES",
            s.view_type
        ))),
        None => Err(WireError::validation(format!(
            "{op}: table `{name}` has no DynamoDB Stream; a global table's replicas require a \
             stream with StreamViewType NEW_AND_OLD_IMAGES"
        ))),
    }
}

/// Ask each of `regions` (concurrently) whether its table can join, changing
/// nothing. `peers_of` is the Regions each target must also be able to reach.
async fn verify_regions<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    meta: &Metadata,
    name: &str,
    regions: &[String],
    group: &[String],
) -> Result<(), WireError> {
    let Some(client) = ctx.mrec.edge_client.get() else {
        return Err(internal(
            "this node cannot reach peer clusters yet (no cross-cluster client installed); \
             retry shortly",
        ));
    };
    let Some(create_table) = create_table_body(meta, name) else {
        return Err(internal(&format!("cannot describe table `{name}`")));
    };
    let write_units = meta.table_throughput(name).map(|t| t.write_units);
    let local = ctx.mrec.region.clone().unwrap_or_default();
    let checks = regions.iter().map(|region| {
        let others: Vec<String> = group
            .iter()
            .filter(|g| **g != *region && **g != local)
            .cloned()
            .collect();
        let control = MrecControl::LegacyReplica {
            create_table: create_table.clone(),
            ttl_attribute: None,
            peers: others,
            write_units,
            created_ms: 0,
            verify_only: true,
        };
        let client = Arc::clone(&client);
        async move {
            let reply = send_verify(ctx, client.as_ref(), region, name, control).await;
            (region.clone(), reply)
        }
    });
    for (region, reply) in futures::future::join_all(checks).await {
        match reply {
            Ok(MrecApplyResponse::Done) => {}
            Ok(MrecApplyResponse::Refused {
                message,
                retryable: false,
            }) => return Err(refusal_to_error(name, &region, &message)),
            Ok(MrecApplyResponse::Refused { message, .. }) => {
                return Err(internal(&format!(
                    "Region `{region}` could not verify table `{name}` yet: {message}; retry \
                     shortly"
                )));
            }
            Ok(MrecApplyResponse::Answers(_)) => {
                return Err(internal(&format!(
                    "Region `{region}` answered the verification with an unexpected reply"
                )));
            }
            Err(e) => {
                return Err(internal(&format!(
                    "Region `{region}` could not be reached to verify table `{name}` ({e}); \
                     nothing was changed, retry shortly"
                )));
            }
        }
    }
    Ok(())
}

/// `send_control` with the edge's short timeout.
async fn send_verify<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    client: &dyn PeerClient,
    region: &str,
    name: &str,
    control: MrecControl,
) -> Result<MrecApplyResponse, String> {
    struct Timed<'a>(&'a dyn PeerClient);
    impl PeerClient for Timed<'_> {
        fn call(
            &self,
            to: usize,
            payload: Vec<u8>,
            _timeout: Duration,
        ) -> futures::future::BoxFuture<'static, Result<Vec<u8>, crate::mrec_peer::PeerError>>
        {
            self.0.call(to, payload, VERIFY_TIMEOUT)
        }
    }
    // `send_control` uses the saga's longer timeout; the wrapper shortens it.
    let timed = Timed(client);
    send_control(ctx, &timed, region, name, control).await
}

/// A peer's non-retryable refusal as the exception type its text names
/// (`"<ExceptionType>: <text>"`), else a `ValidationException`.
fn refusal_to_error(name: &str, region: &str, message: &str) -> WireError {
    let (code, text) = message.split_once(": ").unwrap_or(("", message));
    let text = format!("Region `{region}`: {text}");
    match code {
        "TableNotFoundException" => WireError {
            code: "TableNotFoundException",
            message: text,
            reasons: None,
        },
        "GlobalTableAlreadyExistsException" => lg::global_table_already_exists(name, &text),
        _ => WireError::validation(format!("table `{name}`: {text}")),
    }
}

// ---------------------------------------------------------------------------
// CreateGlobalTable

async fn create_global_table<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    name: &str,
    regions: &[String],
) -> Result<String, WireError> {
    let op = "CreateGlobalTable";
    let local = local_region(ctx, op)?;
    if !regions.contains(&local) {
        return Err(WireError::validation(format!(
            "{op}: the ReplicationGroup must include this cluster's own Region `{local}` (the \
             Region of the endpoint serving the call)"
        )));
    }
    let remote: Vec<String> = regions.iter().filter(|r| **r != local).cloned().collect();
    if remote.is_empty() {
        return Err(WireError::validation(format!(
            "{op}: a global table needs at least two Regions in its ReplicationGroup"
        )));
    }
    if regions.len() > GlobalTableSpec::MREC_MAX_REPLICAS {
        return Err(lg::limit_exceeded(format!(
            "{op}: a global table has at most {} replicas (this request names {})",
            GlobalTableSpec::MREC_MAX_REPLICAS,
            regions.len()
        )));
    }
    for r in &remote {
        if !ctx.mrec.is_peer(r) {
            let peers: Vec<&str> = ctx.mrec.peers.iter().map(|p| p.region.as_str()).collect();
            return Err(WireError::validation(format!(
                "{op}: Region `{r}` is not a configured peer of this cluster (configured peers: \
                 {})",
                peers.join(", ")
            )));
        }
    }
    let meta = ctx.metadata_fresh().await;
    if !meta.has_table_schema(name) {
        return Err(lg::table_not_found(name, Some(&local)));
    }
    match meta.table_global(name) {
        Some(g) if g.is_legacy() => {
            return Err(lg::global_table_already_exists(
                name,
                "it is already a global table (version 2017.11.29)",
            ));
        }
        Some(g) if !(g.is_mrec() && g.replicas.len() == 1) => {
            return Err(lg::global_table_already_exists(
                name,
                "the table is already a global table (version 2019.11.21)",
            ));
        }
        _ => {}
    }
    check_local_table(ctx, &meta, op, name).await?;
    verify_regions(ctx, &meta, name, &remote, regions).await?;

    let created_ms = ctx.env.wall_now().0;
    let (t, l) = (name.to_owned(), local.clone());
    commit(
        ctx,
        MetaCommand::ConvertTableToLegacyGlobal {
            table: name.to_owned(),
            local_region: local.clone(),
            region_id: mrec_region_id(&local),
            created_ms,
        },
        move |m| {
            legacy_spec(m, &t).is_some_and(|s| s.replicas.iter().any(|r| r.local && r.region == l))
        },
    )
    .await?;
    for region in &remote {
        add_creating(ctx, name, region).await?;
    }
    describe(&ctx.metadata_fresh().await, name)
}

async fn add_creating<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    name: &str,
    region: &str,
) -> Result<(), WireError> {
    let (t, r) = (name.to_owned(), region.to_owned());
    commit(
        ctx,
        MetaCommand::AddMrecReplica {
            table: name.to_owned(),
            region: region.to_owned(),
            region_id: mrec_region_id(region),
        },
        move |m| mrec_spec(m, &t).is_some_and(|s| s.replicas.iter().any(|x| x.region == r)),
    )
    .await
    .map(|_| ())
}

// ---------------------------------------------------------------------------
// UpdateGlobalTable

async fn update_global_table<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    name: &str,
    creates: &[String],
    deletes: &[String],
) -> Result<String, WireError> {
    let op = "UpdateGlobalTable";
    let local = local_region(ctx, op)?;
    let meta = ctx.metadata_fresh().await;
    let Some(spec) = legacy_spec(&meta, name) else {
        return Err(lg::global_table_not_found(name));
    };
    let has = |r: &str| spec.replicas.iter().any(|x| x.region == r);
    for r in creates {
        if has(r) {
            return Err(lg::replica_already_exists(name, r));
        }
        if *r == local || !ctx.mrec.is_peer(r) {
            let peers: Vec<&str> = ctx.mrec.peers.iter().map(|p| p.region.as_str()).collect();
            return Err(WireError::validation(format!(
                "{op}: Region `{r}` is not a configured peer of this cluster (configured peers: \
                 {})",
                peers.join(", ")
            )));
        }
    }
    for r in deletes {
        if !has(r) {
            return Err(lg::replica_not_found(name, r));
        }
    }
    let after = spec.replicas.len() + creates.len();
    if after > GlobalTableSpec::MREC_MAX_REPLICAS {
        return Err(lg::limit_exceeded(format!(
            "{op}: a global table has at most {} replicas (this request would make {after})",
            GlobalTableSpec::MREC_MAX_REPLICAS,
        )));
    }
    if !creates.is_empty() {
        // The new replicas must be as empty and as shaped as this cluster's
        // own replica was at creation; this cluster's table is the reference
        // shape (it holds data now, so emptiness is only the peers' rule).
        check_stream(&meta, op, name)?;
        let group: Vec<String> = spec
            .replicas
            .iter()
            .map(|r| r.region.clone())
            .chain(creates.iter().cloned())
            .collect();
        verify_regions(ctx, &meta, name, creates, &group).await?;
    }
    for region in creates {
        add_creating(ctx, name, region).await?;
    }
    for region in deletes {
        let (t, r) = (name.to_owned(), region.clone());
        commit(
            ctx,
            MetaCommand::SetMrecReplicaStatus {
                table: name.to_owned(),
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
    describe(&ctx.metadata_fresh().await, name)
}

// ---------------------------------------------------------------------------
// Settings

/// The settings of every Region as this cluster knows them (`meta`).
fn settings_views(meta: &Metadata, name: &str, spec: &GlobalTableSpec) -> Vec<ReplicaSettingsView> {
    let legacy = spec.legacy.clone().unwrap_or_default();
    let global_write = match legacy.billing {
        Some(GlobalBilling::Provisioned { write_units }) => Some(write_units),
        _ => None,
    };
    let gsis: Vec<&animus_control::IndexDef> = meta
        .table_indexes(name)
        .iter()
        .filter(|d| d.kind == IndexKind::Global)
        .collect();
    let mut views: Vec<ReplicaSettingsView> = spec
        .replicas
        .iter()
        .map(|r| {
            // This cluster knows its own table; for a remote Region it knows
            // only the replicated record and that Region's read overrides.
            let (billing, mut read, write) = if r.local {
                match meta.table_throughput(name) {
                    Some(t) => (
                        Some(BillingMode::Provisioned),
                        Some(t.read_units),
                        Some(t.write_units),
                    ),
                    None => (Some(BillingMode::PayPerRequest), None, None),
                }
            } else {
                match legacy.billing {
                    Some(GlobalBilling::Provisioned { write_units }) => {
                        (Some(BillingMode::Provisioned), None, Some(write_units))
                    }
                    Some(GlobalBilling::PayPerRequest) => {
                        (Some(BillingMode::PayPerRequest), None, None)
                    }
                    // No settings call yet: every replica of a global table
                    // shares one billing mode, so assume this cluster's.
                    None => match meta.table_throughput(name) {
                        Some(t) => (Some(BillingMode::Provisioned), None, Some(t.write_units)),
                        None => (Some(BillingMode::PayPerRequest), None, None),
                    },
                }
            };
            if billing != Some(BillingMode::PayPerRequest) {
                read = r.overrides.read_units.or(read);
            }
            let provisioned = billing == Some(BillingMode::Provisioned);
            let table_read = read;
            let indexes = gsis
                .iter()
                .map(|d| IndexSettingsView {
                    name: d.name.clone(),
                    status: match d.status {
                        animus_control::IndexStatus::Creating => "CREATING",
                        animus_control::IndexStatus::Active => "ACTIVE",
                        animus_control::IndexStatus::Deleting => "DELETING",
                    },
                    read_units: if provisioned {
                        r.overrides
                            .index_read_units
                            .get(&d.name)
                            .copied()
                            .or(table_read)
                    } else {
                        None
                    },
                    write_units: if provisioned {
                        write.or(global_write)
                    } else {
                        None
                    },
                })
                .collect();
            ReplicaSettingsView {
                region: r.region.clone(),
                status: status_of(r.status),
                billing,
                read_units: read,
                write_units: write,
                indexes,
            }
        })
        .collect();
    views.sort_by(|a, b| a.region.cmp(&b.region));
    views
}

fn settings_body(meta: &Metadata, name: &str) -> Result<String, WireError> {
    let spec = legacy_spec(meta, name).ok_or_else(|| lg::global_table_not_found(name))?;
    Ok(lg::settings_body(name, &settings_views(meta, name, spec)))
}

async fn update_settings<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    update: &SettingsUpdate,
) -> Result<String, WireError> {
    let op = "UpdateGlobalTableSettings";
    let name = update.name.as_str();
    let local = local_region(ctx, op)?;
    let meta = ctx.metadata_fresh().await;
    let Some(spec) = legacy_spec(&meta, name) else {
        return Err(lg::global_table_not_found(name));
    };
    for r in &update.replicas {
        if !spec.replicas.iter().any(|x| x.region == r.region) {
            return Err(lg::replica_not_found(name, &r.region));
        }
        for index in r.indexes.keys() {
            let is_gsi = meta
                .table_indexes(name)
                .iter()
                .any(|d| d.kind == IndexKind::Global && d.name == *index);
            if !is_gsi {
                return Err(lg::index_not_found(name, index));
            }
        }
    }
    if spec.replicas.iter().any(|r| {
        matches!(
            r.status,
            MrecReplicaStatus::Creating | MrecReplicaStatus::Deleting
        )
    }) {
        return Err(lg::resource_in_use(format!(
            "{op}: global table `{name}` is being updated (a replica is being created or \
             deleted); retry when it is ACTIVE"
        )));
    }
    // The billing mode the table ends up in.
    let current_write = meta.table_throughput(name).map(|t| t.write_units);
    let target = match (update.billing, update.write_units) {
        (Some(BillingMode::PayPerRequest), _) => Some(GlobalBilling::PayPerRequest),
        (_, Some(write_units)) => Some(GlobalBilling::Provisioned { write_units }),
        (Some(BillingMode::Provisioned), None) => match current_write {
            Some(write_units) => Some(GlobalBilling::Provisioned { write_units }),
            None => {
                return Err(WireError::validation(format!(
                    "{op}: switching global table `{name}` to PROVISIONED needs \
                     GlobalTableProvisionedWriteCapacityUnits"
                )));
            }
        },
        (None, None) => None,
    };
    let provisioned_after = match target {
        Some(GlobalBilling::PayPerRequest) => false,
        Some(GlobalBilling::Provisioned { .. }) => true,
        None => current_write.is_some(),
    };
    let changes_reads = update
        .replicas
        .iter()
        .any(|r| r.read_units.is_some() || !r.indexes.is_empty());
    if changes_reads && !provisioned_after {
        return Err(WireError::validation(format!(
            "{op}: global table `{name}` is PAY_PER_REQUEST; a replica's provisioned read \
             capacity applies only to a PROVISIONED table (set GlobalTableBillingMode \
             PROVISIONED with GlobalTableProvisionedWriteCapacityUnits in the same call)"
        )));
    }

    // 1. The replicated billing mode / write capacity.
    if let Some(billing) = target {
        let cur = spec.legacy.clone().unwrap_or_default();
        if cur.billing != Some(billing) {
            let stamp = ctx.env.now().0.max(cur.stamp.saturating_add(1));
            let origin = mrec_region_id(&local);
            let t = name.to_owned();
            let fresh = commit(
                ctx,
                MetaCommand::SetLegacyGlobalSettings {
                    table: name.to_owned(),
                    billing,
                    stamp,
                    origin,
                },
                move |m| {
                    legacy_spec(m, &t)
                        .and_then(|s| s.legacy.as_ref())
                        .is_some_and(|l| l.order() >= (stamp, origin))
                },
            )
            .await?;
            ctx.recompute_any_table_throughput(&fresh);
        }
        // PAY_PER_REQUEST has no read capacity to override: clear every
        // replica's override (a newer tombstone, pushed to the peers).
        if billing == GlobalBilling::PayPerRequest {
            let fresh = ctx.metadata_fresh().await;
            if let Some(spec) = legacy_spec(&fresh, name) {
                let to_clear: Vec<String> = spec
                    .replicas
                    .iter()
                    .filter(|r| !r.overrides.is_empty())
                    .map(|r| r.region.clone())
                    .collect();
                for region in to_clear {
                    let clear = ReplicaSettings {
                        table: ReadChange::Clear,
                        indexes: fresh_override_indexes(&fresh, name, &region, ReadChange::Clear),
                        unsupported: Vec::new(),
                    };
                    crate::mrec_saga::set_overrides(ctx, name, &local, &region, &clear).await?;
                }
            }
        }
    }
    // 2. Per-Region read capacity (the per-replica overrides).
    for r in &update.replicas {
        if r.read_units.is_none() && r.indexes.is_empty() {
            continue;
        }
        let settings = ReplicaSettings {
            table: r.read_units.map_or(ReadChange::Unchanged, ReadChange::Set),
            indexes: r
                .indexes
                .iter()
                .map(|(i, u)| (i.clone(), ReadChange::Set(*u)))
                .collect(),
            unsupported: Vec::new(),
        };
        crate::mrec_saga::set_overrides(ctx, name, &local, &r.region, &settings).await?;
    }
    settings_body(&ctx.metadata_fresh().await, name)
}

/// `change` for every index the replica currently overrides.
fn fresh_override_indexes(
    meta: &Metadata,
    name: &str,
    region: &str,
    change: ReadChange,
) -> Vec<(String, ReadChange)> {
    legacy_spec(meta, name)
        .and_then(|s| s.replicas.iter().find(|r| r.region == region))
        .map(|r| {
            r.overrides
                .index_read_units
                .keys()
                .map(|i| (i.clone(), change))
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// The peer side

/// What a [`MrecControl::LegacyReplica`] carries, unpacked.
pub(crate) struct LegacyJoin {
    pub(crate) create_table: String,
    pub(crate) ttl_attribute: Option<String>,
    pub(crate) peers: Vec<String>,
    pub(crate) write_units: Option<u64>,
    pub(crate) created_ms: u64,
    pub(crate) verify_only: bool,
}

/// Verify this cluster's table `table` can join a legacy global table the
/// sender `from` belongs to, and (unless `verify_only`) join it.
///
/// Refusals that cannot heal by waiting are `retryable: false` and carry the
/// exception type the edge reports as the message prefix
/// (`"TableNotFoundException: ..."`).
pub(crate) async fn legacy_replica<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    from: &str,
    table: &str,
    join: &LegacyJoin,
) -> MrecApplyResponse {
    if !legacy_gate_open(ctx) {
        return refused(
            "this cluster has not finalized the cluster version that carries the legacy global \
             tables API yet",
            true,
        );
    }
    let Some(local) = ctx.mrec.region.clone() else {
        return refused(
            "this cluster has no cluster_settings.region configured",
            false,
        );
    };
    let Ok(wanted) = serde_json::from_str::<serde_json::Value>(&join.create_table) else {
        return refused("malformed table description", false);
    };
    for p in &join.peers {
        if *p != local && !ctx.mrec.is_peer(p) {
            return refused(
                format!("region `{p}` is not a configured peer of this cluster"),
                false,
            );
        }
    }
    let meta = ctx.metadata_fresh().await;
    if !meta.has_table_schema(table) {
        return refused(
            format!("TableNotFoundException: table `{table}` does not exist"),
            false,
        );
    }
    // Already part of this very legacy global table (a re-sent join): the
    // checks below hold by construction and the table may hold data by now.
    let rejoin =
        legacy_spec(&meta, table).is_some_and(|g| g.replicas.iter().any(|r| r.region == from));
    if !rejoin {
        match meta.table_global(table) {
            Some(g) if g.is_legacy() => {
                return refused(
                    format!(
                        "GlobalTableAlreadyExistsException: table `{table}` is already part of \
                         another legacy global table"
                    ),
                    false,
                );
            }
            Some(g) if !(g.is_mrec() && g.replicas.len() == 1) => {
                return refused(
                    format!(
                        "GlobalTableAlreadyExistsException: table `{table}` is already a global \
                         table (version 2019.11.21)"
                    ),
                    false,
                );
            }
            _ => {}
        }
        let mine = create_table_body(&meta, table).and_then(|b| serde_json::from_str(&b).ok());
        if mine.is_none_or(|m: serde_json::Value| shape_of(&m) != shape_of(&wanted)) {
            return refused(
                format!(
                    "table `{table}` must have the same key schema and secondary indexes as the \
                     other replicas"
                ),
                false,
            );
        }
        if table_status(&meta, table) != "ACTIVE" {
            return refused(format!("table `{table}` is not ACTIVE yet"), true);
        }
        if let Err(e) = check_stream(&meta, "table", table) {
            return refused(e.message, false);
        }
        let have = meta.table_throughput(table).map(|t| t.write_units);
        if have != join.write_units {
            let show = |w: Option<u64>| {
                w.map_or("PAY_PER_REQUEST".to_owned(), |u| {
                    format!("{u} write capacity units")
                })
            };
            return refused(
                format!(
                    "table `{table}` must have the same provisioned write capacity as the other \
                     replicas (it has {}, they have {})",
                    show(have),
                    show(join.write_units)
                ),
                false,
            );
        }
        match crate::global_tables::table_has_items(ctx, table).await {
            Ok(false) => {}
            Ok(true) => {
                return refused(
                    format!("table `{table}` must be empty (it already contains data)"),
                    false,
                );
            }
            Err(e) => return refused(e.message, true),
        }
    }
    if join.verify_only {
        return MrecApplyResponse::Done;
    }
    // Join: the legacy conversion, the TTL attribute, and the replica set.
    if legacy_spec(&meta, table).is_none() {
        let (t, l) = (table.to_owned(), local.clone());
        if let Err(e) = commit(
            ctx,
            MetaCommand::ConvertTableToLegacyGlobal {
                table: table.to_owned(),
                local_region: local.clone(),
                region_id: mrec_region_id(&local),
                created_ms: join.created_ms,
            },
            move |m| {
                legacy_spec(m, &t)
                    .is_some_and(|s| s.replicas.iter().any(|r| r.local && r.region == l))
            },
        )
        .await
        {
            return refused(e.message, true);
        }
    }
    if let Some(attr) = &join.ttl_attribute
        && meta.table_ttl(table).is_none()
    {
        let body = serde_json::json!({
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
    for region in std::iter::once(from).chain(join.peers.iter().map(String::as_str)) {
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

/// Peer side of [`MrecControl::SetLegacySettings`]: apply the sender's record
/// iff it is newer than ours (last-writer-wins); an equal or older one is
/// acknowledged untouched.
pub(crate) async fn set_settings_from_peer<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    table: &str,
    write_units: Option<u64>,
    stamp: u64,
    origin: u32,
) -> MrecApplyResponse {
    if !legacy_gate_open(ctx) {
        return refused(
            "this cluster has not finalized the cluster version that carries the legacy global \
             tables API yet",
            true,
        );
    }
    let billing = match write_units {
        Some(0) => return refused("a write capacity must be at least 1", false),
        Some(write_units) => GlobalBilling::Provisioned { write_units },
        None => GlobalBilling::PayPerRequest,
    };
    let meta = ctx.metadata_fresh().await;
    let Some(spec) = legacy_spec(&meta, table) else {
        return refused(
            format!("table `{table}` is not a legacy global table here (yet)"),
            true,
        );
    };
    let cur = spec.legacy.clone().unwrap_or_default();
    if (stamp, origin) <= cur.order() {
        return MrecApplyResponse::Done;
    }
    let t = table.to_owned();
    match commit(
        ctx,
        MetaCommand::SetLegacyGlobalSettings {
            table: table.to_owned(),
            billing,
            stamp,
            origin,
        },
        move |m| {
            legacy_spec(m, &t)
                .and_then(|s| s.legacy.as_ref())
                .is_none_or(|l| l.order() >= (stamp, origin))
        },
    )
    .await
    {
        Ok(fresh) => {
            ctx.recompute_any_table_throughput(&fresh);
            MrecApplyResponse::Done
        }
        Err(e) => refused(e.message, true),
    }
}

/// Peer side of [`MrecControl::LegacyRemovePeer`].
pub(crate) async fn remove_peer<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    from: &str,
    table: &str,
    region: &str,
) -> MrecApplyResponse {
    if !legacy_gate_open(ctx) {
        return refused(
            "this cluster has not finalized the cluster version that carries the legacy global \
             tables API yet",
            true,
        );
    }
    let meta = ctx.metadata_fresh().await;
    let Some(spec) = legacy_spec(&meta, table) else {
        return MrecApplyResponse::Done;
    };
    // Only a member in good standing may remove someone: a Region that is
    // itself leaving (or already gone) from this table's point of view is
    // dissolving and must not take the others down with it.
    if !spec
        .replicas
        .iter()
        .any(|r| !r.local && r.region == from && r.status != MrecReplicaStatus::Deleting)
    {
        return MrecApplyResponse::Done;
    }
    let leaving_self = spec.replicas.iter().any(|r| r.local && r.region == region);
    let targets: Vec<String> = spec
        .replicas
        .iter()
        .filter(|r| !r.local && r.status != MrecReplicaStatus::Deleting)
        .filter(|r| leaving_self || r.region == region)
        .map(|r| r.region.clone())
        .collect();
    for target in targets {
        let (t, r) = (table.to_owned(), target.clone());
        if let Err(e) = commit(
            ctx,
            MetaCommand::SetMrecReplicaStatus {
                table: table.to_owned(),
                region: target,
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
            return refused(e.message, true);
        }
    }
    MrecApplyResponse::Done
}

// ---------------------------------------------------------------------------
// The saga (sender side)

/// Saga step: push the legacy settings record to every `Active` peer that has
/// not acknowledged it. Convergent: the memo only skips a record the peer
/// acknowledged, a failed send is retried next pass, and the peer's
/// last-writer-wins apply makes any repeat a no-op.
pub(crate) async fn push_settings<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    client: &dyn PeerClient,
    spec: &GlobalTableSpec,
    table: &str,
) -> bool {
    let Some(legacy) = spec.legacy.as_ref().filter(|l| l.billing.is_some()) else {
        return false;
    };
    if !legacy_gate_open(ctx) {
        return false;
    }
    let order = legacy.order();
    let write_units = match legacy.billing {
        Some(GlobalBilling::Provisioned { write_units }) => Some(write_units),
        _ => None,
    };
    let mut did = false;
    for target in spec
        .replicas
        .iter()
        .filter(|r| !r.local && r.status == MrecReplicaStatus::Active)
    {
        let key: SagaKey = (table.to_owned(), target.region.clone());
        let pushed = {
            let h = ctx.mrec.saga.lock().expect("mrec saga memo poisoned");
            h.get(&key).and_then(|e| e.legacy_settings_pushed)
        };
        if pushed == Some(order) {
            continue;
        }
        let reply = send_control(
            ctx,
            client,
            &target.region,
            table,
            MrecControl::SetLegacySettings {
                write_units,
                stamp: order.0,
                origin: order.1,
            },
        )
        .await;
        did = true;
        if matches!(reply, Ok(MrecApplyResponse::Done)) {
            let mut h = ctx.mrec.saga.lock().expect("mrec saga memo poisoned");
            h.entry(key).or_default().legacy_settings_pushed = Some(order);
        }
    }
    did
}

/// Saga step for a `Deleting` replica of a legacy table: tell the departing
/// Region and every other live replica that it leaves
/// ([`MrecControl::LegacyRemovePeer`]). Returns `true` while it still has
/// work (the caller then skips the ordinary `Leave` step this pass); gives up
/// on an unreachable Region after [`REMOVE_ATTEMPTS`] failed passes, like the
/// ordinary `Leave` does.
pub(crate) async fn notify_removal<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    client: &dyn PeerClient,
    spec: &GlobalTableSpec,
    table: &str,
    leaving: &str,
) -> bool {
    let key: SagaKey = (table.to_owned(), leaving.to_owned());
    let (acked, failures) = {
        let h = ctx.mrec.saga.lock().expect("mrec saga memo poisoned");
        h.get(&key).map_or((BTreeSet::new(), 0), |e| {
            (e.legacy_removed.clone(), e.legacy_remove_failures)
        })
    };
    // Every other replica (survivors and other removed Regions alike) first;
    // the departing Region last, and only once all the others acknowledged, so
    // that when it dissolves and tells its former peers, each of them already
    // has it marked `Deleting` and ignores that (see `remove_peer`).
    let others: Vec<String> = spec
        .replicas
        .iter()
        .filter(|r| !r.local && r.region != leaving)
        .map(|r| r.region.clone())
        .filter(|r| !acked.contains(r))
        .collect();
    let pending: Vec<String> = if others.is_empty() {
        spec.replicas
            .iter()
            .filter(|r| !r.local && r.region == leaving)
            .map(|r| r.region.clone())
            .filter(|r| !acked.contains(r))
            .collect()
    } else {
        others
    };
    if pending.is_empty() || failures >= REMOVE_ATTEMPTS {
        return false;
    }
    let mut failed = false;
    for target in pending {
        let reply = send_control(
            ctx,
            client,
            &target,
            table,
            MrecControl::LegacyRemovePeer {
                region: leaving.to_owned(),
            },
        )
        .await;
        let mut h = ctx.mrec.saga.lock().expect("mrec saga memo poisoned");
        let e = h.entry(key.clone()).or_default();
        if matches!(reply, Ok(MrecApplyResponse::Done)) {
            e.legacy_removed.insert(target);
        } else {
            failed = true;
        }
    }
    if failed {
        let mut h = ctx.mrec.saga.lock().expect("mrec saga memo poisoned");
        h.entry(key).or_default().legacy_remove_failures += 1;
    }
    true
}
