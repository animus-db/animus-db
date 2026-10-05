//! The wire edge of **MRSC global tables** (ADR 0075 sections 3.5/3.7/5.1,
//! G-01 stage G-c, milestone M3): `UpdateTable` `ReplicaUpdates` ->
//! [`MetaCommand::ConvertTableToGlobal`], the `DescribeTable` global fields,
//! and the MRSC feature restrictions.
//!
//! Everything here is `E: Env`-generic client path (no wall clock, no raw
//! spawn: the `#[deny(clippy::disallowed_methods)]` on this module's
//! declaration in `lib.rs` enforces it), shared by the production
//! `run_operation` arm and the `SimCluster` `dispatch_table_op` arm so both
//! take the identical code.
//!
//! ## Order of checks in [`update_table_global`]
//!
//! 1. **The gate.** `Gate::GlobalTables` closed -> the pre-G-c rejection text,
//!    byte for byte ([`GlobalTableUpdate::closed_gate_error`]). This is the
//!    emit-site check ADR 0073 requires: the proposer must never append a
//!    command whose gate is closed (the relay receiver re-checks it,
//!    `version_wiring::relay_gate_verdict`).
//! 2. The table exists (`ResourceNotFoundException`).
//! 3. The request-shape rules (pure, `animus_dynamo::global`).
//! 4. The cluster-dependent rules: not already global, this node's own Region
//!    label, every named Region is carried by an `Active` member, the table is
//!    `ACTIVE`, has no TTL and no LSI, and is **empty**.
//!
//! The emptiness check (plan decision D6) is AWS fidelity, not safety: a
//! write racing the conversion is safe because Raft membership change moves
//! data anyway. It is deliberately *not* a replicated precondition.

use std::collections::BTreeSet;

use animus_control::schema::{GlobalTableSpec, MultiRegionConsistency};
use animus_control::version::Gate;
use animus_control::{IndexKind, MetaCommand, Metadata, NodeStatus};
use animus_dynamo::global::{GlobalTableDescription, GlobalTableUpdate, RegionStatus};
use animus_dynamo::wire::{self, WireError};
use animus_env::Env;
use animus_node::host::RelayClient;
use animus_placement::REGION_LABEL;

use crate::dynamo::{
    SCHEMA_COMMIT_TIMEOUT, SCHEMA_POLL_INTERVAL, describe_table_wrapped, internal, registry_error,
    table_status,
};
use crate::{ClientCtx, ReadConsistency};

/// Rows fetched per page by the emptiness scan.
const EMPTY_CHECK_PAGE: usize = 64;

/// The `DescribeTable` global fields for `table`, or `None` for a regional
/// (or unknown) table — whose output is therefore byte-identical to before.
///
/// Replica status is **derived** (plan decision D4), never stored: a Region is
/// `ACTIVE` once every routable tablet of the table has a replica of its
/// desired set there, `CREATING` until then. The local Region is included in
/// `Replicas` (AWS-style); the witness is listed apart.
#[must_use]
pub(crate) fn global_description(meta: &Metadata, table: &str) -> Option<GlobalTableDescription> {
    let spec = meta.table_global(table)?;
    let ready = meta.table_ready_regions(table);
    let status = |region: &String| {
        if ready.contains(region) {
            RegionStatus::Active
        } else {
            RegionStatus::Creating
        }
    };
    Some(GlobalTableDescription {
        replicas: spec
            .regions
            .iter()
            .filter(|r| spec.witness.as_ref() != Some(*r))
            .map(|r| (r.clone(), status(r)))
            .collect(),
        witness: spec.witness.as_ref().map(|r| (r.clone(), status(r))),
    })
}

/// MRSC restriction (ADR 0075 V8/3.7): the transaction APIs error on a global
/// table. Called by `TransactWriteItems`/`TransactGetItems` (and through them
/// `ExecuteTransaction`) for every table they touch.
///
/// # Errors
/// A `ValidationException` naming the API and the table.
pub(crate) fn reject_transaction_on_global(
    meta: &Metadata,
    table: &str,
    api: &str,
) -> Result<(), WireError> {
    if meta.table_global(table).is_some() {
        return Err(WireError::validation(format!(
            "{api} is not supported on table `{table}`: transactions are not supported on a \
             multi-Region strongly consistent global table"
        )));
    }
    Ok(())
}

/// MRSC restriction (ADR 0075 V8/3.7): TTL cannot be enabled on a global
/// table. (Disabling is the no-op it always was, so it stays allowed.)
///
/// # Errors
/// A `ValidationException` naming the table.
pub(crate) fn reject_ttl_on_global(meta: &Metadata, table: &str) -> Result<(), WireError> {
    if meta.table_global(table).is_some() {
        return Err(WireError::validation(format!(
            "UpdateTimeToLive cannot enable TTL on table `{table}`: TTL is not supported on a \
             multi-Region strongly consistent global table"
        )));
    }
    Ok(())
}

/// Whether `table` holds at least one live item (a DynamoDB tombstone row is
/// not an item). A quorum scan over the whole table, page by page, stopping at
/// the first live row.
async fn table_has_items<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    table: &str,
) -> Result<bool, WireError> {
    let mut cursor: Vec<u8> = Vec::new();
    loop {
        let rows = ctx
            .cp_scan(
                table,
                cursor.clone(),
                None,
                Some(EMPTY_CHECK_PAGE),
                false,
                ReadConsistency::Strong,
            )
            .await
            .map_err(|e| internal(&format!("scanning table `{table}` for emptiness: {e}")))?;
        for (_, value) in &rows {
            if wire::decode_stored_item(value)?.is_some() {
                return Ok(true);
            }
        }
        if rows.len() < EMPTY_CHECK_PAGE {
            return Ok(false);
        }
        let mut next = rows.last().expect("a full page is non-empty").0.clone();
        next.push(0x00);
        cursor = next;
    }
}

/// `UpdateTable` with `ReplicaUpdates` (+ optional witness) and
/// `MultiRegionConsistency: STRONG`: convert an **empty** regional table into
/// a multi-Region strongly consistent global table (ADR 0075 section 3.5).
///
/// Proposes one [`MetaCommand::ConvertTableToGlobal`] (set the spec and pin
/// every tablet's placement in one apply), waits for it to be visible, and
/// returns the `TableDescription`. The preferred-leader Region is **the
/// receiving node's own Region** (plan decision D3: AWS has no wire field to
/// choose it; `SetGlobalPreferredLeader` re-points it later).
///
/// # Errors
/// The named `ValidationException`s listed in the module doc;
/// `ResourceNotFoundException` for an unknown table.
pub(crate) async fn update_table_global<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    table: &str,
    update: GlobalTableUpdate,
) -> Result<String, WireError> {
    // 1. The gate (the emit-site check; see the module doc). A closed verdict
    // first re-reads this node's applied view, since the feeder runs on a tick
    // and a finalize this node already applied may not have reached the handle.
    let features = &ctx.edge.version().features;
    if !features.is_open(Gate::GlobalTables) {
        features.update(&ctx.effective_metadata());
        if !features.is_open(Gate::GlobalTables) {
            return Err(update.closed_gate_error());
        }
    }

    // 2. The table.
    let meta = ctx.metadata_fresh().await;
    if !meta.has_table_schema(table) {
        return Err(registry_error(animus_dynamo::RegistryError::NoSuchTable(
            table.to_owned(),
        )));
    }

    // 3. The request shape.
    let request = update.validate()?;

    // 4. The cluster-dependent rules.
    if meta.table_global(table).is_some() {
        return Err(WireError::validation(format!(
            "UpdateTable: table `{table}` is already a global table; the Regions of a \
             multi-Region strongly consistent table are fixed at creation and cannot be changed"
        )));
    }
    let me = ctx.env.node_id();
    let Some(local) = meta
        .members
        .get(&me)
        .and_then(|m| m.labels.get(REGION_LABEL))
        .cloned()
    else {
        return Err(WireError::validation(format!(
            "UpdateTable: this node carries no `{REGION_LABEL}` label, so it has no Region to \
             be the table's own Region"
        )));
    };
    let regions = request.regions(&local);
    let distinct: BTreeSet<&String> = regions.iter().collect();
    if distinct.len() != regions.len() {
        return Err(WireError::validation(format!(
            "UpdateTable: the table's own Region `{local}` (the Region of the node serving this \
             request) must not be named in ReplicaUpdates or GlobalTableWitnessUpdates"
        )));
    }
    let known: BTreeSet<String> = meta
        .members
        .values()
        .filter(|m| m.status == NodeStatus::Active)
        .filter_map(|m| m.labels.get(REGION_LABEL).cloned())
        .collect();
    for region in &regions {
        if !known.contains(region) {
            return Err(WireError::validation(format!(
                "UpdateTable: Region `{region}` is not a Region of this cluster (no active \
                 member carries `{REGION_LABEL}={region}`; known Regions: {})",
                known.iter().cloned().collect::<Vec<_>>().join(", ")
            )));
        }
    }
    if table_status(&meta, table) != "ACTIVE" {
        return Err(WireError::validation(format!(
            "UpdateTable: table `{table}` is not ACTIVE"
        )));
    }
    if meta.table_ttl(table).is_some() {
        return Err(WireError::validation(format!(
            "UpdateTable: table `{table}` has TTL enabled; a multi-Region strongly consistent \
             table does not support TTL (disable it first)"
        )));
    }
    if meta
        .table_indexes(table)
        .iter()
        .any(|d| d.kind == IndexKind::Local)
    {
        return Err(WireError::validation(format!(
            "UpdateTable: table `{table}` has a LocalSecondaryIndex; a multi-Region strongly \
             consistent table does not support local secondary indexes"
        )));
    }
    if table_has_items(ctx, table).await? {
        return Err(WireError::validation(format!(
            "UpdateTable: table `{table}` must be empty to become a multi-Region strongly \
             consistent global table"
        )));
    }

    // The conversion.
    let spec = GlobalTableSpec {
        consistency: MultiRegionConsistency::Strong,
        regions,
        witness: request.witness,
        preferred_leader_region: local,
    };
    spec.validate()
        .map_err(|e| WireError::validation(format!("UpdateTable: {}", e.message())))?;
    let deadline = ctx.env.now().saturating_add(SCHEMA_COMMIT_TIMEOUT);
    loop {
        ctx.propose_schema(&MetaCommand::ConvertTableToGlobal {
            table: table.to_owned(),
            spec: spec.clone(),
        })
        .await;
        let fresh = ctx.metadata_fresh().await;
        match fresh.table_global(table) {
            Some(g) if *g == spec => {
                return describe_table_wrapped(&fresh, table, "TableDescription");
            }
            // A concurrent conversion (a different request) won the race.
            Some(_) => {
                return Err(WireError::validation(format!(
                    "UpdateTable: table `{table}` is already a global table; the Regions of a \
                     multi-Region strongly consistent table are fixed at creation and cannot be \
                     changed"
                )));
            }
            None => {}
        }
        if ctx.env.now() >= deadline {
            return Err(internal(
                "UpdateTable (ReplicaUpdates) did not commit to the control plane in time (no \
                 leader reachable, or the cluster refused the conversion)",
            ));
        }
        ctx.env.sleep(SCHEMA_POLL_INTERVAL).await;
    }
}
