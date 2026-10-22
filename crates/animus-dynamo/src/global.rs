//! The DynamoDB **global tables** wire surface (ADR 0075 section 5, G-01 stage
//! G-c): `UpdateTable`'s `ReplicaUpdates` / `GlobalTableWitnessUpdates` /
//! `MultiRegionConsistency`, `DescribeTable`'s `GlobalTableVersion` /
//! `Replicas` / `MultiRegionConsistency` / `GlobalTableWitnesses`, and the
//! rejection of the legacy 2017.11.29 operations.
//!
//! **Pure**, like the rest of this crate: nothing here sees the replicated
//! catalog, the cluster's Regions or the gate. Division of labour:
//!
//! 1. [`decode_update_table_global`] (called by `wire::decode_update_table`)
//!    only *extracts* what the request said into a [`GlobalTableUpdate`],
//!    recording any malformed shape or combined change as data
//!    ([`GlobalTableUpdate::shape_error`] / `conflicting_change`) instead of
//!    failing. That keeps the **gate-closed rejection byte-identical** to the
//!    pre-G-c text ([`GlobalTableUpdate::closed_gate_error`]): `animusd`
//!    checks `Gate::GlobalTables` first, and only an open gate lets a
//!    malformed `ReplicaUpdates` body produce its own named error.
//! 2. [`GlobalTableUpdate::validate`] (called by `animusd` once the gate is
//!    open) applies every request-shape rule and yields the
//!    [`GlobalTableRequest`] the edge then checks against the cluster
//!    (local Region label, known Regions, empty table, no TTL/LSI).
//!
//! ## Per-replica read overrides (cluster version 8)
//!
//! A `ReplicaUpdates` Create or Update may carry
//! `ProvisionedThroughputOverride.ReadCapacityUnits` and per-GSI overrides on
//! an MREC table ([`ReplicaSettings`], [`ReadChange`]); an Update is accepted
//! only when it changes one of these. `OnDemandThroughputOverride`,
//! `TableClassOverride` and `KMSMasterKeyId` stay refused with a precise
//! message, and every per-replica setting is refused on an MRSC table.
//! [`MrecRequest::needs_overrides_gate`] tells the edge to check
//! `Gate::MrecReplicaOverrides` first.
//!
//! ## AWS field names (verified)
//!
//! The field and enum names below were checked against the AWS SDK for Go v2
//! service model for DynamoDB (`aws-sdk-go-v2/service/dynamodb`, generated
//! from the service's API model), which is reachable from the sandbox when the
//! AWS documentation site is not: `UpdateTableInput.{ReplicaUpdates
//! []ReplicationGroupUpdate, GlobalTableWitnessUpdates
//! []GlobalTableWitnessGroupUpdate, MultiRegionConsistency
//! (EVENTUAL|STRONG)}`; `ReplicationGroupUpdate.{Create, Update, Delete}`;
//! `CreateReplicationGroupMemberAction.{RegionName, KMSMasterKeyId,
//! ProvisionedThroughputOverride, OnDemandThroughputOverride,
//! GlobalSecondaryIndexes, TableClassOverride}`;
//! `GlobalTableWitnessGroupUpdate.{Create, Delete}` with
//! `CreateGlobalTableWitnessGroupMemberAction.RegionName` /
//! `DeleteGlobalTableWitnessGroupMemberAction.RegionName`;
//! `TableDescription.{GlobalTableVersion, Replicas
//! []ReplicaDescription, MultiRegionConsistency, GlobalTableWitnesses
//! []GlobalTableWitnessDescription}`;
//! `GlobalTableWitnessDescription.{RegionName, WitnessStatus
//! (CREATING|DELETING|ACTIVE)}`; `ReplicaDescription.{RegionName,
//! ReplicaStatus}`. What the model does **not** carry is the error *text*
//! AWS returns for a rejected request (ADR 0075 N1): every rejection here is a
//! `ValidationException` with AnimusDB's own wording, kept in this module in
//! one place so a later docs re-read can adjust it.

use serde_json::{Map, Value};

use crate::limits::{MRSC_MAX_WITNESSES, MRSC_MIN_FULL_REPLICAS, MRSC_REQUIRED_REGIONS};
use crate::wire::WireError;

/// The `X-Amz-Target` operation names of the **legacy (2017.11.29)** global
/// tables control plane (ADR 0075 V1/section 5.3). Every one is rejected by
/// name ([`legacy_global_table_operation_error`]); the supported surface is
/// `UpdateTable`'s `ReplicaUpdates` (version 2019.11.21).
pub const LEGACY_GLOBAL_TABLE_OPERATIONS: [&str; 6] = [
    "CreateGlobalTable",
    "UpdateGlobalTable",
    "DescribeGlobalTable",
    "DescribeGlobalTableSettings",
    "ListGlobalTables",
    "UpdateGlobalTableSettings",
];

/// The `ValidationException` for a legacy (2017.11.29) global-tables
/// operation `op`: wire-level and ungated (rejecting an operation is not a
/// new surface).
#[must_use]
pub fn legacy_global_table_operation_error(op: &str) -> WireError {
    WireError::validation(format!(
        "{op}: this is a legacy (version 2017.11.29) global tables operation, which is not \
         supported; use global tables version {} instead (UpdateTable with ReplicaUpdates)",
        crate::limits::GLOBAL_TABLE_VERSION
    ))
}

/// Per-replica settings of `CreateReplicationGroupMemberAction` /
/// `UpdateReplicationGroupMemberAction` whose **underlying table feature does
/// not exist** in AnimusDB (ADR 0075 section 5.1, Q6 as amended 2026-10-21):
/// `(field, why)`. A request carrying one is refused naming the missing
/// feature, in the same spirit as `SSESpecification`'s own refusal.
/// `ProvisionedThroughputOverride` and the per-GSI list are *not* here: read
/// capacity is modelled (MREC tables only, see [`ReplicaSettings`]).
const UNSUPPORTED_REPLICA_FIELDS: [(&str, &str); 3] = [
    (
        "KMSMasterKeyId",
        "server-side encryption (SSESpecification) is not supported, so there is no \
         per-replica KMS key to set",
    ),
    (
        "OnDemandThroughputOverride",
        "on-demand maximum throughput (OnDemandThroughput) is not supported, so there is no \
         per-replica maximum read throughput to override",
    ),
    (
        "TableClassOverride",
        "table classes (TableClass) are not supported, so there is no per-replica table \
         class to override",
    ),
];

/// The `why` text of an [`UNSUPPORTED_REPLICA_FIELDS`] field.
fn unsupported_reason(field: &str) -> &'static str {
    UNSUPPORTED_REPLICA_FIELDS
        .iter()
        .find(|(f, _)| *f == field)
        .map_or("this setting is not supported", |(_, why)| why)
}

/// What a request says about one read-capacity override scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadChange {
    /// The scope was not mentioned: keep what is there.
    #[default]
    Unchanged,
    /// `ProvisionedThroughputOverride` present without `ReadCapacityUnits`:
    /// AWS's "use the source table's settings", i.e. clear the override.
    Clear,
    /// Override the scope's read capacity to this many units (>= 1).
    Set(u64),
}

/// The per-replica settings one `Create` / `Update` action carries.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicaSettings {
    /// `ProvisionedThroughputOverride` on the table.
    pub table: ReadChange,
    /// `GlobalSecondaryIndexes[].ProvisionedThroughputOverride`, by index
    /// name, in request order (an element naming no override is dropped).
    pub indexes: Vec<(String, ReadChange)>,
    /// Present settings whose table feature AnimusDB lacks
    /// ([`UNSUPPORTED_REPLICA_FIELDS`]); the first is named in the refusal.
    pub unsupported: Vec<&'static str>,
}

impl ReplicaSettings {
    /// Whether the action changes any supported setting.
    #[must_use]
    pub fn changes_read_capacity(&self) -> bool {
        self.table != ReadChange::Unchanged
            || self
                .indexes
                .iter()
                .any(|(_, c)| *c != ReadChange::Unchanged)
    }

    /// Whether the action names no setting at all (supported or not).
    #[must_use]
    pub fn is_blank(&self) -> bool {
        !self.changes_read_capacity() && self.unsupported.is_empty()
    }

    /// The refusal for the first present setting whose table feature does not
    /// exist, if any.
    fn unsupported_error(&self, action: &str) -> Option<WireError> {
        let field = self.unsupported.first()?;
        Some(WireError::validation(format!(
            "UpdateTable: ReplicaUpdates {action} does not support `{field}`: {}",
            unsupported_reason(field)
        )))
    }

    /// The MRSC refusal: every per-replica setting is refused there, the
    /// missing-feature ones by their reason, read capacity by its own.
    fn mrsc_error(&self, action: &str) -> Option<WireError> {
        if let Some(e) = self.unsupported_error(action) {
            return Some(e);
        }
        if self.changes_read_capacity() {
            return Some(WireError::validation(format!(
                "UpdateTable: ReplicaUpdates {action} does not support \
                 `ProvisionedThroughputOverride`: per-Region read capacity overrides are \
                 supported on multi-Region eventually consistent tables only (the Regions of a \
                 multi-Region strongly consistent table are one stretched cluster whose read \
                 budget is shared by every Region's replicas)"
            )));
        }
        None
    }
}

/// `UpdateTable` keys that are a *different* change from a global-table
/// conversion; combining them is "more than one change in one call" (Fork C,
/// extended). `BillingMode: PAY_PER_REQUEST` alone is tolerated alongside
/// (see [`conflicting_change`]), exactly as for every other change.
const CONFLICTING_UPDATE_TABLE_KEYS: [&str; 4] = [
    "GlobalSecondaryIndexUpdates",
    "StreamSpecification",
    "ProvisionedThroughput",
    "SSESpecification",
];

/// One element of `UpdateTable.ReplicaUpdates` (`ReplicationGroupUpdate`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaAction {
    /// `Create{RegionName, ..}` with its per-replica settings.
    Create {
        /// The `RegionName`.
        region: String,
        /// The per-replica settings.
        settings: ReplicaSettings,
    },
    /// `Update{RegionName, ..}`: change an existing replica's settings (MREC
    /// read-capacity overrides; refused on MRSC).
    Update {
        /// The `RegionName`.
        region: String,
        /// The per-replica settings.
        settings: ReplicaSettings,
    },
    /// `Delete{RegionName}`: replicas of an MRSC table cannot be removed
    /// (rejected there); an MREC replica can (ADR 0075 section 5.1).
    Delete {
        /// The `RegionName`.
        region: String,
    },
}

/// One element of `UpdateTable.GlobalTableWitnessUpdates`
/// (`GlobalTableWitnessGroupUpdate`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WitnessAction {
    /// `Create{RegionName}`.
    Create {
        /// The witness `RegionName`.
        region: String,
    },
    /// `Delete{RegionName}`: valid only as the witness half of removing two
    /// Regions from an MRSC table ([`GlobalTableUpdate::validate_revert`]).
    /// `region` is the `RegionName` when the request carried one.
    Delete {
        /// The `RegionName`, if present.
        region: Option<String>,
    },
}

/// What an `UpdateTable` carrying any of `ReplicaUpdates`,
/// `GlobalTableWitnessUpdates` or `MultiRegionConsistency` said, undigested
/// (see the module doc for why validation is a separate, later step).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalTableUpdate {
    /// `ReplicaUpdates` elements, in request order. Empty when absent.
    pub replica_actions: Vec<ReplicaAction>,
    /// `GlobalTableWitnessUpdates` elements, in request order.
    pub witness_actions: Vec<WitnessAction>,
    /// The raw `MultiRegionConsistency` string, `None` when absent.
    pub consistency: Option<String>,
    /// Whether `ReplicaUpdates` was present at all (even empty).
    pub has_replica_updates: bool,
    /// Whether `GlobalTableWitnessUpdates` was present at all.
    pub has_witness_updates: bool,
    /// The message for a *different* change combined into the same call
    /// (stream, index, throughput, ...), if any.
    pub conflicting_change: Option<String>,
    /// The message for a malformed shape (a non-array `ReplicaUpdates`, an
    /// element that is not an object, a `Create` without `RegionName`, ...).
    pub shape_error: Option<String>,
}

/// What a *valid* conversion request asks for, before the cluster-dependent
/// checks: the Regions to add beside the table's own, and the witness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalTableRequest {
    /// The `Create` Regions, in request order (one or two).
    pub replicas: Vec<String>,
    /// The witness Region, if the request used the two-replicas-plus-witness
    /// form.
    pub witness: Option<String>,
}

impl GlobalTableRequest {
    /// Every Region of the resulting table in declaration order: the local
    /// (receiving) Region first, then the `Create`s, then the witness.
    #[must_use]
    pub fn regions(&self, local: &str) -> Vec<String> {
        std::iter::once(local.to_owned())
            .chain(self.replicas.iter().cloned())
            .chain(self.witness.iter().cloned())
            .collect()
    }
}

/// Whether `obj` (an `UpdateTable` body) is a global-table request: it
/// carries `ReplicaUpdates`, `GlobalTableWitnessUpdates` or
/// `MultiRegionConsistency`.
#[must_use]
pub fn is_global_table_update(obj: &Map<String, Value>) -> bool {
    obj.contains_key("ReplicaUpdates")
        || obj.contains_key("GlobalTableWitnessUpdates")
        || obj.contains_key("MultiRegionConsistency")
}

/// Why `obj` combined with a global-table change is "more than one change",
/// if it is. A bare `BillingMode: PAY_PER_REQUEST` is tolerated (common
/// SDK/CLI habit); any other `BillingMode` is a conflict.
fn conflicting_change(obj: &Map<String, Value>) -> Option<String> {
    for key in CONFLICTING_UPDATE_TABLE_KEYS {
        if obj.contains_key(key) {
            return Some(format!(
                "UpdateTable supports exactly one change per call: ReplicaUpdates, \
                 GlobalTableWitnessUpdates and MultiRegionConsistency cannot be combined with {key}"
            ));
        }
    }
    if let Some(mode) = obj.get("BillingMode").and_then(Value::as_str)
        && mode != "PAY_PER_REQUEST"
    {
        return Some(format!(
            "UpdateTable supports exactly one change per call: ReplicaUpdates, \
             GlobalTableWitnessUpdates and MultiRegionConsistency cannot be combined with \
             BillingMode `{mode}`"
        ));
    }
    None
}

fn region_name(action: &Map<String, Value>, what: &str) -> Result<String, String> {
    match action.get("RegionName") {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(format!("{what} requires a string `RegionName`")),
    }
}

/// `ProvisionedThroughputOverride` -> a [`ReadChange`]: absent or `null` is
/// [`ReadChange::Unchanged`]; an object without `ReadCapacityUnits` clears;
/// `ReadCapacityUnits` must be a positive integer (AWS `PositiveLongObject`).
fn decode_read_override(parent: &Map<String, Value>, what: &str) -> Result<ReadChange, String> {
    let v = match parent.get("ProvisionedThroughputOverride") {
        None | Some(Value::Null) => return Ok(ReadChange::Unchanged),
        Some(v) => v,
    };
    let o = v
        .as_object()
        .ok_or_else(|| format!("{what} `ProvisionedThroughputOverride` must be an object"))?;
    match o.get("ReadCapacityUnits") {
        None | Some(Value::Null) => Ok(ReadChange::Clear),
        Some(Value::Number(n)) => match n.as_u64() {
            Some(units) if units >= 1 => Ok(ReadChange::Set(units)),
            _ => Err(format!(
                "{what} `ProvisionedThroughputOverride.ReadCapacityUnits` must be a positive \
                 integer"
            )),
        },
        Some(_) => Err(format!(
            "{what} `ProvisionedThroughputOverride.ReadCapacityUnits` must be a positive integer"
        )),
    }
}

/// The per-replica settings of a `Create` / `Update` action body.
fn decode_replica_settings(
    action: &Map<String, Value>,
    what: &str,
) -> Result<ReplicaSettings, String> {
    let mut settings = ReplicaSettings {
        table: decode_read_override(action, what)?,
        ..ReplicaSettings::default()
    };
    for (field, _) in UNSUPPORTED_REPLICA_FIELDS {
        if action.get(field).is_some_and(|v| !v.is_null()) {
            settings.unsupported.push(field);
        }
    }
    match action.get("GlobalSecondaryIndexes") {
        None | Some(Value::Null) => {}
        Some(Value::Array(list)) => {
            let mut seen_indexes: Vec<String> = Vec::new();
            if list.is_empty() {
                return Err(format!(
                    "{what} `GlobalSecondaryIndexes` must contain at least one element"
                ));
            }
            for element in list {
                let o = element.as_object().ok_or_else(|| {
                    format!("each {what} `GlobalSecondaryIndexes` element must be an object")
                })?;
                let name = match o.get("IndexName") {
                    Some(Value::String(n)) if !n.is_empty() => n.clone(),
                    _ => {
                        return Err(format!(
                            "each {what} `GlobalSecondaryIndexes` element requires a string \
                             `IndexName`"
                        ));
                    }
                };
                if o.get("OnDemandThroughputOverride")
                    .is_some_and(|v| !v.is_null())
                    && !settings.unsupported.contains(&"OnDemandThroughputOverride")
                {
                    settings.unsupported.push("OnDemandThroughputOverride");
                }
                let change = decode_read_override(o, what)?;
                if seen_indexes.contains(&name) {
                    return Err(format!(
                        "{what} `GlobalSecondaryIndexes` names index `{name}` more than once"
                    ));
                }
                seen_indexes.push(name.clone());
                if change != ReadChange::Unchanged {
                    settings.indexes.push((name, change));
                }
            }
        }
        Some(_) => {
            return Err(format!("{what} `GlobalSecondaryIndexes` must be a list"));
        }
    }
    Ok(settings)
}

fn decode_replica_actions(value: &Value) -> Result<Vec<ReplicaAction>, String> {
    let list = value
        .as_array()
        .ok_or_else(|| "`ReplicaUpdates` must be a list".to_owned())?;
    let mut out = Vec::with_capacity(list.len());
    for element in list {
        let element = element
            .as_object()
            .ok_or_else(|| "each `ReplicaUpdates` element must be an object".to_owned())?;
        // Exactly one of Create / Update / Delete per element.
        let present: Vec<&str> = ["Create", "Update", "Delete", "Disassociate"]
            .into_iter()
            .filter(|k| element.contains_key(*k))
            .collect();
        match present.as_slice() {
            ["Create"] => {
                let create = element["Create"]
                    .as_object()
                    .ok_or_else(|| "`ReplicaUpdates` `Create` must be an object".to_owned())?;
                let region = region_name(create, "`ReplicaUpdates` `Create`")?;
                let settings = decode_replica_settings(create, "`ReplicaUpdates` `Create`")?;
                out.push(ReplicaAction::Create { region, settings });
            }
            ["Update"] => {
                let update = element["Update"]
                    .as_object()
                    .ok_or_else(|| "`ReplicaUpdates` `Update` must be an object".to_owned())?;
                let region = region_name(update, "`ReplicaUpdates` `Update`")?;
                let settings = decode_replica_settings(update, "`ReplicaUpdates` `Update`")?;
                out.push(ReplicaAction::Update { region, settings });
            }
            ["Delete"] => {
                let delete = element["Delete"]
                    .as_object()
                    .ok_or_else(|| "`ReplicaUpdates` `Delete` must be an object".to_owned())?;
                let region = region_name(delete, "`ReplicaUpdates` `Delete`")?;
                out.push(ReplicaAction::Delete { region });
            }
            ["Disassociate"] => {
                // `ReplicationGroupUpdate` also defines `Disassociate`
                // (service model); it is a different feature, named.
                return Err("`ReplicaUpdates` `Disassociate` is not supported".to_owned());
            }
            _ => {
                return Err(
                    "each `ReplicaUpdates` element must have exactly one of `Create`, `Update` or \
                     `Delete`"
                        .to_owned(),
                );
            }
        }
    }
    Ok(out)
}

fn decode_witness_actions(value: &Value) -> Result<Vec<WitnessAction>, String> {
    let list = value
        .as_array()
        .ok_or_else(|| "`GlobalTableWitnessUpdates` must be a list".to_owned())?;
    let mut out = Vec::with_capacity(list.len());
    for element in list {
        let element = element.as_object().ok_or_else(|| {
            "each `GlobalTableWitnessUpdates` element must be an object".to_owned()
        })?;
        let present: Vec<&str> = ["Create", "Delete"]
            .into_iter()
            .filter(|k| element.contains_key(*k))
            .collect();
        match present.as_slice() {
            ["Create"] => {
                let create = element["Create"].as_object().ok_or_else(|| {
                    "`GlobalTableWitnessUpdates` `Create` must be an object".to_owned()
                })?;
                let region = region_name(create, "`GlobalTableWitnessUpdates` `Create`")?;
                out.push(WitnessAction::Create { region });
            }
            ["Delete"] => {
                let region = element["Delete"]
                    .as_object()
                    .and_then(|d| d.get("RegionName"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                out.push(WitnessAction::Delete { region });
            }
            _ => {
                return Err(
                    "each `GlobalTableWitnessUpdates` element must have exactly one of `Create` \
                     or `Delete`"
                        .to_owned(),
                );
            }
        }
    }
    Ok(out)
}

/// Extract a [`GlobalTableUpdate`] from an `UpdateTable` body already known
/// to satisfy [`is_global_table_update`]. Never fails (see the module doc).
#[must_use]
pub fn decode_update_table_global(obj: &Map<String, Value>) -> GlobalTableUpdate {
    let mut update = GlobalTableUpdate {
        replica_actions: Vec::new(),
        witness_actions: Vec::new(),
        consistency: None,
        has_replica_updates: obj.contains_key("ReplicaUpdates"),
        has_witness_updates: obj.contains_key("GlobalTableWitnessUpdates"),
        conflicting_change: conflicting_change(obj),
        shape_error: None,
    };
    if let Some(v) = obj.get("ReplicaUpdates") {
        match decode_replica_actions(v) {
            Ok(a) => update.replica_actions = a,
            Err(e) => update.shape_error = Some(e),
        }
    }
    if update.shape_error.is_none()
        && let Some(v) = obj.get("GlobalTableWitnessUpdates")
    {
        match decode_witness_actions(v) {
            Ok(a) => update.witness_actions = a,
            Err(e) => update.shape_error = Some(e),
        }
    }
    match obj.get("MultiRegionConsistency") {
        None | Some(Value::Null) => {}
        Some(Value::String(s)) => update.consistency = Some(s.clone()),
        Some(_) => {
            update
                .shape_error
                .get_or_insert_with(|| "`MultiRegionConsistency` must be a string".to_owned());
        }
    }
    update
}

impl GlobalTableUpdate {
    /// The rejection while `Gate::GlobalTables` is closed: byte-identical to
    /// the pre-G-c text for `ReplicaUpdates` (`UpdateTable: ReplicaUpdates is
    /// not supported`), and the same shape for the two keys that arrive with
    /// it.
    #[must_use]
    pub fn closed_gate_error(&self) -> WireError {
        let key = if self.has_replica_updates {
            "ReplicaUpdates"
        } else if self.has_witness_updates {
            "GlobalTableWitnessUpdates"
        } else {
            "MultiRegionConsistency"
        };
        WireError::validation(format!("UpdateTable: {key} is not supported"))
    }

    /// Apply every request-shape rule (ADR 0075 section 5.1, V3/V4/V6/V8):
    /// the cluster-dependent ones (the local Region label, known Regions,
    /// empty table, TTL/LSI, already-global) are the edge's.
    ///
    /// # Errors
    /// A named `ValidationException`.
    pub fn validate(&self) -> Result<GlobalTableRequest, WireError> {
        if let Some(e) = &self.shape_error {
            return Err(WireError::validation(format!("UpdateTable: {e}")));
        }
        if let Some(e) = &self.conflicting_change {
            return Err(WireError::validation(e.clone()));
        }
        // `Update`/`Delete` are refused whatever the consistency says: they are
        // never supported, so the named reason beats the EVENTUAL one.
        for action in &self.replica_actions {
            match action {
                ReplicaAction::Update { settings, .. } => {
                    return Err(settings.mrsc_error("Update").unwrap_or_else(|| {
                        WireError::validation(
                            "UpdateTable: ReplicaUpdates Update is not supported on a \
                             multi-Region strongly consistent table (it names no setting, and \
                             no per-replica setting can be changed there)",
                        )
                    }));
                }
                ReplicaAction::Delete { .. } => {
                    return Err(WireError::validation(
                        "UpdateTable: ReplicaUpdates Delete is not supported: replicas cannot \
                         be removed from a multi-Region strongly consistent table, and no other \
                         kind of global table is supported yet",
                    ));
                }
                ReplicaAction::Create { settings, .. } => {
                    if let Some(e) = settings.mrsc_error("Create") {
                        return Err(e);
                    }
                }
            }
        }
        if self
            .witness_actions
            .iter()
            .any(|a| matches!(a, WitnessAction::Delete { .. }))
        {
            return Err(WireError::validation(
                "UpdateTable: GlobalTableWitnessUpdates Delete is not supported: the \
                 witness of a multi-Region strongly consistent table cannot be removed",
            ));
        }
        // The consistency mode is fixed at creation and a Create is required
        // for it to mean anything (V3). Absent means EVENTUAL (the AWS
        // default), which is MREC: not supported until stage G-d, rejected by
        // name so an SDK default is never silently accepted as something
        // else.
        match self.consistency.as_deref() {
            Some("STRONG") => {}
            None | Some("EVENTUAL") => {
                return Err(WireError::validation(
                    "UpdateTable: multi-Region eventual consistency (MultiRegionConsistency \
                     EVENTUAL, which is the default when it is not specified) is not supported \
                     yet; specify MultiRegionConsistency STRONG to create a multi-Region \
                     strongly consistent (MRSC) global table (ADR 0075 stage G-d)",
                ));
            }
            Some(other) => {
                return Err(WireError::validation(format!(
                    "UpdateTable: unsupported MultiRegionConsistency `{other}` (expected \
                     EVENTUAL or STRONG)"
                )));
            }
        }
        let mut replicas: Vec<String> = Vec::new();
        for action in &self.replica_actions {
            match action {
                ReplicaAction::Create { region, .. } => replicas.push(region.clone()),
                // Already refused by the first loop; kept total, not trusted.
                ReplicaAction::Update { .. } | ReplicaAction::Delete { .. } => {
                    return Err(WireError::validation(
                        "UpdateTable: ReplicaUpdates Update and Delete are not supported on a \
                         multi-Region strongly consistent table",
                    ));
                }
            }
        }
        let mut witnesses: Vec<String> = Vec::new();
        for action in &self.witness_actions {
            match action {
                WitnessAction::Create { region } => witnesses.push(region.clone()),
                WitnessAction::Delete { .. } => {
                    return Err(WireError::validation(
                        "UpdateTable: GlobalTableWitnessUpdates Delete is not supported: the \
                         witness of a multi-Region strongly consistent table cannot be removed",
                    ));
                }
            }
        }
        if witnesses.len() > MRSC_MAX_WITNESSES {
            return Err(WireError::validation(format!(
                "UpdateTable: a multi-Region strongly consistent table has at most \
                 {MRSC_MAX_WITNESSES} witness Region"
            )));
        }
        if replicas.is_empty() {
            return Err(WireError::validation(
                "UpdateTable: MultiRegionConsistency STRONG requires ReplicaUpdates Create \
                 actions",
            ));
        }
        // V6: exactly three Regions in total (the table's own, the Creates,
        // the witness) — three replicas, or two replicas plus one witness.
        let total = 1 + replicas.len() + witnesses.len();
        const { assert!(MRSC_MIN_FULL_REPLICAS + MRSC_MAX_WITNESSES == MRSC_REQUIRED_REGIONS) };
        if total != MRSC_REQUIRED_REGIONS {
            return Err(WireError::validation(format!(
                "UpdateTable: a multi-Region strongly consistent global table spans exactly \
                 {MRSC_REQUIRED_REGIONS} Regions: the table's own Region plus two ReplicaUpdates \
                 Create actions, or plus one Create and one GlobalTableWitnessUpdates Create \
                 (this request names {total})"
            )));
        }
        let mut seen: Vec<&String> = Vec::new();
        for region in replicas.iter().chain(witnesses.iter()) {
            if region.is_empty() {
                return Err(WireError::validation(
                    "UpdateTable: RegionName must not be empty",
                ));
            }
            if seen.contains(&region) {
                return Err(WireError::validation(format!(
                    "UpdateTable: Region `{region}` is named more than once (replicas and the \
                     witness must each be in a different Region)"
                )));
            }
            seen.push(region);
        }
        Ok(GlobalTableRequest {
            replicas,
            witness: witnesses.pop(),
        })
    }
}

/// What a *valid* MRSC replica-removal request asks for (ADR 0075's
/// 2026-10-21 amendment): the one Region that remains, which becomes the
/// regular single-Region table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevertRequest {
    /// The Region that is not deleted.
    pub keep_region: String,
}

/// What a *valid* MREC replica request asks for, before the
/// cluster-dependent checks: the peers to add and/or update, or the peers to
/// remove (a delete is never mixed with a create or an update).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MrecRequest {
    /// `Create` Regions (peer names), in request order.
    pub creates: Vec<String>,
    /// `Update` Regions (existing replicas, the local one included), in
    /// request order. Each changes at least one read-capacity override.
    pub updates: Vec<String>,
    /// `Delete` Regions (peer names), in request order.
    pub deletes: Vec<String>,
    /// The per-replica settings of every `Create` and `Update` Region.
    pub settings: std::collections::BTreeMap<String, ReplicaSettings>,
}

impl GlobalTableUpdate {
    /// Whether the request carries any `Delete` (replica or witness): on an
    /// MRSC table that is the replica-removal path
    /// ([`validate_revert`](Self::validate_revert)).
    #[must_use]
    pub fn has_deletes(&self) -> bool {
        self.replica_actions
            .iter()
            .any(|a| matches!(a, ReplicaAction::Delete { .. }))
            || self
                .witness_actions
                .iter()
                .any(|a| matches!(a, WitnessAction::Delete { .. }))
    }

    /// Apply the request-shape rules of removing Regions from an MRSC table
    /// with Regions `regions` (the full replicas and the witness, as recorded)
    /// and optional `witness` (ADR 0075's 2026-10-21 amendment; AWS: "You
    /// cannot delete a single replica or a witness from a MRSC global table.
    /// You can delete two replicas or delete one replica and a witness ...,
    /// converting the remaining replica to a single-Region DynamoDB table").
    /// Exactly two of the table's three Regions must be named, the remaining
    /// one must be a replica (not the witness), and nothing else may be in
    /// the request.
    ///
    /// # Errors
    /// A named `ValidationException`.
    pub fn validate_revert(
        &self,
        regions: &[String],
        witness: Option<&str>,
    ) -> Result<RevertRequest, WireError> {
        if let Some(e) = &self.shape_error {
            return Err(WireError::validation(format!("UpdateTable: {e}")));
        }
        if let Some(e) = &self.conflicting_change {
            return Err(WireError::validation(e.clone()));
        }
        if self.consistency.is_some() {
            return Err(WireError::validation(
                "UpdateTable: MultiRegionConsistency is only valid when ReplicaUpdates Create \
                 actions are specified",
            ));
        }
        let mut deleted: Vec<String> = Vec::new();
        let mut witness_deleted = false;
        for action in &self.replica_actions {
            match action {
                ReplicaAction::Create { .. } => {
                    return Err(WireError::validation(
                        "UpdateTable: ReplicaUpdates cannot mix Create and Delete actions in one \
                         call, and replicas cannot be added to an existing multi-Region strongly \
                         consistent table",
                    ));
                }
                ReplicaAction::Update { .. } => {
                    return Err(WireError::validation(
                        "UpdateTable: ReplicaUpdates Update is not supported on a multi-Region strongly \
                         consistent table (no per-replica setting can be overridden)",
                    ));
                }
                ReplicaAction::Delete { region } => {
                    if region.is_empty() {
                        return Err(WireError::validation(
                            "UpdateTable: RegionName must not be empty",
                        ));
                    }
                    if witness == Some(region.as_str()) {
                        return Err(WireError::validation(format!(
                            "UpdateTable: Region `{region}` is the witness of this table; remove \
                             it with GlobalTableWitnessUpdates Delete, not ReplicaUpdates Delete"
                        )));
                    }
                    if !regions.contains(region) {
                        return Err(WireError::validation(format!(
                            "UpdateTable: Region `{region}` is not a replica Region of this \
                             table (its Regions are: {})",
                            regions.join(", ")
                        )));
                    }
                    if deleted.contains(region) {
                        return Err(WireError::validation(format!(
                            "UpdateTable: Region `{region}` is named more than once"
                        )));
                    }
                    deleted.push(region.clone());
                }
            }
        }
        for action in &self.witness_actions {
            match action {
                WitnessAction::Create { .. } => {
                    return Err(WireError::validation(
                        "UpdateTable: GlobalTableWitnessUpdates cannot mix Create and Delete \
                         actions, and a witness cannot be added to an existing multi-Region \
                         strongly consistent table",
                    ));
                }
                WitnessAction::Delete { region } => {
                    let Some(w) = witness else {
                        return Err(WireError::validation(
                            "UpdateTable: GlobalTableWitnessUpdates Delete: this table has no \
                             witness",
                        ));
                    };
                    if witness_deleted {
                        return Err(WireError::validation(
                            "UpdateTable: the witness is named more than once",
                        ));
                    }
                    if let Some(r) = region
                        && r != w
                    {
                        return Err(WireError::validation(format!(
                            "UpdateTable: Region `{r}` is not the witness of this table (the \
                             witness is `{w}`)"
                        )));
                    }
                    witness_deleted = true;
                    deleted.push(w.to_owned());
                }
            }
        }
        match deleted.len() {
            0 => Err(WireError::validation(
                "UpdateTable: no Region to delete named",
            )),
            1 => Err(WireError::validation(
                "UpdateTable: a single replica or the witness cannot be deleted from a \
                 multi-Region strongly consistent table; delete two replicas, or one replica \
                 and the witness, which converts the remaining replica to a single-Region table",
            )),
            2 => {
                let mut left = regions.iter().filter(|r| !deleted.contains(r));
                let keep = left.next().cloned();
                match keep {
                    Some(k) if left.next().is_none() && witness != Some(k.as_str()) => {
                        Ok(RevertRequest { keep_region: k })
                    }
                    _ => Err(WireError::validation(
                        "UpdateTable: the Region that remains must be a replica, not the \
                         witness: on a table with a witness, delete the witness and one replica",
                    )),
                }
            }
            _ => Err(WireError::validation(
                "UpdateTable: all Regions of a multi-Region strongly consistent table cannot be \
                 deleted; delete two replicas, or one replica and the witness, and keep one \
                 replica",
            )),
        }
    }

    /// Whether this request is shaped for the MREC path: the consistency is
    /// absent or `EVENTUAL` (the AWS default). `STRONG` and unknown values
    /// stay on the MRSC path ([`validate`](Self::validate)).
    #[must_use]
    pub fn is_eventual(&self) -> bool {
        matches!(self.consistency.as_deref(), None | Some("EVENTUAL"))
    }

    /// Whether the request needs `Gate::MrecReplicaOverrides` open: an
    /// `Update` action, or any read-capacity override on a `Create`. While it
    /// is closed the edge refuses exactly these, with
    /// [`overrides_gate_closed_error`](Self::overrides_gate_closed_error).
    #[must_use]
    pub fn needs_overrides_gate(&self) -> bool {
        self.replica_actions.iter().any(|a| match a {
            ReplicaAction::Update { .. } => true,
            ReplicaAction::Create { settings, .. } => settings.changes_read_capacity(),
            ReplicaAction::Delete { .. } => false,
        })
    }

    /// The rejection while `Gate::MrecReplicaOverrides` is closed.
    #[must_use]
    pub fn overrides_gate_closed_error(&self) -> WireError {
        WireError::validation(
            "UpdateTable: ReplicaUpdates Update and per-replica ProvisionedThroughputOverride \
             are not supported",
        )
    }

    /// Apply every request-shape rule of an MREC replica change (ADR 0075
    /// section 5.1): settings whose table feature does not exist are named and
    /// refused, a witness is MRSC-only, deletes are not mixed with creates or
    /// updates, `EVENTUAL` needs a `Create` (V3) or an `Update`. Cluster-
    /// dependent rules (configured peers, own Region, already a replica, the
    /// table's billing mode, index names) are the edge's.
    ///
    /// # Errors
    /// A named `ValidationException`.
    pub fn validate_mrec(&self) -> Result<MrecRequest, WireError> {
        if let Some(e) = &self.shape_error {
            return Err(WireError::validation(format!("UpdateTable: {e}")));
        }
        if let Some(e) = &self.conflicting_change {
            return Err(WireError::validation(e.clone()));
        }
        if self.has_witness_updates || !self.witness_actions.is_empty() {
            return Err(WireError::validation(
                "UpdateTable: GlobalTableWitnessUpdates is only valid with MultiRegionConsistency \
                 STRONG",
            ));
        }
        let mut req = MrecRequest::default();
        for action in &self.replica_actions {
            match action {
                ReplicaAction::Create { region, settings } => {
                    if let Some(e) = settings.unsupported_error("Create") {
                        return Err(e);
                    }
                    req.creates.push(region.clone());
                    req.settings.insert(region.clone(), settings.clone());
                }
                ReplicaAction::Update { region, settings } => {
                    if let Some(e) = settings.unsupported_error("Update") {
                        return Err(e);
                    }
                    if !settings.changes_read_capacity() {
                        return Err(WireError::validation(format!(
                            "UpdateTable: ReplicaUpdates Update for Region `{region}` names no \
                             setting to change (supported: ProvisionedThroughputOverride, \
                             GlobalSecondaryIndexes[].ProvisionedThroughputOverride)"
                        )));
                    }
                    req.updates.push(region.clone());
                    req.settings.insert(region.clone(), settings.clone());
                }
                ReplicaAction::Delete { region } => req.deletes.push(region.clone()),
            }
        }
        if !req.deletes.is_empty() && (!req.creates.is_empty() || !req.updates.is_empty()) {
            return Err(WireError::validation(
                "UpdateTable: ReplicaUpdates cannot mix Delete with Create or Update actions in \
                 one call",
            ));
        }
        if req.creates.is_empty() && req.updates.is_empty() && req.deletes.is_empty() {
            return Err(WireError::validation(
                "UpdateTable: MultiRegionConsistency EVENTUAL requires ReplicaUpdates Create \
                 actions",
            ));
        }
        if self.consistency.is_some() && req.creates.is_empty() {
            return Err(WireError::validation(
                "UpdateTable: MultiRegionConsistency is only valid when ReplicaUpdates Create \
                 actions are specified",
            ));
        }
        let mut seen: Vec<&String> = Vec::new();
        for region in req
            .creates
            .iter()
            .chain(req.updates.iter())
            .chain(req.deletes.iter())
        {
            if region.is_empty() {
                return Err(WireError::validation(
                    "UpdateTable: RegionName must not be empty",
                ));
            }
            if seen.contains(&region) {
                return Err(WireError::validation(format!(
                    "UpdateTable: Region `{region}` is named more than once"
                )));
            }
            seen.push(region);
        }
        Ok(req)
    }
}

/// A Region's status in a `DescribeTable` response. `animusd` derives it from
/// the tablets' replica sets (plan decision D4): `Active` once every tablet of
/// the table has a replica in the Region, `Creating` until then.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionStatus {
    /// Replicas are still being placed in the Region.
    Creating,
    /// Every tablet has a replica in the Region.
    Active,
    /// The replica is being removed (MREC).
    Deleting,
    /// The replica could not be created (MREC); `Delete` clears it.
    CreationFailed,
}

impl RegionStatus {
    /// The wire string. Every value is in the AWS service model's
    /// `ReplicaStatus` enum (verified 2026-10-21); `WitnessStatus` is
    /// `CREATING`/`DELETING`/`ACTIVE` only, and only MRSC (which never
    /// produces `CreationFailed`) has a witness.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            RegionStatus::Creating => "CREATING",
            RegionStatus::Active => "ACTIVE",
            RegionStatus::Deleting => "DELETING",
            RegionStatus::CreationFailed => "CREATION_FAILED",
        }
    }
}

/// One replica's read-capacity overrides, as `DescribeTable` reports them
/// (`ProvisionedThroughputOverride` / `GlobalSecondaryIndexes[]`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicaReadOverrides {
    /// The table's overridden read capacity units.
    pub table_read_units: Option<u64>,
    /// Per-GSI overridden read capacity units, by index name.
    pub index_read_units: std::collections::BTreeMap<String, u64>,
}

impl ReplicaReadOverrides {
    /// Nothing overridden (the replica object then carries neither field).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.table_read_units.is_none() && self.index_read_units.is_empty()
    }
}

fn throughput_override(units: u64) -> Value {
    let mut o = Map::new();
    o.insert("ReadCapacityUnits".into(), Value::Number(units.into()));
    Value::Object(o)
}

/// The global-table part of a `DescribeTable` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalTableDescription {
    /// Per-replica read-capacity overrides by Region (MREC; absent = none).
    pub overrides: std::collections::BTreeMap<String, ReplicaReadOverrides>,
    /// Every full replica Region (the table's own included, AWS-style), in
    /// the table's declaration order, with its status.
    pub replicas: Vec<(String, RegionStatus)>,
    /// The witness Region, if any.
    pub witness: Option<(String, RegionStatus)>,
    /// `true` for an MREC table (`MultiRegionConsistency: EVENTUAL`), `false`
    /// for MRSC (`STRONG`).
    pub eventual: bool,
}

impl GlobalTableDescription {
    /// Add `GlobalTableVersion`, `MultiRegionConsistency`, `Replicas` and
    /// (when there is a witness) `GlobalTableWitnesses` to a table
    /// description object.
    pub fn apply_to(&self, desc: &mut Map<String, Value>) {
        desc.insert(
            "GlobalTableVersion".into(),
            Value::String(crate::limits::GLOBAL_TABLE_VERSION.into()),
        );
        desc.insert(
            "MultiRegionConsistency".into(),
            Value::String(if self.eventual { "EVENTUAL" } else { "STRONG" }.into()),
        );
        desc.insert(
            "Replicas".into(),
            Value::Array(
                self.replicas
                    .iter()
                    .map(|(region, status)| {
                        let mut r = Map::new();
                        r.insert("RegionName".into(), Value::String(region.clone()));
                        r.insert(
                            "ReplicaStatus".into(),
                            Value::String(status.as_str().into()),
                        );
                        if let Some(o) = self.overrides.get(region).filter(|o| !o.is_empty()) {
                            if let Some(units) = o.table_read_units {
                                r.insert(
                                    "ProvisionedThroughputOverride".into(),
                                    throughput_override(units),
                                );
                            }
                            if !o.index_read_units.is_empty() {
                                r.insert(
                                    "GlobalSecondaryIndexes".into(),
                                    Value::Array(
                                        o.index_read_units
                                            .iter()
                                            .map(|(name, units)| {
                                                let mut g = Map::new();
                                                g.insert(
                                                    "IndexName".into(),
                                                    Value::String(name.clone()),
                                                );
                                                g.insert(
                                                    "ProvisionedThroughputOverride".into(),
                                                    throughput_override(*units),
                                                );
                                                Value::Object(g)
                                            })
                                            .collect(),
                                    ),
                                );
                            }
                        }
                        Value::Object(r)
                    })
                    .collect(),
            ),
        );
        if let Some((region, status)) = &self.witness {
            let mut w = Map::new();
            w.insert("RegionName".into(), Value::String(region.clone()));
            w.insert(
                "WitnessStatus".into(),
                Value::String(status.as_str().into()),
            );
            desc.insert(
                "GlobalTableWitnesses".into(),
                Value::Array(vec![Value::Object(w)]),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{Operation, decode_request};
    use serde_json::json;

    fn decode(body: &Value) -> Operation {
        decode_request("DynamoDB_20120810.UpdateTable", body.to_string().as_bytes())
            .expect("decodes")
    }

    fn update_of(body: &Value) -> GlobalTableUpdate {
        match decode(body) {
            Operation::UpdateTableGlobal { update, .. } => update,
            other => panic!("expected UpdateTableGlobal, got {other:?}"),
        }
    }

    fn validate_err(body: &Value) -> String {
        update_of(body)
            .validate()
            .expect_err("must be rejected")
            .message
    }

    fn create(region: &str) -> Value {
        json!({"Create": {"RegionName": region}})
    }

    #[test]
    fn three_regions_decode_and_validate() {
        let u = update_of(&json!({
            "TableName": "tbl",
            "ReplicaUpdates": [create("b"), create("c")],
            "MultiRegionConsistency": "STRONG",
        }));
        let req = u.validate().unwrap();
        assert_eq!(req.replicas, ["b", "c"]);
        assert_eq!(req.witness, None);
        assert_eq!(req.regions("a"), ["a", "b", "c"]);
    }

    #[test]
    fn two_replicas_plus_witness_decode_and_validate() {
        let u = update_of(&json!({
            "TableName": "tbl",
            "ReplicaUpdates": [create("b")],
            "GlobalTableWitnessUpdates": [create("c")],
            "MultiRegionConsistency": "STRONG",
        }));
        let req = u.validate().unwrap();
        assert_eq!(req.replicas, ["b"]);
        assert_eq!(req.witness.as_deref(), Some("c"));
        assert_eq!(req.regions("a"), ["a", "b", "c"]);
    }

    #[test]
    fn the_op_names_its_table() {
        let op = decode(&json!({
            "TableName": "tbl",
            "ReplicaUpdates": [create("b"), create("c")],
            "MultiRegionConsistency": "STRONG",
        }));
        assert_eq!(op.table(), Some("tbl"));
    }

    #[test]
    fn eventual_and_absent_consistency_are_rejected_by_name() {
        for body in [
            json!({"TableName":"tbl","ReplicaUpdates":[create("b"),create("c")]}),
            json!({"TableName":"tbl","ReplicaUpdates":[create("b"),create("c")],
                   "MultiRegionConsistency":"EVENTUAL"}),
        ] {
            let msg = validate_err(&body);
            assert!(
                msg.contains("EVENTUAL") && msg.contains("not supported yet"),
                "{msg}"
            );
            assert!(msg.contains("MultiRegionConsistency STRONG"), "{msg}");
        }
        let msg = validate_err(
            &json!({"TableName":"tbl","ReplicaUpdates":[create("b"),create("c")],
                                       "MultiRegionConsistency":"WHATEVER"}),
        );
        assert!(
            msg.contains("unsupported MultiRegionConsistency `WHATEVER`"),
            "{msg}"
        );
    }

    #[test]
    fn exactly_three_regions() {
        for (replicas, witnesses, total) in [
            (vec!["b"], vec![], 2),
            (vec!["b", "c", "d"], vec![], 4),
            (vec!["b", "c"], vec!["d"], 4),
        ] {
            let mut body = json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
                "ReplicaUpdates": replicas.iter().map(|r| create(r)).collect::<Vec<_>>()});
            if !witnesses.is_empty() {
                body["GlobalTableWitnessUpdates"] =
                    Value::Array(witnesses.iter().map(|r| create(r)).collect());
            }
            let msg = validate_err(&body);
            assert!(msg.contains("exactly 3 Regions"), "{msg}");
            assert!(msg.contains(&format!("names {total}")), "{msg}");
        }
        // No Create at all with STRONG.
        let msg = validate_err(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
                                       "GlobalTableWitnessUpdates":[create("c")]}));
        assert!(msg.contains("requires ReplicaUpdates Create"), "{msg}");
    }

    #[test]
    fn at_most_one_witness() {
        let msg = validate_err(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
            "ReplicaUpdates":[create("b")],
            "GlobalTableWitnessUpdates":[create("c"), create("d")]}));
        assert!(msg.contains("at most 1 witness"), "{msg}");
    }

    #[test]
    fn duplicate_regions_are_rejected() {
        let msg = validate_err(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
            "ReplicaUpdates":[create("b"), create("b")]}));
        assert!(msg.contains("`b` is named more than once"), "{msg}");
        let msg = validate_err(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
            "ReplicaUpdates":[create("b")],
            "GlobalTableWitnessUpdates":[create("b")]}));
        assert!(msg.contains("`b` is named more than once"), "{msg}");
    }

    #[test]
    fn mrsc_refuses_every_per_replica_setting_naming_it() {
        // Settings whose table feature does not exist: the feature is named.
        for field in [
            "KMSMasterKeyId",
            "OnDemandThroughputOverride",
            "TableClassOverride",
        ] {
            let mut c = json!({"RegionName": "b"});
            c[field] = json!({});
            let msg = validate_err(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
                "ReplicaUpdates":[{"Create": c}, create("c")]}));
            assert!(msg.contains(&format!("`{field}`")), "{field}: {msg}");
            assert!(msg.contains("so there is no"), "{field}: {msg}");
        }
        // Read capacity is modelled, but on MREC only.
        for c in [
            json!({"RegionName":"b","ProvisionedThroughputOverride":{"ReadCapacityUnits":5}}),
            json!({"RegionName":"b","GlobalSecondaryIndexes":[
                {"IndexName":"g","ProvisionedThroughputOverride":{"ReadCapacityUnits":5}}]}),
        ] {
            let msg = validate_err(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
                "ReplicaUpdates":[{"Create": c}, create("c")]}));
            assert!(msg.contains("`ProvisionedThroughputOverride`"), "{msg}");
            assert!(msg.contains("eventually consistent tables only"), "{msg}");
        }
    }

    #[test]
    fn replica_update_and_delete_and_witness_delete_are_rejected() {
        let msg = validate_err(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
            "ReplicaUpdates":[{"Update":{"RegionName":"b"}}]}));
        assert!(
            msg.contains("ReplicaUpdates Update is not supported"),
            "{msg}"
        );
        let msg = validate_err(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
            "ReplicaUpdates":[{"Delete":{"RegionName":"b"}}]}));
        assert!(
            msg.contains("ReplicaUpdates Delete is not supported"),
            "{msg}"
        );
        let msg = validate_err(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
            "ReplicaUpdates":[create("b")],
            "GlobalTableWitnessUpdates":[{"Delete":{"RegionName":"c"}}]}));
        assert!(
            msg.contains("GlobalTableWitnessUpdates Delete is not supported"),
            "{msg}"
        );
    }

    fn del(region: &str) -> Value {
        json!({"Delete": {"RegionName": region}})
    }

    /// The AWS-documented MRSC replica-removal matrix (ADR 0075, 2026-10-21):
    /// exactly two of the three Regions, the remaining one a replica.
    #[test]
    fn revert_matrix_on_a_three_replica_table() {
        let regions: Vec<String> = ["a", "b", "c"].map(str::to_owned).to_vec();
        let revert = |body: Value| update_of(&body).validate_revert(&regions, None);
        let ok = |reps: Vec<Value>| {
            revert(json!({"TableName":"tbl","ReplicaUpdates":reps}))
                .unwrap()
                .keep_region
        };
        assert_eq!(ok(vec![del("b"), del("c")]), "a");
        assert_eq!(ok(vec![del("c"), del("a")]), "b");
        assert_eq!(ok(vec![del("a"), del("b")]), "c");
        let err = |body: Value| revert(body).expect_err("rejected").message;
        // A single replica.
        let m = err(json!({"TableName":"tbl","ReplicaUpdates":[del("b")]}));
        assert!(
            m.contains("single replica or the witness cannot be deleted"),
            "{m}"
        );
        // All three.
        let m = err(json!({"TableName":"tbl","ReplicaUpdates":[del("a"), del("b"), del("c")]}));
        assert!(m.contains("all Regions"), "{m}");
        // Unknown Region.
        let m = err(json!({"TableName":"tbl","ReplicaUpdates":[del("b"), del("zz")]}));
        assert!(m.contains("`zz` is not a replica Region"), "{m}");
        // Duplicate.
        let m = err(json!({"TableName":"tbl","ReplicaUpdates":[del("b"), del("b")]}));
        assert!(m.contains("more than once"), "{m}");
        // Create mixed with Delete.
        let m = err(json!({"TableName":"tbl","ReplicaUpdates":[del("b"), create("d")]}));
        assert!(m.contains("cannot mix Create and Delete"), "{m}");
        let m = err(
            json!({"TableName":"tbl","ReplicaUpdates":[del("b"), del("c")],
            "GlobalTableWitnessUpdates":[create("d")]}),
        );
        assert!(m.contains("cannot mix Create and Delete"), "{m}");
        // A witness Delete on a table that has none.
        let m = err(json!({"TableName":"tbl","ReplicaUpdates":[del("b")],
            "GlobalTableWitnessUpdates":[{"Delete":{"RegionName":"c"}}]}));
        assert!(m.contains("has no witness"), "{m}");
        // MultiRegionConsistency only goes with Create.
        let m = err(json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
            "ReplicaUpdates":[del("b"), del("c")]}));
        assert!(m.contains("only valid when ReplicaUpdates Create"), "{m}");
        // Update is refused by name.
        let m = err(json!({"TableName":"tbl","ReplicaUpdates":[{"Update":{"RegionName":"b"}}]}));
        assert!(m.contains("Update is not supported"), "{m}");
    }

    #[test]
    fn revert_matrix_on_a_witness_table() {
        // a, b replicas; c witness.
        let regions: Vec<String> = ["a", "b", "c"].map(str::to_owned).to_vec();
        let revert = |body: Value| update_of(&body).validate_revert(&regions, Some("c"));
        // One replica + the witness: the other replica remains.
        for wdel in [json!({"Delete":{"RegionName":"c"}}), json!({"Delete":{}})] {
            let r = revert(json!({"TableName":"tbl","ReplicaUpdates":[del("b")],
                "GlobalTableWitnessUpdates":[wdel]}))
            .unwrap();
            assert_eq!(r.keep_region, "a");
        }
        let err = |body: Value| revert(body).expect_err("rejected").message;
        // Two replicas would leave the witness.
        let m = err(json!({"TableName":"tbl","ReplicaUpdates":[del("a"), del("b")]}));
        assert!(m.contains("must be a replica, not the witness"), "{m}");
        // A lone witness Delete.
        let m = err(
            json!({"TableName":"tbl","GlobalTableWitnessUpdates":[{"Delete":{"RegionName":"c"}}]}),
        );
        assert!(
            m.contains("single replica or the witness cannot be deleted"),
            "{m}"
        );
        // The witness named as a replica.
        let m = err(json!({"TableName":"tbl","ReplicaUpdates":[del("a"), del("c")]}));
        assert!(m.contains("is the witness of this table"), "{m}");
        // A witness Delete naming the wrong Region.
        let m = err(json!({"TableName":"tbl","ReplicaUpdates":[del("b")],
            "GlobalTableWitnessUpdates":[{"Delete":{"RegionName":"a"}}]}));
        assert!(m.contains("is not the witness"), "{m}");
        // Everything.
        let m = err(
            json!({"TableName":"tbl","ReplicaUpdates":[del("a"), del("b")],
            "GlobalTableWitnessUpdates":[{"Delete":{"RegionName":"c"}}]}),
        );
        assert!(m.contains("all Regions"), "{m}");
    }

    #[test]
    fn has_deletes_sees_both_kinds() {
        assert!(update_of(&json!({"TableName":"tbl","ReplicaUpdates":[del("b")]})).has_deletes());
        assert!(
            update_of(&json!({"TableName":"tbl","GlobalTableWitnessUpdates":[{"Delete":{}}]}))
                .has_deletes()
        );
        assert!(
            !update_of(&json!({"TableName":"tbl","ReplicaUpdates":[create("b")]})).has_deletes()
        );
    }

    #[test]
    fn malformed_shapes_decode_and_fail_validation_not_decode() {
        for body in [
            json!({"TableName":"tbl","ReplicaUpdates":"x"}),
            json!({"TableName":"tbl","ReplicaUpdates":[1]}),
            json!({"TableName":"tbl","ReplicaUpdates":[{}]}),
            json!({"TableName":"tbl","ReplicaUpdates":[{"Create":{}}]}),
            json!({"TableName":"tbl","ReplicaUpdates":[{"Create":{"RegionName":"b"},"Delete":{}}]}),
            json!({"TableName":"tbl","GlobalTableWitnessUpdates":{}}),
            json!({"TableName":"tbl","ReplicaUpdates":[],"MultiRegionConsistency":3}),
        ] {
            let u = update_of(&body);
            assert!(u.shape_error.is_some(), "{body}");
            assert!(
                u.validate()
                    .unwrap_err()
                    .message
                    .starts_with("UpdateTable: ")
            );
        }
    }

    #[test]
    fn disassociate_is_named_and_refused() {
        let body =
            json!({"TableName":"tbl","ReplicaUpdates":[{"Disassociate":{"RegionName":"b"}}]});
        let msg = validate_err(&body);
        assert!(msg.contains("Disassociate"), "{msg}");
    }

    /// Wire names pinned against the AWS botocore service model
    /// (`ReplicaStatus`, `WitnessStatus`, `MultiRegionConsistency`), 2026-10-21.
    #[test]
    fn wire_enum_values_are_in_the_service_model() {
        const REPLICA_STATUS: [&str; 10] = [
            "CREATING",
            "CREATION_FAILED",
            "UPDATING",
            "DELETING",
            "ACTIVE",
            "REGION_DISABLED",
            "INACCESSIBLE_ENCRYPTION_CREDENTIALS",
            "ARCHIVING",
            "ARCHIVED",
            "REPLICATION_NOT_AUTHORIZED",
        ];
        const WITNESS_STATUS: [&str; 3] = ["CREATING", "DELETING", "ACTIVE"];
        for st in [
            RegionStatus::Creating,
            RegionStatus::Active,
            RegionStatus::Deleting,
            RegionStatus::CreationFailed,
        ] {
            assert!(REPLICA_STATUS.contains(&st.as_str()), "{}", st.as_str());
            if st != RegionStatus::CreationFailed {
                assert!(WITNESS_STATUS.contains(&st.as_str()), "{}", st.as_str());
            }
        }
    }

    #[test]
    fn a_combined_change_is_rejected_by_name() {
        for key in CONFLICTING_UPDATE_TABLE_KEYS {
            let mut body = json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
                "ReplicaUpdates":[create("b"), create("c")]});
            body[key] = json!({});
            let msg = validate_err(&body);
            assert!(msg.contains("exactly one change per call"), "{msg}");
            assert!(msg.contains(key), "{msg}");
        }
        // A bare PAY_PER_REQUEST restatement is tolerated; anything else is not.
        let ok = update_of(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
            "BillingMode":"PAY_PER_REQUEST",
            "ReplicaUpdates":[create("b"), create("c")]}));
        assert!(ok.validate().is_ok());
        let msg = validate_err(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
            "BillingMode":"PROVISIONED",
            "ReplicaUpdates":[create("b"), create("c")]}));
        assert!(msg.contains("BillingMode `PROVISIONED`"), "{msg}");
    }

    /// The gate-closed text is the pre-G-c text, byte for byte.
    #[test]
    fn the_closed_gate_error_is_the_pre_g_c_text() {
        let u = update_of(&json!({"TableName":"tbl",
            "ReplicaUpdates":[{"Create":{"RegionName":"us-west-2"}}]}));
        let e = u.closed_gate_error();
        assert_eq!(e.code, "ValidationException");
        assert_eq!(e.message, "UpdateTable: ReplicaUpdates is not supported");
        let u = update_of(&json!({"TableName":"tbl","GlobalTableWitnessUpdates":[create("c")]}));
        assert_eq!(
            u.closed_gate_error().message,
            "UpdateTable: GlobalTableWitnessUpdates is not supported"
        );
        let u = update_of(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG"}));
        assert_eq!(
            u.closed_gate_error().message,
            "UpdateTable: MultiRegionConsistency is not supported"
        );
    }

    #[test]
    fn legacy_operations_are_rejected_by_name() {
        for op in LEGACY_GLOBAL_TABLE_OPERATIONS {
            let e = decode_request(&format!("DynamoDB_20120810.{op}"), br#"{}"#).unwrap_err();
            assert_eq!(e.code, "ValidationException", "{op}");
            assert!(e.message.starts_with(&format!("{op}: ")), "{}", e.message);
            assert!(e.message.contains("2017.11.29"), "{}", e.message);
            assert!(e.message.contains("2019.11.21"), "{}", e.message);
        }
    }

    fn mrec_of(body: &Value) -> Result<MrecRequest, WireError> {
        update_of(body).validate_mrec()
    }

    fn mrec_err(body: &Value) -> String {
        mrec_of(body).expect_err("must be rejected").message
    }

    #[test]
    fn mrec_create_and_update_carry_read_capacity_overrides() {
        let r = mrec_of(&json!({"TableName":"tbl","ReplicaUpdates":[
            {"Create":{"RegionName":"b",
                "ProvisionedThroughputOverride":{"ReadCapacityUnits":50},
                "GlobalSecondaryIndexes":[
                    {"IndexName":"g1","ProvisionedThroughputOverride":{"ReadCapacityUnits":7}},
                    {"IndexName":"g2"}]}},
            {"Update":{"RegionName":"c","ProvisionedThroughputOverride":{}}},
            {"Update":{"RegionName":"d","GlobalSecondaryIndexes":[
                {"IndexName":"g1","ProvisionedThroughputOverride":{}}]}},
        ]}))
        .unwrap();
        assert_eq!(r.creates, ["b"]);
        assert_eq!(r.updates, ["c", "d"]);
        let b = &r.settings["b"];
        assert_eq!(b.table, ReadChange::Set(50));
        assert_eq!(b.indexes, [("g1".to_string(), ReadChange::Set(7))]);
        assert_eq!(r.settings["c"].table, ReadChange::Clear);
        assert_eq!(
            r.settings["d"].indexes,
            [("g1".to_string(), ReadChange::Clear)]
        );
        // An Update-only request needs no MultiRegionConsistency and no Create.
        assert!(r.deletes.is_empty());
    }

    #[test]
    fn mrec_unsupported_settings_name_the_missing_feature() {
        for (field, feature) in [
            ("KMSMasterKeyId", "SSESpecification"),
            ("OnDemandThroughputOverride", "OnDemandThroughput"),
            ("TableClassOverride", "TableClass"),
        ] {
            for action in ["Create", "Update"] {
                let mut c = json!({"RegionName": "b"});
                c[field] = json!("x");
                let msg = mrec_err(&json!({"TableName":"tbl","ReplicaUpdates":[{action: c}]}));
                assert!(
                    msg.contains(&format!(
                        "ReplicaUpdates {action} does not support `{field}`"
                    )),
                    "{msg}"
                );
                assert!(msg.contains(feature), "{msg}");
                assert!(
                    !msg.contains("per-replica overrides are not supported"),
                    "{msg}"
                );
            }
        }
        // A GSI-level on-demand override is the same refusal.
        let msg = mrec_err(&json!({"TableName":"tbl","ReplicaUpdates":[{"Update":{
            "RegionName":"b","GlobalSecondaryIndexes":[
                {"IndexName":"g","OnDemandThroughputOverride":{"MaxReadRequestUnits":1}}]}}]}));
        assert!(msg.contains("`OnDemandThroughputOverride`"), "{msg}");
        // An Update naming only a missing-feature setting is that refusal, not a blank one.
        let msg = mrec_err(&json!({"TableName":"tbl","ReplicaUpdates":[
            {"Update":{"RegionName":"b","TableClassOverride":"STANDARD_INFREQUENT_ACCESS"}}]}));
        assert!(msg.contains("table classes"), "{msg}");
    }

    #[test]
    fn mrec_update_shape_rules() {
        let msg = mrec_err(&json!({"TableName":"tbl","ReplicaUpdates":[
            {"Update":{"RegionName":"b"}}]}));
        assert!(msg.contains("names no setting to change"), "{msg}");
        let msg = mrec_err(&json!({"TableName":"tbl","ReplicaUpdates":[
            {"Update":{"RegionName":"b","ProvisionedThroughputOverride":{"ReadCapacityUnits":1}}},
            {"Delete":{"RegionName":"c"}}]}));
        assert!(msg.contains("cannot mix Delete"), "{msg}");
        let msg = mrec_err(&json!({"TableName":"tbl","ReplicaUpdates":[
            {"Update":{"RegionName":"b","ProvisionedThroughputOverride":{"ReadCapacityUnits":1}}},
            {"Update":{"RegionName":"b","ProvisionedThroughputOverride":{"ReadCapacityUnits":2}}}]}));
        assert!(msg.contains("`b` is named more than once"), "{msg}");
        // MultiRegionConsistency needs a Create, as before.
        let msg = mrec_err(
            &json!({"TableName":"tbl","MultiRegionConsistency":"EVENTUAL",
            "ReplicaUpdates":[{"Update":{"RegionName":"b",
                "ProvisionedThroughputOverride":{"ReadCapacityUnits":1}}}]}),
        );
        assert!(
            msg.contains("only valid when ReplicaUpdates Create"),
            "{msg}"
        );
    }

    #[test]
    fn malformed_overrides_fail_validation_by_name() {
        for body in [
            json!({"ProvisionedThroughputOverride":"x"}),
            json!({"ProvisionedThroughputOverride":{"ReadCapacityUnits":0}}),
            json!({"ProvisionedThroughputOverride":{"ReadCapacityUnits":-3}}),
            json!({"ProvisionedThroughputOverride":{"ReadCapacityUnits":"5"}}),
            json!({"ProvisionedThroughputOverride":{"ReadCapacityUnits":1.5}}),
            json!({"GlobalSecondaryIndexes":[]}),
            json!({"GlobalSecondaryIndexes":"g"}),
            json!({"GlobalSecondaryIndexes":[{}]}),
            json!({"GlobalSecondaryIndexes":[{"IndexName":"g"},{"IndexName":"g"}]}),
            json!({"GlobalSecondaryIndexes":[{"IndexName":"g",
                "ProvisionedThroughputOverride":{"ReadCapacityUnits":0}}]}),
        ] {
            let mut c = body.clone();
            c["RegionName"] = json!("b");
            let u = update_of(&json!({"TableName":"tbl","ReplicaUpdates":[{"Update": c}]}));
            assert!(u.shape_error.is_some(), "{body}");
            assert!(
                u.validate_mrec()
                    .unwrap_err()
                    .message
                    .starts_with("UpdateTable: "),
                "{body}"
            );
        }
    }

    #[test]
    fn the_overrides_gate_only_guards_update_and_read_overrides() {
        let needs = |body: Value| update_of(&body).needs_overrides_gate();
        assert!(!needs(
            json!({"TableName":"tbl","ReplicaUpdates":[create("b")]})
        ));
        assert!(!needs(json!({"TableName":"tbl","ReplicaUpdates":[
            {"Delete":{"RegionName":"b"}}]})));
        assert!(needs(json!({"TableName":"tbl","ReplicaUpdates":[
            {"Update":{"RegionName":"b"}}]})));
        assert!(needs(
            json!({"TableName":"tbl","ReplicaUpdates":[{"Create":{
            "RegionName":"b","ProvisionedThroughputOverride":{"ReadCapacityUnits":3}}}]})
        ));
        let e = update_of(&json!({"TableName":"tbl","ReplicaUpdates":[
            {"Update":{"RegionName":"b"}}]}))
        .overrides_gate_closed_error();
        assert_eq!(e.code, "ValidationException");
    }

    #[test]
    fn description_reports_overrides_per_replica() {
        let mut desc = Map::new();
        GlobalTableDescription {
            overrides: [
                (
                    "b".to_string(),
                    ReplicaReadOverrides {
                        table_read_units: Some(50),
                        index_read_units: [("g1".to_string(), 7)].into(),
                    },
                ),
                ("c".to_string(), ReplicaReadOverrides::default()),
            ]
            .into(),
            replicas: vec![
                ("a".into(), RegionStatus::Active),
                ("b".into(), RegionStatus::Active),
                ("c".into(), RegionStatus::Active),
            ],
            witness: None,
            eventual: true,
        }
        .apply_to(&mut desc);
        assert_eq!(
            desc["Replicas"],
            json!([
                {"RegionName":"a","ReplicaStatus":"ACTIVE"},
                {"RegionName":"b","ReplicaStatus":"ACTIVE",
                 "ProvisionedThroughputOverride":{"ReadCapacityUnits":50},
                 "GlobalSecondaryIndexes":[{"IndexName":"g1",
                    "ProvisionedThroughputOverride":{"ReadCapacityUnits":7}}]},
                {"RegionName":"c","ReplicaStatus":"ACTIVE"},
            ])
        );
    }

    #[test]
    fn description_shape() {
        let mut desc = Map::new();
        GlobalTableDescription {
            overrides: Default::default(),
            replicas: vec![
                ("a".into(), RegionStatus::Active),
                ("b".into(), RegionStatus::Creating),
            ],
            witness: Some(("c".into(), RegionStatus::Active)),
            eventual: false,
        }
        .apply_to(&mut desc);
        assert_eq!(
            Value::Object(desc),
            json!({
                "GlobalTableVersion": "2019.11.21",
                "MultiRegionConsistency": "STRONG",
                "Replicas": [
                    {"RegionName":"a","ReplicaStatus":"ACTIVE"},
                    {"RegionName":"b","ReplicaStatus":"CREATING"},
                ],
                "GlobalTableWitnesses": [{"RegionName":"c","WitnessStatus":"ACTIVE"}],
            })
        );
        let mut desc = Map::new();
        GlobalTableDescription {
            overrides: Default::default(),
            replicas: vec![("a".into(), RegionStatus::Active)],
            witness: None,
            eventual: false,
        }
        .apply_to(&mut desc);
        assert!(!desc.contains_key("GlobalTableWitnesses"));
    }

    #[test]
    fn catalogue_arithmetic() {
        assert_eq!(MRSC_REQUIRED_REGIONS, 3);
        assert_eq!(MRSC_MAX_WITNESSES, 1);
        assert_eq!(MRSC_MIN_FULL_REPLICAS, 2);
        assert_eq!(crate::limits::GLOBAL_TABLE_VERSION, "2019.11.21");
    }
}
