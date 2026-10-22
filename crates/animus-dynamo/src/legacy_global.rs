//! The **legacy (version 2017.11.29) global tables** wire surface (ADR 0075's
//! 2026-10-21 amendment, closing section 5.3's refusal): `CreateGlobalTable`,
//! `UpdateGlobalTable`, `DescribeGlobalTable`, `ListGlobalTables`,
//! `DescribeGlobalTableSettings` and `UpdateGlobalTableSettings`.
//!
//! **Pure**, like the rest of this crate: nothing here sees the replicated
//! catalog, the cluster's peers or the gate. [`decode_legacy_global`] turns a
//! request body into a [`LegacyGlobalOp`], applying every shape rule the AWS
//! service model states (required members, list bounds, `PositiveLongObject`,
//! the `BillingMode` enum) and refusing, by name, the members whose feature
//! does not exist in AnimusDB (auto scaling, table classes, per-index write
//! capacity). The response builders render the model's shapes
//! ([`global_table_body`], [`list_body`], [`settings_body`]).
//!
//! Field and shape names were checked against the AWS DynamoDB service model
//! (botocore `service-2.json`, API version 2012-08-10): `CreateGlobalTableInput
//! {GlobalTableName, ReplicationGroup []Replica{RegionName}}`,
//! `UpdateGlobalTableInput {GlobalTableName, ReplicaUpdates
//! []ReplicaUpdate{Create{RegionName}, Delete{RegionName}}}`,
//! `ListGlobalTablesInput {ExclusiveStartGlobalTableName, Limit, RegionName}`,
//! `GlobalTableDescription {ReplicationGroup, GlobalTableArn, CreationDateTime,
//! GlobalTableStatus (CREATING|ACTIVE|DELETING|UPDATING), GlobalTableName}`,
//! `UpdateGlobalTableSettingsInput {GlobalTableName, GlobalTableBillingMode,
//! GlobalTableProvisionedWriteCapacityUnits,
//! GlobalTableProvisionedWriteCapacityAutoScalingSettingsUpdate,
//! GlobalTableGlobalSecondaryIndexSettingsUpdate, ReplicaSettingsUpdate}` and
//! `ReplicaSettingsDescription`. The model carries no error *text*, so every
//! rejection of a request shape is a `ValidationException` with AnimusDB's own
//! wording (ADR 0075 N1).

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::global::RegionStatus;
use crate::limits;
use crate::wire::WireError;

/// `ListGlobalTables`' default (and maximum) page size: the model says "if the
/// parameter is not specified DynamoDB defaults to 100".
pub const LIST_DEFAULT_LIMIT: usize = 100;

/// The members of the `UpdateGlobalTableSettings` shapes that carry auto
/// scaling policy (AnimusDB has no auto scaling feature).
const AUTO_SCALING_REFUSAL: &str = "auto scaling is not supported (AnimusDB has no auto scaling \
     feature); remove the AutoScalingSettingsUpdate member, or send only \
     `AutoScalingDisabled: true`";

/// One decoded legacy operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyGlobalOp {
    /// `CreateGlobalTable`: the global table is named like its tables.
    Create { name: String, regions: Vec<String> },
    /// `UpdateGlobalTable`: Regions to add and to remove.
    Update {
        name: String,
        creates: Vec<String>,
        deletes: Vec<String>,
    },
    /// `DescribeGlobalTable`.
    Describe { name: String },
    /// `ListGlobalTables`.
    List {
        exclusive_start: Option<String>,
        limit: usize,
        region: Option<String>,
    },
    /// `DescribeGlobalTableSettings`.
    DescribeSettings { name: String },
    /// `UpdateGlobalTableSettings`.
    UpdateSettings(Box<SettingsUpdate>),
}

impl LegacyGlobalOp {
    /// The `X-Amz-Target` operation name.
    #[must_use]
    pub fn operation(&self) -> &'static str {
        match self {
            LegacyGlobalOp::Create { .. } => "CreateGlobalTable",
            LegacyGlobalOp::Update { .. } => "UpdateGlobalTable",
            LegacyGlobalOp::Describe { .. } => "DescribeGlobalTable",
            LegacyGlobalOp::List { .. } => "ListGlobalTables",
            LegacyGlobalOp::DescribeSettings { .. } => "DescribeGlobalTableSettings",
            LegacyGlobalOp::UpdateSettings(_) => "UpdateGlobalTableSettings",
        }
    }

    /// The global table (= table) the operation targets, if it has one.
    #[must_use]
    pub fn table(&self) -> Option<&str> {
        match self {
            LegacyGlobalOp::Create { name, .. }
            | LegacyGlobalOp::Update { name, .. }
            | LegacyGlobalOp::Describe { name }
            | LegacyGlobalOp::DescribeSettings { name } => Some(name),
            LegacyGlobalOp::UpdateSettings(u) => Some(&u.name),
            LegacyGlobalOp::List { .. } => None,
        }
    }

    /// Whether the operation only reads (authorization class).
    #[must_use]
    pub fn is_read(&self) -> bool {
        matches!(
            self,
            LegacyGlobalOp::Describe { .. }
                | LegacyGlobalOp::List { .. }
                | LegacyGlobalOp::DescribeSettings { .. }
        )
    }
}

/// `GlobalTableBillingMode` (the `BillingMode` enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BillingMode {
    /// `PROVISIONED`.
    Provisioned,
    /// `PAY_PER_REQUEST`.
    PayPerRequest,
}

impl BillingMode {
    /// The wire string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            BillingMode::Provisioned => "PROVISIONED",
            BillingMode::PayPerRequest => "PAY_PER_REQUEST",
        }
    }
}

/// A decoded `UpdateGlobalTableSettings` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingsUpdate {
    /// `GlobalTableName`.
    pub name: String,
    /// `GlobalTableBillingMode`, if given.
    pub billing: Option<BillingMode>,
    /// `GlobalTableProvisionedWriteCapacityUnits`, if given (>= 1).
    pub write_units: Option<u64>,
    /// `ReplicaSettingsUpdate`, in request order.
    pub replicas: Vec<ReplicaSettingsUpdate>,
}

/// One `ReplicaSettingsUpdate` element.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicaSettingsUpdate {
    /// `RegionName`.
    pub region: String,
    /// `ReplicaProvisionedReadCapacityUnits`, if given (>= 1).
    pub read_units: Option<u64>,
    /// `ReplicaGlobalSecondaryIndexSettingsUpdate[].ProvisionedReadCapacityUnits`
    /// by index name.
    pub indexes: BTreeMap<String, u64>,
}

impl SettingsUpdate {
    /// Whether the request changes anything at all.
    #[must_use]
    pub fn is_blank(&self) -> bool {
        self.billing.is_none()
            && self.write_units.is_none()
            && self
                .replicas
                .iter()
                .all(|r| r.read_units.is_none() && r.indexes.is_empty())
    }
}

// ---------------------------------------------------------------------------
// Errors (exactly the exception types the service model lists)

fn error(code: &'static str, message: impl Into<String>) -> WireError {
    WireError {
        code,
        message: message.into(),
        reasons: None,
    }
}

/// `GlobalTableAlreadyExistsException` (CreateGlobalTable).
#[must_use]
pub fn global_table_already_exists(name: &str, why: &str) -> WireError {
    error(
        "GlobalTableAlreadyExistsException",
        format!("Global table `{name}` already exists: {why}"),
    )
}

/// `GlobalTableNotFoundException`.
#[must_use]
pub fn global_table_not_found(name: &str) -> WireError {
    error(
        "GlobalTableNotFoundException",
        format!("Global table `{name}` does not exist (version 2017.11.29)"),
    )
}

/// `ReplicaAlreadyExistsException` (UpdateGlobalTable).
#[must_use]
pub fn replica_already_exists(name: &str, region: &str) -> WireError {
    error(
        "ReplicaAlreadyExistsException",
        format!("Global table `{name}` already has a replica in Region `{region}`"),
    )
}

/// `ReplicaNotFoundException` (UpdateGlobalTable, UpdateGlobalTableSettings).
#[must_use]
pub fn replica_not_found(name: &str, region: &str) -> WireError {
    error(
        "ReplicaNotFoundException",
        format!("Global table `{name}` has no replica in Region `{region}`"),
    )
}

/// `TableNotFoundException` (CreateGlobalTable, UpdateGlobalTable).
#[must_use]
pub fn table_not_found(table: &str, region: Option<&str>) -> WireError {
    match region {
        Some(r) => error(
            "TableNotFoundException",
            format!("Table `{table}` does not exist in Region `{r}`"),
        ),
        None => error(
            "TableNotFoundException",
            format!("Table `{table}` does not exist"),
        ),
    }
}

/// `IndexNotFoundException` (UpdateGlobalTableSettings).
#[must_use]
pub fn index_not_found(table: &str, index: &str) -> WireError {
    error(
        "IndexNotFoundException",
        format!("Table `{table}` has no global secondary index `{index}`"),
    )
}

/// `LimitExceededException` (CreateGlobalTable, UpdateGlobalTableSettings).
#[must_use]
pub fn limit_exceeded(message: impl Into<String>) -> WireError {
    error("LimitExceededException", message)
}

/// `ResourceInUseException` (UpdateGlobalTableSettings).
#[must_use]
pub fn resource_in_use(message: impl Into<String>) -> WireError {
    error("ResourceInUseException", message)
}

// ---------------------------------------------------------------------------
// Decoding

/// Decode one legacy operation's body. `op` is the bare operation name (one
/// of [`crate::global::LEGACY_GLOBAL_TABLE_OPERATIONS`]).
///
/// # Errors
/// `ValidationException` for a malformed or unsupported request shape.
pub fn decode_legacy_global(
    op: &str,
    obj: &Map<String, Value>,
) -> Result<LegacyGlobalOp, WireError> {
    match op {
        "CreateGlobalTable" => {
            let name = global_table_name(obj, "GlobalTableName")?;
            let list = required_list(obj, "ReplicationGroup")?;
            let mut regions = Vec::new();
            for (i, item) in list.iter().enumerate() {
                let o = item.as_object().ok_or_else(|| {
                    WireError::validation(format!("`ReplicationGroup[{i}]` must be an object"))
                })?;
                regions.push(region_name(o, &format!("ReplicationGroup[{i}]"))?);
            }
            reject_duplicates(&regions, "ReplicationGroup")?;
            Ok(LegacyGlobalOp::Create { name, regions })
        }
        "UpdateGlobalTable" => {
            let name = global_table_name(obj, "GlobalTableName")?;
            let list = required_list(obj, "ReplicaUpdates")?;
            let mut creates = Vec::new();
            let mut deletes = Vec::new();
            for (i, item) in list.iter().enumerate() {
                let what = format!("ReplicaUpdates[{i}]");
                let o = item
                    .as_object()
                    .ok_or_else(|| WireError::validation(format!("`{what}` must be an object")))?;
                let create = o.get("Create").filter(|v| !v.is_null());
                let delete = o.get("Delete").filter(|v| !v.is_null());
                let (action, key) = match (create, delete) {
                    (Some(_), Some(_)) => {
                        return Err(WireError::validation(format!(
                            "`{what}` names both Create and Delete; each element is one action"
                        )));
                    }
                    (None, None) => {
                        return Err(WireError::validation(format!(
                            "`{what}` must contain Create or Delete"
                        )));
                    }
                    (Some(a), None) => (a, "Create"),
                    (None, Some(a)) => (a, "Delete"),
                };
                let a = action.as_object().ok_or_else(|| {
                    WireError::validation(format!("`{what}.{key}` must be an object"))
                })?;
                let region = region_name(a, &format!("{what}.{key}"))?;
                if key == "Create" {
                    creates.push(region);
                } else {
                    deletes.push(region);
                }
            }
            let all: Vec<String> = creates.iter().chain(&deletes).cloned().collect();
            reject_duplicates(&all, "ReplicaUpdates")?;
            Ok(LegacyGlobalOp::Update {
                name,
                creates,
                deletes,
            })
        }
        "DescribeGlobalTable" => Ok(LegacyGlobalOp::Describe {
            name: global_table_name(obj, "GlobalTableName")?,
        }),
        "DescribeGlobalTableSettings" => Ok(LegacyGlobalOp::DescribeSettings {
            name: global_table_name(obj, "GlobalTableName")?,
        }),
        "ListGlobalTables" => {
            let exclusive_start = match obj.get("ExclusiveStartGlobalTableName") {
                None | Some(Value::Null) => None,
                Some(_) => Some(global_table_name(obj, "ExclusiveStartGlobalTableName")?),
            };
            let limit = match obj.get("Limit") {
                None | Some(Value::Null) => LIST_DEFAULT_LIMIT,
                Some(Value::Number(n)) => match n.as_u64() {
                    Some(l) if l >= 1 => usize::try_from(l)
                        .unwrap_or(LIST_DEFAULT_LIMIT)
                        .min(LIST_DEFAULT_LIMIT),
                    _ => return Err(WireError::validation("`Limit` must be at least 1")),
                },
                Some(_) => return Err(WireError::validation("`Limit` must be an integer")),
            };
            let region = match obj.get("RegionName") {
                None | Some(Value::Null) => None,
                Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
                Some(_) => {
                    return Err(WireError::validation(
                        "`RegionName` must be a non-empty string",
                    ));
                }
            };
            Ok(LegacyGlobalOp::List {
                exclusive_start,
                limit,
                region,
            })
        }
        "UpdateGlobalTableSettings" => decode_settings(obj),
        other => Err(WireError::unknown_operation(other)),
    }
}

fn global_table_name(obj: &Map<String, Value>, field: &str) -> Result<String, WireError> {
    let name = obj
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| WireError::validation(format!("missing string field `{field}`")))?;
    if limits::is_valid_table_or_index_name(name) {
        return Ok(name.to_owned());
    }
    Err(WireError::validation(format!(
        "1 validation error detected: Value '{name}' at '{field}' failed to satisfy constraint: \
         Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+ and Member must have \
         length between {} and {} inclusive",
        limits::MIN_TABLE_NAME_CHARS,
        limits::MAX_TABLE_NAME_CHARS,
    )))
}

fn required_list<'a>(
    obj: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a Vec<Value>, WireError> {
    obj.get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| WireError::validation(format!("missing list field `{field}`")))
}

fn region_name(obj: &Map<String, Value>, what: &str) -> Result<String, WireError> {
    match obj.get("RegionName") {
        Some(Value::String(s)) if !s.is_empty() => Ok(s.clone()),
        _ => Err(WireError::validation(format!(
            "`{what}.RegionName` must be a non-empty string"
        ))),
    }
}

fn reject_duplicates(regions: &[String], what: &str) -> Result<(), WireError> {
    let mut seen = std::collections::BTreeSet::new();
    for r in regions {
        if !seen.insert(r) {
            return Err(WireError::validation(format!(
                "`{what}` names Region `{r}` more than once"
            )));
        }
    }
    Ok(())
}

fn positive(obj: &Map<String, Value>, field: &str, what: &str) -> Result<Option<u64>, WireError> {
    match obj.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => match n.as_u64() {
            Some(u) if u >= 1 => Ok(Some(u)),
            _ => Err(WireError::validation(format!(
                "`{what}{field}` must be a positive integer"
            ))),
        },
        Some(_) => Err(WireError::validation(format!(
            "`{what}{field}` must be a positive integer"
        ))),
    }
}

/// An `AutoScalingSettingsUpdate` member: absent is fine; present is refused
/// unless it is the no-op `{AutoScalingDisabled: true}` (auto scaling is
/// already "disabled" in AnimusDB).
fn check_auto_scaling(obj: &Map<String, Value>, field: &str, what: &str) -> Result<(), WireError> {
    match obj.get(field) {
        None | Some(Value::Null) => Ok(()),
        Some(Value::Object(o))
            if o.len() == 1 && o.get("AutoScalingDisabled") == Some(&Value::Bool(true)) =>
        {
            Ok(())
        }
        Some(_) => Err(WireError::validation(format!(
            "UpdateGlobalTableSettings: `{what}{field}`: {AUTO_SCALING_REFUSAL}"
        ))),
    }
}

fn decode_settings(obj: &Map<String, Value>) -> Result<LegacyGlobalOp, WireError> {
    let name = global_table_name(obj, "GlobalTableName")?;
    let billing = match obj.get("GlobalTableBillingMode") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s == "PROVISIONED" => Some(BillingMode::Provisioned),
        Some(Value::String(s)) if s == "PAY_PER_REQUEST" => Some(BillingMode::PayPerRequest),
        Some(_) => {
            return Err(WireError::validation(
                "`GlobalTableBillingMode` must be PROVISIONED or PAY_PER_REQUEST",
            ));
        }
    };
    let write_units = positive(obj, "GlobalTableProvisionedWriteCapacityUnits", "")?;
    if billing == Some(BillingMode::PayPerRequest) && write_units.is_some() {
        return Err(WireError::validation(
            "`GlobalTableProvisionedWriteCapacityUnits` cannot be combined with \
             GlobalTableBillingMode PAY_PER_REQUEST",
        ));
    }
    check_auto_scaling(
        obj,
        "GlobalTableProvisionedWriteCapacityAutoScalingSettingsUpdate",
        "",
    )?;
    if let Some(v) = obj.get("GlobalTableGlobalSecondaryIndexSettingsUpdate")
        && !v.is_null()
    {
        let list = v.as_array().filter(|l| (1..=20).contains(&l.len())).ok_or_else(|| {
            WireError::validation(
                "`GlobalTableGlobalSecondaryIndexSettingsUpdate` must be a list of 1 to 20 elements",
            )
        })?;
        for (i, item) in list.iter().enumerate() {
            let what = format!("GlobalTableGlobalSecondaryIndexSettingsUpdate[{i}].");
            let o = item.as_object().ok_or_else(|| {
                WireError::validation(format!(
                    "`{}` must be an object",
                    what.trim_end_matches('.')
                ))
            })?;
            if o.get("IndexName").and_then(Value::as_str).is_none() {
                return Err(WireError::validation(format!(
                    "`{what}IndexName` must be a string"
                )));
            }
            check_auto_scaling(
                o,
                "ProvisionedWriteCapacityAutoScalingSettingsUpdate",
                &what,
            )?;
            if positive(o, "ProvisionedWriteCapacityUnits", &what)?.is_some() {
                return Err(WireError::validation(format!(
                    "UpdateGlobalTableSettings: `{what}ProvisionedWriteCapacityUnits`: a global \
                     secondary index has no write capacity of its own in AnimusDB (it shares its \
                     table's budget); set GlobalTableProvisionedWriteCapacityUnits instead"
                )));
            }
        }
    }
    let mut replicas = Vec::new();
    if let Some(v) = obj.get("ReplicaSettingsUpdate")
        && !v.is_null()
    {
        let list = v
            .as_array()
            .filter(|l| (1..=50).contains(&l.len()))
            .ok_or_else(|| {
                WireError::validation("`ReplicaSettingsUpdate` must be a list of 1 to 50 elements")
            })?;
        for (i, item) in list.iter().enumerate() {
            let what = format!("ReplicaSettingsUpdate[{i}].");
            let o = item.as_object().ok_or_else(|| {
                WireError::validation(format!(
                    "`{}` must be an object",
                    what.trim_end_matches('.')
                ))
            })?;
            let region = region_name(o, what.trim_end_matches('.'))?;
            check_auto_scaling(
                o,
                "ReplicaProvisionedReadCapacityAutoScalingSettingsUpdate",
                &what,
            )?;
            if o.get("ReplicaTableClass").is_some_and(|v| !v.is_null()) {
                return Err(WireError::validation(format!(
                    "UpdateGlobalTableSettings: `{what}ReplicaTableClass`: table classes \
                     (TableClass) are not supported, so there is no replica table class to set"
                )));
            }
            let read_units = positive(o, "ReplicaProvisionedReadCapacityUnits", &what)?;
            let mut indexes = BTreeMap::new();
            if let Some(g) = o.get("ReplicaGlobalSecondaryIndexSettingsUpdate")
                && !g.is_null()
            {
                let glist = g.as_array().filter(|l| (1..=20).contains(&l.len())).ok_or_else(|| {
                    WireError::validation(format!(
                        "`{what}ReplicaGlobalSecondaryIndexSettingsUpdate` must be a list of 1 to \
                         20 elements"
                    ))
                })?;
                for (j, gi) in glist.iter().enumerate() {
                    let gwhat = format!("{what}ReplicaGlobalSecondaryIndexSettingsUpdate[{j}].");
                    let go = gi.as_object().ok_or_else(|| {
                        WireError::validation(format!(
                            "`{}` must be an object",
                            gwhat.trim_end_matches('.')
                        ))
                    })?;
                    let index = go
                        .get("IndexName")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| {
                            WireError::validation(format!("`{gwhat}IndexName` must be a string"))
                        })?;
                    check_auto_scaling(
                        go,
                        "ProvisionedReadCapacityAutoScalingSettingsUpdate",
                        &gwhat,
                    )?;
                    if let Some(u) = positive(go, "ProvisionedReadCapacityUnits", &gwhat)? {
                        indexes.insert(index.to_owned(), u);
                    }
                }
            }
            replicas.push(ReplicaSettingsUpdate {
                region,
                read_units,
                indexes,
            });
        }
        let names: Vec<String> = replicas.iter().map(|r| r.region.clone()).collect();
        reject_duplicates(&names, "ReplicaSettingsUpdate")?;
    }
    let update = SettingsUpdate {
        name,
        billing,
        write_units,
        replicas,
    };
    if update.is_blank() {
        return Err(WireError::validation(
            "UpdateGlobalTableSettings: the request changes no setting (give \
             GlobalTableBillingMode, GlobalTableProvisionedWriteCapacityUnits or a replica read \
             capacity)",
        ));
    }
    Ok(LegacyGlobalOp::UpdateSettings(Box::new(update)))
}

// ---------------------------------------------------------------------------
// Responses

/// The `GlobalTableArn` of a global table (`arn:aws:dynamodb::0:global-table/
/// <name>`; AnimusDB has no accounts or ARN Regions, like its table ARNs).
#[must_use]
pub fn global_table_arn(name: &str) -> String {
    format!("arn:aws:dynamodb::0:global-table/{name}")
}

/// The `GlobalTableStatus` of a legacy global table from its replicas'
/// statuses (the local Region included): `CREATING` while no remote replica is
/// `ACTIVE` yet and one is being created, `DELETING` when every remote
/// replica is being removed, `UPDATING` while any other change is in flight,
/// `ACTIVE` otherwise. A `CREATION_FAILED` replica does not hold the status
/// back (it is visible in its own `ReplicaStatus`).
#[must_use]
pub fn global_table_status(replicas: &[(String, RegionStatus)], local: &str) -> &'static str {
    let remote: Vec<RegionStatus> = replicas
        .iter()
        .filter(|(r, _)| r != local)
        .map(|(_, s)| *s)
        .collect();
    let creating = remote.contains(&RegionStatus::Creating);
    let deleting = remote.contains(&RegionStatus::Deleting);
    let active = remote.contains(&RegionStatus::Active);
    if !remote.is_empty() && remote.iter().all(|s| *s == RegionStatus::Deleting) {
        "DELETING"
    } else if creating && !active {
        "CREATING"
    } else if creating || deleting {
        "UPDATING"
    } else {
        "ACTIVE"
    }
}

/// A legacy global table as `DescribeGlobalTable` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalTableView {
    /// `GlobalTableName`.
    pub name: String,
    /// `CreationDateTime` in milliseconds since the epoch.
    pub created_ms: u64,
    /// `GlobalTableStatus`.
    pub status: &'static str,
    /// `ReplicationGroup`: every Region (the local one included) with its
    /// status.
    pub replicas: Vec<(String, RegionStatus)>,
}

fn ms_timestamp(ms: u64) -> Value {
    Value::Number(
        serde_json::Number::from_f64((ms as f64) / 1000.0)
            .unwrap_or_else(|| serde_json::Number::from(0)),
    )
}

fn replica_description(region: &str, status: RegionStatus) -> Value {
    let mut r = Map::new();
    r.insert("RegionName".into(), Value::String(region.to_owned()));
    r.insert(
        "ReplicaStatus".into(),
        Value::String(status.as_str().to_owned()),
    );
    Value::Object(r)
}

impl GlobalTableView {
    /// The model's `GlobalTableDescription`.
    #[must_use]
    pub fn description(&self) -> Value {
        let mut d = Map::new();
        d.insert(
            "ReplicationGroup".into(),
            Value::Array(
                self.replicas
                    .iter()
                    .map(|(r, s)| replica_description(r, *s))
                    .collect(),
            ),
        );
        d.insert(
            "GlobalTableArn".into(),
            Value::String(global_table_arn(&self.name)),
        );
        d.insert("CreationDateTime".into(), ms_timestamp(self.created_ms));
        d.insert(
            "GlobalTableStatus".into(),
            Value::String(self.status.to_owned()),
        );
        d.insert("GlobalTableName".into(), Value::String(self.name.clone()));
        Value::Object(d)
    }
}

/// The body of `CreateGlobalTable` / `UpdateGlobalTable` /
/// `DescribeGlobalTable`: `{"GlobalTableDescription": {..}}`.
#[must_use]
pub fn global_table_body(view: &GlobalTableView) -> String {
    let mut o = Map::new();
    o.insert("GlobalTableDescription".into(), view.description());
    serde_json::to_string(&Value::Object(o)).expect("global table response serializes")
}

/// The body of `ListGlobalTables`: `GlobalTables` (name and ReplicationGroup
/// Regions) and `LastEvaluatedGlobalTableName` when the page is not the last.
#[must_use]
pub fn list_body(entries: &[(String, Vec<String>)], last_evaluated: Option<&str>) -> String {
    let mut o = Map::new();
    o.insert(
        "GlobalTables".into(),
        Value::Array(
            entries
                .iter()
                .map(|(name, regions)| {
                    let mut g = Map::new();
                    g.insert("GlobalTableName".into(), Value::String(name.clone()));
                    g.insert(
                        "ReplicationGroup".into(),
                        Value::Array(
                            regions
                                .iter()
                                .map(|r| {
                                    let mut x = Map::new();
                                    x.insert("RegionName".into(), Value::String(r.clone()));
                                    Value::Object(x)
                                })
                                .collect(),
                        ),
                    );
                    Value::Object(g)
                })
                .collect(),
        ),
    );
    if let Some(last) = last_evaluated {
        o.insert(
            "LastEvaluatedGlobalTableName".into(),
            Value::String(last.to_owned()),
        );
    }
    serde_json::to_string(&Value::Object(o)).expect("list response serializes")
}

/// One Region's settings as `DescribeGlobalTableSettings` reports them
/// (`ReplicaSettingsDescription`). Every capacity is optional: a Region's
/// number is reported only when this cluster knows it (its own table, an
/// override, or the replicated global write capacity).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSettingsView {
    /// `RegionName`.
    pub region: String,
    /// `ReplicaStatus`.
    pub status: RegionStatus,
    /// `ReplicaBillingModeSummary.BillingMode`, when known.
    pub billing: Option<BillingMode>,
    /// `ReplicaProvisionedReadCapacityUnits`, when known.
    pub read_units: Option<u64>,
    /// `ReplicaProvisionedWriteCapacityUnits`, when known.
    pub write_units: Option<u64>,
    /// `ReplicaGlobalSecondaryIndexSettings`.
    pub indexes: Vec<IndexSettingsView>,
}

/// `ReplicaGlobalSecondaryIndexSettingsDescription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSettingsView {
    /// `IndexName`.
    pub name: String,
    /// `IndexStatus` (`ACTIVE`, `CREATING`, `DELETING`).
    pub status: &'static str,
    /// `ProvisionedReadCapacityUnits`, when known.
    pub read_units: Option<u64>,
    /// `ProvisionedWriteCapacityUnits`, when known (the table's write
    /// capacity: an index shares its table's budget).
    pub write_units: Option<u64>,
}

/// The body of `DescribeGlobalTableSettings` / `UpdateGlobalTableSettings`:
/// `{"GlobalTableName", "ReplicaSettings": [..]}`. No auto scaling member is
/// emitted: auto scaling does not exist, so every optional
/// `*AutoScalingSettings` member is absent.
#[must_use]
pub fn settings_body(name: &str, replicas: &[ReplicaSettingsView]) -> String {
    let mut o = Map::new();
    o.insert("GlobalTableName".into(), Value::String(name.to_owned()));
    o.insert(
        "ReplicaSettings".into(),
        Value::Array(
            replicas
                .iter()
                .map(|r| {
                    let mut m = Map::new();
                    m.insert("RegionName".into(), Value::String(r.region.clone()));
                    m.insert(
                        "ReplicaStatus".into(),
                        Value::String(r.status.as_str().to_owned()),
                    );
                    if let Some(b) = r.billing {
                        let mut s = Map::new();
                        s.insert("BillingMode".into(), Value::String(b.as_str().to_owned()));
                        m.insert("ReplicaBillingModeSummary".into(), Value::Object(s));
                    }
                    if let Some(u) = r.read_units {
                        m.insert(
                            "ReplicaProvisionedReadCapacityUnits".into(),
                            Value::Number(u.into()),
                        );
                    }
                    if let Some(u) = r.write_units {
                        m.insert(
                            "ReplicaProvisionedWriteCapacityUnits".into(),
                            Value::Number(u.into()),
                        );
                    }
                    if !r.indexes.is_empty() {
                        m.insert(
                            "ReplicaGlobalSecondaryIndexSettings".into(),
                            Value::Array(
                                r.indexes
                                    .iter()
                                    .map(|i| {
                                        let mut x = Map::new();
                                        x.insert("IndexName".into(), Value::String(i.name.clone()));
                                        x.insert(
                                            "IndexStatus".into(),
                                            Value::String(i.status.to_owned()),
                                        );
                                        if let Some(u) = i.read_units {
                                            x.insert(
                                                "ProvisionedReadCapacityUnits".into(),
                                                Value::Number(u.into()),
                                            );
                                        }
                                        if let Some(u) = i.write_units {
                                            x.insert(
                                                "ProvisionedWriteCapacityUnits".into(),
                                                Value::Number(u.into()),
                                            );
                                        }
                                        Value::Object(x)
                                    })
                                    .collect(),
                            ),
                        );
                    }
                    Value::Object(m)
                })
                .collect(),
        ),
    );
    serde_json::to_string(&Value::Object(o)).expect("settings response serializes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn decode(op: &str, body: &Value) -> Result<LegacyGlobalOp, WireError> {
        decode_legacy_global(op, body.as_object().expect("object"))
    }

    fn err(op: &str, body: &Value) -> String {
        decode(op, body).expect_err("must be rejected").message
    }

    #[test]
    fn create_decodes_and_validates_the_replication_group() {
        let op = decode(
            "CreateGlobalTable",
            &json!({"GlobalTableName": "orders",
                    "ReplicationGroup": [{"RegionName": "us"}, {"RegionName": "eu"}]}),
        )
        .unwrap();
        assert_eq!(
            op,
            LegacyGlobalOp::Create {
                name: "orders".into(),
                regions: vec!["us".into(), "eu".into()]
            }
        );
        assert_eq!(op.operation(), "CreateGlobalTable");
        assert_eq!(op.table(), Some("orders"));
        assert!(!op.is_read());
        assert!(
            err("CreateGlobalTable", &json!({"ReplicationGroup": []})).contains("GlobalTableName")
        );
        assert!(
            err("CreateGlobalTable", &json!({"GlobalTableName": "orders"}))
                .contains("ReplicationGroup")
        );
        assert!(
            err(
                "CreateGlobalTable",
                &json!({"GlobalTableName": "ab", "ReplicationGroup": []})
            )
            .contains("GlobalTableName")
        );
        assert!(
            err(
                "CreateGlobalTable",
                &json!({"GlobalTableName": "orders",
                        "ReplicationGroup": [{"RegionName": "us"}, {"RegionName": "us"}]})
            )
            .contains("more than once")
        );
        assert!(
            err(
                "CreateGlobalTable",
                &json!({"GlobalTableName": "orders", "ReplicationGroup": [{"RegionName": ""}]})
            )
            .contains("RegionName")
        );
    }

    #[test]
    fn update_decodes_creates_and_deletes_and_rejects_ambiguous_elements() {
        let op = decode(
            "UpdateGlobalTable",
            &json!({"GlobalTableName": "orders", "ReplicaUpdates": [
                {"Create": {"RegionName": "ap"}}, {"Delete": {"RegionName": "eu"}}]}),
        )
        .unwrap();
        assert_eq!(
            op,
            LegacyGlobalOp::Update {
                name: "orders".into(),
                creates: vec!["ap".into()],
                deletes: vec!["eu".into()]
            }
        );
        let body = |u: Value| json!({"GlobalTableName": "orders", "ReplicaUpdates": [u]});
        assert!(
            err(
                "UpdateGlobalTable",
                &body(json!({"Create": {"RegionName": "a"}, "Delete": {"RegionName": "b"}}))
            )
            .contains("both Create and Delete")
        );
        assert!(err("UpdateGlobalTable", &body(json!({}))).contains("Create or Delete"));
        assert!(err("UpdateGlobalTable", &body(json!({"Create": {}}))).contains("RegionName"));
        assert!(
            err(
                "UpdateGlobalTable",
                &json!({"GlobalTableName": "orders", "ReplicaUpdates": [
                    {"Create": {"RegionName": "a"}}, {"Delete": {"RegionName": "a"}}]})
            )
            .contains("more than once")
        );
    }

    #[test]
    fn describe_and_settings_describe_take_only_the_name() {
        for (op, want) in [
            (
                "DescribeGlobalTable",
                LegacyGlobalOp::Describe {
                    name: "orders".into(),
                },
            ),
            (
                "DescribeGlobalTableSettings",
                LegacyGlobalOp::DescribeSettings {
                    name: "orders".into(),
                },
            ),
        ] {
            let got = decode(op, &json!({"GlobalTableName": "orders"})).unwrap();
            assert_eq!(got, want);
            assert!(got.is_read());
            assert!(err(op, &json!({})).contains("GlobalTableName"));
        }
    }

    #[test]
    fn list_decodes_paging_and_filter() {
        let d = decode("ListGlobalTables", &json!({})).unwrap();
        assert_eq!(
            d,
            LegacyGlobalOp::List {
                exclusive_start: None,
                limit: LIST_DEFAULT_LIMIT,
                region: None
            }
        );
        assert_eq!(d.table(), None);
        let d = decode(
            "ListGlobalTables",
            &json!({"ExclusiveStartGlobalTableName": "abc", "Limit": 2, "RegionName": "eu"}),
        )
        .unwrap();
        assert_eq!(
            d,
            LegacyGlobalOp::List {
                exclusive_start: Some("abc".into()),
                limit: 2,
                region: Some("eu".into())
            }
        );
        // The limit is capped at the default page size.
        let LegacyGlobalOp::List { limit, .. } =
            decode("ListGlobalTables", &json!({"Limit": 5000})).unwrap()
        else {
            panic!()
        };
        assert_eq!(limit, LIST_DEFAULT_LIMIT);
        assert!(err("ListGlobalTables", &json!({"Limit": 0})).contains("Limit"));
        assert!(err("ListGlobalTables", &json!({"Limit": "x"})).contains("Limit"));
        assert!(err("ListGlobalTables", &json!({"RegionName": ""})).contains("RegionName"));
        assert!(
            err(
                "ListGlobalTables",
                &json!({"ExclusiveStartGlobalTableName": "a"})
            )
            .contains("ExclusiveStartGlobalTableName")
        );
    }

    #[test]
    fn settings_update_decodes_every_supported_member() {
        let op = decode(
            "UpdateGlobalTableSettings",
            &json!({
                "GlobalTableName": "orders",
                "GlobalTableBillingMode": "PROVISIONED",
                "GlobalTableProvisionedWriteCapacityUnits": 50,
                "GlobalTableProvisionedWriteCapacityAutoScalingSettingsUpdate":
                    {"AutoScalingDisabled": true},
                "ReplicaSettingsUpdate": [{
                    "RegionName": "eu",
                    "ReplicaProvisionedReadCapacityUnits": 30,
                    "ReplicaGlobalSecondaryIndexSettingsUpdate": [
                        {"IndexName": "by_status", "ProvisionedReadCapacityUnits": 20}]
                }]
            }),
        )
        .unwrap();
        let LegacyGlobalOp::UpdateSettings(u) = op else {
            panic!()
        };
        assert_eq!(u.billing, Some(BillingMode::Provisioned));
        assert_eq!(u.write_units, Some(50));
        assert_eq!(u.replicas.len(), 1);
        assert_eq!(u.replicas[0].read_units, Some(30));
        assert_eq!(u.replicas[0].indexes["by_status"], 20);
        assert!(!u.is_blank());
        let LegacyGlobalOp::UpdateSettings(p) = decode(
            "UpdateGlobalTableSettings",
            &json!({"GlobalTableName": "orders", "GlobalTableBillingMode": "PAY_PER_REQUEST"}),
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(p.billing, Some(BillingMode::PayPerRequest));
    }

    #[test]
    fn settings_update_refuses_what_has_no_feature_behind_it() {
        let base = |extra: Value| {
            let mut o = json!({"GlobalTableName": "orders",
                               "GlobalTableProvisionedWriteCapacityUnits": 5});
            o.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            o
        };
        let m = err(
            "UpdateGlobalTableSettings",
            &base(
                json!({"GlobalTableProvisionedWriteCapacityAutoScalingSettingsUpdate":
                {"MinimumUnits": 1, "MaximumUnits": 9}}),
            ),
        );
        assert!(m.contains("auto scaling is not supported"), "{m}");
        let m = err(
            "UpdateGlobalTableSettings",
            &base(json!({"ReplicaSettingsUpdate": [{"RegionName": "eu",
                "ReplicaProvisionedReadCapacityAutoScalingSettingsUpdate":
                    {"AutoScalingDisabled": false}}]})),
        );
        assert!(m.contains("auto scaling is not supported"), "{m}");
        let m = err(
            "UpdateGlobalTableSettings",
            &base(json!({"ReplicaSettingsUpdate": [{"RegionName": "eu",
                "ReplicaTableClass": "STANDARD_INFREQUENT_ACCESS"}]})),
        );
        assert!(m.contains("table classes"), "{m}");
        let m = err(
            "UpdateGlobalTableSettings",
            &base(json!({"GlobalTableGlobalSecondaryIndexSettingsUpdate":
                [{"IndexName": "g", "ProvisionedWriteCapacityUnits": 5}]})),
        );
        assert!(m.contains("no write capacity of its own"), "{m}");
        let m = err(
            "UpdateGlobalTableSettings",
            &base(json!({"GlobalTableGlobalSecondaryIndexSettingsUpdate":
                [{"IndexName": "g", "ProvisionedWriteCapacityAutoScalingSettingsUpdate":
                    {"MaximumUnits": 5}}]})),
        );
        assert!(m.contains("auto scaling is not supported"), "{m}");
    }

    #[test]
    fn settings_update_shape_errors() {
        let e = |b: Value| err("UpdateGlobalTableSettings", &b);
        assert!(e(json!({"GlobalTableName": "orders"})).contains("changes no setting"));
        assert!(
            e(json!({"GlobalTableName": "orders", "GlobalTableBillingMode": "FREE"}))
                .contains("PROVISIONED or PAY_PER_REQUEST")
        );
        assert!(
            e(
                json!({"GlobalTableName": "orders", "GlobalTableBillingMode": "PAY_PER_REQUEST",
                     "GlobalTableProvisionedWriteCapacityUnits": 5})
            )
            .contains("cannot be combined")
        );
        assert!(
            e(json!({"GlobalTableName": "orders",
                     "GlobalTableProvisionedWriteCapacityUnits": 0}))
            .contains("positive integer")
        );
        assert!(
            e(json!({"GlobalTableName": "orders", "ReplicaSettingsUpdate": []}))
                .contains("1 to 50")
        );
        assert!(
            e(
                json!({"GlobalTableName": "orders", "ReplicaSettingsUpdate": [
                {"RegionName": "eu", "ReplicaProvisionedReadCapacityUnits": 1},
                {"RegionName": "eu", "ReplicaProvisionedReadCapacityUnits": 2}]})
            )
            .contains("more than once")
        );
        assert!(
            e(
                json!({"GlobalTableName": "orders", "ReplicaSettingsUpdate": [
                {"RegionName": "eu", "ReplicaProvisionedReadCapacityUnits": -1}]})
            )
            .contains("positive integer")
        );
        // Only a no-op replica element changes nothing.
        assert!(
            e(
                json!({"GlobalTableName": "orders", "ReplicaSettingsUpdate": [
                {"RegionName": "eu"}]})
            )
            .contains("changes no setting")
        );
    }

    #[test]
    fn unknown_operation_is_unknown() {
        let e = decode_legacy_global("NopeGlobalTable", &Map::new()).unwrap_err();
        assert_eq!(e.code, "UnknownOperationException");
    }

    #[test]
    fn error_codes_are_exactly_the_modelled_exception_types() {
        assert_eq!(
            global_table_already_exists("t", "x").code,
            "GlobalTableAlreadyExistsException"
        );
        assert_eq!(
            global_table_not_found("t").code,
            "GlobalTableNotFoundException"
        );
        assert_eq!(
            replica_already_exists("t", "r").code,
            "ReplicaAlreadyExistsException"
        );
        assert_eq!(replica_not_found("t", "r").code, "ReplicaNotFoundException");
        assert_eq!(
            table_not_found("t", Some("r")).code,
            "TableNotFoundException"
        );
        assert_eq!(index_not_found("t", "i").code, "IndexNotFoundException");
        assert_eq!(limit_exceeded("x").code, "LimitExceededException");
        assert_eq!(resource_in_use("x").code, "ResourceInUseException");
    }

    #[test]
    fn status_is_derived_from_the_replicas() {
        use RegionStatus::*;
        let st = |v: &[(&str, RegionStatus)]| {
            global_table_status(
                &v.iter()
                    .map(|(r, s)| ((*r).to_owned(), *s))
                    .collect::<Vec<_>>(),
                "us",
            )
        };
        assert_eq!(st(&[("us", Active), ("eu", Active)]), "ACTIVE");
        assert_eq!(st(&[("us", Active), ("eu", Creating)]), "CREATING");
        assert_eq!(
            st(&[("us", Active), ("eu", Active), ("ap", Creating)]),
            "UPDATING"
        );
        assert_eq!(st(&[("us", Active), ("eu", Deleting)]), "DELETING");
        assert_eq!(
            st(&[("us", Active), ("eu", Deleting), ("ap", Active)]),
            "UPDATING"
        );
        assert_eq!(st(&[("us", Active), ("eu", CreationFailed)]), "ACTIVE");
    }

    #[test]
    fn global_table_body_has_the_modelled_shape() {
        let v = GlobalTableView {
            name: "orders".into(),
            created_ms: 1_700_000_000_500,
            status: "ACTIVE",
            replicas: vec![
                ("us".into(), RegionStatus::Active),
                ("eu".into(), RegionStatus::Creating),
            ],
        };
        let body: Value = serde_json::from_str(&global_table_body(&v)).unwrap();
        let d = &body["GlobalTableDescription"];
        assert_eq!(d["GlobalTableName"], "orders");
        assert_eq!(d["GlobalTableStatus"], "ACTIVE");
        assert_eq!(
            d["GlobalTableArn"],
            "arn:aws:dynamodb::0:global-table/orders"
        );
        assert_eq!(d["CreationDateTime"], json!(1_700_000_000.5));
        assert_eq!(
            d["ReplicationGroup"],
            json!([{"RegionName": "us", "ReplicaStatus": "ACTIVE"},
                   {"RegionName": "eu", "ReplicaStatus": "CREATING"}])
        );
    }

    #[test]
    fn list_body_has_the_modelled_shape() {
        let entries = vec![("a-table".to_owned(), vec!["us".to_owned(), "eu".to_owned()])];
        let body: Value = serde_json::from_str(&list_body(&entries, Some("a-table"))).unwrap();
        assert_eq!(
            body,
            json!({"GlobalTables": [{"GlobalTableName": "a-table",
                    "ReplicationGroup": [{"RegionName": "us"}, {"RegionName": "eu"}]}],
                   "LastEvaluatedGlobalTableName": "a-table"})
        );
        let last: Value = serde_json::from_str(&list_body(&[], None)).unwrap();
        assert_eq!(last, json!({"GlobalTables": []}));
    }

    #[test]
    fn settings_body_reports_known_capacity_and_no_auto_scaling() {
        let views = vec![
            ReplicaSettingsView {
                region: "us".into(),
                status: RegionStatus::Active,
                billing: Some(BillingMode::Provisioned),
                read_units: Some(10),
                write_units: Some(50),
                indexes: vec![IndexSettingsView {
                    name: "g1".into(),
                    status: "ACTIVE",
                    read_units: Some(7),
                    write_units: Some(50),
                }],
            },
            ReplicaSettingsView {
                region: "eu".into(),
                status: RegionStatus::Creating,
                billing: None,
                read_units: None,
                write_units: None,
                indexes: Vec::new(),
            },
        ];
        let body: Value = serde_json::from_str(&settings_body("orders", &views)).unwrap();
        assert_eq!(body["GlobalTableName"], "orders");
        let r = &body["ReplicaSettings"];
        assert_eq!(
            r[0]["ReplicaBillingModeSummary"]["BillingMode"],
            "PROVISIONED"
        );
        assert_eq!(r[0]["ReplicaProvisionedReadCapacityUnits"], 10);
        assert_eq!(r[0]["ReplicaProvisionedWriteCapacityUnits"], 50);
        assert_eq!(
            r[0]["ReplicaGlobalSecondaryIndexSettings"][0]["IndexName"],
            "g1"
        );
        assert_eq!(
            r[0]["ReplicaGlobalSecondaryIndexSettings"][0]["ProvisionedReadCapacityUnits"],
            7
        );
        assert_eq!(
            r[1],
            json!({"RegionName": "eu", "ReplicaStatus": "CREATING"})
        );
        assert!(
            !serde_json::to_string(&body)
                .unwrap()
                .contains("AutoScaling")
        );
    }
}
