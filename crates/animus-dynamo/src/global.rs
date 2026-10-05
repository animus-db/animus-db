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

/// `CreateReplicationGroupMemberAction` per-replica overrides AnimusDB does
/// not model (ADR 0075 section 5.1, Q6): a request carrying one is rejected
/// naming the field.
const UNSUPPORTED_REPLICA_CREATE_FIELDS: [&str; 5] = [
    "KMSMasterKeyId",
    "ProvisionedThroughputOverride",
    "OnDemandThroughputOverride",
    "GlobalSecondaryIndexes",
    "TableClassOverride",
];

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
    /// `Create{RegionName, ..}`: `overrides` names every per-replica field
    /// present that AnimusDB does not model (rejected by [`GlobalTableUpdate::
    /// validate`]).
    Create {
        /// The `RegionName`.
        region: String,
        /// Unsupported override fields present on the action.
        overrides: Vec<&'static str>,
    },
    /// `Update{..}`: no per-replica setting is overridable (rejected).
    Update,
    /// `Delete{RegionName}`: replicas of an MRSC table cannot be removed
    /// (rejected).
    Delete,
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
    /// `Delete{RegionName}`: a witness cannot be removed from an MRSC table
    /// (rejected).
    Delete,
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
        let present: Vec<&str> = ["Create", "Update", "Delete"]
            .into_iter()
            .filter(|k| element.contains_key(*k))
            .collect();
        match present.as_slice() {
            ["Create"] => {
                let create = element["Create"]
                    .as_object()
                    .ok_or_else(|| "`ReplicaUpdates` `Create` must be an object".to_owned())?;
                let region = region_name(create, "`ReplicaUpdates` `Create`")?;
                let overrides = UNSUPPORTED_REPLICA_CREATE_FIELDS
                    .into_iter()
                    .filter(|f| create.contains_key(*f))
                    .collect();
                out.push(ReplicaAction::Create { region, overrides });
            }
            ["Update"] => out.push(ReplicaAction::Update),
            ["Delete"] => out.push(ReplicaAction::Delete),
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
            ["Delete"] => out.push(WitnessAction::Delete),
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
                ReplicaAction::Create { region, overrides } => {
                    if let Some(field) = overrides.first() {
                        return Err(WireError::validation(format!(
                            "UpdateTable: ReplicaUpdates Create does not support `{field}` \
                             (per-replica overrides are not supported)"
                        )));
                    }
                    replicas.push(region.clone());
                }
                ReplicaAction::Update => {
                    return Err(WireError::validation(
                        "UpdateTable: ReplicaUpdates Update is not supported (no per-replica \
                         setting can be overridden)",
                    ));
                }
                ReplicaAction::Delete => {
                    return Err(WireError::validation(
                        "UpdateTable: ReplicaUpdates Delete is not supported: replicas cannot \
                         be removed from a multi-Region strongly consistent table, and no other \
                         kind of global table is supported yet",
                    ));
                }
            }
        }
        let mut witnesses: Vec<String> = Vec::new();
        for action in &self.witness_actions {
            match action {
                WitnessAction::Create { region } => witnesses.push(region.clone()),
                WitnessAction::Delete => {
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
        debug_assert!(MRSC_MIN_FULL_REPLICAS + MRSC_MAX_WITNESSES == MRSC_REQUIRED_REGIONS);
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

/// A Region's status in a `DescribeTable` response. `animusd` derives it from
/// the tablets' replica sets (plan decision D4): `Active` once every tablet of
/// the table has a replica in the Region, `Creating` until then.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionStatus {
    /// Replicas are still being placed in the Region.
    Creating,
    /// Every tablet has a replica in the Region.
    Active,
}

impl RegionStatus {
    /// The wire string (`ReplicaStatus` / `WitnessStatus` share these two
    /// values).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            RegionStatus::Creating => "CREATING",
            RegionStatus::Active => "ACTIVE",
        }
    }
}

/// The global-table part of a `DescribeTable` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalTableDescription {
    /// Every full replica Region (the table's own included, AWS-style), in
    /// the table's declaration order, with its status.
    pub replicas: Vec<(String, RegionStatus)>,
    /// The witness Region, if any.
    pub witness: Option<(String, RegionStatus)>,
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
            Value::String("STRONG".into()),
        );
        desc.insert(
            "Replicas".into(),
            Value::Array(
                self.replicas
                    .iter()
                    .map(|(region, status)| {
                        let mut r = Map::new();
                        r.insert("RegionName".into(), Value::String(region.clone()));
                        r.insert("ReplicaStatus".into(), Value::String(status.as_str().into()));
                        Value::Object(r)
                    })
                    .collect(),
            ),
        );
        if let Some((region, status)) = &self.witness {
            let mut w = Map::new();
            w.insert("RegionName".into(), Value::String(region.clone()));
            w.insert("WitnessStatus".into(), Value::String(status.as_str().into()));
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
        decode_request(
            "DynamoDB_20120810.UpdateTable",
            body.to_string().as_bytes(),
        )
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
            assert!(msg.contains("EVENTUAL") && msg.contains("not supported yet"), "{msg}");
            assert!(msg.contains("MultiRegionConsistency STRONG"), "{msg}");
        }
        let msg = validate_err(&json!({"TableName":"tbl","ReplicaUpdates":[create("b"),create("c")],
                                       "MultiRegionConsistency":"WHATEVER"}));
        assert!(msg.contains("unsupported MultiRegionConsistency `WHATEVER`"), "{msg}");
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
    fn every_unsupported_replica_override_is_rejected_naming_the_field() {
        for field in UNSUPPORTED_REPLICA_CREATE_FIELDS {
            let mut c = json!({"RegionName": "b"});
            c[field] = json!({});
            let msg = validate_err(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
                "ReplicaUpdates":[{"Create": c}, create("c")]}));
            assert!(msg.contains(&format!("`{field}`")), "{field}: {msg}");
        }
    }

    #[test]
    fn replica_update_and_delete_and_witness_delete_are_rejected() {
        let msg = validate_err(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
            "ReplicaUpdates":[{"Update":{"RegionName":"b"}}]}));
        assert!(msg.contains("ReplicaUpdates Update is not supported"), "{msg}");
        let msg = validate_err(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
            "ReplicaUpdates":[{"Delete":{"RegionName":"b"}}]}));
        assert!(msg.contains("ReplicaUpdates Delete is not supported"), "{msg}");
        let msg = validate_err(&json!({"TableName":"tbl","MultiRegionConsistency":"STRONG",
            "ReplicaUpdates":[create("b")],
            "GlobalTableWitnessUpdates":[{"Delete":{"RegionName":"c"}}]}));
        assert!(msg.contains("GlobalTableWitnessUpdates Delete is not supported"), "{msg}");
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
            assert!(u.validate().unwrap_err().message.starts_with("UpdateTable: "));
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

    #[test]
    fn description_shape() {
        let mut desc = Map::new();
        GlobalTableDescription {
            replicas: vec![
                ("a".into(), RegionStatus::Active),
                ("b".into(), RegionStatus::Creating),
            ],
            witness: Some(("c".into(), RegionStatus::Active)),
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
            replicas: vec![("a".into(), RegionStatus::Active)],
            witness: None,
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
