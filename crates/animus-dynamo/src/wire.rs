//! The DynamoDB JSON wire encoding (ADR 0006).
//!
//! DynamoDB clients speak JSON-over-HTTP: a request body like
//! `{"TableName":"t","Item":{"pk":{"S":"a"},"n":{"N":"1"}}}` and an
//! `X-Amz-Target: DynamoDB_20120810.PutItem` header naming the operation. This
//! module is the **pure, deterministic** translation between that JSON and the
//! crate's in-memory [`Item`] / [`AttributeValue`] model — no I/O, no
//! storage, no network. The transport edge (`animusd`) parses HTTP and routes
//! the decoded request through the distributed data plane; everything below the
//! HTTP edge stays on the `Env`-based paths.
//!
//! ## Supported subset
//!
//! Operations: `CreateTable`, `PutItem`, `GetItem`, `DeleteItem`, `Query`,
//! `Scan`. AttributeValue types: the scalars `S` (string), `N` (number, carried
//! as text), `B` (binary, base64), `BOOL`, `NULL`; the document types `M` (map)
//! and `L` (list); and the set types `SS`/`NS`/`BS` — matching
//! [`AttributeValue`]. `PutItem` / `DeleteItem` accept a small
//! `ConditionExpression` subset (see [`crate::condition`]) and an optional
//! `ReturnValues` (`NONE`/`ALL_OLD`). `Query` accepts a partition-key equality
//! plus an optional sort-key condition (`=`, `BETWEEN`, `begins_with`), and an
//! optional `IndexName` to query a secondary index (a composite GSI / LSI may
//! carry the sort condition; a hash-only GSI may not). `CreateTable` accepts
//! `GlobalSecondaryIndexes` (hash-only or composite) and `LocalSecondaryIndexes`
//! declarations. `Scan` reads a whole table with `Limit` / `ExclusiveStartKey`
//! pagination and an optional `FilterExpression` (the same predicate subset as
//! `ConditionExpression`). GetItem/Query/Scan accept a `ProjectionExpression` /
//! `AttributesToGet` (top-level attribute names only). Deferred (rejected with a
//! clear error): document-path projections (`a.b`), per-index projection lists,
//! and `UpdateItem`-only `ReturnValues` modes.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::condition::{ConditionExpression, SortKeyCondition};
use crate::registry::{GlobalSecondaryIndex, IndexProjection, LocalSecondaryIndex, SecondaryIndex};
use crate::{AttributeValue, Item, TableSchema};

/// The `X-Amz-Target` service+version prefix DynamoDB clients send.
pub const TARGET_PREFIX: &str = "DynamoDB_20120810.";

/// A projection: the document paths a read should return (from a
/// `ProjectionExpression` or the legacy `AttributesToGet`). `None` on an
/// operation means "all attributes"; `Some(paths)` keeps only the requested
/// paths (a requested-but-absent path is simply omitted, as in DynamoDB).
///
/// Each element is a **dotted document path** `a.b.c`: a top-level attribute name
/// optionally followed by `.`-separated map keys, so a projection can reach into
/// nested `M` (map) attributes. A path that traverses into a non-map value (or an
/// absent key) yields nothing for that path. List-index paths (`a[0]`) remain
/// deferred — a `[` is rejected at decode time so the limitation is explicit.
///
/// The string form is kept (`Projection(pub Vec<String>)`), so a plain top-level
/// name is just a one-segment path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projection(pub Vec<String>);

impl Projection {
    /// Apply the projection to `item`, keeping only the requested document paths,
    /// reconstructing the nested structure each path reaches (so projecting
    /// `a.b` yields `{a:{b:..}}`). Absent paths are skipped. Multiple paths
    /// sharing a prefix are merged.
    #[must_use]
    pub fn apply(&self, item: &Item) -> Item {
        let mut out = Item::new();
        for path in &self.0 {
            let segments: Vec<&str> = path.split('.').collect();
            project_path(item, &segments, &mut out);
        }
        out
    }
}

/// Project one dotted path's `segments` from `src` into `dst`, reconstructing the
/// nested map structure. The head names a top-level attribute; each further
/// segment descends into an `M` value. A path that does not resolve to a present
/// value (wrong type or absent key) contributes nothing.
fn project_path(src: &Item, segments: &[&str], dst: &mut Item) {
    let Some((head, rest)) = segments.split_first() else {
        return;
    };
    let Some(value) = src.get(*head) else {
        return;
    };
    if rest.is_empty() {
        // Whole sub-tree at this leaf. Merge with anything already projected
        // under `head` (e.g. a sibling deeper path) by preferring the broader
        // (whole-value) projection.
        dst.insert((*head).to_owned(), value.clone());
        return;
    }
    // Descend into a nested map only.
    let AttributeValue::M(inner) = value else {
        return;
    };
    // Recurse into the nested map, accumulating into the nested entry under
    // `head` (created/extended as a map).
    let entry = dst
        .entry((*head).to_owned())
        .or_insert_with(|| AttributeValue::M(BTreeMap::new()));
    if let AttributeValue::M(nested_dst) = entry {
        project_path(inner, rest, nested_dst);
    }
}

/// Apply an optional projection to an item: `Some(p)` projects, `None` returns
/// the item unchanged.
#[must_use]
pub fn project(projection: Option<&Projection>, item: &Item) -> Item {
    match projection {
        Some(p) => p.apply(item),
        None => item.clone(),
    }
}

/// The `ReturnValues` selector on a write. We support `NONE` (the default,
/// returns `{}`) and `ALL_OLD` (echo the item as it was before the write).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnValues {
    /// `NONE` — return nothing (`{}`).
    None,
    /// `ALL_OLD` — return the item's previous state (the whole prior item).
    AllOld,
}

/// The `ReturnValues` selector on an `UpdateItem` — a superset of [`ReturnValues`]
/// that also reports the *new* state (`UpdateItem` is the only op that builds a
/// new item rather than replacing/removing it wholesale).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UpdateReturnValues {
    /// `NONE` (default) — return `{}`.
    #[default]
    None,
    /// `ALL_OLD` — the whole item before the update.
    AllOld,
    /// `ALL_NEW` — the whole item after the update.
    AllNew,
}

/// One action of an `UpdateItem` `UpdateExpression` (the supported subset): set a
/// top-level attribute to a value, or remove one. `ADD`/`DELETE` (set/number
/// arithmetic) are deferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateAction {
    /// `SET attr = :v` — set (or overwrite) a top-level attribute.
    Set(String, AttributeValue),
    /// `REMOVE attr` — drop a top-level attribute if present.
    Remove(String),
}

/// One sub-request of a `BatchWriteItem` (within a single table's request list):
/// a put or a delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteRequest {
    /// `PutRequest`: write `item`.
    Put(Item),
    /// `DeleteRequest`: delete the item identified by `key`.
    Delete(Item),
}

/// One action of a `TransactWriteItems` request. Each is condition-gated like a
/// conditional write; see [`Operation::TransactWriteItems`] for the (documented)
/// non-atomicity caveat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactAction {
    /// `Put`: write `item` in `table`, gated on `condition`.
    Put {
        /// Target table.
        table: String,
        /// The item to write.
        item: Item,
        /// Optional gate.
        condition: Option<ConditionExpression>,
    },
    /// `Delete`: delete `key` from `table`, gated on `condition`.
    Delete {
        /// Target table.
        table: String,
        /// The key to delete.
        key: Item,
        /// Optional gate.
        condition: Option<ConditionExpression>,
    },
    /// `Update`: apply `actions` to `key` in `table`, gated on `condition`.
    Update {
        /// Target table.
        table: String,
        /// The key to update.
        key: Item,
        /// The update actions (`SET`/`REMOVE`).
        actions: Vec<UpdateAction>,
        /// Optional gate.
        condition: Option<ConditionExpression>,
    },
    /// `ConditionCheck`: assert `condition` on `key` in `table` without writing.
    ConditionCheck {
        /// Target table.
        table: String,
        /// The key to check.
        key: Item,
        /// The asserted condition.
        condition: ConditionExpression,
    },
}

impl TransactAction {
    /// The table this action targets.
    #[must_use]
    pub fn table(&self) -> &str {
        match self {
            TransactAction::Put { table, .. }
            | TransactAction::Delete { table, .. }
            | TransactAction::Update { table, .. }
            | TransactAction::ConditionCheck { table, .. } => table,
        }
    }
}

/// A decoded DynamoDB wire operation (the supported subset).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    /// `CreateTable`: register `schema` under `table`, with any secondary
    /// indexes (GSIs and LSIs) in `indexes`.
    CreateTable {
        /// New table name.
        table: String,
        /// The key schema (partition key + optional sort key).
        schema: TableSchema,
        /// Declared secondary indexes (global and local).
        indexes: Vec<SecondaryIndex>,
    },
    /// `PutItem`: insert or replace `item` in `table`.
    PutItem {
        /// Target table name.
        table: String,
        /// The item to write (must contain the table's key attributes).
        item: Item,
        /// Optional condition the write is gated on (e.g. `attribute_not_exists`).
        condition: Option<ConditionExpression>,
        /// What to echo back (`NONE` / `ALL_OLD`).
        return_values: ReturnValues,
    },
    /// `GetItem`: fetch the item identified by `key` from `table`.
    GetItem {
        /// Target table name.
        table: String,
        /// The key attributes (partition key, plus sort key for composite tables).
        key: Item,
        /// Optional projection (the attributes to return; `None` = all).
        projection: Option<Projection>,
    },
    /// `DeleteItem`: remove the item identified by `key` from `table`.
    DeleteItem {
        /// Target table name.
        table: String,
        /// The key attributes.
        key: Item,
        /// Optional condition the delete is gated on.
        condition: Option<ConditionExpression>,
        /// What to echo back (`NONE` / `ALL_OLD`).
        return_values: ReturnValues,
    },
    /// `Query`: items in a partition (`pk = ..`) matching an optional sort-key
    /// condition — against the base table, or a secondary index when `index` is
    /// set (a GSI query is a hash-key equality only; an LSI query may carry a
    /// sort condition on the index's alternate sort key).
    Query {
        /// Target table name.
        table: String,
        /// The secondary index to query, if any (else the base table).
        index: Option<String>,
        /// The partition/index-key value (equality).
        partition_value: AttributeValue,
        /// Optional sort-key narrowing.
        sort_condition: Option<SortKeyCondition>,
        /// Optional projection (the attributes to return; `None` = all).
        projection: Option<Projection>,
    },
    /// `Scan`: a full-table read with pagination and an optional filter.
    Scan {
        /// Target table name.
        table: String,
        /// Max items to return this page (`None` = all remaining).
        limit: Option<usize>,
        /// The exclusive start key (pagination cursor) from a previous page.
        exclusive_start_key: Option<Item>,
        /// Optional post-read filter (the `ConditionExpression` predicate set).
        filter: Option<ConditionExpression>,
        /// Optional projection (the attributes to return; `None` = all).
        projection: Option<Projection>,
    },
    /// `UpdateItem`: read-modify-write the item at `key`, applying `actions`
    /// (`SET`/`REMOVE`), gated on an optional `condition`.
    UpdateItem {
        /// Target table name.
        table: String,
        /// The key attributes.
        key: Item,
        /// The update actions to apply.
        actions: Vec<UpdateAction>,
        /// Optional condition the update is gated on.
        condition: Option<ConditionExpression>,
        /// What to echo back (`NONE`/`ALL_OLD`/`ALL_NEW`).
        return_values: UpdateReturnValues,
    },
    /// `BatchWriteItem`: a batch of put/delete requests grouped by table. Applied
    /// request-by-request (no cross-request atomicity, as in DynamoDB).
    BatchWriteItem {
        /// Per-table request lists, keyed by table name.
        requests: BTreeMap<String, Vec<WriteRequest>>,
    },
    /// `TransactWriteItems`: a list of condition-gated put/delete/update/check
    /// actions. See the (documented) non-atomicity caveat at the edge.
    TransactWriteItems {
        /// The transaction's actions, in order.
        actions: Vec<TransactAction>,
    },
}

impl Operation {
    /// The single table this operation targets, if it has one. `BatchWriteItem`
    /// and `TransactWriteItems` span multiple tables, so they return `None`.
    #[must_use]
    pub fn table(&self) -> Option<&str> {
        match self {
            Operation::CreateTable { table, .. }
            | Operation::PutItem { table, .. }
            | Operation::GetItem { table, .. }
            | Operation::DeleteItem { table, .. }
            | Operation::Query { table, .. }
            | Operation::Scan { table, .. }
            | Operation::UpdateItem { table, .. } => Some(table),
            Operation::BatchWriteItem { .. } | Operation::TransactWriteItems { .. } => None,
        }
    }
}

/// A wire-level decode/encode failure, carrying the DynamoDB-style error code
/// (the `__type` field) and a human message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireError {
    /// The DynamoDB error code, e.g. `ValidationException` or
    /// `UnknownOperationException`. Sent to clients as the `__type` field.
    pub code: &'static str,
    /// A human-readable message.
    pub message: String,
}

impl WireError {
    fn validation(message: impl Into<String>) -> Self {
        Self {
            code: "ValidationException",
            message: message.into(),
        }
    }

    /// An unrecognized `X-Amz-Target` operation.
    #[must_use]
    pub fn unknown_operation(target: &str) -> Self {
        Self {
            code: "UnknownOperationException",
            message: format!("unsupported operation `{target}`"),
        }
    }

    /// A malformed JSON body.
    #[must_use]
    pub fn serialization(message: impl Into<String>) -> Self {
        Self {
            code: "SerializationException",
            message: message.into(),
        }
    }

    /// A `ConditionExpression` evaluated to false — the write is rejected.
    #[must_use]
    pub fn conditional_check_failed(message: impl Into<String>) -> Self {
        Self {
            code: "ConditionalCheckFailedException",
            message: message.into(),
        }
    }

    /// Render as the DynamoDB error JSON body (`{"__type":..,"message":..}`).
    #[must_use]
    pub fn to_json(&self) -> String {
        let body = ErrorBody {
            type_: format!("com.amazonaws.dynamodb.v20120810#{}", self.code),
            message: self.message.clone(),
        };
        serde_json::to_string(&body).expect("error body serializes")
    }
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for WireError {}

#[derive(Serialize)]
struct ErrorBody {
    #[serde(rename = "__type")]
    type_: String,
    message: String,
}

/// Decode a request body for the operation named by `target` (the full
/// `X-Amz-Target` header value, e.g. `DynamoDB_20120810.PutItem`).
///
/// # Errors
/// Returns a [`WireError`] if the target is unsupported or the body is invalid.
pub fn decode_request(target: &str, body: &[u8]) -> Result<Operation, WireError> {
    let op = target.strip_prefix(TARGET_PREFIX).unwrap_or(target);
    let json: Value = serde_json::from_slice(body)
        .map_err(|e| WireError::serialization(format!("invalid JSON body: {e}")))?;
    let obj = json
        .as_object()
        .ok_or_else(|| WireError::validation("request body must be a JSON object"))?;

    match op {
        "CreateTable" => {
            let table = table_name(obj)?;
            let schema = decode_key_schema(obj)?;
            let indexes = decode_indexes(obj)?;
            Ok(Operation::CreateTable {
                table,
                schema,
                indexes,
            })
        }
        "PutItem" => {
            let table = table_name(obj)?;
            let item = decode_item_field(obj, "Item")?;
            let condition = decode_condition(obj)?;
            let return_values = decode_return_values(obj)?;
            Ok(Operation::PutItem {
                table,
                item,
                condition,
                return_values,
            })
        }
        "GetItem" => {
            let table = table_name(obj)?;
            let key = decode_item_field(obj, "Key")?;
            let projection = decode_projection(obj)?;
            Ok(Operation::GetItem {
                table,
                key,
                projection,
            })
        }
        "DeleteItem" => {
            let table = table_name(obj)?;
            let key = decode_item_field(obj, "Key")?;
            let condition = decode_condition(obj)?;
            let return_values = decode_return_values(obj)?;
            Ok(Operation::DeleteItem {
                table,
                key,
                condition,
                return_values,
            })
        }
        "Query" => decode_query(obj),
        "Scan" => decode_scan(obj),
        "UpdateItem" => decode_update_item(obj),
        "BatchWriteItem" => decode_batch_write(obj),
        "TransactWriteItems" => decode_transact_write(obj),
        _ => Err(WireError::unknown_operation(target)),
    }
}

/// Decode an `UpdateItem` body: `Key`, an `UpdateExpression` (`SET`/`REMOVE`
/// clauses), an optional `ConditionExpression`, and an optional `ReturnValues`.
fn decode_update_item(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let table = table_name(obj)?;
    let key = decode_item_field(obj, "Key")?;
    let expr = obj
        .get("UpdateExpression")
        .and_then(Value::as_str)
        .ok_or_else(|| WireError::validation("missing string field `UpdateExpression`"))?;
    let actions = decode_update_expression(obj, expr)?;
    if actions.is_empty() {
        return Err(WireError::validation(
            "`UpdateExpression` has no SET/REMOVE actions",
        ));
    }
    let condition = decode_condition(obj)?;
    let return_values = decode_update_return_values(obj)?;
    Ok(Operation::UpdateItem {
        table,
        key,
        actions,
        condition,
        return_values,
    })
}

/// Decode a DynamoDB `UpdateExpression` (the supported subset). Recognized
/// clauses are `SET a = :v, b = :w` and `REMOVE c, d`, in either order; the
/// attribute names may use `#alias` placeholders and the values `:placeholder`s.
/// `ADD`/`DELETE` clauses are rejected (deferred).
fn decode_update_expression(
    obj: &Map<String, Value>,
    expr: &str,
) -> Result<Vec<UpdateAction>, WireError> {
    // Split into clauses on the SET/REMOVE/ADD/DELETE keywords (case-insensitive).
    // We do a simple scan: find each keyword and the text up to the next keyword.
    let lower = expr.to_ascii_lowercase();
    let keywords = ["set ", "remove ", "add ", "delete "];
    // Collect (keyword, start-of-args) positions in order.
    let mut spans: Vec<(usize, usize, &str)> = Vec::new();
    for kw in keywords {
        let mut from = 0;
        while let Some(rel) = lower[from..].find(kw) {
            let at = from + rel;
            // Only at a clause boundary (start, or preceded by whitespace).
            if at == 0 || lower.as_bytes()[at - 1].is_ascii_whitespace() {
                spans.push((at, at + kw.len(), kw.trim()));
            }
            from = at + kw.len();
        }
    }
    spans.sort_by_key(|(at, _, _)| *at);
    if spans.is_empty() {
        return Err(WireError::validation(format!(
            "unsupported `UpdateExpression` `{expr}` (supported clauses: SET, REMOVE)"
        )));
    }
    let mut actions = Vec::new();
    for (i, (_, args_start, kw)) in spans.iter().enumerate() {
        let args_end = spans.get(i + 1).map_or(expr.len(), |(at, _, _)| *at);
        let args = expr[*args_start..args_end].trim().trim_end_matches(',');
        match *kw {
            "set" => {
                for clause in args.split(',') {
                    let (lhs, rhs) = clause.split_once('=').ok_or_else(|| {
                        WireError::validation("SET clause must be `attr = :value`")
                    })?;
                    let attr = resolve_attr_name(obj, lhs.trim())?;
                    let value = resolve_placeholder(obj, rhs.trim())?;
                    actions.push(UpdateAction::Set(attr, value));
                }
            }
            "remove" => {
                for name in args.split(',') {
                    let attr = resolve_attr_name(obj, name.trim())?;
                    actions.push(UpdateAction::Remove(attr));
                }
            }
            other => {
                return Err(WireError::validation(format!(
                    "`UpdateExpression` clause `{other}` is not supported (SET, REMOVE only)"
                )));
            }
        }
    }
    Ok(actions)
}

/// Resolve a single top-level attribute name in an update clause, following a
/// `#alias` through `ExpressionAttributeNames`. Document paths are rejected here
/// (updates target a top-level attribute in this subset).
fn resolve_attr_name(obj: &Map<String, Value>, raw: &str) -> Result<String, WireError> {
    let name = if let Some(alias) = raw.strip_prefix('#') {
        obj.get("ExpressionAttributeNames")
            .and_then(Value::as_object)
            .and_then(|m| m.get(&format!("#{alias}")))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                WireError::validation(format!("name placeholder `#{alias}` is not defined"))
            })?
    } else {
        raw
    };
    Ok(reject_path(name)?.to_owned())
}

/// Decode an `UpdateItem` `ReturnValues` (`NONE`/`ALL_OLD`/`ALL_NEW`). Absent ⇒
/// `NONE`. `UPDATED_OLD`/`UPDATED_NEW` are deferred (rejected).
fn decode_update_return_values(obj: &Map<String, Value>) -> Result<UpdateReturnValues, WireError> {
    match obj.get("ReturnValues") {
        None | Some(Value::Null) => Ok(UpdateReturnValues::None),
        Some(v) => match v.as_str() {
            Some("NONE") => Ok(UpdateReturnValues::None),
            Some("ALL_OLD") => Ok(UpdateReturnValues::AllOld),
            Some("ALL_NEW") => Ok(UpdateReturnValues::AllNew),
            Some(other) => Err(WireError::validation(format!(
                "unsupported `ReturnValues` `{other}` (supported: NONE, ALL_OLD, ALL_NEW)"
            ))),
            None => Err(WireError::validation("`ReturnValues` must be a string")),
        },
    }
}

/// Decode a `BatchWriteItem` body: `{"RequestItems": {table: [{PutRequest|
/// DeleteRequest}, ..], ..}}`.
fn decode_batch_write(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let items = obj
        .get("RequestItems")
        .and_then(Value::as_object)
        .ok_or_else(|| WireError::validation("missing object field `RequestItems`"))?;
    let mut requests = BTreeMap::new();
    for (table, list) in items {
        let arr = list.as_array().ok_or_else(|| {
            WireError::validation(format!("`RequestItems.{table}` must be an array"))
        })?;
        let mut reqs = Vec::with_capacity(arr.len());
        for entry in arr {
            let e = entry
                .as_object()
                .ok_or_else(|| WireError::validation("each batch request must be an object"))?;
            if let Some(put) = e.get("PutRequest").and_then(Value::as_object) {
                reqs.push(WriteRequest::Put(decode_sub_item(put, "Item")?));
            } else if let Some(del) = e.get("DeleteRequest").and_then(Value::as_object) {
                reqs.push(WriteRequest::Delete(decode_sub_item(del, "Key")?));
            } else {
                return Err(WireError::validation(
                    "batch request must have a `PutRequest` or `DeleteRequest`",
                ));
            }
        }
        requests.insert(table.clone(), reqs);
    }
    Ok(Operation::BatchWriteItem { requests })
}

/// Decode a `TransactWriteItems` body: `{"TransactItems": [{Put|Delete|Update|
/// ConditionCheck}, ..]}`.
fn decode_transact_write(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let items = obj
        .get("TransactItems")
        .and_then(Value::as_array)
        .ok_or_else(|| WireError::validation("missing array field `TransactItems`"))?;
    let mut actions = Vec::with_capacity(items.len());
    for entry in items {
        let e = entry
            .as_object()
            .ok_or_else(|| WireError::validation("each `TransactItems` entry must be an object"))?;
        let (kind, inner) = e.iter().next().filter(|_| e.len() == 1).ok_or_else(|| {
            WireError::validation("each transact item is one Put/Delete/Update/ConditionCheck")
        })?;
        let inner = inner
            .as_object()
            .ok_or_else(|| WireError::validation(format!("`{kind}` must be an object")))?;
        let table = table_name(inner)?;
        let action = match kind.as_str() {
            "Put" => TransactAction::Put {
                table,
                item: decode_sub_item(inner, "Item")?,
                condition: decode_condition(inner)?,
            },
            "Delete" => TransactAction::Delete {
                table,
                key: decode_sub_item(inner, "Key")?,
                condition: decode_condition(inner)?,
            },
            "Update" => {
                let key = decode_sub_item(inner, "Key")?;
                let expr = inner
                    .get("UpdateExpression")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        WireError::validation("transact Update needs `UpdateExpression`")
                    })?;
                TransactAction::Update {
                    table,
                    key,
                    actions: decode_update_expression(inner, expr)?,
                    condition: decode_condition(inner)?,
                }
            }
            "ConditionCheck" => TransactAction::ConditionCheck {
                table,
                key: decode_sub_item(inner, "Key")?,
                condition: decode_condition(inner)?.ok_or_else(|| {
                    WireError::validation("ConditionCheck requires a `ConditionExpression`")
                })?,
            },
            other => {
                return Err(WireError::validation(format!(
                    "unsupported transact action `{other}`"
                )));
            }
        };
        actions.push(action);
    }
    Ok(Operation::TransactWriteItems { actions })
}

/// Decode the attribute-map at `field` of a nested request object into an [`Item`].
fn decode_sub_item(obj: &Map<String, Value>, field: &str) -> Result<Item, WireError> {
    decode_item_field(obj, field)
}

/// Decode a `CreateTable` body's `KeySchema` + `AttributeDefinitions` into a
/// [`TableSchema`]. We use the `KeySchema` (`HASH` / `RANGE`) for the key
/// attribute names; `AttributeDefinitions` types are accepted but, since our
/// model carries types per value, not enforced.
fn decode_key_schema(obj: &Map<String, Value>) -> Result<TableSchema, WireError> {
    let key_schema = obj
        .get("KeySchema")
        .and_then(Value::as_array)
        .ok_or_else(|| WireError::validation("missing array field `KeySchema`"))?;
    let mut partition_key = None;
    let mut sort_key = None;
    for entry in key_schema {
        let e = entry
            .as_object()
            .ok_or_else(|| WireError::validation("each `KeySchema` entry must be an object"))?;
        let name = e
            .get("AttributeName")
            .and_then(Value::as_str)
            .ok_or_else(|| WireError::validation("`KeySchema` entry missing `AttributeName`"))?;
        let role = e
            .get("KeyType")
            .and_then(Value::as_str)
            .ok_or_else(|| WireError::validation("`KeySchema` entry missing `KeyType`"))?;
        match role {
            "HASH" => partition_key = Some(name.to_owned()),
            "RANGE" => sort_key = Some(name.to_owned()),
            other => {
                return Err(WireError::validation(format!(
                    "unknown KeyType `{other}` (expected HASH or RANGE)"
                )));
            }
        }
    }
    let partition_key = partition_key
        .ok_or_else(|| WireError::validation("`KeySchema` has no HASH (partition) key"))?;
    Ok(TableSchema {
        partition_key,
        sort_key,
    })
}

/// Decode the optional `GlobalSecondaryIndexes` + `LocalSecondaryIndexes` of a
/// `CreateTable` into a list of [`SecondaryIndex`]. A GSI's `KeySchema` is a
/// `HASH` attribute plus an optional `RANGE` (a composite GSI); an LSI's
/// `KeySchema` shares the base partition `HASH` and adds a `RANGE` (the index's
/// alternate sort key). Absent ⇒ an empty list.
fn decode_indexes(obj: &Map<String, Value>) -> Result<Vec<SecondaryIndex>, WireError> {
    let mut out = Vec::new();
    if let Some(gsis) = obj.get("GlobalSecondaryIndexes") {
        let gsis = gsis
            .as_array()
            .ok_or_else(|| WireError::validation("`GlobalSecondaryIndexes` must be an array"))?;
        for gsi in gsis {
            let (name, schema, projection) = decode_index_entry(gsi, "GSI")?;
            out.push(SecondaryIndex::Global(GlobalSecondaryIndex {
                name,
                key_attribute: schema.partition_key,
                sort_attribute: schema.sort_key,
                projection,
            }));
        }
    }
    if let Some(lsis) = obj.get("LocalSecondaryIndexes") {
        let lsis = lsis
            .as_array()
            .ok_or_else(|| WireError::validation("`LocalSecondaryIndexes` must be an array"))?;
        let base = decode_key_schema(obj)?;
        for lsi in lsis {
            let (name, schema, projection) = decode_index_entry(lsi, "LSI")?;
            // An LSI's HASH must be the base table's partition key, and it must
            // declare a RANGE (its alternate sort key).
            if schema.partition_key != base.partition_key {
                return Err(WireError::validation(format!(
                    "LSI `{name}` HASH key must be the base partition key `{}`",
                    base.partition_key
                )));
            }
            let sort_attribute = schema.sort_key.ok_or_else(|| {
                WireError::validation(format!("LSI `{name}` must declare a RANGE (sort) key"))
            })?;
            out.push(SecondaryIndex::Local(LocalSecondaryIndex {
                name,
                sort_attribute,
                projection,
            }));
        }
    }
    // Index names must be unique across both kinds.
    let mut seen = std::collections::BTreeSet::new();
    for index in &out {
        if !seen.insert(index.name().to_owned()) {
            return Err(WireError::validation(format!(
                "duplicate index name `{}`",
                index.name()
            )));
        }
    }
    Ok(out)
}

/// Decode one index declaration object into its `(name, KeySchema, Projection)`.
fn decode_index_entry(
    value: &Value,
    kind: &str,
) -> Result<(String, TableSchema, IndexProjection), WireError> {
    let g = value
        .as_object()
        .ok_or_else(|| WireError::validation(format!("each {kind} must be an object")))?;
    let name = g
        .get("IndexName")
        .and_then(Value::as_str)
        .ok_or_else(|| WireError::validation(format!("{kind} missing `IndexName`")))?
        .to_owned();
    let schema = decode_key_schema(g)?;
    let projection = decode_index_projection(g)?;
    Ok((name, schema, projection))
}

/// Decode an index's optional `Projection` object
/// (`{"ProjectionType":"ALL"|"KEYS_ONLY"|"INCLUDE", "NonKeyAttributes":[..]}`).
/// Absent ⇒ `ALL`. `INCLUDE` requires a non-empty `NonKeyAttributes` list; the
/// other types must not carry one.
fn decode_index_projection(g: &Map<String, Value>) -> Result<IndexProjection, WireError> {
    let Some(proj) = g.get("Projection") else {
        return Ok(IndexProjection::All);
    };
    let proj = proj
        .as_object()
        .ok_or_else(|| WireError::validation("`Projection` must be an object"))?;
    let kind = proj
        .get("ProjectionType")
        .and_then(Value::as_str)
        .unwrap_or("ALL");
    let non_key = proj.get("NonKeyAttributes");
    match kind {
        "ALL" => Ok(IndexProjection::All),
        "KEYS_ONLY" => Ok(IndexProjection::KeysOnly),
        "INCLUDE" => {
            let arr = non_key.and_then(Value::as_array).ok_or_else(|| {
                WireError::validation("INCLUDE projection needs `NonKeyAttributes`")
            })?;
            let mut names = Vec::with_capacity(arr.len());
            for v in arr {
                let name = v.as_str().ok_or_else(|| {
                    WireError::validation("`NonKeyAttributes` elements must be strings")
                })?;
                names.push(reject_path(name)?.to_owned());
            }
            if names.is_empty() {
                return Err(WireError::validation("INCLUDE `NonKeyAttributes` is empty"));
            }
            Ok(IndexProjection::Include(names))
        }
        other => Err(WireError::validation(format!(
            "unsupported ProjectionType `{other}` (ALL, KEYS_ONLY, INCLUDE)"
        ))),
    }
}

/// Decode the optional `ConditionExpression` + `ExpressionAttributeValues` of a
/// write. Supported forms: `attribute_not_exists(attr)`,
/// `attribute_exists(attr)`, and `attr = :placeholder`. Absent ⇒ `Ok(None)`.
fn decode_condition(obj: &Map<String, Value>) -> Result<Option<ConditionExpression>, WireError> {
    decode_predicate(obj, "ConditionExpression")
}

/// Decode the optional `ProjectionExpression` (a comma-separated list of
/// top-level attribute names, with `#name` placeholders resolved against
/// `ExpressionAttributeNames`) or the legacy `AttributesToGet` (a JSON array of
/// names). At most one may be present. Absent ⇒ `Ok(None)` (all attributes).
fn decode_projection(obj: &Map<String, Value>) -> Result<Option<Projection>, WireError> {
    let has_expr = obj.contains_key("ProjectionExpression");
    let has_legacy = obj.contains_key("AttributesToGet");
    if has_expr && has_legacy {
        return Err(WireError::validation(
            "supply at most one of `ProjectionExpression` / `AttributesToGet`",
        ));
    }
    if let Some(expr) = obj.get("ProjectionExpression").and_then(Value::as_str) {
        let names = expr
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|raw| resolve_projection_name(obj, raw))
            .collect::<Result<Vec<_>, _>>()?;
        if names.is_empty() {
            return Err(WireError::validation("`ProjectionExpression` is empty"));
        }
        return Ok(Some(Projection(names)));
    }
    if let Some(arr) = obj.get("AttributesToGet") {
        let arr = arr
            .as_array()
            .ok_or_else(|| WireError::validation("`AttributesToGet` must be an array"))?;
        let mut names = Vec::with_capacity(arr.len());
        for v in arr {
            let name = v.as_str().ok_or_else(|| {
                WireError::validation("`AttributesToGet` elements must be strings")
            })?;
            names.push(reject_path(name)?.to_owned());
        }
        if names.is_empty() {
            return Err(WireError::validation("`AttributesToGet` is empty"));
        }
        return Ok(Some(Projection(names)));
    }
    Ok(None)
}

/// Resolve one `ProjectionExpression` element into a **dotted document path**,
/// resolving a `#alias` on each `.`-separated segment through
/// `ExpressionAttributeNames`. The result is the path with aliases substituted
/// (e.g. `#p.#c` → `profile.city`). List-index syntax (`[`) is still rejected.
fn resolve_projection_name(obj: &Map<String, Value>, raw: &str) -> Result<String, WireError> {
    let mut segments = Vec::new();
    for seg in raw.split('.') {
        let seg = seg.trim();
        if seg.is_empty() {
            return Err(WireError::validation(format!(
                "projection path `{raw}` has an empty segment"
            )));
        }
        let resolved = if let Some(alias) = seg.strip_prefix('#') {
            let names = obj
                .get("ExpressionAttributeNames")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    WireError::validation(format!(
                        "projection uses name placeholder `#{alias}` but `ExpressionAttributeNames` is absent"
                    ))
                })?;
            names
                .get(&format!("#{alias}"))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    WireError::validation(format!("name placeholder `#{alias}` is not defined"))
                })?
                .to_owned()
        } else {
            seg.to_owned()
        };
        reject_list_index(&resolved)?;
        segments.push(resolved);
    }
    Ok(segments.join("."))
}

/// Reject a list-index attribute path (containing `[`): nested-map document
/// paths (`a.b`) are supported, but list indexing (`a[0]`) is deferred.
fn reject_list_index(name: &str) -> Result<(), WireError> {
    if name.contains('[') {
        return Err(WireError::validation(format!(
            "list-index projection `{name}` is not supported (map paths `a.b` only)"
        )));
    }
    Ok(())
}

/// Reject a non-top-level attribute name (containing `.` or `[`), used where only
/// a flat attribute name is meaningful (`AttributesToGet`, index
/// `NonKeyAttributes`).
fn reject_path(name: &str) -> Result<&str, WireError> {
    if name.contains('.') || name.contains('[') {
        return Err(WireError::validation(format!(
            "attribute path `{name}` is not supported here (top-level names only)"
        )));
    }
    Ok(name)
}

/// Decode the optional `ReturnValues` field of a write. We support `NONE`
/// (default) and `ALL_OLD`; `UPDATED_OLD`/`ALL_NEW`/`UPDATED_NEW` apply only to
/// `UpdateItem` (not implemented), so they are rejected for `Put`/`Delete`.
fn decode_return_values(obj: &Map<String, Value>) -> Result<ReturnValues, WireError> {
    match obj.get("ReturnValues") {
        None | Some(Value::Null) => Ok(ReturnValues::None),
        Some(v) => match v.as_str() {
            Some("NONE") => Ok(ReturnValues::None),
            Some("ALL_OLD") => Ok(ReturnValues::AllOld),
            Some(other) => Err(WireError::validation(format!(
                "unsupported `ReturnValues` `{other}` (supported: NONE, ALL_OLD)"
            ))),
            None => Err(WireError::validation("`ReturnValues` must be a string")),
        },
    }
}

/// Decode a predicate from the string field named `field` (one of
/// `ConditionExpression` / `FilterExpression`) into a [`ConditionExpression`]:
/// `attribute_not_exists(attr)`, `attribute_exists(attr)`, or `attr = :v`
/// (resolved against `ExpressionAttributeValues`). Absent ⇒ `Ok(None)`.
fn decode_predicate(
    obj: &Map<String, Value>,
    field: &str,
) -> Result<Option<ConditionExpression>, WireError> {
    let Some(expr) = obj.get(field).and_then(Value::as_str) else {
        return Ok(None);
    };
    let expr = expr.trim();
    let cond = if let Some(inner) = func_arg(expr, "attribute_not_exists") {
        ConditionExpression::AttributeNotExists(inner.to_owned())
    } else if let Some(inner) = func_arg(expr, "attribute_exists") {
        ConditionExpression::AttributeExists(inner.to_owned())
    } else if let Some((lhs, rhs)) = expr.split_once('=') {
        let attr = lhs.trim();
        let rhs = rhs.trim();
        let value = resolve_placeholder(obj, rhs)?;
        ConditionExpression::Equals(attr.to_owned(), value)
    } else {
        return Err(WireError::validation(format!(
            "unsupported {field} `{expr}` (supported: \
             attribute_not_exists(a), attribute_exists(a), a = :v)"
        )));
    };
    Ok(Some(cond))
}

/// If `expr` is exactly `name(arg)`, return the trimmed `arg`.
fn func_arg<'a>(expr: &'a str, name: &str) -> Option<&'a str> {
    let rest = expr.strip_prefix(name)?.trim_start();
    let inner = rest.strip_prefix('(')?.strip_suffix(')')?;
    Some(inner.trim())
}

/// Resolve a `:placeholder` against `ExpressionAttributeValues`.
fn resolve_placeholder(
    obj: &Map<String, Value>,
    placeholder: &str,
) -> Result<AttributeValue, WireError> {
    if !placeholder.starts_with(':') {
        return Err(WireError::validation(format!(
            "condition right-hand side `{placeholder}` must be a `:` value placeholder"
        )));
    }
    let values = obj
        .get("ExpressionAttributeValues")
        .and_then(Value::as_object)
        .ok_or_else(|| WireError::validation("missing `ExpressionAttributeValues`"))?;
    let raw = values.get(placeholder).ok_or_else(|| {
        WireError::validation(format!("placeholder `{placeholder}` is not defined"))
    })?;
    decode_attribute_value(placeholder, raw)
}

/// Decode a `Query` body: a `KeyConditionExpression` of the form
/// `<pk> = :pv [AND <sort-condition>]`, with values supplied via
/// `ExpressionAttributeValues`. The supported sort conditions are
/// `<sk> = :v`, `<sk> BETWEEN :lo AND :hi`, and `begins_with(<sk>, :p)`.
fn decode_query(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let table = table_name(obj)?;
    let expr = obj
        .get("KeyConditionExpression")
        .and_then(Value::as_str)
        .ok_or_else(|| WireError::validation("missing string field `KeyConditionExpression`"))?;
    // Split off an optional sort clause on the first ` AND ` (DynamoDB requires
    // the partition equality first).
    let (pk_clause, sort_clause) = match split_once_ci(expr, " AND ") {
        Some((a, b)) => (a.trim(), Some(b.trim())),
        None => (expr.trim(), None),
    };
    // Partition: `<attr> = :placeholder`.
    let (_pk_attr, pk_placeholder) = pk_clause
        .split_once('=')
        .ok_or_else(|| WireError::validation("partition key condition must be `pk = :v`"))?;
    let partition_value = resolve_placeholder(obj, pk_placeholder.trim())?;

    let index = obj
        .get("IndexName")
        .and_then(Value::as_str)
        .map(str::to_owned);

    let sort_condition = match sort_clause {
        None => None,
        Some(clause) => Some(decode_sort_condition(obj, clause)?),
    };
    let projection = decode_projection(obj)?;
    // A sort condition on an index is meaningful only for a local secondary
    // index (which has an alternate sort key). The caller (registry) rejects a
    // sort condition against a hash-only GSI; here we accept the parse so the
    // index-kind decision can live in one place.
    Ok(Operation::Query {
        table,
        index,
        partition_value,
        sort_condition,
        projection,
    })
}

/// Decode a `Scan` body: an optional `Limit`, an optional `ExclusiveStartKey`
/// (the AttributeValue-map cursor from a previous page's `LastEvaluatedKey`),
/// and an optional `FilterExpression` (the `ConditionExpression` predicate set).
fn decode_scan(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let table = table_name(obj)?;
    let limit = match obj.get("Limit") {
        None | Some(Value::Null) => None,
        Some(v) => Some(
            v.as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .ok_or_else(|| WireError::validation("`Limit` must be a non-negative integer"))?,
        ),
    };
    let exclusive_start_key = match obj.get("ExclusiveStartKey") {
        None | Some(Value::Null) => None,
        Some(v) => Some(
            v.as_object()
                .ok_or_else(|| WireError::validation("`ExclusiveStartKey` must be an object"))
                .and_then(decode_item)?,
        ),
    };
    let filter = decode_predicate(obj, "FilterExpression")?;
    let projection = decode_projection(obj)?;
    Ok(Operation::Scan {
        table,
        limit,
        exclusive_start_key,
        filter,
        projection,
    })
}

fn decode_sort_condition(
    obj: &Map<String, Value>,
    clause: &str,
) -> Result<SortKeyCondition, WireError> {
    let clause = clause.trim();
    if let Some(inner) = func_arg(clause, "begins_with") {
        // begins_with(<sk>, :p)
        let (_attr, ph) = inner.split_once(',').ok_or_else(|| {
            WireError::validation("begins_with takes two arguments: begins_with(sk, :p)")
        })?;
        let value = resolve_placeholder(obj, ph.trim())?;
        return Ok(SortKeyCondition::BeginsWith(value));
    }
    if let Some((_attr, rest)) = split_once_ci(clause, " BETWEEN ") {
        let (lo, hi) = split_once_ci(rest, " AND ")
            .ok_or_else(|| WireError::validation("BETWEEN takes `:lo AND :hi`"))?;
        let lo = resolve_placeholder(obj, lo.trim())?;
        let hi = resolve_placeholder(obj, hi.trim())?;
        return Ok(SortKeyCondition::Between(lo, hi));
    }
    if let Some((_attr, ph)) = clause.split_once('=') {
        let value = resolve_placeholder(obj, ph.trim())?;
        return Ok(SortKeyCondition::Equals(value));
    }
    Err(WireError::validation(format!(
        "unsupported sort-key condition `{clause}` (supported: =, BETWEEN, begins_with)"
    )))
}

/// Case-insensitive `split_once` on a `needle` (used for ` AND `/` BETWEEN `,
/// which clients may send in any case). Returns byte-sliced halves of `s`.
fn split_once_ci<'a>(s: &'a str, needle: &str) -> Option<(&'a str, &'a str)> {
    let lower = s.to_ascii_lowercase();
    let pos = lower.find(&needle.to_ascii_lowercase())?;
    Some((&s[..pos], &s[pos + needle.len()..]))
}

fn table_name(obj: &Map<String, Value>) -> Result<String, WireError> {
    obj.get("TableName")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| WireError::validation("missing string field `TableName`"))
}

fn decode_item_field(obj: &Map<String, Value>, field: &str) -> Result<Item, WireError> {
    let map = obj
        .get(field)
        .ok_or_else(|| WireError::validation(format!("missing field `{field}`")))?
        .as_object()
        .ok_or_else(|| WireError::validation(format!("`{field}` must be an object")))?;
    decode_item(map)
}

/// Decode a DynamoDB attribute-map JSON object into an [`Item`].
///
/// # Errors
/// Returns a [`WireError`] if any attribute value is malformed or uses an
/// unsupported type.
pub fn decode_item(map: &Map<String, Value>) -> Result<Item, WireError> {
    let mut item = Item::new();
    for (name, value) in map {
        item.insert(name.clone(), decode_attribute_value(name, value)?);
    }
    Ok(item)
}

fn decode_attribute_value(name: &str, value: &Value) -> Result<AttributeValue, WireError> {
    let obj = value.as_object().ok_or_else(|| {
        WireError::validation(format!("attribute `{name}` must be a typed object"))
    })?;
    let (ty, inner) = match obj.iter().next() {
        Some(entry) if obj.len() == 1 => entry,
        _ => {
            return Err(WireError::validation(format!(
                "attribute `{name}` must be a single-key typed object like {{\"S\":..}}"
            )));
        }
    };
    match ty.as_str() {
        "S" => inner
            .as_str()
            .map(|s| AttributeValue::S(s.to_owned()))
            .ok_or_else(|| WireError::validation(format!("`{name}`.S must be a string"))),
        "N" => inner
            .as_str()
            .map(|s| AttributeValue::N(s.to_owned()))
            .ok_or_else(|| WireError::validation(format!("`{name}`.N must be a string"))),
        "B" => inner
            .as_str()
            .ok_or_else(|| WireError::validation(format!("`{name}`.B must be a base64 string")))
            .and_then(|s| {
                base64_decode(s)
                    .map(AttributeValue::B)
                    .ok_or_else(|| WireError::validation(format!("`{name}`.B is not valid base64")))
            }),
        "BOOL" => inner
            .as_bool()
            .map(AttributeValue::Bool)
            .ok_or_else(|| WireError::validation(format!("`{name}`.BOOL must be a bool"))),
        "NULL" => Ok(AttributeValue::Null),
        "M" => {
            let map = inner.as_object().ok_or_else(|| {
                WireError::validation(format!("`{name}`.M must be an attribute-map object"))
            })?;
            let mut nested = BTreeMap::new();
            for (k, v) in map {
                nested.insert(
                    k.clone(),
                    decode_attribute_value(&format!("{name}.{k}"), v)?,
                );
            }
            Ok(AttributeValue::M(nested))
        }
        "L" => {
            let list = inner.as_array().ok_or_else(|| {
                WireError::validation(format!("`{name}`.L must be a list of typed values"))
            })?;
            let mut out = Vec::with_capacity(list.len());
            for (i, v) in list.iter().enumerate() {
                out.push(decode_attribute_value(&format!("{name}[{i}]"), v)?);
            }
            Ok(AttributeValue::L(out))
        }
        "SS" => decode_string_set(name, "SS", inner).map(AttributeValue::SS),
        "NS" => decode_string_set(name, "NS", inner).map(AttributeValue::NS),
        "BS" => {
            let arr = inner.as_array().ok_or_else(|| {
                WireError::validation(format!("`{name}`.BS must be an array of base64 strings"))
            })?;
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                let s = v.as_str().ok_or_else(|| {
                    WireError::validation(format!("`{name}`.BS elements must be base64 strings"))
                })?;
                let bytes = base64_decode(s).ok_or_else(|| {
                    WireError::validation(format!("`{name}`.BS element is not valid base64"))
                })?;
                out.push(bytes);
            }
            Ok(AttributeValue::BS(dedup_sorted(out)))
        }
        other => Err(WireError::validation(format!(
            "attribute `{name}` uses unsupported type `{other}` \
             (only S, N, B, BOOL, NULL, M, L, SS, NS, BS are supported)"
        ))),
    }
}

/// Decode an `SS`/`NS` array of strings into a sorted, deduplicated set.
fn decode_string_set(name: &str, ty: &str, inner: &Value) -> Result<Vec<String>, WireError> {
    let arr = inner.as_array().ok_or_else(|| {
        WireError::validation(format!("`{name}`.{ty} must be an array of strings"))
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for v in arr {
        let s = v.as_str().ok_or_else(|| {
            WireError::validation(format!("`{name}`.{ty} elements must be strings"))
        })?;
        out.push(s.to_owned());
    }
    Ok(dedup_sorted(out))
}

/// Sort and deduplicate a set's elements, so the in-memory representation is
/// canonical (set membership is order-independent; a canonical form makes
/// equality and storage deterministic).
fn dedup_sorted<T: Ord>(mut items: Vec<T>) -> Vec<T> {
    items.sort();
    items.dedup();
    items
}

/// Encode an [`Item`] as a DynamoDB attribute-map JSON object.
#[must_use]
pub fn encode_item(item: &Item) -> Value {
    let mut map = Map::new();
    for (name, value) in item {
        map.insert(name.clone(), encode_attribute_value(value));
    }
    Value::Object(map)
}

fn encode_attribute_value(value: &AttributeValue) -> Value {
    let mut obj = Map::new();
    match value {
        AttributeValue::S(s) => {
            obj.insert("S".into(), Value::String(s.clone()));
        }
        AttributeValue::N(n) => {
            obj.insert("N".into(), Value::String(n.clone()));
        }
        AttributeValue::B(b) => {
            obj.insert("B".into(), Value::String(base64_encode(b)));
        }
        AttributeValue::Bool(b) => {
            obj.insert("BOOL".into(), Value::Bool(*b));
        }
        AttributeValue::Null => {
            obj.insert("NULL".into(), Value::Bool(true));
        }
        AttributeValue::M(map) => {
            let mut nested = Map::new();
            for (k, v) in map {
                nested.insert(k.clone(), encode_attribute_value(v));
            }
            obj.insert("M".into(), Value::Object(nested));
        }
        AttributeValue::L(list) => {
            let encoded: Vec<Value> = list.iter().map(encode_attribute_value).collect();
            obj.insert("L".into(), Value::Array(encoded));
        }
        AttributeValue::SS(set) => {
            let arr = set.iter().map(|s| Value::String(s.clone())).collect();
            obj.insert("SS".into(), Value::Array(arr));
        }
        AttributeValue::NS(set) => {
            let arr = set.iter().map(|s| Value::String(s.clone())).collect();
            obj.insert("NS".into(), Value::Array(arr));
        }
        AttributeValue::BS(set) => {
            let arr = set
                .iter()
                .map(|b| Value::String(base64_encode(b)))
                .collect();
            obj.insert("BS".into(), Value::Array(arr));
        }
    }
    Value::Object(obj)
}

/// The JSON body for a successful `GetItem`: `{"Item": {..}}`, or `{}` when the
/// item is absent (matching DynamoDB).
#[must_use]
pub fn get_item_response(item: Option<&Item>) -> String {
    let mut obj = Map::new();
    if let Some(item) = item {
        obj.insert("Item".into(), encode_item(item));
    }
    serde_json::to_string(&Value::Object(obj)).expect("response serializes")
}

/// The JSON body for a successful `PutItem` / `DeleteItem` with
/// `ReturnValues: NONE`: `{}`.
#[must_use]
pub fn empty_response() -> String {
    "{}".to_string()
}

/// The JSON body for a successful write echoing `ReturnValues`. `old` is the
/// item as it was before the write (`None` when the key was absent); for
/// `ALL_OLD` a present prior item is returned under `Attributes`, an absent one
/// yields `{}` (matching DynamoDB). For `NONE` this is always `{}`.
#[must_use]
pub fn write_response(return_values: ReturnValues, old: Option<&Item>) -> String {
    match (return_values, old) {
        (ReturnValues::AllOld, Some(item)) => {
            let mut obj = Map::new();
            obj.insert("Attributes".into(), encode_item(item));
            serde_json::to_string(&Value::Object(obj)).expect("write response serializes")
        }
        _ => empty_response(),
    }
}

/// The JSON body for a successful `UpdateItem` echoing `ReturnValues`: `ALL_OLD`
/// returns the item before the update under `Attributes`, `ALL_NEW` the item
/// after, `NONE` returns `{}`. An absent `old`/`new` (e.g. `ALL_OLD` on a key the
/// update created) yields `{}`.
#[must_use]
pub fn update_response(
    return_values: UpdateReturnValues,
    old: Option<&Item>,
    new: Option<&Item>,
) -> String {
    let attrs = match return_values {
        UpdateReturnValues::None => None,
        UpdateReturnValues::AllOld => old,
        UpdateReturnValues::AllNew => new,
    };
    match attrs {
        Some(item) => {
            let mut obj = Map::new();
            obj.insert("Attributes".into(), encode_item(item));
            serde_json::to_string(&Value::Object(obj)).expect("update response serializes")
        }
        None => empty_response(),
    }
}

/// The JSON body for a successful `BatchWriteItem`: `{"UnprocessedItems": {}}`
/// (we process every request, so nothing is ever left unprocessed).
#[must_use]
pub fn batch_write_response() -> String {
    let mut obj = Map::new();
    obj.insert("UnprocessedItems".into(), Value::Object(Map::new()));
    serde_json::to_string(&Value::Object(obj)).expect("batch response serializes")
}

/// Apply a sequence of [`UpdateAction`]s to a starting item (`None` ⇒ a fresh
/// item is built from the key by the caller before this), returning the new item.
/// `SET` sets/overwrites a top-level attribute; `REMOVE` drops one. Pure.
#[must_use]
pub fn apply_update(mut item: Item, actions: &[UpdateAction]) -> Item {
    for action in actions {
        match action {
            UpdateAction::Set(attr, value) => {
                item.insert(attr.clone(), value.clone());
            }
            UpdateAction::Remove(attr) => {
                item.remove(attr);
            }
        }
    }
    item
}

/// The JSON body for a successful `Query`: `{"Items": [..], "Count": n,
/// "ScannedCount": n}`. Items are emitted in sort order, as the caller supplies
/// them.
#[must_use]
pub fn query_response(items: &[Item]) -> String {
    let encoded: Vec<Value> = items.iter().map(encode_item).collect();
    let count = Value::from(items.len());
    let mut obj = Map::new();
    obj.insert("Items".into(), Value::Array(encoded));
    obj.insert("Count".into(), count.clone());
    obj.insert("ScannedCount".into(), count);
    serde_json::to_string(&Value::Object(obj)).expect("query response serializes")
}

/// The JSON body for a successful `Scan`: `{"Items": [..], "Count": n,
/// "ScannedCount": s}`, plus a `LastEvaluatedKey` (the AttributeValue-map
/// pagination cursor) when the page was truncated by a `Limit`. `scanned` counts
/// the items read before filtering; `Count` the items returned after.
#[must_use]
pub fn scan_response(items: &[Item], scanned: usize, last_evaluated_key: Option<&Item>) -> String {
    let encoded: Vec<Value> = items.iter().map(encode_item).collect();
    let mut obj = Map::new();
    obj.insert("Items".into(), Value::Array(encoded));
    obj.insert("Count".into(), Value::from(items.len()));
    obj.insert("ScannedCount".into(), Value::from(scanned));
    if let Some(key) = last_evaluated_key {
        obj.insert("LastEvaluatedKey".into(), encode_item(key));
    }
    serde_json::to_string(&Value::Object(obj)).expect("scan response serializes")
}

/// The JSON body for a successful `CreateTable`: a minimal `TableDescription`
/// echoing the name, key schema, any secondary indexes (under
/// `GlobalSecondaryIndexes` / `LocalSecondaryIndexes`), and an `ACTIVE` status
/// (tables are immediately usable here — there is no provisioning phase).
#[must_use]
pub fn create_table_response(
    table: &str,
    schema: &TableSchema,
    indexes: &[SecondaryIndex],
) -> String {
    let mut key_schema = vec![key_schema_entry(&schema.partition_key, "HASH")];
    if let Some(sk) = &schema.sort_key {
        key_schema.push(key_schema_entry(sk, "RANGE"));
    }
    let mut desc = Map::new();
    desc.insert("TableName".into(), Value::String(table.to_owned()));
    desc.insert("KeySchema".into(), Value::Array(key_schema));
    desc.insert("TableStatus".into(), Value::String("ACTIVE".into()));

    let mut gsis = Vec::new();
    let mut lsis = Vec::new();
    for index in indexes {
        match index {
            SecondaryIndex::Global(g) => {
                let mut ks = vec![key_schema_entry(&g.key_attribute, "HASH")];
                if let Some(sort) = &g.sort_attribute {
                    ks.push(key_schema_entry(sort, "RANGE"));
                }
                gsis.push(index_desc(&g.name, ks));
            }
            SecondaryIndex::Local(l) => {
                let ks = vec![
                    key_schema_entry(&schema.partition_key, "HASH"),
                    key_schema_entry(&l.sort_attribute, "RANGE"),
                ];
                lsis.push(index_desc(&l.name, ks));
            }
        }
    }
    if !gsis.is_empty() {
        desc.insert("GlobalSecondaryIndexes".into(), Value::Array(gsis));
    }
    if !lsis.is_empty() {
        desc.insert("LocalSecondaryIndexes".into(), Value::Array(lsis));
    }
    let mut obj = Map::new();
    obj.insert("TableDescription".into(), Value::Object(desc));
    serde_json::to_string(&Value::Object(obj)).expect("create-table response serializes")
}

/// One index entry in a `TableDescription`: name, key schema, `ACTIVE` status.
fn index_desc(name: &str, key_schema: Vec<Value>) -> Value {
    let mut g = Map::new();
    g.insert("IndexName".into(), Value::String(name.to_owned()));
    g.insert("KeySchema".into(), Value::Array(key_schema));
    g.insert("IndexStatus".into(), Value::String("ACTIVE".into()));
    Value::Object(g)
}

fn key_schema_entry(name: &str, role: &str) -> Value {
    let mut e = Map::new();
    e.insert("AttributeName".into(), Value::String(name.to_owned()));
    e.insert("KeyType".into(), Value::String(role.to_owned()));
    Value::Object(e)
}

// --- base64 (standard alphabet, with padding) ------------------------------
//
// A tiny self-contained codec so the crate takes no new dependency for the `B`
// type. Standard alphabet (`A-Za-z0-9+/`) with `=` padding, matching the
// DynamoDB wire encoding for binary attributes.

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
        out.push(B64[b0 >> 2] as char);
        out.push(B64[((b0 & 0x03) << 4) | (b1 >> 4)] as char);
        out.push(if chunk.len() > 1 {
            B64[((b1 & 0x0f) << 2) | (b2 >> 6)] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[b2 & 0x3f] as char
        } else {
            '='
        });
    }
    out
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    if bytes.len() % 4 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let pad = chunk.iter().filter(|&&c| c == b'=').count();
        if pad > 2 {
            return None;
        }
        let q: Vec<u8> = chunk
            .iter()
            .map(|&c| if c == b'=' { Some(0) } else { val(c) })
            .collect::<Option<_>>()?;
        let n = (usize::from(q[0]) << 18)
            | (usize::from(q[1]) << 12)
            | (usize::from(q[2]) << 6)
            | usize::from(q[3]);
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Some(out)
}

/// The serialized form of an item as stored in the data plane. A live item is
/// `{"item": {..}}`; a deleted item is recorded as a tombstone (`{"tombstone":
/// true}`) because the data plane has no native delete yet (ADR 0010). A
/// [`GetItem`] treats a tombstone as absent.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredItem {
    Item(BTreeMap<String, AttributeValue>),
    Tombstone,
}

/// Serialize a live item to the bytes the data plane stores at its key.
#[must_use]
pub fn encode_stored_item(item: &Item) -> Vec<u8> {
    serde_json::to_vec(&StoredItem::Item(item.clone())).expect("stored item serializes")
}

/// Serialize a delete tombstone (the data plane has no native delete).
#[must_use]
pub fn encode_tombstone() -> Vec<u8> {
    serde_json::to_vec(&StoredItem::Tombstone).expect("tombstone serializes")
}

/// Decode bytes read from the data plane back into an item, or `None` for an
/// absent key or a tombstone.
///
/// # Errors
/// Returns a [`WireError`] if the stored bytes are not a valid encoded item.
pub fn decode_stored_item(bytes: &[u8]) -> Result<Option<Item>, WireError> {
    let stored: StoredItem = serde_json::from_slice(bytes)
        .map_err(|e| WireError::serialization(format!("corrupt stored item: {e}")))?;
    Ok(match stored {
        StoredItem::Item(item) => Some(item),
        StoredItem::Tombstone => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.into())
    }

    #[test]
    fn decodes_a_put_item_request() {
        let body = br#"{"TableName":"users","Item":{"id":{"S":"u1"},"n":{"N":"42"},
                        "ok":{"BOOL":true},"void":{"NULL":true}}}"#;
        let op = decode_request("DynamoDB_20120810.PutItem", body).unwrap();
        let Operation::PutItem { table, item, .. } = op else {
            panic!("expected PutItem");
        };
        assert_eq!(table, "users");
        assert_eq!(item.get("id"), Some(&s("u1")));
        assert_eq!(item.get("n"), Some(&AttributeValue::N("42".into())));
        assert_eq!(item.get("ok"), Some(&AttributeValue::Bool(true)));
        assert_eq!(item.get("void"), Some(&AttributeValue::Null));
    }

    #[test]
    fn decodes_get_and_delete_keys() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}}}"#;
        match decode_request("DynamoDB_20120810.GetItem", body).unwrap() {
            Operation::GetItem { table, key, .. } => {
                assert_eq!(table, "t");
                assert_eq!(key.get("id"), Some(&s("k")));
            }
            other => panic!("expected GetItem, got {other:?}"),
        }
        match decode_request("DynamoDB_20120810.DeleteItem", body).unwrap() {
            Operation::DeleteItem { table, .. } => assert_eq!(table, "t"),
            other => panic!("expected DeleteItem, got {other:?}"),
        }
    }

    #[test]
    fn unknown_target_is_rejected() {
        // `BatchGetItem` is still unsupported (BatchWriteItem now is supported).
        let err = decode_request("DynamoDB_20120810.BatchGetItem", b"{}").unwrap_err();
        assert_eq!(err.code, "UnknownOperationException");
    }

    #[test]
    fn unsupported_type_is_rejected() {
        // `XX` is not a DynamoDB attribute type (M/L/SS/NS/BS are now supported).
        let body = br#"{"TableName":"t","Item":{"id":{"XX":""}}}"#;
        let err = decode_request("DynamoDB_20120810.PutItem", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn item_json_round_trips() {
        let mut item = Item::new();
        item.insert("id".into(), s("u1"));
        item.insert("blob".into(), AttributeValue::B(vec![0, 1, 2, 255, 254]));
        let encoded = encode_item(&item);
        let decoded = decode_item(encoded.as_object().unwrap()).unwrap();
        assert_eq!(decoded, item);
    }

    #[test]
    fn document_and_set_types_round_trip() {
        let mut nested = BTreeMap::new();
        nested.insert("k".into(), AttributeValue::N("9".into()));
        let mut item = Item::new();
        item.insert("id".into(), s("u1"));
        item.insert("map".into(), AttributeValue::M(nested));
        item.insert(
            "list".into(),
            AttributeValue::L(vec![
                s("x"),
                AttributeValue::Bool(true),
                AttributeValue::Null,
            ]),
        );
        // Sets are canonicalized (sorted + deduped) on decode.
        item.insert(
            "ss".into(),
            AttributeValue::SS(vec!["a".into(), "b".into()]),
        );
        item.insert(
            "ns".into(),
            AttributeValue::NS(vec!["1".into(), "2".into()]),
        );
        item.insert(
            "bs".into(),
            AttributeValue::BS(vec![vec![0, 1], vec![2, 3]]),
        );
        let encoded = encode_item(&item);
        let decoded = decode_item(encoded.as_object().unwrap()).unwrap();
        assert_eq!(decoded, item);
    }

    #[test]
    fn set_decode_sorts_and_dedups() {
        let body = br#"{"TableName":"t","Item":{"id":{"S":"k"},
            "tags":{"SS":["c","a","a","b"]}}}"#;
        let Operation::PutItem { item, .. } =
            decode_request("DynamoDB_20120810.PutItem", body).unwrap()
        else {
            panic!("expected PutItem");
        };
        assert_eq!(
            item.get("tags"),
            Some(&AttributeValue::SS(vec![
                "a".into(),
                "b".into(),
                "c".into()
            ]))
        );
    }

    #[test]
    fn projection_keeps_only_requested_attributes() {
        let mut item = Item::new();
        item.insert("id".into(), s("u1"));
        item.insert("name".into(), s("Ada"));
        item.insert("secret".into(), s("hidden"));
        let p = Projection(vec!["id".into(), "name".into(), "absent".into()]);
        let projected = p.apply(&item);
        assert_eq!(projected.len(), 2);
        assert_eq!(projected.get("id"), Some(&s("u1")));
        assert_eq!(projected.get("name"), Some(&s("Ada")));
        assert!(!projected.contains_key("secret"));
    }

    #[test]
    fn decodes_projection_expression_with_name_aliases() {
        let body = br##"{"TableName":"t","Key":{"id":{"S":"k"}},
            "ProjectionExpression":"id, #n",
            "ExpressionAttributeNames":{"#n":"name"}}"##;
        let Operation::GetItem { projection, .. } =
            decode_request("DynamoDB_20120810.GetItem", body).unwrap()
        else {
            panic!("expected GetItem");
        };
        assert_eq!(
            projection,
            Some(Projection(vec!["id".into(), "name".into()]))
        );
    }

    #[test]
    fn decodes_attributes_to_get_legacy_projection() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "AttributesToGet":["id","name"]}"#;
        let Operation::GetItem { projection, .. } =
            decode_request("DynamoDB_20120810.GetItem", body).unwrap()
        else {
            panic!("expected GetItem");
        };
        assert_eq!(
            projection,
            Some(Projection(vec!["id".into(), "name".into()]))
        );
    }

    #[test]
    fn decodes_document_path_projection() {
        // Document-path projections (`a.b`) are now supported.
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "ProjectionExpression":"a.b, c"}"#;
        let Operation::GetItem { projection, .. } =
            decode_request("DynamoDB_20120810.GetItem", body).unwrap()
        else {
            panic!("expected GetItem");
        };
        assert_eq!(projection, Some(Projection(vec!["a.b".into(), "c".into()])));
    }

    #[test]
    fn rejects_list_index_projection() {
        // List-index paths (`a[0]`) remain deferred.
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "ProjectionExpression":"a[0]"}"#;
        let err = decode_request("DynamoDB_20120810.GetItem", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn document_path_projection_reconstructs_nested() {
        let mut inner = BTreeMap::new();
        inner.insert("b".into(), s("keep"));
        inner.insert("z".into(), s("drop"));
        let mut item = Item::new();
        item.insert("a".into(), AttributeValue::M(inner));
        item.insert("c".into(), s("top"));
        item.insert("d".into(), s("gone"));
        let projected = Projection(vec!["a.b".into(), "c".into()]).apply(&item);
        // `a` is reconstructed with only `b`; `c` kept; `d` dropped.
        let AttributeValue::M(a) = projected.get("a").expect("a present") else {
            panic!("a is a map");
        };
        assert_eq!(a.get("b"), Some(&s("keep")));
        assert!(!a.contains_key("z"));
        assert_eq!(projected.get("c"), Some(&s("top")));
        assert!(!projected.contains_key("d"));
    }

    #[test]
    fn decodes_return_values_all_old() {
        let body = br#"{"TableName":"t","Item":{"id":{"S":"k"}},
            "ReturnValues":"ALL_OLD"}"#;
        let Operation::PutItem { return_values, .. } =
            decode_request("DynamoDB_20120810.PutItem", body).unwrap()
        else {
            panic!("expected PutItem");
        };
        assert_eq!(return_values, ReturnValues::AllOld);
    }

    #[test]
    fn write_response_echoes_old_item_for_all_old() {
        let mut old = Item::new();
        old.insert("id".into(), s("k"));
        let body = write_response(ReturnValues::AllOld, Some(&old));
        assert!(body.contains("\"Attributes\""));
        assert!(body.contains("\"S\":\"k\""));
        // ALL_OLD on an absent key is `{}`; NONE is always `{}`.
        assert_eq!(write_response(ReturnValues::AllOld, None), "{}");
        assert_eq!(write_response(ReturnValues::None, Some(&old)), "{}");
    }

    #[test]
    fn base64_round_trips_all_lengths() {
        for len in 0..20usize {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 37 % 256) as u8).collect();
            let encoded = base64_encode(&bytes);
            assert_eq!(base64_decode(&encoded), Some(bytes), "len {len}");
        }
    }

    #[test]
    fn get_item_response_omits_missing_item() {
        assert_eq!(get_item_response(None), "{}");
        let mut item = Item::new();
        item.insert("id".into(), s("u1"));
        let body = get_item_response(Some(&item));
        assert!(body.contains("\"Item\""));
        assert!(body.contains("\"S\":\"u1\""));
    }

    #[test]
    fn stored_item_tombstone_reads_as_absent() {
        let mut item = Item::new();
        item.insert("id".into(), s("u1"));
        let bytes = encode_stored_item(&item);
        assert_eq!(decode_stored_item(&bytes).unwrap(), Some(item));
        let tomb = encode_tombstone();
        assert_eq!(decode_stored_item(&tomb).unwrap(), None);
    }

    #[test]
    fn decodes_create_table_with_composite_key() {
        let body = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                    {"AttributeName":"sk","AttributeType":"S"}]}"#;
        match decode_request("DynamoDB_20120810.CreateTable", body).unwrap() {
            Operation::CreateTable {
                table,
                schema,
                indexes,
            } => {
                assert_eq!(table, "t");
                assert_eq!(schema, TableSchema::composite("pk", "sk"));
                assert!(indexes.is_empty());
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn create_table_simple_key() {
        let body = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#;
        match decode_request("DynamoDB_20120810.CreateTable", body).unwrap() {
            Operation::CreateTable { schema, .. } => {
                assert_eq!(schema, TableSchema::simple("id"));
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn decodes_put_with_attribute_not_exists_condition() {
        let body = br#"{"TableName":"t","Item":{"pk":{"S":"a"}},
            "ConditionExpression":"attribute_not_exists(pk)"}"#;
        let Operation::PutItem { condition, .. } =
            decode_request("DynamoDB_20120810.PutItem", body).unwrap()
        else {
            panic!("expected PutItem");
        };
        assert_eq!(
            condition,
            Some(ConditionExpression::AttributeNotExists("pk".into()))
        );
    }

    #[test]
    fn decodes_put_with_equality_condition() {
        let body = br#"{"TableName":"t","Item":{"pk":{"S":"a"}},
            "ConditionExpression":"v = :want",
            "ExpressionAttributeValues":{":want":{"N":"7"}}}"#;
        let Operation::PutItem { condition, .. } =
            decode_request("DynamoDB_20120810.PutItem", body).unwrap()
        else {
            panic!("expected PutItem");
        };
        assert_eq!(
            condition,
            Some(ConditionExpression::Equals(
                "v".into(),
                AttributeValue::N("7".into())
            ))
        );
    }

    #[test]
    fn decodes_query_partition_only() {
        let body = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"part"}}}"#;
        match decode_request("DynamoDB_20120810.Query", body).unwrap() {
            Operation::Query {
                table,
                index,
                partition_value,
                sort_condition,
                projection,
            } => {
                assert_eq!(table, "t");
                assert_eq!(index, None);
                assert_eq!(partition_value, s("part"));
                assert_eq!(sort_condition, None);
                assert_eq!(projection, None);
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn decodes_query_sort_conditions() {
        // equality
        let body = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p AND sk = :s",
            "ExpressionAttributeValues":{":p":{"S":"x"},":s":{"S":"y"}}}"#;
        let Operation::Query { sort_condition, .. } =
            decode_request("DynamoDB_20120810.Query", body).unwrap()
        else {
            panic!("expected Query");
        };
        assert_eq!(sort_condition, Some(SortKeyCondition::Equals(s("y"))));

        // between (mixed case AND/BETWEEN)
        let body = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p and sk between :lo and :hi",
            "ExpressionAttributeValues":{":p":{"S":"x"},":lo":{"S":"a"},":hi":{"S":"m"}}}"#;
        let Operation::Query { sort_condition, .. } =
            decode_request("DynamoDB_20120810.Query", body).unwrap()
        else {
            panic!("expected Query");
        };
        assert_eq!(
            sort_condition,
            Some(SortKeyCondition::Between(s("a"), s("m")))
        );

        // begins_with
        let body = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p AND begins_with(sk, :pre)",
            "ExpressionAttributeValues":{":p":{"S":"x"},":pre":{"S":"ab"}}}"#;
        let Operation::Query { sort_condition, .. } =
            decode_request("DynamoDB_20120810.Query", body).unwrap()
        else {
            panic!("expected Query");
        };
        assert_eq!(sort_condition, Some(SortKeyCondition::BeginsWith(s("ab"))));
    }

    #[test]
    fn query_response_shape() {
        let mut a = Item::new();
        a.insert("pk".into(), s("p"));
        let body = query_response(&[a]);
        assert!(body.contains("\"Items\""));
        assert!(body.contains("\"Count\":1"));
        assert!(body.contains("\"ScannedCount\":1"));
    }

    #[test]
    fn create_table_response_shape() {
        let body = create_table_response("t", &TableSchema::composite("pk", "sk"), &[]);
        assert!(body.contains("\"TableStatus\":\"ACTIVE\""));
        assert!(body.contains("\"HASH\""));
        assert!(body.contains("\"RANGE\""));
        assert!(!body.contains("GlobalSecondaryIndexes"));
    }

    #[test]
    fn decodes_create_table_with_gsi() {
        let body = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-email",
                 "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#;
        match decode_request("DynamoDB_20120810.CreateTable", body).unwrap() {
            Operation::CreateTable { indexes, .. } => {
                assert_eq!(
                    indexes,
                    vec![SecondaryIndex::Global(GlobalSecondaryIndex {
                        name: "by-email".into(),
                        key_attribute: "email".into(),
                        sort_attribute: None,
                        projection: IndexProjection::All,
                    })]
                );
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn decodes_composite_gsi_and_lsi() {
        let body = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"g",
                 "KeySchema":[{"AttributeName":"a","KeyType":"HASH"},
                              {"AttributeName":"b","KeyType":"RANGE"}]}],
            "LocalSecondaryIndexes":[
                {"IndexName":"l",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"alt","KeyType":"RANGE"}]}]}"#;
        match decode_request("DynamoDB_20120810.CreateTable", body).unwrap() {
            Operation::CreateTable { indexes, .. } => {
                assert_eq!(
                    indexes,
                    vec![
                        SecondaryIndex::Global(GlobalSecondaryIndex {
                            name: "g".into(),
                            key_attribute: "a".into(),
                            sort_attribute: Some("b".into()),
                            projection: IndexProjection::All,
                        }),
                        SecondaryIndex::Local(LocalSecondaryIndex {
                            name: "l".into(),
                            sort_attribute: "alt".into(),
                            projection: IndexProjection::All,
                        }),
                    ]
                );
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn rejects_lsi_with_wrong_partition_key() {
        let body = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "LocalSecondaryIndexes":[
                {"IndexName":"l",
                 "KeySchema":[{"AttributeName":"other","KeyType":"HASH"},
                              {"AttributeName":"alt","KeyType":"RANGE"}]}]}"#;
        let err = decode_request("DynamoDB_20120810.CreateTable", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn decodes_query_against_an_index() {
        let body = br#"{"TableName":"t","IndexName":"by-email",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#;
        match decode_request("DynamoDB_20120810.Query", body).unwrap() {
            Operation::Query {
                index,
                partition_value,
                sort_condition,
                ..
            } => {
                assert_eq!(index.as_deref(), Some("by-email"));
                assert_eq!(partition_value, s("a@x"));
                assert_eq!(sort_condition, None);
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn decodes_scan_with_limit_and_filter() {
        let body = br#"{"TableName":"t","Limit":2,
            "ExclusiveStartKey":{"id":{"S":"k5"}},
            "FilterExpression":"attribute_exists(v)"}"#;
        match decode_request("DynamoDB_20120810.Scan", body).unwrap() {
            Operation::Scan {
                table,
                limit,
                exclusive_start_key,
                filter,
                projection,
            } => {
                assert_eq!(table, "t");
                assert_eq!(limit, Some(2));
                assert_eq!(exclusive_start_key.unwrap().get("id"), Some(&s("k5")));
                assert_eq!(
                    filter,
                    Some(ConditionExpression::AttributeExists("v".into()))
                );
                assert_eq!(projection, None);
            }
            other => panic!("expected Scan, got {other:?}"),
        }
    }

    #[test]
    fn scan_response_includes_cursor_when_truncated() {
        let mut a = Item::new();
        a.insert("id".into(), s("k1"));
        let mut key = Item::new();
        key.insert("id".into(), s("k1"));
        let body = scan_response(&[a.clone()], 3, Some(&key));
        assert!(body.contains("\"Count\":1"));
        assert!(body.contains("\"ScannedCount\":3"));
        assert!(body.contains("\"LastEvaluatedKey\""));
        // No cursor when the page was not truncated.
        let body = scan_response(&[a], 1, None);
        assert!(!body.contains("LastEvaluatedKey"));
    }

    #[test]
    fn decodes_update_item_set_and_remove() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"SET a = :v, b = :w REMOVE c",
            "ExpressionAttributeValues":{":v":{"S":"x"},":w":{"N":"3"}},
            "ReturnValues":"ALL_NEW"}"#;
        let Operation::UpdateItem {
            actions,
            return_values,
            ..
        } = decode_request("DynamoDB_20120810.UpdateItem", body).unwrap()
        else {
            panic!("expected UpdateItem");
        };
        assert_eq!(
            actions,
            vec![
                UpdateAction::Set("a".into(), s("x")),
                UpdateAction::Set("b".into(), AttributeValue::N("3".into())),
                UpdateAction::Remove("c".into()),
            ]
        );
        assert_eq!(return_values, UpdateReturnValues::AllNew);
    }

    #[test]
    fn apply_update_sets_and_removes() {
        let mut item = Item::new();
        item.insert("id".into(), s("k"));
        item.insert("c".into(), s("drop"));
        let new = apply_update(
            item,
            &[
                UpdateAction::Set("a".into(), s("x")),
                UpdateAction::Remove("c".into()),
            ],
        );
        assert_eq!(new.get("a"), Some(&s("x")));
        assert!(!new.contains_key("c"));
        assert_eq!(new.get("id"), Some(&s("k")));
    }

    #[test]
    fn decodes_batch_write() {
        let body = br#"{"RequestItems":{
            "t":[{"PutRequest":{"Item":{"id":{"S":"a"}}}},
                 {"DeleteRequest":{"Key":{"id":{"S":"b"}}}}]}}"#;
        let Operation::BatchWriteItem { requests } =
            decode_request("DynamoDB_20120810.BatchWriteItem", body).unwrap()
        else {
            panic!("expected BatchWriteItem");
        };
        let reqs = requests.get("t").expect("table t present");
        assert_eq!(reqs.len(), 2);
        assert!(matches!(reqs[0], WriteRequest::Put(_)));
        assert!(matches!(reqs[1], WriteRequest::Delete(_)));
    }

    #[test]
    fn decodes_transact_write() {
        let body = br#"{"TransactItems":[
            {"Put":{"TableName":"t","Item":{"id":{"S":"a"}},
                    "ConditionExpression":"attribute_not_exists(id)"}},
            {"Update":{"TableName":"t","Key":{"id":{"S":"b"}},
                       "UpdateExpression":"SET v = :v",
                       "ExpressionAttributeValues":{":v":{"N":"1"}}}},
            {"ConditionCheck":{"TableName":"t","Key":{"id":{"S":"c"}},
                               "ConditionExpression":"attribute_exists(id)"}}]}"#;
        let Operation::TransactWriteItems { actions } =
            decode_request("DynamoDB_20120810.TransactWriteItems", body).unwrap()
        else {
            panic!("expected TransactWriteItems");
        };
        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], TransactAction::Put { .. }));
        assert!(matches!(actions[1], TransactAction::Update { .. }));
        assert!(matches!(actions[2], TransactAction::ConditionCheck { .. }));
    }

    #[test]
    fn update_response_echoes_new_for_all_new() {
        let mut new = Item::new();
        new.insert("a".into(), s("x"));
        let body = update_response(UpdateReturnValues::AllNew, None, Some(&new));
        assert!(body.contains("\"Attributes\""));
        assert!(body.contains("\"S\":\"x\""));
        assert_eq!(
            update_response(UpdateReturnValues::None, None, Some(&new)),
            "{}"
        );
    }

    #[test]
    fn decodes_index_projection_types() {
        let body = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"k","KeySchema":[{"AttributeName":"e","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"KEYS_ONLY"}},
                {"IndexName":"i","KeySchema":[{"AttributeName":"o","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"INCLUDE","NonKeyAttributes":["x","y"]}}]}"#;
        let Operation::CreateTable { indexes, .. } =
            decode_request("DynamoDB_20120810.CreateTable", body).unwrap()
        else {
            panic!("expected CreateTable");
        };
        let SecondaryIndex::Global(k) = &indexes[0] else {
            panic!("gsi 0");
        };
        assert_eq!(k.projection, IndexProjection::KeysOnly);
        let SecondaryIndex::Global(i) = &indexes[1] else {
            panic!("gsi 1");
        };
        assert_eq!(
            i.projection,
            IndexProjection::Include(vec!["x".into(), "y".into()])
        );
    }
}
