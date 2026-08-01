//! The DynamoDB JSON wire encoding (ADR 0006).
//!
//! DynamoDB clients speak JSON-over-HTTP: a request body like
//! `{"TableName":"t","Item":{"pk":{"S":"a"},"n":{"N":"1"}}}` and an
//! `X-Amz-Target: DynamoDB_20120810.PutItem` header naming the operation. This
//! module is the **pure, deterministic** translation between that JSON and the
//! crate's in-memory [`Item`] / [`AttributeValue`] model — no I/O, no
//! storage, no network. The transport edge (`custosd`) parses HTTP and routes
//! the decoded request through the distributed data plane; everything below the
//! HTTP edge stays on the `Env`-based paths.
//!
//! ## Supported subset
//!
//! Operations: `CreateTable`, `PutItem`, `GetItem`, `DeleteItem`, `Query`.
//! AttributeValue types: `S` (string), `N` (number, carried as text), `B`
//! (binary, base64), `BOOL`, `NULL` — matching [`AttributeValue`]. `PutItem` /
//! `DeleteItem` accept a small `ConditionExpression` subset (see
//! [`crate::condition`]); `Query` accepts a partition-key equality plus an
//! optional sort-key condition (`=`, `BETWEEN`, `begins_with`). Not supported
//! (rejected with a clear error): document types (`M`/`L`), sets
//! (`SS`/`NS`/`BS`), projection/filter expressions, `Scan`, secondary indexes,
//! and `ReturnValues` — all deferred.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::condition::{ConditionExpression, SortKeyCondition};
use crate::{AttributeValue, Item, TableSchema};

/// The `X-Amz-Target` service+version prefix DynamoDB clients send.
pub const TARGET_PREFIX: &str = "DynamoDB_20120810.";

/// A decoded DynamoDB wire operation (the supported subset).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    /// `CreateTable`: register `schema` under `table`.
    CreateTable {
        /// New table name.
        table: String,
        /// The key schema (partition key + optional sort key).
        schema: TableSchema,
    },
    /// `PutItem`: insert or replace `item` in `table`.
    PutItem {
        /// Target table name.
        table: String,
        /// The item to write (must contain the table's key attributes).
        item: Item,
        /// Optional condition the write is gated on (e.g. `attribute_not_exists`).
        condition: Option<ConditionExpression>,
    },
    /// `GetItem`: fetch the item identified by `key` from `table`.
    GetItem {
        /// Target table name.
        table: String,
        /// The key attributes (partition key, plus sort key for composite tables).
        key: Item,
    },
    /// `DeleteItem`: remove the item identified by `key` from `table`.
    DeleteItem {
        /// Target table name.
        table: String,
        /// The key attributes.
        key: Item,
        /// Optional condition the delete is gated on.
        condition: Option<ConditionExpression>,
    },
    /// `Query`: items in a partition (`pk = ..`) matching an optional sort-key
    /// condition.
    Query {
        /// Target table name.
        table: String,
        /// The partition-key value (equality).
        partition_value: AttributeValue,
        /// Optional sort-key narrowing.
        sort_condition: Option<SortKeyCondition>,
    },
}

impl Operation {
    /// The table this operation targets.
    #[must_use]
    pub fn table(&self) -> &str {
        match self {
            Operation::CreateTable { table, .. }
            | Operation::PutItem { table, .. }
            | Operation::GetItem { table, .. }
            | Operation::DeleteItem { table, .. }
            | Operation::Query { table, .. } => table,
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
            Ok(Operation::CreateTable { table, schema })
        }
        "PutItem" => {
            let table = table_name(obj)?;
            let item = decode_item_field(obj, "Item")?;
            let condition = decode_condition(obj)?;
            Ok(Operation::PutItem {
                table,
                item,
                condition,
            })
        }
        "GetItem" => {
            let table = table_name(obj)?;
            let key = decode_item_field(obj, "Key")?;
            Ok(Operation::GetItem { table, key })
        }
        "DeleteItem" => {
            let table = table_name(obj)?;
            let key = decode_item_field(obj, "Key")?;
            let condition = decode_condition(obj)?;
            Ok(Operation::DeleteItem {
                table,
                key,
                condition,
            })
        }
        "Query" => decode_query(obj),
        _ => Err(WireError::unknown_operation(target)),
    }
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

/// Decode the optional `ConditionExpression` + `ExpressionAttributeValues` of a
/// write. Supported forms: `attribute_not_exists(attr)`,
/// `attribute_exists(attr)`, and `attr = :placeholder`. Absent ⇒ `Ok(None)`.
fn decode_condition(obj: &Map<String, Value>) -> Result<Option<ConditionExpression>, WireError> {
    let Some(expr) = obj.get("ConditionExpression").and_then(Value::as_str) else {
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
            "unsupported ConditionExpression `{expr}` (supported: \
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

    let sort_condition = match sort_clause {
        None => None,
        Some(clause) => Some(decode_sort_condition(obj, clause)?),
    };
    Ok(Operation::Query {
        table,
        partition_value,
        sort_condition,
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
        other => Err(WireError::validation(format!(
            "attribute `{name}` uses unsupported type `{other}` \
             (only S, N, B, BOOL, NULL are supported)"
        ))),
    }
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

/// The JSON body for a successful `PutItem` / `DeleteItem`: `{}` (we do not
/// implement `ReturnValues`, so nothing is echoed back).
#[must_use]
pub fn empty_response() -> String {
    "{}".to_string()
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

/// The JSON body for a successful `CreateTable`: a minimal `TableDescription`
/// echoing the name, key schema, and an `ACTIVE` status (tables are immediately
/// usable here — there is no provisioning phase).
#[must_use]
pub fn create_table_response(table: &str, schema: &TableSchema) -> String {
    let mut key_schema = vec![key_schema_entry(&schema.partition_key, "HASH")];
    if let Some(sk) = &schema.sort_key {
        key_schema.push(key_schema_entry(sk, "RANGE"));
    }
    let mut desc = Map::new();
    desc.insert("TableName".into(), Value::String(table.to_owned()));
    desc.insert("KeySchema".into(), Value::Array(key_schema));
    desc.insert("TableStatus".into(), Value::String("ACTIVE".into()));
    let mut obj = Map::new();
    obj.insert("TableDescription".into(), Value::Object(desc));
    serde_json::to_string(&Value::Object(obj)).expect("create-table response serializes")
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
            Operation::GetItem { table, key } => {
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
        let err = decode_request("DynamoDB_20120810.Scan", b"{}").unwrap_err();
        assert_eq!(err.code, "UnknownOperationException");
    }

    #[test]
    fn unsupported_type_is_rejected() {
        let body = br#"{"TableName":"t","Item":{"id":{"M":{}}}}"#;
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
            Operation::CreateTable { table, schema } => {
                assert_eq!(table, "t");
                assert_eq!(schema, TableSchema::composite("pk", "sk"));
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
                partition_value,
                sort_condition,
            } => {
                assert_eq!(table, "t");
                assert_eq!(partition_value, s("part"));
                assert_eq!(sort_condition, None);
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
        let body = create_table_response("t", &TableSchema::composite("pk", "sk"));
        assert!(body.contains("\"TableStatus\":\"ACTIVE\""));
        assert!(body.contains("\"HASH\""));
        assert!(body.contains("\"RANGE\""));
    }
}
