//! The DynamoDB **Streams** JSON wire encoding (ADR 0042 §3/§5/§6/§7, PR6):
//! `X-Amz-Target: DynamoDBStreams_20120810.<Op>` decode + response encode for
//! `ListStreams`/`DescribeStream`/`GetShardIterator`/`GetRecords`. Pure and
//! deterministic, mirroring `wire.rs`'s own conventions (an `Operation`-shaped
//! decode, `WireError`, hand-built `serde_json::Value` response objects) for
//! the sibling `DynamoDBStreams_20120810` service that shares this crate's
//! listener (`animusd::dynamo`'s dispatch, decided F-fork). No I/O, no
//! storage — every catalog/tablet-map lookup, label-resolution decision, and
//! store fetch lives in `animusd`, which holds `Metadata` and the segment
//! store; this module only ever translates between JSON and already-decided
//! values.
//!
//! ## Shard ids and iterator tokens
//!
//! `ShardId` is `animus_cp_data::segment::shard_id`'s own
//! `shardId-<tablet>-<epoch>` string — [`parse_shard_id`] is its inverse.
//! A shard iterator is a **stateless, non-expiring** opaque token
//! (`base64url({label, shard_id, position})`, ADR 0042 §6's documented
//! deviation from real DynamoDB's 15-minute-expiring ones) —
//! [`encode_iterator`]/[`decode_iterator`]. `position` is always the
//! **exclusive** lower bound of the next read (`packed_hlc > position`),
//! the same convention `animus_cp_data::segment::slice_to_hlc_range`'s
//! `start_exclusive` and `index_drain::hot_read`'s `from_position` already
//! use — so a token minted against either tier composes with the other's
//! filter with no translation step.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::Item;
use crate::index::ChangeRecord;
use crate::wire::{
    WireError, base64url_decode, base64url_encode, stream_arn, stream_view_type_str,
};
use animus_control::StreamViewType;

/// The `X-Amz-Target` service+version prefix DynamoDB Streams clients send.
pub const TARGET_PREFIX: &str = "DynamoDBStreams_20120810.";

/// A decoded DynamoDB Streams operation (ADR 0042 §3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamsOperation {
    ListStreams {
        table_name: Option<String>,
        limit: Option<usize>,
        exclusive_start_stream_arn: Option<String>,
    },
    DescribeStream {
        stream_arn: String,
        limit: Option<usize>,
        exclusive_start_shard_id: Option<String>,
    },
    GetShardIterator {
        stream_arn: String,
        shard_id: String,
        shard_iterator_type: ShardIteratorType,
        sequence_number: Option<String>,
    },
    GetRecords {
        shard_iterator: String,
        limit: Option<usize>,
    },
}

/// `GetShardIterator`'s `ShardIteratorType` (ADR 0042 §5/§6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardIteratorType {
    TrimHorizon,
    Latest,
    AtSequenceNumber,
    AfterSequenceNumber,
}

/// Decode a DynamoDB Streams request body for the operation named by
/// `target` (the full `X-Amz-Target` header value, e.g.
/// `DynamoDBStreams_20120810.GetRecords`).
///
/// # Errors
/// Returns a [`WireError`] if the target is unsupported or the body is invalid.
pub fn decode_request(target: &str, body: &[u8]) -> Result<StreamsOperation, WireError> {
    let op = target.strip_prefix(TARGET_PREFIX).unwrap_or(target);
    let json: Value = serde_json::from_slice(body)
        .map_err(|e| WireError::serialization(format!("invalid JSON body: {e}")))?;
    let obj = json
        .as_object()
        .ok_or_else(|| WireError::validation("request body must be a JSON object"))?;
    match op {
        "ListStreams" => Ok(StreamsOperation::ListStreams {
            table_name: opt_str(obj, "TableName"),
            limit: opt_usize(obj, "Limit")?,
            exclusive_start_stream_arn: opt_str(obj, "ExclusiveStartStreamArn"),
        }),
        "DescribeStream" => Ok(StreamsOperation::DescribeStream {
            stream_arn: require_str(obj, "StreamArn")?,
            limit: opt_usize(obj, "Limit")?,
            exclusive_start_shard_id: opt_str(obj, "ExclusiveStartShardId"),
        }),
        "GetShardIterator" => {
            let raw_type = require_str(obj, "ShardIteratorType")?;
            let shard_iterator_type = match raw_type.as_str() {
                "TRIM_HORIZON" => ShardIteratorType::TrimHorizon,
                "LATEST" => ShardIteratorType::Latest,
                "AT_SEQUENCE_NUMBER" => ShardIteratorType::AtSequenceNumber,
                "AFTER_SEQUENCE_NUMBER" => ShardIteratorType::AfterSequenceNumber,
                other => {
                    return Err(WireError::validation(format!(
                        "unsupported `ShardIteratorType` `{other}`"
                    )));
                }
            };
            let sequence_number = opt_str(obj, "SequenceNumber");
            if matches!(
                shard_iterator_type,
                ShardIteratorType::AtSequenceNumber | ShardIteratorType::AfterSequenceNumber
            ) && sequence_number.is_none()
            {
                return Err(WireError::validation(
                    "AT_SEQUENCE_NUMBER/AFTER_SEQUENCE_NUMBER requires `SequenceNumber`",
                ));
            }
            Ok(StreamsOperation::GetShardIterator {
                stream_arn: require_str(obj, "StreamArn")?,
                shard_id: require_str(obj, "ShardId")?,
                shard_iterator_type,
                sequence_number,
            })
        }
        "GetRecords" => Ok(StreamsOperation::GetRecords {
            shard_iterator: require_str(obj, "ShardIterator")?,
            limit: opt_usize(obj, "Limit")?,
        }),
        other => Err(WireError::unknown_operation(other)),
    }
}

fn require_str(obj: &Map<String, Value>, field: &str) -> Result<String, WireError> {
    obj.get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| WireError::validation(format!("missing or non-string `{field}`")))
}

fn opt_str(obj: &Map<String, Value>, field: &str) -> Option<String> {
    obj.get(field).and_then(Value::as_str).map(str::to_owned)
}

fn opt_usize(obj: &Map<String, Value>, field: &str) -> Result<Option<usize>, WireError> {
    match obj.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v.as_u64().map(|n| Some(n as usize)).ok_or_else(|| {
            WireError::validation(format!("`{field}` must be a non-negative integer"))
        }),
    }
}

// --- shard ids ---------------------------------------------------------

/// Parse a `ShardId` (`shardId-<tablet>-<epoch>`) into its `(tablet, epoch)`
/// pair — the inverse of `animus_cp_data::segment::shard_id`, duplicated
/// here rather than depending on that crate (this crate stays independent
/// of the data plane; `animusd` already bridges both).
///
/// # Errors
/// Returns `None` for anything not matching the exact shape.
#[must_use]
pub fn parse_shard_id(shard_id: &str) -> Option<(u64, u64)> {
    let rest = shard_id.strip_prefix("shardId-")?;
    let (tablet, epoch) = rest.split_once('-')?;
    Some((tablet.parse().ok()?, epoch.parse().ok()?))
}

/// Parse this adapter's synthetic stream ARN
/// (`arn:aws:dynamodb:animus:0:table/<table>/stream/<label>`, [`stream_arn`])
/// back into `(table, label)`.
///
/// # Errors
/// Returns `None` for anything not matching the exact shape.
#[must_use]
pub fn parse_stream_arn(arn: &str) -> Option<(String, String)> {
    let rest = arn.strip_prefix("arn:aws:dynamodb:animus:0:table/")?;
    let (table, rest) = rest.split_once("/stream/")?;
    if table.is_empty() || rest.is_empty() {
        return None;
    }
    Some((table.to_owned(), rest.to_owned()))
}

// --- iterator tokens -----------------------------------------------------

#[derive(Serialize, Deserialize)]
struct IteratorToken {
    label: String,
    shard_id: String,
    position: u64,
}

/// Mint a stateless, non-expiring shard iterator token (ADR 0042 §6):
/// `base64url({label, shard_id, position})`. `position` is the **exclusive**
/// lower bound the next `GetRecords` call filters on (`packed_hlc >
/// position`) — see the module doc.
#[must_use]
pub fn encode_iterator(label: &str, shard_id: &str, position: u64) -> String {
    let token = IteratorToken {
        label: label.to_owned(),
        shard_id: shard_id.to_owned(),
        position,
    };
    let bytes = serde_json::to_vec(&token).expect("iterator token serializes");
    base64url_encode(&bytes)
}

/// Decode a shard iterator token minted by [`encode_iterator`].
///
/// # Errors
/// A [`WireError::validation`] for anything that fails to decode — a
/// tampered, truncated, or foreign token.
pub fn decode_iterator(token: &str) -> Result<(String, String, u64), WireError> {
    let bytes =
        base64url_decode(token).ok_or_else(|| WireError::validation("malformed shard iterator"))?;
    let parsed: IteratorToken = serde_json::from_slice(&bytes)
        .map_err(|_| WireError::validation("malformed shard iterator"))?;
    Ok((parsed.label, parsed.shard_id, parsed.position))
}

// --- sequence numbers ------------------------------------------------------

/// Parse a `SequenceNumber` string (a decimal packed HLC, ADR 0042 §5) into
/// its `u64`.
///
/// # Errors
/// A [`WireError::validation`] if it is not a valid decimal `u64`.
pub fn parse_sequence_number(s: &str) -> Result<u64, WireError> {
    s.parse()
        .map_err(|_| WireError::validation(format!("malformed SequenceNumber `{s}`")))
}

// --- response encoding -----------------------------------------------------

/// One stream `ListStreams` enumerates (ADR 0042 §3).
#[derive(Debug, Clone)]
pub struct StreamSummary {
    pub table_name: String,
    pub stream_label: String,
}

/// The JSON body for `ListStreams`.
#[must_use]
pub fn list_streams_response(
    streams: &[StreamSummary],
    last_evaluated_stream_arn: Option<&str>,
) -> String {
    let items: Vec<Value> = streams
        .iter()
        .map(|s| {
            let mut o = Map::new();
            o.insert(
                "StreamArn".into(),
                Value::String(stream_arn(&s.table_name, &s.stream_label)),
            );
            o.insert("TableName".into(), Value::String(s.table_name.clone()));
            o.insert("StreamLabel".into(), Value::String(s.stream_label.clone()));
            Value::Object(o)
        })
        .collect();
    let mut obj = Map::new();
    obj.insert("Streams".into(), Value::Array(items));
    if let Some(arn) = last_evaluated_stream_arn {
        obj.insert(
            "LastEvaluatedStreamArn".into(),
            Value::String(arn.to_owned()),
        );
    }
    serde_json::to_string(&Value::Object(obj)).expect("list-streams response serializes")
}

/// One shard `DescribeStream` reports (ADR 0042 §2/ADR 0043 §A4): a closed
/// shard carries `ending_sequence_number`; the one open shard per live
/// tablet (only while `ENABLED`) does not.
#[derive(Debug, Clone)]
pub struct ShardDescriptor {
    pub shard_id: String,
    pub parent_shard_id: Option<String>,
    pub starting_sequence_number: u64,
    pub ending_sequence_number: Option<u64>,
}

/// The pieces [`describe_stream_response`] needs — everything computed by
/// `animusd` from `Metadata` (the catalog + tablet map + schema), never
/// re-derived here.
#[derive(Debug, Clone)]
pub struct StreamDescription {
    pub table_name: String,
    pub stream_label: String,
    /// `true` while the table's *current* schema names this exact label as
    /// its enabled stream; `false` during F12-b's disable grace window.
    pub enabled: bool,
    pub view_type: StreamViewType,
    pub partition_key: String,
    pub sort_key: Option<String>,
    pub shards: Vec<ShardDescriptor>,
    pub last_evaluated_shard_id: Option<String>,
}

/// The JSON body for `DescribeStream` (ADR 0042 §3).
#[must_use]
pub fn describe_stream_response(desc: &StreamDescription) -> String {
    let mut key_schema = vec![key_schema_entry(&desc.partition_key, "HASH")];
    if let Some(sk) = &desc.sort_key {
        key_schema.push(key_schema_entry(sk, "RANGE"));
    }
    let shards: Vec<Value> = desc.shards.iter().map(shard_json).collect();

    let mut sd = Map::new();
    sd.insert(
        "StreamArn".into(),
        Value::String(stream_arn(&desc.table_name, &desc.stream_label)),
    );
    sd.insert(
        "StreamLabel".into(),
        Value::String(desc.stream_label.clone()),
    );
    sd.insert(
        "StreamStatus".into(),
        Value::String(if desc.enabled { "ENABLED" } else { "DISABLED" }.into()),
    );
    sd.insert(
        "StreamViewType".into(),
        Value::String(stream_view_type_str(desc.view_type).into()),
    );
    sd.insert("TableName".into(), Value::String(desc.table_name.clone()));
    sd.insert("KeySchema".into(), Value::Array(key_schema));
    sd.insert("Shards".into(), Value::Array(shards));
    if let Some(id) = &desc.last_evaluated_shard_id {
        sd.insert("LastEvaluatedShardId".into(), Value::String(id.clone()));
    }
    let mut obj = Map::new();
    obj.insert("StreamDescription".into(), Value::Object(sd));
    serde_json::to_string(&Value::Object(obj)).expect("describe-stream response serializes")
}

fn shard_json(shard: &ShardDescriptor) -> Value {
    let mut range = Map::new();
    range.insert(
        "StartingSequenceNumber".into(),
        Value::String(shard.starting_sequence_number.to_string()),
    );
    if let Some(end) = shard.ending_sequence_number {
        range.insert(
            "EndingSequenceNumber".into(),
            Value::String(end.to_string()),
        );
    }
    let mut s = Map::new();
    s.insert("ShardId".into(), Value::String(shard.shard_id.clone()));
    if let Some(parent) = &shard.parent_shard_id {
        s.insert("ParentShardId".into(), Value::String(parent.clone()));
    }
    s.insert("SequenceNumberRange".into(), Value::Object(range));
    Value::Object(s)
}

fn key_schema_entry(name: &str, role: &str) -> Value {
    let mut e = Map::new();
    e.insert("AttributeName".into(), Value::String(name.to_owned()));
    e.insert("KeyType".into(), Value::String(role.to_owned()));
    Value::Object(e)
}

/// The JSON body for a successful `GetShardIterator`.
#[must_use]
pub fn get_shard_iterator_response(iterator: &str) -> String {
    let mut obj = Map::new();
    obj.insert("ShardIterator".into(), Value::String(iterator.to_owned()));
    serde_json::to_string(&Value::Object(obj)).expect("get-shard-iterator response serializes")
}

/// The JSON body for `GetRecords`. `next_shard_iterator: None` is DynamoDB's
/// own "this shard is exhausted, walk to its child" signal — never encoded
/// as `null`'s absence being ambiguous with "not yet computed": every
/// caller of this function always has a definite decision either way.
#[must_use]
pub fn get_records_response(records: Vec<Value>, next_shard_iterator: Option<&str>) -> String {
    let mut obj = Map::new();
    obj.insert("Records".into(), Value::Array(records));
    if let Some(it) = next_shard_iterator {
        obj.insert("NextShardIterator".into(), Value::String(it.to_owned()));
    }
    serde_json::to_string(&Value::Object(obj)).expect("get-records response serializes")
}

// --- record projection ------------------------------------------------------

/// Project `record`'s old/new images per `view_type` (ADR 0042 §3/§15 — a
/// **read-time** projection only; a shard always stores both images
/// regardless of the declared view type).
#[must_use]
pub fn project_view(
    view_type: StreamViewType,
    old_image: Option<Item>,
    new_image: Option<Item>,
) -> (Option<Item>, Option<Item>) {
    match view_type {
        StreamViewType::NewAndOldImages => (old_image, new_image),
        StreamViewType::NewImage => (None, new_image),
        StreamViewType::OldImage => (old_image, None),
        StreamViewType::KeysOnly => (None, None),
    }
}

/// Recover a change record's key attributes (`Keys`, always present
/// regardless of view type) from whichever image is available — the new
/// image (present for `INSERT`/`MODIFY`), else the old one (`REMOVE`). Both
/// images always carry the full item, key attributes included, so no
/// base-table read-back is ever needed.
#[must_use]
pub fn keys_from_images(
    partition_key: &str,
    sort_key: Option<&str>,
    old_image: Option<&Item>,
    new_image: Option<&Item>,
) -> Item {
    let src = new_image.or(old_image);
    let mut keys = Item::new();
    if let Some(src) = src {
        if let Some(v) = src.get(partition_key) {
            keys.insert(partition_key.to_owned(), v.clone());
        }
        if let Some(sk) = sort_key
            && let Some(v) = src.get(sk)
        {
            keys.insert(sk.to_owned(), v.clone());
        }
    }
    keys
}

/// Build one `Records[]` entry (ADR 0042 §3, the AWS `Record` shape) from a
/// decoded [`ChangeRecord`] at shard `shard_id`/sequence number
/// `packed_hlc`, projected per `view_type`.
#[must_use]
pub fn stream_record_json(
    shard_id: &str,
    packed_hlc: u64,
    record: &ChangeRecord,
    view_type: StreamViewType,
    partition_key: &str,
    sort_key: Option<&str>,
    approx_creation_wall_ms: u64,
) -> Value {
    let event_name = record.event_name();
    let seq = packed_hlc.to_string();
    let keys = keys_from_images(
        partition_key,
        sort_key,
        record.old_image.as_ref(),
        record.new_image.as_ref(),
    );
    let (old_image, new_image) = project_view(
        view_type,
        record.old_image.clone(),
        record.new_image.clone(),
    );

    let mut dynamodb = Map::new();
    dynamodb.insert("Keys".into(), crate::wire::encode_item(&keys));
    if let Some(img) = &old_image {
        dynamodb.insert("OldImage".into(), crate::wire::encode_item(img));
    }
    if let Some(img) = &new_image {
        dynamodb.insert("NewImage".into(), crate::wire::encode_item(img));
    }
    dynamodb.insert("SequenceNumber".into(), Value::String(seq));
    dynamodb.insert(
        "SizeBytes".into(),
        Value::Number(serde_json::Number::from(record.encode().len() as u64)),
    );
    dynamodb.insert(
        "StreamViewType".into(),
        Value::String(stream_view_type_str(view_type).into()),
    );
    dynamodb.insert(
        "ApproximateCreationDateTime".into(),
        Value::Number(
            serde_json::Number::from_f64((approx_creation_wall_ms as f64) / 1000.0)
                .unwrap_or_else(|| serde_json::Number::from(0)),
        ),
    );

    let mut r = Map::new();
    r.insert(
        "eventID".into(),
        Value::String(format!("{shard_id}-{packed_hlc}")),
    );
    r.insert("eventName".into(), Value::String(event_name.into()));
    r.insert("eventVersion".into(), Value::String("1.1".into()));
    r.insert("eventSource".into(), Value::String("aws:dynamodb".into()));
    r.insert("awsRegion".into(), Value::String("animus".into()));
    r.insert("dynamodb".into(), Value::Object(dynamodb));
    Value::Object(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AttributeValue;

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.into())
    }

    #[test]
    fn decodes_list_streams_with_all_fields() {
        let body = br#"{"TableName":"orders","Limit":10,"ExclusiveStartStreamArn":"arn:x"}"#;
        let op = decode_request("DynamoDBStreams_20120810.ListStreams", body).unwrap();
        assert_eq!(
            op,
            StreamsOperation::ListStreams {
                table_name: Some("orders".into()),
                limit: Some(10),
                exclusive_start_stream_arn: Some("arn:x".into()),
            }
        );
    }

    #[test]
    fn decodes_list_streams_with_no_fields() {
        let op = decode_request("DynamoDBStreams_20120810.ListStreams", b"{}").unwrap();
        assert_eq!(
            op,
            StreamsOperation::ListStreams {
                table_name: None,
                limit: None,
                exclusive_start_stream_arn: None,
            }
        );
    }

    #[test]
    fn decodes_describe_stream() {
        let body = br#"{"StreamArn":"arn:x","Limit":5,"ExclusiveStartShardId":"shardId-1-0"}"#;
        let op = decode_request("DynamoDBStreams_20120810.DescribeStream", body).unwrap();
        assert_eq!(
            op,
            StreamsOperation::DescribeStream {
                stream_arn: "arn:x".into(),
                limit: Some(5),
                exclusive_start_shard_id: Some("shardId-1-0".into()),
            }
        );
    }

    #[test]
    fn decodes_get_shard_iterator_trim_horizon() {
        let body =
            br#"{"StreamArn":"arn:x","ShardId":"shardId-1-0","ShardIteratorType":"TRIM_HORIZON"}"#;
        let op = decode_request("DynamoDBStreams_20120810.GetShardIterator", body).unwrap();
        assert_eq!(
            op,
            StreamsOperation::GetShardIterator {
                stream_arn: "arn:x".into(),
                shard_id: "shardId-1-0".into(),
                shard_iterator_type: ShardIteratorType::TrimHorizon,
                sequence_number: None,
            }
        );
    }

    #[test]
    fn get_shard_iterator_at_sequence_number_requires_sequence_number() {
        let body = br#"{"StreamArn":"arn:x","ShardId":"shardId-1-0","ShardIteratorType":"AT_SEQUENCE_NUMBER"}"#;
        let err = decode_request("DynamoDBStreams_20120810.GetShardIterator", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn decodes_get_records() {
        let body = br#"{"ShardIterator":"tok","Limit":100}"#;
        let op = decode_request("DynamoDBStreams_20120810.GetRecords", body).unwrap();
        assert_eq!(
            op,
            StreamsOperation::GetRecords {
                shard_iterator: "tok".into(),
                limit: Some(100),
            }
        );
    }

    #[test]
    fn unknown_operation_is_rejected() {
        let err = decode_request("DynamoDBStreams_20120810.Bogus", b"{}").unwrap_err();
        assert_eq!(err.code, "UnknownOperationException");
    }

    #[test]
    fn parses_and_rejects_shard_ids() {
        assert_eq!(parse_shard_id("shardId-7-3"), Some((7, 3)));
        assert_eq!(parse_shard_id("shardId-7"), None);
        assert_eq!(parse_shard_id("bogus-7-3"), None);
        assert_eq!(parse_shard_id("shardId-x-3"), None);
    }

    #[test]
    fn parses_and_rejects_stream_arns() {
        let arn = stream_arn("orders", "L1");
        assert_eq!(
            parse_stream_arn(&arn),
            Some(("orders".to_string(), "L1".to_string()))
        );
        assert_eq!(parse_stream_arn("not-an-arn"), None);
        assert_eq!(
            parse_stream_arn("arn:aws:dynamodb:animus:0:table//stream/"),
            None
        );
    }

    #[test]
    fn iterator_token_round_trips() {
        let tok = encode_iterator("L1", "shardId-1-0", 42);
        let (label, shard_id, position) = decode_iterator(&tok).unwrap();
        assert_eq!(label, "L1");
        assert_eq!(shard_id, "shardId-1-0");
        assert_eq!(position, 42);
    }

    #[test]
    fn tampered_iterator_token_is_rejected() {
        let mut tok = encode_iterator("L1", "shardId-1-0", 42);
        tok.push('!'); // corrupt the base64url
        let err = decode_iterator(&tok).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn garbage_iterator_token_is_rejected() {
        let err = decode_iterator("not-a-real-token").unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn sequence_number_round_trips() {
        assert_eq!(parse_sequence_number("12345").unwrap(), 12345);
        assert!(parse_sequence_number("not-a-number").is_err());
    }

    #[test]
    fn view_projection_matches_declared_type() {
        let old = Some(Item::from([("pk".to_string(), s("a"))]));
        let new = Some(Item::from([("pk".to_string(), s("b"))]));
        assert_eq!(
            project_view(StreamViewType::NewAndOldImages, old.clone(), new.clone()),
            (old.clone(), new.clone())
        );
        assert_eq!(
            project_view(StreamViewType::NewImage, old.clone(), new.clone()),
            (None, new.clone())
        );
        assert_eq!(
            project_view(StreamViewType::OldImage, old.clone(), new.clone()),
            (old.clone(), None)
        );
        assert_eq!(
            project_view(StreamViewType::KeysOnly, old, new),
            (None, None)
        );
    }

    #[test]
    fn keys_recovered_from_new_image_preferred_over_old() {
        let old = Item::from([
            ("pk".to_string(), s("old-pk")),
            ("sk".to_string(), s("old-sk")),
        ]);
        let new = Item::from([
            ("pk".to_string(), s("new-pk")),
            ("sk".to_string(), s("new-sk")),
        ]);
        let keys = keys_from_images("pk", Some("sk"), Some(&old), Some(&new));
        assert_eq!(keys.get("pk"), Some(&s("new-pk")));
        assert_eq!(keys.get("sk"), Some(&s("new-sk")));
    }

    #[test]
    fn keys_recovered_from_old_image_when_new_is_absent() {
        // REMOVE: no new image.
        let old = Item::from([("pk".to_string(), s("gone"))]);
        let keys = keys_from_images("pk", None, Some(&old), None);
        assert_eq!(keys.get("pk"), Some(&s("gone")));
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn stream_record_json_shape() {
        let record = ChangeRecord {
            base_sk: Vec::new(),
            old_image: None,
            new_image: Some(Item::from([("pk".to_string(), s("alice"))])),
            seeded: false,
            marker: false,
        };
        let v = stream_record_json(
            "shardId-1-0",
            42,
            &record,
            StreamViewType::NewAndOldImages,
            "pk",
            None,
            1_700_000_000_000,
        );
        assert_eq!(v["eventID"], "shardId-1-0-42");
        assert_eq!(v["eventName"], "INSERT");
        assert_eq!(v["eventVersion"], "1.1");
        assert_eq!(v["eventSource"], "aws:dynamodb");
        assert_eq!(v["awsRegion"], "animus");
        assert_eq!(v["dynamodb"]["SequenceNumber"], "42");
        assert_eq!(v["dynamodb"]["StreamViewType"], "NEW_AND_OLD_IMAGES");
        assert!(v["dynamodb"]["NewImage"].is_object());
        assert!(v["dynamodb"]["OldImage"].is_null());
        assert!(v["dynamodb"]["Keys"].is_object());
    }

    #[test]
    fn stream_record_json_keys_only_omits_both_images() {
        let record = ChangeRecord {
            base_sk: Vec::new(),
            old_image: Some(Item::from([("pk".to_string(), s("alice"))])),
            new_image: Some(Item::from([("pk".to_string(), s("alice2"))])),
            seeded: false,
            marker: false,
        };
        let v = stream_record_json(
            "shardId-1-0",
            7,
            &record,
            StreamViewType::KeysOnly,
            "pk",
            None,
            1_700_000_000_000,
        );
        assert!(v["dynamodb"]["OldImage"].is_null());
        assert!(v["dynamodb"]["NewImage"].is_null());
        assert!(v["dynamodb"]["Keys"].is_object());
    }

    #[test]
    fn list_streams_response_shape() {
        let body = list_streams_response(
            &[StreamSummary {
                table_name: "orders".into(),
                stream_label: "L1".into(),
            }],
            Some("arn:next"),
        );
        let v: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["Streams"][0]["TableName"], "orders");
        assert_eq!(v["Streams"][0]["StreamLabel"], "L1");
        assert_eq!(v["Streams"][0]["StreamArn"], stream_arn("orders", "L1"));
        assert_eq!(v["LastEvaluatedStreamArn"], "arn:next");
    }

    #[test]
    fn describe_stream_response_shape() {
        let desc = StreamDescription {
            table_name: "orders".into(),
            stream_label: "L1".into(),
            enabled: true,
            view_type: StreamViewType::NewImage,
            partition_key: "pk".into(),
            sort_key: Some("sk".into()),
            shards: vec![
                ShardDescriptor {
                    shard_id: "shardId-1-0".into(),
                    parent_shard_id: None,
                    starting_sequence_number: 0,
                    ending_sequence_number: Some(100),
                },
                ShardDescriptor {
                    shard_id: "shardId-1-1".into(),
                    parent_shard_id: Some("shardId-1-0".into()),
                    starting_sequence_number: 100,
                    ending_sequence_number: None,
                },
            ],
            last_evaluated_shard_id: None,
        };
        let body = describe_stream_response(&desc);
        let v: Value = serde_json::from_str(&body).unwrap();
        let sd = &v["StreamDescription"];
        assert_eq!(sd["StreamStatus"], "ENABLED");
        assert_eq!(sd["StreamViewType"], "NEW_IMAGE");
        assert_eq!(sd["KeySchema"][0]["AttributeName"], "pk");
        assert_eq!(sd["KeySchema"][1]["AttributeName"], "sk");
        assert_eq!(sd["Shards"][0]["ShardId"], "shardId-1-0");
        assert_eq!(
            sd["Shards"][0]["SequenceNumberRange"]["EndingSequenceNumber"],
            "100"
        );
        assert!(sd["Shards"][0]["ParentShardId"].is_null());
        assert_eq!(sd["Shards"][1]["ParentShardId"], "shardId-1-0");
        assert!(sd["Shards"][1]["SequenceNumberRange"]["EndingSequenceNumber"].is_null());
    }

    #[test]
    fn get_records_response_null_iterator_signals_exhaustion() {
        let body = get_records_response(vec![], None);
        let v: Value = serde_json::from_str(&body).unwrap();
        assert!(v["Records"].as_array().unwrap().is_empty());
        assert!(v["NextShardIterator"].is_null());
    }

    #[test]
    fn get_records_response_carries_next_iterator() {
        let body = get_records_response(vec![], Some("tok"));
        let v: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["NextShardIterator"], "tok");
    }
}
