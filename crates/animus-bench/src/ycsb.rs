//! YCSB-on-DynamoDB: table setup, bulk load, and the [`OpExecutor`] that
//! maps each [`Op`] onto real wire requests. See [`crate::workload`] for the
//! key layout.
//!
//! # Mapping (every read honours `consistent_read`)
//!
//! | class | wire |
//! |---|---|
//! | `read` | `GetItem` |
//! | `update` | `UpdateItem SET data = :v` (unconditional) |
//! | `insert` | `PutItem` of a new key (`version = 0`) |
//! | `scan` | `Query pk = :p AND sk >= :s`, `Limit = len` |
//! | `read_modify_write` | `GetItem`, then `UpdateItem SET data = :v, version = :old+1` with `ConditionExpression version = :old` (or `attribute_not_exists(version)` if the read found no item). **One logical op**: its latency spans both calls. A lost race (`ConditionalCheckFailed`) is *not retried* and *not an error*: it is counted in `condition_failed` and the op is recorded as completed. |
//!
//! # Errors vs. expected outcomes
//!
//! `ConditionalCheckFailedException` is an expected outcome (above). A read of a
//! missing item/empty page is counted as `empty_reads`: under
//! `ConsistentRead: false` a replica that has not applied a just-loaded record
//! legitimately misses it. Everything else non-200 is classified by
//! [`classify_error`].
//!
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::client::{Conn, Response};
use crate::cluster::Cluster;
use crate::engine::{ErrorKind, OpExecutor, Outcome};
use crate::rt::{self, Clock};
use crate::workload::{ATTR_DATA, ATTR_PK, ATTR_SK, ATTR_VERSION, Op, key_for, value_for};

/// Map a non-200 response to an [`Outcome`].
#[must_use]
pub fn classify_error(status: u16, body: &[u8]) -> Outcome {
    let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let ty = v["__type"].as_str().unwrap_or("");
    let msg = v["message"]
        .as_str()
        .or_else(|| v["Message"].as_str())
        .unwrap_or("");
    let short = ty.rsplit('#').next().unwrap_or(ty);
    if short == "ConditionalCheckFailedException" {
        return Outcome::ConditionFailed;
    }
    let kind = if short.contains("ProvisionedThroughputExceeded")
        || short.contains("ThrottlingException")
        || short.contains("RequestLimitExceeded")
    {
        ErrorKind::Throttled
    } else {
        ErrorKind::Other
    };
    Outcome::Err {
        kind,
        detail: format!("HTTP {status} {short}: {msg}"),
    }
}

fn io_outcome(e: &std::io::Error) -> Outcome {
    Outcome::Err {
        kind: ErrorKind::Connection,
        detail: format!("io: {e}"),
    }
}

/// The YCSB executor for one table and one `ConsistentRead` setting.
#[derive(Clone, Debug)]
pub struct YcsbExecutor {
    pub table: String,
    pub value_bytes: usize,
    pub consistent_read: bool,
}

fn key_json(idx: u64) -> Value {
    let (pk, sk) = key_for(idx);
    json!({ ATTR_PK: {"S": pk}, ATTR_SK: {"N": sk.to_string()} })
}

impl YcsbExecutor {
    fn get_body(&self, idx: u64) -> String {
        json!({"TableName": self.table, "Key": key_json(idx), "ConsistentRead": self.consistent_read})
            .to_string()
    }

    fn update_body(&self, idx: u64, nonce: u64) -> String {
        json!({
            "TableName": self.table,
            "Key": key_json(idx),
            "UpdateExpression": "SET #d = :v",
            "ExpressionAttributeNames": {"#d": ATTR_DATA},
            "ExpressionAttributeValues": {":v": {"S": value_for(nonce, self.value_bytes)}},
        })
        .to_string()
    }

    fn insert_body(&self, idx: u64, nonce: u64) -> String {
        json!({"TableName": self.table, "Item": item_json(idx, 0, &value_for(nonce, self.value_bytes))})
            .to_string()
    }

    fn scan_body(&self, idx: u64, len: u32) -> String {
        let (pk, sk) = key_for(idx);
        json!({
            "TableName": self.table,
            "KeyConditionExpression": "#p = :p AND #s >= :s",
            "ExpressionAttributeNames": {"#p": ATTR_PK, "#s": ATTR_SK},
            "ExpressionAttributeValues": {":p": {"S": pk}, ":s": {"N": sk.to_string()}},
            "Limit": len,
            "ConsistentRead": self.consistent_read,
        })
        .to_string()
    }

    fn rmw_update_body(&self, idx: u64, nonce: u64, old_version: Option<u64>) -> String {
        let mut v = json!({
            "TableName": self.table,
            "Key": key_json(idx),
            "UpdateExpression": "SET #d = :v, #ver = :new",
            "ExpressionAttributeNames": {"#d": ATTR_DATA, "#ver": ATTR_VERSION},
            "ExpressionAttributeValues": {
                ":v": {"S": value_for(nonce, self.value_bytes)},
                ":new": {"N": old_version.map_or(1, |o| o + 1).to_string()},
            },
        });
        match old_version {
            Some(o) => {
                v["ConditionExpression"] = json!("#ver = :old");
                v["ExpressionAttributeValues"][":old"] = json!({"N": o.to_string()});
            }
            None => v["ConditionExpression"] = json!("attribute_not_exists(#ver)"),
        }
        v.to_string()
    }
}

/// An item as the load / insert path writes it.
#[must_use]
pub fn item_json(idx: u64, version: u64, data: &str) -> Value {
    let (pk, sk) = key_for(idx);
    json!({
        ATTR_PK: {"S": pk},
        ATTR_SK: {"N": sk.to_string()},
        ATTR_VERSION: {"N": version.to_string()},
        ATTR_DATA: {"S": data},
    })
}

async fn call(conn: &mut Conn, target: &str, body: &str) -> Result<Response, Outcome> {
    conn.call(target, body).await.map_err(|e| io_outcome(&e))
}

fn ok_or_classify(r: &Response) -> Result<(), Outcome> {
    if r.status == 200 {
        Ok(())
    } else {
        Err(classify_error(r.status, &r.body))
    }
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

fn is_empty_get(body: &[u8]) -> bool {
    !contains(body, b"\"Item\"")
}

fn is_empty_query(body: &[u8]) -> bool {
    contains(body, b"\"Count\":0")
}

impl OpExecutor for YcsbExecutor {
    type Op = Op;

    fn class(&self, op: &Op) -> &'static str {
        op.class()
    }

    async fn execute<'a>(&'a self, conn: &'a mut Conn, op: &'a Op) -> Outcome {
        match self.execute_inner(conn, op).await {
            Ok(o) | Err(o) => o,
        }
    }
}

impl YcsbExecutor {
    async fn execute_inner(&self, conn: &mut Conn, op: &Op) -> Result<Outcome, Outcome> {
        match *op {
            Op::Read { idx } => {
                let r = call(conn, "GetItem", &self.get_body(idx)).await?;
                ok_or_classify(&r)?;
                Ok(if is_empty_get(&r.body) {
                    Outcome::OkEmpty
                } else {
                    Outcome::Ok
                })
            }
            Op::Update { idx, nonce } => {
                let r = call(conn, "UpdateItem", &self.update_body(idx, nonce)).await?;
                ok_or_classify(&r)?;
                Ok(Outcome::Ok)
            }
            Op::Insert { idx, nonce } => {
                let r = call(conn, "PutItem", &self.insert_body(idx, nonce)).await?;
                ok_or_classify(&r)?;
                Ok(Outcome::Ok)
            }
            Op::Scan { idx, len } => {
                let r = call(conn, "Query", &self.scan_body(idx, len)).await?;
                ok_or_classify(&r)?;
                Ok(if is_empty_query(&r.body) {
                    Outcome::OkEmpty
                } else {
                    Outcome::Ok
                })
            }
            Op::ReadModifyWrite { idx, nonce } => {
                let r = call(conn, "GetItem", &self.get_body(idx)).await?;
                ok_or_classify(&r)?;
                let old = serde_json::from_slice::<Value>(&r.body)
                    .ok()
                    .and_then(|v| v["Item"][ATTR_VERSION]["N"].as_str()?.parse::<u64>().ok());
                let r = call(conn, "UpdateItem", &self.rmw_update_body(idx, nonce, old)).await?;
                match ok_or_classify(&r) {
                    Ok(()) => Ok(Outcome::Ok),
                    Err(Outcome::ConditionFailed) => Ok(Outcome::ConditionFailed),
                    Err(e) => Err(e),
                }
            }
        }
    }
}

/// `CreateTable` (`pk` S HASH + `sk` N RANGE) and wait for `ACTIVE`,
/// retrying transient refusals until `deadline`.
///
/// # Errors
/// With the last response if the table never became active in time.
pub async fn create_table(
    cluster: &Cluster,
    table: &str,
    deadline: Duration,
) -> Result<(), String> {
    let clock = Clock::start();
    let limit = u64::try_from(deadline.as_nanos()).unwrap_or(u64::MAX);
    let endpoints = cluster.dynamo_endpoints();
    let body = json!({
        "TableName": table,
        "AttributeDefinitions": [
            {"AttributeName": ATTR_PK, "AttributeType": "S"},
            {"AttributeName": ATTR_SK, "AttributeType": "N"},
        ],
        "KeySchema": [
            {"AttributeName": ATTR_PK, "KeyType": "HASH"},
            {"AttributeName": ATTR_SK, "KeyType": "RANGE"},
        ],
    })
    .to_string();
    let describe = json!({"TableName": table}).to_string();
    let mut created = false;
    let mut last = String::from("no attempt");
    let mut i = 0usize;
    while clock.now_ns() < limit {
        let addr = endpoints[i % endpoints.len()];
        i += 1;
        let Ok(mut conn) = Conn::connect(addr, cluster.credentials()).await else {
            last = format!("connect {addr} failed");
            rt::sleep(Duration::from_millis(100)).await;
            continue;
        };
        if !created {
            match conn.call("CreateTable", &body).await {
                Ok(r) if r.status == 200 => created = true,
                Ok(r) => last = format!("CreateTable: {}", r.text()),
                Err(e) => last = format!("CreateTable io: {e}"),
            }
        }
        if created {
            match conn.call("DescribeTable", &describe).await {
                Ok(r) if r.status == 200 => {
                    let v: Value = serde_json::from_slice(&r.body).unwrap_or(Value::Null);
                    if v["Table"]["TableStatus"].as_str() == Some("ACTIVE") {
                        return Ok(());
                    }
                    last = format!("DescribeTable: {}", r.text());
                }
                Ok(r) => last = format!("DescribeTable: {}", r.text()),
                Err(e) => last = format!("DescribeTable io: {e}"),
            }
        }
        rt::sleep(Duration::from_millis(50)).await;
    }
    Err(format!(
        "table `{table}` not ACTIVE within {deadline:?}: {last}"
    ))
}

/// Best-effort `DeleteTable` (cleanup; failures are ignored).
pub async fn drop_table(cluster: &Cluster, table: &str) {
    for addr in cluster.dynamo_endpoints() {
        if let Ok(mut c) = Conn::connect(addr, cluster.credentials()).await
            && let Ok(r) = c
                .call("DeleteTable", &json!({"TableName": table}).to_string())
                .await
            && r.status == 200
        {
            return;
        }
    }
}

/// What the (unmeasured) load phase did.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LoadResult {
    pub records: u64,
    pub value_bytes: usize,
    pub elapsed_secs: f64,
    pub records_per_sec: f64,
    /// `BatchWriteItem` calls that had to be re-sent.
    pub retried_batches: u64,
}

/// Bulk-load records `0..records` with `BatchWriteItem` (25 per call) over
/// `parallelism` connections. Not a latency measurement. Each batch is
/// re-sent (puts are idempotent) until it fully lands or `deadline` passes.
///
/// # Errors
/// If a batch could not be written before `deadline`.
pub async fn load_table(
    cluster: &Cluster,
    table: &str,
    records: u64,
    value_bytes: usize,
    parallelism: usize,
    deadline: Duration,
) -> Result<LoadResult, String> {
    const BATCH: u64 = 25;
    let clock = Clock::start();
    let limit = u64::try_from(deadline.as_nanos()).unwrap_or(u64::MAX);
    let next = Arc::new(AtomicU64::new(0));
    let retried = Arc::new(AtomicU64::new(0));
    let endpoints = cluster.dynamo_endpoints();
    let mut tasks = Vec::new();
    for w in 0..parallelism.max(1) {
        let (next, retried, table) = (next.clone(), retried.clone(), table.to_owned());
        let (creds, endpoints) = (cluster.credentials(), endpoints.clone());
        tasks.push(rt::spawn(async move {
            let mut conn: Option<Conn> = None;
            let mut cursor = w;
            loop {
                let start = next.fetch_add(BATCH, Ordering::Relaxed);
                if start >= records {
                    return Ok(());
                }
                let end = (start + BATCH).min(records);
                let reqs: Vec<Value> = (start..end)
                    .map(|i| {
                        json!({"PutRequest": {"Item": item_json(i, 0, &value_for(i, value_bytes))}})
                    })
                    .collect();
                let body = json!({"RequestItems": {table.clone(): reqs}}).to_string();
                let mut first = true;
                loop {
                    if conn.as_ref().is_none_or(Conn::is_broken) {
                        conn = Conn::connect(endpoints[cursor % endpoints.len()], creds.clone())
                            .await
                            .ok();
                        cursor += 1;
                    }
                    let mut last = String::from("no connection");
                    if let Some(c) = conn.as_mut() {
                        match c.call("BatchWriteItem", &body).await {
                            Ok(r) if r.status == 200 && unprocessed_empty(&r.body) => break,
                            Ok(r) => last = r.text(),
                            Err(e) => last = e.to_string(),
                        }
                    }
                    if clock.now_ns() > limit {
                        return Err(format!("load batch {start}..{end} failed: {last}"));
                    }
                    if first {
                        retried.fetch_add(1, Ordering::Relaxed);
                        first = false;
                    }
                    rt::sleep(Duration::from_millis(100)).await;
                }
            }
        }));
    }
    for t in tasks {
        t.await.map_err(|e| e.to_string())??;
    }
    let elapsed = clock.now_ns() as f64 / 1e9;
    Ok(LoadResult {
        records,
        value_bytes,
        elapsed_secs: elapsed,
        records_per_sec: if elapsed > 0.0 {
            records as f64 / elapsed
        } else {
            0.0
        },
        retried_batches: retried.load(Ordering::Relaxed),
    })
}

fn unprocessed_empty(body: &[u8]) -> bool {
    let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    v["UnprocessedItems"]
        .as_object()
        .is_none_or(|m| m.values().all(|x| x.as_array().is_none_or(Vec::is_empty)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exec(cr: bool) -> YcsbExecutor {
        YcsbExecutor {
            table: "t".into(),
            value_bytes: 8,
            consistent_read: cr,
        }
    }

    #[test]
    fn get_body_carries_key_and_consistency() {
        let v: Value = serde_json::from_str(&exec(true).get_body(12_345)).unwrap();
        assert_eq!(v["Key"]["pk"]["S"], "user0000000123");
        assert_eq!(v["Key"]["sk"]["N"], "45");
        assert_eq!(v["ConsistentRead"], true);
        let v: Value = serde_json::from_str(&exec(false).get_body(0)).unwrap();
        assert_eq!(v["ConsistentRead"], false);
    }

    #[test]
    fn rmw_update_is_conditional_on_the_version_read() {
        let v: Value = serde_json::from_str(&exec(true).rmw_update_body(5, 1, Some(7))).unwrap();
        assert_eq!(v["ConditionExpression"], "#ver = :old");
        assert_eq!(v["ExpressionAttributeValues"][":old"]["N"], "7");
        assert_eq!(v["ExpressionAttributeValues"][":new"]["N"], "8");
        let v: Value = serde_json::from_str(&exec(true).rmw_update_body(5, 1, None)).unwrap();
        assert_eq!(v["ConditionExpression"], "attribute_not_exists(#ver)");
        assert_eq!(v["ExpressionAttributeValues"][":new"]["N"], "1");
    }

    #[test]
    fn scan_is_a_bounded_sort_key_range_query() {
        let v: Value = serde_json::from_str(&exec(false).scan_body(250, 17)).unwrap();
        assert_eq!(v["Limit"], 17);
        assert_eq!(v["ExpressionAttributeValues"][":p"]["S"], "user0000000002");
        assert_eq!(v["ExpressionAttributeValues"][":s"]["N"], "50");
        assert_eq!(v["ConsistentRead"], false);
    }

    #[test]
    fn errors_are_classified_by_wire_type() {
        let body = |t: &str| {
            format!(r#"{{"__type":"com.amazonaws.dynamodb.v20120810#{t}","message":"m"}}"#)
        };
        assert!(matches!(
            classify_error(400, body("ConditionalCheckFailedException").as_bytes()),
            Outcome::ConditionFailed
        ));
        assert!(matches!(
            classify_error(
                400,
                body("ProvisionedThroughputExceededException").as_bytes()
            ),
            Outcome::Err {
                kind: ErrorKind::Throttled,
                ..
            }
        ));
        assert!(matches!(
            classify_error(500, body("InternalServerError").as_bytes()),
            Outcome::Err {
                kind: ErrorKind::Other,
                ..
            }
        ));
        assert!(matches!(
            classify_error(500, b"not json"),
            Outcome::Err {
                kind: ErrorKind::Other,
                ..
            }
        ));
    }

    #[test]
    fn empty_detection() {
        assert!(is_empty_get(b"{}"));
        assert!(!is_empty_get(br#"{"Item":{"pk":{"S":"a"}}}"#));
        assert!(is_empty_query(br#"{"Count":0,"Items":[]}"#));
        assert!(!is_empty_query(br#"{"Count":3,"Items":[]}"#));
    }
}
