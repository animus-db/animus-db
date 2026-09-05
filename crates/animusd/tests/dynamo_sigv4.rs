//! End-to-end tests for SigV4 enforcement on the client DynamoDB port (ADR
//! 0057): a correctly-signed request round-trips a real operation; unsigned,
//! mis-signed, unknown-key, and clock-skewed requests are rejected with the
//! exact AWS-faithful `com.amazon.coral.service#...` error codes; `GET
//! /metrics` stays unauthenticated even with auth enabled; and a cluster
//! with no `dynamo_auth` section accepts unsigned requests exactly as
//! before (default-off sanity). Real time/sockets (the `ProdEnv` edge) —
//! `X-Amz-Date` is built from `SystemTime::now()` since these tests are
//! outside the `Env` seam by design (ADR 0057's own testing note).
//!
//! Signing uses `animus_dynamo::sigv4::sign` — the same "hand-rolled test
//! signer, same algorithm, exercised against the same vendored vectors"
//! the ADR calls for (no real `aws-sdk-dynamodb` client: its crypto
//! backends carry license terms `deny.toml`'s allow-list rejects).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use animus_dynamo::sigv4::{self, SigV4Request};
use animusd::config::NodeRole;
use animusd::{ClusterConfig, DynamoAuthConfig, Node, RoleAddrs, StorageBackend};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod support;

/// A fixed test credential pair — the same access-key-id/secret pair
/// `animus-dynamo`'s own `sigv4` unit tests use (`sigv4.rs`'s `AKID`/
/// `SECRET` constants), for no reason beyond familiarity across the two
/// suites; nothing here depends on the specific values.
const TEST_ACCESS_KEY: &str = "AKIDEXAMPLE";
const TEST_SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const TEST_REGION: &str = "us-east-1";
const TEST_SERVICE: &str = "dynamodb";

/// Bring up a single node with a `dynamo_auth` section (ADR 0057),
/// retrying the port-TOCTOU race exactly like `support::start_single_node`
/// does (that helper always builds a config with `dynamo_auth: None`, so
/// this file needs its own copy that populates it) — the minimal
/// support-helper variant this suite needs, kept local per this codebase's
/// own "sibling test modules keep their own fixtures independent"
/// convention (see `dynamo_ttl.rs::start_single_node_fast_ttl` for the
/// identical precedent with a different knob).
async fn start_single_node_with_auth(
    dir: &Path,
    credentials: BTreeMap<String, String>,
) -> (Node, ClusterConfig) {
    let mut last_err = None;
    for attempt in 0..10 {
        let addrs = support::free_addrs(6);
        let config = ClusterConfig {
            nodes: vec![RoleAddrs {
                id: animusd::config::node_id(0),
                role: NodeRole::Both,
                internal: addrs[0],
                client: addrs[1],
                dynamo: addrs[2],
                admin: addrs[3],
                intra: addrs[4],
                console: addrs[5],
                advertise_host: None,
                tls: None,
            }],
            dynamo_auth: Some(DynamoAuthConfig {
                credentials: credentials.clone(),
            }),
            cluster_settings: None,
        };
        match animusd::run_node_with(&config, 0, dir, StorageBackend::default()).await {
            Ok(node) => return (node, config),
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(50 * (attempt + 1))).await;
            }
        }
    }
    panic!("single node (dynamo_auth) failed to start after 10 attempts: {last_err:?}");
}

/// The credential store every test in this file that wants auth enabled
/// uses: `TEST_ACCESS_KEY` → `TEST_SECRET`.
fn test_credentials() -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert(TEST_ACCESS_KEY.to_string(), TEST_SECRET.to_string());
    m
}

/// The current wall-clock `X-Amz-Date` value (`YYYYMMDD'T'HHMMSS'Z'`) —
/// real `SystemTime`, since these are real `ProdEnv` tests (mirrors
/// `dynamo_ttl.rs::now_secs`'s identical "why real time here" note).
fn now_amz_date() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs() as i64;
    epoch_secs_to_amz_date(secs)
}

/// `X-Amz-Date` `delta` seconds away from now (negative = past, positive =
/// future) — used to build the clock-skew test's stale timestamp.
fn amz_date_offset(delta_secs: i64) -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs() as i64
        + delta_secs;
    epoch_secs_to_amz_date(secs)
}

/// Format Unix epoch seconds as `YYYYMMDD'T'HHMMSS'Z'` — a hand-rolled
/// dependency-free formatter (this crate pulls in no calendar library),
/// duplicating the small "Howard Hinnant" civil-date algorithm
/// `animus_dynamo::sigv4`'s own (private) `format_amz_date`/`civil_from_days`
/// use internally. The ADR's own "hand-rolled test signer" note covers this
/// too: the test side re-derives the same small pure-math helpers rather
/// than depending on the production module's private internals.
fn epoch_secs_to_amz_date(epoch_secs: i64) -> String {
    let days = epoch_secs.div_euclid(86400);
    let secs_of_day = epoch_secs.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    let hh = secs_of_day / 3600;
    let mm = (secs_of_day % 3600) / 60;
    let ss = secs_of_day % 60;
    format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z")
}

/// The inverse of days-since-epoch → (year, month, day); see
/// `animus_dynamo::sigv4::civil_from_days`'s own doc for provenance
/// (Howard Hinnant's public-domain algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// A signed-headers set every request in this file uses, pre-sorted
/// alphabetically (`sign`'s own contract — see its doc): `content-type` <
/// `host` < `x-amz-date` < `x-amz-target`.
const SIGNED_HEADERS: [&str; 4] = ["content-type", "host", "x-amz-date", "x-amz-target"];

/// Send one DynamoDB JSON request over a fresh HTTP/1.1 connection, signed
/// with `access_key`/`secret` at `amz_date`, returning `(status, body)`.
/// `host` is the literal `Host` header value both the signature and the
/// wire request use (must match, or the signature can never verify).
#[allow(clippy::too_many_arguments)]
async fn signed_dynamo(
    addr: SocketAddr,
    target: &str,
    body: &str,
    access_key: &str,
    secret: &str,
    amz_date: &str,
) -> (u16, String) {
    let host = addr.to_string();
    let content_type = "application/x-amz-json-1.0";
    let mut headers: BTreeMap<String, String> = BTreeMap::new();
    headers.insert("content-type".to_string(), content_type.to_string());
    headers.insert("host".to_string(), host.clone());
    headers.insert("x-amz-date".to_string(), amz_date.to_string());
    headers.insert("x-amz-target".to_string(), target.to_string());

    let sigv4_req = SigV4Request {
        method: "POST",
        path: "/",
        query: "",
        headers: &headers,
        body: body.as_bytes(),
    };
    let authorization = sigv4::sign(
        &sigv4_req,
        access_key,
        secret,
        amz_date,
        TEST_REGION,
        TEST_SERVICE,
        &SIGNED_HEADERS,
    );

    let request = format!(
        "POST / HTTP/1.1\r\n\
         Host: {host}\r\n\
         X-Amz-Target: {target}\r\n\
         X-Amz-Date: {amz_date}\r\n\
         Authorization: {authorization}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    send_raw(addr, &request).await
}

/// Send one **unsigned** DynamoDB JSON request (no `Authorization`/
/// `X-Amz-Date` headers at all) — mirrors every other `tests/dynamo_*.rs`
/// file's plain `dynamo()` helper.
async fn unsigned_dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let request = format!(
        "POST / HTTP/1.1\r\n\
         Host: animus\r\n\
         X-Amz-Target: {target}\r\n\
         Content-Type: application/x-amz-json-1.0\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    send_raw(addr, &request).await
}

/// Write a full raw HTTP/1.1 request and parse back `(status, body)` —
/// the shared tail of [`signed_dynamo`]/[`unsigned_dynamo`] and the
/// `GET /metrics` check below.
async fn send_raw(addr: SocketAddr, request: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to dynamo");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .expect("read full response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    (status, payload.to_string())
}

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn correctly_signed_request_round_trips_create_table_and_put_get() {
    let dir = support::panic_safe_tempdir();
    let (node, _config) = start_single_node_with_auth(dir.path(), test_credentials()).await;
    let addr = node.dynamo_addr();

    let amz_date = now_amz_date();
    let (status, body) = signed_dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"widgets","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        TEST_ACCESS_KEY,
        TEST_SECRET,
        &amz_date,
    )
    .await;
    assert_eq!(status, 200, "signed CreateTable failed: {body}");

    let amz_date = now_amz_date();
    let (status, body) = signed_dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"widgets","Item":{"id":{"S":"w1"},"color":{"S":"red"}}}"#,
        TEST_ACCESS_KEY,
        TEST_SECRET,
        &amz_date,
    )
    .await;
    assert_eq!(status, 200, "signed PutItem failed: {body}");

    let amz_date = now_amz_date();
    let (status, body) = signed_dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"widgets","Key":{"id":{"S":"w1"}},"ConsistentRead":true}"#,
        TEST_ACCESS_KEY,
        TEST_SECRET,
        &amz_date,
    )
    .await;
    assert_eq!(status, 200, "signed GetItem failed: {body}");
    let v = json(&body);
    assert_eq!(v["Item"]["color"]["S"], "red", "body: {body}");

    node.shutdown_graceful().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsigned_request_is_rejected_with_missing_authentication_token() {
    let dir = support::panic_safe_tempdir();
    let (node, _config) = start_single_node_with_auth(dir.path(), test_credentials()).await;
    let addr = node.dynamo_addr();

    let (status, body) = unsigned_dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"widgets","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    )
    .await;
    assert_eq!(status, 400, "body: {body}");
    let v = json(&body);
    assert_eq!(
        v["__type"], "com.amazon.coral.service#MissingAuthenticationTokenException",
        "body: {body}"
    );

    node.shutdown_graceful().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_secret_is_rejected_with_invalid_signature() {
    let dir = support::panic_safe_tempdir();
    let (node, _config) = start_single_node_with_auth(dir.path(), test_credentials()).await;
    let addr = node.dynamo_addr();

    let amz_date = now_amz_date();
    let (status, body) = signed_dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"widgets","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        TEST_ACCESS_KEY,
        "not-the-configured-secret",
        &amz_date,
    )
    .await;
    assert_eq!(status, 400, "body: {body}");
    let v = json(&body);
    assert_eq!(
        v["__type"], "com.amazon.coral.service#InvalidSignatureException",
        "body: {body}"
    );

    node.shutdown_graceful().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_access_key_is_rejected_with_unrecognized_client() {
    let dir = support::panic_safe_tempdir();
    let (node, _config) = start_single_node_with_auth(dir.path(), test_credentials()).await;
    let addr = node.dynamo_addr();

    let amz_date = now_amz_date();
    let (status, body) = signed_dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"widgets","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        "AKIDNOTCONFIGURED",
        TEST_SECRET,
        &amz_date,
    )
    .await;
    assert_eq!(status, 400, "body: {body}");
    let v = json(&body);
    assert_eq!(
        v["__type"], "com.amazon.coral.service#UnrecognizedClientException",
        "body: {body}"
    );

    node.shutdown_graceful().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_x_amz_date_is_rejected_with_expired_signature() {
    let dir = support::panic_safe_tempdir();
    let (node, _config) = start_single_node_with_auth(dir.path(), test_credentials()).await;
    let addr = node.dynamo_addr();

    // 10 minutes in the past — outside the +/-5 minute window (ADR 0057).
    let stale_date = amz_date_offset(-10 * 60);
    let (status, body) = signed_dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"widgets","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        TEST_ACCESS_KEY,
        TEST_SECRET,
        &stale_date,
    )
    .await;
    assert_eq!(status, 400, "body: {body}");
    let v = json(&body);
    assert_eq!(
        v["__type"], "com.amazon.coral.service#InvalidSignatureException",
        "body: {body}"
    );
    let message = v["message"].as_str().expect("message is a string");
    assert!(
        message.contains("Signature expired"),
        "expected a 'Signature expired' message, got: {message}"
    );

    node.shutdown_graceful().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_stays_unauthenticated_on_an_auth_enabled_port() {
    let dir = support::panic_safe_tempdir();
    let (node, _config) = start_single_node_with_auth(dir.path(), test_credentials()).await;
    let addr = node.dynamo_addr();

    let request = "GET /metrics HTTP/1.1\r\nHost: animus\r\nConnection: close\r\n\r\n";
    let (status, _body) = send_raw(addr, request).await;
    assert_eq!(status, 200, "GET /metrics must stay unauthenticated");

    node.shutdown_graceful().await;
}

/// Default-off sanity (ADR 0057): a cluster with no `dynamo_auth` section
/// accepts an unsigned request exactly as before this ADR — one operation
/// is enough to prove the gate is a genuine no-op when disabled, not just
/// "still compiles."
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_without_dynamo_auth_accepts_unsigned_requests_as_before() {
    let dir = support::panic_safe_tempdir();
    let (node, _config) = support::start_single_node(dir.path(), StorageBackend::default()).await;
    let addr = node.dynamo_addr();

    let (status, body) = unsigned_dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"widgets","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    )
    .await;
    assert_eq!(
        status, 200,
        "unsigned CreateTable on an auth-disabled cluster must succeed: {body}"
    );

    node.shutdown_graceful().await;
}
