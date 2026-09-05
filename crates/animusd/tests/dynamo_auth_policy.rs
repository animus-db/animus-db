//! End-to-end tests for ADR 0066 (SigV4 hardening, S-02 step 3): a
//! replicated credential catalog row authenticates a request; a rotated
//! secret stays valid through its grace window and stops after; a revoked
//! credential is rejected; a scoped policy's per-key allow list is enforced
//! at dispatch (`AccessDeniedException`); `BatchWriteItem` spanning an
//! allowed and a denied table is rejected whole; the static bootstrap
//! credential (ADR 0057/0066 §4) remains unrestricted; and an unknown key's
//! error body is identical whether the key never existed or was revoked.
//!
//! Real time/sockets (the `ProdEnv` edge), mirroring `dynamo_sigv4.rs`'s own
//! testing note — `X-Amz-Date` is built from `SystemTime::now()`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use animus_dynamo::sigv4::{self, SigV4Request};
use animusd::config::NodeRole;
use animusd::{ClusterConfig, DynamoAuthConfig, Node, RoleAddrs, StorageBackend};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

mod support;

/// The static bootstrap credential every test in this file that needs an
/// unrestricted caller (to `CreateTable`, seed data, etc.) uses — ADR 0066
/// §4's bootstrap role. Distinct from any catalog row this file `PUT`s.
const BOOT_ACCESS_KEY: &str = "AKIDBOOTSTRAP";
const BOOT_SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const TEST_REGION: &str = "us-east-1";
const TEST_SERVICE: &str = "dynamodb";

/// Bring up a single combined node with the bootstrap credential configured
/// (ADR 0057) — mirrors `dynamo_sigv4.rs::start_single_node_with_auth`, kept
/// as this file's own copy per this codebase's "sibling test modules keep
/// their own fixtures independent" convention.
async fn bring_up(dir: &Path) -> (Node, ClusterConfig) {
    let mut credentials = BTreeMap::new();
    credentials.insert(BOOT_ACCESS_KEY.to_string(), BOOT_SECRET.to_string());
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
    panic!("single node (auth policy) failed to start after 10 attempts: {last_err:?}");
}

fn now_amz_date() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs() as i64;
    epoch_secs_to_amz_date(secs)
}

fn epoch_secs_to_amz_date(epoch_secs: i64) -> String {
    let days = epoch_secs.div_euclid(86400);
    let secs_of_day = epoch_secs.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    let hh = secs_of_day / 3600;
    let mm = (secs_of_day % 3600) / 60;
    let ss = secs_of_day % 60;
    format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z")
}

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

const SIGNED_HEADERS: [&str; 4] = ["content-type", "host", "x-amz-date", "x-amz-target"];

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

/// A signed call as `access_key`/`secret`, signed at "now" — the common
/// shape every test below actually wants (a fresh timestamp per call).
async fn call(
    addr: SocketAddr,
    target: &str,
    body: &str,
    access_key: &str,
    secret: &str,
) -> (u16, String) {
    signed_dynamo(addr, target, body, access_key, secret, &now_amz_date()).await
}

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

fn json(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed
/// JSON)` — mirrors `admin_endpoint.rs`'s own helper of the same shape.
async fn admin(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.0\r\n\
         Host: animus\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let value: Value = serde_json::from_str(payload)
        .unwrap_or_else(|e| panic!("admin body is not JSON ({e}): {payload}"));
    (status, value)
}

fn create_table_body(table: &str) -> String {
    serde_json::json!({
        "TableName": table,
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
    })
    .to_string()
}

fn put_item_body(table: &str, id: &str) -> String {
    serde_json::json!({
        "TableName": table,
        "Item": {"id": {"S": id}, "color": {"S": "red"}},
    })
    .to_string()
}

fn get_item_body(table: &str, id: &str) -> String {
    serde_json::json!({
        "TableName": table,
        "Key": {"id": {"S": id}},
        "ConsistentRead": true,
    })
    .to_string()
}

/// (a) A catalog credential `PUT` via the admin route authenticates a
/// request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_credential_authenticates_a_request() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) = bring_up(dir.path()).await;
        let dynamo_addr = node.dynamo_addr();
        let admin_addr = node.admin_addr();

        let put_body =
            serde_json::json!({"id": "AKIDCAT", "secret": "s0", "enabled": true}).to_string();
        let (status, resp) = admin(admin_addr, "POST", "/admin/credentials", Some(&put_body)).await;
        assert_eq!(status, 200, "PutCredential: {resp}");

        let (status, body) = call(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            &create_table_body("t_cat"),
            "AKIDCAT",
            "s0",
        )
        .await;
        assert_eq!(status, 200, "catalog-authenticated CreateTable: {body}");

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// (b) Rotation: the outgoing secret keeps working inside its grace window
/// and stops once it closes; the new secret works immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rotation_grace_window_opens_and_closes() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) = bring_up(dir.path()).await;
        let dynamo_addr = node.dynamo_addr();
        let admin_addr = node.admin_addr();

        let put_body =
            serde_json::json!({"id": "AKIDROT", "secret": "s0", "enabled": true}).to_string();
        let (status, resp) = admin(admin_addr, "POST", "/admin/credentials", Some(&put_body)).await;
        assert_eq!(status, 200, "PutCredential: {resp}");

        // A short grace window, driven by real wall-clock time (a
        // `ProdEnv` test — see this file's own doc).
        let rotate_body =
            serde_json::json!({"id": "AKIDROT", "new_secret": "s1", "grace_secs": 2}).to_string();
        let (status, resp) = admin(
            admin_addr,
            "POST",
            "/admin/credentials/rotate",
            Some(&rotate_body),
        )
        .await;
        assert_eq!(status, 200, "RotateCredential: {resp}");

        // Both secrets work immediately after rotating.
        let (status, body) = call(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            &create_table_body("t_rot_old"),
            "AKIDROT",
            "s0",
        )
        .await;
        assert_eq!(status, 200, "old secret inside grace: {body}");
        let (status, body) = call(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            &create_table_body("t_rot_new"),
            "AKIDROT",
            "s1",
        )
        .await;
        assert_eq!(status, 200, "new secret: {body}");

        // Converged-or-timeout: poll until the old secret starts being
        // rejected (never a fixed sleep).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (status, body) = call(
                dynamo_addr,
                "DynamoDB_20120810.DescribeTable",
                &serde_json::json!({"TableName": "t_rot_new"}).to_string(),
                "AKIDROT",
                "s0",
            )
            .await;
            if status == 400 {
                let v = json(&body);
                assert_eq!(
                    v["__type"], "com.amazon.coral.service#InvalidSignatureException",
                    "old secret past grace should fail signature verification: {body}"
                );
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("old secret never stopped working past its grace window");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// (c) A revoked credential is rejected on the next request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revoked_credential_is_rejected() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) = bring_up(dir.path()).await;
        let dynamo_addr = node.dynamo_addr();
        let admin_addr = node.admin_addr();

        let put_body =
            serde_json::json!({"id": "AKIDREV", "secret": "s0", "enabled": true}).to_string();
        admin(admin_addr, "POST", "/admin/credentials", Some(&put_body)).await;

        let (status, body) = call(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            &create_table_body("t_rev"),
            "AKIDREV",
            "s0",
        )
        .await;
        assert_eq!(status, 200, "before revoke: {body}");

        let revoke_body = serde_json::json!({"id": "AKIDREV"}).to_string();
        let (status, resp) = admin(
            admin_addr,
            "POST",
            "/admin/credentials/revoke",
            Some(&revoke_body),
        )
        .await;
        assert_eq!(status, 200, "RevokeCredential: {resp}");

        let (status, body) = call(
            dynamo_addr,
            "DynamoDB_20120810.DescribeTable",
            &serde_json::json!({"TableName": "t_rev"}).to_string(),
            "AKIDREV",
            "s0",
        )
        .await;
        assert_eq!(status, 400, "revoked credential: {body}");
        let v = json(&body);
        assert_eq!(
            v["__type"], "com.amazon.coral.service#UnrecognizedClientException",
            "body: {body}"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// (d) A per-key allow list: a key scoped to `[TableA]`/`Read` can `GetItem`
/// on A, and gets `AccessDeniedException` on `PutItem` on A and on
/// `GetItem` on B.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_policy_enforces_table_and_op_class() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) = bring_up(dir.path()).await;
        let dynamo_addr = node.dynamo_addr();
        let admin_addr = node.admin_addr();

        // Seed both tables with the unrestricted bootstrap credential.
        for table in ["tbl_a", "tbl_b"] {
            let (status, body) = call(
                dynamo_addr,
                "DynamoDB_20120810.CreateTable",
                &create_table_body(table),
                BOOT_ACCESS_KEY,
                BOOT_SECRET,
            )
            .await;
            assert_eq!(status, 200, "seed CreateTable {table}: {body}");
        }
        let (status, body) = call(
            dynamo_addr,
            "DynamoDB_20120810.PutItem",
            &put_item_body("tbl_a", "1"),
            BOOT_ACCESS_KEY,
            BOOT_SECRET,
        )
        .await;
        assert_eq!(status, 200, "seed PutItem tbl_a: {body}");

        // A read-only key scoped to `tbl_a` alone.
        let put_body = serde_json::json!({
            "id": "AKIDSCOPED",
            "secret": "s0",
            "policy": {
                "tables": {"kind": "names", "names": ["tbl_a"]},
                "ops": ["read"],
            },
            "enabled": true,
        })
        .to_string();
        let (status, resp) = admin(admin_addr, "POST", "/admin/credentials", Some(&put_body)).await;
        assert_eq!(status, 200, "PutCredential: {resp}");

        // Allowed: GetItem on tbl_a.
        let (status, body) = call(
            dynamo_addr,
            "DynamoDB_20120810.GetItem",
            &get_item_body("tbl_a", "1"),
            "AKIDSCOPED",
            "s0",
        )
        .await;
        assert_eq!(status, 200, "GetItem on tbl_a: {body}");

        // Denied: PutItem on tbl_a (wrong class).
        let (status, body) = call(
            dynamo_addr,
            "DynamoDB_20120810.PutItem",
            &put_item_body("tbl_a", "2"),
            "AKIDSCOPED",
            "s0",
        )
        .await;
        assert_eq!(status, 400, "PutItem on tbl_a should be denied: {body}");
        let v = json(&body);
        assert_eq!(
            v["__type"],
            "com.amazonaws.dynamodb.v20120810#AccessDeniedException"
        );
        assert!(
            v["message"].as_str().unwrap().contains("PutItem"),
            "message should name the operation: {body}"
        );

        // Denied: GetItem on tbl_b (wrong table).
        let (status, body) = call(
            dynamo_addr,
            "DynamoDB_20120810.GetItem",
            &get_item_body("tbl_b", "1"),
            "AKIDSCOPED",
            "s0",
        )
        .await;
        assert_eq!(status, 400, "GetItem on tbl_b should be denied: {body}");
        let v = json(&body);
        assert_eq!(
            v["__type"],
            "com.amazonaws.dynamodb.v20120810#AccessDeniedException"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// (e) `BatchWriteItem` spanning an allowed and a denied table is rejected
/// whole, writing nothing to either table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_write_spanning_denied_table_writes_nothing() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) = bring_up(dir.path()).await;
        let dynamo_addr = node.dynamo_addr();
        let admin_addr = node.admin_addr();

        for table in ["batch_a", "batch_b"] {
            let (status, body) = call(
                dynamo_addr,
                "DynamoDB_20120810.CreateTable",
                &create_table_body(table),
                BOOT_ACCESS_KEY,
                BOOT_SECRET,
            )
            .await;
            assert_eq!(status, 200, "seed CreateTable {table}: {body}");
        }

        // Scoped to `batch_a` only, but with write access.
        let put_body = serde_json::json!({
            "id": "AKIDBATCH",
            "secret": "s0",
            "policy": {
                "tables": {"kind": "names", "names": ["batch_a"]},
                "ops": ["read", "write"],
            },
            "enabled": true,
        })
        .to_string();
        let (status, resp) = admin(admin_addr, "POST", "/admin/credentials", Some(&put_body)).await;
        assert_eq!(status, 200, "PutCredential: {resp}");

        let batch_body = serde_json::json!({
            "RequestItems": {
                "batch_a": [{"PutRequest": {"Item": {"id": {"S": "x"}}}}],
                "batch_b": [{"PutRequest": {"Item": {"id": {"S": "y"}}}}],
            }
        })
        .to_string();
        let (status, body) = call(
            dynamo_addr,
            "DynamoDB_20120810.BatchWriteItem",
            &batch_body,
            "AKIDBATCH",
            "s0",
        )
        .await;
        assert_eq!(status, 400, "batch spanning a denied table: {body}");
        let v = json(&body);
        assert_eq!(
            v["__type"],
            "com.amazonaws.dynamodb.v20120810#AccessDeniedException"
        );

        // Nothing was written to the allowed table either — whole-request
        // rejection, not partial application.
        let (status, body) = call(
            dynamo_addr,
            "DynamoDB_20120810.GetItem",
            &get_item_body("batch_a", "x"),
            BOOT_ACCESS_KEY,
            BOOT_SECRET,
        )
        .await;
        assert_eq!(status, 200, "GetItem batch_a/x: {body}");
        let v = json(&body);
        assert_eq!(
            v.get("Item"),
            None,
            "the allowed table's own item must not have been written: {body}"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// (f) The static bootstrap credential remains unrestricted, even with an
/// empty replicated catalog.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_credential_stays_unrestricted() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) = bring_up(dir.path()).await;
        let dynamo_addr = node.dynamo_addr();

        let (status, body) = call(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            &create_table_body("t_boot"),
            BOOT_ACCESS_KEY,
            BOOT_SECRET,
        )
        .await;
        assert_eq!(status, 200, "bootstrap CreateTable: {body}");
        let (status, body) = call(
            dynamo_addr,
            "DynamoDB_20120810.PutItem",
            &put_item_body("t_boot", "1"),
            BOOT_ACCESS_KEY,
            BOOT_SECRET,
        )
        .await;
        assert_eq!(status, 200, "bootstrap PutItem: {body}");

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// (g) The unknown-key error body is identical whether the key never
/// existed or was revoked — AWS never confirms whether a key id ever
/// existed (ADR 0066 §3 step 2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_and_revoked_keys_produce_the_identical_error_body() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) = bring_up(dir.path()).await;
        let dynamo_addr = node.dynamo_addr();
        let admin_addr = node.admin_addr();

        let put_body =
            serde_json::json!({"id": "AKIDGONE", "secret": "s0", "enabled": true}).to_string();
        admin(admin_addr, "POST", "/admin/credentials", Some(&put_body)).await;
        let revoke_body = serde_json::json!({"id": "AKIDGONE"}).to_string();
        let (status, resp) = admin(
            admin_addr,
            "POST",
            "/admin/credentials/revoke",
            Some(&revoke_body),
        )
        .await;
        assert_eq!(status, 200, "RevokeCredential: {resp}");

        let describe = serde_json::json!({"TableName": "does-not-matter"}).to_string();
        let (never_status, never_body) = call(
            dynamo_addr,
            "DynamoDB_20120810.DescribeTable",
            &describe,
            "AKIDNEVEREXISTED",
            "whatever",
        )
        .await;
        let (revoked_status, revoked_body) = call(
            dynamo_addr,
            "DynamoDB_20120810.DescribeTable",
            &describe,
            "AKIDGONE",
            "s0",
        )
        .await;

        assert_eq!(never_status, revoked_status);
        let never = json(&never_body);
        let revoked = json(&revoked_body);
        assert_eq!(
            never["__type"],
            "com.amazon.coral.service#UnrecognizedClientException"
        );
        assert_eq!(never["__type"], revoked["__type"]);
        assert_eq!(
            never["message"], revoked["message"],
            "the message must not reveal whether the id ever existed"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}
