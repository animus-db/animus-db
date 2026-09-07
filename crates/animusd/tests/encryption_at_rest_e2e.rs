//! ADR 0069 (S-03 PR 1) — the real-`ProdEnv` end-to-end proof that
//! `--encryption-key PATH`/`RoleAddrs::encryption_key_path` actually works
//! wired into a genuine running node: bring up a node with a key, write
//! through the real DynamoDB wire, restart with the SAME key and read the
//! data back, restart with a DIFFERENT key and get the loud refusal,
//! restart with NO key against the now-encrypted directory and get the
//! loud refusal, and grep the raw data directory for the plaintext item
//! value to confirm it is genuinely absent. Modeled on `shared_wal_e2e.rs`.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod support;

const PLAINTEXT_NEEDLE: &str = "super-secret-value-42";

async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to dynamo");
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

async fn create_table(addr: SocketAddr, name: &str) {
    let body = format!(
        r#"{{"TableName":"{name}","AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}],"KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],"BillingMode":"PAY_PER_REQUEST"}}"#
    );
    let (status, resp) = dynamo(addr, "DynamoDB_20120810.CreateTable", &body).await;
    assert_eq!(status, 200, "CreateTable {name} failed: {resp}");
}

async fn put_item(addr: SocketAddr, table: &str, pk: &str, val: &str) {
    let body = format!(
        r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"{pk}"}},"val":{{"S":"{val}"}}}}}}"#
    );
    let (status, resp) = dynamo(addr, "DynamoDB_20120810.PutItem", &body).await;
    assert_eq!(status, 200, "PutItem {table}/{pk} failed: {resp}");
}

async fn get_item_consistent(addr: SocketAddr, table: &str, pk: &str) -> Option<String> {
    let body =
        format!(r#"{{"TableName":"{table}","Key":{{"pk":{{"S":"{pk}"}}}},"ConsistentRead":true}}"#);
    let (status, resp) = dynamo(addr, "DynamoDB_20120810.GetItem", &body).await;
    assert_eq!(status, 200, "GetItem {table}/{pk} failed: {resp}");
    let v: serde_json::Value = serde_json::from_str(&resp).expect("valid JSON response");
    v.get("Item")
        .and_then(|item| item.get("val"))
        .and_then(|val| val.get("S"))
        .and_then(|s| s.as_str())
        .map(str::to_string)
}

/// See `shared_wal_e2e.rs::poll_get_item_consistent`'s own doc for why this
/// polls rather than a single-shot assert.
async fn poll_get_item_consistent(
    addr: SocketAddr,
    table: &str,
    pk: &str,
    expected: &str,
    budget: Duration,
) {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let got = get_item_consistent(addr, table, pk).await;
        if got.as_deref() == Some(expected) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{table}/{pk} never converged to {expected:?} within {budget:?} (last read: {got:?})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn write_key_file(dir: &Path, name: &str, byte: u8) -> String {
    let path = dir.join(name);
    std::fs::write(&path, hex(byte)).expect("write key file");
    path.to_string_lossy().into_owned()
}

fn hex(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

/// Start (or restart, on a later call with the same `config`/`dir`) a
/// single combined-mode node — `shared_wal` off, matching most of this
/// crate's other e2e fixtures, since it's orthogonal to what this file
/// tests.
async fn start_node_with(
    config: &animusd::ClusterConfig,
    dir: &Path,
) -> Result<animusd::Node, String> {
    animusd::run_node_with_cluster_settings(
        config,
        0,
        dir,
        animusd::StorageBackend::default(),
        animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
        animusd::StreamSealKnobs::default(),
        animusd::SegmentStoreConfig::default(),
        animusd::DEFAULT_STREAM_RETENTION,
        Duration::ZERO,
        false,
        None,
        None,
        None,
        animusd::BackupStoreConfig::default(),
        None,
        None,
        None,
        None,
        None,
        false,
    )
    .await
    .map_err(|e| e.to_string())
}

/// The initial bring-up retries the WHOLE fresh-port-allocation-plus-start
/// as a unit, on a wall-clock deadline — the same port-TOCTOU-resilient
/// shape `shared_wal_e2e.rs::bring_up_with` uses.
async fn bring_up_with(
    base_dir: &Path,
    encryption_key_path: Option<String>,
) -> (animusd::Node, animusd::ClusterConfig, std::path::PathBuf) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut attempt: u64 = 0;
    loop {
        let addrs = support::free_addrs(6);
        let node_cfg = animusd::RoleAddrs {
            id: animusd::config::node_id(0),
            role: animusd::config::NodeRole::Both,
            internal: addrs[0],
            client: addrs[1],
            dynamo: addrs[2],
            admin: addrs[3],
            intra: addrs[4],
            console: addrs[5],
            advertise_host: None,
            tls: None,
            encryption_key_path: encryption_key_path.clone(),
        };
        let config = animusd::ClusterConfig {
            nodes: vec![node_cfg],
            dynamo_auth: None,
            cluster_settings: None,
        };
        let dir = base_dir.join(format!("attempt-{attempt}"));
        match start_node_with(&config, &dir).await {
            Ok(node) => return (node, config, dir),
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "could not bring up the node within the deadline: {e}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
                attempt += 1;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn encrypted_node_survives_a_real_restart_with_the_same_key_and_never_writes_plaintext() {
    let tmp = support::panic_safe_tempdir();
    let key_path = write_key_file(tmp.path(), "key.hex", 0x11);
    let (node, config, dir) = bring_up_with(tmp.path(), Some(key_path.clone())).await;
    let dynamo_addr = node.dynamo_addr();

    create_table(dynamo_addr, "orders").await;
    put_item(dynamo_addr, "orders", "o1", PLAINTEXT_NEEDLE).await;
    assert_eq!(
        get_item_consistent(dynamo_addr, "orders", "o1").await,
        Some(PLAINTEXT_NEEDLE.to_string())
    );

    node.shutdown_graceful().await;

    // The raw data directory must never contain the plaintext value —
    // scanned before the restart below re-touches any file.
    assert_plaintext_absent(&dir);

    // A real process restart with the SAME key: recovery must succeed and
    // every write must still be readable.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let node2 = loop {
        match start_node_with(&config, &dir).await {
            Ok(n) => break n,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "restart on the same dir/addresses did not rebind: {e}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    };
    let dynamo_addr2 = node2.dynamo_addr();
    let converge_budget = Duration::from_secs(10);
    poll_get_item_consistent(
        dynamo_addr2,
        "orders",
        "o1",
        PLAINTEXT_NEEDLE,
        converge_budget,
    )
    .await;
    put_item(dynamo_addr2, "orders", "o2", "second-value").await;
    poll_get_item_consistent(
        dynamo_addr2,
        "orders",
        "o2",
        "second-value",
        converge_budget,
    )
    .await;

    node2.shutdown_graceful().await;
    assert_plaintext_absent(&dir);
}

/// A restart with a DIFFERENT key against an already-encrypted data
/// directory must refuse loudly (ADR 0069's exact refusal text), never
/// silently reset or partially start.
#[tokio::test(flavor = "multi_thread")]
async fn restart_with_a_different_key_is_a_loud_refusal() {
    let tmp = support::panic_safe_tempdir();
    let key_path = write_key_file(tmp.path(), "key.hex", 0x22);
    let other_key_path = write_key_file(tmp.path(), "other-key.hex", 0x33);
    let (node, config, dir) = bring_up_with(tmp.path(), Some(key_path)).await;
    create_table(node.dynamo_addr(), "orders").await;
    put_item(node.dynamo_addr(), "orders", "o1", "x").await;
    node.shutdown_graceful().await;

    let mut wrong_key_config = config.clone();
    wrong_key_config.nodes[0].encryption_key_path = Some(other_key_path);
    let err = match start_node_with(&wrong_key_config, &dir).await {
        Err(e) => e,
        Ok(_) => panic!("a restart with the wrong key must be refused"),
    };
    assert!(
        err.contains("does not match the key this data directory was encrypted with"),
        "error text: {err}"
    );
    assert!(err.contains("--encryption-key"), "error text: {err}");
}

/// A restart with NO key at all against an already-encrypted data
/// directory must refuse loudly too.
#[tokio::test(flavor = "multi_thread")]
async fn restart_with_no_key_against_an_encrypted_directory_is_a_loud_refusal() {
    let tmp = support::panic_safe_tempdir();
    let key_path = write_key_file(tmp.path(), "key.hex", 0x44);
    let (node, config, dir) = bring_up_with(tmp.path(), Some(key_path)).await;
    create_table(node.dynamo_addr(), "orders").await;
    put_item(node.dynamo_addr(), "orders", "o1", "x").await;
    node.shutdown_graceful().await;

    let mut no_key_config = config.clone();
    no_key_config.nodes[0].encryption_key_path = None;
    let err = match start_node_with(&no_key_config, &dir).await {
        Err(e) => e,
        Ok(_) => panic!("a restart with no key against an encrypted directory must be refused"),
    };
    assert!(
        err.contains("no --encryption-key was given"),
        "error text: {err}"
    );
    assert!(
        err.contains("Pass --encryption-key PATH"),
        "error text: {err}"
    );
}

/// A key configured against an existing PLAINTEXT data directory (the
/// reverse mismatch) must also refuse loudly.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_against_an_existing_plaintext_directory_is_a_loud_refusal() {
    let tmp = support::panic_safe_tempdir();
    let (node, config, dir) = bring_up_with(tmp.path(), None).await;
    create_table(node.dynamo_addr(), "orders").await;
    put_item(node.dynamo_addr(), "orders", "o1", "x").await;
    node.shutdown_graceful().await;

    let key_path = write_key_file(tmp.path(), "late-key.hex", 0x55);
    let mut keyed_config = config.clone();
    keyed_config.nodes[0].encryption_key_path = Some(key_path);
    let err = match start_node_with(&keyed_config, &dir).await {
        Err(e) => e,
        Ok(_) => panic!("a key against an existing plaintext directory must be refused"),
    };
    assert!(
        err.contains("already holds unencrypted files"),
        "error text: {err}"
    );
}

/// Walks every regular file under `dir` (recursively — the internal
/// `ProdEnv` data directory nests a few levels, e.g. `node-0/internal/...`)
/// and asserts none of them contain [`PLAINTEXT_NEEDLE`] as raw bytes.
fn assert_plaintext_absent(dir: &Path) {
    fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    walk(dir, &mut files);
    assert!(
        !files.is_empty(),
        "expected at least one file under {dir:?} to scan"
    );
    for file in files {
        let Ok(bytes) = std::fs::read(&file) else {
            continue;
        };
        assert!(
            !contains_subslice(&bytes, PLAINTEXT_NEEDLE.as_bytes()),
            "plaintext value leaked into {file:?}"
        );
    }
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}
