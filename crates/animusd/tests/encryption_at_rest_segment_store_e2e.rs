//! ADR 0069 (S-03 PR 2) — the real-`ProdEnv` end-to-end proof that
//! `--encryption-key PATH` also seals a `SegmentStore` (here, the on-demand
//! **backup** store, `--backup-store fs:PATH`), not just the `Disk` seam PR
//! 1 already covered: a keyed cluster's `CreateBackup` → cross-node
//! `RestoreTableFromBackup` round trip succeeds and the shared backup-store
//! directory never holds the plaintext item value; a node started with a
//! DIFFERENT key against that same (already-marked) directory is refused
//! loudly at startup, before it ever binds a listener; and a key configured
//! against an existing PLAINTEXT backup-store directory is refused the same
//! way. Modeled on `encryption_at_rest_e2e.rs` (PR 1's own e2e), generalized
//! to a real multi-node cluster since a `SegmentStore` object — unlike a
//! `Disk` file — is routinely read by a different node than the one that
//! wrote it (see `animus_env::EncryptedSegmentStore`'s own module doc for
//! the cluster-wide key-scope decision this proves end to end).

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animusd::{BackupStoreConfig, ClusterConfig, Node, RoleAddrs, StorageBackend};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

const PLAINTEXT_NEEDLE: &str = "backup-store-secret-value-77";

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

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
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
    let v: serde_json::Value = json(&resp);
    v.get("Item")
        .and_then(|item| item.get("val"))
        .and_then(|val| val.get("S"))
        .and_then(|s| s.as_str())
        .map(str::to_string)
}

/// Poll `DescribeBackup` **against `addr`** converged-or-timeout to
/// `AVAILABLE` — deliberately parameterized by which node answers, not
/// hardcoded to whichever node issued `CreateBackup`: the backup catalog is
/// ordinary replicated `Metadata`, so a different node's own local apply
/// can legitimately lag behind the node that committed the transition (the
/// same staleness class `docs/adr/0018-*.md`'s "read-your-writes" caveats
/// document elsewhere) — a caller about to act on a *different* node's
/// belief (here, issuing the restore against node 1) must poll that same
/// node, not the one that created the backup.
async fn await_backup_available(addr: SocketAddr, backup_arn: &str) {
    timeout(Duration::from_secs(20), async {
        loop {
            let (status, body) = dynamo(
                addr,
                "DynamoDB_20120810.DescribeBackup",
                &format!(r#"{{"BackupArn":"{backup_arn}"}}"#),
            )
            .await;
            assert_eq!(status, 200, "body: {body}");
            if json(&body)["BackupDescription"]["BackupDetails"]["BackupStatus"] == "AVAILABLE" {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("backup did not become AVAILABLE in 20s");
}

async fn create_available_backup(addr: SocketAddr, table: &str, backup_name: &str) -> String {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateBackup",
        &format!(r#"{{"TableName":"{table}","BackupName":"{backup_name}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "CreateBackup failed: {body}");
    let backup_arn = json(&body)["BackupDetails"]["BackupArn"]
        .as_str()
        .expect("BackupArn")
        .to_owned();
    await_backup_available(addr, &backup_arn).await;
    backup_arn
}

async fn restore_table(
    addr: SocketAddr,
    backup_arn: &str,
    target_table_name: &str,
) -> (u16, String) {
    dynamo(
        addr,
        "DynamoDB_20120810.RestoreTableFromBackup",
        &format!(r#"{{"BackupArn":"{backup_arn}","TargetTableName":"{target_table_name}"}}"#),
    )
    .await
}

async fn await_table_active(addr: SocketAddr, table: &str) {
    timeout(Duration::from_secs(20), async {
        loop {
            let (status, body) = dynamo(
                addr,
                "DynamoDB_20120810.DescribeTable",
                &format!(r#"{{"TableName":"{table}"}}"#),
            )
            .await;
            if status == 200 && json(&body)["Table"]["TableStatus"] == "ACTIVE" {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("table `{table}` did not converge to ACTIVE in 20s"));
}

fn write_key_file(dir: &Path, name: &str, byte: u8) -> String {
    let path = dir.join(name);
    std::fs::write(&path, hex(byte)).expect("write key file");
    path.to_string_lossy().into_owned()
}

fn hex(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

/// Start (or restart) one node of an `n`-node cluster at `index`, pointed at
/// a **shared** `--backup-store fs:PATH` directory (`backup_store_dir`) and
/// (optionally) a per-node `--encryption-key` — every node in a real
/// deployment sharing a `fs:` backup store passes the identical key path,
/// per `EncryptedSegmentStore`'s cluster-wide key-scope contract.
/// `SegmentStoreConfig` stays the default (`Cluster`) throughout — this
/// file's whole point is the **backup** store, PR 1 already proved the
/// `Disk` seam.
async fn start_node_with_backup_store(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<std::path::PathBuf>,
    backup_store_dir: &Path,
) -> Result<Node, String> {
    animusd::run_node_with_cluster_settings(
        config,
        index,
        dir,
        StorageBackend::default(),
        animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
        animusd::StreamSealKnobs::default(),
        animusd::SegmentStoreConfig::default(),
        animusd::DEFAULT_STREAM_RETENTION,
        Duration::ZERO,
        false,
        None,
        None,
        None,
        BackupStoreConfig::Fs(backup_store_dir.to_path_buf()),
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

/// Bring up an `n`-node combined-mode cluster (one process per node, all
/// in this test binary), every node sharing `backup_store_dir` as its
/// `--backup-store fs:PATH` and `encryption_key_path` as its
/// `--encryption-key`. Retries the whole allocate-fresh-ports-and-start-all
/// unit against the port-TOCTOU race, the same shape
/// `support::bring_up_deadline` uses.
async fn bring_up_cluster(
    n: usize,
    base_dir: &Path,
    backup_store_dir: &Path,
    encryption_key_path: Option<String>,
) -> (Vec<Node>, ClusterConfig) {
    let hard_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut attempt: u64 = 0;
    loop {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<RoleAddrs> = (0..n)
            .map(|i| RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
                advertise_host: None,
                tls: None,
                encryption_key_path: encryption_key_path.clone(),
            })
            .collect();
        let config = ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            match start_node_with_backup_store(
                &config,
                i,
                base_dir.join(format!("core-{attempt}-{i}")),
                backup_store_dir,
            )
            .await
            {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            return (nodes, config);
        }
        for node in &nodes {
            node.shutdown_graceful().await;
        }
        assert!(
            tokio::time::Instant::now() < hard_deadline,
            "could not bring up the {n}-node cluster within the deadline"
        );
        sleep(Duration::from_millis(50)).await;
        attempt += 1;
    }
}

async fn await_bootstrap(nodes: &[Node], expected_members: usize) {
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes
                    .iter()
                    .all(|n| n.metadata().members.len() == expected_members)
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap in 20s");
}

/// Walks every regular file under `dir` recursively and asserts none of
/// them contain [`PLAINTEXT_NEEDLE`] as raw bytes.
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

/// The load-bearing proof: a keyed cluster's `CreateBackup` (issued against
/// node 0) followed by `RestoreTableFromBackup` issued against a
/// **different** node (node 1) — which never itself wrote the backup
/// object — succeeds, because both nodes share the same `--encryption-key`
/// over the same shared `fs:` backup-store directory. The raw backup-store
/// directory never holds the plaintext item value at any point.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_on_a_different_node_with_the_same_key_succeeds_and_the_store_holds_no_plaintext() {
    let tmp = support::panic_safe_tempdir();
    let key_path = write_key_file(tmp.path(), "key.hex", 0x61);
    let backup_store_dir = tmp.path().join("shared-backups");
    let (nodes, config) = bring_up_cluster(2, tmp.path(), &backup_store_dir, Some(key_path)).await;
    await_bootstrap(&nodes, 2).await;

    let addr0 = config.nodes[0].dynamo;
    let addr1 = config.nodes[1].dynamo;

    create_table(addr0, "orders").await;
    put_item(addr0, "orders", "o1", PLAINTEXT_NEEDLE).await;

    let backup_arn = create_available_backup(addr0, "orders", "nightly").await;
    assert_plaintext_absent(&backup_store_dir);
    // Node 1 must also observe AVAILABLE before it can accept the restore —
    // its own replicated-catalog apply can legitimately lag node 0's,
    // independent of anything encryption-related (see
    // `await_backup_available`'s own doc).
    await_backup_available(addr1, &backup_arn).await;

    // The restore is issued against node 1 — which never wrote the backup
    // object itself — proving the cluster-wide key actually lets a
    // different node decrypt it.
    let (status, body) = restore_table(addr1, &backup_arn, "orders_restored").await;
    assert_eq!(status, 200, "RestoreTableFromBackup failed: {body}");

    await_table_active(addr1, "orders_restored").await;
    assert_eq!(
        get_item_consistent(addr1, "orders_restored", "o1").await,
        Some(PLAINTEXT_NEEDLE.to_string()),
        "the restored table must serve the exact backup-time value"
    );

    assert_plaintext_absent(&backup_store_dir);

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// A node started with a DIFFERENT `--encryption-key` against an
/// already-marked (by an earlier node's own startup) shared `fs:`
/// backup-store directory is refused loudly at startup — before it ever
/// binds a single listener — with the exact ADR 0069 mismatch text. Every
/// node provisions its backup store unconditionally at assembly time (see
/// `build_backup_store`'s own doc), so this needs no `CreateBackup` call at
/// all: bringing a keyed node up against a fresh `fs:` directory already
/// writes the marker.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_with_a_different_key_against_the_same_backup_store_is_refused_at_startup() {
    let tmp = support::panic_safe_tempdir();
    let key_a = write_key_file(tmp.path(), "key-a.hex", 0x62);
    let key_b = write_key_file(tmp.path(), "key-b.hex", 0x63);
    let backup_store_dir = tmp.path().join("shared-backups");

    let (nodes, _config) =
        bring_up_cluster(1, &tmp.path().join("first"), &backup_store_dir, Some(key_a)).await;
    for node in &nodes {
        node.shutdown_graceful().await;
    }

    // A second, independent single-node "cluster" pointed at the SAME
    // shared backup-store directory, under a DIFFERENT key.
    let addrs = support::free_addrs(6);
    let second_cfg = ClusterConfig {
        nodes: vec![RoleAddrs {
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
            encryption_key_path: Some(key_b),
        }],
        dynamo_auth: None,
        cluster_settings: None,
    };
    let err =
        start_node_with_backup_store(&second_cfg, 0, tmp.path().join("second"), &backup_store_dir)
            .await
            .map(|_| ())
            .expect_err(
                "a mismatched key against the same backup store must be refused at startup",
            );
    assert!(
        err.contains("does not match the key this segment store was encrypted with"),
        "error text: {err}"
    );
    assert!(err.contains("Use the original key"), "error text: {err}");
}

/// A key configured against an existing PLAINTEXT `fs:` backup-store
/// directory (the reverse mismatch) is refused the same way.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_against_an_existing_plaintext_backup_store_is_refused_at_startup() {
    let tmp = support::panic_safe_tempdir();
    let backup_store_dir = tmp.path().join("shared-backups");

    // A first, keyless node writes a real (plaintext) object into the
    // shared backup-store directory.
    let (nodes, config) =
        bring_up_cluster(1, &tmp.path().join("first"), &backup_store_dir, None).await;
    create_table(config.nodes[0].dynamo, "orders").await;
    put_item(config.nodes[0].dynamo, "orders", "o1", "x").await;
    create_available_backup(config.nodes[0].dynamo, "orders", "nightly").await;
    for node in &nodes {
        node.shutdown_graceful().await;
    }

    let key_path = write_key_file(tmp.path(), "late-key.hex", 0x64);
    let addrs = support::free_addrs(6);
    let keyed_cfg = ClusterConfig {
        nodes: vec![RoleAddrs {
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
            encryption_key_path: Some(key_path),
        }],
        dynamo_auth: None,
        cluster_settings: None,
    };
    let err =
        start_node_with_backup_store(&keyed_cfg, 0, tmp.path().join("second"), &backup_store_dir)
            .await
            .map(|_| ())
            .expect_err(
                "a key against an existing plaintext backup store must be refused at startup",
            );
    assert!(
        err.contains("already holds unencrypted objects"),
        "error text: {err}"
    );
}
