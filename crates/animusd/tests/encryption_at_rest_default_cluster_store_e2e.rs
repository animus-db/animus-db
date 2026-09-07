//! ADR 0069 "As-built: cluster store" amendment (closing issue #680) — the
//! real-`ProdEnv` end-to-end proof that `--encryption-key PATH` also seals
//! the **default** `cluster` segment/backup store (`SegmentStoreConfig::
//! Cluster`/`BackupStoreConfig::Cluster`, the store every node runs unless
//! `--segment-store`/`--backup-store` is explicitly overridden), not just
//! the opt-in `fs:`/`s3://` stores `encryption_at_rest_segment_store_e2e.rs`
//! already covers. Every node here is started with the **default** store
//! config (no `--segment-store`/`--backup-store` at all): a streamed table's
//! sealed shard object and an on-demand backup's manifest/data objects both
//! land under `<node dir>/segments`/`<node dir>/backups`, and must never
//! hold the plaintext item value; `RestoreTableFromBackup` issued against a
//! **different** node than the one that captured the backup must still
//! succeed, proving the cluster-wide key lets a peer decrypt what it never
//! wrote itself (the identical property `encryption_at_rest_segment_store_
//! e2e.rs` proves for the `fs:` opt-in). The two loud-refusal directions are
//! proven in isolation from PR 1's own `Disk`-seam marker (which shares the
//! same `--dir` and would otherwise mask the cluster-store-specific check)
//! by constructing a target directory that carries only the cluster store's
//! own local `segments` subdirectory content, not a node's main data files —
//! see each refusal test's own doc for why.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_control::Metadata;
use animus_tablet::TabletId;
use animusd::{
    BackupStoreConfig, Node, SegmentStoreConfig, StorageBackend, StreamSealKnobs,
    bind_cluster_with_advertise_host_and_key, start_cluster_with_growth_and_quiesce_after,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

const PLAINTEXT_NEEDLE: &str = "default-cluster-store-secret-77";

/// Seals almost immediately on any pending byte — mirrors
/// `stream_janitor.rs::tiny_seal_knobs`.
fn tiny_seal_knobs() -> StreamSealKnobs {
    StreamSealKnobs {
        seal_bytes: 1,
        seal_age: Duration::from_secs(3600),
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

/// Bring up an `n`-node combined-mode, in-process cluster (`--cluster N`'s
/// own dev shape) with the **default** `SegmentStoreConfig`/
/// `BackupStoreConfig` (`Cluster`) and, when `encryption_key_path` is
/// `Some`, every node's own `--encryption-key`. No port-TOCTOU retry is
/// needed here (unlike `encryption_at_rest_segment_store_e2e.rs`'s own
/// bring-up): `bind_cluster_with_advertise_host_and_key` binds every
/// listener on an OS-assigned ephemeral port (`SocketAddr` port `0`), so
/// there is no fixed-port race to lose.
async fn bring_up_cluster(
    n: usize,
    dir: &Path,
    retention: Duration,
    encryption_key_path: Option<String>,
) -> Vec<Node> {
    let bound = bind_cluster_with_advertise_host_and_key(
        n,
        "127.0.0.1".parse().expect("valid ip"),
        dir,
        None,
        encryption_key_path,
    )
    .await
    .expect("bind cluster");
    start_cluster_with_growth_and_quiesce_after(
        bound,
        StorageBackend::default(),
        None,
        Duration::from_secs(600),
        tiny_seal_knobs(),
        SegmentStoreConfig::default(),
        retention,
        None,
        None,
        Duration::ZERO,
        false,
        None,
        BackupStoreConfig::default(),
        None,
        None,
        None,
        None,
        false,
    )
    .await
    .expect("start cluster")
}

async fn await_bootstrap(nodes: &[Node]) {
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes
                    .iter()
                    .all(|n| n.metadata().members.len() == nodes.len())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap in 20s");
}

fn tablet_for(meta: &Metadata, table: &str) -> TabletId {
    meta.tablets_for_table(table)
        .next()
        .map(|(&t, _)| t)
        .unwrap_or_else(|| panic!("table `{table}` has no tablet yet"))
}

/// Poll every node's own replicated catalog until `table`'s sole tablet has
/// at least one sealed `stream_shards` row — the "wait for a seal" step.
async fn await_sealed_shard(nodes: &[Node], table: &str) {
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().all(|n| {
                let meta = n.metadata();
                meta.has_table_schema(table)
                    && meta
                        .stream_shards
                        .range((tablet_for(&meta, table), 0)..=(tablet_for(&meta, table), u64::MAX))
                        .next()
                        .is_some()
            }) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("`{table}` never sealed a shard on every node within 20s"));
}

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

async fn create_streamed_table(addr: SocketAddr, name: &str) {
    let body = format!(
        r#"{{"TableName":"{name}","AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}],"KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],"StreamSpecification":{{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
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
    let v = json(&resp);
    v.get("Item")
        .and_then(|item| item.get("val"))
        .and_then(|val| val.get("S"))
        .and_then(|s| s.as_str())
        .map(str::to_string)
}

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

fn copy_dir_recursive(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create dst dir");
    for entry in std::fs::read_dir(src).expect("read src dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if path.is_dir() {
            copy_dir_recursive(&path, &target);
        } else {
            std::fs::copy(&path, &target).expect("copy file");
        }
    }
}

/// The load-bearing proof: a keyed cluster's default `cluster` segment AND
/// backup store never holds the plaintext item value at any point — through
/// a stream seal (segment store) and a `CreateBackup` (backup store), both
/// issued/observed against **different** nodes than the one that led the
/// tablet or captured the backup — and `RestoreTableFromBackup` issued
/// against a node that never itself wrote the backup object still succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_cluster_store_holds_no_plaintext_and_restores_across_nodes() {
    let tmp = support::panic_safe_tempdir();
    let key_path = write_key_file(tmp.path(), "key.hex", 0x71);
    let nodes = bring_up_cluster(2, tmp.path(), Duration::from_secs(600), Some(key_path)).await;
    await_bootstrap(&nodes).await;

    let addr0 = nodes[0].dynamo_addr();
    let addr1 = nodes[1].dynamo_addr();

    create_streamed_table(addr0, "orders").await;
    put_item(addr0, "orders", "o1", PLAINTEXT_NEEDLE).await;
    await_sealed_shard(&nodes, "orders").await;

    // The stream's sealed shard object lives in every replica's own
    // `<node dir>/segments` — assert it holds no plaintext on EITHER node,
    // regardless of which one led the tablet.
    for i in 0..2 {
        assert_plaintext_absent(&tmp.path().join(format!("node-{i}")).join("segments"));
    }

    let backup_arn = create_available_backup(addr0, "orders", "nightly").await;
    // Node 1 must also observe AVAILABLE before it can accept the restore
    // (its own replicated-catalog apply can legitimately lag node 0's,
    // independent of anything encryption-related).
    await_backup_available(addr1, &backup_arn).await;

    for i in 0..2 {
        assert_plaintext_absent(&tmp.path().join(format!("node-{i}")).join("segments"));
        assert_plaintext_absent(&tmp.path().join(format!("node-{i}")).join("backups"));
    }

    // The restore is issued against node 1 — which never captured the
    // backup itself — proving the cluster-wide key actually lets a
    // different node decrypt what it never wrote.
    let (status, body) = restore_table(addr1, &backup_arn, "orders_restored").await;
    assert_eq!(status, 200, "RestoreTableFromBackup failed: {body}");
    await_table_active(addr1, "orders_restored").await;
    assert_eq!(
        get_item_consistent(addr1, "orders_restored", "o1").await,
        Some(PLAINTEXT_NEEDLE.to_string()),
        "the restored table must serve the exact backup-time value"
    );

    for i in 0..2 {
        assert_plaintext_absent(&tmp.path().join(format!("node-{i}")).join("segments"));
        assert_plaintext_absent(&tmp.path().join(format!("node-{i}")).join("backups"));
    }

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// A node started with **no** `--encryption-key` is refused at startup
/// against a default `cluster` store whose own local `segments`/`backups`
/// directories are already marked encrypted — isolated from PR 1's own
/// `Disk`-seam marker by construction: that check runs against
/// `<node dir>/internal` (`Node::bind`'s own `ProdEnv::bind_with_tls_and_key
/// (.., dir.join("internal"), ..)` call), a SIBLING directory of
/// `<node dir>/segments`/`<node dir>/backups`, never scanned by
/// `Disk::list()`'s own non-recursive listing — so a target node directory
/// carrying only the encrypted `segments` subdirectory copied out of a real
/// keyed node's own data directory (no `internal/` content at all) sails
/// through PR 1's own fresh-directory branch and genuinely reaches the NEW
/// `LocalSegmentStore` marker check this change adds, not the pre-existing
/// `Disk`-seam one.
#[tokio::test(flavor = "multi_thread")]
async fn default_cluster_store_restart_without_the_key_is_refused_at_startup() {
    let tmp = support::panic_safe_tempdir();
    let key_path = write_key_file(tmp.path(), "key.hex", 0x72);

    // A throwaway keyed node: booting it at all is enough to initialize
    // `<node dir>/segments`'s own encryption marker (`build_segment_store`
    // runs the marker check unconditionally on every boot, even with zero
    // stream/backup activity) — no table/write/seal needed.
    let source_dir = tmp.path().join("source");
    let source_nodes =
        bring_up_cluster(1, &source_dir, Duration::from_secs(600), Some(key_path)).await;
    await_bootstrap(&source_nodes).await;
    for node in &source_nodes {
        node.shutdown_graceful().await;
    }

    // A fresh target directory carrying ONLY the encrypted `segments`
    // subdirectory — no top-level marker, no WAL, no engine files.
    let target_dir = tmp.path().join("target");
    copy_dir_recursive(
        &source_dir.join("node-0").join("segments"),
        &target_dir.join("node-0").join("segments"),
    );

    let err = bring_up_cluster_expect_err(&target_dir, None).await;
    assert!(
        err.contains("segment store is encrypted"),
        "error text: {err}"
    );
    assert!(
        err.contains("no --encryption-key was given"),
        "error text: {err}"
    );
}

/// A `--encryption-key` configured against a default `cluster` store whose
/// own local `segments` directory already holds genuinely unencrypted
/// (plaintext) objects is refused at startup — the reverse mismatch. This
/// is naturally isolated from PR 1's own `Disk`-seam marker: PR 1's own
/// check runs against `<node dir>/internal` (`Node::bind`'s own
/// `ProdEnv::bind_with_tls_and_key(.., dir.join("internal"), ..)` call — a
/// SIBLING directory of `<node dir>/segments`, never scanned by
/// `Disk::list()`'s own non-recursive listing), so a target directory
/// carrying only a hand-written plaintext object under `segments` (no
/// `internal/` content at all) sails through PR 1's own fresh-directory
/// branch and genuinely reaches the new `LocalSegmentStore` check on
/// `<node dir>/segments` — which has no marker of its own but is not
/// empty, the exact "key against an existing plaintext store" shape.
#[tokio::test(flavor = "multi_thread")]
async fn default_cluster_store_key_against_an_existing_plaintext_store_is_refused_at_startup() {
    let tmp = support::panic_safe_tempdir();
    let key_path = write_key_file(tmp.path(), "key.hex", 0x73);

    let target_dir = tmp.path().join("target");
    // A genuinely plaintext local `segments` directory — real content, no
    // `.animus_segment_store_encryption_marker` of its own — with no
    // `internal/` subdirectory at all, so `Node::bind`'s own `Disk`-seam
    // check sees a genuinely fresh directory and proceeds.
    let segments_dir = target_dir.join("node-0").join("segments");
    std::fs::create_dir_all(&segments_dir).expect("create segments dir");
    std::fs::write(segments_dir.join("plaintext-object"), b"unencrypted bytes")
        .expect("write plaintext object");

    let err = bring_up_cluster_expect_err(&target_dir, Some(key_path)).await;
    assert!(
        err.contains("already holds unencrypted objects"),
        "error text: {err}"
    );
}

/// Like [`bring_up_cluster`], but expects `start_cluster_with_growth_and_
/// quiesce_after` itself to fail (the loud-refusal path) and returns the
/// error text — used by the two refusal tests above, which construct a
/// target directory by hand rather than through a normal bring-up.
async fn bring_up_cluster_expect_err(dir: &Path, encryption_key_path: Option<String>) -> String {
    let bound = bind_cluster_with_advertise_host_and_key(
        1,
        "127.0.0.1".parse().expect("valid ip"),
        dir,
        None,
        encryption_key_path,
    )
    .await
    .expect("bind should not itself refuse — the isolation this test relies on");
    let err = start_cluster_with_growth_and_quiesce_after(
        bound,
        StorageBackend::default(),
        None,
        Duration::from_secs(600),
        tiny_seal_knobs(),
        SegmentStoreConfig::default(),
        Duration::from_secs(600),
        None,
        None,
        Duration::ZERO,
        false,
        None,
        BackupStoreConfig::default(),
        None,
        None,
        None,
        None,
        false,
    )
    .await
    .map(|_| ())
    .expect_err("a mismatched cluster-store marker must be refused at startup");
    err.to_string()
}
