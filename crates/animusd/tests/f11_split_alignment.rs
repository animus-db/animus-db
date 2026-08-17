//! F11 (ADR 0042 §14, growth PR2): the token-alignment choke point at
//! `ClientCtx::trigger_split`. Before this PR, the round-down existed ONLY
//! inside `auto_split_loop` — `POST /admin/tablet/split` and
//! `ClientRequest::SplitTablet` both passed a raw, caller-supplied key
//! straight through, so a manual split on a streamed table could separate
//! one partition's own rows across two **sibling** tablets (no
//! parent/child relation at all — ADR 0043 §A4 makes a tablet's own change
//! scope the physical backing of a stream shard), breaking the per-item
//! ordering ADR 0042 §14 exists to protect.
//!
//! This is a regression through a **follower-connected** node (the house
//! rule for a relayable command: at least one non-leader-issued exercise —
//! see the root `CLAUDE.md`'s "missed allowlist is a bimodal per-process
//! flake" lesson and `docs/engineering-lessons.md`).
//!
//! **What this proves and does not prove.** Two rows sharing a common
//! 8-byte key prefix (standing in for a real partition's token — ADR 0022;
//! this test hand-drives raw KV keys via the plain client protocol rather
//! than going through the DynamoDB wire edge's real `partition_token`
//! hashing, since `align_split_key`'s own gate
//! (`meta.table_stream(table).is_some()`) only reads the table's replicated
//! schema, never how a particular write reached the engine) straddle a
//! deliberately unaligned split key that a pre-PR2 admin path would have
//! accepted verbatim. It proves: (1) the resulting sibling's `range.start`
//! is the ROUNDED key, not the raw requested one, and (2) both rows
//! resolve to the SAME tablet afterward, never one in each. It does not
//! reproduce real DynamoDB Streams change records — the tablet-placement
//! invariant this proves is exactly what keeps a partition's own change
//! records inside one tablet's own change scope (ADR 0043 §A4), so proving
//! placement proves the ordering guarantee that rests on it.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_control::{StreamSpec, StreamViewType};
use animus_tablet::{KeyRange, TabletId};
use animusd::{
    ClientRequest, ClientResponse, ColumnType, MetaCommand, Node, TableSchema, read_frame,
};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// Bring up an `n`-node per-process cluster (each its own edge state) —
/// duplicated fixture, per this codebase's own "sibling test modules keep
/// their own fixtures independent" convention (see `index_backfill.rs`'s
/// identical doc comment).
async fn bring_up(n: usize, dir: &Path) -> Vec<Node> {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                cql: addrs[6 * i + 3],
                admin: addrs[6 * i + 4],
                intra: addrs[6 * i + 5],
            })
            .collect();
        let config = animusd::ClusterConfig { nodes: nodes_cfg };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            match animusd::run_node(&config, i, dir.join(format!("node-{attempt}-{i}"))).await {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            return nodes;
        }
        for node in &nodes {
            node.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up cluster after retries (ports kept getting stolen)");
}

async fn await_bootstrap(nodes: &[Node]) {
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap in 20s");
}

/// One HTTP/1.0 admin request; returns `(status, parsed JSON)` — duplicated
/// from `admin_endpoint.rs`'s own helper (a different compilation unit).
async fn admin_post(addr: SocketAddr, path: &str, body: &str) -> (u16, serde_json::Value) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let request = format!(
        "POST {path} HTTP/1.0\r\n\
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
    let value = serde_json::from_str(payload).unwrap_or(serde_json::Value::Null);
    (status, value)
}

/// Retry a plain-protocol `Put` against `client` until it acks (the CP
/// group needs a moment to elect right after `CreateTablet` commits).
async fn put_retry(client: SocketAddr, table: &str, key: &[u8], value: &[u8]) {
    timeout(Duration::from_secs(15), async {
        loop {
            let resp = call(
                client,
                ClientRequest::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                    table: table.to_string(),
                },
            )
            .await;
            if matches!(resp, ClientResponse::PutOk) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("put {key:?} did not succeed in time"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn manual_split_with_unaligned_key_on_streamed_table_rounds_to_token_boundary() {
    timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().expect("tempdir");
        let nodes = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        let client = nodes[0].client_addr();
        let all_ids: Vec<_> = (0..3).map(animusd::config::node_id).collect();
        // ADR 0047: `ProposeSchema` is intra-only — target the control
        // leader's own intra address for the hand-driven fixture setup
        // below (mirroring `index_backfill.rs`'s identical convention).
        // `Put`/`Get`/the admin HTTP surface are unaffected (still public).
        let leader = nodes
            .iter()
            .position(Node::is_control_leader)
            .expect("await_bootstrap guarantees a leader exists");
        let intra = nodes[leader].intra_addr();

        // Hand-drive the fixture (mirrors `index_backfill.rs`'s own
        // convention): a streamed table's schema + one bootstrap tablet,
        // with real replicas so the CP group actually forms and serves
        // plain-protocol reads/writes.
        call(
            intra,
            ClientRequest::ProposeSchema(MetaCommand::CreateTableSchema {
                table: "orders".into(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
        )
        .await;
        timeout(Duration::from_secs(10), async {
            loop {
                if nodes[0].metadata().has_table_schema("orders") {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("CreateTableSchema did not commit in 10s");

        // `ProposeSchema` is fire-and-forget (the caller confirms via
        // replicated `Metadata`, never a commit-wait reply) — `SetTableStream`
        // must not be sent until the schema it depends on has actually
        // committed, or the control leader's apply arm rejects it as
        // "no such table" with nothing here to notice.
        call(
            intra,
            ClientRequest::ProposeSchema(MetaCommand::SetTableStream {
                table: "orders".into(),
                spec: Some(StreamSpec {
                    view_type: StreamViewType::NewAndOldImages,
                    label: "L1".into(),
                }),
            }),
        )
        .await;
        timeout(Duration::from_secs(10), async {
            loop {
                if nodes[0].metadata().table_stream("orders").is_some() {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("SetTableStream did not commit in 10s");

        call(
            intra,
            ClientRequest::ProposeSchema(MetaCommand::CreateTablet {
                tablet: TabletId(500),
                table: Some("orders".into()),
                range: KeyRange::whole(),
                replicas: all_ids.clone(),
            }),
        )
        .await;
        timeout(Duration::from_secs(10), async {
            loop {
                if nodes[0].metadata().tablets.contains_key(&TabletId(500)) {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("CreateTablet did not commit in 10s");

        // Two rows sharing the 8-byte prefix `"orders-m"` (standing in for
        // a real partition's shared token, ADR 0022) — `"orders-mX"` and
        // `"orders-mY"`, differing only at the 9th byte.
        let key_a = b"orders-mX".to_vec();
        let key_b = b"orders-mY".to_vec();
        put_retry(client, "orders", &key_a, b"va").await;
        put_retry(client, "orders", &key_b, b"vb").await;

        // A deliberately unaligned split key: 10 bytes, NOT the
        // `TOKEN_BYTES == 8` a token-aligned key must be, and strictly
        // between `key_a` and `key_b` in byte order — the common prefix is
        // `"orders-m"` (8 bytes), then `key_a`/`"orders-mXX"` share `X`
        // (0x58) at byte 8 while `key_b` has `Y` (0x59), so `"orders-mXX"`
        // sorts after `key_a` (proper extension) but strictly before
        // `key_b`. A pre-PR2 admin path would have accepted this verbatim,
        // putting `key_a` in the ORIGINAL tablet (500) and `key_b` in the
        // new sibling — the exact per-item ordering violation this PR
        // closes.
        let unaligned_split_key = "orders-mXX";
        assert!(key_a.as_slice() < unaligned_split_key.as_bytes());
        assert!(unaligned_split_key.as_bytes() < key_b.as_slice());

        // Drive the split through a **follower** node's own admin port,
        // never the control leader's — `trigger_split` is relayable
        // (`is_relayable_command`), so this must work identically to
        // hitting the leader directly.
        let follower = nodes
            .iter()
            .find(|n| !n.is_control_leader())
            .expect("a 3-node cluster has at least one follower");
        let (status, body) = admin_post(
            follower.admin_addr(),
            "/admin/tablet/split",
            &format!(r#"{{"tablet":500,"split_key":"{unaligned_split_key}"}}"#),
        )
        .await;
        assert_eq!(status, 200, "split via follower admin port failed: {body}");

        // The copy-based workflow (ADR 0050 rung 5) runs to CUTOVER on its
        // own: the parent (500) leaves the map and two `Active` children
        // partition its range at the rounded key.
        let (left_range, right_range) = timeout(Duration::from_secs(45), async {
            loop {
                let meta = nodes[0].metadata();
                if !meta.tablets.contains_key(&TabletId(500)) {
                    let mut children: Vec<_> = meta
                        .tablets
                        .values()
                        .filter(|t| t.table.as_deref() == Some("orders") && t.is_routable())
                        .map(|t| t.range.clone())
                        .collect();
                    if children.len() == 2 {
                        children.sort_by(|a, b| a.start.cmp(&b.start));
                        return (children.remove(0), children.remove(0));
                    }
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the split workflow never cut over to two Active children");

        // F11's own assertion: the RIGHT child's `range.start` is the
        // ROUNDED key (`"orders-m"`, the leading `TOKEN_BYTES` of the
        // request), not the raw 10-byte request the pre-PR2 admin path
        // would have used verbatim.
        let sibling_start = right_range.start.clone();
        assert_eq!(
            sibling_start,
            b"orders-m".to_vec(),
            "F11 must round the admin-supplied split key down to its token boundary"
        );
        assert_ne!(
            sibling_start,
            unaligned_split_key.as_bytes().to_vec(),
            "the raw, unaligned request must never be used verbatim on a streamed table"
        );

        // Per-item ordering survives: BOTH rows land in the SAME (right)
        // child — `key_a`/`key_b` >= the rounded boundary, so neither
        // stayed behind on the left.
        assert!(
            !left_range.contains(&key_a) && !left_range.contains(&key_b),
            "neither row may land left of the rounded boundary"
        );
        assert!(
            right_range.contains(&key_a) && right_range.contains(&key_b),
            "both rows must belong to the right child — one tablet, one shard lineage"
        );

        // Belt-and-suspenders: both rows are still readable, unharmed by
        // the split (no data loss/corruption), through ordinary re-routed
        // reads.
        assert_eq!(
            call(
                client,
                ClientRequest::Get {
                    key: key_a.clone(),
                    table: "orders".into(),
                }
            )
            .await,
            ClientResponse::Value(Some(b"va".to_vec()))
        );
        assert_eq!(
            call(
                client,
                ClientRequest::Get {
                    key: key_b.clone(),
                    table: "orders".into(),
                }
            )
            .await,
            ClientResponse::Value(Some(b"vb".to_vec()))
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}
