//! DynamoDB Streams catalog + write-gate `ProdEnv`-only regressions (ADR
//! 0042 §2/§4/§9/§1/§11). ADR 0061 rung G (C-07 PR 4) converted every
//! sim-convertible test that used to live here into deterministic
//! `SimCluster` siblings in `crates/animusd/src/sim_cluster_dynamo_streams.rs`
//! — mirroring D3 PR 3b's own "trim to the genuine `ProdEnv` residual"
//! precedent (see that module's own doc for the full twelve-test conversion
//! mapping, including which four are proven in kind by its existing PR 3
//! scenarios rather than duplicated). The three tests kept here each need a
//! real capability `SimCluster` structurally lacks:
//!
//! - [`set_table_stream_enable_propagates_and_survives_restart`] — a real
//!   control-plane WAL restart (ADR 0038's durable mirror); `SimCluster`
//!   never crosses a genuine process restart with a real on-disk WAL.
//! - [`disable_survives_concurrent_periodic_seal_on_local_route`] — races
//!   the real periodic `change_consumer_loop`'s own timer-driven seal arm
//!   (issue #572) against the disable-triggered final seal; `SimCluster`
//!   never spawns that loop at all — every seal there is test-driven, via
//!   `SimCluster::drive_stream_seal` — so this specific race has no sim
//!   analog.
//! - [`bare_stream_hot_read_is_refused`] — the production `Surface::Intra`
//!   port guard (`handle_request`'s client-vs-intra-port refusal);
//!   `SimCluster`'s relay dispatch (`animus_node::sim_relay::
//!   SimRelayClient`) has no port concept at all to reproduce this against.
//!
//! Real time/sockets (the `ProdEnv` edge), so every eventual property is a
//! converged-or-timeout poll, never a fixed sleep.
//!
//! The write-path itself (a streamed-unindexed table committing exactly a
//! base row and a change record, view-type storage invariance, trim staying
//! blocked) is covered in-crate (`animusd::dynamo::stream_write_path_tests`)
//! — those assertions need `CpGroup`'s private kind-scan accessors this
//! external `tests/` crate cannot reach; see that module's own doc.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use animusd::{
    Node, SegmentStoreConfig, StorageBackend, StreamSealKnobs, bind_cluster, start_cluster,
    start_cluster_with_streams,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// One DynamoDB JSON request over a fresh HTTP/1.1 connection → `(status,
/// body)`. Mirrors every other `tests/dynamo_*.rs` file's identical helper.
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

async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not bootstrap within 20s");
}

async fn await_node_bootstrap(node: &Node) {
    let ready = async {
        loop {
            if node.is_control_leader() && !node.metadata().members.is_empty() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("node did not bootstrap within 20s");
}

/// Poll until every node in `nodes` sees `table`'s stream enabled with
/// `label` — the schema-replication regression (ADR 0042 §4/§9).
async fn await_stream_label_everywhere(nodes: &[Node], table: &str, label: &str) {
    let converged = async {
        loop {
            if nodes.iter().all(|n| {
                n.metadata()
                    .table_stream(table)
                    .is_some_and(|s| s.label == label)
            }) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), converged)
        .await
        .unwrap_or_else(|_| panic!("stream label `{label}` never converged on every node"));
}

/// Extract a `"LatestStreamLabel":"..."` (or `"StreamViewType":"..."`) field's
/// value out of a raw JSON response body — a tiny substring parse, matching
/// this codebase's existing `tests/*.rs` convention of not pulling in a JSON
/// crate for response assertions.
fn field(body: &str, name: &str) -> String {
    let needle = format!("\"{name}\":\"");
    let start = body
        .find(&needle)
        .unwrap_or_else(|| panic!("field `{name}` not found in: {body}"))
        + needle.len();
    let end = body[start..].find('"').expect("closing quote") + start;
    body[start..end].to_owned()
}

/// `SetTableStream` enable (via `CreateTable`) replicates to every node's
/// mirrored schema and survives a control-plane restart (ADR 0038's durable
/// mirror) — the schema-replication regression, mirroring
/// `dynamo_schema.rs::create_table_survives_node_restart`'s shape but for
/// the stream configuration rather than the key schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_table_stream_enable_propagates_and_survives_restart() {
    let dir = support::panic_safe_tempdir();
    let node_dir = dir.path().join("node-0");
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let dynamo_addr = config.nodes[0].dynamo;
    await_node_bootstrap(&node).await;

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"orders","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    assert!(body.contains("\"StreamEnabled\":true"), "{body}");
    let label = field(&body, "LatestStreamLabel");

    await_stream_label_everywhere(std::slice::from_ref(&node), "orders", &label).await;

    node.shutdown_graceful().await;
    drop(node);
    sleep(Duration::from_millis(200)).await;

    let node = support::restart_same_addrs(&config, 0, &node_dir, StorageBackend::default()).await;
    await_node_bootstrap(&node).await;
    await_stream_label_everywhere(std::slice::from_ref(&node), "orders", &label).await;
}

/// A bare (non-`Forwarded`) `ClientRequest::StreamHotRead` over the plain
/// **client** protocol is refused — mirroring `KindWrite`/`KindScan`/
/// `ForceSeal`'s identical contract (the house "internal RPC must reject a
/// bare delivery" rule). **ADR 0047**: `StreamHotRead` is classified
/// `Surface::Intra`, so a client-port connection is refused by
/// `handle_request`'s port guard ("send it to this node's intra port")
/// before ever reaching the match arm's own "must be sent wrapped in
/// `Forwarded`" refusal — that wording is still reachable, just only via the
/// intra port now (see `intra_port_split.rs`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_stream_hot_read_is_refused() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let mut stream = TcpStream::connect(nodes[0].client_addr())
        .await
        .expect("connect to client port");
    let request = animusd::ClientRequest::StreamHotRead {
        tablet: 1,
        from_position: 0,
        limit: 10,
    };
    animusd::write_frame(&mut stream, &request)
        .await
        .expect("write frame");
    let response: animusd::ClientResponse = animusd::read_frame(&mut stream)
        .await
        .expect("read frame")
        .expect("connection stayed open for a reply");
    match response {
        animusd::ClientResponse::Error(msg) => {
            assert!(
                msg.contains("cluster-internal request"),
                "expected the ADR 0047 client-port refusal message, got: {msg}"
            );
        }
        other => panic!("expected a bare-request refusal, got: {other:?}"),
    }
}

/// A streamed cluster bring-up over real `ProdEnv` sockets, sealing driven
/// by the genuine `change_consumer_loop` tick against the given knobs
/// (never the production defaults) — the fixture
/// [`disable_survives_concurrent_periodic_seal_on_local_route`] needs to
/// race a real periodic seal.
async fn start_streamed_cluster(
    n: usize,
    dir: &std::path::Path,
    knobs: StreamSealKnobs,
) -> Vec<Node> {
    let bound = bind_cluster(n, "127.0.0.1".parse().unwrap(), dir)
        .await
        .unwrap();
    start_cluster_with_streams(
        bound,
        StorageBackend::default(),
        None,
        Duration::from_secs(600),
        knobs,
        SegmentStoreConfig::default(),
        animusd::DEFAULT_STREAM_RETENTION,
    )
    .await
    .unwrap()
}

/// One `PutItem` with a `pad_len`-byte `val` attribute, over a fresh
/// connection — the same padded-write shape `tests/dynamo_pitr.rs` uses to
/// force the periodic seal arm past `seal_bytes` quickly.
async fn put_item_padded(addr: SocketAddr, table: &str, id: &str, pad_len: usize) {
    let pad = "x".repeat(pad_len);
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        &format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"val":{{"S":"{pad}"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "PutItem({id}) failed: {body}");
}

/// Issue #572 regression, the stream twin of `dynamo_pitr.rs`'s
/// `disable_survives_concurrent_periodic_seal_on_local_route`: the
/// disable-triggered final seal (`ClientCtx::force_seal_tablet`, F12-b) must
/// retry a dueling-seal race against the periodic size-triggered arm
/// (`seal_tick`, `INDEX_DRAIN_INTERVAL` = 200ms) on the `CpRoute::Local`
/// route exactly as it already does on `CpRoute::Forward` — not surface the
/// transient "lost to a concurrent seal ...; retry" error as a hard 500.
///
/// A single-node cluster is deliberate: with one node, the tablet leader is
/// always *this* node, so `resolve_cp_route` can only ever return
/// `CpRoute::Local` — every `UpdateTable` disable in this test exercises
/// exactly the route the bug lives on. A small `seal_bytes` keeps the
/// periodic arm sealing on nearly every tick; several concurrent `PutItem`
/// writers keep running *through* each disable call (not just before it) so
/// a periodic seal can commit mid-flight while the disable's own force-seal
/// is still computing/proposing its own. One attempt reproduces only
/// sporadically (the original report: 1 in 15), so this loops many
/// enable/write/disable cycles, each a fresh chance at the same race, and
/// asserts every single disable succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn disable_survives_concurrent_periodic_seal_on_local_route() {
    let racy_knobs = StreamSealKnobs {
        seal_bytes: 48,
        seal_age: Duration::from_secs(3600),
    };
    let disable = async {
        let dir = support::panic_safe_tempdir();
        let nodes = start_streamed_cluster(1, dir.path(), racy_knobs).await;
        await_bootstrap(&nodes).await;
        let addr = nodes[0].dynamo_addr();
        let table = "t";

        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"t","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                "StreamSpecification":{"StreamEnabled":true,
                    "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        const CYCLES: u32 = 40;
        const WRITERS: u32 = 8;
        for cycle in 0..CYCLES {
            if cycle > 0 {
                let (status, body) = dynamo(
                    addr,
                    "DynamoDB_20120810.UpdateTable",
                    r#"{"TableName":"t","StreamSpecification":
                        {"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
                )
                .await;
                assert_eq!(status, 200, "re-enable (cycle {cycle}) failed: {body}");
            }

            // Several writers hammer PutItem concurrently, kept running
            // through the disable call below (stopped only afterward) so
            // the periodic arm's own seal attempt can race the disable's.
            let stop = Arc::new(AtomicBool::new(false));
            let mut writers = Vec::with_capacity(WRITERS as usize);
            for w in 0..WRITERS {
                let table = table.to_string();
                let stop = Arc::clone(&stop);
                writers.push(tokio::spawn(async move {
                    let mut i = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        put_item_padded(addr, &table, &format!("c{cycle}-w{w}-{i}"), 24).await;
                        i += 1;
                    }
                }));
            }
            // Give the periodic arm at least one full tick's worth of
            // pending bytes to seal before racing the disable call.
            sleep(Duration::from_millis(120)).await;

            let (status, body) = dynamo(
                addr,
                "DynamoDB_20120810.UpdateTable",
                r#"{"TableName":"t","StreamSpecification":{"StreamEnabled":false}}"#,
            )
            .await;

            stop.store(true, Ordering::Relaxed);
            for w in writers {
                let _ = w.await;
            }

            assert_eq!(
                status, 200,
                "UpdateTable(disable) failed on cycle {cycle} — issue #572's dueling-seal \
                 race on the CpRoute::Local route: {body}"
            );
        }

        // One more clean (non-racing) round proves the fix doesn't
        // silently drop coverage across the stress loop above: every write
        // in this fresh label is fully accounted for by that label's own
        // sealed shards before the final disable.
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.UpdateTable",
            r#"{"TableName":"t","StreamSpecification":
                {"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
        )
        .await;
        assert_eq!(status, 200, "final re-enable failed: {body}");
        let label = field(&body, "LatestStreamLabel");
        await_stream_label_everywhere(&nodes, table, &label).await;
        const FINAL_WRITES: u64 = 12;
        for i in 0..FINAL_WRITES {
            put_item_padded(addr, table, &format!("final-{i}"), 40).await;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let sealed: u64 = nodes[0]
                .metadata()
                .stream_shards
                .values()
                .filter(|r| r.table == table && r.label == label)
                .map(|r| r.count)
                .sum();
            if sealed >= FINAL_WRITES {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "this label's stream shards never covered every final write"
            );
            sleep(Duration::from_millis(50)).await;
        }

        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.UpdateTable",
            r#"{"TableName":"t","StreamSpecification":{"StreamEnabled":false}}"#,
        )
        .await;
        assert_eq!(status, 200, "final disable failed: {body}");
    };
    timeout(Duration::from_secs(90), disable)
        .await
        .expect("did not converge in time");
}
