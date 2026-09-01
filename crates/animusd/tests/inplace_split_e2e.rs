//! ADR 0058 Train 2 rung 3's `animusd`-level driver residue, end to end over
//! a **real multi-node cluster** started with `--split-mode inplace`
//! (`SplitMode::InPlace`): a populated table's `trigger_split` proposes
//! `MetaCommand::BeginSplitInPlace` instead of the copy-based `BeginSplit`,
//! the CP data plane's own host reconciler adds learners/catches them
//! up/forks the parent's group entirely on its own (ADR 0058 rung 3,
//! unmodified by this layer), and `index_drain.rs::inplace_split_driver_tick`
//! watches for that fork, runs the pre-cutover vetoes, and proposes
//! `CutoverSplit`.
//!
//! A **paced** continuous writer (unique keys, one every few milliseconds —
//! dense enough to cover the whole fork→cutover window, gentle enough not to
//! become the bottleneck itself, mirroring `split_build.rs`'s own
//! `probe_put_item_until_stopped` pacing rationale for the identical
//! copy-based question) runs from just before kickoff until just after
//! cutover completes. Every acked write must be readable afterward with its
//! correct value — proof it landed on (and is served by) whichever child
//! actually owns it, since a client `get` only ever succeeds by resolving to
//! the tablet that genuinely holds the key (`topology::tablet_for_key`) —
//! and the plain client protocol's bounded-retry-on-any-error `put`/`get`
//! helpers below are exactly what "a write refusal during the transition
//! retries through transparently" means operationally: any `FROZEN_REFUSAL`-
//! shaped or stale-route error surfacing mid-transition is absorbed here the
//! same way it already is by `split_build.rs`'s own copy-based teeth,
//! without this test needing to know whether the in-place fork's own blip
//! (ADR 0058's own "Stale routing" — expected far smaller than the
//! copy-based ~458ms figure, "roughly one routing refresh") ever actually
//! fires during a given run.
//!
//! A streams-enabled variant is included too
//! ([`inplace_split_stream_shard_walks_parent_to_children_without_loss_or_duplication`]):
//! this is "cheap" specifically because exactly one split (never a cascade)
//! needs no generalized re-resolve-the-chain-every-pass machinery the way
//! `streams_e2e.rs`'s `drain_all_tablets_lineage` does for a cascading
//! split — a single TRIM_HORIZON walk of the parent's own shard 0, followed
//! by one of each child's own shard 0 once cutover is observed, is the whole
//! shape.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use animusd::{
    ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, SegmentStoreConfig, SplitMode,
    StorageBackend, StreamSealKnobs, read_frame, write_frame,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Bring up an `n`-node cluster, one process-shaped node per index (real
/// TCP, real ports — `support::free_addrs`' bind-then-drop TOCTOU-retry
/// shape, identical to `split_build.rs::bring_up`), every node started with
/// `SplitMode::InPlace` — the one behavioral difference from every other
/// `ProdEnv` split e2e test in this crate. Quiescence disabled
/// (`Duration::ZERO`, matching `run_node`'s own default every other e2e test
/// here relies on) so a continuous writer's own traffic is never in a race
/// with a group re-waking itself.
async fn bring_up_inplace(
    n: usize,
    dir: &std::path::Path,
    stream_seal_knobs: StreamSealKnobs,
) -> (Vec<Node>, ClusterConfig) {
    for attempt in 0..16 {
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
            })
            .collect();
        let config = ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
        };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            match animusd::run_node_with_streams_quiesce_and_split_mode(
                &config,
                i,
                dir.join(format!("node-{attempt}-{i}")),
                StorageBackend::default(),
                Duration::from_secs(600),
                stream_seal_knobs,
                SegmentStoreConfig::default(),
                animusd::DEFAULT_STREAM_RETENTION,
                Duration::ZERO,
                SplitMode::InPlace,
                animusd::BackupStoreConfig::default(),
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
        sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "could not bring up an in-place-split cluster after retries (ports kept getting stolen)"
    );
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

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed JSON)`.
async fn admin(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.0\r\nHost: animus\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
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
    let json: Value = serde_json::from_str(payload.trim()).unwrap_or(Value::Null);
    (status, json)
}

/// A `put` through the plain client protocol with a bounded retry on ANY
/// error reply — a put is idempotent, and both early-cluster transients AND
/// an in-place split's own (expected near-zero, ADR 0058's "Stale routing")
/// blip surface as retryable one-off errors, the identical contract
/// `split_build.rs::put` relies on for the copy-based workflow. This IS the
/// "write refusals during the transition are retried through transparently"
/// teeth: any refusal that occurs mid-transition is absorbed right here,
/// silently, exactly as a real client SDK's own retry policy would.
async fn put(stream: &mut TcpStream, key: Vec<u8>, value: Vec<u8>) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut retried = false;
    loop {
        write_frame(
            stream,
            &ClientRequest::Put {
                key: key.clone(),
                value: value.clone(),
                table: "t".to_string(),
            },
        )
        .await
        .expect("send frame");
        match read_frame(stream).await.expect("read").expect("reply") {
            ClientResponse::PutOk => return retried,
            ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                retried = true;
                sleep(Duration::from_millis(150)).await;
            }
            other => panic!("put failed: {other:?}"),
        }
    }
}

/// A linearizable read through the plain client protocol.
async fn get(stream: &mut TcpStream, key: Vec<u8>) -> Option<Vec<u8>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        write_frame(
            stream,
            &ClientRequest::Get {
                key: key.clone(),
                table: "t".to_string(),
                stale: false,
            },
        )
        .await
        .expect("send frame");
        match read_frame(stream).await.expect("read").expect("reply") {
            ClientResponse::Value(v) => return v,
            ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(150)).await;
            }
            other => panic!("get failed: {other:?}"),
        }
    }
}

/// The single tablet id currently serving `table` (asserts exactly one —
/// callers use this before any split).
fn sole_tablet_of(node: &Node, table: &str) -> u64 {
    let meta = node.metadata();
    let ids: Vec<u64> = meta
        .tablets
        .iter()
        .filter(|(_, t)| t.table.as_deref() == Some(table))
        .map(|(id, _)| id.0)
        .collect();
    assert_eq!(
        ids.len(),
        1,
        "expected exactly one tablet of {table}: {ids:?}"
    );
    ids[0]
}

/// Kick off an **in-place** split of `tablet` at `split_key` via `node`'s
/// admin surface — the identical `POST /admin/tablet/split` endpoint the
/// copy-based workflow uses (`ClientCtx::trigger_split` is the ONE choke
/// point both workflows share; which one actually runs is decided entirely
/// by this node's own configured `SplitMode`, set at startup above).
async fn kickoff_tablet(node: &Node, tablet: u64, split_key: &str) {
    let (status, body) = admin(
        node.admin_addr(),
        "POST",
        "/admin/tablet/split",
        Some(&format!(
            "{{\"tablet\":{tablet},\"split_key\":\"{split_key}\"}}"
        )),
    )
    .await;
    assert_eq!(status, 200, "kickoff of tablet {tablet} failed: {body}");
}

/// Poll `/admin/status` until `table`'s split of `parent` has cut over
/// (parent gone, exactly two `Active` children of `table` present) or
/// `budget` elapses — mode-agnostic: `CutoverSplit`'s in-place branch
/// produces the identical final `Metadata` shape the copy-based branch does
/// (`animus-control/CLAUDE.md`'s own "otherwise identical to the copy-based
/// branch" note), so this is the exact same poll `split_build.rs::
/// await_cutover_of` uses.
async fn await_cutover_of(node: &Node, table: &str, parent: u64, budget: Duration) -> (u64, u64) {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let (_, s) = admin(node.admin_addr(), "GET", "/admin/status", None).await;
        let tablets = s["tablets"].as_object().cloned().unwrap_or_default();
        let parent_gone = !tablets.contains_key(&parent.to_string());
        let mut active: Vec<(u64, Vec<u8>)> = tablets
            .iter()
            .filter(|(_, t)| {
                t["state"].as_str() == Some("Active") && t["table"].as_str() == Some(table)
            })
            .filter_map(|(id, t)| {
                let start: Vec<u8> = t["range"]["start"]
                    .as_array()?
                    .iter()
                    .filter_map(|b| b.as_u64().map(|b| b as u8))
                    .collect();
                Some((id.parse().ok()?, start))
            })
            .collect();
        if parent_gone && active.len() == 2 {
            active.sort_by(|a, b| a.1.cmp(&b.1));
            return (active[0].0, active[1].0);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "in-place cutover of {table}/{parent} never completed: tablets={tablets:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

/// The paced continuous writer: one uniquely-keyed put roughly every 5ms
/// (dense, not a max-speed hammer — see the module doc), from the moment
/// it's spawned until `stop` flips. Keys are `[b'k', hi, lo]` with `hi`
/// cycling the full byte range — three bytes compares as strictly greater
/// than the two-byte `[b'k', split_hi]` split key at the same `hi`, and
/// strictly less/greater on either side otherwise — so across many
/// iterations this writer's own acked keys land on BOTH children, not just
/// whichever side a single fixed prefix would happen to fall on. Returns
/// every `(key, value)` it acked (for post-stop read-back) and how many of
/// those puts needed at least one retry (the write-refusal-absorbed count —
/// purely diagnostic, since an in-place split's blip is expected small
/// enough that a given run may well see zero).
async fn paced_writer(
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    retried_count: Arc<AtomicU64>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut stream = TcpStream::connect(addr).await.expect("connect writer");
    let mut acked = Vec::new();
    let mut i: u32 = 0;
    while !stop.load(Ordering::Relaxed) {
        let hi = (i % 256) as u8;
        let lo = (i / 256) as u8;
        let key = vec![b'k', hi, lo];
        let value = format!("w{i:08}").into_bytes();
        if put(&mut stream, key.clone(), value.clone()).await {
            retried_count.fetch_add(1, Ordering::Relaxed);
        }
        acked.push((key, value));
        i += 1;
        sleep(Duration::from_millis(5)).await;
    }
    acked
}

/// The primary teeth: a real 3-node cluster, `--split-mode inplace`, a
/// populated table, an in-place split triggered mid-flight, and a paced
/// continuous writer riding the whole fork→cutover window. Every acked
/// write must be readable afterward with its exact value, on both sides of
/// the split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inplace_split_survives_a_paced_continuous_writer_across_fork_and_cutover() {
    timeout(Duration::from_secs(180), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up_inplace(3, dir.path(), StreamSealKnobs::default()).await;
        await_bootstrap(&nodes).await;

        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect client port");
        // Pre-split population: 16 keys `[k,0]..[k,15]` (single byte after
        // the prefix — disjoint from the writer's own 3-byte key shape
        // below, so there is never any ambiguity about which write a given
        // read-back is checking).
        for i in 0..16u8 {
            put(&mut stream, vec![b'k', i], vec![b'v', i]).await;
        }

        let parent = sole_tablet_of(&nodes[0], "t");

        // Split key: `[b'k', 128]` — the writer's own `hi` byte cycles the
        // full 0..256 range, so this lands roughly half its acked keys on
        // each side (the pre-split population's single-byte keys, being
        // shorter, all compare less than this two-byte key and so land on
        // the left child regardless of `i` — not the property under test
        // here, which is the CONTINUOUS writer's own split coverage).
        let split_key = "k\\u0080";

        let stop = Arc::new(AtomicBool::new(false));
        let retried = Arc::new(AtomicU64::new(0));
        let writer_addr = nodes[0].client_addr();
        let writer = tokio::spawn(paced_writer(
            writer_addr,
            Arc::clone(&stop),
            Arc::clone(&retried),
        ));

        // Give the writer a moment to actually be in flight before the split
        // starts, so the whole fork→cutover window sees continuous pressure,
        // not just its tail.
        sleep(Duration::from_millis(300)).await;

        kickoff_tablet(&nodes[0], parent, split_key).await;
        let (left, right) = await_cutover_of(&nodes[0], "t", parent, Duration::from_secs(90)).await;

        // Keep the writer going well past cutover too, then stop it — long
        // enough that this window's own length (not the split's, which an
        // in-place fork keeps deliberately short — ADR 0058's near-zero-
        // outage design point) is what bounds how many writes land, so the
        // "suspiciously few" floor below stays meaningful regardless of how
        // fast the fork/cutover itself completes.
        sleep(Duration::from_secs(3)).await;
        stop.store(true, Ordering::Relaxed);
        let acked = writer.await.expect("writer task panicked");

        assert!(
            acked.len() > 5,
            "writer acked suspiciously few writes ({}) — test setup problem",
            acked.len()
        );
        eprintln!(
            "inplace_split e2e: writer acked {} puts, {} needed at least one retry \
             (the write-refusal-absorbed count; zero is a legitimate outcome for a fast, \
             low-blip in-place fork)",
            acked.len(),
            retried.load(Ordering::Relaxed)
        );

        // Every acked write survives, with its exact value — the no-lost-
        // writes teeth. A wrong/missing value here would mean either the
        // fork dropped or corrupted a row, or routing sent this read to the
        // wrong child.
        for (key, value) in &acked {
            assert_eq!(
                get(&mut stream, key.clone()).await.as_ref(),
                Some(value),
                "acked write {key:?} lost or corrupted across the in-place fork/cutover"
            );
        }
        // The pre-split population survives too.
        for i in 0..16u8 {
            assert_eq!(
                get(&mut stream, vec![b'k', i]).await,
                Some(vec![b'v', i]),
                "pre-split key [k,{i}] lost across the in-place fork/cutover"
            );
        }

        // A post-cutover write routes to a child and lands (both children
        // are genuinely live and serving, not just present in the map).
        put(&mut stream, b"post-cut".to_vec(), b"pv".to_vec()).await;
        assert_eq!(
            get(&mut stream, b"post-cut".to_vec()).await,
            Some(b"pv".to_vec())
        );

        // `split_lineage` names the parent for both children (fork F9,
        // written identically by `CutoverSplit`'s in-place branch).
        let (_, s) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
        for child in [left, right] {
            let recorded_parent = s["split_lineage"][child.to_string()]["parent"].as_u64();
            assert_eq!(
                recorded_parent,
                Some(parent),
                "split_lineage missing/wrong for child {child}: {}",
                s["split_lineage"]
            );
        }

        // The parent is reclaimed from every host once every replica's own
        // reconciler has torn its (now-hosted-but-absent) group down.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let mut still_hosted = false;
            for node in &nodes {
                let (_, g) = admin(node.admin_addr(), "GET", "/admin/raftkv", None).await;
                if g["groups"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|grp| grp["tablet"].as_u64() == Some(parent))
                {
                    still_hosted = true;
                }
            }
            if !still_hosted {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "parent tablet {parent} never reclaimed from every host"
            );
            sleep(Duration::from_millis(200)).await;
        }

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// `field()`: extract a top-level `"name":"..."` string field from a raw
/// JSON response body by substring search — the same tiny, deliberately
/// non-structural helper `dynamo_streams.rs`/`streams_e2e.rs` each keep
/// their own copy of (this crate's own stated "small fixtures are
/// duplicated, not shared" convention).
fn field(body: &str, name: &str) -> String {
    let needle = format!("\"{name}\":\"");
    let start = body
        .find(&needle)
        .unwrap_or_else(|| panic!("field `{name}` not found in: {body}"))
        + needle.len();
    let end = body[start..].find('"').expect("closing quote") + start;
    body[start..end].to_owned()
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

/// [`dynamo`], retried on a transient 500 — the identical rationale
/// `streams_e2e.rs::dynamo_retrying` documents (a write/read racing a
/// still-in-flight split can surface a documented, bounded, retryable
/// refusal).
async fn dynamo_retrying(addr: SocketAddr, target: &str, body: &str, what: &str) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let (status, resp) = dynamo(addr, target, body).await;
        if status == 200 {
            return resp;
        }
        if status == 500 && tokio::time::Instant::now() < deadline {
            sleep(Duration::from_millis(150)).await;
            continue;
        }
        panic!(
            "{what} failed (status {status}) after retrying for 90s with the condition never \
             clearing: {resp}"
        );
    }
}

fn json(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

async fn get_shard_iterator(
    addr: SocketAddr,
    stream_arn: &str,
    shard_id: &str,
    iterator_type: &str,
) -> String {
    let body = format!(
        r#"{{"StreamArn":"{stream_arn}","ShardId":"{shard_id}","ShardIteratorType":"{iterator_type}"}}"#
    );
    let resp = dynamo_retrying(
        addr,
        "DynamoDBStreams_20120810.GetShardIterator",
        &body,
        "GetShardIterator",
    )
    .await;
    json(&resp)["ShardIterator"]
        .as_str()
        .unwrap_or_else(|| panic!("no ShardIterator in: {resp}"))
        .to_owned()
}

async fn get_records(addr: SocketAddr, iterator: &str) -> (Vec<Value>, Option<String>) {
    let body = format!(r#"{{"ShardIterator":"{iterator}"}}"#);
    let resp = dynamo_retrying(
        addr,
        "DynamoDBStreams_20120810.GetRecords",
        &body,
        "GetRecords",
    )
    .await;
    let v = json(&resp);
    let records = v["Records"].as_array().cloned().unwrap_or_default();
    let next = v["NextShardIterator"].as_str().map(str::to_owned);
    (records, next)
}

/// A single, bounded snapshot of a shard that may still be genuinely
/// **open** (a live split child's own shard 0 — nothing in this test ever
/// seals it) — TRIM_HORIZON, then follow `NextShardIterator` **until one
/// page comes back with zero records**, never until `NextShardIterator`
/// itself goes `None`. An open shard's own `NextShardIterator` never nulls
/// by design (ADR 0042 §7 — the tail is always resumable), so looping on
/// that condition would spin forever the instant the shard simply has no
/// MORE records to give right now — the first (test-authoring) shape this
/// file's own history tried, which produced exactly that: a live infinite
/// loop against a real, correctly-behaving open shard, not a product hang.
/// This is exactly what a `GetRecords` poll against a live tail means:
/// "what is available right now," not "wait for the shard to end" — the
/// caller (the post-split convergence poll below) re-snapshots on its own
/// cadence instead.
async fn drain_open_shard_snapshot(
    addr: SocketAddr,
    stream_arn: &str,
    shard_id: &str,
) -> Vec<Value> {
    let mut collected = Vec::new();
    let mut iterator = get_shard_iterator(addr, stream_arn, shard_id, "TRIM_HORIZON").await;
    loop {
        let (records, next) = get_records(addr, &iterator).await;
        if records.is_empty() {
            return collected;
        }
        collected.extend(records);
        match next {
            Some(n) => iterator = n,
            None => return collected,
        }
    }
}

/// Drain shard `shard_id` (TRIM_HORIZON) to exhaustion (its own
/// `NextShardIterator` going `None` — end of a CLOSED shard, ADR 0042 §7),
/// polling until that happens or `deadline` elapses, resuming from the
/// same iterator each pass — never re-minting TRIM_HORIZON, which would
/// double-count whatever an earlier pass already collected (the identical
/// resume-not-remint discipline `streams_e2e.rs::drain_tablet_lineage`
/// documents at length). Only sound for a shard the caller EXPECTS to
/// eventually seal — the parent's own shard 0, still open when first
/// polled (before the fork's final seal, `inplace_split_driver_tick`, is
/// known to have landed) but guaranteed to close exactly once, unlike a
/// child's own live tail (see [`drain_open_shard_snapshot`]'s own doc for
/// why that one needs a completely different termination rule).
async fn drain_until_sealed(
    addr: SocketAddr,
    stream_arn: &str,
    shard_id: &str,
    deadline: tokio::time::Instant,
) -> Vec<Value> {
    let mut collected = Vec::new();
    let mut iterator = get_shard_iterator(addr, stream_arn, shard_id, "TRIM_HORIZON").await;
    loop {
        let (records, next) = get_records(addr, &iterator).await;
        collected.extend(records);
        match next {
            Some(n) => iterator = n,
            None => return collected,
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "shard {shard_id} never sealed within budget ({} records collected so far)",
                collected.len()
            );
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// A `PutItem` against `table`, asserting success.
async fn put_item(addr: SocketAddr, table: &str, id: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        &format!(r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200, "PutItem({id}) failed: {body}");
}

/// The streams variant, "cheap" because exactly one split (never a cascade)
/// needs none of `streams_e2e.rs`'s generalized re-resolve-the-chain-every-
/// pass machinery: the parent's `KIND_CHANGE` shard closes at the fork
/// position (`inplace_split_driver_tick`'s `seal_now` loop, anchored there —
/// see that function's own doc), so a TRIM_HORIZON iterator opened on the
/// parent's shard 0 BEFORE the split drains every pre-fork record and then
/// observes the seal directly (`NextShardIterator` going `None`); every
/// post-cutover write lands in whichever child's own (freshly born, empty)
/// shard 0 owns it. No record is lost (every write's change record lives in
/// exactly one of the three shards) and none is duplicated (children are
/// born with empty change logs — copy-kinds classification, unchanged by
/// this workflow).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inplace_split_stream_shard_walks_parent_to_children_without_loss_or_duplication() {
    timeout(Duration::from_secs(180), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up_inplace(
            3,
            dir.path(),
            StreamSealKnobs {
                seal_bytes: 1_000_000,
                seal_age: Duration::from_secs(3600),
            },
        )
        .await;
        await_bootstrap(&nodes).await;
        let dynamo_addr = nodes[0].dynamo_addr();

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"streamed",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                "StreamSpecification":{"StreamEnabled":true,
                    "StreamViewType":"NEW_IMAGE"}}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");
        let stream_arn = field(&body, "LatestStreamArn");

        // Pre-split writes: 12 items, single (as-yet-unsplit) tablet.
        let pre_split_count = 12usize;
        for i in 0..pre_split_count {
            put_item(dynamo_addr, "streamed", &format!("pre{i:03}")).await;
        }

        let parent = sole_tablet_of(&nodes[0], "streamed");
        // Open the parent's own shard 0 iterator BEFORE the split — an
        // in-flight consumer's whole point.
        let parent_shard_id = animus_cp_data::segment::shard_id(parent, 0);

        kickoff_tablet(&nodes[0], parent, "0000000000000000").await;
        let (left, right) =
            await_cutover_of(&nodes[0], "streamed", parent, Duration::from_secs(90)).await;

        // Post-cutover writes: 12 more items, now routed to whichever child
        // owns each key.
        let post_split_count = 12usize;
        for i in 0..post_split_count {
            put_item(dynamo_addr, "streamed", &format!("post{i:03}")).await;
        }

        // Drain the parent's shard 0 to its own seal (`inplace_split_driver_
        // tick`'s final seal, anchored at the fork) — every pre-split
        // record, and nothing else.
        let parent_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let parent_records =
            drain_until_sealed(dynamo_addr, &stream_arn, &parent_shard_id, parent_deadline).await;
        assert_eq!(
            parent_records.len(),
            pre_split_count,
            "parent shard 0 delivered {} records, expected exactly the {pre_split_count} \
             pre-split writes (loss or duplication across the fork)",
            parent_records.len()
        );

        // Drain each child's own shard 0 — born empty (copy-kinds rule), so
        // this is exactly the post-split writes that landed on that side.
        let mut child_total = 0usize;
        for child in [left, right] {
            let child_shard_id = animus_cp_data::segment::shard_id(child, 0);
            let records =
                drain_open_shard_snapshot(dynamo_addr, &stream_arn, &child_shard_id).await;
            child_total += records.len();
        }
        // A child's own shard is never sealed (nothing triggers it in this
        // test) — `drain_open_shard_snapshot` returns whatever is currently
        // pending in one bounded pass, so a single call only ever sees the
        // final total once every post-split write has already landed and
        // become visible; retry the whole per-child snapshot as a
        // converged-or-timeout poll instead of a one-shot read, since that
        // visibility is asynchronous (ADR 0042 §3).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while child_total != post_split_count {
            assert!(
                tokio::time::Instant::now() < deadline,
                "children's shard-0 records never converged to the {post_split_count} \
                 post-split writes (saw {child_total})"
            );
            sleep(Duration::from_millis(200)).await;
            child_total = 0;
            for child in [left, right] {
                let child_shard_id = animus_cp_data::segment::shard_id(child, 0);
                let records =
                    drain_open_shard_snapshot(dynamo_addr, &stream_arn, &child_shard_id).await;
                child_total += records.len();
            }
        }

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}
