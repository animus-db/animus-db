//! ADR 0058 Train 2's own committed bench — the in-place sibling of
//! `split_build.rs::bench_split_build_serve_latency_and_cutover_blip`.
//!
//! The ADR's own testing plan says it plainly: "the bench that proved the
//! [convergence-predicate starvation] problem for ADR 0050's design is
//! exactly the bench that should be pointed at this one before it is
//! trusted." This file is that pointing — same continuous-writer shape,
//! same workload parameters (2,000 rows, 256-byte values, 3 nodes), so the
//! two files' numbers are directly comparable, run against a `--split-mode
//! inplace` cluster instead of the default copy-based one.
//!
//! Deliberately a **separate file**, not an addition to `split_build.rs`:
//! the two benches need different bring-up (`SplitMode::InPlace` threaded
//! through `run_node_with_streams_quiesce_and_split_mode`, mirroring
//! `inplace_split_e2e.rs::bring_up_inplace`) and a small retry-counting
//! variant of `put_in`, and this crate's own stated convention is that a
//! small fixture is duplicated per test binary rather than shared (see
//! `animusd/CLAUDE.md`'s note on `field()`/`dynamo()`-shaped duplication) —
//! copying the handful of helpers needed here keeps the well-understood
//! copy-based bench file completely untouched.
//!
//! Run explicitly: `cargo test -p animusd --test inplace_split_bench --
//! --ignored --nocapture`.

use std::net::SocketAddr;
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

/// Bring up an `n`-node cluster, one process-shaped node per index, every
/// node started with `SplitMode::InPlace` — otherwise byte-for-byte the same
/// bring-up shape (including quiescence disabled, `Duration::ZERO`) as
/// `split_build.rs::bring_up`'s copy-mode cluster, so the two benches run
/// under identical conditions apart from the one variable under test.
async fn bring_up_inplace(n: usize, dir: &std::path::Path) -> (Vec<Node>, ClusterConfig) {
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
                StreamSealKnobs::default(),
                SegmentStoreConfig::default(),
                animusd::DEFAULT_STREAM_RETENTION,
                Duration::ZERO,
                SplitMode::InPlace,
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

/// [`split_build.rs::put_in`]'s twin, but reporting how many frames it took
/// to land (1 = no retry) instead of discarding that count — the "retry
/// counts" half of the ADR's own requested blip-shape measurement. Same
/// bounded-retry-on-any-error contract otherwise.
async fn put_in_counting(stream: &mut TcpStream, table: &str, key: Vec<u8>, value: Vec<u8>) -> u32 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        write_frame(
            stream,
            &ClientRequest::Put {
                key: key.clone(),
                value: value.clone(),
                table: table.to_string(),
            },
        )
        .await
        .expect("send frame");
        match read_frame(stream).await.expect("read").expect("reply") {
            ClientResponse::PutOk => return attempts,
            ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(150)).await;
            }
            other => panic!("put({table}) failed: {other:?}"),
        }
    }
}

/// [`split_build.rs::put_in`], unchanged — used only for the pre-split
/// population and idle-baseline setup here, where retry counting doesn't
/// matter.
async fn put_in(stream: &mut TcpStream, table: &str, key: Vec<u8>, value: Vec<u8>) {
    put_in_counting(stream, table, key, value).await;
}

/// [`split_build.rs::get_in`], identical.
async fn get_in(stream: &mut TcpStream, table: &str, key: Vec<u8>) -> Option<Vec<u8>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        write_frame(
            stream,
            &ClientRequest::Get {
                key: key.clone(),
                table: table.to_string(),
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
            other => panic!("get({table}) failed: {other:?}"),
        }
    }
}

/// [`split_build.rs::sole_tablet_of`], identical.
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

/// [`split_build.rs::kickoff_tablet`], identical — the identical `POST
/// /admin/tablet/split` endpoint; which workflow actually runs is decided
/// entirely by this cluster's configured `SplitMode` (in-place, above).
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

/// [`split_build.rs::await_cutover_of`], identical — `CutoverSplit`'s
/// in-place branch produces the same final `Metadata` shape the copy-based
/// branch does, so the same poll applies unmodified.
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

/// ADR 0058's own committed bench (Testing plan, "Bench, following the
/// rung-8 lesson directly"). Reports: (i) fork-to-children-Active wall clock
/// (== total split wall clock — the in-place workflow has no separate
/// freeze/tail/cutover phase to add on top of the fork itself), (ii) the
/// parent's sequential serve latency during the live fork vs. an idle
/// baseline (median + p99), (iii) the write blip as observed by a
/// continuously-retrying client — duration/shape (median, p99, max) AND
/// retry counts (total retries absorbed, and the worst single put's own
/// retry count), the two figures `split_build.rs`'s own bench didn't need to
/// separate out because its multi-second freeze window made "did this put
/// retry at all" a foregone conclusion. Workload parameters (`N`, value
/// size, node count, split key, sampling cadence) are IDENTICAL to
/// `split_build.rs::bench_split_build_serve_latency_and_cutover_blip` so the
/// two files' numbers are directly comparable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "bench — run explicitly with --ignored --nocapture"]
async fn bench_inplace_split_serve_latency_and_cutover_blip() {
    const N: usize = 2_000;
    timeout(Duration::from_secs(600), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up_inplace(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        let mut s = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect");
        let filler = vec![b'x'; 256];
        for i in 0..N {
            let key = vec![b'k', (i / 256) as u8, (i % 256) as u8];
            put_in(&mut s, "bench", key, filler.clone()).await;
        }
        let parent = sole_tablet_of(&nodes[0], "bench");

        // (ii) idle baseline: 200 sequential linearizable gets.
        let mut idle = Vec::with_capacity(200);
        for i in 0..200usize {
            let key = vec![b'k', (i / 256) as u8, (i % 256) as u8];
            let t0 = std::time::Instant::now();
            let got = get_in(&mut s, "bench", key).await;
            idle.push(t0.elapsed());
            assert!(got.is_some());
        }

        // (i)+(iii): kick off, then sample alternating get/put until the
        // in-place cutover completes. A put's own attempt count (1 = no
        // retry) is the blip's retry-count half.
        let t_kickoff = std::time::Instant::now();
        kickoff_tablet(&nodes[0], parent, "k\\u0004").await;
        let mut build_gets = Vec::new();
        let mut build_puts = Vec::new();
        let mut total_retries: u64 = 0;
        let mut max_retries_single_put: u32 = 0;
        let (left, right) = loop {
            for i in 0..20usize {
                let key = vec![b'k', (i / 256) as u8, (i % 256) as u8];
                let t0 = std::time::Instant::now();
                let got = get_in(&mut s, "bench", key).await;
                build_gets.push(t0.elapsed());
                assert!(got.is_some());
                let wkey = vec![b'w', (build_puts.len() % 256) as u8];
                let t0 = std::time::Instant::now();
                let attempts = put_in_counting(&mut s, "bench", wkey, vec![b'v']).await;
                build_puts.push(t0.elapsed());
                let retries = attempts.saturating_sub(1);
                total_retries += u64::from(retries);
                max_retries_single_put = max_retries_single_put.max(retries);
            }
            let (_, st) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
            let tablets = st["tablets"].as_object().cloned().unwrap_or_default();
            if !tablets.contains_key(&parent.to_string()) {
                break await_cutover_of(&nodes[0], "bench", parent, Duration::from_secs(30)).await;
            }
            assert!(
                t_kickoff.elapsed() < Duration::from_secs(300),
                "in-place split never completed"
            );
        };
        let split_wall_clock = t_kickoff.elapsed();

        let stats = |mut v: Vec<Duration>| {
            v.sort();
            let med = v[v.len() / 2];
            let p99 = v[(v.len() * 99) / 100];
            let max = *v.last().unwrap();
            (med, p99, max)
        };
        let (i_med, i_p99, _) = stats(idle);
        let (g_med, g_p99, _) = stats(build_gets);
        let (p_med, p_p99, p_max) = stats(build_puts);
        eprintln!(
            "in-place split bench (N={N} rows, 256B values, 3 nodes, children {left}/{right}):"
        );
        eprintln!(
            "  fork-to-children-Active wall clock (== total split wall clock): {split_wall_clock:?}  ({:.0} rows/s)",
            N as f64 / split_wall_clock.as_secs_f64()
        );
        eprintln!("  serve GET idle:   median {i_med:?}  p99 {i_p99:?}");
        eprintln!("  serve GET build:  median {g_med:?}  p99 {g_p99:?}");
        eprintln!("  serve PUT build:  median {p_med:?}  p99 {p_p99:?}");
        eprintln!("  write blip (max PUT incl. fork/cutover window): {p_max:?}");
        eprintln!(
            "  write blip retry shape: {total_retries} total retries absorbed, \
             worst single put retried {max_retries_single_put} time(s)"
        );
        assert!(
            p_max < Duration::from_secs(2),
            "in-place freeze-free write blip materially over the F8 sub-second contract this \
             workflow is supposed to beat: {p_max:?}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("bench timed out");
}
