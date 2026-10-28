//! ADR 0058 Train 2's own committed bench — the in-place sibling of
//! `split_build.rs::bench_split_build_serve_latency_and_cutover_blip`.
//!
//! The ADR's own testing plan says it plainly: "the bench that proved the
//! [convergence-predicate starvation] problem for ADR 0050's design is
//! exactly the bench that should be pointed at this one before it is
//! trusted." This file is that pointing — same continuous-writer shape,
//! same workload parameters (2,000 rows, 256-byte values, 3 nodes), so the
//! two files' numbers are directly comparable, against the (now sole)
//! in-place split workflow — the copy-based workflow and its `--split-mode`
//! flag were deleted in the copy-split-deletion endgame's Layer B1
//! (`docs/adr/0058-*.md` rung 4).
//!
//! Deliberately a **separate file**, not an addition to `split_build.rs`
//! (itself deleted in Layer A): the two benches needed different bring-up
//! (mirroring `inplace_split_e2e.rs::bring_up_inplace`) and a small retry-counting
//! variant of `put_in`, and this crate's own stated convention is that a
//! small fixture is duplicated per test binary rather than shared (see
//! `animusd/CLAUDE.md`'s note on `field()`/`dynamo()`-shaped duplication) —
//! copying the handful of helpers needed here keeps the well-understood
//! copy-based bench file completely untouched.
//!
//! Run explicitly: `cargo test -p animusd --test inplace_split_bench --
//! --ignored --nocapture`.

use std::time::Duration;

use animusd::{
    ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, SegmentStoreConfig,
    StorageBackend, StreamSealKnobs, read_frame, write_frame,
};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;
use support::{admin, kickoff_tablet, sole_tablet_of};

/// Bring up an `n`-node cluster, one process-shaped node per index —
/// byte-for-byte the same bring-up shape (including quiescence disabled,
/// `Duration::ZERO`) the deleted `split_build.rs::bring_up`'s copy-mode
/// cluster used, minus the now-deleted `SplitMode` selection (every split
/// is in-place unconditionally since Layer B1).
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
                advertise_host: None,
                tls: None,
                encryption_key_path: None,
                labels: Default::default(),
                overload: None,
            })
            .collect();
        let config = ClusterConfig {
            version: animusd::config::CLUSTER_CONFIG_VERSION,
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            match animusd::run_node_with_streams_quiesce_and_backup_store(
                &config,
                i,
                dir.join(format!("node-{attempt}-{i}")),
                StorageBackend::default(),
                Duration::from_secs(600),
                StreamSealKnobs::default(),
                SegmentStoreConfig::default(),
                animusd::DEFAULT_STREAM_RETENTION,
                Duration::ZERO,
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
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up_inplace(3, dir.path()).await;
        support::await_bootstrap(&nodes).await;

        let mut s = animusd::connect_client(nodes[0].client_addr())
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

/// `(utime + stime)` clock ticks of this process (Linux), for the storm's CPU
/// cost; `0` elsewhere.
fn self_cpu_ticks() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let rest = stat.rsplit_once(')').map_or("", |(_, r)| r);
    let f: Vec<&str> = rest.split_whitespace().collect();
    let num = |i: usize| f.get(i).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
    num(11) + num(12)
}

/// ADR 0079 step 1 calibration (#1295): a scoped `ProdEnv` split **storm**.
/// `ANIMUS_SPLIT_STORM_TABLES` (default 4) single-tablet tables, each with
/// `ANIMUS_SPLIT_STORM_ROWS` (2000) rows of `ANIMUS_SPLIT_STORM_VALUE_BYTES`
/// (256), on a 3-node LSM cluster in one process (4 worker threads); every
/// split is kicked off back to back, then a separate `probe` table is
/// read/written continuously until every parent is gone. Reports per-split
/// wall clock, total process CPU, and the probe table's get/put latency and
/// retries during the storm. Run: `cargo test -p animusd --release --test
/// inplace_split_bench bench_inplace_split_storm_cost -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "bench — run explicitly with --ignored --nocapture"]
async fn bench_inplace_split_storm_cost() {
    let knob = |k: &str, d: usize| -> usize {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    };
    let tables = knob("ANIMUS_SPLIT_STORM_TABLES", 4);
    let rows = knob("ANIMUS_SPLIT_STORM_ROWS", 2_000);
    let vb = knob("ANIMUS_SPLIT_STORM_VALUE_BYTES", 256);
    timeout(Duration::from_secs(1500), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up_inplace(3, dir.path()).await;
        support::await_bootstrap(&nodes).await;
        let mut s = animusd::connect_client(nodes[0].client_addr())
            .await
            .expect("connect");
        let filler = vec![b'x'; vb];
        let names: Vec<String> = (0..tables).map(|i| format!("storm{i}")).collect();
        for name in &names {
            for i in 0..rows {
                let key = vec![b'k', (i / 256) as u8, (i % 256) as u8];
                put_in(&mut s, name, key, filler.clone()).await;
            }
        }
        for i in 0..200usize {
            put_in(&mut s, "probe", vec![b'k', 0, i as u8], vec![b'p']).await;
        }
        let parents: Vec<u64> = names.iter().map(|n| sole_tablet_of(&nodes[0], n)).collect();

        let (cpu0, t0) = (self_cpu_ticks(), std::time::Instant::now());
        for p in &parents {
            kickoff_tablet(&nodes[0], *p, "k\\u0004").await;
        }
        let mut done_at: Vec<Option<Duration>> = vec![None; parents.len()];
        let (mut gets, mut puts) = (Vec::new(), Vec::new());
        let (mut retries, mut n) = (0u64, 0usize);
        while done_at.iter().any(Option::is_none) {
            let t = std::time::Instant::now();
            let _ = get_in(&mut s, "probe", vec![b'k', 0, (n % 200) as u8]).await;
            gets.push(t.elapsed());
            let t = std::time::Instant::now();
            let a = put_in_counting(&mut s, "probe", vec![b'w', (n % 256) as u8], vec![b'v']).await;
            puts.push(t.elapsed());
            retries += u64::from(a.saturating_sub(1));
            n += 1;
            let (_, st) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
            let live = st["tablets"].as_object().cloned().unwrap_or_default();
            for (i, p) in parents.iter().enumerate() {
                if done_at[i].is_none() && !live.contains_key(&p.to_string()) {
                    done_at[i] = Some(t0.elapsed());
                }
            }
            assert!(
                t0.elapsed() < Duration::from_secs(1200),
                "split storm never completed: {done_at:?}"
            );
        }
        let (total, cpu) = (t0.elapsed(), self_cpu_ticks() - cpu0);
        for (name, p) in names.iter().zip(&parents) {
            await_cutover_of(&nodes[0], name, *p, Duration::from_secs(60)).await;
        }
        let pct = |mut v: Vec<Duration>, q: usize| {
            v.sort();
            v[(v.len() * q / 100).min(v.len() - 1)]
        };
        let mut ds: Vec<Duration> = done_at.iter().flatten().copied().collect();
        ds.sort();
        eprintln!(
            "SPLIT-STORM tables={tables} rows={rows} value_bytes={vb} total_s={:.1} cpu_cores={:.2} \
             first_done_s={:.1} median_done_s={:.1} last_done_s={:.1} probe_ops={n} probe_retries={retries} \
             get_p50={:?} get_p99={:?} put_p50={:?} put_p99={:?} put_max={:?}",
            total.as_secs_f64(),
            cpu as f64 / 100.0 / total.as_secs_f64(),
            ds[0].as_secs_f64(),
            ds[ds.len() / 2].as_secs_f64(),
            ds[ds.len() - 1].as_secs_f64(),
            pct(gets.clone(), 50),
            pct(gets, 99),
            pct(puts.clone(), 50),
            pct(puts.clone(), 99),
            pct(puts, 100),
        );
        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("split storm bench timed out");
}
