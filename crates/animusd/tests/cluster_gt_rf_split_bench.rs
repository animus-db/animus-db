//! ADR 0062's own rung-7 as-built amendment flagged a structural gap in its
//! own bench: `inplace_split_bench.rs` runs 3 nodes at RF 3, so
//! `select_replicas`/`select_replicas_balanced` has no node outside the
//! parent's own current replica set to ever recruit — both a fork-first
//! child's placement-chosen "final home" and the parent's own current
//! replicas are, of structural necessity, the identical set at that scale.
//! That amendment says plainly: "Reproducing that win would need a bench
//! cluster wider than RF... out of scope for this rung." This file is that
//! bench.
//!
//! Bring-up mirrors `tests/split_placing_completion.rs`'s own recipe: a
//! 3-node cluster (RF 3, so `n0`/`n1`/`n2` are the only candidates and the
//! parent's replicas are provably `[n0, n1, n2]`) is GROWN by one more node
//! (`m0`) whose id sorts lexically **below** every original node's —
//! `select_replicas`/`animus_placement::choose` picks eligible candidates in
//! ascending `NodeId` order with no load-awareness, so a *fresh*
//! `select_replicas` computation over the grown 4-node candidate pool
//! prefers `m0` over one of the parent's original three. The split is
//! kicked off immediately after growth (well inside
//! `REBALANCE_EVERY_N_TICKS`'s ~4s cadence) so ordinary cluster-wide
//! rebalance never gets a chance to move the parent first — which would
//! make the scenario as vacuous as an already-satisfying fork. This is the
//! one genuine way to make a split's placement target differ from the
//! parent's current replicas without hand-rolling a load-imbalance policy
//! fixture, and it is the exact recipe `split_placing_completion.rs`'s own
//! e2e already validated for correctness; this file times it instead.
//!
//! Measures three clocks per run, with a paced continuous writer running
//! throughout (same retry-counting `put`/`get` shape as
//! `inplace_split_bench.rs`):
//!
//!   (a) split-request -> children Active (parent gone, cutover complete —
//!       "serving restored/confirmed" in the fork-first design, since a
//!       fork-first child is born `Active` immediately, with no separate
//!       build/freeze/tail phase to wait on)
//!   (b) split-request -> directed-Placing fully converged (every
//!       `split_placing[child]` entry either absent — ADR 0062 §2's
//!       "already satisfying: no entry" case, not expected here since the
//!       growth above deliberately forces a real move — or `done: true`,
//!       observed via `/admin/status`)
//!   (c) max write blip observed by the paced writer across the WHOLE
//!       window (kickoff through (b), not just through (a) — the honest
//!       "how long is the client-visible disruption" number for a design
//!       whose relief (a) and full convergence (b) are two different
//!       instants)
//!
//! Run explicitly: `cargo test -p animusd --test cluster_gt_rf_split_bench --
//! --ignored --nocapture`.
//!
//! See `docs/adr/0062-fork-first-split-directed-placing.md`'s own follow-up
//! amendment for the numbers this bench produced, run alongside the closest
//! equivalent bench on the pre-ADR-0062 baseline (`6d2777d`) — see that
//! amendment for why the baseline's own variant measures only two clocks,
//! not three: under the old F5-fused design, cutover-with-real-placement
//! IS full convergence, so there is no separate (b) to observe.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_env::NodeId;
use animusd::{
    ClientRequest, ClientResponse, ClusterConfig, Node, NodeStatus, RoleAddrs, SegmentStoreConfig,
    SplitMode, StorageBackend, StreamSealKnobs, read_frame, write_frame,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// [`inplace_split_bench.rs::bring_up_inplace`], unchanged.
async fn bring_up_inplace(n: usize, dir: &Path) -> (Vec<Node>, ClusterConfig) {
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
                StreamSealKnobs::default(),
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

/// [`inplace_split_bench.rs::put_in_counting`], unchanged.
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

/// [`inplace_split_bench.rs::put_in`], unchanged.
async fn put_in(stream: &mut TcpStream, table: &str, key: Vec<u8>, value: Vec<u8>) {
    put_in_counting(stream, table, key, value).await;
}

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

/// [`split_placing_completion.rs::join_extra`], unchanged: joins `ids.len()`
/// extra combined nodes with EXPLICIT (not auto-assigned) ids, so a
/// lexically-lower id like `m0` is achievable — `tests/support::
/// grow_deadline` always assigns ids that sort ABOVE every original one,
/// which would never force `select_replicas` to prefer the grown node.
async fn join_extra(core_intra: &[SocketAddr], ids: &[&str], dir: &Path) -> Vec<Node> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let addrs = support::free_addrs(ids.len() * 6);
        let mut nodes = Vec::new();
        let mut failed = false;
        for (i, id) in ids.iter().enumerate() {
            let a = &addrs[6 * i..6 * i + 6];
            let role_addrs = RoleAddrs {
                id: NodeId::propose(id).expect("valid test id"),
                role: animusd::config::NodeRole::Both,
                internal: a[0],
                client: a[1],
                dynamo: a[2],
                admin: a[3],
                intra: a[4],
                console: a[5],
                advertise_host: None,
            };
            match animusd::run_node_join(
                core_intra.iter().map(ToString::to_string).collect(),
                Some(NodeId::propose(id).expect("valid test id")),
                role_addrs,
                &dir.join(format!("join-{id}")),
                StorageBackend::default(),
                std::collections::BTreeMap::new(),
            )
            .await
            {
                Ok(node) => nodes.push(node),
                Err(e) => {
                    eprintln!("DIAG join_extra: run_node_join({id}) failed: {e}");
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
        assert!(
            tokio::time::Instant::now() < deadline,
            "could not join extra nodes {ids:?} after retries"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

/// [`split_placing_completion.rs::await_all_active`], unchanged.
async fn await_all_active(nodes: &[Node], ids: &[&str], budget: Duration) {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let all_ready = nodes.iter().all(|n| {
            let meta = n.metadata();
            ids.iter().all(|id| {
                let nid = NodeId::propose(id).expect("valid test id");
                meta.members
                    .get(&nid)
                    .is_some_and(|m| m.status == NodeStatus::Active)
            })
        });
        if all_ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "grown members {ids:?} never went Active everywhere"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

/// Poll `/admin/status` until `table`'s split of `parent` has cut over
/// (parent gone, exactly two `Active` children of `table` present).
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

/// `split_placing[tablet]`'s own `done` flag from a parsed `/admin/status`
/// body — `None` if no entry exists (ADR 0062 §2's "already satisfying: no
/// entry" case, which converges vacuously).
fn split_placing_done(status: &Value, tablet: u64) -> Option<bool> {
    status
        .get("split_placing")?
        .get(tablet.to_string())?
        .get("done")?
        .as_bool()
}

/// `split_placing[tablet].target`, as plain id strings, for reporting only.
fn split_placing_target(status: &Value, tablet: u64) -> Option<Vec<String>> {
    status
        .get("split_placing")?
        .get(tablet.to_string())?
        .get("target")?
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect()
        })
}

/// The cluster>RF bench itself (ADR 0062's own rung-7 amendment names this
/// exact gap). See the module doc for the full design.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "bench — run explicitly with --ignored --nocapture"]
async fn bench_cluster_gt_rf_split_placing_fork_first() {
    const N: usize = 2_000;
    timeout(Duration::from_secs(900), async {
        let dir = tempfile::tempdir().unwrap();
        let (mut nodes, config) = bring_up_inplace(3, dir.path()).await;
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
        let (_, status0) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
        eprintln!(
            "cluster>RF bench: parent {parent} initial replicas = {:?}",
            status0["tablets"][parent.to_string()]["replicas"]
        );

        // Grow by one lower-sorting node — forces a real placement move
        // (see module doc). Kick off the split immediately after, well
        // inside REBALANCE_EVERY_N_TICKS's ~4s cadence.
        let core_intra: Vec<SocketAddr> = config.nodes.iter().map(|a| a.intra).collect();
        let extra = join_extra(&core_intra, &["m0"], dir.path()).await;
        await_all_active(&nodes, &["m0"], Duration::from_secs(20)).await;
        nodes.extend(extra);

        let t_kickoff = std::time::Instant::now();
        kickoff_tablet(&nodes[0], parent, "k\\u0004").await;

        let mut build_puts = Vec::new();
        let mut total_retries: u64 = 0;
        let mut max_retries_single_put: u32 = 0;

        // Phase (a): kickoff -> children Active (cutover).
        let (left, right) = loop {
            for _ in 0..20usize {
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
                break await_cutover_of(&nodes[0], "bench", parent, Duration::from_secs(120)).await;
            }
            assert!(
                t_kickoff.elapsed() < Duration::from_secs(300),
                "split never cut over"
            );
        };
        let t_cutover = t_kickoff.elapsed();

        let (_, status_at_cutover) =
            admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
        for child in [left, right] {
            eprintln!(
                "cluster>RF bench: child {child} split_placing target = {:?}",
                split_placing_target(&status_at_cutover, child)
            );
        }

        // Phase (b): cutover -> directed-Placing fully converged (every
        // child's split_placing entry either absent or done). The writer
        // keeps sampling throughout, so (c)'s max blip covers this window
        // too. Bounded but NON-panicking: on a heavily contended host,
        // real convergence latency is itself part of the honest number to
        // report, so a run that exceeds the budget records `None` rather
        // than aborting the whole bench and losing (a)/(c).
        const PLACING_BUDGET: Duration = Duration::from_secs(240);
        let placing_deadline = t_kickoff.elapsed() + PLACING_BUDGET;
        let mut t_placed: Option<Duration> = None;
        loop {
            let (_, st) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
            let converged = [left, right]
                .iter()
                .all(|&c| split_placing_done(&st, c).unwrap_or(true));
            if converged {
                t_placed = Some(t_kickoff.elapsed());
                break;
            }
            if t_kickoff.elapsed() >= placing_deadline {
                eprintln!(
                    "cluster>RF bench: WARNING directed-Placing did not converge within \
                     {PLACING_BUDGET:?} of kickoff; last status: {st}"
                );
                break;
            }
            for _ in 0..20usize {
                let wkey = vec![b'w', (build_puts.len() % 256) as u8];
                let t0 = std::time::Instant::now();
                let attempts = put_in_counting(&mut s, "bench", wkey, vec![b'v']).await;
                build_puts.push(t0.elapsed());
                let retries = attempts.saturating_sub(1);
                total_retries += u64::from(retries);
                max_retries_single_put = max_retries_single_put.max(retries);
            }
        }

        let stats = |mut v: Vec<Duration>| {
            v.sort();
            let med = v[v.len() / 2];
            let p99 = v[(v.len() * 99) / 100];
            let max = *v.last().unwrap();
            (med, p99, max)
        };
        let (p_med, p_p99, p_max) = stats(build_puts);
        eprintln!(
            "cluster>RF split bench (N={N} rows, 256B values, 3->4 nodes RF=3, fork-first, \
             children {left}/{right}):"
        );
        eprintln!("  (a) kickoff -> children Active (cutover):      {t_cutover:?}");
        match t_placed {
            Some(d) => eprintln!("  (b) kickoff -> directed-Placing fully converged: {d:?}"),
            None => eprintln!(
                "  (b) kickoff -> directed-Placing fully converged: DID NOT CONVERGE within budget"
            ),
        }
        eprintln!("  serve PUT throughout:  median {p_med:?}  p99 {p_p99:?}");
        eprintln!("  (c) write blip (max PUT, whole window a..b): {p_max:?}");
        eprintln!(
            "  write blip retry shape: {total_retries} total retries absorbed, \
             worst single put retried {max_retries_single_put} time(s)"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("bench timed out");
}
