//! ADR 0050 Train B rungs 4+5 — the copy-based split workflow, end to end
//! over a real 3-node cluster: a populated table's `BeginSplit` kicks off
//! the copy, the parent keeps serving while writes AND transactions race
//! the build, the driver freezes the parent at convergence, ships the
//! final image, and proposes `CutoverSplit` — children go `Active`, the
//! parent is removed and reclaimed everywhere, and no acked write is ever
//! lost across the flip (a stale-routed write gets the frozen refusal and
//! its retry lands on a child). A parent-leader kill AFTER the freeze
//! proves the endgame resumes idempotently on the new leader (the freeze
//! is engine-durable).
//!
//! These are rung 5's behavioral red→green teeth: on rung 4's tip the
//! workflow parks at convergence forever, so every cutover assertion below
//! times out red.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, read_frame};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Bring up an `n`-node cluster, one process per node — the same
/// port-TOCTOU-retrying shape as `split_lifecycle.rs`'s copy.
async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, animusd::ClusterConfig) {
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
            return (nodes, config);
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

/// A put with a bounded retry on ANY error reply: a put is idempotent, and
/// both early-cluster transients AND the rung-5 freeze→cutover blip (the
/// documented retryable refusal) surface as retryable one-off errors. Every
/// put this test ever acks must be readable at the end — that is the
/// no-lost-writes teeth.
async fn put(stream: &mut TcpStream, key: Vec<u8>, value: Vec<u8>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        animusd::write_frame(
            stream,
            &ClientRequest::Put {
                key: key.clone(),
                value: value.clone(),
                table: "t".to_string(),
            },
        )
        .await
        .expect("send frame");
        let reply = read_frame(stream)
            .await
            .expect("read reply")
            .expect("a reply");
        match reply {
            ClientResponse::PutOk => return,
            ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
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
        animusd::write_frame(
            stream,
            &ClientRequest::Get {
                key: key.clone(),
                table: "t".to_string(),
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

/// Every live node's `/admin/raftkv` group entries, flattened.
async fn all_groups(nodes: &[Node], dead: &[usize]) -> Vec<Value> {
    let mut out = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        if dead.contains(&i) {
            continue;
        }
        let (_, view) = admin(node.admin_addr(), "GET", "/admin/raftkv", None).await;
        if let Some(groups) = view["groups"].as_array() {
            out.extend(groups.iter().cloned());
        }
    }
    out
}

/// Kick off the split of tablet 1 at `[k,4]` from a non-control-leader
/// node (the relay-path regression rides along for free).
async fn kickoff(nodes: &[Node]) {
    let follower = nodes
        .iter()
        .position(|n| !n.is_control_leader())
        .expect("a 3-node cluster has a non-leader");
    let (status, body) = admin(
        nodes[follower].admin_addr(),
        "POST",
        "/admin/tablet/split",
        Some("{\"tablet\":1,\"split_key\":\"k\\u0004\"}"),
    )
    .await;
    assert_eq!(status, 200, "kickoff failed: {body}");
}

/// Poll `/admin/status` on the given node until the parent (tablet 1) is
/// GONE from the map and exactly two `Active` children of table `t` cover
/// its range — the cutover-complete signal. Returns the children's ids
/// (left first).
async fn await_cutover(node: &Node, budget: Duration) -> (u64, u64) {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let (_, s) = admin(node.admin_addr(), "GET", "/admin/status", None).await;
        let tablets = s["tablets"].as_object().cloned().unwrap_or_default();
        let parent_gone = !tablets.contains_key("1");
        let mut active: Vec<(u64, Vec<u8>)> = tablets
            .iter()
            .filter(|(_, t)| {
                t["state"].as_str() == Some("Active") && t["table"].as_str() == Some("t")
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
            // Compare actual BYTE arrays (a JSON-stringified sort inverts).
            active.sort_by(|a, b| a.1.cmp(&b.1));
            return (active[0].0, active[1].0);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cutover never completed: parent_gone={parent_gone}, tablets={tablets:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

/// The full workflow: populate, kick off, race plain writes AND
/// transactions against the build/freeze/cutover, then assert — children
/// `Active` at exactly the partitioned totals, parent gone from metadata
/// and reclaimed from every host, every acked write (transactional
/// included) readable, `split_lineage` recorded, and a post-cutover write
/// landing on a child.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_split_workflow_cutover_completes_with_no_lost_writes() {
    timeout(Duration::from_secs(180), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        // Populate: 8 keys `[k,0]..[k,7]` — the split key `[k,4]` puts 4 on
        // each side.
        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect client port");
        for i in 0..8u8 {
            put(&mut stream, vec![b'k', i], vec![b'v', i]).await;
        }

        kickoff(&nodes).await;

        // Race the workflow from every node: 8 plain puts (`[k,i,!]` sorts
        // beside `[k,i]`, 4 per side) plus 4 transactions with 8-byte keys
        // (`[k,i,t,...]`), 2 per side — each acked exactly once, some
        // possibly straddling the freeze window (the retry loops absorb
        // the documented blip).
        let mut racing_streams = Vec::new();
        for node in &nodes {
            racing_streams.push(
                TcpStream::connect(node.client_addr())
                    .await
                    .expect("connect racing client"),
            );
        }
        for i in 0..8u8 {
            let s = racing_streams.len();
            put(
                &mut racing_streams[i as usize % s],
                vec![b'k', i, b'!'],
                b"racing".to_vec(),
            )
            .await;
        }
        for i in 0..4u8 {
            // One single-table txn per key; anchor = tablet 1 (or a child,
            // post-cutover). Keys `[k, 2*i, t, 0, 0, 0, 0, i]` are 8 bytes.
            let key = vec![b'k', 2 * i, b't', 0, 0, 0, 0, i];
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                let s = &mut racing_streams[i as usize % 3];
                animusd::write_frame(
                    s,
                    &ClientRequest::Txn {
                        writes: vec![animusd::TxnTableWrite::plain(
                            "t".to_string(),
                            key.clone(),
                            Some(b"txn-racing".to_vec()),
                        )],
                        preconditions: vec![],
                        write_conditions: vec![],
                    },
                )
                .await
                .expect("send txn");
                match read_frame(s).await.expect("read").expect("reply") {
                    ClientResponse::TxnCommitted { .. } => break,
                    ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                        sleep(Duration::from_millis(150)).await;
                    }
                    other => panic!("racing txn failed hard: {other:?}"),
                }
            }
        }

        // The workflow runs to cutover on its own (rung 5's teeth — rung 4
        // parked forever here).
        let (left, right) = await_cutover(&nodes[0], Duration::from_secs(60)).await;

        // Children serve exactly the partitioned totals: 8+8 puts and 4 txn
        // keys = 10 left / 10 right once every resolve has landed. Assert
        // via read-back of EVERY acked key (the loss check), not counts —
        // counts include txn-record rows by design.
        for i in 0..8u8 {
            assert_eq!(
                get(&mut stream, vec![b'k', i]).await,
                Some(vec![b'v', i]),
                "pre-split key [k,{i}] lost across cutover"
            );
            assert_eq!(
                get(&mut stream, vec![b'k', i, b'!']).await,
                Some(b"racing".to_vec()),
                "racing key [k,{i},!] lost across cutover"
            );
        }
        for i in 0..4u8 {
            let key = vec![b'k', 2 * i, b't', 0, 0, 0, 0, i];
            assert_eq!(
                get(&mut stream, key.clone()).await,
                Some(b"txn-racing".to_vec()),
                "acked transactional write {key:?} lost across cutover (fork F7)"
            );
        }

        // A post-cutover write routes to a child and lands.
        put(&mut stream, b"post-cut".to_vec(), b"pv".to_vec()).await;
        assert_eq!(
            get(&mut stream, b"post-cut".to_vec()).await,
            Some(b"pv".to_vec())
        );

        // `split_lineage` names the parent for both children (fork F9).
        let (_, s) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
        for child in [left, right] {
            let parent = s["split_lineage"][child.to_string()]["parent"].as_u64();
            assert_eq!(
                parent,
                Some(1),
                "split_lineage missing/wrong for child {child}: {}",
                s["split_lineage"]
            );
        }

        // The parent is reclaimed from every host (hosted-but-absent →
        // existing Reclaim; poll to convergence).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let groups = all_groups(&nodes, &[]).await;
            if !groups.iter().any(|g| g["tablet"].as_u64() == Some(1)) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "parent group never reclaimed: {groups:?}"
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

/// Kill the node leading the parent AFTER the freeze has been observed
/// (`split_phase` past `"build"`): the freeze is engine-durable, so the
/// re-led driver resumes the endgame idempotently and the survivors
/// complete cutover with every acked write intact. If the workflow races
/// past cutover before a phase is ever observed (a fast box), the kill
/// degrades to a post-cutover leader kill — completion + no-loss still
/// asserted, and the run says which path it took.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn driver_kill_after_freeze_resumes_and_completes() {
    timeout(Duration::from_secs(180), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect client port");
        for i in 0..30u8 {
            put(&mut stream, vec![b'k', i % 8, b'p', i], vec![b'v', i]).await;
        }

        kickoff(&nodes).await;

        // Watch every node's /admin/raftkv for the parent leader's
        // split_phase leaving "build" (freeze proposed/applied), or the
        // parent vanishing (already cut over).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut kill_target: Option<usize> = None;
        loop {
            let (_, st) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
            let parent_gone = !st["tablets"]
                .as_object()
                .is_some_and(|t| t.contains_key("1"));
            if parent_gone {
                break; // fast path: cutover won the race
            }
            for (i, node) in nodes.iter().enumerate() {
                let (_, view) = admin(node.admin_addr(), "GET", "/admin/raftkv", None).await;
                let frozen_leader = view["groups"].as_array().is_some_and(|gs| {
                    gs.iter().any(|g| {
                        g["tablet"].as_u64() == Some(1)
                            && g["is_leader"].as_bool() == Some(true)
                            && g["split_phase"].as_str().is_some_and(|p| p != "build")
                    })
                });
                if frozen_leader {
                    kill_target = Some(i);
                    break;
                }
            }
            if kill_target.is_some() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "neither a non-build split_phase nor cutover was ever observed"
            );
            sleep(Duration::from_millis(30)).await;
        }

        let dead = match kill_target {
            Some(i) => {
                println!("killing parent leader node {i} at an endgame phase");
                nodes[i].shutdown_graceful().await;
                Some(i)
            }
            None => {
                println!("cutover completed before a phase was observed — post-cutover kill path");
                None
            }
        };

        // Survivors complete (or already completed) the cutover.
        let observer = (0..nodes.len()).find(|i| Some(*i) != dead).unwrap();
        await_cutover(&nodes[observer], Duration::from_secs(90)).await;

        // Every acked write is readable through a survivor.
        let mut s2 = TcpStream::connect(nodes[observer].client_addr())
            .await
            .expect("connect survivor");
        for i in 0..30u8 {
            assert_eq!(
                get(&mut s2, vec![b'k', i % 8, b'p', i]).await,
                Some(vec![b'v', i]),
                "acked write {i} lost across the killed-driver cutover"
            );
        }

        for (i, node) in nodes.iter().enumerate() {
            if Some(i) != dead {
                node.shutdown_graceful().await;
            }
        }
    })
    .await
    .expect("test timed out");
}

// ---------------------------------------------------------------------------
// Rung 8 additions: table-parameterized helpers for the concurrent-tables
// acceptance test and the committed split bench below. The originals above
// stay hardcoded to table "t" (their tests predate multi-table needs).
// ---------------------------------------------------------------------------

/// [`put`], parameterized by table. Same bounded-retry contract.
async fn put_in(stream: &mut TcpStream, table: &str, key: Vec<u8>, value: Vec<u8>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        animusd::write_frame(
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
            ClientResponse::PutOk => return,
            ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(150)).await;
            }
            other => panic!("put({table}) failed: {other:?}"),
        }
    }
}

/// [`get`], parameterized by table.
async fn get_in(stream: &mut TcpStream, table: &str, key: Vec<u8>) -> Option<Vec<u8>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        animusd::write_frame(
            stream,
            &ClientRequest::Get {
                key: key.clone(),
                table: table.to_string(),
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

/// Kick off a split of `tablet` at `split_key` via `node`'s admin surface.
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

/// [`await_cutover`], parameterized by table + parent id.
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
            "cutover of {table}/{parent} never completed: tablets={tablets:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

/// Rung 8 acceptance (b): two DIFFERENT tables' splits racing each other.
/// Each workflow is per-tablet state on the shared control plane — two
/// drivers (possibly on different leaders), two Freeze/Cutover sequences,
/// interleaved arbitrarily. Both must complete; neither table loses a row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_splits_of_two_tables_both_complete() {
    timeout(Duration::from_secs(180), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        let mut s = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect");
        // Populate both tables: keys `a\x00..a\x0f` / `b\x00..b\x0f`.
        for i in 0..16u8 {
            put_in(&mut s, "ta", vec![b'a', i], vec![b'A', i]).await;
            put_in(&mut s, "tb", vec![b'b', i], vec![b'B', i]).await;
        }
        let pa = sole_tablet_of(&nodes[0], "ta");
        let pb = sole_tablet_of(&nodes[0], "tb");

        // Back-to-back kickoffs through a follower (relay path included) —
        // the two workflows run concurrently from here.
        let follower = nodes
            .iter()
            .position(|n| !n.is_control_leader())
            .expect("a follower exists");
        kickoff_tablet(&nodes[follower], pa, "a\\u0008").await;
        kickoff_tablet(&nodes[follower], pb, "b\\u0008").await;

        // Keep writing to BOTH tables while the two builds race.
        for i in 16..28u8 {
            put_in(&mut s, "ta", vec![b'a', i], vec![b'A', i]).await;
            put_in(&mut s, "tb", vec![b'b', i], vec![b'B', i]).await;
        }

        let (a_left, a_right) =
            await_cutover_of(&nodes[0], "ta", pa, Duration::from_secs(90)).await;
        let (b_left, b_right) =
            await_cutover_of(&nodes[0], "tb", pb, Duration::from_secs(90)).await;
        assert_ne!((a_left, a_right), (b_left, b_right));

        // Zero lost writes on either table, through a different node.
        let mut s2 = TcpStream::connect(nodes[1].client_addr())
            .await
            .expect("connect n1");
        for i in 0..28u8 {
            assert_eq!(
                get_in(&mut s2, "ta", vec![b'a', i]).await,
                Some(vec![b'A', i]),
                "ta write {i} lost across the racing cutovers"
            );
            assert_eq!(
                get_in(&mut s2, "tb", vec![b'b', i]).await,
                Some(vec![b'B', i]),
                "tb write {i} lost across the racing cutovers"
            );
        }

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// Rung 8: the committed split bench (ADR 0050's named deliverable). Run
/// explicitly with `cargo test -p animusd --test split_build -- --ignored
/// bench_split --nocapture`. Reports: (i) build wall-clock + rows/s for an
/// N-row table, (ii) the parent's sequential serve latency during the live
/// build vs an idle baseline (median + p99), (iii) the freeze→cutover write
/// blip as observed by a continuously-retrying client (the F8 contract:
/// sub-second). Sequential single-client numbers, same caveat as the ADR
/// 0049 bench.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "bench — run explicitly with --ignored --nocapture"]
async fn bench_split_build_serve_latency_and_cutover_blip() {
    const N: usize = 2_000;
    timeout(Duration::from_secs(600), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
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
        // cutover completes. Reads keep serving through the freeze; a put
        // hitting the freeze window retries inside `put_in` — its observed
        // latency IS the write blip.
        let t_kickoff = std::time::Instant::now();
        kickoff_tablet(&nodes[0], parent, "k\\u0004").await;
        let mut build_gets = Vec::new();
        let mut build_puts = Vec::new();
        let (left, right) = loop {
            for i in 0..20usize {
                let key = vec![b'k', (i / 256) as u8, (i % 256) as u8];
                let t0 = std::time::Instant::now();
                let got = get_in(&mut s, "bench", key).await;
                build_gets.push(t0.elapsed());
                assert!(got.is_some());
                let wkey = vec![b'w', (build_puts.len() % 256) as u8];
                let t0 = std::time::Instant::now();
                put_in(&mut s, "bench", wkey, vec![b'v']).await;
                build_puts.push(t0.elapsed());
            }
            let (_, st) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
            let tablets = st["tablets"].as_object().cloned().unwrap_or_default();
            if !tablets.contains_key(&parent.to_string()) {
                break await_cutover_of(&nodes[0], "bench", parent, Duration::from_secs(30)).await;
            }
            assert!(
                t_kickoff.elapsed() < Duration::from_secs(300),
                "build never completed"
            );
        };
        let build = t_kickoff.elapsed();

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
        eprintln!("split bench (N={N} rows, 256B values, 3 nodes, children {left}/{right}):");
        eprintln!(
            "  build wall-clock: {build:?}  ({:.0} rows/s)",
            N as f64 / build.as_secs_f64()
        );
        eprintln!("  serve GET idle:   median {i_med:?}  p99 {i_p99:?}");
        eprintln!("  serve GET build:  median {g_med:?}  p99 {g_p99:?}");
        eprintln!("  serve PUT build:  median {p_med:?}  p99 {p_p99:?}");
        eprintln!("  write blip (max PUT incl. freeze window): {p_max:?}");
        assert!(
            p_max < Duration::from_secs(2),
            "freeze→cutover write blip materially over the F8 sub-second contract: {p_max:?}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("bench timed out");
}
