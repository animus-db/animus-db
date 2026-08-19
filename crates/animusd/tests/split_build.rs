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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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

/// The build must not pay a consensus round per row it has ALREADY
/// copied. The bulk pass ships in `SEED_CHUNK_BYTES` batches, but the tail
/// used to start from a zero watermark — so its first pass treated every
/// change record in the log as dirty and re-shipped the whole table, one
/// `SeedBatch` (hence one Raft entry, plus a forwarded hop for an off-node
/// child) per partition key. Measured on a 20,000-key split before this
/// was fixed: ~6,000 no-op entries per child and ~85% of the build's wall
/// clock, with the children's key counts flat the entire time.
///
/// The teeth: a quiet table (no writes during the build — the racing-write
/// case is `full_split_workflow_cutover_completes_with_no_lost_writes`)
/// must cut over having spent a number of child Raft entries set by the
/// image's BYTE size, not by its row count. Each `SeedBatch` is exactly
/// one entry, so a child's `commit_index` at cutover is a direct meter:
/// `N` rows of ~28 bytes fit one chunk per side, so anything near `N / 2`
/// means the row-by-row tail is back. Red on the pre-fix tip
/// (~`N / 2` entries per child), green after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_build_tail_does_not_re_ship_the_bulk_image_row_by_row() {
    /// Rows written before the split. Each becomes its own dirty unit (a
    /// sub-token raw key is its own lineage), which is what made the
    /// pre-fix tail cost one round-trip apiece.
    const N: usize = 600;
    /// Every seeded row fits in a single `SEED_CHUNK_BYTES` chunk per
    /// child, so the split's whole entry cost is a handful of ships plus
    /// each group's own formation/bookkeeping entries. Generous enough to
    /// absorb those; far below the ~`N / 2` a row-by-row tail spends.
    const MAX_CHILD_ENTRIES: u64 = 64;

    timeout(Duration::from_secs(180), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        // Keys `[k, i % 8, i / 8]`: the split key `[k,4]` puts half on each
        // side, and every one of them is a distinct dirty unit.
        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect client port");
        for i in 0..N {
            let key = vec![b'k', (i % 8) as u8, (i / 8) as u8];
            put(&mut stream, key, vec![b'v', (i % 251) as u8]).await;
        }

        kickoff(&nodes).await;
        let (left, right) = await_cutover(&nodes[0], Duration::from_secs(120)).await;

        // Sample immediately at cutover: the children are Active from here
        // on, so their own consumer arms would slowly add entries of their
        // own and blur what this measures.
        let groups = all_groups(&nodes, &[]).await;
        for child in [left, right] {
            let entries = groups
                .iter()
                .find(|g| {
                    g["tablet"].as_u64() == Some(child) && g["is_leader"].as_bool() == Some(true)
                })
                .and_then(|g| g["commit_index"].as_u64())
                .unwrap_or_else(|| panic!("no leader group for child {child}: {groups:?}"));
            assert!(
                entries <= MAX_CHILD_ENTRIES,
                "child {child} took {entries} Raft entries to receive a {N}-row split — the tail \
                 is shipping row-by-row again (bulk-image bytes fit one chunk; budget \
                 {MAX_CHILD_ENTRIES})"
            );
        }

        // The cheaper tail must still be a COMPLETE one: every pre-split
        // row survives the cutover.
        for i in 0..N {
            let key = vec![b'k', (i % 8) as u8, (i / 8) as u8];
            assert_eq!(
                get(&mut stream, key.clone()).await,
                Some(vec![b'v', (i % 251) as u8]),
                "pre-split key {key:?} lost across cutover"
            );
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

/// Encode a raw split key as a JSON string literal body for `POST
/// /admin/tablet/split` (`\u00XX`-escaping every byte — all test keys are
/// sub-0x80, so each escape decodes back to exactly one byte).
fn json_key(key: &[u8]) -> String {
    key.iter().map(|b| format!("\\u{b:04x}")).collect()
}

/// Wait until `parent` is gone from the tablet map and both named
/// `children` are `Active`, then return them ordered by range start (left
/// first). The multi-round sibling of [`await_cutover_of`]: that helper
/// counts the table's WHOLE Active set (`== 2`), which is only ever true
/// after a table's first split — from the second split on, earlier rounds'
/// children keep the count above 2 forever, so a repeated-split test must
/// scope its wait to the one round's own children instead.
async fn await_children_active(
    node: &Node,
    parent: u64,
    children: [u64; 2],
    budget: Duration,
) -> (u64, u64) {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let (_, s) = admin(node.admin_addr(), "GET", "/admin/status", None).await;
        let tablets = s["tablets"].as_object().cloned().unwrap_or_default();
        let parent_gone = !tablets.contains_key(&parent.to_string());
        let mut active: Vec<(u64, Vec<u8>)> = children
            .iter()
            .filter_map(|id| {
                let t = tablets.get(&id.to_string())?;
                if t["state"].as_str() != Some("Active") {
                    return None;
                }
                let start: Vec<u8> = t["range"]["start"]
                    .as_array()?
                    .iter()
                    .filter_map(|b| b.as_u64().map(|b| b as u8))
                    .collect();
                Some((*id, start))
            })
            .collect();
        if parent_gone && active.len() == 2 {
            // Compare actual BYTE arrays (a JSON-stringified sort inverts).
            active.sort_by(|a, b| a.1.cmp(&b.1));
            return (active[0].0, active[1].0);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cutover of {parent} -> {children:?} never completed: tablets={tablets:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

/// ADR 0050 fork F5 regression: on a cluster LARGER than the replication
/// factor, `BeginSplit` mints the children at fresh, balance-chosen homes —
/// so the parent's leader routinely hosts NO replica of one child (with 5
/// members and RF 3, the two spare members are always the least loaded).
/// Seeding that child is a cross-node forward addressed by tablet id, and
/// it must chase the "not the leader here" refusal's embedded leader hint
/// (`forward_to_tablet_leader`): the pre-fix code relayed once and re-ran
/// `resolve_cp_route` from scratch on every retry, which deterministically
/// re-picked the child's FIRST metadata replica forever — whenever that
/// first replica was not the child's elected leader, the bulk pass never
/// finished, the parent parked `Splitting` holding every key, and the
/// children sat empty/half-seeded indefinitely (the "auto-split made 2 new
/// tablets but the keys were never rebalanced" field report). The 3-node
/// siblings above structurally cannot catch this — RF == cluster size means
/// every node hosts every child, so a local leader hint always exists.
///
/// The stall needs two shapes at once: a child the parent's leader hosts no
/// replica of, AND that child's leader not being its first-listed replica.
/// The first is placement-deterministic here; the second is election
/// randomness (~2/3 per round). So this runs successive split rounds —
/// every one must cut over — until it has OBSERVED a round with the full
/// hazard shape (and always at least two rounds, so the repeated-split
/// bookkeeping is exercised on every run), which the pre-fix code reliably
/// turns into a red timeout within a round or two. No-hazard-in-8-rounds
/// means the placement/election texture changed and this test must be
/// re-aimed, so it fails loudly rather than silently passing toothless.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_completes_when_a_child_lives_off_the_parent_leader_node() {
    timeout(Duration::from_secs(300), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(5, dir.path()).await;
        await_bootstrap(&nodes).await;

        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect client port");
        let all_keys: Vec<Vec<u8>> = (0..8u8).map(|i| vec![b'k', i]).collect();
        for key in &all_keys {
            put(&mut stream, key.clone(), key.clone()).await;
        }

        // Splittable tablets, front-first: (id, sorted keys it holds).
        let mut queue: Vec<(u64, Vec<Vec<u8>>)> = vec![(1, all_keys.clone())];
        let mut hazard_observed = false;
        let mut rounds_done = 0u32;
        for _round in 0..8 {
            let Some(pos) = queue.iter().position(|(_, keys)| keys.len() >= 2) else {
                break;
            };
            let (parent, keys) = queue.remove(pos);
            let split_key = keys[keys.len() / 2].clone();

            // The node currently leading the parent — the split driver's home.
            let parent_leader = all_groups(&nodes, &[])
                .await
                .into_iter()
                .find(|g| {
                    g["tablet"].as_u64() == Some(parent) && g["is_leader"].as_bool() == Some(true)
                })
                .and_then(|g| g["node"].as_str().map(str::to_owned))
                .expect("parent has a leader");

            let known: Vec<u64> = queue
                .iter()
                .map(|(id, _)| *id)
                .chain(std::iter::once(parent))
                .collect();
            kickoff_tablet(&nodes[0], parent, &json_key(&split_key)).await;

            // The two freshly-minted children and their placement-chosen
            // replica sets (readable while `Building` or already `Active` —
            // replicas don't change across cutover).
            let children: Vec<(u64, Vec<String>)> = {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
                loop {
                    let (_, s) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
                    let tablets = s["tablets"].as_object().cloned().unwrap_or_default();
                    let found: Vec<(u64, Vec<String>)> = tablets
                        .iter()
                        .filter(|(id, t)| {
                            t["table"].as_str() == Some("t")
                                && id.parse::<u64>().is_ok_and(|id| !known.contains(&id))
                        })
                        .filter_map(|(id, t)| {
                            let replicas = t["replicas"]
                                .as_array()?
                                .iter()
                                .filter_map(|r| r.as_str().map(str::to_owned))
                                .collect();
                            Some((id.parse().ok()?, replicas))
                        })
                        .collect();
                    if found.len() == 2 {
                        break found;
                    }
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "children of {parent} never appeared: {tablets:?}"
                    );
                    sleep(Duration::from_millis(100)).await;
                }
            };

            // Hazard shape: a child that (a) will actually be seeded (both
            // halves are non-empty, since the parent held >= 2 keys and the
            // split key is one of them), (b) the parent's leader hosts no
            // replica of, and (c) whose own elected leader is NOT its
            // first-listed replica — the exact combination the un-chased
            // relay loops on.
            for (child, replicas) in &children {
                if replicas.contains(&parent_leader) {
                    continue;
                }
                let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
                let child_leader = loop {
                    let hit = all_groups(&nodes, &[]).await.into_iter().find(|g| {
                        g["tablet"].as_u64() == Some(*child)
                            && g["is_leader"].as_bool() == Some(true)
                    });
                    if let Some(g) = hit {
                        break g["node"].as_str().map(str::to_owned);
                    }
                    if tokio::time::Instant::now() >= deadline {
                        break None; // group formed slowly; treat as no observation
                    }
                    sleep(Duration::from_millis(100)).await;
                };
                if child_leader.is_some_and(|leader| {
                    Some(leader.as_str()) != replicas.first().map(String::as_str)
                }) {
                    hazard_observed = true;
                }
            }

            // Every round must complete regardless — on the un-chased relay
            // a hazard round parks `Splitting` forever and this times out.
            // Scoped to THIS round's own children (parent gone + both
            // Active), never `await_cutover_of`'s whole-table Active count:
            // from round 2 on, the table legitimately holds earlier rounds'
            // children too, so that count can never come back down to 2
            // (the exact predicate bug this test's first CI run tripped).
            let (left, right) = await_children_active(
                &nodes[0],
                parent,
                [children[0].0, children[1].0],
                Duration::from_secs(90),
            )
            .await;
            let (left_keys, right_keys): (Vec<_>, Vec<_>) =
                keys.into_iter().partition(|k| *k < split_key);
            queue.push((left, left_keys));
            queue.push((right, right_keys));
            rounds_done += 1;

            // Run at least two rounds even when round 1 already showed the
            // hazard, so the multi-round bookkeeping (a table with MORE
            // than two Active tablets) is exercised on every run, not only
            // on the unlucky-election runs.
            if hazard_observed && rounds_done >= 2 {
                break;
            }
        }

        assert!(
            hazard_observed,
            "8 split rounds never produced an off-parent-leader child whose leader was not \
             its first-listed replica — placement/election texture changed; re-aim this test"
        );

        // No key lost across however many cutovers ran.
        for key in &all_keys {
            assert_eq!(
                get(&mut stream, key.clone()).await,
                Some(key.clone()),
                "key {key:?} lost across an off-leader-child split"
            );
        }

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// The concurrent-ship error path: the bulk pass now ships to both children
/// at once (`ship_all`/`try_join_all`), so a failure against ONE child also
/// cancels the sibling's in-flight future. Killing a node that leads one
/// child mid-build exercises exactly that — the ship to the dead child's
/// group errors, the sibling ship is dropped wherever it happened to be,
/// and the tick fails.
///
/// The build must still converge: `SeedBatch` merges at carried versions,
/// so re-running the whole bulk pass next tick re-ships everything
/// idempotently once the child's group re-elects out of its surviving two
/// replicas. Asserted end to end — cutover completes and every acked key
/// survives.
///
/// Racy by nature (the kill may land before the build starts or after it
/// finishes), so it degrades exactly the way
/// `driver_kill_after_freeze_resumes_and_completes` does: completion and
/// no-loss are asserted on every path, and the run prints which one it took.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_survives_losing_one_childs_leader_mid_build() {
    /// Enough rows that the bulk pass is a real window to land a kill in,
    /// few enough that writing them through the client protocol is quick.
    const N: usize = 400;

    timeout(Duration::from_secs(300), async {
        let dir = tempfile::tempdir().unwrap();
        // 5 nodes vs RF 3: children get placement-chosen homes (fork F5), so
        // a child leader that is neither node 0 nor the parent's own leader
        // exists to be killed.
        let (nodes, _config) = bring_up(5, dir.path()).await;
        await_bootstrap(&nodes).await;

        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect client port");
        let all_keys: Vec<Vec<u8>> = (0..N)
            .map(|i| vec![b'k', (i % 8) as u8, (i / 8) as u8])
            .collect();
        for key in &all_keys {
            put(&mut stream, key.clone(), key.clone()).await;
        }

        kickoff(&nodes).await;

        // Find a live node that leads one of the two `Building` children and
        // does NOT lead the parent — killing the parent's own leader tests
        // driver re-lead, which the sibling test above already covers; here
        // the driver must stay healthy and have its ship fail underneath it.
        // Node 0 is excluded: this test's client stream and observer admin
        // endpoint live there.
        //
        // Same per-node-view idiom as that sibling: each node's own
        // `/admin/raftkv` reports the groups *it* hosts, so the index into
        // `nodes` is known without mapping ids back and forth.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut victim: Option<usize> = None;
        loop {
            let (_, st) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
            if !st["tablets"]
                .as_object()
                .is_some_and(|t| t.contains_key("1"))
            {
                break; // the build outran the search — degrade
            }
            for (i, node) in nodes.iter().enumerate().skip(1) {
                let (_, view) = admin(node.admin_addr(), "GET", "/admin/raftkv", None).await;
                let groups = view["groups"].as_array().cloned().unwrap_or_default();
                let leads_parent = groups.iter().any(|g| {
                    g["tablet"].as_u64() == Some(1) && g["is_leader"].as_bool() == Some(true)
                });
                let leads_child = groups.iter().any(|g| {
                    g["tablet"].as_u64().is_some_and(|t| t != 1)
                        && g["is_leader"].as_bool() == Some(true)
                });
                if leads_child && !leads_parent {
                    victim = Some(i);
                    break;
                }
            }
            if victim.is_some() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no node ever led a Building child without also leading the parent"
            );
            sleep(Duration::from_millis(30)).await;
        }

        match victim {
            Some(i) => {
                println!("killing node {i}, leader of a Building child, mid-build");
                nodes[i].shutdown();
            }
            None => println!(
                "cutover completed before a killable child leader was observed - \
                 degrading to a plain completion check"
            ),
        }

        // Whatever happened, the workflow converges.
        let (left, right) = await_cutover(&nodes[0], Duration::from_secs(180)).await;
        println!("cutover completed to children {left}/{right}");

        // Every acked key survives the kill and the re-run bulk pass.
        for key in &all_keys {
            assert_eq!(
                get(&mut stream, key.clone()).await,
                Some(key.clone()),
                "key {key:?} lost across a split whose child leader was killed mid-build"
            );
        }

        for (i, node) in nodes.iter().enumerate() {
            if Some(i) != victim {
                node.shutdown_graceful().await;
            }
        }
    })
    .await
    .expect("test timed out");
}

/// One DynamoDB JSON request over a fresh HTTP/1.1 connection → `(status,
/// body)` — mirrors `dynamo_index_writes.rs`'s helper. Deliberately a
/// **single attempt, no client-side retry** (unlike `put`/`put_in` above),
/// since issues #285/#288's regressions are specifically about what the
/// *server* does with one request that lands mid-freeze — a client-side
/// retry loop here would mask a missing server-side one.
async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    let req = format!(
        "POST / HTTP/1.1\r\nHost: x\r\nX-Amz-Target: {target}\r\n\
         Connection: close\r\n\
         Content-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.expect("read");
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
    (status, body.to_owned())
}

/// Fire single-attempt `PutItem`s against `table` continuously (several
/// concurrent connections, no delay, unique keys) until `stop` is raised,
/// recording every non-200 reply. Shared by both issue #288 regressions
/// below — the only difference between them is which table (indexed vs
/// plain) the caller creates beforehand, which selects `cp_kind_write_item`'s
/// evaluated arm vs `cp_kind_write_raw`'s fast arm.
async fn probe_put_item_until_stopped(
    addr: SocketAddr,
    table: &str,
    stop: Arc<AtomicBool>,
) -> (u64, Vec<(u16, String)>) {
    let counter = Arc::new(AtomicU64::new(0));
    let bad = Arc::new(Mutex::new(Vec::new()));
    let mut probers = Vec::new();
    // Two lanes, paced (not a max-speed hammer): dense enough to reliably
    // cover the (sub-second, ADR 0050 rung 8's F8 contract) freeze window,
    // gentle enough not to become the bottleneck itself — an unthrottled
    // flood against an INDEXED table can outpace the GSI drain, and cutover
    // itself waits on the drain's own veto (it must catch up before a
    // parent can retire), so hammering harder here can make the split
    // never converge rather than proving anything about issue #288.
    for _ in 0..2u32 {
        let table = table.to_owned();
        let stop = Arc::clone(&stop);
        let counter = Arc::clone(&counter);
        let bad = Arc::clone(&bad);
        probers.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                let body = format!(
                    r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"k{i}"}},"g":{{"S":"v"}}}}}}"#
                );
                match timeout(
                    Duration::from_secs(12),
                    dynamo(addr, "DynamoDB_20120810.PutItem", &body),
                )
                .await
                {
                    Ok((200, _)) => {}
                    Ok((status, resp)) => bad.lock().unwrap().push((status, resp)),
                    Err(_) => bad
                        .lock()
                        .unwrap()
                        .push((0, "PutItem timed out (12s)".into())),
                }
                sleep(Duration::from_millis(20)).await;
            }
        }));
    }
    for p in probers {
        let _ = p.await;
    }
    let total = counter.load(Ordering::Relaxed);
    let bad = Arc::try_unwrap(bad)
        .expect("every prober has finished")
        .into_inner()
        .unwrap();
    (total, bad)
}

/// Issue #288: a client `PutItem` against an **indexed** table (the
/// evaluated arm, `ClientCtx::cp_kind_write_item` → `dynamo::
/// kind_write_item_at_leader`) racing a split's freeze window must succeed
/// once the child activates, not surface the retryable `FROZEN_REFUSAL` as a
/// terminal 500 — `cp_kind_write_item` did a single `cp_route` + one attempt
/// with no retry loop at all pre-fix.
///
/// Two concurrent single-attempt (no client-side retry) `PutItem` streams
/// hammer the table continuously from just before kickoff until the split
/// cuts over, covering the whole build→freeze→cutover window (whose freeze
/// sub-window is deliberately kept sub-second, ADR 0050 rung 8's F8
/// contract) without needing to guess its exact timing. Pre-fix, whichever
/// attempt's routing landed on the still-frozen parent got a bare 500;
/// post-fix every attempt succeeds — a fresh `cp_route` resolution on the
/// server's own internal retry lands it on the activated child.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_put_item_racing_the_freeze_window_retries_to_success() {
    timeout(Duration::from_secs(120), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;
        let addr = nodes[0].dynamo_addr();

        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"idx288",
                "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
                "GlobalSecondaryIndexes":[
                    {"IndexName":"by-g",
                     "KeySchema":[{"AttributeName":"g","KeyType":"HASH"}],
                     "Projection":{"ProjectionType":"ALL"}}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        let parent = sole_tablet_of(&nodes[0], "idx288");
        let stop = Arc::new(AtomicBool::new(false));
        let probing = tokio::spawn(probe_put_item_until_stopped(
            addr,
            "idx288",
            Arc::clone(&stop),
        ));
        // Give the probers a moment to actually be in flight before the
        // split starts, so the whole build→freeze→cutover window — not just
        // its tail — sees continuous write pressure.
        sleep(Duration::from_millis(50)).await;

        kickoff_tablet(&nodes[0], parent, "k\\u0004").await;
        await_cutover_of(&nodes[0], "idx288", parent, Duration::from_secs(90)).await;

        stop.store(true, Ordering::Relaxed);
        let (total, bad) = probing.await.expect("prober task panicked");
        assert!(
            total > 0,
            "no PutItem attempt ever completed — test setup problem"
        );
        assert!(
            bad.is_empty(),
            "an indexed PutItem racing the freeze window must retry to success, never a \
             terminal error: {bad:?}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// Issue #288's symmetric fast-arm coverage: a plain (unindexed,
/// unstreamed) table's `PutItem` — `dynamo::fast_marker_write` →
/// `ClientCtx::cp_kind_write_raw`, the "fast/marker" arm also used by CQL
/// and the raw client protocol — racing the identical freeze window. This
/// path had no coverage either way before (the issue's premise that plain
/// writes already retried across a split was itself mistaken — ADR 0049's
/// funnel unification dropped the old higher-layer retry along with the
/// evaluated arm's). Same shape as the indexed test above, just a plain
/// table (no `GlobalSecondaryIndexes`) so `PutItem` takes the fast arm
/// instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_put_item_racing_the_freeze_window_retries_to_success() {
    timeout(Duration::from_secs(120), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;
        let addr = nodes[0].dynamo_addr();

        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"plain288",
                "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        let parent = sole_tablet_of(&nodes[0], "plain288");
        let stop = Arc::new(AtomicBool::new(false));
        let probing = tokio::spawn(probe_put_item_until_stopped(
            addr,
            "plain288",
            Arc::clone(&stop),
        ));
        sleep(Duration::from_millis(50)).await;

        kickoff_tablet(&nodes[0], parent, "k\\u0004").await;
        await_cutover_of(&nodes[0], "plain288", parent, Duration::from_secs(90)).await;

        stop.store(true, Ordering::Relaxed);
        let (total, bad) = probing.await.expect("prober task panicked");
        assert!(
            total > 0,
            "no PutItem attempt ever completed — test setup problem"
        );
        assert!(
            bad.is_empty(),
            "a plain-table PutItem racing the freeze window must retry to success, never a \
             terminal error: {bad:?}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}
