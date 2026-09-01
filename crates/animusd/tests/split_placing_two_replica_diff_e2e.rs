//! Issue #513 investigation, end to end over a REAL multi-threaded
//! `ProdEnv` cluster: grow a 3-node cluster by TWO lower-sorting-id nodes
//! (instead of one, mirroring `split_placing_completion.rs`'s own growth
//! technique verbatim) so the fork-first split's directed-Placing target
//! differs from the parent-inherited replicas by TWO of three — the exact
//! shape reported to make `reconfigure_step`'s live Raft membership
//! "oscillate indefinitely" under a real cluster (see
//! `docs/engineering-lessons.md` and ADR 0062's #513 amendment for the
//! original finding).
//!
//! **This test does not reproduce that oscillation.** Every run (25+
//! consecutive, including several where the tablet's own leader genuinely
//! transfers mid-sequence via `reconfigure_step`'s own step-6 self-removal
//! case — see the `leader=` column this test prints) shows the live Raft
//! voter set for both children pass through the transient, genuinely
//! over-replicated 5-voter intermediate the issue names, then shrink
//! monotonically to the 3-voter target with no reversion — on a real
//! on-disk `LsmEngine` backend, under continuous write traffic, exactly
//! this crate's real production `host::Reconciler` driving
//! `RaftKvNode::reconfigure_step`. See
//! `crates/animus-cp-data/tests/reconfigure_multi_replica_diff.rs` for the
//! `SimEnv` side of the same investigation (many more seeds, several
//! harness shapes) and this repo's engineering-lessons entry for the full
//! writeup and the likely explanation for the original finding.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_env::NodeId;
use animusd::{ClusterConfig, Node, NodeStatus, RoleAddrs, StorageBackend};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;
use support::free_addrs;

async fn bring_up_inplace(n: usize, dir: &Path) -> (Vec<Node>, ClusterConfig) {
    for attempt in 0..16 {
        let addrs = free_addrs(n * 6);
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
            match animusd::run_node_with_streams_quiesce_and_backup_store(
                &config,
                i,
                dir.join(format!("node-{attempt}-{i}")),
                StorageBackend::default(),
                Duration::from_secs(600),
                animusd::StreamSealKnobs::default(),
                animusd::SegmentStoreConfig::default(),
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
    panic!("could not bring up an in-place-split cluster after retries");
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

async fn put(stream: &mut TcpStream, key: Vec<u8>, value: Vec<u8>) {
    use animusd::{ClientRequest, ClientResponse, read_frame, write_frame};
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
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
            ClientResponse::PutOk => return,
            ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(150)).await;
            }
            other => panic!("put failed: {other:?}"),
        }
    }
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

async fn join_extra(core_intra: &[SocketAddr], ids: &[&str], dir: &Path) -> Vec<Node> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let addrs = free_addrs(ids.len() * 6);
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
                BTreeMap::new(),
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

fn tablet_replicas(status: &Value, tablet: u64) -> Vec<String> {
    status["tablets"][tablet.to_string()]["replicas"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// The live Raft `voters` set for `tablet` plus the id of whichever node
/// currently reports itself leader for it (searching every node, since
/// `/admin/raftkv` is node-local) — `None` for the leader half if no node
/// currently claims leadership (a transfer/election in flight).
async fn live_voters_leader(nodes: &[Node], tablet: u64) -> Option<(Vec<String>, Option<String>)> {
    let mut any: Option<Vec<String>> = None;
    let mut leader: Option<String> = None;
    for n in nodes {
        let (_, body) = admin(n.admin_addr(), "GET", "/admin/raftkv", None).await;
        if let Some(groups) = body["groups"].as_array() {
            for g in groups {
                if g["tablet"].as_u64() == Some(tablet) {
                    let mut voters: Vec<String> = g["voters"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .map(|v| v.as_str().unwrap_or_default().to_string())
                                .collect()
                        })
                        .unwrap_or_default();
                    voters.sort();
                    if g["is_leader"].as_bool() == Some(true) {
                        leader = g["node"].as_str().map(str::to_string);
                        any = Some(voters);
                    } else if any.is_none() {
                        any = Some(voters);
                    }
                }
            }
        }
    }
    any.map(|v| (v, leader))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn two_of_three_replica_diff_placing_target_converges_end_to_end() {
    timeout(Duration::from_secs(150), async {
        let dir = support::panic_safe_tempdir();
        let (mut nodes, config) = bring_up_inplace(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        let mut client = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect client port");
        put(&mut client, vec![b'k', 0], vec![b'v', 0]).await;
        let parent = sole_tablet_of(&nodes[0], "t");
        let (_, status0) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
        assert_eq!(
            {
                let mut r = tablet_replicas(&status0, parent);
                r.sort();
                r
            },
            vec!["n0", "n1", "n2"],
            "unexpected initial replica placement"
        );

        // Grow by TWO lower-sorting nodes ("m0" < "m1" < "n0") — the exact
        // two-of-three shape issue #513 reports.
        let core_intra: Vec<SocketAddr> = config.nodes.iter().map(|a| a.intra).collect();
        let extra = join_extra(&core_intra, &["m0", "m1"], dir.path()).await;
        await_all_active(&nodes, &["m0", "m1"], Duration::from_secs(20)).await;
        nodes.extend(extra);

        let split_key = "k\\u0080";
        kickoff_tablet(&nodes[0], parent, split_key).await;
        let (left, right) = await_cutover_of(&nodes[0], "t", parent, Duration::from_secs(60)).await;

        let want_target = {
            let mut t = vec!["m0".to_string(), "m1".to_string(), "n0".to_string()];
            t.sort();
            t
        };
        let (_, status) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
        eprintln!("post-cutover status split_placing: {}", status["split_placing"]);
        for child in [left, right] {
            let entry = status["split_placing"][child.to_string()].clone();
            eprintln!("child {child} split_placing entry: {entry}");
        }

        // A paced continuous writer, mirroring the ADR 0062 rung-6 e2e's own
        // shape, so catch-up/commit-index are genuine moving targets
        // throughout the observation window below, not a quiescent group.
        let writer_addr = nodes[0].client_addr();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop2 = std::sync::Arc::clone(&stop);
        let writer = tokio::spawn(async move {
            let mut stream = TcpStream::connect(writer_addr)
                .await
                .expect("connect writer client port");
            let mut i: u64 = 1;
            while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                put(&mut stream, format!("w{i}").into_bytes(), vec![7u8; 64]).await;
                i += 1;
                sleep(Duration::from_millis(15)).await;
            }
        });

        // Poll every 200ms, recording the live voter set for BOTH children,
        // looking for genuine growth-then-shrink oscillation, up to a
        // generous 90s budget.
        let mut trace_left: Vec<(u128, usize, Vec<String>, Option<String>)> = Vec::new();
        let mut trace_right: Vec<(u128, usize, Vec<String>, Option<String>)> = Vec::new();
        let start = tokio::time::Instant::now();
        let deadline = start + Duration::from_secs(90);
        let mut converged = false;
        loop {
            let t = tokio::time::Instant::now().duration_since(start).as_millis();
            if let Some((v, l)) = live_voters_leader(&nodes, left).await {
                trace_left.push((t, v.len(), v, l));
            }
            if let Some((v, l)) = live_voters_leader(&nodes, right).await {
                trace_right.push((t, v.len(), v, l));
            }
            let l_ok = trace_left.last().map(|(_, _, v, _)| v) == Some(&want_target);
            let r_ok = trace_right.last().map(|(_, _, v, _)| v) == Some(&want_target);
            if l_ok && r_ok {
                converged = true;
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            sleep(Duration::from_millis(200)).await;
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = writer.await;

        eprintln!("=== left child {left} voter trajectory ===");
        for (t, n, v, l) in &trace_left {
            eprintln!("  t={t}ms n={n} leader={l:?} voters={v:?}");
        }
        eprintln!("=== right child {right} voter trajectory ===");
        for (t, n, v, l) in &trace_right {
            eprintln!("  t={t}ms n={n} leader={l:?} voters={v:?}");
        }

        assert!(
            converged,
            "two-of-three-diff Placing target never converged within 90s (left len trace: {:?}, right len trace: {:?})",
            trace_left.iter().map(|(_, n, _, _)| *n).collect::<Vec<_>>(),
            trace_right.iter().map(|(_, n, _, _)| *n).collect::<Vec<_>>(),
        );
        // This must have genuinely passed through the over-replicated
        // 5-voter intermediate the issue names — otherwise this is only
        // proving the strictly easier single-diff case under a different
        // harness.
        let max_left = trace_left.iter().map(|(_, n, _, _)| *n).max().unwrap_or(0);
        let max_right = trace_right.iter().map(|(_, n, _, _)| *n).max().unwrap_or(0);
        assert!(
            max_left >= 5 && max_right >= 5,
            "expected to observe the transient 5-voter intermediate on both children \
             (max seen: left={max_left}, right={max_right})"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}
