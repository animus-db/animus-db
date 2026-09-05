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
//!
//! **Issue #596**: proving the 5-voter intermediate genuinely occurred used
//! to rest on sampling `/admin/raftkv` externally every 200ms and asserting
//! on the observed max — flaky under load (~1 in 3 on a 2-core-pinned,
//! contended run), because the intermediate's own *duration* was never a
//! property this crate promises, only that it is logically reached; a fast
//! enough pair of consecutive reconciler ticks can remove both extras
//! between two samples. The 200ms poll below is now a diagnostic print
//! only — the real proof reads `RaftKvNode::voter_history()`
//! (`animus-cp-data`, via the `/admin/raftkv` `voter_history` field it
//! exposes) after convergence: a durable, in-process record of every
//! distinct voter configuration each replica has actually adopted, so
//! nothing external has to catch the transient state while it's happening.
//! The retained replica's own history is checked for the floor/ceiling and
//! the 5-voter intermediate, but not the exact `[3,4,5,4,3]` sequence a
//! `SimEnv` run can assert — under real timing a starved replica's own
//! `handle_append_entries` can adopt two config entries in one batch and
//! skip an intermediate its own consensus loop never got a chance to
//! observe, which the union check (unaffected, since some OTHER replica is
//! never starved on the same batch) already covers. See
//! `docs/engineering-lessons.md`'s matching entry for the general lesson.

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
                tls: None,
            })
            .collect();
        let config = ClusterConfig {
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
                tls: None,
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

/// Every node's own recorded `RaftKvNode::voter_history()` for `tablet`
/// (issue #596), keyed by that node's own id — `/admin/raftkv`'s
/// `voter_history` field, sorted node-id-wise within each entry so two
/// equal configurations compare equal regardless of adoption-order
/// artifacts in how the wire happened to list them. A node not currently
/// hosting `tablet` at all (an ex-replica already torn down, or one that
/// never hosted it) is simply absent from the map — the caller decides
/// whether that's expected.
async fn voter_history_of(nodes: &[Node], tablet: u64) -> BTreeMap<String, Vec<Vec<String>>> {
    let mut out = BTreeMap::new();
    for n in nodes {
        let (_, body) = admin(n.admin_addr(), "GET", "/admin/raftkv", None).await;
        let Some(groups) = body["groups"].as_array() else {
            continue;
        };
        for g in groups {
            if g["tablet"].as_u64() != Some(tablet) {
                continue;
            }
            let Some(node_id) = g["node"].as_str() else {
                continue;
            };
            let history: Vec<Vec<String>> = g["voter_history"]
                .as_array()
                .map(|entries| {
                    entries
                        .iter()
                        .map(|entry| {
                            let mut voters: Vec<String> = entry
                                .as_array()
                                .map(|a| {
                                    a.iter()
                                        .map(|v| v.as_str().unwrap_or_default().to_string())
                                        .collect()
                                })
                                .unwrap_or_default();
                            voters.sort();
                            voters
                        })
                        .collect()
                })
                .unwrap_or_default();
            out.insert(node_id.to_string(), history);
        }
    }
    out
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
        // generous 90s budget. "Converged" requires `SETTLE_SAMPLES`
        // CONSECUTIVE matches against `want_target`, not a single momentary
        // one: a bare one-shot match can fire while the control plane's own
        // reconcile/rebalance loop is still nudging the tablet further (the
        // production `split_placing_completion.rs` loop has the identical
        // `SPLIT_PLACING_DONE_SETTLE` discipline for exactly this reason —
        // see its own doc). Found live building this test's own
        // `voter_history`-based assertions: a bare momentary match let the
        // test proceed to read `voter_history` while the group was still
        // being reconfigured further (once observed continuing on, well
        // past `want_target`, toward something close to the ORIGINAL
        // replicas again) — a real gap in this test's own "converged"
        // definition, not a mechanism bug, and orthogonal to issue #596.
        const SETTLE_SAMPLES: usize = 3;
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
            let settled = |trace: &[(u128, usize, Vec<String>, Option<String>)]| {
                trace.len() >= SETTLE_SAMPLES
                    && trace[trace.len() - SETTLE_SAMPLES..]
                        .iter()
                        .all(|(_, _, v, _)| v == &want_target)
            };
            if settled(&trace_left) && settled(&trace_right) {
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
        // Diagnostic only (issue #596) — the 200ms sampled poll above can
        // race a fast-enough pair of consecutive reconciler ticks and miss
        // the transient 5-voter intermediate even when it genuinely
        // occurred, so a low number here proves nothing either way. The
        // real proof is `voter_history` below.
        let max_left = trace_left.iter().map(|(_, n, _, _)| *n).max().unwrap_or(0);
        let max_right = trace_right.iter().map(|(_, n, _, _)| *n).max().unwrap_or(0);
        eprintln!(
            "diagnostic only, not asserted (issue #596): 200ms-sampled max voters seen \
             left={max_left} right={max_right}"
        );

        // The real proof (issue #596): `RaftKvNode::voter_history()`, read
        // via `/admin/raftkv` from every node still up, is a durable
        // in-process record of every distinct configuration each replica
        // actually adopted — nothing external has to catch the transient
        // state while it's happening.
        //
        // "n0" is present in both the parent's inherited replicas
        // (`n0,n1,n2`, ADR 0062's fork-first inheritance) and `want_target`
        // (`m0,m1,n0`) by this test's own construction, for BOTH children —
        // it is the one replica retained throughout the whole swap on
        // either side, so its own history is the one record that saw every
        // step from a single fixed vantage point.
        const RETAINED: &str = "n0";
        for (child, label) in [(left, "left"), (right, "right")] {
            let by_node = voter_history_of(&nodes, child).await;
            eprintln!("=== {label} child {child} voter_history by node ===");
            for (node_id, history) in &by_node {
                eprintln!("  {node_id}: {history:?}");
            }

            // (a)+(b): the UNION of every currently-hosting node's own
            // history must show the over-replicated intermediate was
            // reached and never show fewer than the 3-voter floor either
            // side of the swap.
            //
            // One real, pre-existing (and orthogonal to issue #596) wrinkle
            // found running this: `host::Reconciler::host`'s bootstrap for a
            // replica joining an ALREADY-LED group (`initial_formation:
            // false`) seeds that replica's OWN local `RaftCore` from
            // `Metadata`'s CURRENT `t.replicas` **minus itself**
            // (`crates/animus-cp-data/src/host.rs`'s `let config = ...
            // else { others }`) — pure scaffolding to know initial peer
            // addresses before this replica has ever heard from the real
            // leader, not a value any quorum ever agreed on. Since
            // `Metadata::tablets[..].replicas` already reflects the
            // DIRECTED-PLACING final target the instant `split_placing`
            // computes it (ADR 0062 §3) — well before the live Raft swap
            // catches up — a replica bootstrapping through this path (a
            // genuinely new joiner, or an original replica that fell behind
            // enough to learn of the child via `Metadata` rather than
            // directly witnessing the fork) can record a transient,
            // structurally-nonsensical FIRST entry that excludes itself and
            // is smaller than any real committed configuration ever was
            // (observed live: `["m1", "n0"]`, 2 entries, on a node whose
            // real join sequence was 3→4→5→4→3 like every other replica's).
            // It self-corrects the moment real sync begins. A node's own
            // reported history is only meaningful from the first entry that
            // actually includes itself onward — no real committed config
            // ever excludes a member that hasn't joined it yet, and Raft's
            // one-member-at-a-time discipline means a later legitimate
            // "config no longer includes me" entry (this replica's own
            // eventual removal) can only ever follow a genuine
            // self-inclusive one, never precede it — so trimming this
            // leading run cannot hide a real regression.
            let mut union: Vec<Vec<String>> = Vec::new();
            for (node_id, history) in &by_node {
                let trusted_from = history.iter().position(|e| e.iter().any(|v| v == node_id));
                let Some(start) = trusted_from else {
                    // This node's own history never once included itself —
                    // it never actually became real (or the group was torn
                    // down on it before real sync); nothing it recorded is
                    // trustworthy either way.
                    continue;
                };
                for entry in &history[start..] {
                    if !union.contains(entry) {
                        union.push(entry.clone());
                    }
                }
            }
            let union_counts: Vec<usize> = union.iter().map(Vec::len).collect();
            assert!(
                union_counts.contains(&5),
                "{label} child {child}: voter_history union across every hosting node never \
                 recorded the transient 5-voter intermediate: {union_counts:?} (union: {union:?})"
            );
            assert!(
                union_counts.iter().all(|&c| c >= 3),
                "{label} child {child}: voter_history union dropped below the 3-voter floor: \
                 {union_counts:?} (union: {union:?})"
            );

            // (c): the retained replica's own history, read from a single
            // fixed vantage point, corroborates (a)+(b) end to end rather
            // than only across the union. **Not** the exact `[3,4,5,4,3]`
            // sequence here, deliberately: under real `ProdEnv` timing a
            // CPU-starved n0 can have its `handle_append_entries` adopt TWO
            // config-change entries in one batch (the leader only needs a
            // majority of the OTHER voters to commit the first one before
            // proposing the second — n0 itself is never on the critical
            // path for either commit), recording 3→5 directly and skipping
            // the 4-voter step this replica's own consensus loop simply
            // never got a chance to observe between the two. That is a
            // sampling gap in THIS replica's own once-per-iteration
            // recording, not a reversion or an under-replication — (a)+(b)
            // above (the union across every hosting replica, at least one
            // of which is never starved on the same batch) already prove
            // the property line 458 existed for: the swap genuinely reached
            // 5 and never dropped below the 3-voter floor. The `SimEnv`
            // regression (`voter_history_reconfigure_diff.rs`,
            // `animus-cp-data`) has no such starvation and keeps the exact
            // sequence assertion.
            let retained_history = by_node.get(RETAINED).unwrap_or_else(|| {
                panic!(
                    "{label} child {child}: expected {RETAINED} (retained throughout by this \
                     test's own construction) to still be hosting it — nodes seen: {:?}",
                    by_node.keys().collect::<Vec<_>>()
                )
            });
            let retained_counts: Vec<usize> = retained_history.iter().map(Vec::len).collect();
            assert_eq!(
                retained_counts.first(),
                Some(&3),
                "{label} child {child}: {RETAINED}'s own voter_history did not start at the \
                 3-voter floor (full history: {retained_history:?})"
            );
            assert_eq!(
                retained_counts.last(),
                Some(&3),
                "{label} child {child}: {RETAINED}'s own voter_history did not end at the \
                 3-voter floor (full history: {retained_history:?})"
            );
            assert!(
                retained_counts.contains(&5),
                "{label} child {child}: {RETAINED}'s own voter_history never recorded the \
                 transient 5-voter intermediate (full history: {retained_history:?})"
            );
            assert!(
                retained_counts.iter().all(|&c| c >= 3),
                "{label} child {child}: {RETAINED}'s own voter_history dropped below the \
                 3-voter floor (full history: {retained_history:?})"
            );
            assert_eq!(
                retained_history.last(),
                Some(&want_target),
                "{label} child {child}: {RETAINED}'s own voter_history did not end on the \
                 directed-Placing target (full history: {retained_history:?})"
            );
        }

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}
