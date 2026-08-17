//! ADR 0050 Train B rung 4 — the split-build driver, end to end over a real
//! 3-node cluster: a populated table's `BeginSplit` kicks off the copy, the
//! parent keeps serving while writes RACE the build (the change-log tail
//! must observe them), both `Building` children converge to exactly their
//! halves, and a parent-leader kill mid-lifecycle re-runs the (idempotent)
//! build on the new leader to the same converged answer, racing writes
//! included. The workflow deliberately stops at convergence in this rung —
//! freeze/cutover are B5's.

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
/// early-cluster transients (a peer's intra listener still binding, an
/// election in flight) surface as retryable one-off errors.
async fn put(stream: &mut TcpStream, key: Vec<u8>, value: Vec<u8>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_build_converges_racing_writes_and_survives_a_parent_leader_kill() {
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

        // Kick off from a non-control-leader node (relay path) at `[k,4]`.
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

        // Writes RACING the build: 8 more keys, one per existing key with a
        // `!` suffix — `[k,i,!]` sorts above `[k,i]`, so i<4 lands left of
        // `[k,4]` and i>=4 right; totals become 8 per side.
        for i in 0..8u8 {
            put(&mut stream, vec![b'k', i, b'!'], b"racing".to_vec()).await;
        }

        // The two Building children (from /admin/status), left = the one
        // whose range starts at the table's origin.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let children: (u64, u64) = loop {
            let (_, s) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
            let tablets = s["tablets"].as_object().cloned().unwrap_or_default();
            let mut building: Vec<(u64, Vec<u8>)> = tablets
                .iter()
                .filter(|(_, t)| t["state"].as_str() == Some("Building"))
                .filter_map(|(id, t)| {
                    let start: Vec<u8> = t["range"]["start"]
                        .as_array()?
                        .iter()
                        .filter_map(|b| b.as_u64().map(|b| b as u8))
                        .collect();
                    Some((id.parse().ok()?, start))
                })
                .collect();
            if building.len() == 2 {
                // The left child's range starts at the parent's own origin —
                // compare the actual BYTE arrays (a JSON-stringified sort
                // inverts: "[107,4]" < "[]").
                building.sort_by(|a, b| a.1.cmp(&b.1));
                break (building[0].0, building[1].0);
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "two Building children never appeared: {tablets:?}"
            );
            sleep(Duration::from_millis(100)).await;
        };

        // Phase 1: the build converges with the racing writes included.
        await_converged(&nodes, &[], &children, (8, 8), Duration::from_secs(45)).await;

        // The parent still serves its whole range (reads AND writes), and
        // its change log was NOT trimmed out from under the build (the
        // Splitting trim hold): a fresh write still converges below.
        for i in 0..8u8 {
            let req = ClientRequest::Get {
                key: vec![b'k', i],
                table: "t".to_string(),
            };
            animusd::write_frame(&mut stream, &req).await.expect("send");
            let got = read_frame(&mut stream).await.expect("read").expect("reply");
            assert!(
                matches!(got, ClientResponse::Value(Some(_))),
                "parent read mid-build: {got:?}"
            );
        }

        // Phase 2: kill the node leading the parent group — the new
        // leader's driver re-runs the whole build from scratch
        // (idempotent) and picks up further racing writes.
        let mut leader_host = None;
        for (i, node) in nodes.iter().enumerate() {
            let (_, view) = admin(node.admin_addr(), "GET", "/admin/raftkv", None).await;
            let leads = view["groups"].as_array().is_some_and(|gs| {
                gs.iter().any(|g| {
                    g["tablet"].as_u64() == Some(1) && g["is_leader"].as_bool() == Some(true)
                })
            });
            if leads {
                leader_host = Some(i);
                break;
            }
        }
        let dead = leader_host.expect("some node leads the parent");
        nodes[dead].shutdown_graceful().await;

        // Two more keys through a surviving node — both land LEFT of [k,4].
        let alive = (0..nodes.len()).find(|i| *i != dead).unwrap();
        let mut stream2 = TcpStream::connect(nodes[alive].client_addr())
            .await
            .expect("connect surviving client port");
        for i in 0..2u8 {
            put(&mut stream2, vec![b'j', i], b"after-kill".to_vec()).await;
        }

        // Converge again on the survivors: left grew to 10.
        await_converged(&nodes, &[dead], &children, (10, 8), Duration::from_secs(60)).await;

        for (i, node) in nodes.iter().enumerate() {
            if i != dead {
                node.shutdown_graceful().await;
            }
        }
    })
    .await
    .expect("test timed out");
}

/// Poll until the parent's build reports converged on its leader AND both
/// children's leader-side key counts equal `expect` — the whole-build
/// convergence check, retried as one unit (counts move while the tail runs).
async fn await_converged(
    nodes: &[Node],
    dead: &[usize],
    children: &(u64, u64),
    expect: (u64, u64),
    budget: Duration,
) {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let groups = all_groups(nodes, dead).await;
        let parent_converged = groups.iter().any(|g| {
            g["tablet"].as_u64() == Some(1)
                && g["is_leader"].as_bool() == Some(true)
                && g["split_converged"].as_bool() == Some(true)
        });
        let count_of = |tablet: u64| {
            groups
                .iter()
                .find(|g| {
                    g["tablet"].as_u64() == Some(tablet) && g["is_leader"].as_bool() == Some(true)
                })
                .and_then(|g| g["key_count"].as_u64())
        };
        let left = count_of(children.0);
        let right = count_of(children.1);
        if parent_converged && left == Some(expect.0) && right == Some(expect.1) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "build never converged to {expect:?}: parent_converged={parent_converged}, \
             left={left:?}, right={right:?}"
        );
        sleep(Duration::from_millis(200)).await;
    }
}
