//! ADR 0050 Train B rung 3 — the copy-based split's **metadata lifecycle**,
//! end to end over a real 3-node cluster (`ProdEnv`, one process per node):
//! a follower-connected admin kickoff (the relay-allowlist regression for
//! `MetaCommand::BeginSplit`), the parent observed `Splitting` while still
//! serving reads AND writes over its whole range, two `Building` children
//! minted at placement-chosen homes, hosted (their groups form on their
//! replica nodes) yet **unroutable** (every client op still lands on the
//! parent). The workflow deliberately stops there in this rung — no driver,
//! no cutover — so this end state is the rung's contract.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, read_frame};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Bring up an `n`-node cluster, one process per node — the same
/// port-TOCTOU-retrying shape as `admin_endpoint.rs`'s copy.
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

async fn client_op(stream: &mut TcpStream, req: &ClientRequest) -> ClientResponse {
    animusd::write_frame(stream, req).await.expect("send frame");
    read_frame(stream)
        .await
        .expect("read reply")
        .expect("a reply")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn begin_split_lifecycle_over_three_nodes_via_a_follower() {
    timeout(Duration::from_secs(90), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        // Populate the table through node 0's client port — enough keys to
        // land on both sides of any interior split point.
        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect client port");
        let keys: Vec<Vec<u8>> = (0..8u8).map(|i| vec![b'k', i]).collect();
        for key in &keys {
            let put = client_op(
                &mut stream,
                &ClientRequest::Put {
                    key: key.clone(),
                    value: key.clone(),
                    table: "t".to_string(),
                },
            )
            .await;
            assert!(matches!(put, ClientResponse::PutOk), "populate: {put:?}");
        }

        // Kick off the split from a node that is NOT the control leader —
        // the `BeginSplit` relay-allowlist regression (a missed
        // `is_relayable_command` entry is a bimodal per-process flake; this
        // pins the follower-connected path deterministically).
        let follower = nodes
            .iter()
            .position(|n| !n.is_control_leader())
            .expect("a 3-node cluster has a non-leader");
        let (status, body) = admin(
            nodes[follower].admin_addr(),
            "POST",
            "/admin/tablet/split",
            Some(r#"{"tablet":1,"split_key":"k"}"#),
        )
        .await;
        assert_eq!(
            status, 200,
            "follower-connected kickoff must relay and succeed, got: {body}"
        );

        // The lifecycle states ride `/admin/status`: poll until the parent
        // reads `Splitting` with exactly two `Building` children.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let children: Vec<u64> = loop {
            let (_, status_body) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
            let tablets = status_body["tablets"]
                .as_object()
                .cloned()
                .unwrap_or_default();
            let building: Vec<u64> = tablets
                .iter()
                .filter(|(_, t)| t["state"].as_str() == Some("Building"))
                .filter_map(|(id, _)| id.parse().ok())
                .collect();
            if tablets.get("1").map(|t| t["state"].as_str()) == Some(Some("Splitting"))
                && building.len() == 2
            {
                break building;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "parent never read Splitting with two Building children; tablets: {tablets:?}"
            );
            sleep(Duration::from_millis(100)).await;
        };

        // The children are HOSTED — their groups form on their (placement-
        // chosen) replica nodes: poll the union of every node's
        // `/admin/raftkv` until both child ids appear.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let mut hosted: Vec<u64> = Vec::new();
            for node in &nodes {
                let (_, view) = admin(node.admin_addr(), "GET", "/admin/raftkv", None).await;
                if let Some(groups) = view["groups"].as_array() {
                    hosted.extend(groups.iter().filter_map(|g| g["tablet"].as_u64()));
                }
            }
            if children.iter().all(|c| hosted.contains(c)) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "children {children:?} never hosted; hosted set: {hosted:?}"
            );
            sleep(Duration::from_millis(100)).await;
        }

        // Every client op keeps routing to an AUTHORITATIVE group — the
        // parent while it serves, never a `Building` child (whose engine
        // would answer a false `None`). Rung 5 completes the workflow, so
        // on a table this small the driver can freeze/cut over at any
        // moment — an op racing that window gets the documented retryable
        // refusal and its retry lands on the parent or an activated child
        // (acked exactly once, never lost, never answered from a Building
        // engine). Bounded retry, the same shape as every client loop.
        for key in &keys {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                let got = client_op(
                    &mut stream,
                    &ClientRequest::Get {
                        key: key.clone(),
                        table: "t".to_string(),
                    },
                )
                .await;
                match got {
                    ClientResponse::Value(Some(ref v)) if v == key => break,
                    ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                        sleep(Duration::from_millis(100)).await;
                    }
                    other => panic!(
                        "read of {key:?} mid-split must serve the written value \
                         (a Building child would answer None): {other:?}"
                    ),
                }
            }
        }
        for key in &keys {
            let mut k2 = key.clone();
            k2.push(b'!');
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                let put = client_op(
                    &mut stream,
                    &ClientRequest::Put {
                        key: k2.clone(),
                        value: b"post-split".to_vec(),
                        table: "t".to_string(),
                    },
                )
                .await;
                match put {
                    ClientResponse::PutOk => break,
                    ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                        sleep(Duration::from_millis(100)).await;
                    }
                    other => panic!("write mid-split never acked: {other:?}"),
                }
            }
        }

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}
