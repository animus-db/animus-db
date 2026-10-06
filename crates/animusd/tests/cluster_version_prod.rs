//! ADR 0073 Phase 2 (P2-C) over real sockets (`ProdEnv`): the version era
//! starts by itself on a live cluster, `GET /admin/cluster-version` and
//! `POST /admin/cluster-version/finalize` work end to end, a data-only node
//! learns the era through its mirror, and the CHS1 handshake refuses an
//! empty-`ext` (Phase 1) peer on the **intra** port once the era is on while
//! still admitting it on the **client** port (the CLI and external clients
//! are not members and never advertise an `ext`).
//!
//! Every wait is a converged-or-timeout poll, and the whole test is
//! timeout-guarded (`ProdEnv` determinism guarantees do not apply, so a hang
//! must fail loudly rather than stall CI).
//!
//! The real `MAX_SUPPORTED` is 3 (G-01 stage G-c shipped `Gate::GlobalTables`
//! at 2, stage G-d `Gate::MrecReplication` at 3), so every real binary
//! advertises `[1, 3]` and live Finalizes to 2 and then 3 succeed here: the
//! era, the view, the leader-only routing and the finalizes that open
//! `Gate::GlobalTables` and `Gate::MrecReplication` on every node (a
//! data-only node's mirror included). The blocker matrix and the one-step
//! rules are proven over seeds in `sim_cluster_cluster_version.rs`.

use std::net::SocketAddr;
use std::time::Duration;

use animus_env::{CLIENT_PROTOCOL, exchange_preamble};
use animusd::{ClientRequest, ClientResponse, Node, StorageBackend, read_frame, write_frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// One HTTP/1.0 request to the admin endpoint; `(status, JSON body)`.
async fn admin(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> (u16, serde_json::Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.0\r\nHost: animus\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
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
    (
        status,
        serde_json::from_str(payload).expect("admin body is JSON"),
    )
}

/// A dial that advertises NO `ext`, exactly what a Phase 1 binary (or the
/// CLI) does: the fused preamble exchange with an empty extension area.
async fn phase1_dial(addr: SocketAddr) -> std::io::Result<TcpStream> {
    let mut stream = TcpStream::connect(addr).await?;
    exchange_preamble(&mut stream, &CLIENT_PROTOCOL, Duration::from_secs(10))
        .await
        .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
    Ok(stream)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_era_starts_over_real_sockets_and_the_admin_surface_works() {
    timeout(Duration::from_secs(180), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) =
            support::bring_up_deadline(3, dir.path(), Duration::from_secs(60)).await;
        support::await_bootstrap(&nodes).await;

        // The era starts by itself: every node was bound with its own
        // handshake `ext`, the leader observed every member, proposed the
        // initial reports, and each node's feeder read the era from
        // replicated state.
        timeout(Duration::from_secs(60), async {
            loop {
                if nodes.iter().all(|n| n.features().era_active()) {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the version era never became active on every node");

        // GET /admin/cluster-version converges on every node: era on, version
        // 1, every member reported, and Finalize to 2 is within reach (every
        // binary's range is [MIN_SUPPORTED, MAX_SUPPORTED] = [1, 3]).
        for node in &nodes {
            let addr = node.admin_addr();
            timeout(Duration::from_secs(30), async {
                loop {
                    let (status, v) = admin(addr, "GET", "/admin/cluster-version", None).await;
                    if status == 200
                        && v["era_active"] == true
                        && v["active"] == 1
                        && v["nodes"].as_array().is_some_and(|n| {
                            n.len() == 3 && n.iter().all(|x| x["reported"] == true)
                        })
                    {
                        // Every member's range is [1,3], so nothing blocks the
                        // next step (target 2) and the safe target (the highest
                        // version every member supports) is the binary's max.
                        assert_eq!(v["can_finalize"], true, "{v}");
                        assert_eq!(v["target"], 2, "{v}");
                        assert_eq!(v["safe_target"], 3, "{v}");
                        assert!(v["blockers"].as_array().unwrap().is_empty(), "{v}");
                        return;
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .expect("GET /admin/cluster-version never showed the era with every member reported");
        }

        // Finalize: a follower is refused as not-the-leader; the leader
        // accepts (one step, 1 -> 2) and every node then opens
        // `Gate::GlobalTables`.
        let leader = nodes
            .iter()
            .position(Node::is_control_leader)
            .expect("leader");
        let follower = (0..nodes.len()).find(|i| *i != leader).unwrap();
        let (status, v) = admin(
            nodes[follower].admin_addr(),
            "POST",
            "/admin/cluster-version/finalize",
            Some("{}"),
        )
        .await;
        assert_eq!(status, 409, "{v}");
        assert!(
            v["error"]
                .as_str()
                .unwrap()
                .contains("not the control-plane leader"),
            "{v}"
        );
        // A beyond-one-step target is refused by name first.
        let (status, v) = admin(
            nodes[leader].admin_addr(),
            "POST",
            "/admin/cluster-version/finalize",
            Some(r#"{"to":3}"#),
        )
        .await;
        assert_eq!(status, 400, "{v}");
        assert_eq!(nodes[leader].metadata().cluster_version, 1);
        let (status, v) = admin(
            nodes[leader].admin_addr(),
            "POST",
            "/admin/cluster-version/finalize",
            Some(r#"{"to":2,"expected":1}"#),
        )
        .await;
        assert_eq!(status, 200, "{v}");
        assert_eq!(v["active"], 2, "{v}");
        timeout(Duration::from_secs(60), async {
            loop {
                if nodes.iter().all(|n| {
                    n.features()
                        .is_open(animus_control::version::Gate::GlobalTables)
                }) {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("Gate::GlobalTables never opened on every node after the finalize");

        // The second step, 2 -> 3, opens `Gate::MrecReplication` (G-01 stage
        // G-d) on every node, a data-only node's mirror included.
        let (status, v) = admin(
            nodes[leader].admin_addr(),
            "POST",
            "/admin/cluster-version/finalize",
            Some(r#"{"to":3,"expected":2}"#),
        )
        .await;
        assert_eq!(status, 200, "{v}");
        assert_eq!(v["active"], 3, "{v}");
        timeout(Duration::from_secs(60), async {
            loop {
                if nodes.iter().all(|n| {
                    n.features()
                        .is_open(animus_control::version::Gate::MrecReplication)
                }) {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("Gate::MrecReplication never opened on every node after the finalize");

        // CHS1: an empty-`ext` dial is still welcome on the CLIENT port (the
        // CLI and external clients) ...
        let mut s = phase1_dial(nodes[0].client_addr())
            .await
            .expect("client-port dial");
        write_frame(&mut s, &ClientRequest::Status)
            .await
            .expect("send");
        match read_frame::<ClientResponse, _>(&mut s).await {
            Ok(Some(ClientResponse::Status { .. })) => {}
            other => panic!("client port must still serve a Phase 1 dial: {other:?}"),
        }
        // ... and refused on the INTRA port (node-to-node), where the era
        // requires every peer to advertise one. The refusal is a closed
        // connection, never a frame.
        let mut s = phase1_dial(nodes[0].intra_addr())
            .await
            .expect("intra preamble exchange");
        let _ = write_frame(&mut s, &ClientRequest::Status).await;
        match read_frame::<ClientResponse, _>(&mut s).await {
            Ok(None) | Err(_) => {}
            Ok(Some(resp)) => panic!("intra port served an empty-ext peer: {resp:?}"),
        }

        // A data-only node (no local control Raft, no apply task) learns the
        // era through its metadata mirror.
        let seeds: Vec<SocketAddr> = nodes.iter().map(Node::intra_addr).collect();
        let data = support::join_data_fresh_deadline(
            &seeds,
            3,
            dir.path(),
            StorageBackend::default(),
            Duration::from_secs(60),
        )
        .await;
        timeout(Duration::from_secs(60), async {
            loop {
                if data.features().era_active() {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the data-only node never learned the era through its mirror");
        let (status, v) = admin(data.admin_addr(), "GET", "/admin/cluster-version", None).await;
        assert_eq!(status, 200, "{v}");
        assert_eq!(v["era_active"], true, "{v}");
        timeout(Duration::from_secs(60), async {
            loop {
                if data
                    .features()
                    .is_open(animus_control::version::Gate::GlobalTables)
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the data-only node never opened Gate::GlobalTables through its mirror");
        timeout(Duration::from_secs(60), async {
            loop {
                if data
                    .features()
                    .is_open(animus_control::version::Gate::MrecReplication)
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the data-only node never opened Gate::MrecReplication through its mirror");
        let (status, v) = admin(
            data.admin_addr(),
            "POST",
            "/admin/cluster-version/finalize",
            Some("{}"),
        )
        .await;
        assert_eq!(
            status, 409,
            "a data-only node is never the control leader: {v}"
        );

        data.shutdown_graceful().await;
        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("cluster_version_prod timed out");
}
