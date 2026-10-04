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
//! The real `MAX_SUPPORTED` is 1 (no gate has shipped), so Finalize cannot
//! succeed here: the success path, the blocker matrix and the one-step rules
//! are proven over seeds in `sim_cluster_cluster_version.rs` with synthetic
//! `[1, 2]` binaries. What this test pins is the live wiring: the era, the
//! view, the by-name refusal and the leader-only routing.

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
        // 1, every member reported, and Finalize is out of reach (every
        // binary's max is the current MAX_SUPPORTED).
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
                        // Every member's range is [1,1] (the real MAX_SUPPORTED),
                        // so each one is a named blocker for target 2 and the
                        // safe target is the active version itself.
                        assert_eq!(v["can_finalize"], false, "{v}");
                        assert_eq!(v["safe_target"], 1, "{v}");
                        let blockers = v["blockers"].as_array().unwrap();
                        assert_eq!(blockers.len(), 3, "{v}");
                        assert!(
                            blockers
                                .iter()
                                .all(|b| b["reason"] == "range [1,1] excludes target 2"),
                            "{v}"
                        );
                        return;
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .expect("GET /admin/cluster-version never showed the era with every member reported");
        }

        // Finalize: a follower is refused as not-the-leader; the leader
        // refuses by name (this binary supports only version 1) and the
        // cluster stays at 1.
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
        let (status, v) = admin(
            nodes[leader].admin_addr(),
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
                .contains("supports cluster versions up to 1"),
            "{v}"
        );
        assert_eq!(nodes[leader].metadata().cluster_version, 1);

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
