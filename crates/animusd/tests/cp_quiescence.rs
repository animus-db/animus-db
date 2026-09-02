//! ADR 0044 phase-1 PR4: the **critical `ProdEnv` liveness test** for
//! quiescence — the one property `SimEnv` structurally cannot prove (real
//! OS-thread/timer liveness, not just logic/ordering — see the root
//! `CLAUDE.md`'s "`SimEnv` proves logic and ordering, not real-thread
//! liveness" lesson). The pure-core state machine and the reconciler's
//! wake-on-`down` (fork H) are already proven deterministically
//! (`animus-control/tests/quiescence.rs`, `animus-cp-data/tests/
//! quiescence.rs`, and `animus-cp-data/tests/reconciler_corpus.rs`'s
//! `quiesced_group_wakes_when_a_replica_goes_down` scenario); this file
//! proves the same shape actually holds over real time/threads/sockets, and
//! specifically exercises **PR4 item 1** — `resolve_cp_route`'s
//! wake-on-demand — since here the surviving replicas are never told
//! anything is `down` (no reconciler-driven fork H wake is even possible:
//! there is no failure detector marking anyone `Down` in this test's short
//! window), so the *only* thing that can un-quiesce a surviving follower is
//! a genuine client write attempt reaching it through `resolve_cp_route`.
//!
//! Test-only entry point: [`animusd::start_cluster_with_quiesce_after`] opts
//! every data-plane CP group into quiescence with a short idle threshold —
//! no CLI flag exists for this yet (PR7 adds `--quiesce-after SECS`); the
//! underlying knob (`animus_cp_data::host::Reconciler::enable_quiescence`)
//! is exactly what that flag will wire in production.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{
    ClientRequest, ClientResponse, Node, StorageBackend, bind_cluster, read_frame,
    start_cluster_with_quiesce_after,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Short relative to this test's own idle-wait margin (below), long enough
/// that ordinary election/heartbeat/bootstrap settle traffic has long died
/// down before the idle clock starts counting for real — mirrors every
/// other quiescence test's own `QUIESCE_AFTER` choice.
const QUIESCE_AFTER: Duration = Duration::from_millis(300);

async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(30), ready)
        .await
        .expect("cluster did not bootstrap within 30s");
}

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect to node");
    animusd::write_frame(&mut stream, &req)
        .await
        .expect("send request");
    read_frame(&mut stream)
        .await
        .expect("read reply")
        .expect("a reply")
}

/// Retry a put against every client address in turn until one accepts it —
/// mirrors `cp_reconfigure.rs`'s own `put` helper. Tolerant of `Error`
/// (still electing, or a connection to a just-killed node) as a retryable
/// blip, never a hard failure.
async fn put(clients: &[SocketAddr], key: &[u8], value: &[u8], secs: u64, table: &str) {
    let w = async {
        loop {
            for &c in clients {
                let resp = tokio::time::timeout(
                    Duration::from_secs(2),
                    call(
                        c,
                        ClientRequest::Put {
                            key: key.to_vec(),
                            value: value.to_vec(),
                            table: table.to_string(),
                        },
                    ),
                )
                .await;
                if let Ok(ClientResponse::PutOk) = resp {
                    return;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(secs), w)
        .await
        .unwrap_or_else(|_| panic!("write of {key:?} never committed within {secs}s"));
}

async fn admin_get(addr: SocketAddr, path: &str) -> Value {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let request = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (_head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    serde_json::from_str(payload).expect("admin body is JSON")
}

/// The local group's `(is_leader, voter count)` for this test's one and only
/// tablet, from this node's node-local `/admin/raftkv` view (one group per
/// node) — mirrors `cp_reconfigure.rs`'s own `group_view` (duplicated rather
/// than shared: that helper lives in a sibling `tests/` binary, each of
/// which compiles as its own separate crate). This test never provisions
/// more than one table/tablet, so the first (only) entry is unambiguous —
/// unlike `cp_reconfigure.rs`, which hardcodes `tablet == 1`, this doesn't
/// assume anything about tablet-id minting.
async fn group_view(admin_addr: SocketAddr) -> Option<(bool, usize)> {
    let v = admin_get(admin_addr, "/admin/raftkv").await;
    let g = v["groups"].as_array()?.first()?;
    let voters = g["voters"].as_array()?.len();
    Some((g["is_leader"].as_bool().unwrap_or(false), voters))
}

const TABLE: &str = "quiesce_t";

/// The critical property: a quiesced group's surviving replicas are never
/// stranded by their leader's death, even though the *only* wake this test
/// gives them is an ordinary client write attempt (`resolve_cp_route`'s
/// wake-on-demand, PR4 item 1) — nothing here ever marks anyone `Down`, so
/// fork H's reconciler-driven wake plays no part in this specific test.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn write_after_leader_kill_of_a_quiesced_group_converges() {
    let dir = support::panic_safe_tempdir();
    let ip = "127.0.0.1".parse().unwrap();
    let bound = bind_cluster(3, ip, dir.path()).await.unwrap();
    let clients: Vec<SocketAddr> = bound.iter().map(animusd::BoundNode::client_addr).collect();
    let admins: Vec<SocketAddr> = bound.iter().map(animusd::BoundNode::admin_addr).collect();

    let nodes =
        start_cluster_with_quiesce_after(bound, StorageBackend::default(), None, QUIESCE_AFTER)
            .await
            .unwrap();
    await_bootstrap(&nodes).await;

    // ADR 0023: a fresh cluster has no data tablet — provision it by writing
    // first (auto-provisioned on the first write).
    put(&clients, b"k0", b"v0", 30, TABLE).await;

    // Wait until the group has formed with all three voters and elected a
    // leader on some node.
    let leader_idx = {
        let formed = async {
            loop {
                for (i, addr) in admins.iter().enumerate() {
                    if let Some((true, 3)) = group_view(*addr).await {
                        return i;
                    }
                }
                sleep(Duration::from_millis(100)).await;
            }
        };
        timeout(Duration::from_secs(30), formed)
            .await
            .expect("CP group did not form with 3 voters + a leader within 30s")
    };

    // Idle well past `QUIESCE_AFTER` on every replica — no traffic of any
    // kind touches this tablet in the meantime. Real wall-clock time (this
    // is the whole point: a real quiesced consensus loop genuinely parks
    // with no timer, so nothing manufactured here can substitute for
    // actually waiting).
    sleep(QUIESCE_AFTER * 6).await;

    // Kill the leader's entire node (aborts every task it owns, including
    // its own consensus loop) — the survivors' driver tasks run on
    // independent processes/ports, untouched.
    nodes[leader_idx].shutdown();

    let survivor_clients: Vec<SocketAddr> = clients
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader_idx)
        .map(|(_, c)| *c)
        .collect();
    let survivor_admins: Vec<SocketAddr> = admins
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader_idx)
        .map(|(_, a)| *a)
        .collect();

    // A plain client write through a surviving node: `resolve_cp_route`
    // wakes this node's own local handle unconditionally before deciding
    // (PR4 item 1) — if that wake is missing (or the reconciler's fork H
    // wake were somehow required here, which it is not — nothing marks the
    // dead node `Down` within this test's window), a quiesced follower's own
    // consensus loop would never notice its leader died (no timer at all)
    // and this write would time out.
    put(&survivor_clients, b"k1", b"v1", 60, TABLE).await;

    // And a genuinely new leader must have taken over among the survivors.
    let new_leader_elected = async {
        loop {
            for addr in &survivor_admins {
                if let Some((true, _)) = group_view(*addr).await {
                    return;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), new_leader_elected)
        .await
        .expect("no survivor ever became the new leader within 20s");

    // Read the just-written key back through the other survivor too — the
    // recovered group is genuinely serving, not just accepting one lucky
    // write.
    let got = call(
        survivor_clients[0],
        ClientRequest::Get {
            key: b"k1".to_vec(),
            table: TABLE.to_string(),
            stale: false,
        },
    )
    .await;
    assert_eq!(
        got,
        ClientResponse::Value(Some(b"v1".to_vec())),
        "the recovered, previously-quiesced group must serve its own just-committed write"
    );

    for (i, node) in nodes.iter().enumerate() {
        if i != leader_idx {
            node.shutdown_graceful().await;
        }
    }
}

/// Companion sanity check: with quiescence enabled, ordinary reads/writes on
/// a group nobody ever lets go idle long enough still work exactly as
/// without it (PR3 already proves this deterministically; this is a cheap
/// `ProdEnv` corroboration that the production wiring itself — the new
/// `quiesce_after` plumbing through `BoundNode::start_with_growth` and
/// `host::Reconciler` — introduces no regression on the ordinary path).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn quiescence_enabled_does_not_disrupt_ordinary_traffic() {
    let dir = support::panic_safe_tempdir();
    let ip = "127.0.0.1".parse().unwrap();
    let bound = bind_cluster(3, ip, dir.path()).await.unwrap();
    let clients: Vec<SocketAddr> = bound.iter().map(animusd::BoundNode::client_addr).collect();

    let nodes =
        start_cluster_with_quiesce_after(bound, StorageBackend::default(), None, QUIESCE_AFTER)
            .await
            .unwrap();
    await_bootstrap(&nodes).await;

    for i in 0..20u32 {
        let key = format!("k{i}").into_bytes();
        let value = format!("v{i}").into_bytes();
        put(&clients, &key, &value, 20, TABLE).await;
        let got = call(
            clients[i as usize % clients.len()],
            ClientRequest::Get {
                key: key.clone(),
                table: TABLE.to_string(),
                stale: false,
            },
        )
        .await;
        assert_eq!(got, ClientResponse::Value(Some(value)));
        // A brief gap between writes so at least some of this run's traffic
        // crosses the quiesce threshold — proving a write that lands *after*
        // the group settled still commits cleanly, not just back-to-back
        // traffic that never gives it a chance to idle.
        if i % 5 == 0 {
            sleep(QUIESCE_AFTER * 2).await;
        }
    }

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}
