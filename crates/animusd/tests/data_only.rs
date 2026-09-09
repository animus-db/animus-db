//! `animusd data` — the data-only process with `ControlHandle::Remote` (ADR
//! 0035 PR4): no local control `RaftCore` at all, reaching a
//! separately-deployed control plane exclusively over the network.
//!
//! **Trimmed (ADR 0061 rung L, C-12 PR 4a).** Of this file's original five
//! tests, four converted whole to `SimCluster`
//! (`sim_cluster_control_data_split.rs`'s own classification table has the
//! per-test mapping): `schema_ddl_via_a_data_node_relays_and_commits`,
//! `data_node_falls_over_to_a_remaining_control_seed`, `data_node_restart_
//! rejoins_and_serves_reads_again`, and `data_node_observes_live_control_
//! voters_after_a_fresh_fetch` are all fully converted. Kept here whole:
//! `split_cluster_serves_reads_and_writes_across_data_nodes` — its own
//! real-socket residual is the `/admin/storage/control` and `/admin/
//! system-table` availability contract (`ctx.control_storage`, always
//! `None` under `SimCluster` regardless of role — a fixture-wide capability
//! gap unrelated to this rung), asserted here on BOTH sides of a genuine
//! split deployment (present on every control node, absent on every data
//! node) — the cross-data-node routing and fixed-control-node forwarding
//! it also covers are otherwise fully reproduced by `sim_cluster_control_
//! data_split.rs`'s `mixed_cluster_put_via_control_node_forwards_to_data_
//! node`/`split_cluster_serves_reads_and_writes_across_data_nodes` sim
//! siblings. See that module's own doc for the full classification.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, read_frame};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;
use support::{await_data_nodes_active, await_leader, bring_up_split};

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// One HTTP/1.0 GET to the admin endpoint; returns `(status, parsed JSON)`.
async fn admin_get(addr: SocketAddr, path: &str) -> (u16, serde_json::Value) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let value: serde_json::Value = serde_json::from_str(payload)
        .unwrap_or_else(|e| panic!("admin body is not JSON ({e}): {payload}"));
    (status, value)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn split_cluster_serves_reads_and_writes_across_data_nodes() {
    timeout(Duration::from_secs(90), async {
        let dir = support::panic_safe_tempdir();
        let (control_nodes, data_nodes, config) = bring_up_split(3, 2, dir.path()).await;
        await_leader(&control_nodes).await;

        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (3..5).map(animusd::config::node_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        // A `Put` issued against ONE data node's client port; a `Get`
        // against the OTHER data node — proving both provisioning (the
        // table's tablet is created with the two `Active` data members as
        // replicas) and cross-data-node routing/forwarding work with **no**
        // control node involved in the data path at all.
        timeout(Duration::from_secs(20), async {
            loop {
                let put = call(
                    data_nodes[0].client_addr(),
                    ClientRequest::Put {
                        key: b"split-key".to_vec(),
                        value: b"split-val".to_vec(),
                        table: "split_t".to_string(),
                    },
                )
                .await;
                if matches!(put, ClientResponse::PutOk) {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("put via a data node did not succeed in 20s");

        let get = call(
            data_nodes[1].client_addr(),
            ClientRequest::Get {
                key: b"split-key".to_vec(),
                table: "split_t".to_string(),
                stale: false,
            },
        )
        .await;
        assert_eq!(
            get,
            ClientResponse::Value(Some(b"split-val".to_vec())),
            "read-back via the other data node"
        );

        // A `Put`/`Get` issued against a **single, fixed control-only**
        // node's client port — that node hosts zero local CP replicas of
        // anything (`ClientCtx.data == None`), so `resolve_cp_route` must
        // take its no-local-replica "forward to any known route" branch to
        // reach the data fleet at all. `control_only.rs`'s own mixed-cluster
        // test predates PR4 and only exercises this against an ADR 0030
        // growth node; this is the genuine-`Remote`-data-fleet version of
        // the same assertion.
        //
        // This used to need a round-robin across every node's client
        // address: a zero-replica node's fallback-forward target was a
        // *fixed* replica pick (the first one in the tablet's replica list),
        // not necessarily the group's actual current leader, and a forwarded
        // op landing on a non-leader replica errored ("not the leader here")
        // forever rather than re-forwarding (routing is bounded to one hop).
        // The forwarder (`ClientCtx::cp_forward`) now retries a "not the
        // leader here" refusal at the refusing node's own embedded leader
        // hint, then at the tablet's other known replicas, so a single fixed
        // control node resolves deterministically — this is the intended
        // regression proof that the hazard is closed (see the root
        // `CLAUDE.md`'s "zero-replica blind-forward" entry).
        let fixed_control = control_nodes[0].client_addr();
        timeout(Duration::from_secs(20), async {
            loop {
                let put = call(
                    fixed_control,
                    ClientRequest::Put {
                        key: b"via-control-key".to_vec(),
                        value: b"via-control-val".to_vec(),
                        table: "split_t".to_string(),
                    },
                )
                .await;
                if matches!(put, ClientResponse::PutOk) {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("put via a fixed control node did not succeed in 20s");
        timeout(Duration::from_secs(20), async {
            loop {
                if let ClientResponse::Value(Some(v)) = call(
                    fixed_control,
                    ClientRequest::Get {
                        key: b"via-control-key".to_vec(),
                        table: "split_t".to_string(),
                        stale: false,
                    },
                )
                .await
                    && v == b"via-control-val"
                {
                    return;
                }
                sleep(Duration::from_millis(150)).await;
            }
        })
        .await
        .expect("read-back via a fixed control node never observed the written value in 20s");

        // The data-only nodes' own `/admin/health` reports data-plane
        // readiness with `is_control_leader` hardcoded false (no local
        // control RaftCore to ever lead) — falls out of `ControlHandle::
        // Remote::is_leader()` returning `false` unconditionally, no
        // Remote-specific code in `admin.rs` at all. `hosts_cp` is polled,
        // not snapshotted immediately: the Get above can succeed via a
        // one-hop forward before *this* node's own tablet-host reconciler
        // has finished standing its own replica of the group up (an
        // eventual, not immediate, property of a just-provisioned tablet).
        for n in &data_nodes {
            let (status, health) = admin_get(n.admin_addr(), "/admin/health").await;
            assert_eq!(status, 200, "admin/health on {}", n.admin_addr());
            assert_eq!(
                health["is_control_leader"], false,
                "a data-only node never leads the control plane: {health}"
            );

            // ADR 0038 PR4: a data-only node has no local control role at
            // all (`ControlHandle::Remote`), so it has no system-keyspace
            // engine to surface here — a plain, honest "not available",
            // never a 404 (mirrors every other `/admin/storage/*` route's
            // absence-is-data shape).
            let (status, ctl_storage) = admin_get(n.admin_addr(), "/admin/storage/control").await;
            assert_eq!(status, 200, "admin/storage/control on {}", n.admin_addr());
            assert_eq!(
                ctl_storage["available"], false,
                "a data-only node has no local control-plane engine: {ctl_storage}"
            );

            // The system-table browse surface (plan-syskv-ui) is the same
            // absence-is-data shape as `/admin/storage/control` right above
            // — a data-only node has no `ctx.control_storage` engine to
            // scan, never a 404.
            let (status, syst) = admin_get(n.admin_addr(), "/admin/system-table").await;
            assert_eq!(status, 200, "admin/system-table on {}", n.admin_addr());
            assert_eq!(
                syst["available"], false,
                "a data-only node has no system keyspace to browse: {syst}"
            );
        }
        // The dual, on the control-only side of the same split deployment:
        // each control node's own dedicated system-keyspace engine is
        // available (control_only.rs covers this in more depth; asserted
        // here too since this fixture is the genuine-split-deployment one).
        for n in &control_nodes {
            let (status, ctl_storage) = admin_get(n.admin_addr(), "/admin/storage/control").await;
            assert_eq!(status, 200, "admin/storage/control on {}", n.admin_addr());
            assert_eq!(
                ctl_storage["available"], true,
                "a control-only node has its own dedicated system-keyspace engine: {ctl_storage}"
            );

            // A control-only node's own system-table is available and lists
            // at least its own membership rows (control_only.rs covers the
            // full endpoint contract; this just proves it composes with a
            // genuine split deployment, matching the storage/control dual
            // above).
            let (status, syst) = admin_get(n.admin_addr(), "/admin/system-table").await;
            assert_eq!(status, 200, "admin/system-table on {}", n.admin_addr());
            assert_eq!(
                syst["available"], true,
                "a control-only node has a system keyspace to browse: {syst}"
            );
            assert!(
                syst["count"].as_u64().is_some_and(|c| c > 0),
                "a running control-only node's system keyspace has at least member rows: {syst}"
            );
        }
        timeout(Duration::from_secs(20), async {
            loop {
                let mut all_host = true;
                for n in &data_nodes {
                    let (_, health) = admin_get(n.admin_addr(), "/admin/health").await;
                    if health["hosts_cp"] != true {
                        all_host = false;
                    }
                }
                if all_host {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("both data nodes never converged to hosting the tablet's CP group (20s)");

        // Every node has one id and one internal address regardless of role
        // (ADR 0040 PR1) — a data-only node included, even though it has no
        // local control `RaftCore`.
        for n in &data_nodes {
            let (status, cfg) = admin_get(n.admin_addr(), "/admin/config").await;
            assert_eq!(status, 200);
            assert!(
                !cfg["node_id"].is_null(),
                "a data-only node still has its own id: {cfg}"
            );
            assert!(
                !cfg["addrs"]["internal"].is_null(),
                "a data-only node still has its own internal address: {cfg}"
            );
        }

        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
        let _ = config;
    })
    .await
    .expect("split_cluster_serves_reads_and_writes_across_data_nodes timed out");
}
