//! `split_cluster.rs`-style multi-process scenario for the control-plane
//! membership-change stack (ADR 0037 PR5, plan §13): a **genuine** split
//! deployment (control-only + data-only processes, no combined-mode node
//! anywhere) grows its control quorum by one at runtime through the real
//! admin HTTP surface, then replaces a voter (remove a genuinely dead one,
//! add a fresh one) — all while continuous data-plane traffic keeps flowing
//! through the whole scenario. This is the end-to-end proof that runtime
//! control-plane membership change composes with a real split deployment,
//! not just the single-action admin-level coverage in
//! `control_membership_admin.rs`.
//!
//! Real TCP/time throughout — every wait is a bounded, converged-or-timeout
//! poll, never a fixed sleep used as an assertion.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animusd::config::NodeRole;
use animusd::{
    ClientRequest, ClientResponse, ClusterConfig, MetaCommand, Node, NodeAddrs, RoleAddrs,
    read_frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;
use animus_env::nid;
use support::{await_data_nodes_active, await_leader, bring_up_split};

async fn call(addr: SocketAddr, req: ClientRequest) -> Option<ClientResponse> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    animusd::write_frame(&mut stream, &req).await.ok()?;
    read_frame(&mut stream).await.ok()?
}

/// Try every client address in `clients` (round-robin) until one accepts the
/// write (mirrors `tests/split_cluster.rs`'s `put`).
async fn put(clients: &[SocketAddr], table: &str, key: &[u8], value: &[u8], secs: u64) {
    let mut last: Option<ClientResponse> = None;
    let w = async {
        loop {
            for &c in clients {
                let resp = call(
                    c,
                    ClientRequest::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        table: table.to_string(),
                    },
                )
                .await;
                if let Some(ClientResponse::PutOk) = &resp {
                    return;
                }
                last = resp;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(secs), w)
        .await
        .unwrap_or_else(|_| {
            panic!("write of {table}/{key:?} never committed; last reply: {last:?}")
        });
}

async fn await_value(clients: &[SocketAddr], table: &str, key: &[u8], want: &[u8], secs: u64) {
    let p = async {
        loop {
            for &c in clients {
                if let Some(ClientResponse::Value(Some(v))) = call(
                    c,
                    ClientRequest::Get {
                        key: key.to_vec(),
                        table: table.to_string(),
                        stale: false,
                    },
                )
                .await
                    && v == want
                {
                    return;
                }
            }
            sleep(Duration::from_millis(150)).await;
        }
    };
    timeout(Duration::from_secs(secs), p)
        .await
        .unwrap_or_else(|_| panic!("key {table}/{key:?} never read back as {want:?}"));
}

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed JSON)`.
async fn admin(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> (u16, serde_json::Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.0\r\nHost: animus\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );
    stream.write_all(request.as_bytes()).await.expect("send");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .expect("status line");
    let value: serde_json::Value = serde_json::from_str(payload).unwrap_or(serde_json::Value::Null);
    (status, value)
}

async fn control_members(admin_addr: SocketAddr) -> (u16, serde_json::Value) {
    admin(admin_addr, "GET", "/admin/control/members", None).await
}

async fn add_control_member(
    admin_addr: SocketAddr,
    node: u64,
    addr: SocketAddr,
) -> (u16, serde_json::Value) {
    let body =
        serde_json::json!({"node": nid(node).to_string(), "addr": addr.to_string()}).to_string();
    admin(admin_addr, "POST", "/admin/control/member/add", Some(&body)).await
}

async fn remove_control_member(admin_addr: SocketAddr, node: u64) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"node": nid(node).to_string()}).to_string();
    admin(
        admin_addr,
        "POST",
        "/admin/control/member/remove",
        Some(&body),
    )
    .await
}

fn voters_of(body: &serde_json::Value) -> Option<Vec<String>> {
    body["voters"].as_array().map(|a| {
        a.iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect()
    })
}

/// Join a **quiet non-voter** control-only node to the split deployment
/// described by `config` (mirrors `control_membership_admin.rs`'s
/// `join_control_nonvoter`, duplicated here per this codebase's existing
/// per-test-file-helper convention): `peers`/`control_ids` cover only
/// `config`'s ORIGINAL control-only entries — deliberately not updated as
/// the live group grows/shrinks, since a fresh joiner only needs ONE
/// reachable original voter to relay its own self-registration through, and
/// (barring a total wipeout of the original three) at least one always
/// survives in every scenario this test drives.
async fn join_control_nonvoter(
    config: &ClusterConfig,
    new_control_id: u64,
    dir: &Path,
) -> (Node, RoleAddrs) {
    // **Issue #406/#450 (Bug A)**: ports are picked ONCE, outside the retry
    // loop, and reused on every `bind_control` retry — never re-randomized
    // per attempt. `start_control_with`'s own one-shot self-registration
    // (`spawn_common_tail`, ADR 0040 Decision C) is keyed on this id's
    // `NodeAddrs`; re-picking fresh ports on a retry after a *prior* attempt
    // in the same call had already gotten far enough to self-register would
    // make that retry's own claim collide against its own earlier one —
    // `join_fresh_deadline`'s sibling fix in `tests/support/mod.rs` has the
    // full account. This helper's own registration only ever fires *after*
    // a successful bind (so today's bind-only retry loop can't reach that
    // state mid-loop), but keeping the same fixed-ports-outside-the-loop
    // shape here too avoids relying on that ordering staying true, and
    // matches the one other retry-the-whole-step-with-fresh-ports helper
    // this bug class was found in.
    let raw = support::free_addrs(6);
    let addrs = RoleAddrs {
        id: nid(new_control_id),
        role: NodeRole::Control,
        internal: raw[0],
        client: raw[1],
        dynamo: raw[2],
        admin: raw[3],
        intra: raw[4],
        console: raw[5],
        advertise_host: None,
        tls: None,
    };
    for attempt in 0..16 {
        let bound = match animusd::Node::bind_control(
            nid(new_control_id),
            addrs.clone(),
            dir.join(format!("grow-{new_control_id}-{attempt}")),
        )
        .await
        {
            Ok(b) => b,
            Err(_) => {
                sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let mut client_route: BTreeMap<animus_env::NodeId, String> = BTreeMap::new();
        for (i, a) in config.nodes.iter().enumerate() {
            client_route.insert(animusd::config::node_id(i), a.client.to_string());
        }
        let mut intra_route: BTreeMap<animus_env::NodeId, String> = BTreeMap::new();
        for (i, a) in config.nodes.iter().enumerate() {
            intra_route.insert(animusd::config::node_id(i), a.intra.to_string());
        }
        let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();
        let node = bound
            .start_control_with(
                config.peer_book(),
                config.control_ids(),
                client_route,
                intra_route,
                admin_addrs,
                animusd::StorageBackend::Memory,
                animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
                animusd::SegmentStoreConfig::default(),
                animusd::BackupStoreConfig::default(),
                animusd::DEFAULT_STREAM_RETENTION,
            )
            .await
            .expect("open the growth control-only node's system-keyspace engine");
        return (node, addrs);
    }
    panic!("could not bind a growth control-only node (id {new_control_id}) after retries");
}

/// Poll `GET /admin/control/members` on every address in `probes` until each
/// reports exactly `want` as its voter set.
async fn await_voters_everywhere(probes: &[SocketAddr], want: &[u64], secs: u64, what: &str) {
    let want: Vec<String> = want.iter().map(|&n| nid(n).to_string()).collect();
    for &addr in probes {
        let converged = async {
            loop {
                let (status, body) = control_members(addr).await;
                if status == 200 && voters_of(&body).as_deref() == Some(want.as_slice()) {
                    return;
                }
                sleep(Duration::from_millis(150)).await;
            }
        };
        timeout(Duration::from_secs(secs), converged)
            .await
            .unwrap_or_else(|_| panic!("{what}: node at {addr} never converged to {want:?}"));
    }
}

/// Grow the control quorum by one control-only voter at runtime, then
/// replace one of the ORIGINAL voters (remove it once dead, add a fresh
/// replacement) — all against a genuine split deployment, with continuous
/// data-plane writes spanning the entire scenario.
#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn grow_then_replace_a_voter_over_a_split_deployment_with_live_data_traffic() {
    timeout(Duration::from_secs(150), async {
        let dir = support::panic_safe_tempdir();
        // 3 control-only (ids 0,1,2) + 2 data-only (ids 3,4 — ADR 0040 PR1:
        // one identity per node).
        let (control_nodes, data_nodes, config) = bring_up_split(3, 2, dir.path()).await;
        await_leader(&control_nodes).await;
        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (3..5).map(animusd::config::node_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        let data_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();

        // Continuous write traffic spanning the whole grow + replace flow —
        // each key gets its own bounded (10s) retry, so an attempt straddling
        // a control-plane admin action (which never touches the data plane's
        // own Raft groups) survives it rather than failing outright.
        let acked: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let acked_writer = acked.clone();
        let traffic_clients = data_clients.clone();
        let traffic = tokio::spawn(async move {
            for i in 0..40usize {
                let key = format!("membership-{i}").into_bytes();
                let value = format!("v{i}").into_bytes();
                let ok = timeout(Duration::from_secs(10), async {
                    loop {
                        for &c in &traffic_clients {
                            if let Some(ClientResponse::PutOk) = call(
                                c,
                                ClientRequest::Put {
                                    key: key.clone(),
                                    value: value.clone(),
                                    table: "membership_t".to_string(),
                                },
                            )
                            .await
                            {
                                return;
                            }
                        }
                        sleep(Duration::from_millis(75)).await;
                    }
                })
                .await
                .is_ok();
                if ok {
                    acked_writer.lock().unwrap().push(i);
                }
                sleep(Duration::from_millis(50)).await;
            }
        });

        // Let a few writes land (proving the table provisioned) before doing
        // anything to the control plane.
        timeout(Duration::from_secs(15), async {
            loop {
                if acked.lock().unwrap().len() >= 3 {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("no writes landed before the membership-change scenario started");

        let control_admin: Vec<SocketAddr> = control_nodes.iter().map(Node::admin_addr).collect();
        let data_admin: Vec<SocketAddr> = data_nodes.iter().map(Node::admin_addr).collect();

        // ---- Phase 1: grow the control quorum 3 -> 4 -----------------------
        let leader_idx = control_nodes
            .iter()
            .position(Node::is_control_leader)
            .expect("no control leader");
        // Free id: `bring_up_split(3, 2, ..)`'s 5 nodes already claim
        // `{0,1,2,3,4}`.
        let new_id = 5u64;
        let (grown, grown_addrs) = join_control_nonvoter(&config, new_id, dir.path()).await;
        let grown_admin = grown.admin_addr();
        let grown_control_addr = grown_addrs.internal;

        // Wait for `grown`'s own one-shot self-registration (`MetaCommand::
        // RegisterNode`'s CAS, ADR 0040 PR4) to land before adding it as a
        // control voter — calling `control/member/add` first races two
        // *independent* proposals for the same id's `node_addrs` entry
        // against each other, and the CAS correctly refuses whichever loses
        // (unlike the pre-ADR-0040 blind-overwrite behavior this
        // supersedes) — mirrors `control_membership_admin.rs`'s identical
        // "confirm it's up first" sequencing (plan §3's real operator
        // runbook).
        timeout(Duration::from_secs(15), async {
            loop {
                if control_nodes[leader_idx]
                    .metadata()
                    .node_addrs
                    .contains_key(&nid(new_id))
                {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("grown node's own self-registration never landed on the real cluster");

        let (status, body) =
            add_control_member(control_admin[leader_idx], new_id, grown_control_addr).await;
        assert_eq!(status, 200, "control/member/add (grow) failed: {body}");

        let mut probes = control_admin.clone();
        probes.push(grown_admin);
        probes.extend(data_admin.iter().copied());
        await_voters_everywhere(&probes, &[0, 1, 2, 5], 30, "post-grow").await;

        // Data traffic still flows after the grow.
        put(&data_clients, "membership_t", b"post-grow", b"ok", 20).await;
        await_value(&data_clients, "membership_t", b"post-grow", b"ok", 20).await;

        // ---- Phase 2: replace one of the ORIGINAL voters --------------------
        // Kill a non-leader ORIGINAL voter for good (not the just-grown one —
        // this proves the ADR 0037 §7 "replace a dead voter" runbook against
        // one of the founding voters, the harder/more realistic case).
        let current_leader = (0..3)
            .find(|&i| control_nodes[i].is_control_leader())
            .expect("no control leader among the original three");
        let dead_original = (0..3usize)
            .find(|&i| i != current_leader)
            .expect("a non-leader original voter exists");
        let dead_id = dead_original as u64;

        // We can't consume `control_nodes[dead_original]` out of the Vec
        // without disturbing indices used above, so shut it down in place —
        // `shutdown_graceful` takes `&self`.
        control_nodes[dead_original].shutdown_graceful().await;

        // Remove the dead voter via a live leader's admin port — succeeds;
        // may carry a quorum-loss warning depending on how many voters
        // remain (4 -> 3 here, comfortably above the down-to-1 threshold, so
        // no warning is actually expected, but this call's success is the
        // load-bearing assertion, not the warning field). Poll for whichever
        // survivor is leader rather than assuming it's still the original
        // one (killing a follower never disturbs leadership, but this stays
        // robust either way).
        let live_admin = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            loop {
                if let Some(&i) = [0usize, 1, 2]
                    .iter()
                    .find(|&&i| i != dead_original && control_nodes[i].is_control_leader())
                {
                    break control_admin[i];
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "no live original voter is leader after killing one"
                );
                sleep(Duration::from_millis(50)).await;
            }
        };

        let (status, body) = remove_control_member(live_admin, dead_id).await;
        assert_eq!(
            status, 200,
            "control/member/remove (replace) failed: {body}"
        );

        let mut probes_after_remove: Vec<SocketAddr> = control_admin
            .iter()
            .enumerate()
            .filter(|&(i, _)| i != dead_original)
            .map(|(_, &a)| a)
            .collect();
        probes_after_remove.push(grown_admin);
        probes_after_remove.extend(data_admin.iter().copied());
        let mut expected_after_remove: Vec<u64> = (0..3u64).filter(|&i| i != dead_id).collect();
        expected_after_remove.push(new_id);
        expected_after_remove.sort_unstable();
        await_voters_everywhere(
            &probes_after_remove,
            &expected_after_remove,
            30,
            "post-remove",
        )
        .await;

        // Add a brand-new voter (a fresh, still-unused id) as the replacement.
        let replacement_id = 6u64;
        let (replacement, replacement_addrs) =
            join_control_nonvoter(&config, replacement_id, dir.path()).await;
        let replacement_admin = replacement.admin_addr();
        let replacement_control_addr = replacement_addrs.internal;

        // Find whichever surviving node (the two live originals, or the
        // just-grown 4th) is currently leader — **before** waiting for the
        // replacement's own self-registration to land (issue #406/#450
        // fix): determine the actor `control/member/add` will actually be
        // called against first, then poll *that exact node's* own
        // `.metadata()`, mirroring phase 1's already-correct "grown node's
        // own self-registration" pattern above exactly. The old code polled
        // "any surviving node" and only afterwards separately picked
        // whichever happened to be leader by the time the poll returned —
        // those two reads could name different nodes, so the leader that
        // ultimately served `control/member/add` could still have an
        // ADR-0038-lagged local cache with no trace of the replacement's own
        // registration at all, forcing `admin_add_control_member` down its
        // racy "genuinely unclaimed" branch (see that function's own doc for
        // the full account this test now avoids by construction rather than
        // relying on the production fix alone).
        let (leader_idx_after_remove, leader_admin) = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            loop {
                if let Some(&i) = [0usize, 1, 2]
                    .iter()
                    .find(|&&i| i != dead_original && control_nodes[i].is_control_leader())
                {
                    break (Some(i), control_admin[i]);
                }
                if grown.is_control_leader() {
                    break (None, grown_admin);
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "no surviving control node reports itself leader before adding the replacement"
                );
                sleep(Duration::from_millis(100)).await;
            }
        };

        // Wait for `replacement`'s own one-shot self-registration
        // (`MetaCommand::RegisterNode`'s CAS, ADR 0040 PR4) to land on THAT
        // SAME leader node before adding it as a control voter — calling
        // `control/member/add` first races two *independent* proposals for
        // the same id's `node_addrs` entry against each other, and the CAS
        // correctly refuses whichever loses.
        timeout(Duration::from_secs(15), async {
            loop {
                let seen = match leader_idx_after_remove {
                    Some(i) => control_nodes[i]
                        .metadata()
                        .node_addrs
                        .contains_key(&nid(replacement_id)),
                    None => grown
                        .metadata()
                        .node_addrs
                        .contains_key(&nid(replacement_id)),
                };
                if seen {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("replacement node's own self-registration never landed on the real cluster");

        let (status, body) =
            add_control_member(leader_admin, replacement_id, replacement_control_addr).await;
        assert_eq!(
            status, 200,
            "control/member/add (replacement) failed: {body}"
        );

        let mut final_probes = probes_after_remove.clone();
        final_probes.push(replacement_admin);
        let mut expected_final = expected_after_remove.clone();
        expected_final.push(replacement_id);
        expected_final.sort_unstable();
        await_voters_everywhere(&final_probes, &expected_final, 30, "post-replace").await;

        // Data traffic still flows after the full replace cycle.
        put(&data_clients, "membership_t", b"post-replace", b"ok", 20).await;
        await_value(&data_clients, "membership_t", b"post-replace", b"ok", 20).await;

        // The continuous write loop spanning the whole scenario mostly
        // succeeded — a control-plane-only admin action never touches the
        // data plane's own Raft groups, so this is a strong bound, not a
        // generous one.
        traffic.await.expect("traffic task panicked");
        let acked_indices = acked.lock().unwrap().clone();
        assert!(
            acked_indices.len() >= 35,
            "too many writes failed outright across the grow+replace scenario: {} / 40 acked",
            acked_indices.len()
        );
        for &i in &acked_indices {
            let key = format!("membership-{i}").into_bytes();
            let value = format!("v{i}").into_bytes();
            await_value(&data_clients, "membership_t", &key, &value, 20).await;
        }

        grown.shutdown_graceful().await;
        replacement.shutdown_graceful().await;
        for (i, n) in control_nodes.into_iter().enumerate() {
            if i != dead_original {
                n.shutdown_graceful().await;
            }
        }
        for n in data_nodes {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("grow_then_replace_a_voter_over_a_split_deployment_with_live_data_traffic timed out");
}

/// **Issue #406/#450 (Bug B) regression.** `ClientCtx::admin_add_control_
/// member` used to gate its "already registered?" check on `Metadata::
/// members` alone — always false for a control-only id by design (that role
/// never claims `members`, see `animus-control::meta.rs`'s `RegisterNode`
/// doc) — so it *always* took the "genuinely unclaimed" branch and
/// re-derived a fresh `NodeAddrs` from this leader's own local
/// (ADR 0038 apply-task-lagged) `metadata_cached()` snapshot. If that
/// snapshot hadn't yet caught up to the growth node's own already-committed
/// self-registration, the reconstruction had empty `client`/`intra`/`admin`
/// fields, and the eventual apply-order mismatch was a **permanent**
/// "already claimed by a different registration" collision — or, in the
/// investigation's own observed worst case, the malformed proposal *won*
/// the race and left the node's address book durably blank.
///
/// Reproduces the exact "committed but not yet locally applied **on this
/// leader specifically**" window directly: `Metadata` is `DRIVER_APPLIED`
/// (ADR 0038) — a `RegisterNode` committed on the control leader's own Raft
/// log is only *applied* to that leader's own `Metadata` cache by a
/// separate, async apply task (`animus-control::node.rs`'s
/// `meta_apply_loop`) on its own schedule, tracked by `GET /admin/raft`'s
/// own `commit_index`/`engine_applied_index` fields. This test proposes
/// directly against the **leader itself**, polls that same `/admin/raft`
/// until `commit_index` has genuinely advanced (the entry is durably
/// committed, not merely locally appended and not yet acked by any
/// follower), and fires `control/member/add` at the exact reading where
/// `engine_applied_index` still lags `commit_index` — landing inside the
/// real gap between "this leader's log has committed the entry" and "this
/// leader's own apply task has caught up to it". A tiny bounded search over
/// distinct ids absorbs the (rare) case where the apply task wins a given
/// attempt before this test's own poll can observe the gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn admin_add_control_member_races_a_control_only_self_registration_and_still_converges() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        // 3 control-only voters, no data-only nodes needed — this race lives
        // entirely inside the control plane's own commit-vs-apply timing.
        let (control_nodes, _data_nodes, _config) = bring_up_split(3, 0, dir.path()).await;
        await_leader(&control_nodes).await;

        // `ProposeSchema` is `Surface::Intra` — the client listener refuses
        // it outright, so this must dial the intra port, not the client one.
        let control_intra: Vec<SocketAddr> = control_nodes.iter().map(Node::intra_addr).collect();
        let control_admin: Vec<SocketAddr> = control_nodes.iter().map(Node::admin_addr).collect();
        let leader_idx = control_nodes
            .iter()
            .position(Node::is_control_leader)
            .expect("no control leader");

        async fn raft_indices(admin_addr: SocketAddr) -> (u64, u64) {
            let (status, body) = admin(admin_addr, "GET", "/admin/raft", None).await;
            assert_eq!(status, 200, "GET /admin/raft failed: {body}");
            (
                body["commit_index"].as_u64().expect("commit_index field"),
                body["engine_applied_index"]
                    .as_u64()
                    .expect("engine_applied_index field"),
            )
        }

        // Search for a live instance of the race window: propose a fresh,
        // fully-formed control-only `NodeAddrs` (simulating a real joining
        // node's own complete self-registration — `RegisterNode`'s CAS never
        // checks reachability, so proposing this directly with no real bound
        // listener behind it is exactly as observable as a genuine one)
        // straight at the leader, then poll `/admin/raft` until it commits,
        // firing the admin call the instant apply is still behind.
        let mut caught: Option<(u64, NodeAddrs, u16, serde_json::Value)> = None;
        'search: for attempt in 0..50u64 {
            let this_id = 100 + attempt;
            let addrs = NodeAddrs {
                internal: format!("127.0.0.1:{}", 20000 + attempt),
                client: format!("127.0.0.1:{}", 21000 + attempt),
                admin: format!("127.0.0.1:{}", 22000 + attempt),
                intra: format!("127.0.0.1:{}", 23000 + attempt),
                role: "control".to_string(),
            };
            let register = MetaCommand::RegisterNode {
                node: nid(this_id),
                addrs: addrs.clone(),
                labels: BTreeMap::new(),
            };

            let (before_commit, _) = raft_indices(control_admin[leader_idx]).await;
            call(
                control_intra[leader_idx],
                ClientRequest::ProposeSchema(register),
            )
            .await;

            let poll_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while tokio::time::Instant::now() < poll_deadline {
                let (commit, applied) = raft_indices(control_admin[leader_idx]).await;
                if commit > before_commit {
                    if applied < commit {
                        let (status, body) = add_control_member(
                            control_admin[leader_idx],
                            this_id,
                            addrs.internal.parse().expect("valid socket addr"),
                        )
                        .await;
                        caught = Some((this_id, addrs, status, body));
                        break 'search;
                    }
                    // The apply task already caught up before this poll
                    // observed the commit — try a fresh id.
                    break;
                }
            }
        }
        let (this_id, addrs, status, body) = caught.expect(
            "never observed the committed-but-not-yet-applied window across 50 attempts — \
             the race this test targets did not manifest on this run",
        );

        assert_eq!(
            status, 200,
            "control/member/add must succeed even when it races the target's own \
             not-yet-locally-applied self-registration, not fail with \"already \
             claimed by a different registration\": {body}"
        );

        // `NodeId`'s `Ord` is a plain string compare, not numeric — "n100" <
        // "n2" lexicographically (the same zero-padding gotcha
        // `animusd::config`'s own doc calls out) — so `this_id` (100+) does
        // not necessarily sort after `n0`/`n1`/`n2` the way
        // `await_voters_everywhere`'s own hard-coded-order tests can assume.
        // Sort both sides as strings instead of relying on numeric order.
        let mut want: Vec<String> = [0, 1, 2, this_id]
            .iter()
            .map(|&n| nid(n).to_string())
            .collect();
        want.sort();
        for &a in &control_admin {
            let converged = async {
                loop {
                    let (status, body) = control_members(a).await;
                    if status == 200
                        && let Some(mut v) = voters_of(&body)
                    {
                        v.sort();
                        if v == want {
                            return;
                        }
                    }
                    sleep(Duration::from_millis(150)).await;
                }
            };
            timeout(Duration::from_secs(30), converged)
                .await
                .unwrap_or_else(|_| panic!("race: node at {a} never converged to {want:?}"));
        }

        // The address book must exactly match the real self-registration —
        // never a synthesized/blank one (the "malformed entry wins the
        // race" corruption variant the investigation also observed).
        let final_addrs = control_nodes[leader_idx]
            .metadata()
            .node_addrs
            .get(&nid(this_id))
            .cloned()
            .expect("grown node's own address book entry must exist");
        assert_eq!(
            final_addrs, addrs,
            "the address book must reflect the real self-registration, never a \
             synthesized/blank one"
        );

        for node in control_nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect(
        "admin_add_control_member_races_a_control_only_self_registration_and_still_converges \
         timed out",
    );
}
