//! **Removed-replica GC over `ProdEnv`** (ADR 0029): when a tablet's replica set
//! moves *off* a node (a manual drain, an automatic failure-repair swap, or a
//! rebalance move) while the tablet itself still exists, that node's `cp_gc_loop`
//! release phase must stop the idle group and tombstone its data out of the
//! node's shared engine — the dual of the drop-table reclaim phase, which only
//! fires when the whole table (and so the tablet) is gone.
//!
//! These are the production-wiring counterparts of the pure `tablets_to_release`
//! unit tests in `src/topology.rs`; they exercise the release phase's two live
//! guards (the node's own-Raft-config gate + the epoch-stability dampener) over
//! real TCP/time, which the deterministic suite cannot. Real time + sockets, so
//! everything polls with generous timeouts — never a fixed sleep as the
//! assertion mechanism.
//!
//! 1. `moved_off_replica_is_stopped_and_its_scope_erased` — a `CasTabletReplicas`
//!    proposal drops a follower from a still-existing tablet's replica set (the
//!    exact mechanism `cp_reconfigure.rs` uses); the dropped node stops hosting
//!    the group and erases its data.
//! 2. `release_survives_a_restart_replay` — the release converges (and does not
//!    resurrect) across a restart of the dropped node, and a *different* tablet
//!    the node is still a valid replica of is **not** erased (the gate is
//!    tablet-specific + config-based, not a blanket wipe).
//! 3. `a_joining_spare_is_never_released` — the failure-repair-onto-a-spare flow
//!    (`cp_reconfigure.rs::failure_auto_replaces_replica_onto_spare`): a spare
//!    mid-join (a non-voter, still-forming group) is never prematurely erased by
//!    the release phase, because it *is* in the replica set — a regression test
//!    on the local-config gate + dampener together.
//! 4. `split_then_immediate_release_spares_the_new_siblings_data` — the bug this
//!    file's fix addresses: split a tablet, then *immediately* (no sleep — the
//!    exact race window in which the dropped node's own `StorageScope` may not
//!    yet have re-narrowed to the post-split range) CAS the *parent's* replicas
//!    off a node that stays a replica of the freshly-minted *child* tablet. Once
//!    the release GC erases the parent on that node, the child's data — sharing
//!    the same per-node engine (ADR 0026/0028) — must survive, both cluster-wide
//!    and in that node's own local storage. To force the actual race (rather
//!    than let a fast local cluster's `cp_join_host_loop` tick win first and
//!    self-heal the scope before the drop lands, hiding the bug), the split and
//!    the parent's replica-set CAS are proposed **back-to-back on the control
//!    leader's local Raft log**, not round-tripped through the wire protocol.

mod support;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use animus_env::NodeId;
use animus_tablet::TabletId;
use animusd::{
    ClientRequest, ClientResponse, ClusterConfig, MetaCommand, Node, RoleAddrs, read_frame,
    write_frame,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

const KV_TABLET: TabletId = TabletId(1);

// ---- bring-up + polling helpers (mirrors cp_reconfigure.rs / drop_table_gc.rs) --

/// Bring up an `n`-node cluster, one process per node (node-local admin views +
/// separate edge state — the real deployment shape), retrying the
/// (allocate-fresh-ports + start-all) unit on a bind race. Returns the per-node
/// data dirs so a test can assert on-disk WAL state and restart nodes in place.
async fn bring_up(n: usize, dir: &Path) -> (Vec<Node>, ClusterConfig, Vec<PathBuf>) {
    for attempt in 0..16 {
        let a = support::free_addrs(n * 6);
        let config = ClusterConfig {
            nodes: (0..n)
                .map(|i| RoleAddrs {
                    id: animusd::config::node_id(i),
                    role: animusd::config::NodeRole::Both,
                    internal: a[6 * i],
                    client: a[6 * i + 1],
                    dynamo: a[6 * i + 2],
                    admin: a[6 * i + 3],
                    intra: a[6 * i + 4],
                    console: a[6 * i + 5],
                })
                .collect(),
        };
        let dirs: Vec<PathBuf> = (0..n)
            .map(|i| dir.join(format!("node-{attempt}-{i}")))
            .collect();
        let mut nodes = Vec::new();
        let mut ok = true;
        for (i, node_dir) in dirs.iter().enumerate() {
            match animusd::run_node(&config, i, node_dir).await {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            return (nodes, config, dirs);
        }
        for node in &nodes {
            node.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up cluster after retries (ports kept getting stolen)");
}

async fn await_bootstrap(nodes: &[Node]) {
    timeout(Duration::from_secs(30), async {
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
    .expect("cluster did not bootstrap in 30s");
}

/// One HTTP/1.0 GET to the admin endpoint; `None` if the node is unreachable
/// (e.g. shut down), else `(status, parsed JSON)`.
async fn admin_get(addr: SocketAddr, path: &str) -> Option<(u16, Value)> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    let request = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    stream.flush().await.ok()?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.ok()?;
    let text = String::from_utf8(raw).ok()?;
    let (head, payload) = text.split_once("\r\n\r\n")?;
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())?;
    let value: Value = serde_json::from_str(payload).unwrap_or(Value::Null);
    Some((status, value))
}

/// This node's `(is_leader, voters)` for `tablet` from its node-local
/// `/admin/raftkv` view, or `None` if the node doesn't host the tablet's group
/// (never formed, or released) / is unreachable.
async fn tablet_group(admin_addr: SocketAddr, tablet: TabletId) -> Option<(bool, Vec<NodeId>)> {
    let (_s, v) = admin_get(admin_addr, "/admin/raftkv").await?;
    let g = v["groups"]
        .as_array()?
        .iter()
        .find(|g| g["tablet"] == tablet.0)?;
    let voters = g["voters"]
        .as_array()?
        .iter()
        .filter_map(|r| r.as_str()?.parse::<NodeId>().ok())
        .collect();
    Some((g["is_leader"].as_bool().unwrap_or(false), voters))
}

/// Whether this node currently hosts a group for `tablet` at all.
async fn hosts_tablet(admin_addr: SocketAddr, tablet: TabletId) -> bool {
    tablet_group(admin_addr, tablet).await.is_some()
}

async fn call(addr: SocketAddr, req: ClientRequest) -> Option<ClientResponse> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    write_frame(&mut stream, &req).await.ok()?;
    read_frame(&mut stream).await.ok()?
}

/// Write `key = value` into `table` via any client port, retrying until one
/// commits (routing waits out group formation/election).
async fn put(clients: &[SocketAddr], table: &str, key: &[u8], value: &[u8], secs: u64) {
    let w = async {
        loop {
            for &c in clients {
                if let Some(ClientResponse::PutOk) = call(
                    c,
                    ClientRequest::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        table: table.to_string(),
                    },
                )
                .await
                {
                    return;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(secs), w)
        .await
        .unwrap_or_else(|_| panic!("write of {key:?} never committed"));
}

/// Read `key` from `table` via one node's client port (linearizable).
async fn client_get(addr: SocketAddr, table: &str, key: &[u8]) -> Option<Vec<u8>> {
    match call(
        addr,
        ClientRequest::Get {
            key: key.to_vec(),
            table: table.to_string(),
            stale: false,
        },
    )
    .await
    {
        Some(ClientResponse::Value(v)) => v,
        _ => None,
    }
}

/// Whether `tablet`'s own per-tablet Raft WAL file exists in `dir` — the
/// node-local artifact the release phase must delete (the shared LSM engine's
/// files span every tablet a node hosts, so they aren't a per-tablet signal).
fn tablet_wal_present(dir: &Path, tablet: TabletId) -> bool {
    match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .any(|e| e.file_name().to_string_lossy() == animus_cp_data::wal_file(tablet.0)),
        Err(_) => false,
    }
}

/// Poll `cond` (a future) until it resolves to `true`, panicking after `secs`.
async fn await_true<F, Fut>(secs: u64, what: &str, cond: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let done = async {
        loop {
            if cond().await {
                return;
            }
            sleep(Duration::from_millis(150)).await;
        }
    };
    timeout(Duration::from_secs(secs), done)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for: {what}"));
}

/// Provision the `kv` tablet (KV_TABLET) on nodes 0..3, wait for it to form with
/// 3 voters + a leader, and return the leading node's index. The replica set is
/// the first `min(N, RF=3)` Active members (node ids 0..2),
/// so node 3 (if present) is an idle spare.
async fn form_kv_group(nodes: &[Node], clients: &[SocketAddr]) -> usize {
    put(clients, "kv", b"k0", b"v0", 30).await;
    let formed = async {
        loop {
            for (i, node) in nodes.iter().enumerate() {
                if let Some((true, voters)) = tablet_group(node.admin_addr(), KV_TABLET).await
                    && voters.len() == 3
                {
                    return i;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(30), formed)
        .await
        .expect("kv group did not form with 3 voters + a leader within 30s")
}

/// Commit a `CasTabletReplicas` on the control leader (epoch-CAS), retrying past
/// racing epoch bumps / leadership moves until the tablet map shows `replicas`.
async fn set_replicas(nodes: &[Node], tablet: TabletId, replicas: &[NodeId]) {
    let want: std::collections::BTreeSet<NodeId> = replicas.iter().cloned().collect();
    let change = async {
        loop {
            if let Some(epoch) = nodes[0].metadata().tablets.get(&tablet).map(|t| t.epoch) {
                let cmd = MetaCommand::CasTabletReplicas {
                    tablet,
                    expected_epoch: epoch,
                    replicas: replicas.to_vec(),
                };
                for node in nodes {
                    if node.is_control_leader() {
                        node.propose_meta(cmd.clone());
                    }
                }
            }
            let now: Option<std::collections::BTreeSet<NodeId>> = nodes[0]
                .metadata()
                .tablets
                .get(&tablet)
                .map(|t| t.replicas.iter().cloned().collect());
            if now.as_ref() == Some(&want) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(30), change)
        .await
        .expect("replica-set change did not replicate within 30s");
}

// ---- Test 1: a moved-off replica is stopped and its scope erased -------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn moved_off_replica_is_stopped_and_its_scope_erased() {
    timeout(Duration::from_secs(120), async {
        let tmp = tempfile::tempdir().unwrap();
        // 4 nodes, RF=3: kv lands on ids 0..2; node 3 is
        // a spare, so we can move a replica off onto it and leave a stable RF-3 set.
        let (nodes, config, dirs) = bring_up(4, tmp.path()).await;
        await_bootstrap(&nodes).await;
        let raftkv_ids = config.data_ids(); // [0, 1, 2, 3]
        let spare = raftkv_ids[3].clone();
        let clients: Vec<SocketAddr> = config.nodes.iter().map(|a| a.client).collect();

        let leader_idx = form_kv_group(&nodes, &clients).await;

        // Move a *follower* replica off (the leader can't remove itself): the new
        // set is the two kept replicas + the spare — a stable RF-3 placement the
        // policy reconciler won't revert, so the dropped node stays out for good.
        let drop_idx = (0..3)
            .find(|&i| i != leader_idx)
            .expect("a follower replica");
        let dropped_id = raftkv_ids[drop_idx].clone();
        let kept: Vec<NodeId> = raftkv_ids[..3]
            .iter()
            .filter(|&id| *id != dropped_id)
            .cloned()
            .chain([spare.clone()])
            .collect();
        assert_eq!(kept.len(), 3);
        set_replicas(&nodes, KV_TABLET, &kept).await;

        // The dropped node's own release phase stops hosting the group…
        let dropped_admin = config.nodes[drop_idx].admin;
        await_true(60, "dropped node stops hosting the group", || async {
            !hosts_tablet(dropped_admin, KV_TABLET).await
        })
        .await;
        // …and erases its data (its per-tablet WAL file is deleted, and the
        // node-local storage view no longer serves the tablet).
        let dropped_raftkv_dir = dirs[drop_idx].join("internal");
        await_true(60, "dropped node's tablet WAL is reclaimed", || {
            let d = dropped_raftkv_dir.clone();
            async move { !tablet_wal_present(&d, KV_TABLET) }
        })
        .await;
        await_true(30, "dropped node reports the tablet not hosted", || async {
            matches!(
                admin_get(dropped_admin, "/admin/storage/scan?tablet=1").await,
                Some((404, _))
            )
        })
        .await;

        // The tablet still exists and still serves from its surviving replicas.
        let survivor_clients: Vec<SocketAddr> = (0..4)
            .filter(|&i| i != drop_idx)
            .map(|i| config.nodes[i].client)
            .collect();
        assert_eq!(
            client_get(survivor_clients[0], "kv", b"k0").await,
            Some(b"v0".to_vec()),
            "the tablet still serves its data after the move"
        );

        for node in nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

// ---- Test 2: release converges across a restart; unrelated tablet survives ---

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn release_survives_a_restart_replay() {
    timeout(Duration::from_secs(150), async {
        let tmp = tempfile::tempdir().unwrap();
        let (nodes, config, dirs) = bring_up(4, tmp.path()).await;
        await_bootstrap(&nodes).await;
        let raftkv_ids = config.data_ids();
        let spare = raftkv_ids[3].clone();
        let clients: Vec<SocketAddr> = config.nodes.iter().map(|a| a.client).collect();

        // Two tables, both provisioned onto the same first-3 replica set (nodes
        // 0..2). `kv` (tablet 1) will be moved off the dropped node; `other`
        // (tablet 2) must stay put — the gate is tablet-specific, not a wipe.
        let leader_idx = form_kv_group(&nodes, &clients).await;
        put(&clients, "other", b"keep", b"keepval", 30).await;
        let other_tablet = {
            let ready = async {
                loop {
                    if let Some((id, _)) = nodes[0]
                        .metadata()
                        .tablets
                        .iter()
                        .find(|(_, t)| t.table.as_deref() == Some("other"))
                        .map(|(id, t)| (*id, t.clone()))
                    {
                        return id;
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            };
            timeout(Duration::from_secs(20), ready)
                .await
                .expect("`other` tablet provisioned")
        };

        // Drop a follower of `kv` (which is also a replica of `other`) off `kv`.
        let drop_idx = (0..3)
            .find(|&i| i != leader_idx)
            .expect("a follower replica");
        let dropped_id = raftkv_ids[drop_idx].clone();
        // Sanity: the dropped node really is a replica of `other` (so the inverse
        // check below is meaningful).
        assert!(
            nodes[0]
                .metadata()
                .tablets
                .get(&other_tablet)
                .is_some_and(|t| t.replicas.contains(&dropped_id)),
            "the dropped node must also be a replica of `other`"
        );
        let kept: Vec<NodeId> = raftkv_ids[..3]
            .iter()
            .filter(|&id| *id != dropped_id)
            .cloned()
            .chain([spare.clone()])
            .collect();
        set_replicas(&nodes, KV_TABLET, &kept).await;

        // Wait for the dropped node to release `kv`.
        let dropped_admin = config.nodes[drop_idx].admin;
        let dropped_raftkv_dir = dirs[drop_idx].join("internal");
        await_true(60, "kv released on the dropped node", || {
            let d = dropped_raftkv_dir.clone();
            async move {
                !hosts_tablet(dropped_admin, KV_TABLET).await && !tablet_wal_present(&d, KV_TABLET)
            }
        })
        .await;

        // Restart the dropped node on the same dir/addresses.
        nodes[drop_idx].shutdown_graceful().await;
        let node = support::restart_same_addrs(
            &config,
            drop_idx,
            &dirs[drop_idx],
            animusd::StorageBackend::default(),
        )
        .await;
        // NB: don't wait for this node to be *control leader* — it's 1 of a 4-node
        // control group, so it rejoins as a follower (the majority is still up).
        // The replay-completion poll below waits for it to catch up instead.

        // The restarted control replica re-applies its log through *historical*
        // map states, in which it was briefly still a replica of `kv` — so the
        // join-host loop may transiently re-host an empty `kv` group, which the
        // release phase reclaims again once replay passes the move. Convergent,
        // not one-shot: wait for replay to complete, then poll to converged.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some((200, raft)) = admin_get(dropped_admin, "/admin/raft").await {
                let applied = raft["last_applied"].as_u64().unwrap_or(0);
                let commit = raft["commit_index"].as_u64().unwrap_or(u64::MAX);
                let full = raft["snapshot_index"].as_u64().unwrap_or(0)
                    + raft["log_len"].as_u64().unwrap_or(0);
                if applied == commit && commit >= full && applied > 0 {
                    break;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "control replay did not complete on the restarted node"
            );
            sleep(Duration::from_millis(100)).await;
        }

        // `kv` stays released (never resurrects) after the restart…
        await_true(30, "kv stays released after restart", || {
            let d = dropped_raftkv_dir.clone();
            async move {
                !hosts_tablet(dropped_admin, KV_TABLET).await && !tablet_wal_present(&d, KV_TABLET)
            }
        })
        .await;

        // …and the unrelated tablet the node is still a valid replica of is NOT
        // erased: it keeps hosting `other`, its WAL is present, and it serves the
        // key. (The gate is tablet-specific + config-based, not a blanket wipe.)
        await_true(30, "the node keeps hosting `other`", || async {
            hosts_tablet(node.admin_addr(), other_tablet).await
        })
        .await;
        assert!(
            tablet_wal_present(&dropped_raftkv_dir, other_tablet),
            "the unrelated tablet's WAL must not be erased"
        );
        assert_eq!(
            client_get(node.client_addr(), "other", b"keep").await,
            Some(b"keepval".to_vec()),
            "the unrelated tablet still serves its data on the restarted node"
        );

        node.shutdown_graceful().await;
        for (i, n) in nodes.iter().enumerate() {
            if i != drop_idx {
                n.shutdown_graceful().await;
            }
        }
    })
    .await
    .expect("test timed out");
}

// ---- Test 3: a joining spare is never released -------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_joining_spare_is_never_released() {
    timeout(Duration::from_secs(150), async {
        let tmp = tempfile::tempdir().unwrap();
        // 4 nodes, RF=3: kv on ids 0..2, spare id 3. Kill a replica -> the reconciler
        // moves the tablet onto the spare, which join-hosts a fresh (initially
        // non-voter, empty) group. The release phase must NEVER touch that spare's
        // group: the spare IS in the replica set, and the local-config gate +
        // epoch dampener absorb the brief non-voter window during the join.
        let (nodes, config, dirs) = bring_up(4, tmp.path()).await;
        await_bootstrap(&nodes).await;
        let raftkv_ids = config.data_ids();
        let spare = raftkv_ids[3].clone();
        let clients: Vec<SocketAddr> = config.nodes.iter().map(|a| a.client).collect();

        let leader_idx = form_kv_group(&nodes, &clients).await;
        put(&clients, "kv", b"k1", b"v1", 20).await;

        // Kill a follower replica; the spare (node 3) is the repair target.
        let kill_idx = (0..3)
            .find(|&i| i != leader_idx)
            .expect("a follower replica");
        let killed_id = raftkv_ids[kill_idx].clone();
        nodes[kill_idx].shutdown();
        let survivors: Vec<usize> = (0..4).filter(|&i| i != kill_idx).collect();

        // The cascade: the map swaps the dead replica for the spare, and the
        // spare's group reconfigures in as a real voter (3 voters incl. the spare,
        // excl. the killed node).
        let reconfigured = async {
            loop {
                for &i in &survivors {
                    if let Some((true, voters)) =
                        tablet_group(config.nodes[i].admin, KV_TABLET).await
                        && voters.len() == 3
                        && voters.contains(&spare)
                        && !voters.contains(&killed_id)
                    {
                        return;
                    }
                }
                sleep(Duration::from_millis(150)).await;
            }
        };
        timeout(Duration::from_secs(90), reconfigured)
            .await
            .expect("the CP group did not reconfigure onto the spare within 90s");

        // The spare joined and was NOT prematurely released: it hosts the tablet,
        // its WAL file is present, and it stays that way over a sustained window
        // (several release-GC ticks) — the regression on the gate + dampener.
        let spare_admin = config.nodes[3].admin;
        let spare_raftkv_dir = dirs[3].join("internal");
        assert!(
            hosts_tablet(spare_admin, KV_TABLET).await,
            "the spare must host the tablet after joining"
        );
        assert!(
            tablet_wal_present(&spare_raftkv_dir, KV_TABLET),
            "the spare's tablet WAL must exist after joining"
        );
        // Hold across several CP_GC_INTERVAL (500ms) ticks — well past
        // RELEASE_CONFIRM_TICKS — to prove nothing releases it.
        let hold_deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        while tokio::time::Instant::now() < hold_deadline {
            assert!(
                hosts_tablet(spare_admin, KV_TABLET).await
                    && tablet_wal_present(&spare_raftkv_dir, KV_TABLET),
                "the joining/joined spare was erroneously released"
            );
            sleep(Duration::from_millis(300)).await;
        }

        // The healed group still serves through the spare-backed set.
        let survivor_clients: Vec<SocketAddr> =
            survivors.iter().map(|&i| config.nodes[i].client).collect();
        put(&survivor_clients, "kv", b"k2", b"v2", 30).await;

        for &i in &survivors {
            nodes[i].shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}
// TOMBSTONE (ADR 0050 Train B rung 5): `split_then_immediate_release_
// spares_the_new_siblings_data` was deleted here, not merely parked. It
// hand-drove the OLD zero-copy `MetaCommand::SplitTablet` on a POPULATED
// tablet, racing the metadata-only range-narrow against a replica-set CAS
// to prove release's erase spared the new sibling's data on the SHARED
// engine — a hazard (and a mechanism) that no longer exists: under
// per-tablet engines a release/reclaim deletes only the released tablet's
// own engine (sibling-sparing is structural — `animus-cp-data`'s
// reconciler teeth + corpus scenario 4), and a populated-tablet split is
// the rung-3..5 copy-based workflow, whose release-during-build safety is
// `tests/split_build.rs`'s driver-kill e2e. The old command survives only
// as an empty-table topology fixture until the B7 sweep deletes it.
