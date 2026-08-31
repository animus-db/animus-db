//! ADR 0062 §3, rung 6 — the in-place split **directed-Placing completion
//! loop**, end to end over a real multi-node cluster.
//!
//! The scenario every test here shares: a 3-node cluster provisions a table
//! (replicas = its only 3 candidates), is then GROWN by one more node whose
//! id sorts **lexically below** every original node's id (`select_replicas`
//! — `animus_placement::choose` — picks eligible candidates in ascending
//! `NodeId` order with no load-awareness, `animus-control::meta::
//! active_candidates` iterating a `BTreeMap<NodeId, _>` — so this is what
//! makes a *fresh* `select_replicas` computation over the grown candidate
//! pool prefer the new node over one of the parent's original three).
//! The split is kicked off immediately after growth (well inside
//! `REBALANCE_EVERY_N_TICKS`'s ~4s cadence, `animus-control::node`) so
//! ordinary cluster-wide rebalance never gets a chance to move the PARENT
//! first — if it did, the child would fork already-placed and no
//! `split_placing` entry would ever be written (ADR 0062 §2's own "already
//! satisfying: no entry" rule), which would defeat every assertion below.
//! Both children therefore fork onto the parent's ORIGINAL (now-suboptimal)
//! three nodes, and `CutoverSplit`'s own apply records a real, differing
//! target — the one genuine way to exercise the completion loop's own
//! convergence-observing job rather than its vacuous "already done" case.
//!
//! **Deliberately a ONE-replica difference, not two.** A target replacing
//! two of three replicas at once was found, empirically, to make the live
//! Raft group's own membership oscillate under a real `ProdEnv` cluster
//! (`reconfigure_step`'s add-before-remove sequencing passes through a
//! genuinely over-replicated 5-member intermediate state before shrinking
//! back to 3) rather than settling — a pre-existing `animus-cp-data`
//! concern, unrelated to and unmodified by this rung's own completion loop
//! (confirmed: the oscillation reproduces with zero `MarkSplitPlacingDone`
//! proposes ever having fired), out of scope to fix here. See
//! `split_placing_completion.rs`'s own module doc and
//! `docs/engineering-lessons.md` for the fuller account. A one-replica
//! target change is a fully sufficient, and far more stable, proof of this
//! rung's own "placement's fresh target differs from the parent's homes"
//! requirement.
//!
//! Real TCP/time → converged-or-timeout polling throughout, never a fixed
//! sleep for anything this file asserts on.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_env::NodeId;
use animus_tablet::{Epoch, TabletId};
use animusd::{
    ClientRequest, ClientResponse, ClusterConfig, MetaCommand, Node, NodeStatus, RoleAddrs,
    SplitMode, StorageBackend, read_frame, write_frame,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Bring up an `n`-node `--split-mode inplace` cluster — the identical
/// helper `inplace_split_e2e.rs` uses, duplicated here since each test
/// binary in this crate only shares `mod support` (see that file's own
/// module doc for why quiescence is disabled: a continuous background
/// wake/re-wake would otherwise race this file's own polling, though this
/// file runs no continuous writer of its own).
async fn bring_up_inplace(n: usize, dir: &Path) -> (Vec<Node>, ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
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
            match animusd::run_node_with_streams_quiesce_and_split_mode(
                &config,
                i,
                dir.join(format!("node-{attempt}-{i}")),
                StorageBackend::default(),
                Duration::from_secs(600),
                animusd::StreamSealKnobs::default(),
                animusd::SegmentStoreConfig::default(),
                animusd::DEFAULT_STREAM_RETENTION,
                Duration::ZERO,
                SplitMode::InPlace,
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
    panic!(
        "could not bring up an in-place-split cluster after retries (ports kept getting stolen)"
    );
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

/// One `ClientRequest`/`ClientResponse` round trip over a fresh connection
/// (mirrors `schema_ddl_relay.rs::call` — `ProposeSchema` is intra-only,
/// ADR 0047).
async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream).await.expect("read").expect("reply")
}

/// A `put` through the plain client protocol, auto-provisioning the table on
/// its first call (the identical bounded-retry idiom every e2e test in this
/// crate uses).
async fn put(stream: &mut TcpStream, key: Vec<u8>, value: Vec<u8>) {
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

/// A linearizable read through the plain client protocol, bounded-retry on
/// any error (the same idiom `put` above and every other e2e test in this
/// crate uses).
async fn get(stream: &mut TcpStream, key: Vec<u8>) -> Option<Vec<u8>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        write_frame(
            stream,
            &ClientRequest::Get {
                key: key.clone(),
                table: "t".to_string(),
                stale: false,
            },
        )
        .await
        .expect("send frame");
        match read_frame(stream).await.expect("read").expect("reply") {
            ClientResponse::Value(v) => return v,
            ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(150)).await;
            }
            other => panic!("get failed: {other:?}"),
        }
    }
}

/// The single tablet id currently serving `table`.
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

/// Poll `/admin/status` until `table`'s split of `parent` has cut over
/// (parent gone, exactly two `Active` children of `table` present) — see
/// `inplace_split_e2e.rs::await_cutover_of`'s identical doc.
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

/// Join `ids.len()` extra combined nodes into `core`'s cluster (ADR 0032
/// PR2's seed/join startup, not `run_node_growth`'s expanded-static-config
/// shape — the same real self-registering path `tests/seed_join.rs`
/// proves), each with an EXPLICIT id from `ids` — the whole reason this test
/// doesn't just use `tests/support::grow_deadline` (which always assigns
/// the grown nodes ids that sort ABOVE every original one). Retries the
/// whole batch on a port-TOCTOU bind failure OR a discovery timeout, the
/// standard idiom — deadline derived from the actual mechanism, not a
/// guess: `run_node_join`'s own internal discovery poll is bounded by
/// `JOIN_DISCOVERY_BUDGET` (10s, `animusd::lib`'s own private constant,
/// == `SCHEMA_COMMIT_TIMEOUT`), so one failed attempt can itself cost up to
/// that much before this loop ever gets to retry — six such attempts'
/// worth of budget comfortably absorbs a slow/contended `cargo test
/// --workspace` run (many test binaries competing for CPU) without masking
/// a genuine hang, since a real hang still exhausts it and fails loudly.
async fn join_extra(core_intra: &[SocketAddr], ids: &[&str], dir: &Path) -> Vec<Node> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let addrs = support::free_addrs(ids.len() * 6);
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

/// Poll every node's own `/admin/status` until `ids` all show
/// `status: "Active"` in `members` on EVERY node — the growth nodes'
/// self-registration + first heartbeat promotion (ADR 0012/0032) must have
/// replicated everywhere before a split relying on the wider candidate pool
/// is safe to kick off.
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

/// `split_placing[tablet]`'s own `(target, done)` pair from a parsed
/// `/admin/status` body, or `None` if no entry exists for it.
fn split_placing_entry(status: &Value, tablet: u64) -> Option<(Option<Vec<String>>, bool)> {
    let entry = status.get("split_placing")?.get(tablet.to_string())?;
    let target = entry.get("target")?.as_array().map(|arr| {
        arr.iter()
            .map(|v| v.as_str().unwrap_or_default().to_string())
            .collect()
    });
    let done = entry.get("done")?.as_bool()?;
    Some((target, done))
}

/// A tablet's current replica set, as plain id strings — from a parsed
/// `/admin/status` body.
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

fn tablet_epoch(status: &Value, tablet: u64) -> u64 {
    status["tablets"][tablet.to_string()]["epoch"]
        .as_u64()
        .expect("tablet epoch present")
}

/// The primary teeth: a fork-first in-place split whose children need a
/// REAL move (the parent's original nodes are not where a fresh
/// `select_replicas` would place them) — proving the whole
/// fork → cutover → directed-Placing → completion-loop pipeline actually
/// relocates data onto the ideal target and reports it done, exactly as ADR
/// 0062's own testing plan names it (`placing_relocates_data`). Also
/// verifies the derived "split complete" signal (ADR 0062 §3, fork A) is
/// fully computable from `/admin/status`'s existing serialized `Metadata` —
/// `split_lineage` plus `split_placing`, with no additional wire surface
/// needed (Scope 2 of this rung).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn placing_relocates_a_child_off_the_parents_original_nodes_and_the_completion_loop_marks_it_done()
 {
    timeout(Duration::from_secs(120), async {
        let dir = tempfile::tempdir().unwrap();
        let (mut nodes, config) = bring_up_inplace(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        // Provision "t" on the original 3 nodes (n0, n1, n2 — the only
        // candidates that exist yet, so this IS what `select_replicas`
        // would compute too).
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
            "unexpected initial replica placement — the growth-forces-a-move \
             premise of this test depends on it"
        );

        // Grow the cluster by one node whose id sorts BELOW every existing
        // one ("m" < "n" lexically) — see this file's own module doc for
        // why this is what forces `select_replicas` to prefer a different
        // 3-of-4 set than the parent's own current replicas (and why only
        // one, not two).
        let core_intra: Vec<SocketAddr> = config.nodes.iter().map(|a| a.intra).collect();
        let extra = join_extra(&core_intra, &["m0"], dir.path()).await;
        await_all_active(&nodes, &["m0"], Duration::from_secs(20)).await;
        nodes.extend(extra);

        // Kick off the split IMMEDIATELY — well inside
        // `REBALANCE_EVERY_N_TICKS`'s ~4s cadence, so ordinary rebalance
        // never gets a chance to move the parent onto the grown candidates
        // FIRST (which would make this scenario as vacuous as an
        // already-satisfying fork — see the module doc).
        let split_key = "k\\u0080";
        kickoff_tablet(&nodes[0], parent, split_key).await;
        let (left, right) = await_cutover_of(&nodes[0], "t", parent, Duration::from_secs(60)).await;

        // `CutoverSplit`'s own apply recorded a REAL, differing target — the
        // premise this whole test depends on, asserted explicitly rather
        // than assumed. Checked ONLY against `split_placing[child].target`,
        // never against the tablet's own CURRENT `replicas` or `done` here:
        // both of those are live, converging state — the directed-Placing
        // reconcile phase (500ms cadence) and this rung's own completion
        // loop can legitimately have already moved/finished a child by the
        // time this line runs (their combined worst-case latency is not
        // bounded below `await_cutover_of`'s own 100ms poll granularity),
        // so asserting a "not yet moved"/"not yet done" snapshot here would
        // be exactly the one-shot-assert-on-an-eventual-property mistake
        // this file's own module doc warns against. `target`, by contrast,
        // is written ONCE at cutover and never rewritten (ADR 0062 §2's own
        // "never trusts or rewrites `target`" rule) — the one piece of
        // `split_placing` state that is safe to assert on synchronously.
        let want_target = vec!["m0".to_string(), "n0".to_string(), "n1".to_string()];
        let (_, status) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
        for child in [left, right] {
            let (target, _done) = split_placing_entry(&status, child)
                .unwrap_or_else(|| panic!("no split_placing entry for child {child}: {status}"));
            assert_eq!(
                target.map(|mut t| {
                    t.sort();
                    t
                }),
                Some(want_target.clone()),
                "child {child}'s directed-Placing target"
            );
        }

        // The completion loop (this rung) is what turns that recorded
        // target into a converged, `done: true` reality — converged-or-
        // timeout, no fixed sleep.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            let (_, status) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
            let converged = [left, right].iter().all(|&child| {
                let mut replicas = tablet_replicas(&status, child);
                replicas.sort();
                let done = split_placing_entry(&status, child).is_some_and(|(_, d)| d);
                replicas == want_target && done
            });
            if converged {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "directed-Placing + completion loop never converged both children: {status}"
            );
            sleep(Duration::from_millis(200)).await;
        }

        // The derived "split complete" signal (ADR 0062 §3, fork A):
        // `split_lineage` present for both children AND every
        // `split_placing` entry `done` — fully computable from the two raw
        // fields `/admin/status` already carries, verified here directly
        // rather than through any new endpoint.
        let (_, status) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
        let lineage = status["split_lineage"]
            .as_object()
            .cloned()
            .unwrap_or_default();
        for child in [left, right] {
            assert!(
                lineage.contains_key(&child.to_string()),
                "split_lineage missing an entry for child {child}"
            );
            let (_, done) = split_placing_entry(&status, child).expect("entry still present");
            assert!(done, "child {child}'s split_placing entry is not done");
        }

        // The data itself is still intact on its (relocated) new home —
        // polled rather than a one-shot read, converged-or-timeout: a
        // linearizable `stale:false` read is routed to the tablet's current
        // leader regardless of which node answers, so this is not testing
        // routing staleness, only that the value genuinely survives.
        let mut client = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect client port");
        let data_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let value = get(&mut client, vec![b'k', 0]).await;
            if value.as_deref() == Some([b'v', 0].as_slice()) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < data_deadline,
                "key [k,0] unreadable (or wrong value {value:?}) after the split relocated its child"
            );
            sleep(Duration::from_millis(200)).await;
        }

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// The completion-loop-specific teeth: a stale/duplicate
/// `MetaCommand::MarkSplitPlacingDone` propose — the exact shape a leader
/// change mid-placing would produce (the OLD leader's own in-flight tick
/// racing the NEW leader's fresh one, or simply this loop's own next tick
/// re-observing an already-`done` entry before its local metadata mirror
/// catches up) — is harmless whether it lands BEFORE convergence (a
/// stale/wrong epoch, rejected) or AFTER (an already-`done` entry,
/// idempotent no-op), proposed from a node that is neither the tablet's own
/// leader nor the control-plane leader (the real relay path, `ADR 0047`'s
/// intra-only `ProposeSchema`) — never corrupting the tablet's own replicas
/// or epoch either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn mark_split_placing_done_tolerates_a_stale_or_duplicate_relayed_propose() {
    timeout(Duration::from_secs(120), async {
        let dir = tempfile::tempdir().unwrap();
        let (mut nodes, config) = bring_up_inplace(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        let mut client = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect client port");
        put(&mut client, vec![b'k', 0], vec![b'v', 0]).await;
        let parent = sole_tablet_of(&nodes[0], "t");

        let core_intra: Vec<SocketAddr> = config.nodes.iter().map(|a| a.intra).collect();
        let extra = join_extra(&core_intra, &["m0"], dir.path()).await;
        await_all_active(&nodes, &["m0"], Duration::from_secs(20)).await;
        nodes.extend(extra);

        let split_key = "k\\u0080";
        kickoff_tablet(&nodes[0], parent, split_key).await;
        let (left, right) = await_cutover_of(&nodes[0], "t", parent, Duration::from_secs(60)).await;
        let child = left;

        // A relay target: neither this tablet's own leader (unknown/
        // irrelevant here) nor necessarily the control-plane leader —
        // `nodes[0]`'s own intra port, the identical choke point
        // `schema_ddl_relay.rs` uses for its own follower-issued DDL.
        let relay_addr = nodes[0].intra_addr();

        // (a) STALE: `Epoch(0)` — strictly below `Epoch::INITIAL` (1), so it
        // can NEVER legitimately match ANY tablet's real epoch, at any
        // point in this test's timeline (epoch only ever increases,
        // rung 2's own epoch-CAS discipline) — a deterministically-stale
        // confirm, deliberately not "whatever epoch happens to be current
        // right now" (racy: this test's own read could win or lose against
        // the real reconcile loop's own first `CasTabletReplicas`, and
        // either outcome would still be "harmless" but not cleanly
        // provable either way — see this rung's own engineering-lessons
        // entry on this exact race). Must be rejected at apply (epoch
        // mismatch) — never a crash, never a wrongly-recorded `done`.
        let stale_reply = call(
            relay_addr,
            ClientRequest::ProposeSchema(MetaCommand::MarkSplitPlacingDone {
                tablet: TabletId(child),
                expected_epoch: Epoch(0),
            }),
        )
        .await;
        assert!(
            !matches!(stale_reply, ClientResponse::Error(_)),
            "a relayed ProposeSchema itself should not error even when the \
             underlying command is later rejected at apply: {stale_reply:?}"
        );

        // The completion loop still converges normally despite the stale
        // relay above.
        let want_target = {
            let mut t = vec!["m0".to_string(), "n0".to_string(), "n1".to_string()];
            t.sort();
            t
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            let (_, status) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
            let mut replicas = tablet_replicas(&status, child);
            replicas.sort();
            let done = split_placing_entry(&status, child).is_some_and(|(_, d)| d);
            if replicas == want_target && done {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "completion loop never converged after a stale relayed propose: {status}"
            );
            sleep(Duration::from_millis(200)).await;
        }

        // (b) DUPLICATE: now that the entry is genuinely `done`, re-propose
        // the SAME command with the tablet's now-current epoch — the
        // idempotent-on-already-done shape rung 2's own apply arm
        // guarantees. Must be a harmless no-op — `done` stays true and
        // nothing panics/corrupts. Deliberately NOT asserted
        // byte-for-byte-unchanged against a captured epoch/replica set:
        // once `done` is true the tablet legitimately rejoins ordinary
        // `rebalance_placement`'s own eligible population (ADR 0062 §2's
        // own exclusion rule lifts), so an independent, correct, LATER
        // rebalance move racing this exact window is real system behavior,
        // not a defect this test should conflate with the duplicate
        // propose's own effect.
        let (_, converged_status) =
            admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
        let converged_epoch = tablet_epoch(&converged_status, child);
        let dup_reply = call(
            relay_addr,
            ClientRequest::ProposeSchema(MetaCommand::MarkSplitPlacingDone {
                tablet: TabletId(child),
                expected_epoch: Epoch(converged_epoch),
            }),
        )
        .await;
        assert!(
            !matches!(dup_reply, ClientResponse::Error(_)),
            "a duplicate relayed ProposeSchema itself should not error: {dup_reply:?}"
        );
        sleep(Duration::from_millis(500)).await; // let it commit+replicate if it were going to do anything
        let (_, final_status) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
        assert!(
            split_placing_entry(&final_status, child).is_some_and(|(_, d)| d),
            "the entry must still read done after the duplicate propose"
        );

        // The sibling child is unaffected throughout.
        assert!(
            split_placing_entry(&final_status, right).is_some_and(|(_, d)| d),
            "sibling child {right} unexpectedly not done"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}
