//! **Multi-participant transactions over `ProdEnv`** (ADR 0018 §2/PR4):
//! `ClientCtx::cp_txn` end to end, through the real client TCP protocol, a
//! real 3-process cluster (each node its own `ClusterEdgeState`, exactly
//! like `cp_cross_process.rs`), and a real pre-split table (forcing a
//! transaction to span two independent tablet Raft groups, each possibly
//! led by a different node).
//!
//! The multi-tablet 2PC mechanics themselves (prepare/commit/resolve,
//! foreign-intent resolution, abort cleanup, participant leader-kill,
//! fence/seal interplay, seed reproducibility) are proven at the primitive
//! level, deterministically, in `animus-cp-data/tests/txn_multi.rs`. This
//! suite proves the **wire-level coordinator** composes those primitives
//! correctly over real forwarding — in particular the mandated regression
//! for a newly-forwarded command enum (`docs/engineering-lessons.md`): a
//! client connected to **every** node, including ones that don't host
//! either participant's leader, must still see the transaction commit
//! atomically (the `TxnPrepare`/`TxnDecide`/`TxnResolve`/`TxnStatus`
//! forwarding arms this PR adds to `cp_serve_forwarded`).
//!
//! Real TCP/time → polls with generous timeouts, never a fixed sleep.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, read_frame};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Send `request` wrapped in `ClientRequest::Forwarded` directly to `addr`
/// — the shape `ClientCtx::cp_forward` uses internally, and the mechanism
/// [`prepare_via_any_node`]/the coordinator-crash tests below use to drive
/// the 2PC prepare phase **without** a coordinator ever completing the
/// transaction (simulating a crash between prepare and decide/resolve —
/// see those tests' own doc for why this is the cleanest way to express
/// that over a real `ProdEnv` cluster: `ClientCtx::cp_txn` runs to
/// completion synchronously inside one request handler, so there is no
/// separate long-lived "coordinator process" to kill after prepare and
/// before decide; driving the internal wire requests directly and simply
/// never sending the rest is the equivalent event).
async fn call_forwarded(addr: SocketAddr, request: ClientRequest) -> ClientResponse {
    call(
        addr,
        ClientRequest::Forwarded {
            request: Box::new(request),
            traceparent: None,
        },
    )
    .await
}

/// `Forwarded { TxnPrepare }` only succeeds when sent directly to a
/// tablet's own current group leader (`cp_serve_forwarded` never
/// re-forwards) — this cycles through every node in `addrs` until one
/// replies `TxnPrepared`, bounded by an overall timeout. Mirrors a real
/// coordinator's own one-hop routing, done by hand since this is
/// deliberately bypassing `ClientCtx::cp_txn`'s own routing.
async fn prepare_via_any_node(
    addrs: &[SocketAddr],
    request: ClientRequest,
) -> (
    animus_cp_data::TxnId,
    Vec<u8>,
    String,
    animus_cp_data::hlc::HlcTimestamp,
) {
    timeout(Duration::from_secs(20), async {
        loop {
            for &addr in addrs {
                if let ClientResponse::TxnPrepared {
                    txn_id,
                    record_key,
                    record_table,
                    ts,
                    outcome: _,
                } = call_forwarded(addr, request.clone()).await
                {
                    return (txn_id, record_key, record_table, ts);
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("prepare did not succeed against any node within 20s")
}

/// As [`prepare_via_any_node`], but for `Forwarded { TxnDecide }` — used by
/// the "commit already applied, but never resolved" dual below to drive
/// the anchor's own decision directly, bypassing `ClientCtx::cp_txn`
/// entirely (which would also resolve every participant before returning —
/// exactly the step this test needs to skip).
async fn decide_via_any_node(
    addrs: &[SocketAddr],
    table: String,
    txn_id: animus_cp_data::TxnId,
    record_key: Vec<u8>,
    commit: bool,
    min_commit_ts: animus_cp_data::hlc::HlcTimestamp,
) -> animus_cp_data::TxnOutcome {
    timeout(Duration::from_secs(20), async {
        loop {
            for &addr in addrs {
                if let ClientResponse::TxnDecided { outcome } = call_forwarded(
                    addr,
                    ClientRequest::TxnDecide {
                        table: table.clone(),
                        txn_id: txn_id.clone(),
                        record_key: record_key.clone(),
                        commit,
                        min_commit_ts,
                        orphan_created_ts: None,
                    },
                )
                .await
                {
                    return outcome;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("decide did not succeed against any node within 20s")
}

/// **Coordinator crash between prepare and decide** (ADR 0018 §2/PR5): both
/// participants stage successfully, but nobody ever proposes a commit or
/// abort — the shape a coordinator process crashing right after `cp_txn`'s
/// prepare phase takes. `ClientCtx::cp_txn` runs synchronously inside one
/// request handler with no separate long-lived coordinator process to
/// literally kill, so this drives the internal `TxnPrepare` wire requests
/// directly (`prepare_via_any_node`, mirroring exactly what `cp_txn` itself
/// does over the network) and simply stops there — the cleanest way to
/// express "the coordinator is gone" over a real multi-process cluster
/// without a dedicated test hook. A plain `Get` of the staged (still
/// `Pending`) key from a **different, uninvolved** node must, within
/// `RECOVERY_GRACE` plus resolver/read-push margin, converge to the
/// committed value — proving in-doubt recovery, not any coordinator, is
/// what finishes this transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn coordinator_crash_between_prepare_and_decide_recovers_to_commit() {
    let n = 3;
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].client;
    let all_addrs: Vec<SocketAddr> = config.nodes.iter().map(|c| c.intra).collect(); // ADR 0047: Forwarded is intra-only

    put_until_ok(addr0, "txn_t5", b"k1", b"seed-lower").await;
    put_until_ok(addr0, "txn_t5", b"k9", b"seed-upper").await;
    split_and_settle(&nodes, addr0, "txn_t5", b"k5").await;

    let lower_key = txn_key("k2", "-crash");
    let upper_key = txn_key("k8", "-crash");

    // Prepare (stage) the anchor — mints the txn id + record key.
    let (txn_id, record_key, record_table, _ts) = prepare_via_any_node(
        &all_addrs,
        ClientRequest::TxnPrepare {
            table: "txn_t5".to_string(),
            anchor: None,
            writes: vec![animus_cp_data::TxnWrite::plain(
                lower_key.clone(),
                Some(b"lower-recovered".to_vec()),
            )],
            conditions: Vec::new(),
            participant_spans: Vec::new(),
            pending_kind_writes: Vec::new(),
        },
    )
    .await;

    // Prepare the participant, referencing the anchor's record. Both are
    // now genuinely staged — every recovery push must decide Committed.
    prepare_via_any_node(
        &all_addrs,
        ClientRequest::TxnPrepare {
            table: "txn_t5".to_string(),
            anchor: Some((txn_id.clone(), record_key.clone(), record_table.clone())),
            writes: vec![animus_cp_data::TxnWrite::plain(
                upper_key.clone(),
                Some(b"upper-recovered".to_vec()),
            )],
            conditions: Vec::new(),
            participant_spans: Vec::new(),
            pending_kind_writes: Vec::new(),
        },
    )
    .await;

    // Never send TxnDecide/TxnResolve — "the coordinator crashed here."
    // Read from a node not otherwise involved in driving prepare (routing
    // above cycled through all of them, so this is best-effort — the point
    // is a plain client `Get`, not a special recovery-aware call).
    let reader = config.nodes[(0) % n].client;
    for (key, expected) in [
        (lower_key.clone(), b"lower-recovered".to_vec()),
        (upper_key.clone(), b"upper-recovered".to_vec()),
    ] {
        let got = timeout(Duration::from_secs(30), async {
            loop {
                match call(
                    reader,
                    ClientRequest::Get {
                        key: key.clone(),
                        table: "txn_t5".to_string(),
                        stale: false,
                    },
                )
                .await
                {
                    v @ ClientResponse::Value(Some(_)) => return v,
                    _ => sleep(Duration::from_millis(200)).await,
                }
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "key {key:?} never recovered to a committed value within 30s \
                 (RECOVERY_GRACE + resolver/read-push margin)"
            )
        });
        assert_eq!(got, ClientResponse::Value(Some(expected)));
    }

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// **The dual: commit already applied, but never resolved.** The anchor's
/// own decision (`TxnDecide { commit: true }`) is driven directly — the
/// record is genuinely `Committed` — but `TxnResolve` is never sent to
/// either participant, so both keys sit as still-staged intents forever
/// unless something resolves them. Reads must still converge to the
/// committed value (via the foreign-intent read-path's status query, which
/// finds the record already decided — no grace wait needed at all, unlike
/// the still-`Pending` case above) or via `txn_resolver_loop`'s own
/// periodic fan-out.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn commit_already_applied_but_unresolved_converges_via_reads() {
    let n = 3;
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].client;
    let all_addrs: Vec<SocketAddr> = config.nodes.iter().map(|c| c.intra).collect(); // ADR 0047: Forwarded is intra-only

    put_until_ok(addr0, "txn_t6", b"k1", b"seed-lower").await;
    put_until_ok(addr0, "txn_t6", b"k9", b"seed-upper").await;
    split_and_settle(&nodes, addr0, "txn_t6", b"k5").await;

    let lower_key = txn_key("k2", "-done");
    let upper_key = txn_key("k8", "-done");

    let (txn_id, record_key, record_table, anchor_ts) = prepare_via_any_node(
        &all_addrs,
        ClientRequest::TxnPrepare {
            table: "txn_t6".to_string(),
            anchor: None,
            writes: vec![animus_cp_data::TxnWrite::plain(
                lower_key.clone(),
                Some(b"lower-done".to_vec()),
            )],
            conditions: Vec::new(),
            participant_spans: Vec::new(),
            pending_kind_writes: Vec::new(),
        },
    )
    .await;
    prepare_via_any_node(
        &all_addrs,
        ClientRequest::TxnPrepare {
            table: "txn_t6".to_string(),
            anchor: Some((txn_id.clone(), record_key.clone(), record_table.clone())),
            writes: vec![animus_cp_data::TxnWrite::plain(
                upper_key.clone(),
                Some(b"upper-done".to_vec()),
            )],
            conditions: Vec::new(),
            participant_spans: Vec::new(),
            pending_kind_writes: Vec::new(),
        },
    )
    .await;

    let outcome = decide_via_any_node(
        &all_addrs,
        "txn_t6".to_string(),
        txn_id.clone(),
        record_key.clone(),
        true,
        anchor_ts,
    )
    .await;
    assert!(
        matches!(outcome, animus_cp_data::TxnOutcome::Committed { .. }),
        "the anchor's own decision should be a genuine commit here: {outcome:?}"
    );

    // No `TxnResolve` ever sent. A read from any node must still converge —
    // both via the immediate on-the-fly intent resolution (the record is
    // already decided, so this needs no grace wait) and, given enough
    // time, `txn_resolver_loop` finishing the actual rewrite in the
    // background.
    let reader = config.nodes[1 % n].client;
    for (key, expected) in [
        (lower_key, b"lower-done".to_vec()),
        (upper_key, b"upper-done".to_vec()),
    ] {
        let got = timeout(Duration::from_secs(15), async {
            loop {
                match call(
                    reader,
                    ClientRequest::Get {
                        key: key.clone(),
                        table: "txn_t6".to_string(),
                        stale: false,
                    },
                )
                .await
                {
                    v @ ClientResponse::Value(Some(_)) => return v,
                    _ => sleep(Duration::from_millis(150)).await,
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("key {key:?} never converged within 15s"));
        assert_eq!(got, ClientResponse::Value(Some(expected)));
    }

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// **Issue #298 residuals, item 2: `unresolved_decided`'s lookup-failure
/// fallback (`txn_resolver_loop`) — "a decided txn whose record's tablet
/// retires before resolve."** Unlike the sibling test above, the split
/// happens **after** the decide, not before: the anchor's own record sits
/// `Committed`-but-unresolved in tablet 1's `TxnTracker` when tablet 1 itself
/// is split and retires (ADR 0058 in-place fork, the default split mode).
/// The record's key rides the ordinary base-row clone/trim path onto
/// whichever child inherits its range — exactly like any other row — so the
/// child's own group-start `rebuild_txn_tracker` scan (not log replay, see
/// that function's doc) must re-derive the identical `unresolved_decided`
/// entry from the cloned, still-`Committed`-tagged record, and
/// `txn_resolver_loop` must keep driving it to a real resolve on whichever
/// node now leads the child — with no coordinator, and no second `TxnDecide`
/// or `TxnResolve` ever sent by this test.
///
/// **A `Scan` is the discriminating probe**, not a plain `Get` — for the
/// identical reason `recovery_resolve_correctly_commits_both_tablets_of_a_
/// two_tablet_transaction` uses one: a point read resolves a still-`Pending`
/// intent on the fly the moment it can determine the record's decided
/// status, which would trivially succeed here (the record is already
/// `Committed` before the split even starts) and mask whether the
/// *physical* rewrite — the thing `unresolved_decided`/`txn_resolver_loop`
/// are actually responsible for — ever happened on the child at all. A scan
/// silently omits a still-physically-unresolved intent instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn decided_but_unresolved_record_survives_its_own_tablet_splitting_before_resolve() {
    let n = 3;
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].client;
    let all_addrs: Vec<SocketAddr> = config.nodes.iter().map(|c| c.intra).collect(); // ADR 0047: Forwarded is intra-only

    put_until_ok(addr0, "txn_split_retire", b"k1", b"seed-lower").await;
    put_until_ok(addr0, "txn_split_retire", b"k9", b"seed-upper").await;

    // Single-key, single-participant transaction — the anchor's own record
    // is the whole story here, so no second `TxnPrepare` is needed. The key
    // sits well below the split point chosen further down, so it lands on
    // the LOWER child once the split this test drives actually cuts over.
    let record_data_key = txn_key("k2", "-retire");
    let (txn_id, record_key, _record_table, anchor_ts) = prepare_via_any_node(
        &all_addrs,
        ClientRequest::TxnPrepare {
            table: "txn_split_retire".to_string(),
            anchor: None,
            writes: vec![animus_cp_data::TxnWrite::plain(
                record_data_key.clone(),
                Some(b"retire-committed".to_vec()),
            )],
            conditions: Vec::new(),
            participant_spans: Vec::new(),
            pending_kind_writes: Vec::new(),
        },
    )
    .await;

    // Drive the anchor's own decision directly (mirrors
    // `commit_already_applied_but_unresolved_converges_via_reads`): the
    // record is genuinely `Committed`, in tablet 1's own `unresolved_decided`
    // map, with `TxnResolve` never sent — "the coordinator decided and then
    // vanished before resolving" — same as that test, except THIS test now
    // splits tablet 1 itself before anything else ever touches this record.
    let outcome = decide_via_any_node(
        &all_addrs,
        "txn_split_retire".to_string(),
        txn_id,
        record_key,
        true,
        anchor_ts,
    )
    .await;
    assert!(
        matches!(outcome, animus_cp_data::TxnOutcome::Committed { .. }),
        "the anchor's own decision should be a genuine commit here: {outcome:?}"
    );

    // Split tablet 1 (still hosting the just-decided, unresolved record)
    // *after* the decide — `split_and_settle` also seeds a harmless probe
    // key well outside `record_data_key`'s own range, confirming cutover
    // completed before this test proceeds.
    split_and_settle(&nodes, addr0, "txn_split_retire", b"k5").await;

    // No second `TxnDecide`/`TxnResolve` ever sent. The only thing that can
    // still finish rewriting `record_data_key` is `txn_resolver_loop`
    // running on whichever node now leads the CHILD tablet that inherited
    // it — which first requires that child's own `rebuild_txn_tracker` to
    // have picked the entry back up from its cloned engine state.
    let mut scan_end = record_data_key.clone();
    scan_end.push(0);
    let by_key = timeout(Duration::from_secs(30), async {
        loop {
            if let ClientResponse::Pairs(pairs) = call(
                addr0,
                ClientRequest::Scan {
                    start: record_data_key.clone(),
                    end: Some(scan_end.clone()),
                    limit: None,
                    reverse: false,
                    table: "txn_split_retire".to_string(),
                    stale: false,
                },
            )
            .await
            {
                let by_key: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
                    pairs.into_iter().collect();
                if by_key.contains_key(&record_data_key) {
                    return by_key;
                }
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "record_data_key never became visible to a Scan within 30s after its own \
             tablet split and retired mid-recovery — the child tablet that inherited it \
             must rebuild the decided-but-unresolved entry from its own cloned engine \
             state and txn_resolver_loop must still drive it to a real resolve (issue \
             #298 residuals item 2)"
        )
    });
    assert_eq!(
        by_key.get(&record_data_key).map(Vec::as_slice),
        Some(b"retire-committed".as_slice()),
        "record_data_key must scan back as committed"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// A key at least [`TOKEN_BYTES`]-long (ADR 0022's floor `cp_txn` enforces
/// for every transaction write, since `RaftKvNode::txn_stage`'s anchor-key
/// assert is now wire-reachable — see `cp_txn`'s doc), embedding `suffix`
/// for uniqueness and padded with `_` to reach the minimum.
const TOKEN_BYTES: usize = 8;
fn txn_key(prefix: &str, suffix: &str) -> Vec<u8> {
    let mut s = format!("{prefix}{suffix}");
    while s.len() < TOKEN_BYTES {
        s.push('_');
    }
    s.into_bytes()
}

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// Mirrors `cp_cross_process.rs`'s identical helper: `n` per-process nodes,
/// each with its own edge state (a real multi-process deployment), wrapped
/// in the documented port-TOCTOU retry.
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
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
                advertise_host: None,
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
        };
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

async fn put_until_ok(addr: SocketAddr, table: &str, key: &[u8], value: &[u8]) {
    // Remember each failed attempt's error so a timeout names WHAT kept
    // failing (issue #268's first diagnostic gap: "25s put timeout" alone
    // doesn't say whether provisioning, routing, or the commit stalled).
    let last_err = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let seen = std::sync::Arc::clone(&last_err);
    timeout(Duration::from_secs(25), async move {
        loop {
            match call(
                addr,
                ClientRequest::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                    table: table.to_string(),
                },
            )
            .await
            {
                ClientResponse::PutOk => return,
                ClientResponse::Error(e) => {
                    *seen.lock().unwrap() = e;
                    sleep(Duration::from_millis(150)).await
                }
                other => panic!("unexpected put response: {other:?}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "put {table}/{key:?} did not succeed in 25s (last error: {})",
            last_err.lock().unwrap()
        )
    });
}

/// Split the bootstrap tablet (id 1) of `table` at `split_key`, waiting
/// until the control plane's tablet map records two tablets and the new
/// upper half is genuinely servable — mirrors `cp_plane.rs`'s
/// `cp_tablet_splits_and_both_halves_serve`.
///
/// Drives the REAL split workflow via the public kickoff
/// (`ClientRequest::SplitTablet` → `trigger_split` → `BeginSplitInPlace`,
/// the only workflow since the copy-based one was deleted in Layer B1),
/// then waits for the driver to retire the parent — populated tables
/// included (the fork/cutover materializes the data, generically proven
/// here).
async fn split_and_settle(nodes: &[Node], addr: SocketAddr, table: &str, split_key: &[u8]) {
    match call(
        addr,
        ClientRequest::SplitTablet {
            tablet: 1,
            split_key: split_key.to_vec(),
        },
    )
    .await
    {
        ClientResponse::PutOk => {}
        other => panic!("split kickoff refused: {other:?}"),
    }
    timeout(Duration::from_secs(30), async {
        loop {
            if nodes.iter().all(|n| {
                let m = n.metadata();
                m.tablets.len() == 2 && !m.tablets.contains_key(&animus_tablet::TabletId(1))
            }) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the split workflow did not cut over within 30s");

    // Confirm the new (upper) group actually serves before relying on it —
    // write a key we know lands there.
    let probe_key = [split_key, b"zzz-probe"].concat();
    put_until_ok(addr, table, &probe_key, b"probe").await;
}

/// **Atomicity across a genuinely split table**: a transaction writing one
/// key below the split point and one above commits, and both become
/// visible together via a node that may not host either participant's
/// leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn multi_tablet_txn_commits_atomically_across_a_split_table() {
    let n = 3;
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].client;

    // Seed a lower and an upper key so the split has real data on both
    // sides, then split at "k5".
    put_until_ok(addr0, "txn_t", b"k1", b"seed-lower").await;
    put_until_ok(addr0, "txn_t", b"k9", b"seed-upper").await;
    split_and_settle(&nodes, addr0, "txn_t", b"k5").await;

    // The transaction: one key in each half. Every txn write key must be at
    // least 8 bytes (ADR 0022 — `RaftKvNode::txn_stage`'s anchor-token
    // assert; `cp_txn` itself validates this and returns a clean error for
    // a short key rather than ever reaching that assert, but every key here
    // is intentionally already valid).
    let resp = timeout(
        Duration::from_secs(25),
        call(
            addr0,
            ClientRequest::Txn {
                writes: vec![
                    animusd::TxnTableWrite::plain(
                        "txn_t".to_string(),
                        b"k2000000".to_vec(),
                        Some(b"lower-txn".to_vec()),
                    ),
                    animusd::TxnTableWrite::plain(
                        "txn_t".to_string(),
                        b"k8000000".to_vec(),
                        Some(b"upper-txn".to_vec()),
                    ),
                ],
                preconditions: vec![],
                write_conditions: vec![],
            },
        ),
    )
    .await
    .expect("txn call did not return in 25s");
    assert!(
        matches!(resp, ClientResponse::TxnCommitted { .. }),
        "multi-tablet txn should commit: {resp:?}"
    );

    // Read both keys back via a DIFFERENT node than the one the txn was
    // issued through — both must be visible together.
    let reader = config.nodes[1 % n].client;
    for (key, expected) in [
        (b"k2000000".to_vec(), b"lower-txn".to_vec()),
        (b"k8000000".to_vec(), b"upper-txn".to_vec()),
    ] {
        let got = timeout(
            Duration::from_secs(10),
            call(
                reader,
                ClientRequest::Get {
                    key: key.clone(),
                    table: "txn_t".to_string(),
                    stale: false,
                },
            ),
        )
        .await
        .expect("read did not return in 10s");
        assert_eq!(
            got,
            ClientResponse::Value(Some(expected)),
            "key {key:?} missing after a committed cross-tablet transaction"
        );
    }

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// **Follower-connected regression**: issue the identical multi-tablet
/// transaction from **every** node in turn. With one leader per tablet
/// among three nodes, at least one of these calls originates on a node
/// that hosts neither participant's leader — proving the
/// `TxnPrepare`/`TxnDecide`/`TxnResolve` forwarding arms this PR adds to
/// `cp_serve_forwarded` are wired correctly (a missing arm here is exactly
/// the "bimodal per-process flake" the house lesson on forwarded command
/// enums warns about).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn txn_through_every_node_including_followers_succeeds() {
    let n = 3;
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].client;

    put_until_ok(addr0, "txn_t2", b"k1", b"seed-lower").await;
    put_until_ok(addr0, "txn_t2", b"k9", b"seed-upper").await;
    split_and_settle(&nodes, addr0, "txn_t2", b"k5").await;

    for i in 0..n {
        let addr = config.nodes[i].client;
        let lower_key = txn_key("k2", &format!("-{i}"));
        let upper_key = txn_key("k8", &format!("-{i}"));
        let resp = timeout(
            Duration::from_secs(25),
            call(
                addr,
                ClientRequest::Txn {
                    writes: vec![
                        animusd::TxnTableWrite::plain(
                            "txn_t2".to_string(),
                            lower_key.clone(),
                            Some(b"lo".to_vec()),
                        ),
                        animusd::TxnTableWrite::plain(
                            "txn_t2".to_string(),
                            upper_key.clone(),
                            Some(b"hi".to_vec()),
                        ),
                    ],
                    preconditions: vec![],
                    write_conditions: vec![],
                },
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("txn via node {i} did not return in 25s"));
        assert!(
            matches!(resp, ClientResponse::TxnCommitted { .. }),
            "txn issued via node {i} should commit: {resp:?}"
        );

        for (key, expected) in [(lower_key, b"lo".to_vec()), (upper_key, b"hi".to_vec())] {
            let got = call(
                addr0,
                ClientRequest::Get {
                    key: key.clone(),
                    table: "txn_t2".to_string(),
                    stale: false,
                },
            )
            .await;
            assert_eq!(
                got,
                ClientResponse::Value(Some(expected)),
                "key {key:?} from the txn issued via node {i} must be visible"
            );
        }
    }

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// **Concurrent transactions are individually atomic**: several `cp_txn`
/// calls run concurrently, each its own independent two-tablet pair; every
/// one's own pair must be atomically visible (both keys, never one without
/// the other), regardless of how their prepare/commit rounds interleave in
/// real wall-clock time.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_transactions_are_individually_atomic() {
    let n = 3;
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].client;

    put_until_ok(addr0, "txn_t3", b"k1", b"seed-lower").await;
    put_until_ok(addr0, "txn_t3", b"k9", b"seed-upper").await;
    split_and_settle(&nodes, addr0, "txn_t3", b"k5").await;

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let addr = config.nodes[i % n].client;
            tokio::spawn(async move {
                let lower_key = txn_key("k2", &format!("-c{i}"));
                let upper_key = txn_key("k8", &format!("-c{i}"));
                let resp = timeout(
                    Duration::from_secs(25),
                    call(
                        addr,
                        ClientRequest::Txn {
                            writes: vec![
                                animusd::TxnTableWrite::plain(
                                    "txn_t3".to_string(),
                                    lower_key.clone(),
                                    Some(format!("lo{i}").into_bytes()),
                                ),
                                animusd::TxnTableWrite::plain(
                                    "txn_t3".to_string(),
                                    upper_key.clone(),
                                    Some(format!("hi{i}").into_bytes()),
                                ),
                            ],
                            preconditions: vec![],
                            write_conditions: vec![],
                        },
                    ),
                )
                .await
                .unwrap_or_else(|_| panic!("concurrent txn {i} did not return in 25s"));
                assert!(
                    matches!(resp, ClientResponse::TxnCommitted { .. }),
                    "concurrent txn {i} should commit: {resp:?}"
                );
                (i, lower_key, upper_key)
            })
        })
        .collect();

    for handle in handles {
        let (i, lower_key, upper_key) = handle.await.expect("txn task panicked");
        let lower = call(
            addr0,
            ClientRequest::Get {
                key: lower_key,
                table: "txn_t3".to_string(),
                stale: false,
            },
        )
        .await;
        let upper = call(
            addr0,
            ClientRequest::Get {
                key: upper_key,
                table: "txn_t3".to_string(),
                stale: false,
            },
        )
        .await;
        assert_eq!(
            lower,
            ClientResponse::Value(Some(format!("lo{i}").into_bytes())),
            "concurrent txn {i}'s lower key must be visible"
        );
        assert_eq!(
            upper,
            ClientResponse::Value(Some(format!("hi{i}").into_bytes())),
            "concurrent txn {i}'s upper key must be visible"
        );
    }

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// **Condition-read precondition**: a transaction whose precondition is
/// already false (a stale expected value) aborts with a retryable conflict
/// error, and — critically — commits **nothing**, including the other
/// participant's keys, proving the whole-transaction abort is atomic too.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn violated_precondition_aborts_the_whole_transaction() {
    let n = 3;
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].client;

    let lower_key = txn_key("k2", "");
    let upper_key = txn_key("k8", "");

    put_until_ok(addr0, "txn_t4", b"k1", b"seed-lower").await;
    put_until_ok(addr0, "txn_t4", b"k9", b"seed-upper").await;
    split_and_settle(&nodes, addr0, "txn_t4", b"k5").await;
    // The guarded key's real current value.
    put_until_ok(addr0, "txn_t4", &lower_key, b"real-value").await;

    let resp = timeout(
        Duration::from_secs(25),
        call(
            addr0,
            ClientRequest::Txn {
                writes: vec![
                    animusd::TxnTableWrite::plain(
                        "txn_t4".to_string(),
                        lower_key.clone(),
                        Some(b"should-not-land".to_vec()),
                    ),
                    animusd::TxnTableWrite::plain(
                        "txn_t4".to_string(),
                        upper_key.clone(),
                        Some(b"should-not-land-either".to_vec()),
                    ),
                ],
                preconditions: vec![(
                    "txn_t4".to_string(),
                    lower_key.clone(),
                    Some(b"wrong-expected-value".to_vec()),
                )],
                write_conditions: vec![],
            },
        ),
    )
    .await
    .expect("txn call did not return in 25s");
    assert!(
        matches!(resp, ClientResponse::Error(_)),
        "a violated precondition must abort the transaction: {resp:?}"
    );

    // Neither write landed — the guarded key keeps its real value, and the
    // other participant's key was never written at all.
    let k2 = call(
        addr0,
        ClientRequest::Get {
            key: lower_key,
            table: "txn_t4".to_string(),
            stale: false,
        },
    )
    .await;
    assert_eq!(k2, ClientResponse::Value(Some(b"real-value".to_vec())));
    let k8 = call(
        addr0,
        ClientRequest::Get {
            key: upper_key,
            table: "txn_t4".to_string(),
            stale: false,
        },
    )
    .await;
    assert_eq!(
        k8,
        ClientResponse::Value(None),
        "the other participant's key must never have been committed"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}
