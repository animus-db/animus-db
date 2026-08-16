//! **ADR 0018 §2/PR5 corrective note, task #18**: `ClientCtx::cp_txn`'s
//! production coordinator did NOT actually hand the anchor's stage every
//! *other* participant's `(table, span)` list, despite the PR5 amendment and
//! `animusd/CLAUDE.md` both describing it that way — a grep of
//! `crates/animusd/src/lib.rs` found no reference to `participant_spans`
//! anywhere in `ClientCtx::cp_txn`/`txn_prepare` at all; the coordinator's
//! own anchor-stage call site always handed `RaftKvNode::txn_stage`
//! (equivalently, `txn_stage_anchor` with an EMPTY `participant_spans`),
//! never populating it from the write groups `cp_txn` already computes.
//! `animus-cp-data`'s primitive itself was always correct (proven directly
//! in `txn_recovery.rs`/`txn_serializable.rs`, which call `txn_stage_anchor`
//! by hand with a real `participant_spans`); this was a coordinator-level
//! wiring gap.
//!
//! **Why this mattered for correctness, not just observability**:
//! `ClientCtx::txn_recover`'s in-doubt recovery decides `all_staged` by
//! walking ONLY `TxnRecordView::intent_spans` (`lib.rs`'s `txn_recover`, the
//! `for (table, span) in &view.intent_spans` loop) — with that list only
//! ever naming the anchor's own keys, a transaction whose coordinator staged
//! the anchor and then crashed **before ever attempting a participant's own
//! stage** looked, to recovery, exactly like a **single-participant**
//! transaction that staged completely: `all_staged` came back `true`
//! (trivially — every entry in the too-short list did stage), so recovery
//! **committed** a transaction whose participant write never happened. A
//! genuine cross-tablet atomicity violation on the recovery path: one half
//! of an intended atomic write became visible while the other half silently
//! never landed, and nothing else in the system ever revisited the
//! decision. **Confirmed live** before this fix: hand-driving the exact
//! wire bytes the unfixed coordinator sent (`participant_spans: Vec::new()`)
//! in this scenario reliably reproduced a wrongly-`Committed` record and a
//! visible anchor-key value within ~7s (well under `RECOVERY_GRACE` +
//! margin) — see this ADR's own corrective note and the task's own report
//! for the exact failing run.
//!
//! **The fix**: `cp_txn` now computes the full participant `(table, span)`
//! list from the same `groups` map it already builds (right after removing
//! the anchor's own entry, so what remains in `groups` *is* every other
//! participant) and threads it through `txn_prepare`/`txn_prepare_pushing`
//! into the anchor's stage call — locally via `CpGroup::txn_stage`, which
//! now calls `RaftKvNode::txn_stage_anchor` directly instead of the
//! single-participant `txn_stage` convenience, or over the wire via a new
//! `ClientRequest::TxnPrepare::participant_spans` field.
//!
//! **This test is the regression**: it constructs the exact wire bytes a
//! *fixed* coordinator now sends for "anchor stages, participant is
//! declared but never gets a chance to stage" (mirroring `cp_txn.rs`'s
//! coordinator-crash tests' own technique — drive the internal `TxnPrepare`
//! wire request directly, then simply never send the rest, since
//! `ClientCtx::cp_txn` runs synchronously with no separate coordinator
//! process to literally kill) and polls `/admin/txns` (mirroring
//! `dynamo_txn.rs`'s `admin_txns_shows_a_pending_record_then_clears_after_
//! recovery`) to confirm recovery now correctly decides **Aborted**, never
//! letting the anchor's own key become visible.

use std::net::SocketAddr;
use std::time::Duration;

use animus_tablet::KeyRange;
use animusd::{ClientRequest, ClientResponse, Node, read_frame};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// Mirrors `cp_txn.rs`'s identical helper: send `request` wrapped in
/// `ClientRequest::Forwarded` directly — the shape `ClientCtx::cp_forward`
/// uses internally, and the mechanism that lets this test drive the 2PC
/// prepare phase without a coordinator ever completing the transaction.
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

/// Mirrors `cp_txn.rs`'s identical helper.
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

/// Mirrors `dynamo_txn.rs`'s identical helper: one admin HTTP/1.0 request →
/// `(status, parsed JSON)`.
async fn admin_get(addr: SocketAddr, path: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let request = format!(
        "GET {path} HTTP/1.0\r\n\
         Host: animus\r\n\
         Connection: close\r\n\
         \r\n"
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
    let value: Value = serde_json::from_str(payload)
        .unwrap_or_else(|e| panic!("admin body is not JSON ({e}): {payload}"));
    (status, value)
}

async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, animusd::ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 5);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[5 * i],
                client: addrs[5 * i + 1],
                dynamo: addrs[5 * i + 2],
                cql: addrs[5 * i + 3],
                admin: addrs[5 * i + 4],
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

async fn put_until_ok(addr: SocketAddr, table: &str, key: &[u8], value: &[u8]) {
    timeout(Duration::from_secs(25), async {
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
                ClientResponse::Error(_) => sleep(Duration::from_millis(150)).await,
                other => panic!("unexpected put response: {other:?}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("put {table}/{key:?} did not succeed in 25s"));
}

/// Mirrors `cp_txn.rs`'s identical helper.
async fn split_and_settle(nodes: &[Node], addr: SocketAddr, table: &str, split_key: &[u8]) {
    let resp = call(
        addr,
        ClientRequest::SplitTablet {
            tablet: 1,
            split_key: split_key.to_vec(),
        },
    )
    .await;
    assert!(
        matches!(resp, ClientResponse::PutOk),
        "split trigger rejected: {resp:?}"
    );
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().all(|n| n.metadata().tablets.len() == 2) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("split was not recorded in the tablet map within 20s");

    let probe_key = [split_key, b"zzz-probe"].concat();
    put_until_ok(addr, table, &probe_key, b"probe").await;
}

const TOKEN_BYTES: usize = 8;
fn txn_key(prefix: &str, suffix: &str) -> Vec<u8> {
    let mut s = format!("{prefix}{suffix}");
    while s.len() < TOKEN_BYTES {
        s.push('_');
    }
    s.into_bytes()
}

/// **The damning test.** A genuine two-tablet transaction (a real pre-split
/// table, exactly like `cp_txn.rs`'s multi-tablet tests): the anchor's own
/// stage is driven directly and completes; the participant's own stage is
/// **never even attempted** — the shape a coordinator dying between
/// staging the anchor and staging the second participant produces. Nobody
/// ever proposes commit/abort/resolve. `participant_spans` is populated
/// exactly as the fixed `ClientCtx::cp_txn` now computes it — the upper
/// key's own point-span — since a real fixed coordinator computes this
/// list *before* ever staging anything, so a crash in this exact window
/// would always carry it.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn anchor_only_stage_with_a_declared_but_unstaged_participant_recovers_to_abort() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].client;
    let all_addrs: Vec<SocketAddr> = config.nodes.iter().map(|c| c.client).collect();
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|c| c.admin).collect();

    put_until_ok(addr0, "txn_spans_a", b"k1", b"seed-lower").await;
    put_until_ok(addr0, "txn_spans_a", b"k9", b"seed-upper").await;
    split_and_settle(&nodes, addr0, "txn_spans_a", b"k5").await;

    let lower_key = txn_key("k2", "-anchoronly");
    // `upper_key` is declared to the anchor's own stage via
    // `participant_spans` (exactly as a fixed coordinator would) but is
    // deliberately never staged at all — this test simulates a crash
    // after the anchor's stage completed but before the participant's own
    // `TxnPrepare` was ever sent.
    let upper_key = txn_key("k8", "-anchoronly");
    let mut upper_span_end = upper_key.clone();
    upper_span_end.push(0);
    let participant_spans = vec![(
        "txn_spans_a".to_string(),
        KeyRange::new(upper_key.clone(), Some(upper_span_end)),
    )];

    let (txn_id, _record_key, _record_table, _ts) = prepare_via_any_node(
        &all_addrs,
        ClientRequest::TxnPrepare {
            table: "txn_spans_a".to_string(),
            anchor: None,
            writes: vec![animus_cp_data::TxnWrite::plain(
                lower_key.clone(),
                Some(b"should-not-commit".to_vec()),
            )],
            conditions: Vec::new(),
            participant_spans,
            pending_kind_writes: Vec::new(),
        },
    )
    .await;
    // Never prepare the participant. Never send TxnDecide/TxnResolve.

    let expected_txn_id = format!("{txn_id:?}");

    // Wait for the record to appear as pending, and capture its
    // `intent_spans` — confirms both keys are named (the anchor's own,
    // plus the declared-but-unstaged participant's), matching what the
    // fixed coordinator now sends.
    let pending_spans: Vec<String> = timeout(Duration::from_secs(10), async {
        loop {
            for &addr in &admin_addrs {
                let (status, body) = admin_get(addr, "/admin/txns").await;
                if status != 200 {
                    continue;
                }
                let groups = body["groups"].as_array().cloned().unwrap_or_default();
                for g in &groups {
                    let pending = g["pending"].as_array().cloned().unwrap_or_default();
                    if let Some(p) = pending
                        .iter()
                        .find(|p| p["txn_id"].as_str() == Some(expected_txn_id.as_str()))
                    {
                        let spans: Vec<String> = p["intent_spans"]
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .filter_map(|v| v.as_str().map(str::to_string))
                                    .collect()
                            })
                            .unwrap_or_default();
                        return spans;
                    }
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("/admin/txns never showed the pending transaction within 10s");

    assert_eq!(
        pending_spans.len(),
        2,
        "the record's intent_spans must name BOTH the anchor's own key and the declared \
         participant's key — got {pending_spans:?}"
    );

    // Past RECOVERY_GRACE (5s) + resolver margin, `txn_resolver_loop` must
    // push this record to a decision: `all_staged` must come back false
    // (the participant's span was never actually staged), so the record
    // must decide Aborted, and the anchor's own key must never become
    // visible. Poll both the admin view and the anchor key's own value —
    // the ground truth is the value, but the admin view is useful
    // diagnostic context if this ever regresses.
    let mut last_seen_outcome: Option<String> = None;
    let final_get = timeout(Duration::from_secs(30), async {
        loop {
            for &addr in &admin_addrs {
                let (status, body) = admin_get(addr, "/admin/txns").await;
                if status == 200 {
                    let groups = body["groups"].as_array().cloned().unwrap_or_default();
                    for g in &groups {
                        let unresolved = g["unresolved_decided"]
                            .as_array()
                            .cloned()
                            .unwrap_or_default();
                        if let Some(u) = unresolved
                            .iter()
                            .find(|u| u["txn_id"].as_str() == Some(expected_txn_id.as_str()))
                        {
                            last_seen_outcome =
                                Some(u["outcome"].as_str().unwrap_or_default().to_string());
                        }
                    }
                }
            }
            if let ClientResponse::Value(Some(v)) = call(
                addr0,
                ClientRequest::Get {
                    key: lower_key.clone(),
                    table: "txn_spans_a".to_string(),
                },
            )
            .await
            {
                return Some(v);
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap_or(None);

    assert!(
        final_get.is_none(),
        "ATOMICITY VIOLATION: the anchor's key became visible (value={final_get:?}, last \
         observed decision={last_seen_outcome:?}) even though its declared participant's \
         write never happened anywhere — recovery must decide Aborted, not Committed \
         (task #18)."
    );

    for node in &nodes {
        node.shutdown();
    }
}

/// **ADR 0018 §2 write-loss amendment (Bug 3) regression.** A genuine
/// two-tablet, two-participant transaction — anchor and participant BOTH
/// staged, on a real pre-split table — whose coordinator then does nothing
/// else at all: no `TxnDecide`, no `TxnResolve`. The **only** path that ever
/// commits and resolves this transaction is `txn_resolver_loop`'s automatic
/// background sweep, past `RECOVERY_GRACE`: `txn_recover` decides Committed
/// (`all_staged` is genuinely true this time), then `ClientCtx::
/// recovery_resolve` must resolve BOTH keys.
///
/// **This is exactly the shape `ClientCtx::recovery_resolve`'s pre-fix
/// table-only grouping mishandled**: the anchor's own span is always first
/// in `TxnRecord::intent_spans` (`txn_stage_anchor` puts its own span before
/// `participant_spans`), so the old `by_table` grouping bundled BOTH keys
/// into one `txn_resolve_participant` call routed by `keys[0]` — the
/// anchor's own key — landing the WHOLE bundle on the anchor's tablet. With
/// `KvCommand::TxnResolve`'s fence now in place, that misrouted entry is
/// rejected **whole-or-nothing** (the participant's key falls outside the
/// anchor tablet's fence), so neither key would ever resolve — not even the
/// anchor's own, despite it being on the "right" tablet, because it rode
/// the same bundled entry as the participant's foreign key. Both keys would
/// stay `Pending` forever (nothing else ever calls resolve on this
/// transaction). Post-fix, `recovery_resolve` re-resolves each key's own
/// current tablet and issues one call per `(table, tablet)`, so both land
/// correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn recovery_resolve_correctly_commits_both_tablets_of_a_two_tablet_transaction() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].client;
    let all_addrs: Vec<SocketAddr> = config.nodes.iter().map(|c| c.client).collect();

    put_until_ok(addr0, "txn_group_fix", b"k1", b"seed-lower").await;
    put_until_ok(addr0, "txn_group_fix", b"k9", b"seed-upper").await;
    split_and_settle(&nodes, addr0, "txn_group_fix", b"k5").await;

    let lower_key = txn_key("k2", "-groupfix");
    let upper_key = txn_key("k8", "-groupfix");
    let mut upper_span_end = upper_key.clone();
    upper_span_end.push(0);
    let participant_spans = vec![(
        "txn_group_fix".to_string(),
        KeyRange::new(upper_key.clone(), Some(upper_span_end)),
    )];

    let (txn_id, record_key, record_table, _ts) = prepare_via_any_node(
        &all_addrs,
        ClientRequest::TxnPrepare {
            table: "txn_group_fix".to_string(),
            anchor: None,
            writes: vec![animus_cp_data::TxnWrite::plain(
                lower_key.clone(),
                Some(b"lower-committed".to_vec()),
            )],
            conditions: Vec::new(),
            participant_spans,
            pending_kind_writes: Vec::new(),
        },
    )
    .await;

    // Actually stage the participant this time (unlike the sibling
    // abort-recovery test above) — both halves genuinely land, so recovery
    // must decide Committed, not Aborted.
    let (_txn_id2, _record_key2, _record_table2, _ts2) = prepare_via_any_node(
        &all_addrs,
        ClientRequest::TxnPrepare {
            table: "txn_group_fix".to_string(),
            anchor: Some((txn_id, record_key, record_table)),
            writes: vec![animus_cp_data::TxnWrite::plain(
                upper_key.clone(),
                Some(b"upper-committed".to_vec()),
            )],
            conditions: Vec::new(),
            participant_spans: Vec::new(),
            pending_kind_writes: Vec::new(),
        },
    )
    .await;

    // Never send TxnDecide/TxnResolve — `txn_resolver_loop`'s automatic
    // background sweep (past RECOVERY_GRACE, 5s) is the only thing that
    // will ever commit and resolve this record.
    //
    // **A plain `Get` is the wrong probe here**: a point read resolves a
    // still-`Pending` intent it hits *on demand*, purely at read time
    // (`resolve_once_step`/`resolve_decided` — see `RaftKvNode::local_get`'s
    // doc), the moment it can determine the covering transaction's decided
    // status — which it already can here, since `TxnDecide` (unaffected by
    // this bug) committed the record regardless of whether the per-key
    // resolve itself ever physically landed. That on-demand resolution
    // would mask exactly the bug this test exists to catch. A **`Scan`**
    // is the discriminating probe instead: scan resolution is
    // non-blocking and silently **omits** a still-`Pending` row rather
    // than resolving it on the fly (`resolve_scan_rows`'s doc) — so a
    // physically-unresolved intent is genuinely invisible to it, exactly
    // the residual correctness gap the pre-fix grouping bug leaves behind
    // even with `KvCommand::TxnResolve`'s fence protecting the physical
    // key from corruption.
    // Bounded to just below the split's own probe key (`split_and_settle`
    // seeds `[split_key]zzz-probe`, which also falls in this window) —
    // rather than count pairs, just check the two keys this test cares
    // about are both present, so an unrelated third key in-range doesn't
    // change this test's pass/fail signal.
    let mut scan_end = upper_key.clone();
    scan_end.push(0);
    let by_key = timeout(Duration::from_secs(30), async {
        loop {
            if let ClientResponse::Pairs(pairs) = call(
                addr0,
                ClientRequest::Scan {
                    start: lower_key.clone(),
                    end: Some(scan_end.clone()),
                    limit: None,
                    table: "txn_group_fix".to_string(),
                },
            )
            .await
            {
                let by_key: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
                    pairs.into_iter().collect();
                if by_key.contains_key(&lower_key) && by_key.contains_key(&upper_key) {
                    return by_key;
                }
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "both keys never became visible to a Scan within 30s — recovery_resolve must \
             resolve every tablet of a multi-tablet transaction, not just the one its \
             bundled keys[0] happens to route to (ADR 0018 §2 write-loss amendment); a \
             physically-unresolved intent is silently omitted from a scan even once its \
             transaction is known-committed"
        )
    });
    assert_eq!(
        by_key.get(&lower_key).map(Vec::as_slice),
        Some(b"lower-committed".as_slice()),
        "lower_key must scan back as committed"
    );
    assert_eq!(
        by_key.get(&upper_key).map(Vec::as_slice),
        Some(b"upper-committed".as_slice()),
        "upper_key must scan back as committed"
    );

    for node in &nodes {
        node.shutdown();
    }
}
