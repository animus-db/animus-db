//! **Atomic `TransactWriteItems` + `TransactGetItems` over `ProdEnv`** (ADR
//! 0018 §2/PR7): the DynamoDB wire surface's cross-tablet transactional API,
//! through a real multi-process cluster with a genuinely pre-split table —
//! mirroring `cp_txn.rs`'s harness style, but issuing the transactional calls
//! through the actual DynamoDB JSON/HTTP edge (`dynamo.rs::run_transact`/
//! `run_transact_get`) instead of the raw `ClientRequest::Txn`.
//!
//! The multi-tablet 2PC mechanics themselves are proven at the primitive
//! level (`animus-cp-data/tests/txn_multi.rs`/`txn_recovery.rs`) and the
//! wire-level coordinator's forwarding is proven generically in `cp_txn.rs`;
//! this suite's job is narrower — proving `dynamo.rs`'s own atomic-rewrite
//! layer (condition evaluation, the whole-or-nothing guarantee, and the new
//! `TransactGetItems` quiescent-read primitive) is wired correctly end to
//! end, plus the `/admin/txns` observability surface this PR adds.
//!
//! Real TCP/time → polls with generous timeouts, never a fixed sleep.

use std::net::SocketAddr;
use std::time::Duration;

use animus_dynamo::AttributeValue;
use animusd::{ClientRequest, ClientResponse, Node, read_frame};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

// ---------------------------------------------------------------------------
// Shared bring-up + protocol helpers (mirrors cp_txn.rs / admin_endpoint.rs).
// ---------------------------------------------------------------------------

async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, animusd::ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 7);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[7 * i],
                client: addrs[7 * i + 1],
                dynamo: addrs[7 * i + 2],
                cql: addrs[7 * i + 3],
                admin: addrs[7 * i + 4],
                intra: addrs[7 * i + 5],
                console: addrs[7 * i + 6],
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

/// One DynamoDB request over a fresh HTTP/1.1 connection → `(status, body)`.
async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to dynamo");
    let request = format!(
        "POST / HTTP/1.1\r\n\
         Host: animus\r\n\
         X-Amz-Target: {target}\r\n\
         Content-Type: application/x-amz-json-1.0\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .expect("read full response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    (status, payload.to_string())
}

/// One admin HTTP/1.0 request → `(status, parsed JSON)` (mirrors
/// `admin_endpoint.rs`'s helper of the same name).
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

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

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

/// `Forwarded { TxnPrepare }` only succeeds against a tablet's own current
/// leader — cycle every node until one replies, mirroring `cp_txn.rs`'s
/// helper of the same name.
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

/// The exact data-plane key `dynamo.rs::item_key` computes for a simple
/// (partition-key-only) item: `partition_token(escape(pk)) ||
/// escape(pk)`. Replicated here (over the same public
/// `animus_dynamo`/`animus_tablet` primitives `dynamo.rs` itself calls) so
/// this test can pick two item ids that land on opposite sides of a chosen
/// split point *before* creating them — there is no other way to predict a
/// DynamoDB item's tablet placement from outside the edge.
fn item_key(pk: &str) -> Vec<u8> {
    let av = AttributeValue::S(pk.to_string());
    let escaped = animus_dynamo::storage_key(&av, None);
    let token = animus_tablet::partition_token(&escaped);
    let mut key = token.to_vec();
    key.extend_from_slice(&escaped);
    key
}

/// `CreateTable` (simple `id: S` partition key) then split its bootstrap
/// tablet (id 1) so a chosen pair of item ids lands in different tablets.
/// Returns `(lower_id, upper_id)` — two item ids known to straddle the
/// split, picked from a candidate pool by actually computing each
/// candidate's [`item_key`] and choosing an adjacent pair in sorted order
/// (mirrors `cp_txn.rs`'s `split_and_settle`, adapted to predict DynamoDB
/// item placement).
async fn create_table_pre_split(
    nodes: &[Node],
    dynamo_addr: SocketAddr,
    client_addr: SocketAddr,
    table: &str,
) -> (String, String) {
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable({table}) failed: {body}");

    let mut candidates: Vec<(String, Vec<u8>)> = (0..40)
        .map(|i| {
            let id = format!("item{i:03}");
            let key = item_key(&id);
            (id, key)
        })
        .collect();
    candidates.sort_by(|a, b| a.1.cmp(&b.1));
    let mid = candidates.len() / 2;
    let (lower_id, lower_key) = candidates[mid - 1].clone();
    let (upper_id, upper_key) = candidates[mid].clone();
    assert!(lower_key < upper_key, "candidates must be strictly ordered");

    // ADR 0050 (Train B rung 1): the client-facing split surface is disabled
    // during the storage pivot; the harness proposes the metadata command
    // directly instead. Sound ONLY because the table is still EMPTY here —
    // both children form over their own empty private engines; this gives
    // the transaction tests a genuine two-group topology, it does not
    // exercise split itself. `create_table` above waits for the bootstrap
    // tablet to exist, but the first PutItem is what lazily provisions it —
    // so provision it first with a throwaway probe write, then split.
    // The bootstrap tablet is provisioned lazily by the first write; the
    // CreateTable wait above only covers the schema. Ensure it exists.
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(|n| {
                n.metadata()
                    .tablets
                    .contains_key(&animus_tablet::TabletId(1))
            }) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("bootstrap tablet was never provisioned before the harness split");
    // ADR 0050 (Train B rung 5+): drive the REAL workflow via the public
    // kickoff on the client protocol; the driver cuts over on its own.
    match call(
        client_addr,
        ClientRequest::SplitTablet {
            tablet: 1,
            split_key: upper_key,
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

    // Confirm both halves actually serve before relying on them.
    for id in [&lower_id, &upper_id] {
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.PutItem",
            &format!(r#"{{"TableName":"{table}","Item":{{"id":{{"S":"probe-{id}"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200, "probe put for {id} failed: {body}");
    }

    (lower_id, upper_id)
}

// ---------------------------------------------------------------------------
// (a) Cross-tablet atomicity + follower-connected visibility.
// ---------------------------------------------------------------------------

/// **Atomic `TransactWriteItems` across a genuinely split table**: a
/// transaction writing one item below the split point and one above commits
/// as a single 2PC transaction, and both become visible together via
/// ordinary `GetItem` from a node that may not host either participant's
/// leader — the exact regression the old serial-loop implementation could
/// never give (each `Put` was its own independent write with no cross-tablet
/// coordination at all).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn transact_write_items_commits_atomically_across_a_split_table() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;
    let client0 = config.nodes[0].client;

    let (lower_id, upper_id) = create_table_pre_split(&nodes, addr0, client0, "txitems_a").await;

    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.TransactWriteItems",
        &format!(
            r#"{{"TransactItems":[
                {{"Put":{{"TableName":"txitems_a","Item":{{"id":{{"S":"{lower_id}"}},"v":{{"S":"lo"}}}}}}}},
                {{"Put":{{"TableName":"txitems_a","Item":{{"id":{{"S":"{upper_id}"}},"v":{{"S":"hi"}}}}}}}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "TransactWriteItems failed: {body}");

    // Read both back through a DIFFERENT node than the one the transaction
    // was issued through — both must be visible together.
    let reader = config.nodes[1 % n].dynamo;
    for (id, expected) in [(&lower_id, "lo"), (&upper_id, "hi")] {
        let (status, body) = dynamo(
            reader,
            "DynamoDB_20120810.GetItem",
            &format!(r#"{{"TableName":"txitems_a","Key":{{"id":{{"S":"{id}"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200, "GetItem({id}) failed: {body}");
        assert!(
            body.contains(&format!(r#""v":{{"S":"{expected}"}}"#)),
            "item {id} missing/wrong after a committed cross-tablet transaction: {body}"
        );
    }

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

// ---------------------------------------------------------------------------
// (b) A failing ConditionCheck cancels the WHOLE transaction.
// ---------------------------------------------------------------------------

/// **The old bug's exact counter-example, now closed**: a `TransactWriteItems`
/// with a `Put` in each half of a split table, plus a `ConditionCheck` that
/// fails. The old serial-loop implementation applied actions in order, so the
/// two `Put`s (listed before the failing check) would have already landed by
/// the time the check failed. The atomic rewrite must leave **neither** item
/// written.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn failing_condition_check_cancels_the_whole_transaction() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;
    let client0 = config.nodes[0].client;

    let (lower_id, upper_id) = create_table_pre_split(&nodes, addr0, client0, "txitems_b").await;

    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.TransactWriteItems",
        &format!(
            r#"{{"TransactItems":[
                {{"Put":{{"TableName":"txitems_b","Item":{{"id":{{"S":"{lower_id}"}},"v":{{"S":"should-not-land"}}}}}}}},
                {{"Put":{{"TableName":"txitems_b","Item":{{"id":{{"S":"{upper_id}"}},"v":{{"S":"should-not-land-either"}}}}}}}},
                {{"ConditionCheck":{{"TableName":"txitems_b","Key":{{"id":{{"S":"nonexistent-guard"}}}},
                                     "ConditionExpression":"attribute_exists(id)"}}}}]}}"#
        ),
    )
    .await;
    assert_eq!(
        status, 400,
        "expected the condition failure to cancel: {body}"
    );
    assert!(
        body.contains("TransactionCanceledException"),
        "expected TransactionCanceledException, got: {body}"
    );

    // Neither Put action landed, even though both preceded the failing check
    // in list order — proving whole-or-nothing, not serial-apply-then-fail.
    for id in [&lower_id, &upper_id] {
        let (status, body) = dynamo(
            addr0,
            "DynamoDB_20120810.GetItem",
            &format!(r#"{{"TableName":"txitems_b","Key":{{"id":{{"S":"{id}"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(
            body, "{}",
            "item {id} must NOT have been written by a cancelled transaction: {body}"
        );
    }

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

// ---------------------------------------------------------------------------
// (c) TransactGetItems returns a consistent snapshot under contention.
// ---------------------------------------------------------------------------

/// **`TransactGetItems` never observes a torn pair.** A background writer
/// repeatedly `TransactWriteItems`-updates two keys together so they always
/// sum to zero (`a = n`, `b = -n`); a concurrent reader repeatedly
/// `TransactGetItems`-reads the pair and asserts every observed pair is one
/// of the writer's own legal atomic states (`a + b == 0`) — a torn read
/// (the writer's old `a` paired with its new `b`, or vice versa) would sum
/// to something else.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn transact_get_items_never_observes_a_torn_pair_under_concurrent_writes() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;
    let client0 = config.nodes[0].client;

    let (a_id, b_id) = create_table_pre_split(&nodes, addr0, client0, "txitems_c").await;

    // Seed both keys at n=0.
    for id in [&a_id, &b_id] {
        let (status, body) = dynamo(
            addr0,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"txitems_c","Item":{{"id":{{"S":"{id}"}},"n":{{"N":"0"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "seed put for {id} failed: {body}");
    }

    let writer_addr = addr0;
    let a_id_w = a_id.clone();
    let b_id_w = b_id.clone();
    let writer = tokio::spawn(async move {
        for step in 1..=15i64 {
            let (status, body) = dynamo(
                writer_addr,
                "DynamoDB_20120810.TransactWriteItems",
                &format!(
                    r#"{{"TransactItems":[
                        {{"Put":{{"TableName":"txitems_c","Item":{{"id":{{"S":"{a_id_w}"}},"n":{{"N":"{step}"}}}}}}}},
                        {{"Put":{{"TableName":"txitems_c","Item":{{"id":{{"S":"{b_id_w}"}},"n":{{"N":"{}"}}}}}}}}]}}"#,
                    -step
                ),
            )
            .await;
            assert_eq!(status, 200, "writer step {step} failed: {body}");
        }
    });

    let reader_addr = config.nodes[2 % n].dynamo;
    let a_id_r = a_id.clone();
    let b_id_r = b_id.clone();
    let reader = tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut rounds = 0u32;
        while tokio::time::Instant::now() < deadline {
            let (status, body) = dynamo(
                reader_addr,
                "DynamoDB_20120810.TransactGetItems",
                &format!(
                    r#"{{"TransactItems":[
                        {{"Get":{{"TableName":"txitems_c","Key":{{"id":{{"S":"{a_id_r}"}}}}}}}},
                        {{"Get":{{"TableName":"txitems_c","Key":{{"id":{{"S":"{b_id_r}"}}}}}}}}]}}"#
                ),
            )
            .await;
            // A retryable cancellation (never quiesced within budget) is
            // acceptable under contention — it must never be silently torn.
            if status != 200 {
                assert!(
                    body.contains("TransactionCanceledException"),
                    "unexpected TransactGetItems failure: {body}"
                );
                continue;
            }
            let v: Value = serde_json::from_str(&body).expect("valid JSON");
            let responses = v["Responses"].as_array().expect("Responses array");
            assert_eq!(responses.len(), 2, "expected 2 responses: {body}");
            let n_of = |idx: usize| -> Option<i64> {
                responses[idx]["Item"]["n"]["N"]
                    .as_str()
                    .and_then(|s| s.parse::<i64>().ok())
            };
            if let (Some(a), Some(b)) = (n_of(0), n_of(1)) {
                assert_eq!(
                    a + b,
                    0,
                    "torn TransactGetItems snapshot: a={a}, b={b}, body={body}"
                );
                rounds += 1;
            }
        }
        rounds
    });

    writer.await.expect("writer task panicked");
    let rounds = reader.await.expect("reader task panicked");
    assert!(
        rounds > 0,
        "reader never observed a single consistent snapshot in 20s"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

// ---------------------------------------------------------------------------
// (d) Concurrent TransactWriteItems on overlapping keys: one winner.
// ---------------------------------------------------------------------------

/// **Concurrent transactions racing a shared, conditionally-written key are
/// each individually atomic — one wins, the loser's own OTHER key never
/// lands either.** Two transactions both attempt
/// `Put(shared, attribute_not_exists) + Put(own)`; exactly one must commit
/// both its writes, and the other must cancel with **neither** of its own
/// writes visible (proving the whole transaction aborts, not just the
/// conflicting action).
///
/// **Both racers go through the same node, deliberately** — see
/// `run_transact`'s own doc: a write action's own `ConditionExpression` is
/// protected only by this node's `rmw_lock` (the same guarantee a
/// single-item conditional `PutItem` always had, `dynamo_extended.rs`'s
/// `concurrent_conditional_puts_one_wins`), not by `cp_txn`'s cross-node OCC
/// mechanism — feeding a *written* key's own precondition into `cp_txn`
/// causes the self-referential stall documented there. Two transactions
/// racing the same contended key through *different* nodes have no such
/// guarantee (nothing here claims otherwise); this test proves what the
/// design actually delivers, not a stronger claim it doesn't.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_transact_write_items_on_a_shared_key_resolve_one_winner() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;
    let client0 = config.nodes[0].client;

    // Three items spanning the split: `shared` (contended), `own_a`/`own_b`
    // (each transaction's own, non-conflicting key).
    let (id0, id1) = create_table_pre_split(&nodes, addr0, client0, "txitems_d").await;
    // A third, deliberately re-derived id on the same side as id0 so all
    // three item keys are known ahead of time; reusing id0/id1 as `shared`/
    // `own_a` and minting `own_b` fresh (any id works — it doesn't need to
    // straddle the split for this test, only `shared` needs a known key).
    let shared = id0;
    let own_a = id1;
    let own_b = "concurrent-own-b".to_string();

    let addr_a = addr0;
    let addr_b = addr0;
    let shared_a = shared.clone();
    let shared_b = shared.clone();
    let own_a2 = own_a.clone();
    let own_b2 = own_b.clone();
    let table = "txitems_d".to_string();
    let table2 = table.clone();

    let body_a = format!(
        r#"{{"TransactItems":[
            {{"Put":{{"TableName":"{table}","Item":{{"id":{{"S":"{shared_a}"}},"owner":{{"S":"a"}}}},
                    "ConditionExpression":"attribute_not_exists(id)"}}}},
            {{"Put":{{"TableName":"{table}","Item":{{"id":{{"S":"{own_a2}"}},"owner":{{"S":"a"}}}}}}}}]}}"#
    );
    let body_b = format!(
        r#"{{"TransactItems":[
            {{"Put":{{"TableName":"{table2}","Item":{{"id":{{"S":"{shared_b}"}},"owner":{{"S":"b"}}}},
                    "ConditionExpression":"attribute_not_exists(id)"}}}},
            {{"Put":{{"TableName":"{table2}","Item":{{"id":{{"S":"{own_b2}"}},"owner":{{"S":"b"}}}}}}}}]}}"#
    );
    let (resp_a, resp_b) = tokio::join!(
        dynamo(addr_a, "DynamoDB_20120810.TransactWriteItems", &body_a),
        dynamo(addr_b, "DynamoDB_20120810.TransactWriteItems", &body_b),
    );

    let outcomes = [&resp_a, &resp_b];
    let wins = outcomes.iter().filter(|(s, _)| *s == 200).count();
    assert_eq!(
        wins, 1,
        "exactly one racing transaction should commit, got a={resp_a:?} b={resp_b:?}"
    );
    let winner_is_a = resp_a.0 == 200;

    // The winner's own key landed; the loser's own key did NOT — proving the
    // loser's whole transaction aborted, not just the contended action.
    let (winner_own, loser_own) = if winner_is_a {
        (&own_a, &own_b)
    } else {
        (&own_b, &own_a)
    };
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.GetItem",
        &format!(r#"{{"TableName":"txitems_d","Key":{{"id":{{"S":"{winner_own}"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200);
    assert_ne!(
        body, "{}",
        "the winning transaction's own key must be visible: {body}"
    );
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.GetItem",
        &format!(r#"{{"TableName":"txitems_d","Key":{{"id":{{"S":"{loser_own}"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        body, "{}",
        "the losing transaction's own key must NOT be visible: {body}"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

// ---------------------------------------------------------------------------
// (e) /admin/txns observability: a pending record appears, then clears.
// ---------------------------------------------------------------------------

/// **`/admin/txns` shows a pending transaction while its coordinator is
/// (simulated) stalled, and empties once recovery decides it** — driven by
/// sending the internal `TxnPrepare` wire requests directly and never
/// deciding (mirroring `cp_txn.rs`'s coordinator-crash test), since
/// `ClientCtx::cp_txn` runs synchronously with no separate coordinator
/// process to kill after prepare.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admin_txns_shows_a_pending_record_then_clears_after_recovery() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].client;
    // ADR 0047: Forwarded is intra-only.
    let all_addrs: Vec<SocketAddr> = config.nodes.iter().map(|c| c.intra).collect();

    // A single-participant (degenerate) transaction is enough here — the
    // anchor's own tablet is what `/admin/txns` on that tablet's leader
    // reports on.
    call(
        addr0,
        ClientRequest::Put {
            key: b"admin-txn-seed".to_vec(),
            value: b"seed".to_vec(),
            table: "admintxn".to_string(),
        },
    )
    .await;

    let (txn_id, _record_key, _record_table, _ts) = prepare_via_any_node(
        &all_addrs,
        ClientRequest::TxnPrepare {
            table: "admintxn".to_string(),
            anchor: None,
            writes: vec![animus_cp_data::TxnWrite::plain(
                b"admin-txn-key".to_vec(),
                Some(b"pending-value".to_vec()),
            )],
            conditions: Vec::new(),
            participant_spans: Vec::new(),
            pending_kind_writes: Vec::new(),
        },
    )
    .await;
    // Never send TxnDecide/TxnResolve — "the coordinator crashed here."

    // Poll every node's `/admin/txns` until one reports this exact txn as
    // pending (whichever node currently leads the anchor's tablet).
    let expected_txn_id = format!("{txn_id:?}");
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|c| c.admin).collect();
    timeout(Duration::from_secs(10), async {
        loop {
            for &addr in &admin_addrs {
                let (status, body) = admin_get(addr, "/admin/txns").await;
                if status != 200 {
                    continue;
                }
                let groups = body["groups"].as_array().cloned().unwrap_or_default();
                for g in &groups {
                    let pending = g["pending"].as_array().cloned().unwrap_or_default();
                    if pending
                        .iter()
                        .any(|p| p["txn_id"].as_str() == Some(expected_txn_id.as_str()))
                    {
                        return;
                    }
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("/admin/txns never showed the pending transaction within 10s");

    // Past RECOVERY_GRACE (5s) + a resolver tick, the record must be decided
    // and eventually resolved — `/admin/txns` on every node converges to no
    // longer listing this txn_id as pending anywhere.
    timeout(Duration::from_secs(20), async {
        loop {
            let mut still_pending = false;
            for &addr in &admin_addrs {
                let (status, body) = admin_get(addr, "/admin/txns").await;
                if status != 200 {
                    continue;
                }
                let groups = body["groups"].as_array().cloned().unwrap_or_default();
                for g in &groups {
                    let pending = g["pending"].as_array().cloned().unwrap_or_default();
                    if pending
                        .iter()
                        .any(|p| p["txn_id"].as_str() == Some(expected_txn_id.as_str()))
                    {
                        still_pending = true;
                    }
                }
            }
            if !still_pending {
                return;
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect(
        "/admin/txns still listed the transaction as pending after RECOVERY_GRACE + resolver \
         margin",
    );

    // And the write itself converged to committed (recovery drove it, since
    // every participant — here just the anchor — genuinely staged).
    let got = timeout(Duration::from_secs(10), async {
        loop {
            match call(
                addr0,
                ClientRequest::Get {
                    key: b"admin-txn-key".to_vec(),
                    table: "admintxn".to_string(),
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
    .expect("admin-txn-key never converged to committed");
    assert_eq!(got, ClientResponse::Value(Some(b"pending-value".to_vec())));

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

// ---------------------------------------------------------------------------
// (f) ADR 0018 §2 apply-time write-key conditions amendment: a write
// action's own `ConditionExpression` now has real CROSS-NODE OCC, not just
// the same-node protection `concurrent_transact_write_items_on_a_shared_
// key_resolve_one_winner` above already proved.
// ---------------------------------------------------------------------------

/// **The test PR7 couldn't write.** Two clients on DIFFERENT nodes both read
/// the same (absent) initial state and race a `TransactWriteItems` whose own
/// `Put` carries a condition on that exact key it also writes — the
/// self-referential case the PR7 amendment documented as protected only by
/// `ctx.data().rmw_lock` (same-node only). Since this amendment, the
/// condition is checked at *apply* time on the tablet itself
/// (`animus_cp_data::KvCommand::TxnStage`'s `conditions`), so which node
/// issued the request no longer matters: exactly one commits, the loser gets
/// a genuine `TransactionCanceledException`, and the final state reflects
/// exactly one write.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_node_racing_own_key_conditional_writes_resolve_exactly_one_winner() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr_a = config.nodes[0].dynamo;
    let addr_b = config.nodes[1].dynamo;

    let (status, body) = dynamo(
        addr_a,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"txitems_cross","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    let table = "txitems_cross".to_string();
    let shared = "cross-node-shared".to_string();
    let body_a = format!(
        r#"{{"TransactItems":[
            {{"Put":{{"TableName":"{table}","Item":{{"id":{{"S":"{shared}"}},"owner":{{"S":"a"}}}},
                    "ConditionExpression":"attribute_not_exists(id)"}}}}]}}"#
    );
    let body_b = format!(
        r#"{{"TransactItems":[
            {{"Put":{{"TableName":"{table}","Item":{{"id":{{"S":"{shared}"}},"owner":{{"S":"b"}}}},
                    "ConditionExpression":"attribute_not_exists(id)"}}}}]}}"#
    );

    let (resp_a, resp_b) = timeout(Duration::from_secs(15), async {
        tokio::join!(
            dynamo(addr_a, "DynamoDB_20120810.TransactWriteItems", &body_a),
            dynamo(addr_b, "DynamoDB_20120810.TransactWriteItems", &body_b),
        )
    })
    .await
    .expect("racing cross-node conditional writes did not both resolve within 15s");

    let wins = [&resp_a, &resp_b].iter().filter(|(s, _)| *s == 200).count();
    assert_eq!(
        wins, 1,
        "exactly one cross-node racing transaction should commit, got a={resp_a:?} b={resp_b:?}"
    );
    let (loser_status, loser_body) = if resp_a.0 == 200 { &resp_b } else { &resp_a };
    assert_eq!(
        *loser_status, 400,
        "the losing cross-node transaction must be cancelled: {loser_body}"
    );
    assert!(
        loser_body.contains("TransactionCanceledException"),
        "expected TransactionCanceledException, got: {loser_body}"
    );

    // Final state, read from a THIRD node, reflects exactly one write.
    let (status, body) = dynamo(
        config.nodes[2 % n].dynamo,
        "DynamoDB_20120810.GetItem",
        &format!(r#"{{"TableName":"{table}","Key":{{"id":{{"S":"{shared}"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains(r#""owner":{"S":"a"}"#) || body.contains(r#""owner":{"S":"b"}"#),
        "the shared key must reflect exactly one committed write, not both/neither: {body}"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// **An own-key condition failure cancels a multi-tablet transaction
/// wholly** — the own-key-condition counterpart of `failing_condition_
/// check_cancels_the_whole_transaction` above (which exercises a
/// `ConditionCheck` action's failure, a structurally different code path).
/// The conditioned `Put` targets one tablet (below the split) and fails its
/// own condition; the other tablet's action (above the split, no condition
/// of its own) must never land either.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn own_key_condition_failure_cancels_a_multi_tablet_transaction_wholly() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;
    let client0 = config.nodes[0].client;

    let (lower_id, upper_id) = create_table_pre_split(&nodes, addr0, client0, "txitems_e").await;

    // Pre-seed the lower-tablet key so its own `attribute_not_exists`
    // condition is guaranteed to fail.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        &format!(
            r#"{{"TableName":"txitems_e","Item":{{"id":{{"S":"{lower_id}"}},"v":{{"S":"original"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "pre-seed failed: {body}");

    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.TransactWriteItems",
        &format!(
            r#"{{"TransactItems":[
                {{"Put":{{"TableName":"txitems_e","Item":{{"id":{{"S":"{lower_id}"}},"v":{{"S":"overwrite"}}}},
                        "ConditionExpression":"attribute_not_exists(id)"}}}},
                {{"Put":{{"TableName":"txitems_e","Item":{{"id":{{"S":"{upper_id}"}},"v":{{"S":"should-not-land"}}}}}}}}]}}"#
        ),
    )
    .await;
    assert_eq!(
        status, 400,
        "the own-key condition failure should cancel the whole transaction: {body}"
    );
    assert!(
        body.contains("TransactionCanceledException"),
        "expected TransactionCanceledException, got: {body}"
    );

    // The conditioned key keeps its ORIGINAL value, never the overwrite.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.GetItem",
        &format!(r#"{{"TableName":"txitems_e","Key":{{"id":{{"S":"{lower_id}"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains(r#""v":{"S":"original"}"#),
        "the conditioned key must keep its original value, never the overwrite: {body}"
    );

    // The OTHER tablet's action — carrying no condition of its own — must
    // never have landed either: no partial state on the other tablet.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.GetItem",
        &format!(r#"{{"TableName":"txitems_e","Key":{{"id":{{"S":"{upper_id}"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        body, "{}",
        "the OTHER tablet's action must never land when an own-key condition on a \
         DIFFERENT tablet fails: {body}"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// **The PR7 stall regression stays dead.** A `TransactWriteItems` `Put`
/// carrying a `ConditionExpression` on its own key — the exact shape that,
/// before PR7's fix, fed the pre-read into `cp_txn`'s cross-key precondition
/// mechanism and stalled for `RECOVERY_GRACE` (5s) waiting on its own
/// in-flight intent (see `run_transact`'s doc). PR7's own fix (never routing
/// a write's own condition through that mechanism) already killed the
/// stall; this amendment's apply-time OCC must not reintroduce it — and
/// structurally can't, since the condition is checked once, inside the same
/// atomic apply step that stages the intent, never as a second read of a
/// key this same transaction just staged.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn own_key_condition_completes_quickly_with_no_recovery_grace_stall() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;

    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"txitems_f","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    let started = std::time::Instant::now();
    let (status, body) = timeout(
        Duration::from_secs(2),
        dynamo(
            addr0,
            "DynamoDB_20120810.TransactWriteItems",
            r#"{"TransactItems":[
                {"Put":{"TableName":"txitems_f","Item":{"id":{"S":"stall-guard"},"v":{"S":"v1"}},
                        "ConditionExpression":"attribute_not_exists(id)"}}]}"#,
        ),
    )
    .await
    .expect(
        "an own-key conditioned TransactWriteItems must complete within 2s, never stall for \
         RECOVERY_GRACE (the PR7 stall-bug shape)",
    );
    assert_eq!(
        status, 200,
        "the own-key conditioned transaction should commit: {body}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "own-key condition must not stall (elapsed={:?})",
        started.elapsed()
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}
