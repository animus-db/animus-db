//! The recorded DynamoDB-wire workload and the oracle feed.
//!
//! The model is the same list-append model the sim corpus
//! (`src/sim_cluster_dynamo_corpus.rs`) uses, so `animus-test`'s oracles run
//! unchanged: every write is
//! `UpdateItem SET items = list_append(if_not_exists(items,:empty),:v)` with a
//! globally-unique `:v`; each key has exactly one writer client; consistent
//! reads (`GetItem`/`TransactGetItems`) feed `check_cycles`; eventual reads
//! are held aside and checked as prefixes of the converged final state.
//!
//! History discipline (animus-test/CLAUDE.md): an op that may have been sent
//! and got no clean answer (timeout, reset, non-200) is `info`, never `fail`;
//! only a connect failure (no byte sent) is `fail`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use animus_test::history::{Key, ListVal, Mop, Process};
use animus_test::{
    CheckReport, History, Recorder, check_convergence, check_cycles, check_durability,
};
use serde_json::{Value, json};

use super::client::{CallErr, dynamo_call};
use super::rng::Rng;

pub const TABLE: &str = "chaos";
pub const KEYS: u64 = 24;
pub const CLIENTS: u64 = 6;
const OP_TIMEOUT: Duration = Duration::from_secs(8);

pub fn pk(key: Key) -> Value {
    json!({"pk": {"S": format!("k{key}")}, "sk": {"S": "s"}})
}

fn one_elem(v: u64) -> Value {
    json!({"L": [{"N": v.to_string()}]})
}

fn decode_items(item: &Value) -> Vec<u64> {
    item.get("items")
        .and_then(|v| v.get("L"))
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|e| e.get("N")?.as_str()?.parse::<u64>().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// One `ConsistentRead: false` observation, with enough context to tell a
/// permanently diverged replica from a transient read anomaly: which node
/// the client asked (the node serves from its own replica when it can, else
/// forwards to another replica, so this bounds the serving replica to
/// "this node or a peer") and when (ns since the run started).
#[derive(Clone, Debug)]
pub struct EventualRead {
    pub key: Key,
    pub list: Vec<u64>,
    pub node: SocketAddr,
    pub at_ns: u64,
}

/// A two-key transaction's appends, for the atomicity check.
#[derive(Clone, Debug)]
pub struct TxnPair {
    pub k0: Key,
    pub v0: u64,
    pub k1: Key,
    pub v1: u64,
    pub acked: bool,
}

#[derive(Default)]
pub struct Stats {
    pub ok_writes: AtomicU64,
    pub info_writes: AtomicU64,
    pub fail_writes: AtomicU64,
    pub ok_reads: AtomicU64,
    pub info_reads: AtomicU64,
    pub eventual_reads: AtomicU64,
    pub ok_txn_writes: AtomicU64,
    pub ok_txn_reads: AtomicU64,
}

pub struct Shared {
    pub seed: u64,
    pub rec: Mutex<Recorder>,
    pub eventual: Mutex<Vec<EventualRead>>,
    pub txns: Mutex<Vec<TxnPair>>,
    pub trace: Mutex<Vec<String>>,
    /// `ANIMUS_CHAOS_TXN=0` drops the multi-key transaction ops (bisecting aid).
    pub txn_ops: bool,
    next_val: AtomicU64,
    pub stats: Stats,
    pub stop: AtomicBool,
    pub start: Instant,
    /// Added to every key a client picks (0 for chaos). The soak harness
    /// gives each epoch a fresh key range so histories stay bounded.
    pub key_base: u64,
    /// Inter-op pause: `base + below(spread)` ms (chaos: 5 + below(20)).
    pub pace_ms: (u64, u64),
}

impl Shared {
    pub fn new(seed: u64) -> Self {
        Self::with_base(seed, 0, (5, 20))
    }

    pub fn with_base(seed: u64, key_base: u64, pace_ms: (u64, u64)) -> Self {
        Self {
            seed,
            rec: Mutex::new(Recorder::new(seed)),
            eventual: Mutex::new(Vec::new()),
            txns: Mutex::new(Vec::new()),
            trace: Mutex::new(Vec::new()),
            txn_ops: std::env::var("ANIMUS_CHAOS_TXN").map_or(true, |v| v.trim() != "0"),
            next_val: AtomicU64::new(1),
            stats: Stats::default(),
            stop: AtomicBool::new(false),
            start: Instant::now(),
            key_base,
            pace_ms,
        }
    }

    fn now(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }

    fn fresh(&self) -> u64 {
        self.next_val.fetch_add(1, Ordering::Relaxed)
    }
}

/// `info` for any error after a request may have left; `fail` for a connect
/// failure (nothing was sent).
fn conclude_write(
    sh: &Shared,
    proc: Process,
    mops: Vec<Mop>,
    res: &Result<(u16, String), CallErr>,
    node: SocketAddr,
) -> bool {
    if !matches!(res, Ok((200, _))) {
        let what = match res {
            Ok((s, b)) => format!("{s} {}", b.chars().take(160).collect::<String>()),
            Err(e) => e.to_string(),
        };
        let mut trace = sh.trace.lock().expect("trace");
        // Bounded: a multi-day soak must not grow this without limit.
        if trace.len() < 50_000 {
            trace.push(format!(
                "t={:.2}s p{proc} via {node} {mops:?} -> {what}",
                sh.start.elapsed().as_secs_f64()
            ));
        }
    }
    let mut rec = sh.rec.lock().expect("recorder");
    match res {
        Ok((200, _)) => {
            rec.ok(proc, sh.now(), mops);
            true
        }
        Err(CallErr::Connect(_)) => {
            rec.fail(proc, sh.now(), mops);
            sh.stats.fail_writes.fetch_add(1, Ordering::Relaxed);
            false
        }
        _ => {
            rec.info(proc, sh.now(), mops);
            sh.stats.info_writes.fetch_add(1, Ordering::Relaxed);
            false
        }
    }
}

async fn run_write(sh: &Shared, proc: Process, key: Key, node: SocketAddr) {
    let value = sh.fresh();
    let mops = vec![Mop::Append { key, value }];
    sh.rec
        .lock()
        .expect("recorder")
        .invoke(proc, sh.now(), mops.clone());
    let body = json!({
        "TableName": TABLE,
        "Key": pk(key),
        "UpdateExpression": "SET items = list_append(if_not_exists(items, :empty), :v)",
        "ExpressionAttributeValues": {":empty": {"L": []}, ":v": one_elem(value)},
    })
    .to_string();
    let res = dynamo_call(node, "UpdateItem", &body, OP_TIMEOUT).await;
    if conclude_write(sh, proc, mops, &res, node) {
        sh.stats.ok_writes.fetch_add(1, Ordering::Relaxed);
    }
}

async fn run_txn_write(sh: &Shared, proc: Process, keys: [Key; 2], node: SocketAddr) {
    let (v0, v1) = (sh.fresh(), sh.fresh());
    let mops = vec![
        Mop::Append {
            key: keys[0],
            value: v0,
        },
        Mop::Append {
            key: keys[1],
            value: v1,
        },
    ];
    sh.rec
        .lock()
        .expect("recorder")
        .invoke(proc, sh.now(), mops.clone());
    let upd = |k: Key, v: u64| {
        json!({"Update": {
            "TableName": TABLE,
            "Key": pk(k),
            "UpdateExpression": "SET items = list_append(if_not_exists(items, :empty), :v)",
            "ExpressionAttributeValues": {":empty": {"L": []}, ":v": one_elem(v)},
        }})
    };
    let body = json!({"TransactItems": [upd(keys[0], v0), upd(keys[1], v1)]}).to_string();
    let res = dynamo_call(node, "TransactWriteItems", &body, OP_TIMEOUT).await;
    let acked = conclude_write(sh, proc, mops, &res, node);
    if acked {
        sh.stats.ok_txn_writes.fetch_add(1, Ordering::Relaxed);
        sh.stats.ok_writes.fetch_add(1, Ordering::Relaxed);
    }
    // A definite connect failure never happened; anything else may have.
    if !matches!(res, Err(CallErr::Connect(_))) {
        sh.txns.lock().expect("txns").push(TxnPair {
            k0: keys[0],
            v0,
            k1: keys[1],
            v1,
            acked,
        });
    }
}

async fn run_get(sh: &Shared, proc: Process, key: Key, consistent: bool, node: SocketAddr) {
    let read = |observed| vec![Mop::Read { key, observed }];
    if consistent {
        sh.rec
            .lock()
            .expect("recorder")
            .invoke(proc, sh.now(), read(None));
    }
    let body =
        json!({"ConsistentRead": consistent, "TableName": TABLE, "Key": pk(key)}).to_string();
    let res = dynamo_call(node, "GetItem", &body, OP_TIMEOUT).await;
    let list = match &res {
        Ok((200, b)) => serde_json::from_str::<Value>(b)
            .ok()
            .map(|v| v.get("Item").map(decode_items).unwrap_or_default()),
        _ => None,
    };
    match (consistent, list) {
        (true, Some(l)) => {
            sh.rec
                .lock()
                .expect("recorder")
                .ok(proc, sh.now(), read(Some(l)));
            sh.stats.ok_reads.fetch_add(1, Ordering::Relaxed);
        }
        (true, None) => {
            let mut rec = sh.rec.lock().expect("recorder");
            if matches!(res, Err(CallErr::Connect(_))) {
                rec.fail(proc, sh.now(), read(None));
            } else {
                rec.info(proc, sh.now(), read(None));
                sh.stats.info_reads.fetch_add(1, Ordering::Relaxed);
            }
        }
        (false, Some(l)) => {
            sh.eventual.lock().expect("eventual").push(EventualRead {
                key,
                list: l,
                node,
                at_ns: sh.now(),
            });
            sh.stats.eventual_reads.fetch_add(1, Ordering::Relaxed);
        }
        (false, None) => {}
    }
}

async fn run_txn_get(sh: &Shared, proc: Process, keys: [Key; 2], node: SocketAddr) {
    let reads = |o0, o1| {
        vec![
            Mop::Read {
                key: keys[0],
                observed: o0,
            },
            Mop::Read {
                key: keys[1],
                observed: o1,
            },
        ]
    };
    sh.rec
        .lock()
        .expect("recorder")
        .invoke(proc, sh.now(), reads(None, None));
    let get = |k: Key| json!({"Get": {"TableName": TABLE, "Key": pk(k)}});
    let body = json!({"TransactItems": [get(keys[0]), get(keys[1])]}).to_string();
    let res = dynamo_call(node, "TransactGetItems", &body, OP_TIMEOUT).await;
    let decoded = match &res {
        Ok((200, b)) => serde_json::from_str::<Value>(b)
            .ok()
            .and_then(|v| v.get("Responses").and_then(Value::as_array).cloned())
            .filter(|r| r.len() == 2)
            .map(|r| {
                (
                    r[0].get("Item").map(decode_items).unwrap_or_default(),
                    r[1].get("Item").map(decode_items).unwrap_or_default(),
                )
            }),
        _ => None,
    };
    let mut rec = sh.rec.lock().expect("recorder");
    match decoded {
        Some((a, b)) => {
            rec.ok(proc, sh.now(), reads(Some(a), Some(b)));
            sh.stats.ok_txn_reads.fetch_add(1, Ordering::Relaxed);
            sh.stats.ok_reads.fetch_add(1, Ordering::Relaxed);
        }
        None if matches!(res, Err(CallErr::Connect(_))) => {
            rec.fail(proc, sh.now(), reads(None, None))
        }
        None => {
            rec.info(proc, sh.now(), reads(None, None));
            sh.stats.info_reads.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// One client: owns `key % CLIENTS == proc - 1`, runs until `stop`.
pub async fn client_loop(sh: &Shared, proc: Process, nodes: Vec<SocketAddr>) {
    let mut rng = Rng::new(sh.seed ^ proc.wrapping_mul(0xA24B_AED4_963E_E407));
    let base = sh.key_base;
    let owned: Vec<Key> = (0..KEYS)
        .filter(|k| k % CLIENTS == proc - 1)
        .map(|k| k + base)
        .collect();
    while !sh.stop.load(Ordering::Relaxed) {
        let node = nodes[rng.below(nodes.len() as u64) as usize];
        let roll = rng.below(100);
        if roll < 50 {
            run_write(
                sh,
                proc,
                owned[rng.below(owned.len() as u64) as usize],
                node,
            )
            .await;
        } else if roll < 60 && sh.txn_ops && owned.len() >= 2 {
            let a = rng.below(owned.len() as u64) as usize;
            let mut b = rng.below(owned.len() as u64 - 1) as usize;
            if b >= a {
                b += 1;
            }
            run_txn_write(sh, proc, [owned[a], owned[b]], node).await;
        } else if roll < 80 {
            run_get(sh, proc, base + rng.below(KEYS), true, node).await;
        } else if roll < 90 && sh.txn_ops {
            let a = rng.below(KEYS);
            let mut b = rng.below(KEYS - 1);
            if b >= a {
                b += 1;
            }
            run_txn_get(sh, proc, [base + a, base + b], node).await;
        } else {
            run_get(sh, proc, base + rng.below(KEYS), false, node).await;
        }
        tokio::time::sleep(Duration::from_millis(
            sh.pace_ms.0 + rng.below(sh.pace_ms.1),
        ))
        .await;
    }
}

// ---- bring-up --------------------------------------------------------------

/// `CreateTable` (idempotent) through any node, retrying until the cluster
/// has a control leader. `tablets` > 1 provisions throughput so the ADR 0067
/// min-tablet-count loop splits the table (cross-tablet 2PC under chaos).
pub async fn create_table(
    nodes: &[SocketAddr],
    tablets: u64,
    budget: Duration,
) -> Result<(), String> {
    let mut body = json!({
        "TableName": TABLE,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
    });
    if tablets > 1 {
        // ceil(RCU/3000 + WCU/1000) == tablets
        body["BillingMode"] = json!("PROVISIONED");
        body["ProvisionedThroughput"] = json!({
            "ReadCapacityUnits": 3000 * (tablets / 2),
            "WriteCapacityUnits": 1000 * (tablets - tablets / 2),
        });
    } else {
        body["BillingMode"] = json!("PAY_PER_REQUEST");
    }
    let body = body.to_string();
    let deadline = Instant::now() + budget;
    let mut last = String::new();
    while Instant::now() < deadline {
        for n in nodes {
            match dynamo_call(*n, "CreateTable", &body, Duration::from_secs(15)).await {
                Ok((200, _)) => return Ok(()),
                Ok((_, b)) if b.contains("ResourceInUse") => return Ok(()),
                Ok((s, b)) => last = format!("{s} {b}"),
                Err(e) => last = e.to_string(),
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(format!(
        "CreateTable did not succeed within {budget:?}: {last}"
    ))
}

/// A consistent read of `key` through `node`, retried until it answers.
pub async fn final_read(
    node: SocketAddr,
    key: Key,
    budget: Duration,
) -> Result<Vec<ListVal>, String> {
    let body = json!({"ConsistentRead": true, "TableName": TABLE, "Key": pk(key)}).to_string();
    let deadline = Instant::now() + budget;
    let mut last = String::new();
    while Instant::now() < deadline {
        match dynamo_call(node, "GetItem", &body, Duration::from_secs(10)).await {
            Ok((200, b)) => {
                let v: Value = serde_json::from_str(&b).map_err(|e| e.to_string())?;
                return Ok(v.get("Item").map(decode_items).unwrap_or_default());
            }
            Ok((s, b)) => last = format!("{s} {b}"),
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!(
        "final read of key {key} via {node} did not answer in {budget:?}: {last}"
    ))
}

/// A `ConsistentRead: false` read of `key` through `node` (which serves it
/// from its own replica when that replica passes the freshness gate). `None`
/// when the node did not answer cleanly.
pub async fn eventual_read(node: SocketAddr, key: Key) -> Option<Vec<ListVal>> {
    let body = json!({"ConsistentRead": false, "TableName": TABLE, "Key": pk(key)}).to_string();
    match dynamo_call(node, "GetItem", &body, Duration::from_secs(10)).await {
        Ok((200, b)) => serde_json::from_str::<Value>(&b)
            .ok()
            .map(|v| v.get("Item").map(decode_items).unwrap_or_default()),
        _ => None,
    }
}

/// Post-heal replica-convergence probe: after everything is healed and the
/// final state is known, every node's own eventual read of every key must
/// converge to that final state within `budget`. A node that stays different
/// is a **permanently diverged replica**, which this separates from a
/// transient read anomaly (a stale read that later catches up passes).
/// Returns one violation line per (node, key) that never converged, naming
/// both lists' differences.
pub async fn replica_convergence(
    nodes: &[SocketAddr],
    fin: &BTreeMap<Key, Vec<ListVal>>,
    budget: Duration,
) -> Vec<String> {
    let mut out = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        for (key, want) in fin {
            let deadline = Instant::now() + budget;
            let mut last: Option<Vec<ListVal>> = None;
            loop {
                last = eventual_read(*node, *key).await.or(last);
                if last.as_ref() == Some(want) || Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            if last.as_ref() != Some(want) {
                let got = last.unwrap_or_default();
                let lacks: Vec<u64> = want.iter().copied().filter(|v| !got.contains(v)).collect();
                let extra: Vec<u64> = got.iter().copied().filter(|v| !want.contains(v)).collect();
                out.push(format!(
                    "[replica-convergence] node n{i} ({node}) key {key}: eventual read did not \
                     converge to the final state within {budget:?}; lacks {lacks:?}, extra {extra:?}"
                ));
            }
        }
    }
    out
}

/// Post-heal availability probe: a write plus a consistent read-back through
/// `node` (its own probe key, outside the checked history). Returns the time
/// it took to first succeed.
pub async fn probe_available(
    node: SocketAddr,
    tag: u64,
    budget: Duration,
) -> Result<Duration, String> {
    let t0 = Instant::now();
    let key = json!({"pk": {"S": format!("probe{tag}")}, "sk": {"S": "s"}});
    let mut last = String::new();
    while t0.elapsed() < budget {
        let put = json!({"TableName": TABLE, "Key": key,
            "UpdateExpression": "SET v = :v", "ExpressionAttributeValues": {":v": {"N": "1"}}})
        .to_string();
        let get = json!({"ConsistentRead": true, "TableName": TABLE, "Key": key}).to_string();
        let w = dynamo_call(node, "UpdateItem", &put, Duration::from_secs(10)).await;
        if matches!(w, Ok((200, _))) {
            match dynamo_call(node, "GetItem", &get, Duration::from_secs(10)).await {
                Ok((200, b)) if b.contains("\"v\"") => return Ok(t0.elapsed()),
                Ok((st, b)) => last = format!("read-back: {st} {b}"),
                Err(e) => last = format!("read-back: {e}"),
            }
        } else {
            last = match w {
                Ok((st, b)) => format!("write: {st} {b}"),
                Err(e) => format!("write: {e}"),
            };
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!(
        "node {node} not available after heal within {budget:?}: {last}"
    ))
}

// ---- oracles ---------------------------------------------------------------

pub struct Verdict {
    pub violations: Vec<String>,
    pub reports: Vec<(&'static str, CheckReport)>,
}

/// Run the animus-test oracles over the recorded history plus the converged
/// final state, and the two harness-level checks (eventual-read prefixes,
/// transaction atomicity).
pub fn run_oracles(
    sh: &Shared,
    final_a: &BTreeMap<Key, Vec<ListVal>>,
    final_b: &BTreeMap<Key, Vec<ListVal>>,
) -> (History, Verdict) {
    let mut history = sh.rec.lock().expect("recorder").history().clone();
    let mut verdict = Verdict {
        violations: Vec::new(),
        reports: Vec::new(),
    };
    let seed = sh.seed;

    let durability = check_durability(&history, final_a);
    let convergence = check_convergence(seed, final_a, final_b);

    // Feed the converged state back in as ok reads so check_cycles also
    // proves every workload read is consistent with it.
    {
        let mut rec = Recorder::new(seed);
        for e in &history.entries {
            rec_push(&mut rec, e);
        }
        let t = sh.now();
        for (k, l) in final_a {
            rec.invoke(
                0,
                t,
                vec![Mop::Read {
                    key: *k,
                    observed: None,
                }],
            );
            rec.ok(
                0,
                t,
                vec![Mop::Read {
                    key: *k,
                    observed: Some(l.clone()),
                }],
            );
        }
        history = rec.into_history();
    }
    let cycles = check_cycles(&history);

    for (name, r) in [
        ("durability", durability),
        ("convergence", convergence),
        ("cycles", cycles),
    ] {
        for v in &r.violations {
            verdict.violations.push(format!("[{name}] {v}"));
        }
        verdict.reports.push((name, r));
    }

    // Eventual reads must be prefixes of the converged state. A violation
    // names the node asked and the read time, and classifies every value the
    // read lacks (or has out of place) by the op that wrote it, so one failing
    // run says whether the lost values are transaction halves, plain
    // `UpdateItem`s, acked or indeterminate, and when they were written.
    let writers = writer_index(&history);
    for r in sh.eventual.lock().expect("eventual").iter() {
        let fin = final_a.get(&r.key).cloned().unwrap_or_default();
        if !fin.starts_with(&r.list) {
            let missing: Vec<u64> = fin
                .iter()
                .copied()
                .take_while(|v| *v <= r.list.last().copied().unwrap_or(0))
                .filter(|v| !r.list.contains(v))
                .collect();
            let extra: Vec<u64> = r
                .list
                .iter()
                .copied()
                .filter(|v| !fin.contains(v))
                .collect();
            let describe = |v: &u64| match writers.get(v) {
                Some(w) => format!(
                    "{v}={}{}@{:.2}s..{}",
                    if w.txn { "txn" } else { "single" },
                    w.outcome,
                    w.invoked_ns as f64 / 1e9,
                    w.done_ns
                        .map_or("?".to_owned(), |d| format!("{:.2}s", d as f64 / 1e9)),
                ),
                None => format!("{v}=unknown-writer"),
            };
            verdict.violations.push(format!(
                "[eventual-prefix] key {k}: eventual read via {node} at t={t:.2}s is not a prefix \
                 of final; lacks {nm} value(s) [{miss}], has {ne} value(s) absent from final \
                 [{extra}]; read {l:?}; final {fin:?}",
                k = r.key,
                node = r.node,
                t = r.at_ns as f64 / 1e9,
                nm = missing.len(),
                miss = missing.iter().map(describe).collect::<Vec<_>>().join(", "),
                ne = extra.len(),
                extra = extra.iter().map(describe).collect::<Vec<_>>().join(", "),
                l = r.list,
            ));
        }
    }

    // TransactWriteItems atomicity: both appends present or neither.
    for t in sh.txns.lock().expect("txns").iter() {
        let p0 = final_a.get(&t.k0).is_some_and(|l| l.contains(&t.v0));
        let p1 = final_a.get(&t.k1).is_some_and(|l| l.contains(&t.v1));
        if p0 != p1 {
            verdict.violations.push(format!(
                "[txn-atomicity] transaction (key {} <- {}, key {} <- {}, acked={}) applied one half only: {p0} / {p1}",
                t.k0, t.v0, t.k1, t.v1, t.acked
            ));
        }
    }
    (history, verdict)
}

fn rec_push(rec: &mut Recorder, e: &animus_test::Entry) {
    use animus_test::Outcome;
    match e.outcome {
        Outcome::Invoke => rec.invoke(e.process, e.time, e.mops.clone()),
        Outcome::Ok => rec.ok(e.process, e.time, e.mops.clone()),
        Outcome::Fail => rec.fail(e.process, e.time, e.mops.clone()),
        Outcome::Info => rec.info(e.process, e.time, e.mops.clone()),
    }
}

/// Who wrote a value, from the recorded history.
struct Writer {
    txn: bool,
    /// `ok` / `info` / `fail`, as recorded at completion.
    outcome: &'static str,
    invoked_ns: u64,
    done_ns: Option<u64>,
}

/// value -> its writing op (a two-append entry is a `TransactWriteItems`).
/// Values are globally unique, so the map is exact.
fn writer_index(history: &History) -> BTreeMap<u64, Writer> {
    use animus_test::Outcome;
    let mut out: BTreeMap<u64, Writer> = BTreeMap::new();
    // One client runs one op at a time, so the open invoke per process is the
    // op the next terminal entry of that process completes.
    let mut open: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for e in &history.entries {
        let appends: Vec<u64> = e
            .mops
            .iter()
            .filter_map(|m| match m {
                Mop::Append { value, .. } => Some(*value),
                Mop::Read { .. } => None,
            })
            .collect();
        match e.outcome {
            Outcome::Invoke => {
                open.insert(e.process, appends.clone());
                for v in appends {
                    out.insert(
                        v,
                        Writer {
                            txn: e.mops.len() > 1,
                            outcome: "pending",
                            invoked_ns: e.time,
                            done_ns: None,
                        },
                    );
                }
            }
            Outcome::Ok | Outcome::Info | Outcome::Fail => {
                if let Some(vals) = open.remove(&e.process) {
                    let name = match e.outcome {
                        Outcome::Ok => "ok",
                        Outcome::Info => "info",
                        _ => "fail",
                    };
                    for v in vals {
                        if let Some(w) = out.get_mut(&v) {
                            w.outcome = name;
                            w.done_ns = Some(e.time);
                        }
                    }
                }
            }
        }
    }
    out
}
