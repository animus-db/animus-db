//! Hand-rolled cluster-wire benchmark: latency percentiles + throughput for
//! the DynamoDB JSON/HTTP edge over a real in-process 3-node cluster
//! (**`ProdEnv`**, real sockets/disk/clock — this is not `SimEnv`).
//!
//! Like [`animus-storage`'s `engine_bench`](../../animus-storage/benches/engine_bench.rs),
//! this is a plain binary (`harness = false`), not a criterion target: **zero
//! extra dependencies**, hand-rolled timing (`std::time::Instant`) and
//! percentiles, `serde_json` (already a dependency) for request/response
//! bodies and the optional machine-readable output. Cluster bring-up follows
//! `tests/inplace_split_bench.rs`/`tests/split_build.rs`'s bounded-retry
//! bring-up idiom (port-TOCTOU, `docs/engineering-lessons.md`) and drives the
//! wire with the canonical raw-HTTP/1.1-over-`TcpStream` `dynamo()` helper
//! every `animusd` DynamoDB integration test uses
//! (`tests/dynamo_batch_get.rs` etc.).
//!
//! ## Methodology (must track `website/performance.html`'s stated commitments)
//!
//! - **Tail latency, not averages**: every operation class reports p50/p99/
//!   p99.9/mean, computed by hand over sorted per-op latency samples.
//! - **Both `ConsistentRead` modes, separately**: `ConsistentRead: true` (the
//!   linearizable ReadIndex path) and `ConsistentRead: false` (the wire
//!   default — served from any replica's own applied state, may be stale by
//!   design, ADR 0055) are two distinct rows, never blended.
//! - **A failure/degraded phase in every run**: after the healthy-cluster
//!   classes, this kills the tablet's own leader node and re-measures
//!   `PutItem`/`GetItem(ConsistentRead:true)` *through* the resulting leader
//!   election, via a bounded-retry helper that counts (and reports) retries
//!   instead of treating a transient "not the leader here" as a bench
//!   failure.
//! - **Reproducible**: every workload knob is printed in the header, and
//!   `ANIMUS_BENCH_JSON=<path>` writes a JSON document (params + per-class
//!   stats + the concurrency sweep) alongside the human-readable table.
//! - **No DynamoDB comparison**, anywhere in this file's output.
//!
//! `docs/engineering-lessons.md`'s "a historical bench figure from a
//! different host is not a baseline" entry applies here too: numbers from
//! this bench are comparable only to another run on the **same host, same
//! session** — never across machines, and never against a managed service.
//!
//! ## Running
//!
//! ```sh
//! cargo bench -p animusd                                   # default sizes
//! ANIMUS_BENCH_ITEMS=200 ANIMUS_BENCH_OPS=100 \
//!   ANIMUS_BENCH_CLIENTS=1,4 cargo bench -p animusd         # a quick smoke run
//! ANIMUS_BENCH_JSON=/tmp/cluster_bench.json cargo bench -p animusd
//! ```
//!
//! This bench is manual/local — it is not run in CI (real sockets + real
//! disk + real elapsed wall clock make it unsuitable for a shared runner's
//! noise floor); see `crates/animusd/CLAUDE.md` for the same discipline
//! spelled out for maintainers.
//!
//! Env knobs (all with defaults sized to finish in a few minutes on a
//! laptop): `ANIMUS_BENCH_NODES` (3), `ANIMUS_BENCH_ITEMS` (2_000, the
//! preloaded dataset size), `ANIMUS_BENCH_OPS` (1_000, measured ops per
//! class), `ANIMUS_BENCH_VALUE_BYTES` (256), `ANIMUS_BENCH_CLIENTS`
//! ("1,8,32", the concurrent-`PutItem` sweep's client counts), and
//! `ANIMUS_BENCH_JSON` (unset — a file path to also write results as JSON).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use animusd::{Node, bind_cluster, start_cluster};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::sleep;

/// The bench's own table: a composite key (`pk` hash, `sk` numeric range) so
/// `Query` (several `sk`s per `pk`) is meaningful alongside `GetItem`/`Scan`.
const TABLE: &str = "cluster_bench_items";
const PK: &str = "pk";
const SK: &str = "sk";
/// Sort keys per partition in the preloaded dataset.
const SK_PER_PK: u64 = 10;

/// Workload parameters, read from the environment with defaults sized so a
/// default run finishes in a few minutes on a laptop.
struct Config {
    nodes: usize,
    items: u64,
    ops: u64,
    value_bytes: usize,
    clients: Vec<usize>,
    json_path: Option<PathBuf>,
}

impl Config {
    fn from_env() -> Self {
        let var_u64 = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        let var_usize = |name: &str, default: usize| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        let clients = std::env::var("ANIMUS_BENCH_CLIENTS")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter_map(|p| p.trim().parse().ok())
                    .collect::<Vec<usize>>()
            })
            .filter(|v: &Vec<usize>| !v.is_empty())
            .unwrap_or_else(|| vec![1, 8, 32]);
        Self {
            nodes: var_usize("ANIMUS_BENCH_NODES", 3),
            items: var_u64("ANIMUS_BENCH_ITEMS", 2_000),
            ops: var_u64("ANIMUS_BENCH_OPS", 1_000),
            value_bytes: var_usize("ANIMUS_BENCH_VALUE_BYTES", 256),
            clients,
            json_path: std::env::var("ANIMUS_BENCH_JSON").ok().map(PathBuf::from),
        }
    }
}

fn num_partitions(items: u64) -> u64 {
    (items / SK_PER_PK).max(1)
}

/// The `(pk, sk)` pair the preloaded dataset uses for logical row `i`.
fn pk_sk_for(i: u64) -> (String, u64) {
    let pk_idx = i / SK_PER_PK;
    let sk = i % SK_PER_PK;
    (format!("pk-{pk_idx:07}"), sk)
}

/// A small linear-congruential PRNG (no `rand` dependency), matching
/// `animus-storage/benches/engine_bench.rs`'s `Lcg` — pseudo-random read
/// order, deterministic for a fixed seed.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.0 >> 11
    }
}

/// A deterministic `n`-byte alphanumeric value string (no quotes/backslashes
/// to escape, so it can be spliced straight into a JSON literal).
fn value_string(seed: u64, n: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut s = String::with_capacity(n);
    let mut x = seed ^ 0x9e37_79b9_7f4a_7c15;
    for _ in 0..n {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
        let idx = ((x >> 33) as usize) % ALPHABET.len();
        s.push(ALPHABET[idx] as char);
    }
    s
}

fn item_json(pk: &str, sk: u64, value: &str) -> String {
    serde_json::json!({
        "TableName": TABLE,
        "Item": {
            "pk": {"S": pk},
            "sk": {"N": sk.to_string()},
            "v": {"S": value},
        }
    })
    .to_string()
}

/// Latency + retry stats over a set of per-op samples, computed by hand
/// (p50/p99/p99.9/mean/throughput — `website/performance.html`'s "tail
/// latency, not averages" commitment).
struct Stats {
    count: u64,
    total: Duration,
    mean: Duration,
    p50: Duration,
    p99: Duration,
    p999: Duration,
    retries: u64,
}

impl Stats {
    fn from_samples(mut samples: Vec<Duration>, retries: u64) -> Self {
        samples.sort_unstable();
        let count = samples.len() as u64;
        let total: Duration = samples.iter().sum();
        let mean = if count > 0 {
            total / count as u32
        } else {
            Duration::ZERO
        };
        let pct = |p: f64| {
            if samples.is_empty() {
                Duration::ZERO
            } else {
                let idx = ((p * samples.len() as f64) as usize).min(samples.len() - 1);
                samples[idx]
            }
        };
        Self {
            count,
            total,
            mean,
            p50: pct(0.50),
            p99: pct(0.99),
            p999: pct(0.999),
            retries,
        }
    }

    fn throughput_per_sec(&self) -> f64 {
        if self.total.is_zero() {
            0.0
        } else {
            self.count as f64 / self.total.as_secs_f64()
        }
    }

    fn to_json(&self) -> Value {
        serde_json::json!({
            "count": self.count,
            "retries": self.retries,
            "throughput_ops_per_sec": self.throughput_per_sec(),
            "mean_ms": self.mean.as_secs_f64() * 1000.0,
            "p50_ms": self.p50.as_secs_f64() * 1000.0,
            "p99_ms": self.p99.as_secs_f64() * 1000.0,
            "p999_ms": self.p999.as_secs_f64() * 1000.0,
        })
    }
}

fn report_row(label: &str, stats: &Stats) {
    println!(
        "  {label:<38} {:>9.0} ops/s   p50 {:>7.2}ms  p99 {:>7.2}ms  p99.9 {:>7.2}ms  mean {:>7.2}ms  n={:<5} retries={}",
        stats.throughput_per_sec(),
        stats.p50.as_secs_f64() * 1000.0,
        stats.p99.as_secs_f64() * 1000.0,
        stats.p999.as_secs_f64() * 1000.0,
        stats.mean.as_secs_f64() * 1000.0,
        stats.count,
        stats.retries,
    );
}

// ---------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------

/// One DynamoDB request over a fresh HTTP/1.1 connection → `(status, body)`
/// — the canonical helper every `animusd` DynamoDB integration test uses
/// (see `tests/dynamo_batch_get.rs`), duplicated here per this crate's own
/// documented convention of copying small fixtures into each bench/test
/// binary rather than sharing them (`crates/animusd/CLAUDE.md`).
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

/// `dynamo`, retried on a retryable `500` for up to `deadline` — the same
/// idiom `tests/dynamo_batch_get.rs::dynamo_retry` uses for a transient
/// "not the leader here"/leadership-churn refusal, extended to also return
/// the attempt count so callers can report retries absorbed rather than
/// silently discarding them (the degraded phase's whole point).
async fn dynamo_retry(
    addr: SocketAddr,
    target: &str,
    body: &str,
    deadline: Duration,
) -> (u16, String, u32) {
    let hard_deadline = tokio::time::Instant::now() + deadline;
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        let (status, resp) = dynamo(addr, target, body).await;
        if status != 500 || tokio::time::Instant::now() >= hard_deadline {
            return (status, resp, attempts);
        }
        sleep(Duration::from_millis(150)).await;
    }
}

/// One HTTP/1.0 `GET` to the admin endpoint; returns `(status, parsed JSON)`
/// — mirrors `tests/inplace_split_bench.rs::admin`.
async fn admin_get(addr: SocketAddr, path: &str) -> (u16, Value) {
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
    let json: Value = serde_json::from_str(payload.trim()).unwrap_or(Value::Null);
    (status, json)
}

/// A persistent DynamoDB wire connection — used only by the concurrent
/// `PutItem` sweep, where each client is meant to hold **one** TCP
/// connection across its whole share of ops (unlike `dynamo()`'s
/// connect-per-request `Connection: close` shape, which the sequential
/// latency classes use deliberately since it is the canonical per-test
/// idiom). HTTP/1.1 keep-alive by default (`crates/animusd/src/http.rs`),
/// so this reuses one socket for many requests.
struct Conn(BufReader<TcpStream>);

impl Conn {
    async fn connect(addr: SocketAddr) -> Self {
        Conn(BufReader::new(
            TcpStream::connect(addr)
                .await
                .expect("connect (keep-alive)"),
        ))
    }

    async fn request(&mut self, target: &str, body: &str) -> (u16, String) {
        let request = format!(
            "POST / HTTP/1.1\r\n\
             Host: animus\r\n\
             X-Amz-Target: {target}\r\n\
             Content-Type: application/x-amz-json-1.0\r\n\
             Content-Length: {}\r\n\
             Connection: keep-alive\r\n\
             \r\n\
             {body}",
            body.len(),
        );
        self.0
            .write_all(request.as_bytes())
            .await
            .expect("send request");
        self.0.flush().await.expect("flush");

        let mut status_line = String::new();
        self.0
            .read_line(&mut status_line)
            .await
            .expect("read status line");
        let status: u16 = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .expect("status code");

        let mut content_length = 0usize;
        loop {
            let mut line = String::new();
            self.0.read_line(&mut line).await.expect("read header line");
            if line == "\r\n" || line.is_empty() {
                break;
            }
            let lower = line.to_ascii_lowercase();
            if let Some(rest) = lower.strip_prefix("content-length:") {
                content_length = rest.trim().parse().unwrap_or(0);
            }
        }
        let mut body_buf = vec![0u8; content_length];
        self.0.read_exact(&mut body_buf).await.expect("read body");
        (status, String::from_utf8(body_buf).expect("utf8 body"))
    }
}

// ---------------------------------------------------------------------
// Cluster bring-up
// ---------------------------------------------------------------------

/// Bring up an `n`-node in-process cluster via [`bind_cluster`]/
/// [`start_cluster`], retrying the whole (bind + start) unit against the
/// port-TOCTOU race documented in `docs/engineering-lessons.md` and followed
/// by every bring-up helper in this crate's `tests/support/mod.rs` — the
/// port is free the instant `bind_cluster`'s ephemeral-port bind resolves
/// the address, so a rare loss of that race under `cargo bench`/`cargo
/// test`-level contention is retried as a unit rather than panicking.
async fn bring_up(n: usize, dir: &Path) -> Vec<Node> {
    let hard_deadline = Instant::now() + Duration::from_secs(60);
    let mut attempt: u32 = 0;
    loop {
        let attempt_dir = dir.join(format!("attempt-{attempt}"));
        let outcome = match bind_cluster(n, "127.0.0.1".parse().unwrap(), &attempt_dir).await {
            Ok(bound) => start_cluster(bound).await,
            Err(e) => Err(e),
        };
        match outcome {
            Ok(nodes) => return nodes,
            Err(e) => {
                eprintln!("cluster bring-up attempt {attempt} failed ({e}); retrying");
                assert!(
                    Instant::now() < hard_deadline,
                    "could not bring up a {n}-node cluster within the retry budget"
                );
                sleep(Duration::from_millis(200)).await;
                attempt += 1;
            }
        }
    }
}

async fn await_bootstrap(nodes: &[Node]) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let leader = nodes.iter().any(Node::is_control_leader);
        let everyone_has_tablet = nodes.iter().all(|n| !n.metadata().members.is_empty());
        if leader && everyone_has_tablet {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster did not elect a leader and bootstrap within 20s"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

async fn create_table(addr: SocketAddr) {
    let body = serde_json::json!({
        "TableName": TABLE,
        "AttributeDefinitions": [
            {"AttributeName": PK, "AttributeType": "S"},
            {"AttributeName": SK, "AttributeType": "N"},
        ],
        "KeySchema": [
            {"AttributeName": PK, "KeyType": "HASH"},
            {"AttributeName": SK, "KeyType": "RANGE"},
        ],
    })
    .to_string();
    let (status, resp) = dynamo(addr, "DynamoDB_20120810.CreateTable", &body).await;
    assert_eq!(status, 200, "CreateTable failed: {resp}");
}

/// Converged-or-timeout poll for `TABLE` reaching `ACTIVE` via
/// `DescribeTable` — never a fixed sleep (`docs/engineering-lessons.md`'s
/// "eventual properties get a converged-or-timeout poll" rule).
async fn await_table_active(addr: SocketAddr, deadline: Duration) {
    let hard_deadline = tokio::time::Instant::now() + deadline;
    loop {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.DescribeTable",
            &format!(r#"{{"TableName":"{TABLE}"}}"#),
        )
        .await;
        if status == 200 {
            let v: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
            if v["Table"]["TableStatus"].as_str() == Some("ACTIVE") {
                return;
            }
        }
        assert!(
            tokio::time::Instant::now() < hard_deadline,
            "table {TABLE} never reached ACTIVE: last response {body}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------
// Preload
// ---------------------------------------------------------------------

async fn preload(addr: SocketAddr, cfg: &Config) -> Duration {
    let t0 = Instant::now();
    for i in 0..cfg.items {
        let (pk, sk) = pk_sk_for(i);
        let value = value_string(i, cfg.value_bytes);
        let body = item_json(&pk, sk, &value);
        let (status, resp) = dynamo(addr, "DynamoDB_20120810.PutItem", &body).await;
        assert_eq!(status, 200, "preload PutItem {i} failed: {resp}");
    }
    t0.elapsed()
}

// ---------------------------------------------------------------------
// Measured operation classes
// ---------------------------------------------------------------------

async fn measure_put(
    addr: SocketAddr,
    ops: u64,
    value_bytes: usize,
    tag: &str,
    deadline: Duration,
) -> Stats {
    let mut samples = Vec::with_capacity(ops as usize);
    let mut retries = 0u64;
    for i in 0..ops {
        let pk = format!("extra-{tag}-{i:07}");
        let value = value_string(i ^ 0xdead_beef, value_bytes);
        let body = item_json(&pk, 0, &value);
        let t0 = Instant::now();
        let (status, resp, attempts) =
            dynamo_retry(addr, "DynamoDB_20120810.PutItem", &body, deadline).await;
        samples.push(t0.elapsed());
        assert_eq!(status, 200, "PutItem[{tag}] {i} failed: {resp}");
        retries += u64::from(attempts.saturating_sub(1));
    }
    Stats::from_samples(samples, retries)
}

async fn measure_get(
    addr: SocketAddr,
    items: u64,
    ops: u64,
    consistent: bool,
    deadline: Duration,
) -> Stats {
    let mut samples = Vec::with_capacity(ops as usize);
    let mut retries = 0u64;
    let mut rng = Lcg(0x1234_5678_9abc_def1 ^ u64::from(consistent));
    for _ in 0..ops {
        let idx = rng.next() % items.max(1);
        let (pk, sk) = pk_sk_for(idx);
        let body = serde_json::json!({
            "TableName": TABLE,
            "Key": {"pk": {"S": pk}, "sk": {"N": sk.to_string()}},
            "ConsistentRead": consistent,
        })
        .to_string();
        let t0 = Instant::now();
        let (status, resp, attempts) =
            dynamo_retry(addr, "DynamoDB_20120810.GetItem", &body, deadline).await;
        samples.push(t0.elapsed());
        assert_eq!(
            status, 200,
            "GetItem(ConsistentRead={consistent}) failed: {resp}"
        );
        retries += u64::from(attempts.saturating_sub(1));
    }
    Stats::from_samples(samples, retries)
}

/// One `Query` per op, each narrowed to a single (cycled) partition key —
/// `SK_PER_PK` items expected back per call.
async fn measure_query(addr: SocketAddr, items: u64, ops: u64, deadline: Duration) -> Stats {
    let num_pk = num_partitions(items);
    let mut samples = Vec::with_capacity(ops as usize);
    let mut retries = 0u64;
    for i in 0..ops {
        let pk_idx = i % num_pk;
        let pk = format!("pk-{pk_idx:07}");
        let body = serde_json::json!({
            "TableName": TABLE,
            "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": {"S": pk}},
        })
        .to_string();
        let t0 = Instant::now();
        let (status, resp, attempts) =
            dynamo_retry(addr, "DynamoDB_20120810.Query", &body, deadline).await;
        samples.push(t0.elapsed());
        assert_eq!(status, 200, "Query failed: {resp}");
        retries += u64::from(attempts.saturating_sub(1));
    }
    Stats::from_samples(samples, retries)
}

/// A full paged `Scan` (`Limit`/`ExclusiveStartKey`/`LastEvaluatedKey`, the
/// real DynamoDB pagination contract — see `tests/dynamo_parallel_scan.rs`).
/// Returns per-page latency stats, the scan's own total wall clock, and the
/// item count it walked.
async fn measure_scan(addr: SocketAddr, deadline: Duration) -> (Stats, Duration, u64) {
    const LIMIT: u64 = 200;
    let mut page_samples = Vec::new();
    let mut cursor: Option<Value> = None;
    let mut total_items = 0u64;
    let mut retries = 0u64;
    let wall0 = Instant::now();
    loop {
        let mut body = serde_json::json!({"TableName": TABLE, "Limit": LIMIT});
        if let Some(c) = &cursor {
            body["ExclusiveStartKey"] = c.clone();
        }
        let t0 = Instant::now();
        let (status, resp, attempts) =
            dynamo_retry(addr, "DynamoDB_20120810.Scan", &body.to_string(), deadline).await;
        page_samples.push(t0.elapsed());
        assert_eq!(status, 200, "Scan failed: {resp}");
        retries += u64::from(attempts.saturating_sub(1));
        let v: Value = serde_json::from_str(&resp).expect("scan response is JSON");
        total_items += v["Count"].as_u64().unwrap_or(0);
        cursor = match v.get("LastEvaluatedKey") {
            Some(k) if !k.is_null() => Some(k.clone()),
            _ => None,
        };
        if cursor.is_none() {
            break;
        }
    }
    (
        Stats::from_samples(page_samples, retries),
        wall0.elapsed(),
        total_items,
    )
}

/// Aggregate `PutItem` throughput at `level` concurrent clients, each its
/// own persistent TCP connection ([`Conn`]) writing a disjoint key range.
async fn concurrent_put_throughput(
    addr: SocketAddr,
    level: usize,
    total_ops: u64,
    value_bytes: usize,
) -> f64 {
    let per = (total_ops / level as u64).max(1);
    let t0 = Instant::now();
    let mut handles = Vec::with_capacity(level);
    for c in 0..level {
        handles.push(tokio::spawn(async move {
            let mut conn = Conn::connect(addr).await;
            for j in 0..per {
                let pk = format!("extra-sweep-{level}-{c}-{j:06}");
                let value = value_string((c as u64) * 1_000_003 + j, value_bytes);
                let body = item_json(&pk, 0, &value);
                let (status, resp) = conn.request("DynamoDB_20120810.PutItem", &body).await;
                assert_eq!(status, 200, "sweep PutItem failed: {resp}");
            }
        }));
    }
    for h in handles {
        h.await.expect("sweep client task");
    }
    let elapsed = t0.elapsed();
    let done = per * level as u64;
    if elapsed.is_zero() {
        0.0
    } else {
        done as f64 / elapsed.as_secs_f64()
    }
}

// ---------------------------------------------------------------------
// Degraded phase
// ---------------------------------------------------------------------

fn tablet_id_for_table(node: &Node, table: &str) -> u64 {
    node.metadata()
        .tablets
        .values()
        .find(|t| t.table.as_deref() == Some(table))
        .map(|t| t.id.0)
        .expect("bench table has a tablet")
}

/// One-shot sweep of every node's `/admin/raftkv` for the current leader of
/// `tablet`, mirroring `tests/inplace_split_bench.rs`/`split_build.rs`'s
/// `is_leader` view check.
async fn find_leader_index(nodes: &[Node], tablet: u64) -> Option<usize> {
    for (i, node) in nodes.iter().enumerate() {
        let (status, v) = admin_get(node.admin_addr(), "/admin/raftkv").await;
        if status != 200 {
            continue;
        }
        let is_leader = v["groups"].as_array().is_some_and(|groups| {
            groups.iter().any(|g| {
                g["tablet"].as_u64() == Some(tablet) && g["is_leader"].as_bool() == Some(true)
            })
        });
        if is_leader {
            return Some(i);
        }
    }
    None
}

/// Converged-or-timeout poll (never a fixed sleep) for the tablet's leader.
async fn await_leader_index(nodes: &[Node], tablet: u64, deadline: Duration) -> usize {
    let hard_deadline = tokio::time::Instant::now() + deadline;
    loop {
        if let Some(i) = find_leader_index(nodes, tablet).await {
            return i;
        }
        assert!(
            tokio::time::Instant::now() < hard_deadline,
            "no leader was ever observed for tablet {tablet}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------
// JSON output
// ---------------------------------------------------------------------

fn write_json(path: &Path, cfg: &Config, classes: &[(String, Stats)], sweep: &[(usize, f64)]) {
    let doc = serde_json::json!({
        "params": {
            "nodes": cfg.nodes,
            "items": cfg.items,
            "ops": cfg.ops,
            "value_bytes": cfg.value_bytes,
            "clients": cfg.clients,
        },
        "operation_classes": classes.iter().map(|(label, s)| {
            let mut v = s.to_json();
            v["label"] = serde_json::json!(label);
            v
        }).collect::<Vec<_>>(),
        "concurrent_put_sweep": sweep.iter().map(|(clients, ops_per_sec)| {
            serde_json::json!({"clients": clients, "ops_per_sec": ops_per_sec})
        }).collect::<Vec<_>>(),
        "note": "comparable only to another run on the same host/session; not a DynamoDB comparison",
    });
    std::fs::write(
        path,
        serde_json::to_string_pretty(&doc).expect("serialize results"),
    )
    .unwrap_or_else(|e| panic!("failed to write {}: {e}", path.display()));
}

// ---------------------------------------------------------------------
// main
// ---------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cfg = Config::from_env();
    println!(
        "animusd cluster wire benchmark (ProdEnv, DynamoDB JSON/HTTP edge): nodes={}, items={}, \
         ops={}, value_bytes={}, clients={:?}",
        cfg.nodes, cfg.items, cfg.ops, cfg.value_bytes, cfg.clients
    );
    println!(
        "  json_output={}",
        cfg.json_path
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "  NOTE: these numbers are comparable only to another run on the SAME host, in the same \
         session — a figure from a different machine or an earlier session is an anecdote, not a \
         baseline (docs/engineering-lessons.md). This bench draws no comparison to DynamoDB."
    );

    let dir = tempfile::TempDir::new().expect("tempdir");
    let nodes = bring_up(cfg.nodes, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = nodes[0].dynamo_addr();

    create_table(addr0).await;
    await_table_active(addr0, Duration::from_secs(30)).await;

    println!(
        "\ntable {TABLE} is ACTIVE; preloading {} items ({} partitions x {SK_PER_PK} sort keys)...",
        cfg.items,
        num_partitions(cfg.items)
    );
    let preload_elapsed = preload(addr0, &cfg).await;
    println!(
        "  preload done in {:.2}s ({:.0} rows/s)",
        preload_elapsed.as_secs_f64(),
        cfg.items as f64 / preload_elapsed.as_secs_f64().max(1e-9)
    );

    const HEALTHY_DEADLINE: Duration = Duration::from_secs(15);
    let mut classes: Vec<(String, Stats)> = Vec::new();

    println!("\nhealthy-cluster phase:");
    let put_stats = measure_put(addr0, cfg.ops, cfg.value_bytes, "put", HEALTHY_DEADLINE).await;
    report_row("PutItem", &put_stats);
    classes.push(("PutItem".to_string(), put_stats));

    let get_true = measure_get(addr0, cfg.items, cfg.ops, true, HEALTHY_DEADLINE).await;
    report_row("GetItem ConsistentRead=true", &get_true);
    classes.push(("GetItem ConsistentRead=true".to_string(), get_true));

    let get_false = measure_get(addr0, cfg.items, cfg.ops, false, HEALTHY_DEADLINE).await;
    report_row("GetItem ConsistentRead=false", &get_false);
    classes.push(("GetItem ConsistentRead=false".to_string(), get_false));

    let query_stats = measure_query(addr0, cfg.items, cfg.ops, HEALTHY_DEADLINE).await;
    report_row("Query (one partition)", &query_stats);
    classes.push(("Query (one partition)".to_string(), query_stats));

    let (scan_stats, scan_wall, scan_items) = measure_scan(addr0, HEALTHY_DEADLINE).await;
    report_row("Scan (per page)", &scan_stats);
    println!(
        "    scan total: {} page(s), {} item(s), wall clock {:.2}s",
        scan_stats.count,
        scan_items,
        scan_wall.as_secs_f64()
    );
    classes.push(("Scan (per page)".to_string(), scan_stats));

    println!("\nconcurrent PutItem throughput sweep:");
    let mut sweep = Vec::with_capacity(cfg.clients.len());
    for &level in &cfg.clients {
        let tput = concurrent_put_throughput(addr0, level, cfg.ops, cfg.value_bytes).await;
        println!("  clients={level:<4} {tput:>10.0} puts/s");
        sweep.push((level, tput));
    }

    let mut dead_node_idx: Option<usize> = None;
    if nodes.len() >= 3 {
        println!(
            "\ndegraded phase: killing the bench tablet's leader and re-measuring through the election..."
        );
        let tablet = tablet_id_for_table(&nodes[0], TABLE);
        let leader_idx = await_leader_index(&nodes, tablet, Duration::from_secs(20)).await;
        println!("  leader of tablet {tablet} is node {leader_idx}; shutting it down now");
        nodes[leader_idx].shutdown_graceful().await;
        dead_node_idx = Some(leader_idx);
        let survivor_idx = (0..nodes.len())
            .find(|&i| i != leader_idx)
            .expect("a survivor remains");
        let survivor_addr = nodes[survivor_idx].dynamo_addr();
        // A smaller op count here bounds the degraded phase's worst case
        // (an election that takes a few retry cycles per op) without
        // weakening the percentile shape.
        let degraded_ops = cfg.ops.clamp(1, 200);
        const DEGRADED_DEADLINE: Duration = Duration::from_secs(30);

        let degraded_put = measure_put(
            survivor_addr,
            degraded_ops,
            cfg.value_bytes,
            "degraded",
            DEGRADED_DEADLINE,
        )
        .await;
        report_row("PutItem [degraded: leader killed]", &degraded_put);
        classes.push((
            "PutItem [degraded: leader killed]".to_string(),
            degraded_put,
        ));

        let degraded_get = measure_get(
            survivor_addr,
            cfg.items,
            degraded_ops,
            true,
            DEGRADED_DEADLINE,
        )
        .await;
        report_row("GetItem ConsistentRead=true [degraded]", &degraded_get);
        classes.push((
            "GetItem ConsistentRead=true [degraded]".to_string(),
            degraded_get,
        ));
    } else {
        println!(
            "\ndegraded phase skipped: ANIMUS_BENCH_NODES={} < 3, no quorum margin to kill a node \
             against",
            nodes.len()
        );
    }

    println!("\nsummary:");
    for (label, stats) in &classes {
        report_row(label, stats);
    }

    if let Some(path) = &cfg.json_path {
        write_json(path, &cfg, &classes, &sweep);
        println!("\nwrote machine-readable results to {}", path.display());
    }

    for (i, node) in nodes.iter().enumerate() {
        if Some(i) != dead_node_idx {
            node.shutdown_graceful().await;
        }
    }
}
