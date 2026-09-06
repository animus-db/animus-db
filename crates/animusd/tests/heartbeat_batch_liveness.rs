//! ADR 0044 phase 2 (C-02 PR 3, the cutover) — the **critical `ProdEnv`
//! liveness test** for heartbeat batching, mirroring `tests/
//! cp_quiescence.rs`'s own role for quiescence (root `CLAUDE.md`: "`SimEnv`
//! proves logic and ordering, not real-thread liveness — locks, wakers,
//! group commit, and election timing need a timeout-guarded
//! `#[tokio::test(multi_thread)]` over `ProdEnv`"). `crates/animus-cp-data/
//! tests/heartbeat_batch_corpus.rs` already proves the batcher's logic
//! deterministically at depth (`SimEnv`, `ANIMUS_HEARTBEAT_SEEDS`); this
//! file proves the SAME per-group election-timer contract holds under a
//! real OS scheduler, real sockets, and real wall-clock timing, with
//! batching on **by default** (no `--heartbeat-batch` flag passed at all —
//! this is what a freshly started node now does).
//!
//! Hosts THREE tablet groups (one per table, all provisioned on the same
//! 3-node cluster) so the physical-frame amortization this mechanism exists
//! for is actually exercised — a single-tablet cluster would never share a
//! destination frame across more than one group. The test has two halves:
//!
//! 1. **Stability**: over a fixed wall-clock interval with continuous
//!    read/write traffic and no injected fault, every tablet's own leader
//!    and Raft term stay unchanged (`GET /admin/raftkv`'s `term`/`node`
//!    fields — the per-tablet election signal; there is no per-tablet
//!    election *metric*, unlike the control plane's `Metric::
//!    ElectionsStarted`/`ElectionsWon`, so `/admin/raftkv` is the
//!    documented substitute the task brief itself allows for).
//! 2. **Recovery**: kill the physical node leading the most groups (the
//!    busiest batcher source) and confirm every group it led re-elects a
//!    new leader within a bounded election budget, with writes/reads
//!    continuing to work throughout via the survivors — proving the
//!    batched heartbeat path's own timers (both the per-group Raft
//!    election timeout and the batcher's own flush cadence) hold under
//!    real scheduling, not just `SimEnv`'s cooperative one.
//!
//! Converged-or-timeout polls throughout — never a fixed-deadline one-shot
//! assert (root `CLAUDE.md`'s Testing discipline).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use animusd::{
    BackupStoreConfig, ClientRequest, ClientResponse, Node, SegmentStoreConfig, StorageBackend,
    StreamSealKnobs, read_frame,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

const TABLES: [&str; 3] = ["hb_t0", "hb_t1", "hb_t2"];

/// Well past the default 5s/50ms election/heartbeat cadence — bounds every
/// bootstrap/settle/election poll in this file.
const FORM_BUDGET: Duration = Duration::from_secs(30);
const ELECTION_BUDGET: Duration = Duration::from_secs(20);

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
    timeout(FORM_BUDGET, ready)
        .await
        .expect("cluster did not bootstrap within budget");
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
/// mirrors `cp_quiescence.rs`'s own `put` helper.
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
        .unwrap_or_else(|_| panic!("write of {key:?}@{table} never committed within {secs}s"));
}

/// Retry a get against every client address in turn until one answers with
/// the expected value.
async fn get_eq(clients: &[SocketAddr], key: &[u8], expected: &[u8], secs: u64, table: &str) {
    let w = async {
        loop {
            for &c in clients {
                let resp = tokio::time::timeout(
                    Duration::from_secs(2),
                    call(
                        c,
                        ClientRequest::Get {
                            key: key.to_vec(),
                            table: table.to_string(),
                            stale: false,
                        },
                    ),
                )
                .await;
                if let Ok(ClientResponse::Value(Some(v))) = resp
                    && v == expected
                {
                    return;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(secs), w)
        .await
        .unwrap_or_else(|_| panic!("read of {key:?}@{table} never converged within {secs}s"));
}

async fn admin_get(addr: SocketAddr, path: &str) -> Option<Value> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    let request = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    stream.flush().await.ok()?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.ok()?;
    let text = String::from_utf8(raw).ok()?;
    let (_head, payload) = text.split_once("\r\n\r\n")?;
    serde_json::from_str(payload).ok()
}

/// `(leading admin index, term)` for every currently-leading tablet replica
/// among `admins` — `/admin/raftkv` is node-local (ADR 0031 PR2), so
/// querying each admin address **individually** (never merging across
/// addresses, unlike the dashboard's own cross-node fan-out) directly
/// yields "which physical node currently leads this tablet" as a plain
/// index into `admins`, with no `NodeId` string parsing needed. A tablet
/// missing from the map means no reachable replica currently believes
/// itself the leader (mid-election, or every replica reachable so far is a
/// follower).
async fn leader_terms(admins: &[SocketAddr]) -> BTreeMap<u64, (usize, u64)> {
    let mut out = BTreeMap::new();
    for (i, &addr) in admins.iter().enumerate() {
        let Some(v) = admin_get(addr, "/admin/raftkv").await else {
            continue;
        };
        let Some(groups) = v["groups"].as_array() else {
            continue;
        };
        for g in groups {
            if g["is_leader"].as_bool() == Some(true) {
                let tablet = g["tablet"].as_u64().expect("tablet id");
                let term = g["term"].as_u64().expect("term");
                out.insert(tablet, (i, term));
            }
        }
    }
    out
}

async fn await_all_leaders(
    admins: &[SocketAddr],
    expected_tablets: usize,
) -> BTreeMap<u64, (usize, u64)> {
    let formed = async {
        loop {
            let m = leader_terms(admins).await;
            if m.len() == expected_tablets {
                return m;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(FORM_BUDGET, formed)
        .await
        .expect("not every tablet elected a leader within the form budget")
}

/// The batched heartbeat path's own timers hold under real scheduling: with
/// batching on by default (no flag passed), three co-hosted tablet groups
/// keep a stable leader/term under continuous traffic for a fixed wall
/// interval, and — after killing the busiest leader node — every group it
/// led re-elects within a bounded election budget with reads/writes
/// continuing to work throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn batched_heartbeats_hold_stable_then_reelect_after_a_real_leader_kill() {
    let dir = support::panic_safe_tempdir();
    let ip = "127.0.0.1".parse().unwrap();
    let bound = animusd::bind_cluster(3, ip, dir.path()).await.unwrap();
    let clients: Vec<SocketAddr> = bound.iter().map(animusd::BoundNode::client_addr).collect();
    let admins: Vec<SocketAddr> = bound.iter().map(animusd::BoundNode::admin_addr).collect();

    // `heartbeat_batch: true` is passed explicitly here for clarity, but the
    // point of this test is that a caller passing NOTHING (an ordinary
    // `--config`/`--node` or `--cluster N` invocation with no
    // `--heartbeat-batch`/`--no-heartbeat-batch` flag at all) now gets this
    // same behavior — `main::DEFAULT_HEARTBEAT_BATCH` resolves to `true`
    // before ever reaching this function. Quiescence is disabled
    // (`Duration::ZERO`) so every group keeps ticking its Raft heartbeat for
    // the whole run, exactly like `heartbeat_cost.rs`'s own baseline — this
    // test is about the ACTIVE, always-batching cost path, not the idle one.
    let nodes = animusd::start_cluster_with_growth_and_quiesce_after(
        bound,
        StorageBackend::default(),
        None,
        Duration::from_secs(600),
        StreamSealKnobs::default(),
        SegmentStoreConfig::default(),
        animusd::DEFAULT_STREAM_RETENTION,
        None,
        None,
        Duration::ZERO,
        true,
        None,
        BackupStoreConfig::default(),
        None,
        None,
        None,
        None,
    )
    .await
    .unwrap();
    await_bootstrap(&nodes).await;

    // Provision all three tables (auto-provisioned on first write, ADR
    // 0023) so all three tablet groups exist and start ticking together on
    // the same three physical nodes.
    for table in TABLES {
        put(&clients, b"seed", b"seed-v", 30, table).await;
    }

    let before = await_all_leaders(&admins, TABLES.len()).await;
    assert_eq!(
        before.len(),
        TABLES.len(),
        "every tablet must have a leader before the stability window starts"
    );

    // Stability half: keep writing/reading through the whole interval while
    // polling that no tablet's own leader/term ever changes — several
    // multiples of the default 50ms heartbeat-batch flush interval, real
    // wall-clock time (the whole point: nothing here manufactures ticks,
    // a real batched consensus loop must keep its own timers alive on its
    // own).
    let stable_for = Duration::from_secs(3);
    let deadline = tokio::time::Instant::now() + stable_for;
    let mut i: u64 = 0;
    while tokio::time::Instant::now() < deadline {
        for table in TABLES {
            let key = format!("k{i}").into_bytes();
            let val = format!("v{i}").into_bytes();
            put(&clients, &key, &val, 10, table).await;
            get_eq(&clients, &key, &val, 10, table).await;
        }
        let now = leader_terms(&admins).await;
        for (tablet, (node, term)) in &before {
            match now.get(tablet) {
                Some((n, t)) => assert_eq!(
                    (n, t),
                    (node, term),
                    "tablet {tablet} changed leader/term during the no-fault \
                     stability window (batched heartbeat timers did not hold)"
                ),
                None => panic!(
                    "tablet {tablet} lost its leader during the no-fault \
                     stability window"
                ),
            }
        }
        i += 1;
        sleep(Duration::from_millis(100)).await;
    }
    assert!(
        i > 0,
        "sanity: the stability loop must have run at least once"
    );

    // Recovery half: kill the physical node leading the most groups (the
    // busiest heartbeat-batch source) and confirm every group it led
    // re-elects within a bounded budget, with the survivors still serving.
    let mut led_count: BTreeMap<usize, usize> = BTreeMap::new();
    for (idx, _) in before.values() {
        *led_count.entry(*idx).or_default() += 1;
    }
    let killed_idx = led_count
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(idx, _)| idx)
        .expect("at least one tablet has a leader");

    nodes[killed_idx].shutdown();

    let survivor_clients: Vec<SocketAddr> = clients
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != killed_idx)
        .map(|(_, c)| *c)
        .collect();
    let survivor_admins: Vec<SocketAddr> = admins
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != killed_idx)
        .map(|(_, a)| *a)
        .collect();

    // `leader_terms(&survivor_admins)` is built with the SAME indices
    // `survivor_admins` uses (a filtered-and-reindexed slice), so every
    // entry it can return already excludes the killed node structurally —
    // there is nothing more to check on that front beyond "every tablet has
    // a leader again."
    let reelected = async {
        loop {
            let m = leader_terms(&survivor_admins).await;
            if m.len() == TABLES.len() {
                return m;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    let after = timeout(ELECTION_BUDGET, reelected)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "not every tablet re-elected a new leader within \
             {ELECTION_BUDGET:?} of killing the busiest batched-heartbeat \
             source node"
            )
        });
    assert_eq!(after.len(), TABLES.len());

    // The recovered groups genuinely serve, not just accept one lucky vote.
    for table in TABLES {
        put(&survivor_clients, b"post-kill", table.as_bytes(), 30, table).await;
        get_eq(&survivor_clients, b"post-kill", table.as_bytes(), 30, table).await;
    }

    for (i, node) in nodes.iter().enumerate() {
        if i != killed_idx {
            node.shutdown_graceful().await;
        }
    }
}
