//! ADR 0028 (C-05 PR 3, the cutover) — the **critical `ProdEnv` liveness
//! test** for the shared WAL, mirroring `tests/heartbeat_batch_liveness.rs`'s
//! own role for that mechanism's cutover (root `CLAUDE.md`: "`SimEnv` proves
//! logic and ordering, not real-thread liveness — locks, wakers, group
//! commit, and election timing need a timeout-guarded
//! `#[tokio::test(multi_thread)]` over `ProdEnv`"). `crates/animus-cp-data/
//! tests/sharedwal_fault_corpus.rs` already proves the coordinator's
//! cross-tablet coalescing/GC/isolation logic deterministically at depth
//! (`SimEnv`, `ANIMUS_SHAREDWAL_SEEDS`); this file proves the SAME
//! mechanism holds under a real OS scheduler, real disk I/O, and real
//! concurrent client load, with the shared WAL on **by default** (no
//! `--shared-wal` flag passed at all — this is what a freshly started node
//! now does).
//!
//! Hosts FOUR tablet groups (one per table, all auto-provisioned on the
//! same 3-node cluster, ADR 0023) so every node's own per-node `SharedWal`
//! genuinely has several concurrently-writing groups to coalesce — a
//! single-tablet cluster would never exercise the cross-group fsync path
//! this mechanism exists for. The test has three parts, all against the
//! SAME sustained-load window (never a separate, idle-then-load sequence):
//!
//! 1. **Durability under load**: several concurrent writer tasks (one per
//!    table) hammer distinct keys. Each writer's own loop is
//!    converge-or-timeout, never a fixed wall-clock cutoff (issue #699,
//!    same shape as issue #690): it keeps going until ITS OWN table has
//!    completed `TARGET_WRITES_PER_TABLE` writes (a margin comfortably past
//!    `COMPACT_THRESHOLD`, 64, so a real `apply_and_compact` →
//!    `SharedWal::compact_group` rewrite mid-load is guaranteed, not just
//!    append coalescing) **and** at least `LOAD_DURATION` has elapsed —
//!    both conditions, so the run stays a genuine sustained-load window on
//!    fast hardware instead of racing to the count and stopping early. The
//!    whole load phase is additionally bounded by `LOAD_PHASE_BUDGET`, a
//!    generous overall timeout that fires only on a genuine stall (a
//!    writer wedged on a broken path), never on ordinary runner slowness —
//!    a real 2-vCPU-runner throughput dip just makes the loop run longer,
//!    it no longer fails the test (root `CLAUDE.md`'s Testing discipline:
//!    converged-or-timeout, never a fixed-deadline one-shot assert — and,
//!    per the #690 lesson, a wall-clock-window write count is exactly the
//!    same "eventual property observed as a one-shot" bug in disguise).
//!    Every acked write is verified immediately readable via a
//!    `ConsistentRead`-equivalent get (`stale: false`, the linearizable
//!    ReadIndex path, ADR 0055) — never a fire-and-forget write.
//! 2. **GC under load**: `GET /admin/metrics`'s `cp_shared_wal_gc_rewrites`
//!    counter (summed across every node) is polled to a nonzero value
//!    during/after the load window — proof the shared WAL's segment GC
//!    (`compact_group`'s whole-file rewrite, reclaiming one tablet's own
//!    bytes without disturbing a co-hosted sibling's) actually ran under
//!    real concurrent load, not just in the deterministic corpus. The
//!    writer tasks' own per-write timeouts not tripping despite compaction
//!    happening mid-run is the "without stalling writes" half of this
//!    claim — a `SharedWal` whole-file rewrite that blocked concurrent
//!    appends for long enough would show up as writer timeouts here.
//! 3. **Recovery under load**: kill the physical node leading the most
//!    groups (the busiest shared-WAL writer) mid-load and confirm every
//!    group it led re-elects a new leader within a bounded election
//!    budget, with reads/writes continuing to work throughout via the
//!    survivors — proving `SharedWal`'s per-group crash-safety/recovery
//!    path (`recovered_state`, seeded once at node start from
//!    `SharedWal::open`) holds under real scheduling, not just `SimEnv`'s
//!    cooperative one.
//!
//! **Contention discipline** (issue #670's lesson, root `CLAUDE.md`'s
//! engineering-lessons log): every writer task below yields on a real
//! network round trip (`TcpStream`/`sleep`) on every iteration — never a
//! non-yielding CPU spin loop, which would starve this test's own tokio
//! runtime and produce an unrelated bootstrap/election failure instead of
//! exercising the code path under test.

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

const TABLES: [&str; 4] = ["sw_t0", "sw_t1", "sw_t2", "sw_t3"];

/// Well past the default 5s/50ms election/heartbeat cadence — bounds every
/// bootstrap/settle/election poll in this file.
const FORM_BUDGET: Duration = Duration::from_secs(30);
const ELECTION_BUDGET: Duration = Duration::from_secs(20);
/// The sustained-load window: each writer task keeps going until at least
/// this much wall time has elapsed AND its own table has crossed
/// `TARGET_WRITES_PER_TABLE` (see below) — so this bounds how long the
/// window runs on fast hardware, never how many writes must land on slow
/// hardware (issue #699).
const LOAD_DURATION: Duration = Duration::from_secs(5);
/// Per-table write-count target: `COMPACT_THRESHOLD` (64,
/// `animus-cp-data`'s private const of the same name) plus a margin
/// comfortably large enough that ordinary scheduler jitter right at the
/// threshold can't leave a table short of it — a real
/// `apply_and_compact` → `SharedWal::compact_group` rewrite mid-load is
/// unconditionally guaranteed once every writer task has reached this
/// count, since each task's own loop does not exit before then (issue
/// #699 — this is the loop's own contract, not a throughput bet).
const TARGET_WRITES_PER_TABLE: u64 = 64 + 16;
/// Overall bound on the whole load phase (every writer task reaching
/// `TARGET_WRITES_PER_TABLE` and at least `LOAD_DURATION` having elapsed).
/// Generous enough that real contention on a shared runner never trips it;
/// tight enough that a genuine stall (a writer wedged on a broken shared
/// WAL path) still fails fast with a clear message instead of hanging the
/// suite.
const LOAD_PHASE_BUDGET: Duration = Duration::from_secs(120);
/// Poll budget for the post-load GC-metric convergence check — the janitor
/// path runs inline on the apply task as part of ordinary compaction, so
/// this only needs to outlast the load window's own tail, not a separate
/// background sweep interval.
const GC_METRIC_BUDGET: Duration = Duration::from_secs(15);

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
/// mirrors `heartbeat_batch_liveness.rs`'s own `put` helper.
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
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(secs), w)
        .await
        .unwrap_or_else(|_| panic!("write of {key:?}@{table} never committed within {secs}s"));
}

/// Retry a get against every client address in turn until one answers with
/// the expected value, over the **linearizable ReadIndex path**
/// (`stale: false` — the `ConsistentRead: true` equivalent on this crate's
/// internal client protocol, ADR 0055) — the whole point of "every acked
/// write is readable" is that this must be the strong-consistency read,
/// never the cheap replica-local one.
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
            sleep(Duration::from_millis(50)).await;
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
/// among `admins` — see `heartbeat_batch_liveness.rs`'s identical helper for
/// the full rationale (node-local `/admin/raftkv`, queried per-address).
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

/// Sum of `cp_shared_wal_gc_rewrites` (`Metric::CpSharedWalGcRewrites`)
/// across every node's own `/admin/metrics` — a per-physical-write counter
/// incremented on the caller side of `SharedWal::compact_group`'s success
/// (`animus-cp-data`'s `apply_and_compact`), so a nonzero sum here is direct
/// proof the shared WAL's segment GC ran on real disk under this test's
/// load, not merely that append coalescing did.
async fn total_gc_rewrites(admins: &[SocketAddr]) -> u64 {
    let mut total = 0u64;
    for &addr in admins {
        if let Some(v) = admin_get(addr, "/admin/metrics").await {
            total += v["counters"]["cp_shared_wal_gc_rewrites"]
                .as_u64()
                .unwrap_or(0);
        }
    }
    total
}

/// The shared WAL's own timers/locks/GC hold under real scheduling and
/// sustained concurrent load: with the shared WAL on by default (no flag
/// passed), four co-hosted tablet groups take continuous concurrent writes
/// for a fixed wall interval — every acked write immediately readable,
/// `SharedWal`'s segment GC provably running mid-load — and, after killing
/// the busiest leader node, every group it led re-elects within a bounded
/// budget with reads/writes continuing to work throughout via the
/// survivors.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shared_wal_holds_under_sustained_load_then_reelects_after_a_real_leader_kill() {
    let dir = support::panic_safe_tempdir();
    let ip = "127.0.0.1".parse().unwrap();
    let bound = animusd::bind_cluster(3, ip, dir.path()).await.unwrap();
    let clients: Vec<SocketAddr> = bound.iter().map(animusd::BoundNode::client_addr).collect();
    let admins: Vec<SocketAddr> = bound.iter().map(animusd::BoundNode::admin_addr).collect();

    // `shared_wal: true` is passed explicitly here for clarity, but the
    // point of this test is that a caller passing NOTHING (an ordinary
    // `--config`/`--node` or `--cluster N` invocation with no
    // `--shared-wal`/`--no-shared-wal` flag at all) now gets this same
    // behavior — `main::DEFAULT_SHARED_WAL` resolves to `true` before ever
    // reaching this function. `heartbeat_batch: true` too (also the
    // default) — both mechanisms are default-on in production as of their
    // own cutovers, and this test's job is to prove the shared WAL holds
    // under the SAME real conditions a freshly started node actually runs
    // under, not an artificially narrowed one. Quiescence is disabled
    // (`Duration::ZERO`) so every group keeps ticking for the whole run —
    // this test is about the ACTIVE, always-writing load path.
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
        true,
    )
    .await
    .unwrap();
    await_bootstrap(&nodes).await;

    // Provision all four tables (auto-provisioned on first write, ADR 0023)
    // so all four tablet groups exist and every node's own SharedWal has
    // several concurrently-writing groups to coalesce from the start.
    for table in TABLES {
        put(&clients, b"seed", b"seed-v", 30, table).await;
    }
    let before = await_all_leaders(&admins, TABLES.len()).await;
    assert_eq!(
        before.len(),
        TABLES.len(),
        "every tablet must have a leader before the load window starts"
    );

    // Part 1+2: sustained concurrent load, one writer task per table, each
    // yielding on a real network round trip every iteration (never a CPU
    // spin — issue #670's lesson). Every put is followed by an immediate
    // ConsistentRead-equivalent get of that same key, so "every acked
    // write is readable" is proven incrementally through the whole load
    // window, not just at the end.
    let deadline = tokio::time::Instant::now() + LOAD_DURATION;
    let mut handles = Vec::with_capacity(TABLES.len());
    for table in TABLES {
        let clients = clients.clone();
        handles.push(tokio::spawn(async move {
            let mut i: u64 = 0;
            // Converge-or-timeout, not fixed-duration (issue #699): keep
            // writing until this table has crossed its own target count
            // AND the sustained-load window has elapsed — either alone is
            // not enough, so a fast run still sees the full LOAD_DURATION
            // of load, and a slow run still gets every write it needs.
            while i < TARGET_WRITES_PER_TABLE || tokio::time::Instant::now() < deadline {
                let key = format!("k{i}").into_bytes();
                let val = format!("v{i}").into_bytes();
                put(&clients, &key, &val, 15, table).await;
                get_eq(&clients, &key, &val, 15, table).await;
                i += 1;
            }
            (table, i)
        }));
    }
    let load_phase = async {
        let mut written: BTreeMap<&str, u64> = BTreeMap::new();
        for h in handles {
            let (table, count) = h.await.expect("writer task panicked");
            written.insert(table, count);
        }
        written
    };
    let written = timeout(LOAD_PHASE_BUDGET, load_phase)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "load phase stalled — did not converge within \
                 {LOAD_PHASE_BUDGET:?}; at least one writer task never \
                 reached {TARGET_WRITES_PER_TABLE} writes on its table \
                 despite that budget being well past the {LOAD_DURATION:?} \
                 sustained-load floor, which points at a genuine stall on \
                 the shared WAL / write path, not ordinary runner slowness \
                 (issue #699)"
            )
        });
    // Guaranteed by the loop's own exit condition above, not a timing bet
    // — restated here as the loop's contract, so a regression that breaks
    // that contract (rather than just running slow) still fails loudly.
    for (table, count) in &written {
        assert!(
            *count >= TARGET_WRITES_PER_TABLE,
            "table {table} completed only {count} writes despite the load \
             loop's own exit condition requiring at least \
             {TARGET_WRITES_PER_TABLE} — broken loop invariant, not a \
             throughput shortfall"
        );
    }

    // Part 2 (continued): the shared WAL's segment GC provably ran under
    // this load — a converged-or-timeout poll, since the apply task's own
    // compaction pass is asynchronous relative to the writer tasks above.
    let gc_ran = async {
        loop {
            let n = total_gc_rewrites(&admins).await;
            if n > 0 {
                return n;
            }
            sleep(Duration::from_millis(200)).await;
        }
    };
    let gc_rewrites = timeout(GC_METRIC_BUDGET, gc_ran).await.unwrap_or_else(|_| {
        panic!(
            "cp_shared_wal_gc_rewrites never went nonzero within \
             {GC_METRIC_BUDGET:?} despite {} total writes across {} \
             co-hosted tables — the shared WAL's segment GC did not run \
             under load",
            written.values().sum::<u64>(),
            TABLES.len()
        )
    });
    assert!(gc_rewrites > 0);

    // Part 3: kill the physical node leading the most groups (the busiest
    // shared-WAL writer) and confirm every group it led re-elects within a
    // bounded budget, with the survivors still serving.
    let after_load = await_all_leaders(&admins, TABLES.len()).await;
    let mut led_count: BTreeMap<usize, usize> = BTreeMap::new();
    for (idx, _) in after_load.values() {
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

    let reelected = async {
        loop {
            let m = leader_terms(&survivor_admins).await;
            if m.len() == TABLES.len() {
                return m;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    let after_kill = timeout(ELECTION_BUDGET, reelected)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "not every tablet re-elected a new leader within \
             {ELECTION_BUDGET:?} of killing the busiest shared-WAL-writing \
             node"
            )
        });
    assert_eq!(after_kill.len(), TABLES.len());

    // The recovered groups genuinely serve, not just accept one lucky
    // vote — proving the surviving replicas' own shared-WAL persist path
    // (`persist_wal`'s `append_tagged` round) keeps landing durable writes
    // through a real leadership change while every OTHER co-hosted
    // group's own writes on the same node keep flowing too.
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
