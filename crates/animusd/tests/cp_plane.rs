//! The **leaderful CP data plane** runs in the assembled node over `ProdEnv`
//! (ADR 0017 #3a / v1 ADR 0019: CP-only). Every client read/write is routed to a
//! per-tablet Raft group (`animus-cp-data`) hosted on the nodes' `raftkv` role —
//! the single, linearizable source of truth.
//!
//! This is the production assembly of the CP plane whose mechanism is sim-proven
//! in `animus-cp-data` (single-tablet linearizable KV, ReadIndex reads). Here we
//! drive it over real TCP/time through the same client API the CLI uses.
//!
//! Real TCP/time, so it polls with generous timeouts rather than asserting
//! deterministic timing. Cross-process CP routing (forwarding to the leader's
//! node) is covered by `cp_cross_process.rs`.
//!
//! **ADR 0061 rung O, issue #997**: this file used to hold five tests.
//! `reads_and_writes_route_through_the_raft_group` was deleted outright as
//! redundant — `tests/cluster.rs::cluster_serves_put_get_and_status_over_tcp`
//! drives the identical `bind_cluster`/`start_cluster` production entry point
//! and proves a strictly *stronger* version of the same put/get/absent-key
//! property (plus a cross-node overwrite proving quorum-derived versioning,
//! and a `Status`/`control_voters` check neither test here ever had).
//! `cp_member_addresses_register_and_replicate` and
//! `cp_tablet_splits_and_both_halves_serve` converted to deterministic
//! `SimCluster` siblings, `crates/animusd/src/sim_cluster_cp_plane.rs` — see
//! that module's own doc for the exact per-property mapping.
//! `tablet_auto_splits_on_bytes_with_skewed_value_sizes` (below) ALSO now has
//! a deterministic `SimCluster` sibling there (the rung's own bounded spike
//! succeeded — `SimCluster::put_raw`'s literal keys make the byte-weighted-
//! median-to-token-range correlation reproducible after all) but **stays
//! here too, unconverted, permanently** — the sim sibling proves the same
//! quantitative claim, not the real per-tablet Raft group/real-thread commit
//! path this test's own `ProdEnv` assembly exercises it through.
//! `single_write_latency_is_low` (below) remains — a permanent `ProdEnv`
//! regression, real-thread wall-clock latency `SimEnv` cannot reproduce.

use std::time::Duration;

use animusd::{
    ClientRequest, ClientResponse, Node, StorageBackend, bind_cluster, read_frame, start_cluster,
    start_cluster_with_auto_split_bytes,
};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

const CP_TABLE: &str = "cp_t";

async fn call(addr: std::net::SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect to node");
    animusd::write_frame(&mut stream, &req)
        .await
        .expect("send request");
    read_frame(&mut stream)
        .await
        .expect("read reply")
        .expect("a reply")
}

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
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not bootstrap within 20s");
}

/// **Single-write latency (deferred fix #2).** A lone CP write used to eat two
/// ~50ms floors: the cp-data driver waited for the next heartbeat tick before
/// replicating a freshly proposed entry, and `cp_put_local` confirmed with a fixed
/// 50ms poll. With **wake-on-propose** (the proposer nudges the consensus loop to
/// replicate immediately) + a **fine adaptive confirm poll**, a warmed lone write
/// round-trips in a few ms. Real TCP/time, so we assert a **median well under the
/// old ~100ms floor** (a generous bound that still fails loudly if either floor
/// regresses) and that the loop neither deadlocks nor busy-spins (the whole thing
/// completes far inside the timeout). The `multi_thread` `ProdEnv` run is the
/// liveness check the deterministic sim cannot give (root CLAUDE.md rule).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn single_write_latency_is_low() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr0 = nodes[0].client_addr();

    // Warm up: provision the tablet + elect the CP leader with a first write
    // (retried while the group settles). Latency of this cold path is not measured.
    let warm = async {
        loop {
            match call(
                addr0,
                ClientRequest::Put {
                    key: b"warm".to_vec(),
                    value: b"warm".to_vec(),
                    table: CP_TABLE.into(),
                },
            )
            .await
            {
                ClientResponse::PutOk => return,
                ClientResponse::Error(_) => sleep(Duration::from_millis(100)).await,
                other => panic!("unexpected warm-up put: {other:?}"),
            }
        }
    };
    timeout(Duration::from_secs(20), warm)
        .await
        .expect("warm-up write did not succeed within 20s");

    // Measure a batch of lone, sequential writes (each returns only once durably
    // applied on the leader — durable-before-ack).
    const N: usize = 50;
    let mut samples: Vec<Duration> = Vec::with_capacity(N);
    let measure = async {
        for i in 0..N {
            let key = format!("lat{i:03}").into_bytes();
            let start = tokio::time::Instant::now();
            let resp = call(
                addr0,
                ClientRequest::Put {
                    key,
                    value: b"v".to_vec(),
                    table: CP_TABLE.into(),
                },
            )
            .await;
            let elapsed = start.elapsed();
            assert!(
                matches!(resp, ClientResponse::PutOk),
                "write {i} failed: {resp:?}"
            );
            samples.push(elapsed);
        }
    };
    // If the consensus loop deadlocked (never woke to replicate) or busy-spun into
    // starvation, the batch would blow this budget; a healthy warm write is a few ms.
    timeout(Duration::from_secs(15), measure)
        .await
        .expect("write batch did not complete — driver deadlock/starvation?");

    samples.sort();
    let median = samples[N / 2];
    let max = *samples.last().unwrap();
    let min = *samples.first().unwrap();
    println!(
        "single-write latency over {N} warm writes: min={min:?} median={median:?} max={max:?}"
    );

    // The old floor was up to ~100ms (heartbeat-tick wait + fixed 50ms confirm
    // poll). Wake-on-propose + the fine confirm poll put the median far below it.
    // Threshold is generous for CI jitter while still catching a regression of
    // either floor (each of which alone would push the median to ~50ms+). A raw
    // wall-clock median under `cargo test --workspace`-level contention can flake
    // even with no code regression — if this fails, re-run the test in isolation
    // before treating it as real (docs/engineering-lessons.md, the "newly-added
    // heavy test... isolate-and-reconfirm" entry).
    assert!(
        median < Duration::from_millis(75),
        "median single-write latency {median:?} is not below the 75ms bound \
         (old ~100ms floor); wake-on-propose / fine confirm poll may have regressed"
    );

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

/// ADR 0061 rung D4 PR 2: `tablet_auto_splits_when_it_grows` (uniform
/// over-threshold keys, no manual trigger, both halves serve) was removed
/// from here — `sim_cluster_auto_split.rs`'s scenario (a),
/// `a_byte_threshold_crossing_forks_exactly_once`, proves the identical
/// property deterministically, through the real `auto_split_loop` (now
/// `<E: Env, R: RelayClient>`-generic) driven under `SimEnv`. This file
/// keeps `tablet_auto_splits_on_bytes_with_skewed_value_sizes` just below
/// — its specific byte-weighted-median quantitative-balance claim (a loose
/// 15% floor derived from correlating written keys against real token
/// ranges) wasn't reproduced by that file's own DynamoDB-wire scenarios,
/// which don't correlate a DynamoDB item's `pk` to its token range.
///
/// **ADR 0061 rung O, issue #997 — a bounded spike closed this gap after
/// all**: `sim_cluster_cp_plane.rs`'s own scenario (3),
/// `tablet_auto_splits_on_bytes_with_skewed_value_sizes`, reproduces the
/// identical byte-weighted-median claim deterministically via the
/// **raw-KV** path (`SimCluster::put_raw`'s literal, un-encoded keys —
/// this file's own `cp_tablet_splits_and_both_halves_serve` conversion's
/// own primitive) instead of the DynamoDB-wire path — a literal key can be
/// correlated against a child tablet's own `KeyRange` directly, no token
/// hashing in the way. This test stays here anyway, unconverted and
/// permanent: the sim sibling proves the same quantitative claim, not the
/// real per-tablet Raft group / real-thread commit path this test's own
/// `ProdEnv` assembly exercises it through. See `crates/animusd/CLAUDE.md`'s
/// matching entry.
///
/// ADR 0034 — **byte-based** auto-split trigger with skewed value sizes. A
/// tablet with only a handful of keys auto-splits purely on the **byte**
/// threshold (the only trigger this crate has since the key-count trigger's
/// removal), and — the point of the byte-weighted median — the resulting
/// halves are roughly **byte**-balanced, not just key-count-balanced (a
/// plain positional median here would put nearly all the bytes on one side:
/// 6 tiny keys sort before 6 large ones, so the positional median falls
/// right at the first large key).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn tablet_auto_splits_on_bytes_with_skewed_value_sizes() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    // 6 tiny rows (~10 bytes each) + 6 large rows (~2000 bytes each)
    // comfortably exceed this *combined*, while each post-split half
    // (~6,000 bytes, see the byte-weighted-median math in the comment
    // below) stays under it — so the tablet splits exactly once, not
    // repeatedly.
    const BYTES_THRESHOLD: u64 = 8_000;
    let nodes = start_cluster_with_auto_split_bytes(
        bound,
        StorageBackend::default(),
        Some(BYTES_THRESHOLD),
    )
    .await
    .unwrap();
    await_bootstrap(&nodes).await;
    let addr0 = nodes[0].client_addr();

    // Tiny keys sort before large ones ("a" < "b"), so a plain positional
    // median would land right at the first large key — putting ~99% of the
    // bytes on one side.
    let mut written: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for i in 0..6u32 {
        written.push((format!("a{i:02}").into_bytes(), b"tiny".to_vec()));
    }
    for i in 0..6u32 {
        written.push((format!("b{i:02}").into_bytes(), vec![b'x'; 2000]));
    }
    let total_bytes: u64 = written
        .iter()
        .map(|(k, v)| (k.len() + v.len()) as u64)
        .sum();

    for (key, value) in &written {
        let put = async {
            loop {
                match call(
                    addr0,
                    ClientRequest::Put {
                        key: key.clone(),
                        value: value.clone(),
                        table: "kv".to_string(),
                    },
                )
                .await
                {
                    ClientResponse::PutOk => return,
                    ClientResponse::Error(_) => sleep(Duration::from_millis(100)).await,
                    other => panic!("unexpected put: {other:?}"),
                }
            }
        };
        timeout(Duration::from_secs(20), put)
            .await
            .unwrap_or_else(|_| panic!("write {key:?} timed out"));
    }

    // The auto-split loop (no manual trigger) splits the over-byte-threshold
    // tablet, driven purely by the byte estimate/confirm — the key count (12)
    // never approaches a meaningful key-count threshold.
    let auto_split = async {
        loop {
            if nodes.iter().all(|n| n.metadata().tablets.len() >= 2) {
                return;
            }
            sleep(Duration::from_millis(200)).await;
        }
    };
    timeout(Duration::from_secs(30), auto_split)
        .await
        .expect("tablet did not byte-auto-split within 30s");

    // Both halves serve: a tiny-side key and a large-side key both read back.
    for (k, want) in [
        (b"a00".to_vec(), b"tiny".to_vec()),
        (b"b05".to_vec(), vec![b'x'; 2000]),
    ] {
        let read = async {
            loop {
                let got = call(
                    nodes[2].client_addr(),
                    ClientRequest::Get {
                        key: k.clone(),
                        table: "kv".to_string(),
                        stale: false,
                    },
                )
                .await;
                if got == ClientResponse::Value(Some(want.clone())) {
                    return;
                }
                sleep(Duration::from_millis(150)).await;
            }
        };
        timeout(Duration::from_secs(30), read)
            .await
            .unwrap_or_else(|_| panic!("key {k:?} not served after byte auto-split"));
    }

    // The point of the byte-weighted median: correlate each tablet's own
    // (post-split) key range against the pairs we actually wrote, and check
    // the two halves are roughly byte-balanced — loose bounds, since this is
    // an estimate-driven split, but tight enough to distinguish it from a
    // plain positional median (which would put ~99% of the bytes on one
    // side here).
    // Wait for the workflow to CUT OVER (>= 2 routable tablets), not just
    // begin — a mid-split snapshot has one routable tablet (the `Splitting`
    // parent), which would make the balance assertion below vacuous.
    let tablets = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let tablets = nodes[0].metadata().tablets;
            if tablets.values().filter(|t| t.is_routable()).count() >= 2 {
                break tablets;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "cutover never produced 2 routable tablets: {tablets:?}"
            );
            sleep(Duration::from_millis(100)).await;
        }
    };
    // Only ROUTABLE tablets partition the keyspace (ADR 0050): a snapshot
    // taken mid-workflow legitimately holds a `Splitting` parent AND its
    // two `Building` children — overlapping by design until cutover — so
    // the no-gap/no-overlap contract is stated over `is_routable()` ones.
    let mut per_tablet_bytes: Vec<u64> = tablets
        .values()
        .filter(|t| t.is_routable())
        .map(|t| {
            written
                .iter()
                .filter(|(k, _)| t.range.contains(k))
                .map(|(k, v)| (k.len() + v.len()) as u64)
                .sum()
        })
        .collect();
    per_tablet_bytes.sort_unstable();
    let covered: u64 = per_tablet_bytes.iter().sum();
    assert_eq!(
        covered, total_bytes,
        "every written key must fall in exactly one ROUTABLE tablet's range (no gap/overlap)"
    );
    // The two tablets carrying the most data (in the common 2-tablet case,
    // both of them) should each hold a non-trivial share of the total bytes —
    // a loose 15% floor that a plain positional median (which would give the
    // smaller side well under 1% here) could not meet.
    let smallest_share = *per_tablet_bytes.first().unwrap();
    assert!(
        smallest_share as f64 >= 0.15 * total_bytes as f64,
        "byte-weighted split should be roughly balanced: smallest tablet has \
         {smallest_share} of {total_bytes} total bytes (per-tablet: {per_tablet_bytes:?})"
    );

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

// ADR 0061 rung D4 PR 2: `already_split_tablet_splits_again_once_it_regrows`
// (an already-split lineage regrows past the byte threshold and splits
// again, every key across both rounds still reachable) was removed from
// here — `sim_cluster_auto_split.rs`'s scenario (c),
// `c_a_regrown_child_forks_again`, proves the identical property
// deterministically under `SimEnv`. See this file's own doc comment just
// above `tablet_auto_splits_on_bytes_with_skewed_value_sizes` for the one
// sibling test that stayed and why, and `crates/animusd/CLAUDE.md`'s
// matching entry for the full account.
