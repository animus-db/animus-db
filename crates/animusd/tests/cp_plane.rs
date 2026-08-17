//! The **leaderful CP data plane** runs in the assembled node over `ProdEnv`
//! (ADR 0017 #3a / v1 ADR 0019: CP-only). Every client read/write is routed to a
//! per-tablet Raft group (`animus-cp-data`) hosted on the nodes' `raftkv` role —
//! the single, linearizable source of truth.
//!
//! This is the production assembly of the CP plane whose mechanism is sim-proven
//! in `animus-cp-data` (single-tablet linearizable KV, ReadIndex reads). Here we
//! drive it over real TCP/time through the same client API the CLI uses:
//!
//! 1. bring up a 3-node cluster and bootstrap it;
//! 2. write a key through one node's client API — the node routes it to the CP
//!    group leader (in-process: the shared cluster edge state reaches the leader);
//! 3. read it back through a *different* node — the CP group is the single source
//!    of truth, so the linearizable read observes the committed write;
//! 4. an absent key reads as `None` (not a phantom); an untagged key round-trips
//!    the same way (the optional `table` no longer selects a plane — there is only
//!    the CP plane).
//!
//! Real TCP/time, so it polls with generous timeouts rather than asserting
//! deterministic timing. Cross-process CP routing (forwarding to the leader's
//! node) is covered by `cp_cross_process.rs`.

use std::time::Duration;

use animus_env::NodeId;
use animusd::{
    ClientRequest, ClientResponse, Node, StorageBackend, bind_cluster, read_frame, start_cluster,
    start_cluster_auto_split, start_cluster_with_auto_split_bytes,
};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn reads_and_writes_route_through_the_raft_group() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    // Write a key through node 0's client API, tagged with a table name. The node
    // routes it to the CP group leader. Retry until PutOk: the CP group may still
    // be electing its own leader (independent of the control plane's), so `cp_put`
    // errors until it settles.
    let addr0 = nodes[0].client_addr();
    let put_ok = async {
        loop {
            let resp = call(
                addr0,
                ClientRequest::Put {
                    key: b"k".to_vec(),
                    value: b"cp-value".to_vec(),
                    table: CP_TABLE.into(),
                },
            )
            .await;
            match resp {
                ClientResponse::PutOk => return,
                ClientResponse::Error(_) => sleep(Duration::from_millis(100)).await,
                other => panic!("unexpected CP put response: {other:?}"),
            }
        }
    };
    timeout(Duration::from_secs(20), put_ok)
        .await
        .expect("CP write did not succeed within 20s");

    // Read it back through a *different* node's client API: the CP group is the
    // single source of truth (reached via the shared cluster edge state), so the
    // linearizable read observes the committed write.
    let addr2 = nodes[2].client_addr();
    let got = call(
        addr2,
        ClientRequest::Get {
            key: b"k".to_vec(),
            table: CP_TABLE.into(),
        },
    )
    .await;
    assert_eq!(
        got,
        ClientResponse::Value(Some(b"cp-value".to_vec())),
        "CP read must observe the committed CP write"
    );

    // A read of an absent key reads as `None` (not a phantom).
    let absent = call(
        addr0,
        ClientRequest::Get {
            key: b"absent".to_vec(),
            table: CP_TABLE.into(),
        },
    )
    .await;
    assert_eq!(absent, ClientResponse::Value(None));

    // An **untagged** key round-trips the same way (there is only the CP plane; the
    // optional `table` no longer selects a plane).
    let untagged_put = call(
        addr0,
        ClientRequest::Put {
            key: b"u".to_vec(),
            value: b"u-value".to_vec(),
            table: "kv".to_string(),
        },
    )
    .await;
    assert!(
        matches!(untagged_put, ClientResponse::PutOk),
        "untagged put failed: {untagged_put:?}"
    );
    let untagged_got = call(
        addr2,
        ClientRequest::Get {
            key: b"u".to_vec(),
            table: "kv".to_string(),
        },
    )
    .await;
    assert_eq!(
        untagged_got,
        ClientResponse::Value(Some(b"u-value".to_vec()))
    );

    for n in &nodes {
        n.shutdown();
    }
}

/// Phase 2.3a / ADR 0032 PR1 — **CP member address distribution.** Each
/// CP-group node registers its full address book (`raftkv`/`client`/`admin`) in
/// the replicated control-plane `Metadata` (`node_addrs` — `RegisterNodeAddrs`
/// superseded `RegisterCpAddr` for this self-registration; the older command
/// stays only for WAL back-compat), so a peer-sync loop on every node can reach
/// a runtime-created group member (a split sibling, a joined node), and any
/// node can forward/relay to any other. Here we assert the bootstrap members
/// register and the entries replicate cluster-wide: every node sees a
/// parseable `raftkv` address for each of the 3 CP group member ids
/// (node ids `0..3`).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cp_member_addresses_register_and_replicate() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let want: Vec<NodeId> = (0..3).map(animusd::config::node_id).collect();
    let replicated = async {
        loop {
            // Every node's replicated view has a parseable raftkv address for
            // all 3 members.
            let ok = nodes.iter().all(|n| {
                let m = n.metadata();
                want.iter().all(|id| {
                    m.node_addrs
                        .get(id)
                        .and_then(|a| a.internal.parse::<std::net::SocketAddr>().ok())
                        .is_some()
                })
            });
            if ok {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), replicated)
        .await
        .expect("CP member addresses did not register + replicate within 20s");

    for n in &nodes {
        n.shutdown();
    }
}

/// **CP tablet split over `ProdEnv`.** A single, atomic control-plane command
/// (`SplitTablet`) narrows the source tablet's range and mints a new sibling
/// tablet covering the upper range — both served from the same replicas' one
/// shared per-node storage engine (ADR 0026/0028), so no data moves. The
/// per-node join-host loop then forms the new tablet's Raft group on each
/// replica. Both halves keep serving, and a value written before the split
/// (into what becomes the upper range) is still there after.
///
/// Real TCP/time: bring up a 3-node cluster, write a lower + an upper key, trigger
/// the split, then poll until the new tablet is in the map and the upper key is
/// served by the new group (its election takes a moment).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cp_tablet_splits_and_both_halves_serve() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr0 = nodes[0].client_addr();

    // Helper: a Put that retries until PutOk (CP group may still be electing).
    async fn put_until_ok(addr: std::net::SocketAddr, key: &[u8], value: &[u8]) {
        let put = async {
            loop {
                match call(
                    addr,
                    ClientRequest::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        table: "kv".to_string(),
                    },
                )
                .await
                {
                    ClientResponse::PutOk => return,
                    ClientResponse::Error(_) => sleep(Duration::from_millis(100)).await,
                    other => panic!("unexpected put response: {other:?}"),
                }
            }
        };
        timeout(Duration::from_secs(20), put)
            .await
            .expect("put did not succeed within 20s");
    }

    // Write a lower key and an upper key (split point will be "k5").
    put_until_ok(addr0, b"k1", b"lower").await;
    put_until_ok(addr0, b"k9", b"upper").await; // rides the handoff to the new group

    // Trigger the split of the bootstrap tablet (id 1) at "k5".
    let resp = call(
        addr0,
        ClientRequest::SplitTablet {
            tablet: 1,
            split_key: b"k5".to_vec(),
        },
    )
    .await;
    assert!(
        matches!(resp, ClientResponse::PutOk),
        "split trigger rejected: {resp:?}"
    );

    // The control plane now has a second tablet (the new upper-range tablet).
    let split_recorded = async {
        loop {
            if nodes.iter().all(|n| n.metadata().tablets.len() == 2) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), split_recorded)
        .await
        .expect("split was not recorded in the tablet map within 20s");

    // The upper key is now served by the NEW group (seeded from the handoff): read
    // it back via a different node, retrying while the new group elects + its
    // members' addresses propagate.
    let read_upper = async {
        loop {
            let got = call(
                nodes[2].client_addr(),
                ClientRequest::Get {
                    key: b"k9".to_vec(),
                    table: "kv".to_string(),
                },
            )
            .await;
            if got == ClientResponse::Value(Some(b"upper".to_vec())) {
                return;
            }
            sleep(Duration::from_millis(150)).await;
        }
    };
    timeout(Duration::from_secs(30), read_upper)
        .await
        .expect("upper key not served by the new tablet's group within 30s");

    // The lower key still round-trips on the original group.
    let lower = call(
        addr0,
        ClientRequest::Get {
            key: b"k1".to_vec(),
            table: "kv".to_string(),
        },
    )
    .await;
    assert_eq!(lower, ClientResponse::Value(Some(b"lower".to_vec())));

    // A *new* upper-range write routes to the new group and round-trips.
    put_until_ok(addr0, b"k7", b"upper2").await;
    let new_upper = call(
        nodes[1].client_addr(),
        ClientRequest::Get {
            key: b"k7".to_vec(),
            table: "kv".to_string(),
        },
    )
    .await;
    assert_eq!(new_upper, ClientResponse::Value(Some(b"upper2".to_vec())));

    for n in &nodes {
        n.shutdown();
    }
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
    let dir = tempfile::tempdir().unwrap();
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
        n.shutdown();
    }
}

/// Phase 2.4 — **automatic size-telemetry split trigger.** With the auto-split
/// loop enabled at a low key-count threshold, writing past it causes the tablet's
/// leader to split it at the median **with no manual trigger**; afterwards both
/// halves serve. Closes the auto-shard loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn tablet_auto_splits_when_it_grows() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    // Auto-split once a tablet exceeds 16 keys (a test threshold; production is
    // size-based + higher).
    let nodes = start_cluster_auto_split(bound, 16).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr0 = nodes[0].client_addr();

    // Write 24 distinct keys (> threshold) to the single bootstrap tablet.
    for i in 0..24u32 {
        let key = format!("key{i:02}").into_bytes();
        let value = format!("v{i}").into_bytes();
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
            .unwrap_or_else(|_| panic!("write key{i:02} timed out"));
    }

    // The auto-split loop (no manual trigger) splits the over-threshold tablet.
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
        .expect("tablet did not auto-split within 30s");

    // Both halves serve: a low key and a high key both read back, retrying while the
    // new (upper) group elects + its addresses propagate.
    for (k, want) in [
        (b"key00".to_vec(), b"v0".to_vec()),
        (b"key23".to_vec(), b"v23".to_vec()),
    ] {
        let read = async {
            loop {
                let got = call(
                    nodes[2].client_addr(),
                    ClientRequest::Get {
                        key: k.clone(),
                        table: "kv".to_string(),
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
            .unwrap_or_else(|_| panic!("key {k:?} not served after auto-split"));
    }

    for n in &nodes {
        n.shutdown();
    }
}

/// ADR 0034 — **byte-based** auto-split trigger with skewed value sizes. A
/// tablet with only a handful of keys (well under any reasonable key-count
/// threshold — this cluster is started with `keys: None`, so the key-count
/// trigger is disabled entirely) but a few large values auto-splits purely
/// on the **byte** threshold, and — the point of the byte-weighted median —
/// the resulting halves are roughly **byte**-balanced, not just
/// key-count-balanced (a plain positional median here would put nearly all
/// the bytes on one side: 6 tiny keys sort before 6 large ones, so the
/// positional median falls right at the first large key).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn tablet_auto_splits_on_bytes_with_skewed_value_sizes() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    // Bytes-only trigger (no key-count threshold at all): 6 tiny rows (~10
    // bytes each) + 6 large rows (~2000 bytes each) comfortably exceed this
    // *combined*, while each post-split half (~6,000 bytes, see the
    // byte-weighted-median math in the comment below) stays under it — so
    // the tablet splits exactly once, not repeatedly. The key count (12)
    // would never trip any sane key threshold.
    const BYTES_THRESHOLD: u64 = 8_000;
    let nodes = start_cluster_with_auto_split_bytes(
        bound,
        StorageBackend::default(),
        None,
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
        n.shutdown();
    }
}

/// Regression: a tablet can be split **more than once** over its life — since
/// split is just a metadata range-narrowing command (epoch-CAS gated, no
/// one-shot latch anywhere), a tablet that already split once and keeps
/// absorbing writes must split *again* once it regrows past `threshold`, not
/// sit frozen forever. This proves the tablet count keeps growing as the same
/// lineage repeatedly crosses the threshold, and that every resulting tablet
/// ends up with a real CP group (structurally guaranteed now — see the root
/// `CLAUDE.md`).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn already_split_tablet_splits_again_once_it_regrows() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster_auto_split(bound, 16).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr0 = nodes[0].client_addr();

    async fn put(addr0: std::net::SocketAddr, key: Vec<u8>, value: Vec<u8>) {
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
    }

    // First 24 keys trip the threshold and the tablet auto-splits once.
    for i in 0..24u32 {
        let key = format!("key{i:02}").into_bytes();
        let value = format!("v{i}").into_bytes();
        timeout(Duration::from_secs(20), put(addr0, key, value))
            .await
            .unwrap_or_else(|_| panic!("write key{i:02} timed out"));
    }

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
        .expect("tablet did not auto-split within 30s");
    let tablet_count_after_first_split = nodes[0].metadata().tablets.len();

    // Keep writing keys that sort *below* every `key##` key already written —
    // the split's median falls somewhere inside `key00..key23`, so every
    // `aaa##` key lands below it, in the original tablet's lower range (the
    // one whose group already split once). Deliberately avoiding new keys in
    // the *upper* range keeps this test isolated to the already-split
    // tablet's own lineage re-splitting, not the sibling splitting for its
    // own first time.
    for i in 0..40u32 {
        let key = format!("aaa{i:02}").into_bytes();
        let value = format!("v{i}").into_bytes();
        timeout(Duration::from_secs(20), put(addr0, key, value))
            .await
            .unwrap_or_else(|_| panic!("write aaa{i:02} timed out"));
    }

    // The already-split lineage must split *again* — tablet count grows past
    // where it stopped after the first split.
    let second_split = async {
        loop {
            if nodes
                .iter()
                .all(|n| n.metadata().tablets.len() > tablet_count_after_first_split)
            {
                return;
            }
            sleep(Duration::from_millis(200)).await;
        }
    };
    timeout(Duration::from_secs(30), second_split)
        .await
        .expect("already-split tablet did not split again after regrowing");

    // Every key written across both split rounds is still reachable — no
    // orphans/lost routing left behind by splitting the same lineage twice.
    for (k, want) in [
        (b"aaa00".to_vec(), b"v0".to_vec()),
        (b"aaa39".to_vec(), b"v39".to_vec()),
        (b"key00".to_vec(), b"v0".to_vec()),
        (b"key23".to_vec(), b"v23".to_vec()),
    ] {
        let read = async {
            loop {
                let got = call(
                    addr0,
                    ClientRequest::Get {
                        key: k.clone(),
                        table: "kv".to_string(),
                    },
                )
                .await;
                if got == ClientResponse::Value(Some(want.clone())) {
                    return;
                }
                sleep(Duration::from_millis(150)).await;
            }
        };
        timeout(Duration::from_secs(20), read)
            .await
            .unwrap_or_else(|_| panic!("key {k:?} not served after repeated auto-split"));
    }

    for n in &nodes {
        n.shutdown();
    }
}
