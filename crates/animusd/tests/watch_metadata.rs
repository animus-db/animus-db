//! **Long-poll metadata watch** (ADR 0035 PR5 for the long-poll mechanism
//! itself, ADR 0038 PR5 for the incremental reply shape):
//! the wire primitive `remote_metadata_watch_loop` drives for a data-only
//! node's mirror sync, tested directly here against the server side
//! (`ClientCtx::watch_metadata`) rather than through the client-side loop —
//! the loop itself has no externally observable behavior beyond "the mirror
//! stays current," already covered end-to-end by `tests/data_only.rs`; what
//! this file proves is the disciplines the server side must get right:
//!
//! - a genuine control-group replica (`ControlHandle::Local`) parked on
//!   `WatchMetadata` actually **wakes on the commit**, not merely on its own
//!   `WATCH_METADATA_SERVER_TIMEOUT` bound elapsing (the reply comes back
//!   with a strictly higher watermark within a couple hundred milliseconds
//!   of the propose, not near the 8-second timeout) — and, since ADR 0038
//!   PR5, that this specific reply is a cheap [`ClientResponse::MetadataDelta`],
//!   not a full [`ClientResponse::Status`] clone (a fresh cluster's delta
//!   ring trivially covers a couple of commits);
//! - a data-only node (`ControlHandle::Remote`) **rejects** `WatchMetadata`
//!   outright instead of degrading (see `ClientCtx::watch_metadata`'s doc for
//!   why serving it there would be worse than the pre-PR5 fixed-interval
//!   poll, not better);
//! - a control-only node's ring resets across a real process restart (ADR
//!   0038 PR5): a watcher whose `last_seen` predates the restart falls back
//!   to a full `Status` reply, while one already caught up to the
//!   post-restart watermark still gets a cheap trivial delta.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, MetaCommand, read_frame};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;
use support::{bring_up_split, start_single_node};

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// The watermark carried by either `WatchMetadata` reply shape (ADR 0038
/// PR5) — most assertions in this file only care about the watermark, not
/// which shape carried it.
fn watermark_of(reply: &ClientResponse) -> u64 {
    match reply {
        ClientResponse::Status { watermark, .. } => *watermark,
        ClientResponse::MetadataDelta { watermark, .. } => *watermark,
        other => panic!("expected a Status or MetadataDelta reply, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn control_node_wakes_the_watch_on_a_real_commit_not_the_server_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let (control_nodes, data_nodes, _config) = bring_up_split(1, 1, dir.path()).await;
    timeout(Duration::from_secs(20), async {
        loop {
            if control_nodes[0].is_control_leader() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("control node never became leader");
    let control_client = control_nodes[0].intra_addr(); // ADR 0047: WatchMetadata/ProposeSchema are intra-only

    let watermark0 = watermark_of(&call(control_client, ClientRequest::Status).await);

    // Park the watch, then propose a schema change shortly after — the watch
    // must wake on THAT commit, well inside the 8s server-side bound
    // (`WATCH_METADATA_SERVER_TIMEOUT`), not merely time out and reply with
    // whatever happens to be current.
    let watch_task = tokio::spawn(async move {
        let started = tokio::time::Instant::now();
        let reply = call(
            control_client,
            ClientRequest::WatchMetadata {
                last_seen: watermark0,
            },
        )
        .await;
        (started.elapsed(), reply)
    });

    // Give the watch a moment to actually connect and register on the
    // server's `MetadataWatch` before the commit lands — otherwise this
    // would (harmlessly) test the "already changed before the first poll"
    // path instead of the wake-while-parked path.
    sleep(Duration::from_millis(150)).await;
    let propose_reply = call(
        control_client,
        ClientRequest::ProposeSchema(MetaCommand::CreateKeyspace {
            keyspace: "watch_metadata_wake_test".into(),
        }),
    )
    .await;
    assert!(
        matches!(propose_reply, ClientResponse::PutOk),
        "CreateKeyspace propose was rejected: {propose_reply:?}"
    );

    let (elapsed, reply) = timeout(Duration::from_secs(10), watch_task)
        .await
        .expect("watch task did not finish within 10s")
        .expect("watch task panicked");
    assert!(
        matches!(reply, ClientResponse::MetadataDelta { .. }),
        "a fresh cluster's delta ring trivially covers a couple of commits — \
         expected an incremental MetadataDelta reply, got {reply:?}"
    );
    let watermark1 = watermark_of(&reply);
    assert!(
        watermark1 > watermark0,
        "watch resolved without the watermark advancing ({watermark0} -> {watermark1})"
    );
    // Under the 8s server-side park bound with margin — a watch that only
    // ever resolved by timing out would take close to 8s. The bound is 6s,
    // not lower: this is a wall-clock assertion in a suite full of heavy
    // multi-node ProdEnv tests, and a tighter bound flakes purely from
    // parallel `cargo test --workspace` load (the watermark assertion above
    // is the primary proof the watch woke on the commit; this one only has
    // to discriminate against the 8s timeout).
    assert!(
        elapsed < Duration::from_secs(6),
        "watch took {elapsed:?} to wake on a real commit — looks like it fell through to the \
         server-side timeout instead of waking on the commit"
    );

    for node in control_nodes.iter().chain(data_nodes.iter()) {
        node.shutdown_graceful().await;
    }
}

/// Regression for issue #276: a **combined-mode** node hands the same
/// `Arc<MetadataWatchInner>` to two independent concurrent consumers — its
/// own `tablet_host_reconciler_loop` (which re-registers a fresh `changed()`
/// future every `RECONCILE_FALLBACK_INTERVAL`, 500ms, whenever nothing wakes
/// it sooner) and each inbound `WatchMetadata` long-poll. Under the old
/// single-slot `AtomicWaker`, the reconciler's own periodic re-registration
/// deterministically evicted a long-poll's waker the moment the fallback
/// timer ticked — so a commit landing after that would only ever be
/// observed via the long-poll's own `WATCH_METADATA_SERVER_TIMEOUT` (8s)
/// fallback, not the wake. This proves both consumers now wake independently.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn combined_node_reconciler_and_long_poll_both_wake_on_one_commit() {
    let dir = tempfile::tempdir().unwrap();
    let (node, _config) = start_single_node(dir.path(), animusd::StorageBackend::Lsm).await;
    timeout(Duration::from_secs(20), async {
        loop {
            if node.is_control_leader() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("single combined node never became leader");
    let addr = node.intra_addr(); // ADR 0047: WatchMetadata/ProposeSchema are intra-only

    // Let bootstrap's own commits (self-registration, address-book entries)
    // settle before capturing `watermark0` — otherwise a long-poll parked at
    // a watermark that is already stale resolves on its very first poll
    // (`current > last_seen` already holds), never actually exercising the
    // wake-while-parked path this test exists to prove.
    let watermark0 = timeout(Duration::from_secs(10), async {
        let mut last = watermark_of(&call(addr, ClientRequest::Status).await);
        loop {
            sleep(Duration::from_millis(200)).await;
            let now = watermark_of(&call(addr, ClientRequest::Status).await);
            if now == last {
                return now;
            }
            last = now;
        }
    })
    .await
    .expect("bootstrap watermark never settled");

    let watch_task = tokio::spawn(async move {
        let started = tokio::time::Instant::now();
        let reply = call(
            addr,
            ClientRequest::WatchMetadata {
                last_seen: watermark0,
            },
        )
        .await;
        (started.elapsed(), reply)
    });

    // Let the long-poll actually connect and register on the node's shared
    // `MetadataWatch`, then sit past at least one full
    // `RECONCILE_FALLBACK_INTERVAL` (500ms) so the reconciler loop's own
    // fallback tick fires and re-registers its own `changed()` future on the
    // very same watch — on the old single-slot `AtomicWaker` this
    // deterministically evicts the long-poll's registration before the
    // commit below ever lands.
    sleep(Duration::from_millis(900)).await;

    let propose_reply = call(
        addr,
        ClientRequest::ProposeSchema(MetaCommand::CreateKeyspace {
            keyspace: "combined_multi_waiter_wake_test".into(),
        }),
    )
    .await;
    assert!(
        matches!(propose_reply, ClientResponse::PutOk),
        "CreateKeyspace propose was rejected: {propose_reply:?}"
    );

    let (elapsed, reply) = timeout(Duration::from_secs(10), watch_task)
        .await
        .expect("watch task did not finish within 10s")
        .expect("watch task panicked");
    let watermark1 = watermark_of(&reply);
    assert!(
        watermark1 > watermark0,
        "watch resolved without the watermark advancing ({watermark0} -> {watermark1})"
    );
    // Well under the 8s server-side park bound (WATCH_METADATA_SERVER_TIMEOUT):
    // a long-poll whose waker was evicted by the reconciler's own
    // re-registration would only ever resolve near that timeout.
    assert!(
        elapsed < Duration::from_secs(4),
        "watch took {elapsed:?} to wake on a real commit — looks like the \
         reconciler loop's concurrent registration evicted the long-poll's \
         own waker (issue #276), falling through toward the 8s server timeout"
    );

    node.shutdown_graceful().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn data_only_node_rejects_watch_metadata_instead_of_degrading() {
    let dir = tempfile::tempdir().unwrap();
    let (control_nodes, data_nodes, _config) = bring_up_split(1, 1, dir.path()).await;
    timeout(Duration::from_secs(20), async {
        loop {
            if control_nodes[0].is_control_leader() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("control node never became leader");

    // A data-only node's `ControlHandle` is `Remote` — it has no local
    // `MetadataWatch` tied to any authority, so it must reject the request
    // outright (see `ClientCtx::watch_metadata`'s doc) rather than silently
    // degrading to an ~8s effective poll. Dialed on the **intra** port (ADR
    // 0047: `WatchMetadata` is intra-only) so this test still exercises the
    // `ControlHandle::Remote` rejection this doc is about, not just the
    // (also-correct, but different) client-port surface guard.
    let reply = call(
        data_nodes[0].intra_addr(),
        ClientRequest::WatchMetadata { last_seen: 0 },
    )
    .await;
    assert!(
        matches!(reply, ClientResponse::Error(_)),
        "expected a data-only node to reject WatchMetadata, got {reply:?}"
    );

    for node in control_nodes.iter().chain(data_nodes.iter()) {
        node.shutdown_graceful().await;
    }
}

/// ADR 0038 PR5: a control-only node's delta ring lives only in that
/// process's memory (`RaftNode`'s own `Arc<Mutex<DeltaRing>>`) — a real
/// process restart gets a brand-new, empty one, even though the *engine*
/// (and therefore `Metadata` itself) survives on the durable LSM backend. A
/// `WatchMetadata` caller whose `last_seen` predates the restart must fall
/// back to a full `Status` reply rather than the restarted node silently
/// under-reporting; a caller already caught up to the post-restart watermark
/// still gets the cheap trivial delta.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_control_node_resets_its_ring_and_pre_restart_watchers_fall_back() {
    let dir = tempfile::tempdir().unwrap();
    let addrs = {
        let free = || {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap()
        };
        animusd::RoleAddrs {
            id: animusd::config::node_id(0),
            role: animusd::config::NodeRole::Control,
            internal: free(),
            client: free(),
            dynamo: free(),
            cql: free(),
            admin: free(),
            intra: free(),
            console: free(),
        }
    };
    let config = animusd::ClusterConfig { nodes: vec![addrs] };

    let node = animusd::run_node_control(&config, 0, dir.path(), animusd::StorageBackend::Lsm)
        .await
        .expect("control-only node starts");
    timeout(Duration::from_secs(20), async {
        loop {
            if node.is_control_leader() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("single control node did not elect itself leader in 20s");
    let client_addr = node.intra_addr(); // ADR 0047: WatchMetadata/ProposeSchema are intra-only

    // A watermark from BEFORE any of the commands below — this is the one
    // that must fall back post-restart: those commands land durably in the
    // *engine* pre-crash (so the restarted node's rebuilt watermark already
    // covers them), but a plain restart's tail-replay never individually
    // pushes an already-engine-covered index into the ring at all (see
    // `meta_apply_and_compact`'s watermark-gated skip) — so the freshly
    // reset ring's own history never included them either. A watcher stuck
    // this far back is missing real information the ring cannot serve, not
    // merely re-deriving what it already had.
    let early_watermark = watermark_of(&call(client_addr, ClientRequest::Status).await);

    // Commit a handful of commands so there is real history to fall behind.
    // `PutOk` only means "accepted," not "committed" (the caller confirms via
    // replicated `Metadata` — see `ClientRequest::ProposeSchema`'s handler
    // doc), so poll this node's own `metadata()` directly for each one.
    for i in 0..3u64 {
        let keyspace = format!("watch_deltas_restart_ks_{i}");
        let resp = call(
            client_addr,
            ClientRequest::ProposeSchema(MetaCommand::CreateKeyspace {
                keyspace: keyspace.clone(),
            }),
        )
        .await;
        assert!(
            matches!(resp, ClientResponse::PutOk),
            "propose {i} rejected: {resp:?}"
        );
        timeout(Duration::from_secs(10), async {
            loop {
                if node.metadata().keyspaces.contains(&keyspace) {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("proposed keyspace did not land in 10s");
    }
    let pre_restart_watermark = watermark_of(&call(client_addr, ClientRequest::Status).await);
    assert!(pre_restart_watermark > early_watermark);

    node.shutdown_graceful().await;

    // Restart on the same addresses/dir — control-only, so drives
    // `run_node_control` directly rather than `support::restart_same_addrs`
    // (which is `run_node_with`, the combined-mode entry point). Retries the
    // rebind briefly against the same port-TOCTOU `support::free_addrs`
    // documents (a clean shutdown frees the ports, but another probe can
    // grab one for a moment).
    let node = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match animusd::run_node_control(&config, 0, dir.path(), animusd::StorageBackend::Lsm)
                .await
            {
                Ok(n) => break n,
                Err(e) => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "restart on the same dir/addresses did not rebind: {e}"
                    );
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
    };
    timeout(Duration::from_secs(20), async {
        loop {
            if node.is_control_leader() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("restarted control node did not re-elect itself leader in 20s");

    // `Metadata` survived (the engine is durable) — confirm the restarted
    // node's own watermark is at least what it was pre-restart (it may have
    // advanced further, e.g. a fresh election no-op).
    let post_restart_watermark = watermark_of(&call(client_addr, ClientRequest::Status).await);
    assert!(post_restart_watermark >= pre_restart_watermark);

    // A watcher stuck at `early_watermark` (before any of the 3 commands)
    // is missing real history the freshly reset ring never retained — falls
    // back to a full `Status` reply rather than silently under-reporting.
    let reply = call(
        client_addr,
        ClientRequest::WatchMetadata {
            last_seen: early_watermark,
        },
    )
    .await;
    assert!(
        matches!(reply, ClientResponse::Status { .. }),
        "a watcher predating the restart must get the full-fetch fallback, got {reply:?}"
    );

    // But a watcher already caught up to the current (post-restart)
    // watermark still gets the cheap trivial delta — the ring, though
    // freshly reset, trivially covers "nothing new since exactly now."
    let current = watermark_of(&call(client_addr, ClientRequest::Status).await);
    let reply = call(
        client_addr,
        ClientRequest::WatchMetadata { last_seen: current },
    )
    .await;
    match reply {
        ClientResponse::MetadataDelta { writes, .. } => {
            assert!(writes.is_empty(), "nothing changed since `current`");
        }
        other => panic!("expected a trivial MetadataDelta, got {other:?}"),
    }

    node.shutdown_graceful().await;
}
