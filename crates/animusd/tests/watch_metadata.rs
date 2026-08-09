//! **Long-poll metadata watch** (ADR 0035 PR5, `ClientRequest::WatchMetadata`):
//! the wire primitive `remote_metadata_watch_loop` drives for a data-only
//! node's mirror sync, tested directly here against the server side
//! (`ClientCtx::watch_metadata`) rather than through the client-side loop —
//! the loop itself has no externally observable behavior beyond "the mirror
//! stays current," already covered end-to-end by `tests/data_only.rs`; what
//! this file proves is the two disciplines the server side must get right:
//!
//! - a genuine control-group replica (`ControlHandle::Local`) parked on
//!   `WatchMetadata` actually **wakes on the commit**, not merely on its own
//!   `WATCH_METADATA_SERVER_TIMEOUT` bound elapsing (the reply comes back
//!   with a strictly higher watermark within a couple hundred milliseconds
//!   of the propose, not near the 8-second timeout);
//! - a data-only node (`ControlHandle::Remote`) **rejects** `WatchMetadata`
//!   outright instead of degrading (see `ClientCtx::watch_metadata`'s doc for
//!   why serving it there would be worse than the pre-PR5 fixed-interval
//!   poll, not better).

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, MetaCommand, read_frame};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;
use support::bring_up_split;

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
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
    let control_client = control_nodes[0].client_addr();

    let watermark0 = match call(control_client, ClientRequest::Status).await {
        ClientResponse::Status { watermark, .. } => watermark,
        other => panic!("expected a Status reply, got {other:?}"),
    };

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
    let watermark1 = match reply {
        ClientResponse::Status { watermark, .. } => watermark,
        other => panic!("expected a Status reply, got {other:?}"),
    };
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
    // degrading to an ~8s effective poll.
    let reply = call(
        data_nodes[0].client_addr(),
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
