//! Issue #1220: a panicked consensus-loop task must be visible on the wire.
//!
//! `ProdEnv` already counted a spawned task's panic (issue #939) but nothing
//! exported it, and a node whose Raft driver or apply loop had died kept
//! answering `/admin/health` 200 while serving nothing for that group. Now
//! `Metric::SpawnedTaskPanics` (any task) and `Metric::ConsensusTaskPanics`
//! (a `spawn_critical` task: control driver / `Metadata` apply loop, a tablet
//! group's driver / apply loop) are exported on `/admin/metrics`, and the
//! latter flips `/admin/health` to 503 with a `consensus_task_panics` field.
//!
//! Real tasks and real sockets, so this is its own `ProdEnv` target (not a
//! `SimEnv` module). A bounded poll, never a fixed sleep: `spawn` keeps no
//! `JoinHandle` to await.
use std::net::SocketAddr;
use std::time::Duration;

use animus_env::EnvExt;
use animusd::StorageBackend;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod support;

/// One `GET <path>` against `addr`: `(status, parsed JSON body)`.
async fn get(addr: SocketAddr, path: &str) -> Option<(u16, serde_json::Value)> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    let req = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.ok()?;
    stream.flush().await.ok()?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.ok()?;
    let text = String::from_utf8(raw).ok()?;
    let (head, payload) = text.split_once("\r\n\r\n")?;
    let status: u16 = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some((status, serde_json::from_str(payload).ok()?))
}

/// Poll `path` until `pred(status, body)` holds, or fail with the last answer.
async fn wait_for(
    addr: SocketAddr,
    path: &str,
    what: &str,
    pred: impl Fn(u16, &serde_json::Value) -> bool,
) -> (u16, serde_json::Value) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut last = None;
    loop {
        if let Some((s, b)) = get(addr, path).await {
            if pred(s, &b) {
                return (s, b);
            }
            last = Some((s, b));
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what} on {path}; last answer: {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panicked_consensus_task_fails_health_and_counts() {
    let dir = support::panic_safe_tempdir();
    let (node, _config) = support::start_single_node(dir.path(), StorageBackend::Memory).await;
    let admin = node.admin_addr();
    let env = node
        .envs_for_test()
        .first()
        .expect("a node has at least one role env")
        .clone();

    // Healthy to begin with (a single node elects itself), counters at zero.
    let (_, body) = wait_for(admin, "/admin/health", "a healthy node", |s, _| s == 200).await;
    assert_eq!(body["consensus_task_panics"], 0, "{body}");
    assert_eq!(body["ok"], true, "{body}");
    // Issue #1274: `/admin/ready` (the readiness probe) is 200 too.
    let (rs, rbody) = get(admin, "/admin/ready").await.expect("ready answers");
    assert_eq!(rs, 200, "{rbody}");
    assert_eq!(rbody["metadata_synced"], true, "{rbody}");

    // A panic in an ordinary spawned task is counted but is NOT a consensus
    // loop: health stays 200.
    env.spawn_task(async { panic!("issue-1220 injected ordinary-task panic") });
    wait_for(
        admin,
        "/admin/metrics",
        "spawned_task_panics == 1",
        |_, b| b["counters"]["spawned_task_panics"] == 1,
    )
    .await;
    let (s, body) = get(admin, "/admin/health").await.expect("health answers");
    assert_eq!(
        s, 200,
        "an ordinary task panic must not fail health: {body}"
    );
    assert_eq!(body["consensus_task_panics"], 0, "{body}");
    assert_eq!(env.consensus_task_panics(), 0);

    // A panic in a consensus-loop task flips health and bumps both metrics.
    env.spawn_critical_task(async { panic!("issue-1220 injected consensus-loop panic") });
    let (status, body) = wait_for(admin, "/admin/health", "health to fail", |s, _| s != 200).await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["ok"], false, "{body}");
    assert_eq!(body["consensus_task_panics"], 1, "{body}");
    // ... and so does `/admin/ready`: only a restart repairs a dead loop.
    let (rs, rbody) = get(admin, "/admin/ready").await.expect("ready answers");
    assert_eq!(rs, 503, "{rbody}");
    assert_eq!(rbody["ok"], false, "{rbody}");
    assert_eq!(rbody["consensus_task_panics"], 1, "{rbody}");
    let (_, m) = get(admin, "/admin/metrics").await.expect("metrics answer");
    assert_eq!(m["counters"]["consensus_task_panics"], 1, "{m}");
    assert_eq!(m["counters"]["spawned_task_panics"], 2, "{m}");
    assert_eq!(env.consensus_task_panics(), 1);
    assert_eq!(env.spawned_task_panics(), 2);

    // Liveness is deliberately independent: a restart repairs this, and the
    // kubelet must still be able to reach the process to do it.
    let (live, _) = get(admin, "/admin/live").await.expect("live answers");
    assert_eq!(live, 200);

    node.shutdown_and_wait().await;
}
