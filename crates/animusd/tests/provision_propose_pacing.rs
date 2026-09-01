//! **Schema-proposal pacing in `provision_tablet`** (issue #268): while a
//! `CreateTablet` cannot commit, the provisioning loop must NOT re-propose it
//! on every 50ms poll tick — each duplicate is a real control-plane log
//! append (WAL + replication + apply work) piled on under exactly the
//! slow-commit conditions that made the wait long in the first place. This
//! is the same retry-amplification family `propose_and_await` already fixed
//! one layer up (`docs/engineering-lessons.md`, "the pattern's most common
//! instance was hiding one layer below"); `provision_tablet`'s hand-rolled
//! loop was the remaining unpaced instance on the first-put path — measured
//! at 264 `CreateTablet` proposals for six tablets under a deliberately
//! slowed disk, the self-amplification behind the cp_txn.rs 25s seed-put
//! flake on starved CI runners.
//!
//! The scenario pins the amplification down deterministically: kill both
//! followers of a 3-node cluster so the control leader keeps **accepting**
//! (appending) proposals but can never commit them (no quorum). A put on a
//! fresh table then grinds through `provision_tablet`'s whole
//! commit-timeout budget; the leader's own log growth over a fixed window
//! bounds how many duplicates were proposed. Pre-fix: one append per 50ms
//! poll tick (~60+ over the window). Post-fix: one per
//! `SCHEMA_PROPOSE_PATIENCE` (1s), plus a little unrelated background noise.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, read_frame, write_frame};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed
/// JSON)` — the same helper shape every admin-consuming test carries.
async fn admin(addr: SocketAddr, path: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let request = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.expect("send");
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
    let value: Value = serde_json::from_str(payload).unwrap_or(Value::Null);
    (status, value)
}

/// The control group's full log position on one node: everything ever
/// appended, compaction-proof (`snapshot_index + log_len`).
async fn control_log_position(admin_addr: SocketAddr) -> u64 {
    let (status, raft) = admin(admin_addr, "/admin/raft").await;
    assert_eq!(status, 200, "/admin/raft failed: {raft}");
    raft["snapshot_index"].as_u64().unwrap_or(0) + raft["log_len"].as_u64().unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn provisioning_against_a_quorumless_control_plane_does_not_spam_proposals() {
    let n = 3;
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = support::bring_up_deadline(n, dir.path(), support::JOIN_DEADLINE).await;

    // Bootstrap: a control leader exists and every node registered.
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| n.metadata().members.len() == 3)
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap in 20s");

    let leader_idx = nodes
        .iter()
        .position(|n| n.is_control_leader())
        .expect("just observed a leader");
    let leader_admin = config.nodes[leader_idx].admin;
    let leader_client = config.nodes[leader_idx].client;

    // Let the post-bootstrap registration burst settle so the baseline below
    // is quiet: poll until the leader's log position holds still briefly.
    timeout(Duration::from_secs(20), async {
        let mut last = control_log_position(leader_admin).await;
        let mut stable_since = tokio::time::Instant::now();
        loop {
            sleep(Duration::from_millis(200)).await;
            let now_pos = control_log_position(leader_admin).await;
            if now_pos != last {
                last = now_pos;
                stable_since = tokio::time::Instant::now();
            } else if stable_since.elapsed() >= Duration::from_secs(1) {
                return;
            }
        }
    })
    .await
    .expect("control log never went quiet after bootstrap");

    // Kill both followers: the leader keeps leading (pre-vote; no
    // check-quorum step-down) and keeps *accepting* proposals into its own
    // log, but nothing can commit — the deterministic stand-in for "the
    // control plane is slow to commit," with the window held open as long
    // as we need instead of racing a real slow disk.
    for (i, node) in nodes.iter().enumerate() {
        if i != leader_idx {
            node.shutdown();
        }
    }

    // The quorumless leader is NOT otherwise quiet: its failure detector
    // keeps re-proposing `Down` for the two dead members every tick (the
    // proposal never commits, so the committed membership it re-reads never
    // reflects the transition). That churn is steady-rate, so the put's own
    // contribution is measured as a *difference*: first a control window
    // with no put in flight, then an equal window with `provision_tablet`
    // grinding. Let the detector reach its steady re-propose rate first.
    sleep(Duration::from_secs(2)).await;
    let control_start = control_log_position(leader_admin).await;
    sleep(Duration::from_secs(4)).await;
    let control_appends = control_log_position(leader_admin)
        .await
        .saturating_sub(control_start);

    // A put on a fresh table: `provision_tablet` proposes `CreateTablet`
    // toward the leader and waits for a commit that cannot come. Fire it
    // from a task — it will grind out its own 10s budget and fail; this
    // test only cares how many proposals it appends while grinding.
    let put = tokio::spawn(async move {
        let mut stream = TcpStream::connect(leader_client).await.expect("connect");
        write_frame(
            &mut stream,
            &ClientRequest::Put {
                key: b"pace-key".to_vec(),
                value: b"pace-value".to_vec(),
                table: "pace_t".to_string(),
            },
        )
        .await
        .expect("send put");
        // The reply (an error, after the provision budget) is irrelevant;
        // reading it just keeps the connection open for the duration.
        let _ = read_frame::<ClientResponse>(&mut stream).await;
    });

    // Sample the leader's log growth over an equal window inside the grind.
    let put_start = control_log_position(leader_admin).await;
    sleep(Duration::from_secs(4)).await;
    let put_appends = control_log_position(leader_admin)
        .await
        .saturating_sub(put_start);
    put.abort();

    // Pre-fix: ~one `CreateTablet` append per 50ms poll tick ⇒ ~80 extra
    // over 4s on top of the steady detector rate. Post-fix: one per
    // `SCHEMA_PROPOSE_PATIENCE` (1s) ⇒ ~4-5 extra. The bound sits far from
    // both, with room for detector-rate jitter between the two windows.
    let extra = put_appends.saturating_sub(control_appends);
    assert!(
        extra <= 25,
        "provisioning re-proposed too aggressively against a stalled control plane: \
         {put_appends} log appends in its 4s window vs {control_appends} in the \
         no-put control window (= {extra} put-attributable; expected ~5 with \
         SCHEMA_PROPOSE_PATIENCE pacing, ~80 from an unpaced 50ms loop)"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}
