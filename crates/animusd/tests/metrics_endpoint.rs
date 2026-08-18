//! End-to-end test of the admin `/metrics` HTTP endpoint over real TCP
//! (`ProdEnv`), as an operator or a Prometheus-style scraper would hit it
//! (ADR 0015).
//!
//! Starts a 3-node in-process cluster, lets it elect a control-plane leader and
//! bootstrap (which exercises `RaftNode`'s election + `AppendEntries`
//! replication, so the control-plane counters move), then `GET /metrics` on a
//! node's HTTP endpoint and asserts the body is the text-format snapshot
//! (`name value` lines) and that the expected control-plane counters appear with
//! the values a real election implies. Like the other `animusd` tests this uses
//! real time and sockets, so it polls with generous timeouts.

use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, bind_cluster, read_frame, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// Wait until every node has the bootstrap tablet replicated, or panic.
async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            let leader = nodes.iter().any(Node::is_control_leader);
            let everyone_has_tablet = nodes.iter().all(|n| !n.metadata().members.is_empty());
            if leader && everyone_has_tablet {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not elect a leader and bootstrap within 20s");
}

/// `GET /metrics` over a fresh HTTP/1.1 connection to `addr` (the node's HTTP
/// endpoint). Returns `(status_code, body)`.
async fn get_metrics(addr: std::net::SocketAddr) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to metrics");
    let request = "GET /metrics HTTP/1.1\r\n\
         Host: animus\r\n\
         Connection: close\r\n\
         \r\n";
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
    assert!(
        head.to_ascii_lowercase()
            .contains("content-type: text/plain"),
        "metrics response should be text/plain, got headers:\n{head}"
    );
    (status, payload.to_string())
}

/// Parse a `name value` line export into a value lookup.
fn parse_metric(body: &str, name: &str) -> Option<i64> {
    body.lines().find_map(|line| {
        let (n, v) = line.split_once(' ')?;
        if n == name {
            v.trim().parse().ok()
        } else {
            None
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_endpoint_surfaces_control_plane_counters() {
    let body = timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
            .await
            .unwrap();
        let nodes = start_cluster(bound).await.unwrap(); // R = W = 2 over 3

        await_bootstrap(&nodes).await;

        // Do a quorum write so the data plane is exercised too (the endpoint
        // aggregates the control + data + coord role sinks). This table's
        // first write right after bootstrap can legitimately race the
        // tablet-host reconciler standing up the freshly auto-provisioned
        // tablet's group, so retry any clean `Error` reply for up to 20s
        // (`docs/engineering-lessons.md`'s "CP write-forward path has no
        // retry-on-not-the-leader-here" entry).
        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            animusd::write_frame(
                &mut stream,
                &ClientRequest::Put {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    table: "kv".to_string(),
                },
            )
            .await
            .expect("send put");
            let put: ClientResponse = read_frame(&mut stream)
                .await
                .expect("read reply")
                .expect("a reply");
            match put {
                ClientResponse::PutOk => break,
                ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                    sleep(Duration::from_millis(150)).await;
                }
                other => panic!("put failed: {other:?}"),
            }
        }

        // Scrape `/metrics` on the node that believes it is leader, so the
        // leadership gauge and the leader-only counters are populated.
        let leader_idx = nodes
            .iter()
            .position(Node::is_control_leader)
            .expect("a leader exists after bootstrap");
        let (status, body) = get_metrics(nodes[leader_idx].dynamo_addr()).await;
        assert_eq!(status, 200, "metrics endpoint should return 200");

        // Also confirm the endpoint is reachable on a *follower* node (every node
        // serves its own aggregated snapshot, not only the leader).
        let follower_idx = (0..nodes.len())
            .find(|&i| i != leader_idx)
            .expect("a follower exists");
        let (fstatus, fbody) = get_metrics(nodes[follower_idx].dynamo_addr()).await;
        assert_eq!(fstatus, 200, "follower metrics endpoint should return 200");
        assert!(
            parse_metric(&fbody, "control_is_leader") == Some(0),
            "follower should report control_is_leader 0, body:\n{fbody}"
        );

        body
    })
    .await
    .expect("test timed out");

    // The body is the ADR 0015 text format: stable `name value` lines, the first
    // being `control_elections_started`, the last the leadership gauge.
    assert!(
        body.starts_with("control_elections_started "),
        "unexpected first line, body:\n{body}"
    );
    assert!(
        body.trim_end().ends_with(&format!(
            "control_is_leader {}",
            parse_metric(&body, "control_is_leader").unwrap()
        )),
        "leadership gauge should be the last line, body:\n{body}"
    );

    // A real election happened: a candidate started and won an election, and the
    // leader has sent `AppendEntries` (heartbeats + the bootstrap replication).
    assert!(
        parse_metric(&body, "control_elections_started").unwrap() >= 1,
        "expected >=1 election started, body:\n{body}"
    );
    assert!(
        parse_metric(&body, "control_elections_won").unwrap() >= 1,
        "expected >=1 election won, body:\n{body}"
    );
    assert!(
        parse_metric(&body, "control_append_entries_sent").unwrap() >= 1,
        "expected >=1 AppendEntries sent, body:\n{body}"
    );
    // We scraped the leader node, so its leadership gauge is set.
    assert_eq!(
        parse_metric(&body, "control_is_leader").unwrap(),
        1,
        "leader node should report control_is_leader 1, body:\n{body}"
    );

    // Every known control metric name is present (closed enum → stable surface).
    for name in [
        "control_elections_started",
        "control_elections_won",
        "control_append_entries_sent",
        "control_append_entries_rejected",
        "control_snapshot_installs",
        "control_failure_detector_down",
        "control_failure_detector_up",
        "control_is_leader",
    ] {
        assert!(
            parse_metric(&body, name).is_some(),
            "metric `{name}` missing from export, body:\n{body}"
        );
    }
}
