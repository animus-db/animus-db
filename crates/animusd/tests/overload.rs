//! R-01 (d), ADR 0074 §2 — real-`ProdEnv`, real-TCP proof of the DynamoDB
//! listener's overload bounds: the connection cap (D-4), the node-wide
//! in-flight admission bound (D-5), and the `overload_shed_*` counters (D-8).
//! Real sockets and real threads, so it lives in its own target (a `SimEnv`
//! cannot prove accept-loop/permit behavior), and every wait is a
//! converged-or-timeout poll, never a fixed sleep used as an assertion.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use animusd::config::{NodeRole, OverloadSection};
use animusd::{ClusterConfig, Node, RoleAddrs};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, timeout};

mod support;

const STEP: Duration = Duration::from_millis(25);
const DEADLINE: Duration = Duration::from_secs(30);

async fn start(dir: &std::path::Path, overload: OverloadSection) -> (Node, SocketAddr) {
    let a = support::reserve_addrs(6);
    let addrs = RoleAddrs {
        id: animusd::config::node_id(0),
        role: NodeRole::Both,
        internal: a[0],
        client: a[1],
        dynamo: a[2],
        admin: a[3],
        intra: a[4],
        console: a[5],
        advertise_host: None,
        tls: None,
        encryption_key_path: None,
        labels: Default::default(),
        overload: Some(overload),
    };
    let bound = Node::bind(animusd::config::node_id(0), addrs.clone(), dir)
        .await
        .expect("bind");
    let config = ClusterConfig {
        version: animusd::config::CLUSTER_CONFIG_VERSION,
        nodes: vec![RoleAddrs {
            internal: bound.internal_addr(),
            client: bound.client_addr(),
            dynamo: bound.dynamo_addr(),
            admin: bound.admin_addr(),
            intra: bound.intra_addr(),
            console: bound.console_addr(),
            ..addrs
        }],
        dynamo_auth: None,
        cluster_settings: None,
    };
    let dynamo = bound.dynamo_addr();
    let node = animusd::run_bound_node(bound, &config, 0)
        .await
        .expect("start");
    support::await_bootstrap(std::slice::from_ref(&node)).await;
    (node, dynamo)
}

/// Read one HTTP response (head + `Content-Length` body) off a keep-alive
/// connection; `None` if the peer closed first.
async fn read_response(s: &mut TcpStream) -> Option<(u16, String)> {
    let mut buf = Vec::new();
    loop {
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..p]).to_string();
            let len: usize = head
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|v| v.trim().parse().unwrap())
                })
                .unwrap_or(0);
            while buf.len() < p + 4 + len {
                let mut c = [0u8; 4096];
                let n = s.read(&mut c).await.ok()?;
                if n == 0 {
                    return None;
                }
                buf.extend_from_slice(&c[..n]);
            }
            let status = head.split_whitespace().nth(1)?.parse().ok()?;
            let body = String::from_utf8_lossy(&buf[p + 4..p + 4 + len]).to_string();
            return Some((status, body));
        }
        let mut c = [0u8; 4096];
        let n = s.read(&mut c).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&c[..n]);
    }
}

fn request(target: Option<&str>, body: &str, close: bool) -> String {
    let conn = if close { "close" } else { "keep-alive" };
    match target {
        None => format!("GET /metrics HTTP/1.1\r\nHost: a\r\nConnection: {conn}\r\n\r\n"),
        Some(t) => format!(
            "POST / HTTP/1.1\r\nHost: a\r\nX-Amz-Target: {t}\r\n\
             Content-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\
             Connection: {conn}\r\n\r\n{body}",
            body.len()
        ),
    }
}

/// One request on a fresh connection, bounded by `timeout`.
async fn once(addr: SocketAddr, target: Option<&str>, body: &str) -> Option<(u16, String)> {
    timeout(Duration::from_secs(10), async {
        let mut s = TcpStream::connect(addr).await.ok()?;
        s.write_all(request(target, body, true).as_bytes())
            .await
            .ok()?;
        read_response(&mut s).await
    })
    .await
    .ok()
    .flatten()
}

fn metric(text: &str, name: &str) -> u64 {
    text.lines()
        .filter_map(|l| {
            let mut it = l.split_whitespace();
            (it.next()? == name).then(|| it.next()?.parse().ok())?
        })
        .next()
        .unwrap_or(0)
}

async fn metrics(addr: SocketAddr) -> String {
    once(addr, None, "").await.expect("metrics").1
}

/// D-4: over the cap a new connection is answered 503 `ServiceUnavailable`
/// promptly (never parked), the shed is counted, and once load drops the
/// listener serves again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connections_over_the_cap_get_503_and_service_recovers() {
    let dir = support::panic_safe_tempdir();
    let (node, dynamo) = start(
        dir.path(),
        OverloadSection {
            max_connections: Some(3),
            ..Default::default()
        },
    )
    .await;

    // Hold three keep-alive connections; each proves itself admitted by
    // completing a request.
    let mut held = Vec::new();
    for _ in 0..3 {
        let mut s = TcpStream::connect(dynamo).await.expect("connect");
        s.write_all(request(None, "", false).as_bytes())
            .await
            .unwrap();
        let (status, _) = timeout(Duration::from_secs(10), read_response(&mut s))
            .await
            .expect("admitted connection answered")
            .expect("response");
        assert_eq!(status, 200);
        held.push(s);
    }

    // The fourth is refused with a typed, retryable 503 — and promptly.
    let started = Instant::now();
    let (status, body) = once(dynamo, Some("DynamoDB_20120810.ListTables"), "{}")
        .await
        .expect("a shed connection must still get a response");
    assert_eq!(status, 503, "{body}");
    assert!(body.contains("ServiceUnavailable"), "{body}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shed took {:?}",
        started.elapsed()
    );

    // The shed is counted (read over a held connection, which is still admitted).
    held[0]
        .write_all(request(None, "", false).as_bytes())
        .await
        .unwrap();
    let (_, text) = read_response(&mut held[0]).await.expect("metrics");
    assert!(metric(&text, "overload_shed_conn_cap") >= 1, "{text}");

    // Load drops -> the listener recovers (converged-or-timeout).
    drop(held);
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Some((200, _)) = once(dynamo, Some("DynamoDB_20120810.ListTables"), "{}").await {
            break;
        }
        assert!(Instant::now() < deadline, "listener never recovered");
        sleep(STEP).await;
    }
    node.shutdown_graceful().await;
}

/// D-5/D-8: with a tiny in-flight bound and many concurrent clients, requests
/// beyond the bound are shed with 503 `ServiceUnavailable` (never queued, never
/// any other status), the shed is counted, and the node serves normally again
/// once the load stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_over_the_inflight_bound_are_shed_and_service_recovers() {
    let dir = support::panic_safe_tempdir();
    let (node, dynamo) = start(
        dir.path(),
        OverloadSection {
            max_inflight_requests: Some(2),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = once(
        dynamo,
        Some("DynamoDB_20120810.CreateTable"),
        r#"{"TableName":"tbl","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}"#,
    )
    .await
    .expect("create");
    assert_eq!(status, 200, "{body}");

    let stop = Arc::new(AtomicBool::new(false));
    let ok = Arc::new(AtomicU64::new(0));
    let shed = Arc::new(AtomicU64::new(0));
    let other = Arc::new(std::sync::Mutex::new(Vec::<(u16, String)>::new()));
    let mut clients = Vec::new();
    for c in 0..24 {
        let (stop, ok, shed, other) = (stop.clone(), ok.clone(), shed.clone(), other.clone());
        clients.push(tokio::spawn(async move {
            let mut s = TcpStream::connect(dynamo).await.expect("connect");
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let body = format!(r#"{{"TableName":"tbl","Item":{{"id":{{"S":"c{c}-{i}"}}}}}}"#);
                i += 1;
                s.write_all(request(Some("DynamoDB_20120810.PutItem"), &body, false).as_bytes())
                    .await
                    .unwrap();
                match timeout(Duration::from_secs(20), read_response(&mut s)).await {
                    Ok(Some((200, _))) => {
                        ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Some((503, b))) if b.contains("ServiceUnavailable") => {
                        shed.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Some(r)) => other.lock().unwrap().push(r),
                    _ => other.lock().unwrap().push((0, "no response".into())),
                }
            }
        }));
    }

    // Run until sheds were observed (converged-or-timeout).
    let deadline = Instant::now() + DEADLINE;
    while shed.load(Ordering::Relaxed) == 0 {
        assert!(
            Instant::now() < deadline,
            "no request was ever shed with 24 clients against max_inflight_requests=2 \
             (ok={})",
            ok.load(Ordering::Relaxed)
        );
        sleep(STEP).await;
    }
    stop.store(true, Ordering::Relaxed);
    for c in clients {
        c.await.expect("client task");
    }
    assert!(ok.load(Ordering::Relaxed) > 0, "nothing was ever admitted");
    assert!(
        other.lock().unwrap().is_empty(),
        "only 200 and 503 ServiceUnavailable are allowed under overload: {:?}",
        other.lock().unwrap()
    );
    let text = metrics(dynamo).await;
    assert!(metric(&text, "overload_shed_admission") >= 1, "{text}");

    // Load is gone: the node serves again.
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Some((200, _)) = once(
            dynamo,
            Some("DynamoDB_20120810.PutItem"),
            r#"{"TableName":"tbl","Item":{"id":{"S":"after"}}}"#,
        )
        .await
        {
            break;
        }
        assert!(Instant::now() < deadline, "node never recovered");
        sleep(STEP).await;
    }
    node.shutdown_graceful().await;
}
