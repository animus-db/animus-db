//! Client/intra port handshake preamble tests (ADR 0073 Phase 0, workstream
//! D, layer 3) — the accept-side and dial-side counterparts of
//! `animus-env`'s own `prod::tests` network-handshake suite (layer 2), at
//! this crate's own `serve_requests`/`connect_client`.
//!
//! Every dial in this file except the raw ones under deliberate test uses
//! [`animusd::connect_client`] — the one shared dial helper every real
//! caller of this wire goes through (see that function's own doc).

use std::time::Duration;

use animus_env::handshake;
use animusd::{ClientRequest, ClientResponse, bind_cluster, read_frame, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

mod support;

/// `GET /metrics` over a fresh HTTP/1.1 connection to `addr` (the node's
/// dynamo listener, which also serves the ADR 0015 text export) — the exact
/// helper `tests/metrics_endpoint.rs` uses, duplicated here per this crate's
/// own per-file-fixture-helper convention (see that file's own doc).
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
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, body) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    (status, body.to_string())
}

/// The `client_handshake_refused` counter's current value, read off the
/// text metrics export (`Metric::ClientHandshakeRefused`,
/// `animus-env/src/metrics.rs`).
fn client_handshake_refused(metrics_text: &str) -> u64 {
    metrics_text
        .lines()
        .find_map(|line| line.strip_prefix("client_handshake_refused "))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("no client_handshake_refused line in:\n{metrics_text}"))
}

/// Send one request to a node's client address through the real, shared
/// dial helper and return the reply — proof the node keeps serving
/// well-behaved dialers after refusing a malformed one.
async fn call(addr: std::net::SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = animusd::connect_client(addr)
        .await
        .expect("connect to node");
    animusd::write_frame(&mut stream, &req)
        .await
        .expect("send request");
    read_frame(&mut stream)
        .await
        .expect("read reply")
        .expect("a reply")
}

/// Case 1: a raw `TcpStream` sending a right-magic, wrong-version (`CHS1`
/// v2) preamble is refused on the client port — mirroring `animus-env`'s
/// own `accept_refuses_mismatched_version_and_keeps_serving` one layer up.
/// Asserts: the client reads the server's own genuine `CHS1` v1 preamble
/// first, then a clean EOF; `client_handshake_refused` increments; and the
/// same node keeps serving a correct client request afterward.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accept_refuses_mismatched_version_and_keeps_serving() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    support::await_bootstrap(&nodes).await;

    let client_addr = nodes[0].client_addr();
    let dynamo_addr = nodes[0].dynamo_addr();

    let (_, before) = get_metrics(dynamo_addr).await;
    let refused_before = client_handshake_refused(&before);

    let mut raw = timeout(Duration::from_secs(5), TcpStream::connect(client_addr))
        .await
        .expect("connect within budget")
        .expect("connect");

    // The server writes its own preamble first, unconditionally — read it
    // back before sending anything ourselves, and check it names the real
    // protocol/version this build actually speaks.
    // `read_preamble` reads the header AND the extension area (ADR 0073
    // Phase 2: a node now advertises its supported cluster-version range in
    // the `ext`, so a bare `HEADER_LEN` read would leave those bytes behind).
    let their_preamble = timeout(
        Duration::from_secs(5),
        animus_env::read_preamble(&mut raw, &handshake::CLIENT_PROTOCOL),
    )
    .await
    .expect("read within budget")
    .expect("read the server's own preamble");
    assert!(
        !their_preamble.extensions.is_empty(),
        "a Phase 2 node advertises its version range in the preamble ext"
    );
    assert_eq!(their_preamble.magic, handshake::CLIENT_PROTOCOL.magic);
    assert_eq!(their_preamble.version, handshake::CLIENT_PROTOCOL.version);

    // Now send our own, deliberately mismatched preamble.
    let bad = handshake::Preamble {
        magic: handshake::CLIENT_PROTOCOL.magic,
        version: handshake::CLIENT_PROTOCOL.version + 1,
        extensions: Vec::new(),
    };
    raw.write_all(&handshake::encode(&bad))
        .await
        .expect("write our mismatched preamble");
    raw.flush().await.expect("flush");

    // The server refuses without ever reading/writing a frame: the next
    // read is a clean EOF, never a `ClientResponse`.
    let mut scratch = [0u8; 1];
    let read = timeout(Duration::from_secs(5), raw.read(&mut scratch))
        .await
        .expect("the refusal must not hang past the handshake timeout");
    assert_eq!(
        read.expect("a read, not an error"),
        0,
        "expected a clean EOF"
    );

    // The refusal was counted…
    let refused_after = poll_refused_count(dynamo_addr, refused_before).await;
    assert!(refused_after > refused_before);

    // …and the node keeps serving a genuine dialer right afterward.
    let resp = call(client_addr, ClientRequest::Status).await;
    assert!(
        matches!(resp, ClientResponse::Status { .. }),
        "node stopped serving after refusing a mismatched peer: {resp:?}"
    );
}

/// Poll `client_handshake_refused` (via [`get_metrics`]) until it exceeds
/// `baseline`, or panic after a generous, bounded budget — never a fixed
/// sleep (the root `CLAUDE.md`'s eventual-property discipline).
async fn poll_refused_count(dynamo_addr: std::net::SocketAddr, baseline: u64) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (_, text) = get_metrics(dynamo_addr).await;
        let v = client_handshake_refused(&text);
        if v > baseline {
            return v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "client_handshake_refused never incremented past {baseline}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Case 2: a raw client that sends a request frame directly, with no
/// preamble at all (the pre-baseline shape) is refused as bad magic —
/// mirroring `animus-env`'s own
/// `accept_refuses_bad_magic_pre_baseline_frame_and_keeps_serving`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accept_refuses_a_pre_baseline_frame_with_no_preamble() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    support::await_bootstrap(&nodes).await;

    let client_addr = nodes[0].client_addr();
    let dynamo_addr = nodes[0].dynamo_addr();
    let (_, before) = get_metrics(dynamo_addr).await;
    let refused_before = client_handshake_refused(&before);

    let mut raw = timeout(Duration::from_secs(5), TcpStream::connect(client_addr))
        .await
        .expect("connect within budget")
        .expect("connect");

    // Read (and discard) the server's own preamble — present regardless of
    // what the client does.
    timeout(
        Duration::from_secs(5),
        animus_env::read_preamble(&mut raw, &handshake::CLIENT_PROTOCOL),
    )
    .await
    .expect("read within budget")
    .expect("read the server's own preamble");

    // A pre-baseline client's own first bytes: a plain, unversioned
    // length-prefixed `ClientRequest::Status` frame, never this preamble.
    let framed =
        animus_node::codec::encode_client_frame(&ClientRequest::Status).expect("encode a frame");
    raw.write_all(&framed).await.expect("write the raw frame");
    raw.flush().await.expect("flush");

    // The server refuses on the header alone (bad magic, before it ever
    // reads the rest of our frame) and closes — its own socket then has
    // unread bytes of ours still sitting in its receive buffer at close,
    // which a real TCP stack reports back to us as a reset rather than a
    // clean FIN/EOF; either is the same refusal from this test's point of
    // view (never a `ClientResponse`).
    let mut scratch = [0u8; 1];
    let read = timeout(Duration::from_secs(5), raw.read(&mut scratch))
        .await
        .expect("the refusal must not hang past the handshake timeout");
    match read {
        Ok(0) => {}
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
        other => panic!("expected a clean EOF or a reset, got {other:?}"),
    }

    let refused_after = poll_refused_count(dynamo_addr, refused_before).await;
    assert!(refused_after > refused_before);

    let resp = call(client_addr, ClientRequest::Status).await;
    assert!(
        matches!(resp, ClientResponse::Status { .. }),
        "node stopped serving after refusing a pre-baseline peer: {resp:?}"
    );
}

/// Case 3 (dial side): a fake "server" that replies with a wrong-version
/// preamble makes the shared dial helper, [`animusd::connect_client`],
/// return the named handshake error — never hang past its own timeout,
/// never panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_client_surfaces_a_dial_side_version_mismatch() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
    let addr = listener.local_addr().expect("local addr");

    let stub = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        // Read (and discard) the dialer's own preamble, then reply with a
        // deliberately wrong version.
        let mut header = [0u8; handshake::HEADER_LEN];
        let _ = sock.read_exact(&mut header).await;
        let bad = handshake::Preamble {
            magic: handshake::CLIENT_PROTOCOL.magic,
            version: handshake::CLIENT_PROTOCOL.version + 1,
            extensions: Vec::new(),
        };
        let _ = sock.write_all(&handshake::encode(&bad)).await;
        let _ = sock.flush().await;
    });

    let result = timeout(Duration::from_secs(15), animusd::connect_client(addr)).await;
    let err = result
        .expect("connect_client must not hang past its own handshake timeout")
        .expect_err("a version-mismatched stub must be refused, not accepted");
    let msg = err.to_string();
    assert!(
        msg.contains("unsupported version") || msg.contains("UnsupportedVersion"),
        "error should name the version mismatch: {msg}"
    );

    stub.await.expect("stub task");
}
