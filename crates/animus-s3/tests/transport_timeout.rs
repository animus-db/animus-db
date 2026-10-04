//! S-08 M3: `HyperRustlsTransport` must never hang on a stalled endpoint.
//! A real-socket test (hence its own target, `required-features = ["prod"]`):
//! a local listener accepts the connection and then never answers.

#![cfg(feature = "prod")]
#![allow(
    clippy::disallowed_methods,
    reason = "real-socket liveness test: tokio::spawn holds the listener's \
              connections open and Instant::now measures that the real \
              request deadline fired — the determinism seam is SimEnv-only \
              and this test deliberately runs over real sockets/time"
)]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use animus_s3::client::{HttpRequest, Transport, TransportError};
use animus_s3::prod::{HyperRustlsTransport, TransportTimeouts};

#[tokio::test]
async fn stalled_endpoint_times_out_with_a_retryable_error() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    // Accept and hold the connection open without ever responding.
    let _holder = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((sock, _)) = listener.accept().await {
            held.push(sock);
        }
    });

    let transport = HyperRustlsTransport::new_allow_insecure_http()
        .expect("transport")
        .with_timeouts(TransportTimeouts {
            connect: Duration::from_secs(5),
            request: Duration::from_millis(300),
        });
    let mut headers = BTreeMap::new();
    headers.insert("host".to_string(), addr.to_string());
    let started = Instant::now();
    let err = transport
        .send(HttpRequest {
            method: "GET",
            uri: "/bucket/key".to_string(),
            headers,
            body: Vec::new(),
        })
        .await
        .expect_err("a stalled endpoint must time out");
    assert!(matches!(err, TransportError::Timeout(_)), "{err:?}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the request deadline, not the connect deadline, must fire"
    );
}
