//! ADR 0027: proves cross-process trace-context propagation at the protobuf
//! level. `cp_forward` injects the current span's W3C `traceparent` onto the
//! wire (`animusd::otel::current_traceparent`); `cp_serve_forwarded`'s caller
//! re-parents its span from that string (`animusd::otel::
//! set_parent_traceparent`) before handling the forwarded request. This test
//! drives those two primitives directly against a real OTLP/HTTP receiver and
//! decodes the exported spans, so a regression in the inject/extract wiring
//! (wrong propagator, wrong span-entry ordering, wrong endpoint resolution)
//! fails here instead of only being visible as an unjoined trace in a real
//! backend.
//!
//! Not exercised here: `Node`/`ClientCtx` — this targets the tracing seam in
//! isolation, the same primitives real request forwarding calls.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Runs a minimal OTLP/HTTP receiver: accepts connections, captures each
/// request's body (assumed to be the whole remaining read after the `\r\n\r\n`
/// header terminator), and replies 200. Good enough for one exporter's batch
/// POSTs — not a spec-complete HTTP server.
fn spawn_capturing_receiver() -> (std::net::SocketAddr, Arc<Mutex<Vec<Vec<u8>>>>) {
    let bodies: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let bodies_for_task = bodies.clone();
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind receiver");
    std_listener.set_nonblocking(true).expect("nonblocking");
    let addr = std_listener.local_addr().expect("local_addr");
    let listener = TcpListener::from_std(std_listener).expect("tokio listener");

    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let bodies = bodies_for_task.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 65536];
                loop {
                    match tokio::time::timeout(Duration::from_millis(500), stream.read(&mut chunk))
                        .await
                    {
                        Ok(Ok(0)) | Err(_) => break,
                        Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
                        Ok(Err(_)) => break,
                    }
                }
                if let Some(header_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    bodies.lock().unwrap().push(buf[header_end + 4..].to_vec());
                }
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                    .await;
            });
        }
    });

    (addr, bodies)
}

#[tokio::test]
async fn forwarded_span_joins_the_originating_trace() {
    let (addr, bodies) = spawn_capturing_receiver();

    // `init_tracing_with_endpoint` takes the endpoint as a parameter rather
    // than reading `OTEL_EXPORTER_OTLP_ENDPOINT` from the process — mutating
    // process env is `unsafe` (edition 2024) and this workspace forbids
    // `unsafe_code` outright, so this is the test-facing seam (see its doc
    // comment in `otel.rs`). It also sidesteps any ambient `RUST_LOG` the
    // test happens to run under, since `info_span!` below is always enabled.
    let endpoint = format!("http://{addr}");
    let provider = animusd::otel::init_tracing_with_endpoint("otel-tracing-test", Some(&endpoint))
        .expect("an explicit endpoint was passed, so export must be enabled");

    // Mirror `cp_forward`: create a root span (the request as first accepted),
    // extract its `traceparent` while it's the active span, then drop it.
    let origin = tracing::info_span!("test_origin_span");
    let traceparent = {
        let _guard = origin.enter();
        animusd::otel::current_traceparent()
            .expect("a span under an active OpenTelemetry layer must carry a valid context")
    };
    drop(origin);

    // Mirror `cp_serve_forwarded`'s caller: build a new span for the request
    // as re-received on another node, and re-parent it from the propagated
    // `traceparent` *before* entering it.
    let forwarded = tracing::info_span!("test_forwarded_span");
    animusd::otel::set_parent_traceparent(&forwarded, &traceparent);
    drop(forwarded.enter());
    drop(forwarded);

    // `force_flush` blocks the calling OS thread until the exporter's HTTP
    // call completes; `#[tokio::test]`'s default current-thread runtime has
    // no other thread to drive this test's own receiver task meanwhile, so
    // run it via `spawn_blocking` (a dedicated blocking-pool thread) rather
    // than starving the runtime the receiver needs to accept the connection.
    tokio::task::spawn_blocking({
        let provider = provider.clone();
        move || provider.force_flush()
    })
    .await
    .expect("spawn_blocking join")
    .expect("force_flush should succeed");
    // the batch processor hands off to the exporter synchronously, but the
    // exporter's own HTTP POST happens on its background thread — give it a
    // moment to actually land at the receiver.
    tokio::time::sleep(Duration::from_secs(1)).await;
    provider.shutdown().ok();

    let captured = bodies.lock().unwrap().clone();
    assert!(
        !captured.is_empty(),
        "no OTLP export reached the test receiver"
    );

    let mut spans = Vec::new();
    for body in &captured {
        let request =
            ExportTraceServiceRequest::decode(body.as_slice()).expect("valid OTLP protobuf body");
        for resource_spans in request.resource_spans {
            for scope_spans in resource_spans.scope_spans {
                spans.extend(scope_spans.spans);
            }
        }
    }

    let origin_span = spans
        .iter()
        .find(|s| s.name == "test_origin_span")
        .expect("origin span was exported");
    let forwarded_span = spans
        .iter()
        .find(|s| s.name == "test_forwarded_span")
        .expect("forwarded span was exported");

    assert_eq!(
        origin_span.trace_id, forwarded_span.trace_id,
        "the forwarded span must join the origin's trace, not start its own"
    );
    assert_eq!(
        forwarded_span.parent_span_id, origin_span.span_id,
        "the forwarded span must be a direct child of the origin span"
    );
}
