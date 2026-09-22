//! Regression test for issue #1010 (layer 1): `serve_requests`'s
//! per-connection handler used to be a bare, fire-and-forget `tokio::spawn`
//! — invisible to `Node::shutdown_and_wait`, which only ever aborts the two
//! tasks running `serve_requests` itself (`Node.tasks`) plus whatever each
//! internal `ProdEnv` role's own abort registry tracks. A handler accepted
//! just before teardown was in neither set, so it kept running — and its
//! `ClientCtx` kept living — past the point its owning `Node` was
//! supposedly shut down.
//!
//! `serve_requests` now owns every handler in a `tokio::task::JoinSet`
//! local to its own accept-loop future, so aborting that future (which
//! `shutdown_and_wait` already does, via `Node.tasks`) drops the `JoinSet`
//! too — and a `JoinSet`'s `Drop` aborts every task still live in it,
//! cascade-style. See `serve_requests`'s own doc comment (`lib.rs`) for the
//! full mechanism.
//!
//! This test proves it end to end against a real `ProdEnv` node, on **both**
//! listeners `serve_requests` serves (ADR 0047's client + intra ports):
//! connect, prime the connection with one request/response round trip (this
//! proves a handler task genuinely exists for it — a bare TCP `connect()`
//! can succeed the instant the kernel completes the handshake into the
//! listen backlog, before userspace `accept()` ever runs, so a successful
//! `connect()` alone would not prove anything about `serve_requests`'s own
//! `JoinSet`), then leave the connection open and idle so its handler parks
//! mid-`read_frame` waiting for a next request that never comes — exactly
//! the shape of a real client sitting on an open, quiescent connection.
//! `shutdown_and_wait()` the node, then assert — via a single bounded
//! `tokio::time::timeout` around the blocking read, never a bare sleep —
//! that each client-side socket converges to EOF or a reset-class error:
//! proof the server-side handler that held it is gone. `handle_connection`
//! has no idle-read timeout of its own (`read_frame` blocks indefinitely on
//! an idle socket, see `lib.rs`), so this can only converge because
//! `shutdown_and_wait` actually tore the handler down, never because of an
//! unrelated timeout racing to the same result.
//!
//! **Verified RED on unfixed code**: reverting `serve_requests` to its
//! pre-fix bare `tokio::spawn`-per-connection (stashing this commit's
//! `lib.rs` change) makes both assertions below time out instead of ever
//! observing EOF/reset — see this commit's own message for the captured
//! red-run output.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use animusd::{ClientRequest, ClientResponse, StorageBackend, read_frame, write_frame};

mod support;

/// How long the post-shutdown assertion waits for a held-open client
/// socket to observe its server-side handler go away (EOF or reset) —
/// generous for CI/sandbox contention, matching this crate's other
/// post-shutdown polls (`bring_up_allocation.rs`'s own `poll_deadline`).
const HANDLER_GONE_TIMEOUT: Duration = Duration::from_secs(5);

/// Connect to `addr`, send one [`ClientRequest::Status`] and read its
/// reply — proving a handler task now genuinely exists for this connection
/// (accept() has returned and `serve_requests` has spawned it into its
/// `JoinSet`, not just that the TCP handshake completed) — then leave the
/// socket open and idle so the handler parks mid-`read_frame` on the next
/// request.
async fn prime_and_park(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr)
        .await
        .unwrap_or_else(|e| panic!("connect to {addr} failed: {e}"));
    write_frame(&mut stream, &ClientRequest::Status)
        .await
        .expect("write priming Status request");
    let _reply: ClientResponse = read_frame(&mut stream)
        .await
        .expect("read priming Status reply I/O")
        .expect("priming Status reply (connection closed before responding)");
    stream
}

/// Bounded wait (never a bare sleep) for `stream`'s server-side handler to
/// have gone away: a read either resolves `Ok(0)` (clean EOF — the peer
/// closed its side, i.e. the handler's owned socket half was dropped) or an
/// `Err` (reset/broken-pipe-class — same conclusion). Panics naming
/// `label` if neither happens within [`HANDLER_GONE_TIMEOUT`] — on
/// unfixed code this is exactly what happens, since nothing ever aborts
/// the leaked handler.
async fn assert_handler_goes_away(mut stream: TcpStream, label: &str) {
    let mut buf = [0u8; 1];
    match tokio::time::timeout(HANDLER_GONE_TIMEOUT, stream.read(&mut buf)).await {
        Ok(Ok(0)) => {} // Clean EOF: the server-side handler is gone.
        Ok(Ok(n)) => panic!(
            "{label}: unexpectedly read {n} byte(s) from a connection nothing server-side \
             should have written to"
        ),
        Ok(Err(_)) => {} // Reset/broken-pipe-class: the handler is gone.
        Err(_) => panic!(
            "{label}: server-side handler was still alive {HANDLER_GONE_TIMEOUT:?} after \
             shutdown_and_wait() returned (its connection never reached EOF/reset) — on \
             unfixed code this is the untracked fire-and-forget handler outliving its node \
             (issue #1010)"
        ),
    }
}

/// `Node::shutdown_and_wait` tears down a live handler on **both**
/// listeners `serve_requests` serves — the client port and the intra port
/// (ADR 0047) — not just the accept loop itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_and_wait_aborts_live_connection_handlers() {
    let dir = support::panic_safe_tempdir();
    let (node, _config) = support::start_single_node(dir.path(), StorageBackend::Memory).await;
    // A hard `abort()` on a handler task must never be counted as a
    // spawned-task panic (issue #939's own "cancelled != panicked"
    // invariant) — kept alive across the whole test, checked at drop.
    let _guard = support::watch_task_panics(&[&node]);

    let client_stream = prime_and_park(node.client_addr()).await;
    let intra_stream = prime_and_park(node.intra_addr()).await;

    node.shutdown_and_wait().await;

    assert_handler_goes_away(client_stream, "client-listener handler").await;
    assert_handler_goes_away(intra_stream, "intra-listener handler").await;
}
