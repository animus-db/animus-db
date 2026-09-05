//! The client protocol's frame-length cap (`MAX_FRAME_LEN`): the `u32` length
//! prefix is untrusted input on the client + cross-node relay ports, so an
//! oversized declaration must be rejected with a clean error — never a
//! multi-gigabyte allocation. Uses a plain loopback socket pair (`read_frame` /
//! `write_frame` are the exact functions the node's listeners run).

use animusd::{MAX_FRAME_LEN, read_frame, write_frame};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

/// A loopback (client, server) stream pair.
async fn socket_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
    let (server, _) = accepted.expect("accept");
    (client.expect("connect"), server)
}

/// An attacker-controlled length prefix over the cap is rejected as
/// `InvalidData` before any body bytes arrive (and before any allocation) —
/// the connection errors out cleanly instead of the server buffering up to
/// 4 GiB.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_frame_rejects_oversized_length_prefix() {
    let (mut client, mut server) = socket_pair().await;

    // Declare a frame just over the cap; send only a few bytes of "body" (the
    // reject must not wait for — or try to allocate — the declared length).
    let declared = (MAX_FRAME_LEN as u32) + 1;
    client
        .write_all(&declared.to_be_bytes())
        .await
        .expect("write length prefix");
    client.write_all(b"junk").await.expect("write junk");
    client.flush().await.expect("flush");

    let err = read_frame::<Value, _>(&mut server)
        .await
        .expect_err("an over-cap frame must be an error, not a read");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "got: {err}");
    assert!(
        err.to_string().contains("MAX_FRAME_LEN"),
        "error should name the cap: {err}"
    );
}

/// Sanity: an ordinary frame still round-trips under the cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_frame_round_trips_a_normal_frame() {
    let (mut client, mut server) = socket_pair().await;
    let msg = json!({"op": "put", "key": "k", "value": "v"});
    write_frame(&mut client, &msg).await.expect("write frame");
    let got = read_frame::<Value, _>(&mut server)
        .await
        .expect("read frame")
        .expect("not EOF");
    assert_eq!(got, msg);
}

/// The sender-side guard: `write_frame` refuses to emit a frame over the cap
/// (the receiver would reject it anyway — failing at the sender names the
/// culprit instead of a mysterious peer hang-up).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_frame_rejects_an_over_cap_message() {
    let (mut client, _server) = socket_pair().await;
    // A 65 MiB string serializes past the 64 MiB cap (one JSON byte per char).
    let oversized = json!({ "value": "x".repeat(65 << 20) });
    let err = write_frame(&mut client, &oversized)
        .await
        .expect_err("an over-cap message must be refused at the sender");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "got: {err}");
}
