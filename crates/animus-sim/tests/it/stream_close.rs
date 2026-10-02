//! ADR 0026's 2026-09-28 amendment: `Network::close_stream` on `SimEnv` —
//! close/reopen/tombstone semantics, and the load-bearing property this
//! change must NOT disturb: a stream that has never been closed keeps
//! buffering before its first `recv_stream` (the split-fork "deterministic
//! first leader" mechanism, `animus-cp-data/CLAUDE.md`, depends on exactly
//! this).
//!
//! Mirrors `tests/stop_semantics.rs`'s own style and its issue #841
//! node-prefix-scan regressions (this file's crash/stop tombstone-clearing
//! tests are that same family, generalized to the new `closed_streams` set).

use std::time::Duration;

use animus_env::{EnvExt, Network, nid};
use animus_sim::Simulator;

/// Send `N` frames to a stream nobody has polled, close it, and confirm the
/// queue is dropped to zero. A frame arriving after the close must be
/// discarded (never queued) while the stream stays closed.
#[test]
fn close_stream_drops_queued_frames_and_marks_closed() {
    let mut sim = Simulator::new(0xC105_E001);
    let sender = sim.env(nid(1));
    let target = sim.env(nid(0));
    const STREAM: u64 = 7;

    for i in 0..5u8 {
        let sender = sender.clone();
        sim.env(nid(1)).clone().spawn_task(async move {
            sender.send_stream(nid(0), STREAM, vec![i]).await;
        });
    }
    sim.run_for(Duration::from_millis(200));
    assert_eq!(
        sim.inbox_len(nid(0), STREAM),
        5,
        "all 5 frames must be queued before anyone ever calls recv_stream"
    );
    assert!(!sim.stream_is_closed(nid(0), STREAM));

    target.close_stream(STREAM);
    assert_eq!(
        sim.inbox_len(nid(0), STREAM),
        0,
        "close_stream must drop every already-queued frame"
    );
    assert!(sim.stream_is_closed(nid(0), STREAM));

    // A frame sent while closed must be dropped, not queued.
    let sender2 = sender.clone();
    sim.env(nid(1)).clone().spawn_task(async move {
        sender2.send_stream(nid(0), STREAM, vec![99]).await;
    });
    sim.run_for(Duration::from_millis(200));
    assert_eq!(
        sim.inbox_len(nid(0), STREAM),
        0,
        "a frame addressed to a closed stream must be discarded, not queued"
    );
    assert!(sim.stream_is_closed(nid(0), STREAM));

    let trace = sim.trace_lines();
    assert!(
        trace.iter().any(|l| l.contains("stream-closed")),
        "the dropped frame must be traced with reason \"stream-closed\": {trace:?}"
    );
}

/// `recv_stream` reopens a closed stream: the closed mark clears the moment
/// `recv_stream` is called (even before any frame arrives), and a
/// subsequently-sent frame is delivered normally.
#[test]
fn recv_stream_reopens_a_closed_stream() {
    let mut sim = Simulator::new(0xC105_E002);
    let target = sim.env(nid(0));
    const STREAM: u64 = 11;

    target.close_stream(STREAM);
    assert!(sim.stream_is_closed(nid(0), STREAM));

    let received: std::sync::Arc<std::sync::Mutex<Option<u8>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let out = std::sync::Arc::clone(&received);
    let recv_env = target.clone();
    sim.env(nid(0)).clone().spawn_task(async move {
        let env = recv_env.recv_stream(STREAM).await;
        *out.lock().unwrap() = Some(env.payload[0]);
    });
    // Give the spawned task a chance to run its first poll (registers the
    // reopen + parks on the empty queue) before checking the tombstone.
    sim.run_for(Duration::from_millis(1));
    assert!(
        !sim.stream_is_closed(nid(0), STREAM),
        "recv_stream must clear the closed mark up front, before it ever has \
         a frame to hand back"
    );

    let sender = sim.env(nid(1));
    sender.clone().spawn_task(async move {
        sender.send_stream(nid(0), STREAM, vec![42]).await;
    });
    sim.run_for(Duration::from_millis(200));
    assert_eq!(
        *received.lock().unwrap(),
        Some(42),
        "a frame sent after reopen must be delivered normally"
    );
}

/// The load-bearing property this change must not disturb: a stream that
/// has **never** been closed keeps buffering frames sent before its first
/// `recv_stream` — the split-fork "deterministic first leader" mechanism
/// (see `animus-cp-data/CLAUDE.md`) depends on this exact behavior for a
/// freshly-materialized child's very first `PreVote`.
#[test]
fn a_never_closed_stream_still_buffers_before_first_recv() {
    let mut sim = Simulator::new(0xC105_E003);
    const STREAM: u64 = 21;

    let sender = sim.env(nid(1));
    sender.clone().spawn_task(async move {
        sender.send_stream(nid(0), STREAM, vec![7]).await;
    });
    sim.run_for(Duration::from_millis(50));
    assert_eq!(
        sim.inbox_len(nid(0), STREAM),
        1,
        "a never-closed stream must still queue a frame ahead of its first recv"
    );
    assert!(!sim.stream_is_closed(nid(0), STREAM));

    let target = sim.env(nid(0));
    let got = futures::executor::block_on(async move { target.recv_stream(STREAM).await });
    assert_eq!(got.payload, vec![7]);
}

/// Issue #841's node-prefix-scan discipline, generalized to `closed_streams`
/// (ADR 0026, 2026-09-28 amendment): a crashed node's tombstones are
/// cleared, mirroring a fresh `ProdEnv` process starting with none. Also
/// proves a DIFFERENT node's own tombstone is untouched by the crash.
#[test]
fn crash_clears_this_nodes_closed_stream_tombstones_but_not_anothers() {
    let sim = Simulator::new(0xC105_E004);
    const STREAM: u64 = 31;

    sim.env(nid(0)).close_stream(STREAM);
    sim.env(nid(1)).close_stream(STREAM);
    assert!(sim.stream_is_closed(nid(0), STREAM));
    assert!(sim.stream_is_closed(nid(1), STREAM));

    sim.crash(nid(0));
    assert!(
        !sim.stream_is_closed(nid(0), STREAM),
        "a crashed node's volatile stream-closed tombstones must clear, \
         exactly like a fresh ProdEnv process starting with none"
    );
    assert!(
        sim.stream_is_closed(nid(1), STREAM),
        "a different node's own tombstone must be untouched by another \
         node's crash"
    );
}

/// The `stop` twin of the crash test above (mirrors `stop_semantics.rs`'s
/// own crash/stop pairing).
#[test]
fn stop_clears_this_nodes_closed_stream_tombstones_but_not_anothers() {
    let sim = Simulator::new(0xC105_E005);
    const STREAM: u64 = 41;

    sim.env(nid(0)).close_stream(STREAM);
    sim.env(nid(1)).close_stream(STREAM);

    sim.stop(nid(0));
    assert!(
        !sim.stream_is_closed(nid(0), STREAM),
        "a stopped node's volatile stream-closed tombstones must clear, \
         exactly like a fresh process restart"
    );
    assert!(sim.stream_is_closed(nid(1), STREAM));
}

/// A stream never closed at all reports `stream_is_closed == false` and an
/// `inbox_len` of `0` for a node that has no entry whatsoever — the "not yet
/// opened" case must never read as "closed".
#[test]
fn an_untouched_stream_is_neither_closed_nor_queued() {
    let sim = Simulator::new(0xC105_E006);
    assert!(!sim.stream_is_closed(nid(0), 99));
    assert_eq!(sim.inbox_len(nid(0), 99), 0);
}
