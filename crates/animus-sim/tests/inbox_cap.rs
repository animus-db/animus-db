//! ADR 0026's 2026-09-28 inbox-cap amendment: a per-stream byte/frame cap on
//! `SimEnv`'s inbox, drop-oldest on overflow — the `SimEnv` mirror of
//! `animus-env`'s `ProdEnv::prod::tests::
//! inbox_cap_drops_oldest_frames_past_the_cap_and_counts_them`. This is the
//! fix for the "consumer never started polling at all" leak
//! `Network::close_stream` cannot reach on its own (nothing ever calls
//! `close_stream` for a stream this node never locally recognized as its
//! own to host) — see `docs/adr/0026-multiplexed-node-stream-addressing.md`'s
//! "Per-stream inbox cap" amendment.

use std::time::Duration;

use animus_env::{EnvExt, InboxCap, Network, nid};
use animus_sim::{NetConfig, Simulator};

/// A `NetConfig` with zero jitter, so `run_for`'s draining of every
/// already-spawned send task produces deliveries in the exact same order
/// the frames were sent — the default `NetConfig` draws up to 4ms of
/// uniform jitter per message (`NetConfig::default`), which reorders
/// delivery relative to send order and would make a "drop-oldest retains
/// the newest, in send order" assertion flaky by construction, not a real
/// bug in the cap itself.
fn deterministic_order_net_config() -> NetConfig {
    let mut cfg = NetConfig::default();
    cfg.max_jitter = Duration::ZERO;
    cfg
}

/// Send `N` frames (well past a tiny configured frame cap) to a stream
/// nobody polls: the queue never grows past the cap, the newest frames are
/// retained (drop-oldest), and every eviction is traced with reason
/// `"inbox-overflow"`.
#[test]
fn frame_cap_drops_oldest_and_retains_newest() {
    let mut sim = Simulator::new(0x1AB0_0001);
    const STREAM: u64 = 7;
    const CAP_FRAMES: usize = 5;
    const N: u8 = 20;

    sim.set_net_config(deterministic_order_net_config());
    sim.set_inbox_cap(InboxCap {
        max_bytes: usize::MAX,
        max_frames: CAP_FRAMES,
    });
    assert_eq!(sim.inbox_cap().max_frames, CAP_FRAMES);

    let sender = sim.env(nid(1));
    for i in 0..N {
        let sender = sender.clone();
        sim.env(nid(1)).clone().spawn_task(async move {
            sender.send_stream(nid(0), STREAM, vec![i]).await;
        });
    }
    sim.run_for(Duration::from_millis(200));

    assert_eq!(
        sim.inbox_len(nid(0), STREAM),
        CAP_FRAMES,
        "the queue must never grow past the configured frame cap"
    );

    // Drop-oldest: the surviving frames must be exactly the LAST
    // CAP_FRAMES values sent, in order — nothing from the first
    // N - CAP_FRAMES sends should remain.
    let target = sim.env(nid(0));
    for expected in (N - CAP_FRAMES as u8)..N {
        let env = futures::executor::block_on(target.recv_stream(STREAM));
        assert_eq!(
            env.payload,
            vec![expected],
            "drop-oldest must retain the newest frames, in send order"
        );
    }
    assert_eq!(sim.inbox_len(nid(0), STREAM), 0);

    let trace = sim.trace_lines();
    let overflow_drops = trace
        .iter()
        .filter(|l| l.contains("inbox-overflow"))
        .count();
    assert_eq!(
        overflow_drops,
        (N as usize) - CAP_FRAMES,
        "exactly the overflowed frames must be traced as inbox-overflow drops: {trace:?}"
    );
}

/// The byte cap enforces independently of the frame cap: a handful of large
/// frames trips the byte cap long before the (generously sized) frame cap
/// would.
#[test]
fn byte_cap_drops_oldest_independently_of_frame_cap() {
    let mut sim = Simulator::new(0x1AB0_0002);
    const STREAM: u64 = 8;
    const VALUE_LEN: usize = 100;
    const CAP_BYTES: usize = 250; // room for 2 frames, not 3
    const N: usize = 6;

    sim.set_net_config(deterministic_order_net_config());
    sim.set_inbox_cap(InboxCap {
        max_bytes: CAP_BYTES,
        max_frames: usize::MAX,
    });

    let sender = sim.env(nid(1));
    for i in 0..N as u8 {
        let sender = sender.clone();
        sim.env(nid(1)).clone().spawn_task(async move {
            sender.send_stream(nid(0), STREAM, vec![i; VALUE_LEN]).await;
        });
    }
    sim.run_for(Duration::from_millis(200));

    // At most 2 frames of 100 bytes fit under a 250-byte cap.
    assert_eq!(sim.inbox_len(nid(0), STREAM), 2);

    let target = sim.env(nid(0));
    for expected in (N as u8 - 2)..N as u8 {
        let env = futures::executor::block_on(target.recv_stream(STREAM));
        assert_eq!(env.payload, vec![expected; VALUE_LEN]);
    }
}

/// `close_stream` composes with the cap: closing drops everything (capped
/// or not) and further sends while closed are discarded exactly like the
/// un-capped case (`tests/stream_close.rs`), not merely capped.
#[test]
fn close_stream_still_drops_everything_even_when_capped() {
    let mut sim = Simulator::new(0x1AB0_0003);
    const STREAM: u64 = 9;
    const CAP_FRAMES: usize = 3;

    sim.set_inbox_cap(InboxCap {
        max_bytes: usize::MAX,
        max_frames: CAP_FRAMES,
    });

    let sender = sim.env(nid(1));
    for i in 0..10u8 {
        let sender = sender.clone();
        sim.env(nid(1)).clone().spawn_task(async move {
            sender.send_stream(nid(0), STREAM, vec![i]).await;
        });
    }
    sim.run_for(Duration::from_millis(200));
    assert_eq!(sim.inbox_len(nid(0), STREAM), CAP_FRAMES);

    sim.env(nid(0)).close_stream(STREAM);
    assert_eq!(
        sim.inbox_len(nid(0), STREAM),
        0,
        "close_stream must drop every queued frame regardless of the cap"
    );

    // Further sends while closed stay at zero (dropped as "stream-closed",
    // never re-admitted up to the cap).
    let sender2 = sim.env(nid(1));
    for i in 0..10u8 {
        let sender2 = sender2.clone();
        sim.env(nid(1)).clone().spawn_task(async move {
            sender2.send_stream(nid(0), STREAM, vec![i]).await;
        });
    }
    sim.run_for(Duration::from_millis(200));
    assert_eq!(sim.inbox_len(nid(0), STREAM), 0);
}

/// The default cap is high enough that no existing scenario at default
/// scale ever trips it (a determinism/no-regression sanity check, not just
/// an assertion about the constant): a modest burst of small frames stays
/// entirely below both defaults and produces no overflow drop at all.
#[test]
fn default_cap_does_not_disturb_ordinary_traffic() {
    let mut sim = Simulator::new(0x1AB0_0004);
    const STREAM: u64 = 10;

    assert_eq!(sim.inbox_cap(), InboxCap::default());

    let sender = sim.env(nid(1));
    for i in 0..200u32 {
        let sender = sender.clone();
        sim.env(nid(1)).clone().spawn_task(async move {
            sender
                .send_stream(nid(0), STREAM, i.to_le_bytes().to_vec())
                .await;
        });
    }
    sim.run_for(Duration::from_millis(500));

    assert_eq!(
        sim.inbox_len(nid(0), STREAM),
        200,
        "well under the default cap — nothing should have been evicted"
    );
    let trace = sim.trace_lines();
    assert!(
        !trace.iter().any(|l| l.contains("inbox-overflow")),
        "no overflow drop should occur at this scale under the default cap"
    );
}
