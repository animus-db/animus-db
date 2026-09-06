//! The per-node **heartbeat batcher** (ADR 0044 phase 2, C-02 PR 2): one
//! instance per physical node, shared by every [`crate::RaftKvNode`] this
//! node hosts, that coalesces every co-hosted group's own bare (no-entries)
//! Raft heartbeat toward the same destination node into **one** physical
//! wire frame per destination per [`RaftCore::heartbeat_interval`]
//! (`animus_control::raft`) tick, instead of one frame per group per tick.
//! See `docs/design/heartbeat-send-sites.md` for the full cost model and
//! design rationale this implements, and this crate's own `CLAUDE.md` for
//! the as-built summary.
//!
//! **Off by default** (ADR 0044 phase 2's own "behind a flag" scope) — a
//! [`RaftKvNode`](crate::RaftKvNode) with no [`HeartbeatBatcher`] registered
//! behaves byte-for-byte as before this module existed: every bare heartbeat
//! still ships via `env.send_stream` on its own group's stream, one call per
//! group per tick.
//!
//! # Design, as built
//!
//! **Sender side.** A [`RaftKvNode`](crate::RaftKvNode)'s own consensus loop
//! (`crate::drive`), on reaching the ordinary `RaftCore::tick`-driven
//! heartbeat branch, filters `tick`'s output: an outbound `RaftMsg::
//! AppendEntries` with `entries.is_empty()` (a bare heartbeat — never one
//! carrying real replication data, and never `TimeoutNow`/`Quiesce`/other
//! tick output, which always ships exactly as before) is handed to
//! [`HeartbeatBatcher::register`] instead of the ordinary
//! `env.send_stream` pipeline. **Only `tick`'s own heartbeat branch is
//! filtered this way** — a bare `AppendEntries` produced by
//! `replicate_now`'s wake-on-propose path (a peer already caught up at the
//! moment of a local propose) ships immediately, unbatched, exactly as
//! before, so this change never touches ADR 0017's single-write-latency
//! path. This node's own **one** per-node flush task (spawned once by
//! [`HeartbeatBatcher::new`], never once per group) wakes every
//! [`heartbeat_interval`](DEFAULT_HEARTBEAT_BATCH_INTERVAL) and, for every
//! destination with a nonempty buffer, sends **one** [`crate::KvWire::
//! HeartbeatBatch`] frame on the reserved [`HEARTBEAT_BATCH_STREAM`]
//! carrying every buffered group's own already-built `RaftMsg::
//! AppendEntries` verbatim (no field re-derivation — the message the
//! `tick()` call originally built is forwarded unchanged, tagged with the
//! stream id of the group it came from). A destination with nothing
//! buffered gets no frame that tick.
//!
//! **Receiver side.** This node's own **one** per-node demux task (also
//! spawned once by [`HeartbeatBatcher::new`]) is the sole consumer of
//! [`HEARTBEAT_BATCH_STREAM`] (ADR 0026: `(node, stream)` is
//! single-consumer). For each `(stream, msg)` entry in a received frame, it
//! looks up `stream` in this node's own hosted-group registry (populated by
//! every [`RaftKvNode`](crate::RaftKvNode) that starts with a batcher
//! attached, via [`HeartbeatBatcher::register_hosted`]/
//! [`HeartbeatBatcher::unregister_hosted`]) and, if hosted, pushes `msg`
//! into that group's own [`HeartbeatInbox`] — a plain, executor-agnostic
//! `AtomicWaker`-backed queue, the identical shape this crate's own
//! `ProposeSignal`/`WakeSignal`/`ApplySignal` already use, except this one
//! carries a payload rather than a bare flag. An unknown/not-currently-
//! hosted `stream` is dropped with [`Metric::CpHeartbeatDemuxDropped`] —
//! never a panic (a batch frame in flight when a group is released or not
//! yet hosted is an ordinary, harmless race).
//!
//! The hosted group's own `drive` loop polls its `HeartbeatInbox` as one
//! more `select` arm (alongside propose/wake/recv/timer). On a delivery it
//! runs the message through **exactly** the same code path an ordinary
//! wire-arrived `RaftMsg::Raft` message already takes — `witness_append_
//! entries`, then `core.handle(from, msg, now, entropy)` under the same
//! lock, then the same durability-gate/send pipeline for whatever `Out`s
//! that produces. **`from` is taken from the message's own embedded
//! `leader: NodeId` field, never the physical wire envelope of the batch
//! frame** — `RaftCore::handle`'s `AppendEntries` dispatch arm (`animus-
//! control/src/raft.rs`) reads only `msg.leader` for routing/becoming-
//! follower/addressing its response, never the `from` parameter, so this is
//! not a workaround but simply the correct value to pass. This is what
//! makes the receiver side provably preserve every per-group invariant in
//! `docs/design/heartbeat-send-sites.md` §3 (election-timer reset, term,
//! `leader_commit`-driven commit-index advance, ReadIndex confirmation is
//! untouched since it never rode this path to begin with) with **zero**
//! duplicated logic: the demux is a redelivery of the identical message
//! through the identical handler, not a second implementation of what
//! `handle_append_entries` already does.
//!
//! **Decision: responses are not batched (an explicit open question this
//! PR closes).** The `AppendEntriesResp` `core.handle` produces for a
//! demuxed heartbeat ships back on the responding group's own stream,
//! individually, exactly like any other response — never re-aggregated
//! into a return batch frame. Two reasons: first, the demux is a serial,
//! on-arrival dispatch (not a deadline-driven loop like the send side), so
//! there is no natural aggregation point without adding a second, purely
//! response-direction buffering-and-timer layer for traffic that is much
//! rarer and much smaller (a bare ack carries no log entries); second, and
//! more load-bearing, an `AppendEntriesResp` is exactly the traffic class
//! `animus_control::persist_round::ships_before_durable` gates on a
//! durability round — batching it would need to either reproduce that
//! gating outside the drive loop or defer the batch until every
//! constituent response's own round lands, materially complicating this
//! PR for a direction that was never the measured cost (§4 of the design
//! doc measures the request direction only). PR 3's cutover can revisit
//! this if the response-direction request-rate is ever independently
//! shown to matter.
//!
//! **Reserved stream.** [`HEARTBEAT_BATCH_STREAM`] is the fourth reserved,
//! well-known stream constant in this crate — see its own doc for why
//! `u64::MAX - 2`, distinct from [`crate::cluster_segment_store::
//! SEGMENT_STREAM`] (`u64::MAX`) and [`crate::backup::
//! BACKUP_SEGMENT_STREAM`] (`u64::MAX - 1`).

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use animus_control::raft::RaftMsg;
use animus_env::{Env, EnvExt, Metric, MetricsHandle, NodeId};
use futures::task::AtomicWaker;

use crate::{KvCommand, KvWire, codec};

/// The heartbeat batcher's own reserved `(node, stream)` address (ADR 0026,
/// ADR 0044 phase 2) — the fourth reserved constant in this crate, chosen at
/// the far end of the `u64` space like its three siblings, and
/// **deliberately distinct from every one of them**: `(node, stream)` is
/// single-consumer (ADR 0026), so two independent serving tasks bound to
/// the same stream on the same node would race for the same inbox and
/// silently steal each other's frames — exactly the incident
/// `SEGMENT_STREAM`'s own doc records for the streams-vs-backup pair. A
/// `TabletId` (`animus_tablet::TabletId`, `u64`, monotonic from 1, never
/// reused — see `Metadata::next_tablet_id`) can never plausibly reach this
/// range, so it can never collide with any real group's own `stream =
/// tablet_id` (ADR 0026) address either.
///
/// [`SEGMENT_STREAM`]: crate::cluster_segment_store::SEGMENT_STREAM
/// [`BACKUP_SEGMENT_STREAM`]: crate::backup::BACKUP_SEGMENT_STREAM
pub const HEARTBEAT_BATCH_STREAM: u64 = u64::MAX - 2;

/// The batcher's own flush cadence — must match `RaftCore::
/// heartbeat_interval`'s default (`crates/animus-control/src/raft.rs`,
/// `Duration::from_millis(50)`), since nothing in this codebase exposes a
/// setter for that field (grep confirms `heartbeat_interval` is written
/// exactly once, at `RaftCore::new`'s own default construction) — every
/// group's own tick cadence is this value, unconditionally, so a batcher
/// flushing on the same cadence never adds a systematic extra delay beyond
/// one tick's own jitter.
pub const DEFAULT_HEARTBEAT_BATCH_INTERVAL: Duration = Duration::from_millis(50);

/// One co-hosted group's own bare heartbeat, tagged with the stream id
/// (this node's own tablet id for that group) it came from — the element
/// type of a batched frame's payload (`KvWire::HeartbeatBatch`) and of the
/// send-side per-destination buffer. Named to match `docs/design/
/// heartbeat-send-sites.md` §5's own design-sketch naming.
pub(crate) type GroupHeartbeat = (u64, RaftMsg<KvCommand>);

/// A demuxed heartbeat awaiting delivery to one hosted group's own
/// consensus loop — the [`HeartbeatBatcher`]'s per-group delivery queue.
/// Same executor-agnostic `AtomicWaker` shape as this crate's
/// `ProposeSignal`/`WakeSignal`/`ApplySignal` (`lib.rs`), except this one
/// carries a payload instead of a bare flag: `poll` registers the waker
/// *then* checks the queue, so a [`push`](Self::push) racing a park can
/// never be lost regardless of ordering, and multiple pending entries (a
/// slow-to-poll group falling behind the flush cadence) queue rather than
/// overwrite.
#[derive(Default)]
pub(crate) struct HeartbeatInbox {
    queue: Mutex<VecDeque<RaftMsg<KvCommand>>>,
    waker: AtomicWaker,
}

impl HeartbeatInbox {
    fn push(&self, msg: RaftMsg<KvCommand>) {
        self.queue
            .lock()
            .expect("heartbeat inbox poisoned")
            .push_back(msg);
        self.waker.wake();
    }
}

/// A future that resolves with the next demuxed heartbeat for one hosted
/// group, for that group's own `drive` loop `select` — the [`HeartbeatInbox`]
/// counterpart to `crate::ProposePending`/`WakePending`.
pub(crate) struct HeartbeatPending<'a> {
    inbox: &'a HeartbeatInbox,
}

impl<'a> HeartbeatPending<'a> {
    pub(crate) fn new(inbox: &'a HeartbeatInbox) -> Self {
        Self { inbox }
    }
}

impl Future for HeartbeatPending<'_> {
    type Output = RaftMsg<KvCommand>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<RaftMsg<KvCommand>> {
        self.inbox.waker.register(cx.waker());
        match self
            .inbox
            .queue
            .lock()
            .expect("heartbeat inbox poisoned")
            .pop_front()
        {
            Some(msg) => Poll::Ready(msg),
            None => Poll::Pending,
        }
    }
}

/// Shared state behind [`HeartbeatBatcher`]'s cheap `Arc` clone.
struct BatcherInner<E: Env> {
    env: E,
    interval: Duration,
    metrics: MetricsHandle,
    /// Send-side: per-destination-node buffer of `(this node's own group
    /// stream id, that group's own already-built bare-heartbeat
    /// `AppendEntries`)` pairs awaiting the next flush. A `BTreeMap` (ADR
    /// 0003 — no `HashMap` in logic): flush order across destinations is
    /// deterministic, though it carries no correctness weight of its own
    /// (each destination's own frame is independent).
    pending: Mutex<BTreeMap<NodeId, Vec<GroupHeartbeat>>>,
    /// Receive-side: every group this node currently hosts with batching
    /// enabled, by stream id — the demux task's own lookup table. Owned
    /// here, inside `animus-cp-data`, rather than reaching into `animusd`'s
    /// `ClusterEdgeState` registry or `host::Reconciler`'s own `hosted` map
    /// (per the design doc's own open question — this keeps the demux
    /// working under a bare `SimEnv` test with no `animusd` in the loop at
    /// all).
    hosted: Mutex<BTreeMap<u64, Arc<HeartbeatInbox>>>,
}

/// A per-node heartbeat batcher (ADR 0044 phase 2) — construct **one** per
/// physical node (typically once, at node start, alongside this node's
/// [`host::Reconciler`](crate::host::Reconciler)) and pass a clone to every
/// [`RaftKvNode`](crate::RaftKvNode) this node hosts via
/// [`RaftKvNode::start_hosted_with_batcher`](crate::RaftKvNode::start_hosted_with_batcher)/
/// [`RaftKvNode::start_hosted_campaigning_with_batcher`](crate::RaftKvNode::start_hosted_campaigning_with_batcher)
/// — or, in production, via [`host::Reconciler::enable_heartbeat_batching`](crate::host::Reconciler::enable_heartbeat_batching),
/// which mints and threads one through every group it hosts from then on,
/// mirroring [`enable_quiescence`](crate::host::Reconciler::enable_quiescence)'s
/// own shape. Cloning is a cheap `Arc` bump; every clone shares the same
/// send buffer, hosted registry, and the two background tasks
/// [`new`](Self::new) spawns exactly once.
pub struct HeartbeatBatcher<E: Env> {
    inner: Arc<BatcherInner<E>>,
}

impl<E: Env> Clone for HeartbeatBatcher<E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<E: Env> HeartbeatBatcher<E> {
    /// Construct a fresh batcher for this node and spawn its two background
    /// tasks (the flush loop and the demux loop) once, via
    /// [`EnvExt::spawn_task`] — never once per hosted group. `metrics` is
    /// the sink [`Metric::CpHeartbeatFramesSent`]/[`Metric::
    /// CpHeartbeatDemuxDropped`] record into; production callers pass
    /// `env.metrics()` (a no-op under `SimEnv`, a real sink under
    /// `ProdEnv`), mirroring [`RaftKvNode::start`](crate::RaftKvNode::start)'s
    /// own default — a sim test that wants to observe these counters
    /// passes a recording handle instead, the same
    /// [`start_with_metrics`](crate::RaftKvNode::start_with_metrics)
    /// convention.
    #[must_use]
    pub fn new(env: E, interval: Duration, metrics: MetricsHandle) -> Self {
        let inner = Arc::new(BatcherInner {
            env: env.clone(),
            interval,
            metrics,
            pending: Mutex::new(BTreeMap::new()),
            hosted: Mutex::new(BTreeMap::new()),
        });
        env.spawn_task(flush_loop(Arc::clone(&inner)));
        env.spawn_task(demux_loop(Arc::clone(&inner)));
        Self { inner }
    }

    /// Register `stream` (a bare, no-entries `RaftMsg::AppendEntries`
    /// produced by `RaftCore::tick`'s heartbeat branch) into the buffer for
    /// `to`, to ship in this node's next flush. Also records
    /// [`Metric::CpAppendEntriesSent`] right here — the design doc's own
    /// recommendation is that this counter keep meaning "one per logical
    /// per-group heartbeat," and a batched heartbeat never reaches the
    /// ordinary `record_kv_outbound` call site (it never enters that `outs`
    /// list at all), so it must be counted at the point it is actually
    /// handed off instead.
    ///
    /// **Caller contract**: `msg` must be a bare `RaftMsg::AppendEntries`
    /// (`entries.is_empty()`) — the one shape `tick()`'s heartbeat branch
    /// produces. Anything else (real replication data, `TimeoutNow`, a
    /// quiesce/wake message) must ship through the ordinary immediate
    /// pipeline instead; this type has no way to enforce that at the call
    /// site beyond this doc, since `RaftMsg` is shared with the control
    /// plane and not worth narrowing here.
    pub(crate) fn register(&self, to: NodeId, stream: u64, msg: RaftMsg<KvCommand>) {
        self.inner
            .pending
            .lock()
            .expect("heartbeat batcher pending poisoned")
            .entry(to)
            .or_default()
            .push((stream, msg));
        self.inner.metrics.incr(Metric::CpAppendEntriesSent);
    }

    /// Register `stream` as hosted on this node with batching enabled,
    /// returning the [`HeartbeatInbox`] handle this group's own `drive`
    /// loop polls as an extra `select` arm. Call once, at group start
    /// (mirrors `wal_file`/every other per-group one-time setup `drive`
    /// already does); pair with [`unregister_hosted`](Self::unregister_hosted)
    /// at group teardown.
    pub(crate) fn register_hosted(&self, stream: u64) -> Arc<HeartbeatInbox> {
        let inbox = Arc::new(HeartbeatInbox::default());
        self.inner
            .hosted
            .lock()
            .expect("heartbeat batcher hosted poisoned")
            .insert(stream, Arc::clone(&inbox));
        inbox
    }

    /// Remove `stream` from this node's hosted registry — call once, at
    /// group teardown (this crate's own `drive`'s `halted` exit branch), so
    /// a batch frame that arrives for an already-released group is dropped
    /// (counted, never delivered into a stale/dangling inbox nobody polls
    /// any more) instead of silently accumulating in a queue nothing will
    /// ever drain.
    pub(crate) fn unregister_hosted(&self, stream: u64) {
        self.inner
            .hosted
            .lock()
            .expect("heartbeat batcher hosted poisoned")
            .remove(&stream);
    }
}

/// The batcher's own per-node flush task (spawned once by
/// [`HeartbeatBatcher::new`]): wakes every `interval` and sends one
/// [`KvWire::HeartbeatBatch`] frame per destination with a nonempty buffer.
/// A destination with nothing buffered that tick gets no frame — the design
/// doc's own "a destination with no buffered heartbeats gets no frame"
/// requirement. Runs for the life of the owning `Env`; nothing spawns a
/// second copy (one `HeartbeatBatcher` per node, shared, never
/// reconstructed per group), and `ProdEnv::shutdown()`/`Env` teardown aborts
/// it along with every other task the env owns — see `animus-env/CLAUDE.md`'s
/// "`ProdEnv::shutdown()` aborts every task the env owns" entry.
async fn flush_loop<E: Env>(inner: Arc<BatcherInner<E>>) {
    loop {
        inner.env.sleep(inner.interval).await;
        let batch: Vec<(NodeId, Vec<GroupHeartbeat>)> = {
            let mut pending = inner
                .pending
                .lock()
                .expect("heartbeat batcher pending poisoned");
            std::mem::take(&mut *pending).into_iter().collect()
        };
        for (to, entries) in batch {
            if entries.is_empty() {
                continue;
            }
            inner.metrics.incr(Metric::CpHeartbeatFramesSent);
            let payload = codec::encode_wire(&KvWire::HeartbeatBatch(entries));
            inner
                .env
                .send_stream(to, HEARTBEAT_BATCH_STREAM, payload)
                .await;
        }
    }
}

/// The batcher's own per-node demux task (spawned once by
/// [`HeartbeatBatcher::new`]) — the **sole** consumer of
/// [`HEARTBEAT_BATCH_STREAM`] on this node (ADR 0026: `(node, stream)` is
/// single-consumer). Decodes each received frame and delivers every entry
/// naming a currently-hosted group into that group's own [`HeartbeatInbox`];
/// an entry naming an unknown/not-hosted stream is dropped and counted
/// ([`Metric::CpHeartbeatDemuxDropped`]), never delivered and never a panic
/// — an ordinary, harmless race against a group being released or not yet
/// hosted. A malformed frame (a corrupt payload, or a decoded [`KvWire`]
/// variant other than [`KvWire::HeartbeatBatch`] arriving on this reserved
/// stream — which nothing in this codebase sends, but a hostile/buggy peer
/// could) is logged and dropped whole, the same discipline the ordinary
/// per-group receive path already applies to an undecodable message.
async fn demux_loop<E: Env>(inner: Arc<BatcherInner<E>>) {
    loop {
        let envelope = inner.env.recv_stream(HEARTBEAT_BATCH_STREAM).await;
        match codec::decode_wire(&envelope.payload) {
            Ok(KvWire::HeartbeatBatch(entries)) => {
                for (stream, msg) in entries {
                    let inbox = inner
                        .hosted
                        .lock()
                        .expect("heartbeat batcher hosted poisoned")
                        .get(&stream)
                        .cloned();
                    match inbox {
                        Some(inbox) => inbox.push(msg),
                        None => inner.metrics.incr(Metric::CpHeartbeatDemuxDropped),
                    }
                }
            }
            Ok(_) => {
                tracing::warn!(
                    "unexpected KvWire variant on the reserved heartbeat-batch stream, dropped"
                );
            }
            Err(err) => {
                tracing::warn!(?err, "undecodable heartbeat batch frame dropped");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use animus_control::raft::RaftMsg;
    use animus_env::{Metric, MetricsHandle, nid};
    use animus_sim::{SimEnv, Simulator};

    use super::*;

    fn heartbeat(term: u64, leader: NodeId) -> RaftMsg<KvCommand> {
        RaftMsg::AppendEntries {
            term,
            leader,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        }
    }

    /// One frame carries every co-hosted group's own heartbeat to a shared
    /// destination, and an unknown group id in a received frame is dropped
    /// and counted rather than delivered or panicking — the two structural
    /// properties this module's own doc promises, proven directly against
    /// the batcher's public surface with no `RaftKvNode` involved.
    #[test]
    fn frames_multiplex_groups_and_drop_unknown_ones() {
        let mut sim = Simulator::new(0xB47C_0001);
        let env_a: SimEnv = sim.env(nid(0));
        let env_b: SimEnv = sim.env(nid(1));
        let metrics_a = MetricsHandle::recording();
        let metrics_b = MetricsHandle::recording();
        let batcher_a = HeartbeatBatcher::new(
            env_a.clone(),
            DEFAULT_HEARTBEAT_BATCH_INTERVAL,
            metrics_a.clone(),
        );
        let batcher_b = HeartbeatBatcher::new(
            env_b.clone(),
            DEFAULT_HEARTBEAT_BATCH_INTERVAL,
            metrics_b.clone(),
        );
        // Node B hosts group 7 only — group 9 is a stranger to it.
        let inbox7 = batcher_b.register_hosted(7);

        batcher_a.register(nid(1), 7, heartbeat(3, nid(0)));
        batcher_a.register(nid(1), 9, heartbeat(3, nid(0)));

        sim.run_for(DEFAULT_HEARTBEAT_BATCH_INTERVAL * 3);

        // Exactly one physical frame carried both logical heartbeats.
        assert_eq!(metrics_a.get(Metric::CpHeartbeatFramesSent), 1);
        assert_eq!(metrics_a.get(Metric::CpAppendEntriesSent), 2);
        // Group 7's own inbox got its message; group 9's was dropped+counted.
        assert!(
            inbox7
                .queue
                .lock()
                .expect("heartbeat inbox poisoned")
                .pop_front()
                .is_some()
        );
        assert_eq!(metrics_b.get(Metric::CpHeartbeatDemuxDropped), 1);
    }

    /// A destination with nothing buffered gets no frame — the flush loop
    /// must not send an empty frame just because the timer fired.
    #[test]
    fn an_idle_destination_gets_no_frame() {
        let mut sim = Simulator::new(0xB47C_0002);
        let env_a: SimEnv = sim.env(nid(0));
        let metrics_a = MetricsHandle::recording();
        let _batcher_a = HeartbeatBatcher::new(
            env_a.clone(),
            DEFAULT_HEARTBEAT_BATCH_INTERVAL,
            metrics_a.clone(),
        );
        sim.run_for(DEFAULT_HEARTBEAT_BATCH_INTERVAL * 5);
        assert_eq!(metrics_a.get(Metric::CpHeartbeatFramesSent), 0);
    }
}
