//! The `Env`-driven Raft node: a thin driver that owns the environment and
//! ferries time and messages between the network and the synchronous
//! [`RaftCore`].

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Env, EnvExt, Metric, MetricsHandle, NodeId};
use futures::future::{Either, select};

use crate::detector::FailureDetector;
use crate::meta::{Member, MetaCommand, Metadata, NodeStatus};
use crate::persist::PersistedState;
use crate::raft::{Out, ProposeResult, RaftCore, RaftMsg, Role};

/// File name of the per-node Raft write-ahead log on the `Env` disk.
const WAL: &str = "raft.wal";

/// Snapshot (truncating the covered log prefix) and rewrite the WAL once this
/// many applied entries have accumulated beyond the current snapshot base. This
/// bounds both the in-memory log and the WAL to roughly the live tail.
const SNAPSHOT_THRESHOLD: u64 = 64;

/// How often the leader re-evaluates placement and proposes any corrective
/// `CasTabletReplicas` (ADR 0005). Long relative to the heartbeat interval:
/// reconciliation is a slow background activity, not on any request path.
const RECONCILE_INTERVAL: Duration = Duration::from_millis(500);

/// How often a member emits a liveness heartbeat to the control group
/// (ADR 0012). On the order of the Raft heartbeat interval, and short relative to
/// [`DETECT_TIMEOUT`] so a live member is comfortably seen within the window.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

/// How long the leader tolerates silence from a member before marking it `Down`
/// (ADR 0012). Several heartbeat intervals, so a single delayed/dropped heartbeat
/// does not flap a healthy member.
pub const DETECT_TIMEOUT: Duration = Duration::from_millis(500);

/// How often the leader re-evaluates member liveness and proposes any
/// `UpsertMember{status}` transitions (ADR 0012).
const DETECT_INTERVAL: Duration = Duration::from_millis(100);

/// Grace period after this node first observes itself leader for a term, during
/// which it will **not** mark any member `Down` (ADR 0012). The
/// [`FailureDetector`] is per-node volatile state (only the transitions it drives
/// are replicated), so a freshly elected leader starts with a **cold** detector:
/// it has observed no heartbeats yet and would otherwise immediately judge every
/// live member silent and propose a flurry of false `Down`s before the first
/// heartbeat round arrives. Suppressing `Down` proposals for at least one
/// [`DETECT_TIMEOUT`] worth of time after gaining leadership gives heartbeats
/// time to repopulate the detector first. Recoveries (`Down`→`Active`) are *not*
/// suppressed — a heartbeat is positive evidence, with no false-positive risk —
/// and the gate is purely `Env`-time based, so it stays deterministic.
const LEADER_GRACE: Duration = DETECT_TIMEOUT;

/// A running control-plane node. Cheap to clone; clones share one [`RaftCore`]
/// and one [`FailureDetector`].
#[derive(Clone)]
pub struct RaftNode<E: Env> {
    env: E,
    core: Arc<Mutex<RaftCore>>,
    /// Shared heartbeat failure detector (ADR 0012). The driver feeds it observed
    /// heartbeats; the `detect_loop` reads it and, when leader, proposes liveness
    /// transitions. Shared so both run against one view.
    detector: Arc<Mutex<FailureDetector>>,
    /// Observability sink (ADR 0015). The driver loops record control-plane
    /// counters into it (elections, append-entries, snapshot installs, failure
    /// detector transitions) and keep the leadership gauge current. Cheap to
    /// clone; a clone is moved into each spawned loop.
    metrics: MetricsHandle,
}

impl<E: Env> RaftNode<E> {
    /// Start a node: build its [`RaftCore`] and spawn the driver loop on `env`.
    /// `all_nodes` is the full control-group membership (including this node).
    ///
    /// Metrics (ADR 0015) are recorded into the env's own sink (`env.metrics()`)
    /// — for `ProdEnv` a real recording handle, so an assembled production node
    /// accumulates control-plane counters with no extra wiring. To observe the
    /// counters under deterministic simulation (where `SimEnv::metrics()` is the
    /// no-op default), construct with [`start_with_metrics`](Self::start_with_metrics)
    /// and pass a recording [`MetricsHandle`] the test keeps.
    pub fn start(env: E, all_nodes: Vec<NodeId>) -> Self {
        let metrics = env.metrics();
        Self::start_with_metrics(env, all_nodes, metrics)
    }

    /// Like [`start`](Self::start), but records into the supplied `metrics`
    /// handle instead of `env.metrics()`. Additive (existing callers use
    /// `start`); the sim observability test threads in a recording handle here so
    /// it can read counters back without editing `animus-sim`, and integration
    /// can pass `env.metrics()` (or any chosen sink) explicitly.
    pub fn start_with_metrics(env: E, all_nodes: Vec<NodeId>, metrics: MetricsHandle) -> Self {
        let core = Arc::new(Mutex::new(RaftCore::new(
            env.node_id(),
            &all_nodes,
            env.now(),
            env.next_u64(),
        )));
        let detector = Arc::new(Mutex::new(FailureDetector::new(DETECT_TIMEOUT)));
        let node = Self {
            env: env.clone(),
            core: Arc::clone(&core),
            detector: Arc::clone(&detector),
            metrics: metrics.clone(),
        };
        env.spawn_task(drive(
            env.clone(),
            Arc::clone(&core),
            Arc::clone(&detector),
            all_nodes,
            metrics.clone(),
        ));
        // The placement reconciler runs alongside the driver; it only ever
        // *proposes* on the core (no I/O of its own), and proposals are honored
        // only when this node is leader — so it is safe to run on every node.
        env.spawn_task(reconcile_loop(env.clone(), Arc::clone(&core)));
        // The failure detector evaluates member liveness on a timer and, when
        // leader, proposes `UpsertMember` transitions (ADR 0012). Like the
        // reconciler it only *proposes*, so it is safe to run on every node.
        env.spawn_task(detect_loop(env.clone(), core, detector, metrics));
        node
    }

    /// This node's metrics handle (ADR 0015). A snapshot of it
    /// (`node.metrics().snapshot()`) is the control-plane observability surface.
    #[must_use]
    pub fn metrics(&self) -> &MetricsHandle {
        &self.metrics
    }

    /// Propose a metadata command. See [`ProposeResult`].
    pub fn propose(&self, command: MetaCommand) -> ProposeResult {
        self.lock().propose(command)
    }

    /// Drain and durably persist (append + `fsync`) any WAL records the core has
    /// buffered but the driver loop has not yet flushed; returns the count.
    ///
    /// `propose` advances commit/apply and returns **synchronously**, while the
    /// driver loop fsyncs the WAL **asynchronously** — and that loop is normally
    /// parked in its `select` between ticks. So there is a window where an applied
    /// (already client-visible, already acked) command is not yet durable on disk.
    /// A graceful teardown calls this **before** stopping the driver so a clean
    /// shutdown does not lose an acked command (see `animusd`'s
    /// `Node::shutdown_graceful`). Because the driver is parked at that point, this
    /// is the sole WAL writer.
    ///
    /// NOTE: this does **not** close the *crash* window — a `kill -9` between apply
    /// and the next flush still loses the entry. Making the commit itself durable
    /// *before* it becomes client-visible is the proper fix, tracked as a follow-up
    /// (ADR 0009 — see the root CLAUDE.md engineering-practices note).
    pub async fn flush(&self) -> usize {
        flush_wal(&self.env, &self.core).await
    }

    /// This node's environment handle.
    pub fn env(&self) -> &E {
        &self.env
    }

    /// Whether this node currently believes it is leader.
    pub fn is_leader(&self) -> bool {
        self.lock().is_leader()
    }

    /// The node's current role.
    pub fn role(&self) -> Role {
        self.lock().role()
    }

    /// The current term.
    pub fn term(&self) -> u64 {
        self.lock().term()
    }

    /// Best-known leader id.
    pub fn leader(&self) -> Option<NodeId> {
        self.lock().leader()
    }

    /// A clone of the applied metadata state.
    pub fn metadata(&self) -> Metadata {
        self.lock().metadata()
    }

    /// The sequence of commands applied so far, in order.
    pub fn applied(&self) -> Vec<MetaCommand> {
        self.lock().applied()
    }

    /// Highest committed log index.
    pub fn commit_index(&self) -> u64 {
        self.lock().commit_index()
    }

    /// The current snapshot base index (0 if no snapshot has been taken). A
    /// follower that caught up via `InstallSnapshot` will have a non-zero value
    /// it never reached by applying alone.
    pub fn snapshot_index(&self) -> u64 {
        self.lock().snapshot_index()
    }

    /// Whether this node's failure detector currently judges `member` alive
    /// (a heartbeat seen within the timeout). Observability for tests; the
    /// authoritative liveness lives in the replicated `Metadata` status, which
    /// the leader drives from this verdict.
    pub fn believes_alive(&self, member: NodeId) -> bool {
        self.detector
            .lock()
            .expect("detector poisoned")
            .is_alive(member, self.env.now())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RaftCore> {
        self.core.lock().expect("raft core poisoned")
    }
}

/// Emit a liveness heartbeat (ADR 0012) from this `env`'s node to every node in
/// `control`, once. A member spawns [`heartbeat_loop`] (which calls this on a
/// timer) so the control-plane leader can detect its failure. Sends are
/// fire-and-forget over the `Env` network; a partitioned/crashed member's
/// heartbeats are simply not delivered, which is exactly what the detector keys
/// off.
pub async fn send_heartbeat<E: Env>(env: &E, control: &[NodeId]) {
    let msg = RaftMsg::Heartbeat {
        node: env.node_id(),
    };
    let bytes = serde_json::to_vec(&msg).expect("heartbeat serializes");
    for &c in control {
        env.send(c, bytes.clone()).await;
    }
}

/// A member's heartbeat loop: every [`HEARTBEAT_INTERVAL`] of `Env` time, send a
/// heartbeat to every control node. Run by a (data-plane) member node so the
/// control plane can detect its liveness; stop it (e.g. `Simulator::stop`) or
/// partition the member to simulate a failure.
pub async fn heartbeat_loop<E: Env>(env: E, control: Vec<NodeId>) {
    loop {
        send_heartbeat(&env, &control).await;
        env.sleep(HEARTBEAT_INTERVAL).await;
    }
}

/// The per-node driver loop: recover durable state, then repeatedly wait for the
/// next message or timer, hand it to the core, persist the resulting durable
/// changes, and ship whatever the core wants sent.
async fn drive<E: Env>(
    env: E,
    core: Arc<Mutex<RaftCore>>,
    detector: Arc<Mutex<FailureDetector>>,
    all_nodes: Vec<NodeId>,
    metrics: MetricsHandle,
) {
    // Recover from the WAL before serving anything.
    let bytes = env.read(WAL).await.unwrap_or_default();
    let state = PersistedState::replay(PersistedState::decode(&bytes));
    if !state.is_empty() {
        let recovered =
            RaftCore::recovered(env.node_id(), &all_nodes, state, env.now(), env.next_u64());
        *core.lock().expect("raft core poisoned") = recovered;
    }

    loop {
        // Persist anything queued out-of-band (e.g. a client `propose`).
        flush_and_maybe_compact(&env, &core).await;

        let now = env.now();
        let deadline = core.lock().expect("raft core poisoned").next_deadline();
        let wait = Duration::from_nanos(deadline.0.saturating_sub(now.0));

        // Snapshot role/term before stepping the core so we can attribute any
        // state transition the step causes to a metric (ADR 0015). All inputs to
        // the metric decisions are `Env`-supplied or core-derived, so recording
        // stays a deterministic function of the run.
        let (before_role, before_term) = {
            let c = core.lock().expect("raft core poisoned");
            (c.role(), c.term())
        };

        let outs = match select(env.recv(), env.sleep(wait)).await {
            Either::Left((envelope, _)) => {
                let entropy = env.next_u64();
                match serde_json::from_slice::<RaftMsg>(&envelope.payload) {
                    // A heartbeat is not consensus traffic (ADR 0012): record it
                    // in the failure detector and don't hand it to the core. The
                    // `now` we observe at is `Env`-supplied, so the recorded
                    // instant is deterministic.
                    Ok(RaftMsg::Heartbeat { node }) => {
                        detector
                            .lock()
                            .expect("detector poisoned")
                            .observe(node, env.now());
                        Vec::new()
                    }
                    Ok(msg) => {
                        // A follower rejecting an `AppendEntries` surfaces as an
                        // outbound `AppendEntriesResp { success: false }`, so the
                        // "rejected" counter is recorded from the core's output
                        // (`record_outbound`) where the rejection is produced —
                        // not from the inbound message.
                        let outs = core.lock().expect("raft core poisoned").handle(
                            envelope.from,
                            msg,
                            env.now(),
                            entropy,
                        );
                        record_outbound(&metrics, &outs);
                        outs
                    }
                    Err(err) => {
                        tracing::warn!(?err, "undecodable raft message dropped");
                        Vec::new()
                    }
                }
            }
            Either::Right(((), _)) => {
                let entropy = env.next_u64();
                let outs = core
                    .lock()
                    .expect("raft core poisoned")
                    .tick(env.now(), entropy);
                record_outbound(&metrics, &outs);
                outs
            }
        };

        // Attribute role/term transitions to election metrics + keep the
        // leadership gauge current.
        let (after_role, after_term) = {
            let c = core.lock().expect("raft core poisoned");
            (c.role(), c.term())
        };
        record_transition(&metrics, before_role, before_term, after_role, after_term);

        // Durability before action: persist (and fsync) the core's state changes
        // before sending the responses that depend on them (a granted vote, an
        // acknowledged append).
        flush_and_maybe_compact(&env, &core).await;

        for (to, msg) in outs {
            let bytes = serde_json::to_vec(&msg).expect("raft message serializes");
            env.send(to, bytes).await;
        }
    }
}

/// Record the metrics implied by the messages the core just emitted (ADR 0015):
/// every outbound `AppendEntries` is one replication/heartbeat *sent*; an
/// outbound `AppendEntriesResp { success: false }` is a *rejection* this follower
/// produced; an outbound `InstallSnapshotResp` whose `last_index > 0` marks a
/// completed snapshot *install* on this follower. A pure read of `outs`.
fn record_outbound(metrics: &MetricsHandle, outs: &[Out]) {
    for (_, msg) in outs {
        match msg {
            RaftMsg::AppendEntries { .. } => metrics.incr(Metric::AppendEntriesSent),
            RaftMsg::AppendEntriesResp { success: false, .. } => {
                metrics.incr(Metric::AppendEntriesRejected);
            }
            RaftMsg::InstallSnapshotResp { last_index, .. } if *last_index > 0 => {
                metrics.incr(Metric::SnapshotInstalls);
            }
            _ => {}
        }
    }
}

/// Record election metrics + the leadership gauge from a role/term transition
/// (ADR 0015). Becoming a candidate at a higher term is an election *started*;
/// transitioning into `Leader` is an election *won*. The gauge tracks whether
/// this node currently believes it is leader. Pure in its inputs.
fn record_transition(
    metrics: &MetricsHandle,
    before_role: Role,
    before_term: u64,
    after_role: Role,
    after_term: u64,
) {
    // A new election: this node bumped its term and is now a candidate.
    if after_role == Role::Candidate && after_term > before_term {
        metrics.incr(Metric::ElectionsStarted);
    }
    // Won: entered the leader role from a non-leader role.
    if after_role == Role::Leader && before_role != Role::Leader {
        metrics.incr(Metric::ElectionsWon);
    }
    // Keep the gauge level current on any leadership change.
    if (after_role == Role::Leader) != (before_role == Role::Leader) {
        metrics.set_leader(after_role == Role::Leader);
    }
}

/// The leader's placement reconciler (ADR 0005): on a slow timer, if this node
/// is leader, recompute the desired replica set for every tablet that has a
/// policy and propose a `CasTabletReplicas` for any that drifted out of
/// compliance (e.g. a replica's member went `Down`).
///
/// The decision is the **pure, deterministic** [`Metadata::reconcile`]; this
/// driver supplies only timing (over the `Env` seam) and the propose. It runs on
/// every node but is a no-op off the leader (`propose` returns `NotLeader`), and
/// a no-op when nothing drifted (`reconcile` returns no commands) — so it is
/// idempotent and produces no churn at steady state. The proposed entries are
/// flushed and replicated by the [`drive`] loop's regular WAL handling.
async fn reconcile_loop<E: Env>(env: E, core: Arc<Mutex<RaftCore>>) {
    loop {
        env.sleep(RECONCILE_INTERVAL).await;
        let proposals = {
            let core = core.lock().expect("raft core poisoned");
            if !core.is_leader() {
                continue;
            }
            core.metadata().reconcile()
        };
        for command in proposals {
            // Off-leader transitions between the check and here are harmless:
            // a stale `CasTabletReplicas` is rejected by the epoch guard, and a
            // non-leader `propose` is dropped.
            core.lock().expect("raft core poisoned").propose(command);
        }
    }
}

/// The leader's failure detector (ADR 0012): on a timer, if this node is leader,
/// compare each tracked member's heartbeat liveness against its replicated
/// status and propose an `UpsertMember{status}` transition for any whose
/// liveness changed.
///
/// The **decision** is the pure [`FailureDetector`] verdict, taken at an
/// `Env`-supplied `now`; this driver supplies only timing (over the `Env` seam)
/// and the propose — mirroring the placement reconciler. It is **idempotent**: a
/// member already at the status its liveness implies yields no proposal, so a
/// steady cluster produces no churn and there is no flapping at the status level
/// (the detector's `timeout` absorbs a single delayed/dropped heartbeat). Once
/// committed, a `Down` transition is exactly what the placement reconciler reacts
/// to (ADR 0005), so a detected failure cascades into tablet re-placement.
///
/// Only members the detector *tracks* (have heartbeated at least once) are
/// judged, so a freshly-registered member is never marked `Down` before its
/// first heartbeat. `Joining`/`Leaving` members are left alone — their lifecycle
/// is operator-driven, not liveness-driven.
///
/// A freshly elected leader observes a **grace period** ([`LEADER_GRACE`]) before
/// proposing any `Down`: its detector is cold (per-node volatile state), so it
/// must hear a heartbeat round before it can fairly judge silence. The loop
/// records when it first sees itself leader for a term (`leader_since`) and
/// re-arms the grace whenever leadership or term changes.
async fn detect_loop<E: Env>(
    env: E,
    core: Arc<Mutex<RaftCore>>,
    detector: Arc<Mutex<FailureDetector>>,
    metrics: MetricsHandle,
) {
    // The (term, instant) at which this node last observed itself leader. `None`
    // while not leader; re-armed on each fresh leadership/term so the cold-start
    // grace applies after every election, not just the first.
    let mut leader_since: Option<(u64, animus_env::Nanos)> = None;
    loop {
        env.sleep(DETECT_INTERVAL).await;
        let now = env.now();
        let proposals = {
            let core = core.lock().expect("raft core poisoned");
            if !core.is_leader() {
                leader_since = None;
                continue;
            }
            let term = core.term();
            // Re-arm the grace on a fresh leadership or a new term.
            let since = match leader_since {
                Some((t, since)) if t == term => since,
                _ => {
                    leader_since = Some((term, now));
                    now
                }
            };
            // Suppress `Down` until the cold detector has had a heartbeat round.
            let allow_down = now.duration_since(since) >= LEADER_GRACE;
            let meta = core.metadata();
            liveness_transitions(
                &meta,
                &detector.lock().expect("detector poisoned"),
                now,
                allow_down,
            )
        };
        for command in proposals {
            // Attribute each liveness transition to its failure-detector metric
            // (ADR 0012/0015) before proposing it. `liveness_transitions` only
            // emits an `UpsertMember` when a tracked member's status actually
            // changes, so a `Down` here is a fresh Active->Down verdict and an
            // `Active` is a Down->Active recovery — the exact up/down edges we
            // want to count. Recording from the proposed command keeps the metric
            // a deterministic function of the (pure) verdict, and counts the edge
            // once on the leader that drives it.
            if let MetaCommand::UpsertMember { status, .. } = &command {
                match status {
                    NodeStatus::Down => metrics.incr(Metric::FailureDetectorDown),
                    NodeStatus::Active => metrics.incr(Metric::FailureDetectorUp),
                    _ => {}
                }
            }
            core.lock().expect("raft core poisoned").propose(command);
        }
    }
}

/// Pure helper: the `UpsertMember` transitions needed to bring each tracked
/// member's replicated status in line with the detector's liveness verdict at
/// `now`. Returns commands only for members whose status would actually change
/// (idempotent), in ascending node-id order (the detector iterates a `BTreeMap`),
/// so the result is a deterministic function of `(meta, detector, now, allow_down)`.
///
/// `allow_down` gates the `Active`→`Down` transition: a freshly elected leader
/// passes `false` during its post-election grace period so a cold detector does
/// not falsely mark live members `Down` before their heartbeats arrive
/// (ADR 0012). Recoveries (`Down`→`Active`) are always allowed.
fn liveness_transitions(
    meta: &Metadata,
    detector: &FailureDetector,
    now: animus_env::Nanos,
    allow_down: bool,
) -> Vec<MetaCommand> {
    detector
        .evaluate(now)
        .into_iter()
        .filter_map(|l| {
            let member = meta.members.get(&l.node)?;
            let desired = match (member.status, l.alive) {
                // A live member believed dead recovers to `Active`.
                (NodeStatus::Down, true) => NodeStatus::Active,
                // A silent member believed alive is marked `Down` — unless this
                // leader is still inside its post-election grace period.
                (NodeStatus::Active, false) if allow_down => NodeStatus::Down,
                // Already consistent, a status we don't drive (`Joining`/
                // `Leaving`), or a `Down` suppressed by the grace period: nothing
                // to propose.
                _ => return None,
            };
            Some(transition(l.node, member, desired))
        })
        .collect()
}

/// Build an `UpsertMember` that changes only `member`'s status, preserving its
/// topology labels (so a liveness transition never disturbs residency/spread).
fn transition(node: NodeId, member: &Member, status: NodeStatus) -> MetaCommand {
    MetaCommand::UpsertMember {
        node,
        labels: member.labels.clone(),
        status,
    }
}

/// Flush pending records, then rewrite the WAL to its compact image when needed:
/// either a snapshot base moved (a local snapshot or an installed one — which
/// must be materialized as a full rewrite before we act on it), or enough
/// applied entries have accumulated that we take a threshold snapshot (which
/// truncates the covered log prefix) and rewrite.
async fn flush_and_maybe_compact<E: Env>(env: &E, core: &Arc<Mutex<RaftCore>>) {
    flush_wal(env, core).await;

    let (mut rewrite, behind) = {
        let mut core = core.lock().expect("raft core poisoned");
        (core.take_snapshot_dirty(), core.applied_since_snapshot())
    };
    if behind >= SNAPSHOT_THRESHOLD {
        core.lock().expect("raft core poisoned").snapshot();
        rewrite = true;
    }
    if rewrite {
        compact_wal(env, core).await;
        // Clear the dirty flag `snapshot()` may have just set — we are writing
        // exactly that image now.
        core.lock()
            .expect("raft core poisoned")
            .take_snapshot_dirty();
    }
}

/// Append and `fsync` any pending durable-state records to the WAL, then advance
/// the core's durable watermark so the now-on-disk entries become client-visible
/// (durable-before-visible, ADR 0009). Returns how many records were written.
async fn flush_wal<E: Env>(env: &E, core: &Arc<Mutex<RaftCore>>) -> usize {
    // Capture the log high-water under the same lock as the drain: after we sync
    // the drained records, every entry up to here is durable. Entries appended
    // after this point ride the next flush.
    let (records, through) = {
        let mut core = core.lock().expect("raft core poisoned");
        let records = core.drain_persist();
        (records, core.last_log_index())
    };
    if records.is_empty() {
        return 0;
    }
    for record in &records {
        env.append(WAL, &PersistedState::encode_record(record))
            .await
            .expect("wal append");
    }
    env.sync(WAL).await.expect("wal sync");
    // The records are now durable: advance the watermark (which applies any
    // now-durable committed entries). Only after this is the proposal observable.
    core.lock()
        .expect("raft core poisoned")
        .mark_durable_through(through);
    records.len()
}

/// Atomically rewrite the WAL to the core's compact image (latest checkpoint +
/// hard state + current log). Safe because [`flush_wal`] has already persisted
/// everything the image is built from.
async fn compact_wal<E: Env>(env: &E, core: &Arc<Mutex<RaftCore>>) {
    let image = core.lock().expect("raft core poisoned").wal_image();
    let mut bytes = Vec::new();
    for record in &image {
        bytes.extend(PersistedState::encode_record(record));
    }
    env.replace(WAL, &bytes).await.expect("wal compaction");
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use animus_env::Nanos;

    use super::*;
    use crate::detector::FailureDetector;
    use crate::meta::{Member, Metadata};

    fn meta_with(node: NodeId, status: NodeStatus) -> Metadata {
        let mut m = Metadata::default();
        m.members.insert(
            node,
            Member {
                labels: BTreeMap::new(),
                status,
            },
        );
        m
    }

    fn detector_silent_since(node: NodeId, last_seen: Nanos) -> FailureDetector {
        let mut d = FailureDetector::new(DETECT_TIMEOUT);
        d.observe(node, last_seen);
        d
    }

    #[test]
    fn grace_period_suppresses_down_then_allows_it() {
        // An Active member that has been silent past DETECT_TIMEOUT.
        let meta = meta_with(7, NodeStatus::Active);
        let det = detector_silent_since(7, Nanos(0));
        let now = Nanos(DETECT_TIMEOUT.as_nanos() as u64 + 1);

        // Inside the grace period (allow_down = false): no Down proposed.
        assert!(liveness_transitions(&meta, &det, now, false).is_empty());

        // Grace elapsed (allow_down = true): the Down transition is proposed.
        let outs = liveness_transitions(&meta, &det, now, true);
        assert_eq!(outs.len(), 1);
        assert!(matches!(
            &outs[0],
            MetaCommand::UpsertMember {
                node: 7,
                status: NodeStatus::Down,
                ..
            }
        ));
    }

    #[test]
    fn recovery_is_allowed_even_during_grace() {
        // A Down member whose heartbeat just arrived recovers regardless of the
        // grace gate (positive evidence, no false-positive risk).
        let meta = meta_with(7, NodeStatus::Down);
        let det = detector_silent_since(7, Nanos(1_000));
        let now = Nanos(1_000); // fresh heartbeat → alive
        let outs = liveness_transitions(&meta, &det, now, false);
        assert_eq!(outs.len(), 1);
        assert!(matches!(
            &outs[0],
            MetaCommand::UpsertMember {
                node: 7,
                status: NodeStatus::Active,
                ..
            }
        ));
    }
}
