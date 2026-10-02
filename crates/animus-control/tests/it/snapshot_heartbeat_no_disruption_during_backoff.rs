//! Companion regression to `animus-cp-data/tests/
//! snapshot_heartbeat_reconnect_latency.rs`: proves that
//! `SnapshotResend::Backoff`'s steady-state cadence (once past
//! `SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS`, a resend every that-many
//! heartbeat ticks — ~1.6s at the default 50ms `heartbeat_interval`) is
//! **slower than the default election timeout** (`election_base` = 150ms,
//! so a randomized `[150, 300)` window) for a peer whose *only*
//! leader-liveness signal is the chunk itself (`replicate_to` never sends a
//! plain `AppendEntries` to a peer whose `next_index <= snapshot_index` —
//! see `snapshot_chunk_for`'s and `SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS`'s
//! own docs).
//!
//! That gap is real: a REACHABLE (not partitioned — this test never calls
//! `Simulator::partition*`/`crash`) voter stuck in snapshot mode for
//! several seconds *will* time out and run repeated pre-vote rounds between
//! chunks. The claim this test pins down is that this is genuinely
//! harmless, not merely "usually fine": pre-vote's own safety design (ADR
//! 0009) already means every OTHER voter — which still sees this cluster's
//! one real leader as alive — refuses to grant it, so the stuck voter's own
//! churn produces no term bump, no leadership change, and no effect on any
//! other replica's view. This is proven directly at the `RaftCore` level
//! (no `RaftKvNode`/`SimEnv` network needed) by hand-driving all three
//! cores' `tick`/`handle` calls ourselves, so every `PreVote` round the
//! stuck voter runs is visible and its rejection can be asserted on the
//! spot, not merely inferred from "the leader is still leader at the end."

use animus_control::raft::SNAPSHOT_CHUNK_BYTES;
use animus_control::{RaftCore, RaftMsg, Role};
use animus_env::{Nanos, NodeId, nid};

fn members() -> [NodeId; 3] {
    [nid(0), nid(1), nid(2)]
}

/// The three hand-driven `RaftCore`s, bundled so the routing/pump helpers
/// below take one argument instead of three.
struct Cluster {
    leader: RaftCore,
    healthy: RaftCore,
    stuck: RaftCore,
}

impl Cluster {
    /// One hop: deliver `msg` (from `from`) to whichever of the three cores
    /// `to` names, returning its own replies tagged with their
    /// destinations.
    fn route(
        &mut self,
        from: NodeId,
        to: NodeId,
        msg: RaftMsg,
        now: Nanos,
        entropy: u64,
    ) -> Vec<(NodeId, NodeId, RaftMsg)> {
        let out = if to == nid(0) {
            self.leader.handle(from, msg, now, entropy)
        } else if to == nid(1) {
            self.healthy.handle(from, msg, now, entropy)
        } else {
            self.stuck.handle(from, msg, now, entropy)
        };
        out.into_iter()
            .map(|(dst, m)| (to.clone(), dst, m))
            .collect()
    }

    /// Pump a batch of `(from, to, msg)` triples to a fixed point (bounded
    /// steps), routing each reply back in. Mirrors `install_snapshot.rs`'s
    /// own "pump until empty" idiom.
    fn pump(&mut self, mut pending: Vec<(NodeId, NodeId, RaftMsg)>, now: Nanos, entropy: u64) {
        let mut steps = 0;
        while !pending.is_empty() {
            steps += 1;
            assert!(steps < 1000, "message exchange did not terminate");
            let mut next = Vec::new();
            for (from, to, msg) in pending {
                next.extend(self.route(from, to, msg, now, entropy));
            }
            pending = next;
        }
    }

    /// `snapshot_index()` doubles as "the compacted prefix," so a peer with
    /// no per-peer accessor exposed here is instead checked indirectly:
    /// once the leader's own commit index has advanced to its last log
    /// index, node 1 must have acked everything for that to have happened
    /// (it is the only other voter in this 3-node group).
    fn healthy_caught_up(&self) -> bool {
        self.leader.commit_index() == self.leader.last_log_index()
            && self.healthy.role() == Role::Follower
    }
}

#[test]
fn snapshot_mode_voter_backoff_causes_no_leader_disruption() {
    let ids = members();
    let entropy = 7u64;
    let mut now = Nanos(1_000_000_000);

    let mut cluster = Cluster {
        leader: RaftCore::new(nid(0), &ids, Nanos(0), entropy),
        healthy: RaftCore::new(nid(1), &ids, Nanos(0), entropy),
        stuck: RaftCore::new(nid(2), &ids, Nanos(0), entropy),
    };

    // Elect node 0, using only node 1 (a real, fully-participating core) to
    // reach a 2-of-3 majority — node 2 never sees any message from this
    // point on until the main loop below, exactly like a peer that was
    // simply never caught up.
    let pv = cluster.leader.tick(now, entropy);
    cluster.pump(
        pv.into_iter()
            .filter(|(to, _)| *to == nid(1))
            .map(|(to, m)| (nid(0), to, m))
            .collect(),
        now,
        entropy,
    );
    assert!(
        cluster.leader.is_leader(),
        "node 0 should have won the election"
    );
    let elected_term = cluster.leader.term();

    // Commit + durably replicate a modest log to node 1 for real (a genuine
    // multi-round `AppendEntries` exchange, not a synthetic ack) — node 1
    // must be a genuinely caught-up, healthy follower, or it could itself
    // fall into snapshot mode and confound the "every OTHER voter still
    // sees a live leader" claim this test rests on.
    for i in 0..40u64 {
        let cmd = animus_control::MetaCommand::UpsertMember {
            node: nid(100 + i),
            labels: std::collections::BTreeMap::new(),
            status: animus_control::NodeStatus::Active,
        };
        let _ = cluster.leader.propose(cmd);
    }
    for _ in 0..10 {
        now = Nanos(now.0 + cluster.leader.election_timeout().as_nanos() as u64 / 3);
        let out = cluster.leader.tick(now, entropy);
        cluster.pump(
            out.into_iter()
                .filter(|(to, _)| *to == nid(1))
                .map(|(to, m)| (nid(0), to, m))
                .collect(),
            now,
            entropy,
        );
        if cluster.healthy_caught_up() {
            break;
        }
    }
    assert!(
        cluster.healthy_caught_up(),
        "node 1 must genuinely catch up before this test's main loop"
    );
    cluster
        .leader
        .mark_durable_through(cluster.leader.last_log_index());
    cluster.leader.snapshot();
    assert!(
        cluster.leader.snapshot_index() > 0,
        "leader should have a snapshot to ship"
    );
    // Several chunks' worth, so the stuck peer needs more than one — not
    // load-bearing for this test's own property, but keeps it honest that
    // this is a genuine multi-chunk snapshot-mode catch-up, not a
    // zero-byte edge case.
    cluster
        .leader
        .set_snapshot_blob(vec![0xEEu8; 3 * SNAPSHOT_CHUNK_BYTES + 17]);

    // Main loop: advance in real 50ms heartbeat-interval steps (the
    // default `heartbeat_interval`), ticking every core every step and
    // routing whatever each one produces to completion. Node 2 receives
    // NOTHING on a Backoff-suppressed heartbeat (see `snapshot_chunk_for`'s
    // own doc: a snapshot-mode peer's outstanding chunk is its only
    // leader-liveness signal) — it is never partitioned, this loop simply
    // never manufactures a message for it on a suppressed tick, exactly
    // matching what `RaftCore::tick`'s own `broadcast_append` actually
    // produces (or doesn't) for it.
    //
    // 20s of virtual time is comfortably past
    // `SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS`'s ~1.6s steady-state cadence
    // (reached within the first couple of seconds) and, at the default
    // [150, 300)ms election timeout, gives node 2 many chances to time out
    // and run a pre-vote round between chunks.
    let heartbeat_step = Nanos(50_000_000);
    let mut stuck_ever_pre_voted = false;
    for _ in 0..400 {
        now = Nanos(now.0 + heartbeat_step.0);

        // The leader's own heartbeat tick: an ordinary `AppendEntries` to
        // node 1 (pumped bidirectionally, keeping it genuinely caught up)
        // and a Backoff-gated `InstallSnapshot` chunk to node 2 whenever the
        // schedule allows one. Node 2's own reply is deliberately DROPPED
        // rather than fed back to the leader — modeling the genuinely-
        // stuck-but-reachable peer this ceiling exists for (real progress
        // never lands), so the leader's own tracked offset never advances
        // and the exact same Backoff schedule keeps gating every later
        // resend, rather than the whole transfer completing in one
        // ack-driven cascade the instant a single chunk gets through (which
        // is what a fully bidirectional exchange would do here — see
        // `handle_install_snapshot_resp`'s own immediate ack-driven
        // resend). Node 2 still genuinely RECEIVES the chunk when one is
        // sent, resetting its own election timer exactly as a real
        // reachable peer would — this is not a partition.
        for (to, msg) in cluster.leader.tick(now, entropy) {
            if to == nid(1) {
                cluster.pump(vec![(nid(0), nid(1), msg)], now, entropy);
            } else {
                let _ = cluster.stuck.handle(nid(0), msg, now, entropy);
            }
        }

        // Node 1's own tick — should stay quiet; it was just refreshed
        // above, every single heartbeat.
        let healthy_out: Vec<(NodeId, NodeId, RaftMsg)> = cluster
            .healthy
            .tick(now, entropy)
            .into_iter()
            .map(|(to, m)| (nid(1), to, m))
            .collect();
        cluster.pump(healthy_out, now, entropy);

        // Node 2's own tick — this is where the election-timeout gap under
        // test shows up: once its own `election_deadline` lapses with no
        // fresh contact, it runs a pre-vote round, which the leader and
        // node 1 (both still seeing a live leader) must refuse.
        let stuck_out: Vec<(NodeId, NodeId, RaftMsg)> = cluster
            .stuck
            .tick(now, entropy)
            .into_iter()
            .map(|(to, m)| (nid(2), to, m))
            .collect();
        if stuck_out
            .iter()
            .any(|(_, _, m)| matches!(m, RaftMsg::PreVote { .. }))
        {
            stuck_ever_pre_voted = true;
        }
        cluster.pump(stuck_out, now, entropy);

        // The property under test, checked on EVERY step, not just at the
        // end: node 2's own churn must never move the leader's term or
        // depose it, at any point during a long snapshot-mode stall.
        assert!(
            cluster.leader.is_leader() && cluster.leader.term() == elected_term,
            "leader was disrupted (is_leader={}, term={} vs elected_term={}) by a snapshot-\
             mode voter's own election-timeout churn — SnapshotResend::Backoff's steady-\
             state cadence must never cause real disruption, only harmless, refused \
             pre-vote rounds",
            cluster.leader.is_leader(),
            cluster.leader.term(),
            elected_term
        );
    }

    assert!(
        stuck_ever_pre_voted,
        "node 2 never ran a single pre-vote round across the whole window — this test \
         proves nothing about the concern it's meant to cover unless the snapshot-mode \
         peer genuinely experiences the election-timeout gap between backed-off chunks"
    );
}
