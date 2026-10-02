//! Regression for issue #595: `/admin/health`'s readiness probe used to read
//! straight off `RaftCore::leader()` — `leader_id`, which `start_pre_vote`
//! clears the instant a follower's OWN election timer lapses (ADR 0009
//! pre-vote), before any pre-vote round is even answered. That is correct
//! for pre-vote safety (a stale belief must never be trusted for granting a
//! vote), but it gives any boolean health/readiness consumer a false-
//! negative window on every transient one-sided delay of one election
//! timeout or more — even while the real leader stays fully healthy and
//! heartbeating every other replica the whole time.
//!
//! This proves the fix at the `RaftCore`/`RaftNode` level: a partitioned
//! follower's own `leader()` clears immediately (unchanged, and still
//! correct — pre-vote must keep doing exactly this), but `leader_within` at
//! a grace of `3 * election_timeout()` (the constant `animusd::admin::
//! health` actually uses, `HEALTH_LEADER_GRACE_ELECTION_TIMEOUTS`) survives
//! that same window, and only goes `None` once the partition is held
//! genuinely past the grace — the false-positive bound: a readiness probe
//! built on this accessor still degrades a truly leaderless replica, just
//! later than the very first missed heartbeat.
//!
//! Repro shape (unchanged from the original investigation): partition the
//! link leader<->follower (both directions) for longer than the election
//! window but keep the OTHER follower fully connected, so the leader never
//! loses its own majority and never steps down. This models a CPU-starved
//! follower missing heartbeats just as well as a real scheduling stall
//! would — the raft core has no notion of *why* no `AppendEntries` arrived
//! within the election window.

use std::time::Duration;

use animus_control::raft::ProposeResult;
use animus_control::{ColumnType, MetaCommand, RaftNode, TableSchema};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

/// The election base every `RaftCore::new` constructs with
/// (`raft.rs`'s `election_base: Duration::from_millis(150)`), and the same
/// value `admin::health`'s own grace multiplies. Duplicated here (rather
/// than read off a live node) only to size this test's own `run_for`
/// windows relative to it — the actual assertions read `election_timeout()`
/// off the live node, never this constant, so a future change to the
/// default would not silently make this test assert the wrong thing.
const ELECTION_BASE_MS: u64 = 150;

/// Mirrors `animusd::admin::HEALTH_LEADER_GRACE_ELECTION_TIMEOUTS` — kept
/// in sync by hand since this crate cannot depend on `animusd`.
const HEALTH_LEADER_GRACE_ELECTION_TIMEOUTS: u64 = 3;

/// A fixed, deterministic seed set (>= 10 seeds) proving the property holds
/// across a spread of election-timing/entropy draws, not just one
/// hand-picked seed. `ANIMUS_SEED` (set) overrides this list with a single
/// seed, matching every other corpus/regression test in this repo (root
/// `CLAUDE.md`'s "Replaying a failed simulation" section).
const DEFAULT_SEEDS: [u64; 12] = [1, 2, 3, 4, 5, 7, 11, 17, 42, 99, 1000, 31337];

fn seeds() -> Vec<u64> {
    match std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(seed) => vec![seed],
        None => DEFAULT_SEEDS.to_vec(),
    }
}

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn leader(nodes: &[RaftNode<SimEnv>]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    (ls.len() == 1).then(|| ls[0])
}

/// One seed's worth of the whole property: `leader()` clears immediately on
/// partition (pre-vote's own correct behavior, unchanged by this fix) while
/// `leader_within(health_grace)` survives it — right up until the partition
/// is held past the grace, at which point `leader_within` degrades too (the
/// false-positive bound).
fn run_one_seed(seed: u64) {
    let (mut sim, nodes) = cluster(seed);

    // Elect normally.
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes).unwrap_or_else(|| panic!("no leader elected (seed={seed})"));
    let elected_term = nodes[l].term();
    let elected_id = nid(NODES[l]);

    // The health grace this test asserts against — read off the live node's
    // own `election_timeout()`, exactly like `admin::health` does, not the
    // `ELECTION_BASE_MS` constant above (which only sizes the `run_for`
    // windows below).
    let health_grace = nodes[l].election_timeout() * HEALTH_LEADER_GRACE_ELECTION_TIMEOUTS as u32;
    assert_eq!(
        health_grace,
        Duration::from_millis(ELECTION_BASE_MS * HEALTH_LEADER_GRACE_ELECTION_TIMEOUTS),
        "election_timeout() drifted from this test's own assumed default (seed={seed})"
    );

    // Pick a follower to starve/partition, and the other follower as a
    // witness that the leader stayed healthy throughout.
    let others: Vec<usize> = (0..3).filter(|&i| i != l).collect();
    let starved = others[0];
    let witness = others[1];
    let starved_id = nid(NODES[starved]);

    // Partition the starved follower from the leader in BOTH directions —
    // it hears no more heartbeats, and its own pre-vote/vote traffic can't
    // reach the leader either. The witness stays fully connected.
    sim.partition_pair(elected_id.clone(), starved_id.clone());

    // Run past at least one election window (150ms) so the starved
    // follower's own pre-vote timer has fired, but comfortably short of the
    // health grace (450ms) — the false-negative-survival half of the claim.
    sim.run_for(Duration::from_millis(300));

    assert!(
        nodes[l].is_leader(),
        "the leader lost leadership even though it kept a majority (seed={seed})"
    );
    assert_eq!(
        nodes[l].term(),
        elected_term,
        "the leader's term advanced — disrupted by the partitioned follower's \
         pre-vote traffic, which pre-vote is specifically supposed to prevent \
         (seed={seed})"
    );
    assert_eq!(
        nodes[witness].leader(),
        Some(elected_id.clone()),
        "the connected witness follower should still see the healthy leader \
         (seed={seed})"
    );

    // THE BUG this issue is about: the raw `leader()` belief is already
    // cleared on the partitioned-but-otherwise-fine follower.
    assert_eq!(
        nodes[starved].leader(),
        None,
        "expected the partitioned follower's leader_id to have been cleared \
         by start_pre_vote on its own election-timer expiry (seed={seed})"
    );

    // THE FIX: `leader_within` at the health grace survives this exact
    // window — the false-negative half of the property.
    assert_eq!(
        nodes[starved].leader_within(health_grace),
        Some(elected_id.clone()),
        "leader_within(health_grace) should still report the healthy leader \
         during a one-sided delay well inside the grace, even though the raw \
         leader() belief already cleared (seed={seed})"
    );

    // Proposals against the leader must still succeed throughout — the
    // cluster itself is not in trouble, only the partitioned follower's own
    // vantage point (and thus its readiness probe) is degraded.
    for i in 0..5 {
        let result = nodes[l].propose(MetaCommand::CreateTableSchema {
            table: format!("t{seed}_{i}"),
            schema: TableSchema::simple("id", ColumnType::Uuid),
        });
        assert!(
            matches!(result, ProposeResult::Accepted { .. }),
            "propose against the still-healthy leader failed (seed={seed})"
        );
        sim.run_for(Duration::from_millis(20));
    }

    // Hold the partition well past the health grace (450ms total is the
    // threshold; this run has already spent >300ms plus five 20ms steps —
    // top it up so the total time since the last real contact is
    // unambiguously past the grace regardless of scheduling jitter).
    sim.run_for(Duration::from_millis(400));

    // THE FALSE-POSITIVE BOUND: a readiness probe built on `leader_within`
    // still degrades — it is hysteresis, not a permanent trust — once the
    // partition genuinely outlasts the grace.
    assert_eq!(
        nodes[starved].leader_within(health_grace),
        None,
        "leader_within(health_grace) should ALSO clear once the partition is \
         held well past the grace — this accessor must never suppress a \
         genuinely leaderless-from-this-replica's-view state forever (seed={seed})"
    );
    // The raw belief is naturally still cleared too.
    assert_eq!(nodes[starved].leader(), None, "seed={seed}");

    // Heal and confirm the starved follower recovers its view (both the raw
    // belief and the hysteresis-gated one) on the very next heartbeat it can
    // actually receive.
    sim.heal(elected_id.clone(), starved_id.clone());
    sim.heal(starved_id, elected_id.clone());
    sim.run_for(Duration::from_millis(500));
    assert_eq!(
        nodes[starved].leader(),
        Some(elected_id.clone()),
        "the previously-partitioned follower should recover leader_id once \
         healed and a heartbeat lands (seed={seed})"
    );
    assert_eq!(
        nodes[starved].leader_within(health_grace),
        Some(elected_id),
        "leader_within should also report the recovered leader immediately \
         (seed={seed})"
    );
}

#[test]
fn leader_within_survives_a_one_sided_partition_shorter_than_the_health_grace_and_then_degrades() {
    for seed in seeds() {
        run_one_seed(seed);
    }
}
