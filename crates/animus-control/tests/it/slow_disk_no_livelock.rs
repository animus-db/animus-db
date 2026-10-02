//! Issue #279, control-plane half: a slow `fsync` must not livelock the control
//! group.
//!
//! The data plane's consensus loop was fixed first (`animus-cp-data`'s
//! `slow_disk_no_livelock.rs`, ADR 0017's 2026-08-18 amendment) because that is
//! where the production symptom surfaced. This driver had the byte-identical
//! defect and was simply never the one observed failing: `drive` awaited
//! `persist_wal` (drain → `append` → `fsync`) inline, twice per iteration,
//! before it could return to its `select`. While that I/O blocks, a leader
//! sends no heartbeats and a follower neither processes inbound ones nor
//! re-arms its election deadline, so on a disk whose `fsync` outlasts the 150 ms
//! `election_base` followers campaign, every leadership change's no-op commit
//! makes more persist work on every replica, and the group never settles.
//!
//! The control group is not a bystander to the workload that triggered this: it
//! is one of the replicas `fsync`ing concurrently during a split-during-backfill
//! (parent, two children, the GSI's hidden-table tablet and the control plane,
//! times three replicas), and a control group that loses its leader takes
//! metadata, placement and failure detection down with it.
//!
//! Same two assertions as the data plane's, deliberately together — either one
//! alone can pass by coincidence:
//!
//! 1. **No runaway term churn.** A healthy slow-but-stable group elects once or
//!    twice; a livelocked one climbs without bound.
//! 2. **Proposals still commit and apply.** A low term proves nothing on its
//!    own without a leader whose commit index is actually advancing.
//!
//! The group is deliberately elected on a **fast** disk before the delay is
//! injected: a disk slower than the election timeout makes even a correct first
//! election inherently hard, which is an operational limit, not this bug.

use std::time::Duration;

use animus_control::raft::ProposeResult;
use animus_control::{ColumnType, MetaCommand, RaftNode, TableSchema};
use animus_env::nid;
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

/// Well past `RaftCore`'s 150 ms `election_base`, so a driver that blocks on the
/// I/O provably misses its own deadline.
const SYNC_DELAY: Duration = Duration::from_millis(400);
/// A slow-but-healthy group elects once, maybe twice under jitter. Runaway term
/// churn is the livelock's signature, and it climbs far past this.
const MAX_HEALTHY_TERM: u64 = 6;
const PROPOSALS: usize = 10;
/// Spaced so the group has to keep persisting throughout the window rather than
/// absorbing one burst and then idling.
const SPACING: Duration = Duration::from_secs(1);

fn seed() -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7)
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
    // Two nodes claiming leadership at once is a stale-term artifact of an
    // in-flight election, not a usable leader.
    (ls.len() == 1).then(|| ls[0])
}

fn slow_disk(sim: &Simulator) {
    let mut cfg = DiskConfig::default();
    cfg.set_sync_delay(SYNC_DELAY);
    sim.set_disk_config(cfg.clone());
    for &id in &NODES {
        sim.set_disk_config_for(nid(id), cfg.clone());
    }
}

/// A schema create, deliberately **not** an `UpsertMember`: declaring members
/// for node ids that will never heartbeat puts the leader's own orphan sweep
/// (ADR 0040 PR6) and failure detector into a proposal loop, which floods the
/// log with hundreds of rejected entries and measures their throughput rather
/// than this fix. A schema create is inert — nothing in the driver reacts to it.
fn schema(i: usize) -> MetaCommand {
    MetaCommand::CreateTableSchema {
        table: format!("t{i}"),
        schema: TableSchema::simple("id", ColumnType::Uuid),
    }
}

#[test]
fn the_control_group_on_a_disk_slower_than_the_election_timeout_stays_led_and_applies() {
    let seed = seed();
    let (mut sim, nodes) = cluster(seed);

    // Elect on a fast disk — see the module doc for why.
    sim.run_for(Duration::from_secs(5));
    let l =
        leader(&nodes).unwrap_or_else(|| panic!("no leader elected on a fast disk (seed={seed})"));
    let elected_term = nodes[l].term();

    slow_disk(&sim);

    // Sustained proposals against whoever currently leads. Some are expected to
    // be refused mid-election even in a healthy run, so this asserts a floor,
    // not every attempt — a run where almost nothing is accepted is itself the
    // "mostly leaderless" failure.
    let mut accepted = 0usize;
    for i in 0..PROPOSALS {
        if let Some(l) = leader(&nodes)
            && matches!(nodes[l].propose(schema(i)), ProposeResult::Accepted { .. })
        {
            accepted += 1;
        }
        sim.run_for(SPACING);
    }
    assert!(
        accepted * 2 >= PROPOSALS,
        "only {accepted}/{PROPOSALS} proposals were accepted — the control group \
         spent the window without a usable leader (seed={seed})"
    );

    // Let the backlog drain and any election settle.
    sim.run_for(Duration::from_secs(30));

    let term = nodes.iter().map(RaftNode::term).max().expect("nodes");
    assert!(
        term <= MAX_HEALTHY_TERM,
        "term churned to {term} (elected at {elected_term}) on a slow disk — the \
         consensus loop is missing its own election deadline while persisting \
         (seed={seed})"
    );

    // A low term is not enough on its own: the leader's commit index must
    // actually be advancing, which means the accepted proposals reached the
    // applied `Metadata` a reader sees.
    let l = leader(&nodes)
        .unwrap_or_else(|| panic!("no stable leader after the proposal window (seed={seed})"));
    let schemas = nodes[l].metadata().schemas;
    let applied = (0..PROPOSALS)
        .filter(|i| schemas.get(&format!("t{i}")).is_some())
        .count();
    assert_eq!(
        applied, accepted,
        "only {applied} of {accepted} accepted schemas are visible in the \
         leader's applied metadata — commit/apply is not advancing on a slow \
         disk (seed={seed})"
    );
}
