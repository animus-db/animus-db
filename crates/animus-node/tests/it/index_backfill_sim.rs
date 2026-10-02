//! `animus_node::index_backfill::index_backfill_loop`, driven deterministically
//! under `SimEnv` (ADR 0061 rung C2) — the first coverage this loop has ever
//! had that isn't a real-TCP, real-clock multi-node bring-up
//! (`animusd/tests/index_backfill.rs`'s own `bring_up`).
//!
//! The host here is a thin [`animus_node::host::ControlLeaderHost`] wrapper
//! around a real, but tiny and purely in-process, single-voter control
//! `RaftNode<SimEnv>` — no sockets, no multi-node cluster, no `ClientCtx`.
//! A single-voter Raft group commits its own proposals immediately (see
//! `animus-control/CLAUDE.md`'s "Election no-op is committed in
//! `become_leader` itself" note), which is what makes this fixture cheap
//! enough to spin up per test while still exercising the loop's own control
//! flow (the `env.sleep`-paced tick, the leader-handle self-gate, the real
//! `RaftNode::propose`) rather than a hand-called pure function.
//!
//! What this proves that the pure unit tests in `index_backfill.rs` don't:
//! the loop, run end to end under a virtual clock, converges to the correct
//! terminal state on its own pacing — including the straggler-tablet case,
//! where a premature flip would be a real, user-visible correctness bug
//! (an index reported `Active` while still missing rows).

use std::time::Duration;

use animus_control::schema::{IndexKind, IndexProjection};
use animus_control::{ColumnType, IndexDef, IndexStatus, MetaCommand, RaftNode, TableSchema};
use animus_env::{EnvExt, nid};
use animus_node::host::ControlLeaderHost;
use animus_node::index_backfill::index_backfill_loop;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TOKEN_BYTES, TabletId};

/// A fake host — no `ClientCtx`, no data role, nothing but "here is the
/// control-plane leader handle" — wrapping a real single-voter
/// `RaftNode<SimEnv>`.
#[derive(Clone)]
struct FakeControlHost(RaftNode<SimEnv>);

impl ControlLeaderHost<SimEnv> for FakeControlHost {
    fn control_leader(&self) -> Option<RaftNode<SimEnv>> {
        Some(self.0.clone())
    }
}

fn single_voter(seed: u64) -> (Simulator, RaftNode<SimEnv>) {
    let sim = Simulator::new(seed);
    let node = RaftNode::start(sim.env(nid(0)), vec![nid(0)], MemoryEngine::new());
    (sim, node)
}

fn index_status(node: &RaftNode<SimEnv>, table: &str, index: &str) -> Option<IndexStatus> {
    node.metadata()
        .table_schema(table)?
        .indexes
        .iter()
        .find(|i| i.name == index)
        .map(|i| i.status)
}

#[test]
fn a_straggler_tablet_blocks_the_flip_until_it_reports_too() {
    run(0x1DBF_0001);
}

#[test]
fn a_straggler_tablet_blocks_the_flip_until_it_reports_too_seed2() {
    run(0x1DBF_0002);
}

/// A table with a `Creating` index but **zero** tablets never flips — the
/// vacuous-true trap the loop's own `has_any_tablet` guard exists to close.
/// Run under `SimEnv` (not just the pure unit test in `index_backfill.rs`)
/// so this is proven against the loop's real pacing too, not just the
/// decision function in isolation.
#[test]
fn zero_tablets_never_flip_under_sim() {
    let seed = 0x1DBF_0003;
    let (mut sim, node) = single_voter(seed);
    sim.run_for(Duration::from_millis(500));
    assert!(node.is_leader(), "seed={seed}");

    let host = FakeControlHost(node.clone());
    let loop_env = node.env().clone();
    loop_env.spawn_task(index_backfill_loop(node.env().clone(), host, 20));

    assert!(matches!(
        node.propose(MetaCommand::CreateTableSchema {
            table: "orders".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
        animus_control::raft::ProposeResult::Accepted { .. }
    ));
    assert!(matches!(
        node.propose(MetaCommand::CreateTableIndex {
            table: "orders".into(),
            index: IndexDef {
                name: "gsi1".into(),
                kind: IndexKind::Global,
                hash_attribute: "email".into(),
                sort_attribute: None,
                projection: IndexProjection::All,
                status: IndexStatus::Creating,
                hash_attribute_type: None,
                sort_attribute_type: None,
            },
        }),
        animus_control::raft::ProposeResult::Accepted { .. }
    ));

    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        index_status(&node, "orders", "gsi1"),
        Some(IndexStatus::Creating),
        "a table with no tablets at all must never flip (seed={seed})"
    );
}

fn run(seed: u64) {
    let (mut sim, node) = single_voter(seed);
    sim.run_for(Duration::from_millis(500));
    assert!(
        node.is_leader(),
        "the sole voter must be its own leader (seed={seed})"
    );

    let host = FakeControlHost(node.clone());
    let loop_env = node.env().clone();
    loop_env.spawn_task(index_backfill_loop(node.env().clone(), host, 20));

    // Provision a table with one `Creating` GSI and two tablets — the
    // straggler scenario `index_backfill_loop`'s own "fresh read every
    // tick" property must hold against.
    assert!(matches!(
        node.propose(MetaCommand::CreateTableSchema {
            table: "orders".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
        animus_control::raft::ProposeResult::Accepted { .. }
    ));
    assert!(matches!(
        node.propose(MetaCommand::CreateTableIndex {
            table: "orders".into(),
            index: IndexDef {
                name: "gsi1".into(),
                kind: IndexKind::Global,
                hash_attribute: "email".into(),
                sort_attribute: None,
                projection: IndexProjection::All,
                status: IndexStatus::Creating,
                hash_attribute_type: None,
                sort_attribute_type: None,
            },
        }),
        animus_control::raft::ProposeResult::Accepted { .. }
    ));
    // `CreateTablet` allows only one tablet per table (ADR 0023) — a real
    // second tablet on the same table comes only from a split, so drive
    // `BeginSplitInPlace`/`CutoverSplit` (the in-place, control-plane-only
    // command pair — ADR 0062: children inherit the parent's own replicas
    // verbatim) to end up with two `Active` tablets, ids 2 and 3. This is
    // topology scaffolding only (the table is empty) — no data-plane fork
    // is materialized here, and nothing in this test exercises the fork
    // itself.
    assert!(matches!(
        node.propose(MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".into()),
            range: KeyRange::whole(),
            replicas: vec![nid(0)],
        }),
        animus_control::raft::ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_millis(50));
    let parent_epoch = node.metadata().tablets[&TabletId(1)].epoch;
    assert!(matches!(
        node.propose(MetaCommand::BeginSplitInPlace {
            parent: TabletId(1),
            expected_epoch: parent_epoch,
            split_key: [0x80; TOKEN_BYTES].to_vec(),
            children: [(TabletId(2), vec![nid(0)]), (TabletId(3), vec![nid(0)])],
        }),
        animus_control::raft::ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_millis(50));
    let parent_epoch = node.metadata().tablets[&TabletId(1)].epoch;
    assert!(matches!(
        node.propose(MetaCommand::CutoverSplit {
            parent: TabletId(1),
            expected_epoch: parent_epoch,
            cutover_wall_ms: 0,
        }),
        animus_control::raft::ProposeResult::Accepted { .. }
    ));
    let (tablet_a, tablet_b) = (TabletId(2), TabletId(3));

    // Let a few ticks pass with nothing reported yet — must stay `Creating`.
    sim.run_for(Duration::from_millis(200));
    assert_eq!(
        index_status(&node, "orders", "gsi1"),
        Some(IndexStatus::Creating),
        "must not flip before any tablet has reported (seed={seed})"
    );

    // Tablet A reports; tablet B is still a straggler — must still not flip.
    assert!(matches!(
        node.propose(MetaCommand::MarkIndexBackfilled {
            table: "orders".into(),
            index: "gsi1".into(),
            tablet: tablet_a,
        }),
        animus_control::raft::ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_millis(200));
    assert_eq!(
        index_status(&node, "orders", "gsi1"),
        Some(IndexStatus::Creating),
        "one straggler tablet must still block the flip (seed={seed})"
    );

    // Tablet B finally reports — every tablet has now reported, so the next
    // tick must flip the index to `Active`.
    assert!(matches!(
        node.propose(MetaCommand::MarkIndexBackfilled {
            table: "orders".into(),
            index: "gsi1".into(),
            tablet: tablet_b,
        }),
        animus_control::raft::ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_millis(500));
    assert_eq!(
        index_status(&node, "orders", "gsi1"),
        Some(IndexStatus::Active),
        "every tablet has reported — the loop must have flipped the index (seed={seed})"
    );
}
