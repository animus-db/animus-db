//! Offline force-new-configuration recovery of the control plane after a
//! permanent loss of a voter majority (ADR 0077, issue #1178).
//!
//! Each cell builds an `n`-voter control group over `SimEnv` (retained
//! `MemoryEngine` per node, the production shape of a node restart), runs a
//! workload, then **loses the majority for good**: every voter is
//! `sim.stop`ped (process exit; durable disk survives) and only one
//! survivor's disk is kept. The recovery tool ([`animus_control::recover`])
//! runs against that survivor's retained disk through the same `Env` disk
//! seam a node uses, the survivor restarts, and the test asserts:
//!
//! 1. **Serves.** It elects itself, its configuration is exactly `{survivor}`
//!    and a new write commits.
//! 2. **Exact preservation.** Its `Metadata` holds everything its own
//!    durable log had, and nothing else: acknowledged writes it never
//!    received are gone (`LaggingSurvivor`), entries it held without knowing
//!    they were committed are kept as if committed (`UncommittedTail`).
//! 3. **Stale voters are fenced.** An old voter that comes back *without* a
//!    wipe never leads and never disturbs the survivor; it is told it was
//!    removed (`RaftMsg::Removed`) and stays a non-voter.
//! 4. **Wiped voters rejoin** through the supported path: wiped disk, fresh
//!    engine, `add_learner` then `promote_learner` on the survivor.
//!
//! Negative controls prove the assertions can fail: the same scenario with
//! the tool *not* run leaves the survivor unable to elect; an empty WAL is
//! refused and left untouched; re-running the tool is idempotent.
//!
//! Replay: `ANIMUS_SEED=<seed> cargo test -p animus-control --test it
//! force_new_configuration::` (the seed is printed on failure). Depth knob:
//! `ANIMUS_FORCE_RECOVER_SEEDS=K` (default 1).

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::persist::CONTROL_WAL;
use animus_control::recover::{
    self, CONTROL_WAL_FILE, DataLossAcknowledged, ForceConfigError, ForceNewConfigOutcome,
    ForceNewConfigPlan, RECOVERY_TERM_JUMP,
};
use animus_control::{
    DeltaRing, MetaCommand, Metadata, NodeStatus, PersistedState, ProposeResult, RaftNode, Role,
};
use animus_env::{Disk, Env, EnvExt, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_test::corpus;

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

/// Workload ids start here so they never collide with a voter id.
const BASE: u64 = 1000;

fn workload_ids(md: &Metadata) -> BTreeSet<NodeId> {
    // Compare by the parsed number, not the string order.
    md.members
        .keys()
        .filter(|k| {
            k.as_str()
                .strip_prefix('n')
                .and_then(|n| n.parse::<u64>().ok())
                .is_some_and(|n| n >= BASE)
        })
        .cloned()
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum Shape {
    /// The survivor has everything the group committed.
    CaughtUp { survivor_is_leader: bool },
    /// The survivor was cut off while the others kept committing: those
    /// acknowledged writes are lost.
    LaggingSurvivor,
    /// The survivor is a leader cut off from the majority that appended
    /// entries it could never commit: they are kept.
    UncommittedTail,
}

#[derive(Clone, Copy, Debug)]
struct Cell {
    name: &'static str,
    voters: u64,
    entries: u64,
    shape: Shape,
}

const CELLS: &[Cell] = &[
    Cell {
        name: "three_caught_up_follower",
        voters: 3,
        entries: 20,
        shape: Shape::CaughtUp {
            survivor_is_leader: false,
        },
    },
    Cell {
        // 150 entries cross SNAPSHOT_THRESHOLD (64) twice: the survivor's
        // WAL is a snapshot record plus a tail, and the engine is retained.
        name: "five_caught_up_leader_compacted",
        voters: 5,
        entries: 150,
        shape: Shape::CaughtUp {
            survivor_is_leader: true,
        },
    },
    Cell {
        name: "three_lagging_survivor_loses_acked_writes",
        voters: 3,
        entries: 30,
        shape: Shape::LaggingSurvivor,
    },
    Cell {
        name: "five_uncommitted_tail_is_kept",
        voters: 5,
        entries: 40,
        shape: Shape::UncommittedTail,
    },
];

fn seeds(cell: &str) -> Vec<u64> {
    if let Ok(s) = std::env::var("ANIMUS_SEED") {
        return vec![s.parse().expect("ANIMUS_SEED must be a u64")];
    }
    let k = corpus::seeds_from_env("ANIMUS_FORCE_RECOVER_SEEDS");
    (0..k)
        .map(|i| corpus::name_seed(&format!("force_new_configuration_{cell}_s{i}")))
        .collect()
}

fn replay_hint(cell: &str, seed: u64) -> String {
    format!(
        "seed={seed} cell={cell} (replay: ANIMUS_SEED={seed} cargo test -p animus-control --test it force_new_configuration::)"
    )
}

struct World {
    sim: Simulator,
    ids: Vec<u64>,
    voters: Vec<NodeId>,
    engines: Vec<MemoryEngine>,
    nodes: Vec<Option<RaftNode<SimEnv>>>,
    hint: String,
}

impl World {
    fn start_node(&mut self, i: usize, engine: MemoryEngine) {
        self.engines[i] = engine.clone();
        self.nodes[i] = Some(RaftNode::start_with_orphan_sweep_after(
            self.sim.env(nid(self.ids[i])),
            self.voters.clone(),
            self.sim.env(nid(self.ids[i])).metrics(),
            engine,
            DeltaRing::default(),
            Duration::ZERO,
        ));
    }

    fn new(seed: u64, voters: u64, hint: String) -> Self {
        let ids: Vec<u64> = (0..voters).collect();
        let mut w = World {
            sim: Simulator::new(seed),
            voters: ids.iter().copied().map(nid).collect(),
            engines: ids.iter().map(|_| MemoryEngine::new()).collect(),
            nodes: ids.iter().map(|_| None).collect(),
            ids,
            hint,
        };
        for i in 0..w.ids.len() {
            let e = w.engines[i].clone();
            w.start_node(i, e);
        }
        w
    }

    fn node(&self, i: usize) -> &RaftNode<SimEnv> {
        self.nodes[i].as_ref().expect("node is running")
    }

    fn run(&mut self, d: Duration) {
        self.sim.run_for(d);
    }

    fn leader_among(&self, live: &[usize]) -> usize {
        let leaders: Vec<usize> = live
            .iter()
            .copied()
            .filter(|&i| self.nodes[i].as_ref().is_some_and(RaftNode::is_leader))
            .collect();
        assert_eq!(
            leaders.len(),
            1,
            "expected exactly one leader among {live:?}, found {leaders:?}; {}",
            self.hint
        );
        leaders[0]
    }

    fn propose_ok(&self, i: usize, cmd: MetaCommand, what: &str) {
        assert!(
            matches!(self.node(i).propose(cmd), ProposeResult::Accepted { .. }),
            "{what}: node {i} must accept the proposal; {}",
            self.hint
        );
    }

    /// Propose `ids` on node `i`, pacing so the log is not one burst.
    fn propose_run(&mut self, i: usize, ids: impl Iterator<Item = u64>, what: &str) {
        for (n, id) in ids.enumerate() {
            self.propose_ok(i, upsert(id), what);
            if n % 16 == 15 {
                self.run(Duration::from_millis(300));
            }
        }
    }

    fn stop_all(&mut self) {
        for i in 0..self.ids.len() {
            self.sim.stop(nid(self.ids[i]));
            self.nodes[i] = None;
        }
    }

    fn heal_all(&self) {
        for a in &self.ids {
            for b in &self.ids {
                if a < b {
                    self.sim.heal(nid(*a), nid(*b));
                    self.sim.heal(nid(*b), nid(*a));
                }
            }
        }
    }

    /// Run `fut` as a task on `env` (a stopped node's disk is still
    /// reachable through a fresh env) and return its output.
    fn run_task<T: Send + 'static>(
        &mut self,
        env: &SimEnv,
        fut: impl Future<Output = T> + Send + 'static,
    ) -> T {
        let slot = Arc::new(Mutex::new(None));
        let slot2 = Arc::clone(&slot);
        env.spawn_task(async move {
            let v = fut.await;
            *slot2.lock().unwrap() = Some(v);
        });
        self.run(Duration::from_secs(1));
        slot.lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| panic!("tool task did not finish; {}", self.hint))
    }

    fn plan_task(&mut self, i: usize) -> Result<ForceNewConfigPlan, ForceConfigError> {
        let env = self.sim.env(nid(self.ids[i]));
        let voters = self.voters.clone();
        let env2 = env.clone();
        self.run_task(&env, async move {
            recover::plan::<_, MetaCommand, Metadata>(&env2, CONTROL_WAL_FILE, &voters).await
        })
    }

    fn apply_task(
        &mut self,
        i: usize,
        plan: ForceNewConfigPlan,
    ) -> Result<ForceNewConfigOutcome, ForceConfigError> {
        let env = self.sim.env(nid(self.ids[i]));
        let voters = self.voters.clone();
        let env2 = env.clone();
        self.run_task(&env, async move {
            recover::apply::<_, MetaCommand, Metadata>(
                &env2,
                CONTROL_WAL_FILE,
                &voters,
                &plan,
                DataLossAcknowledged::acknowledging_that_unseen_writes_are_lost_and_other_voters_must_be_wiped(),
            )
            .await
        })
    }

    fn read_wal(&mut self, i: usize) -> Vec<u8> {
        let env = self.sim.env(nid(self.ids[i]));
        let env2 = env.clone();
        self.run_task(&env, async move {
            env2.read(CONTROL_WAL_FILE).await.unwrap_or_default()
        })
    }
}

/// What the scenario set up, for the oracle.
struct Prepared {
    world: World,
    survivor: usize,
    /// Everything the survivor's own durable log holds.
    expected: BTreeSet<NodeId>,
    /// Acknowledged by the old group's majority but never seen by the survivor.
    acked_lost: BTreeSet<NodeId>,
    old_term: u64,
}

fn prepare(cell: &Cell, seed: u64) -> Prepared {
    let hint = replay_hint(cell.name, seed);
    let mut w = World::new(seed, cell.voters, hint.clone());
    let all: Vec<usize> = (0..cell.voters as usize).collect();
    w.run(Duration::from_secs(2));
    let leader = w.leader_among(&all);
    w.propose_run(leader, BASE..BASE + cell.entries, "workload");
    w.run(Duration::from_secs(3));
    for i in &all {
        assert_eq!(
            workload_ids(&w.node(*i).metadata()).len() as u64,
            cell.entries,
            "node {i} must hold the whole workload before the loss; {hint}"
        );
    }

    let others = |s: usize| -> Vec<usize> { all.iter().copied().filter(|i| *i != s).collect() };
    let seeded_follower = || {
        let followers = others(leader);
        followers[(seed as usize) % followers.len()]
    };

    let mut acked_lost = BTreeSet::new();
    let (survivor, expected) = match cell.shape {
        Shape::CaughtUp { survivor_is_leader } => {
            let s = if survivor_is_leader {
                leader
            } else {
                seeded_follower()
            };
            let expected = workload_ids(&w.node(s).metadata());
            (s, expected)
        }
        Shape::LaggingSurvivor => {
            let s = seeded_follower();
            for o in others(s) {
                w.sim.partition_pair(nid(w.ids[s]), nid(w.ids[o]));
            }
            let expected = workload_ids(&w.node(s).metadata());
            // The leader and the other follower(s) keep a majority and
            // acknowledge more writes the survivor will never see.
            let lost: Vec<u64> = (BASE + cell.entries..BASE + cell.entries + 5).collect();
            w.propose_run(leader, lost.iter().copied(), "acked-but-unseen writes");
            w.run(Duration::from_secs(2));
            for id in &lost {
                assert!(
                    w.node(leader).metadata().members.contains_key(&nid(*id)),
                    "the majority must acknowledge write {id}; {hint}"
                );
                assert!(
                    !w.node(s).metadata().members.contains_key(&nid(*id)),
                    "the partitioned survivor must not have write {id}; {hint}"
                );
                acked_lost.insert(nid(*id));
            }
            (s, expected)
        }
        Shape::UncommittedTail => {
            let s = leader;
            for o in others(s) {
                w.sim.partition_pair(nid(w.ids[s]), nid(w.ids[o]));
            }
            let committed_before = workload_ids(&w.node(s).metadata());
            // The cut-off leader appends entries it can never commit.
            let tail: Vec<u64> = (5000..5004).collect();
            w.propose_run(s, tail.iter().copied(), "uncommitted tail");
            w.run(Duration::from_secs(4));
            for id in &tail {
                assert!(
                    !w.node(s).metadata().members.contains_key(&nid(*id)),
                    "write {id} must not commit without a majority; {hint}"
                );
            }
            // The majority elects a new leader and acknowledges other writes.
            let rest = others(s);
            let new_leader = w.leader_among(&rest);
            let lost: Vec<u64> = (6000..6003).collect();
            w.propose_run(new_leader, lost.iter().copied(), "acked-but-unseen writes");
            w.run(Duration::from_secs(2));
            for id in &lost {
                assert!(
                    w.node(new_leader)
                        .metadata()
                        .members
                        .contains_key(&nid(*id)),
                    "the majority must acknowledge write {id}; {hint}"
                );
                acked_lost.insert(nid(*id));
            }
            let mut expected = committed_before;
            expected.extend(tail.iter().map(|id| nid(*id)));
            (s, expected)
        }
    };
    let old_term = w.node(survivor).term();
    w.stop_all();
    w.heal_all();
    Prepared {
        world: w,
        survivor,
        expected,
        acked_lost,
        old_term,
    }
}

/// Run the whole recovery and every assertion for one cell and seed.
fn run_cell(cell: &Cell, seed: u64) {
    let hint = replay_hint(cell.name, seed);
    let Prepared {
        mut world,
        survivor,
        expected,
        acked_lost,
        old_term,
    } = prepare(cell, seed);
    let s = survivor;
    let sid = world.ids[s];

    // ---- the tool: a dry-run plan is read-only; apply rewrites the WAL ----
    let before = world.read_wal(s);
    let plan = world
        .plan_task(s)
        .unwrap_or_else(|e| panic!("plan: {e}; {hint}"));
    assert_eq!(
        world.read_wal(s),
        before,
        "plan must not write anything; {hint}"
    );
    assert_eq!(plan.node, nid(sid), "{hint}");
    assert_eq!(
        plan.old_voters,
        world.voters.iter().cloned().collect(),
        "{hint}"
    );
    assert!(plan.old_learners.is_empty(), "{hint}");
    assert!(!plan.already_single_voter, "{hint}");
    assert!(
        plan.new_term >= plan.highest_known_term + RECOVERY_TERM_JUMP
            && plan.highest_known_term >= old_term,
        "the new term must clear everything the survivor has seen ({plan:?}); {hint}"
    );

    let outcome = world
        .apply_task(s, plan.clone())
        .unwrap_or_else(|e| panic!("apply: {e}; {hint}"));
    let backup = outcome.backup_file.clone().expect("backup written");
    let sim_env = world.sim.env(nid(sid));
    let backup_copy = {
        let env2 = sim_env.clone();
        let b = backup.clone();
        world.run_task(
            &sim_env,
            async move { env2.read(&b).await.unwrap_or_default() },
        )
    };
    assert_eq!(
        backup_copy, before,
        "the backup is the pre-rewrite WAL; {hint}"
    );

    // The rewrite is two ordinary records: a term bump and a config entry.
    let after = world.read_wal(s);
    assert!(
        after.starts_with(&before),
        "records are only appended; {hint}"
    );
    let recs = PersistedState::<MetaCommand, Metadata>::decode(&after)
        .unwrap_or_else(|e| panic!("rewritten WAL decodes: {e}; {hint}"));
    let state = PersistedState::<MetaCommand, Metadata>::replay(recs);
    assert_eq!(state.term, plan.new_term, "{hint}");
    let last = state.log.last().expect("log tail");
    assert_eq!(last.config, Some(set(&[sid])), "{hint}");
    assert_eq!(last.learners, Some(BTreeSet::new()), "{hint}");
    assert_eq!(
        (last.term, last.index),
        (plan.new_term, plan.config_entry_index),
        "{hint}"
    );
    // The tag is the ordinary current WAL tag (ADR 0073: no new format).
    assert!(after.windows(4).any(|w| w == CONTROL_WAL.magic), "{hint}");

    // ---- idempotence: re-planning after the rewrite is a no-op ----
    let replan = world
        .plan_task(s)
        .unwrap_or_else(|e| panic!("replan: {e}; {hint}"));
    assert!(replan.already_single_voter, "{hint}");
    let again = world
        .apply_task(s, replan)
        .unwrap_or_else(|e| panic!("reapply: {e}; {hint}"));
    assert!(again.backup_file.is_none(), "{hint}");
    assert_eq!(
        world.read_wal(s),
        after,
        "a repeat run writes nothing; {hint}"
    );

    // ---- restart the survivor: it elects itself and serves ----
    let engine = world.engines[s].clone();
    world.start_node(s, engine);
    world.run(Duration::from_secs(3));
    {
        let n = world.node(s);
        assert!(n.is_leader(), "the survivor must elect itself; {hint}");
        assert_eq!(n.config(), set(&[sid]), "{hint}");
        assert!(n.term() > plan.new_term, "{hint}");
        assert!(n.term() > old_term, "{hint}");
        assert_eq!(
            workload_ids(&n.metadata()),
            expected,
            "exactly what the survivor held durably must be preserved; {hint}"
        );
        for lost in &acked_lost {
            assert!(
                !n.metadata().members.contains_key(lost),
                "write {lost} was never received by the survivor and cannot reappear; {hint}"
            );
        }
        assert_eq!(n.engine_applied_index(), n.commit_index(), "{hint}");
    }
    world.propose_ok(s, upsert(9000), "post-recovery write");
    world.run(Duration::from_secs(1));
    assert!(
        world.node(s).metadata().members.contains_key(&nid(9000)),
        "a new write must commit on the one-voter group; {hint}"
    );

    // ---- a stale old voter (not wiped) is fenced ----
    let candidates: Vec<usize> = (0..world.ids.len()).filter(|i| *i != s).collect();
    let stale = candidates[(seed as usize) % candidates.len()];
    let stale_engine = world.engines[stale].clone();
    world.start_node(stale, stale_engine);
    for _ in 0..100 {
        world.run(Duration::from_millis(100));
        assert!(
            !world.node(stale).is_leader() && !matches!(world.node(stale).role(), Role::Candidate),
            "a stale old voter must never lead or campaign past the survivor; {hint}"
        );
        assert!(
            world.node(s).is_leader(),
            "the survivor keeps its seat; {hint}"
        );
        assert_eq!(
            world.node(s).config(),
            set(&[sid]),
            "the stale voter is not re-admitted; {hint}"
        );
    }
    // Deliberately NOT asserted: the stale voter's *own* applied state. It
    // keeps whatever it had applied under the old configuration (in the
    // lagging/uncommitted cells that includes writes the recovered group does
    // not have), which is exactly why ADR 0077 requires such a node to be
    // wiped before it is ever re-admitted. What is fenced is its effect on
    // the recovered group.
    assert!(
        world.node(stale).removed_by_leader() || world.node(stale).config() == set(&[sid]),
        "the survivor must have told the stale voter it was removed (a Removed notice, or the \
         configuration entry itself replicated to it); {hint}"
    );
    assert_eq!(
        world.node(stale).term(),
        world.node(s).term(),
        "the stale voter adopted the survivor's term; {hint}"
    );
    assert_eq!(
        workload_ids(&world.node(s).metadata()).len(),
        expected.len() + 1,
        "the stale voter changed nothing on the survivor; {hint}"
    );
    world.sim.stop(nid(world.ids[stale]));
    world.nodes[stale] = None;

    // ---- a wiped old voter rejoins through the learner path ----
    let wiped = *candidates.iter().find(|i| **i != stale).unwrap();
    let wid = world.ids[wiped];
    world.sim.wipe_disk(nid(wid));
    world.start_node(wiped, MemoryEngine::new());
    world.run(Duration::from_secs(1));
    assert!(
        !world.node(wiped).refused_as_voter(),
        "a wiped voter the survivor does not name resolves as fresh; {hint}"
    );
    assert!(
        matches!(
            world.node(s).add_learner(nid(wid)),
            ProposeResult::Accepted { .. }
        ),
        "{hint}"
    );
    let mut caught_up = false;
    for _ in 0..100 {
        world.run(Duration::from_millis(100));
        if world.node(s).learner_caught_up(&nid(wid), 0) {
            caught_up = true;
            break;
        }
    }
    assert!(
        caught_up,
        "the wiped voter must catch up as a learner; {hint}"
    );
    assert!(
        matches!(
            world.node(s).promote_learner(nid(wid)),
            ProposeResult::Accepted { .. }
        ),
        "{hint}"
    );
    world.run(Duration::from_secs(3));
    assert_eq!(world.node(s).config(), set(&[sid, wid]), "{hint}");
    assert_eq!(world.node(wiped).config(), set(&[sid, wid]), "{hint}");
    world.propose_ok(s, upsert(9001), "write with the regrown group");
    world.run(Duration::from_secs(2));
    for i in [s, wiped] {
        let md = world.node(i).metadata();
        assert!(
            md.members.contains_key(&nid(9001)),
            "node {i} sees the new write; {hint}"
        );
        let mut want = expected.clone();
        want.insert(nid(9000));
        want.insert(nid(9001));
        assert_eq!(
            workload_ids(&md),
            want,
            "node {i} converged on the recovered state; {hint}"
        );
    }
}

#[test]
fn force_new_configuration_recovers_from_one_survivor() {
    for cell in CELLS {
        for seed in seeds(cell.name) {
            run_cell(cell, seed);
        }
    }
}

/// Negative control: the same loss with the tool *not* run. A lone survivor
/// of a 3-voter configuration cannot elect itself or commit anything, so the
/// "serves" assertions above cannot pass by accident.
#[test]
fn negative_control_without_the_tool_a_lone_survivor_cannot_serve() {
    let cell = &CELLS[0];
    for seed in seeds("negative_untouched") {
        let hint = replay_hint("negative_untouched", seed);
        let Prepared {
            mut world,
            survivor,
            ..
        } = prepare(cell, seed);
        let engine = world.engines[survivor].clone();
        world.start_node(survivor, engine);
        world.run(Duration::from_secs(15));
        let n = world.node(survivor);
        assert!(!n.is_leader(), "without the tool nobody can lead; {hint}");
        let before = n.commit_index();
        let _ = n.propose(upsert(9000));
        world.run(Duration::from_secs(3));
        assert_eq!(
            world.node(survivor).commit_index(),
            before,
            "nothing commits without a majority; {hint}"
        );
        assert!(
            !world
                .node(survivor)
                .metadata()
                .members
                .contains_key(&nid(9000)),
            "{hint}"
        );
    }
}

/// An empty WAL (a blank or wiped disk) must never become a one-node
/// cluster, and the refusal writes nothing.
#[test]
fn refuses_an_empty_wal_and_writes_nothing() {
    let seed = corpus::name_seed("force_new_configuration_empty_wal");
    let hint = replay_hint("empty_wal", seed);
    let mut w = World::new(seed, 3, hint.clone());
    w.run(Duration::from_secs(2));
    w.stop_all();
    w.sim.wipe_disk(nid(0));
    match w.plan_task(0) {
        Err(ForceConfigError::EmptyState) => {}
        other => panic!("expected EmptyState, got {other:?}; {hint}"),
    }
    assert!(w.read_wal(0).is_empty(), "{hint}");
}

/// A node that is not a voter in its own recorded configuration is refused.
#[test]
fn refuses_a_node_that_is_not_a_voter() {
    let seed = corpus::name_seed("force_new_configuration_not_a_voter");
    let hint = replay_hint("not_a_voter", seed);
    let mut w = World::new(seed, 3, hint.clone());
    w.run(Duration::from_secs(2));
    let leader = w.leader_among(&[0, 1, 2]);
    w.propose_run(leader, BASE..BASE + 5, "workload");
    w.run(Duration::from_secs(2));
    w.stop_all();
    // Plan from node 0's disk but with a voter list that excludes it AND a
    // WAL with no config entries: the initial config is what decides.
    let env = w.sim.env(nid(0));
    let env2 = env.clone();
    let others = vec![nid(1), nid(2)];
    let res = w.run_task(&env, async move {
        recover::plan::<_, MetaCommand, Metadata>(&env2, CONTROL_WAL_FILE, &others).await
    });
    match res {
        Err(ForceConfigError::NotAVoter { node, .. }) => assert_eq!(node, nid(0), "{hint}"),
        other => panic!("expected NotAVoter, got {other:?}; {hint}"),
    }
}

/// The tool is crash-safe: an `fsync` failure after the append (the process
/// then exits, dropping un-synced bytes) leaves the original WAL, and a
/// re-run completes the recovery.
#[test]
fn an_interrupted_apply_leaves_the_original_and_can_be_rerun() {
    let cell = &CELLS[0];
    for seed in seeds("interrupted_apply") {
        let hint = replay_hint("interrupted_apply", seed);
        let Prepared {
            mut world,
            survivor: s,
            expected,
            ..
        } = prepare(cell, seed);
        let sid = world.ids[s];
        let original = world.read_wal(s);
        let plan = world
            .plan_task(s)
            .unwrap_or_else(|e| panic!("plan: {e}; {hint}"));

        let mut broken = animus_sim::DiskConfig::default();
        broken.set_sync_error_prob(1.0);
        world.sim.set_disk_config_for(nid(sid), broken);
        let failed = world.apply_task(s, plan);
        assert!(
            matches!(failed, Err(ForceConfigError::Io(_))),
            "the injected fsync failure must surface as an I/O error, got {failed:?}; {hint}"
        );
        // The process dies: un-synced bytes are gone.
        world.sim.stop(nid(sid));
        world
            .sim
            .set_disk_config_for(nid(sid), animus_sim::DiskConfig::default());
        assert_eq!(
            world.read_wal(s),
            original,
            "an unsynced rewrite must not survive the exit; {hint}"
        );

        let plan = world
            .plan_task(s)
            .unwrap_or_else(|e| panic!("re-plan: {e}; {hint}"));
        assert!(!plan.already_single_voter, "{hint}");
        world
            .apply_task(s, plan)
            .unwrap_or_else(|e| panic!("re-apply: {e}; {hint}"));
        let engine = world.engines[s].clone();
        world.start_node(s, engine);
        world.run(Duration::from_secs(3));
        assert!(world.node(s).is_leader(), "{hint}");
        assert_eq!(workload_ids(&world.node(s).metadata()), expected, "{hint}");
    }
}
