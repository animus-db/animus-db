//! **In-place split, group-mint-at-apply, end to end** (ADR 0058 Train 2 rung
//! 3) — the per-node tablet-host reconciler drives Stages 1 (learner add),
//! 2 (ordinary Raft catch-up), 3 (the atomic fork + local materialization),
//! and 5 (post-cutover trim) purely through `animus_cp_data::host`, with no
//! dependency on `animus-control`/`animusd`: `MetadataView`/
//! `Tablet::inplace_split` are constructed directly here, standing in for
//! what `MetaCommand::BeginSplitInPlace`/`CutoverSplit` would produce — the
//! identical "this module doesn't need a live control-plane `RaftNode`"
//! posture `tests/reconciler_corpus.rs` already takes for the ordinary
//! lifecycle.
//!
//! **Harness shape**, borrowed from `tests/reconciler_corpus.rs` (kept small
//! and self-contained here rather than shared, since integration test
//! binaries can't share private items): one [`Simulator`], a small
//! [`Cluster`] of `Reconciler<SimEnv, MemoryEngine>` instances (one per node
//! id, each its own `MemoryTabletEngines` registry standing in for a
//! durable per-node engine store), each scenario running as a spawned task
//! polled to completion — never a bare `block_on` of a tick that might tear
//! a group down (`Reconciler::teardown` polls `env.sleep()` internally,
//! which only resolves while `Simulator::run_for` is advancing virtual
//! time).
//!
//! Depth knob: `ANIMUS_INPLACE_SPLIT_SEEDS` (default 1), following the house
//! corpus convention (ADR 0014).

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::host::{MemoryTabletEngines, MetadataView, Reconciler};
use animus_cp_data::{KIND_BASE, KIND_CHANGE, KIND_CURSOR, RaftKvNode};
use animus_env::{Clock, EnvExt, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{InPlaceSplitIntent, KeyRange, SplitChild, Tablet, TabletId, TabletState};
use animus_test::corpus::{self, SeedVariant};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;
type Recon = Reconciler<SimEnv, MemoryEngine>;

const TABLE: &str = "t";
const PARENT: TabletId = TabletId(1);
const LEFT: TabletId = TabletId(2);
const RIGHT: TabletId = TabletId(3);

const SCENARIO_BUDGET: Duration = Duration::from_secs(150);
const SCENARIO_STEP: Duration = Duration::from_secs(1);

fn driver_id() -> NodeId {
    nid(900)
}
fn n(i: u64) -> NodeId {
    nid(i)
}

fn split_key() -> Vec<u8> {
    b"m".to_vec()
}

fn physical(key: &[u8]) -> Vec<u8> {
    let mut out = vec![KIND_BASE];
    out.extend_from_slice(key);
    out
}

fn intent(left_homes: Vec<NodeId>, right_homes: Vec<NodeId>) -> InPlaceSplitIntent {
    InPlaceSplitIntent {
        split_key: split_key(),
        children: [
            SplitChild {
                id: LEFT,
                replicas: left_homes,
            },
            SplitChild {
                id: RIGHT,
                replicas: right_homes,
            },
        ],
    }
}

fn parent_tablet(replicas: Vec<NodeId>, split: Option<InPlaceSplitIntent>) -> Tablet {
    let mut t = Tablet::new_for_table(PARENT, TABLE, KeyRange::whole(), replicas);
    if split.is_some() {
        t.state = TabletState::Splitting;
    }
    t.inplace_split = split;
    t
}

/// The post-cutover view: parent gone, both children `Active` at their own
/// FINAL replica sets — exactly what `MetaCommand::CutoverSplit`'s in-place
/// branch would produce.
fn cutover_view(left_final: Vec<NodeId>, right_final: Vec<NodeId>) -> MetadataView {
    let (left_range, right_range) = KeyRange::whole().split_at(&split_key()).expect("splits");
    view([
        Tablet::new_for_table(LEFT, TABLE, left_range, left_final),
        Tablet::new_for_table(RIGHT, TABLE, right_range, right_final),
    ])
}

fn view(tablets: impl IntoIterator<Item = Tablet>) -> MetadataView {
    MetadataView {
        tablets: tablets.into_iter().map(|t| (t.id, t)).collect(),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Harness (mirrors tests/reconciler_corpus.rs; kept self-contained here).
// ---------------------------------------------------------------------------

struct ClusterNode {
    reconciler: Recon,
    engines: MemoryTabletEngines,
}

struct Cluster {
    sim: Simulator,
    nodes: BTreeMap<NodeId, ClusterNode>,
}

impl Cluster {
    fn new(sim: Simulator) -> Self {
        Cluster {
            sim,
            nodes: BTreeMap::new(),
        }
    }

    fn add_node(&mut self, id: NodeId) {
        self.add_node_with_storage(id, MemoryTabletEngines::new());
    }

    fn add_node_with_storage(&mut self, id: NodeId, engines: MemoryTabletEngines) {
        let reconciler: Recon = Reconciler::new(
            self.sim.env(id.clone()),
            engines.clone(),
            id.clone(),
            |_, _| {},
            |_| {},
        );
        self.nodes.insert(
            id,
            ClusterNode {
                reconciler,
                engines,
            },
        );
    }

    /// Simulate a process crash+restart (mirrors `reconciler_corpus.rs`'s
    /// own `crash_restart`): kills every task this node owns and rebuilds a
    /// fresh `Reconciler` over the SAME `MemoryTabletEngines` registry — the
    /// durable-engine-survives-a-crash stand-in.
    fn crash_restart(&mut self, id: NodeId) {
        self.sim.stop(id.clone());
        let engines = self.nodes.remove(&id).expect("node exists").engines;
        self.add_node_with_storage(id, engines);
    }

    async fn tick(&mut self, id: NodeId, view: &MetadataView) {
        self.nodes
            .get_mut(&id)
            .expect("node exists")
            .reconciler
            .tick(view)
            .await;
    }

    async fn tick_all(&mut self, ids: &[NodeId], view: &MetadataView) {
        for id in ids {
            self.tick(id.clone(), view).await;
        }
    }

    fn node(&self, id: NodeId) -> &Recon {
        &self.nodes[&id].reconciler
    }

    fn storage(&self, id: NodeId, tablet: TabletId) -> MemoryEngine {
        self.nodes[&id].engines.engine(tablet)
    }

    fn hosted_set(&self, id: NodeId) -> BTreeSet<TabletId> {
        self.node(id).local_state().hosted.clone()
    }
}

fn poll_until(
    sim: &mut Simulator,
    budget: Duration,
    step: Duration,
    msg: &str,
    mut check: impl FnMut() -> bool,
) {
    let mut waited = Duration::ZERO;
    while waited < budget {
        sim.run_for(step);
        waited += step;
        if check() {
            return;
        }
    }
    panic!("{msg} (seed={})", sim.seed());
}

fn run<F, Fut>(seed: u64, body: F)
where
    F: FnOnce(Simulator) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut sim = Simulator::new(seed);
    let driver_env = sim.env(driver_id());
    let done = Arc::new(Mutex::new(false));
    let done2 = Arc::clone(&done);
    let sim_in_task = sim.clone();
    driver_env.spawn_task(async move {
        body(sim_in_task).await;
        *done2.lock().unwrap() = true;
    });
    poll_until(
        &mut sim,
        SCENARIO_BUDGET,
        SCENARIO_STEP,
        "scenario never completed",
        move || *done.lock().unwrap(),
    );
}

/// Tick every node in `ids` against `view` in a bounded loop until `check`
/// holds — the shared "drive the reconciler fleet to some Raft-timing-
/// dependent condition" idiom every scenario below uses (learner catch-up,
/// the fork applying, the cutover-driven trim), rather than a fixed tick
/// count.
async fn converge(
    c: &mut Cluster,
    env: &SimEnv,
    ids: &[NodeId],
    view: &MetadataView,
    mut check: impl FnMut(&Cluster) -> bool,
) -> bool {
    for _ in 0..300 {
        c.tick_all(ids, view).await;
        if check(c) {
            return true;
        }
        env.sleep(Duration::from_millis(100)).await;
    }
    check(c)
}

fn leader_of<'a>(c: &'a Cluster, ids: &[NodeId], tablet: TabletId) -> Option<&'a KvNode> {
    ids.iter().find_map(|id| {
        c.node(id.clone())
            .hosted_node(tablet)
            .filter(|h| h.is_leader())
    })
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

/// The full happy path: parent {p0,p1,p2}, left child final homes
/// {p0,p1,d3}, right child final homes {p1,p2,d4} — d3/d4 are genuinely new
/// nodes, so Stage 1 must add real learners and Stage 2 must genuinely catch
/// them up before Stage 3 forks. Asserts: every stage fires in order, both
/// children materialize on EVERY fork participant (not just their own final
/// homes — the over-replication Stage 5 depends on), pre-fork data lands in
/// exactly the right child, children are born with EMPTY change/cursor
/// scopes, and the post-cutover `Reconfigure` trims each child down to its
/// own final placement.
fn scenario_happy_path(seed: u64) {
    run(seed, move |sim| async move {
        let env = sim.env(driver_id());
        let mut c = Cluster::new(sim.clone());
        let (p0, p1, p2, d3, d4) = (n(0), n(1), n(2), n(3), n(4));
        let parents = [p0.clone(), p1.clone(), p2.clone()];
        for id in [p0.clone(), p1.clone(), p2.clone(), d3.clone(), d4.clone()] {
            c.add_node(id);
        }
        let left_homes = vec![p0.clone(), p1.clone(), d3.clone()];
        let right_homes = vec![p1.clone(), p2.clone(), d4.clone()];
        let base_view = view([parent_tablet(
            vec![p0.clone(), p1.clone(), p2.clone()],
            None,
        )]);

        // Stand up + elect the parent, then write some pre-fork data.
        let elected = converge(&mut c, &env, &parents, &base_view, |c| {
            leader_of(c, &parents, PARENT).is_some()
        })
        .await;
        assert!(elected, "parent never elected (seed={seed})");
        {
            let leader = leader_of(&c, &parents, PARENT).expect("elected above");
            match leader.put(b"left-key".to_vec(), b"lv".to_vec()) {
                ProposeResult::Accepted { .. } => {}
                other => panic!("pre-fork left-key put rejected: {other:?} (seed={seed})"),
            }
            match leader.put(b"z-right-key".to_vec(), b"rv".to_vec()) {
                ProposeResult::Accepted { .. } => {}
                other => panic!("pre-fork right-key put rejected: {other:?} (seed={seed})"),
            }
        }
        env.sleep(Duration::from_millis(500)).await;

        // Stage 1/2: publish the in-place split intent. d3/d4 must
        // start hosting the parent (as quiet non-voters, then real
        // learners) and catch up; the ORIGINAL parent nodes must NOT
        // release them as stray "not in replicas" tablets.
        let all_participants = [p0.clone(), p1.clone(), p2.clone(), d3.clone(), d4.clone()];
        let pending_view = view([parent_tablet(
            vec![p0.clone(), p1.clone(), p2.clone()],
            Some(intent(left_homes.clone(), right_homes.clone())),
        )]);
        let forked = converge(&mut c, &env, &all_participants, &pending_view, |c| {
            [p0.clone(), p1.clone(), p2.clone(), d3.clone(), d4.clone()]
                .iter()
                .all(|id| {
                    c.node(id.clone())
                        .hosted_node(PARENT)
                        .is_some_and(|h| block_on(h.pending_split()).is_some())
                })
        })
        .await;
        assert!(
            forked,
            "the in-place split never forked on every participant (seed={seed})"
        );

        // Stage 3 materialization: both children must appear on EVERY
        // fork participant (over-replication by construction), not just
        // their own final homes.
        let materialized = converge(&mut c, &env, &all_participants, &pending_view, |c| {
            all_participants.iter().all(|id| {
                let hosted = c.hosted_set(id.clone());
                hosted.contains(&LEFT) && hosted.contains(&RIGHT)
            })
        })
        .await;
        assert!(
            materialized,
            "both children did not materialize on every fork participant (seed={seed})"
        );

        // Data property: pre-fork writes land in exactly the right
        // child, on every participant's own local clone.
        for id in &all_participants {
            let left_engine = c.storage(id.clone(), LEFT);
            let right_engine = c.storage(id.clone(), RIGHT);
            assert!(
                left_engine
                    .get(&physical(b"left-key"))
                    .await
                    .unwrap()
                    .is_some(),
                "node {id}: left-key missing from LEFT child (seed={seed})"
            );
            assert!(
                left_engine
                    .get(&physical(b"z-right-key"))
                    .await
                    .unwrap()
                    .is_none(),
                "node {id}: z-right-key leaked into LEFT child (seed={seed})"
            );
            assert!(
                right_engine
                    .get(&physical(b"z-right-key"))
                    .await
                    .unwrap()
                    .is_some(),
                "node {id}: z-right-key missing from RIGHT child (seed={seed})"
            );
            assert!(
                right_engine
                    .get(&physical(b"left-key"))
                    .await
                    .unwrap()
                    .is_none(),
                "node {id}: left-key leaked into RIGHT child (seed={seed})"
            );
            // ADR 0050's copy-kinds rule: children are born with EMPTY
            // change logs and cursors, on BOTH children, on every
            // participant.
            for (engine, side) in [(&left_engine, "LEFT"), (&right_engine, "RIGHT")] {
                let change = engine.entries().await.unwrap();
                let leaked_change = change.iter().any(|(k, _)| {
                    k.first() == Some(&KIND_CHANGE) || k.first() == Some(&KIND_CURSOR)
                });
                assert!(
                    !leaked_change,
                    "node {id}: {side} child was born with a non-empty change/cursor scope (seed={seed})"
                );
            }
        }

        // Stage 4/5: cutover, then confirm the ordinary post-cutover
        // Reconfigure trims each child down to its own FINAL placement
        // (over-replicated on the union right after the fork).
        let post = cutover_view(left_homes.clone(), right_homes.clone());
        let converged = converge(&mut c, &env, &all_participants, &post, |c| {
            let left_ok = left_homes
                .iter()
                .all(|id| c.node(id.clone()).hosted_node(LEFT).is_some());
            let right_ok = right_homes
                .iter()
                .all(|id| c.node(id.clone()).hosted_node(RIGHT).is_some());
            let left_leader_config = leader_of(c, &left_homes, LEFT).map(|h| h.config());
            let right_leader_config = leader_of(c, &right_homes, RIGHT).map(|h| h.config());
            left_ok
                && right_ok
                && left_leader_config.as_ref()
                    == Some(&left_homes.iter().cloned().collect::<BTreeSet<_>>())
                && right_leader_config.as_ref()
                    == Some(&right_homes.iter().cloned().collect::<BTreeSet<_>>())
        })
        .await;
        assert!(
            converged,
            "post-cutover reconfigure never trimmed each child to its own final placement \
                 (seed={seed})"
        );

        // The retired parent is reclaimed everywhere (the ordinary
        // hosted-but-absent-from-Metadata path, unmodified).
        let reclaimed = converge(&mut c, &env, &all_participants, &post, |c| {
            all_participants
                .iter()
                .all(|id| !c.hosted_set(id.clone()).contains(&PARENT))
        })
        .await;
        assert!(
            reclaimed,
            "the retired parent was never reclaimed everywhere (seed={seed})"
        );
    });
}

/// A leader crash mid-learner-catch-up (Stage 1/2): the split must not
/// wedge — a new leader picks up where the old one left off (the SAME
/// intent, read fresh from `Metadata` every tick) and the fork still
/// eventually completes.
fn scenario_leader_crash_mid_catch_up(seed: u64) {
    run(seed, move |sim| async move {
        let env = sim.env(driver_id());
        let mut c = Cluster::new(sim.clone());
        let (p0, p1, p2, d3) = (n(10), n(11), n(12), n(13));
        let parents = [p0.clone(), p1.clone(), p2.clone()];
        for id in [p0.clone(), p1.clone(), p2.clone(), d3.clone()] {
            c.add_node(id);
        }
        // A single child (LEFT) suffices to exercise a leader crash during
        // its own learner catch-up; RIGHT's homes are the original parent
        // voters (trivially "ready", no learner needed) so the scenario
        // stays focused on the one property under test.
        let left_homes = vec![p0.clone(), p1.clone(), d3.clone()];
        let right_homes = vec![p0.clone(), p1.clone(), p2.clone()];
        let all = [p0.clone(), p1.clone(), p2.clone(), d3.clone()];
        let base_view = view([parent_tablet(
            vec![p0.clone(), p1.clone(), p2.clone()],
            None,
        )]);
        assert!(
            converge(&mut c, &env, &parents, &base_view, |c| leader_of(
                c, &parents, PARENT
            )
            .is_some())
            .await,
            "parent never elected (seed={seed})"
        );

        let pending_view = view([parent_tablet(
            vec![p0.clone(), p1.clone(), p2.clone()],
            Some(intent(left_homes.clone(), right_homes.clone())),
        )]);
        // Drive a FEW ticks (enough to add the learner, not enough to catch
        // it up or fork), then crash whichever original node currently
        // leads.
        for _ in 0..3 {
            c.tick_all(&all, &pending_view).await;
            env.sleep(Duration::from_millis(50)).await;
        }
        if let Some(leader_id) = parents
            .iter()
            .find(|id| {
                c.node((*id).clone())
                    .hosted_node(PARENT)
                    .is_some_and(|h| h.is_leader())
            })
            .cloned()
        {
            c.crash_restart(leader_id);
        }

        let forked = converge(&mut c, &env, &all, &pending_view, |c| {
            all.iter().all(|id| {
                c.node(id.clone())
                    .hosted_node(PARENT)
                    .is_some_and(|h| block_on(h.pending_split()).is_some())
            })
        })
        .await;
        assert!(
            forked,
            "the split never recovered from a leader crash mid-catch-up (seed={seed})"
        );
    });
}

/// **The fast path (ADR 0058 Train 2 rung 4)**: right after materialization,
/// the replica that was the parent's leader at the fork campaigns
/// immediately in both children — so a leader emerges within a handful of
/// virtual milliseconds, far short of a freshly-bootstrapped group's own
/// randomized election timeout (150ms base). Every home here is an original
/// parent voter, so materialization fires on the very first pending tick
/// with no learner catch-up in the way, keeping the scenario focused
/// purely on this property (and distinct from `scenario_happy_path`, which
/// exercises the fast path only incidentally while asserting the rest of
/// the workflow).
fn scenario_campaigning_replica_wins_leadership_almost_immediately(seed: u64) {
    run(seed, move |sim| async move {
        let env = sim.env(driver_id());
        let mut c = Cluster::new(sim.clone());
        let (p0, p1, p2) = (n(50), n(51), n(52));
        let parents = [p0.clone(), p1.clone(), p2.clone()];
        for id in parents.iter().cloned() {
            c.add_node(id);
        }
        let homes = vec![p0.clone(), p1.clone(), p2.clone()];
        let base_view = view([parent_tablet(homes.clone(), None)]);
        assert!(
            converge(&mut c, &env, &parents, &base_view, |c| {
                leader_of(c, &parents, PARENT).is_some()
            })
            .await,
            "parent never elected (seed={seed})"
        );

        let pending_view = view([parent_tablet(
            homes.clone(),
            Some(intent(homes.clone(), homes.clone())),
        )]);
        assert!(
            converge(&mut c, &env, &parents, &pending_view, |c| {
                parents.iter().all(|id| {
                    let hosted = c.hosted_set(id.clone());
                    hosted.contains(&LEFT) && hosted.contains(&RIGHT)
                })
            })
            .await,
            "both children never materialized on every fork participant (seed={seed})"
        );

        // The property under test: a handful of milliseconds of virtual
        // time after materialization — far short of a fresh group's own
        // 150ms randomized election-timeout base — is already enough for
        // both children to have a leader, because `start_hosted_campaigning`
        // solicited pre-votes at construction instead of waiting one out.
        env.sleep(Duration::from_millis(20)).await;
        c.tick_all(&parents, &pending_view).await;
        assert!(
            leader_of(&c, &parents, LEFT).is_some(),
            "LEFT never elected a leader within 20ms of materializing — the immediate-campaign \
             fast path did not fire (seed={seed})"
        );
        assert!(
            leader_of(&c, &parents, RIGHT).is_some(),
            "RIGHT never elected a leader within 20ms of materializing — the immediate-campaign \
             fast path did not fire (seed={seed})"
        );
    });
}

/// **The fallback path (ADR 0058 Train 2 rung 4)**: the parent's leader
/// crashes at the exact instant of the fork — before its own reconciler (or
/// anyone else's) ever ticks past it — so no replica's materialize action
/// ever campaigns for either child (every survivor's own `TabletFacts::
/// is_leader` for the frozen parent reads `false`, since neither has had a
/// chance to notice the leader is gone and campaign for the PARENT itself,
/// let alone the children). The split must still complete and both children
/// must still elect leaders, purely via the untouched ordinary
/// randomized-timeout path — the "nothing breaks" half of this rung's own
/// contract. Every home is an original parent voter, so the fork can be
/// proposed directly (bypassing the reconciler entirely, mirroring
/// `scenario_crash_between_fork_and_materialization`'s own raw-API
/// technique) with no learner catch-up needed.
fn scenario_parent_leader_crash_at_fork_falls_back_to_ordinary_election(seed: u64) {
    run(seed, move |sim| async move {
        let env = sim.env(driver_id());
        let mut c = Cluster::new(sim.clone());
        let (p0, p1, p2) = (n(60), n(61), n(62));
        let parents = [p0.clone(), p1.clone(), p2.clone()];
        for id in parents.iter().cloned() {
            c.add_node(id);
        }
        let homes = vec![p0.clone(), p1.clone(), p2.clone()];
        let base_view = view([parent_tablet(homes.clone(), None)]);
        assert!(
            converge(&mut c, &env, &parents, &base_view, |c| {
                leader_of(c, &parents, PARENT).is_some()
            })
            .await,
            "parent never elected (seed={seed})"
        );

        // Force leadership onto p0 so this scenario is deterministic
        // regardless of which node Raft happened to elect above (mirrors
        // `scenario_crash_between_fork_and_materialization`'s identical
        // technique).
        let mut on_p0 = c
            .node(p0.clone())
            .hosted_node(PARENT)
            .is_some_and(|h| h.is_leader());
        for _ in 0..150 {
            if on_p0 {
                break;
            }
            if let Some(leader) = leader_of(&c, &parents, PARENT) {
                leader.transfer_leadership(p0.clone());
            }
            env.sleep(Duration::from_millis(100)).await;
            on_p0 = c
                .node(p0.clone())
                .hosted_node(PARENT)
                .is_some_and(|h| h.is_leader());
        }
        assert!(
            on_p0,
            "could not force leadership onto p0 before the crash (seed={seed})"
        );

        // Propose the fork directly via the raw API, entirely independent
        // of any reconciler tick — nothing has had a chance to materialize
        // (or campaign) on anyone yet.
        let children = [
            SplitChild {
                id: LEFT,
                replicas: homes.clone(),
            },
            SplitChild {
                id: RIGHT,
                replicas: homes.clone(),
            },
        ];
        {
            let leader = c
                .node(p0.clone())
                .hosted_node(PARENT)
                .expect("p0 leads, forced above");
            match leader.propose_split_tablet(split_key(), children) {
                ProposeResult::Accepted { .. } => {}
                other => panic!("propose_split_tablet rejected: {other:?} (seed={seed})"),
            }
        }
        // Let the fork replicate+apply on ALL THREE nodes via ordinary
        // background Raft traffic ONLY, in small enough increments that we
        // never risk crossing an election timeout before every replica has
        // observed it — no reconciler tick has run yet, so nothing has
        // materialized or campaigned anywhere.
        let mut all_forked = false;
        for _ in 0..20 {
            env.sleep(Duration::from_millis(5)).await;
            all_forked = parents.iter().all(|id| {
                c.node(id.clone())
                    .hosted_node(PARENT)
                    .is_some_and(|h| block_on(h.pending_split()).is_some())
            });
            if all_forked {
                break;
            }
        }
        assert!(
            all_forked,
            "the fork never replicated+applied to every parent replica with no reconciler tick \
             involved (seed={seed})"
        );

        // The crash: p0 — the ONLY replica that has ever led this group —
        // goes away right now, with p1/p2 still believing (correctly, from
        // their own stale-but-unexpired view) that p0 is their leader.
        c.crash_restart(p0.clone());
        let survivors = [p1.clone(), p2.clone()];
        assert!(
            !survivors.iter().any(|id| {
                c.node(id.clone())
                    .hosted_node(PARENT)
                    .is_some_and(|h| h.is_leader())
            }),
            "test fixture invariant: a survivor must not already believe itself leader at the \
             instant of the crash (seed={seed})"
        );
        let pending_view = view([parent_tablet(
            homes.clone(),
            Some(intent(homes.clone(), homes.clone())),
        )]);
        // Ticking the survivors' reconcilers NOW, with zero further elapsed
        // time, is the very first time either of their `TabletFacts::
        // is_leader` for the parent is computed post-crash — it must read
        // `false` on both, so both materialize with `campaign: false`.
        c.tick_all(&survivors, &pending_view).await;
        assert!(
            !survivors.iter().any(|id| {
                c.node(id.clone())
                    .hosted_node(LEFT)
                    .is_some_and(|h| h.is_leader())
            }),
            "test fixture invariant: materializing must not itself create a leader — only the \
             untouched randomized-timeout path may (seed={seed})"
        );

        // The whole point of the fallback: both children must still,
        // eventually, elect a leader via the ordinary randomized-timeout
        // path, with no special-cased recovery.
        let elected = converge(&mut c, &env, &survivors, &pending_view, |c| {
            leader_of(c, &survivors, LEFT).is_some() && leader_of(c, &survivors, RIGHT).is_some()
        })
        .await;
        assert!(
            elected,
            "neither child ever elected a leader via the ordinary fallback after the parent \
             leader crashed exactly at the fork (seed={seed})"
        );
    });
}

/// **G4 crash idempotency**: a node crashes AFTER its parent has forked
/// locally but BEFORE it materializes the children — on restart, it must
/// independently re-derive the SAME fork (from its own durable marker) and
/// complete materialization exactly once (no double-clone, no lost child).
fn scenario_crash_between_fork_and_materialization(seed: u64) {
    run(seed, move |sim| async move {
        let env = sim.env(driver_id());
        let mut c = Cluster::new(sim.clone());
        let (p0, p1, p2) = (n(20), n(21), n(22));
        let parents = [p0.clone(), p1.clone(), p2.clone()];
        for id in parents.iter().cloned() {
            c.add_node(id);
        }
        // Every home is an original parent voter — Stage 1 has nothing to
        // add, so the fork can trigger the very first pending-tick, leaving
        // only the crash-before-materialization window to test.
        let left_homes = vec![p0.clone(), p1.clone(), p2.clone()];
        let right_homes = vec![p0.clone(), p1.clone(), p2.clone()];
        let base_view = view([parent_tablet(
            vec![p0.clone(), p1.clone(), p2.clone()],
            None,
        )]);
        assert!(
            converge(&mut c, &env, &parents, &base_view, |c| leader_of(
                c, &parents, PARENT
            )
            .is_some())
            .await,
            "parent never elected (seed={seed})"
        );
        // This scenario deliberately ticks only p0/p1's reconcilers during
        // the isolation window below — force leadership onto p0 first (Raft
        // election has no reason to prefer p0/p1 over p2 on its own) so
        // that window can actually observe a `ProposeSplitFork` action. The
        // identical bounded retry dance `reconciler_corpus.rs::
        // remove_replica_for_real` uses for a real membership change.
        let mut on_p0 = c
            .node(p0.clone())
            .hosted_node(PARENT)
            .is_some_and(|h| h.is_leader());
        for _ in 0..150 {
            if on_p0 {
                break;
            }
            if let Some(leader) = leader_of(&c, &parents, PARENT) {
                leader.transfer_leadership(p0.clone());
            }
            env.sleep(Duration::from_millis(100)).await;
            on_p0 = c
                .node(p0.clone())
                .hosted_node(PARENT)
                .is_some_and(|h| h.is_leader());
        }
        assert!(
            on_p0,
            "could not force leadership onto p0 before the isolation window (seed={seed})"
        );
        {
            let leader = leader_of(&c, &parents, PARENT).expect("elected above");
            match leader.put(b"left-key".to_vec(), b"lv".to_vec()) {
                ProposeResult::Accepted { .. } => {}
                other => panic!("pre-fork put rejected: {other:?} (seed={seed})"),
            }
        }
        env.sleep(Duration::from_millis(300)).await;

        let pending_view = view([parent_tablet(
            vec![p0.clone(), p1.clone(), p2.clone()],
            Some(intent(left_homes.clone(), right_homes.clone())),
        )]);
        // Drive ONLY p0/p1's reconcilers to propose+observe the fork —
        // deliberately never ticking p2's own reconciler here, so p2's
        // Raft group (already running since the base-view stage, entirely
        // independent of reconciler ticks) can replicate+apply the fork
        // via ordinary AppendEntries while its RECONCILER never gets a
        // chance to notice and materialize anything. This is what isolates
        // "the fork applied to p2's own engine" from "p2's LocalState acted
        // on it" — the exact G4 crash window.
        let drivers = [p0.clone(), p1.clone()];
        assert!(
            converge(&mut c, &env, &drivers, &pending_view, |c| {
                drivers.iter().all(|id| {
                    c.node(id.clone())
                        .hosted_node(PARENT)
                        .is_some_and(|h| block_on(h.pending_split()).is_some())
                })
            })
            .await,
            "the fork never applied on p0/p1 (seed={seed})"
        );
        // p2's own Raft group replicates the fork with NO reconciler
        // involvement — poll its raw accessor directly.
        let mut p2_forked = false;
        for _ in 0..100 {
            if c.node(p2.clone())
                .hosted_node(PARENT)
                .is_some_and(|h| block_on(h.pending_split()).is_some())
            {
                p2_forked = true;
                break;
            }
            env.sleep(Duration::from_millis(50)).await;
        }
        assert!(
            p2_forked,
            "p2's own Raft log never replicated the fork (seed={seed})"
        );
        assert!(
            !c.hosted_set(p2.clone()).contains(&LEFT),
            "test fixture invariant: p2's reconciler must never have ticked past the fork \
             (seed={seed})"
        );
        c.crash_restart(p2.clone());

        // p2 restarts with a FRESH LocalState (per the crash model) but the
        // SAME durable engine registry — it must re-discover the fork from
        // its own already-forked parent engine and materialize both
        // children exactly once, with the pre-fork write intact.
        let recovered = converge(&mut c, &env, &parents, &pending_view, |c| {
            let hosted = c.hosted_set(p2.clone());
            hosted.contains(&LEFT) && hosted.contains(&RIGHT)
        })
        .await;
        assert!(
            recovered,
            "p2 never re-materialized both children after a restart (seed={seed})"
        );
        let left_engine = c.storage(p2.clone(), LEFT);
        assert!(
            left_engine
                .get(&physical(b"left-key"))
                .await
                .unwrap()
                .is_some(),
            "p2's recovered LEFT child lost the pre-fork write (seed={seed})"
        );
    });
}

/// A concurrent unrelated rebalance of an ORDINARY (non-splitting) tablet
/// must not be disturbed by the split machinery, and — the direction this
/// scenario actually guards — ticking the split's own parent must never
/// route through the ordinary `Reconfigure` action (which would see the
/// split's added learner as stale and immediately try to remove it).
fn scenario_unrelated_rebalance_does_not_race_the_split(seed: u64) {
    run(seed, move |sim| async move {
        let env = sim.env(driver_id());
        let mut c = Cluster::new(sim.clone());
        let (p0, p1, p2, d3, spare) = (n(30), n(31), n(32), n(33), n(34));
        for id in [
            p0.clone(),
            p1.clone(),
            p2.clone(),
            d3.clone(),
            spare.clone(),
        ] {
            c.add_node(id);
        }
        const OTHER: TabletId = TabletId(9);
        let left_homes = vec![p0.clone(), p1.clone(), d3.clone()];
        let right_homes = vec![p0.clone(), p1.clone(), p2.clone()];
        let all = [
            p0.clone(),
            p1.clone(),
            p2.clone(),
            d3.clone(),
            spare.clone(),
        ];
        // `spare` never hosts PARENT (it's only OTHER's own replacement
        // replica) — the split-fork check below must only look at nodes
        // that actually participate in the split.
        let split_participants = [p0.clone(), p1.clone(), p2.clone(), d3.clone()];

        // An unrelated, ordinary tablet mid-rebalance (a fourth spare
        // replacing p2) ticking alongside the split — must converge
        // normally, on its own schedule, unaffected by the split's own
        // learner-add on the PARENT tablet.
        let other_tablet = |replicas: Vec<NodeId>| {
            Tablet::new_for_table(OTHER, "other", KeyRange::whole(), replicas)
        };

        // Let the parent (and OTHER, already at its final placement) form
        // and elect before introducing the split intent, mirroring the
        // other scenarios' own staging — a split intent present from a
        // cluster's very first tick is not the property this scenario is
        // about.
        let base_view = MetadataView {
            tablets: [
                (
                    PARENT,
                    parent_tablet(vec![p0.clone(), p1.clone(), p2.clone()], None),
                ),
                (
                    OTHER,
                    other_tablet(vec![p0.clone(), p1.clone(), spare.clone()]),
                ),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        assert!(
            converge(&mut c, &env, &all, &base_view, |c| {
                leader_of(c, &all, PARENT).is_some() && leader_of(c, &all, OTHER).is_some()
            })
            .await,
            "the parent and the unrelated tablet never both elected (seed={seed})"
        );

        let pending_view = MetadataView {
            tablets: [
                (
                    PARENT,
                    parent_tablet(
                        vec![p0.clone(), p1.clone(), p2.clone()],
                        Some(intent(left_homes.clone(), right_homes.clone())),
                    ),
                ),
                (
                    OTHER,
                    other_tablet(vec![p0.clone(), p1.clone(), spare.clone()]),
                ),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        assert!(
            converge(&mut c, &env, &all, &pending_view, |c| {
                split_participants.iter().all(|id| {
                    c.node(id.clone())
                        .hosted_node(PARENT)
                        .is_some_and(|h| block_on(h.pending_split()).is_some())
                })
            })
            .await,
            "the split never forked while an unrelated tablet was also converging (seed={seed})"
        );
        assert!(
            converge(&mut c, &env, &all, &pending_view, |c| {
                leader_of(c, &all, OTHER).map(|h| h.config()).as_ref()
                    == Some(
                        &[p0.clone(), p1.clone(), spare.clone()]
                            .into_iter()
                            .collect(),
                    )
            })
            .await,
            "the unrelated tablet never converged to its own desired placement while the split \
             was in progress (seed={seed})"
        );
    });
}

/// **ADR 0058 Train 2 rung 4 layer 1 — the eager wake fires with NO tick
/// involved, and a second, immediately-following tick (standing in for the
/// reconciler's own later periodic fallback tick rediscovering the
/// identical already-forked state) is a benign no-op.** Isolates p2's own
/// reconciler from ever ticking (mirroring `scenario_crash_between_fork_
/// and_materialization`'s own isolation technique) while its Raft group
/// replicates+applies the fork purely via ordinary background `AppendEntries`
/// — so `fork_wake()` resolving is the FIRST thing that ever happens to p2's
/// reconciler with respect to this split, with no tick preceding it. The
/// first tick after that must materialize both children; a second,
/// back-to-back tick right after (the double-attempt this rung's own G4
/// discipline must absorb — `EngineFactory::probe` skips the re-clone, the
/// optimistic `LocalState::hosted` claim skips the re-host) must change
/// nothing.
fn scenario_eager_wake_and_reconciler_tick_race_benignly(seed: u64) {
    run(seed, move |sim| async move {
        let env = sim.env(driver_id());
        let mut c = Cluster::new(sim.clone());
        let (p0, p1, p2) = (n(70), n(71), n(72));
        let parents = [p0.clone(), p1.clone(), p2.clone()];
        for id in parents.iter().cloned() {
            c.add_node(id);
        }
        let homes = vec![p0.clone(), p1.clone(), p2.clone()];
        let base_view = view([parent_tablet(homes.clone(), None)]);
        assert!(
            converge(&mut c, &env, &parents, &base_view, |c| leader_of(
                c, &parents, PARENT
            )
            .is_some())
            .await,
            "parent never elected (seed={seed})"
        );
        // This scenario deliberately ticks only p0/p1's reconcilers below —
        // force leadership onto p0 first (mirroring `scenario_crash_
        // between_fork_and_materialization`'s identical technique) so that
        // window can actually observe a `ProposeSplitFork` action; Raft
        // election has no reason to prefer p0/p1 over p2 on its own.
        let mut on_p0 = c
            .node(p0.clone())
            .hosted_node(PARENT)
            .is_some_and(|h| h.is_leader());
        for _ in 0..150 {
            if on_p0 {
                break;
            }
            if let Some(leader) = leader_of(&c, &parents, PARENT) {
                leader.transfer_leadership(p0.clone());
            }
            env.sleep(Duration::from_millis(100)).await;
            on_p0 = c
                .node(p0.clone())
                .hosted_node(PARENT)
                .is_some_and(|h| h.is_leader());
        }
        assert!(
            on_p0,
            "could not force leadership onto p0 before ticking only p0/p1 (seed={seed})"
        );

        let pending_view = view([parent_tablet(
            homes.clone(),
            Some(intent(homes.clone(), homes.clone())),
        )]);
        // Every home is an original parent voter, so p0's very first tick
        // proposes the fork directly (no learner catch-up needed) — drive
        // ONLY p0/p1, never p2, so p2's own Raft group applies the fork
        // purely via background replication with its reconciler an
        // untouched bystander throughout.
        let drivers = [p0.clone(), p1.clone()];
        assert!(
            converge(&mut c, &env, &drivers, &pending_view, |c| {
                drivers.iter().all(|id| {
                    c.node(id.clone())
                        .hosted_node(PARENT)
                        .is_some_and(|h| block_on(h.pending_split()).is_some())
                })
            })
            .await,
            "the fork never applied on p0/p1 (seed={seed})"
        );
        assert!(
            !c.hosted_set(p2.clone()).contains(&LEFT),
            "test fixture invariant: p2's reconciler must never have ticked past the fork \
             (seed={seed})"
        );

        // The property under test: p2's OWN fork_wake() resolves once its
        // own apply task observes the fork — with zero ticks of p2's
        // reconciler ever having run beforehand. Reduced to a plain `bool`
        // (rather than keeping the `Either` around) so the still-borrowing
        // "loser" future is dropped, and the borrow of `c` it holds
        // released, before `c` is used mutably below.
        let fork_seen = match futures::future::select(
            Box::pin(c.node(p2.clone()).fork_wake()),
            Box::pin(env.sleep(Duration::from_secs(10))),
        )
        .await
        {
            futures::future::Either::Left(_) => true,
            futures::future::Either::Right(_) => false,
        };
        assert!(
            fork_seen,
            "p2's fork_wake() never resolved even though its own Raft log already applied the \
             fork (seed={seed})"
        );

        // The eager attempt: the very first tick after the wake resolved
        // must materialize both children.
        c.tick(p2.clone(), &pending_view).await;
        let after_first = c.hosted_set(p2.clone());
        assert!(
            after_first.contains(&LEFT) && after_first.contains(&RIGHT),
            "p2 did not materialize both children on its first tick after fork_wake() resolved \
             (seed={seed})"
        );
        let left_engine = c.storage(p2.clone(), LEFT);
        let right_engine = c.storage(p2.clone(), RIGHT);

        // The race: a second, immediately-following tick — standing in for
        // the reconciler's own later periodic fallback tick rediscovering
        // the identical already-forked, already-materialized state — must
        // be a pure no-op: same hosted set, same two engines (no re-clone,
        // no double-host).
        c.tick(p2.clone(), &pending_view).await;
        let after_second = c.hosted_set(p2.clone());
        assert_eq!(
            after_first, after_second,
            "a second, immediately-following tick changed p2's hosted set — the eager/\
             reconciler double-attempt race is not benign (seed={seed})"
        );
        // `hosted_node` returning a live handle for both children after the
        // second tick (rather than, say, a torn-down-and-rehosted one) is
        // the structural half of "no double-host"; `after_first ==
        // after_second` above is the behavioral half. Cross-check the
        // engines themselves are still exactly the SAME two, not a second
        // clone silently replacing the first (which `EngineFactory::probe`
        // gating is supposed to prevent).
        assert_eq!(
            left_engine.entries_with_tombstones().await.unwrap(),
            c.storage(p2.clone(), LEFT)
                .entries_with_tombstones()
                .await
                .unwrap(),
            "LEFT's engine content changed across the double-attempt (seed={seed})"
        );
        assert_eq!(
            right_engine.entries_with_tombstones().await.unwrap(),
            c.storage(p2.clone(), RIGHT)
                .entries_with_tombstones()
                .await
                .unwrap(),
            "RIGHT's engine content changed across the double-attempt (seed={seed})"
        );
    });
}

/// **ADR 0058 Train 2 rung 4 layer 1 — the eager wake is NOT durable, and
/// recovery must never depend on it.** `ForkSignal` is a plain in-memory
/// flag+waker (`animus-cp-data/CLAUDE.md`'s own discipline: apply is sync
/// and I/O-free, so this notify is best-effort, never persisted) — a crash
/// on this replica right after its own apply task raises it, but strictly
/// BEFORE any tick (eager or otherwise) ever consumes it, discards the
/// signal along with the rest of the process's in-memory state. This
/// scenario proves that loss is harmless: on restart this replica has no
/// signal left to wait on at all, and must instead re-discover the fork the
/// ordinary way — the reconciler's ordinary periodic tick reading the
/// durable `pending_split()` marker straight off its own re-opened engine
/// (the identical G4 recovery path `scenario_crash_between_fork_and_
/// materialization` already proves; this cell's own point is narrower and
/// additive: that the LOST eager wake specifically is not on the critical
/// path for that recovery).
fn scenario_crash_after_apply_loses_the_eager_wake_but_reconciler_fallback_recovers(seed: u64) {
    run(seed, move |sim| async move {
        let env = sim.env(driver_id());
        let mut c = Cluster::new(sim.clone());
        let (p0, p1, p2) = (n(80), n(81), n(82));
        let parents = [p0.clone(), p1.clone(), p2.clone()];
        for id in parents.iter().cloned() {
            c.add_node(id);
        }
        let homes = vec![p0.clone(), p1.clone(), p2.clone()];
        let base_view = view([parent_tablet(homes.clone(), None)]);
        assert!(
            converge(&mut c, &env, &parents, &base_view, |c| leader_of(
                c, &parents, PARENT
            )
            .is_some())
            .await,
            "parent never elected (seed={seed})"
        );
        // This scenario deliberately ticks only p0/p1's reconcilers below —
        // force leadership onto p0 first (mirroring `scenario_crash_
        // between_fork_and_materialization`'s identical technique) so that
        // window can actually observe a `ProposeSplitFork` action.
        let mut on_p0 = c
            .node(p0.clone())
            .hosted_node(PARENT)
            .is_some_and(|h| h.is_leader());
        for _ in 0..150 {
            if on_p0 {
                break;
            }
            if let Some(leader) = leader_of(&c, &parents, PARENT) {
                leader.transfer_leadership(p0.clone());
            }
            env.sleep(Duration::from_millis(100)).await;
            on_p0 = c
                .node(p0.clone())
                .hosted_node(PARENT)
                .is_some_and(|h| h.is_leader());
        }
        assert!(
            on_p0,
            "could not force leadership onto p0 before ticking only p0/p1 (seed={seed})"
        );

        {
            let leader = leader_of(&c, &parents, PARENT).expect("elected above");
            match leader.put(b"left-key".to_vec(), b"lv".to_vec()) {
                ProposeResult::Accepted { .. } => {}
                other => panic!("pre-fork put rejected: {other:?} (seed={seed})"),
            }
        }
        env.sleep(Duration::from_millis(300)).await;

        let pending_view = view([parent_tablet(
            homes.clone(),
            Some(intent(homes.clone(), homes.clone())),
        )]);
        let drivers = [p0.clone(), p1.clone()];
        assert!(
            converge(&mut c, &env, &drivers, &pending_view, |c| {
                drivers.iter().all(|id| {
                    c.node(id.clone())
                        .hosted_node(PARENT)
                        .is_some_and(|h| block_on(h.pending_split()).is_some())
                })
            })
            .await,
            "the fork never applied on p0/p1 (seed={seed})"
        );
        // p2's own Raft group replicates+applies the fork purely via
        // background `AppendEntries` — which is also where its own apply
        // task raises (and, since nobody ever polls it, strands) the eager
        // `ForkSignal` this scenario is about losing.
        let mut p2_forked = false;
        for _ in 0..100 {
            if c.node(p2.clone())
                .hosted_node(PARENT)
                .is_some_and(|h| block_on(h.pending_split()).is_some())
            {
                p2_forked = true;
                break;
            }
            env.sleep(Duration::from_millis(50)).await;
        }
        assert!(
            p2_forked,
            "p2's own Raft log never replicated the fork (seed={seed})"
        );
        assert!(
            !c.hosted_set(p2.clone()).contains(&LEFT),
            "test fixture invariant: p2's reconciler must never have ticked past the fork — the \
             eager wake must still be sitting unconsumed at the moment of the crash below \
             (seed={seed})"
        );

        // The crash: p2's entire process (including its in-memory, never-
        // durable `ForkSignal`) is gone. `crash_restart` rebuilds a fresh
        // `Reconciler`/`RaftKvNode` over the SAME durable engine registry —
        // there is no signal left for the new instance to inherit; the only
        // way it can ever learn the fork happened is the marker `split.rs`
        // wrote into its own (durable) engine.
        c.crash_restart(p2.clone());
        assert!(
            !c.hosted_set(p2.clone()).contains(&LEFT),
            "test fixture invariant: a freshly restarted reconciler must start with an empty \
             hosted set (seed={seed})"
        );

        // Recovery: driving every parent's reconciler via ORDINARY ticks —
        // no `fork_wake()` involved at all — must still converge, purely
        // off `pending_split()`'s durable read.
        let recovered = converge(&mut c, &env, &parents, &pending_view, |c| {
            let hosted = c.hosted_set(p2.clone());
            hosted.contains(&LEFT) && hosted.contains(&RIGHT)
        })
        .await;
        assert!(
            recovered,
            "p2 never re-materialized both children after losing its eager wake to a crash \
             (seed={seed})"
        );
        let left_engine = c.storage(p2.clone(), LEFT);
        assert!(
            left_engine
                .get(&physical(b"left-key"))
                .await
                .unwrap()
                .is_some(),
            "p2's recovered LEFT child lost the pre-fork write (seed={seed})"
        );
    });
}

// ---------------------------------------------------------------------------
// The frozen corpus.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Scenario {
    name: String,
    seed: u64,
    run: fn(u64),
}

impl SeedVariant for Scenario {
    fn scenario_name(&self) -> &str {
        &self.name
    }
    fn reseeded(&self, name: String, seed: u64) -> Self {
        Scenario {
            name,
            seed,
            run: self.run,
        }
    }
}

fn scenario_cells() -> Vec<Scenario> {
    macro_rules! scenario {
        ($name:literal, $f:expr) => {
            Scenario {
                name: $name.to_string(),
                seed: corpus::name_seed($name),
                run: $f,
            }
        };
    }
    vec![
        scenario!("happy_path", scenario_happy_path),
        scenario!(
            "leader_crash_mid_catch_up",
            scenario_leader_crash_mid_catch_up
        ),
        scenario!(
            "crash_between_fork_and_materialization",
            scenario_crash_between_fork_and_materialization
        ),
        scenario!(
            "unrelated_rebalance_does_not_race_the_split",
            scenario_unrelated_rebalance_does_not_race_the_split
        ),
        scenario!(
            "campaigning_replica_wins_leadership_almost_immediately",
            scenario_campaigning_replica_wins_leadership_almost_immediately
        ),
        scenario!(
            "parent_leader_crash_at_fork_falls_back_to_ordinary_election",
            scenario_parent_leader_crash_at_fork_falls_back_to_ordinary_election
        ),
        scenario!(
            "eager_wake_and_reconciler_tick_race_benignly",
            scenario_eager_wake_and_reconciler_tick_race_benignly
        ),
        scenario!(
            "crash_after_apply_loses_the_eager_wake_but_reconciler_fallback_recovers",
            scenario_crash_after_apply_loses_the_eager_wake_but_reconciler_fallback_recovers
        ),
    ]
}

fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_INPLACE_SPLIT_SEEDS")
}

#[test]
fn inplace_split_reconciler_corpus_names_are_unique() {
    let cells = scenario_cells();
    let mut names: Vec<&str> = cells.iter().map(|c| c.name.as_str()).collect();
    names.sort_unstable();
    let mut deduped = names.clone();
    deduped.dedup();
    assert_eq!(names, deduped, "duplicate scenario name in the corpus");
}

#[test]
fn inplace_split_reconciler_corpus_runs_every_scenario() {
    let k = seeds_per_cell();
    for cell in corpus::seed_expand(scenario_cells(), k) {
        (cell.run)(cell.seed);
    }
}

#[test]
fn happy_path_is_reproducible_from_its_seed() {
    let seed = corpus::name_seed("happy_path");
    scenario_happy_path(seed);
    scenario_happy_path(seed);
}
