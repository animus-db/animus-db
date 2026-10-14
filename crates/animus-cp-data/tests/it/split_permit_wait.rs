//! ADR 0079 step 5: an explicit (manual) split waits for the split pool's
//! permit, FIFO, instead of being refused; it only gets `Busy` once its bound
//! has passed. `SplitPermits::admit_wait` over a `SimEnv`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::{ApplyOutcome, MetaCommand, Metadata, NodeStatus};
use animus_cp_data::background_budget::{
    BackgroundBudget, BudgetSizes, SplitAdmission, SplitPermits,
};
use animus_env::{Clock, EnvExt, MetricsHandle, Nanos, nid};
use animus_sim::Simulator;
use animus_tablet::{KeyRange, TabletId};

const POLL: Duration = Duration::from_millis(50);

fn meta() -> Arc<Mutex<Metadata>> {
    let mut m = Metadata::default();
    for n in [1u64, 2, 3] {
        let _ = m.apply(&MetaCommand::UpsertMember {
            node: nid(n),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        });
    }
    for i in 1..=3u64 {
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(i),
                table: Some(format!("t{i}")),
                range: KeyRange::whole(),
                replicas: vec![nid(1), nid(2), nid(3)],
            }),
            ApplyOutcome::Applied
        );
    }
    Arc::new(Mutex::new(m))
}

fn begin_split(m: &Mutex<Metadata>, t: TabletId, child_base: u64) {
    let mut m = m.lock().unwrap();
    let epoch = m.tablets[&t].epoch;
    let reps = vec![nid(1), nid(2), nid(3)];
    assert_eq!(
        m.apply(&MetaCommand::BeginSplitInPlace {
            parent: t,
            expected_epoch: epoch,
            split_key: 0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
            children: [
                (TabletId(child_base), reps.clone()),
                (TabletId(child_base + 1), reps),
            ],
        }),
        ApplyOutcome::Applied
    );
}

fn cutover(m: &Mutex<Metadata>, t: TabletId) {
    let mut m = m.lock().unwrap();
    let epoch = m.tablets[&t].epoch;
    assert_eq!(
        m.apply(&MetaCommand::CutoverSplit {
            parent: t,
            expected_epoch: epoch,
            cutover_wall_ms: 1,
        }),
        ApplyOutcome::Applied
    );
}

fn permits() -> SplitPermits {
    let mut s = BudgetSizes::off();
    s.splits = Some(1);
    SplitPermits::new(BackgroundBudget::new(s, MetricsHandle::recording()))
}

type Slot = Arc<Mutex<Option<(Nanos, bool)>>>;

/// Spawn a manual split of `tablet` that waits up to `bound`; on a grant it
/// begins the split in `m` and holds the permit, like `trigger_split`.
fn spawn_split(
    env: &animus_sim::SimEnv,
    p: &SplitPermits,
    m: &Arc<Mutex<Metadata>>,
    tablet: TabletId,
    child_base: u64,
    bound: Duration,
) -> Slot {
    let out: Slot = Arc::default();
    let (env2, p, m2, o) = (env.clone(), p.clone(), m.clone(), out.clone());
    env.spawn_task(async move {
        let m3 = m2.clone();
        let adm = p
            .admit_wait(
                &env2,
                tablet,
                move || m3.lock().unwrap().clone(),
                bound,
                POLL,
            )
            .await;
        let granted = match adm {
            SplitAdmission::Granted(permit) => {
                begin_split(&m2, tablet, child_base);
                if let Some(permit) = permit {
                    p.hold(tablet, permit);
                }
                true
            }
            SplitAdmission::Busy => false,
        };
        *o.lock().unwrap() = Some((env2.now(), granted));
    });
    out
}

#[test]
fn second_manual_split_waits_for_the_first_permit() {
    for seed in 0..4u64 {
        let mut sim = Simulator::new(0x5917_0000 + seed);
        let env = sim.env(nid(1));
        let (p, m) = (permits(), meta());
        let a = spawn_split(&env, &p, &m, TabletId(1), 10, Duration::from_secs(30));
        sim.run_for(Duration::from_millis(200));
        let b = spawn_split(&env, &p, &m, TabletId(2), 20, Duration::from_secs(30));
        sim.run_for(Duration::from_secs(5));
        let (_, a_ok) = a.lock().unwrap().expect("first split decided");
        assert!(a_ok, "seed {seed}: the first split is granted at once");
        assert_eq!(p.held(), 1);
        assert!(
            b.lock().unwrap().is_none(),
            "seed {seed}: the second split must still be waiting, not refused"
        );
        cutover(&m, TabletId(1));
        sim.run_for(Duration::from_secs(1));
        let (at, b_ok) = b.lock().unwrap().expect("second split decided");
        assert!(b_ok, "seed {seed}: the second split got the freed permit");
        assert!(
            at.0 >= Duration::from_secs(5).as_nanos() as u64,
            "seed {seed}: and only after the first finished"
        );
    }
}

#[test]
fn waiters_are_served_in_arrival_order() {
    let mut sim = Simulator::new(0x5917_1111);
    let env = sim.env(nid(1));
    let (p, m) = (permits(), meta());
    let _a = spawn_split(&env, &p, &m, TabletId(1), 10, Duration::from_secs(60));
    sim.run_for(Duration::from_millis(100));
    let b = spawn_split(&env, &p, &m, TabletId(2), 20, Duration::from_secs(60));
    sim.run_for(Duration::from_millis(100));
    let c = spawn_split(&env, &p, &m, TabletId(3), 30, Duration::from_secs(60));
    sim.run_for(Duration::from_secs(2));
    cutover(&m, TabletId(1));
    sim.run_for(Duration::from_secs(1));
    assert!(b.lock().unwrap().is_some(), "the earlier waiter went first");
    assert!(c.lock().unwrap().is_none(), "the later waiter still waits");
    cutover(&m, TabletId(2));
    sim.run_for(Duration::from_secs(1));
    assert!(c.lock().unwrap().is_some_and(|(_, ok)| ok));
}

#[test]
fn a_permit_that_never_frees_returns_busy_at_the_bound() {
    let mut sim = Simulator::new(0x5917_2222);
    let env = sim.env(nid(1));
    let (p, m) = (permits(), meta());
    let _a = spawn_split(&env, &p, &m, TabletId(1), 10, Duration::from_secs(30));
    sim.run_for(Duration::from_millis(100));
    let b = spawn_split(&env, &p, &m, TabletId(2), 20, Duration::from_secs(3));
    sim.run_for(Duration::from_millis(2900));
    assert!(
        b.lock().unwrap().is_none(),
        "still waiting before the bound"
    );
    sim.run_for(Duration::from_millis(500));
    let (at, ok) = b.lock().unwrap().expect("decided after the bound");
    assert!(!ok, "Busy once the bound passed");
    assert!(at.0 >= Duration::from_secs(3).as_nanos() as u64);
}
