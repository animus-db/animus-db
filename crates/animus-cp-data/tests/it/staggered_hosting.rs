//! ADR 0078 section 3 (staggered hosting): a `SimEnv` test that a node asked to
//! host far more tablets than `host_unsettled_max` hosts them in waves, every
//! wave in ascending `TabletId` order, never holds more than `H` unsettled
//! groups, and ends with every group led. The control (`H = 0`) hosts all of
//! them in the first tick, as before the ADR.
//!
//! Three real `Reconciler`s (RF3, one per node) drive real Raft groups under
//! `SimEnv`; the whole script is one spawned task and the test thread only
//! advances virtual time and reads the shared report (never `block_on` a tick
//! under `SimEnv`, see the root `CLAUDE.md`).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::host::{
    DEFAULT_HOST_SETTLE_TIMEOUT, HostingRamp, MemoryTabletEngines, MetadataView, Reconciler,
};
use animus_env::{Clock, Env, EnvExt, Metric, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, Tablet, TabletId};

type Recon = Reconciler<SimEnv, MemoryEngine>;

const TABLETS: u64 = 300;
const H: usize = 100;

fn nodes() -> [NodeId; 3] {
    [nid(300), nid(301), nid(302)]
}

fn view() -> MetadataView {
    MetadataView {
        tablets: (1..=TABLETS)
            .map(|id| {
                (
                    TabletId(id),
                    Tablet::new_for_table(TabletId(id), "t", KeyRange::whole(), nodes().to_vec()),
                )
            })
            .collect(),
        ..Default::default()
    }
}

#[derive(Default)]
struct Report {
    /// Node 300's hosted count after each tick.
    hosted_after_tick: Vec<usize>,
    /// The largest unsettled count any node reported after any tick.
    max_unsettled: usize,
    /// Node 300's ramp reading after its first tick.
    first_ramp: Option<HostingRamp>,
    /// `next_wake` of node 300 after its first tick.
    first_wake: Option<Option<Duration>>,
    /// Node 300's hosted tablet ids (in `hosted_log` order = host order).
    host_order: Vec<TabletId>,
    /// Every group on every node was hosted and had a leader.
    all_led: bool,
    ticks: usize,
}

fn run(seed: u64, stagger: bool) -> (Report, u64) {
    let mut sim = Simulator::new(seed);
    let driver = sim.env(nid(900));
    let envs: Vec<SimEnv> = nodes().into_iter().map(|n| sim.env(n)).collect();
    let metrics_env = envs[0].clone();
    let report = Arc::new(Mutex::new(Report::default()));
    let rep = Arc::clone(&report);
    let order: Arc<Mutex<Vec<TabletId>>> = Arc::new(Mutex::new(Vec::new()));
    let order2 = Arc::clone(&order);

    driver.clone().spawn_task(async move {
        let mut recs: Vec<Recon> = nodes()
            .into_iter()
            .zip(envs)
            .map(|(id, env)| {
                let log = Arc::clone(&order2);
                let first = id == nid(300);
                let mut r: Recon = Reconciler::new(
                    env,
                    MemoryTabletEngines::new(),
                    id,
                    move |t, _n| {
                        if first {
                            log.lock().unwrap().push(t);
                        }
                    },
                    |_t| {},
                );
                r.set_host_stagger(if stagger { H } else { 0 }, DEFAULT_HOST_SETTLE_TIMEOUT);
                r
            })
            .collect();
        let v = view();
        let mut first = true;
        loop {
            for r in &mut recs {
                r.tick(&v).await;
            }
            {
                let mut rep = rep.lock().unwrap();
                rep.ticks += 1;
                let hosted0 = recs[0].local_state().hosted.len();
                rep.hosted_after_tick.push(hosted0);
                for r in &recs {
                    rep.max_unsettled = rep.max_unsettled.max(r.hosting_ramp().unsettled);
                }
                if first {
                    first = false;
                    rep.first_ramp = Some(recs[0].hosting_ramp());
                    rep.first_wake = Some(recs[0].next_wake());
                }
                let all_led = recs.iter().all(|r| {
                    (1..=TABLETS).all(|t| {
                        r.hosted_node(TabletId(t))
                            .is_some_and(|n| n.is_leader() || n.leader().is_some())
                    })
                });
                if all_led {
                    rep.all_led = true;
                    break;
                }
            }
            let wake = recs
                .iter()
                .filter_map(Recon::next_wake)
                .min()
                .unwrap_or(Duration::from_millis(500));
            driver.sleep(wake).await;
        }
    });

    let mut waited = Duration::ZERO;
    while waited < Duration::from_secs(120) {
        sim.run_for(Duration::from_millis(250));
        waited += Duration::from_millis(250);
        if report.lock().unwrap().all_led {
            break;
        }
    }
    let mut rep = std::mem::take(&mut *report.lock().unwrap());
    rep.host_order.clone_from(&order.lock().unwrap());
    let deferred = metrics_env.metrics().get(Metric::CpHostWaveDeferred);
    (rep, deferred)
}

#[test]
fn a_node_hosting_300_tablets_with_h_100_hosts_in_waves_and_every_group_gets_a_leader() {
    for seed in [0x5A6E_0001u64, 0x5A6E_0002, 0x5A6E_0003] {
        let (rep, deferred) = run(seed, true);
        assert!(rep.all_led, "seed {seed:#x}: not every group got a leader");

        // Wave 1 is exactly H tablets, the lowest ids, with 200 held back.
        assert_eq!(rep.hosted_after_tick[0], H, "seed {seed:#x}");
        let ramp = rep.first_ramp.expect("first tick ran");
        assert_eq!(ramp.hosted, H);
        assert_eq!(ramp.deferred, TABLETS as usize - H);
        assert_eq!(ramp.unsettled, H);
        assert!(ramp.active());
        assert_eq!(rep.first_wake, Some(Some(Duration::from_millis(50))));

        // Waves: the hosted count rose in several steps, never past 300.
        let mut steps: Vec<usize> = rep.hosted_after_tick.clone();
        steps.dedup();
        assert!(
            steps.len() >= 3,
            "seed {seed:#x}: expected at least 3 waves, hosted counts {steps:?}"
        );
        assert_eq!(*steps.last().unwrap(), TABLETS as usize);

        // The unsettled set never exceeded H.
        assert!(
            rep.max_unsettled <= H,
            "seed {seed:#x}: {} unsettled groups, cap is {H}",
            rep.max_unsettled
        );

        // Hosting order is ascending TabletId across waves (the budget is
        // taken from the front of the ascending order each tick).
        let mut sorted = rep.host_order.clone();
        sorted.sort();
        assert_eq!(rep.host_order, sorted, "seed {seed:#x}: waves out of order");
        assert_eq!(rep.host_order.len(), TABLETS as usize);

        assert!(deferred > 0, "CpHostWaveDeferred must have been bumped");
    }
}

#[test]
fn with_staggering_off_every_tablet_is_hosted_in_the_first_tick() {
    let (rep, deferred) = run(0x5A6E_0004, false);
    assert!(rep.all_led);
    assert_eq!(rep.hosted_after_tick[0], TABLETS as usize);
    let ramp = rep.first_ramp.expect("first tick ran");
    assert_eq!(ramp.deferred, 0);
    assert!(!ramp.active());
    assert_eq!(rep.first_wake, Some(None));
    assert_eq!(deferred, 0, "H = 0 must never defer");
}
