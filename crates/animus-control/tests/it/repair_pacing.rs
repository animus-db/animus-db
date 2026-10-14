//! Repair-proposal pacing (ADR 0079 section 3, step 6): Tier 1 cell 9 (the
//! control-plane half) and negative controls (a) and (f).
//!
//! 6 data nodes, `TABLETS` RF3 tablets spread over every triple of
//! neighbours; one node dies, every tablet it held (`V` of them) is a
//! violation, and the real leader `reconcile_loop` repairs them. The cell
//! asserts, per seed:
//!
//! * proposals: no `RECONCILE_INTERVAL` window commits more than `R` repairs;
//! * in flight: the leader's `CpRepairInflightHwm` never exceeds `R_inflight`;
//! * liveness: every tablet reaches RF3 on live nodes within
//!   `REPAIR_DWELL + ceil(V / slots) * (TTL + tick) + slack`;
//! * a repair never holds its in-flight slot forever: the gauge returns to 0
//!   once the repairs have aged out (`REPAIR_INFLIGHT_TTL`).
//!
//! The data-plane half of cell 9 (data above the snapshot threshold, no acked
//! write lost) lives in `animusd`'s `sim_cluster_scale_node_loss_*` cells.
//!
//! Negative controls, each asserting the oracle FAILS: (a) pacing off, the
//! whole burst lands in one window; (f) pacing without the in-flight limit,
//! the in-flight bound (set below `V`) is exceeded.
//!
//! Depth knob `ANIMUS_REPAIR_PACING_SEEDS=K` (default 1; K seeds per cell),
//! replay with `ANIMUS_SEED=<seed>`.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::node::{REPAIR_DWELL, heartbeat_loop};
use animus_control::raft::ProposeResult;
use animus_control::repair_pacing::{REPAIR_INFLIGHT_TTL, RepairPacing};
use animus_control::{MetaCommand, NodeStatus, RaftNode};
use animus_env::{EnvExt, Metric, MetricsHandle, NodeId, nid};
use animus_placement::PlacementPolicy;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId};

const CONTROL: [u64; 3] = [0, 1, 2];
const DATA: [u64; 6] = [10, 11, 12, 13, 14, 15];
const TABLETS: u64 = 60;
const TICK: Duration = Duration::from_millis(500);
const SAMPLE: Duration = Duration::from_millis(10);

struct Report {
    violating: usize,
    max_window: usize,
    inflight_hwm: u64,
    deferred: u64,
    converged_after: Option<Duration>,
    final_inflight: u64,
}

fn run_cell(seed: u64, pacing: RepairPacing) -> Report {
    let mut sim = Simulator::new(seed);
    let handles: Vec<MetricsHandle> = CONTROL.iter().map(|_| MetricsHandle::recording()).collect();
    let nodes: Vec<RaftNode<SimEnv>> = CONTROL
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            let n = RaftNode::start_with_metrics(
                sim.env(nid(id)),
                CONTROL.iter().copied().map(nid).collect(),
                handles[i].clone(),
                MemoryEngine::new(),
            );
            n.set_repair_pacing(pacing);
            n
        })
        .collect();
    for id in DATA {
        let env = sim.env(nid(id));
        env.spawn_task(heartbeat_loop(
            env.clone(),
            CONTROL.iter().copied().map(nid).collect(),
        ));
    }
    sim.run_for(Duration::from_secs(2));
    let leader = (0..3).find(|&i| nodes[i].is_leader()).expect("a leader");
    for id in DATA {
        let ok = nodes[leader].propose(MetaCommand::UpsertMember {
            node: nid(id),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        });
        assert!(matches!(ok, ProposeResult::Accepted { .. }));
    }
    sim.run_for(Duration::from_secs(1));
    let policy = PlacementPolicy::simple("rf3", 3);
    for i in 0..TABLETS {
        let replicas: Vec<NodeId> = (0..3).map(|k| nid(DATA[((i + k) % 6) as usize])).collect();
        for cmd in [
            MetaCommand::CreateTablet {
                tablet: TabletId(i + 1),
                table: None,
                range: KeyRange::whole(),
                replicas,
            },
            MetaCommand::SetTabletPolicy {
                tablet: TabletId(i + 1),
                policy: Some(policy.clone()),
            },
        ] {
            assert!(matches!(
                nodes[leader].propose(cmd),
                ProposeResult::Accepted { .. }
            ));
        }
        // Keep the leader's log pipeline modest.
        if i % 10 == 9 {
            sim.run_for(Duration::from_millis(200));
        }
    }
    sim.run_for(Duration::from_secs(2));
    assert_eq!(nodes[leader].metadata().tablets.len(), TABLETS as usize);

    // Fault: node 10 dies.
    let dead = nid(DATA[0]);
    sim.crash(dead.clone());
    assert!(matches!(
        nodes[leader].propose(MetaCommand::UpsertMember {
            node: dead.clone(),
            labels: BTreeMap::new(),
            status: NodeStatus::Down,
        }),
        ProposeResult::Accepted { .. }
    ));
    let violating = nodes[leader]
        .metadata()
        .tablets
        .values()
        .filter(|t| t.replicas.contains(&dead))
        .count();
    let t_kill = sim.now();

    // Sample the leader's view every 10 ms: repairs committed so far.
    let repaired = |n: &RaftNode<SimEnv>| {
        n.metadata()
            .tablets
            .values()
            .filter(|t| !t.replicas.contains(&dead))
            .count()
    };
    let base = repaired(&nodes[leader]);
    let mut samples: Vec<(Duration, usize)> = Vec::new();
    let mut converged_after = None;
    let horizon = REPAIR_DWELL + Duration::from_secs(120);
    let mut elapsed = Duration::ZERO;
    while elapsed < horizon {
        sim.run_for(SAMPLE);
        elapsed += SAMPLE;
        let done = repaired(&nodes[leader]) - base;
        samples.push((elapsed, done));
        let m = nodes[leader].metadata();
        if converged_after.is_none()
            && m.tablets
                .values()
                .all(|t| t.replicas.len() == 3 && !t.replicas.contains(&dead))
        {
            converged_after = Some(sim.now().duration_since(t_kill));
        }
        if converged_after.is_some() && elapsed > converged_after.unwrap() + Duration::from_secs(1)
        {
            break;
        }
    }
    // Windows of one tick anchored at the first committed repair.
    let first = samples
        .iter()
        .find(|s| s.1 > 0)
        .map_or(Duration::ZERO, |s| s.0);
    let anchor = first.saturating_sub(SAMPLE);
    let mut max_window = 0usize;
    let mut prev = 0usize;
    let mut w = anchor + TICK;
    loop {
        let upto = samples
            .iter()
            .take_while(|s| s.0 <= w)
            .last()
            .map_or(0, |s| s.1);
        max_window = max_window.max(upto - prev);
        prev = upto;
        if w > elapsed {
            break;
        }
        w += TICK;
    }
    // Let the in-flight repairs age out, then read the gauge.
    sim.run_for(REPAIR_INFLIGHT_TTL + 2 * TICK);
    let h = &handles[leader];
    eprintln!(
        "repair-pacing seed={seed} {pacing:?} V={violating} max_window={max_window} \
         inflight_hwm={} deferred={} converged_after={converged_after:?}",
        h.get(Metric::CpRepairInflightHwm),
        h.get(Metric::CpRepairDeferred),
    );
    Report {
        violating,
        max_window,
        inflight_hwm: h.get(Metric::CpRepairInflightHwm),
        deferred: h.get(Metric::CpRepairDeferred),
        converged_after,
        final_inflight: h.get(Metric::CpRepairInflight),
    }
}

/// The oracle. `Err` names the violated bound.
fn check(r: &Report, p: RepairPacing) -> Result<(), String> {
    if p.per_tick > 0 && r.max_window > p.per_tick {
        return Err(format!(
            "{} repairs in one tick window > R={}",
            r.max_window, p.per_tick
        ));
    }
    if p.inflight_max > 0 && r.inflight_hwm as usize > p.inflight_max {
        return Err(format!(
            "in-flight high-water {} > R_inflight={}",
            r.inflight_hwm, p.inflight_max
        ));
    }
    let slots = match (p.per_tick, p.inflight_max) {
        (0, 0) => r.violating.max(1),
        (0, m) => m,
        (rr, 0) => rr,
        (rr, m) => rr.min(m),
    };
    let rounds = r.violating.div_ceil(slots) as u32;
    let bound = REPAIR_DWELL + (REPAIR_INFLIGHT_TTL + TICK) * rounds + Duration::from_secs(5);
    match r.converged_after {
        Some(t) if t <= bound => {}
        other => return Err(format!("not converged within {bound:?}: {other:?}")),
    }
    if r.final_inflight != 0 {
        return Err(format!("in-flight gauge stuck at {}", r.final_inflight));
    }
    Ok(())
}

fn seeds(base: u64) -> Vec<u64> {
    if let Ok(s) = std::env::var("ANIMUS_SEED") {
        return vec![s.parse().expect("ANIMUS_SEED")];
    }
    let k: u64 = std::env::var("ANIMUS_REPAIR_PACING_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    (0..k).map(|i| base + i * 0x9E37).collect()
}

#[test]
fn cell9_default_pacing_bounds_proposals_and_inflight() {
    let p = RepairPacing::default();
    for seed in seeds(0x9A11_0001) {
        let r = run_cell(seed, p);
        assert!(
            r.violating >= 20,
            "seed {seed}: only {} violating",
            r.violating
        );
        assert!(r.deferred > 0, "seed {seed}: nothing was paced");
        check(&r, p).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

#[test]
fn cell9_binding_inflight_cap_slows_but_converges() {
    let p = RepairPacing {
        per_tick: 8,
        inflight_max: 4,
    };
    for seed in seeds(0x9A11_0002) {
        let r = run_cell(seed, p);
        assert!(r.inflight_hwm >= 1, "seed {seed}");
        check(&r, p).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

#[test]
fn negative_control_a_pacing_off_lands_the_whole_burst() {
    // Oracle of the default pacing against an unpaced run must fail.
    for seed in seeds(0x9A11_0003) {
        let r = run_cell(seed, RepairPacing::OFF);
        assert!(
            check(&r, RepairPacing::default()).is_err(),
            "seed {seed}: unpaced burst satisfied the paced oracle (window {})",
            r.max_window
        );
    }
}

#[test]
fn negative_control_f_no_inflight_limit_breaks_the_inflight_bound() {
    let oracle = RepairPacing {
        per_tick: 8,
        inflight_max: 4,
    };
    let run = RepairPacing {
        per_tick: 8,
        inflight_max: 0,
    };
    for seed in seeds(0x9A11_0004) {
        let r = run_cell(seed, run);
        let err = check(&r, oracle).expect_err("oracle must fail without the in-flight limit");
        assert!(
            err.contains("in-flight"),
            "seed {seed}: wrong failure: {err}"
        );
    }
}
