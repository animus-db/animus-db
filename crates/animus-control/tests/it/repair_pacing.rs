//! Repair-proposal pacing (ADR 0079 section 3, step 6): Tier 1 cell 9 (the
//! control-plane half) and negative controls (a) and (f).
//!
//! 6 data nodes, `TABLETS` RF3 tablets spread over every triple of
//! neighbours; one node dies, every tablet it held (`V` of them) is a
//! violation, and the real leader `reconcile_loop` repairs them. The cell
//! asserts, per seed:
//!
//! * proposals: cumulative commits never exceed `R x (ticks since the first
//!   commit + 2)` (a single tick window flaked: commit latency can put two
//!   batches in one);
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
use animus_control::version::VersionRange;
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
    /// `(time since the kill, repairs committed so far)`, every `SAMPLE`.
    samples: Vec<(Duration, usize)>,
    inflight_hwm: u64,
    deferred: u64,
    converged_after: Option<Duration>,
    final_inflight: u64,
}

/// Which repaired tablets the simulated data plane reports `RepairSettled` for,
/// and how long after the repair's CAS applied it does so (the learner
/// catch-up and promotion it stands in for).
#[derive(Clone, Copy)]
enum Settle {
    /// No report is ever sent (the gate closed, or completion detection off).
    Never,
    /// Every repaired tablet reports after this long.
    All(Duration),
    /// Only even-numbered tablets report; odd ones never settle.
    EvenOnly(Duration),
}

fn run_cell(seed: u64, pacing: RepairPacing) -> Report {
    run_cell_settle(seed, pacing, Settle::Never)
}

fn run_cell_settle(seed: u64, pacing: RepairPacing, settle: Settle) -> Report {
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
    // The settled report is gated (`Gate::NodeCapacity`, cluster version 5):
    // the settle cells run an era-active cluster finalized to it.
    let gated = !matches!(settle, Settle::Never);
    if gated {
        for id in CONTROL.iter().chain(&DATA) {
            sim.set_network_ext_for(
                nid(*id),
                animus_env::handshake::encode_ext(Some((1, 5)), Some("t")),
            );
        }
        for n in &nodes {
            n.set_own_version_range(Some(VersionRange::new(1, 5)));
            n.set_own_build("t");
        }
    }
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
    if gated {
        for _ in 0..100 {
            if nodes[leader].metadata().versioning_active() {
                break;
            }
            sim.run_for(Duration::from_millis(200));
        }
        assert!(
            nodes[leader].metadata().versioning_active(),
            "era never started"
        );
        for id in DATA {
            nodes[leader].propose(MetaCommand::ReportNodeVersion {
                node: nid(id),
                range: VersionRange::new(1, 5),
                build: "t".into(),
            });
            sim.run_for(Duration::from_millis(200));
        }
        for target in 2..=5 {
            nodes[leader].propose(MetaCommand::FinalizeClusterVersion {
                expected: target - 1,
                target,
            });
            sim.run_for(Duration::from_millis(200));
        }
        assert!(nodes[leader].metadata().capacity_gate_open(), "gate closed");
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
    // tablet -> (epoch, first seen at that epoch) for the simulated data plane.
    let mut seen: BTreeMap<TabletId, (animus_tablet::Epoch, Duration)> = BTreeMap::new();
    while elapsed < horizon {
        sim.run_for(SAMPLE);
        elapsed += SAMPLE;
        let after = match settle {
            Settle::Never => None,
            Settle::All(d) | Settle::EvenOnly(d) => Some(d),
        };
        if let Some(d) = after {
            let m = nodes[leader].metadata();
            for t in m.tablets.values() {
                let eligible = t.epoch > animus_tablet::Epoch::INITIAL
                    && !t.replicas.contains(&dead)
                    && t.settled_epoch != Some(t.epoch)
                    && (!matches!(settle, Settle::EvenOnly(_)) || t.id.0 % 2 == 0);
                if !eligible {
                    continue;
                }
                let e = seen.entry(t.id).or_insert((t.epoch, elapsed));
                if e.0 != t.epoch {
                    *e = (t.epoch, elapsed);
                }
                if elapsed.saturating_sub(e.1) >= d && e.1 <= elapsed {
                    let _ = nodes[leader].propose(MetaCommand::RepairSettled {
                        tablet: t.id,
                        epoch: t.epoch,
                    });
                    e.1 = elapsed + Duration::from_secs(3600);
                }
            }
        }
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
        samples,
        inflight_hwm: h.get(Metric::CpRepairInflightHwm),
        deferred: h.get(Metric::CpRepairDeferred),
        converged_after,
        final_inflight: h.get(Metric::CpRepairInflight),
    }
}

/// The oracle. `Err` names the violated bound.
fn check(r: &Report, p: RepairPacing) -> Result<(), String> {
    if p.per_tick > 0 {
        // The leader proposes at most R repairs per RECONCILE_INTERVAL tick, but
        // they are observed when they *commit*, and the commit latency of one
        // tick's batch can exceed the next one's (the first window is anchored
        // at the first commit), so a single tick-long window may legitimately
        // hold two batches: the per-window bound `R` flaked on seed 2584926886
        // (16 in one window, two ticks of 8). The sound bound is cumulative:
        // by `e` after the first commit at most `R * (floor(e / TICK) + 2)`
        // repairs can have committed (two batches of slack for that jitter). An
        // unpaced burst (30 at once) still breaks it in the first window.
        let first = r
            .samples
            .iter()
            .find(|s| s.1 > 0)
            .map_or(Duration::ZERO, |s| s.0);
        for &(e, done) in r.samples.iter().filter(|s| s.1 > 0) {
            let ticks = ((e - first).as_millis() / TICK.as_millis()) as usize;
            let allowed = p.per_tick * (ticks + 2);
            if done > allowed {
                return Err(format!(
                    "{done} repairs committed {e:?} after the kill > R={} x (ticks since the \
                     first commit + 2) = {allowed}",
                    p.per_tick
                ));
            }
        }
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
    animus_test::corpus::shard_retain((0..k).map(|i| base + i * 0x9E37).collect())
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

// ---- Observable completion (ADR 0079 step 6, `RepairSettled`) ----------------

/// The data plane reports a repair's learner promoted this long after its CAS.
const PROMOTE_AFTER: Duration = Duration::from_millis(300);

/// Throughput oracle: with completion observable, the whole burst proposes at
/// the `R` / tick rate (plus two ticks of slack), not at `R_inflight / TTL`.
fn check_throughput(r: &Report, p: RepairPacing) -> Result<(), String> {
    let rounds = r.violating.div_ceil(p.per_tick) as u32;
    let bound = REPAIR_DWELL + TICK * (rounds + 2) + Duration::from_secs(1);
    match r.converged_after {
        Some(t) if t <= bound => Ok(()),
        other => Err(format!("repairs took {other:?} > R-bound {bound:?}")),
    }
}

#[test]
fn cell9_settled_report_frees_slots_before_the_ttl() {
    let p = RepairPacing::default();
    for seed in seeds(0x9A11_0005) {
        let s = run_cell_settle(seed, p, Settle::All(PROMOTE_AFTER));
        let t = run_cell(seed, p);
        assert!(
            s.violating > p.inflight_max,
            "seed {seed}: V must exceed R_inflight"
        );
        check(&s, p).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        check_throughput(&s, p).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        let (cs, ct) = (s.converged_after.unwrap(), t.converged_after.unwrap());
        eprintln!(
            "repair-settled seed={seed} V={} settled_converged={cs:?} ttl_only_converged={ct:?} \
             inflight_hwm settled={} ttl_only={}",
            s.violating, s.inflight_hwm, t.inflight_hwm
        );
        // The TTL-only run must wait out a full TTL for the second round.
        assert!(
            cs + Duration::from_secs(2) < ct,
            "seed {seed}: {cs:?} vs {ct:?}"
        );
    }
}

#[test]
fn cell9_unsettled_repairs_are_freed_by_the_ttl() {
    let p = RepairPacing::default();
    for seed in seeds(0x9A11_0006) {
        let r = run_cell_settle(seed, p, Settle::EvenOnly(PROMOTE_AFTER));
        // Odd tablets never report: their slots age out, everything converges,
        // and the gauge returns to 0 (the `check` liveness + gauge clauses).
        check(&r, p).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        assert!(r.converged_after.is_some(), "seed {seed}");
    }
}

#[test]
fn negative_control_g_completion_detection_off_fails_the_throughput_oracle() {
    let p = RepairPacing::default();
    for seed in seeds(0x9A11_0007) {
        // Reports are sent, but the TTL-only run is the one with detection off:
        // `Settle::Never` = no `RepairSettled` ever applies, so only the TTL frees.
        let r = run_cell_settle(seed, p, Settle::Never);
        assert!(
            check_throughput(&r, p).is_err(),
            "seed {seed}: TTL-only run met the observable-completion throughput bound ({:?})",
            r.converged_after
        );
    }
}
