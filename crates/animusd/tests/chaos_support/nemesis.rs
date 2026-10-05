//! Seeded fault schedules and their executor.
//!
//! The *schedule* (which fault, which node, when, how long) is a pure
//! function of `(scenario, seed, duration, n)` and is printed up front, so a
//! failure reproduces the same faults. The processes themselves are real and
//! not deterministic (ADR 0003: determinism is `SimEnv`-only), so a replay is
//! the same fault sequence against a similar, not identical, execution.

use std::time::Duration;

use tokio::time::Instant;

use super::cluster::ChaosCluster;
use super::proxy::CutMode;
use super::rng::Rng;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scenario {
    /// One control-leader kill + one isolation: the CI smoke.
    Smoke,
    Kill,
    Partition,
    Pause,
    Delay,
    Mixed,
}

impl Scenario {
    pub fn name(self) -> &'static str {
        match self {
            Scenario::Smoke => "smoke",
            Scenario::Kill => "kill",
            Scenario::Partition => "partition",
            Scenario::Pause => "pause",
            Scenario::Delay => "delay",
            Scenario::Mixed => "mixed",
        }
    }

    /// Default fault-window length in seconds (`ANIMUS_CHAOS_SECS` overrides).
    pub fn default_secs(self) -> u64 {
        match self {
            Scenario::Smoke => 90,
            _ => 150,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Sel {
    Idx(usize),
    ControlLeader,
}

#[derive(Clone, Debug)]
pub enum Fault {
    /// `kill -9`, stay down for `down`, restart on the same data dir.
    Kill { node: Sel, down: Duration },
    /// Power cut: `kill -9` every node, restart them all.
    KillAll { down: Duration },
    /// Cut `node` from everyone (both directions on the Raft wire).
    Isolate {
        node: usize,
        mode: CutMode,
        dur: Duration,
    },
    /// Cut a minority group from the rest.
    Split {
        minority: Vec<usize>,
        mode: CutMode,
        dur: Duration,
    },
    /// Cut one direction of one link.
    OneWay {
        from: usize,
        to: usize,
        dur: Duration,
    },
    /// SIGSTOP then SIGCONT.
    Pause { node: Sel, dur: Duration },
    /// Add per-chunk latency on every proxied link.
    Delay { delay: Duration, dur: Duration },
}

#[derive(Clone, Debug)]
pub struct Step {
    pub at: Duration,
    pub fault: Fault,
}

fn secs(s: u64) -> Duration {
    Duration::from_secs(s)
}

fn pick_mode(rng: &mut Rng) -> CutMode {
    if rng.below(4) == 0 {
        CutMode::Reset
    } else {
        CutMode::Stall
    }
}

fn random_fault(kind: Scenario, rng: &mut Rng, n: usize) -> Fault {
    let node = rng.below(n as u64) as usize;
    let dur = secs(rng.range(6, 16));
    match kind {
        Scenario::Kill => Fault::Kill {
            node: if rng.below(3) == 0 {
                Sel::ControlLeader
            } else {
                Sel::Idx(node)
            },
            down: dur,
        },
        Scenario::Partition => match rng.below(3) {
            0 => Fault::Isolate {
                node,
                mode: pick_mode(rng),
                dur,
            },
            1 => {
                let k = rng.range(1, ((n - 1) / 2) as u64) as usize;
                let minority: Vec<usize> = (0..k).map(|j| (node + j) % n).collect();
                Fault::Split {
                    minority,
                    mode: pick_mode(rng),
                    dur,
                }
            }
            _ => Fault::OneWay {
                from: node,
                to: (node + 1 + rng.below(n as u64 - 1) as usize) % n,
                dur,
            },
        },
        Scenario::Pause => Fault::Pause {
            node: if rng.below(3) == 0 {
                Sel::ControlLeader
            } else {
                Sel::Idx(node)
            },
            dur: secs(rng.range(4, 14)),
        },
        Scenario::Delay => Fault::Delay {
            delay: Duration::from_millis(rng.range(100, 600)),
            dur,
        },
        Scenario::Smoke | Scenario::Mixed => {
            let pick = [
                Scenario::Kill,
                Scenario::Partition,
                Scenario::Pause,
                Scenario::Delay,
            ][rng.below(4) as usize];
            random_fault(pick, rng, n)
        }
    }
}

/// The fault schedule for a run of `total` fault-window length.
pub fn plan(scn: Scenario, seed: u64, total: Duration, n: usize) -> Vec<Step> {
    let mut rng = Rng::new(seed);
    let total_s = total.as_secs();
    if scn == Scenario::Smoke {
        // One control-leader kill, then one isolation: fixed shape, seeded
        // victim.
        let victim = rng.below(n as u64) as usize;
        return vec![
            Step {
                at: secs(total_s / 10),
                fault: Fault::Kill {
                    node: Sel::ControlLeader,
                    down: secs(10),
                },
            },
            Step {
                at: secs(total_s * 45 / 100),
                fault: Fault::Isolate {
                    node: victim,
                    mode: CutMode::Stall,
                    dur: secs(12),
                },
            },
        ];
    }
    let mut steps = Vec::new();
    let mut t = 8;
    if scn == Scenario::Kill {
        steps.push(Step {
            at: secs(t),
            fault: Fault::Kill {
                node: Sel::ControlLeader,
                down: secs(10),
            },
        });
        t += 10 + 12;
    }
    let reserve = 30; // quiet tail inside the window
    while t + 20 < total_s.saturating_sub(reserve) {
        let fault = random_fault(scn, &mut rng, n);
        let busy = fault_span(&fault);
        steps.push(Step { at: secs(t), fault });
        t += busy.as_secs() + rng.range(6, 14);
    }
    if scn == Scenario::Kill && t + 25 < total_s {
        steps.push(Step {
            at: secs(t),
            fault: Fault::KillAll { down: secs(5) },
        });
    }
    steps
}

fn fault_span(f: &Fault) -> Duration {
    match f {
        Fault::Kill { down, .. } | Fault::KillAll { down } => *down,
        Fault::Isolate { dur, .. }
        | Fault::Split { dur, .. }
        | Fault::OneWay { dur, .. }
        | Fault::Pause { dur, .. }
        | Fault::Delay { dur, .. } => *dur,
    }
}

async fn resolve(cluster: &ChaosCluster, sel: Sel, rng: &mut Rng) -> (usize, &'static str) {
    match sel {
        Sel::Idx(i) => (i, "random"),
        Sel::ControlLeader => match cluster.control_leader().await {
            Some(i) => (i, "control leader"),
            None => (
                rng.below(cluster.n as u64) as usize,
                "random (no leader found)",
            ),
        },
    }
}

/// Run the plan against the cluster, logging every action to `events`.
/// Each fault is undone before the next starts.
pub async fn run_plan(
    cluster: &mut ChaosCluster,
    plan: &[Step],
    start: Instant,
    seed: u64,
    events: &mut Vec<String>,
) {
    let mut rng = Rng::new(seed ^ 0x5EED);
    let log = |events: &mut Vec<String>, msg: String| {
        let line = format!("[t={:6.1}s] {msg}", start.elapsed().as_secs_f64());
        eprintln!("chaos: {line}");
        events.push(line);
    };
    for step in plan {
        tokio::time::sleep_until(start + step.at).await;
        match &step.fault {
            Fault::Kill { node, down } => {
                let (i, how) = resolve(cluster, *node, &mut rng).await;
                log(events, format!("kill -9 n{i} ({how}) for {down:?}"));
                cluster.kill9(i);
                tokio::time::sleep(*down).await;
                cluster.start(i);
                log(events, format!("restarted n{i} (same data dir)"));
            }
            Fault::KillAll { down } => {
                log(
                    events,
                    format!("kill -9 ALL nodes (power cut) for {down:?}"),
                );
                for i in 0..cluster.n {
                    cluster.kill9(i);
                }
                tokio::time::sleep(*down).await;
                cluster.start_all();
                log(events, "restarted all nodes".into());
            }
            Fault::Isolate { node, mode, dur } => {
                let rest: Vec<usize> = (0..cluster.n).filter(|j| j != node).collect();
                log(events, format!("isolate n{node} ({mode:?}) for {dur:?}"));
                cluster.faults.partition(&[vec![*node], rest], *mode);
                tokio::time::sleep(*dur).await;
                cluster.faults.heal();
                log(events, "healed partition".into());
            }
            Fault::Split {
                minority,
                mode,
                dur,
            } => {
                let rest: Vec<usize> = (0..cluster.n).filter(|j| !minority.contains(j)).collect();
                log(
                    events,
                    format!("split {minority:?} | {rest:?} ({mode:?}) for {dur:?}"),
                );
                cluster.faults.partition(&[minority.clone(), rest], *mode);
                tokio::time::sleep(*dur).await;
                cluster.faults.heal();
                log(events, "healed partition".into());
            }
            Fault::OneWay { from, to, dur } => {
                log(events, format!("one-way cut n{from} -> n{to} for {dur:?}"));
                cluster.faults.cut_one_way(*from, *to, CutMode::Stall);
                tokio::time::sleep(*dur).await;
                cluster.faults.heal();
                log(events, "healed partition".into());
            }
            Fault::Pause { node, dur } => {
                let (i, how) = resolve(cluster, *node, &mut rng).await;
                log(events, format!("SIGSTOP n{i} ({how}) for {dur:?}"));
                cluster.pause(i);
                tokio::time::sleep(*dur).await;
                cluster.resume(i);
                log(events, format!("SIGCONT n{i}"));
            }
            Fault::Delay { delay, dur } => {
                log(events, format!("delay {delay:?} on every link for {dur:?}"));
                cluster.faults.set_delay(*delay);
                tokio::time::sleep(*dur).await;
                cluster.faults.heal();
                log(events, "delay removed".into());
            }
        }
        for (i, status) in cluster.unexpected_exits() {
            log(
                events,
                format!("UNEXPECTED EXIT: n{i} died on its own ({status})"),
            );
        }
    }
}
