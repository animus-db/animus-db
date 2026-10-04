//! Reusable phase plans and the YCSB scenario built from them.
//!
//! [`run_steady`] and [`run_degraded`] are workload-agnostic — any
//! [`OpSource`]/[`OpExecutor`] pair (a C-17 scale scenario, say) gets the same
//! warm-up / steady / sweep and baseline / degraded / recovery sequencing and
//! the same report types. [`run_ycsb`] is the YCSB A-F glue.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::cluster::{Cluster, FaultAction};
use crate::dist::Distribution;
use crate::engine::{FaultPlan, OpExecutor, OpSource, PhaseResult, PhaseSpec, run_phase};
use crate::envinfo;
use crate::report::{RunResult, SweepPoint};
use crate::rt::Clock;
use crate::workload::{OpStream, WorkloadKind, WorkloadSpec};
use crate::ycsb::{self, YcsbExecutor};

/// Warm-up + steady-state (or a throughput sweep) timing.
#[derive(Clone, Debug)]
pub struct PhasePlan {
    /// Steady-state arrival rate (ignored when `sweep` is non-empty).
    pub rate: f64,
    /// Discarded warm-up, run once at the first rate.
    pub warmup: Duration,
    /// Length of the steady phase (and of each sweep point).
    pub steady: Duration,
    /// Target rates to sweep; empty = a single steady phase at `rate`.
    pub sweep: Vec<f64>,
    pub connections: usize,
    pub op_timeout: Duration,
    /// Grace after each phase window for queued ops to start; ops still
    /// unstarted after it are `abandoned` (CLI `--drain-secs`, default 30 s).
    pub drain_timeout: Duration,
}

impl PhasePlan {
    fn spec(&self, name: impl Into<String>, duration: Duration, rate: f64) -> PhaseSpec {
        let mut s = PhaseSpec::new(name, duration, rate);
        s.connections = self.connections;
        s.op_timeout = self.op_timeout;
        s.drain_timeout = self.drain_timeout;
        s
    }
}

/// Run warm-up then steady state (or the sweep). Returns the phases and, for
/// a sweep, one [`SweepPoint`] per rate.
pub async fn run_steady<S, X>(
    cluster: &Cluster,
    clock: &Clock,
    plan: &PhasePlan,
    source: &mut S,
    exec: Arc<X>,
) -> (Vec<PhaseResult>, Vec<SweepPoint>)
where
    S: OpSource,
    X: OpExecutor<Op = S::Op>,
{
    let first = plan.sweep.first().copied().unwrap_or(plan.rate);
    let mut phases = Vec::new();
    if !plan.warmup.is_zero() {
        let mut w = plan.spec("warmup", plan.warmup, first);
        w.discard = true;
        phases.push(run_phase(cluster, clock, &w, source, exec.clone()).await);
    }
    let mut sweep = Vec::new();
    if plan.sweep.is_empty() {
        let s = plan.spec("steady", plan.steady, plan.rate);
        phases.push(run_phase(cluster, clock, &s, source, exec.clone()).await);
    } else {
        for &rate in &plan.sweep {
            let s = plan.spec(format!("sweep@{rate:.0}"), plan.steady, rate);
            let p = run_phase(cluster, clock, &s, source, exec.clone()).await;
            sweep.push(SweepPoint::from_phase(&p));
            phases.push(p);
        }
    }
    (phases, sweep)
}

/// Baseline → degraded → recovery around one injected fault.
#[derive(Clone, Debug)]
pub struct DegradedPlan {
    pub rate: f64,
    pub warmup: Duration,
    /// Healthy measurement just before the fault (the comparison baseline).
    pub baseline: Duration,
    /// Measured window starting at the fault.
    pub degraded: Duration,
    /// Measured window after the fault window; the killed node is restarted
    /// at its start when the cluster supports it.
    pub recovery: Duration,
    pub fault: FaultAction,
    pub restart: bool,
    pub connections: usize,
    pub op_timeout: Duration,
    pub drain_timeout: Duration,
}

/// Run the degraded sequence. The fault fires at the very start of the
/// `degraded` phase; recovery restarts the node it killed (if `restart`).
pub async fn run_degraded<S, X>(
    cluster: &Cluster,
    clock: &Clock,
    plan: &DegradedPlan,
    source: &mut S,
    exec: Arc<X>,
) -> Vec<PhaseResult>
where
    S: OpSource,
    X: OpExecutor<Op = S::Op>,
{
    let mk = |name: &str, d: Duration| {
        let mut s = PhaseSpec::new(name, d, plan.rate);
        s.connections = plan.connections;
        s.op_timeout = plan.op_timeout;
        s.drain_timeout = plan.drain_timeout;
        s
    };
    let mut phases = Vec::new();
    if !plan.warmup.is_zero() {
        let mut w = mk("warmup", plan.warmup);
        w.discard = true;
        phases.push(run_phase(cluster, clock, &w, source, exec.clone()).await);
    }
    phases.push(
        run_phase(
            cluster,
            clock,
            &mk("baseline", plan.baseline),
            source,
            exec.clone(),
        )
        .await,
    );
    let mut d = mk("degraded", plan.degraded);
    d.fault = Some(FaultPlan {
        after: Duration::ZERO,
        action: plan.fault.clone(),
    });
    let degraded = run_phase(cluster, clock, &d, source, exec.clone()).await;
    let killed = degraded
        .fault
        .as_ref()
        .and_then(|f| f.ok.then_some(f.node).flatten());
    phases.push(degraded);
    let mut r = mk("recovery", plan.recovery);
    if plan.restart
        && let Some(node) = killed
    {
        r.fault = Some(FaultPlan {
            after: Duration::ZERO,
            action: FaultAction::Restart { node },
        });
    }
    phases.push(run_phase(cluster, clock, &r, source, exec).await);
    phases
}

/// Which `ConsistentRead` settings to measure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadModes {
    Consistent,
    Eventual,
    Both,
}

impl ReadModes {
    /// Parse `true` / `false` / `both`.
    ///
    /// # Errors
    /// On anything else.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "true" => Ok(Self::Consistent),
            "false" => Ok(Self::Eventual),
            "both" => Ok(Self::Both),
            o => Err(format!(
                "--consistent-read wants true|false|both, got `{o}`"
            )),
        }
    }

    fn list(self) -> Vec<bool> {
        match self {
            Self::Consistent => vec![true],
            Self::Eventual => vec![false],
            Self::Both => vec![true, false],
        }
    }
}

/// How the degraded run picks its victim.
#[derive(Clone, Debug)]
pub enum DegradedKind {
    /// The node leading the table's first tablet.
    Leader,
    /// A node hosting a non-leader replica of it.
    Follower,
    /// A specific node index (`node:N`), whatever it hosts: it may be the
    /// tablet's leader, a follower, or a node holding no replica of the
    /// table at all. Distinct from [`Self::Follower`], which resolves to a
    /// node that is verifiably a non-leader replica.
    Node(usize),
}

/// The degraded run: which workload, which fault, how long.
#[derive(Clone, Debug)]
pub struct DegradedConfig {
    pub kind: DegradedKind,
    pub workload: WorkloadKind,
    pub consistent_read: bool,
    pub baseline: Duration,
    pub degraded: Duration,
    pub recovery: Duration,
    pub restart: bool,
}

/// A whole YCSB invocation.
#[derive(Clone, Debug)]
pub struct YcsbConfig {
    pub workloads: Vec<WorkloadKind>,
    pub record_count: u64,
    pub value_bytes: usize,
    pub distribution: Distribution,
    pub max_scan_len: u32,
    pub read_modes: ReadModes,
    pub plan: PhasePlan,
    pub seed: u64,
    pub table_prefix: String,
    pub keep_tables: bool,
    pub load_parallelism: usize,
    pub setup_timeout: Duration,
    /// Runs once, last, on a fresh table (it damages the cluster).
    pub degraded: Option<DegradedConfig>,
}

fn spec_of(cfg: &YcsbConfig, kind: WorkloadKind) -> WorkloadSpec {
    WorkloadSpec {
        kind,
        record_count: cfg.record_count,
        value_bytes: cfg.value_bytes,
        distribution: cfg.distribution,
        max_scan_len: cfg.max_scan_len,
    }
}

fn with_placement(
    mut params: serde_json::Value,
    placement: &serde_json::Value,
) -> serde_json::Value {
    params["table_topology_after_load"] = placement.clone();
    params
}

fn params_of(cfg: &YcsbConfig, kind: WorkloadKind, table: &str, cr: bool) -> serde_json::Value {
    json!({
        "workload": kind.name(),
        "description": kind.description(),
        "mix": kind.mix(),
        "table": table,
        "record_count": cfg.record_count,
        "value_bytes": cfg.value_bytes,
        "distribution": if kind == WorkloadKind::D { "latest (zipfian distance from newest insert)".to_owned() } else { format!("{:?}", cfg.distribution).to_lowercase() },
        "max_scan_len": cfg.max_scan_len,
        "consistent_read": cr,
        "seed": cfg.seed,
        "arrival_rate": cfg.plan.rate,
        "sweep_rates": cfg.plan.sweep,
        "warmup_secs": cfg.plan.warmup.as_secs_f64(),
        "steady_secs": cfg.plan.steady.as_secs_f64(),
        "connections": cfg.plan.connections,
        "op_timeout_secs": cfg.plan.op_timeout.as_secs_f64(),
        "drain_secs": cfg.plan.drain_timeout.as_secs_f64(),
        "table_state": "this measurement ran on a table freshly created and loaded for it alone (never one a previous read mode or workload already mutated or warmed)",
        "key_layout": "pk S=user<10-digit idx/100>, sk N=idx%100; item {pk,sk,version:N,data:S(value_bytes)}; E scans = Query pk=:p AND sk>=:s Limit=len, within one partition",
    })
}

/// The table one `(workload, read mode)` measurement runs on.
#[must_use]
pub fn table_name(prefix: &str, kind: WorkloadKind, consistent_read: bool) -> String {
    format!(
        "{prefix}_{}_{}",
        kind.name().to_lowercase(),
        if consistent_read { "cr" } else { "ev" }
    )
}

/// Run every configured workload — each `(workload, read mode)` on its own
/// freshly created and loaded table — then the optional degraded run.
///
/// # Errors
/// If table creation or the load fails (a bench that cannot set up measures
/// nothing).
pub async fn run_ycsb(
    cluster: &Cluster,
    clock: &Clock,
    cfg: &YcsbConfig,
) -> Result<Vec<RunResult>, String> {
    let mut runs = Vec::new();
    for &kind in &cfg.workloads {
        // Each read mode gets its own freshly created and loaded table, so
        // the second mode never measures a table the first already mutated
        // (updates, inserts), compacted or warmed. The op stream is the
        // same seeded one for both.
        for cr in cfg.read_modes.list() {
            let table = table_name(&cfg.table_prefix, kind, cr);
            ycsb::create_table(cluster, &table, cfg.setup_timeout).await?;
            let load = ycsb::load_table(
                cluster,
                &table,
                cfg.record_count,
                cfg.value_bytes,
                cfg.load_parallelism,
                cfg.setup_timeout,
            )
            .await?;
            let placement = envinfo::capture_topology(cluster, Some(&table)).await["table"].clone();
            let mut source = OpStream::new(spec_of(cfg, kind), cfg.seed);
            let exec = Arc::new(YcsbExecutor {
                table: table.clone(),
                value_bytes: cfg.value_bytes,
                consistent_read: cr,
            });
            let (phases, sweep) = run_steady(cluster, clock, &cfg.plan, &mut source, exec).await;
            runs.push(RunResult {
                name: format!("ycsb-{}/consistent_read={cr}", kind.name()),
                params: with_placement(params_of(cfg, kind, &table, cr), &placement),
                load: Some(load),
                phases,
                sweep,
            });
            if !cfg.keep_tables {
                ycsb::drop_table(cluster, &table).await;
            }
        }
    }
    if let Some(d) = &cfg.degraded {
        runs.push(run_ycsb_degraded(cluster, clock, cfg, d).await?);
    }
    Ok(runs)
}

async fn run_ycsb_degraded(
    cluster: &Cluster,
    clock: &Clock,
    cfg: &YcsbConfig,
    d: &DegradedConfig,
) -> Result<RunResult, String> {
    let table = format!(
        "{}_degraded_{}",
        cfg.table_prefix,
        d.workload.name().to_lowercase()
    );
    if let DegradedKind::Node(n) = &d.kind
        && *n >= cluster.nodes().len()
    {
        return Err(format!(
            "--degraded node:{n}: the cluster has {} nodes (indices 0..{})",
            cluster.nodes().len(),
            cluster.nodes().len()
        ));
    }
    ycsb::create_table(cluster, &table, cfg.setup_timeout).await?;
    let load = ycsb::load_table(
        cluster,
        &table,
        cfg.record_count,
        cfg.value_bytes,
        cfg.load_parallelism,
        cfg.setup_timeout,
    )
    .await?;
    let fault = match &d.kind {
        DegradedKind::Leader => FaultAction::KillLeader {
            table: table.clone(),
        },
        DegradedKind::Follower => FaultAction::KillFollower {
            table: table.clone(),
        },
        DegradedKind::Node(n) => FaultAction::KillNode { node: *n },
    };
    let plan = DegradedPlan {
        rate: cfg.plan.rate,
        warmup: cfg.plan.warmup,
        baseline: d.baseline,
        degraded: d.degraded,
        recovery: d.recovery,
        fault,
        restart: d.restart,
        connections: cfg.plan.connections,
        op_timeout: cfg.plan.op_timeout,
        drain_timeout: cfg.plan.drain_timeout,
    };
    let mut source = OpStream::new(spec_of(cfg, d.workload), cfg.seed);
    let exec = Arc::new(YcsbExecutor {
        table: table.clone(),
        value_bytes: cfg.value_bytes,
        consistent_read: d.consistent_read,
    });
    let placement = envinfo::capture_topology(cluster, Some(&table)).await["table"].clone();
    let phases = run_degraded(cluster, clock, &plan, &mut source, exec).await;
    let mut params = with_placement(
        params_of(cfg, d.workload, &table, d.consistent_read),
        &placement,
    );
    params["degraded"] = json!({
        "victim": format!("{:?}", d.kind),
        "baseline_secs": d.baseline.as_secs_f64(),
        "degraded_secs": d.degraded.as_secs_f64(),
        "recovery_secs": d.recovery.as_secs_f64(),
        "restart_at_recovery": d.restart,
    });
    if !cfg.keep_tables {
        ycsb::drop_table(cluster, &table).await;
    }
    Ok(RunResult {
        name: format!(
            "ycsb-{}/degraded/consistent_read={}",
            d.workload.name(),
            d.consistent_read
        ),
        params,
        load: Some(load),
        phases,
        sweep: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_workload_and_read_mode_gets_its_own_table() {
        let names: Vec<String> = [WorkloadKind::A, WorkloadKind::B]
            .into_iter()
            .flat_map(|k| [true, false].map(|cr| table_name("ycsb1", k, cr)))
            .collect();
        let uniq: std::collections::BTreeSet<_> = names.iter().collect();
        assert_eq!(uniq.len(), names.len(), "{names:?}");
        assert_eq!(table_name("p", WorkloadKind::A, true), "p_a_cr");
        assert_eq!(table_name("p", WorkloadKind::A, false), "p_a_ev");
    }

    #[test]
    fn read_modes_expand_in_order() {
        assert_eq!(ReadModes::Both.list(), vec![true, false]);
        assert_eq!(ReadModes::Eventual.list(), vec![false]);
    }
}
