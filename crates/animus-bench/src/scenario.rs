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
}

impl PhasePlan {
    fn spec(&self, name: impl Into<String>, duration: Duration, rate: f64) -> PhaseSpec {
        let mut s = PhaseSpec::new(name, duration, rate);
        s.connections = self.connections;
        s.op_timeout = self.op_timeout;
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
    /// A specific node index.
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
        "key_layout": "pk S=user<10-digit idx/100>, sk N=idx%100; item {pk,sk,version:N,data:S(value_bytes)}; E scans = Query pk=:p AND sk>=:s Limit=len, within one partition",
    })
}

/// Run every configured workload (each on its own freshly created and
/// loaded table, once per read mode), then the optional degraded run.
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
        let table = format!("{}_{}", cfg.table_prefix, kind.name().to_lowercase());
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
        for (n, cr) in cfg.read_modes.list().into_iter().enumerate() {
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
                // The load happened once per table; attach it to the first run.
                load: (n == 0).then(|| load.clone()),
                phases,
                sweep,
            });
        }
        if !cfg.keep_tables {
            ycsb::drop_table(cluster, &table).await;
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
