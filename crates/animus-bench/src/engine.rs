//! The open-loop engine: the thin I/O shell around the pure
//! [`Schedule`](crate::schedule::Schedule) and
//! [`OpRecorder`](crate::recorder::OpRecorder).
//!
//! `run_phase` is the library's central entry point. A phase is a fixed
//! arrival rate held for a fixed duration:
//!
//! 1. The **dispatcher** (the caller's task) walks the schedule. For arrival
//!    `i` it pulls the next op from the [`OpSource`], sleeps until the
//!    intended send time (sends immediately if already late — it never skips
//!    or delays an arrival), and pushes `(intended, op)` onto an unbounded
//!    queue.
//! 2. `connections` **workers**, each with one keep-alive connection, pull
//!    from the queue, execute via the [`OpExecutor`], and record
//!    `(intended, started, completed)`. If the server stalls, or the offered
//!    rate exceeds what the connection pool can drive, ops wait in the queue
//!    and that wait is *charged to the op's latency* (it is measured from the
//!    intended time) — nothing is hidden by the generator slowing down.
//! 3. After the last arrival the workers drain the queue; ops still unstarted
//!    `drain_timeout` after the phase window ends are counted as `abandoned`
//!    (and flagged in the report) rather than waited on forever.
//!
//! Errors are **counted by kind, not recorded in the latency histograms**
//! (a timeout is not a latency sample); a `ConditionalCheckFailed` or an
//! empty read is a *completed* operation and is recorded. A fault (node kill)
//! can be scheduled at an offset into the phase.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc};

use crate::client::Conn;
use crate::cluster::{Cluster, FaultAction, FaultRecord};
use crate::recorder::{ClassResult, OpRecorder};
use crate::rt::{self, Clock};
use crate::schedule::{NS_PER_SEC, Schedule};

/// Why an operation failed (it is then counted, not timed).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    /// `ProvisionedThroughputExceeded` / `ThrottlingException` / `RequestLimitExceeded`.
    Throttled,
    /// The client-side per-operation timeout fired.
    Timeout,
    /// Connect / socket / framing failure (a killed node looks like this).
    Connection,
    /// Any other error response (5xx, validation, ...).
    Other,
}

/// What happened to one executed operation.
#[derive(Clone, Debug)]
pub enum Outcome {
    /// Completed normally.
    Ok,
    /// Completed, but a read found nothing (counted separately: under
    /// `ConsistentRead: false` a lagging replica can legitimately miss a
    /// just-written key).
    OkEmpty,
    /// A conditional write's condition did not hold — an *expected*
    /// outcome of workload F's races, not an error; recorded as completed.
    ConditionFailed,
    /// Failed; counted by kind.
    Err { kind: ErrorKind, detail: String },
}

/// Where a phase's operations come from. Called from one task, in arrival
/// order, so a seeded implementation yields a reproducible stream.
pub trait OpSource: Send {
    type Op: Send + 'static;
    fn next_op(&mut self) -> Self::Op;
}

/// How an operation is executed over a connection.
pub trait OpExecutor: Send + Sync + 'static {
    type Op: Send + 'static;
    /// The latency class this op is bucketed under.
    fn class(&self, op: &Self::Op) -> &'static str;
    /// Execute `op` over `conn`. Any socket error should leave `conn`
    /// broken (the [`Conn`] methods do this) and map to
    /// [`ErrorKind::Connection`]; the engine redials.
    fn execute<'a>(
        &'a self,
        conn: &'a mut Conn,
        op: &'a Self::Op,
    ) -> impl Future<Output = Outcome> + Send + 'a;
}

/// A fault to inject `after` into a phase.
#[derive(Clone, Debug)]
pub struct FaultPlan {
    pub after: Duration,
    pub action: FaultAction,
}

/// One phase: `rate` ops/sec for `duration`.
#[derive(Clone, Debug)]
pub struct PhaseSpec {
    pub name: String,
    pub duration: Duration,
    pub rate: f64,
    /// Run it, but report only counts (no histograms): warm-up.
    pub discard: bool,
    /// Worker connections (the client-side concurrency cap).
    pub connections: usize,
    /// Per-operation client timeout.
    pub op_timeout: Duration,
    /// Grace after the phase window for queued ops to finish.
    pub drain_timeout: Duration,
    pub fault: Option<FaultPlan>,
}

impl PhaseSpec {
    /// A phase with the default 64 connections, 10 s op timeout, 30 s drain.
    #[must_use]
    pub fn new(name: impl Into<String>, duration: Duration, rate: f64) -> Self {
        Self {
            name: name.into(),
            duration,
            rate,
            discard: false,
            connections: 64,
            op_timeout: Duration::from_secs(10),
            drain_timeout: Duration::from_secs(30),
            fault: None,
        }
    }
}

/// Error counts by kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorCounts {
    pub throttled: u64,
    pub timeout: u64,
    pub connection: u64,
    pub other: u64,
}

impl ErrorCounts {
    #[must_use]
    pub fn total(&self) -> u64 {
        self.throttled + self.timeout + self.connection + self.other
    }

    fn bump(&mut self, k: ErrorKind) {
        match k {
            ErrorKind::Throttled => self.throttled += 1,
            ErrorKind::Timeout => self.timeout += 1,
            ErrorKind::Connection => self.connection += 1,
            ErrorKind::Other => self.other += 1,
        }
    }

    fn add(&mut self, o: &Self) {
        self.throttled += o.throttled;
        self.timeout += o.timeout;
        self.connection += o.connection;
        self.other += o.other;
    }
}

/// The measured outcome of one phase (JSON-serialisable).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PhaseResult {
    pub name: String,
    /// Warm-up: histograms intentionally omitted.
    pub discarded: bool,
    /// Phase start, ms since the run's clock origin.
    pub start_offset_ms: u64,
    /// Configured window.
    pub duration_secs: f64,
    /// Window plus drain: start to the last completion.
    pub elapsed_secs: f64,
    pub target_rate: f64,
    /// Arrivals scheduled (== sent or abandoned).
    pub dispatched: u64,
    /// Operations that completed and were timed (ok + empty + condition-failed).
    pub completed: u64,
    /// `completed / elapsed_secs`.
    pub achieved_rate: f64,
    pub errors: ErrorCounts,
    /// Conditional-write races lost (expected under F; **not** an error).
    pub condition_failed: u64,
    /// Reads that returned no item / empty page.
    pub empty_reads: u64,
    /// Arrivals never started because the drain deadline passed (overload).
    pub abandoned: u64,
    /// The first few distinct error messages seen.
    pub error_samples: Vec<String>,
    pub overall: ClassResult,
    pub classes: BTreeMap<String, ClassResult>,
    pub fault: Option<FaultRecord>,
}

#[derive(Default)]
struct PhaseRecorder {
    classes: BTreeMap<&'static str, OpRecorder>,
    overall: OpRecorder,
    errors: ErrorCounts,
    condition_failed: u64,
    empty_reads: u64,
    abandoned: u64,
    samples: Vec<String>,
}

impl PhaseRecorder {
    fn merge(&mut self, o: Self) {
        for (k, v) in o.classes {
            self.classes.entry(k).or_default().merge(&v);
        }
        self.overall.merge(&o.overall);
        self.errors.add(&o.errors);
        self.condition_failed += o.condition_failed;
        self.empty_reads += o.empty_reads;
        self.abandoned += o.abandoned;
        for s in o.samples {
            if self.samples.len() < 5 && !self.samples.contains(&s) {
                self.samples.push(s);
            }
        }
    }
}

struct Queued<Op> {
    intended_ns: u64,
    op: Op,
}

#[allow(clippy::too_many_arguments)]
async fn worker<X: OpExecutor>(
    id: usize,
    cluster: Cluster,
    exec: Arc<X>,
    rx: Arc<Mutex<mpsc::UnboundedReceiver<Queued<X::Op>>>>,
    clock: Clock,
    op_timeout: Duration,
    hard_deadline_ns: u64,
) -> PhaseRecorder {
    let endpoints = cluster.dynamo_endpoints();
    let creds = cluster.credentials();
    let mut cursor = id % endpoints.len().max(1);
    let mut conn: Option<Conn> = cluster.checkout();
    let mut rec = PhaseRecorder::default();
    loop {
        let item = {
            let mut g = rx.lock().await;
            g.recv().await
        };
        let Some(q) = item else { break };
        let class = exec.class(&q.op);
        let started = clock.now_ns();
        if started > hard_deadline_ns {
            rec.abandoned += 1;
            continue;
        }
        if conn.as_ref().is_none_or(Conn::is_broken) {
            conn = None;
            for attempt in 0..endpoints.len() {
                let i = (cursor + attempt) % endpoints.len();
                if let Some(Ok(c)) = rt::timeout(
                    Duration::from_secs(2),
                    Conn::connect(endpoints[i], creds.clone()),
                )
                .await
                {
                    cursor = i;
                    conn = Some(c);
                    break;
                }
            }
        }
        let outcome = match conn.as_mut() {
            None => {
                cursor = (cursor + 1) % endpoints.len().max(1);
                Outcome::Err {
                    kind: ErrorKind::Connection,
                    detail: "no endpoint reachable".to_owned(),
                }
            }
            Some(c) => match rt::timeout(op_timeout, exec.execute(c, &q.op)).await {
                Some(o) => o,
                None => {
                    // The request's fate is unknown: never reuse the socket.
                    conn = None;
                    Outcome::Err {
                        kind: ErrorKind::Timeout,
                        detail: format!("no response within {op_timeout:?}"),
                    }
                }
            },
        };
        let completed = clock.now_ns();
        if conn.as_ref().is_some_and(Conn::is_broken) {
            conn = None;
            cursor = (cursor + 1) % endpoints.len().max(1);
        }
        match outcome {
            Outcome::Ok | Outcome::OkEmpty | Outcome::ConditionFailed => {
                match outcome {
                    Outcome::OkEmpty => rec.empty_reads += 1,
                    Outcome::ConditionFailed => rec.condition_failed += 1,
                    _ => {}
                }
                rec.classes
                    .entry(class)
                    .or_default()
                    .record(q.intended_ns, started, completed);
                rec.overall.record(q.intended_ns, started, completed);
            }
            Outcome::Err { kind, detail } => {
                rec.errors.bump(kind);
                if rec.samples.len() < 5 && !rec.samples.contains(&detail) {
                    rec.samples.push(detail);
                }
            }
        }
    }
    if let Some(c) = conn {
        cluster.checkin(c);
    }
    rec
}

/// Run one open-loop phase against `cluster`. See the module docs.
pub async fn run_phase<S, X>(
    cluster: &Cluster,
    clock: &Clock,
    spec: &PhaseSpec,
    source: &mut S,
    exec: Arc<X>,
) -> PhaseResult
where
    S: OpSource,
    X: OpExecutor<Op = S::Op>,
{
    let t0 = clock.now_ns();
    let duration_ns = u64::try_from(spec.duration.as_nanos()).unwrap_or(u64::MAX);
    let schedule = Schedule::new(t0, spec.rate);
    let count = schedule.count_in(duration_ns);
    let hard_deadline_ns = t0
        .saturating_add(duration_ns)
        .saturating_add(u64::try_from(spec.drain_timeout.as_nanos()).unwrap_or(u64::MAX));

    let (tx, rx) = mpsc::unbounded_channel::<Queued<S::Op>>();
    let rx = Arc::new(Mutex::new(rx));
    let workers: Vec<_> = (0..spec.connections.max(1))
        .map(|id| {
            rt::spawn(worker(
                id,
                cluster.clone(),
                exec.clone(),
                rx.clone(),
                *clock,
                spec.op_timeout,
                hard_deadline_ns,
            ))
        })
        .collect();

    let fault = spec.fault.clone().map(|plan| {
        let (c, clk) = (cluster.clone(), *clock);
        rt::spawn(async move {
            rt::sleep(plan.after).await;
            let at_ms = clk.now_ns().saturating_sub(t0) / 1_000_000;
            let mut rec = c.apply_fault(&plan.action).await;
            rec.at_offset_ms = at_ms;
            rec
        })
    });

    for i in 0..count {
        let intended_ns = schedule.intended_ns(i);
        let op = source.next_op();
        let now = clock.now_ns();
        if intended_ns > now {
            rt::sleep(Duration::from_nanos(intended_ns - now)).await;
        }
        // Receivers outlive the loop; a send can only fail if every worker
        // panicked, which the join below surfaces.
        let _ = tx.send(Queued { intended_ns, op });
    }
    drop(tx);

    let mut merged = PhaseRecorder::default();
    for w in workers {
        merged.merge(w.await.expect("bench worker panicked"));
    }
    let fault = match fault {
        Some(h) => Some(h.await.expect("fault task panicked")),
        None => None,
    };
    let end = clock.now_ns();
    let elapsed_secs = (end.saturating_sub(t0)) as f64 / NS_PER_SEC as f64;
    let completed = merged.overall.count();
    PhaseResult {
        name: spec.name.clone(),
        discarded: spec.discard,
        start_offset_ms: t0 / 1_000_000,
        duration_secs: spec.duration.as_secs_f64(),
        elapsed_secs,
        target_rate: spec.rate,
        dispatched: count,
        completed,
        achieved_rate: if elapsed_secs > 0.0 {
            completed as f64 / elapsed_secs
        } else {
            0.0
        },
        errors: merged.errors,
        condition_failed: merged.condition_failed,
        empty_reads: merged.empty_reads,
        abandoned: merged.abandoned,
        error_samples: merged.samples,
        overall: if spec.discard {
            ClassResult::default()
        } else {
            merged.overall.summary()
        },
        classes: if spec.discard {
            BTreeMap::new()
        } else {
            merged
                .classes
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.summary()))
                .collect()
        },
        fault,
    }
}
