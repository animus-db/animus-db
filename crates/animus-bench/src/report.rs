//! The results document (`"schema": "animus-bench/v1"`) and its text
//! rendering. Everything here is plain serde data: another scenario (C-17)
//! produces [`RunResult`]s and pushes them into a [`Report`] without touching
//! the CLI.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cluster::NodeEndpoints;
use crate::engine::PhaseResult;
use crate::envinfo::HostInfo;
use crate::ycsb::LoadResult;

/// The results-file schema tag. Bump (and keep reading the old one) on a
/// breaking change to this document's shape.
pub const SCHEMA: &str = "animus-bench/v1";

/// One row of a throughput sweep: enough to plot latency vs. throughput and
/// find the knee.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SweepPoint {
    pub target_rate: f64,
    pub achieved_rate: f64,
    pub completed: u64,
    pub errors: u64,
    pub condition_failed: u64,
    pub abandoned: u64,
    /// Corrected (from intended send time) percentiles, µs.
    pub p50_us: u64,
    pub p99_us: u64,
    pub p99_9_us: u64,
    pub p99_99_us: u64,
    pub max_us: u64,
    /// The uncorrected (service-time) p99, µs, for contrast.
    pub service_p99_us: u64,
}

impl SweepPoint {
    /// Summarise a phase as a sweep row.
    #[must_use]
    pub fn from_phase(p: &PhaseResult) -> Self {
        Self {
            target_rate: p.target_rate,
            achieved_rate: p.achieved_rate,
            completed: p.completed,
            errors: p.errors.total(),
            condition_failed: p.condition_failed,
            abandoned: p.abandoned,
            p50_us: p.overall.corrected.p50_us,
            p99_us: p.overall.corrected.p99_us,
            p99_9_us: p.overall.corrected.p99_9_us,
            p99_99_us: p.overall.corrected.p99_99_us,
            max_us: p.overall.corrected.max_us,
            service_p99_us: p.overall.service.p99_us,
        }
    }
}

/// One measurement run: a workload (or scenario), its parameters, the load
/// that preceded it, its phases and optional sweep.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RunResult {
    /// e.g. `ycsb-A/consistent_read=true`.
    pub name: String,
    /// Scenario parameters, fully disclosed (free-form per scenario).
    pub params: Value,
    pub load: Option<LoadResult>,
    pub phases: Vec<PhaseResult>,
    pub sweep: Vec<SweepPoint>,
}

/// Where and how the run executed.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Environment {
    /// The machine running the load generator.
    pub client_host: HostInfo,
    /// True when the servers share the client's host (the bench launched
    /// them, or every endpoint is loopback).
    pub client_and_server_colocated: bool,
    /// `external` / `processes` / `in-process`.
    pub launch_mode: String,
    pub target_endpoints: Vec<NodeEndpoints>,
    pub node_count: usize,
    /// Requests are SigV4-signed.
    pub sigv4: bool,
    /// This client speaks plain TCP only; always false today.
    pub tls: bool,
    pub tls_note: String,
}

/// The whole results file.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Report {
    pub schema: String,
    pub generated_at_epoch_secs: u64,
    pub tool_version: String,
    pub git_sha: String,
    pub git_dirty: Option<bool>,
    /// The exact command line (`argv[1..]`).
    pub args: Vec<String>,
    pub seed: u64,
    /// False for any run that cannot back a published number.
    pub publishable: bool,
    pub publishable_reason: Option<String>,
    pub environment: Environment,
    /// Fixed disclosures (CO method, key layout, definitions).
    pub methodology: Value,
    /// Cluster topology/config as reported by `/admin`, before the run.
    pub topology_start: Value,
    /// ... and after it (splits and failovers may change it).
    pub topology_end: Value,
    /// Anything the run skipped or could not do, said plainly.
    pub notes: Vec<String>,
    pub runs: Vec<RunResult>,
}

impl Report {
    /// Serialise as pretty JSON.
    ///
    /// # Panics
    /// Never in practice (plain data).
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("report is plain data")
    }

    /// Parse a results file.
    ///
    /// # Errors
    /// On malformed JSON or a shape mismatch.
    pub fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }

    /// The human-readable summary printed to stdout.
    #[must_use]
    pub fn render_text(&self) -> String {
        use std::fmt::Write as _;
        let mut o = String::new();
        let e = &self.environment;
        let _ = writeln!(
            o,
            "animus-bench {}  schema {}",
            self.tool_version, self.schema
        );
        let _ = writeln!(
            o,
            "git {}{}   seed {}   host {} ({}, {} cpu {}, kernel {})",
            self.git_sha,
            if self.git_dirty == Some(true) {
                "+dirty"
            } else {
                ""
            },
            self.seed,
            e.client_host.hostname,
            e.client_host.cpu_model,
            e.client_host.cpu_count,
            e.client_host.memory_total_kb.map_or_else(
                || "mem unknown".to_owned(),
                |k| format!("{} GiB", k / 1024 / 1024)
            ),
            e.client_host.kernel,
        );
        let _ = writeln!(
            o,
            "cluster: {} nodes, launch={}, colocated={}, sigv4={}, tls={}",
            e.node_count, e.launch_mode, e.client_and_server_colocated, e.sigv4, e.tls
        );
        if !self.publishable {
            let _ = writeln!(
                o,
                "*** NOT PUBLISHABLE: {}",
                self.publishable_reason.as_deref().unwrap_or("see report")
            );
        }
        for n in &self.notes {
            let _ = writeln!(o, "note: {n}");
        }
        for run in &self.runs {
            let _ = writeln!(o, "\n== {} ==", run.name);
            let _ = writeln!(o, "params: {}", run.params);
            if let Some(l) = &run.load {
                let _ = writeln!(
                    o,
                    "load: {} records x {} B in {:.1}s ({:.0}/s, {} retried batches) [not a latency measurement]",
                    l.records, l.value_bytes, l.elapsed_secs, l.records_per_sec, l.retried_batches
                );
            }
            let _ = writeln!(
                o,
                "{:<22} {:>7} {:>8} {:>8} | {:>9} {:>9} {:>9} {:>9} {:>9} | {:>9} | err(thr/to/conn/oth) cond empty aband",
                "phase",
                "target",
                "achieved",
                "done",
                "p50",
                "p99",
                "p99.9",
                "p99.99",
                "max",
                "svc p99"
            );
            for p in &run.phases {
                if p.discarded {
                    let _ = writeln!(
                        o,
                        "{:<22} {:>7.0} {:>8.0} {:>8} | (warm-up: discarded)",
                        p.name, p.target_rate, p.achieved_rate, p.completed
                    );
                    continue;
                }
                let c = &p.overall.corrected;
                let _ = writeln!(
                    o,
                    "{:<22} {:>7.0} {:>8.0} {:>8} | {:>9} {:>9} {:>9} {:>9} {:>9} | {:>9} | {}/{}/{}/{} {} {} {}",
                    p.name,
                    p.target_rate,
                    p.achieved_rate,
                    p.completed,
                    fmt_us(c.p50_us),
                    fmt_us(c.p99_us),
                    fmt_us(c.p99_9_us),
                    fmt_us(c.p99_99_us),
                    fmt_us(c.max_us),
                    fmt_us(p.overall.service.p99_us),
                    p.errors.throttled,
                    p.errors.timeout,
                    p.errors.connection,
                    p.errors.other,
                    p.condition_failed,
                    p.empty_reads,
                    p.abandoned,
                );
                if let Some(f) = &p.fault {
                    let _ = writeln!(
                        o,
                        "    fault: {} node={:?} at +{} ms ok={} ({})",
                        f.action, f.node, f.at_offset_ms, f.ok, f.detail
                    );
                }
                for s in &p.error_samples {
                    let _ = writeln!(o, "    error sample: {s}");
                }
            }
            if !run.sweep.is_empty() {
                let _ = writeln!(o, "sweep (latencies corrected, from intended send time):");
                let _ = writeln!(
                    o,
                    "{:>9} {:>9} {:>9} {:>9} {:>9} {:>9} | {:>9} | errors aband",
                    "target/s", "achieved", "p50", "p99", "p99.9", "max", "svc p99"
                );
                for s in &run.sweep {
                    let _ = writeln!(
                        o,
                        "{:>9.0} {:>9.0} {:>9} {:>9} {:>9} {:>9} | {:>9} | {} {}",
                        s.target_rate,
                        s.achieved_rate,
                        fmt_us(s.p50_us),
                        fmt_us(s.p99_us),
                        fmt_us(s.p99_9_us),
                        fmt_us(s.max_us),
                        fmt_us(s.service_p99_us),
                        s.errors,
                        s.abandoned
                    );
                }
            }
        }
        let _ = writeln!(
            o,
            "\nlatencies: p50..max are measured from each op's INTENDED send time (coordinated-omission \
             corrected); 'svc p99' is from the actual send (what a closed-loop client would report)."
        );
        o
    }
}

fn fmt_us(us: u64) -> String {
    if us >= 1_000_000 {
        format!("{:.2}s", us as f64 / 1e6)
    } else if us >= 1_000 {
        format!("{:.2}ms", us as f64 / 1e3)
    } else {
        format!("{us}us")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_round_trips_through_json() {
        let mut r = Report {
            schema: SCHEMA.to_owned(),
            publishable: false,
            publishable_reason: Some("colocated".into()),
            ..Report::default()
        };
        r.runs.push(RunResult {
            name: "x".into(),
            params: serde_json::json!({"a": 1}),
            ..RunResult::default()
        });
        let back = Report::from_json(&r.to_json()).unwrap();
        assert_eq!(back, r);
        assert!(back.render_text().contains("NOT PUBLISHABLE"));
    }

    #[test]
    fn us_formatting() {
        assert_eq!(fmt_us(999), "999us");
        assert_eq!(fmt_us(1_500), "1.50ms");
        assert_eq!(fmt_us(2_000_000), "2.00s");
    }
}
