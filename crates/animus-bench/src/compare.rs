//! `animus-bench compare`: an A/B comparison of results files — pure, no
//! clock, no I/O (the CLI wrapper in `main.rs` reads the files).
//!
//! Input is two *groups* of results files: `--base` (the reference build) and
//! `--head` (the candidate), each typically 2+ interleaved runs of the same
//! command (`base, head, base, head`) so run-to-run spread is visible. For
//! every `(run, phase, metric)` present in both groups the table shows each
//! group's median, its **spread** (`(max-min)/median`, the run-to-run noise
//! of that group) and the median delta, then a verdict:
//!
//! - `ok` — `|delta|` is within the threshold (a *disclosed input*, never a
//!   hidden constant);
//! - `REGRESSION` / `improvement` — `|delta|` exceeds the threshold **and** the
//!   two groups' [min, max] ranges do not overlap, i.e. the shift is larger
//!   than the observed run-to-run noise;
//! - `noisy` — `|delta|` exceeds the threshold but the ranges overlap (or a
//!   group has a single run), so the data cannot tell a change from noise.
//!
//! This is **reporting only**: [`Comparison::render_markdown`] never fails on
//! a latency number, and the CLI exits 0 for any well-formed input. It
//! compares builds on *one* host; it says nothing about absolute performance
//! and nothing about any other database.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::engine::PhaseResult;
use crate::report::{Report, RunResult};

/// Default flag threshold, percent.
pub const DEFAULT_THRESHOLD_PCT: f64 = 10.0;

/// Usage for the subcommand.
pub const COMPARE_USAGE: &str = "\
animus-bench compare [--threshold-pct P] [--out FILE] --base A.json [A2.json ...] --head B.json [B2.json ...]

  --base FILES        results of the reference build (>= 1; >= 2 shows run-to-run spread)
  --head FILES        results of the candidate build
  --threshold-pct P   a median delta beyond P percent is flagged when it also exceeds the
                      run-to-run spread (default 10); reporting only, the exit code is 0
  --out FILE          also write the markdown table here
";

/// Parsed `compare` arguments.
#[derive(Clone, Debug, PartialEq)]
pub struct CompareArgs {
    pub base: Vec<String>,
    pub head: Vec<String>,
    pub threshold_pct: f64,
    pub out: Option<String>,
}

/// Parse the arguments after the `compare` word.
///
/// # Errors
/// A message for an unknown flag, a bad threshold, or an empty group.
pub fn parse_compare_args(args: &[String]) -> Result<CompareArgs, String> {
    #[derive(PartialEq)]
    enum Group {
        None,
        Base,
        Head,
    }
    let mut a = CompareArgs {
        base: Vec::new(),
        head: Vec::new(),
        threshold_pct: DEFAULT_THRESHOLD_PCT,
        out: None,
    };
    let mut group = Group::None;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--base" => group = Group::Base,
            "--head" => group = Group::Head,
            "--threshold-pct" => {
                let v = it.next().ok_or("--threshold-pct needs a value")?;
                a.threshold_pct = v
                    .parse()
                    .map_err(|_| format!("--threshold-pct: cannot parse `{v}`"))?;
                if !(a.threshold_pct.is_finite() && a.threshold_pct >= 0.0) {
                    return Err("--threshold-pct must be finite and >= 0".into());
                }
                group = Group::None;
            }
            "--out" => {
                a.out = Some(it.next().ok_or("--out needs a value")?.clone());
                group = Group::None;
            }
            f if f.starts_with("--") => return Err(format!("unknown argument `{f}`")),
            file => match group {
                Group::Base => a.base.push(file.to_owned()),
                Group::Head => a.head.push(file.to_owned()),
                Group::None => {
                    return Err(format!("`{file}` is not under --base or --head"));
                }
            },
        }
    }
    if a.base.is_empty() || a.head.is_empty() {
        return Err("both --base and --head need at least one results file".into());
    }
    Ok(a)
}

/// The compared metrics: (label, higher is better, extractor over a phase).
type Extract = fn(&PhaseResult) -> f64;
const PHASE_METRICS: &[(&str, bool, Extract)] = &[
    ("p50 ms", false, |p| p.overall.corrected.p50_us as f64 / 1e3),
    ("p99 ms", false, |p| p.overall.corrected.p99_us as f64 / 1e3),
    ("p99.9 ms", false, |p| {
        p.overall.corrected.p99_9_us as f64 / 1e3
    }),
    ("achieved/s", true, |p| p.achieved_rate),
];

/// Verdict for one row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Ok,
    Regression,
    Improvement,
    /// Beyond the threshold, but not distinguishable from run-to-run noise.
    Noisy,
}

impl Verdict {
    fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Regression => "**REGRESSION**",
            Self::Improvement => "improvement",
            Self::Noisy => "noisy",
        }
    }
}

/// One `(run, phase, metric)` comparison.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub run: String,
    pub phase: String,
    pub metric: &'static str,
    pub higher_is_better: bool,
    pub base: Vec<f64>,
    pub head: Vec<f64>,
    pub base_median: f64,
    pub head_median: f64,
    /// `(head - base) / base`, percent (`inf` when base is 0 and head is not).
    pub delta_pct: f64,
    pub base_spread_pct: f64,
    pub head_spread_pct: f64,
    pub verdict: Verdict,
}

/// The whole comparison.
#[derive(Clone, Debug, PartialEq)]
pub struct Comparison {
    pub threshold_pct: f64,
    pub base_runs: usize,
    pub head_runs: usize,
    pub rows: Vec<Row>,
    /// Things the reader must know: mismatched hosts, series present on one
    /// side only, single-run groups.
    pub notes: Vec<String>,
}

fn median(v: &[f64]) -> f64 {
    let mut s = v.to_vec();
    s.sort_by(f64::total_cmp);
    let n = s.len();
    if n == 0 {
        0.0
    } else if n % 2 == 1 {
        s[n / 2]
    } else {
        f64::midpoint(s[n / 2 - 1], s[n / 2])
    }
}

fn min_max(v: &[f64]) -> (f64, f64) {
    v.iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), x| {
            (lo.min(*x), hi.max(*x))
        })
}

fn spread_pct(v: &[f64]) -> f64 {
    let m = median(v);
    if v.len() < 2 || m == 0.0 {
        return 0.0;
    }
    let (lo, hi) = min_max(v);
    (hi - lo) / m.abs() * 100.0
}

/// Flatten a report into `(run, phase, metric label, higher_is_better) -> value`.
fn flatten(r: &Report) -> BTreeMap<(String, String, &'static str, bool), f64> {
    let mut m = BTreeMap::new();
    for run in &r.runs {
        for p in run.phases.iter().filter(|p| !p.discarded) {
            for (label, hib, f) in PHASE_METRICS {
                m.insert((run.name.clone(), p.name.clone(), *label, *hib), f(p));
            }
        }
        sweep_rows(run, &mut m);
    }
    m
}

fn sweep_rows(run: &RunResult, m: &mut BTreeMap<(String, String, &'static str, bool), f64>) {
    for s in &run.sweep {
        let phase = format!("sweep@{:.0}/s", s.target_rate);
        let vals: [(&'static str, bool, f64); 4] = [
            ("p50 ms", false, s.p50_us as f64 / 1e3),
            ("p99 ms", false, s.p99_us as f64 / 1e3),
            ("p99.9 ms", false, s.p99_9_us as f64 / 1e3),
            ("achieved/s", true, s.achieved_rate),
        ];
        for (label, hib, v) in vals {
            m.insert((run.name.clone(), phase.clone(), label, hib), v);
        }
    }
}

/// Compare two groups of reports.
#[must_use]
pub fn compare(base: &[Report], head: &[Report], threshold_pct: f64) -> Comparison {
    type Key = (String, String, &'static str, bool);
    let collect = |g: &[Report]| {
        let mut by: BTreeMap<Key, Vec<f64>> = BTreeMap::new();
        for r in g {
            for (k, v) in flatten(r) {
                by.entry(k).or_default().push(v);
            }
        }
        by
    };
    let (b, h) = (collect(base), collect(head));
    let mut notes = Vec::new();
    if base.len() < 2 || head.len() < 2 {
        notes.push(
            "a group has a single run: no run-to-run spread can be measured, so nothing beyond the \
             threshold is called a regression/improvement (reported as `noisy`)"
                .to_owned(),
        );
    }
    let hosts = |g: &[Report]| {
        g.iter()
            .map(|r| {
                format!(
                    "{} / {} cpu",
                    r.environment.client_host.cpu_model, r.environment.client_host.cpu_count
                )
            })
            .collect::<std::collections::BTreeSet<_>>()
    };
    let (bh, hh) = (hosts(base), hosts(head));
    if bh != hh || bh.len() > 1 {
        notes.push(format!(
            "the runs did not all execute on the same host shape (base: {bh:?}, head: {hh:?}); \
             the comparison is only meaningful on one host"
        ));
    }
    if base
        .iter()
        .chain(head)
        .any(|r| r.environment.client_and_server_colocated)
    {
        notes.push(
            "client and server were colocated on one host: use these deltas to spot build-to-build \
             movement only; the absolute numbers are not a baseline and are not publishable"
                .to_owned(),
        );
    }
    let mut rows = Vec::new();
    for (key, bv) in &b {
        let Some(hv) = h.get(key) else {
            notes.push(format!("{} / {}: present in base only", key.0, key.1));
            continue;
        };
        let (bm, hm) = (median(bv), median(hv));
        let delta_pct = if bm == 0.0 {
            if hm == 0.0 { 0.0 } else { f64::INFINITY }
        } else {
            (hm - bm) / bm.abs() * 100.0
        };
        let (bmin, bmax) = min_max(bv);
        let (hmin, hmax) = min_max(hv);
        let disjoint = hmin > bmax || bmin > hmax;
        let verdict = if delta_pct.abs() <= threshold_pct {
            Verdict::Ok
        } else if bv.len() < 2 || hv.len() < 2 || !disjoint {
            Verdict::Noisy
        } else if (delta_pct > 0.0) == key.3 {
            Verdict::Improvement
        } else {
            Verdict::Regression
        };
        rows.push(Row {
            run: key.0.clone(),
            phase: key.1.clone(),
            metric: key.2,
            higher_is_better: key.3,
            base: bv.clone(),
            head: hv.clone(),
            base_median: bm,
            head_median: hm,
            delta_pct,
            base_spread_pct: spread_pct(bv),
            head_spread_pct: spread_pct(hv),
            verdict,
        });
    }
    for key in h.keys().filter(|k| !b.contains_key(*k)) {
        notes.push(format!("{} / {}: present in head only", key.0, key.1));
    }
    notes.sort();
    notes.dedup();
    Comparison {
        threshold_pct,
        base_runs: base.len(),
        head_runs: head.len(),
        rows,
        notes,
    }
}

impl Comparison {
    /// Rows with a verdict other than `ok`.
    #[must_use]
    pub fn flagged(&self) -> Vec<&Row> {
        self.rows
            .iter()
            .filter(|r| r.verdict != Verdict::Ok)
            .collect()
    }

    /// A GitHub-flavoured markdown table plus the disclosures.
    #[must_use]
    pub fn render_markdown(&self) -> String {
        let mut o = String::new();
        let _ = writeln!(
            o,
            "### animus-bench A/B: base ({} runs) vs head ({} runs)\n",
            self.base_runs, self.head_runs
        );
        let _ = writeln!(
            o,
            "Latencies are coordinated-omission corrected (from intended send time). Medians over the \
             runs; **spread** = (max-min)/median across a group's runs. A row is flagged only when \
             |delta| > **{:.1}%** (threshold input) **and** the base and head [min,max] ranges do not \
             overlap (the shift exceeds the observed run-to-run noise); otherwise a large delta is \
             `noisy`. Reporting only: no latency number fails a job.\n",
            self.threshold_pct
        );
        let _ = writeln!(
            o,
            "| run | phase | metric | base median | base spread | head median | head spread | delta | verdict |"
        );
        let _ = writeln!(o, "|---|---|---|---:|---:|---:|---:|---:|---|");
        for r in &self.rows {
            let delta = if r.delta_pct.is_finite() {
                format!("{:+.1}%", r.delta_pct)
            } else {
                "n/a (base 0)".to_owned()
            };
            let _ = writeln!(
                o,
                "| {} | {} | {} | {:.2} | {:.1}% | {:.2} | {:.1}% | {} | {} |",
                r.run,
                r.phase,
                r.metric,
                r.base_median,
                r.base_spread_pct,
                r.head_median,
                r.head_spread_pct,
                delta,
                r.verdict.label()
            );
        }
        let flagged = self.flagged();
        let _ = writeln!(
            o,
            "\n{} of {} rows beyond the threshold ({} regression, {} improvement, {} noisy).",
            flagged.len(),
            self.rows.len(),
            flagged
                .iter()
                .filter(|r| r.verdict == Verdict::Regression)
                .count(),
            flagged
                .iter()
                .filter(|r| r.verdict == Verdict::Improvement)
                .count(),
            flagged
                .iter()
                .filter(|r| r.verdict == Verdict::Noisy)
                .count(),
        );
        for n in &self.notes {
            let _ = writeln!(o, "\n> note: {n}");
        }
        o
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::PhaseResult;
    use crate::recorder::{ClassResult, LatencySummary};

    fn report(p50_ms: f64, p99_ms: f64, rate: f64) -> Report {
        let lat = |p: f64| LatencySummary {
            p50_us: (p50_ms * 1e3) as u64,
            p99_us: (p * 1e3) as u64,
            p99_9_us: (p * 1e3) as u64,
            ..LatencySummary::default()
        };
        let mut r = Report::default();
        r.environment.client_host.cpu_count = 4;
        r.runs.push(RunResult {
            name: "ycsb-A/consistent_read=true".into(),
            phases: vec![
                PhaseResult {
                    name: "warmup".into(),
                    discarded: true,
                    ..PhaseResult::default()
                },
                PhaseResult {
                    name: "steady".into(),
                    achieved_rate: rate,
                    overall: ClassResult {
                        count: 1,
                        corrected: lat(p99_ms),
                        service: lat(p99_ms),
                    },
                    ..PhaseResult::default()
                },
            ],
            ..RunResult::default()
        });
        r
    }

    fn row<'a>(c: &'a Comparison, metric: &str) -> &'a Row {
        c.rows.iter().find(|r| r.metric == metric).unwrap()
    }

    #[test]
    fn identical_groups_flag_nothing() {
        let g = vec![report(5.0, 20.0, 300.0), report(5.0, 20.0, 300.0)];
        let c = compare(&g, &g, 10.0);
        assert!(c.flagged().is_empty());
        assert_eq!(c.rows.len(), 4, "warm-up is never compared");
        assert_eq!(row(&c, "p99 ms").delta_pct, 0.0);
    }

    #[test]
    fn disjoint_shift_beyond_threshold_is_a_regression() {
        let base = vec![report(5.0, 20.0, 300.0), report(5.0, 22.0, 300.0)];
        let head = vec![report(5.0, 40.0, 300.0), report(5.0, 44.0, 300.0)];
        let c = compare(&base, &head, 10.0);
        let r = row(&c, "p99 ms");
        assert_eq!(r.verdict, Verdict::Regression);
        assert!((r.base_median - 21.0).abs() < 1e-9 && (r.head_median - 42.0).abs() < 1e-9);
        assert!((r.delta_pct - 100.0).abs() < 1e-9);
        assert!((r.base_spread_pct - 2.0 / 21.0 * 100.0).abs() < 1e-9);
        assert_eq!(row(&c, "p50 ms").verdict, Verdict::Ok);
        assert!(c.render_markdown().contains("**REGRESSION**"));
    }

    #[test]
    fn a_big_delta_inside_the_run_to_run_spread_is_noisy_not_a_regression() {
        // Base runs span 10..40 ms; head 30..45 ms: median delta is large
        // but the ranges overlap, so the data cannot call it.
        let base = vec![report(5.0, 10.0, 300.0), report(5.0, 40.0, 300.0)];
        let head = vec![report(5.0, 30.0, 300.0), report(5.0, 45.0, 300.0)];
        let c = compare(&base, &head, 10.0);
        assert_eq!(row(&c, "p99 ms").verdict, Verdict::Noisy);
    }

    #[test]
    fn a_single_run_group_can_never_call_a_regression() {
        let c = compare(
            &[report(5.0, 20.0, 300.0)],
            &[report(5.0, 80.0, 300.0)],
            10.0,
        );
        assert_eq!(row(&c, "p99 ms").verdict, Verdict::Noisy);
        assert!(c.render_markdown().contains("single run"));
    }

    #[test]
    fn throughput_direction_is_inverted() {
        let base = vec![report(5.0, 20.0, 300.0), report(5.0, 20.0, 298.0)];
        let head = vec![report(5.0, 20.0, 200.0), report(5.0, 20.0, 205.0)];
        let c = compare(&base, &head, 10.0);
        assert_eq!(row(&c, "achieved/s").verdict, Verdict::Regression);
        let c = compare(&head, &base, 10.0);
        assert_eq!(row(&c, "achieved/s").verdict, Verdict::Improvement);
    }

    #[test]
    fn threshold_is_disclosed_and_respected() {
        let base = vec![report(5.0, 20.0, 300.0), report(5.0, 20.0, 300.0)];
        let head = vec![report(5.0, 22.0, 300.0), report(5.0, 22.0, 300.0)];
        assert_eq!(
            row(&compare(&base, &head, 20.0), "p99 ms").verdict,
            Verdict::Ok
        );
        let c = compare(&base, &head, 5.0);
        assert_eq!(row(&c, "p99 ms").verdict, Verdict::Regression);
        assert!(c.render_markdown().contains("**5.0%**"));
    }

    #[test]
    fn series_on_one_side_only_are_reported_not_dropped_silently() {
        let base = vec![report(5.0, 20.0, 300.0)];
        let mut other = report(5.0, 20.0, 300.0);
        other.runs[0].name = "ycsb-B/consistent_read=true".into();
        let c = compare(&base, &[other], 10.0);
        assert!(c.rows.is_empty());
        let md = c.render_markdown();
        assert!(md.contains("ycsb-A/consistent_read=true / steady: present in base only"));
        assert!(md.contains("ycsb-B/consistent_read=true / steady: present in head only"));
    }

    #[test]
    fn args_parse_and_reject() {
        let s = |v: &[&str]| v.iter().map(|x| (*x).to_owned()).collect::<Vec<_>>();
        let a = parse_compare_args(&s(&[
            "--base",
            "a1.json",
            "a2.json",
            "--head",
            "b1.json",
            "--threshold-pct",
            "7.5",
            "--out",
            "o.md",
        ]))
        .unwrap();
        assert_eq!(a.base, ["a1.json", "a2.json"]);
        assert_eq!(a.head, ["b1.json"]);
        assert!((a.threshold_pct - 7.5).abs() < f64::EPSILON);
        assert_eq!(a.out.as_deref(), Some("o.md"));
        assert!(parse_compare_args(&s(&["--base", "a.json"])).is_err());
        assert!(parse_compare_args(&s(&["stray.json"])).is_err());
        assert!(
            parse_compare_args(&s(&["--threshold-pct", "-1", "--base", "a", "--head", "b"]))
                .is_err()
        );
    }
}
