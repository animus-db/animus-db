//! The command line: argument parsing and the top-level `execute` that
//! launches/attaches a cluster, runs the scenarios and assembles the
//! [`Report`]. `main` is a thin wrapper; the integration test calls
//! [`execute`] directly.

use std::path::PathBuf;
use std::time::Duration;

use serde_json::json;

use crate::client::Credentials;
use crate::cluster::{Cluster, NodeEndpoints};
use crate::dist::Distribution;
use crate::envinfo::{self, HostInfo};
use crate::report::{Environment, Report, SCHEMA, publishability};
use crate::rt::{self, Clock};
use crate::scenario::{DegradedConfig, DegradedKind, PhasePlan, ReadModes, YcsbConfig, run_ycsb};
use crate::workload::WorkloadKind;

/// Usage text.
pub const USAGE: &str = "\
animus-bench [run] [options]      open-loop YCSB A-F load generator over the DynamoDB wire
animus-bench compare ...          A/B-compare results files (see `animus-bench compare --help`)

cluster (pick one):
  --nodes D@A[,D@A...]        attach to a running cluster: DynamoDB addr @ admin addr per node
  --launch processes          spawn --cluster-size `animusd` children on this host (NOT publishable)
  --launch in-process         run the nodes inside this process (NOT publishable)
  --cluster-size N            nodes to launch (default 3)
  --animusd-bin PATH          animusd binary for --launch processes (default: next to this binary)
  --animusd-arg ARG           extra animusd flag (repeatable), e.g. --animusd-arg --no-shared-wal
  --data-dir DIR              launch working dir (default: a fresh dir under $TMPDIR)
auth:
  --access-key ID --secret-key SECRET   SigV4 credentials (or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)
  --no-auth                   do not sign (launched clusters sign by default)
workload:
  --workloads A,B,..|all      (default A)
  --records N                 records loaded per table (default 10000)
  --value-bytes N             `data` attribute size (default 256)
  --distribution zipfian|uniform   (default zipfian; workload D always reads latest)
  --max-scan-len N            workload E scan length 1..=N (default 100)
  --consistent-read true|false|both   (default both; reported apart)
  --seed N                    seeds the op/key stream (default 42)
load shape:
  --rate R                    arrival rate, ops/sec (default 1000)
  --sweep R1,R2,...           throughput sweep instead of a single steady phase
  --warmup-secs S             discarded warm-up (default 10)
  --steady-secs S             measured steady phase / each sweep point (default 30)
  --connections N             client connections (default 64)
  --op-timeout-secs S         per-operation timeout (default 10)
  --drain-secs S              grace after each phase for queued ops to start; unstarted ops are
                              reported as `abandoned` (default 30)
degraded run (one, last, on a fresh table):
  --degraded none|leader|follower|node:N   (default leader when a kill mechanism exists, else none)
                              leader = the node leading the table's first tablet; follower = a node
                              hosting a non-leader replica of it; node:N = node index N, whatever it hosts
  --degraded-workload A       (default: first --workloads)
  --degraded-consistent-read true|false   (default true)
  --baseline-secs S --degraded-secs S --recovery-secs S   (default 15 / 20 / 20)
  --kill-cmd TEMPLATE         external cluster: sh -c template; {node} {host} {dynamo} {admin}
  --restart-cmd TEMPLATE      external cluster: restart template (recovery restarts the node)
  --no-restart                do not restart the killed node for the recovery phase
output:
  --out FILE                  results JSON (default ./animus-bench-<epoch>.json)
  --table-prefix P            (default ycsb<epoch>)   --keep-tables
";

/// Which way the cluster is obtained.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Launch {
    External(Vec<NodeEndpoints>),
    Processes,
    InProcess,
}

/// Parsed command line.
#[derive(Clone, Debug)]
pub struct Options {
    pub launch: Launch,
    pub cluster_size: usize,
    pub animusd_bin: Option<PathBuf>,
    pub animusd_args: Vec<String>,
    pub data_dir: Option<PathBuf>,
    pub creds: Option<Credentials>,
    pub ycsb: YcsbConfig,
    pub kill_cmd: Option<String>,
    pub restart_cmd: Option<String>,
    pub out: PathBuf,
}

fn secs(s: &str, flag: &str) -> Result<Duration, String> {
    let v: f64 = s
        .parse()
        .map_err(|_| format!("{flag} wants a number of seconds, got `{s}`"))?;
    if v < 0.0 || !v.is_finite() {
        return Err(format!("{flag} must be >= 0"));
    }
    Ok(Duration::from_secs_f64(v))
}

fn num<T: std::str::FromStr>(s: &str, flag: &str) -> Result<T, String> {
    s.parse().map_err(|_| format!("{flag}: cannot parse `{s}`"))
}

fn parse_nodes(s: &str) -> Result<Vec<NodeEndpoints>, String> {
    s.split(',')
        .enumerate()
        .map(|(index, pair)| {
            let (d, a) = pair
                .split_once('@')
                .ok_or_else(|| format!("--nodes entry `{pair}` must be DYNAMO_ADDR@ADMIN_ADDR"))?;
            Ok(NodeEndpoints {
                index,
                dynamo: d
                    .parse()
                    .map_err(|e| format!("bad dynamo addr `{d}`: {e}"))?,
                admin: a
                    .parse()
                    .map_err(|e| format!("bad admin addr `{a}`: {e}"))?,
            })
        })
        .collect()
}

/// Parse `args` (without `argv[0]`).
///
/// # Errors
/// A message for any unknown flag or bad value.
pub fn parse_args(args: &[String]) -> Result<Options, String> {
    let epoch = rt::wall_epoch_secs();
    let mut launch: Option<Launch> = None;
    let mut cluster_size = 3usize;
    let (mut animusd_bin, mut data_dir) = (None, None);
    let mut animusd_args = Vec::new();
    let (mut access, mut secret, mut no_auth) = (None, None, false);
    let mut workloads = vec![WorkloadKind::A];
    let (mut records, mut value_bytes, mut max_scan_len) = (10_000u64, 256usize, 100u32);
    let mut distribution = Distribution::Zipfian;
    let mut read_modes = ReadModes::Both;
    let mut seed = 42u64;
    let mut rate = 1000.0f64;
    let mut sweep: Vec<f64> = Vec::new();
    let (mut warmup, mut steady) = (Duration::from_secs(10), Duration::from_secs(30));
    let (mut connections, mut op_timeout) = (64usize, Duration::from_secs(10));
    let mut drain = Duration::from_secs(30);
    let mut degraded_arg: Option<String> = None;
    let mut degraded_workload: Option<WorkloadKind> = None;
    let mut degraded_cr = true;
    let (mut baseline, mut degraded_secs, mut recovery) = (
        Duration::from_secs(15),
        Duration::from_secs(20),
        Duration::from_secs(20),
    );
    let (mut kill_cmd, mut restart_cmd, mut no_restart) = (None, None, false);
    let mut out: Option<PathBuf> = None;
    let mut table_prefix = format!("ycsb{epoch}");
    let mut keep_tables = false;

    let mut it = args.iter().peekable();
    if it.peek().is_some_and(|a| a.as_str() == "run") {
        it.next();
    }
    while let Some(flag) = it.next() {
        let mut val = |f: &str| -> Result<String, String> {
            it.next()
                .cloned()
                .ok_or_else(|| format!("{f} needs a value"))
        };
        match flag.as_str() {
            "--nodes" => launch = Some(Launch::External(parse_nodes(&val(flag)?)?)),
            "--launch" => {
                launch = Some(match val(flag)?.as_str() {
                    "processes" => Launch::Processes,
                    "in-process" => Launch::InProcess,
                    o => return Err(format!("--launch wants processes|in-process, got `{o}`")),
                });
            }
            "--cluster-size" => cluster_size = num(&val(flag)?, flag)?,
            "--animusd-bin" => animusd_bin = Some(PathBuf::from(val(flag)?)),
            "--animusd-arg" => animusd_args.push(val(flag)?),
            "--data-dir" => data_dir = Some(PathBuf::from(val(flag)?)),
            "--access-key" => access = Some(val(flag)?),
            "--secret-key" => secret = Some(val(flag)?),
            "--no-auth" => no_auth = true,
            "--workloads" => {
                let v = val(flag)?;
                workloads = if v.eq_ignore_ascii_case("all") {
                    WorkloadKind::ALL.to_vec()
                } else {
                    v.split(',')
                        .map(WorkloadKind::parse)
                        .collect::<Result<_, _>>()?
                };
            }
            "--records" => records = num(&val(flag)?, flag)?,
            "--value-bytes" => value_bytes = num(&val(flag)?, flag)?,
            "--distribution" => distribution = Distribution::parse(&val(flag)?)?,
            "--max-scan-len" => max_scan_len = num(&val(flag)?, flag)?,
            "--consistent-read" => read_modes = ReadModes::parse(&val(flag)?)?,
            "--seed" => seed = num(&val(flag)?, flag)?,
            "--rate" => rate = num(&val(flag)?, flag)?,
            "--sweep" => {
                sweep = val(flag)?
                    .split(',')
                    .map(|r| num::<f64>(r, flag))
                    .collect::<Result<_, _>>()?;
            }
            "--warmup-secs" => warmup = secs(&val(flag)?, flag)?,
            "--steady-secs" => steady = secs(&val(flag)?, flag)?,
            "--connections" => connections = num(&val(flag)?, flag)?,
            "--op-timeout-secs" => op_timeout = secs(&val(flag)?, flag)?,
            "--drain-secs" => drain = secs(&val(flag)?, flag)?,
            "--degraded" => degraded_arg = Some(val(flag)?),
            "--degraded-workload" => degraded_workload = Some(WorkloadKind::parse(&val(flag)?)?),
            "--degraded-consistent-read" => degraded_cr = num(&val(flag)?, flag)?,
            "--baseline-secs" => baseline = secs(&val(flag)?, flag)?,
            "--degraded-secs" => degraded_secs = secs(&val(flag)?, flag)?,
            "--recovery-secs" => recovery = secs(&val(flag)?, flag)?,
            "--kill-cmd" => kill_cmd = Some(val(flag)?),
            "--restart-cmd" => restart_cmd = Some(val(flag)?),
            "--no-restart" => no_restart = true,
            "--out" => out = Some(PathBuf::from(val(flag)?)),
            "--table-prefix" => table_prefix = val(flag)?,
            "--keep-tables" => keep_tables = true,
            other => return Err(format!("unknown argument `{other}`\n\n{USAGE}")),
        }
    }
    let launch =
        launch.ok_or("choose a cluster: --nodes ..., --launch processes or --launch in-process")?;
    if !(rate.is_finite() && rate > 0.0) || sweep.iter().any(|r| !(r.is_finite() && *r > 0.0)) {
        return Err("rates must be finite and > 0".into());
    }
    if workloads.is_empty() || records == 0 || connections == 0 || cluster_size == 0 {
        return Err(
            "--workloads, --records, --connections and --cluster-size must be non-empty/non-zero"
                .into(),
        );
    }
    let launched = !matches!(launch, Launch::External(_));
    let creds = match (access, secret) {
        _ if no_auth => None,
        (Some(a), Some(s)) => Some(Credentials::new(a, s)),
        (None, None) => match (
            std::env::var("AWS_ACCESS_KEY_ID").ok(),
            std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
        ) {
            (Some(a), Some(s)) => Some(Credentials::new(a, s)),
            _ if launched => Some(Credentials::new("animus-bench", "animus-bench-secret")),
            _ => None,
        },
        _ => return Err("--access-key and --secret-key go together".into()),
    };
    let can_kill = launched || kill_cmd.is_some();
    let can_restart = matches!(launch, Launch::Processes) || restart_cmd.is_some();
    let degraded_kind = match degraded_arg.as_deref() {
        None => can_kill.then_some(DegradedKind::Leader),
        Some("none") => None,
        Some("leader") => Some(DegradedKind::Leader),
        Some("follower") => Some(DegradedKind::Follower),
        Some(s) if s.starts_with("node:") => Some(DegradedKind::Node(num(&s[5..], "--degraded")?)),
        Some("node") => {
            return Err(
                "--degraded node needs an index (node:N); use `follower` to kill a non-leader replica"
                    .into(),
            );
        }
        Some(o) => {
            return Err(format!(
                "--degraded wants none|leader|follower|node:N, got `{o}`"
            ));
        }
    };
    let degraded = degraded_kind.map(|kind| DegradedConfig {
        kind,
        workload: degraded_workload.unwrap_or(workloads[0]),
        consistent_read: degraded_cr,
        baseline,
        degraded: degraded_secs,
        recovery,
        restart: can_restart && !no_restart,
    });
    Ok(Options {
        launch,
        cluster_size,
        animusd_bin,
        animusd_args,
        data_dir,
        creds,
        ycsb: YcsbConfig {
            workloads,
            record_count: records,
            value_bytes,
            distribution,
            max_scan_len,
            read_modes,
            plan: PhasePlan {
                rate,
                warmup,
                steady,
                sweep,
                connections,
                op_timeout,
                drain_timeout: drain,
            },
            seed,
            table_prefix,
            keep_tables,
            load_parallelism: 8,
            setup_timeout: Duration::from_secs(180),
            degraded,
        },
        kill_cmd,
        restart_cmd,
        out: out.unwrap_or_else(|| PathBuf::from(format!("animus-bench-{epoch}.json"))),
    })
}

fn default_animusd_bin() -> Result<PathBuf, String> {
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let cand = exe.with_file_name("animusd");
    if cand.exists() {
        Ok(cand)
    } else {
        Err(format!(
            "no animusd next to {} — build it (`cargo build -p animusd`) or pass --animusd-bin",
            exe.display()
        ))
    }
}

/// Launch/attach, run, assemble the report, write the JSON to `opts.out`.
///
/// # Errors
/// On a launch, setup or I/O failure.
pub async fn execute(opts: &Options, argv: Vec<String>) -> Result<Report, String> {
    let clock = Clock::start();
    let dir = opts.data_dir.clone().unwrap_or_else(|| {
        std::env::temp_dir().join(format!(
            "animus-bench-{}-{}",
            std::process::id(),
            rt::wall_epoch_secs()
        ))
    });
    let cluster = match &opts.launch {
        Launch::External(nodes) => Cluster::external(
            nodes.clone(),
            opts.creds.clone(),
            opts.kill_cmd.clone(),
            opts.restart_cmd.clone(),
        ),
        Launch::InProcess => {
            Cluster::launch_in_process(opts.cluster_size, &dir, opts.creds.clone())
                .await
                .map_err(|e| format!("in-process launch: {e}"))?
        }
        Launch::Processes => {
            let bin = match &opts.animusd_bin {
                Some(b) => b.clone(),
                None => default_animusd_bin()?,
            };
            Cluster::launch_processes(
                opts.cluster_size,
                &dir,
                &bin,
                opts.creds.clone(),
                opts.animusd_args.clone(),
            )
            .await
            .map_err(|e| format!("process launch: {e}"))?
        }
    };
    let result = run_with_cluster(&cluster, &clock, opts, argv).await;
    cluster.shutdown().await;
    let report = result?;
    std::fs::write(&opts.out, report.to_json())
        .map_err(|e| format!("write {}: {e}", opts.out.display()))?;
    Ok(report)
}

async fn run_with_cluster(
    cluster: &Cluster,
    clock: &Clock,
    opts: &Options,
    argv: Vec<String>,
) -> Result<Report, String> {
    cluster.await_ready(Duration::from_secs(90)).await?;
    let mut notes = Vec::new();
    let mut cfg = opts.ycsb.clone();
    if cfg.degraded.is_none() {
        notes.push(
            "no degraded run: this cluster has no kill mechanism (give --kill-cmd) or --degraded none was set; \
             a publishable run needs one"
                .to_owned(),
        );
    }
    if let Some(d) = &mut cfg.degraded
        && d.restart
    {
        notes.push("the killed node is restarted at the start of the recovery phase".to_owned());
    }
    let topology_start = envinfo::capture_topology(cluster, None).await;
    let runs = run_ycsb(cluster, clock, &cfg).await?;
    let topology_end = envinfo::capture_topology(cluster, None).await;

    let loopback = cluster.nodes().iter().all(|n| n.dynamo.ip().is_loopback());
    let colocated = cluster.launched_here() || loopback;
    let (git_sha, git_dirty) = envinfo::git_state();
    let (publishable, reason) = publishability(cluster.launched_here(), loopback, &runs);
    Ok(Report {
        schema: SCHEMA.to_owned(),
        generated_at_epoch_secs: rt::wall_epoch_secs(),
        tool_version: env!("CARGO_PKG_VERSION").to_owned(),
        git_sha,
        git_dirty,
        args: argv,
        seed: cfg.seed,
        publishable,
        publishable_reason: reason,
        environment: Environment {
            client_host: HostInfo::capture(),
            client_and_server_colocated: colocated,
            launch_mode: cluster.launch_mode().to_owned(),
            target_endpoints: cluster.nodes().to_vec(),
            node_count: cluster.nodes().len(),
            sigv4: cluster.credentials().is_some(),
            tls: false,
            tls_note: "this client speaks plain TCP only (TLS client support not implemented)"
                .to_owned(),
        },
        methodology: methodology(),
        topology_start,
        topology_end,
        notes,
        runs,
    })
}

fn methodology() -> serde_json::Value {
    json!({
        "load_model": "open loop: operation i has intended send time start + i/rate; a fixed arrival schedule independent of server speed",
        "latency": "reported latency is measured from the INTENDED send time (coordinated-omission corrected); 'service' latency (from actual send) is recorded alongside",
        "histogram": "HdrHistogram, microseconds, 3 significant figures",
        "errors": "counted by kind (throttled/timeout/connection/other), not recorded in latency histograms; ConditionalCheckFailed is an expected outcome (workload F races), counted separately and recorded as completed",
        "empty_reads": "reads that found nothing are counted separately (under ConsistentRead=false a lagging replica may miss a just-written key)",
        "achieved_rate": "completed operations / (phase start to last completion)",
        "phases": "load (unmeasured bulk insert) -> warm-up (discarded) -> steady (measured) [or sweep]; degraded run last: baseline -> degraded (fault at phase start) -> recovery",
        "consistent_read": "every read workload runs with ConsistentRead true and false as separate runs; never blended",
        "retries": "none: the generator never retries a failed operation",
        "comparison": "no comparison against any other database is made or implied",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(a: &[&str]) -> Result<Options, String> {
        let v: Vec<String> = a.iter().map(|s| (*s).to_owned()).collect();
        parse_args(&v)
    }

    #[test]
    fn degraded_victims_are_distinct_and_bare_node_is_rejected() {
        let kind = |d: &str| {
            parse(&["--launch", "processes", "--degraded", d])
                .map(|o| o.ycsb.degraded.map(|c| c.kind))
        };
        assert!(matches!(kind("follower"), Ok(Some(DegradedKind::Follower))));
        assert!(matches!(kind("node:2"), Ok(Some(DegradedKind::Node(2)))));
        assert!(matches!(kind("leader"), Ok(Some(DegradedKind::Leader))));
        assert!(matches!(kind("none"), Ok(None)));
        assert!(kind("node").unwrap_err().contains("node:N"));
        assert!(kind("node:x").is_err());
    }

    #[test]
    fn drain_grace_defaults_to_30s_and_is_a_flag() {
        let d = parse(&["--launch", "processes"]).unwrap();
        assert_eq!(d.ycsb.plan.drain_timeout, Duration::from_secs(30));
        let d = parse(&["--launch", "processes", "--drain-secs", "2.5"]).unwrap();
        assert_eq!(d.ycsb.plan.drain_timeout, Duration::from_millis(2500));
        assert!(parse(&["--launch", "processes", "--drain-secs", "-1"]).is_err());
    }
}
