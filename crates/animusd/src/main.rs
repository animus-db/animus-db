//! AnimusDB node server (`animusd`).
//!
//! Three modes:
//!
//! ```text
//! animusd gen-config --nodes N [--host H] [--base-port P]   # print a cluster config (JSON)
//! animusd --config FILE --node I [--dir DIR] [--ephemeral] # run node I of a cluster (one process)
//! animusd --cluster N [--dir DIR] [--ip ADDR] [--ephemeral] [--auto-split K] # run an N-node cluster in one process
//! ```
//!
//! The data replica is durable by default (an on-disk LSM under the node's data
//! dir, so values survive a restart); `--ephemeral` selects a volatile
//! in-memory engine instead.
//!
//! Per-process deployment: generate a config once, copy it to each host, and run
//! `animusd --config cluster.json --node I` with a distinct `I` per process.

use std::net::IpAddr;
use std::process::ExitCode;

use animusd::ClusterConfig;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("gen-config") => gen_config(&args[1..]),
        _ => run(&args).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("animusd: {msg}\n\n{USAGE}");
            ExitCode::FAILURE
        }
    }
}

const USAGE: &str = "usage:\n  \
    animusd gen-config --nodes N [--host H] [--base-port P]\n  \
    animusd --config FILE --node I [--dir DIR] [--ephemeral]\n  \
    animusd --cluster N [--dir DIR] [--ip ADDR] [--ephemeral] [--auto-split K]";

/// `gen-config`: print a generated cluster config as JSON.
fn gen_config(args: &[String]) -> Result<(), String> {
    let mut nodes = None;
    let mut host: IpAddr = "127.0.0.1".parse().unwrap();
    let mut base_port: u16 = 7100;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--nodes" => nodes = Some(parse_next(&mut it, "--nodes")?),
            "--host" => host = parse_next(&mut it, "--host")?,
            "--base-port" => base_port = parse_next(&mut it, "--base-port")?,
            other => return Err(format!("unknown gen-config argument `{other}`")),
        }
    }
    let nodes: usize = nodes.ok_or("gen-config needs --nodes N")?;
    if nodes == 0 {
        return Err("--nodes must be at least 1".into());
    }
    println!(
        "{}",
        ClusterConfig::generate(nodes, host, base_port).to_json()
    );
    Ok(())
}

/// `--config/--node` (per-process) or `--cluster` (in-process) run modes.
async fn run(args: &[String]) -> Result<(), String> {
    let mut config_path: Option<String> = None;
    let mut node: Option<usize> = None;
    let mut cluster: Option<usize> = None;
    let mut dir: Option<std::path::PathBuf> = None;
    let mut ip: IpAddr = "127.0.0.1".parse().unwrap();
    // Data replica engine: durable on-disk LSM by default; `--ephemeral` selects
    // the volatile in-memory engine (data does not survive restart).
    let mut backend = animusd::StorageBackend::default();
    // `--auto-split K`: in `--cluster` mode, a CP-hosting node auto-splits a tablet
    // it leads once it exceeds K keys (Phase 2.4). Handy for testing sharding by
    // bulk-seeding past the threshold.
    let mut auto_split: Option<usize> = None;

    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--config" => config_path = Some(parse_next(&mut it, "--config")?),
            "--node" => node = Some(parse_next(&mut it, "--node")?),
            "--cluster" => cluster = Some(parse_next(&mut it, "--cluster")?),
            "--dir" => dir = Some(parse_next::<String>(&mut it, "--dir")?.into()),
            "--ip" => ip = parse_next(&mut it, "--ip")?,
            "--ephemeral" => backend = animusd::StorageBackend::Memory,
            "--auto-split" => auto_split = Some(parse_next(&mut it, "--auto-split")?),
            other => return Err(format!("unknown argument `{other}`")),
        }
    }

    match (config_path, cluster) {
        (Some(_), Some(_)) => Err("use either --config or --cluster, not both".into()),
        (Some(path), None) => {
            let index = node.ok_or("--config requires --node I")?;
            run_single(&path, index, dir, backend).await
        }
        (None, Some(n)) => run_in_process_cluster(n, ip, dir, backend, auto_split).await,
        (None, None) => Err("nothing to do".into()),
    }
}

/// Per-process: run node `index` from the config file.
async fn run_single(
    path: &str,
    index: usize,
    dir: Option<std::path::PathBuf>,
    backend: animusd::StorageBackend,
) -> Result<(), String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("reading {path}: {e}"))?;
    let config = ClusterConfig::from_json(&text).map_err(|e| format!("parsing {path}: {e}"))?;
    let dir = dir.unwrap_or_else(|| std::env::temp_dir().join(format!("animusd-node-{index}")));

    let node = animusd::run_node_with(&config, index, &dir, backend)
        .await
        .map_err(|e| format!("failed to start node {index}: {e}"))?;
    println!(
        "animusd: node {index}/{} up (CP) — client {} — dynamo http {} — cql {} — admin {}",
        config.len(),
        node.client_addr(),
        node.dynamo_addr(),
        node.cql_addr(),
        node.admin_addr(),
    );
    println!("animusd: ready — Ctrl-C to stop");
    wait_for_ctrl_c().await;
    node.shutdown_graceful().await;
    Ok(())
}

/// In-process: run an `n`-node cluster (dev convenience).
async fn run_in_process_cluster(
    n: usize,
    ip: IpAddr,
    dir: Option<std::path::PathBuf>,
    backend: animusd::StorageBackend,
    auto_split: Option<usize>,
) -> Result<(), String> {
    if n == 0 {
        return Err("--cluster must be at least 1".into());
    }
    let dir = dir.unwrap_or_else(|| std::env::temp_dir().join("animusd"));
    let bound = animusd::bind_cluster(n, ip, &dir)
        .await
        .map_err(|e| format!("failed to bind cluster: {e}"))?;
    let nodes = animusd::start_cluster_with_auto_split(bound, backend, auto_split)
        .await
        .map_err(|e| format!("failed to start cluster: {e}"))?;

    match auto_split {
        Some(k) => {
            println!("animusd: started {n}-node cluster (CP) — auto-split at {k} keys/tablet")
        }
        None => println!("animusd: started {n}-node cluster (CP)"),
    }
    for (i, node) in nodes.iter().enumerate() {
        println!(
            "  node {i}: client {} — dynamo http {} — cql {} — admin {}",
            node.client_addr(),
            node.dynamo_addr(),
            node.cql_addr(),
            node.admin_addr(),
        );
    }
    println!("animusd: ready — Ctrl-C to stop");
    wait_for_ctrl_c().await;
    for node in &nodes {
        node.shutdown_graceful().await;
    }
    Ok(())
}

async fn wait_for_ctrl_c() {
    if let Err(e) = tokio::signal::ctrl_c().await {
        tracing::warn!(?e, "failed to listen for Ctrl-C");
    }
    println!("animusd: shutting down");
}

fn parse_next<T: std::str::FromStr>(
    it: &mut std::slice::Iter<'_, String>,
    flag: &str,
) -> Result<T, String> {
    let raw = it.next().ok_or_else(|| format!("{flag} needs a value"))?;
    raw.parse()
        .map_err(|_| format!("invalid value for {flag}: `{raw}`"))
}
