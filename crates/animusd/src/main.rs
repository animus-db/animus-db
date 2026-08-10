//! AnimusDB node server (`animusd`).
//!
//! Eight modes:
//!
//! ```text
//! animusd gen-config --nodes N [--host H] [--base-port P]   # print a combined-mode cluster config (JSON)
//! animusd gen-config --control-nodes N --data-nodes M [--host H] [--base-port P] # print a split-deployment config (ADR 0035)
//! animusd --config FILE --node I [--dir DIR] [--ephemeral] # run node I of a cluster (one process)
//! animusd --cluster N [--dir DIR] [--ip ADDR] [--ephemeral] [--auto-split K] [--auto-split-bytes B] # run an N-node cluster in one process
//! animusd --cluster-control N --cluster-data M [--dir DIR] [--ip ADDR] [--ephemeral] [--auto-split K] [--auto-split-bytes B] # run a whole split deployment in one process (ADR 0035)
//! animusd join --seed ADDR[,ADDR...] --node I [--ip A] [--base-port P] [--dir D] [--ephemeral] # seed/join startup (ADR 0032 PR2)
//! animusd control --config FILE --node I [--dir DIR] # run node I as a control-only node (ADR 0035 PR3)
//! animusd data --config FILE --node I [--dir DIR] [--ephemeral] # run node I as a data-only node (ADR 0035 PR4)
//! animusd data --seed ADDR[,ADDR...] --node I [--ip A] [--base-port P] [--dir D] [--ephemeral] # data-only seed/join (ADR 0035 PR5)
//! ```
//!
//! The data replica is durable by default (an on-disk LSM under the node's data
//! dir, so values survive a restart); `--ephemeral` selects a volatile
//! in-memory engine instead.
//!
//! Per-process deployment: generate a config once, copy it to each host, and run
//! `animusd --config cluster.json --node I` with a distinct `I` per process. A
//! node that has no expanded config at all — just the client address of any
//! already-running node — can instead `animusd join --seed <that address>
//! --node I`, learning everything else it needs from the cluster itself (ADR
//! 0032 PR2: a real data-plane member, control group unchanged, ADR 0030); a
//! data-only node has the same option (`animusd data --seed <that address>
//! --node I`, ADR 0035 PR5) against a separately-deployed control plane.
//!
//! `animusd control` runs one of a config's control-role node(s) only — no
//! storage engine, no `raftkv` env, no DynamoDB/CQL listeners; `animusd data`
//! runs one of a config's data-role node(s) only — no control env, no local
//! control `RaftCore` at all, reaching a **separately-deployed** control
//! plane over the network instead (ADR 0035: control plane as a separate
//! deployment). `gen-config --control-nodes/--data-nodes` prints the
//! split-deployment config *shape* (control-role entries with no `raftkv`
//! address, data-role entries with no `control` address) both target — run
//! every control-role index with `animusd control` and every data-role index
//! with `animusd data` against the same config for a genuine split
//! deployment (see `crates/animusd/CLAUDE.md`). A mixed deployment — some
//! nodes still combined-mode (`--config FILE --node I` against a `Both`-role
//! config) — keeps working exactly as before.
//!
//! `--cluster-control N --cluster-data M` is `--cluster N`'s split-deployment
//! sibling: a whole split deployment (`N` control-only + `M` data-only
//! nodes, no combined-mode node) in **one process**, dev-convenience the same
//! way `--cluster N` is — the real multi-process shape is `animusd control`/
//! `animusd data` above. Each node still gets its own edge state and reaches
//! every other node only through real forwarding/relay/mirror paths, exactly
//! as the multi-process deployment does.

use std::net::{IpAddr, SocketAddr};
use std::process::ExitCode;

use animusd::{ClusterConfig, RoleAddrs};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let tracer_provider = animusd::otel::init_tracing(&otel_instance_label(&args));

    let result = match args.first().map(String::as_str) {
        Some("gen-config") => gen_config(&args[1..]),
        Some("join") => run_join(&args[1..]).await,
        Some("control") => run_control(&args[1..]).await,
        Some("data") => run_data(&args[1..]).await,
        _ => run(&args).await,
    };

    // Flush any spans still buffered in the OTLP batch exporter (ADR 0027)
    // before the process exits; a no-op if export isn't configured.
    if let Some(provider) = tracer_provider
        && let Err(err) = provider.shutdown()
    {
        tracing::warn!(%err, "failed to flush OpenTelemetry tracer provider on exit");
    }

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("animusd: {msg}\n\n{USAGE}");
            ExitCode::FAILURE
        }
    }
}

/// A short `service.instance.id` label for the OTLP `Resource` (ADR 0027):
/// this process's node index for a `--config/--node` run — the `--node` scan
/// also covers `join --node I` (ADR 0032 PR2), which runs one node per
/// process just like `--config` mode — or a cluster-level label for a
/// `--cluster N` run (which hosts several logical nodes in one process, so
/// no single node id applies at the process/resource level — per-span
/// `node_id` fields still distinguish them within a trace).
fn otel_instance_label(args: &[String]) -> String {
    if let Some(pos) = args.iter().position(|a| a == "--node")
        && let Some(index) = args.get(pos + 1)
    {
        return format!("node-{index}");
    }
    if args.iter().any(|a| a == "--cluster") {
        return "cluster".to_owned();
    }
    "animusd".to_owned()
}

const USAGE: &str = "usage:\n  \
    animusd gen-config --nodes N [--host H] [--base-port P]\n  \
    animusd gen-config --control-nodes N --data-nodes M [--host H] [--base-port P]\n  \
    animusd --config FILE --node I [--dir DIR] [--ephemeral]\n  \
    animusd --cluster N [--dir DIR] [--ip ADDR] [--ephemeral] [--auto-split K] [--auto-split-bytes B]\n  \
    animusd --cluster-control N --cluster-data M [--dir DIR] [--ip ADDR] [--ephemeral] [--auto-split K] [--auto-split-bytes B]\n  \
    animusd join --seed ADDR[,ADDR...] --node I [--ip A] [--base-port P] [--dir D] [--ephemeral]\n  \
    animusd control --config FILE --node I [--dir DIR]\n  \
    animusd data --config FILE --node I [--dir DIR] [--ephemeral]\n  \
    animusd data --seed ADDR[,ADDR...] --node I [--ip A] [--base-port P] [--dir D] [--ephemeral]";

/// `gen-config`: print a generated cluster config as JSON — either combined-mode
/// (`--nodes N`) or the ADR 0035 split-deployment shape (`--control-nodes N
/// --data-nodes M`, [`ClusterConfig::generate_split`]).
fn gen_config(args: &[String]) -> Result<(), String> {
    let mut nodes: Option<usize> = None;
    let mut control_nodes: Option<usize> = None;
    let mut data_nodes: Option<usize> = None;
    let mut host: IpAddr = "127.0.0.1".parse().unwrap();
    let mut base_port: u16 = 7100;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--nodes" => nodes = Some(parse_next(&mut it, "--nodes")?),
            "--control-nodes" => control_nodes = Some(parse_next(&mut it, "--control-nodes")?),
            "--data-nodes" => data_nodes = Some(parse_next(&mut it, "--data-nodes")?),
            "--host" => host = parse_next(&mut it, "--host")?,
            "--base-port" => base_port = parse_next(&mut it, "--base-port")?,
            other => return Err(format!("unknown gen-config argument `{other}`")),
        }
    }
    if nodes.is_some() && (control_nodes.is_some() || data_nodes.is_some()) {
        return Err("use either --nodes or --control-nodes/--data-nodes, not both".into());
    }
    if let Some(nodes) = nodes {
        if nodes == 0 {
            return Err("--nodes must be at least 1".into());
        }
        println!(
            "{}",
            ClusterConfig::generate(nodes, host, base_port).to_json()
        );
        return Ok(());
    }
    let control_n =
        control_nodes.ok_or("gen-config needs --nodes N, or --control-nodes N --data-nodes M")?;
    let data_n = data_nodes.ok_or("--control-nodes also needs --data-nodes M")?;
    if control_n == 0 || data_n == 0 {
        return Err("--control-nodes and --data-nodes must each be at least 1".into());
    }
    println!(
        "{}",
        ClusterConfig::generate_split(control_n, data_n, host, base_port).to_json()
    );
    Ok(())
}

/// `--config/--node` (per-process) or `--cluster` (in-process) run modes.
async fn run(args: &[String]) -> Result<(), String> {
    let mut config_path: Option<String> = None;
    let mut node: Option<usize> = None;
    let mut cluster: Option<usize> = None;
    let mut cluster_control: Option<usize> = None;
    let mut cluster_data: Option<usize> = None;
    let mut dir: Option<std::path::PathBuf> = None;
    let mut ip: IpAddr = "127.0.0.1".parse().unwrap();
    // Data replica engine: durable on-disk LSM by default; `--ephemeral` selects
    // the volatile in-memory engine (data does not survive restart).
    let mut backend = animusd::StorageBackend::default();
    // `--auto-split K`: in `--cluster` mode, a CP-hosting node auto-splits a tablet
    // it leads once it exceeds K keys (Phase 2.4). Handy for testing sharding by
    // bulk-seeding past the threshold.
    let mut auto_split: Option<usize> = None;
    // `--auto-split-bytes B` (ADR 0034): same, but on an (approximate) scoped
    // bytes threshold instead of a key count — the metric splitting is meant
    // to bound in production (snapshot/compaction/replica-move/recovery cost
    // scales with bytes, not key count). Either, both, or neither may be set;
    // when both are set, whichever threshold is hit first triggers.
    let mut auto_split_bytes: Option<u64> = None;

    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--config" => config_path = Some(parse_next(&mut it, "--config")?),
            "--node" => node = Some(parse_next(&mut it, "--node")?),
            "--cluster" => cluster = Some(parse_next(&mut it, "--cluster")?),
            "--cluster-control" => {
                cluster_control = Some(parse_next(&mut it, "--cluster-control")?)
            }
            "--cluster-data" => cluster_data = Some(parse_next(&mut it, "--cluster-data")?),
            "--dir" => dir = Some(parse_next::<String>(&mut it, "--dir")?.into()),
            "--ip" => ip = parse_next(&mut it, "--ip")?,
            "--ephemeral" => backend = animusd::StorageBackend::Memory,
            "--auto-split" => auto_split = Some(parse_next(&mut it, "--auto-split")?),
            "--auto-split-bytes" => {
                auto_split_bytes = Some(parse_next(&mut it, "--auto-split-bytes")?);
            }
            other => return Err(format!("unknown argument `{other}`")),
        }
    }

    if cluster_control.is_some() || cluster_data.is_some() {
        if config_path.is_some() || cluster.is_some() {
            return Err(
                "use either --config, --cluster, or --cluster-control/--cluster-data, not both"
                    .into(),
            );
        }
        let control_n = cluster_control.ok_or("--cluster-data also needs --cluster-control N")?;
        let data_n = cluster_data.ok_or("--cluster-control also needs --cluster-data M")?;
        return run_in_process_split_cluster(
            control_n,
            data_n,
            ip,
            dir,
            backend,
            auto_split,
            auto_split_bytes,
        )
        .await;
    }

    match (config_path, cluster) {
        (Some(_), Some(_)) => Err("use either --config or --cluster, not both".into()),
        (Some(path), None) => {
            let index = node.ok_or("--config requires --node I")?;
            run_single(&path, index, dir, backend).await
        }
        (None, Some(n)) => {
            run_in_process_cluster(n, ip, dir, backend, auto_split, auto_split_bytes).await
        }
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
        "animusd: node {index}/{} up (CP) — client {} — dynamo http {} — cql {} — admin http://{}",
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

/// `control`: run node `index` of `config` as a **control-only** node (ADR
/// 0035 PR3) — no storage engine, no `raftkv` env, no DynamoDB/CQL listeners.
async fn run_control(args: &[String]) -> Result<(), String> {
    let mut config_path: Option<String> = None;
    let mut node: Option<usize> = None;
    let mut dir: Option<std::path::PathBuf> = None;

    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--config" => config_path = Some(parse_next(&mut it, "--config")?),
            "--node" => node = Some(parse_next(&mut it, "--node")?),
            "--dir" => dir = Some(parse_next::<String>(&mut it, "--dir")?.into()),
            other => return Err(format!("unknown control argument `{other}`")),
        }
    }
    let path = config_path.ok_or("control requires --config FILE")?;
    let index = node.ok_or("control requires --node I")?;
    let text = std::fs::read_to_string(&path).map_err(|e| format!("reading {path}: {e}"))?;
    let config = ClusterConfig::from_json(&text).map_err(|e| format!("parsing {path}: {e}"))?;
    let dir = dir.unwrap_or_else(|| std::env::temp_dir().join(format!("animusd-control-{index}")));

    let node = animusd::run_node_control(&config, index, &dir)
        .await
        .map_err(|e| format!("failed to start control node {index}: {e}"))?;
    println!(
        "animusd: control node {index}/{} up — client {} — admin http://{}",
        config.len(),
        node.client_addr(),
        node.admin_addr(),
    );
    println!("animusd: ready — Ctrl-C to stop");
    wait_for_ctrl_c().await;
    node.shutdown_graceful().await;
    Ok(())
}

/// `data`: run node `index` as a **data-only** node (ADR 0035 PR4, or PR5's
/// `--seed` join variant) — no control env, no local control `RaftCore`,
/// reaching the separately-deployed control plane over the network instead.
/// Either `--config FILE` (an operator-assembled `ClusterConfig` listing the
/// control deployment's addresses up front — see
/// [`animusd::run_node_data`]'s doc) or `--seed ADDR[,ADDR...]` (this node
/// discovers the control deployment from any already-running node's client
/// address, mirroring `animusd join`'s discovery — see
/// [`animusd::run_node_data_join`]'s doc), never both.
async fn run_data(args: &[String]) -> Result<(), String> {
    let mut config_path: Option<String> = None;
    let mut node: Option<usize> = None;
    let mut dir: Option<std::path::PathBuf> = None;
    let mut backend = animusd::StorageBackend::default();
    let mut seed_arg: Option<String> = None;
    let mut ip: IpAddr = "127.0.0.1".parse().unwrap();
    let mut base_port: Option<u16> = None;

    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--config" => config_path = Some(parse_next(&mut it, "--config")?),
            "--node" => node = Some(parse_next(&mut it, "--node")?),
            "--dir" => dir = Some(parse_next::<String>(&mut it, "--dir")?.into()),
            "--ephemeral" => backend = animusd::StorageBackend::Memory,
            "--seed" => seed_arg = Some(parse_next::<String>(&mut it, "--seed")?),
            "--ip" => ip = parse_next(&mut it, "--ip")?,
            "--base-port" => base_port = Some(parse_next(&mut it, "--base-port")?),
            other => return Err(format!("unknown data argument `{other}`")),
        }
    }
    let index = node.ok_or("data requires --node I")?;

    match (config_path, seed_arg) {
        (Some(_), Some(_)) => Err("use either --config or --seed, not both".into()),
        (Some(path), None) => run_data_config(&path, index, dir, backend).await,
        (None, Some(seed_arg)) => {
            run_data_join(&seed_arg, index, ip, base_port, dir, backend).await
        }
        (None, None) => Err("data requires --config FILE or --seed ADDR[,ADDR...]".into()),
    }
}

/// `animusd data --config FILE --node I` (ADR 0035 PR4): the operator-assembled-config half of [`run_data`].
async fn run_data_config(
    path: &str,
    index: usize,
    dir: Option<std::path::PathBuf>,
    backend: animusd::StorageBackend,
) -> Result<(), String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("reading {path}: {e}"))?;
    let config = ClusterConfig::from_json(&text).map_err(|e| format!("parsing {path}: {e}"))?;
    let dir = dir.unwrap_or_else(|| std::env::temp_dir().join(format!("animusd-data-{index}")));

    let node = animusd::run_node_data(&config, index, &dir, backend)
        .await
        .map_err(|e| format!("failed to start data node {index}: {e}"))?;
    println!(
        "animusd: data node {index}/{} up (CP) — client {} — dynamo http {} — cql {} — admin http://{}",
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

/// `animusd data --seed ADDR[,ADDR...] --node I` (ADR 0035 PR5): the
/// seed/join half of [`run_data`] — mirrors [`run_join`]'s port-derivation
/// and CLI shape exactly, minus the control port (a data-only `RoleAddrs` has
/// none) and using [`animusd::run_node_data_join`] instead of
/// [`animusd::run_node_join`].
async fn run_data_join(
    seed_arg: &str,
    index: usize,
    ip: IpAddr,
    base_port: Option<u16>,
    dir: Option<std::path::PathBuf>,
    backend: animusd::StorageBackend,
) -> Result<(), String> {
    let seeds: Vec<SocketAddr> = seed_arg
        .split(',')
        .map(|s| {
            s.trim()
                .parse::<SocketAddr>()
                .map_err(|e| format!("invalid --seed address `{s}`: {e}"))
        })
        .collect::<Result<_, _>>()?;
    if seeds.is_empty() {
        return Err("data --seed requires at least one address".into());
    }

    // Same six-port-per-index stride as `run_join`/`gen-config`, minus the
    // control port this data-only node never binds (left `None` below).
    let base_port = base_port.unwrap_or(7100_u16.wrapping_add((index as u16).wrapping_mul(6)));
    let p = |role: u16| SocketAddr::new(ip, base_port.wrapping_add(role));
    let addrs = RoleAddrs {
        role: animusd::config::NodeRole::Data,
        control: None,
        client: p(1),
        dynamo: p(2),
        cql: p(3),
        raftkv: Some(p(4)),
        admin: p(5),
    };
    let dir =
        dir.unwrap_or_else(|| std::env::temp_dir().join(format!("animusd-data-join-{index}")));

    let node = animusd::run_node_data_join(seeds, index, addrs, &dir, backend)
        .await
        .map_err(|e| format!("failed to join as data node {index}: {e}"))?;
    println!(
        "animusd: data node {index} joined (CP) — client {} — dynamo http {} — cql {} — admin http://{}",
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

/// `join`: seed/join startup (ADR 0032 PR2) — a new node starts knowing only
/// its own addresses + a seed list (client addresses of any existing nodes),
/// learning the pre-growth control group + peer/route/admin address books
/// from the cluster itself instead of an operator-assembled expanded
/// `ClusterConfig`. See [`animusd::run_node_join`]'s doc for the collision
/// guard + growth semantics this drives.
async fn run_join(args: &[String]) -> Result<(), String> {
    let mut seed_arg: Option<String> = None;
    let mut index: Option<usize> = None;
    let mut ip: IpAddr = "127.0.0.1".parse().unwrap();
    let mut base_port: Option<u16> = None;
    let mut dir: Option<std::path::PathBuf> = None;
    let mut backend = animusd::StorageBackend::default();

    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--seed" => seed_arg = Some(parse_next::<String>(&mut it, "--seed")?),
            "--node" => index = Some(parse_next(&mut it, "--node")?),
            "--ip" => ip = parse_next(&mut it, "--ip")?,
            "--base-port" => base_port = Some(parse_next(&mut it, "--base-port")?),
            "--dir" => dir = Some(parse_next::<String>(&mut it, "--dir")?.into()),
            "--ephemeral" => backend = animusd::StorageBackend::Memory,
            other => return Err(format!("unknown join argument `{other}`")),
        }
    }

    let seed_arg = seed_arg.ok_or("join requires --seed ADDR[,ADDR...]")?;
    let index = index.ok_or("join requires --node I")?;
    let seeds: Vec<SocketAddr> = seed_arg
        .split(',')
        .map(|s| {
            s.trim()
                .parse::<SocketAddr>()
                .map_err(|e| format!("invalid --seed address `{s}`: {e}"))
        })
        .collect::<Result<_, _>>()?;
    if seeds.is_empty() {
        return Err("join requires at least one --seed address".into());
    }

    // Six consecutive ports, same stride/role order as `ClusterConfig::generate`
    // (control/client/dynamo/cql/raftkv/admin) — defaults to `7100 + 6*index`,
    // mirroring `gen-config`'s own per-node base port so a joined node's
    // default addresses land in the same conventional range as a
    // `gen-config`-generated cluster's node `index`, without colliding with
    // it (each index's 6-port block is disjoint). Pass `--base-port`
    // explicitly for anything less conventional (a different host, a
    // manually-chosen port range).
    let base_port = base_port.unwrap_or(7100_u16.wrapping_add((index as u16).wrapping_mul(6)));
    let p = |role: u16| SocketAddr::new(ip, base_port.wrapping_add(role));
    let addrs = RoleAddrs {
        role: animusd::config::NodeRole::Both,
        control: Some(p(0)),
        client: p(1),
        dynamo: p(2),
        cql: p(3),
        raftkv: Some(p(4)),
        admin: p(5),
    };
    let dir = dir.unwrap_or_else(|| std::env::temp_dir().join(format!("animusd-join-{index}")));

    let node = animusd::run_node_join(seeds, index, addrs, &dir, backend)
        .await
        .map_err(|e| format!("failed to join as node {index}: {e}"))?;
    println!(
        "animusd: node {index} joined (CP) — client {} — dynamo http {} — cql {} — admin http://{}",
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
    auto_split_bytes: Option<u64>,
) -> Result<(), String> {
    if n == 0 {
        return Err("--cluster must be at least 1".into());
    }
    let dir = dir.unwrap_or_else(|| std::env::temp_dir().join("animusd"));
    let bound = animusd::bind_cluster(n, ip, &dir)
        .await
        .map_err(|e| format!("failed to bind cluster: {e}"))?;
    let nodes =
        animusd::start_cluster_with_auto_split_bytes(bound, backend, auto_split, auto_split_bytes)
            .await
            .map_err(|e| format!("failed to start cluster: {e}"))?;

    match (auto_split, auto_split_bytes) {
        (Some(k), Some(b)) => println!(
            "animusd: started {n}-node cluster (CP) — auto-split at {k} keys or {b} bytes/tablet"
        ),
        (Some(k), None) => {
            println!("animusd: started {n}-node cluster (CP) — auto-split at {k} keys/tablet")
        }
        (None, Some(b)) => {
            println!("animusd: started {n}-node cluster (CP) — auto-split at {b} bytes/tablet")
        }
        (None, None) => println!("animusd: started {n}-node cluster (CP)"),
    }
    for (i, node) in nodes.iter().enumerate() {
        println!(
            "  node {i}: client {} — dynamo http {} — cql {} — admin http://{}",
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

/// In-process: run a whole split deployment (`control_n` control-only nodes
/// and `data_n` data-only nodes, no combined-mode node) in one process — the
/// `--cluster-control N --cluster-data M` dev-convenience sibling of
/// `--cluster N` ([`run_in_process_cluster`]), backed by
/// [`animusd::start_split_cluster_with`].
async fn run_in_process_split_cluster(
    control_n: usize,
    data_n: usize,
    ip: IpAddr,
    dir: Option<std::path::PathBuf>,
    backend: animusd::StorageBackend,
    auto_split: Option<usize>,
    auto_split_bytes: Option<u64>,
) -> Result<(), String> {
    if control_n == 0 || data_n == 0 {
        return Err("--cluster-control and --cluster-data must each be at least 1".into());
    }
    let dir = dir.unwrap_or_else(|| std::env::temp_dir().join("animusd"));
    let nodes = animusd::start_split_cluster_with(
        control_n,
        data_n,
        &dir,
        ip,
        backend,
        auto_split,
        auto_split_bytes,
    )
    .await
    .map_err(|e| format!("failed to start split cluster: {e}"))?;

    match (auto_split, auto_split_bytes) {
        (Some(k), Some(b)) => println!(
            "animusd: started split cluster ({control_n} control + {data_n} data, CP) — auto-split at {k} keys or {b} bytes/tablet"
        ),
        (Some(k), None) => println!(
            "animusd: started split cluster ({control_n} control + {data_n} data, CP) — auto-split at {k} keys/tablet"
        ),
        (None, Some(b)) => println!(
            "animusd: started split cluster ({control_n} control + {data_n} data, CP) — auto-split at {b} bytes/tablet"
        ),
        (None, None) => {
            println!("animusd: started split cluster ({control_n} control + {data_n} data, CP)")
        }
    }
    for (i, node) in nodes.iter().take(control_n).enumerate() {
        println!(
            "  control node {i}: client {} — admin http://{}",
            node.client_addr(),
            node.admin_addr(),
        );
    }
    for (i, node) in nodes.iter().skip(control_n).enumerate() {
        println!(
            "  data node {i}: client {} — dynamo http {} — cql {} — admin http://{}",
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
