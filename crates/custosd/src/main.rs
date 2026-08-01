//! CustosDB node server (`custosd`).
//!
//! Usage:
//!   custosd --cluster N [--dir DIR] [--ip ADDR]
//!
//! Starts an `N`-node CustosDB cluster in one process over real TCP loopback
//! (each node still runs its own control/data/coord `ProdEnv` listeners and a
//! client server), prints each node's client address, and runs until Ctrl-C.
//! This is the simplest runnable form; per-process deployment with a config
//! file is future work.

use std::net::IpAddr;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = match Args::parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(msg) => {
            eprintln!("custosd: {msg}\n\nusage: custosd --cluster N [--dir DIR] [--ip ADDR]");
            return ExitCode::FAILURE;
        }
    };

    let (r, w) = quorum(args.cluster);
    let bound = match custosd::bind_cluster(args.cluster, args.ip, &args.dir).await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("custosd: failed to bind cluster: {e}");
            return ExitCode::FAILURE;
        }
    };
    let nodes = custosd::start_cluster(bound, r, w);

    println!(
        "custosd: started {}-node cluster (R={r}, W={w})",
        nodes.len()
    );
    for (i, node) in nodes.iter().enumerate() {
        println!("  node {i}: client {}", node.client_addr());
    }
    println!("custosd: ready — Ctrl-C to stop");

    if let Err(e) = tokio::signal::ctrl_c().await {
        tracing::warn!(?e, "failed to listen for Ctrl-C");
    }
    println!("custosd: shutting down");
    ExitCode::SUCCESS
}

/// A simple majority quorum for `n` replicas with `R + W > N`.
fn quorum(n: usize) -> (usize, usize) {
    let majority = n / 2 + 1;
    (majority, majority)
}

struct Args {
    cluster: usize,
    dir: std::path::PathBuf,
    ip: IpAddr,
}

impl Args {
    fn parse(mut args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut cluster = None;
        let mut dir = std::env::temp_dir().join("custosd");
        let mut ip: IpAddr = "127.0.0.1".parse().unwrap();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--cluster" => {
                    let v = args.next().ok_or("--cluster needs a value")?;
                    cluster = Some(v.parse().map_err(|_| "--cluster must be a number")?);
                }
                "--dir" => dir = args.next().ok_or("--dir needs a value")?.into(),
                "--ip" => {
                    ip = args
                        .next()
                        .ok_or("--ip needs a value")?
                        .parse()
                        .map_err(|_| "--ip must be an IP address")?;
                }
                other => return Err(format!("unknown argument `{other}`")),
            }
        }
        let cluster = cluster.ok_or("--cluster N is required")?;
        if cluster == 0 {
            return Err("--cluster must be at least 1".into());
        }
        Ok(Self { cluster, dir, ip })
    }
}
