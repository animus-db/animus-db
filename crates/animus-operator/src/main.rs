//! `animus-operator` — two subcommands:
//!
//! ```text
//! animus-operator run [--admin-access {proxy,direct}]
//!   # run the controller (in-cluster, or local kubeconfig).
//!   # --admin-access selects how admin-port calls (the scale-down drain
//!   # sequence) reach a pod: `proxy` (default) goes through the
//!   # Kubernetes API server's pod-proxy subresource, which works whether
//!   # this process runs in-cluster or out-of-cluster (a local kubeconfig,
//!   # e.g. scripts/e2e-kind.sh); `direct` dials the pod itself, which
//!   # only works in-cluster. See crate::admin_client's own doc.
//! animus-operator crd   # print the AnimusCluster CustomResourceDefinition YAML to stdout
//! ```

use animus_operator::admin_client::AdminAccessMode;
use kube::CustomResourceExt;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("crd") => print_crd(),
        Some("run") | None => {
            let admin_access = match parse_admin_access(&args) {
                Ok(mode) => mode,
                Err(e) => {
                    eprintln!("animus-operator: {e}");
                    std::process::exit(2);
                }
            };
            run(admin_access).await;
        }
        Some(other) => {
            eprintln!("animus-operator: unknown subcommand `{other}` (expected `run` or `crd`)");
            std::process::exit(2);
        }
    }
}

/// Parse `--admin-access {proxy,direct}` (either `--admin-access proxy` or
/// `--admin-access=proxy`) out of `run`'s own argv, defaulting to
/// [`AdminAccessMode::Proxy`] when the flag is absent.
fn parse_admin_access(args: &[String]) -> Result<AdminAccessMode, String> {
    let mut iter = args.iter().skip(2);
    while let Some(arg) = iter.next() {
        if let Some(value) = arg.strip_prefix("--admin-access=") {
            return AdminAccessMode::parse(value);
        }
        if arg == "--admin-access" {
            let value = iter.next().ok_or_else(|| {
                "--admin-access requires a value (`proxy` or `direct`)".to_string()
            })?;
            return AdminAccessMode::parse(value);
        }
    }
    Ok(AdminAccessMode::default())
}

fn print_crd() {
    let crd = animus_operator::AnimusCluster::crd();
    print!(
        "{}",
        serde_yaml::to_string(&crd).expect("CustomResourceDefinition serializes to YAML")
    );
}

async fn run(admin_access: AdminAccessMode) {
    tracing_subscriber::fmt::init();
    // rustls 0.23 has no process-level default CryptoProvider unless exactly
    // one of its provider features is enabled across the whole dependency
    // graph; kube's `rustls-tls` leaves that choice to the application, so
    // building the client below panics without an explicit install.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("no other CryptoProvider is installed before this");
    let client = match kube::Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("animus-operator: failed to build a Kubernetes client: {e}");
            std::process::exit(1);
        }
    };
    animus_operator::controller::run(client, admin_access).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(rest: &[&str]) -> Vec<String> {
        let mut v = vec!["animus-operator".to_string(), "run".to_string()];
        v.extend(rest.iter().map(|s| s.to_string()));
        v
    }

    #[test]
    fn parse_admin_access_defaults_to_proxy_when_absent() {
        assert_eq!(
            parse_admin_access(&args(&[])).unwrap(),
            AdminAccessMode::Proxy
        );
    }

    #[test]
    fn parse_admin_access_accepts_a_space_separated_value() {
        assert_eq!(
            parse_admin_access(&args(&["--admin-access", "direct"])).unwrap(),
            AdminAccessMode::Direct
        );
    }

    #[test]
    fn parse_admin_access_accepts_an_equals_separated_value() {
        assert_eq!(
            parse_admin_access(&args(&["--admin-access=proxy"])).unwrap(),
            AdminAccessMode::Proxy
        );
    }

    #[test]
    fn parse_admin_access_rejects_an_unknown_value() {
        assert!(parse_admin_access(&args(&["--admin-access", "bogus"])).is_err());
    }

    #[test]
    fn parse_admin_access_rejects_a_missing_value() {
        assert!(parse_admin_access(&args(&["--admin-access"])).is_err());
    }
}
