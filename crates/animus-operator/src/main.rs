//! `animus-operator` — three subcommands:
//!
//! ```text
//! animus-operator run [--admin-access {proxy,direct}]
//!            [--webhook-addr ADDR --webhook-cert PATH --webhook-key PATH]
//!            [--webhook-only]
//!   # run the controller (in-cluster, or local kubeconfig).
//!   # --admin-access selects how admin-port calls (the scale-down drain
//!   # sequence) reach a pod: `proxy` (default) goes through the
//!   # Kubernetes API server's pod-proxy subresource, which works whether
//!   # this process runs in-cluster or out-of-cluster (a local kubeconfig,
//!   # e.g. scripts/e2e-kind.sh); `direct` dials the pod itself, which
//!   # only works in-cluster. See crate::admin_client's own doc.
//!   # --webhook-{addr,cert,key} (S-07e, ADR 0070) opt into the validating
//!   # admission webhook (crate::webhook): all three or none. Omitted (the
//!   # default), nothing listens beyond the reconcile loop above — the
//!   # out-of-cluster `cargo run -p animus-operator -- run` flow
//!   # scripts/e2e-kind.sh's plain leg uses is unchanged.
//!   # --webhook-only serves *only* the webhook — no reconcile loop, and no
//!   # Kubernetes client is ever built at all (validate_spec is pure, so
//!   # the webhook itself never touches the API). Requires the three
//!   # --webhook-* flags. This is what lets `scripts/e2e-kind.sh`'s
//!   # E2E_WEBHOOK=1 leg run a *second*, minimal, in-cluster operator
//!   # process serving just the webhook (reachable by the API server,
//!   # which an out-of-cluster process is not) while the ordinary
//!   # reconcile loop keeps running out-of-cluster exactly as every other
//!   # leg already does — see this crate's own CLAUDE.md e2e section for
//!   # why that split, not a full in-cluster reconciler, is this PR's
//!   # deliberately smaller e2e scope.
//! animus-operator crd   # print the AnimusCluster CustomResourceDefinition YAML to stdout
//! animus-operator webhook-cert --namespace NS --service NAME --issuer-name NAME
//!            [--issuer-kind Issuer|ClusterIssuer] [--secret-name NAME]
//!            [--duration DUR] [--renew-before DUR]
//!   # print a standalone cert-manager Certificate YAML for the webhook's
//!   # own TLS material, to stdout — pipe into `kubectl apply -f -`. See
//!   # deploy/operator/README.md's webhook section for the full picture,
//!   # including the alternative hand-issued-Secret path.
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;

use animus_operator::admin_client::AdminAccessMode;
use animus_operator::crd::IssuerRef;
use animus_operator::desired::certificate;
use kube::CustomResourceExt;

/// `--webhook-addr`/`--webhook-cert`/`--webhook-key` (S-07e, ADR 0070),
/// resolved and validated together — see [`parse_webhook_config`].
#[derive(Debug)]
struct WebhookConfig {
    addr: SocketAddr,
    cert_path: PathBuf,
    key_path: PathBuf,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("crd") => print_crd(),
        Some("webhook-cert") => match print_webhook_cert(&args) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("animus-operator: {e}");
                std::process::exit(2);
            }
        },
        Some("run") | None => {
            let admin_access = match parse_admin_access(&args) {
                Ok(mode) => mode,
                Err(e) => {
                    eprintln!("animus-operator: {e}");
                    std::process::exit(2);
                }
            };
            let webhook = match parse_webhook_config(&args) {
                Ok(cfg) => cfg,
                Err(e) => {
                    eprintln!("animus-operator: {e}");
                    std::process::exit(2);
                }
            };
            let webhook_only = args.iter().skip(2).any(|a| a == "--webhook-only");
            if webhook_only && webhook.is_none() {
                eprintln!(
                    "animus-operator: --webhook-only requires --webhook-addr/--webhook-cert/\
                     --webhook-key to also be given"
                );
                std::process::exit(2);
            }
            run(admin_access, webhook, webhook_only).await;
        }
        Some(other) => {
            eprintln!(
                "animus-operator: unknown subcommand `{other}` (expected `run`, `crd`, or \
                 `webhook-cert`)"
            );
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

/// Parse `--webhook-addr`/`--webhook-cert`/`--webhook-key` out of `run`'s
/// own argv (S-07e, ADR 0070) — `Ok(None)` when none of the three are
/// given (the default: no webhook server, byte-for-byte the pre-S-07e
/// behavior), `Ok(Some(..))` when all three are, `Err` when only some are
/// (an admission webhook needs a real address and a real cert/key pair;
/// there is no sensible partial default for any of the three).
fn parse_webhook_config(args: &[String]) -> Result<Option<WebhookConfig>, String> {
    let mut addr: Option<String> = None;
    let mut cert: Option<String> = None;
    let mut key: Option<String> = None;
    let mut iter = args.iter().skip(2);
    while let Some(arg) = iter.next() {
        if let Some(value) = arg.strip_prefix("--webhook-addr=") {
            addr = Some(value.to_string());
        } else if arg == "--webhook-addr" {
            addr = Some(
                iter.next()
                    .ok_or("--webhook-addr requires a value (e.g. 0.0.0.0:9443)")?
                    .clone(),
            );
        } else if let Some(value) = arg.strip_prefix("--webhook-cert=") {
            cert = Some(value.to_string());
        } else if arg == "--webhook-cert" {
            cert = Some(
                iter.next()
                    .ok_or("--webhook-cert requires a value (a PEM cert chain file path)")?
                    .clone(),
            );
        } else if let Some(value) = arg.strip_prefix("--webhook-key=") {
            key = Some(value.to_string());
        } else if arg == "--webhook-key" {
            key = Some(
                iter.next()
                    .ok_or("--webhook-key requires a value (a PEM private key file path)")?
                    .clone(),
            );
        }
    }
    match (addr, cert, key) {
        (None, None, None) => Ok(None),
        (Some(addr), Some(cert), Some(key)) => {
            let parsed_addr = addr
                .parse::<SocketAddr>()
                .map_err(|e| format!("--webhook-addr {addr:?}: {e}"))?;
            Ok(Some(WebhookConfig {
                addr: parsed_addr,
                cert_path: PathBuf::from(cert),
                key_path: PathBuf::from(key),
            }))
        }
        _ => Err(
            "--webhook-addr/--webhook-cert/--webhook-key must be given together (all three, \
             or none) to enable the validating admission webhook"
                .to_string(),
        ),
    }
}

fn print_crd() {
    let crd = animus_operator::AnimusCluster::crd();
    print!(
        "{}",
        serde_yaml::to_string(&crd).expect("CustomResourceDefinition serializes to YAML")
    );
}

/// `animus-operator webhook-cert` — print a standalone cert-manager
/// `Certificate` for the admission webhook's own TLS material (S-07e, ADR
/// 0070), reusing `crate::desired::certificate`'s builder (the same one an
/// `AnimusCluster`'s own `spec.tls.certManager` uses, generalized —
/// `certificate::build_standalone`). Not applied to the cluster by this
/// process itself — piped into `kubectl apply -f -`, mirroring `crd`'s own
/// "print YAML to stdout" shape, since this is a one-time, cluster-
/// independent object the operator's own reconcile loop has no reason to
/// own or re-apply on every tick.
fn print_webhook_cert(args: &[String]) -> Result<(), String> {
    let mut ns: Option<String> = None;
    let mut service: Option<String> = None;
    let mut issuer_name: Option<String> = None;
    let mut issuer_kind = "ClusterIssuer".to_string();
    let mut secret_name: Option<String> = None;
    let mut duration: Option<String> = None;
    let mut renew_before: Option<String> = None;

    let mut iter = args.iter().skip(2);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--namespace" => {
                ns = Some(iter.next().ok_or("--namespace requires a value")?.clone());
            }
            "--service" => {
                service = Some(iter.next().ok_or("--service requires a value")?.clone());
            }
            "--issuer-name" => {
                issuer_name = Some(iter.next().ok_or("--issuer-name requires a value")?.clone());
            }
            "--issuer-kind" => {
                issuer_kind = iter.next().ok_or("--issuer-kind requires a value")?.clone();
            }
            "--secret-name" => {
                secret_name = Some(iter.next().ok_or("--secret-name requires a value")?.clone());
            }
            "--duration" => {
                duration = Some(iter.next().ok_or("--duration requires a value")?.clone());
            }
            "--renew-before" => {
                renew_before = Some(
                    iter.next()
                        .ok_or("--renew-before requires a value")?
                        .clone(),
                );
            }
            other => return Err(format!("webhook-cert: unknown flag `{other}`")),
        }
    }
    let ns = ns.ok_or("webhook-cert: --namespace is required")?;
    let service = service.ok_or("webhook-cert: --service is required")?;
    let issuer_name = issuer_name.ok_or("webhook-cert: --issuer-name is required")?;
    if issuer_kind != "Issuer" && issuer_kind != "ClusterIssuer" {
        return Err(format!(
            "webhook-cert: --issuer-kind must be `Issuer` or `ClusterIssuer`, got {issuer_kind:?}"
        ));
    }
    let cert_name = format!("{service}-cert");
    let secret_name = secret_name.unwrap_or_else(|| format!("{service}-tls"));
    let dns_names = certificate::webhook_dns_names(&service, &ns);
    let issuer_ref = IssuerRef {
        name: issuer_name,
        kind: issuer_kind,
        group: None,
    };
    let obj = certificate::build_standalone(
        &cert_name,
        &ns,
        &secret_name,
        &dns_names,
        &issuer_ref,
        duration.as_deref(),
        renew_before.as_deref(),
    );
    print!(
        "{}",
        serde_yaml::to_string(&obj).expect("DynamicObject serializes to YAML")
    );
    Ok(())
}

async fn run(admin_access: AdminAccessMode, webhook: Option<WebhookConfig>, webhook_only: bool) {
    tracing_subscriber::fmt::init();
    // rustls 0.23 has no process-level default CryptoProvider unless exactly
    // one of its provider features is enabled across the whole dependency
    // graph; kube's `rustls-tls` leaves that choice to the application, so
    // building the client below panics without an explicit install. The
    // webhook server's own `TlsAcceptor` (below) reuses this same
    // process-global provider — installed once, here, before either is
    // built.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("no other CryptoProvider is installed before this");

    // S-07e (ADR 0070): opt-in validating admission webhook. A TLS-material
    // or bind failure is fatal either way — a cluster whose deployment
    // named these flags expects the webhook to actually be up; silently
    // continuing without it would leave `failurePolicy: Fail` rejecting
    // every write against a `ValidatingWebhookConfiguration` pointing at
    // nothing.
    if let Some(webhook) = webhook {
        let acceptor = match animus_operator::webhook::load_tls_acceptor(
            &webhook.cert_path,
            &webhook.key_path,
        ) {
            Ok(a) => a,
            Err(e) => {
                eprintln!("animus-operator: failed to load webhook TLS material: {e}");
                std::process::exit(1);
            }
        };
        let addr = webhook.addr;

        if webhook_only {
            // `--webhook-only`: this IS the whole process — no reconcile
            // loop, no Kubernetes client ever built (`validate_spec` is
            // pure, so nothing the webhook does needs one). Run directly in
            // the foreground rather than spawning, since there is nothing
            // else for this task to run alongside. See this file's own
            // module doc and `scripts/e2e-kind.sh`'s E2E_WEBHOOK leg for
            // why this mode exists.
            if let Err(e) = animus_operator::webhook::run(addr, acceptor).await {
                eprintln!("animus-operator: webhook server failed: {e}");
                std::process::exit(1);
            }
            return;
        }

        // Spawned before the reconcile loop below (which never returns on
        // success) so a bind/TLS failure surfaces at startup, not silently
        // never starting the listener.
        // ADR 0003 / ADR 0061 Decision 4 (rung B5): this crate has no `Env`
        // seam — the webhook server is a real process boundary, mirroring
        // every other real-socket spawn in this crate (see `crate::
        // webhook`'s own module doc).
        #[allow(
            clippy::disallowed_methods,
            reason = "animus-operator has no Env seam (see its own CLAUDE.md); the webhook server is a real process boundary, outside ADR 0003's scope"
        )]
        tokio::spawn(async move {
            if let Err(e) = animus_operator::webhook::run(addr, acceptor).await {
                eprintln!("animus-operator: webhook server failed: {e}");
                std::process::exit(1);
            }
        });
    }

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

    #[test]
    fn parse_webhook_config_defaults_to_none_when_absent() {
        assert!(parse_webhook_config(&args(&[])).unwrap().is_none());
    }

    #[test]
    fn parse_webhook_config_accepts_all_three() {
        let cfg = parse_webhook_config(&args(&[
            "--webhook-addr",
            "0.0.0.0:9443",
            "--webhook-cert",
            "/etc/webhook/tls.crt",
            "--webhook-key",
            "/etc/webhook/tls.key",
        ]))
        .unwrap()
        .expect("all three given");
        assert_eq!(cfg.addr, "0.0.0.0:9443".parse().unwrap());
        assert_eq!(cfg.cert_path, PathBuf::from("/etc/webhook/tls.crt"));
        assert_eq!(cfg.key_path, PathBuf::from("/etc/webhook/tls.key"));
    }

    #[test]
    fn parse_webhook_config_accepts_equals_separated_values() {
        let cfg = parse_webhook_config(&args(&[
            "--webhook-addr=0.0.0.0:9443",
            "--webhook-cert=/etc/webhook/tls.crt",
            "--webhook-key=/etc/webhook/tls.key",
        ]))
        .unwrap()
        .expect("all three given");
        assert_eq!(cfg.addr, "0.0.0.0:9443".parse().unwrap());
    }

    #[test]
    fn parse_webhook_config_rejects_a_partial_set() {
        assert!(
            parse_webhook_config(&args(&["--webhook-addr", "0.0.0.0:9443"]))
                .unwrap_err()
                .contains("together")
        );
        assert!(
            parse_webhook_config(&args(&[
                "--webhook-cert",
                "a.crt",
                "--webhook-key",
                "a.key"
            ]))
            .unwrap_err()
            .contains("together")
        );
    }

    #[test]
    fn parse_webhook_config_rejects_an_unparseable_addr() {
        assert!(
            parse_webhook_config(&args(&[
                "--webhook-addr",
                "not-an-addr",
                "--webhook-cert",
                "a.crt",
                "--webhook-key",
                "a.key",
            ]))
            .is_err()
        );
    }

    #[test]
    fn parse_webhook_config_ignores_admin_access_flags() {
        let cfg = parse_webhook_config(&args(&["--admin-access", "direct"])).unwrap();
        assert!(cfg.is_none());
    }

    #[test]
    fn print_webhook_cert_requires_namespace_service_and_issuer_name() {
        let argv = |rest: &[&str]| {
            let mut v = vec!["animus-operator".to_string(), "webhook-cert".to_string()];
            v.extend(rest.iter().map(|s| s.to_string()));
            v
        };
        assert!(print_webhook_cert(&argv(&[])).is_err());
        assert!(
            print_webhook_cert(&argv(&["--namespace", "ns"]))
                .unwrap_err()
                .contains("--service")
        );
        assert!(
            print_webhook_cert(&argv(&["--namespace", "ns", "--service", "svc"]))
                .unwrap_err()
                .contains("--issuer-name")
        );
    }

    #[test]
    fn print_webhook_cert_rejects_an_unknown_issuer_kind() {
        let argv = vec![
            "animus-operator".to_string(),
            "webhook-cert".to_string(),
            "--namespace".to_string(),
            "ns".to_string(),
            "--service".to_string(),
            "svc".to_string(),
            "--issuer-name".to_string(),
            "issuer".to_string(),
            "--issuer-kind".to_string(),
            "NotARealKind".to_string(),
        ];
        assert!(
            print_webhook_cert(&argv)
                .unwrap_err()
                .contains("--issuer-kind")
        );
    }

    #[test]
    fn print_webhook_cert_succeeds_with_the_minimal_required_flags() {
        let argv = vec![
            "animus-operator".to_string(),
            "webhook-cert".to_string(),
            "--namespace".to_string(),
            "animus-operator".to_string(),
            "--service".to_string(),
            "animus-operator-webhook".to_string(),
            "--issuer-name".to_string(),
            "selfsigned".to_string(),
        ];
        assert!(print_webhook_cert(&argv).is_ok());
    }
}
