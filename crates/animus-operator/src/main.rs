//! `animus-operator` — two subcommands:
//!
//! ```text
//! animus-operator run   # run the controller (in-cluster, or local kubeconfig)
//! animus-operator crd   # print the AnimusCluster CustomResourceDefinition YAML to stdout
//! ```

use kube::CustomResourceExt;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("crd") => print_crd(),
        Some("run") | None => run().await,
        Some(other) => {
            eprintln!("animus-operator: unknown subcommand `{other}` (expected `run` or `crd`)");
            std::process::exit(2);
        }
    }
}

fn print_crd() {
    let crd = animus_operator::AnimusCluster::crd();
    print!(
        "{}",
        serde_yaml::to_string(&crd).expect("CustomResourceDefinition serializes to YAML")
    );
}

async fn run() {
    tracing_subscriber::fmt::init();
    let client = match kube::Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("animus-operator: failed to build a Kubernetes client: {e}");
            std::process::exit(1);
        }
    };
    animus_operator::controller::run(client).await;
}
