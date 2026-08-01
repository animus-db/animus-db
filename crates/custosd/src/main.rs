//! CustosDB node server daemon.
//!
//! Placeholder entry point. The node wiring (control-plane membership, data-plane
//! request handling over `ProdEnv`) is assembled in later milestones.

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        "custosd is a placeholder; node wiring is not implemented yet"
    );
}
