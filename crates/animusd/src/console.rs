//! The AnimusDB **Data Console** (ADR 0052): a DynamoDB-shaped data app for
//! application developers — browsing/querying/editing their own tables and
//! items — deliberately separate from the operator dashboard the admin port
//! serves (`dashboard.rs`, ADR 0021). Its defining rule, enforced structurally
//! here rather than just documented: **this listener never serves
//! cluster-shaped state** — no nodes, replicas, tablets, Raft, quorum,
//! leaders, placement, or health. This module has no [`crate::ClientCtx`] at
//! all, so it cannot reach any of that even by accident.
//!
//! This PR is plumbing only: its own port (`RoleAddrs::console`, ADR 0052 —
//! not the admin port, which is documented no-auth/trusted-interface-only,
//! and not the DynamoDB wire port, which speaks a binary protocol, not HTTP)
//! serving a minimal self-contained placeholder shell. No JSON endpoints, no
//! real screens — later PRs in this stack add the tables list, the table
//! page, and the create-table form, each reading real data through
//! [`crate::ClientCtx`]'s CP primitives the way `dynamo.rs`/`cql.rs` already
//! do, at which point this module gains the same asset-route shape
//! `admin.rs`/`dashboard.rs` use today.
//!
//! Embedded at compile time (`include_str!`), no bundler/build step/external
//! assets — the same constraints `dashboard.rs` documents for the operator
//! console.

use tokio::net::{TcpListener, TcpStream};

use crate::http;

/// The console's page shell, embedded at compile time.
const HTML: &str = include_str!("console.html");
/// The console's (minimal) stylesheet.
const CSS: &str = include_str!("console.css");

/// Accept loop for the console HTTP endpoint. One task per connection,
/// mirroring `admin::serve`/`dynamo::serve`'s own shape.
pub(crate) async fn serve(listener: TcpListener) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tokio::spawn(async move {
                    if let Err(err) = handle_conn(stream).await {
                        tracing::debug!(?err, "console connection closed");
                    }
                });
            }
            Err(err) => {
                tracing::warn!(?err, "console accept failed");
                return;
            }
        }
    }
}

async fn handle_conn(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buf = Vec::new();
    loop {
        let Some(request) = http::read_http_request(&mut stream, &mut buf).await? else {
            return Ok(()); // clean EOF
        };
        let keep_alive = request.keep_alive;
        if request.method != "GET" {
            http::write_response(&mut stream, 405, "text/plain", "GET only", keep_alive).await?;
            if !keep_alive {
                return Ok(());
            }
            continue;
        }
        if request.path == "/console/ui/console.css" {
            http::write_response(&mut stream, 200, "text/css; charset=utf-8", CSS, keep_alive)
                .await?;
        } else if is_shell_path(&request.path) {
            http::write_response(
                &mut stream,
                200,
                "text/html; charset=utf-8",
                HTML,
                keep_alive,
            )
            .await?;
        } else {
            http::write_response(&mut stream, 404, "text/plain", "not found", keep_alive).await?;
        }
        if !keep_alive {
            return Ok(());
        }
    }
}

/// Whether `path` should serve the console shell — the root, a couple of
/// `/console` aliases, and any `/console/ui/<screen>` deep link (mirroring
/// `admin::is_ui_path`'s own shape): a bookmark/refresh of a future screen's
/// URL lands back on the shell instead of a 404, exactly like the operator
/// dashboard's own deep-link contract.
fn is_shell_path(path: &str) -> bool {
    matches!(path, "/" | "/console" | "/console/" | "/console/ui")
        || path.starts_with("/console/ui/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_path_covers_root_aliases_and_deep_links() {
        assert!(is_shell_path("/"));
        assert!(is_shell_path("/console"));
        assert!(is_shell_path("/console/"));
        assert!(is_shell_path("/console/ui"));
        assert!(is_shell_path("/console/ui/tables"));
        assert!(is_shell_path("/console/ui/tables/orders"));
        assert!(!is_shell_path("/admin"));
        assert!(!is_shell_path("/consoleX"));
        // `is_shell_path` alone also matches the one static asset path
        // (`/console/ui/console.css`) — harmless, since `handle_conn`
        // checks the exact asset path FIRST and only falls through to this
        // predicate afterward, so the asset route always wins in practice.
        assert!(is_shell_path("/console/ui/console.css"));
    }
}
