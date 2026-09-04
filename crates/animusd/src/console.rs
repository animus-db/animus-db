//! animusd console (ADR 0052's "AnimusDB Data Console"): a DynamoDB-shaped
//! data app for application developers — browsing/querying/editing their
//! own tables and items — deliberately separate from the operator
//! dashboard the admin port serves (`dashboard.rs`, ADR 0021).
//!
//! **The pure routing logic moved to `animus_node::console`** (ADR 0061
//! rung C4c): every request/response type, the [`ConsoleBackend`] trait,
//! and `route` itself. See that crate's own module doc for the full per-PR
//! design history (why this is a trait and not a bare closure, the shape/
//! kind discipline every widening followed, and the "never show
//! cluster-shaped state" rule this trait's signatures enforce
//! structurally). What stays here: `serve`/`handle_conn` (the
//! `TcpListener`/`TcpStream` accept loop — real I/O, never under `SimEnv`),
//! the three console-shell assets (`console.html`/`console.css`/
//! `console.js`, plus the shared `fonts.css`/`tokens.css` — kept physically
//! here rather than moved, since `dashboard.rs`'s operator console
//! `include_str!`s `fonts.css`/`tokens.css` too and this rung's "no
//! behaviour change" charter is not here to relocate or duplicate shared
//! assets), and `lib.rs`'s `impl console::ConsoleBackend for ClientCtx` —
//! the one place a schema-catalog type is ever in scope on the console's
//! behalf.

use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};

// Re-exported so every existing `console::X` call site in `lib.rs` (the
// `ConsoleBackend` impl, the `TableSnapshotFn` construction in
// `spawn_common_tail`) keeps compiling unchanged — only the types `lib.rs`
// actually names; `animus_node::console` also carries the request types
// consumed purely inside its own `route`/`table_api_response`
// (`GetItemRequest`/`PutItemRequest`/`DeleteItemRequest`/
// `CreateGsiRequest`/`CreateLsiRequest`/`CreateKeyAttribute`), which never
// need a name here.
pub(crate) use animus_node::console::{
    AddGsiRequest, BackupSummary, ConsoleBackend, ConsoleError, CreateTableRequest,
    GetShardIteratorRequest, GetStreamRecordsRequest, GsiDetail, IndexKeySummary, ItemsPage,
    KeySummary, LsiDetail, PitrStatus, ProjectionSummary, QueryItemsRequest, ScanItemsRequest,
    SetStreamRequest, SetTtlRequest, ShardSummary, SortKeyQuery, StreamRecordsPage,
    StreamShardsPage, StreamShardsRequest, StreamSummary, TableDetail, TableSnapshotFn,
    TableSummary, TtlSummary, WireItem,
};

use crate::http;

/// The console's page shell, embedded at compile time.
const HTML: &str = include_str!("console.html");
/// The console's stylesheet: the embedded webfaces, then the shared design
/// tokens (ADR 0056), then this surface's own skin — same `concat!` shape as
/// `dashboard::CSS`, so both consoles serve one stylesheet from one constant.
const CSS: &str = concat!(
    include_str!("fonts.css"),
    include_str!("tokens.css"),
    include_str!("console.css"),
);
/// The console's client-side app (routing + every screen), vanilla JS, no
/// bundler — mirrors `dashboard.rs`'s `include_str!`'d asset shape.
const JS: &str = include_str!("console.js");

/// Accept loop for the console HTTP endpoint. One task per connection,
/// mirroring `admin::serve`/`dynamo::serve`'s own shape.
pub(crate) async fn serve(
    listener: TcpListener,
    tables: TableSnapshotFn,
    backend: Arc<dyn ConsoleBackend>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let tables = tables.clone();
                let backend = backend.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_conn(stream, tables, backend).await {
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

async fn handle_conn(
    mut stream: TcpStream,
    tables: TableSnapshotFn,
    backend: Arc<dyn ConsoleBackend>,
) -> std::io::Result<()> {
    let mut buf = Vec::new();
    loop {
        let Some(request) = http::read_http_request(&mut stream, &mut buf).await? else {
            return Ok(()); // clean EOF
        };
        let keep_alive = request.keep_alive;
        let (status, content_type, body) =
            animus_node::console::route(&request, &tables, backend.as_ref(), HTML, CSS, JS).await;
        http::write_response(&mut stream, status, content_type, &body, keep_alive).await?;
        if !keep_alive {
            return Ok(());
        }
    }
}
