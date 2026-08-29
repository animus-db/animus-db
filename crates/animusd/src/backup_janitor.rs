//! The on-demand backup janitor (ADR 0059 §3, Train 1 PR④) — **moved to
//! `animus_node::backup_janitor`** (ADR 0061 rung C2). This module is now a
//! thin wrapper threading this crate's own `ClientCtx` (which implements
//! `animus_node::host::{ControlLeaderHost, BackupObjectStore}` — see
//! `client_ctx_host.rs`) and its `ProdEnv` handle into the moved,
//! `E: Env`-generic loop. See `animus_node::backup_janitor`'s own module
//! doc for the full design — this file carries no logic of its own.

pub(crate) async fn backup_janitor_loop(ctx: crate::ClientCtx) {
    let env = ctx.env.clone();
    animus_node::backup_janitor::backup_janitor_loop(env, ctx).await;
}
