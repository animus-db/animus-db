//! On-demand backup completion aggregator (ADR 0059 §3/§4, Train 1 PR③) —
//! **moved to `animus_node::backup_completion`** (ADR 0061 rung C2). This
//! module is now a thin wrapper threading this crate's own `ClientCtx`
//! (which implements `animus_node::host::{ControlLeaderHost,
//! BackupObjectStore}` — see `client_ctx_host.rs`) and its `ProdEnv` handle
//! into the moved, `E: Env`-generic loop. See
//! `animus_node::backup_completion`'s own module doc for the full design —
//! this file carries no logic of its own.

pub(crate) async fn backup_completion_loop(ctx: crate::ClientCtx) {
    let env = ctx.env.clone();
    animus_node::backup_completion::backup_completion_loop(env, ctx).await;
}
