//! The DynamoDB-style TTL reaper (ADR 0051) — **moved to
//! `animus_node::ttl_reaper`** (ADR 0061 rung C2). This module is now a
//! thin wrapper threading this crate's own `ClientCtx` (which implements
//! `animus_node::host::TtlScanHost` — see `client_ctx_host.rs`) and its
//! `ProdEnv` handle into the moved, `E: Env`-generic loop. See
//! `animus_node::ttl_reaper`'s own module doc for the full design — this
//! file carries no logic of its own.

use std::time::Duration;

pub(crate) use animus_node::ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL;

pub(crate) async fn ttl_reaper_loop(ctx: crate::ClientCtx, interval: Duration) {
    let env = ctx.env.clone();
    animus_node::ttl_reaper::ttl_reaper_loop(env, ctx, interval).await;
}
