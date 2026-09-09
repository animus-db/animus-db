//! The DynamoDB-style TTL reaper (ADR 0051) — **moved to
//! `animus_node::ttl_reaper`** (ADR 0061 rung C2). This module is now a
//! thin wrapper threading this crate's own `ClientCtx` (which implements
//! `animus_node::host::TtlScanHost` — see `client_ctx_host.rs`) and its
//! `ProdEnv` handle into the moved, `E: Env`-generic loop. See
//! `animus_node::ttl_reaper`'s own module doc for the full design — this
//! file carries no logic of its own.
//!
//! **Widened to `<E: Env, R: RelayClient>` (ADR 0061 rung I, C-09 PR 2)** —
//! previously pinned to the concrete `ClientCtx` alias (`E = ProdEnv, R =
//! AnimusdRelayClient`). Every production call site (`lib.rs`) passes a
//! bare `ctx.clone()` (a `ClientCtx` with no explicit type arguments), so
//! those calls keep inferring the same defaults and stay byte-identical —
//! this is a pure signature widening, the same shape rung D4 PR 5 already
//! used for `client_ctx_host.rs`'s `TtlScanHost` impl.

use std::time::Duration;

use animus_env::Env;
use animus_node::host::RelayClient;

pub(crate) use animus_node::ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL;

pub(crate) async fn ttl_reaper_loop<E: Env, R: RelayClient>(
    ctx: crate::ClientCtx<E, R>,
    interval: Duration,
) {
    let env = ctx.env.clone();
    animus_node::ttl_reaper::ttl_reaper_loop(env, ctx, interval).await;
}
