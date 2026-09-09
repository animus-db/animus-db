//! The secondary-index **backfill-completion aggregator** (ADR 0045 §4) —
//! **moved to `animus_node::index_backfill`** (ADR 0061 rung C2). This
//! module is now a thin wrapper threading this crate's own `ClientCtx`
//! (which implements `animus_node::host::ControlLeaderHost<ProdEnv>` —
//! see `client_ctx_host.rs`) and its `ProdEnv` handle into the moved,
//! `E: Env`-generic loop. See `animus_node::index_backfill`'s own module
//! doc for the full design (the decision, the control-only-leader scope
//! note, why it has none) — this file carries no logic of its own.
//!
//! **Widened to `<E: Env, R: RelayClient>` (ADR 0061 rung J, C-10 PR 2)** —
//! previously pinned to the concrete `ClientCtx` alias (`E = ProdEnv, R =
//! AnimusdRelayClient`). Every production call site (`sim_cluster.rs`'s own
//! new spawn aside, `lib.rs`) passes a bare `ClientCtx` with no explicit
//! type arguments, so those calls keep inferring the same defaults and stay
//! byte-identical — the same pure signature-widening shape `ttl_reaper.rs`
//! already used (C-09 PR 2).

use animus_env::Env;
use animus_node::host::RelayClient;

pub(crate) async fn index_backfill_loop<E: Env, R: RelayClient>(ctx: crate::ClientCtx<E, R>) {
    let env = ctx.env.clone();
    animus_node::index_backfill::index_backfill_loop(
        env,
        ctx,
        animus_node::index_backfill::INDEX_BACKFILL_LOOP_INTERVAL_MS,
    )
    .await;
}
