//! The secondary-index **backfill-completion aggregator** (ADR 0045 §4) —
//! **moved to `animus_node::index_backfill`** (ADR 0061 rung C2). This
//! module is now a thin wrapper threading this crate's own `ClientCtx`
//! (which implements `animus_node::host::ControlLeaderHost<ProdEnv>` —
//! see `client_ctx_host.rs`) and its `ProdEnv` handle into the moved,
//! `E: Env`-generic loop. See `animus_node::index_backfill`'s own module
//! doc for the full design (the decision, the control-only-leader scope
//! note, why it has none) — this file carries no logic of its own.

pub(crate) async fn index_backfill_loop(ctx: crate::ClientCtx) {
    let env = ctx.env.clone();
    animus_node::index_backfill::index_backfill_loop(
        env,
        ctx,
        animus_node::index_backfill::INDEX_BACKFILL_LOOP_INTERVAL_MS,
    )
    .await;
}
