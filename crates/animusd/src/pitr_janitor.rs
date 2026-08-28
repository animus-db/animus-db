//! PITR periodic base snapshots + retention (ADR 0059 §9, Train 3) —
//! **moved to `animus_node::pitr_janitor`** (ADR 0061 rung C2). This module
//! is now a thin wrapper threading this crate's own `ClientCtx` (which
//! implements `animus_node::host::{ControlLeaderHost, BackupObjectStore}` —
//! see `client_ctx_host.rs`) and its `ProdEnv` handle into the moved,
//! `E: Env`-generic loops. See `animus_node::pitr_janitor`'s own module doc
//! for the full design — this file carries no logic of its own.

use std::time::Duration;

pub(crate) use animus_node::pitr_janitor::{DEFAULT_PITR_RETENTION, DEFAULT_PITR_SNAPSHOT_CADENCE};

pub(crate) async fn pitr_snapshot_loop(ctx: crate::ClientCtx, snapshot_cadence: Duration) {
    let env = ctx.env.clone();
    animus_node::pitr_janitor::pitr_snapshot_loop(env, ctx, snapshot_cadence).await;
}

pub(crate) async fn pitr_janitor_loop(ctx: crate::ClientCtx, retention: Duration) {
    let env = ctx.env.clone();
    animus_node::pitr_janitor::pitr_janitor_loop(env, ctx, retention).await;
}
