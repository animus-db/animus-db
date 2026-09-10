//! The on-demand backup janitor (ADR 0059 §3, Train 1 PR④) — **moved to
//! `animus_node::backup_janitor`** (ADR 0061 rung C2). This module is now a
//! thin wrapper threading this crate's own `ClientCtx` (which implements
//! `animus_node::host::{ControlLeaderHost, BackupObjectStore}` — see
//! `client_ctx_host.rs`) and its `Env` handle into the moved,
//! `E: Env`-generic loop. See `animus_node::backup_janitor`'s own module
//! doc for the full design — this file carries no logic of its own.
//!
//! **Widened to `<E, R>` (ADR 0061 rung D4 PR 5)** — previously hardcoded to
//! the concrete `ClientCtx` alias (`E = ProdEnv, R = AnimusdRelayClient`),
//! the last piece stopping this loop from being spawnable under `SimEnv` at
//! all (`client_ctx_host.rs`'s own four trait impls were the other one, and
//! are widened the same rung). Production's own call site
//! (`spawn_common_tail`) still infers `E = ProdEnv, R = AnimusdRelayClient`
//! from the concrete `ClientCtx` it passes in, so this is a pure signature
//! widening with no behavior change there.
//!
//! **Threads an explicit `interval` (ADR 0061 rung I C-09 PR 3's follow-on,
//! 2026-09-09)** — `animus_node::backup_janitor::backup_janitor_loop` no
//! longer hardcodes its own tick cadence internally; both real production
//! spawn sites (`lib.rs`) pass [`BACKUP_JANITOR_INTERVAL`] explicitly (a
//! re-export, byte-identical to the constant this loop used to hardcode),
//! so production behavior is unchanged. `SimCluster` passes its own shared
//! fallback-tick constant instead — see that fixture's own doc for why.

use std::time::Duration;

use animus_env::Env;
use animus_node::host::RelayClient;

pub(crate) use animus_node::backup_janitor::BACKUP_JANITOR_INTERVAL;

pub(crate) async fn backup_janitor_loop<E: Env, R: RelayClient>(
    ctx: crate::ClientCtx<E, R>,
    interval: Duration,
) {
    let env = ctx.env.clone();
    animus_node::backup_janitor::backup_janitor_loop(env, ctx, interval).await;
}
