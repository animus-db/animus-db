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
//!
//! **Threads an explicit `interval` (ADR 0061 rung I C-09 PR 3's follow-on,
//! 2026-09-09)** — this wrapper no longer hardcodes
//! `animus_node::index_backfill::INDEX_BACKFILL_LOOP_INTERVAL_MS`
//! internally; both real production spawn sites (`lib.rs`) pass
//! [`INDEX_BACKFILL_LOOP_INTERVAL`] explicitly (a `Duration` view of the
//! same millisecond constant this wrapper used to hardcode, so production
//! behavior is unchanged). `SimCluster` passes its own shared fallback-tick
//! constant instead — see that fixture's own doc for why.

use std::time::Duration;

use animus_env::Env;
use animus_node::host::RelayClient;

/// [`INDEX_BACKFILL_LOOP_INTERVAL_MS`](animus_node::index_backfill::INDEX_BACKFILL_LOOP_INTERVAL_MS)
/// as a `Duration` — this wrapper's own production-default `interval`
/// argument. The underlying `animus_node::index_backfill::
/// index_backfill_loop` still takes a plain `interval_ms: u64` (unchanged),
/// so this crate's own `Duration`-typed call sites (mirroring every other
/// widened loop's own `interval: Duration` parameter) convert once here
/// rather than at each of the two `lib.rs` spawn sites.
pub(crate) const INDEX_BACKFILL_LOOP_INTERVAL: Duration =
    Duration::from_millis(animus_node::index_backfill::INDEX_BACKFILL_LOOP_INTERVAL_MS);

pub(crate) async fn index_backfill_loop<E: Env, R: RelayClient>(
    ctx: crate::ClientCtx<E, R>,
    interval: Duration,
) {
    let env = ctx.env.clone();
    let interval_ms = u64::try_from(interval.as_millis()).unwrap_or(u64::MAX);
    animus_node::index_backfill::index_backfill_loop(env, ctx, interval_ms).await;
}
