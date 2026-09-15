# A propose-then-confirm loop must end its wait the moment confirmation is provably futile, not "wait out the client timeout, which is correct"

**A propose-then-confirm loop must end its wait the moment confirmation
is provably futile, not "wait out the client timeout, which is correct"**
(issue #268, 2026-08-17). The CP write confirm loops (`cp_put_local`/
`cp_delete_local`/`cp_batch_local`/`cp_kind_local`/`cp_kind_raw_local`)
polled value-equality for the full 10s `CLIENT_TIMEOUT` whenever an
*accepted* entry's effect never appeared — a deposed leader's truncated
entry, a freeze/seal apply-time no-op, a failed `KindBatch` condition.
Each such attempt is a 10s client-visible stall, and the caller's retry
then starts another: under the brief election churn a starved CI
runner's slow fsyncs produce, two stacked burns exceeded cp_txn.rs's
whole 25s put budget (observed live as an 11s "kind batch did not apply
in time" attempt whose immediate retry succeeded in milliseconds).
`animus-cp-data`'s own `wait_stage_outcome` already had the right shape
(`!is_leader()` bails immediately); the animusd confirm loops now share
it via `ClientCtx::confirm_wait_is_futile` — futile once
`engine_applied_index() >= accepted_index` without the effect (sound
because the apply task advances `engine_applied` only after merges are
readable, and any re-elected leader's no-op pushes apply past a
truncated index promptly) or once `!is_leader()`. **The coarse signal
only ever ends a wait with a retryable error, never acks one** — success
still requires exact effect equality (the false-ack hazard
`cp_put_local`'s doc spells out is unchanged). Regression:
`confirm_futility_tests` (in-crate, `cargo test -p animusd --lib`) — a
condition-failed `KindBatch` no-ops at apply and must surface as a fast
`"; retry"` error, not a 10s generic timeout. General form: when a
confirm poll can distinguish "still in flight" from "can no longer
land," burning the full timeout on the latter converts transient churn
into stacked client stalls that read as unavailability.
