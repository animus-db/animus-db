# Fixing a deadlock that was masking a second, independent bug does not mean the scenario it hid now passes — verify the NEW failure text, not just the absence of the old one (issue #731's own follow-up, same day)

Landing the fix above and re-running its own motivating scenario
(`coordinator_never_finished_past_prepare_recovers_atomically`,
`sim_cluster_dynamo_transact.rs`) did **not** turn it green. The
deadlock's own symptom — `SimRelayClient::relay`'s timeout text, repeating
unchanged for the full poll budget — was gone, confirming the fix genuinely
worked: the nested relay hop this scenario needs now completes. But the
scenario still failed, with a *different*, equally unchanging error:
`Err("transaction covering this key is still pending; retry")`. Tracing
that text to its source (`cp_get_local_resolving_inner`'s
`TxnDecisionStatus::Pending` arm, reached only once `confirm_or_push`/
`txn_recover` both run to completion) led straight to a second, wholly
unrelated bug: `ClientCtx::txn_recover`'s own grace-check computes
`now_ms` as `self.env.now().duration_since(self.env.now())` — the elapsed
gap between two back-to-back clock reads, always near-zero — instead of
an absolute timestamp, whenever the recovery push runs on a node that
isn't the record's own local leader (the ordinary case for an on-demand
push triggered by a foreign read, as opposed to a background sweep that
only ever recovers tablets it itself leads). A near-zero `now_ms` can
never exceed `created_ts.wall_ms + RECOVERY_GRACE`, so the grace check
never passes and recovery declines forever, regardless of how much
virtual time has genuinely elapsed. This bug is pre-existing — introduced,
and *knowingly* left unfixed as out of scope, by an earlier `tokio::time::
Instant::now().elapsed()` → `Env` conversion rung, whose own commit
message said as much ("reproducing the identical near-zero result rather
than 'fixing' what reads like a pre-existing latent bug") — and it was
never reachable before this fix because the relay deadlock intercepted
every recovery attempt earlier in the same call chain.

**The general lesson**: when a fix removes a deadlock (or any other
"nothing ever gets far enough to fail informatively" bug) from a call
chain, re-running the scenario that found it can uncover a *second* bug
sitting immediately downstream — one the first bug was accidentally
shielding from ever being exercised at all. A green re-run is real
evidence the first bug is fixed; a **still-red** re-run needs the same
scrutiny as a brand-new failure, not a shrug of "the fix didn't work" —
diagnose the *current* error text on its own merits (here, tracing one
short string straight to its one source line) before concluding anything
about the original fix. The two bugs are almost always in different
subsystems (a testing primitive's dispatch shape vs. a distributed
recovery protocol's clock-comparison logic, in this case) and belong in
different fixes/PRs/issues — closing the first issue and filing a fresh
one for the second is the correct disposition, not stretching one fix to
cover both or declaring the original finding "not actually fixed."
