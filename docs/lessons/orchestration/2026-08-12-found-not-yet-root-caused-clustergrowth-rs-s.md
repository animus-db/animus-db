# Found, not yet root-caused: `cluster_growth.rs`'s `dashboard_health_recovers_after_grown_cluster_loses_an_original_node` can hang indefinitely (300s+ backstop, no recovery) rather than merely lag, when all three of this file's tests run concurrently in the same binary — and this is very likely a real reconciliation livelock, not the ordinary ADR 0038 apply-task lag the rest of this file's polls were converted to tolerate.

**Found, not yet root-caused: `cluster_growth.rs`'s
`dashboard_health_recovers_after_grown_cluster_loses_an_original_node` can
hang indefinitely (300s+ backstop, no recovery) rather than merely lag,
when all three of this file's tests run concurrently in the same binary —
and this is very likely a real reconciliation livelock, not the ordinary
ADR 0038 apply-task lag the rest of this file's polls were converted to
tolerate.** Discovered while modernizing this file's flat-deadline polls
into the `poll_until_or_stalled` shape above: the new idle-progress
diagnostics showed the control-plane leader's OWN `/admin/raft`
`commit_index`/`last_applied`/`engine_applied_index` frozen **solid**
(zero movement across a 200s+ instrumented window, sampled every 3s) while
one tablet sat under-replicated (2 of 3 replicas, both non-voting ADR 0030
growth nodes, after an ORIGINAL control-voter was killed) — i.e. the
leader wasn't slowly catching up, it had stopped proposing *anything* new.
Reproduction data: 6/6 solo runs of this test clean (~18s each); every
pairwise combination with this file's other two tests clean; only the
full three-test-concurrent binary reproduces it, and only intermittently
(roughly 40% of sampled full-binary runs in this investigation). This
rules out a simple "always-broken" logic bug (solo and pairwise runs
prove the repair logic itself is correct and fast) but does NOT look like
ordinary contention-driven lag either — genuine lag should still make
*some* progress over 200s of sampling, not read as frozen at every 3s
sample. Left as a known, precisely-characterized open issue rather than
chased further in the poll-modernization PR that found it (this repo's own
convention: root-cause+fix an incidental live bug as its own PR, not
folded into unrelated work) — the `poll_until_or_stalled` conversion
itself is unaffected and still correct (it just now reports this failure
mode far more precisely, in ~60–300s with a frozen-watermark diagnostic,
instead of the old flat 120s timeout's opaque "never repaired" message).
Next step for whoever picks this up: reproduce with `RUST_LOG`/tracing on
the control-plane leader specifically (not just `/admin/raft` polling) to
see whether the placement reconciler's event loop is scheduled at all
during the stall, or whether it runs but its `replan`/rebalance step
concludes no action is needed for a still-under-replicated tablet whose
only remaining replicas are both non-voting growth nodes.

**Update: root-caused, and it's the second branch above, not a livelock at
all.** Instrumenting `reconcile_loop` directly (a raw `eprintln!` per tick,
removed before committing) showed it ticking exactly on schedule the
entire time, correctly leader-elected, with a fully accurate `PlacementView`
— and correctly computing **zero** proposals, every tick. The leader was
never stuck; it was correctly enforcing a policy that was itself wrong.
Tracing into `animus_placement::replan` found it: the stuck tablet's
recorded RF was **2**, not 3 like its siblings — `2` replicas legitimately
satisfies a policy of RF 2 forever, no matter how large the cluster grows.
The bug was in `animusd::ClientCtx::provision_tablet`
(`crates/animusd/src/lib.rs`): a tablet's placement policy was set to
`PlacementPolicy::simple("cp-rf", t.replicas.len())` — the size of its
*initial* replica set, observed at creation — instead of the fixed target
`MAX_REPLICATION_FACTOR`. Under `cluster_growth.rs`'s heavy
three-concurrent-cluster contention, the very first `put()` on a
freshly-bootstrapped 3-node cluster could race ahead of all 3 original
members' `Active` promotion landing in `Metadata`, provisioning the
table's tablet with only 2 replicas — a legitimate, expected best-effort
*initial* set — but then permanently recording RF 2 as the *policy*,
which growing the cluster to 5 nodes later never revisited (`reconcile_
placement` only repairs *violations of the recorded policy* — an
under-observed RF simply becomes a new, permanently-satisfied target).
Fixed by no longer deriving the policy from the observed replica count at
all: it now always records `MAX_REPLICATION_FACTOR`, so a best-effort
under-sized *initial* set self-heals via the reconciler's existing
violation-repair path (the same one that already replaces a killed
replica) the moment enough candidates are `Active` — see `provision_
tablet`'s own doc, `meta::tests::reconcile_with_insufficient_candidates_
is_a_stable_noop` (the "no proposal storm while under-candidated" proof),
and `animusd/tests/tablet_rf_self_heals.rs` (the end-to-end regression:
provision on a genuinely 2-node cluster, grow to 3, assert the tablet
grows to 3 replicas too — which fails/hangs against the unfixed code,
confirmed by temporarily reverting the fix and re-running it). Two
generalizable lessons follow, below.
