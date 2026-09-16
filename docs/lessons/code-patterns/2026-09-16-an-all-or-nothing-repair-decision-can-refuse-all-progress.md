# An all-or-nothing repair decision can refuse all progress, not just full success

Found root-causing issue #957 (`tablet_rf_self_heals.rs`'s ~5% flake): a
2-node cluster's tablet, minted with only 1 replica by a genuine
initial-placement race, never gained its second replica even though a
second `Active` candidate was sitting right there the whole time.

## The trap: the issue's own hypothesized mechanism was wrong, and looked
## exactly right on paper

Issue #957 named the boot-time "wiped voter" cluster check (issue
#667/#900 — `RaftCore::begin_cluster_check`) as the prime suspect: it is
the most recently-landed, most actively-discussed mechanism on this exact
boot path, and every "candidate" in the issue's own write-up pointed at
it. It was wrong. A captured repro with `/admin/raftkv`+`/admin/raft`+
`/admin/status` polled once a second through the whole 60s stall showed
`cluster_check_pending: false` and `refused_as_voter: false` on every
replica, throughout — and the control plane's own consensus (`commit_index
== last_applied == engine_applied_index`, steady, `is_leader`/`leader`
consistent) was completely healthy and idle. Nothing was stuck at the Raft
layer at all. The real defect was one layer up, in the *placement policy
engine* `reconcile_placement` calls, not the *consensus* layer the issue's
own hypothesis pointed at.

**The generalizable rule**: a flaky-test issue's own "candidate
mechanisms" list is a set of hypotheses a previous investigation wrote
down, not a conclusion — however specific and well-argued each candidate
reads (and these were: real prior incidents, real prior fixes, a
plausible-sounding narrative), they are still unverified until a captured
repro's own state contradicts or confirms them. Capture the state first
(admin endpoints, tracing, whatever the stuck component's own diagnostics
already expose), and let *that* narrow the mechanism — don't let a
well-written prior hypothesis substitute for looking.

## The actual bug: `choose()`'s "reach the target or fail" contract

`animus_placement::choose` (the shared core of `select_replicas`/`replan`)
fills a replica set up to `policy.replication_factor` and, if it can't
reach that exact count, returns `Err(InsufficientCandidates)` — even when
it filled in *some* new, strictly-improving replicas along the way. A
tablet's placement policy always records the cluster's **target** RF
(`MAX_REPLICATION_FACTOR`), never the observed candidate count at
provision time — by design, so a later-grown cluster can repair up to it.
But `reconcile_placement`'s repair pass called plain `replan`, so the
instant the *current* candidate pool was smaller than that target RF (an
ordinary small cluster, or one node transiently down), the repair pass
computed `Err`, `.ok()?`'d it away inside a `filter_map`, and proposed
**nothing at all** — not even the smaller, achievable improvement. A
1-replica tablet on a 2-node/RF-3 cluster was therefore stuck at 1 replica
**forever**, not just "until enough nodes are present" — the two are very
different failure modes, and only the second is a mild capacity limit.

**The generalizable rule**: when a decision function is used to drive
*incremental repair* (not just fresh, one-shot placement), audit whether
its own success/failure boundary is "reached the ideal target" or "made
no progress" — those are the same boolean in a naive implementation
(`Err` on `chosen.len() < target`) but very different properties for a
repair loop that runs forever and is expected to *converge over time* as
resources become available. A repair pass that discards every partial
improvement because it isn't the full target will look identical to "the
repair mechanism is broken" from the outside (nothing ever gets proposed,
metrics stay flat, no error surfaces anywhere) while looking completely
correct from the inside (every individual call did exactly what its own
contract promised). The fix here — a `replan_repair` sibling that grows to
`min(target, eligible)` instead of refusing below `target`, added as a
new function rather than changing `replan`'s existing all-or-nothing
contract — is the general shape: give the *repair* caller a
best-effort variant, keep the *fresh-placement*/other callers on the
strict one, and make the best-effort variant a provable strict superset
(same answer whenever the strict one would already succeed, and an
explicit "never worse than what's already held" guarantee) so it can't
regress the callers that never needed the relaxation.
