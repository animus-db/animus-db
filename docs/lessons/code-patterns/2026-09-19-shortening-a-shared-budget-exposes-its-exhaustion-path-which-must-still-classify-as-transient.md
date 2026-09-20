# Shortening a shared budget exposes its exhaustion path — and that path's own error must still classify as transient, or a correctness fix turns into a new terminal-error class

**Fixing a doubled budget (making a chase spend from one shared deadline
instead of two independently-minted ones) makes that chase exhaust its
budget more often, not less — and the code path that runs on exhaustion is
usually the least-exercised one in the whole function, because the old,
buggy double-budget behavior made it structurally rare to reach.** Issue
#961's own fix (see the sibling lesson,
`2026-09-19-a-shared-caller-owned-budget-must-be-threaded-not-re-minted-at-each-layer.md`)
threaded one caller-minted `deadline` through `cp_route`/`cp_forward`/
`forward_to_tablet_leader` (`crates/animusd/src/forwarding.rs`) instead of
letting each mint its own fresh `CLIENT_TIMEOUT`. That fix was correct and
necessary — but it also meant the *hop-chase-runs-out-of-time-while-still-
legitimately-chasing-a-transient-condition* branch, which used to need two
whole timeouts' worth of bad luck to reach, now needed only one. It went
from rare-in-practice to routinely reachable, and nobody had ever audited
what that branch actually *returned*.

**What it returned was wrong, and had been wrong all along**: the LAST
HOP's raw response — a relay timeout string, a transport-failure sentinel,
or a parsed not-leader refusal, none of which end in the house `"; retry"`
suffix `decide::read_should_retry` uses to classify an error as transient.
So the moment this branch became reachable in practice, every caller that
relied on that convention — `cp_kind_write_raw_bounded`'s own retry
wrapper, a test's own retry helper, eventually the DynamoDB wire's typed
error mapping — started treating a merely-exhausted, entirely-legitimate
transient chase (a leader mid-reformation, a hop that hadn't yet timed out
when the whole attempt's clock ran out) as a permanent failure. One test in
this repo's own suite (`sim_cluster_split_cluster.rs::decommission_racing_
a_tablet_split_converges_with_no_data_loss`) went from a rare, hard-to-hit
edge case to a reliably-reproducing failure the instant the budget-sharing
fix landed — not because the fix was wrong, but because it did exactly what
it was supposed to do: exhaust the shared budget on schedule, every time,
instead of masking the exhaustion behind a second independent timeout that
usually gave the chase enough extra runway to succeed anyway.

**The generalizable rule**: when tightening or sharing a budget that used
to be looser (removing a doubled timeout, lowering a retry cap, merging two
independent deadlines into one), always re-audit what the function returns
on exhaustion — not just whether the new, tighter bound is itself correct.
A budget that used to be generous enough that exhaustion was theoretical is
exactly the kind of path nobody wrote a test for, because nobody expected
to reach it. Making the budget correct is often the same change that makes
the exhaustion path load-bearing for the first time. Concretely, in a
codebase with a house "error ends in some fixed suffix means retry" or
"error implements some marker trait means retry" convention: grep every
early-return inside the tightened function for whether it preserves that
convention, not just whether it returns *an* error. A `return resp`
(forwarding the last sub-operation's raw failure) is the natural thing to
write when nobody expects to hit it — and the wrong thing once you've just
made hitting it routine.

**The fix's own shape, worth naming for reuse**: distinguish, at the
exhaustion point, between a genuinely *terminal* answer (a live peer
explicitly refused the operation — a real answer, return it verbatim) and
exhaustion while still chasing a condition every earlier branch in the same
function had already classified as *transient* (a timeout, a confirmed-
dead-but-retryable-elsewhere transport, an election in progress). Only the
second case needs the fix: synthesize a new error, carrying the required
"retry me" shape, that still cites the last sub-operation's own failure for
diagnosability (`format!("{PREFIX} (last hop: {last_hop_error}); retry")`)
rather than silently swallowing it. Add a small pure formatting helper with
its own direct unit test asserting the retry-classification property holds
— that property, not the exact wording, is what every caller actually
depends on.
