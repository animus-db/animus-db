# Superseded by ADR 0028

**Superseded by ADR 0028**: `propose_split_data`/`applied_split_key`/the
`pending`-retry map this entry discusses are all deleted along with the
data-plane half of split. Retained for historical record; the general
"`Accepted` isn't `committed`" lesson still applies (see `cp_put_local`'s
own confirm-by-index). **`ProposeResult::Accepted` means "appended to the leader's local log," never
"committed" — every proposer must confirm, and a bare boolean flag isn't always
enough to confirm the caller's *specific* request.** Fixing the auto-split
orphan bug (the entry above) closed the case where step 2 (`propose_split`)
returns a hard error, but tracing (ADR 0027 instrumentation added specifically
to diagnose this) showed a second, worse failure mode: `propose_split_data`
trusted `Accepted` as final success, like every CP-data write path *except*
this one (`cp_put_local`/`cp_delete_local` already poll for local confirmation
first — see `engine_applied_index`'s doc). Under the leader churn this
workload causes, an accepted-but-uncommitted `Split` entry is silently
truncated, and since the control-plane `SplitTablet` metadata was *already*
committed by that point, an unconfirmed "success" permanently stranded the
tablet with nothing left to retry it (my own `pending`-retry fix never
engaged, because nothing looked like a failure). Fix: added
`RaftKvNode::applied_split_key()` (mirrors `engine_applied_index`'s
confirm-by-index shape) and made `propose_split_data`/`cp_split_here` poll it
before reporting success. **The key, not just a flag, mattered**: an initial
version exposed a bare `is_split() -> bool`, but live verification (rebuild +
reproduce + re-check `/admin/raftkv`, not just re-reading the fix) showed
tablets *still* going unminted — under `--cluster N`'s shared edge state, two
different nodes' auto-split loops can independently read a tablet's live pairs
and compute a *different* median in the same tick (the pairs can shift
between two racing reads), each proposing a `Split` at their own key; the
group splits once, so the loser's key never applies even though *a* split
did — a bare "has it split" boolean can't tell a caller its own key lost.
Comparing the *exact* applied key closed it. **Corollary**: a `pending`-retry
map keyed on "keep replaying the same request" needs an exit for "this exact
request can now never succeed, but that's fine" (a losing key retried forever
is a live-forever no-op that also wrongly excludes the tablet from future
triggers) — not just "retry until it succeeds." **Method note**: when a live
reproduction is available, verify a fix by rebuilding and re-observing state,
not just re-reading the diff — the first fix compiled, passed all tests, and
still didn't fully close the bug in practice.
