# A routed operation's error policy must be identical on the local and forwarded routes (2026-09-02, issue #572)

`ClientCtx::force_pitr_seal_tablet`/`force_seal_tablet` (`animusd/src/
schema.rs`) resolve a tablet's leader and split on `CpRoute::Local(leader)`
(this node) vs. `CpRoute::Forward(addr)` (another node). The `Forward` arm
had always retried a transient `ClientResponse::Error` inside its own
`loop { .. deadline .. }` (`SCHEMA_COMMIT_TIMEOUT`/`SCHEMA_POLL_INTERVAL`).
The `Local` arm called `index_drain::seal_now`/`pitr_seal_now` once and
returned the first `Err` straight through `dynamo.rs`'s `internal(..)`
wrapper as a hard 500 — but that call has a *documented* dueling-seal race
against the periodic `seal_tick`/`pitr_tick` arm (same `INDEX_DRAIN_
INTERVAL` tick): when the periodic arm commits the same `(tablet,
next_epoch)` slot first with a shorter range, the call returns a
`"; retry"`-suffixed transient error (`index_drain::is_retryable_
elsewhere`), not a permanent one. So the identical client request —
`UpdateContinuousBackups(disable)` or a stream-disabling `UpdateTable` —
succeeded or 500'd depending on nothing about the request itself, only on
whether it happened to land on the tablet leader's own node. Caught by
`prod-liveness` (#566)'s no-retry-on-flake posture surfacing an
intermittent CI failure that a looser gate would have silently re-run past.

**The general lesson**: whenever a routed operation has separate
local-execution and forward-to-leader code paths — a shape this codebase
uses throughout the `ClientCtx` request-routing layer — the two paths must
apply the *same* retry/error classification to the *same* underlying
failure, not just produce equivalent results on the happy path. It is easy
to write the `Forward` arm's retry loop carefully (because forwarding is
the "exotic" path that obviously needs backoff and a deadline) and treat
`Local` as a shortcut that just calls the local primitive and propagates
whatever it returns, on the unstated assumption that "local" means
"no transient failures worth retrying." That assumption breaks the moment
the local primitive has *any* concurrency-sourced transient failure mode of
its own (here: racing against a different concurrently-running arm of the
same background loop) — which a routed primitive calling into shared local
state should be assumed to have unless proven otherwise. A test that only
exercises the `Forward` route (e.g. a multi-node cluster where the request
never happens to land on the leader) can pass indefinitely while the
`Local` route stays broken; the regression test for this fix deliberately
uses a **single-node** cluster specifically so `resolve_cp_route` can only
ever return `Local`, making the route under test unambiguous rather than
probabilistic.

This is the same *family* of bug as the missed-forwarding-allowlist lesson
elsewhere in this log (adding a variant to a replicated/forwarded command
enum and forgetting one gating match site) — both are "a request's outcome
silently depends on which of two code paths happened to serve it," i.e. a
**bimodal per-process flake**. The fix here: type the retryability
distinction (`index_drain::is_retryable_elsewhere`, reused — not
re-implemented — by the `Local` arm) rather than leaving it implicit in
"whichever path already happens to retry," and when reviewing a
`Local`/`Forward` (or any local-vs-remote) split, ask explicitly: does a
transient failure of the underlying primitive get the same treatment on
both arms? If not, that's not a stylistic inconsistency, it's a
correctness gap a client can observe as a coin-flip 500.
