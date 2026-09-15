# A `StatefulSet`'s aggregate `N/N ready` count is not proof that the ONE pod a port-forward is about to hit is itself ready right now (issue #595, `scripts/e2e-kind.sh`)

`scripts/e2e-kind.sh` waited for `readyReplicas == 3`, then immediately
`kubectl port-forward svc/{name}-dynamo`'d and issued `CreateTable` — and
flaked twice (identical signature, unrelated commits) with the port-
forwarded pod having dropped `Ready: False` again within the few seconds
between the aggregate count check and the actual wire call, because a
Service's own `Endpoints`/port-forward target is resolved to exactly ONE
specific pod, while the count the script polled is a fleet-wide aggregate
that says nothing about *that one pod's* state a moment later. The general
form: **when a script or test is about to talk to one specific replica out
of a fleet, the readiness precondition it waits on must be that replica's
own, checked through the same path the real traffic will take — never a
fleet-wide count used as a proxy for one member's state.** Fixed by
resolving the actual backing pod off the Service's `Endpoints` first, then
port-forwarding that pod directly (not the Service) so the readiness check
and the real wire calls are provably the same connection target, and
polling that one pod's own `/admin/health` before ever issuing the real
call.

A second, independent lesson from the same investigation: the underlying
flake this exposed (a follower's `/admin/health` flipping to `503` on a
transient one-sided delay, ADR 0020's 2026-09-04 amendment) was real and
worth fixing at the source — but the script-side hardening above is still
correct and worth keeping alongside that fix, not instead of it. A
one-shot call immediately following a bootstrap/scale/rollout event is
exactly the "eventual property" shape this repo's own testing rule already
names (converged-or-timeout, never a fixed-deadline one-shot assert) — the
first write after a cluster comes up is not different in kind from a read
right after a leader kill, and both need the same treatment. When adding
such a retry, scope it to the exact narrow, named failure the investigation
found (here: a specific 500 message) rather than a blanket retry-on-any-
error, and verify the retried operation's own idempotency contract first
(`CreateTable`'s pre-existing duplicate-name check made a retry landing on
an already-committed table safe to treat as success) — a blanket retry
without that check can silently paper over a genuinely different failure
or double-apply a non-idempotent operation.
