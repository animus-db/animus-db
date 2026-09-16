# A Kubernetes `livenessProbe` must never gate on a distributed-consensus signal a healthy process cannot satisfy alone (issue #705/#710, ADR 0020/0060 2026-09-07 amendments)

`crates/animus-operator/src/desired/statefulset.rs` pointed both the
`readinessProbe` and the `livenessProbe` at `GET /admin/health`, which 503s
until the node's control Raft has had a leader **recently** (issue #595's
`leader_within` hysteresis, ADR 0020). That's the right signal for
readiness — don't route traffic to a node with no known leader — and the
wrong one for liveness: a pod recreated by the config-hash rolling restart
(or any fresh growth pod) has no control leader until
`animus-operator`'s own `advance_control_growth` (`controller.rs`) admits
it as a voter, which happens at most once per 30s reconcile cycle and only
after the pod's own admin port reports `role: "combined"`. A perfectly
healthy, correctly-joining process can legitimately outlast a
`livenessProbe`'s failure window (here, `initialDelaySeconds: 30` +
`periodSeconds: 10` × `failureThreshold: 6` ≈ 80s) with `/admin/health`
still, correctly, `503`. The kubelet's only response to a liveness failure
is a hard restart — so a healthy process got `SIGTERM`'d, its join reset,
and if the next attempt also outran the same window the pod cycled
indefinitely (`CrashLoopBackOff`, each cycle paced by the liveness
thresholds themselves — a slow-motion failure that looks like "the cluster
never comes up" from the outside, not like an obvious crash). Issue #705's
own investigation caught the symptom (three clean `SIGTERM`-driven exits
over ~4 minutes on a freshly-promoted combined-role pod, `restarts=0→6` at
~80s intervals in the eventual full repro) but could not pin the exact
`SIGTERM` source without `kubectl logs --previous` on the killed
instances; adding that capture in the same investigation is what let #710
find the actual cause.

**The general rule, not just this one probe**: a `livenessProbe` exists to
catch a genuinely wedged process (stopped answering entirely, deadlocked,
spinning) — its only fix, a hard restart, cannot help a process that is
correctly waiting on a cluster-level condition (an election, a quorum
being reached, a leader being elected or reachable), and actively hurts by
resetting whatever progress that wait had made. Never let a liveness route
answer anything but "is this process itself alive enough to respond" —
push every other kind of health signal (readiness, cluster convergence,
"is this specific tablet caught up") onto `readinessProbe`/`startupProbe`
or a separate diagnostic route instead. Concretely here: `animusd::admin`
gained `GET /admin/live`, unconditionally `200` whenever the admin server
can answer at all with no dependency on control-leader knowledge, tablet
hosting, or role; `readinessProbe` kept `GET /admin/health`, `livenessProbe`
moved to `GET /admin/live`. When adding ANY new probe (Kubernetes or
otherwise) against a distributed system, ask explicitly which of the two
questions ("should this instance receive traffic right now" vs. "is this
instance's own process alive") the probe is answering, and route it
accordingly — the two questions have different failure responses (pull
from load balancing vs. kill-and-restart) and conflating them turns a
transient, self-healing cluster-formation delay into a self-inflicted
restart storm.
