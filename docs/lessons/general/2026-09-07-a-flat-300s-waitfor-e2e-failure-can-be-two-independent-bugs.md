# A "flat 300s wait_for" e2e failure can be two independent bugs stacked on the same call site — and "the only Ok(()) path" is a fast, code-grounded way to bound a signal-driven restart hypothesis (issue #705, #703)

`scripts/e2e-kind.sh`'s S-07d `controlNodes` growth wait
(`wait_for "control group reports 4 voters" 300 5 -- control_voters_equals
4`) timed out on two separate real CI runs for two *different* reasons,
both landing on the exact same line: (1) issue #705's own job log
(34085241118) — the newly-promoted ordinal's pod exited cleanly three
times over ~4 minutes before stabilizing, eating most of the flat budget
before the growth machinery (gated on that ordinal's `/admin/config`
reporting `role: "combined"`, polled on the operator's own 30s reconcile
cadence) ever got a chance to run; (2) PR #703's e2e-kind-encryption run
(34100368447) — the *pinned* port-forward this wait polls through died
when the S-07d rolling restart recycled its own target pod, and the old
`control_voters_count` folded every subsequent connection failure into a
bare `0`, so the wait spun its entire remaining budget on a false reading
instead of noticing it had lost its connection. **Neither symptom alone
explains the other's run** — a fix aimed only at the diagnosis in front of
you (whichever run you happen to be looking at) would have left the other
one still flaky. Root-causing "why did this flat wait_for time out" needs
to ask whether the SAME call site can time out for unrelated reasons
before concluding a single fix closes it.

Root-causing (1) without live cluster access (no `kubectl logs --previous`
had been captured — see below): the strongest code-grounded technique
available was tracing every `Ok(())` return path in the process being
restarted. `crates/animusd/src/main.rs`'s `wait_for_ctrl_c()` (SIGINT/
SIGTERM → `shutdown_graceful()` → `Ok(())`) turned out to be the **only**
way a running `animusd` (combined or data role) returns success — every
startup/runtime error instead takes the `Err` path to a nonzero exit code,
and a `grep` for `process::exit` across the whole crate came up empty. That
single fact converts "why did this pod exit 0 three times" from "could be
anything" into "was necessarily SIGTERM'd three times" — a much narrower,
useful claim, even though it does *not* pin down who sent the signal
(livenessProbe kill vs. something else remained unconfirmed). General
technique: when a process's exit code is the one hard fact you have and
its source isn't inspectable, grep every `Ok(())`/success-exit site in
`main` and see how many of them are actually reachable from the observed
starting state — often only one is, which narrows "why" a great deal even
without a repro.

A tempting but wrong lead was ruled out explicitly rather than left
unaddressed: a stale-mounted-`ConfigMap`-volume theory (kubelet resyncs a
mounted `ConfigMap` only periodically, so a freshly recreated pod could in
principle start against pre-update content). It doesn't survive contact
with this operator's actual shape: `entrypoint.sh` and `cluster.json` are
two keys of the *same* `ConfigMap` object, so a pod can never mount one
fresh and the other stale (they share a `resourceVersion` by construction)
— and even a wholesale-stale mount would just re-run the promoted ordinal
in its own prior, already-healthy role, not produce a crash. Worth stating
plainly in a root-cause writeup ("investigated and ruled out, here's why")
rather than silently dropping it — the next person chasing the same
symptom shouldn't have to re-derive that it doesn't fit.

The general fix pattern for a wait whose own convergence path can
legitimately restart things along the way: a flat timeout can't
distinguish "healthy but slow" from "genuinely stuck," so replace it with
a converge-or-STALL wait — keep waiting while *any* independent,
observable signal is still changing (here: the target pod's own restart
count/ready/phase read straight off the API server, plus the live voter
count when reachable), fail only after a bounded stretch with *no* signal
changing, under a generous hard ceiling so the job still can't hang
forever. This is not "a wider timeout" (which the root `CLAUDE.md`
correctly forbids as a fix for flakiness) — it's a materially different
failure condition (stall, not elapsed-time) that happens to also need a
ceiling. For problem (2)'s half: a polling helper that can lose its
connection must never fold "couldn't connect" into a valid-looking reading
like `0` — return "unknown" (here: empty stdout) and let the caller
self-heal (re-resolve + re-forward) or treat it as "no progress," never as
data.

Also generalizable: a diagnostics dump that captures `kubectl describe
pods`/current-instance logs still can't explain a crash-and-restart cycle,
because a restarted container's *own* prior stdout/stderr is gone from the
live log stream the moment it restarts — only `kubectl logs --previous`
(scoped per-container, keyed off `restartCount > 0`) recovers it. Add that
capture *before* you need it; by the time a flake is being root-caused
after the fact, the crashed instance's own evidence is already gone.
