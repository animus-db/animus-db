# A green e2e leg that never waits for the state its own fix produces proves nothing — issue #913

The first fix for issue #913 (a node-count-invariant Certificate SAN set
plus a CA-backed issuer for the e2e's own TLS leg) reported a passing
`e2e-kind-tls` run in this same investigation. It was a false positive.

## What happened

The branch's own run (job 104616520700) reached "control group reports 4
voters: converged" and stopped there — the script had no step after that
point which required pod 3's own mTLS to actually work. `4 voters
converged` is reported by `GET /admin/control/members`, answerable by any
of the three **pre-existing** voters (`e2e-0`/`e2e-1`/`e2e-2`) once they
see the membership-change entry commit; it says nothing about whether the
**fourth**, newly-promoted pod can talk to them over the wire this fix was
supposed to repair. Pod logs are only dumped by this script on failure
(`dump_diagnostics`, called from the `ERR` trap and `fail()`), so a script
that never asserts on pod 3's own readiness never even *looks* at pod 3's
logs — the run reported 0 `BadCertificate` lines not because there were
none, but because nothing checked.

A sibling investigation (stacking this fix under PR #909's own additions,
which include `kubectl rollout status statefulset/e2e --timeout=300s`
right after the growth-converged check) reran the identical fix and
caught the real failure: pod 3 stuck `Running`/never `Ready`, log full of
`BadCertificate`, for the whole 300s wait.

## The generalizable lesson

**A test (e2e, integration, or otherwise) that exercises a mechanism but
stops polling before the mechanism's own claimed effect would show up is
not a passing test of that mechanism — it is a test of an earlier,
unrelated mechanism that happens to run first.** Before trusting a green
run as evidence a fix works, ask: *what is the last thing this run
actually waited on, and does reaching that point require the fix's own
effect to have taken hold?* Here, "4 voters" is a Raft **membership**
fact (settled once a majority commits the config-change entry — 3 of 4
already-connected voters suffice) that is provably reachable **without**
the 4th pod's own network connectivity ever working, so it could never
have been evidence the mTLS fix succeeded, no matter how green it
reported. The fix belonged one layer up: waiting for the **StatefulSet
rollout**, or equivalently the 4th pod's own readiness probe, which
*does* require pod 3 to actually serve traffic. When adding a
regression test for "X now works after a change," trace the assertion
back to something that is false if X is broken, not something that was
already true before the change existed.

## Where this showed up

`scripts/e2e-kind.sh`'s S-07d growth phase (`wait_for_progress "control
group reports 4 voters" ...`) reports a real, correct membership fact but
is not sufficient proof the growth's own newly-promoted pod is
network-healthy; `kubectl rollout status statefulset/${AC_NAME} -n
"$NAMESPACE" --timeout=300s`, added right after it, is what actually
closes that gap (originally added on the concurrent issue #864/#909
branch, ported onto this branch in the same change so this leg cannot
report a false pass again).
