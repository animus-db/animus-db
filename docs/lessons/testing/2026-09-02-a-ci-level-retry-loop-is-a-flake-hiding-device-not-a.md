# A CI-level retry loop is a flake-hiding device, not a contention mitigation — remove it once the actual contention fix lands (2026-09-02, the ci-speed stack).

**A CI-level retry loop is a flake-hiding device, not a contention
mitigation — remove it once the actual contention fix lands (2026-09-02,
the ci-speed stack).** `.github/workflows/ci.yml`'s `prod-liveness` job
wrapped its two test steps in a bounded 2-attempt shell loop, justified as
absorbing transient runner-class contention on the shared 2-vCPU box
(issues #280/#286). Measured over 7 runs in one day, that loop's first
attempt failed 3 times — and every one of those 3 was a real, since-fixed
bug (issues #555, #559, #561: a race-outcome assertion that only checked
one of three possible resolutions, a converged-or-timeout poll against one
fixed replica that doesn't prove the *group* converged, and its sibling
cross-replica flake), not contention. The retry's second attempt passed
every time, so the run still reported green, and nobody looked at any of
the three until this stack's own measurement pass counted first-attempt
failures on purpose. **This is CLAUDE.md's Session operating mode item 4
in miniature**: "green is an invariant, a flaky test is a real bug, don't
retry until green" is written as a rule for humans and agents driving a
merge by hand, but a shell loop in the workflow file is the exact same
bypass, automated and silent — it retries until green just as much as a
person clicking "re-run failed jobs" would, except nobody has to notice
it happening. Removed outright (`ci-speed-3-no-retry` in the stack): a
first-attempt failure in `prod-liveness` is red, full stop, same as
`gates`. The actual contention problem the retry was a stopgap for gets
its real fix in the same stack (`ci-speed-2-nextest-shards`): sharding
the tier across parallel runners so each real-thread test gets a runner
with nothing else contending for its 2 vCPUs, instead of papering over
contention by re-running the whole binary and hoping. **General rule**:
a retry that reruns a whole test (or a whole binary, or a whole job)
cannot distinguish "this failed because of transient environmental
contention" from "this failed because of a real, non-deterministic bug in
the code or the test" — both look identical from outside the retry loop,
which is exactly why a retry can never be the fix for either one. If
contention is real, fix the contention (more isolation, fewer concurrent
neighbors, a dedicated runner); if a test is flaky, root-cause it. A
retry loop sitting in front of either problem just moves the evidence
outside the window anyone is looking through.
