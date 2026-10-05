# A cross-version job that needs an external binary must fail, not skip, when it is missing

The previous-release rolling-upgrade test (ADR 0073 Phase 3, D10) needs R-1's real
`animusd`. The usual pattern for an env-gated test ("skip loudly when
`ANIMUS_..._ENDPOINT` is unset") would let a CI job that forgot to build or cache
the binary go green having tested nothing, which is exactly the failure the job
exists to prevent (cf. the S3 real-endpoint job's `..._REQUIRE_ENDPOINT`).
Make the test target itself opt-in (`required-features`, so a plain
`cargo test`/the sharded `prod-liveness` tier never selects it) and make the
feature-on path **panic** on a missing binary, with a message naming the script
that builds it; have the CI job also assert `--list` shows every expected test.
Locally you lose nothing: the one-line command builds the reference and runs it.

Related, learned building it: (1) a measurement that depends on a time threshold
(placement repair's 5 s dwell) measures nothing if the harness restarts a node
faster than the threshold; the run must hold the node down past it, and say so in
the report. (2) After a workload "stops", a write the client gave up on can still
commit; a before/after-restart equality check is therefore wrong, the right check
is "everything seen before is still there, in order, and anything new was really
issued". (3) A supervisor CLI's `plan` that is computed from *recorded* node
ranges cannot be re-asked mid-roll over binaries that predate the range
reporting; take the plan once and follow it.

What the first real run found (2026-10-05, reference `ac57d56a`): a real previous
release binary imports **its own known bugs** into the job (the Phase 1 build still
had the abort-tombstone defect `efcaa6cb` fixed a day later), and it exposes formats
the sim profiles cannot: a format change shipped after the reference (`txn-envelope`
v2, intent tag 2) was written ungated, and an old replica that receives an engine
image from an upgraded node (repair snapshot) panics on it. Therefore (a) keep a
`same-binary` control (`ANIMUS_UPGRADE_FROM_CONTROL`) so a failure can be classified
as a mixed-version defect or an R-1/restart defect in one extra run, and (b) treat a
format that crosses nodes inside an engine image or snapshot as replicated (needing
a gate), not as a node-local file format, even though its bytes live in the local
engine.
