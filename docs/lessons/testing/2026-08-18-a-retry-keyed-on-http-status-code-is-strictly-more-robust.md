# A retry keyed on HTTP status code is strictly more robust than one keyed on a message substring, because distinct exhaustion points at the same call site share the status long after they diverge in wording

**A retry keyed on HTTP status code is strictly more robust than one keyed
on a message substring, because distinct exhaustion points at the same
call site share the status long after they diverge in wording** (issue
#287). `streams_e2e.rs`'s `dynamo_retrying` (added by the issue #278 fix)
retries any 500 unconditionally; the cascade-split lineage test's own
`put` closure still asserted `status == 200` in one shot, and PR #294's
retrofit of `dynamo_retrying` across this file's soak tests missed it.
The CI failure this produced wasn't even the documented freeze→cutover
blip `dynamo_retrying`'s own doc comment names — it was a *different*
transient (`"CP kind write did not commit in time"`, a confirm-timeout
under runner starvation) that happens to also surface as a 500, and the
status-keyed retry absorbed it without needing to know its message at
all. **General rule**: when retrofitting a proven retry helper across a
file, grep every remaining single-shot status assert in that file, not
just the one test the CI failure report named — a helper's own doc
comment naming one root cause doesn't mean it's the only one the retry
will end up masking, and that's a feature of status-code keying, not a
bug to narrow away. (`animusd/tests/streams_e2e.rs`.)
