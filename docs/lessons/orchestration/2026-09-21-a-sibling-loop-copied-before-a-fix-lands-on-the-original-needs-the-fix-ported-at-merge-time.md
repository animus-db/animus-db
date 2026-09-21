# A sibling loop copied before a fix lands on the original needs the fix ported at merge time

**What happened.** Issue #996 layer 2 added `write_path.rs::
cp_kind_write_batch`, the batched sibling of `cp_kind_write_item`, by
copying that function's `cp_route`/`cp_forward` retry loop almost verbatim.
Issue #994, developed and merged the same day, changed
`cp_kind_write_item` in two ways: a post-sleep `now >= deadline` re-check
(so the loop's last iteration can't sleep past its budget and overwrite a
real, informative refusal with a generic budget-exhausted one), and a
terminal-return mapping of a still-`decide::read_should_retry`-satisfying
error to `WireError::service_unavailable` (HTTP 503, DynamoDB's own
`ServiceUnavailable`) instead of letting it fall through as a bare 500
`InternalServerError`. `cp_kind_write_batch` was branched from, or written
against, the *pre-#994* shape of `cp_kind_write_item` and both PRs' own CI
was green — each proved its own head. The two branches merged into `main`
minutes apart (PR #1017 then PR #1018), and nothing caught that the newer
copy never received the older fix: no compiler error, no test failure —
`cp_kind_write_batch` was a semantically incomplete copy, not a broken one,
so it built and its own tests (which never exercised an exhausted-budget
freeze) passed. The gap sat on `main` until issue #996's regression task
found it by re-reading both functions side by side.

**Why the gates did not catch it.** This is the same shape as the
rename/caller split documented in
`docs/lessons/orchestration/2026-09-16-two-green-prs-can-merge-into-a-red-main-when-one-renames-what-the-other-calls.md`,
but subtler: that case was a compile error (E0599), loud and immediate.
Here both functions independently compile and independently pass their own
tests forever — the defect is a missing behavior, not a missing symbol, so
nothing short of a test that actually drives the fixed condition (an
exhausted retry budget on a transient, `"; retry"`-suffixed refusal) will
ever fail. A code-only diff review of either PR in isolation also does not
surface it: `cp_kind_write_batch`'s diff looks complete against the
*version of `cp_kind_write_item` it was copied from*, and `cp_kind_write_item`'s
own diff (issue #994) never mentions the batch sibling, because it did not
exist on that branch.

**The obvious two-line port was not the whole gap — the copy had already
drifted structurally, not just chronologically.** Porting #994's two literal
changes (the post-sleep deadline re-check; the terminal
`read_should_retry` → `service_unavailable` mapping) into
`cp_kind_write_batch`'s own `cp_route`/`cp_forward` loop compiled, looked
complete, and was still not enough to turn the regression test green.
The reason: `cp_kind_write_item`'s `Local` arm captures a leader-side
failure as `Err(e) => e` and lets it fall through to the loop's own
`err`-based retry/mapping logic; `cp_kind_write_batch`'s `Local` **and**
`Forward` arms instead `return` `kind_write_batch_at_leader`'s whole
per-item result set immediately, on success *or* failure — so the loop's
`err` match is only ever reached by a bare routing failure
(`CpRoute::None`, an actual `ClientResponse::Error`), never by the
realistic case (a frozen/superseded whole-entry propose failure). The
*true* terminal-return site for that case turned out to be
`kind_write_batch_at_leader` itself (`dynamo.rs`), which needed the
identical mapping applied to its own `Err(e)` arm — and a THIRD site,
`wire_error_from_batch_rejected` (the closed-set allowlist deciding which
`WireError` codes survive a forwarded `KindWriteBatch` hop's per-item
reply), needed a `"ServiceUnavailable"` entry it never had, mirroring the
entry `decode_relayed_error`'s own allowlist got when #994 first shipped.
A test written against only the literally-named defect ("port these two
lines") would have gone green without exercising the real scenario at
all — it took a regression test built from the actual DynamoDB request
shape (`BatchWriteItem` against a genuinely frozen tablet, from both a
leader and a non-leader node) to expose that the fix had to land at
three sites, two of which weren't in the original bug report.

**What to do.**

- When a new function is deliberately written as "the batched/plural
  sibling of X" (or copied from X as a starting point), treat X's own
  in-flight or recently-landed fixes as a checklist to re-apply, not just
  its shape at branch time. Search history/PRs against X for fixes that
  landed after the copy was taken, not just before.
- A comment on the sibling that says "mirrors X's own retry loop" is a
  standing invitation to diff the two functions line-by-line whenever
  either one changes — do that diff explicitly as part of reviewing any
  follow-up PR to either function, not just at copy time.
- The regression test for a "loop exhausts its budget on a transient
  refusal" fix needs one instance per entry point that has its own retry
  loop, not one instance per *mechanism* — a shared root cause does not
  imply a shared test surface once the code has been duplicated (by copy,
  not by a shared helper).
- Trust the regression test over the bug report's own diagnosis of *where*
  the fix goes. A ticket that names specific line numbers is describing
  one plausible mechanism, not a verified one; if the literal port doesn't
  turn the test green, the next step is to trace the real data flow (here:
  which arm of the caller actually captures an `Err`, and whether a value
  crossing a wire hop passes through a closed-set allowlist) rather than
  assume the test is wrong. A test built from the real external request
  shape (an actual `BatchWriteItem`, an actual frozen tablet, a real
  non-leader hop) will find every site in the chain; a test built to match
  the ticket's own described fix would only confirm the ticket's own
  incomplete diagnosis.
- Where feasible, prefer factoring the shared retry-loop shape into one
  function both callers use, rather than two independently-maintained
  copies — this exact class of drift is why `docs/engineering-lessons.md`
  and this repo's own "when adding a variant, grep every gating match
  site" convention exist; a copied loop is not a gating match site a grep
  finds by name, so it is easy to miss entirely.
