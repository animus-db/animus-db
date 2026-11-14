# A status checklist goes stale within weeks; re-verify it with the tree and CI history, not with its own prose

**What happened (2026-11-14).** `docs/production-readiness.md` (the beta exit
checklist) said "Last verified 2026-10-04" and a re-verification a month later
found about 30 stale rows: a runbook directory "absent" that held 27 pages, a
version "0.0.0", signing and SBOM steps "none" that had shipped, a fuzz guard
"fixed by a pending PR" whose fix had already landed (only the guard and its
comment were left behind), and a nightly `kind` leg "unverified" that had
since gone green. Sibling docs (`docs/chaos.md`, `docs/resource-bounds.md`,
`docs/runbook/upgrade.md`, the roadmap's R-01 "Gap" text, two website pages)
repeated the old state.

**Rules.**
- Every landing PR that completes part of a criterion flips its row in the same
  change (the doc already says so); the re-verification found that the rule was
  followed for new files but not for rows whose evidence is "a follow-up PR will
  fix X".
- Verify against the tree (`ls`, `grep` for the named symbol) AND the CI history
  (`gh run list --workflow <wf>`): "has a green run" and "the workflow exists"
  are different claims. A workflow that has never run on its trigger (release
  workflows with no tag) is `Partially met`, not `Met`.
- When a fix lands for a fenced known issue, remove the fence and the
  `known-issues` row in the same PR; a leftover guard keeps the property
  unproven while every doc says it is fixed.
- A link-bearing criterion (alert -> runbook page) is checkable by script;
  check it, and prefer a CI test over a one-off check.
