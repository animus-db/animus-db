# A long foreground gate run can let a sibling stacked branch move underneath you — verify against the base sha you actually rebased onto, not just the branch name (C-11 PR 3, ADR 0061 rung K)

Rebasing `-118` (C-11 PR 3) onto `-117` at its documented tip (`bb801bd4`,
PR 2 landed) and then running the required gates — including a ~22-minute
foreground `cargo test -p animusd --lib sim_cluster` — left a wide enough
window that another process force-pushed `-117` to a new tip (`cbee4b81`)
mid-run (the whole stacked series was being rebased onto a moved `main`,
which brought an unrelated admin seed-latency change in underneath). The tier run itself was unaffected (it
runs against the checked-out worktree, not the branch ref), but the final
verification step (`git diff --stat -117..HEAD`) silently picked up the
moved ref and reported a much larger, wrong-looking diff — files this PR
never touched showing as pure deletions, because `HEAD` (built on the old
`bb801bd4`) simply lacked what the new `-117` tip had added. Diffing a
branch *name* for a "what did my PR change" check is only safe if you can
guarantee nothing else writes to that name for the whole session; on a
shared main tree across a long foreground command, that guarantee doesn't
hold. **Fix/discipline**: record the exact base sha immediately after the
rebase (not just the branch name), and use *that* sha for the "is my diff
what I think it is" check (`git diff --stat <recorded-sha>..HEAD` and
`<recorded-sha>..HEAD -- Cargo.lock`) — it stays correct regardless of
what the branch name points to later. If the branch name's tip really has
moved by the time you're done, that's a separate, real finding to report
to the maintainer (the stack's base changed after you built on it, a
stacked-PR management question), not something to silently "fix" by
re-rebasing a commit whose own file scope was never in question — re-check
first whether the new tip conflicts with or duplicates your own change
before touching anything.
- **A stacked PR whose parent PR has already merged into `main` must be
  retargeted to `main` before it too is merged — `gh-stack merge` does this
  automatically, a hand merge through the GitHub UI does not (2026-09-09,
  PRs #794/#795/#796/#797/#799/#800).** Six PRs (C-10 PR 6-7, C-11 PRs 1-4)
  were each merged this morning into their own stacked *parent branch*,
  after the parent PR (#793) had already landed on `main` — so every merge
  target was a branch that had already stopped feeding `main`. GitHub
  reported all six "Merged," CI stayed green, and each head branch was
  auto-deleted on merge, leaving the six commits reachable only through
  whatever clone still held a remote-tracking ref to the deleted branch.
  The symptom is the same shape as issue #279, one layer up the stack: a
  merge commit for the parent PR sits in `main`'s log, while
  `git merge-base --is-ancestor <child-head-sha> origin/main` returns false
  for every child. **Detection rule**: after any stack merge, don't trust
  the PRs' merged badges — verify every PR head is an ancestor of
  `origin/main` before calling the stack landed. **Recovery**: re-land the
  stranded heads as one flat PR rebased onto the current `main`, from
  whatever ref still holds them.
