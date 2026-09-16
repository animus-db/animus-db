# Two independently green PRs can merge into a red `main` when one renames what the other calls

**What happened.** PR #942 renamed `RaftKvNode::pending_changes` to
`pending_changes_key_order` and updated every caller. PR #976 (issue #859),
branched before that rename landed, added a new test that called the old
name. Each PR's CI was green on its own head. They merged minutes apart in
one batch; git merged them cleanly because they touched different lines,
and `main`'s next `clippy -D warnings` gate failed to compile the
`snapshot_catchup` test binary (E0599). PR #985 is the one-line fix.

**Why the gates did not catch it.** A PR's CI proves the PR's head, which
is "old base + this change", not "current base + this change". When many
PRs merge in quick succession, every one after the first is tested against
a base that no longer exists. A textual merge conflict at least stops the
merge; a *semantic* one (a rename on one side, a new caller on the other)
sails through. The atomic `gh stack merge` protects a stack's internal
ordering, not the relationship between independent PRs.

**What to do.**

- Before merging a batch, look for renames or signature changes among the
  batch (`git log --oneline main..<branch> --grep rename`, or a PR title
  that says "rename"): every other PR in the batch that touches the same
  crate should have `main` merged into it, and its CI re-run, *after* the
  renaming PR lands, before it is merged itself.
- The cheapest general guard is GitHub's merge queue (or auto-merge on a
  head that was merged forward and re-tested after the base moved), which
  re-runs CI on "current base + this change" for every entry.
- After a batch lands, treat `main`'s own next CI run as the gate of
  record: watch it to completion before declaring the batch done. Green
  PRs are evidence about their heads, not about `main`.
