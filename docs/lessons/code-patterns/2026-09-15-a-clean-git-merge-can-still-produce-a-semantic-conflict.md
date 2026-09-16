# A clean git merge can still produce a semantic conflict a build catches and a diff review misses

Merging `claude/issue-811-apply-loop-livelock` (PR #905, based on an
older point of `main`) into a branch already carrying `main`'s later
`meta_apply_and_compact` signature change (PR #901/issue #898, which
added two trailing parameters) produced a clean, non-conflicting 3-way
merge — git auto-merged `crates/animus-control/src/node.rs` with no
`<<<<<<<` markers. The result still didn't compile:
`meta_apply_and_compact`'s definition (from `main`'s side) kept its
13-parameter signature, but PR #905's own new regression test — written
against the *old* 11-parameter signature its own branch base predates —
called it with only 11 arguments. The two branches' diffs never touched
overlapping lines (one added parameters to the function definition, the
other added a whole new test function elsewhere), so git had nothing
textual to flag, even though the combined result was inconsistent.

**The general lesson**: a clean (no-conflict-marker) merge across two
branches with different bases is not proof the result compiles or is
semantically consistent, whenever either side's diff touches a function
whose *signature* the other side also changed — a call site added on one
side, written against that side's own (older or newer) signature, can
silently disagree with the definition merged in from the other side. Git
merges text, not APIs. The only reliable check is building (and, for a
`#[cfg(test)]` module buried in a `src/*.rs` file rather than
`tests/*.rs`, building with `--all-targets`, since a bare `cargo build`
doesn't compile in-crate test modules) immediately after any merge that
touches a function signature on either side — never trust "no conflict
markers" as sufficient by itself. This generalizes past a single fix:
whenever pulling in a second scratch/sibling branch to satisfy a gate
(as opposed to your own branch's own linear commit history), rebuild
with the widest target set before trusting the merge.
