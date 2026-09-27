# A merge-base-diffing CI guard needs full history and a grep, not a git pathspec glob, or it silently no-ops

Building `scripts/check-format-fixtures.sh` (ADR 0073 Phase 0's append-only
fixtures guard) hit two independent ways a "diff against `origin/main`"
script can pass every test locally and then do nothing at all in CI:

1. **`git ls-tree`/`git diff -- '**/some/dir/**'` doesn't do what it looks
   like it does.** Git's pathspec wildcard matching does not cross `/` by
   default — `**` needs the `:(glob)` pathspec magic to match across
   directory levels, and without it the pattern silently matches *nothing*
   rather than erroring. A script that filters `git ls-tree`/`git diff`
   output with a bare `'**/tests/fixtures/formats/**'` pathspec looks
   correct, runs without error, and returns an empty result on every input
   — which for a guard whose "nothing to check" case is also a valid,
   printed outcome is very easy to mistake for "correctly found no
   fixtures" instead of "the filter matched nothing, ever". The fix: filter
   with a plain `grep -E` on the path strings from an unfiltered `git
   ls-tree`/`git diff`, not a git pathspec, when the match needs to cross
   directory levels — a substring/regex match is unambiguous where pathspec
   glob semantics are not.
2. **`actions/checkout`'s default `fetch-depth: 1` makes `git merge-base
   HEAD origin/main` (or any ref your script needs) silently fail to
   resolve.** A guard that computes a merge base and, on failure to resolve
   it, degrades to "skip this check" (the right behavior for a genuinely
   ref-less environment, e.g. a fresh scratch repo) will *always* take that
   skip path in ordinary CI, because the shallow clone never fetched
   `origin/main`'s history in the first place — same failure shape as
   above: no error, just quietly does nothing. `dco.yml` in this repo
   already carries this fix (`fetch-depth: 0`) for its own `merge-base`
   need; a new merge-base-based guard job's checkout step needs the same,
   named in a comment so a later "let's speed up checkout" pass doesn't
   remove it without knowing why it's there.

**General form:** any CI guard whose core logic is "diff against a base
ref, filtered to a subset of paths" has two failure points that both look
identical from the outside (an unconditional pass) and neither errors:
the path filter and the base-ref resolution. Test such a script against a
*local simulated history* that has the target condition in it (a fixture
already merged, then modified in a later commit) — not just against the
literal current worktree, which usually has no meaningful merge-base
history to exercise the diff logic against at all. A temporary
`git update-ref refs/remotes/origin/main <sha>` (restored after the test)
is a safe, local-only way to simulate "already on `origin/main`" without
touching the real remote.
