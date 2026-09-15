# A CI check whose failure mode is "intermittent, unresolved root cause" doesn't need a diagnosis before it can be fixed — remove the risky mechanism instead (issue #466, `.github/workflows/dco.yml`)

`tim-actions/get-pr-commits` + `tim-actions/dco` intermittently died with
`Argument list too long` (`E2BIG`) on some PRs. The issue's own first theory
— "our commit messages are unusually large, and that's what crosses
`ARG_MAX`" — was directly disproved by an A/B comparison: a 37-commit/42KB
PR failed twice while a 63-commit/73KB PR (~74% more payload) passed on the
same runner fleet minutes apart. The true trigger inside the third-party
action was never identified, and didn't need to be: the fix wasn't "explain
the flake," it was "stop doing the thing that has this failure mode at
all." Both actions round-trip every commit message through argv/env
(`get-pr-commits`'s JSON output becomes `dco`'s `commits:` input, which the
action re-serializes onto a command line internally) — replaced with an
in-workflow script that walks `git rev-list --no-merges
<merge-base>..<head>` and checks `git log -1
--format='%(trailers:key=Signed-off-by,valueonly)' <sha>` per commit,
capturing trailer content only into a shell variable that's compared/
printed, never re-interpolated into another command or eval'd — commit
SHAs (fixed-width hex from `rev-list`, never derived from message content)
are the only thing that reaches a command line. **General lesson: when a CI
failure is real, reproducible, but resists root-causing (especially inside
a third-party action's own internals), don't burn more budget chasing the
exact trigger before fixing it — if the failure mode has a known shape
(here: "large untrusted content on argv/env can exceed `ARG_MAX`"), a
local reimplementation that structurally can't hit that shape closes the
bug regardless of which precise condition was tripping it, and is usually
smaller than bisecting a dependency's black box.** Separately: computing the
PR's commit range via `git merge-base "$BASE_SHA" "$HEAD_SHA"` (both
supplied by the `pull_request` event, `head.sha`/`base.sha`) rather than
diffing against the base branch tip works identically for same-repo and
fork PRs, because GitHub mirrors a fork PR's commits into the base repo's
own ref namespace — a plain `actions/checkout` with `fetch-depth: 0` and an
explicit `ref: ${{ github.event.pull_request.head.sha }}` (not the default
merge-ref checkout) is enough to fetch them all from `origin`.
