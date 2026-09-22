# `gh stack submit` needs GitHub GraphQL, which Claude Code web sessions block — open a stack's PRs over REST with explicit bases and let the maintainer link the stack locally

**`gh stack submit --auto --open` cannot open PRs from a Claude Code web
session** (2026-09-22, ADR 0072 stack). The extension pushes the branches
fine, then queries each branch's PR state over GraphQL, and the session's
GitHub proxy answers `403 GitHub GraphQL is not available from Claude Code
sessions; use the REST API`. `submit` reports "Pushed and synced 4
branches" with a warning per branch and creates nothing; `gh stack view
--json` then shows every branch with no `pr` entry. `gh stack init
<b1> <b2> ...` (adopting already-rebased branches), `gh stack view --json`
and `gh stack push` all work, because they are local or REST-only.

**What to do instead:** after `gh stack init` has adopted the chain and
the branches are pushed, open each PR through the REST-backed GitHub tool
(`create_pull_request`) with the base set explicitly to the branch below
it (`main` for the bottom), ready for review, and say in the PR bodies
which layer of the stack each one is. The branches are a proper linear
chain, so a maintainer running `gh stack checkout <pr>` locally adopts
the same stack and can still `gh stack merge <top> --yes` atomically;
nothing is lost except the GitHub-side "Stack" linkage on the PR pages.
Don't loop retrying `submit` or look for a flag — the block is on the
transport, not the command.
