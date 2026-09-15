# Do not override git author/committer to a human identity in a web session (2026-08-23).

**Do not override git author/committer to a human identity in a web
session (2026-08-23).** This container's commit-signing hook requires the
committer to be the pre-configured `Claude <noreply@anthropic.com>`
identity; a subagent that sets `user.name`/`user.email` (or `git commit
--author=...`) per-commit to a maintainer's name produces a commit the push
gate flags **Unverified**, forcing the orchestrator to rewrite it before it
can land. Let the configured identity stand and keep `git commit -s`
sign-offs matching it — signing off as someone else is exactly the mismatch
the hook exists to catch.
