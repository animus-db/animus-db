# Resolve-the-leader-or-refuse lives in one helper

`ClientCtx::leader_for` (`animusd/src/forwarding.rs`) is the one place a
forwarded-CP arm turns a tablet into its local leader or the exact
`not_leader_refusal(tablet)`. A new `cp_serve_forwarded` arm calls it instead of
re-spelling `tablet.and_then(|t| self.edge.cp_leader(t))`; the `tablet` it
returns is a plain `TabletId`, so no `None` arm is needed after it.

Gotchas:

- `ClientResponse` is over 600 bytes, so a `Result<_, ClientResponse>` trips
  clippy `result_large_err`; the helper returns `Result<_, Box<ClientResponse>>`.
- Do not bulk-rewrite match arms with a non-greedy multi-line regex: one
  `match ... { Ok(()) => PutOk ...}` pattern silently swallowed the next arm's
  body. Review the diff for every regex-driven edit.
- The eventual-read (`stale: true`) arms are deliberately not folded into the
  helper: they never need a leader, they check the replica's scope range, and
  `Get` scopes by key while the scans scope by range.
