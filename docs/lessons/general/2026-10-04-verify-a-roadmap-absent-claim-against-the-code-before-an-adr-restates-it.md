# Verify a roadmap "absent" claim against the code before an ADR or checklist restates it

- **Context:** writing ADR 0074 / `docs/production-readiness.md` from
  roadmap R-01's list of things "checked absent".
- **What happened:** most claims held (no `fuzz/`, `CHANGELOG`,
  `SECURITY.md`, runbook, signing; version `0.0.0`), but re-grepping
  found the roadmap's overload story was incomplete or wrong in three
  ways: the uncapped spot is the accept-and-spawn loop in
  `crates/animusd/src/dynamo.rs` (not just `animus-node/src/http.rs`),
  `ThrottlingException` is not emitted anywhere while `ServiceUnavailable`
  (503) already is, and `image.yml` publishes single-arch images, so a
  "multi-arch builds" row I had drafted as Met was false.
- **Why it matters:** a readiness checklist whose `Met` rows were never
  checked is worse than none. A row is `Met` only when its evidence path
  exists and was just verified; for an error-code decision, grep what
  the code already emits before naming codes.
- **Rule:** before stating a status in a criteria doc, `ls`/`grep` the
  evidence path in the same session, and re-derive "absent" claims
  rather than copying them.
