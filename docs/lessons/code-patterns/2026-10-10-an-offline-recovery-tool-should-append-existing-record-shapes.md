# An offline recovery tool should append records the node already understands

Issue #1178 (force-new-configuration, ADR 0077). The recovery rewrites a Raft
survivor's WAL. The cheap and safe shape is to **append existing record types**
(a term-raising `Hard`, then a config-bearing no-op `Append`) rather than edit or
truncate history or invent a "reset" record:

- Nothing to version: ADR 0073's format rules do not trigger, and every decoder,
  `RaftCore::recovered` and the stale-voter fencing treat it as an ordinary
  membership change.
- It is idempotent and reversible: back up first, and a crashed apply leaves the
  original (or a tail that normal recovery cuts back).
- Stale old voters are fenced by the same mechanisms as any removed voter (higher
  term, `Removed` notice), not by new code.

Also: a process with no data-directory lock cannot prove "no node is running
here" from the filesystem. The tool binds the node's own listen address and holds
it; that costs nothing and also stops a node starting mid-rewrite. Keep the
test's restarts real (`sim.stop` + fresh start on the retained engine); a muted
`crash`/`restart` never exercises the recovery path.
