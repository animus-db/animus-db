# Map a refusal string to a wire error once, at the terminal return, and keep it out of inline retry loops (ADR 0080)

New overload refusals (`busy:`, `no-leader:`) travel as strings on the existing
`ClientResponse::Error` path so they need no `Gate` (class L). Two rules keep that safe:

- **Map in one pure step at the edge's terminal return** (`map_overload`). Mapping earlier
  lets an intermediate layer re-wrap or re-classify it, and a forwarded hop would then see a
  different string than the origin produced.
- **Exclude them from every predicate that decides to retry inline** (`read_should_retry`,
  the confirm loops). A refusal that is retried inline turns load shedding into a spin that
  holds the very permit it was meant to release. A grep for the retry predicates is part of
  adding a refusal.
- A `Busy` that means "never appended" must be a distinct `ProposeResult` variant, not an
  `Err`: proposers treat `Accepted` as "appended locally, confirm later" and need to tell
  the two apart.
