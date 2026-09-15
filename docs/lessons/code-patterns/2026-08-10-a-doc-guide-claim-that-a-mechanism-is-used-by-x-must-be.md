# A doc/guide claim that a mechanism is "used by X" must be verified against *call sites*, not symbol existence — a superseded mechanism often survives as a compilable, even unit-tested, vestige.

**A doc/guide claim that a mechanism is "used by X" must be verified against
*call sites*, not symbol existence — a superseded mechanism often survives as
a compilable, even unit-tested, vestige.** During the 2026-07 CLAUDE.md
restructure audit, `Coresident::sibling` + the listener pool,
`ProdEnv::shutdown_tasks`, and Accord's `ShardedOwner`/`ShardRouter` were all
documented (in three different guides) as live, load-bearing plumbing; every
symbol still existed and compiled, but a `grep` for *callers* found zero —
each had been superseded (streams, ADR 0026) or trimmed (ADR 0018/0019)
without its guide entry being updated. This is the dual of the existing
"before implementing a documented gap, grep the code — the mechanism may
already exist" rule: before trusting a doc's "X uses this," grep for the
consumers, not the definition. A doc-staleness audit is cheapest done per
mechanism ("who calls this?") rather than per claim.
