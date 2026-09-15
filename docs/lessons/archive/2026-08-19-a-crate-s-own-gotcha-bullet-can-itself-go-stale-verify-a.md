# A crate's own gotcha bullet can itself go stale — verify a "the log index is the version" premise against the primary source before trusting it enough to build on (2026-08-19).

**A crate's own gotcha bullet can itself go stale — verify a "the log
index is the version" premise against the primary source before trusting
it enough to build on (2026-08-19).** Tasked with dropping the split
driver's version-floor pre-pass scan (`index_drain.rs`) in favor of an
O(1) `group.engine_applied_index()` read, on the stated premise "CP
writes need no client-assigned version — the Raft log index *is* the
MVCC version" (verbatim, then-current text of this crate's own
`CLAUDE.md`). That premise was true once but had been superseded over a
year earlier: ADR 0018 §2/PR2 (2026-08-11) retired the Raft-index MVCC
encoding and replaced it with a packed HLC commit timestamp
(`hlc::pack(ts) = wall_ms << 20 | logical`) — a completely different
value space from a Raft log index (wall-clock milliseconds vs. an entry
count), so the proposed substitution was unsound in both directions:
under real workloads it would under-filter back to the exact unfiltered-
final-image regression a prior rung of the same ADR had fixed, buying
nothing for a known cost. (A first draft of this entry also claimed it
could *over-filter* under `SimEnv`; that was withdrawn on review —
`animusd` has no `animus-sim` dependency, so this driver never runs
under a simulated clock. Reviewing your own supporting arguments as
hard as the conclusion is part of the lesson: the conclusion held, one
of its three legs did not.) The tell was
in the *type* the target field held (`ts: HlcTimestamp` on every
`KvCommand` variant, `KvCommand`'s own doc comment naming `hlc::pack` as
"the engine's MVCC version at apply") — one grep away from the code the
premise was supposedly about, and a mismatch the crate's own summary
bullet had quietly drifted away from. **Rule:** before implementing an
optimization whose soundness rests on an invariant stated only in a
summary doc (a `CLAUDE.md` gotcha bullet, a one-line ADR recap), grep the
actual type/field the invariant is about and read its own doc comment —
the summary is a pointer, not the source, and it can lag the code by
exactly as long as nobody happened to need that bullet to be right. This
generalizes the existing "before implementing a 'close this documented
gap' task, grep the code" rule (root `CLAUDE.md`) to invariants, not just
missing-feature claims. Found and corrected in the same change that
fixed the stale bullet (`crates/animusd/CLAUDE.md`'s "CP writes need no
client-assigned version" entry) and recorded the rejected optimization
(ADR 0050's 2026-08-19 "investigated and rejected" amendment) so it
isn't re-attempted on the same false premise.
