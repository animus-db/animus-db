# A value a child inherits from a parent that keeps mutating must be frozen at the inheritance event, never derived live from the parent's current state.

**A value a child inherits from a parent that keeps mutating must be
frozen at the inheritance event, never derived live from the parent's
current state.** `Metadata::effective_stream_shard_watermark`/
`stream_shard_parent_id` (ADR 0042 §8/ADR 0043 §A4/§A6) used to walk
`split_parents` to the parent tablet's *current* seal chain on every
call — correct only so long as the parent never sealed again before the
child did. The moment it did, the parent's later (necessarily higher)
end-HLC retroactively became the child's own effective watermark too,
making a pre-split backlog the child had physically inherited in place
(ADR 0043 §A4's shared-storage split design) look already-sealed before
the child ever sealed it itself — a silent, permanent loss, invisible
unless the child happened to seal first (the race that let this ship
undetected through round 3). The fix (PR1) captures the parent's stream
state **once**, at the instant `MetaCommand::SplitTablet` applies, into
a frozen `Metadata::stream_split_basis` entry — a single-hop lookup
thereafter, not a live walk. **Corollary: a test comment that
acknowledges a derivation's time-dependency (e.g. "this assertion must
run before the parent seals again, since X is derived live") is a
signal to fix the derivation, not to order the test around it** — the
pre-fix `stream_lineage_corpus.rs::scenario_split_mid_stream` had
exactly such a comment, naming its own ordering constraint, for months
before this bug was found and its literal inverse
(`split_then_parent_seals_first`) written as the regression.
