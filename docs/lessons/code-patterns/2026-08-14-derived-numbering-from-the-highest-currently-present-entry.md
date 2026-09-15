# Derived numbering from "the highest currently-present entry" is only safe for an append-only collection — the moment anything in the system starts physically *removing* old entries, that derivation can silently collide.

**Derived numbering from "the highest currently-present entry" is only
safe for an append-only collection — the moment anything in the system
starts physically *removing* old entries, that derivation can silently
collide.** ADR 0042/0043's stream-shard epoch (`seal_now`'s `next_epoch`,
`dynamo_streams::current_open_epoch`) was designed as "chain length,"
computed fresh each time from `stream_shards.range(..).next_back()` —
correct for two full rounds of PRs (4/5/6) because nothing ever removed
a row yet. The instant round-3 PR7 added retention (the *first* code
path that physically deletes a `stream_shards` entry), this became a
live hazard: reclaiming a tablet's own highest-epoch row would make the
very next seal recompute the *identical* epoch for genuinely different
data — two objects claiming the same identity at different points in
time, with nothing to tell them apart. The fix is a narrow, explicit
guard at the one call site that removes rows (`segment_janitor.rs`'s
`may_remove_row`: never remove a tablet's current max epoch while the
tablet still exists), not a redesign of the numbering scheme — but the
general lesson is the one to carry forward: **whenever a later PR adds
the first deletion/reclaim path over a collection some earlier, already-
shipped code derives an identity or ordering from via "count/max/last of
what currently exists," go back and re-audit every such derivation** —
the earlier code was correct when written, and the later PR's own review
has no reason to re-examine code it never touches, which is exactly how
this class of bug survives review. Grep for `.next_back()`/`.count()`/
`.len()` over the same collection a new deletion path touches as a
starting point. (`crates/animusd/src/segment_janitor.rs`, ADR 0043 §A9,
round-3 PR7, 2026-08-14.)
