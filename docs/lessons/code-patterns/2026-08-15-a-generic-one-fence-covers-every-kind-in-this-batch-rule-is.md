# A generic "one fence covers every kind in this batch" rule is only sound for keys whose byte value actually falls inside the tablet's own live range — a kind-scoped *bookkeeping* key (not real user data) can silently violate that assumption even though it structurally belongs to this tablet.

**A generic "one fence covers every kind in this batch" rule is only sound
for keys whose byte value actually falls inside the tablet's own live
range — a kind-scoped *bookkeeping* key (not real user data) can silently
violate that assumption even though it structurally belongs to this
tablet.** ADR 0045 §2's backfill-seeder cursor advance
(`ctx.cp_kind_write_raw`, whose fence is always `leader.scope_range()`,
the tablet's *current live* range) was rejected as "outside this group's
live range" on *every* real split, at every seed, with no fault injection
needed — found by `animus-test/tests/backfill_fault_corpus.rs`'s very
first run of its split cells (`concurrent_split_during_backfill`/
`split_after_tablet_already_reported_done`), which hung forever ("backfill
sweep never reached its end after 10,000 ticks") rather than failing
loudly, because the *data* seed writes (keyed by real base keys, which do
satisfy the fence) kept silently succeeding while only the *cursor's own
persistence* failed — restarting the sweep from scratch every tick instead
of resuming, masked in every prior test by tables small enough that one
tick's `BACKFILL_SEED_BATCH` (256) covers a whole side in one pass
regardless. Root cause: `cursor::cursor_key` truncates its `range_start`
argument to a bare `TOKEN_BYTES`-wide token (`cursor::token_of`) — sound
for the key's own *disjointness* proof (never collides with a real
client key) but **not** for range-*containment*: a split's own
`split_key` is essentially never token-aligned (chosen from real row
content, never the hash ring), so the truncated cursor key sorts *below*
the child's own (longer) `range.start` the instant the byte right after
the token is non-zero — true of `escape(pk)`'s leading byte for
essentially any real partition key. Fixed by giving the cursor's own
advance write a dedicated path (`advance_backfill_cursor`, a direct
`group.put_kind_batch_fenced(.., KeyRange::whole())`, bypassing
`cp_kind_write_raw`'s auto-derived narrow fence entirely) — a cursor
row's identity is already fully captured by its own token (disjoint from
base data by row-kind, ADR 0041 §3) and immutable across a narrowing, so
it needs no range-fencing at all, the same reasoning `seal.rs`/
`ceiling.rs`'s engine-global markers already rely on for a *different*
flavor of range-independent bookkeeping key. **General check: before
reusing a "fence every key in this batch against the tablet's live
range" primitive for a *new* kind of key, ask whether that key's own byte
value is guaranteed to satisfy `range.contains` — a bookkeeping key
derived from a truncated/hashed/otherwise-transformed version of the
range boundary is not automatically safe just because it lives in a
disjoint row-kind scope.** Also worth naming: the pre-existing `"gsi"`
cursor tag has the *identical* underlying gap (its own cursor write goes
through the same `cp_kind_write_raw` path) but it is harmless there only
because that caller already tolerates a perpetually-`None` cursor as "just
reconcile everything, always correct" — a latent bug can be real for one
caller and merely a masked inefficiency for another, so "it works today"
is not evidence a shared primitive is fencing correctly for a new use.
(`crates/animusd/src/index_drain.rs::advance_backfill_cursor`; ADR 0045
§2/§3, 2026-08-15.)
