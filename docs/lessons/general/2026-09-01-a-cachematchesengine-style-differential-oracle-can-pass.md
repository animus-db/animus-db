# A `cache_matches_engine`-style differential oracle can pass while every command inside it is silently rejected — assert the materialized state, not just that two sides of a diff agree (copy-split deletion stack, layer 1)

While porting `apply_engine.rs`'s `cache_matches_engine_through_a_mixed_
scenario_and_a_restart` from the deprecated `MetaCommand::BeginSplit` to
`BeginSplitInPlace`, the test's own "split round" turned out to have been a
complete no-op since it was written: `nodes[leader].propose(..)` for the
split silently returns `ApplyOutcome::Rejected` (epoch mismatch — something
earlier in the same scenario bumps the parent tablet's epoch past the
`Epoch::INITIAL` the split command hardcodes as its `expected_epoch`), and
every downstream command in the scenario (a child's own stream seal, a
second split round, the eventual `DropTableTablets`) silently no-ops right
along with it. Every assertion in the test still passed the entire time,
because `assert_cache_matches_engine`'s own invariant — "the apply task's
published cache agrees with an independent rebuild of its own engine" —
holds trivially for a command that never applied: both sides simply stay
unchanged and agree. Confirmed by instrumenting the scenario directly
(`eprintln!`ing `nodes[leader].metadata().tablets` and the propose's own
`ProposeResult` at each step) rather than trusting the green run; a
standalone `Metadata::apply` repro of the same command, minus one
intervening command from the real scenario, proved the command itself was
valid — narrowing the cause to a state difference the live scenario built
up that a hand-written standalone repro didn't.

**The general lesson**: a differential oracle (cache-vs-engine,
delta-applied-vs-full-fetch, shadow-vs-real, etc.) only proves its two
sides never *diverge* — it says nothing about whether either side actually
*changed* the way the test's own comments claim. A long-running scenario
test built from many `propose()` calls with no per-command outcome
assertion is exactly the shape where this goes undetected for a long
time: nothing crashes, nothing diverges, the test just silently stops
exercising what it was written to exercise the moment an early command in
the chain starts getting rejected (here, plausibly from the moment F11
token-alignment or some other apply-gate was added after the test was
originally written, though the actual cause turned out to be an unrelated
stale hardcoded epoch). When a task requires touching a specific `propose`
call in this kind of test, it's worth spot-checking with a debug print
that the command you're touching actually applies — not just that the
suite stays green — before concluding the port is faithful. Found and
noted rather than fixed here (root-causing and correcting the stale-epoch
assumption is an unrelated pre-existing bug, out of scope for a
test-surface-porting layer with no production-code changes); the
NOTE left in `apply_engine.rs` at both split call sites points here.

**Fixed for issue #539**: both `BeginSplitInPlace`/`CutoverSplit` rounds in
`cache_matches_engine_through_a_mixed_scenario_and_a_restart` now read each
tablet's `expected_epoch` fresh off `metadata()` immediately before
proposing (the same just-in-time pattern `animusd::ClientCtx::trigger_
split`'s confirm loop uses) instead of a hardcoded `Epoch::INITIAL`, and
every step now carries a positive assertion of the command's actual
effect (state/epoch/intent after `BeginSplitInPlace`; parent-gone +
children-Active + `split_lineage` after `CutoverSplit`; catalog-row-present/
expired/removed for `SealStreamShard`/`ExpireStreamShards`; empty
`tablets_for_table` after `DropTableTablets`) — a full audit of every
epoch-CAS'd or otherwise legitimately-no-op-capable command in this
scenario, not just the split. **One thing the epoch fix alone didn't
uncover, worth generalizing**: `BeginSplitInPlace`'s apply arm evaluates
its epoch-CAS *before* its F11 token-alignment seatbelt, so the original
stale-epoch proposal's rejection reason ("epoch mismatch") was correct but
incomplete — it silently hid a SECOND, independent defect in the same
scenario (a 1-byte, non-token-aligned split key on a table that already
had a stream enabled) that only surfaced once the epoch was fixed and the
apply arm actually reached the next guard. A chain of sequential guard
checks in one apply arm means fixing whichever one your rejection message
names does not prove the command now succeeds — only a positive
post-propose assertion (not just "no longer rejected for reason X") does.
