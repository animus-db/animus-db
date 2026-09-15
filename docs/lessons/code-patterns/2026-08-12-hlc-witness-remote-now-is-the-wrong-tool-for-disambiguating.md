# `Hlc::witness(remote, now)` is the wrong tool for disambiguating a value that is *deliberately* far in the future relative to the clock's own normal progression — witnessing doesn't just validate the value, it adopts it as the clock's new baseline, poisoning every ordinary `mint` that follows until real wall-clock time catches up.

**`Hlc::witness(remote, now)` is the wrong tool for disambiguating a
value that is *deliberately* far in the future relative to the clock's
own normal progression — witnessing doesn't just validate the value, it
adopts it as the clock's new baseline, poisoning every ordinary `mint`
that follows until real wall-clock time catches up.** ADR 0018 §2/PR2b's
logged-read-ceiling design proposes a ceiling candidate
`uncertainty_upper(ts) = ts.wall_ms + max_offset` (deliberately ~500ms
ahead, so ceiling proposals amortize across many reads instead of firing
per-read) — but two `ensure_ceiling_above` calls that happen to compute
the *same* millisecond-granular margin (`uncertainty_upper` collapses
`logical` to 0) would otherwise propose byte-identical `ReadCeiling`
entries, tripping the apply-time monotonicity assert every command must
satisfy. The obvious fix — `self.hlc.witness(margin, now)` to
disambiguate, since `witness`'s contract guarantees the result strictly
exceeds both the margin and everything previously minted/witnessed — is
actually a *worse* bug: witnessing a 500ms-future value drags the
group's own `Hlc` forward to match it, so the very next *ordinary* read
mints a `ts` already close to that inflated baseline, immediately
exceeding the ceiling just committed and forcing a fresh proposal —
turning an intended O(1)-amortized mechanism into O(N) (one proposal per
read), caught by a test that specifically drove many sequential reads
and counted proposals rather than just checking correctness. **General
rule: reach for `witness` only when the goal is genuinely "fold this
observed value into my notion of *now*"; when the goal is merely "make
this candidate value unique against others like it" without changing
what the clock reports for anything else, use a separate ratchet (a
small CAS loop over its own counter) instead — sharing the *same* clock
used for the rest of the system's ordinary time-keeping is exactly what
causes the leak.** (`RaftKvNode::next_ceiling_candidate`,
`crates/animus-cp-data/src/lib.rs`; regression in
`tests/ts_cache.rs`'s amortization test.)
