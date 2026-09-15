# A validation/transformation rule enforced at only SOME of several call sites that all build the same command is a gap, not redundancy — move it to the one function every caller actually funnels through.

**A validation/transformation rule enforced at only SOME of several call
sites that all build the same command is a gap, not redundancy — move it
to the one function every caller actually funnels through.** F11 (ADR
0042 §14, a streamed table's split key must round down to its own token
boundary) was implemented inside `auto_split_loop` only; the two manual
paths (`POST /admin/tablet/split`, `ClientRequest::SplitTablet`) both
called `ClientCtx::trigger_split` directly with the caller's raw key,
bypassing the rounding entirely — a manual split on a hot partition could
silently separate one token's own records across two **sibling** tablets
with no parent/child relation, the exact per-item ordering violation the
rule exists to prevent. The fix (growth PR2) generalizes: **grep every
call site that builds the guarded command** (here, every caller of
`trigger_split` — there were exactly three), confirm they all fall through
one shared function, and move the rule INTO that function rather than
duplicating it at each site (which just recreates the same gap the next
time a fourth caller appears). Add the apply-time seatbelt (the ADR 0028
fence idiom) as defense in depth, not as the primary fix — a structural
check at apply guards against a *future* bypass of the choke point, but a
caller that reaches apply through the guarded rule was never the bug.
Corollary the fix also had to handle: rounding a key can produce a
**degenerate** result (here, collapsing onto the tablet's own
`range.start` when a single hot token owns the whole tablet) that the
underlying command legitimately rejects — decide up front whether that's
an error (a manual, explicit caller should hear about it) or an expected,
metered no-op (a periodic background trigger retrying forever should
neither spam a warning nor silently loop with no signal at all); conflating
the two callers' needs into one behavior gets one of them wrong.
(2026-08-16, `growth/1-f11-fence`.)
