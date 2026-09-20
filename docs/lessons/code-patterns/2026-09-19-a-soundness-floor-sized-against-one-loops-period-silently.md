# A soundness floor sized against one loop's own period silently under-covers every other loop that later adopts the same skip — derive the floor as the max over all consumers, and name each one in its doc (#302 → #992)

`animusd::MIN_QUIESCE_AFTER` exists to enforce a single load-bearing
invariant: `--quiesce-after` must be long enough that a periodic
background sweeper is guaranteed to observe a tablet while it is genuinely
awake at least once before it can re-quiesce. Issue #302 (2026-08-19)
found and fixed the first instance of this — `change_consumer_loop`'s own
quiesce-veto freshness argument needs at least one sweep, at its own
`INDEX_DRAIN_INTERVAL` (200ms) cadence, before a group first quiesces — and
sized `MIN_QUIESCE_AFTER` to exactly that one loop's period.

By the time issue #992 was filed, a *second* loop had independently
adopted the identical "skip a quiesced tablet outright" optimization:
`auto_split_loop`'s bytes/change-rate/ops-rate triggers, at its own,
*ten-times-longer* `AUTO_SPLIT_INTERVAL` (2s) cadence. Nothing forced
`MIN_QUIESCE_AFTER` to widen to cover it — the constant's own name and doc
described "the change-consumer sweep interval" specifically, not "every
loop that skips a quiesced tablet." A `--quiesce-after` value comfortably
above the first loop's floor (anywhere from 200ms to 2s — in practice, on
this codebase's whole-seconds CLI, exactly the single value `1`) passed
validation cleanly while sitting below the second loop's own requirement,
silently reopening the identical class of bug #302 had just closed one
loop earlier: a bursty tablet's threshold crossing landing, and being
missed, in the gap between two widely-spaced sweeps.

A 2026-09-08 investigation (this log's own "speeding up a confirm loop can
expose a latent race" entry) had already found and precisely characterized
this exact mechanism — a test fixture's `quiesce_after` (300ms) sitting
below `AUTO_SPLIT_INTERVAL` (2s) let a completed write burst hide from
`auto_split_loop` forever — but fixed only the one fixture that tripped
over it, leaving the underlying, un-enforced coupling in place for every
other caller, including production's own CLI.

**The general form**: when a correctness floor exists because "loop X's
own period bounds how long a resource can go unobserved," that floor is
never really about loop X specifically — it is about *every* consumer that
depends on the same "at least one observation before this state becomes
sticky" argument. The day a second loop adopts the identical skip
optimization, the floor's own derivation must become `max` over every
such consumer's period, not stay pinned to whichever one motivated the
floor first. Two structural habits catch this before it needs its own
incident:

1. **Derive the floor as an explicit `max(..)` over named constants, not a
   single hardcoded value or an alias of one loop's own constant.** A
   `const fn` comparing `Duration::as_nanos()` (the one `Duration`
   accessor that actually is `const fn`) makes this cheap even when the
   language's own `Duration::max` isn't `const`-callable. The moment
   someone adds a third consumer, the fix is one more argument to that
   `max`, not a re-derivation from scratch.
2. **State every consumer by name in the constant's own doc comment**,
   with an explicit instruction that a future loop adopting the same skip
   must add its own period to the `max` — turning "audit every quiesce-
   dependent loop by hand" into "read one doc comment and add one line."

**A secondary lesson from the same investigation, worth stating plainly
since it shaped the fix's own shape**: not every trigger sharing a loop's
skip guard has the identical coupling to the floor. `auto_split_loop` has
*four* trigger arms; only three (bytes, change-rate, ops-rate) sit behind
the `is_quiesced()` skip this floor exists for — the fourth (ADR 0067's
throughput-derived minimum tablet count) deliberately does not skip a
quiesced candidate at all, since it fires on a *configured* value, not
*observed* activity, and quiescence's "nothing could have changed" premise
simply does not apply to it. A blanket "raise every floor for every
trigger, every time quiescence is enabled" belt would have been both
overbroad (penalizing test fixtures that configure quiescence with no
opt-in auto-split trigger at all) and, worse, would have obscured that the
fourth arm's own exemption is a *design property*, not an oversight —
conditioning the in-process `debug_assert!` belt on which triggers are
actually configured keeps that distinction visible in the code, not just
in a comment.
