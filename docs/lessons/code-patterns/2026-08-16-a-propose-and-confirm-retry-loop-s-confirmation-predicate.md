# A propose-and-confirm retry loop's confirmation predicate must prove THIS call's own effect, not merely "something with the expected shape now exists" — an allocator-derived id computed from a possibly-stale read can collide with a *different* concurrent command's id, and a confirmation that can't tell them apart silently reports success for a proposal that was actually rejected.

**A propose-and-confirm retry loop's confirmation predicate must prove
THIS call's own effect, not merely "something with the expected shape now
exists" — an allocator-derived id computed from a possibly-stale read can
collide with a *different* concurrent command's id, and a confirmation
that can't tell them apart silently reports success for a proposal that
was actually rejected.** `ClientCtx::trigger_split` (`animusd`) computed a
new tablet id once (`next_free_tablet_id()` from one metadata snapshot),
proposed `SplitTablet`, then confirmed via `tablets.contains_key(&new_id)`
— reasonable in isolation, but growth PR3's `grow_stream` calls
`trigger_split` for **two different source tablets** in quick succession,
each independently computing `new_id` from its own (possibly differently
stale, possibly cross-node-forwarded) metadata read. When both attempts
computed the identical `new_id`, the control leader correctly rejected
the second as "id already exists" — but that second call's own
confirmation loop kept polling, and the moment the FIRST call's real
commit replicated to it, `tablets.contains_key(&new_id)` turned true and
it reported `PutOk` for a split that never happened (only one of the two
source tablets actually gained a child). Found by a real-cluster test
going from "2 tablets → should be 4" to "2 tablets → 3," flaky at roughly
50%. The fix: recompute the allocator-derived id fresh on every retry
(so a corrected later attempt, once this node's own metadata catches up,
mints a genuinely free id) and confirm via a property **intrinsic to the
call's own target** — here, the source tablet's own epoch having
advanced past what was observed at the start (the CAS-gated apply arm
only ever bumps it on a real committed split of that exact tablet) —
rather than a derived, potentially-colliding id. **General form**: when a
retry loop proposes command X and separately polls "did X's effect
land," ask whether the poll can also be satisfied by some OTHER command's
effect that merely looks the same from the outside; if the answer isn't a
clean no, the confirmation is checking the wrong thing. (2026-08-16,
`growth/2-stream-grow`, `ClientCtx::trigger_split`.)
