# Superseded by ADR 0028

**Superseded by ADR 0028**: the abandon path + `pending` map this entry
describes are deleted along with the rest of the two-phase split retry
machinery. Retained for historical record. **An "abandon and forget" exit from a retry loop must still leave the
cooldown state a *fresh* attempt would have set — otherwise the tablet is
eligible again on the very next tick, not after backing off.**
`auto_split_loop`'s pending-retry loop drops a tablet from `pending` when it
detects its own key lost the group's one-time split to a different proposer
(`abandoned`, see the entry above on the confirm-by-key check) — correct,
since retrying a losing key can never succeed. But it only cleared `pending`
and didn't touch `last_triggered`, so `is_fresh_split_candidate` (which
excludes a tablet only while it's `pending` *or* within
`AUTO_SPLIT_COOLDOWN` of `last_triggered`) saw a clean slate immediately —
this node's *next* tick could propose a brand-new fresh split for the same
source tablet with a new median, right away. Combined with any repeated
contention on that tablet (this is exactly what the cross-node
`auto_split_loop` contention entry above produces), this manifested as a
tablet's split id climbing every tick forever (8→10→12…), each attempt
abandoned in turn, never actually converging. Fixed by inserting into
`last_triggered` on the abandon path too, identical to what a fresh trigger
already does — so an abandoned tablet backs off for the normal cooldown
before being reconsidered, giving the *winning* split's data move a chance
to actually shrink it below `threshold`. **General check for any "give up
and exit the retry path" branch: does it leave the surrounding loop's
own rate-limit/cooldown state as if this had been a normal, successful
cycle — not just as if this attempt had never happened?**
