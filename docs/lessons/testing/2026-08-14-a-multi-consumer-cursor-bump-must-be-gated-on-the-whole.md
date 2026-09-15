# A multi-consumer cursor bump must be gated on the WHOLE sweep succeeding, never fused into each individual partition's own commit entry — even though the two look interchangeable at first glance.

**A multi-consumer cursor bump must be gated on the WHOLE sweep succeeding,
never fused into each individual partition's own commit entry — even
though the two look interchangeable at first glance.** Reworking the GSI
drain from "consuming is trimming" to a cursor (ADR 0042 §7/§8), the
obvious design was: each `reconcile_partition` call writes its own
footprint update *and* bumps the tablet-wide "gsi" cursor to this tick's
overall max HLC, in the same atomic entry (mirroring the old design's
"footprint + delete the records it covers, one entry" shape). That is
unsound: the cursor is a **single row covering every partition in the
tablet**, not a per-partition value, so bumping it to the *tick-wide* max
the instant the *first* partition's entry lands would claim every
**other**, not-yet-reconciled partition's records (up to that same max) as
consumed too — a crash between the first and second partition's entries
then leaves the cursor over-claiming coverage the second partition never
got, and the trim janitor would delete its records regardless, silently
and permanently freezing that partition's GSI rows stale (no change record
survives to ever re-trigger it). The fix: compute the sweep's overall max
HLC once, reconcile every dirty partition sequentially (propagating any
error immediately, before the loop advances), and only *after* the whole
loop returns `Ok` does a single trailing write bump the cursor — by that
point every partition the max HLC could implicate has already had its own
footprint update independently confirmed durable, so a crash before the
trailing write just leaves the cursor wherever it was (safe, re-covers
everything on the next tick) and a crash after it is the fully-covered
case. The general form: a watermark that summarizes N independent
sub-operations is only safe to advance once *all* N have been individually
confirmed, not on the first one succeeding, even if advancing it earlier
would be "usually" correct. (`crates/animusd/src/index_drain.rs`,
`drain_tablet`, 2026-08-14.)
