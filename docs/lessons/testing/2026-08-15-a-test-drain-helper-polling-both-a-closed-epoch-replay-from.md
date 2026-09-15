# A test-drain helper polling both a "closed epoch, replay from `TRIM_HORIZON`" path and a "current open tail, resume from last position" path must resume the SAME iterator when an epoch transitions from open to closed mid-poll, never re-mint a fresh `TRIM_HORIZON` walk for it.

**A test-drain helper polling both a "closed epoch, replay from
`TRIM_HORIZON`" path and a "current open tail, resume from last
position" path must resume the SAME iterator when an epoch transitions
from open to closed mid-poll, never re-mint a fresh `TRIM_HORIZON` walk
for it.** `streams_e2e.rs`'s `drain_tablet_lineage`/
`drain_all_tablets_lineage` always re-minted `TRIM_HORIZON` for a
newly-closed epoch, discarding whatever position the open-tail poll had
already reached in it one pass earlier — double-delivering any record
the open-tail poll had already returned before that epoch sealed. This
was invisible under `tiny_seal_knobs()` (`seal_bytes: 1`), whose open
tail is always empty the instant it's polled (every write seals as its
own epoch immediately), so no existing test before PR1's
production-shaped-knobs regression cell ever left more than one record
in an open tail across two poll passes. General form: a resumable
iterator's identity survives a state transition (here: open → closed);
a caller that mints a fresh one anyway on the transition, instead of
continuing the one it already has, double-reads whatever the old one
had already delivered.
