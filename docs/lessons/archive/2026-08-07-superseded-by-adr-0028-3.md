# Superseded by ADR 0028

**Superseded by ADR 0028**: `auto_split_loop`'s `pending` retry map (and step 2
it was retrying) no longer exist — split is a single-step command. Retained
for historical record. **A retry loop keyed on a resource id must recheck the resource still
exists — a precondition that only checks its own transient state ("did *my*
attempt fail in a way I recognize") silently assumes the resource itself is
immortal.** `auto_split_loop`'s pending-retry map has one such gap left even
after the fix above: dropping the whole table out from under a still-pending
split (`DropTableTablets` removes the source tablet, and any child it had
minted, in one apply) leaves a `pending` entry retrying a tablet id that can
never have a leader again — the existing "did a different key win"
abandon-check doesn't fire (`local_cp` on an unregistered id returns `None`),
so the loop reads this as "still committing" and retries forever, one wasted
routing round trip per tick. Fixed with the obvious missing check: does the
target still exist in `Metadata` at all, before retrying. **Not every fix
needs (or can usefully get) a regression test** — this one has no black-box
behavioral difference between "gave up" and "quietly retries forever" other
than resource waste and log noise, so it's verified by review + the existing
suite staying green rather than by a new assertion; don't force a test where
there's nothing external to assert against. (`animusd` `auto_split_loop`'s
retry phase, the tablet-existence guard added right before the confirm
call.)
