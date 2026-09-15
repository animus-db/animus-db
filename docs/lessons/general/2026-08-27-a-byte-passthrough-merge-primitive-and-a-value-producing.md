# A byte-passthrough merge primitive and a value-producing read primitive can disagree about who owns the envelope tag (ADR 0059 §7, Train 2 restore)

Building the restore driver (`animusd::backup_restore`), the first real
end-to-end test run panicked on an ordinary `ConsistentRead: true` `GetItem`
against a freshly-restored row: `txn: unknown envelope tag 123 (corrupt
engine value)`. Not a fault-injection scenario — every seed, every time,
on the very first row read back.

The root cause was two correct primitives disagreeing about a byte-format
contract neither one's own doc stated explicitly enough to catch by
inspection. `animus-cp-data`'s apply path wraps every ordinary write's
value in a 1-byte-tagged envelope (`0` = committed, `1` = intent) before
merging it into the engine — every read path unwraps this before a caller
ever sees a value. `KvCommand::SeedBatch` (the split-build driver's own
history-transfer command, reused verbatim by restore per ADR 0050) is
deliberately the *exception*: it merges the exact bytes handed to it,
envelope tag included, because a split child's rows are still-enveloped
physical bytes from the same live transaction blast radius as their
parent — copying them verbatim is what lets an in-flight intent continue
resolving correctly wherever it lands.

Backup capture (ADR 0059 §5) reads through intent resolution by design —
it deliberately stores each row's already-*resolved*, plain value, with no
envelope tag at all, specifically so a restored table never carries a
dangling, unresolvable intent envelope pointing at an anchor that may not
even exist anymore. That decision is exactly right on capture's own side.
It just means the restore driver's own input (a plain resolved value) and
`SeedBatch`'s own contract (an already-enveloped physical byte string) are
not the same shape — feeding one into the other merges a byte string whose
first byte the read path's decoder can't recognize as either envelope tag,
producing exactly the panic above the moment anything ever reads the row
back.

**The general form**: a merge primitive that is deliberately "verbatim
bytes in, verbatim bytes out" (no re-encoding, by design, for its own
documented reason) is not a safe target for a *different* producer whose
own output was already decoded/normalized one layer down from what that
primitive expects — even when both producers are "giving it a value" in
the loosest sense. The fix is never to make the merge primitive smarter
(that would break the property it exists for); it's to make the seam
between the two explicit: `animus_cp_data::backup::encode_restored_value`
is a one-line, clearly-doc'd wrapper the restore driver calls on every
captured value before it ever reaches `SeedBatch`, named after what it's
for rather than what it does, so a future caller reads its doc before
reusing the pattern instead of rediscovering the panic. Before wiring a
second producer into an existing "verbatim passthrough" primitive, check
what shape its *existing* callers actually hand it — "the same trait
method" is not "the same byte contract."

Caught by the project's own first real integration test for the feature,
not by review or a fault-injection sweep — a reminder that an end-to-end
test exercising the full production stack (not just the unit-level pieces)
remains the cheapest way to catch a cross-module contract mismatch that
both sides' own type signatures happily agree on.
