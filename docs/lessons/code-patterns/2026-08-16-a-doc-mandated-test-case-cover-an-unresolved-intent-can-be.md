# A doc-mandated test case ("cover an unresolved intent") can be provably inapplicable to the primitive under test — check the primitive's own invariants before reaching for test-harness tricks to force the case.

**A doc-mandated test case ("cover an unresolved intent") can be provably
inapplicable to the primitive under test — check the primitive's own
invariants before reaching for test-harness tricks to force the case.**
Asked to prove `RaftKvNode::local_scan_kind`'s new `limit` truncates
*after* its intent-drop filter (mirroring `local_scan`'s existing
ordering), the natural instinct is to scan over a row holding an
unresolved `Envelope::Intent` and check it doesn't consume a `limit` slot.
But `local_scan_kind`'s own doc (and `linearizable_scan_kind`'s) already
states a non-base row-kind scope **only ever holds committed values** —
only `KvCommand::KindBatch` writes them, and it always commits outright;
no external test harness constructs an intent there without reaching into
crate-private construction functions. Forcing the scenario anyway would
either not compile against the public test surface or would silently test
something other than the real code path. The regression this repo settled
for instead documents *why* the case can't arise (in the test's own
comment) and proves the ordering-relevant contract that legitimately can
be tested (limit bounds the materialized count, not the raw scan width) —
a `Some("if the existing harness makes that cheap")`-qualified test
request is exactly this: adapt or skip with a documented reason, don't
contort the harness to satisfy the letter of the ask.
