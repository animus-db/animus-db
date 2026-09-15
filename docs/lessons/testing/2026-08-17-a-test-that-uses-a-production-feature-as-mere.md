# A test that uses a production feature as mere INFRASTRUCTURE (not as the thing under test) breaks the moment that feature is gated — audit which is which before parking anything.

**A test that uses a production feature as mere INFRASTRUCTURE (not as
the thing under test) breaks the moment that feature is gated — audit
which is which before parking anything.** When ADR 0050's storage pivot
disabled the zero-copy split surface, every `cp_txn`/`dynamo_txn`/
recovery test went red at once — not because their subject (2PC across
Raft groups) broke, but because their shared harness split an EMPTY
table purely to get a two-group topology. The fix was not parking the
binaries (which would have thrown away all transaction coverage) but
re-sourcing the topology at the layer that owns it: propose the
metadata command directly (`Node::propose_meta`), sound precisely
because the table is still empty (nothing to inherit). Only tests whose
SUBJECT was the disabled mechanism (split-of-populated-tablet e2e) got
parked. General form: for each red test under a feature gate, ask "does
this test *test* the feature, or merely *use* it to build a fixture?" —
the second kind wants a fixture rewrite, never an `#[ignore]`.
