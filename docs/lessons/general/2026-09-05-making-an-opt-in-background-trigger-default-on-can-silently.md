# Making an opt-in background trigger default-on can silently reinterpret an unrelated test's own large fixture values as a real signal (ADR 0067, W-08b)

`auto_split_loop`'s first three triggers (bytes, change-rate, ops-rate) are
all genuinely opt-in — a cluster that sets none of their flags gets zero
behavior change, so any existing test's fixture values (a huge byte
threshold, a huge declared rate) were always safe: nothing was watching
them. ADR 0067's fourth trigger inverted that: its ceilings default ON
(DynamoDB's own 3000 RCU / 1000 WCU), so the loop itself had to become
unconditionally spawned rather than gated on "at least one trigger
configured." The moment that happened, a **pre-existing** test file
(`tests/dynamo_throttling.rs`) that declares a table with
`ProvisionedThroughput: {WriteCapacityUnits: 1000000}` — a value chosen
purely to exercise the *throttle bucket's* arithmetic, with no intent to
say anything about tablet count — silently became a live input to a brand
new mechanism: under the new default ceilings, that single value implies a
derived minimum of ~1000 tablets, which the now-always-running loop would
have started forking toward mid-test, on every test run, for a fixture
that had nothing to do with this feature.

**General form**: when a change moves ANY threshold, trigger, or gate from
"opt-in, so pre-existing large/small edge-case fixture values are inert"
to "on by default," grep every existing large-magnitude fixture value the
newly-live mechanism could read — not just the tests written *for* the new
feature. A value that was deliberately chosen to be extreme for one
purpose (draining a token bucket fast) is exactly the kind of input a
second, unrelated, newly-default-on mechanism will also treat as extreme,
and the two purposes are easy to conflate because both consume the same
field (`ProvisionedThroughput` here). The fix here was not to shrink the
throttling test's fixture values (which would have weakened what they're
actually testing) but to have that test's own cluster bring-up explicitly
opt out of the new trigger (`tablet_max_{read,write}_units: Some(0)`) —
scoping the new default-on behavior away from a test that was never about
it, rather than letting an unrelated fixture's magnitude become an
accidental second test of the new feature.
