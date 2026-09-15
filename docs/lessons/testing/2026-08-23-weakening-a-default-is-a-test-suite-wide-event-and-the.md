# Weakening a default is a test-suite-wide event, and the compiler cannot see any of it (2026-08-23, ADR 0055).

**Weakening a default is a test-suite-wide event, and the compiler cannot
see any of it (2026-08-23, ADR 0055).** Making `ConsistentRead: false` —
DynamoDB's *default* — a genuinely eventually-consistent read broke a
scatter of tests across unrelated files, all with the same shape: write
something, then immediately read it back to check the write landed. Every
one of them had been silently relying on the read path being *stronger than
the API promised*. Two things worth carrying forward. **First, the failures
are non-deterministic by construction** — a follower that happens to have
applied in time passes — so "the suite went green once" proves nothing
here; the real signal is auditing which reads exist to verify a write, not
re-running until it passes. **Second, the fix is never to re-strengthen the
default**: each such test asks for `ConsistentRead: true`, which is exactly
what a real DynamoDB client must do, and the resulting test says out loud
which consistency it depends on instead of inheriting one by accident.
Generalized: when a change makes a default *weaker* (a read, a lock, a
timeout, a durability level), expect the breakage to land in tests that
never mention the thing you changed, expect it to be flaky rather than
deterministic, and treat each break as a test that was under-specified
rather than as evidence against the change.
