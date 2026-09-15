# A rate-change on a token bucket only ever governs refill for elapsed time AFTER the check that applies it — the very next check still pays the OLD rate (ADR 0065, W-08 step 4)

`ThrottleBucket::set_rate` refills at `self.rate` (the OLD value) for
whatever time has elapsed since the bucket's last touch, and only *then*
reassigns `self.rate`/`self.capacity` to the new values — a deliberate
design (a lowered budget must never retroactively grant burst it could not
legally have earned). The consequence, easy to miss when writing a test
for "raising a table's `ProvisionedThroughput` admits more": raising the
rate does nothing to the bucket's *current* token count — there is no
retroactive top-up — and the very first check-write/check-read call after
the raise is the one that pays the reassignment, refilling at the OLD rate
for the (possibly large) gap since the bucket was last touched, since the
config-changing call itself (`UpdateTable`) never touches the bucket at
all. Only the check AFTER that one sees real elapsed time refill at the
NEW rate. A first draft of `update_table_raising_units_admits_more`
(`tests/dynamo_throttling.rs`) raised the write units by six orders of
magnitude, slept 50ms, and asserted the very next `PutItem` succeeded —
and failed deterministically on every run, not flakily, because that
single post-raise write was exactly the reassignment-paying call. The fix
was a converged-or-timeout retry loop (root `CLAUDE.md`'s own testing
discipline for an eventual property) rather than a one-shot assert after a
fixed sleep of any length. **General form**: a token bucket (or any
stateful rate limiter) whose "current rate" is a field mutated lazily on
next use, not proactively on every rate change, needs at least two
touches after a rate change before the new rate is genuinely reflected in
admission decisions — write the test as a bounded retry, not a single
post-change assertion, and don't assume "raise the rate enough and any gap
will do."
