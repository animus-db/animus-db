# A mere timeout is not a confirmed failure — discarding warm state (a cache entry, a pooled connection) on one can turn transient slowness under load into a permanent, self-sustaining stall

**A mere timeout is not a confirmed failure — discarding warm state (a
cache entry, a pooled connection) on one can turn transient slowness under
load into a permanent, self-sustaining stall.** While hardening
`animus-env`'s pooled-connection send path against a vanished peer (issue
#924), an additional "defensive" change was tried alongside the real fix:
whenever the outer per-send `SEND_TIMEOUT` bound elapsed — not just when
the underlying write returned a genuine error — also drop the cached
connection, reasoning that a spurious reconnect against a merely-slow-but-
live peer "only costs one extra handshake." That reasoning is true in
isolation and wrong under sustained load, and the difference matters: a
`SEND_TIMEOUT` elapsing is not evidence the peer or the connection is
broken, only that *this one send* didn't finish inside the bound — on a
real, busy host (CPU oversubscribed, the scheduler itself delayed) that is
routinely just scheduling latency on an otherwise perfectly healthy
connection. Discarding the connection anyway forces the *next* unit of
work (the next chunk of a large streaming transfer, the next heartbeat) to
pay a fresh connect from scratch — and if the host is still under the same
sustained contention, that fresh connect can *itself* exceed the same
timeout, discard itself in turn, and repeat indefinitely. What looks like
"a harmless extra handshake here and there" under light, transient
contention becomes "zero forward progress, ever" under sustained
contention, because every attempt keeps resetting to a cold start just
before it might have succeeded. Caught by a large-state real-`ProdEnv`
catch-up test (`animus-control`'s `large_metadata_catch_up_stays_live`,
streaming roughly 1100 `InstallSnapshot` chunks) going from "slow" to
"literally zero bytes delivered in 12 seconds" specifically on a heavily
loaded shared CI/dev box — the exact condition under which the flaw
actually bites, which is also exactly the condition an unlucky isolated
test run might never hit, making this the kind of change that looks
correct in review and in a quiet CI run and then destabilizes the first
genuinely busy one.

**The general form**: a timeout is a statement about the *caller's own
patience*, not about the *callee's health* — collapsing "I stopped
waiting" into "it is broken" is only safe when the two are actually
equivalent, which requires either a timeout tight enough that a live-but-
slow peer could never plausibly hit it (not true for a bound shared with
real host contention) or independent confirmation that the underlying
resource is actually dead (a real error, not just an elapsed clock).
Before making a timeout branch discard shared/pooled/cached state as a
side effect, ask specifically: under sustained resource contention (not
just a one-off blip), does hitting this branch make the *next* attempt
strictly more likely to succeed, or does it reset exactly the progress
that attempt was relying on? If the answer is the latter, the timeout
branch should log and give up on *this one unit of work* (which is
already fire-and-forget-safe in most designs) without touching state that
a later, luckier attempt could still reuse.
