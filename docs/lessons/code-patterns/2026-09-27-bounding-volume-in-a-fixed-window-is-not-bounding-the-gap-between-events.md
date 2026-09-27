# Bounding total volume in a fixed window is not the same claim as bounding the gap between events

Follow-up to the same-day lesson on `animus-control`'s `SnapshotResend::
Backoff` (the "keyed off a successful-attempts counter freezes once
suppressed" entry): fixing that freeze produced an exponential-doubling
schedule (resend on attempt `0` or a power of two) with no ceiling, which
looked done — it demonstrably bounds total resend volume within any FIXED
measurement window (doubling is logarithmic, so a 30s/30s split-window
regression test stays comfortably under its bound whether or not a
ceiling exists at all). Review caught what that test couldn't: an
unbounded doubling schedule also lets the GAP between consecutive resends
grow without bound, right alongside the count. For a peer whose
outstanding chunk is its *only* leader-liveness signal (the leader never
sends a plain heartbeat while a peer is in chunked-snapshot mode), a stall
of N heartbeats means the next scheduled resend can be up to N heartbeats
away — so a peer stuck for an hour can, upon reconnecting, wait up to
another full hour before catch-up actually resumes. A test built to prove
"volume stays bounded over a fixed window" cannot see this: it measures a
static property (how much traffic fits inside a window of fixed length)
which trivially holds for any schedule whose growth is sub-linear,
completely independent of whether the schedule ever stops growing.

**The general lesson**: "total volume is bounded within any given window"
and "the interval between successive events is bounded" are two different
claims about a growth curve, and a fix — or its regression test — that
proves one does not thereby prove the other. A monotonically-growing,
never-capped interval (exponential, or any super-constant growth) always
satisfies the first (a longer stall just means fewer, more spread-out
events, and a fixed window can only ever contain finitely many terms of
any divergent series) while failing the second by construction. Whenever
a backoff/retry schedule's purpose is partly "the retry rate must not be
allowed to become arbitrarily rare" — not just "the retry rate must not be
allowed to stay arbitrarily fast forever" — the schedule needs an explicit
ceiling that flattens growth into a bounded steady-state period, and the
regression that proves it needs to measure elapsed time between a stall
and its resumption directly (partition-then-heal-then-measure-recovery-
latency), not merely traffic volume inside one arbitrarily-chosen window.

**A second, narrower lesson from choosing the ceiling itself**: when a
periodic resend is also serving double duty as a liveness/heartbeat signal
to the receiver (here, `RaftCore::handle_install_snapshot`'s election-timer
reset, since a snapshot-mode peer gets no separate `AppendEntries`), there
can be real tension between "make the steady-state period long enough to
keep volume genuinely bounded over a long stall" and "make it short enough
that the receiver's own unrelated timeout (its election timer) never
fires between events." Shrinking the ceiling to satisfy the second goal
can silently defeat the first (at the limit, resending at nearly full
tick rate to avoid a 150ms timeout reproduces almost exactly the original
unbounded-volume flood the mechanism exists to fix). Where the receiver
already has an independent, well-understood tolerance for spurious
signals in that gap (here, Raft pre-vote's design already makes a lone
straggler's repeated failed pre-vote rounds harmless and cost-free to the
rest of the cluster), leaning on that existing tolerance — and writing a
dedicated regression that proves the "harmless churn" claim directly,
rather than merely inferring it from "the test didn't fail" — is often
the sounder choice than shrinking a volume-bounding ceiling down to satisfy
an unrelated timeout it was never designed around.
