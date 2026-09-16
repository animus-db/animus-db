# An aggressive resend loop can be both a flood source AND the retransmit mechanism a regression test's own convergence depends on — a fix must separate the roles, not throttle the shared knob (issues #532/#537, ADR 0009's third amendment)

The third mechanism behind the residual above (`InstallSnapshot` chunk
resend *frequency*, as opposed to the *batch-size*/*compaction-timing*
mechanisms the first two fixes closed) looked, at first, like a simple
"throttle the resend" job: `replicate_now`'s wake-on-propose fires on
every write and unconditionally re-sends whatever chunk is still
outstanding, so gating that one call site should cut the flood. Two
narrower shapes built on exactly that framing — skip a mid-snapshot peer
from wake-on-propose entirely; throttle it by propose count (1-in-2,
1-in-20) — both regressed `animus-cp-data/tests/
learner_catchup_under_load.rs` (the existing regression test for the
first two fixes): the learner stopped converging at all. The instinct at
that point is to conclude "this test needs the flood, so some flood must
stay" and hand-tune a smaller one back in. That instinct is wrong, and
finding out why required instrumenting the *live per-peer state itself*
(the same "watch the value the stuck predicate reads, don't audit the
code around it" move as the entry above) rather than reasoning about the
resend policy in the abstract:

- **The wake-on-propose call site was never the flood's real amplifier in
  this test.** `RaftCore::propose`'s wake-on-propose is a single coalesced
  `AtomicBool` (`ProposeSignal`), not a per-propose counter — under this
  test's own tight synchronous burst of ten proposes with no yield between
  them, it already fires at most once per burst regardless of any throttle
  applied to it. Gating it changed almost nothing about that test's own
  message count. The actual flood in THAT test's own failure mode came
  from a completely different call site: the ack-handler's own resend
  (`handle_install_snapshot_resp`), which fires once per received message
  — not coalesced at all — and answers every ack, including a stale
  duplicate that carries no new information, with another unconditional
  resend. That is a **self-sustaining loop**, bounded only by round-trip
  time, and it is *also* — this is the double duty — the mechanism that
  lets a genuinely stuck transfer recover before the next heartbeat
  (`heartbeat_deadline` itself is perpetually deferred by wake-on-propose's
  own reset on every write, so it rarely fires in time under sustained
  load). Gating this call site down to zero-tolerance (the same treatment
  correctly applied to wake-on-propose) reproduced the exact same
  convergence failure the two rejected prototypes did — not because the
  reasoning about wake-on-propose was wrong, but because it was answering
  a question ("is *this* call site the flood source") that didn't
  generalize to a *different* call site playing a *different* role.
- **A second, independent defect was hiding under the flood's own volume**:
  `handle_install_snapshot_resp`'s tracked offset had no monotonic guard,
  so an ack processed out of real-progress order could regress it backward
  — confirmed directly by instrumenting the pre-fix code (217 regressions
  in one run, each one chunk backward). The pre-fix flood's sheer resend
  volume papered over this bug by brute-forcing enough duplicate attempts
  to re-advance past any transient regression anyway. A throttled resend
  has no such slack — it fixes forward once per attempt, so a regression
  it can't recover from in one round trip compounds instead of
  self-healing. This is why *both* narrower-throttle prototypes failed
  even with a reasonable resend budget: they were quietly exposed to a
  bug the flood had been silently absorbing, and no amount of tuning the
  throttle's own numeric cap would have fixed a monotonicity defect
  underneath it.

**The generalizable lesson**: when a fix targets "throttle this aggressive
retry loop" and the *existing* regression test for a related, earlier fix
breaks, don't assume the loop needs to stay aggressive — check whether the
loop is doing two jobs at once (flooding, and legitimately carrying a
recovery path a test depends on) and whether throttling it is silently
exposing an unrelated bug the flood's own redundancy was masking. The fix
that actually worked kept the resend cap **structurally separated by
caller**: the call site diagnosed as the flood source (wake-on-propose)
got a strict cap; the call site actually carrying the recovery role
(the ack-handler) kept a bounded — not zero, not unlimited — cap of its
own; and the masked regression got its own independent, unconditional fix
(a `max`, not a bare `insert`) so the throttle didn't have to compensate
for it. One shared numeric knob applied uniformly to "the resend
mechanism" cannot express any of that — it was tried, twice, and failed
both times for reasons that only became visible once the two roles were
identified separately.
