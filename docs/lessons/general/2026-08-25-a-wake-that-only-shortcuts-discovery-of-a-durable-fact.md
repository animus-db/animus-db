# A wake that only shortcuts discovery of a durable fact never needs to be durable itself (ADR 0058 rung 4 layer 1)

The rung-4 fix above closed the first-vote latency; a second residual
remained — the SECOND voter a fresh child needs to grant that vote was
still discovering the fork on its own next *scheduled* poll (up to the
50ms fast-poll interval, on top of this host's ~100ms round-trip floor).
The fix followed the identical "move the trigger, not the mechanism"
shape one more time: a new signal (`ForkSignal`, the same `AtomicBool` +
`AtomicWaker` pattern `animus-cp-data` already uses for `ProposeSignal`/
`ApplySignal`/`WakeSignal`) wakes the reconciler's tick the instant the
apply task observes the fork, instead of waiting out the poll — but the
materialization logic it wakes is untouched.

The part worth generalizing on its own: **the new signal is deliberately
not durable, and that is fine precisely because the fact it announces
already is.** A crash between the apply task raising `ForkSignal` and
anything consuming it loses the wake completely — a restarted replica has
no memory it ever fired. That is safe by construction, not by luck,
because the wake never became the *only* way to learn the fact: the
reconciler's ordinary periodic tick already re-derives "did this tablet
fork" from a durable marker (`pending_split()`) on every single pass,
whether or not any wake ever arrived. The eager path is purely additive —
it makes the common case fast; the unconditional fallback (already
required for the crash-recovery case a slower poll always had to handle)
is what makes correctness independent of whether the wake landed at all.

This generalizes beyond this one signal: **when adding an eager
notification on top of an existing polling/fallback loop, resist the urge
to make the notification itself crash-safe.** Persisting it (or rebuilding
it deterministically across a restart) is extra machinery solving a
problem that doesn't exist as long as the poll loop it shortcuts remains
correct and unconditional on its own. The corollary is a real, checkable
test obligation, not just an argument: prove the crash-loses-the-wake case
explicitly (this rung's `crash_after_apply_loses_the_eager_wake_but_
reconciler_fallback_recovers` scenario), rather than treating "the eager
path worked in every other test" as evidence the fallback still does its
job unattended.

A second, smaller point the same rung's own corpus cell needed: **an eager
notification and the fallback it shortcuts are, by construction, going to
race** — the eager wake fires a tick, and the ordinary periodic tick can
still land moments later on the identical already-handled state. This is
not a bug to prevent; it is a property to prove benign. If the mechanism
being triggered is already idempotent (as any correct crash-recovery path
must be — G4's optimistic-claim-then-execute discipline here), the fix
needs no debouncing, coordination, or "only once" bookkeeping between the
two triggers at all — just a test that fires both, back to back, and
asserts nothing changed the second time.
