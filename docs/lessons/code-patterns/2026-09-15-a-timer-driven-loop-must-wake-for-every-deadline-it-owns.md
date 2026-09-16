# A driver that sleeps until "the next deadline" must compute the minimum over EVERY independent deadline a component owns, not just the one it originally had

Found chasing a real CI regression in issue #667's boot-time cluster check
(PR #902): `RaftCore::tick()` already had correct, independent resend logic
for a still-pending cluster check (`cluster_check_resend_deadline`,
deliberately never touched by `handle_append_entries`'s legitimate
`election_deadline` reset — see the sibling code-patterns entry on why that
decoupling was necessary). The fix still didn't close the regression,
because `tick()` was never being **called** at the right time to run it.

## The trap: adding a new deadline field to a component doesn't make the driver wait for it

`node.rs`'s driver loop doesn't poll `tick()` on a fixed interval — it reads
`RaftCore::next_deadline()` once per iteration and sleeps for exactly that
long before calling `tick()`. `next_deadline()`, for a non-leader, returned
only `Some(self.election_deadline)`. Adding `cluster_check_resend_deadline`
as a new field on the core and giving `tick()` correct logic to act on it
was necessary but not sufficient: `next_deadline()` never mentioned the new
field, so the driver kept sleeping until `election_deadline` alone —
which `handle_append_entries` legitimately pushes far into the future on
every valid leader contact. A still-checking founder that starts receiving
ordinary heartbeats from an already-elected sibling could have its own
driver oversleep past its own resend deadline for as long as
`election_deadline` kept getting reset, even though the resend code itself
was correct and would have fired instantly if only it had been invoked.

This was invisible to every test that drives a `RaftCore` by hand (calling
`tick()` at literal, test-chosen `Nanos` values, as `next_deadline.rs`'s own
pre-existing suite and `wiped_voter_double_vote_safety.rs` both do) — those
tests exercise `tick()`'s own logic directly and never go through
`next_deadline()`'s wake computation at all, so a bug purely in "when does
the driver decide to call `tick()`" has no unit-level surface to be caught
on. It only manifested end-to-end, under a real driver loop, under real
staggered timing.

## The fix

`next_deadline()` for a non-leader now returns
`min(election_deadline, cluster_check_resend_deadline)` whenever a cluster
check is pending, instead of `election_deadline` alone.

## The generalizable rule

Whenever a stateful component (a Raft core, a state machine, anything a
driver loop polls via "sleep until the next thing I need to do") gains a
**second independent deadline** — a retry timer, a lease renewal, a resend
schedule — the function that tells the driver *when to wake up* must be
audited and updated at the same time as the deadline field itself, not
assumed to already cover it. Adding correct logic inside the handler that
runs *after* a wake is not the same fix as making sure the wake actually
happens when needed; a driver that computes "next wake" from a stale or
incomplete deadline set will silently oversleep past a real, correctly-
implemented timer, and the failure only shows up in an end-to-end,
real-scheduling test — never in a hand-driven unit test that already skips
straight to calling the handler. When adding a new per-component deadline,
grep every function whose job is "what's the next time I need attention"
(often named `next_deadline`/`next_wake`/`poll_interval`) and add the new
field to its own `min(...)` computation explicitly, with a test that proves
the driver actually wakes for it (not just that the handler behaves
correctly once invoked).
