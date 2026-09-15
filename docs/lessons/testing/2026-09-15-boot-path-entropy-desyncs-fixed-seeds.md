# A boot-path behavior change can desync a fixed-seed corpus test with zero logic bugs

Found building issue #667's fix (`RaftCore::begin_cluster_check`, called from
`animus-control::node::drive` on every node whose persisted state replays
empty — i.e. every genesis node).

## The trap

Two unrelated, fixed-seed `SimEnv` tests broke while adding a boot-time
mechanism that runs unconditionally for every fresh (empty-persisted-state)
`RaftCore`:

- `crates/animus-control/tests/control_corpus.rs`'s
  `chunked_snapshot_receiver_stop_restart_3` (a hand-tuned seed via
  `corpus::name_seed`) started failing with a genuine-looking convergence
  failure — a restarted follower's engine stuck at its pre-restart applied
  index forever.
- `crates/animus-control/tests/transfer_third_voter_wins.rs` (a seed
  hand-picked "by an exhaustive scan of seeds 0..3000" per its own doc,
  pinned to a specific race outcome) started asserting the *wrong* voter won
  the pinned election.

Neither failure was caused by the new mechanism's *logic*. Both were caused
by the new mechanism's mere *existence* on the boot path:

1. **An extra `env.next_u64()` draw reshuffles the entire subsequent random
   sequence for the whole `SimEnv` run.** Confirmed by injecting a single
   harmless `let _ = env.next_u64();` into *unmodified* `main`'s boot path
   (no new logic at all) and reproducing the exact same
   `chunked_snapshot_receiver_stop_restart_3` failure. This is not a
   determinism bug — the run is still perfectly reproducible for that exact
   sequence — it's that a fixed-seed test's whole point is landing in one
   narrow scenario out of a huge space, and *any* change to how many times
   `next_u64()` is called before the interesting part of the test shifts
   which scenario that seed now lands in.
2. **Even with the draw count made identical to `main`'s (by threading the
   *same* already-drawn entropy through instead of drawing fresh — see the
   code-patterns lesson below), an extra spawned task and a couple of extra
   wire messages during genesis are *still* enough to desync a seed this
   finely tuned.** `SimEnv`'s own deterministic tie-breaking between
   simultaneously-ready tasks/messages is itself part of what such a seed
   pins — a purely topological change to *when* things become ready can
   flip a two-way race's outcome with no extra randomness involved at all.

## What actually caused each failure, and how it was NOT "fixed" by tuning the boot path

- `chunked_snapshot_receiver_stop_restart_3`: root-caused separately as a
  **real, pre-existing bug** (issue #899): `RaftCore::
  handle_install_snapshot_resp`'s monotonic `max` guard on `snapshot_offset`
  also swallowed a genuinely restarted follower's own honest `next_offset:
  0` reset, permanently pinning the leader's resend at a stale high offset.
  This was a real defect the entropy shift *exposed*, not caused — fixed on
  its own merits (`next_offset == 0` is now an authoritative reset, not
  folded into the max), with its own dedicated regression test that needs
  no seed at all.
- `transfer_third_voter_wins`'s pinned seed: **not a bug**. The seed was
  re-scanned (the identical "exhaustive scan for a case where X" technique
  the test's own doc used to find the original one) against the post-fix
  codebase and re-pinned to a newly-found match. The underlying mechanism
  the test protects (issue #688's "read the stepped-down leader's own live
  belief, don't trust a bare step-down") was never in question.

## The generalizable rule

When a change adds *any* real behavior to a path every `SimEnv` node
executes unconditionally at boot (or any other near-universal hot path):

1. **Don't assume a fixed-seed corpus failure it causes is a logic bug in
   the new code.** Reproduce the failure by injecting an equivalent,
   deliberately-inert perturbation (an extra no-op entropy draw, an extra
   no-op spawned task) into the *unmodified* baseline first. If the same
   failure reproduces with zero new logic, it's a pre-existing bug the
   perturbation exposed, or a legitimately re-pinnable "which exact outcome
   does this seed hit" test — not a regression in the new code.
2. **Minimize entropy/topology perturbation where the design allows it**
   (reuse already-drawn entropy instead of drawing fresh when re-arming
   something that was just armed moments ago at construction) — but accept
   that this reduces, not eliminates, the risk for the most finely-tuned
   seeds, and budget time to re-scan/re-pin them rather than treating that
   as a red flag.
3. **A test whose own doc says "found by an exhaustive scan of seeds"
   is declaring itself fragile to exactly this class of change** — treat a
   failure there as "go re-run the scan," not "something is broken," once
   you've confirmed (per rule 1) it isn't.

See ADR 0009's 2026-09-15 amendment (issue #667) for the concrete case this
was extracted from.
