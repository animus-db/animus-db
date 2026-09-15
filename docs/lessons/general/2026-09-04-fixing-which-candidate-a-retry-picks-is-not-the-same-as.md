# Fixing which candidate a retry picks is not the same as fixing how long that retry is allowed to run (issue #585, a third continuation)

The second #585 fix (above) let `forward_to_tablet_leader`'s chase RETURN to
a candidate that had only timed out, not merely avoid starving on one — but
it left every hop, including that return trip, capped at the same
`FORWARD_HOP_TIMEOUT` regardless of *why* the candidate was chosen. That is
fatal for exactly the case the second fix exists for: each retry is a
**fresh** proposal on the receiving leader, not a resumed wait on the
original attempt, so a leader that genuinely needs longer than
`FORWARD_HOP_TIMEOUT` to commit on every attempt (a real membership-change
storm serializing proposes ahead of this one, not just the first attempt)
can never succeed no matter how many times the chase correctly circles back
to it — each retry restarts the clock and dies at the identical wall.
Confirmed live in CI: `tests/split_placing_two_replica_diff_e2e.rs`'s paced
writer failed `put failed: Error("no CP group leader reachable")` twice out
of two runs with the second fix already in place, during the 5-voter →
3-voter trajectory where the leader moves n2 → m0 — the chase was
demonstrably resolving to the right candidate (the second fix's own
property held) and still never succeeding, because every return trip to it
was capped exactly like an ungrounded first guess.

**Fix, and why the earlier fix's own test didn't catch this.** Give the
forward-candidate decision a classification, not just an address:
[`ForwardCandidate::Hinted`/`Guessed`](../crates/animus-node/src/decide.rs)
(`resolve_forward_candidate`'s own doc has the full per-case reasoning). A
`Guessed` candidate — nothing vouches for it, a deterministic pick among
known replicas or the no-local-replica fallback — stays capped at
`FORWARD_HOP_TIMEOUT`: safe, because something else is always tried first,
so nothing is starved by only giving it a slice. A `Hinted` candidate —
some live replica named it, including a `timed_out` last-resort retry with
nothing else left to protect a cap from — now gets the full remaining
`CLIENT_TIMEOUT` budget instead of another capped hop. The existing
regression for the second fix
(`forward_hop_timeout_tests::forward_to_tablet_leader_waits_out_a_slow_but_
live_leader_instead_of_giving_up`) stayed green through this whole
regression window without ever catching it: its stub
(`spawn_slow_then_ok_stub`) special-cases only the *first* connection to
stall — every later connection, retry included, answers instantly — so
whether the retry's own hop happens to be capped or not never mattered to
that fixture. It proves the *candidate-selection* half of #585 (the chase
correctly returns to the timed-out address) but says nothing about the
*timeout* half (once returned, is that retry allowed to actually finish).
The new regression,
`forward_hop_timeout_tests::forward_to_tablet_leader_does_not_cap_a_hinted_
retry_of_a_genuinely_slow_leader`, uses a different stub
(`spawn_always_slow_ok_stub`) that answers every connection — first attempt
and every retry alike — only after a delay past `FORWARD_HOP_TIMEOUT`
(matching the actual production shape: a leader whose commit latency is a
real, standing cost on every attempt, not a one-time slow start), so only a
genuinely uncapped hinted retry can succeed inside `CLIENT_TIMEOUT`.
**General form: when a fixture's job is to prove a specific fix stays
fixed, ask whether its own stand-in models *every* dimension the next
regression in the same mechanism could vary along** — this fixture modeled
"slow now, fast later" faithfully for the candidate-selection fix, but that
exact faithfulness is what made it blind to a *timeout* regression in the
same code path, because "fast later" made the retry's own timeout budget
irrelevant by construction.

**Testing note, reinforcing the sibling entry above.** Reproducing the
real end-to-end failure by looping
`split_placing_two_replica_diff_e2e` ten times back-to-back post-fix (this
investigation's own validation pass) turned up two failures, both a real
per-process `"Too many open files (os error 24)"` from
`animus_cp_data::persist_wal`/`apply_kind_batch` inside the in-process
multi-node `ProdEnv` fixture — the same sandbox-capacity bucket the sibling
entry above already documents, not a regression in either #585 fix. Eight
of ten runs passed cleanly with no `"no CP group leader reachable"` (the
actual pre-fix failure signature) anywhere in the log. The general lesson
stands: read what actually failed before concluding a flaky loaded-sandbox
rerun means the fix regressed.
