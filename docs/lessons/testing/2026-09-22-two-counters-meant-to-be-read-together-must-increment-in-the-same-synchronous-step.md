# Two counters meant to be read together as one derived value must increment in the same synchronous step — not "eventually, once the operation they describe finishes"

**What happened (issue #1037, following on from #974).**
`crates/animusd/tests/batch_write.rs::batched_write_beats_per_key` scrapes
`cp_proposals_accepted` and `cp_housekeeping_proposals_accepted` off
`GET /metrics` before/after a write burst and asserts
`raw - housekeeping == expected_client_proposals`, exactly. #974 had
already found that the per-tablet trim janitor
(`animusd::index_drain::trim_janitor`) shares the raw counter with real
client writes, and "fixed" it by having `trim_janitor` also increment
`cp_housekeeping_proposals_accepted` — once its own write call returned.
That looked complete (a fault-free local run reproduced "9 raw / 1
housekeeping" and the test passed), but the test went red again under
CPU contention, this time as **9 raw / 0 housekeeping** — and a follow-up
GitHub Actions run turned up the identical signature on two unrelated PRs
the same day, i.e. near-deterministic on a loaded runner, not a rare race.

**The mechanism.** The raw counter increments synchronously, the instant
`RaftCore::propose` accepts the entry onto the leader's own local log
(`animus-cp-data::record_propose`, inside `put_kind_batch`). The
attribution counter, in #974's shape, incremented one layer up
(`trim_janitor`, in `animusd`) **after** the whole write call returned —
which for that write means after its full commit+apply **confirm loop**
had finished polling/parking, a real span of `ProdEnv` async time. Nothing
made those two increments atomic with each other; they were two
independent writes to two independent counters, separated by an `.await`
that can take anywhere from microseconds to the write's full timeout. A
`/metrics` scrape landing in that gap sees the first counter's new value
and the second counter's stale one — exactly what "N raw, N-1
housekeeping" means. The window scales with how loaded the confirm loop's
own scheduling is, which is why it went from "one flake in two attempts"
locally to "2 of 2 CI runs" on a busier runner: slower real time makes the
gap wider, not the race rarer.

**The fix** moved the attribution increment to the **same synchronous
step** as the raw one: `RaftKvNode::metrics_handle`/`CpGroup::metrics`
expose the group's own `MetricsHandle` (the identical sink
`record_propose` already writes into), and the low-level propose-and-confirm
function (`ClientCtx::cp_kind_raw_local`) takes a `housekeeping: bool` that,
when set, marks `CpHousekeepingProposalsAccepted` immediately after
`put_kind_batch` returns `Accepted` — before the confirm loop below it ever
awaits anything. The two counters now change together, in one poll, with
no `.await` between them for any scrape to land inside.

**What to do.**

- When two counters are read together and their *difference* (or ratio,
  or join) is the actual observable — "proposals a client caused" here,
  but the shape recurs for any "total minus a known subcategory" metric —
  treat "increment A" and "increment B" as one atomic step, not two
  sequential ones separated by other work. If B can only be computed after
  some of that work finishes (a confirm, an apply, a downstream lookup),
  either move B's increment to the same call frame as A's (as here), or
  make the two counters genuinely joinable another way (a single combined
  event, a shared sequence number) — never assume the two writes are
  "close enough" to appear atomic to a reader.
- A metrics-attribution bug and a duplicate-propose bug produce an
  **identical symptom** (`raw` higher than expected) but need opposite
  fixes — one is "make the missing counter increment," the other is "stop
  proposing twice." Don't assume which one you have from the symptom
  alone; trace where each counter's increment actually sits relative to
  the caller's own await points. Here, tracing `record_propose`'s call
  sites against where `CpHousekeepingProposalsAccepted` was incremented
  immediately showed a whole confirm loop's worth of `.await` between the
  two — the tell that this was an attribution race, not a second propose
  (which the #911/#971 confirm-loop fix had already ruled out structurally:
  no superseded/no-op/retry path re-proposes an already-accepted entry).
- A regression test for this class of bug doesn't need `ProdEnv`
  nondeterminism at all — it needs to observe the operation **mid-flight**.
  A seeded `SimEnv` three-voter group, with the write's task driven only to
  its first park (`sim.run_for(Duration::ZERO)`, before any other voter has
  had a chance to ack), pins the invariant directly: assert both counters
  already agree at that exact paused instant, deterministically and
  seed-reproducibly, rather than trying to reproduce a wall-clock race.
  See `crates/animusd/src/write_path.rs`'s
  `cp_kind_raw_local_housekeeping_attribution_tests` module.
- A single-voter `SimEnv` group cannot exercise this: `core.propose` can
  advance commit + apply **inline** when the group has no other voters to
  wait on (ADR 0044 phase-1 PR1), so the accepted-but-unconfirmed window
  collapses to nothing and the write's task never actually parks. Use a
  three-voter group (see `cp_kind_raw_local_outcome_confirm_tests` for the
  same requirement, for the same reason, from issue #911).
