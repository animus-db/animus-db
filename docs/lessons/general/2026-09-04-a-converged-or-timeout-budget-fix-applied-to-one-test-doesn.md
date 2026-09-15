# A converged-or-timeout budget fix applied to one test doesn't fix its sibling that shares the same relay shape (issue #591, following #281/PR #301)

`data_only.rs`'s `schema_ddl_via_a_data_node_relays_and_commits` and
`control_only.rs`'s `mixed_cluster_put_via_control_node_forwards_to_data_node`
both drive a data-only-shaped node's `ProposeSchema` through the identical
mechanism: `ClientCtx::propose_schema`'s ADR 0030 broadcast fallback,
capped per candidate at `FORWARD_HOP_TIMEOUT` since issue #585. Issue #281
diagnosed the `data_only.rs` copy's 20s retry budget as too thin — a single
`call()` there chases up to 4 other known intra addresses, so one *whole*
attempt can legitimately cost 4 * `FORWARD_HOP_TIMEOUT` = 8s, leaving a 20s
budget barely 2-3 worst-case attempts of headroom, not real margin under a
starved runner. PR #301 fixed it there (20s → 60s, plus the outer wrapper
90s → 150s) but touched only `data_only.rs` — it never occurred to grep for
the same shape in `control_only.rs`'s own mixed-cluster test, which drives
the exact same `propose_schema` broadcast path (just 3 candidates instead
of 4, so if anything *thinner* margin proportionally: 20s / 6s worst-case
≈ 3.3 attempts vs. the fixed test's 60s / 8s ≈ 7.5). It surfaced
independently, months later, as the same `Elapsed(())` panic under heavy
host load (issue #591), passing on every retry — the signature of a rare
tail-latency flake, not a deterministic bug, so a 10-run repro attempt
under artificial `yes`-loop CPU contention (4 cores, 4 loops) did not
reproduce it; the arithmetic argument (not a live repro) is what carries
the diagnosis here, exactly as it did for #281.

**General rule**: when a converged-or-timeout budget fix lands because one
test's retry attempt rides a network-hop timeout (`FORWARD_HOP_TIMEOUT`,
`CLIENT_TIMEOUT`, or any per-hop cap), grep every other test that drives
the *same underlying relay function* — not just tests with a similar name
or in the same file — before considering the fix complete. `propose_schema`
had exactly two real-node callers issuing it against a data-only-shaped
node (`data_only.rs` and `control_only.rs`); fixing one and shipping is an
incomplete fix, not a smaller one. (`crates/animusd/tests/control_only.rs`,
`crates/animusd/tests/data_only.rs`.)

**This is a budget-arithmetic alignment with an already-accepted fix, not a
timeout widened to hide an unexplained failure** — worth being explicit
about given this repo's standing "a flaky test is a bug, never fixed with a
wider timeout" rule (root `CLAUDE.md`): the old 20s budget covered only
~3 worst-case attempts (3 candidates x `FORWARD_HOP_TIMEOUT` each), which
PR #301 already established, for the identical mechanism, as *not* a real
margin — the fix here is computing the same arithmetic for this test's own
candidate count and matching the sibling's already-reviewed number, not
picking a bigger number to make a flake go away unexamined.

**The actual structural fix, if this shape needs revisiting again**: the
broadcast fallback tries each candidate *serially*, so its worst case is
N x `FORWARD_HOP_TIMEOUT` and grows with cluster size — a concurrent,
first-success-wins broadcast (fire all known candidates at once, take
whichever answers first, cancel the rest) would bound the worst case to
one hop's `FORWARD_HOP_TIMEOUT` regardless of N, removing the multiplier
this budget calculation depends on entirely rather than just re-deriving
it correctly. Not done here because the current fix (matching the #301
precedent) is minimal and the serial chase is otherwise working as
designed (ADR 0030's "best-effort, first connectable candidate resolves
the real leader" contract) — but it's the right next step if a *third*
`propose_schema` broadcast caller surfaces the same flake, or if the
control-group size ever grows enough that N x `FORWARD_HOP_TIMEOUT` alone
threatens `CLIENT_TIMEOUT`-scale budgets again.
