# CLAUDE.md — animus-test

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

Elle/Jepsen-style history recording and consistency checking. A library other
crates' tests use to record operation histories and assert correctness
properties; it also hosts cross-crate fault sweeps.

## Entry points

- `history.rs` — `Recorder` (`invoke`/`ok`/`fail`/`info`), `History`, `Mop`
  (list-append `Append`/`Read`), `Outcome`.
- `check.rs` — `check_cycles` (serializability), `check_durability`,
  `check_convergence`; each returns a `CheckReport` carrying the run seed.
- `export.rs` — `to_json`, `to_edn` (Jepsen/Elle).

## What's non-obvious

- **Indeterminate outcomes (e.g. a timeout) MUST be recorded `info`, never
  `fail`.** `fail` asserts the op definitely did not happen; misclassifying
  makes a checker draw false conclusions. The data-plane harnesses follow this:
  a non-quorum write is `info`.
- `check_cycles` is the core Elle idea: recover each key's append order from
  observed reads, build wr/ww/**rw** edges, run Tarjan SCC. The `rw`
  anti-dependency rule (a read precedes the appenders of values it did *not*
  observe) is what catches write skew — keep it if you refactor.
- AP checks assume `R + W > N`: durability = every `ok` append is in a final
  quorum read; convergence = two final quorum reads agree (not raw per-replica
  state — there's no read-repair yet).
- `CheckReport.seed` exists so a flagged anomaly is replayable; surface it in
  assertion messages.

## Tests

`cargo test -p animus-test` — `cycle_checker.rs` (hand-built histories),
`ap_data_plane.rs` and `fault_sweep.rs` (the real quorum data plane through the
recorder, including an injected lost write), and `end_to_end.rs` (the **whole
assembled stack**: a 3-node control-plane Raft owning a 2-tablet map, six data
replicas with background anti-entropy, four concurrent client coordinators
running list-append over disjoint keys spanning both tablets, faults injected
mid-run — partition + control-plane leader kill + data-replica crash + heal —
then all three checkers run over the recorded history; `dev-deps` on
`animus-control` for this).

### Elle-against-Accord + the frozen scenario corpus (ADR 0014)

`end_to_end.rs`'s disjoint-key workload can never form a serializability cycle,
so it doesn't actually exercise `check_cycles`. The Accord-targeted suite does:

- `negative_control.rs` — the **teeth proof**: hand-built non-serializable
  histories (write skew G2, circular read dep G1c, a 3-txn cycle) the checker
  *must* reject, plus serializable ones it must accept. Run/read this before
  trusting any green corpus run.
- `support/mod.rs` — the shared harness: assembles an Accord replica set wired to
  the data plane (`start_with_data_plane`), drives **concurrent conflicting**
  multi-key read/write transactions over a small shared key space, records an
  Elle list-append `History`, and runs all three checkers. Also defines the
  declarative `Scenario` / `NemesisAction` model, the `run_scenario` runner, and
  the frozen `corpus()` generator.
- `elle_accord.rs` — Accord under contention: a no-fault contended run + seed
  sweep + a determinism check, with teeth-guards asserting the run genuinely
  contended.
- `corpus.rs` — the parametric runner over the **frozen, named, seeded** corpus
  (~119 scenarios: fault type × timing × workload shape × cluster shape, plus
  baselines and compound lossy/overlapping scenarios), a coverage guard, a
  non-vacuity guard, and a determinism check.

- **List-append over a register (the Accord modelling).** Accord's execution
  effect is "write my txn id" — a *register*, not list-append. The harness
  recovers each key's list from Accord's agreed **`applied_order`**: a key's list
  is the (globally-unique) values of the write transactions Accord ordered to it;
  a read observes the prefix ordered before it. This is faithful — it is exactly
  the order Accord claims, so an ordering bug surfaces as a divergent read or a
  real cycle. **Do not** run `check_durability` against a final *register* read of
  this data plane: the register holds only the last writer, so every overwritten
  append looks "lost". Build the final list-append state from `applied_order`
  (the harness's `list_state`), and read convergence from **two distinct
  replicas'** orders (a genuine cross-replica agreement check).
- **The corpus is a committed generator, not a live-random test.** Each
  scenario's seed is a stable hash of its name, so the suite runs the same set
  every time and a failure names one scenario + seed. Regenerating/growing it is
  an explicit edit to `support::corpus`; a bug-catching scenario stays forever.
- **Indeterminate (`info`) vs the recovered universe.** A write whose client
  timed out is recorded `info`, but if it actually executed it still appears in
  `applied_order` (hence in reads and the final list). That's sound: reads and
  the final state both derive uniformly from the consensus order, so they stay
  prefix-consistent; `info` values simply have no `ok` appender, so they form no
  dependency edges. Never record an indeterminate op `ok`.

- **Workload modeling gotcha (Elle).** Every appended element must be **globally
  unique**, not just per-key-per-round. Reusing a value across rounds/phases (a
  single-writer key that re-appends the "same" element) makes `recover` collapse
  distinct transactions onto one `(key, value)` appender, manufacturing
  **spurious cycles** the checker correctly flags. `end_to_end.rs` draws values
  from one monotonic `fresh_value()` source for exactly this reason. If
  `check_cycles` trips on a single-writer-per-key workload, suspect value reuse
  before suspecting the system.
- Use **single-writer-per-key** for list-append over the LWW KV store:
  concurrent writers to one key lose updates by the *data model* (LWW), which is
  not a consistency bug and would otherwise drown the checker in false positives.
  Concurrency still comes from many clients running interleaved across keys.
