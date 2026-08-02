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
  multi-key read/write transactions over a small shared key space as **genuine
  black-box list-append** (real list values stored and observed — see below),
  records an Elle list-append `History`, and runs all three checkers. Also defines
  the declarative `Scenario` / `NemesisAction` model, the `run_scenario` runner,
  and the frozen `corpus()` generator.
- `elle_accord.rs` — Accord under contention: a no-fault contended run + seed
  sweep + a determinism check, with teeth-guards asserting the run genuinely
  contended.
- `corpus.rs` — the parametric runner over the **frozen, named, seeded** corpus
  (~119 scenarios: fault type × timing × workload shape × cluster shape, plus
  baselines and compound lossy/overlapping scenarios), a coverage guard, a
  non-vacuity guard, and a determinism check.

- **Genuine black-box list-append over Accord (ADR 0014, closed limitation).**
  With **arbitrary write values** (ADR 0011) each key stores a *real list value*:
  a write is a list-append (append this txn's globally-unique element to the key's
  list, write the whole new list back as the stored value via
  `AccordNode::submit_writes`), and a read observes the **actual stored list**
  (decoded from `AccordNode::read_value_result`). The recovered order comes from
  observed *values* (Elle's `recover`), **not** from `applied_order` — so
  `check_cycles` is a real black-box serializability check (a single
  globally-agreed-but-non-serializable order now shows as a cycle, not just
  cross-replica divergence). The earlier "recover the list from `applied_order`"
  modelling is **obsolete** — do not reintroduce it.
- **`list_state` reads stored values, not the order.** Final state is read
  straight from each key's actually-stored value on two *distinct* replicas
  (`store_value` → `decode_list`), keeping convergence a real cross-replica
  agreement check and durability ("every ok append is in the final list")
  meaningful. **Do not** run `check_durability` against a single raw per-replica
  read with no read-repair expectation; read the converged stored list per
  replica.
- **Single-writer-per-key is load-bearing here (not just an optimisation).** Each
  key is written by exactly one client (`owner(key) = key % clients`); a write
  only appends to keys it owns. Two clients appending to one key lose updates by
  the *data model* (per-key LWW), which would show as a false-positive durability
  failure / divergent read — not a consistency bug. Cross-transaction conflict
  (the wr/rw/ww edges) still comes from multi-key transactions and from reads
  observing keys *other* clients write. **A client builds each append on its own
  authoritative in-memory list**, *not* a begin-time quorum read: the apply marks
  a txn `Applied` before its fire-and-forget data-plane write lands, so a
  begin-time read can see a stale base and lose the client's own earlier appends
  (this bit during development — the seed sweep caught it as a divergent read).
- **The corpus is a committed generator, not a live-random test.** Each
  scenario's seed is a stable hash of its name, so the suite runs the same set
  every time and a failure names one scenario + seed. Regenerating/growing it is
  an explicit edit to `support::corpus`; a bug-catching scenario stays forever.
- **Indeterminate (`info`) vs the observed universe.** A write whose client timed
  out is recorded `info`, but if it actually executed its element is in the key's
  stored list (hence observed by later reads and present in the final list).
  That's sound: reads and the final state both read stored values, so they stay
  prefix-consistent under single-writer-per-key; `info` values simply have no `ok`
  appender, so they form no dependency edges. Never record an indeterminate op
  `ok`.
- **Workload modeling gotcha (Elle).** Every appended element must be **globally
  unique**, not just per-key-per-round. Reusing a value across rounds/phases makes
  `recover` collapse distinct transactions onto one `(key, value)` appender,
  manufacturing **spurious cycles** the checker correctly flags. The harness draws
  values from one monotonic `fresh_value()` source for exactly this reason. If
  `check_cycles` trips on a single-writer-per-key workload, suspect value reuse
  before suspecting the system.
