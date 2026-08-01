# CLAUDE.md — custos-test

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

`cargo test -p custos-test` — `cycle_checker.rs` (hand-built histories),
`ap_data_plane.rs` and `fault_sweep.rs` (the real quorum data plane through the
recorder, including an injected lost write), and `end_to_end.rs` (the **whole
assembled stack**: a 3-node control-plane Raft owning a 2-tablet map, six data
replicas with background anti-entropy, four concurrent client coordinators
running list-append over disjoint keys spanning both tablets, faults injected
mid-run — partition + control-plane leader kill + data-replica crash + heal —
then all three checkers run over the recorded history; `dev-deps` on
`custos-control` for this).

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
