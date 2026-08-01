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
recorder, including an injected lost write).
