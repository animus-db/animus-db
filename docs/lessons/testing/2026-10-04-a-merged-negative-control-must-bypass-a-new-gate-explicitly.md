# A negative control that emits a forbidden value must bypass a newly added gate explicitly

When enforcement (a propose-site gate that refuses, plus a `debug_assert!`) lands in
parallel with a corpus whose negative control *is* the forbidden emission, the merge is
textually clean and semantically broken: the control is either refused (vacuous pass) or
panics on the assert. Give the control a test-only entry point
(`cfg(any(test, feature = "sim-versions"))`) that skips the gate, and keep the control's
observable assertion (here: capped-decode rejection plus the wedged replica) unchanged.
Never weaken the production gate.

Second, same merge: a per-profile "does this binary decode gate g" predicate written
against `Gate::version()` treats a new always-open `Gate::Base` (version 1) as unknown to
a Phase 1 binary (max known version 0). Any new variant added to an enum a test oracle
matches on needs that oracle re-read. Replace stand-in classifiers with the production
`required_gate` tables as soon as they exist.

Disk note: the sandbox disk fills at ~29 GB of `target/`; with `CARGO_INCREMENTAL=0` and
`CARGO_PROFILE_{DEV,TEST}_DEBUG=0` the full workspace is ~6 GB and builds fine.
