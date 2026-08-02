# ADR 0014 — Elle consistency testing against Accord + a frozen scenario corpus

- **Status:** Accepted (the register-recovery limitation **closed** 2026-08-02:
  reads are now genuinely observed from stored state, see the increment below)
- **Date:** 2026-08-02 (genuine-black-box increment: 2026-08-02)

## Context

The Elle-style serializability checker (`animus-test::check_cycles`) recovers a
per-key list-append order from observed reads, builds Adya wr/ww/rw dependency
edges, and looks for a cycle (G1c/G2). A cycle checker only has *teeth* under a
workload that can actually form a cycle: cross-transaction conflicts on shared
keys. Three gaps made the existing coverage weak:

1. The only end-to-end Elle test (`end_to_end.rs`) ran a **single-writer-per-key,
   disjoint-keys** workload over the AP/LWW data plane. With no two transactions
   ever touching the same key, no dependency cycle can form — the checker was
   effectively asserting nothing about serializability.
2. **Accord** — the layer that actually *claims* a consistent serialization
   order — was never driven through the Elle checker; only the lower-level
   `animus-consensus` ordering tests exercised it.
3. Fault injection was a single fixed ~4-event script, not a coverage-oriented
   matrix.

A complication: Accord's execution effect is "write my transaction id" — a
*register*, not a list-append datatype. So list-append cannot be stored directly;
it has to be *recovered* from Accord's agreed order.

## Decision

**We will point the serializability checker at Accord under real contention, and
drive it from a frozen, generated scenario corpus.**

- **Model list-append over Accord's register by recovering each key's list from
  the consensus execution order.** Each write transaction is assigned a
  globally-unique value (the Elle uniqueness requirement); a key's "list" is the
  sequence of write transactions Accord ordered to it, read from a replica's
  `applied_order`. A read transaction's observed list is the prefix of that key's
  writers ordered before the read. This is faithful: it is exactly the order
  Accord claims; if Accord ordered transactions inconsistently across replicas,
  two replicas' reads would observe non-prefix-compatible lists (flagged as a
  *divergent read*) and/or a real dependency cycle would form.
- **Drive concurrent, conflicting multi-key transactions** over a small shared
  key space (overlapping key sets across concurrent transactions) so the checker
  has genuine wr/ww/rw edges. A no-fault contended run, plus a seed sweep, must
  show no cycle; teeth-guards assert the run genuinely contended (a key written
  ≥ 2 times, a healthy acknowledged-write count) so a green verdict is not
  vacuous.
- **Keep a negative control** (`negative_control.rs`): hand-built non-serializable
  histories (write skew G2, circular read dependency G1c, a 3-txn cycle) the
  checker *must* reject, and known-serializable ones it must accept. Without this,
  a green corpus run is meaningless.
- **A declarative `Scenario` / `NemesisAction` model + a deterministic runner.**
  A scenario is `{ name, seed, cluster shape, workload spec, faults: Vec<(at,
  NemesisAction)> }`; the runner applies the fault schedule at the listed virtual
  times while the workload runs, always heals at the end (so the tail + final
  snapshot run on a healthy cluster), then runs all three checkers. Faults cover
  partition variants (minority / majority-split / single-isolate), crash,
  stop+restart, a data-plane "leader" kill, heal, and lossy links.
- **Materialize a frozen, named, indexed corpus** via a committed deterministic
  generator (`support::corpus`): a structured cross-product of
  { fault type × timing × workload shape × cluster shape } plus no-fault baselines
  and compound (lossy / overlapping-fault) scenarios. Each scenario's seed is a
  stable FNV-1a hash of its name, so the suite runs the *same* scenarios every
  time, a failure names the specific scenario and carries its seed for replay,
  and growing the corpus does not perturb existing entries. Regenerating/growing
  is an explicit edit to `corpus()`.
- **The AP/LWW data plane is checked for what it offers** (convergence / no lost
  acknowledged write), not serializability. Convergence is asserted across two
  *distinct* Accord replicas' recovered list state — a genuine cross-replica
  agreement check.

## Consequences

- The serializability checker now runs against the layer that claims the
  property, under contention that can actually form a cycle — so a green run is
  meaningful (and the negative control proves the detector fires).
- ~119 scenarios run as part of `cargo test -p animus-test` in roughly 15s; all
  119 genuinely contend (verified). Coverage and non-vacuity are themselves
  asserted (`corpus_covers_the_fault_matrix`, `corpus_has_real_contention`) so a
  dimension cannot silently stop being tested.
- The list-append-over-register recovery was originally read from `applied_order`
  — a reconstruction, not bytes the system stores. **This limitation is now
  closed** (see the increment below): with arbitrary write values (ADR 0011) each
  key stores a real list and reads observe it directly.
- A scenario that ever catches a bug stays in the corpus forever as a regression.
  No real consistency violation surfaced at introduction.
- Determinism (ADR 0003) is preserved: the workload's key choice and read/write
  mix are drawn from the seeded simulator RNG, faults fire on virtual-time
  deadlines, and the Accord driver's perpetual retry timer means the runner
  always uses `run_for`/`run_until`, never `run()`.

## Genuine black-box list-append increment (2026-08-02)

The original modelling **recovered** each read's observed list from a replica's
`applied_order` rather than from actually-stored state, because Accord's effect
was a register ("write my txn id"). That limited the checker's teeth to
cross-replica order *divergence*: a single globally-agreed-but-non-serializable
order could not show as a cycle, because the lists were derived from the very
order under test. With **arbitrary caller-supplied write values** (ADR 0011) that
limitation is closed.

- **The workload is now genuine black-box list-append.** Each key stores a real
  `Vec<u64>` (encoded as concatenated big-endian u64s). A **write** appends this
  transaction's globally-unique value to the key's list and writes the **whole
  new list back** as the real stored value (the value-carrying
  `AccordNode::submit_writes`). A **read** observes the **actual stored list**,
  decoded from the bytes the read transaction returns
  (`AccordNode::read_value_result`). The recovered order now comes from observed
  *values* (Elle's `recover`), **not** from `applied_order`. So `check_cycles` is
  a real black-box serializability check: a non-serializable agreed order would
  surface as a dependency cycle.
- **Single-writer-per-key (the LWW guard).** Each key is written by exactly one
  client (`owner(key) = key % clients`); a write only appends to keys it owns. Two
  clients writing the same key would lose appends by the *data model* (per-key
  LWW) — not a consistency bug, and it would drown the checker in false positives.
  Cross-transaction conflict (the wr/rw/ww edges the cycle checker needs) still
  comes from **multi-key transactions** and from reads observing keys *other*
  clients write. A client builds each append on its own authoritative in-memory
  list (it is the sole serial writer), because a begin-time quorum read can lag
  the previous write's fire-and-forget data-plane propagation and read a stale
  base — which would lose the client's own earlier appends.
- **Final state is read straight from stored values** on two *distinct* replicas
  (`store_value` → decode), keeping convergence a real cross-replica agreement
  check and durability ("every ok append is in the final list") meaningful.
- The negative control, teeth-guards (`ok_writes ≥ 8`, contention witness,
  `nonempty_reads ≥ 1`), and all ~119 frozen corpus scenarios stay green; the
  stronger check surfaced **no** real serializability violation. The change is
  entirely in `animus-test/tests/support/mod.rs` plus the additive
  `animus-consensus` value API; the corpus seeds/names are untouched, so every
  frozen scenario reproduces exactly.
