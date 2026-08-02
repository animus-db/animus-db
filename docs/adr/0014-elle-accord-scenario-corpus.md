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

## Coverage expansion: seed-depth, dimension-breadth, and the topology split (2026-08-02)

The frozen corpus was **broad but shallow** — one structural cell (fault ×
timing × workload × shape) explored down exactly **one** interleaving (its
name-hashed seed). Deterministic simulation's bug-finding power comes from many
seeds per configuration, so coverage was scaled along two tiered, env-gated axes,
and a latent unsoundness the single seeds had masked was fixed.

- **Depth (`ANIMUS_CORPUS_SEEDS=K`, default 1).** `support::seed_expand` emits `K`
  seed variants of every structural cell. Variant 0 keeps the cell's canonical
  (frozen) name+seed; variants `1..K` get a `_sNN` suffix and a fresh
  name-derived seed. `K=1` is the identity, so the always-on default is
  byte-identical to the committed corpus.
- **Breadth (`ANIMUS_CORPUS_FULL=1`, default off).** `support::corpus_extended`
  adds new dimension *values* — the `SlowLinks` fault (a degraded-but-connected
  network: a coordinator looks *slow*, not dead, stressing the failure-detector
  bound), 7-node and asymmetric (3+5 / 5+3) cluster shapes, extra fault timings
  (very-early / wind-down), extra workloads (write-only / big-txn /
  low-contention), and richer multi-fault schedules. Extended scenarios are
  `ext_`-prefixed so they never perturb a base name/seed.
- **Tiering.** Defaults (`K=1`, no FULL) keep `cargo test` at the frozen base
  set. The deep tier (`ANIMUS_CORPUS_SEEDS=40 ANIMUS_CORPUS_FULL=1`) runs nightly
  in CI (`.github/workflows/corpus-deep.yml`). The structural guards in
  `corpus.rs` assert against the env-independent `corpus_base()` so they stay fast
  and stable regardless of the knobs.

**The unsoundness depth exposed (and the fix).** The deep run immediately flagged
serializability cycles — but only on **faulted `wide_write`** cells, **never**
no-fault, and always cycle-only (convergence + durability passed). Root cause: the
harness observed workload reads through the **AP data-plane frontier**
(`read_value_result` → a current quorum read), while final-state reads used
`store_value` (Accord's local executed store). Under a data-replica fault a
committed multi-key write is acked by Accord *before* it is quorum-durable
(fire-and-forget), so a later conflicting read could observe one key's new value
but not the other's — a torn read the cycle checker correctly flagged. This is the
AP data plane being *eventually* consistent, **not** an Accord ordering bug; it
directly violated the repo principle *"point a serializability checker at the
layer that claims it (Accord); check the AP plane for convergence/RYW, not
serializability."*

The fix introduces a **`Topology`** to the harness:

- **`Authoritative`** (pure Accord, `AccordNode::start`): each replica executes
  the agreed order into its **local** store and a read is a versioned snapshot
  (`get_at(key, execute_at)`) — exactly the writes ordered before it, none after,
  identically on every replica, **robust to faults**. This is the *only sound
  target for `check_cycles`*, and it is what the corpus now runs. All three
  checkers are meaningful here.
- **`Frontier`** (`start_with_data_plane`): the AP data-plane wiring, exercised by
  a separate corpus (`corpus.rs::frontier_corpus_converges_and_is_durable`) that
  asserts **convergence + durability only** — what that layer offers.

With the authoritative topology the entire deep tier
(`ANIMUS_CORPUS_SEEDS=5 ANIMUS_CORPUS_FULL=1`, ~945 scenarios) is green, and the
stronger coverage confirms Accord's serialization order holds under the full fault
matrix. The change is confined to `animus-test` (tests only); no production code
changed.

## Deep-tier findings: serializability is safety, convergence/durability are eventual (2026-08-02)

Running the deep tier (`ANIMUS_CORPUS_SEEDS=40 ANIMUS_CORPUS_FULL=1`, ~7,560
scenarios, ~4 min) refined the topology split into a **safety vs. eventual**
distinction — the load-bearing rule for *which* checker is sound to assert at
adversarial seed-depth.

Two seed-variant scenarios (present only at depth) diverged, with **opposite**
verdicts by topology:

- `ext_t_stop_restart_winddown_s39`: **pure Accord diverged** (key 2 `[10]` vs
  `[10,13]`, value 13 acked then absent), **frontier converged**.
- `lossy_stop_restart_mid_s36`: **frontier diverged** (key 2 `[3,5,7]` vs
  `[3,5,7,9]`, value 9 acked then absent), **pure Accord converged**.

Serializability (`check_cycles`) held on **all** 7,560 authoritative scenarios.

The lesson: **neither layer converges within a *fixed* drain window under every
compound fault at seed-depth.** Pure Accord guarantees committed-*order* safety,
not per-replica store convergence (backfill is the data plane's anti-entropy job);
the data plane converges *eventually*, but under a compound fault (e.g. `lossy` +
`stop_restart`) anti-entropy can still be in flight when the runner's post-heal
drain ends. So convergence + durability are **eventual** (liveness) properties:
checking them as a *hard* assertion at a fixed deadline is only sound on a
bounded, non-pathological set — at adversarial depth it is flaky on *both*
topologies without exposing any safety bug.

Resulting design (what the corpus asserts):

- **Serializability is a *safety* property** → asserted on the
  **`Authoritative`** topology (pure Accord) and **scaled to the full deep tier**
  (`corpus_is_consistent` over the env-scaled `corpus()`). This is the high-value,
  sound, hard check, and it is green at depth.
- **Convergence + durability are *eventual* properties** → asserted on the
  **`Frontier`** topology with a **converged-or-timeout** verdict (see the
  increment below), which makes them sound to scale to the full deep tier alongside
  serializability.

This is the same "check each layer for what it offers" principle, now also split by
*property class*: safety is judged at a point; eventual/liveness is judged with a
bounded poll. Both now scale to depth.

## Converged-or-timeout verdict: scaling the frontier corpus to depth (2026-08-02)

The deep-tier finding above bounded `frontier_corpus_converges_and_is_durable` to
`corpus_base()` because the runner judged convergence/durability off a **single
fixed post-heal drain snapshot** (`run_for(40s)`): at adversarial seed-depth a
compound fault can legitimately leave anti-entropy still in flight when that fixed
drain ends, so a hard deadline-assertion was flaky without revealing any safety
bug. The "future option" deferred there — a *converged-or-timeout* verdict — is now
**done**.

- **The runner polls instead of snapshotting (eventual checks only).** After
  healing, the runner still drives the fixed `DRAIN` (so the recorded history, hence
  the `cycles` verdict, is snapshotted at exactly the same point as before — the
  authoritative run is byte-identical to the fixed-drain era). It then drives a
  **bounded converged-or-timeout poll**: in fixed virtual-time increments
  (`CONVERGENCE_POLL_STEP`), re-read the two final replicas' actually-stored list
  state and re-run `check_convergence` + `check_durability`; **stop early** the
  moment both hold. If `CONVERGENCE_BUDGET` (120s of *additional* virtual time)
  elapses without converging, the last (failing) verdict is surfaced as a **genuine**
  non-convergence/durability failure (scenario name + replay seed + the divergence),
  not masked by widening the bound. The poll is a pure function of the seed — only
  `run_for`/`run_until` advance time, in a bounded loop (ADR 0003).
- **The frontier corpus scales to depth.** With a sound verdict at depth,
  `frontier_corpus_converges_and_is_durable` now iterates the **env-scaled**
  `corpus()` (like `corpus_is_consistent`), so `ANIMUS_CORPUS_SEEDS` /
  `ANIMUS_CORPUS_FULL` scale convergence + durability coverage to the full deep tier.
  The two previously-divergent seed variants (`lossy_stop_restart_mid_s36`,
  `ext_t_stop_restart_winddown_s39`) converge within the poll bound — confirming
  they were anti-entropy still in flight at a fixed deadline, not safety bugs.
- **Validation.** Default `cargo test -p animus-test` stays green (all 7 corpus
  tests; `K=1`, no FULL → the frozen 119). The deep smoke
  (`ANIMUS_CORPUS_SEEDS=10 ANIMUS_CORPUS_FULL=1 cargo test -p animus-test --test
  corpus`) is green in ~113s — both the cycles-at-depth and the
  convergence/durability-at-depth verdicts. Change confined to `animus-test`
  (tests only); no production code changed; corpus seeds/names untouched.
