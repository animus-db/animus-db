# A different pre-existing failure, exposed by the same `--no-fail-fast` workspace run that found the RF policy bug above: `animusd/tests/self_heal.rs` panicked under concurrent client load with `assert_ts_monotonic` — a hard-assert HLC/MVCC invariant (ADR 0018 §2), "raftkv apply: HLC ts ... did not strictly exceed the last applied ... the witnessing chain is broken."

**A different pre-existing failure, exposed by the same `--no-fail-fast`
workspace run that found the RF policy bug above: `animusd/tests/self_heal.rs`
panicked under concurrent client load with `assert_ts_monotonic` — a
hard-assert HLC/MVCC invariant (ADR 0018 §2), "raftkv apply: HLC ts ...
did not strictly exceed the last applied ... the witnessing chain is
broken."** Root-caused and fixed in `animus-cp-data`; two distinct bugs,
found in sequence, both in the same neighborhood:
1. **Minting a proposal's `ts` and appending it to the Raft log were two
   separate, unsynchronized steps with no `.await` between them.** Every
   mutating propose method did `let ts = self.mint_pushed(..); self.
   propose_and_wake(command)` — two sequential, non-yielding calls. Two
   concurrent proposers could mint ts=A then ts=B (A < B, correctly
   monotonic *as mints* — `Hlc`'s own mutex guarantees that much) but race
   to actually call `core.propose(..)` in the *opposite* order, so B's
   entry lands at a *lower* log index than A's — apply then sees ts=B
   then ts=A, a real decrease. **This specific shape is `ProdEnv`-only,
   provably**: with no `.await` point between mint and propose, two tasks
   can never be preempted mid-way under `SimEnv`'s single-threaded
   cooperative scheduler — only genuine OS-thread parallelism
   (`ProdEnv`'s multi-threaded tokio runtime) can interleave two
   sequential, non-yielding function calls from different tasks. Every
   other regression in this 25-binary crate drives `SimEnv`; this bug
   needed the one real-thread exception (`tests/prod_concurrent_ts_
   monotonic.rs`) to even exist, let alone catch. Fixed by
   `propose_ordered`: hold the group's own `core` lock across "compute
   `ts`" *and* "propose" as one atomic step — since every proposal to one
   group already funnels through that lock to get ordered at all, this
   adds no new bottleneck, it just closes the gap between two steps that
   already needed to agree.
2. **A narrower, purely-logical bug surfaced only once (1) was fixed**:
   the write-push floor (`mint_pushed`) and the read-ceiling ratchet
   (`next_ceiling_candidate`) both needed a *new* floor — this leader's
   own last-*proposed* (logged, not necessarily applied yet) `ts` — since
   `committed_ceiling`/`ts_cache` only reflect *applied* state, and the
   apply task can lag the consensus loop by design (the driver-liveness
   split, ADR 0017). The first attempt folded this new floor in as
   `margin.max(last_proposed_ts)` and returned it **unmodified** whenever
   it beat the ratchet's own history — reproducing the exact bug it was
   supposed to fix, one level up: a `ReadCeiling` proposed right after a
   write could get the write's *exact* `ts`, an exact tie (not an
   inversion) `assert_ts_monotonic` also rejects. `margin` (always
   freshly `HLC_MAX_OFFSET` in the future) was safe to return verbatim;
   `last_proposed_ts` — a value some *other* command just used — was not.
   Fixed by treating both `last_proposed_ts` and the ratchet's own
   history as floors to *strictly exceed* (reusing the same
   bump-the-logical-component branch for both), never handing either
   back unmodified.
**Diagnostic lesson**: found entirely by adding a temporary `eprintln!` at
each `assert_ts_monotonic` call site printing `(index, command variant,
ts)`, then re-running the failing test until it captured the exact
colliding pair — for bug 2 this immediately showed `index=388 Put
ts={13257,3}` followed by `index=389 ReadCeiling ts={13257,3}`, an
*exact* tie between a *different* command type, which is what pointed
straight at `next_ceiling_candidate` rather than back at the mint/propose
race bug 1 had already fixed. A vaguer signal (just "ts inverted") would
have wasted time re-litigating the already-fixed bug; printing the actual
command types and indices at the failure point turned a second red
herring into a five-minute diagnosis. General rule: when a hard assert
fires inside a hot, generic loop (here, six near-identical `match` arms
each calling the same assert), instrument with enough context to
distinguish *which* case fired, not just that one did — cheap to add,
removed before committing, and often the single highest-leverage step in
the whole investigation.
Regression: `animus-cp-data/tests/prod_concurrent_ts_monotonic.rs` (a
real-thread `ProdEnv` test, confirmed to fail reliably against each of
the two unfixed states in turn by temporarily reverting just that piece
and re-running) plus `self_heal.rs` itself, now green. See
`animus-cp-data/CLAUDE.md`'s `propose_ordered`/`next_ceiling_candidate`
entries for the full mechanism.
**The generalizable lesson**: *an invariant spanning two locks is not an
invariant.* `Hlc`'s own mutex made minting monotonic in isolation;
`core`'s mutex made proposing (log-order) serial in isolation — but
nothing tied the two together, so "mint order == log order" was true by
coincidence under low contention and false under real concurrency. A
monotonic source feeding an ordered sink (a clock feeding a log, a
sequence number feeding a queue, a version counter feeding a commit) must
mint and enqueue in **one** critical section, or the two invariants each
hold individually while their composition doesn't. When reviewing code
that reads "compute X, then use X somewhere ordered," ask what stops a
second caller's "compute X" from running between those two steps — if
the answer is "nothing, but it's fine because X's own source is
monotonic," that reasoning is the bug.
