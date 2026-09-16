# Issue #298, a fifth shape: two candidate mechanisms found, neither confirmed as root cause (2026-08-26)

A fresh investigation (ADR 0058's G5 row, all four previously-fixed #298
mechanisms already on `main`) re-ran `streams_e2e.rs::multi_split_soak_
streamed_gsi_table_under_mixed_load` un-pinned from `SplitMode::Copy` to the
default (`InPlace`) 21 times. 4/21 (~19%) failed, in two distinct shapes —
consistent with the rung-4-layer-2 entry's own point that in-place's higher
splits-per-budget just gives a rare pre-existing race far more rolls of the
dice, not a new trigger condition.

**Shape A (2 runs) — the literal "shape 5" this investigation was scoped
to**: the delivered count came back *over*, not under, expected
(`delivered=146/144`), and one member of a transactional write pair
appeared **twice under the SAME shard (tablet+epoch)** with two distinct
sequence numbers — a genuine double-append into the hot change log before
either copy was ever sealed, not a seal-boundary race (which would show as
two *different* epochs, per mechanism (2)'s own already-fixed signature).
In one of the two runs, the OTHER member of the identical pair *did* show
that already-fixed cross-epoch signature in the same run — the clean
interpretation is one shared underlying event (the transaction's own
resolve running more than once) landing on two different tablets, one of
which happened to have a seal race between the two applies (cross-epoch)
and one of which didn't (same-epoch, hence "shape 5"'s literal signature) —
not two independent bugs coincidentally co-occurring.

**Shape B (2 runs) — found by the same soak, not previously documented
under #298**: `ClientCtx::txn_recover`'s in-doubt-recovery sweep decided
**Abort** (diagnostic: `all_staged=false`) for a transaction whose write the
test's own client had already recorded as acked (a `TransactWriteItems` 200
response) — permanently losing that item (one run failed the immediate
`ConsistentRead: true` read-back; a second, independent run failed the same
way further downstream, via the lineage-delivery deadline timing out one
record short). This is a live instance of the "duelling decider" hazard ADR
0018 §2/PR5's own amendment names as *legal* — but that legality rests on
both deciders reaching an objectively correct decision from independently
verified state, an assumption a false-negative verify breaks.

**Method**: `crates/animus-test/tests/stream_lineage_corpus.rs` (the
`ANIMUS_STREAM_SEEDS` corpus) was checked first, per this round's own
"SimEnv repro is worth more than a ProdEnv soak hit" instruction, and ruled
out as a repro vehicle for this specific bug — it drives the copy-based
split's lineage *purely at the `Metadata`/`MetaCommand::BeginSplit`/
`CutoverSplit` level* (`complete_copy_split`), never through `animusd`'s own
async orchestration (`cp_txn`, `txn_resolver_loop`, `resolve_all_parallel`'s
timeout) where this bug's candidate mechanisms live — and `animusd` itself
has no `animus-sim` dependency at all (already named as a gap in ADR 0058's
G5 row for mechanism (2)), so a deterministic seed-reproducible repro of
this specific race is not reachable without new cross-crate test
infrastructure, out of scope for this pass. The soak itself reproduced
fast: ~30s/run, first hit on run 5 of the first 7 unpinned attempts.
Temporary `eprintln!` instrumentation (propose/resolve/recovery tracing at
`cp_txn`'s participant-stage-error, abort-vs-actual-outcome, awaited-resolve
timeout, and `TxnStage`'s apply-time "would this re-stage land on an
already-Committed value" check; `txn_recover`'s own decide call) — modeled
on this file's own "land a permanent diagnostic before any fix attempt"
entry above, but kept local to the investigation and reverted before
committing, never shipped — captured shape B live (the exact `all_staged=
false`/`proposed=Aborted` sequence immediately preceding the lost-write
panic) across ~15 further runs, but never caught a live full causal chain
for shape A's own double-materialize, and the "re-stage over an
already-Committed value" diagnostic never fired in any captured run either
— so the candidate mechanism below for shape A is a **confirmed code gap
via reading**, not a confirmed root cause via a captured trace.

**Two structural gaps found, presented as candidates for the next
investigation, neither fixed this round (no speculative fix, per this
round's standing rule)**:

1. `KvCommand::TxnStage`'s apply arm (`crates/animus-cp-data/src/lib.rs`,
   the `blocked_by` computation) only rejects a stage that would overwrite a
   *different* transaction's currently-unresolved `Envelope::Intent` — it
   never checks whether the target key's current value is already
   `Envelope::Committed`. Same-txn re-staging is documented as deliberately
   unaffected ("a WAL-replay re-application"), but nothing distinguishes
   that from a genuinely late/duplicate `TxnStage` propose landing *after*
   its own transaction has already fully resolved: such a propose would
   silently resurrect the key from `Committed` back into `Intent`, and a
   later resolve (the normal flow, the resolver-loop safety net, or a
   recovery push) would then re-run `materialize_derived` a second time, at
   a fresh HLC — the literal "two distinct sequence ids for one write"
   signature. (The per-key resolve path itself, checked carefully during
   this investigation, *is* idempotent once a value is genuinely
   `Committed` — `TxnResolve`'s own apply only materializes when the target
   key currently holds `Envelope::Intent` naming that exact `txn_id`; the
   gap is specifically that `TxnStage` has no equivalent guard preventing a
   value from re-entering `Intent` state to begin with.)
2. `ClientCtx::txn_recover`'s `all_staged` loop (`crates/animusd/src/
   lib.rs`) folds `Ok(false)` (genuinely not staged) and `Err(_)` (the
   verify call itself failed — most commonly `txn_verify`'s own
   `"no CP group leader reachable"`/forwarding error, exactly what a
   participant's tablet mid-fork/cutover produces routinely) into the same
   `all_staged = false` bucket. A transient routing failure during exactly
   the high-split-cadence window this soak stresses gets treated identically
   to a permanent "never staged" fact, which can push recovery to Abort a
   transaction whose coordinator (`cp_txn`) is concurrently deciding — or
   has already decided — Commit from its own, unaffected view.

**General lesson**: an exactly-once investigation under a much-higher
split cadence should expect **more than one** symptom shape to fall out of
the same soak run, not just the one shape it was scoped to chase — shape B
here was found purely because the same instrumented soak was run enough
times to surface it, not because it was anticipated. Treat every distinct
panic/diagnostic signature a soak produces as its own data point even when
only one was the assignment, and say so explicitly in the writeup rather
than only reporting against the originally-named shape. Related to the
"an intermittent deficit... should get a permanent on-failure diagnostic
landed... before any fix attempt" entry above: here the diagnostic was
temporary and reverted (this round's own instruction), which is the right
call for an exploratory pass, but it means shape A's own root cause is
still only a well-argued candidate, not a captured fact — a durable,
committed diagnostic (gated by an env var or `#[cfg(test)]`, never firing
in production) is the natural next step before the next investigation round
attempts a fix.
