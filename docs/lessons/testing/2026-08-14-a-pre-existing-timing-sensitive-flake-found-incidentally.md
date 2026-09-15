# A pre-existing, timing-sensitive flake found incidentally while running the full workspace gate — not caused by, or related to, the change in flight — should be reported, not silently fixed or silently ignored.

**A pre-existing, timing-sensitive flake found incidentally while
running the full workspace gate — not caused by, or related to, the
change in flight — should be reported, not silently fixed or silently
ignored.** `animusd`'s `tests/dynamo_txn.rs::
transact_get_items_never_observes_a_torn_pair_under_concurrent_writes`
failed once under `cargo test --workspace`, then failed again roughly 1
in 4 *solo* re-runs (untouched by this PR's changes — a torn-snapshot
assertion in the ADR 0018 §2/PR7 `TransactGetItems` quiescence-retry
path, nothing to do with streams) — genuinely flaky on its own, not a
regression this PR introduced (confirmed by repeated solo runs both
passing and failing with identical code). Per this repo's own "separate
PRs for incidental bugs" convention, the fix belongs in its own change,
not folded into an unrelated PR's diff — but the *discovery* still
belongs in this log and in the reporting PR's own description, so the
next person who hits it doesn't have to re-derive "is this me?" from
scratch. (2026-08-14.)
**Baseline adjudication (round-3 PR8, so the eventual fix has numbers to
work against)**: solo re-runs of exactly this test — `main` 4/10, the
streams round-3 salvage boundary `064bbac` 4/10, `3b3c7ae` (PR7's tip,
also this PR's own base — no txn-path changes landed between them) 5/10.
Flat within noise across three points spanning the whole round-3 stack;
streams work never touched this path. Genuinely pre-existing, not
introduced or worsened by any PR in this stack.
**Update (2026-08-15, torn-pair-fix stack PR2) — the mechanism, and a
worse baseline that still isn't this PR's fault.** The torn-pair-fix
stack (PR1: `mint_pushed` clock-witnessing-runaway fix; PR2: this file's
`run_transact_get`/`quiescent_multi_get` uniform-single-shot-round fix,
ADR 0018 §2's newest amendment) targeted two *read-timing* mechanisms
that can produce a torn `TransactGetItems` snapshot. Both are fixed and
independently verified (a dedicated `SimEnv` regression,
`txn_serializable.rs::tight_pair_transactions_never_observe_a_torn_
snapshot`, 0 failures across 30+ seeds). Yet solo re-runs of *this* wire-
level test against the fixed stack still fail at a rate at least as high
as ever (PR1-only baseline: 7/10; PR1+PR2: 17/20) — debugging traced *why*,
not just confirmed *that*: the participant key ("b" in the test) simply
**stops receiving any further writes partway through the writer's loop**
(observed stuck anywhere from step 4 to step 14 of 15) while the anchor
key ("a") keeps committing correctly to the very end, and the writer's
own `TransactWriteItems` calls never see a failure throughout — i.e. this
is a **write-side 2PC participant-write-loss** bug (the participant's
own intent silently stops advancing while the coordinator keeps
reporting success on every subsequent step), structurally unrelated to
either read-side mechanism the torn-pair-fix stack closes. It reproduces
identically with zero of this stack's code present, confirming (again)
it is pre-existing, not introduced by either PR. **Still needs its own
root-cause delivery** (a "Bug 3," in the participant-stage/recovery-push
interaction, likely a duelling-decider-class race given how aggressively
a live reader's own recovery pushes can now fire — worth checking first
whether `ClientCtx::txn_prepare_pushing`'s `IntentBlocked` retry ever
itself pushes the blocking transaction, since today it only waits and
hopes something else clears it). Do not fold it into a future PR's diff
without its own investigation and acceptance evidence — same convention
as the entry above.
**RESOLVED (2026-08-15, torn-pair-fix stack PR3)** — not a duelling-
decider race after all: `ClientCtx::recovery_resolve` grouped a
transaction's participants by table name alone, misrouting a resolve to
the wrong tablet of a split table, and `KvCommand::TxnResolve` had no
apply-time fence to catch it (every *other* key-writing variant did).
See ADR 0018's 2026-08-15 amendment for the full mechanism/fix, and the
three entries below for the generalizable lessons this investigation
leaves behind.
