# A correctness fix that changes record SIZE broke a sibling test's hardcoded-epoch assumption, caught only by the final full-suite gate (ADR 0059 §9/§10, Train 3 PR②)

Fixing `table_change_records_carry_images` to include PITR (recorded above:
a PITR-only table used to get image-less markers, which PITR replay cannot
reconstruct content from) is unambiguously correct and necessary. It has a
side effect nothing in that fix's own review caught: every change record on
a PITR-enabled table is now a **full item image** instead of a tiny marker
— dramatically bigger. `index_drain.rs`'s own pre-existing
`pitr_seal_happy_path_and_disable_reenable_continues_epoch_chain` test (from
Train 3 PR①, unmodified by this PR) writes 10 padded items against a
`seal_bytes: 200` threshold and then hardcodes `pitr_segments[&(tablet, 1)]`
as "the seal that happens after re-enable." With markers, the whole 10-item
burst apparently stayed within one seal (epoch 0); with full images, the
same burst legitimately crosses the 200-byte threshold **three times**
before the test ever reaches its own disable call (epochs 0, 1, and 2 all
minted under generation 1, confirmed by instrumenting the test directly)
— so the test's own hardcoded `(tablet, 1)` lookup found a stale
pre-disable row instead of the new post-re-enable one, and asserted the
wrong generation. This was caught only by this task's own final full
`cargo test -p animusd --lib --tests -- --test-threads=1` gate — neither
`cargo test -p animusd --lib` scoped to the new code, nor this PR's own new
e2e suite, exercises that pre-existing test at all, and its silent
pre-existing passing state gave no signal that its own assumption had
become load-bearing on marker-sized records specifically.

**The fix**: make the test observe reality instead of asserting a specific
epoch number — capture "whatever the chain's own current tip epoch is"
immediately before the disable call, then wait for and assert against
`tip + 1` afterward, exactly mirroring what the analogous stream test
(`disable_final_seal_then_reenable_continues_the_epoch_chain`) already does
by using a deliberately *huge* `seal_bytes` so its own burst never
size-triggers on its own — a pattern worth copying directly rather than
reinventing when writing a new disable/re-enable epoch-continuity test.

**The generalizable rule**: a test that hardcodes a specific epoch/index/
sequence number as "the Nth thing to happen" is implicitly asserting a
fixed number of trigger firings from a fixed byte budget — a completely
independent, unrelated correctness fix that changes per-record *size*
(image vs. marker, a longer key, an added field) can silently invalidate
that count without changing anything the two features would ever be
reviewed together for. Prefer asserting against a *chain's own observed
tip* (or an explicitly huge/disabled trigger threshold, forcing the seal to
happen only at the deliberate point the test controls) over a literal
epoch/sequence number whenever the trigger is size- or count-based rather
than a single explicit action. And: run the full existing test suite (not
just new/directly-touched tests) before considering ANY change to a shared
gating predicate (`table_change_records_carry_images` here) complete — a
predicate widening is exactly the shape of change whose blast radius is
everything downstream of its own `true` branch, not just the feature that
motivated it.
### Amendment (2026-08-26, later the same day): shape B confirmed and fixed; a second, sibling conflation found and fixed alongside it

A follow-up round re-instrumented both candidate sites from the entry above
(temporary `eprintln!`s, reverted before this amendment's own commit) and
re-ran the un-pinned `SplitMode::InPlace` soak. **Shape B fired and was
captured live within the first ~12 runs**: the exact predicted sequence —
`ClientCtx::txn_verify` returning `Err` for a participant span, folded into
`all_staged = false`, immediately followed by `txn_recover` proposing
`Aborted` and the test's own "acked write lost" panic on the identical item.
Confirmed mechanism: `txn_recover`'s `all_staged` loop (`crates/animusd/src/
lib.rs`) treated `Ok(false)` (genuinely not staged) and `Err(_)` (the verify
query itself failed, most commonly a transient routing hiccup while a
participant's tablet is mid-fork/cutover) identically. **Fixed** by
separating them: any `Err` now makes the whole push **inconclusive** —
`txn_recover` declines (returns `Pending`, proposes nothing) instead of ever
letting an unconfirmed span feed a decision. A new `txn_resolver_loop`-local
grace tracker (mirroring `unresolved_decided`'s own lookup-failure tracker,
issue #298 residuals commit) logs+meters once (`Metric::
CpTxnRecoveryStuckInconclusive`) if a transaction stays inconclusive well
past `RECOVERY_GRACE`, purely a liveness signal — correctness never depends
on it firing.

**A sibling conflation, found while chasing a SECOND "acked write lost"
recurrence after the first fix landed**: `RaftKvNode::txn_record_view`
(`animus-cp-data`) — the primitive `txn_recover`'s *orphan-record* branch
reads to decide "does no record exist at all, past grace, so an abort
tombstone is safe" — had the exact same shape of bug, one level up: its
plain `Option` return conflated "not served" (this replica's own read
barrier failed, e.g. mid-fork/cutover) with "genuinely no record" into the
same bare `None`. `ClientCtx::txn_record_view`'s wrapper turned *any* `None`
into `Err`, and `txn_recover`'s orphan branch treated *any* `Err` as
license to proceed toward an orphan-abort decision once past grace — so a
transient barrier failure on this ONE read could synthesize an abort
tombstone for a transaction whose record was fine (staged, or already
committing) and simply unreachable by this one query attempt. This is the
**general lesson generalized**: an "an `Err`/`None` from a query is UNKNOWN,
never evidence of absence" audit must cover *every* query a decision is
built on, not just the first one found — a fix scoped to the `intent_spans`
verify loop alone left an identically-shaped gap one level up in the same
function. **Fixed** by widening `RaftKvNode::txn_record_view` to the
existing `stale_get_served`/`linearizable_get_served` "served" discipline
this crate already uses elsewhere: `Option<Option<TxnRecordView>>` — outer
`None` = not served (decline), `Some(None)` = definitively no record
(the ONLY value that may feed an orphan-abort decision), `Some(Some(view))`
= found. Propagated through `ClientCtx::txn_record_view` (now `Result<Option
<TxnRecordView>, String>`, not `Result<TxnRecordView, String>`) and the
`ClientRequest::TxnRecordView`/`ClientResponse::TxnRecordViewReply` wire
pair (`view: Option<TxnRecordView>`, `TxnRecordView` gaining `Serialize`/
`Deserialize`). Every caller that only ever collapsed both `Option` layers
for a best-effort read (the `/admin/txns` diagnostic view, `animus-cli`'s
raw-reply printer) keeps doing exactly that — `.flatten()` is correct there,
since nothing downstream makes a safety decision off the distinction; only
`txn_recover`'s own two call sites needed to keep the layers apart.

**Method note**: a captured live trace matching a predicted symptom exactly
(diagnostic fires, panic follows immediately, same item) is strong enough
confirmation to fix from directly — no need to *also* chase a fully
deterministic `SimEnv` repro when the mechanism is this legible from one
clean trace, especially given `animusd` has no `animus-sim` dependency at
all (a standing gap this round didn't attempt to close). The nearest thing
to a deterministic regression is `animus-cp-data/tests/
txn_record_view_served.rs`, proving the FIXED primitive's own "served"
contract directly (a genuinely absent key answers `Some(None)`; a deposed/
partitioned leader's own barrier failure answers the outer `None`; a real
staged record answers `Some(Some(view))`) — and `animus-test/tests/
txn_serializable.rs`'s own `push`/`resolver_tick` (a SimEnv-based
reimplementation of this exact recovery protocol, ADR 0018 §4 PR6) was
independently carrying the identical two conflations in its own test-double
logic; both were fixed to match, closing the gap for that corpus's own
future fault-injection scenarios too, even though today's frozen scenario
set never happened to trip either one.

### Amendment (2026-08-26, later still): shape A's literal double-delivery confirmed live — but its real trigger is a THIRD, deeper mechanism than either candidate named

The same re-instrumented soak that caught shape B (above) also caught
shape A's own literal signature directly: `delivered=146/144`, with
`x0079a` appearing **twice under the identical shard** (tablet 67, epoch
0, two distinct sequence numbers) and its transactional sibling `x0079b`
appearing twice **cross-epoch** (the already-fixed mechanism-2 pattern) —
exactly the "one shared underlying event manifesting two ways depending on
whether a seal happened to land between the two applies" shape the
original investigation predicted. The `TxnStage`-over-`Committed`
diagnostic fired for both keys at the moment of the resurrecting stage,
confirming the structural gap named in the base entry above is real and
reachable.

**But tracing the captured `txn_id`s through the trace showed the live
mechanism is narrower AND deeper than either original candidate**: the
*resurrecting* stage used a genuinely fresh `txn_id`, distinct from
whatever transaction first wrote `x0079a`/`x0079b` (never captured, since
the diagnostic only fires on the resurrecting attempt) — this is not "the
same transaction re-staging its own already-resolved key" (the shape the
`blocked_by` gap's literal wording suggested), it is **two independent
transactions, each individually legitimate from `KvCommand::TxnStage`'s
own narrow point of view, racing to write the identical logical item**.
The only way that happens for ids the workload never reuses is a
**client-level retry**: `dynamo_retrying_transact`'s own retry loop
resubmits `TransactWriteItems` (an un-tokened call, exactly DynamoDB's own
documented duplicate-execution risk) whenever the response is a 500, or a
400 whose message carries the house `"; retry"` convention — and
`cp_txn`/`txn_prepare_pushing`'s own error messages for a **confirmation
loss** (`"CP group leader moved during participant/anchor stage/commit;
retry"`, minted whenever `wait_stage_outcome`/the leader handle races a
leadership change mid-poll) carry that exact suffix. Unlike a
`StageOutcome::Fenced` outcome — provably a no-op, since the apply-time
gate rejected it before anything landed — a confirmation loss proves
**nothing** about whether the underlying stage/commit actually applied; it
only means *this* call couldn't learn the answer. A retry that mints a
fresh `txn_id` after a confirmation-lost first attempt that in fact
succeeded races its own already-committed work, and `KvCommand::
TxnStage`'s missing already-Committed check (the base entry's own finding)
is exactly what lets the retry silently win instead of being rejected.

**This is the general lesson from the shape B amendment above, at a third
layer**: "an unconfirmed outcome is UNKNOWN, never evidence of a specific
result" needed applying not just to `txn_recover`'s own two queries, but
to the **coordinator's own error-reporting convention** — marking a
confirmation-loss message retryable with the same blanket `"; retry"` tag
a provably-safe `Fenced` refusal uses conflates "retry costs nothing" with
"retry might race your own prior success." Not fixed this round (a
coordinator-side fix — verify-before-erroring on a lost confirmation,
mirroring the self-verification `txn_prepare_pushing` already does for the
*known* `IntentBlocked` case — is a genuinely new, substantial mechanism,
out of scope for a same-round stacked fix per this repo's own "an
incidental bug gets its own PR" convention); see ADR 0018's shape B
amendment §6 for the pointer carried forward.

**What WAS fixed this round, on its own merits, regardless of not being
the live trigger**: `KvCommand::TxnStage`'s apply arm now rejects a stage
targeting a key that THIS EXACT `txn_id` already resolved on this group
(`TxnTracker::recently_resolved`, a bounded best-effort seatbelt, checked
by `(key, txn_id)` identity — the same "never trust an outcome without
confirming it names the SAME thing" discipline the `KindBatchOutcome`
false-ack fix established). This closes the narrower same-txn resurrection
the original candidate mechanism's wording named (a genuine gap, still
worth closing even though it wasn't what fired in the captured trace) and
is red/green proven directly:
`animus-cp-data`'s in-crate `pr5_orphan_and_resurrection_tests::
a_resolved_key_rejects_a_same_txn_restage_issue_298_shape_a`. **It does
not close the live trigger** — a genuinely different, fresh `txn_id`
staging over an already-`Committed` value from an unrelated (or, as here,
duplicate-client-retried) transaction is the ordinary, correct write path
and must keep succeeding; only same-identity resurrection is rejected.

**Method note, reinforcing the base entry's own lesson**: tracing a
captured diagnostic's own identity fields (here, `txn_id`) all the way
through — not just confirming the diagnostic fired at the predicted
symptom — is what separated "the named candidate mechanism is confirmed"
from "a structurally real but not-the-live-cause gap, with the ACTUAL live
cause one layer further out." A diagnostic firing at the right place and
the right time is necessary but not sufficient evidence that the
candidate mechanism it was built to catch is the one actually operating.

### Amendment (2026-08-26, the proof soak itself): both fixes hold, but a fourth residual keeps the pin in place — soak NOT un-pinned

With both fixes above landed, the mandated 30-run un-pinned `SplitMode::
InPlace` proof soak was run in several batches (some contaminated by
concurrent `cargo build`/`cargo test` invocations on the same host, which
measurably inflated the failure rate — a real methodological trap worth
naming on its own: **a real-thread `ProdEnv` soak's failure rate is not
trustworthy while anything else on the host is competing for CPU**; the
election timers, `RECOVERY_GRACE`, and split-cadence knobs this soak
depends on are all wall-clock-relative, so host contention can manufacture
timing-sensitive failures indistinguishable from real ones without a
controlled re-run). Under genuinely contention-free conditions across
~70 total runs: the two fixed mechanisms (shape A's literal resurrection,
shape B's wrong-abort-from-unconfirmed-query) **did not recur even once**
with instrumentation re-armed and watching for them directly. But the
soak is still not clean — three residual failure categories remain, at a
combined rate noticeably lower than the pre-fix ~19% baseline but not
zero:

1. **The already-documented "deep shape A" mechanism** (this entry's own
   prior amendment): a client-level retry of an un-tokened
   `TransactWriteItems` racing its own already-committed first attempt.
   Not fixed this round, named there.
2. **The pre-existing lineage-delivery-timeout residual** (ADR 0058's G5
   row, present since before this round): `drain_all_tablets_lineage`
   hits its own deadline one or a few records short, distinct from the
   exactly-once assertion itself — a slow convergence, not a lost or
   duplicated write. Unrelated to shapes A/B; already an acknowledged open
   item this round did not scope in.
3. **A newly-observed, NOT-yet-root-caused variant, caught live with the
   fix's own instrumentation still watching**: `txn_recover`'s
   `all_staged` loop computing a **genuine, non-inconclusive**
   `all_staged=false` (every `txn_verify` call returned an affirmative
   `Ok`, at least one `Ok(false)` — the exact case this round's fix
   correctly leaves alone, since a confirmed negative is real evidence)
   for a transactional pair, immediately coincident with (in one capture)
   an "acked write lost" panic on the same item, and (in a separate,
   otherwise-passing run) with no visible ill effect. Both captures share
   an unexplained structural oddity worth flagging for whoever picks this
   up next: `view.intent_spans` held **two** entries covering **both**
   members of what the workload only ever issues as a plain two-item
   `TransactWriteItems` (one anchor + one participant, per `cp_txn`'s own
   construction — `participant_spans` is built strictly from `groups`
   *after* the anchor's own group is removed) — meaning the record's own
   anchor was apparently a **third**, distinct key neither of the two
   panicking/logged item ids, which this round could not identify before
   time ran out. Plausibly the same "deep shape A" client-retry family
   (a correctly-aborted abandoned first attempt, with the eventual
   "acked write lost" panic actually caused by a *different*, successful
   retry attempt racing it) rather than a fourth independent mechanism —
   but this is a hypothesis, not a confirmed account, and is recorded here
   precisely so the next investigation starts from the raw captured shape
   instead of re-deriving it. Two raw captures (redacted only for length):
   `all_staged=false`, `intent_spans` = point-spans for both `x0069a` and
   `x0069b` immediately before the soak's own "acked write x0069a lost"
   panic; and the structurally identical shape for `x0029a`/`x0029b` in an
   otherwise-clean run.

**Per this round's own instructions, none of these three residuals is
fixed here, and the soak stays pinned to `SplitMode::Copy`** — un-pinning
requires 30 *clean* runs, and none of the three most recent honest
attempts (uncontaminated by host contention) reached that bar. Rung 4's
remaining copy-workflow-deletion layer stays blocked on all three,
exactly as it was blocked on shape A/B before this round root-caused two
of what turned out to be (at least) five distinct mechanisms sharing the
same soak.
