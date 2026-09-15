# A resolve's silent no-op is invisible to its own proposer — a write-side sibling of the read path's foreign-intent push, plus two bugs found auditing around it (issue #298, confirmed and closed 2026-08-27)

This round confirmed live the one residual the amendment above left open:
a fresh-`TxnId` retry (`TxnAbortReason::is_safe_to_retry_fresh`) hitting
`StageOutcome::IntentBlocked` on **its own coordinator's immediately-prior
attempt's** still-live intent. See ADR 0018's matching 2026-08-27 amendment
for the full account; this entry is the generalizable lessons.

**The confirmed mechanism, and the general shape it is an instance of**:
`ClientCtx::txn_resolve_participant`'s `CpRoute::Local` branch called
`leader.txn_resolve(..).await` and discarded its own `Option<HlcTimestamp>`
return, unconditionally reporting success. The underlying `KvCommand::
TxnResolve` entry had, in fact, silently no-op'd — its own `fence` check
rejected it because the target tablet's range had shifted (a concurrent
split) between the coordinator's `cp_route` lookup and the entry's actual
apply. **`RaftKvNode::txn_resolve`'s only signal is "did this entry apply,"
never "did it resolve anything"** — a fence-miss no-op and a genuine
resolve are indistinguishable to the caller, because `TxnResolve`, unlike
`TxnStage`, never got a `StageOutcome`-shaped per-attempt outcome channel.
**The general lesson: when one command in a family gets a "why did this
no-op" outcome channel because a bug demanded it (`TxnStage`'s
`StageOutcome`), audit every SIBLING command that can ALSO silently no-op
via the identical gate (a fence check, a seal, an OCC condition) for the
same missing channel — the fix that closed the first one does not
generalize automatically just because the mechanism looks similar.** This
gap is named, not fixed, in the ADR amendment (§3) — the fix that shipped
closes the one reachable symptom (a blocked stage) without adding the
larger channel, and a second, more severe consequence of the same gap
(§4 below) was found but NOT closed this round.

**The fix, and why it targets the symptom rather than the root gap**:
`StageOutcome::IntentBlocked` gained the blocker's own `record_key`/
`record_table` (already carried by every `Envelope::Intent`, exactly what
the READ path's `IntentInfo` already exposes for the identical reason) so
`ClientCtx::push_resolution_if_decided` — the write-side sibling of the
read path's `confirm_or_push`/`resolve_intent_given_status` — can query the
blocker's own decision and, if already `Committed`/`Aborted`, push its
resolution with FRESH routing (sidestepping the stale-fence race the
original resolve hit) before retrying the stage. This closes the reachable
symptom without touching `TxnResolve`'s apply arm at all — a smaller,
lower-risk change than adding an outcome channel, and correct regardless of
whether the root gap is ever closed.

**Two more real bugs found auditing the surrounding code the same way,
worth their own note**:

1. **A wrapping `format!` around an already-classified message is itself a
   classification bug.** `txn_prepare_pushing`'s exhaustion arm nested a
   retryable-shaped `Other` message (already proven safe to retry — it
   never even reached its own propose) inside new sentence text ending in
   `")"`. This silently moved the house `"; retry"` suffix `is_ambiguous`
   keys on to *before* the closing paren, and moved the message out from
   under `is_safe_to_retry_fresh`'s own `starts_with(...)` check — a
   message that was safe to retry a moment earlier was recorded a
   **definite** `CANCELLED` purely because of how the exhaustion text
   happened to be assembled. **The general rule this reinforces (already
   stated for `TxnAbortReason::Other` construction in this file's sibling
   entries, now shown to apply to any RE-wrapping of one too): any code
   that builds a new message around an existing classified one must
   preserve the classification, not just the informative content — the
   safest way to guarantee that is to not wrap it at all** (the fix here:
   pass the message through byte-for-byte, `Err(TxnAbortReason::
   Other(msg))`, rather than re-deriving a "did this preserve the suffix"
   invariant by hand every time the wrapper's own prose changes).
2. **An allowlisted-safe reason is not safe at every call site it can fire
   from.** `TxnAbortReason::is_safe_to_retry_fresh`'s own safety argument
   ("an anchor staged without every participant confirming can only ever
   be recovered as Abort") is true for a STAGE-time freeze but silently
   false for the IDENTICAL `FROZEN_REFUSAL` string reached from `cp_txn`'s
   own DECIDE step, once every participant has already staged — the
   premise "not every participant staged" doesn't hold there at all,
   because by construction of reaching that branch every one of them did.
   Retrying with a fresh `TxnId` there raced `txn_recover`'s own legitimate
   `all_staged`-driven commit for the ORIGINAL `txn_id`, reproducing the
   double-materialize signature this whole file's own prior entries already
   closed once. **The general lesson: a message-string-keyed allowlist
   predicate's safety argument is a claim about the CALL SITE the reasoning
   was built from, not about the string itself — the same string reached
   from a different call site can carry a different, unstated precondition.
   Audit every call site a shared error constant (`FROZEN_REFUSAL` here)
   can actually be reached from, not just the one the original bug report
   named.** Fixed by retrying the SAME decision (never a fresh `TxnId`) —
   `ClientCtx::txn_decide_anchor_retrying`, mirroring `cp_kind_write_item`'s
   issue #288 freeze-refusal retry shape — since retrying an idempotent
   decide can never abandon already-staged work the way a fresh identity
   would.

**A background passive safety net can rescue a broken regression test just
as easily as it rescues production traffic — and that is a test bug, not a
feature.** The first version of this round's own regression test called
the full `txn_prepare_pushing` (sleep-based retry loop included) and passed
identically whether `push_resolution_if_decided` was live or `return`-
stubbed to a no-op. Root cause: local single-voter leader election alone
ate roughly 750ms of the test's own setup, leaving just enough real
wall-clock time before the test's own final assertion for
`txn_resolver_loop`'s independent, unconditional one-second passive sweep
to win the race and clean up the deliberately-unresolved intent on its own
— a coincidence indistinguishable from the fix actually working, from the
test's own point of view. **The fix was to stop testing through the
timing-sensitive retry loop at all**: call the two `txn_prepare` attempts
directly (bypassing `txn_prepare_pushing`'s sleeps entirely) with the fix
under test invoked explicitly in between — deterministic, no `sleep`
anywhere in the critical path, and the whole test completes in well under
a tenth of the background sweep's own interval. **The general rule: a
`ProdEnv` regression test for a fix that races a background periodic task
must either complete fast enough that the periodic task provably cannot
have ticked yet, or must disable/bypass that task for the duration of the
test — "it passed" is not evidence the fix under test is what made it pass
unless something has ruled out every OTHER path to the same outcome,
background loops very much included.** Caught here only because this
round deliberately verified red-before-green by temporarily disabling the
fix and re-running — the same "temporarily invert the fix, confirm the
test fails" discipline this file's own Testing section already recommends
for retry-loop regressions, now shown to also catch a test that a
production safety net (not test flakiness) was silently propping up.

**Found, NOT fixed, and the reason the un-pin stayed blocked yet again**:
chasing the confirmed residual through further soak runs surfaced a
FOURTH, independent, and more severe failure — a `TransactWriteItems` call
that returned `Ok` outright (no retry, no ambiguity, nothing logged) for a
transaction whose own later `ConsistentRead: true` read of one of its own
keys came back completely empty. Leading, unconfirmed hypothesis: the same
`TxnResolve`-has-no-outcome-channel gap named above, but on the
COMMITTER's own resolve path rather than a blocked stage — under the
un-pinned soak's cascading-split cadence, every resolution attempt for one
committed write (the awaited `resolve_all_parallel` call, and
`txn_resolver_loop`'s own passive sweep afterward) could in principle keep
racing a fresh split before applying, each one silently no-op'ing and each
one reporting apparent success, so the write's own intent never actually
resolves anywhere while nothing ever signals a problem. Not root-caused
live — recorded here, per this file's own "capture the raw shape, don't
re-derive it" instruction, exactly so the next round does not have to
start from zero. **Soak result (41 runs total across the fix-confirmation
and gate phases, tallied honestly rather than stopping at a clean-looking
subset): the two sibling bugs (message-wrapping, decide-time-fresh-retry)
did not recur even once once their own fixes landed. The confirmed
self-conflict `TransactionConflict` residual DID recur once more, 17 clean
runs into the formal gate, even with the write-side push active** — the
captured trace shows the identical false-success resolve shape recurring a
second time on the same key, but at a log level that didn't capture
whether the push itself found the blocker unconfirmable or hit the
identical fence race on its own resolve attempt; not root-caused to that
depth, recorded for the next round rather than guessed at. **This new
"acked write lost" residual also recurred** (verified in isolation, a
genuine reproduction, not host-contention noise) — `SplitMode::InPlace`
stays not un-pinned, and the write-side push should be read as a real,
tested narrowing of the residual's reachable surface, not a categorical
close of it; `TxnResolve`'s own missing outcome channel is the more
durable fix a future round should reach for instead of another
individually-discovered-symptom patch.

**Amendment (2026-08-29): the outcome channel above shipped, and it surfaced
a SECOND, independent bug hiding behind the same missing signal.**
`KvCommand::TxnResolve` now records a `ResolveOutcome` (`Resolved`/
`Fenced`/`OutcomeMismatch`) per apply, keyed by Raft log index and paired
with the entry's own term — the exact `StageOutcome`-shaped channel this
entry's own closing paragraph named as the durable fix, built the same way
`CasResults`/`StageOutcomes`/`KindBatchOutcomes` already are (see this
file's own entries on those three for the shared term-identity doctrine).
`RaftKvNode::txn_resolve` now returns `Option<(HlcTimestamp,
ResolveOutcome)>`; `animusd::ClientCtx::txn_resolve_participant` returns
`Result<ResolveOutcome, String>`; a new bounded-retry wrapper
(`txn_resolve_participant_retrying`) re-resolves routing **fresh** on every
attempt when the outcome comes back `Fenced`, instead of the old behavior
of treating any applied entry as done.

**The second bug, found wiring the channel in, not in a soak**: the apply
arm's own `TxnTracker::unresolved_decided.remove(&txn_id)` call — which is
exactly what `txn_resolver_loop`'s passive per-second sweep reads to find
a decided-but-still-unresolved transaction to keep pushing — ran
**unconditionally**, before the entry's own fence/mismatch outcome was even
computed. A `Fenced` resolve therefore erased this group's own memory that
the transaction still needed resolving, in the identical tick the fence-miss
happened — the passive safety net had already given up on exactly the
transaction it exists to rescue, with no way to tell from the outside. This
is a second, independent, concrete mechanism behind the "resolve reports
success but the intent stays live" shape this whole entry chases, found
purely by reading the apply arm's own code once the new outcome value gave
something to condition the clear on — **the general lesson: adding an
outcome channel to an apply arm is also the moment to re-audit every OTHER
side effect that same arm already performs unconditionally, since "this
entry applied" and "this entry did what it meant to" were conflated
everywhere in that arm, not just in the one return value with a name**.
Fixed by gating the `remove` on `resolve_outcome == ResolveOutcome::
Resolved`.

**A pre-existing regression test's own unstated assumption broke the moment
this landed, which is the correct outcome, not a regression to work
around.** `animus-cp-data/tests/txn_recovery.rs::
pending_txns_reflects_applies_across_restart` resolved with `TxnOutcome::
Committed { commit_ts: txn_id.ts }` — the pre-decision *candidate*
timestamp, not the real decided value `commit_at_least` actually returns
(`mint_at_least` mints strictly above the candidate, so the two are
essentially never equal) — and asserted `unresolved_decided` cleared
afterward. Before this fix, the resolve's own `outcome_mismatch` no-op
still got treated as "done" by the then-unconditional clear, so the test
passed for the wrong reason: it never actually exercised a genuine resolve
at all. After the fix, the same stale value correctly no-ops
(`OutcomeMismatch`) and leaves the tracker untouched, failing the
assertion — not because the fix is wrong, but because the test was
silently relying on the exact bug this round closed. **The general lesson,
sharpened from this file's own recurring theme: a test asserting an
eventual-consistency-shaped postcondition ("X eventually clears") can pass
for years on a value that's subtly wrong, as long as something ELSE
(here, an unconditional side effect one call away) makes the postcondition
true regardless of whether the call under test actually did its job —
fixing the masking bug is what makes the test start asserting what it was
always supposed to.** Fixed by resolving with `commit_at_least`'s own
returned ts (sound specifically because this test's own scenario has no
concurrent recovery decider to race — the general rule the crate's own
docs already state, that a caller must re-read the record's *actual*
status rather than trust a propose call's own return, still applies
everywhere a second decider is possible).

New regression at the primitive level: `animus-cp-data/tests/
txn_resolve_outcome.rs` (a real in-place split forking the **participant's**
own tablet between the anchor's commit and the participant's resolve,
proving `Fenced` and — since the participant holds no local copy of the
anchor's record to reconstruct the value from — that the key stays
genuinely unreachable, not silently marked done). **Still not attempted**:
the mandated fresh 30-run un-pinned `SplitMode::InPlace` soak — this round
closes the structural gap the soak's own investigation named as the
clearest path forward, but per this file's own "any failure keeps the pin"
discipline, only that soak can actually move the pin. See ADR 0018's
2026-08-29 amendment and ADR 0058's matching note.
