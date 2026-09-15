# A safety-critical retry classification must be an allowlist of proven-safe cases, never a denylist of known-dangerous ones (issue #298 "deep shape A", same round)

The fix above went through three shapes before it was safe, and the
difference between the second and third is the more important lesson of
the two. **First shape**: retry every ambiguous `cp_txn` outcome
unconditionally. Reproduced the target bug immediately (a fresh `TxnId`
racing an original that had, in fact, already fully committed via a path
the retrying call never observed) — expected, this shape was never meant to
ship. **Second shape**: retry every ambiguous outcome EXCEPT a small,
explicitly-named denylist of the two message shapes then believed to be
the only genuine confirmation losses (`"CP group leader moved during
{anchor,participant} stage"`). This shape looked principled — it was
built by tracing the actual apply-time code paths, not guessed — and it
still reproduced the identical `delivered=146/144` duplicate-pair
signature on essentially the next soak run. **The denylist had missed a
whole call-site FAMILY**: `resolve_all`'s own DECIDE-phase confirmation-
loss messages ("CP group leader moved during anchor commit/abort", "after
decide", "during orphan abort", and their forwarded-hop twins) were never
enumerated, so by construction of "retry unless denylisted" they fell
through to "safe to retry" — and a confirmed DECIDE, unlike a confirmed
STAGE, fully materializes every participant's derived writes, so retrying
one with a fresh identity is exactly the race being closed. **Third shape,
the one that shipped**: invert the predicate into an ALLOWLIST — name only
the reasons proven safe (occurring before any propose for this transaction
could have applied at all), and let everything else, known or not-yet-
discovered, default to the conservative, never-retried path.

**Why this generalizes beyond this one bug**: a denylist and an allowlist
look symmetric in code (`if excluded { deny } else { allow }` vs. `if
included { allow } else { deny }`) but are not remotely symmetric in
failure mode. A denylist's blind spot is silent and unbounded — any call
site this file's author didn't happen to trace, present today or added by
someone else next month, is automatically "safe" the moment it starts
emitting the same `"; retry"` convention every other transient error here
already uses, with no compiler warning and no test failure until a soak
happens to exercise it. An allowlist's blind spot is loud and bounded — an
unrecognized reason degrades to the SAFE default (here, "leave the
idempotency record `PENDING`, self-heal via TTL"), which can cost latency
but never correctness, and is trivially auditable by reading one function
top to bottom rather than having to prove a negative across the whole
codebase. **The general rule: when classifying inputs for a safety-
relevant decision (retry-or-not, trust-or-not, allow-or-not) where the two
outcomes have asymmetric cost — one side is merely slow, the other side is
silently wrong — enumerate the cheap, provably-safe side and default
everything else to the expensive-but-safe side, never the reverse.** This
is the identical shape as `Self::is_confirmation_loss`'s own doomed first
draft mirrors: a denylist of "known-dangerous" strings is a poor substitute
for a real characterization of *why* a case is safe, since a denylist can
only ever encode what's already been found, and one soak run is all it
took to find the gap here — see `TxnAbortReason::is_safe_to_retry_fresh`
(`crates/animusd/src/lib.rs`) for the shipped allowlist and its own unit
test for the denylist's exact counterexamples now pinned as regressions.

**Soak result with the shipped (allowlist) design: 24/25 clean, one residual
found, soak stays pinned to `Copy`.** Two isolated, contention-free batches
(15 runs, then a further 10 chasing the one failure with a targeted
diagnostic that did not happen to recur) totalled 24 clean runs and one
failure — none of the earlier double-materialize signature, and none of the
old lineage-delivery-timeout residual either. The one failure was a
**genuine, non-ambiguous** `TransactionCanceledException` ("cached
cancelled outcome for this ClientRequestToken") on a transaction the soak's
own writer never conditions and never reuses a key for — meaning either a
real `ConditionFailed` (impossible by construction here) or a real
`TransactionConflict` (`IntentBlocked` exhausted). **Leading hypothesis, not
confirmed**: `is_safe_to_retry_fresh`'s own allowlisted retries mint a
FRESH `TxnId` per attempt; if one such attempt's `Fenced`/frozen-refusal
classification is a false negative for one specific replica's own read
(i.e. the stage in fact partially applied on some participant despite the
coordinator seeing a provably-safe-shaped refusal), a SUBSEQUENT retry's
own fresh `TxnId` could hit that first attempt's still-unresolved intent on
the same key and exhaust `TXN_STAGE_PUSH_ATTEMPTS`' worth of
`IntentBlocked` pushes before `txn_resolver_loop`'s passive sweep ever gets
a chance to clear it (`TXN_STAGE_PUSH_ATTEMPTS`/`TXN_STAGE_PUSH_BACKOFF`'s
combined budget, 3 × 250ms, is far shorter than `RECOVERY_GRACE`'s 5s) —
a genuine `TransactionConflict`, correctly definite from `cp_txn`'s own
point of view, but a self-inflicted one this feature's own retry loop
created rather than a pre-existing race. **Not confirmed live** — this
round's time ran out before a diagnostic could capture the exact
`TxnId`/key of a reproduced instance — recorded here, per this file's own
standing "capture the raw shape, don't re-derive it" instruction, so the
next investigation starts from a real hypothesis instead of from zero. If
confirmed, the fix is likely narrow: either shorten
`is_safe_to_retry_fresh`'s own backoff so a fresh retry only fires after
giving the FIRST attempt's own possible partial effects a chance to
resolve, or have `txn_prepare_pushing`'s `IntentBlocked` handling
recognize "blocked by a `TxnId` this same coordinator minted moments ago
for the identical logical request" as a signal to wait longer rather than
exhausting the ordinary cross-transaction contention budget. **Genuinely
correctness-safe either way**: a real `TransactionConflict` is a definite,
non-ambiguous abort — `run_transact` correctly records `CANCELLED` for it
and never risks a double-materialize; the residual is a spurious-failure/
liveness cost, not a data-safety one, unlike the bug this round's own fix
closed.

**Un-pin decision: NOT taken.** Per this task's own standing instruction, a
single failure in the mandated clean-run bar means the `SplitMode::InPlace`
soak stays pinned to `Copy` — `start_streamed_cluster_full_copy_pinned`'s
pin is unchanged from `main`. Rung 4's remaining copy-workflow-deletion
layer stays blocked on this one residual, down from the three this round
began with.
