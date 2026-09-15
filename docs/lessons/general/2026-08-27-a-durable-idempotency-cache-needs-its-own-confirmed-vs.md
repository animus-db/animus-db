# A durable idempotency cache needs its OWN confirmed-vs-unconfirmed audit — the recovery path that reads state isn't the only place that writes it (issue #298 "deep shape A", closed 2026-08-27)

This round closed the one residual named — and deliberately left open — by
this file's own two amendments above: **residual 1, a client-level retry of
an un-tokened `TransactWriteItems` racing its own already-committed first
attempt.** See ADR 0018's 2026-08-27 amendment for the full account; this
entry is the generalizable lesson.

**The bug, once found, was almost embarrassingly close to the two this file
already fixed twice over**: `dynamo.rs::run_transact`'s `ClientRequestToken`
idempotency preflight (already shipped, 2026-08-24 amendment — a durable
`token → outcome` record, conditionally claimed so the transaction executes
at most once per token regardless of bookkeeping) recorded **every** `cp_txn`
failure as a confirmed `CANCELLED`, including a genuinely ambiguous one (a
leader move mid stage, no leader reachable at all, or a `StageOutcome::
Fenced` naming a concurrent in-doubt-recovery decision) where the
transaction might have committed via a path that exact call never observed.
This is the identical "an unconfirmed `Err` is UNKNOWN, never evidence of a
specific outcome" defect this file's two amendments above already fixed in
`RaftKvNode::txn_recover`'s `all_staged` loop and, one level deeper, in
`RaftKvNode::txn_record_view` — found a **third** time in a completely
different function, written by a different amendment, three days apart.

**The generalizable lesson**: when a system gains a durable idempotency/
outcome cache sitting in front of an existing fallible operation, the cache's
own write path needs the identical confirmed-vs-unconfirmed audit the
*read*/*recovery* side already got — auditing `txn_recover`'s two queries
alone (as both prior amendments did, carefully) left an identically-shaped
gap sitting in the ONE OTHER function in the codebase that also decides
"did this transaction commit or not" from a fallible call's result, because
it was added later, for an unrelated feature, and nobody re-ran the same
audit against it. **A search for this defect class should grep for every
site that classifies a `Result`/`Option` from a distributed call into a
committed/aborted decision, not just the ones a previous investigation
already found** — "already fixed this bug class" is not the same claim as
"already fixed every site the bug class reaches," and a feature shipped
between two rounds of the same investigation is exactly the kind of site an
audit scoped to "the functions this investigation started with" will miss.

**The fix generalizes too**: reuse the SAME retryability convention the rest
of the codebase already carries (the `"; retry"` message suffix,
`Self::read_should_retry`'s own shape) rather than inventing a second
ambiguity taxonomy — `TxnAbortReason::is_ambiguous` is a one-line
`.ends_with("; retry")` check, and auditing every `TxnAbortReason::Other`
construction site against it found exactly one real gap (`CpRoute::None`'s
message was missing the suffix) rather than needing a new mechanism. Where a
false-negative "definitely didn't happen" is the failure mode (not a false
"definitely succeeded" — the two are not symmetric; only the negative
direction here can cause a client to safely-in-appearance retry into a
double-execution class of bug), the safe default on "I genuinely don't know"
is to retry the underlying operation a bounded number of times first (a
fresh attempt after a transient blip usually just works — bounded internal
retry absorbed the overwhelming common case here, mirroring
`txn_prepare_pushing`'s own `IntentBlocked` retry one layer down), and if
still unconfirmed, leave the cache exactly as ambiguous as it already was
(`PENDING`, not a fabricated `CANCELLED`) rather than manufacturing a
confident wrong answer — the identical "bound the retry, don't fabricate a
decision" rule this file's own `unresolved_decided` entry already states for
the sibling recovery-side case, now confirmed to generalize to a cache
sitting in front of the same underlying operation, not just to the
operation's own recovery path.

**Verifying a soak failure under host contention, reinforced**: the mandated
30-run un-pinned `SplitMode::InPlace` proof-soak batches for this round (see
below) hit the exact contention trap this file already names — a
`cargo test --workspace` run surfaced one `multi_split_soak_streamed_gsi_
table_under_mixed_load` failure (`drain_all_tablets_lineage` one record
short of 144, the already-documented lineage-delivery-timeout residual, ADR
0058's G5 row), which reran clean in isolation on the first attempt. Treated
as contention noise, not counted against the soak, per this file's own
standing instruction — restated here only because it is the mechanism this
round's own dedicated 30-run batches (isolated, one at a time) were run to
avoid in the first place.
