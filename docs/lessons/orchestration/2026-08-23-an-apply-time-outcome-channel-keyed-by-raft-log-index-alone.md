# An apply-time outcome channel keyed by Raft log index alone can tell no-op from failure, but it is NOT a confirm-of-success by itself — the proposer must also prove the applied entry is genuinely its OWN entry

**An apply-time outcome channel keyed by Raft log index alone can tell
no-op from failure, but it is NOT a confirm-of-success by itself — the
proposer must also prove the applied entry is genuinely its OWN entry**
(found in review of PR #334, 2026-08-23). `KindBatch`'s outcome channel
(`KindBatchOutcomes`, `animus-cp-data`) was modeled directly on `Cas`'s
`CasResults` — record what an entry did, keyed by the Raft log index
`ProposeResult::Accepted` handed the proposer — and that shape is sound
for CAS (a *committed* entry's index is unambiguous once you have it,
because `compare_and_swap` only ever reads `cas_result` after confirming
the entry applied via a value/ceiling read that already implies commit).
It stopped being sound the moment `animusd::poll_probe` used the outcome
alone, ahead of any value check, to end the wait: `ProposeResult::
Accepted{index}` means "appended to **my own log**," never "committed" —
and an appended-but-not-yet-committed entry's index can be **reoccupied**
by a completely different command if this node loses leadership first
(Raft log-matching truncates the original and a new leader's own entry
lands at the identical position). Every replica — including the original
proposer, once it reconnects as a follower — then records `Applied` at
that index for the *reoccupying* entry's content, not the original
proposer's. A `poll_probe` that trusted `Some(KindBatchOutcome::Applied)`
alone read exactly that false signal and returned `Confirmed` to the
client — a silently dropped write reported as a success, the precise
failure class the at-most-once confirm-loop work (issue #268, this same
log) exists to prevent, and worse for a non-idempotent numeric `ADD` than
for an idempotent `Put` (a lost increment can't be told from a landed
one by re-reading). **The sibling channel already had the fix**:
`TxnStage`'s own `StageOutcome` carries the identical "index alone means
no-op-vs-failure, never success" caveat in its doc, and every real
coordinator (`ClientCtx::txn_prepare_pushing`) pairs a `Some(ts)` stage
result with `txn_verify_staged` — an explicit read proving the staged
content is genuinely present — before ever trusting it; `KindBatchOutcome`
reused `StageOutcome`'s shape (index-keyed, apply-time-recorded) without
reusing that verification discipline, because unlike a transaction stage a
`KindBatch`'s own apply is fire-and-forget from the state machine's side —
there was no second "verify" call anywhere in the design to carry the
fix. The closed fix pairs the outcome with the entry's own Raft **term**
(`ProposeResult::Accepted` now carries `term` alongside `index`;
`KindBatchOutcomes` records `(term, outcome)`) and requires `term ==
accepted_term` before ever treating `Applied` as a confirm — sound by
Raft's log-matching property (identical index **and** term implies
identical entry, cluster-wide, for the life of the log), the same
identity guarantee a content check would need to approximate with a
fingerprint (rejected here: a fixed-size hash risks a — admittedly
astronomically unlikely — collision reintroducing the exact false-ack
class the fix exists to close, where a cheap integer-term comparison
carries zero such risk and needs no extra bytes in the bounded outcome
map). **The general rule**: before reusing an existing outcome-channel
*shape* for a new apply-time signal, ask what verification discipline the
original shape's callers relied on to stay safe (a value check that
implicitly proved commit, an explicit `verify_staged`-style read, a
requirement that the caller only ever consult the channel after already
knowing the entry committed) — copying the struct without copying (or
deliberately, consciously replacing) that discipline is how a channel
that was safe in its original home becomes a false-ack in its new one.
Regression: `animus-cp-data/tests/kind_batch_outcome_identity.rs`
(isolates a leader, lets it accept two entries that never commit, lets
the survivors elect a new leader whose own election no-op and first real
`KindBatch` occupy the identical two log positions, heals the partition,
and asserts the truncated write never appears on any replica — proven red
pre-fix by reverting `KindBatchOutcomes::record`'s term to a constant);
`animusd`'s `kind_batch_signal_tests` module (a focused, table-driven unit
suite for the extracted `classify_kind_batch_outcome` predicate
`poll_probe` now calls, including the term-mismatch case — proven red
pre-fix by dropping the predicate's term-equality guard).
**Amendment (2026-08-29): the two siblings this entry's own "audit every
sibling" rule pointed at — `Cas`'s `CasResults` and `TxnStage`'s
`StageOutcomes` — turned out to have the identical gap, and this entry's
own earlier claim that `CasResults`' shape was "sound for CAS" is
corrected here rather than left to mislead a future reader.** That claim
reasoned `compare_and_swap` only ever consulted `cas_result` "after
confirming the entry applied via a value/ceiling read that already
implies commit" — but the actual code never did any such confirming read:
`compare_and_swap`'s own poll loop called `cas_result(index)` directly,
with no value/ceiling check and (worse) no `is_leader()` guard either,
despite a comment claiming a step-down check existed. `stage_outcome`/
`wait_stage_outcome` had the `is_leader()` guard but the identical
index-only lookup. Both are now fixed exactly like `KindBatchOutcomes`:
`CasResults`/`StageOutcomes` store `(term, outcome)`, and
`cas_result`/`stage_outcome` take the caller's own accepted `term`,
returning `None` (never a stale `Some`) on a mismatch — propagating up
through `wait_stage_outcome`, `txn_stage_anchor`/`txn_stage_participant`,
and `compare_and_swap` (which also gained the missing `is_leader()` check
its own comment had wrongly implied was already there). Regression:
`animus-cp-data/tests/cas_outcome_identity.rs`, the `Cas` mirror of
`kind_batch_outcome_identity.rs` — same isolate/accept/elect/collide/heal
shape, proven red pre-fix by reverting `cas_result` to its index-only
form, plus an end-to-end check that the public `compare_and_swap` async
entry point itself never surfaces a false `Some(_)` for a truncated
attempt. **`TxnResolve` has a related but distinct gap — it has no
outcome channel at all, not a term-unsafe one — tracked separately, not
closed by this round** (see this file's "A resolve's silent no-op is
invisible to its own proposer" entry). **The generalizable lesson,
restated**: "audit every sibling" is not satisfied by naming the siblings
in a doc comment — it means actually reading each sibling's own call
chain down to its lowest-level accessor before asserting any one of them
is safe by a different mechanism; an assumption of safety that isn't
independently verified is exactly as dangerous as the missing fix itself,
because it makes a future auditor skip the very sibling that needed it.
