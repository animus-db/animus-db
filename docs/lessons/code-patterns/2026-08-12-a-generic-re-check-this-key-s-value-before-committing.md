# A generic "re-check this key's value before committing" primitive is unsound when the key being re-checked is also the key being written by the same transaction

**A generic "re-check this key's value before committing" primitive is
unsound when the key being re-checked is also the key being written by
the same transaction** (ADR 0018 §2/PR7, atomic Dynamo
`TransactWriteItems`). `animus-cp-data`'s `cp_txn` precondition mechanism
(`(table, key, expected)`, re-read once before staging and again right
before the commit decision) was designed for the classic cross-key
read-modify-write shape — check key A, write key B — and is documented as
such on `cp_txn` itself. An early version of `dynamo.rs::run_transact` fed
*every* condition-gated action's observed value into it, including a
`Put`/`Delete`/`Update`'s own key — i.e., "check that key K still looks
like what I read, then commit a write that also targets K." That
precondition's own re-read runs *after* this same transaction has already
staged its own intent at K, so it can only ever observe either the
pre-stage value (if racing ahead of the stage) or this transaction's own
still-`Pending` intent (the common case) — which cannot resolve until the
transaction itself decides, which hasn't happened yet in `cp_txn`'s own
control flow. The read doesn't fail cleanly; it blocks in the ordinary
client-facing read's retry loop until something *else* resolves the
intent — here, the background in-doubt-recovery resolver, several seconds
later (past `RECOVERY_GRACE`) — at which point the precondition "finds" a
value that of course differs from the pre-stage observation and reports a
spurious conflict. The bug didn't look like a hang; it looked like an
intermittent, several-second-delayed false cancellation, caught by an
existing regression (`animusd/tests/dynamo_schema.rs::extended_surface`)
whose runtime jumped from under a second to several seconds before it
started failing — the *timing* signature (roughly `RECOVERY_GRACE`) was
the clue that pointed at "something's blocking on this transaction's own
unresolved state," not a genuine data race. Fixed by restricting the
precondition mechanism to keys structurally guaranteed distinct from
every write in the same transaction (here, `ConditionCheck`'s key only —
never a `Put`/`Delete`/`Update`'s own), and documenting the resulting,
narrower guarantee for a write's own condition (protected only by
whatever same-node serialization already existed, not cross-node OCC)
rather than silently accepting the broken stronger claim. **The
generalizable lesson**: before wiring a "verify unchanged, then commit"
precondition/OCC/CAS mechanism onto a key, check whether that mechanism's
own re-read can observe *this same in-flight operation's own effect* —
if the thing being written and the thing being checked can be the same
key, the mechanism's re-check window necessarily straddles this
operation's own not-yet-decided state, and "verify unchanged" degrades
into "wait for myself to finish," which for a mechanism whose only
liveness backstop is a several-second background sweep looks exactly
like an intermittent bug, not a hang.
**Update (ADR 0018 §2, 2026-08-12 follow-up): the self-referential-stall
case above is now closed by a different primitive, not the workaround.**
`animus_cp_data::KvCommand::TxnStage` gained its own `conditions` field —
byte-level OCC checked once, *inside the same apply arm* that stages the
intent, against the key's pre-intent committed value — so a write's own
condition never needs a re-read at all, self-referential or otherwise.
The lesson above (never route a key through a re-read-based
precondition mechanism if that same key is also being written by the
operation being verified) still generalizes to any *other*
re-read-based OCC design; what's new is the alternative it points
toward: if the write and the check happen inside the same atomic
decision step (an apply arm, a single critical section), there is no
window for the check to observe the write's own not-yet-decided effect
in the first place, so the whole class of stall is structurally
unreachable rather than merely mitigated.
