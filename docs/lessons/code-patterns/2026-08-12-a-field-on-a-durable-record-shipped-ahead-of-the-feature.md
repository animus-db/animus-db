# A field on a durable record shipped ahead of the feature that will read it back can be structurally present but semantically empty — the type checker cannot catch "nobody actually populated this for the case that matters," only a caller that greps every writer can.

**A field on a durable record shipped ahead of the feature that will read
it back can be structurally present but semantically empty — the type
checker cannot catch "nobody actually populated this for the case that
matters," only a caller that greps every writer can.** ADR 0018's
`TxnRecord::intent_spans` (`animus-cp-data/src/txn.rs`) shipped in PR3
(single-participant transactions) computed purely from the anchor's own
writes — sound at the time, since the anchor was the only participant
that existed. PR4 added real multi-participant transactions but never
revisited the field: a non-anchor stage passed `spans: Vec::new()`
("no local record is ever created here" — true, but irrelevant to
whether the *anchor's* record should have known about this participant
anyway). The field kept compiling, kept round-tripping through
encode/decode, and kept *looking* like "the transaction's spans" right
up until PR5 needed to actually walk every participant for recovery and
found the anchor's own record had never heard of anyone else. This is
the same shape as PR4's own `record_table` fix one PR earlier (a bare
record key not carrying the routing info a *later* feature needed) — a
recurring pattern worth naming: **when a staged delivery's early PR
creates a durable record/marker type "to be filled in as the design
grows," the PR that actually needs the fuller picture must grep every
site that constructs the type, not trust the field's presence/type
signature as evidence it was fully populated for every case that now
exists.** The fix pattern is also identical both times: whoever has the
complete picture *before* the type is ever constructed (a coordinator
that already grouped every participant by table/tablet) hands the fuller
data to the constructor explicitly, rather than the constructor trying
to reconstruct it locally from information it structurally doesn't have.
See `docs/adr/0018-cross-tablet-transactions.md`'s PR5 amendment §2 for
the full account and the closing fix.
**Update: fixing a gap like this one is worth a second pass asking "what
if this record doesn't exist at all yet?"** — review of the fix above
(a second reviewer, not the original implementer) immediately surfaced a
further corner the fix itself didn't close: PR4's prepare phase stages
participants *concurrently*, so a participant's own intent can be
discovered by a reader while the record that would name it never gets
created at all (the anchor's own stage can silently no-op on a
fence/seal miss, exactly like a participant's already could — the same
class of gap, just on the *other* side of the anchor/participant split).
Any "read this record to decide what to do" path needs a **third**
branch beyond "found, decided" / "found, pending" — **"not found at
all"** — with its own safe decision (here: always abort, never commit,
since committing needs a participant list only the record would have
provided), *and* a symmetric guard against a **late arrival of the
thing that would have created the record** overwriting whatever that
third branch already decided (a "resurrection" hazard — the same
first-decision-wins principle the original fix already established for
*conflicting* decisions, extended to record *creation* itself). The
general check to run whenever a fix makes some entity's *fields* more
complete: does the fix's own precondition ("the entity exists") still
hold in every case the system can reach, or did fixing the fields
quietly assume creation is atomic with the read that discovers a need
for it? See the PR5 amendment's §2b for the full closing fix and its
regression test.
**Update (2026-08-12, task #18): the fix above closed the *primitive*'s
shape but nobody ever verified its *real caller* actually used it — for
three subsequent PRs (PR5 through PR7), `ClientCtx::cp_txn` kept calling
`RaftKvNode::txn_stage` (an empty participant list) instead of the new
`txn_stage_anchor(.., participant_spans)` this very fix introduced,
so every production multi-participant transaction's `intent_spans` still
only ever named the anchor's own keys — a live, exploitable atomicity
violation on the recovery path (a transaction whose participant never
staged could be wrongly recovered as `Committed`), not merely the
observability gap it looked like on paper.** Nobody caught this because
every test that exercised recovery's participant-verification logic
called `txn_stage_anchor` **directly**, by hand, with a real span list —
proving the primitive, never the coordinator's wiring of it. And every
test that *did* go through the real coordinator (`animusd/tests/
cp_txn.rs`'s PR5 coordinator-crash pair) always staged every participant
genuinely before letting recovery run, so the verification loop's
incompleteness (checking a list that was silently too short) was never
exercised against a case where the answer would have been wrong — the
loop just never found anything to disagree about. **The generalizable
lesson, sharper than the one above**: when a fix teaches "the caller must
supply the fuller data" and changes a primitive's signature to accept
it, that is necessary but not sufficient — a follow-up (ideally the same
change) must grep the actual production call site and confirm it was
updated to *pass* the fuller data, not just that a test constructing the
call by hand now can. An ADR/CLAUDE.md sentence asserting "the
coordinator already computes X and hands it to the stage call" is a
claim about a specific call site, not about the type system — verify it
by reading that exact function, especially when what depends on it is a
correctness property (recovery's own atomicity), not a nice-to-have. See
ADR 0018's own corrective note on this section for the full account, the
wire-level test that reproduced the live failure, and the fix.
