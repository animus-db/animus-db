# Lowering a new multi-statement wire operation onto an *existing atomic primitive* (not a per-item loop) inherits that primitive's whole-set authorization/idempotency/cancellation-reporting for free — verify by grepping for what the primitive already checks, not by re-deriving it (2026-09-07, W-07 PR 5)

`ExecuteTransaction` (ADR 0071) needed: 1..=25 statements parsed and
lowered, whole-set `AccessDeniedException` before anything runs, duplicate-
key rejection, `ClientRequestToken` idempotency for a write transaction,
and per-statement `CancellationReasons` on a condition failure. The
obvious-looking way to build this is a loop over the parsed statements,
each calling into `PutItem`/`UpdateItem`/`DeleteItem`'s single-item path
(the same shape `BatchExecuteStatement`, PR 4, correctly uses — a *batch*
is genuinely per-request, no cross-statement atomicity by DynamoDB's own
design). A *transaction*, though, already had a real cross-tablet atomic
primitive one layer down (`animusd::dynamo::run_transact`/
`run_transact_get`, ADR 0018 §2's 2PC machinery) built for
`TransactWriteItems`/`TransactGetItems` — and that primitive already had
every one of the five requirements above, because ADR 0066 §5 and ADR
0018's own idempotency/`CancellationReasons` amendments were built into it
directly, not bolted onto the wire decode layer. So the actual
`execute_transaction` implementation is thin: parse every statement, lower
each to one `TransactGet`/`TransactAction` (the wire-level building blocks
`TransactGetItems`/`TransactWriteItems` already decode to), and call
`run_transact_get`/`run_transact` unmodified — **zero new authorization
code, zero new idempotency code, zero new cancellation-reporting code**.
The whole-set `AccessDeniedException` end-to-end test
(`dynamo_auth_policy.rs::execute_transaction_spanning_denied_table_writes_
nothing`) passed on the first run with no `execute_transaction`-side
authz call at all, because `run_transact`'s own `authz::
authorize_each_table` already ran before any table was touched. **General
lesson: before writing a new multi-item/multi-statement wire operation's
authorization, idempotency, or atomicity from scratch, grep for an
existing lower-level primitive built for the operation's real semantic
shape (atomic-across-N vs. independent-per-N) and check what it already
enforces** — re-deriving those properties at the new call site is not just
extra work, it is a second place they can silently drift apart from the
original (the exact "grep every gating match site" lesson elsewhere in
this log, generalized from enum-variant classification to whole-function
reuse). The one place new code genuinely was needed — the *lowering* from
parsed PartiQL onto `TransactGet`/`TransactAction`, and the two
transaction-specific restrictions neither `TransactGetItems` nor
`TransactWriteItems` can express (a `SELECT` must be an exact-key read; a
statement's own `RETURNING`/`ON CONFLICT DO NOTHING` has no
success-path echo or per-statement-swallow analogue once any statement's
failure cancels the whole transaction) — is exactly the part that could
not have been inherited from anywhere, which is a useful signal in
itself: if a requirement *can* be satisfied by delegating to an existing
primitive, delegate; the requirements that can't are the ones that
actually need new code and new tests.
