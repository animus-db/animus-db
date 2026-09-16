# A batch operation authorizes each item independently; a transaction authorizes the whole set up front — don't let the batch precedent leak into the transaction one (2026-09-07, W-07 PR 4)

`BatchExecuteStatement`'s per-statement handler
(`animusd::dynamo::execute_one_batch_statement`) authorizes each
statement's own table with a plain `authz::authorize` call and turns a
denial into that one statement's own `AccessDenied` response entry —
deliberately **not** `authz::authorize_each_table`, the whole-request
pre-check `BatchGetItem`/`BatchWriteItem` already use to reject a request
spanning an allowed and a denied table *before any of it runs*. This is
the right call for a batch (AWS's own `BatchExecuteStatement` authorizes
each statement independently against IAM, exactly like
`BatchWriteItem`/`BatchGetItem` authorize each request item — but reports
per-item, not per-request, since a batch already has no cross-item
atomicity to protect: partially applying it is the *normal* outcome, not
a hazard). The general shape worth remembering before implementing W-07
PR 5 (`ExecuteTransaction`): **a batch's "no cross-item atomicity" and a
transaction's "everything commits or nothing does" are opposite
authorization postures, not two applications of the same pattern.**
`TransactWriteItems`/`TransactGetItems` (`animusd::dynamo::run_transact`/
`run_transact_get`) already use `authorize_each_table` for exactly this
reason — a transaction cannot discover mid-flight that action #3 of 5 is
denied and "partially commit" the first two, so the whole set must be
checked before anything stages. `ExecuteTransaction` lowers onto
`TransactWriteItems`/`TransactGetItems` (ADR 0071's own lowering table),
so its authorization should inherit that same whole-set check — copying
`BatchExecuteStatement`'s per-statement `AccessDenied` pattern onto it
would be a silent semantic regression (a transaction that partially
authorizes and partially runs), not a reuse win. When implementing a new
batch/multi-item wire operation, decide this explicitly by asking "does
this operation have cross-item atomicity to protect?" — if yes,
whole-set pre-check (`authorize_each_table`); if no (each item's own
success/failure is already independently reported), per-item check
inside the loop.
