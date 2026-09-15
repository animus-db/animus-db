# A new generic sibling calling back into the SAME dispatcher its own caller just gained a match arm for is a mutual-recursion cycle the compiler catches, not either function's own local reasoning (ADR 0061 rung F, C-06 PR 5, 2026-09-08)

Widening `execute_statement` into a new generic sibling
(`execute_statement_as`, `crates/animusd/src/dynamo.rs`) followed the
established D2/D3/D4 template: never widen the concrete production
function itself, add a parallel `_as` sibling instead, and route
`dispatch_item_op` (the generic core `SimCluster` drives) to it via a new
match arm. The design doc written *before* implementing it reasoned, in
isolation, that `execute_statement_as` needed no `Box::pin` — unlike
`execute_statement`'s own pre-existing `Box::pin(run_operation(..))`
(needed because `run_operation` calls back into `execute_statement` for
its `ExecuteStatement` arm), `dispatch_item_op` "never calls back into
`execute_statement_as`." That was true of `dispatch_item_op` *before* this
same change — but this change's other half was adding
`dispatch_item_op`'s own new `ExecuteStatement` arm, which calls
`execute_statement_as`, whose `INSERT`/`UPDATE`/`DELETE` branches call a
new `dispatch_lowered_write_as` helper, which calls back into
`dispatch_item_op` to actually run the lowered write. Three functions, one
cycle, invisible from any single function's own local doc reasoning
because the cycle is only closed once *both* halves of the same PR (the
new sibling, and the new match arm routing to it) exist together.

`cargo build` caught it immediately and unambiguously: `E0733: recursion
in an async fn requires boxing`, naming the exact three functions in the
cycle in its own note chain. The fix was one `Box::pin` — inside
`dispatch_lowered_write_as`'s own call to `dispatch_item_op`, not at
`execute_statement_as`'s three call sites into `dispatch_lowered_write_as`
— since the cycle has exactly one edge that needs breaking regardless of
how many paths lead into it from the sibling's own INSERT/UPDATE/DELETE
branches.

**The generalizable lesson**: whenever a PR both (a) adds a new generic
sibling of a concrete function, and (b) adds a match arm on some
dispatcher routing to that sibling, check whether the sibling (or
anything it transitively calls) can call back into that SAME dispatcher —
not just whether the sibling itself looks recursive when read alone. This
is a distinct hazard from the "narrowed generic split becomes the
production dispatcher's only path" lesson elsewhere in this log (that one
is about production behavior silently changing; this one is a compile
error, so it can never land unnoticed) — but it means a design doc's own
"no `Box::pin` needed here" claim, written before both halves of a change
are in place together, is a prediction to verify at `cargo build` time,
not a fact to state as settled. When it fires, `rustc`'s own note chain
already names every function in the cycle — box the one call that closes
it (usually the innermost one, so a caller with several branches into the
cycle needs only one `Box::pin`, not one per branch), then update the doc
that made the wrong prediction rather than leaving it uncorrected for the
next reader.
