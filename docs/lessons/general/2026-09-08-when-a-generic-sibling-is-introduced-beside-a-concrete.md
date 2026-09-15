# When a generic sibling is introduced beside a concrete production dispatcher, keep the original `ProdEnv` suite as the permanent equivalence regression — don't trim it once the sim twin passes (ADR 0061 rung F, C-06 close-out, 2026-09-08)

C-06's whole shape — widen `run_transact`/`run_transact_get` and add
`execute_statement_as`/`execute_transaction_as`/`run_batch_execute_
statement_as`/`execute_one_batch_statement_as` as new, strictly additive,
`<E: Env, R: RelayClient>`-generic siblings, never touching
`run_operation`/`execute_statement`/`execute_transaction`/`run_batch_
execute_statement` themselves — is D3/D4's own template applied a fifth and
sixth time. Every prior application of that template (D3 PR 2a/3a/3b, D4
PR 2/5) faced the same fork in the road once its own sim twin existed and
passed: trim the original real-socket file down to whatever the sim tier
can't yet reach, or keep it whole. D3 PR 3b took the "keep it whole"
option once, deliberately, for exactly one file
(`dynamo_indexes.rs::gsi_write_then_query`) while trimming everything else
around it that PR converted — and C-06 generalizes that single decision
into a standing rule, made explicit rather than left implicit: **when the
new generic path is a parallel sibling of a concrete production function,
not a replacement for it, the original real-socket suite proving that
production function's own behavior stays in the repo, unedited, forever**
— `crates/animusd/tests/dynamo_partiql.rs`/`dynamo_execute_transaction.rs`
(37 tests) were run, never trimmed, at every one of C-06's six PRs. The
reason is not nostalgia for the old file: it is the *only* thing that
keeps the "the two paths are byte-identical" claim checked on every future
change to either one, rather than true only at the moment the sim twin was
first written and silently rottable afterward. A sim twin proves the
generic path is *correct*; it says nothing about whether the *concrete*
path a real client's request actually goes through still matches it six
months and a dozen unrelated refactors later — only a live regression
still exercising the concrete path can say that. **General lesson:**
finishing a "make X reachable from `SimCluster` via a new generic sibling"
task is not the same task as "delete X's old real-socket test file" —
those are two different, independently-justified decisions, and the
default for the second one, absent a specific reason to trim (the
function itself was widened in place with zero behavior change, the D3
PR 2a/2b/5 shape, where the *same* code now serves both), is to keep the
original suite whole and say so explicitly, so a later contributor doesn't
read an unconverted real-socket file as an oversight and "clean it up."
