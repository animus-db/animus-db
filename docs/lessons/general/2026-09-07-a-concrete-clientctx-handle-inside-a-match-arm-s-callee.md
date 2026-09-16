# A concrete `&ClientCtx` handle inside a match arm's callee chain silently blocks widening the whole dispatcher — grep every callee before promising "the widening is mechanical" (ADR 0061 rung D3 PR 3a)

Widening `run_index_query`/`run_gsi_query`/`run_lsi_query`/`run_index_
scan`/`run_gsi_scan`/`run_lsi_scan`/`paginated_kind_examine`/`paginated_
kind_examine_one` to `<E: Env, R: RelayClient>` (`dynamo.rs`) went exactly
as a design pass predicted: every one of the eight functions' only
concrete binding was its own `ctx: &ClientCtx` parameter, and every callee
inside them (`ctx.cp_scan_kind`/`ctx.cp_scan_kind_table`, `paginated_
table_examine`, `table_known`, `mirror_catalog_schema`) was already
generic since rung C5/D2 PR 1 — so the whole widening was a pure
signature-only change, zero body edits. That prediction was only safe to
make *because* the design pass (and this PR) actually walked the callee
chain of all eight functions before starting, the same discipline ADR
0061's own C5-step-1 entry establishes for `CpGroup`/`CpRoute` call
chains: a bare `&ClientCtx` (or `&CpGroup`) parameter anywhere in a chain
a widened function calls into is a hard, silent block on genericizing the
caller — it will not show up as "this doesn't compile yet," it shows up as
"the widened signature compiles but is dead code because nothing generic
can call it," or worse, compiles by accident against the wrong default
type parameter (see the "default type parameter resolves to its default,
never an enclosing scope's own parameter" gotcha this file's rung-C5-step-1
entry already documents). Grepping every callee's own signature before
starting is what turns "this widening should be mechanical" from an
assumption into a checked fact — worth repeating explicitly here because
this is now the *third* rung (D2 PR 1, D3 PR 2a's `create_table`/`enable_
stream`, and this one) where that check paid off by confirming the
prediction rather than by catching a surprise, which is exactly what a
grep-first discipline is supposed to produce most of the time.
