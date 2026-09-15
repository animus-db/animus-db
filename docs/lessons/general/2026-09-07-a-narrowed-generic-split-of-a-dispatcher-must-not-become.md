# A narrowed generic split of a dispatcher must not become the production dispatcher's ONLY path for cases the narrowing dropped (ADR 0061 rung D2 PR 1)

Splitting `dynamo::run_operation` into "a new generic core for the ops that
had no `ProdEnv` entanglement" plus "the concrete production entry point
for the rest" is a sound shape — but the first cut wired it wrong in a way
that looked safe and wasn't. `dispatch_item_op<E, R>` was built to cover
only a **base-table** `Query`/`Scan` (an `index` name deliberately returns
"not yet supported," since genericizing the real GSI/LSI dispatch tree was
out of this PR's scope). The first version then routed `run_operation`'s
own `Query`/`Scan` arms through that same narrowed function — reasoning
that "it's monomorphized at `E = ProdEnv` for production, so behavior is
unchanged" felt true by analogy with the other six delegated operations
(`PutItem`/`DeleteItem`/`GetItem`/`BatchGetItem`/`UpdateItem`/
`BatchWriteItem`, which really were a byte-identical pure move). It wasn't:
for `Query`/`Scan` specifically, the delegation replaced a call to the
*full-featured* `run_query`/`run_scan` (real GSI/LSI dispatch) with a call
to a function that structurally cannot serve an index query at all — a
real behavior change, not merely the same logic at a different type
parameter. `cargo test -p animusd --lib` caught it immediately: four
`index_drain::gsi_drain_cursor_tests` failures, each a real GSI query
timing out on the narrowed function's own "not yet supported" error. The
fix was to exclude `Query`/`Scan` from the delegated set entirely and keep
`run_operation` calling the original, unmodified `run_query`/`run_scan`
directly — `dispatch_item_op`'s own narrower `Query`/`Scan` arms exist only
for the new generic entry point (`execute_item_op_as`, reached by
`SimCluster`), never for production. **General lesson: when factoring "a
generic core covering only a subset of cases" out of an existing
dispatcher, a production call site that used to reach the FULL behavior
must keep reaching the full behavior — never get silently rerouted through
the narrowed core just because the core happens to share a name/shape with
what it replaced. "The call is monomorphized so it's unaffected" is not by
itself proof of anything if the callee itself is a genuinely different,
narrower function — verify by running the touched dispatcher's own full
existing test suite (not just the new harness) before trusting that
argument.**
