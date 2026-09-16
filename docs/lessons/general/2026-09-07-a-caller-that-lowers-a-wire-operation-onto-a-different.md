# A caller that lowers a wire operation onto a *different, already-implemented* `Operation` and re-dispatches it hits genuine mutual `async fn` recursion — one `Box::pin` at the recursive edge is the whole fix (2026-09-07, W-07 PR 3)

`animusd::dynamo::execute_statement` (called from one match arm of
`run_operation`) needed to run a lowered `Operation::PutItem`/
`UpdateItem`/`DeleteItem` through the *exact* write path a client-built
request of that shape already takes — the only way to genuinely inherit
conditions/index-maintenance/streams/throttling with zero new write-path
code, rather than re-deriving a parallel slice of it. The obvious way to
do that is to call `run_operation(ctx, principal, op).await` from inside
`execute_statement` — but `run_operation` itself calls `execute_statement`
in its own `Operation::ExecuteStatement` arm, so this is a genuine mutual
`async fn` recursion: `Future<run_operation>` embeds `Future<
execute_statement>` embeds `Future<run_operation>` ... — an infinitely
recursive type, `error[E0733]`, if written naively. **The fix needs no
signature change and no separate "extract the shared body into a plain
function" refactor** (which would have meant peeling apart three
already-large, already-tested match arms and re-threading their bodies as
standalone functions purely to avoid recursion — a materially bigger,
riskier diff for the same behavior): box **one edge** of the cycle,
`Box::pin(run_operation(ctx, principal, op)).await` at the call site
inside `execute_statement`, and leave `run_operation`'s own call into
`execute_statement` as a plain unboxed `.await`. `Box<T>` is
pointer-sized regardless of `T`'s own (here, recursively-defined) size, so
inserting it at even one point in the cycle turns the outer type from
"infinitely recursive, unrepresentable" into "one opaque type embedding a
heap pointer to another opaque type" — finite, and exactly the same
technique (`Box::pin` at the recursive call) used for a plain
self-recursive `async fn`. Verified: `cargo build -p animusd --lib`
compiled clean with no other change. **General lesson: lowering one wire
operation onto another already-implemented one and running it through the
existing dispatcher (rather than duplicating that dispatcher's logic) is
the right reuse instinct, and the mutual-recursion compile error it
produces is not a sign the approach is wrong — box the one call site that
closes the cycle and move on**, rather than reaching for a bigger
refactor the compiler error doesn't actually require.
