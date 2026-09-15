# Holding a node-local lock across a call that can recurse back into the same lock, on the same node, is a self-deadlock waiting for the one deployment shape that makes the recursion local.

**Holding a node-local lock across a call that can recurse back into the
same lock, on the same node, is a self-deadlock waiting for the one
deployment shape that makes the recursion local.** `dynamo.rs::
run_transact` held `ctx.data().rmw_lock` across its entire span,
including the `cp_txn` call at the end — safe for as long as `cp_txn`
never itself tried to take `rmw_lock`. Adding kind-write-path evaluation
(`eval_kind_txn_write`, inside `ClientCtx::txn_stage_local`) gave it
exactly that: a *second* acquisition of the same lock, reached the
instant a write targets a table whose tablet leader is hosted
**on this same node** — true for every combined-role/single-node
deployment, i.e. most local dev and every single-node test. A
`tokio::sync::Mutex` is not reentrant, so this is not a rare race; it is
a guaranteed hang the first time the code path is exercised on the
deployment shape that makes it local. Found immediately by a real
`ProdEnv` integration test hanging (not a `SimEnv` corpus, which cannot
express real-thread self-deadlock at all — see this doc's own "a flaky
`ProdEnv` test is a real bug" entry). **General form**: before adding a
new call inside a function that already holds a lock across its own
return path, check whether the new call's *own* call graph can reach the
identical lock — "it never has before" is not evidence it never will
once the new code path funnels a same-node case through it; scope the
guard to the exact span that needed it, not the whole function, unless
every downstream call is provably lock-free. (2026-08-16, `TxnStage`
kind-writes stack PR2.)
