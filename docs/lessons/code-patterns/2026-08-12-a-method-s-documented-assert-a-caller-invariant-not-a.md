# A method's documented `assert!` — "a caller invariant, not a recoverable condition" — stops being safe the moment a new caller can reach it with untrusted input, even if the method itself never changes.

**A method's documented `assert!` — "a caller invariant, not a recoverable
condition" — stops being safe the moment a new caller can reach it with
untrusted input, even if the method itself never changes.**
`RaftKvNode::txn_stage` (ADR 0018 §2/PR3) hard-asserts its anchor key is
at least `TOKEN_BYTES` long; this was correct and safe when its only
callers were tests and a Dynamo/CQL edge that always builds ADR-0022-
shaped keys. ADR 0018 §2/PR4 added the first genuinely wire-facing caller
(`ClientRequest::Txn` → `ClientCtx::cp_txn`), which can hand it an
arbitrary client-supplied key — an unvalidated short key would have
panicked the whole node process, a real DoS vector for a distributed
database, not a graceful error. The bug was caught by the PR's own
ProdEnv integration test, not code review, because the test used a raw
literal key shorter than the invariant. **Whenever a change makes a
previously-internal-only function reachable from an external/untrusted
caller (a new wire request type, an admin action, a CLI flag), grep that
function for every `assert!`/`panic!`/`unwrap!`/`expect!` documented as a
"caller invariant" and add validation at the new boundary** — the fix
belongs at the boundary (`cp_txn` now validates and returns a clean
`Err`), not by softening the assert itself, which is still the right
contract for the trusted internal callers.
