# A pure predicate trapped behind a stateful handle is extracted by widening its parameter list to the primitive facts the caller already reads off that handle, not by trying to make the handle itself pure

**A pure predicate trapped behind a stateful handle is extracted by
widening its parameter list to the primitive facts the caller already
reads off that handle, not by trying to make the handle itself pure**
(ADR 0061 rung A6, `animusd`'s `decide` module). `ClientCtx::
confirm_wait_is_futile`/`frozen_refusal` looked `&CpGroup`-shaped
(`fn confirm_wait_is_futile(leader: &CpGroup, accepted_index: u64) ->
bool`), and `CpGroup` wraps a real `RaftKvNode<ProdEnv, _>` -- genuinely
impossible to construct in a unit test without full cluster bring-up. But
every line of each function's *body* only ever called two or three cheap,
already-`pub(crate)` accessors on it (`leader.engine_applied_index()`,
`leader.is_leader()`, `leader.is_frozen()`). Changing the signature to
take those return values directly (`fn confirm_wait_is_futile
(engine_applied_index: u64, is_leader: bool, accepted_index: u64) ->
bool`) turns an apparently-entangled method into a plain, directly
unit-testable function with a full truth table in under ten lines -- the
caller's one-line change is reading the same fields it already read, just
before the call instead of inside it. The tell that a function is a false
negative for "needs bring-up" is exactly this: grep its body for what it
actually touches on `&self`/the handle parameter, not what the
parameter's *type* looks like capable of.
