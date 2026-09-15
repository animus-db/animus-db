# A "weakened" sim conversion's own assumption about which mechanism still holds must be run, not inferred from a sibling test's precedent (ADR 0061 rung L, C-12 PR 4c)

`tests/console_endpoint.rs::console_addr_panics_on_control_only_node`
asserts a `Node`-level fact (`Node::console_addr()` panics, since a
control-only node never binds the console listener) that has no
`SimCluster` analog at all — no `Node` struct, no listener binding for any
role. The natural "weakened conversion" move (this rung's own established
pattern — keep the *shape* of a real-socket test's invariant even when the
literal mechanism can't be reproduced) was to prove the underlying reason
the panic exists: a control-only node structurally has no data role. The
first draft assumed — by analogy by to this same rung's own PR 4a
precedent, `mixed_cluster_put_via_control_node_forwards_to_data_node`,
which proves a **plain-client-protocol** write issued from a control-only
node's `ClientCtx` forwards cleanly to the data node with no panic
anywhere — that a **console item write** issued the same way would behave
identically. It does not: `dynamo::fast_marker_write` (the ADR 0049 fast
arm the console's own unconditioned `PutItem` route takes) reads
`ctx.data().request_rates` unconditionally, on the ISSUING node's own
`ctx`, before any routing/forwarding decision — a real panic, caught
immediately by the very first `cargo test` run of the scenario (a full
backtrace through `console.rs` → `dynamo.rs`'s `fast_marker_write` →
`ClientCtx::data`), not something inspection of the code would have
obviously predicted from the plain-protocol precedent alone. The two
paths look interchangeable from the outside (both are "a write, forwarded
to wherever the tablet actually lives") but differ in exactly the
dimension that mattered: `cp_kind_write_raw`/`resolve_cp_route` never
touch `ctx.data()` on the issuing node (only `write_path.rs`'s functions
do, at the LEADER); the DynamoDB-shaped fast arm does, unconditionally, on
whichever node's `ClientCtx` originates the call. Once found, the fixed
scenario asserts the panic directly (`#[should_panic(expected = ..)]`,
mirroring the real test's own shape) — a *better*, more faithful analog of
the original invariant than "forwards cleanly" would have been, since it
is exactly the mechanism `console_addr()`'s own panic keeps structurally
unreachable in production.

**The general rule**: when a "weakened conversion" leans on a *different*
test's precedent for "this mechanism behaves safely regardless of role,"
don't assume the precedent transfers to a superficially similar but
structurally different code path (a different write primitive, a
different edge) — run the new scenario and let a real panic/failure
correct the assumption, the same way this rung's own auto-split/split-
cluster appendices already document for post-fault timing assumptions. A
scenario that "should" pass by analogy is not proven until it actually
runs.
