# Verifying a new `SimCluster` fixture scenario without a compiler

**Verifying a new `SimCluster` fixture scenario without a compiler**: when
a task's own worktree constraints forbid running `cargo` (concurrent
writers gating elsewhere), the substitute for "compile it and see" is
reading the exact production functions the fixture calls end to end —
every dispatch method's real signature, the wire/JSON shape it returns
(response wrapper keys, field names, status codes), and any helper's
actual `pub(crate)` visibility and parameter order — rather than
reasoning from a sibling scenario's shape alone (2026-09-09, ADR 0061
rung J, C-10 PR 6). Concretely: `console_add_gsi_payload`'s validation-
before-dispatch order (confirms a malformed attribute type never reaches
`dispatch_table_op` at all), `GsiDetail`'s plain (non-renamed) field
names, and `table_api_response`'s `wrap_json("gsi", ...)`/`ok_json()`
response shapes were all read directly from `animus-node/src/console.rs`
and `animusd/src/lib.rs` before being asserted against in the new tests,
rather than assumed from the sibling module's own already-converted
scenarios. List every such API read as "uncertain, verify on the next
compile" in the handoff report so the compiling session checks them
first if anything fails.
