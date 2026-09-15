# A same-listener "dispatch" fork between two sibling services is a gate every shortcut caller must go through, not just the real edge.

**A same-listener "dispatch" fork between two sibling services is a gate
every shortcut caller must go through, not just the real edge.** ADR
0042 §3 put the DynamoDB item API and the DynamoDB Streams read API on
one listener, forked by `X-Amz-Target` prefix in `dynamo::dispatch`. The
admin dashboard's write proxy (`POST /admin/data/dynamo` →
`action_data_dynamo`) reused the item API's own decode+run helper
(`dynamo::execute`) directly — a reasonable-looking shortcut at the time,
since Streams didn't exist yet — but never went through `dispatch`
itself. Once Streams landed one layer up, the proxy could build a
perfectly well-formed `DynamoDBStreams_20120810.*` target and it would
still 400 as "unknown operation," because it never reached the code that
checks the prefix. Nothing caught this: the fork is a plain `if` in one
function, not a match arm on an enum clippy/exhaustiveness can flag, and
every dispatch-side test call went through the real edge, never the
admin route. The general shape to watch for: when a request can enter a
multi-service surface through more than one caller (a real wire edge and
an in-process admin/test shortcut alike), extract the fork itself into
its own named function and have *every* entry point call that function
— never let a shortcut call the fork's *callee* on the assumption "this
target will always be item-API." Fixed by factoring the `if target.
starts_with(..)` fork out of `dynamo::dispatch` into `dynamo::
execute_routed`, then pointing both the edge and the admin proxy at it.
(`crates/animusd/src/dynamo.rs::execute_routed`; `crates/animusd/src/
admin.rs::action_data_dynamo`; ADR 0042 §3, 2026-08-14.)
