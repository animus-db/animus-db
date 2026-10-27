# Two impls that must stay separate only for one call can share every other line: put the body in a generic helper and pass the dispatch as a zero-sized trait type (animusd `ConsoleBackend`, 2026-10-27)

`impl ConsoleBackend for ClientCtx` (production) and `GenericConsoleBackend<E, R>`
(SimCluster) had to stay two impls (coherence, plus the 2026-09-08 lesson that
the generic dispatch covers a narrower operation set than production), but
they had drifted into ~650 duplicated lines. A `diff` of the two bodies showed
the only differences were the receiver (`self` vs `self.0`) and the
`execute_routed` vs `execute_routed_as_generic(.., &Principal::unrestricted(), ..)`
call. The fix keeps the separation where it matters: each method body is a
`console_op_*<E, R, D: ConsoleDispatch<E, R>>(ctx, ..)` helper, and
`ProductionDispatch` / `GenericDispatch` are the two (distinct) dispatch
strategies; the impls are one-line forwards naming `D`.

Gotchas: (1) diff the two bodies first and list every non-receiver
difference; here there were none, and for `AdminHost` there was a hidden one
(`debug_inboxes_view` only overridden on the concrete impl, `action_data_dynamo`
dispatch), which is why that pair was left alone. (2) A dispatch trait with a
static `async fn` needs `#[async_trait]` so the futures stay `Send` for the
outer `#[async_trait]` impl. (3) A unit struct only constructed by test code
needs the same `#[cfg_attr(not(test), allow(dead_code))]` as its sibling.
(4) Generating the forwards with a Python script: closures defined in a loop
late-bind their variables; bind them as default arguments.
