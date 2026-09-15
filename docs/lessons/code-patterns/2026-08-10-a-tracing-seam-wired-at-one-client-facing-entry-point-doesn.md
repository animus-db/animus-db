# A tracing seam wired at one client-facing entry point doesn't cover every caller of the primitives underneath it — an internal path that "emulates a client" by calling the same primitives directly, bypassing the wrapped entry point, needs its own span or context-propagating calls inside it become silent no-ops.

**A tracing seam wired at one client-facing entry point doesn't cover every
caller of the primitives underneath it — an internal path that "emulates a
client" by calling the same primitives directly, bypassing the wrapped entry
point, needs its own span or context-propagating calls inside it become
silent no-ops.** `handle_client` wraps every accepted request in a
`client_request` span (ADR 0027), so `cp_forward`'s
`otel::current_traceparent()` has an active span to inject when a *client*
write forwards to another node. The admin bulk seeder
(`admin.rs::action_data_seed`) calls `ctx.cp_batch_write` directly — never
through `handle_client` — so it wrote real data with zero spans exported no
matter how much it wrote, a gap invisible from the code (no error, no
warning — `current_traceparent()` just returned `None`, its documented
no-op-when-there's-nothing-to-propagate behavior, indistinguishable from
"export is disabled"). Fixed by giving it its own `admin_seed` root span
(mirroring `client_request`'s granularity) with per-chunk `admin_seed_batch`
children. The general check: when adding a new internal caller of a
primitive whose forwarding path reads ambient span context, ask "does this
caller sit under a span at all" — not just "does the primitive still work."
