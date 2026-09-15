# A generic dispatch's own coverage gaps are a real production regression the moment a NON-primary but still-production-reachable proxy is switched to it — "SimCluster-only gap" and "production gap" are not automatically the same claim (ADR 0061 rung H, C-08 PR 2, 2026-09-08)

Every prior rung in this series (D2/D3/D4/F/G) widened a dispatcher whose
*only* production caller was the real wire edge (`dynamo.rs::dispatch`),
which never changed target — the generic sibling was purely additive, and
"production stays byte-identical" held by construction. C-08 PR 2 widened
two DIFFERENT production entry points instead — `admin.rs::
action_data_dynamo` (`/admin/data/dynamo`, the dashboard's own DynamoDB
proxy, ADR 0021) and `impl console::ConsoleBackend for ClientCtx` (the
animusd console's own mutating endpoints) — because `AdminHost`/
`ConsoleBackend`'s trait requirements force EVERY method to be
`<E: Env, R: RelayClient>`-generic once the surrounding `impl` is widened,
with no way to keep one method concrete while its siblings go generic (see
the archive-bound entry below for exactly why). Both are genuinely
**production-reachable** — real operators use the dashboard's Data Browser
and the console app — but neither is the primary wire edge, and it was
tempting to reason "the generic dispatch already covers most DynamoDB
operations, so this is basically the same swap the prior rungs made
safely." It is not: unlike the wire edge, these two callers can be asked
to run **any** operation a client chooses, including ones the generic core
never claimed to cover (`UpdateTimeToLive`, `CreateBackup`/`DeleteBackup`,
and `UpdateTable` with an index change — the last a *named, accepted*
scope cut, "blocker (d)," not an oversight). Swapping their dispatch target
to the narrower `execute_routed_as_generic` without first widening those
specific operations turned nine real-socket tests red
(`admin_endpoint.rs`/`console_create_table.rs`/`console_table_config.rs`/
`dashboard_endpoint.rs`) — a genuine regression in shipped functionality,
not a SimCluster-only coverage gap, caught only because this rung's own
gate explicitly requires running the untrimmed suite **before** trusting
the swap (the D2 PR 1 lesson's own prescribed check). Six of the nine
closed cheaply: `update_time_to_live`/`create_backup`/`delete_backup` had
no `tokio::spawn` blocking them, just the same `tokio::time` → `ctx.env`
conversion this whole series already does mechanically, so widening them
and adding three `dispatch_item_op` arms was the *correct* fix, not a
workaround. The remaining two (`add_gsi`/`drop_gsi`, genuinely blocked on
GSI backfill machinery this rung was never going to build) needed a
different, structural answer — see the sibling entry just below.

**The general form**: before assuming a trait-forced widening's dispatch
swap is "the same shape as last time," ask whether the caller being
widened is a **narrow, single-purpose edge** (always the same handful of
operations) or a **general-purpose proxy** (can be asked to run anything a
client sends) — only the former's coverage gap is automatically confined
to the new generic-only caller (SimCluster); the latter's gap is visible
to every existing caller the moment the switch lands, so the untrimmed
gate isn't a formality for it, it's the only thing separating "found a
groundwork residual" from "shipped a functional regression."
