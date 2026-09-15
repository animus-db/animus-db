# `propose()`'s `Accepted` says nothing about apply-time rejection, and a test can silently misconstruct its own fixture because of it

Building a `SimEnv` test for the (now moved) `index_backfill` loop, a
two-tablet-per-table fixture was built by calling `MetaCommand::CreateTablet`
twice for the same table. Both calls returned `ProposeResult::Accepted` (the
`assert!(matches!(.., Accepted))` on each passed), so the test proceeded
believing both tablets existed — but the *second* `CreateTablet` is a
deliberate apply-time rejection (`Metadata::apply`'s own rule, ADR 0023: "one
`CreateTablet` per table; every further tablet comes only from a real
split"). Since `Metadata` is `DRIVER_APPLIED` (ADR 0038), `propose()`'s
`Accepted` means only "appended to my own Raft log" — the semantic
accept/reject decision happens later, asynchronously, in the apply task, and
`propose()`'s return value cannot see it. The test's own straggler-tablet
assertion then passed for the wrong reason: with only one tablet actually in
`Metadata.tablets`, "every tablet has reported" went vacuously true the
moment that one tablet reported, which looked identical to the intended
"both tablets must report" property from the assertion's own perspective.

General lesson: **a green assertion on `ProposeResult::Accepted` is not
evidence a command's own semantic rule accepted it** — for any
`DRIVER_APPLIED` state machine (`Metadata` here; the CP-data `KvState`
plane the same way), check the *post-apply* state (`node.metadata()`,
re-read after enough `sim.run_for`/`run_until` for the apply task to have
run) before trusting a fixture built from a sequence of proposals, especially
one hand-built for a test rather than driven through a real client that
would have surfaced the rejection. The existing `animus-control` test suite
already knows this rule (`complete_backup_requires_every_pinned_tablet`
drives `BeginSplit`/`CutoverSplit` for exactly this reason, with a comment
saying so) — the lesson here is that the same trap is easy to walk into
fresh when writing a *new* crate's *first* `SimEnv` fixture, where there is
no existing sibling test to copy the pattern from.
