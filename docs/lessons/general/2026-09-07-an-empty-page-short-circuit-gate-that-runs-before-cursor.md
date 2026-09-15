# An empty-page short-circuit gate that runs before cursor validation makes that validation unreachable from one direction, in a fixture that can never make the gate false (ADR 0061 rung D3 PR 3a)

`run_gsi_query`/`run_gsi_scan`'s own `!meta.has_table_tablet(&index_table)`
gate — "the GSI's hidden table has no tablet yet, so answer empty rather
than routing a read at a group that doesn't exist" — was added for
`ProdEnv`, where it is a narrow, transient window: true only in the moment
between `CreateTable` returning and the drain's first materialized row.
Under `SimCluster` (ADR 0061 rung D1) that gate is not transient at all —
`SimCluster` never spawns `index_drain::change_consumer_loop`, so a GSI's
hidden table gets a tablet only via that loop's own lazy provisioning, and
under this fixture that provisioning simply never happens. The gate is
therefore **unconditionally true, forever**, for any GSI under `SimCluster`.

The consequence was not just "a GSI `Query` reads back empty" (expected,
and already documented as this rung's own scope boundary) — it also meant
a cursor-shape check gated *behind* the same early return became
unreachable from that one call direction specifically. Converting
`cross_index_cursor_mismatch_is_rejected` (`dynamo_query_pagination.rs`)
assumed, on first pass, that `validate_query_cursor_shape` — a pure
attribute-name check with no row read at all — would need nothing but a
syntactically well-formed `ExclusiveStartKey`, so a hand-crafted cursor
literal (rather than one extracted from a real, materialized GSI page)
should suffice to prove the rejection in every direction. Three of the
four directions worked exactly that way. The fourth — a base cursor
replayed *against the GSI* — returned `200` with an empty result instead
of the expected `400`, because `run_gsi_query` never got as far as calling
`validate_query_cursor_shape` at all: its own empty-page gate answered
first.

**The general lesson**: when a function has an early-return gate ahead of
a validation step, "does this validation need real data" is the wrong
question to ask when deciding whether a fixture without that data can
still exercise it — the right question is "can this fixture ever make the
*gate* false." A gate that is structurally always-true under one fixture
makes everything behind it unreachable from that fixture, independent of
whether the thing behind the gate itself needs the fixture's missing
capability. Found by running the converted test and getting a genuine,
unexpected `200` rather than by static reasoning about the code — exactly
the kind of thing "run the test, don't just read the function" catches
that a design pass alone would not. Fixed by narrowing the converted
test's own scope (dropping the one unreachable sub-case, with the reason
stated in both the test's own doc and the sibling module's) rather than by
forcing the fixture to materialize a GSI table it fundamentally cannot yet
create outside `ProdEnv`.
