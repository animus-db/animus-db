# A cascading delete across replicated definitions must read the definitions *before* deleting whatever they're keyed on, and needs a second sweep keyed on a structural invariant for anything that can be provisioned concurrently with the delete.

**A cascading delete across replicated definitions must read the
definitions *before* deleting whatever they're keyed on, and needs a
second sweep keyed on a structural invariant for anything that can be
provisioned concurrently with the delete.** `ClientCtx::drop_table` (ADR
0041 §5) enumerates a table's GSI `IndexDef`s via `metadata_fresh` before
dropping the base schema — reversing the order would delete the base
schema (and the defs riding on it) first, leaving nothing to enumerate on
a retry after a mid-drop crash. But enumeration-then-cascade only catches
what existed at enumeration time; a background process that lazily
provisions the very thing being cascaded (here, the GSI drain
provisioning a hidden table's first tablet) can race a fresh one into
existence afterward. The fix pairs the definition-keyed pass with a
second sweep keyed on a structural invariant that survives the
definitions' deletion — here, the tablet map's own `<base>$<index>` naming
convention (`animus_dynamo::split_index_table_name`), not the (by-then-gone)
`IndexDef`s. The second sweep is what also makes the fix retroactive: it
cleans up orphans left by every **pre-fix** drop, for free, since it
depends on nothing the fix itself created. (`crates/animusd/src/lib.rs`,
`ClientCtx::drop_table`, 2026-08-13.)
