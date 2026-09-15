# A convergent per-name cursor/checkpoint row that survives its owner's deletion can silently poison a same-named recreation — audit every "resume from where I left off" row for whether its *key* is unique to one lifetime of the thing it tracks, not just to its current name.

**A convergent per-name cursor/checkpoint row that survives its owner's
deletion can silently poison a same-named recreation — audit every
"resume from where I left off" row for whether its *key* is unique to one
lifetime of the thing it tracks, not just to its current name.** The
backfill seeder's cursor row (`KIND_CURSOR`, tag `backfill:{index_name}`,
ADR 0045 §2) is keyed purely by index *name*. Dropping an index (ADR 0045
§5) removes its catalog entry and hidden table but, absent an explicit
fix, leaves that cursor row exactly where it was — harmless in isolation
(the row just describes a scan position for an index that no longer
exists), until a *later* `CreateTableIndex` proposes a **new** index
under the **same name**: its fresh seeder reads the old row, believes it
has "already scanned" up to the deleted index's old position, and skips
every partition before that point — the recreated index can flip
`Active` having backfilled *nothing*, a silent, non-crashing correctness
bug (the row is bytes-valid, just semantically stale). Two considered
fixes: (1) make the cursor's key incorporate something that changes
across a delete/recreate of the same name — not chosen here, since it
would touch the already-shipped `IndexDef` wire shape for a problem step
(2) closes with zero schema change; (2) **actively delete the row when
the owner is deleted**, chosen — but a *single* delete pass raced a
seeder tick that had already read the schema (as still-`Creating`) a
moment before the owner's deletion committed and could still write a
fresh, stale value *after* the delete. Closed the practical window by
running the delete twice (immediately after the status-`Deleting`
transition commits — after which the seeder's own gate excludes the
index from every *new* tick — and again at the very end of the drop
cascade), which is not a full formal proof but is the same
documented-residual-gap posture `cursor.rs`'s own module doc already
takes for a different byte-alignment gap; the create-drop-recreate
regression (`tests/update_table_drop_index.rs::create_drop_recreate_
same_index_name_backfills_from_scratch`) is the test that would catch a
regression here. **General rule: whenever a delete path removes the
*thing* a checkpoint row tracks but the checkpoint's own key could be
reused by a future recreation of the same name, either scope the key to
a value that can't repeat, or make deletion of the thing also delete the
checkpoint — "the row is harmless garbage" is only true until the name
comes back.** (`crates/animusd/src/index_drain.rs::clear_backfill_cursor`,
`crates/animusd/src/dynamo.rs::drop_index`; ADR 0045 §5, 2026-08-15.)
