# Consolidate duplicated test helpers by hashing the bodies, not by trusting the name

Sixty `sim_*` files each carried their own `env_seed`, a dozen their own
`poll_until`, and so on, under a "per-file-fixture convention". Folding them
into `sim_support` was safe only because each family was first split into
hash classes of the normalised bodies (visibility prefix stripped, a path-only
`animus_tablet::TabletId` vs `TabletId` difference folded, a renamed copy
folded). The name was no guide: `create_table` was twelve different bodies in
three classes, `error_type` two classes with different failure behaviour
(panic vs empty string), and three `leader_of_table` bodies that resolve the
same tablet with different panic messages.

What worked:

- Extract every candidate by brace-matching a script, hash it, and migrate by
  class. A class with one member stays local.
- When two non-identical bodies share a name, keep both in the shared module
  under distinct names and have the caller `use ... as old_name`; no call site
  and no panic message changes.
- A helper that calls another (`converge_gsi_active` calls `leader_of_table`)
  must be moved with the exact callee its original copy resolved to, not the
  nearest-looking one, or its failure text silently changes.
- Run `cargo fix` for the unused imports the deletions strand, then check that
  the only added lines in the diff are `use` lines.
- Inherent methods on file-local types (`fresh_value` on two recorder structs)
  are not free-function duplicates and are not candidates.

The `sim_*` files' "duplicated per this crate's convention" comments pointed at
the opposite of what the code now does; the crate guide was updated in the
same change.
