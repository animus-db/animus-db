# Extracting a `&self` method's body into a pure free function can strand a second, now-orphaned `&self`-taking helper as dead code, even though nothing about *it* changed

**Extracting a `&self` method's body into a pure free function can strand
a second, now-orphaned `&self`-taking helper as dead code, even though
nothing about *it* changed** (ADR 0061 rung A3). `LsmEngine::
next_compaction` called `self.level_table_budget(level)`; pulling
`next_compaction`'s body out into a free `next_compaction_plan(tables,
opts)` meant the new free function now calls a *new* free
`level_table_budget(level, opts)` directly — and the old `&self` method
wrapper, never called from anywhere else, became a silent `dead_code`
warning (which is `-D warnings` under this repo's clippy gate, so it's a
build failure, not just noise). Caught by running the crate's own
`cargo clippy --all-targets` before committing, not by the extraction
itself. **General rule for this class of refactor** (the same shape ADR
0061's A6 keystone rung is about to do at much larger scale): after
pulling a method's logic out into a free function, `grep` every sibling
helper the old method used — a helper with exactly one caller doesn't
survive the caller's disappearance.
