# Before deleting a "just adds knob X" wrapper, check what else it quietly grew (copy-split-deletion endgame, Layer B1, ADR 0058 rung 4)

Deleting the copy-based split-build driver meant deleting `SplitMode` end
to end, including `run_node_with_streams_quiesce_and_split_mode` — a
wrapper that, by its own name and its originating commit, existed *only*
to add a `split_mode` parameter over the layer below it
(`run_node_with_streams_quiesce_and_ttl_sweep_interval`, with
`ttl_sweep_interval`/`pitr_snapshot_cadence` defaulted). The naive move
once `split_mode` is gone is "delete the wrapper, point every caller at
the layer below" — and that would have been correct, except a *later*,
unrelated commit (ADR 0059's backup-store plumbing) had tacked
`backup_store_config` onto this exact wrapper's signature too, without
renaming it, because that PR's own scope was "thread this one new knob
through the existing layered stack," not "reconsider this wrapper's
reason to exist." The result: by the time this deletion touched it, the
wrapper's name promised one thing (`split_mode`) but its actual value
(letting `main.rs`/tests set `backup_store_config` without also spelling
out `ttl_sweep_interval`/`pitr_snapshot_cadence`, which have no public
default reachable from outside the crate — those modules are plain `mod`,
not `pub mod`) was independent of the knob in its name. Deleting it
outright would have forced every caller down to the layer below, which
needs two parameters (`ttl_sweep_interval`, `pitr_snapshot_cadence`) no
external caller can supply a value for. The fix was to rename it
(`run_node_with_streams_quiesce_and_backup_store`) and keep it, dropping
only the `split_mode` parameter.

**The general lesson**: a layered-wrapper function's name reflects the
knob it was *created* to add, not necessarily every knob it has
*accumulated* since — later PRs routinely widen an existing wrapper's
signature without renaming it (the path of least diff for a change whose
own scope is "thread one more knob through," not "reconsider this
wrapper"). Before deleting a wrapper because "it only exists to add knob
X," read its current parameter list against the layer immediately below
it and diff them — if there's more than the one knob the deletion is
about, and that residue isn't reachable any other way (a private default
constant, a module that isn't `pub`), the wrapper survives the deletion
under a name reflecting what it *actually* does now, not what its name
used to promise. `git log -p --follow` (or reading the commit that
introduced the wrapper) tells you what it was created for; a fresh diff
against the callee tells you what it's for *now* — trust the diff, not
the name, when they disagree.
