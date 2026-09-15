# A rustdoc comment naming a deleted *file* or *function* by string is invisible to every compiler gate — `grep` for the deleted symbol's bare name, not just its `Rust`-identifier occurrences, after any deletion.

**A rustdoc comment naming a deleted *file* or *function* by string is
invisible to every compiler gate — `grep` for the deleted symbol's bare
name, not just its `Rust`-identifier occurrences, after any deletion.**
While deleting `MergeTablets` (ADR 0044 PR2), `grep -rn "MergeTablets\|
merged_tablets\|absorbed_by"` cleanly found every real reference, but a
separate case-insensitive sweep for the bare word `merge` turned up doc
comments in `animusd/src/lib.rs` still naming `tests/tablet_merge.rs` (a
file PR1 had already deleted) and describing a "merge crossover"/
"merge-residue cursor-row cleanup" that a *different*, already-deleted
function used to handle — none of it a compile error, since `rustc` never
parses doc-comment prose for symbol references. The fix is procedural:
after grepping for and fixing every exact-symbol match, do one more
broad, case-insensitive grep for the deleted mechanism's plain-English
name across every file actually touched (not the whole workspace, which
turns up too many unrelated senses of a common word like "merge" — LSM
merge, git merge, `StorageEngine::merge`) and read each hit for staleness,
not just symbol presence.
