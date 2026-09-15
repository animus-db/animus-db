# The copy-based split-build driver (deleted by the copy-split-deletion stack, 2026-09-01)

Moved verbatim from `docs/engineering-lessons.md` when the copy-based
split's own background driver (ADR 0050 Train B — `SplitBuild`/
`split_driver_tick`/`ship`/`ship_all`/`tail_pass`/`SeedRows`/
`MetaCommand::BeginSplit` itself, and the `--split-mode {copy,inplace}`
selector that used to choose it) was deleted whole (Layers A/B1/B2 of that
stack; see `docs/adr/0058-*.md`'s 2026-09-01 as-built note and
`docs/adr/0050-*.md`'s matching amendment). The in-place split (ADR 0058
Train 2, directed by ADR 0062) is the sole surviving split mechanism. The
generalized forms keep pointers in the live log.
