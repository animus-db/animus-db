# Every re-merge up an N-deep stacked series costs N full CI runs, and an image workflow keyed on `crates/**` charges two release compiles for a test-only change (2026-09-07, CI load)

**What happened.** With an 18-PR `gh-stack` series open, each "keep the
stack current" cascade (merge `main` into the bottom, then each branch into
the next) pushed 18 new heads, and every head re-ran the whole per-PR
matrix: `gates`, the four `prod-liveness-animusd` shards, the scattered and
hammer-pair tiers, the e2e-kind legs, and `image`'s two release builds. Two
cascades in one afternoon plus a handful of flat fix PRs put more than
twenty runs in the queue with the oldest waiting over an hour; the
maintainer noticed before the queue did. The `concurrency` group only
cancels a PR's *superseded* head, so it never helps here: every cascade head
is the PR's newest.

**Why it generalizes.** A stacked series multiplies the cost of freshness by
its depth. Re-merging the base proactively "so the maintainer never sees a
conflict" trades one cheap conflict resolution for N expensive CI runs, and
does it again after every merge to `main`. The right cadence is: re-merge
only on an actual conflict GitHub reports, or once, right before the
maintainer's merge window. The same multiplication applies to any workflow
whose trigger is broader than what it proves: `image.yml` proves the two
container images still build, but its `crates/**` trigger fired on every
`tests/`, `benches/`, `sim_cluster*.rs` and per-crate `CLAUDE.md` change,
none of which can reach the three release binaries the Dockerfile builds.
Its `paths` list now carries negations for exactly those (the workflow
header lists the evidence for each); the `gates` job's `--all-targets`
build still catches a compile break in any of them.

**Rules.** (1) Never cascade a stack proactively; a conflict or an imminent
merge is the trigger, and the union merge driver makes the resolution
itself cheap. (2) While the CI queue is deep, hold pushes rather than adding
to it; the queue is shared with the maintainer's own merges. (3) When a
workflow's trigger says `crates/**`, ask what it proves and carve out the
paths that cannot change that, each with a one-line reason a reader can
verify in the tree.
