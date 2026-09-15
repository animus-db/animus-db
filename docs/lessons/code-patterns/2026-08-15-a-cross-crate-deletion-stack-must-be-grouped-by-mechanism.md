# A cross-crate deletion stack must be grouped by MECHANISM (producer symbol + every consumer + every test asserting the behavior), not by crate — a crate-scoped rung of one logical deletion is structurally incapable of staying green, at both gates that matter.

**A cross-crate deletion stack must be grouped by MECHANISM (producer
symbol + every consumer + every test asserting the behavior), not by
crate — a crate-scoped rung of one logical deletion is structurally
incapable of staying green, at both gates that matter.** Removing tablet
merge (ADR 0044, split-only tablets) was first planned as "PR1:
animus-cp-data deletes the reconciler's Absorb/WidenScope, PR2:
animus-control deletes `MergeTablets`, PR3: animusd/CLI deletes the wire
surface" — a clean-looking crate-by-crate split that turned out to be
unbuildable/untestable at every intermediate rung, for two independent
reasons discovered in sequence:
1. **Compile-time**: deleting `animus_cp_data::host::MetadataView`'s
   `merged`/`absorbed_by` fields is invisible to `cargo build -p
   animus-cp-data` (a pub field with no internal reader triggers no
   dead-code lint — the crate can't see whether some *other* crate reads
   it) and passes `cargo test -p animus-cp-data` fully green, then breaks
   `animusd::lib.rs`'s `MetadataView { merged: .., absorbed_by: .., .. }`
   struct literal with `E0560` the moment `cargo build --workspace` runs
   — the same failure shape the "grep every gating match site when adding
   a variant to a replicated/forwarded command enum" lesson (below)
   already warns about for *enum* variants, generalized to plain *struct
   fields*, and to *deletion* rather than addition.
2. **Run-time, and much easier to miss**: even after fixing the struct
   literal so the workspace *builds*, `cargo test --workspace` still
   failed — on real end-to-end `animusd` integration tests
   (`tests/tablet_merge.rs` in full, one test in `tests/split_cluster.rs`,
   one in-crate test in `index_drain.rs`) that call the *admin/wire*
   surface (`POST /admin/tablet/merge`, still fully present and
   functional) to commit a real `MetaCommand::MergeTablets`, then assert
   on the *data-plane* consequence (`HostAction::WidenScope`/`Absorb`
   actually widening the survivor's scope) that PR1 had just deleted. The
   command's own admin/CLI/wire plumbing compiles and runs fine in
   isolation — it is a specific downstream *test's assertion*, not a
   symbol reference, that silently depends on a mechanism owned by a
   different crate. No `grep`-for-a-symbol technique catches this class;
   only actually running the test suite does, and the failure reads
   exactly like an unrelated flake (a `"key .. outside tablet's current
   range; retry"` or "expected the absorbed sibling's own row" panic)
   until traced back to the deleted mechanism.
A `cargo build -p <this-crate>` / `cargo test -p <this-crate>` pair going
green is evidence the crate's *own* logic is internally consistent —
**it is not evidence the deletion is safe**, for either compile-time or
run-time consumers, whenever the mechanism being deleted has callers
outside the crate. **The fix**: plan (and land) a cross-crate deletion as
one rung per *mechanism* — producer symbol, every downstream construction
site, and every test anywhere in the workspace that asserts the deleted
behavior, all in one change — not one rung per crate. `cargo build
--workspace --all-targets` **and** `cargo test --workspace` are the two
gates that actually prove it, and both must run (and their output
actually read, not just "exit code 0 assumed") even when a task's stated
scope is "one crate only" — a green `-p` run proves nothing about a
sibling crate's literals or its test assertions. When a genuine
intermediate red state seems unavoidable, that is itself the signal the
rung boundary is wrong (regroup by mechanism), not a cost to accept and
document around.
