# Flipping an enum's `#[default]` silently re-points every `.default()` pin — audit callers for ones that meant "the current default," not "whatever the default is" (ADR 0058 rung 4 layer 2)

`animusd::config::SplitMode` had a documented "pinned to `Copy`" convention:
every deployment shape/test that didn't have an explicit override called
`SplitMode::default()`, and the doc comments at each of those call sites
said things like "byte-for-byte the original ADR 0050 workflow." Flipping
`#[default]` from `Copy` to `InPlace` (the whole point of this layer) is a
one-line change at the enum, but it silently changes the behavior of every
one of those `.default()` call sites at once — including ones nobody was
thinking about when they wrote `SplitMode::default()` instead of
`SplitMode::Copy` explicitly. That fan-out is exactly the feature for
production code (it is what makes "every deployment shape splits in-place
unless told otherwise" a one-line change) and exactly the hazard for tests:
a test that calls a `.default()`-threading bring-up helper because it
never needed to think about the knob is fine either way, but a test that
calls it because it happens to currently produce the behavior the test
actually asserts on will silently start asserting on the wrong thing, with
no compiler error and — if the two workflows converge to similar-looking
end states — sometimes no test failure either.

The concrete miss this rung found: two test files
(`animusd/tests/split_lifecycle.rs`, `animusd/tests/admin_endpoint.rs`'s
`admin_split_kicks_off_the_copy_based_workflow`) poll for a `Splitting`
parent with exactly two `Building` children — an intermediate metadata
shape that only the ADR 0050 copy workflow ever produces (the ADR 0058
in-place fork mints both children directly `Active` at cutover, with no
`Building` row ever recorded). Both files brought clusters up through
`animusd::run_node`, which threads `SplitMode::default()` with no override.
Before this layer, that was an accurate (if implicit) pin to `Copy`; after
it, the exact same code silently starts requesting `BeginSplitInPlace`
instead, and the poll would simply never observe `building.len() == 2` —
a hang-then-timeout failure with a confusing symptom (the assertion
message names the state it never saw, not the mode that made it
unreachable). `split_build.rs` (ADR 0050's own end-to-end file: build,
freeze, tail, cutover, and the copy bench) had the identical exposure. All
three were fixed the same way — call the split-mode-taking entry point
directly with `SplitMode::Copy` explicit, with a comment naming *why* (this
file/test is about the copy workflow's own mechanics, not "a split" in
general) so a future default flip doesn't quietly re-break the same
assertion.

The generalizable rule: **when a type's `#[default]` is about to change,
grep every `Type::default()` call site, not just the ones with a comment
already flagging them as pinned** — a caller that never explicitly named
the variant is exactly the one most likely to be *relying* on today's
default without saying so. Classify each one: does this caller want
"whatever the type's default currently is" (generic behavior, safe to let
ride the flip — and often the *point* of flipping the default, since it's
how the new behavior gets exercised by the existing test suite for free),
or does it want "the specific variant that happens to be the default
today" (needs an explicit pin, or it silently starts testing something
else, or nothing at all). The tell for the second category in this
codebase was assertions on a workflow's own *intermediate* state shape
(not just its converged end state) — two different mechanisms that
converge to equivalent-looking final outcomes can still have completely
different, mutually exclusive transient states along the way, and a test
built around one mechanism's transient will simply never fire under the
other's.
