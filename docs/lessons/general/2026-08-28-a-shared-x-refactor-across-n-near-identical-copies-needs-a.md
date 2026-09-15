# A "shared X" refactor across N near-identical copies needs a captured before/after mapping, not just a source read — and a Cargo dev-dependency cycle is fine (ADR 0061 rung B1, shared corpus harness)

Extracting `animus-test::corpus` (`name_seed`/`seeds_from_env`/`seed_expand`/
`for_each_seed`) out of 9 corpus files' hand-rolled copies looked, from
reading the source, like one identical FNV-1a function duplicated nine
times. It wasn't: four of the nine (`backup_fault_corpus.rs`,
`backfill_fault_corpus.rs`, `stream_lineage_corpus.rs`,
`pitr_fault_corpus.rs`) OR'd the low bit onto the hash
(`h | 1`, no comment anywhere explaining why) while the other five didn't —
a genuine, silent divergence that a "these all look the same, let me
dedupe" pass would have collapsed into a single behavior, quietly moving
every one of those four corpora's committed regression seeds. The fix
was to keep both hash flavors as two distinct public functions
(`name_seed`/`odd_name_seed`) rather than unify them, and to structure the
migration so drift was provable rather than asserted: **before** touching
each corpus file, append a throwaway `#[test] fn temp_dump_seed_map()`
that prints every frozen scenario's `(name, seed)` pair, capture that
output; migrate the file onto the shared module; rerun the identical dump
test; `diff` the two captures for byte-for-byte equality; only then delete
the temp test. Nine independent before/after diffs, all empty, is what
actually backs the "seeds didn't move" claim — the corpus's own internal
`assert_eq!(seed, name_seed(name))`-style guards are not independent
evidence, because after the refactor they check the refactored function
against itself. The general form: when consolidating N call sites that
"do the same thing," capture each site's *observable output* independently
of the code before assuming the code is identical enough to merge, and
treat that captured baseline (not a source-level read) as the diff target
for the refactored version.

The second finding was a design-constraint, not a bug: two of the corpora
being migrated (`animus-cp-data`, `animus-control`) live in crates
`animus-test` **already** dev-depends on (for its own corpus tests), so
adding `animus-test` as those crates' dev-dependency looked like it would
create a manifest cycle. It doesn't — `cargo check -p animus-cp-data
--tests`/`-p animus-control --tests` built clean immediately. Cargo
permits a dependency cycle that exists **only** through `[dev-dependencies]`
edges on both sides, because a dev-dependency is never required to build a
crate's own library — only its own tests/examples/benches, which are never
on the path of building anything that depends on that crate normally. Two
crates whose test suites need each other's test-only library code is a
legitimate shape, not a smell to route around with a lower shared crate;
worth checking with an actual `cargo check` before assuming a cycle is
real and re-architecting to avoid it.

Third, smaller finding: two authoring shapes for "expand a frozen scenario
into K seed variants" existed and both were worth preserving as distinct
primitives rather than forcing one on the other. Corpora that build a
`Vec<Scenario>` up front (`raftkv_linearizable.rs`, `txn_serializable.rs`,
`reconciler_corpus.rs`, `inplace_split_reconciler.rs`) share a
`seed_expand<T: SeedVariant>(cells: Vec<T>, k: usize) -> Vec<T>`, generic
via a two-method trait (`scenario_name`/`reseeded`) so the harness owns the
expansion loop without forcing every corpus's `Scenario` into one shape.
Corpora that drive one named scenario directly with no `Vec<Scenario>`
(`backup_fault_corpus.rs` and its three siblings) share a closure-based
`for_each_seed(name, k, body)` instead — trying to unify these into one
API would have meant either building a throwaway `Vec<Scenario>` where none
existed before, or threading a trait through call sites that have no
struct to hang it on. Two of the four `seed_expand`-style corpora
(`reconciler_corpus.rs`, `inplace_split_reconciler.rs`) also carried a
`name: &'static str` field purely to dodge threading an owned `String`
through expansion, with `reconciler_corpus.rs` actually paying for it via
`Box::leak` (a small, bounded, but real per-test-run leak) while
`inplace_split_reconciler.rs` avoided the leak by rebuilding the struct
field-by-field with a Copy-only reasoning that quietly relied on `name`
staying `&'static str` forever. Switching both to owned `String` (and
deriving `Clone`) removed the leak entirely with no observable behavior
change — the field's `'static`-ness was never load-bearing, just an
artifact of the original author reaching for the same shape as a sibling
corpus that also never needed `'static` originally.
