# A fixture sweep scoped to `tests/*.rs` misses `src/`'s own `#[cfg(test)]` modules (W-11 follow-up)

Roadmap W-11 (commit `7031145`) made `animus_dynamo::wire::
check_attribute_definitions` reject a `CreateTable`/`UpdateTable` whose key
schema names an attribute absent from `AttributeDefinitions`. The
groundwork commit (`c5a6a03`) swept every JSON body under
`crates/animusd/tests/*.rs` (plus the two production call sites the ADR
0061-era `CLAUDE.md` module map explicitly enumerates:
`ConsoleBackend::create_table`/`add_gsi` in `lib.rs`,
`dashboard_browser.js`'s Add-index form) to declare a real type for every
key attribute — deliberately reviewable on its own, still green against
the pre-strict decoder. But `crates/animusd/src/*.rs` also carries several
in-crate `#[cfg(test)] mod`s that build their own `CreateTable` JSON bodies
directly, for the ordinary reason this crate's own `CLAUDE.md` names
(they need private handles — a raw `CpGroup`, `pending_changes`,
`local_scan_kind_bounded` — that no external `tests/*.rs` file can reach):
`dynamo.rs::stream_write_path_tests` and `index_drain.rs::
{gsi_drain_cursor_tests, stream_sealer_tests}`. None of these were touched
by the sweep, so the strict-check commit (`7031145`) landed with 28
latent failures in `cargo test -p animusd --lib` that neither commit's own
gate run caught — the sweep was validated against the specific
`--test <binary>` targets it touched (this crate's `cargo test -p animusd`
is too heavy to run per change), and nobody ran `--lib` alongside them.

**Fixed** by extending the identical convention (declare exactly the key
attributes used, typed to match what the test actually writes — `S` unless
a test asserts `N`/`B`, no unused declarations) across all eight fixture
functions in those three modules: `dynamo.rs`'s `create_streamed_table`
plus three inline bodies (`mb`/`benchp`/`plain`), `index_drain.rs`'s
`create_table_with_gsi`, `create_streamed_table`, `create_base_table`,
`create_plain_table`, and one inline GSI+stream body in
`hot_trim_min_rule_gsi_and_stream_together`. 28 failures → 0; `cargo test
-p animusd --lib` now green (111 passed), alongside `cargo fmt --all
--check` and `cargo clippy -p animusd --lib --tests -- -D warnings`.

**General form**: a fixture sweep motivated by a wire-decoder change (or
any change to what a JSON/RPC body must contain) must grep `src/`'s own
`#[cfg(test)] mod`s for the same shape, not just the crate's `tests/`
directory — a crate with private-handle test fixtures living in-tree (this
one has at least eight such modules across `lib.rs`/`dynamo.rs`/
`index_drain.rs`/`admin.rs`/`segment_janitor.rs`, per that `CLAUDE.md`'s
own Tests section) has two disjoint test surfaces. A full `cargo test -p
<crate>` covers both, but a per-change gate that names its `--test
<binary>` targets (the usual shape here, since the whole crate is
expensive) must add `--lib` explicitly — and both must be run (and
reported) before a fixture sweep — or the check it's preparing for — can
be called complete.
