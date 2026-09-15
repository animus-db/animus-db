# A `#[cfg(any(test, feature = "X"))]` module is invisible to an integration test without a self dev-dependency enabling that feature

`animus-s3`'s in-process fake transport (`fake.rs`) is gated
`#[cfg(any(test, feature = "fake"))]` — the intent being "always available
to this crate's own tests, and to anyone else's tests via an explicit
feature." That gate alone does NOT make `fake` visible to an integration
test under `tests/`: `#[cfg(test)]` is only active for the crate's own
lib/bin compiled *as a test binary* (`cargo test`'s unittest target); an
integration test in `tests/` links against the crate's plain,
non-`cfg(test)` `rlib` — the same one a normal downstream dependent would
get. Without the `fake` feature explicitly on, `tests/client_fake.rs` got
`error[E0432]: unresolved import` with `note: found an item that was
configured out`, even though `cargo test -p animus-s3` (no `--features`)
was clearly running "this crate's own tests."

**The fix**: add the crate as its own `[dev-dependencies]` entry with the
feature turned on (`animus-s3 = { path = ".", features = ["fake"] }`) —
Cargo's standard, documented idiom for "this optional/test-only module
should be available to every test target of this crate without requiring
`--features` on the command line." Cargo's newer feature resolver unifies
a package's dev-dependency features into the build specifically when
building dev targets (tests/benches/examples) in the same invocation, so a
plain `cargo build -p foo` (no test targets) still doesn't pull the
feature in — exactly the boundary this crate wants (`prod`/`fake` off by
default for a plain library consumer, on automatically the moment any test
target of the crate itself is built).
