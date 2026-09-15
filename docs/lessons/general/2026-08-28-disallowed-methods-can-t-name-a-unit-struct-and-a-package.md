# `disallowed-methods` can't name a unit struct, and a package-level Cargo `[lints]` override beats scattering `#![allow]` across dozens of same-crate test-binary roots (ADR 0061 rung B5)

Two mechanical findings from adding `clippy.toml` `disallowed-methods`
entries for the non-`HashMap`/`HashSet` half of ADR 0003's determinism rule
(`Instant::now`, `SystemTime::now`, `tokio::spawn`,
`tokio::time::{sleep,timeout}`, `thread_rng`, `OsRng`).

**A `disallowed-methods` entry for a type used as a bare value (`OsRng`,
`rand::rngs::OsRng` — a zero-sized unit struct instantiated inline, e.g.
`&mut rand::rngs::OsRng`) silently fails its own config, not the build.**
Clippy accepts the `clippy.toml` entry but emits `warning: expected a
function, found a struct` at every crate root the moment anything in the
workspace triggers a clippy pass — a real signal, easy to read as noise and
scroll past since it doesn't fail `-D warnings` on its own (a malformed
`disallowed-methods` config entry is a warning, not an error). The fix is
routing that specific item through `disallowed-types` instead (same
`clippy::disallowed_types` lint HashMap/HashSet already use, same
`[workspace.lints.clippy]` level already set to `"warn"` for it) — a type
name goes in `disallowed-types`, a callable path goes in
`disallowed-methods`, and clippy's own warning names which one you picked
wrong. Generalizable check before adding any `disallowed-methods` entry: is
the target actually a function/method, or a type that merely gets
constructed/referenced bare? `OsRng`, `PhantomData`, a marker/unit struct
used as a value — all belong in `disallowed-types`, regardless of how
"function-like" the call site reads (`&mut rand::rngs::OsRng` looks like it
could be an argument to a function named `OsRng`, but it's a value of type
`OsRng`).

**Cargo's `[lints]` table is package-scoped, not crate(-root)-scoped — it
applies to every target the package builds (lib, every `[[bin]]`, every
`tests/*.rs` integration-test binary, every bench), which makes it the
right tool for exempting a lint across a package with many separate
compilation-unit roots, not just a `#![allow]` at one crate root.** A
source-level `#![allow(clippy::disallowed_methods, reason = "...")]` inner
attribute only covers the one file it's written in plus any `mod`s it
pulls in from the *same* crate root — it does **not** reach a sibling
`tests/other_file.rs`, since each file under `tests/` compiles as its own
independent crate root under Cargo's integration-test model. A package like
`animusd` with one `lib.rs`, one `main.rs`, and ~70 separate `tests/*.rs`
crate roots therefore has ~72 independent places a file-level `#![allow]`
would need to go to fully exempt the package by that method — worse than
useless, since it reads as "someone forgot 12 of them" rather than "this is
deliberately exempted." A package-level override
(`crates/animusd/Cargo.toml`'s `[lints.clippy] disallowed_methods =
"allow"`, replacing the `[lints] workspace = true` shorthand with the
workspace's other lints copied in by hand plus this one override) is a
single, visible, one-time decision that covers every target in the package
by construction — no target can silently fall outside it the way a missed
file would. The general principle: when a lint needs to be off for "this
whole package, as a documented exception," reach for the package's own
`Cargo.toml` `[lints]` table before reaching for `#![allow]` — the latter
is the right tool for a *specific file or item* inside an otherwise-linted
package, not for exempting the package itself, and picking the wrong one
for the latter produces an incomplete-looking wall of near-duplicate
annotations that a reviewer has to trust is exhaustive rather than a single
statement that obviously is.

Third, softer finding: **before assuming a new lint needs a lot of
`#[allow]`s, grep the real call sites first** — `grep`-ing every crate this
task actually targeted (the ones this ADR's `Env` seam is meant to cover)
for real code (not doc comments mentioning the disallowed name) found
`src/` already at zero violations everywhere except `animus-env`'s own
`ProdEnv` and the crate this rung already expected to be the hard case
(`animusd`). The discipline the lint was about to start enforcing had, in
every other crate, already held under review alone — worth confirming
before writing a single `#[allow]`, since it changes the shape of the work
from "find and annotate a pile of violations" to "confirm there's nothing
to annotate, then handle the two known exceptions."
