# Feature-gating an item everything transitively depends on (ADR 0061 rung C0)

Gating `animus-env`'s `ProdEnv`/`FsSegmentStore` behind a default-off `prod`
feature (so a crate can depend on `animus-env` with `default-features =
false` and genuinely not have `ProdEnv` in its build) turned up two
generalizable points, beyond the "grep real code, not doc comments" one
above (the same trap bit again here: `tracing` was declared as an
unconditional dependency, but every non-doc-comment `tracing::` call in the
crate lived in `prod.rs` — the crate had been unknowingly carrying a
prod-only dependency as if the whole crate needed it, for a `tracing`
version bump exactly zero call sites outside `prod.rs` would ever notice).

**First: to prove a feature gate actually closes the door, don't just read
the `#[cfg]` and reason about it — compile against it.** A tiny scratch
crate (own `Cargo.toml`, path-dependency on the gated crate with
`default-features = false`, one line naming the item that should be
unreachable) is cheap and unambiguous: either it fails to compile with a
"configured out ... gated behind the `X` feature" note pointing at the
`#[cfg]`, or it doesn't, and either way you have the compiler's word for it
instead of your own reading of the manifest. (Needs the target crate's
`rust-toolchain.toml` copied alongside it if the workspace pins a toolchain
newer than whatever `rustc` is on `PATH` — otherwise the scratch build fails
on an unrelated MSRV mismatch before it ever gets to the question being
tested.) The same technique generalizes to proving any "X should not be
reachable from Y" claim about a manifest, not just this one.

**Second: `cargo tree -e features -p <crate>` on its own is not the tree
that ships** — by default it includes the package's `[dev-dependencies]`,
so a dev-only feature (like `prod` on a `[dev-dependencies]` entry added
specifically so the *library* stays clean) shows up in the default `tree`
output and can look like it leaked into the library when it didn't. Add
`--no-dev-dependencies` to see the graph `cargo build -p <crate>` (library
only, no tests/benches) actually resolves — that is the tree that answers
"does the library itself pull this in." Separately: with the workspace's
resolver (`resolver = "3"`, the edition-2024 default), a package's own
`[dev-dependencies]` features are *not* unified into its own library build,
confirmed by comparing `--no-dev-dependencies` output (no `prod`) against
plain `cargo tree` output (shows `prod`) for the same package — but a
single `cargo build --workspace --all-targets` invocation still unifies
features across every target it builds *together*, including other
packages' test binaries, so putting a feature on `[dev-dependencies]`
documents "the library doesn't need this" precisely rather than
mechanically *enforcing* it under every possible invocation — say so
plainly rather than overclaiming the boundary when reporting work like
this.
