# Promoting a per-file test helper (`free_addrs`, `start_single_node`) into the shared `tests/support/mod.rs` makes every consumer that doesn't call every helper trip `dead_code` under `cargo build`/`clippy -D warnings` — this is inherent to the `mod support;`-per-binary-crate structure (each `tests/*.rs` file is compiled as its own crate, so per-binary dead-code analysis only sees the subset of `support` items *that binary* references), not a sign the consolidation is wrong.

**Promoting a per-file test helper (`free_addrs`, `start_single_node`) into
the shared `tests/support/mod.rs` makes every consumer that doesn't call
every helper trip `dead_code` under `cargo build`/`clippy -D warnings` — this
is inherent to the `mod support;`-per-binary-crate structure (each
`tests/*.rs` file is compiled as its own crate, so per-binary dead-code
analysis only sees the subset of `support` items *that binary* references),
not a sign the consolidation is wrong.** `#![allow(dead_code)]` at the top of
`tests/support/mod.rs` is the standard, correct fix for a shared
multi-consumer test-support module — cheaper and more honest than adding a
synthetic use of every helper in every file. Before unifying near-duplicate
helpers across files (e.g. several `start_single_node` copies that differ
only in whether they take a `backend: StorageBackend` param, or whether they
call `run_node` vs `run_node_with` — the latter is just the former with
`StorageBackend::default()`), diff the actual bodies first: identical
bodies unify trivially; a narrower shape (fewer params) can be expressed as
the wider shared shape with an explicit default argument at each narrower
call site, so unification doesn't require the shared version to be
polymorphic. (`crates/animusd/tests/support/mod.rs`.)
