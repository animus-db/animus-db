# A `dtolnay/rust-toolchain@1.96.0` `targets:` input does not reach the toolchain `rust-toolchain.toml` selects

**What happened (2026-10-30, ADR 0082 browser build).** The new `demo-browsers`
job installed Rust with `dtolnay/rust-toolchain@1.96.0` and
`targets: wasm32-unknown-unknown`, then failed in `cargo build --target
wasm32-unknown-unknown` with `can't find crate for core`: the target "may not
be installed".

**Why.** The repo's `rust-toolchain.toml` pins `channel = "1.96"`. rustup treats
`1.96` and `1.96.0` as two different toolchain names, so the action installed the
target on `1.96.0` while `cargo`, run inside the repo, resolved the override to
`1.96` (with no wasm target).

**Rule.** In a workflow that needs a non-host target, add an explicit
`rustup target add <target>` step run inside the checkout (so it honours
`rust-toolchain.toml`), rather than relying on the action's `targets:` input.
Host-only jobs are unaffected, which is why the existing gates never showed it.
