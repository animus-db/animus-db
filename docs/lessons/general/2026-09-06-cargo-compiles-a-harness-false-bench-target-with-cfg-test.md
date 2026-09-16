# Cargo compiles a `harness = false` `[[bench]]` target with `--cfg test` (confirmed empirically, not assumed)

Writing `wal_fsync_bench.rs`, the natural instinct was to import
`animus_env::nid` unconditionally (`main()` uses it directly, not inside
any `#[cfg(test)]` block). `animus-storage`'s own `engine_bench.rs` —
the established precedent this bench's style otherwise mirrors exactly —
instead imports it as `#[cfg(test)] use animus_env::nid;`, which looks at
first glance like a latent bug (an import gated to test builds, used from
plain `fn main()`). It is not: `cargo check -p animus-storage --bench
engine_bench` (and the equivalent for this new bench) both compile clean
with that exact gating, confirming Cargo passes `--cfg test` when
compiling ANY `[[bench]]` target, `harness = false` included, not only
the built-in libtest harness shape. Mirrored the same gating in this
bench's own `nid` import rather than "fixing" it to a plain unconditional
`use`, which would have been a harmless but unnecessary deviation from
the established convention.

**General form**: before "fixing" an import gate that looks wrong in an
existing, working file this repo already gates the identical way, verify
the actual Cargo/rustc behavior with a quick `cargo check` rather than
trusting first-glance reasoning about what `#[cfg(test)]` should or
shouldn't reach — Cargo's bench-target compilation semantics are not
obvious from the attribute's name alone.
