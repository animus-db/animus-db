# Hoisting duplicated test helpers: hash the bodies, and move dependents only with their dependencies

When de-duplicating helpers that were copy-pasted across `animusd`'s `tests/*.rs`
binaries into `tests/support/mod.rs`:

- Extract every function with a comment-masking brace matcher, normalise
  whitespace, and hash it. Same *name* does not mean same body: `call`, `json`,
  `admin` and `create_table` each have several hash classes (a `Value` vs
  `serde_json::Value` spelling, `Option` vs plain return, different `.expect`
  text). Only one class per name can live in `support`; the others stay local.
- A helper that calls another helper (`error_code` -> `json`, `kickoff_tablet`
  -> `admin`) moves only in files whose dependency also moved. Otherwise the
  moved copy would silently bind to a different class of the dependency.
- `cargo fix` leaves some now-unused `use` lines (single-item imports); finish
  with clippy `-D warnings` in a loop. `support/mod.rs` has
  `#![allow(dead_code)]`, so moved helpers need no per-binary gating.
