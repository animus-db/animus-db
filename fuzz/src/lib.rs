//! AnimusDB fuzzing (roadmap R-01 (c)).
//!
//! * [`targets`] — one function per fuzz target; the libFuzzer binaries under
//!   `fuzz_targets/` are one-line wrappers around them.
//! * [`seeds`] — seed inputs: the golden format fixtures
//!   (`crates/*/tests/fixtures/formats/`, mapped by `seeds.tsv`), a few
//!   decoder-specific slices derived from them, and the hand-written seeds under
//!   `fuzz/seeds/`.
//! * [`mutate`] — a tiny seeded PRNG + byte mutator, so the stable-toolchain
//!   smoke (`tests/smoke.rs`) is deterministic and needs no libFuzzer.
//!
//! See `fuzz/README.md`.

pub mod mutate;
pub mod seeds;
pub mod targets;
