//! AnimusDB in a browser tab (ADR 0082).
//!
//! Two layers:
//!
//! * [`core`] — plain Rust, no JavaScript types: a [`core::Core`] wrapping
//!   `animusd::embedded::EmbeddedCluster` with the request pump (submit, advance
//!   virtual time, resolve finished requests, time out stuck ones) and a
//!   status-as-JSON accessor. Native unit tests cover it.
//! * [`shell`] — the thin `#[wasm_bindgen]` class `AnimusCluster` the website
//!   loads. It owns only the JS-facing glue (promises, `JsError`).
//!
//! Real time never enters Rust: the page measures elapsed milliseconds with
//! `requestAnimationFrame` and passes them to `tick`, so a run is a function of
//! the seed and the sequence of calls (see `EmbeddedCluster`).

pub mod core;

// `wasm-bindgen`'s macros expand to `unsafe` glue that the workspace's
// `unsafe_code = "forbid"` cannot admit, so this crate is `deny` with this one
// allow, on the one module that uses the macros. The shell holds no `unsafe`
// of its own.
#[allow(
    unsafe_code,
    reason = "wasm-bindgen's #[wasm_bindgen] macro expands to unsafe FFI glue; the module itself contains no unsafe"
)]
pub mod shell;
