# Splitting a crate into `server` and `embedded` builds: gate whole items, keep the generic core

`animusd` had two jobs in one crate: the `E: Env`-generic request path (the
node's brain, already exercised under `SimEnv`) and the real process around it
(sockets, TLS, `ProdEnv`, OpenTelemetry, signals, the binary). Making the first
compile for `wasm32-unknown-unknown` needed no rewrite, only a cargo feature
split (`server`, default on; `embedded`) and `#[cfg(feature = "server")]` on whole
items.

Rules that came out of it:

- Gate whole items or whole `impl` blocks, never statements inside a body. A body
  half-gated by cfg is two programs and only one of them is ever compiled by the
  person editing it. Where an enum has a server-only variant (the store handles
  keep their `S3(Arc<dyn SegmentStore>)` variant and gate `Fs`/`Cluster`), gate
  the variant and each match arm, and keep a catch-all-free `match` so the
  compiler still proves exhaustiveness in both builds.
- Do not fix a gating error by deleting the gate the compiler complains about:
  the error usually points at the *consumer* of a gated item, and the right move
  is to gate that consumer as well (a mechanical fixpoint: gate, recompile,
  repeat). Review each auto-gated item, though: an `impl<E: Env, R> ClientCtx<E, R>`
  method that merely names `TlsMaterial` is not server-only, its parameter is.
- A type parameter defaulted to `ProdEnv` (`ClientCtx<E = ProdEnv, ...>`) needs a
  build-dependent alias (`DefaultEnv`) so the bare name still means something in
  a build with no `ProdEnv`. Make every defaulted site use the alias; do not
  add a second default.
- Tokio features are additive across the whole graph and a member cannot subtract
  a workspace feature. Declare the crate's own `tokio` directly with the minimal
  feature set and let `server` add `net`/`fs`/`signal`/`rt-multi-thread`/`time`.
  Leaving `time` out of the embedded build turns every stray `tokio::time` into a
  compile error instead of a runtime panic in a browser.
- `autotests` cannot carry `required-features`. A crate with ~100 socket tests
  needs `autotests = false` and an explicit `[[test]]` list; say so in a comment,
  because a new `tests/foo.rs` is otherwise silently not built.
- A background loop that was concrete over `ProdEnv` only because it called
  `tokio::time::sleep` becomes generic by calling `ctx.env.sleep` (`ProdEnv`
  implements `sleep` as exactly `tokio::time::sleep`, so the server build is
  unchanged). That is what let an embedded cluster run the GSI/stream change
  consumer on the virtual clock.
- The embedded-only build legitimately has dead code and unused imports (it reaches
  a subset of the client path). Allow those two lint families for that build only,
  with a reason, and rely on the `server` build's lint run for dead-code coverage.
  Do not silence anything else.
- A test-only helper that a fixture now needs in a non-test build is widened from
  `cfg(test)` to `cfg(any(test, feature = "embedded"))`, never to `pub`.
