# animus-web

AnimusDB in a browser tab (ADR 0082): a `wasm-bindgen` shell over
`animusd::embedded::EmbeddedCluster`, the deterministic `SimCluster` on a
virtual clock. It powers `website/demo.html`.

## Layout

- `src/core.rs`: **plain Rust**, no JS types, native-tested. `Core` owns the
  `EmbeddedCluster` and the request pump: `submit` -> `tick(real_ms, speed)` ->
  completed requests; a request unanswered after 30 virtual seconds resolves
  `504`; `tick` clamps one frame to 100 real ms and speed to 100x so a returning
  background tab does not burst. Put logic here, not in the shell.
- `src/shell.rs`: the `#[wasm_bindgen]` class `AnimusCluster`:
  `new AnimusCluster(seed, nodes)`, `request(node, target, bodyJson) -> Promise<{status, body}>`,
  `tick(realMs, speed)`, `crash/restart/partition/healAll/grow`, `status() -> JSON string`.
  It holds only JS glue (promises, `JsError`) and a panic hook to `console.error`.

## Rules

- **Real time stays in JS.** The page measures elapsed ms in `requestAnimationFrame`
  and passes them in; never read a clock here (the workspace `disallowed_methods`
  lint is on). A run is a function of the seed and the calls/steps made on it.
- **No panics reachable from a click.** A panic aborts the wasm instance and the
  page can only ask for a reload. `EmbeddedCluster` methods return `Err` for bad
  input (unknown node, `grow` while degraded); `random_clicks_never_panic` drives
  random fault/request sequences natively. Add the case there when a new control
  is added.
- **Lints:** the workspace forbids `unsafe_code`, which `wasm-bindgen`'s macros
  need. This crate is `deny` with one justified `#[allow(unsafe_code)]` on the
  `shell` module only; `core` is unsafe-free. Keep the clippy lints.
- **Dependencies:** `animusd` with `default-features = false, features = ["embedded"]`
  (a path dep, because a workspace dep cannot turn default features off). CI's
  `wasm` job asserts `socket2`, `tokio-rustls` and `opentelemetry` are not in the
  wasm graph; anything that adds one is a `server`-feature leak in `animusd`.
- **Single-threaded.** No threads on `wasm32-unknown-unknown`; no stack-size flag
  was needed (7-9 nodes, transactions, GSIs and PartiQL ran clean at the default
  1 MB wasm stack). If a deep future ever overflows, set
  `-C link-arg=-zstack-size=...` through `.cargo/config.toml` target rustflags.

## Building the demo

`scripts/build-web-demo.sh` (profile `wasm-release` in the root `Cargo.toml`, then
a `wasm-bindgen-cli` whose version equals the `wasm-bindgen` entry in `Cargo.lock`)
writes `website/demo/pkg/` (gitignored). `wasm-opt` is deliberately not run: it
shrank the raw file but grew the gzipped one. Size on 2026-10-30: 5.7 MB raw,
1.68 MB gzipped.

## Testing

`cargo test -p animus-web` (native; about 5 s). The browser run is manual:
`scripts/build-web-demo.sh`, serve `website/`, drive with Playwright (start the
cluster, CreateTable/PutItem/GetItem/Query, crash the leader node, write, heal).
