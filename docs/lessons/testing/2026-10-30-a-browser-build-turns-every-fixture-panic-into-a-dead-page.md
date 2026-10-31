# A browser build turns every fixture panic into a dead page: make the embedding API total

`SimCluster` was written for tests, where `assert!`/`expect` on a stuck condition
is the right failure mode. Embedded in a wasm page the same panic aborts the wasm
instance: the JS object stays borrowed and every later call fails with
"recursive use of an object detected", with no message unless a panic hook
forwards it to `console.error`.

Found by driving the real page: `grow` with one node crashed hit
`poll_until`'s "condition did not converge" assertion, because the fixture waits
for *every* node to see the new member and a muted node never does.

Rules:

- Anything a UI control can reach through the embedding wrapper (`EmbeddedCluster`)
  returns `Result`, and refuses preconditions the fixture would assert on
  (`grow` while degraded). Do not rely on the page greying out the button.
- Install a panic hook that writes to `console.error` in the wasm shell, and have
  the page show a "reload" message when a call throws.
- Keep a native randomized "random clicks never panic" test over the plain-Rust core
  (`animus-web` `random_clicks_never_panic`), and drive the real page with
  Playwright after adding a control: native tests passed the same `grow` case
  that panicked in the browser only when no node was crashed.
