# A CLI path with only parser-level tests can be broken against the real server for weeks

**Context.** Every `animus admin ...` subcommand failed against a real `animusd` admin port with
`client handshake with <addr> failed: TimedOut`. ADR 0073 Phase 0 (workstream D, layer 3) put the
client-protocol preamble (`exchange_preamble`) inside `maybe_tls_connect`, the dial shared by the
client-port commands (`status`/`put`/`get`) **and** `http_call` (the admin HTTP client). The admin
listener is plain HTTP (optionally server-only TLS) and never answers a preamble, so the CLI waited out
its 10s handshake timeout. A second bug hid behind it: over server-only TLS the admin server drops the
socket without a TLS `close_notify`, and `http_call`'s `read_to_end` surfaced rustls's `UnexpectedEof`
as `recv failed`.

**Why the tests missed it.**
- `animus-cli` had only pure unit tests (`admin_request` parsing, `extract_tls_ca`, ...) and no `tests/`
  tree; `animusd` has no dependency on `animus-cli`, so no `animusd` test ever ran the CLI binary
  (the crate guide even recorded this as an accepted gap).
- The preamble change was verified against the client port (where it is correct) and `animusd`'s own
  accept paths; nothing exercised the *other* caller of the shared dial helper.
- `animusd/tests/tls_e2e.rs` dials the admin port over TLS but deliberately ignores the read result
  (it comments on the missing `close_notify`), so the second bug was invisible there too.

**Lessons.**
- When you add protocol behavior to a shared helper (a dial, a framing function), enumerate **every**
  caller and prove each against the real peer. A helper that serves two different protocols should not
  carry per-protocol handshakes: keep the generic dial (`dial`) separate from the protocol-specific
  step (`maybe_tls_connect` = `dial` + preamble).
- A thin client with only pure parser tests needs at least one real-socket end-to-end test against the
  real server (`crates/animus-cli/tests/admin_real_listener.rs`: real `animus` binary via
  `CARGO_BIN_EXE_animus`, in-process `ProdEnv` node, plain and TLS). `animus-cli` can host it because
  it already depends on `animusd`; the reverse is not possible.
- "Ignoring the read result" in a test to dodge a protocol wart hides a real client-visible defect;
  assert on what a real client does.
- A pre-fix run was observed failing both tests with `TimedOut`; after the preamble fix alone the TLS
  test still failed with `recv failed ... close_notify`, which is how the second bug was found.
