# A wire preamble's decode should not know which protocol it expects — split structural decode from the semantic "is this my peer" check

**What happened.** Building the ADR 0073 Phase 0 workstream D handshake
preamble (`crates/animus-env/src/handshake.rs`) for two independent
protocols sharing one codec (the internal `Network` transport and the
client/intra JSON-RPC wire, each with its own magic and version counter),
the first draft's `decode` signature took an expected `ProtocolSpec` so its
error messages could name which protocol refused the peer (`BadMagic {
protocol, found }`). That coupling turned out to be wrong the moment a
second call site appeared: a listener that accepts *either* protocol on the
same port (not this layer's problem yet, but a real Phase-2-era shape)
would have no single expected spec to pass to `decode` before it has even
read the bytes that would tell it which one arrived.

**The general shape.** A framed value's decode has two genuinely separate
jobs that are easy to fuse into one function because they run back-to-back
at the same call site: (1) turning bytes into a structured value —
purely mechanical, needs no context about what the caller *wanted* to
receive — and (2) deciding whether that value is an acceptable one for
this connection — inherently contextual, since "acceptable" depends on
which protocol/version/peer the caller expected. Fusing them forces every
decode call site to already know the answer to a question decode's own
job is partly to help answer, and it means a "wrong protocol entirely"
input (a client-protocol preamble arriving on a connection expecting the
network protocol) either has no clean way to report which mismatch
occurred, or requires threading the expectation through anyway — at which
point the split bought nothing.

**What to do.**

- **Decode structurally first, without the caller's expectation in scope.**
  Return the parsed value (or a purely structural error: truncated input,
  a length field that's out of bounds) with no opinion about whether it's
  the *right* value. Here, `decode(&[u8]) -> Result<(Preamble, usize),
  HandshakeError>` never takes a `ProtocolSpec` — `Incomplete` and
  `ExtensionTooLong` are the only errors it can produce, and both are true
  regardless of which protocol the caller expected.
- **Check the decoded value against the expectation in a separate,
  explicitly-named function.** `check_peer(expected: &ProtocolSpec, peer:
  &Preamble) -> Result<(), HandshakeError>` is where `BadMagic`/
  `UnsupportedVersion` come from — it has the expectation in hand because
  the caller passes it in exactly at the point context actually exists.
  This is also what let one error variant (`BadMagic`) correctly cover
  both "a pre-baseline peer whose first bytes are a raw frame" (garbage
  that still happens to parse into *some* magic) and "the right handshake
  shape, wrong protocol" (a client preamble checked against the network
  spec) — both are just "the peer's magic didn't match what I expected,"
  decided at the one place that has an expectation to compare against.
- **This composes for free with incremental/partial reads.** Because
  `decode` doesn't need to know the target protocol to make progress, a
  caller can read a fixed header, call `decode`, get `Incomplete`, read
  more, and retry — all before it has any reason to have picked which
  `ProtocolSpec` to check against. Fusing the two would have made that
  retry loop carry the expectation through every partial-read attempt for
  no reason, since nothing before a *complete* decode can use it anyway.

See `crates/animus-env/src/handshake.rs`'s own module doc for the concrete
byte layout and `crates/animus-env/CLAUDE.md`'s handshake entry for how
this is meant to be reused by later layers (`ProdEnv`'s accept/connect
paths, `animusd`'s client/intra port, `SimEnv`'s per-node delivery check).
