# A peer-supplied length prefix needs a cap before the allocation

`ProdEnv`'s internal-wire `read_frames` read a `u32` sender-id length and a
`u32` payload length off the socket and did `vec![0u8; len]` for each with no
bound. The internal port is open unless mutual TLS is configured, so any
reachable host could force ~4 GiB of allocation per frame (memory-exhaustion
DoS). The client protocol's codec (`animus-node` `MAX_FRAME_LEN`) already
checked its length before allocating; the internal frame reader was written
separately and never inherited the habit.

Rules:
- Any `read_uN` followed by `vec![0; n]` / `Vec::with_capacity(n)` on a
  network edge checks `n` against a named cap first and closes with a named
  `InvalidData` error. The cap on the *id* field is separate and tiny.
- `animus-env` cannot import `animus_node::MAX_FRAME_LEN` (the dependency runs
  the other way), so the value is duplicated there; keep the two equal.
- Test the reader generically over `AsyncRead` with a `tokio::io::duplex`
  carrying only a header: an unchecked reader allocates and then hangs in
  `read_exact`, so a short timeout around the call distinguishes the two.
- When fixing one such site, grep the whole edge (`read_u32`/`read_u64`,
  `vec![0`) for siblings.
