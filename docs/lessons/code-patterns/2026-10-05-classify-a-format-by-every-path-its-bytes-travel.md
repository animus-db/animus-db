# Classify a format by every path its bytes travel, not by where it is stored: an engine row value rides the snapshot image to peers.

**`efcaa6cb` added `txn-envelope` v2 (intent tag 2) and classed it node-local
("durable engine row values"), then wrote it unconditionally. Engine values leave
a node inside the tablet `InstallSnapshot` image, which ADR 0073 already
classed G, so a previous-release replica that received a repair snapshot from an
upgraded leader panicked on `txn: unknown envelope tag 2` (#1237, found by the
real-process previous-release roll job, not by any SimEnv corpus).**

- A value that lives *inside another carrier* is as shippable as that carrier.
  When a format row says "inside X", the class is the strictest of its own and
  X's. Ask "which of this format's bytes can another node read?" for every
  channel (log entries, wire frames, snapshot images, seed/import rows), not
  "where is it persisted".
- The fix shape for a stored-and-shipped value: **apply writes the new version
  unconditionally into the local engine (apply never branches on a gate), and the
  sender down-converts at the one exit** (`engine_image`) until the gate opens.
  Check what the old reader's *fallback* needs from the down-converted form:
  dropping v2's `prior` re-enables the abort-tombstones-an-acked-value bug on
  the receiver unless the prior is shipped where the v1 lookback reads it (the
  committed row one MVCC version below the intent). A naive downgrade would have
  regressed the very fix that introduced the format.
- A same-code `BinaryProfile` corpus cannot see this class of bug (the "old"
  node runs the new decoder); only a real previous-release binary can. Keep the
  P3-E job's transactions variant on, and keep a SimEnv test that asserts the
  *bytes* the sender ships while the gate is closed (`txn_envelope_gate.rs`).
