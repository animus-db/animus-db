# Check a pooled async lock's real scope before restructuring around it

**What happened.** Wiring the ADR 0073 Phase 0 workstream D handshake
preamble into `ProdEnv`'s dial path (`crates/animus-env/src/prod.rs`'s
`send_frame_pooled`/`connect_maybe_tls`) meant adding one more network round
trip — writing this build's preamble, then waiting for the peer's — to a
connect that already happens under a lock. The reviewing instruction for
that layer explicitly called this out: check whether the dial happens while
a "pool lock" is held, and whether waiting an RTT under it would serialize
sends to *other* peers or risk a deadlock, restructuring (dial outside the
lock, then insert) if so.

The answer turned out to be "no restructuring needed," but only because the
lock's *actual* scope had to be read carefully rather than assumed from the
word "pool." `Inner.conns` is a `BTreeMap` behind a `StdMutex`, but
`send_stream` clones the per-*address* `Arc<tokio::sync::Mutex<..>>` out and
drops the map's guard *before* any `.await` — that map lock is never the one
in question. The lock actually held across the connect (and now the
handshake) is the per-address `tokio::sync::Mutex` slot itself, taken once
per destination. Two sends to *different* peers get two different slots and
never contend; two sends to the *same* peer already had to serialize behind
this lock before the handshake existed at all, for a reason with nothing to
do with connecting — it's what keeps concurrent senders' frames from
interleaving on one connection. Adding a round trip inside that scope costs
concurrent senders to that one peer a bit more wait; it costs every other
peer's sends nothing, because they were never sharing this lock to begin
with. This was also exactly the scope the TLS handshake had already been
paying for since ADR 0064 (`tls.connector.connect(..)` runs under the same
slot), so the new code could just extend `connect_maybe_tls`'s existing
body rather than invent a new locking shape.

**The general shape.** "Is X a global lock, a per-key lock, or something in
between?" is a question about the *type* backing the lock and *where the
guard is dropped*, not about what the surrounding code calls it (a "pool,"
a "cache," a "registry"). A map-of-locks pattern — a coarse lock guarding
only insertion/lookup into a map of finer per-key locks, with the coarse
guard dropped before the finer lock is ever taken — is common enough
(connection pools, per-shard caches, per-tablet state) that it's worth
naming explicitly: check which lock a piece of new latency-adding work would
actually run under by tracing the guard's lifetime, not by pattern-matching
the field's name. Getting this wrong in the paranoid direction (assuming the
outer map lock is held and restructuring to dial outside it "just in case")
would have added real complexity — a two-phase dial-then-insert dance — to
fix a problem that provably could not occur, because the code already had a
narrower, correct lock scope than its name suggested.

**What to do.**

- Before adding blocking/awaiting work to a path that runs "under a lock,"
  find every lock actually held at that point by tracing guard
  acquire/drop, not by the variable or field's name.
- If a map-of-per-key-locks pattern already exists (guard the map only for
  lookup/insert, hold a per-key lock for the actual work), a new round trip
  added to that per-key work inherits the same isolation for free — verify
  it, then say so, rather than assuming coarser scope and restructuring
  around a lock that was never actually in the way.
- Look for prior art already accepting the same cost: if an existing
  handshake (here, TLS) already runs inside the scope you're worried about,
  that's both proof the scope is safe and a template for where the new code
  belongs.

See `crates/animus-env/src/prod.rs`'s `send_frame_pooled`/`connect_maybe_tls`
doc comments for the concrete lock-scope note, and
`crates/animus-env/CLAUDE.md`'s handshake entry for how this composes with
the accept side's own (unrelated) per-connection task.
