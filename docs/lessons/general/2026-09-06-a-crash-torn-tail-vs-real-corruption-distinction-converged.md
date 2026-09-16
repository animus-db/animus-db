# A crash-torn-tail-vs-real-corruption distinction converged independently on the same rule an existing sibling layer already uses — a signal the rule is right, not a coincidence to ignore (ADR 0069, encryption at rest)

Building the AEAD frame-index scanner for encryption at rest
(`animus-env/src/encrypted.rs`), the question was: when a frame in an
already-marker-verified (so provably correctly-keyed) file fails to parse
or authenticate, is that a torn tail (safe to truncate and forget) or
real corruption of already-durable bytes (which must be a hard error, or
silent data loss follows for every intact frame that happened to sit
after the corrupted one)? The answer arrived at — continue scanning past
a failure using its own still-intact length field purely to check whether
anything *valid* follows; nothing valid after it means a genuine tear
(truncate), something valid after it means real corruption (refuse) —
was derived independently, before rereading `animus-storage`'s own
hand-rolled WAL-record codec.

It turned out `lsm.rs`'s own doc already states the identical rule for
its own, unrelated CRC32-framed WAL records, in almost the same words:
"distinguishing a legitimate crash-torn trailing record from real
corruption is not a magnitude check on the frame — it's positional... a
bad frame *followed* by more valid frames can only be corruption of
previously-durable data (a crash cannot reach past the tear point)."
Two independently-designed framing layers, at different levels of the
stack, converged on the same rule because the rule follows directly from
what a crash physically *can* and *cannot* do to a file (only ever tear
the true end; never leave valid bytes downstream of where it stopped) —
not from either format's own specifics. **General form**: when a new
framed/chunked on-disk format needs a torn-tail recovery rule, check
whether a sibling format in the same codebase already solved the
identical positional question — a match is strong validation the
answer is right, not merely convenient; a mismatch is worth understanding
before shipping either design as the codebase's convention.
