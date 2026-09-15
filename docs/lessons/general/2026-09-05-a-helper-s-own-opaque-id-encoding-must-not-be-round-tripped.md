# A helper's own opaque id encoding must not be round-tripped through `Display`/`FromStr` when a plain accessor already avoids it (ADR 0061 rung D1, `SimCluster`)

Building `SimCluster` (a multi-node `SimEnv` fixture over `ClientCtx<SimEnv,
SimRelayClient<SimEnv>>`, ADR 0061 rung D1), `leader_of(tablet)` was written
to return `Option<NodeId>` (matching the rung's own suggested signature),
and several early test call sites then did the obvious-looking thing to get
back a plain node index for every *other* method on the fixture (`put`/
`get`/`crash`/`restart`, all of which take a `u64` index): `leader_of(t)
.expect(..).to_string().parse::<u64>().expect(..)`. Every one of those
calls panicked with `ParseIntError`. `animus_env::nid(n)` encodes a node id
as the literal string `"n{n}"` (`NodeId::new_unchecked(format!("n{n}"))`),
not the bare decimal `n` — a `Display`-then-`parse::<u64>` round trip is
therefore not the inverse of `nid` at all, it's parsing a string that
starts with a letter as an integer.

The fix wasn't to fix the parsing (strip the leading `n` and parse the
rest) — that would work, but it permanently couples every caller of this
fixture to `nid`'s own concrete string format, which is `animus-env`'s
private implementation choice, not part of any contract `SimCluster`
should be re-exposing. Instead, `leader_of` grew a sibling,
`leader_index_of(tablet) -> Option<u64>`, which is the *same* internal
`replicas.iter().copied().find(..)` search `leader_of` already runs — it
simply returns the `u64` it already has in hand, before wrapping it in
`nid(..)` for `leader_of`'s own return, instead of throwing that plain
index away and asking a caller to reconstruct it by parsing a string that
was never designed to be parsed back.

**General form**: when a type's own id encoding is a deliberately opaque
implementation detail (a struct wrapping a `String`, an internal counter
formatted for a wire, a hash), never let API design push a caller toward
`Display`-then-`FromStr` as an implicit "get the underlying value back"
idiom — that only works when the type's author *intended* the string
representation to be the canonical roundtrip encoding, and nothing marks
that intent at the call site (the code compiles and looks correct; it
simply panics or silently misparses at runtime, exactly the "no compiler
error, only a runtime symptom" class of bug the sibling `SimRelayClient`
receive-loop entry above also describes). If a caller plausibly needs the
value in another shape, expose an accessor that returns that shape
directly from wherever the value is already in hand — never make the
caller reconstruct it by parsing the type's own display format.
