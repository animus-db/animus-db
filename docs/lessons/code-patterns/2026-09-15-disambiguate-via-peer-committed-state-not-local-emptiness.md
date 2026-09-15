# When local state can't disambiguate two safe-looking cases, ask a peer what it already knows about YOU specifically

Found building issue #667's fix (a voter with wiped/empty persisted state
must not vote/campaign until it knows whether it's a genuine fresh
bootstrap or an already-established voter restarting amnesiac).

## The trap: "any peer with real history" is not the same question as "is THIS restart safe"

A `RaftCore` whose persisted state replays empty looks byte-for-byte
identical in two very different situations:

- **Genuine fresh bootstrap or ADR 0060 growth join**: this exact node
  identity has never cast a vote anywhere, ever. Safe to vote/campaign
  immediately — it cannot possibly contradict a vote it never cast.
- **Wiped, already-established voter**: this exact node identity DID cast
  a real, persisted vote before, and the disk holding the memory of that is
  gone. Unsafe — it could grant a second, contradicting vote in a term it
  already voted in.

The first attempt at disambiguating these asked peers "do you have real
history?" (`term > 0 || committed_index > 0`) and treated any `true` answer
as proof of the second case. This is wrong: an ADR 0060 growth join's peers
*also* have real history (that's the whole point of joining an established
cluster) — the naive rule refused every legitimate new voter the instant
its own `change_membership` committed, verified live via
`wiped_voter_rejoin.rs`'s `growth_then_wiped_leader_rejoin_reestablishes_
leader`: a freshly-grown 4th voter livelocked the whole group by refusing
itself, dropping the live quorum below majority.

## The fix: ask "do you already know ME as a voter," not "do you know anything"

The real disambiguator was never "does the cluster have history" — it was
"does that history already include a claim on MY OWN identity as a voter."
A peer's honest *committed configuration* answers exactly that, for free,
with no extra round trip: if the responding peer's own config doesn't (yet)
name the asker, the asker cannot have a forgotten prior vote under this
identity in this cluster, full stop, regardless of how much history
everyone else has. If it does, that's the actual wiped-voter signal.

## The generalizable rule

When two situations are locally indistinguishable and the naive
disambiguator is "does anyone know anything real" (existence of history in
general), check instead whether that history **specifically already
concerns the asker's own identity** — a config membership check, a
"do you already have a row keyed by me" check, whatever the domain's
equivalent is. The general form: **local emptiness is ambiguous between
"never existed" and "existed and was erased"; a peer's committed state
about a specific identity resolves it, but a peer's committed state in
general does not** — the two are easy to conflate when the general form
happens to be the more obvious/available signal to reach for first, and the
bug it causes (refusing every legitimate participant, not just the
genuinely hazardous one) is a liveness regression severe enough to look
exactly like the safety bug being fixed, just inverted.

See ADR 0009's 2026-09-15 amendment (issue #667) for the concrete mechanism
(`RaftMsg::ClusterProbeResp`'s `config` field) this was extracted from.
