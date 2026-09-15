# A per-peer "any single false answer is decisive" signal is unsafe when peers legitimately differ in what they can honestly report

Found landing issue #667's `ever_heard_from_prober` boot-time voter-refusal
signal, via a real, **deterministically reproducing** (not intermittent)
`ProdEnv` failure in `prod_liveness.rs`'s
`wiped_voter_refuses_and_the_rest_of_the_cluster_keeps_serving` — CI caught
it even though a full local `cargo test -p animus-control` run in the same
session had reported green, because that run never happened to isolate and
loop this one test enough times against the specific victim/peer pairing
that triggers it (in fact it reproduced 10/10 once looked for).

**The mechanism.** `RaftCore::handle_cluster_probe_resp` added a third
disambiguating signal, `ever_heard_from_prober`, alongside two pre-existing
ones (`term`/`committed_index` both `0`, and `!config.contains(self)`), to
tell a genuine same-bootstrap genesis race apart from a truly established,
previously-active voter whose disk was wiped. The first two signals are
*safely* decisive on a single peer's reply, because they describe a
property that is symmetric across the whole peer set in the case they
guard against (a genuinely fresh peer's `0`/`0` state, or a peer that
plainly does not yet list this node as a voter, is real evidence
regardless of what any other peer says). `ever_heard_from_prober` was
coded the same way — decisive on the very first peer to answer `false` —
on the claim that "a genuinely wiped voter's peers HAVE received real
messages from it pre-wipe and keep answering `true`."

That claim was false for an entirely ordinary case the design didn't
check against: **`heard_from` is only marked at the specific message
sites that route through a candidate or leader** (a `RequestVote` reaching
a peer, a granted `RequestVoteResp` reaching the candidate, an
`AppendEntries`/`InstallSnapshot` reaching a follower) — never on a
leader's receipt of a plain `AppendEntriesResp`. Two ordinary followers
that have never themselves campaigned never exchange a single message
directly, so **any 3-voter cluster with one stable leader and two
followers is guaranteed to have at least one follower-follower pair that
permanently, legitimately shows `ever_heard_from_prober: false` for each
other** — not a rare timing artifact, a structural certainty. Once the
wiped voter's peer set includes that fellow follower, its honest `false`
resolved the whole check as "fresh," silently defeating the refusal.

**The fix.** Don't let a signal be decisive-on-any-single-reply unless the
property it tests is genuinely symmetric across every peer in the failure
case being guarded against. Here, that meant folding
`ever_heard_from_prober` into the *same* wait-for-every-peer aggregation
the "established" verdict already used (record it, decide only once every
peer has answered, and require only *one* `true` among them — not all —
to refuse), rather than one `false` shortcutting the wait. This is the
same asymmetry the two original signals already got right by construction
(any *one* `0`/`0` or any *one* `!contains` is real, symmetric evidence);
the bug was adding a third signal with different symmetry properties but
wiring it with the same "any one reply decides" shape as the first two,
without checking whether that shape actually held for it.

**The generalizable rule**: when a boot-time / distributed-agreement check
combines several independent peer signals, verify separately, for *each*
signal, whether "any single peer showing X is decisive" is actually true
in the scenario the signal is meant to prove — don't assume a shape that
worked for one signal transfers to the next just because they're checked
in the same function. A regression test that hand-crafts *mixed* peer
evidence (some peers true, some false, in both orders) rather than only
uniform evidence (all-true or all-false, which is what every prior
regression test for this mechanism used) is what would have caught this
before it shipped — see the new
`crates/animus-control/tests/wiped_voter_follower_peer_evidence.rs`.
