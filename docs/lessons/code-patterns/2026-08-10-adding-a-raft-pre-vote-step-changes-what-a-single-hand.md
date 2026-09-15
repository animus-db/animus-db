# Adding a Raft *pre-vote* step changes what a single hand-driven `tick` produces — update the election-driving tests, but single-node `tick` stays a leader.

**Adding a Raft *pre-vote* step changes what a single hand-driven `tick`
produces — update the election-driving tests, but single-node `tick` stays
a leader.** Pre-vote (ADR 0009) makes an election-timeout `tick` yield a
`PreCandidate` + `PreVote` (no term bump), *not* a `Candidate` + `RequestVote`;
every test that hand-drives a *multi-node* election (`tick` then feed
`RequestVoteResp`) must now also feed a `PreVoteResp` grant to reach the pre-vote
quorum first — but a *single-node* group still elects on one `tick` (self is a
pre-vote majority, which short-circuits straight to the real election), so those
tests are unchanged. The correctness invariant a pre-vote must hold: it **never**
mutates a node's term/vote/role (both `PreVote` and `PreVoteResp` bypass the
step-down-on-higher-term rule) — the sole exception is a *rejecting* `PreVoteResp`
carrying a higher real term, which reverts a stale pre-candidate to a follower at
that term. Assert this directly (`pre_vote.rs`: a live-leader lease rejects and
the term is untouched); the multi-node `SimEnv` teeth is that an *isolated*
follower's repeated pre-vote rounds leave the stable leader's term unchanged
(without pre-vote it would ratchet the term every timeout and disrupt on heal).
