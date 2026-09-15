# A single peer's honest answer is not the cluster's answer — genesis bootstrap can outrace a symmetric-timing assumption

Found finishing issue #667's fix: a real `ProdEnv` regression
(`forward_to_tablet_leader_survives_a_dead_first_guess` flaking under real
threading, captured live with a temporary `tracing` subscriber) in the
boot-time "wiped voter or genuine genesis bootstrap?" check
(`RaftCore::begin_cluster_check`, ADR 0009's 2026-09-15 amendment,
extracted in the matching code-patterns entry
`2026-09-15-disambiguate-via-peer-committed-state-not-local-emptiness.md`).

## The trap: deciding on the first reply assumes every founder starts the clock at the same instant

The original mechanism resolved a refusal verdict the instant **any one**
configured peer answered with real history (`term > 0 || committed_index >
0`) whose own committed config already named this node. The design's own
comment argued this was safe for a genuine N-node genesis bootstrap because
"network latency << election_base, so every founding peer's probe round
resolves before any of them could legitimately start a real election" —
true only if every founder's own clock starts at the same wall-clock
instant. Under `SimEnv` this holds by construction (every node's `Simulator`
task is scheduled from the same virtual-time zero). Under real `ProdEnv`
threading it does not: a real bring-up starts nodes **sequentially**
(`bring_up`'s own `for i in 0..n { run_node(...).await }`), and even
simultaneous starts race real OS thread scheduling. A captured failure
showed a majority (3 of 4) genesis founders completing a real election
among themselves (term 1) while the 4th founder's own probe round to one
specific peer was merely delayed — and that peer, once it finally answered,
honestly reported real history naming the 4th founder as a voter (every
genesis founder is in every other founder's config from construction,
regardless of whether it has voted yet) — triggering a **permanent, wrong**
refusal. The failure cascaded: three of four founders were refused in one
captured run, deadlocking the cluster forever (quorum could never re-form).

## The fix: require every peer's evidence, and let a fresh peer veto a false "established" read

A single peer's "established, and it names me" answer cannot be trusted in
isolation — it is indistinguishable, from the merely-slower founder's own
point of view, between "this cluster is genuinely established and I really
am a wiped voter of it" and "some of my peers just happen to have formed a
real quorum faster than I finished exchanging probes with all of them."
The fix waits for **every** configured peer to answer (reusing the
mechanism's own existing wait-for-all discipline, previously used only for
the "genuine fresh bootstrap" resolution) before ever committing to a
refusal, and adds a second signal: if **any** peer answers genuinely empty
(`term == 0 && committed_index == 0`), that peer has itself never
participated in anything — proof that whatever "established" answer came
from a different peer is a same-bootstrap timing artifact, not evidence of
a truly established cluster (a genuinely established cluster's surviving
voters — the audience a wiped voter's request would actually contradict —
would all already show real history). Only when every peer has answered,
none showed fresh state, and at least one named this node as an
already-established voter does the mechanism refuse.

## The generalizable rule

When inferring a global property (“has this cluster already been
established”) from a set of peers that are *themselves* mid-formation, a
single respondent’s honest answer is not equivalent to the group’s answer
— early responders in a race are exactly as honest as the interpretation
built on top of them is wrong. Two remedies compose here and generalize
independently: (1) require evidence from **every** member of the
relevant peer set before treating a conclusion as final, not just the
first or a simple majority, when a false positive is far more costly than
a slower resolution; and (2) look for a **contradicting** signal (a peer
that is *itself* still fresh) as a structural veto — its mere existence
disproves the alternative hypothesis regardless of how many other peers
support it. Neither remedy fully eliminates the underlying ambiguity in
every conceivable timing (see the ADR's own amendment for the residual
edge case — multiple simultaneous wipes racing a fresh peer — this does
not close), but both are cheap, deterministic to reason about, and closed
the concrete, reproducible regression found here. As with the sibling
entropy-desync lesson, any change to a genesis boot path's own
message/wait shape is a change to the whole process's entropy/timing
footprint — re-verify every fixed-seed test that boots a multi-node
cluster from scratch, not just the ones already known to touch the
mechanism directly.
