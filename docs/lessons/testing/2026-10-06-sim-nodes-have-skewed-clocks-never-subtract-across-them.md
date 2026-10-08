# Sim nodes have skewed clocks: never subtract timestamps across them

A freshness assertion computed `sampler_env.now() - leader.last_contact` and
read a steady 500-550 ms "stale ack" for one seed, even though the trace showed
the leader receiving acks every 25 ms. `SimEnv` gives each node its own clock
skew, so the sampler's `now` and the leader's stamped `now` were different
domains. An hour went on hypotheses about held acks before a per-message debug
print showed the stamps were fresh.

Rule: a diagnostic that reports "how long ago" must compute the age on the clock
that stamped it (`RaftKvNode::peer_health` now returns a `Duration`), and a test
must never subtract timestamps taken from two nodes' envs. When a number is
suspiciously constant, suspect a constant offset before suspecting the protocol.
