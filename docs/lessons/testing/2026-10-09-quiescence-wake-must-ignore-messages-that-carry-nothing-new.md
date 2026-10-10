# "Any inbound message wakes" is wrong for a quiesced group once messages can be late or reordered

Issue #1226: on WAN links the heartbeat acks in flight at the quiesce instant woke the leader forever, and
(found while fixing it, via the preferred-leader cell on 40+ seeds) reordered pre-`Quiesce` heartbeats woke
followers and deposed a healthy leader. A wake trigger must be about *information* (entries, higher commit,
term, a vote, a rejection), not mere arrival. **Diagnose with `Metric::CpUnquiesces` over time, then log
which message variant woke the node**: the culprit was a no-entry heartbeat whose indices equalled the
node's own tip. Test quiescence on the WAN-profile links with heavy-tail jitter and a converged-or-timeout
poll plus a stability window; 1 ms links hide both bugs. Run the cell at 40+ seeds: the follower variant
passed at the default one seed.
