# A fresh leader's `match_index` seed of 0 means "unknown", not "caught up to 0"

`RaftCore::compaction_floor` read `match_index == 0` (the `become_leader` seed,
before any ack) as a real position and excluded the peer as "too far behind"
once the absolute `last_log_index` passed `COMPACT_RETENTION_CAP_ENTRIES`.
After every leader change the new leader therefore compacted past peers that
were fine (including the just-restarted ex-leader) and shipped them a full
`InstallSnapshot` under live load. In the soak this showed up at the fifth
restart, exactly when the logs crossed 4096 entries; throughput fell ~30 %.

Fix: record `elected_at_commit` in `become_leader` and estimate an unheard peer
there; the cap still bounds a peer that never answers.

Lessons: (1) a seed default that doubles as a measurement is a latent bug in
any "min over peers" rule; check what a freshly seeded state does to it.
(2) The log index is absolute, so thresholds on `last - x` change behaviour
only once the group is old: the repro needs a log past the cap
(`follower_aware_compaction::a_peer_away_across_a_leader_change_*`), which
short sim tests never reach. (3) Pure-core test:
`animus-control` `compaction_floor_leader_change`.
