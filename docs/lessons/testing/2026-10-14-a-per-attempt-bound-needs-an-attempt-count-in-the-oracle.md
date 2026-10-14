# A liveness bound's oracle needs the per-attempt count and the harness's own latencies

**Context.** ADR 0078's lost-leader bound (`2*base + ceil(Q/K)*T_hold_max`) was asserted literally on a re-election. The canonical seed passed (0.92 s against 0.95 s); reseeded variants (`ANIMUS_ELECTION_BP_SEEDS>=2`) failed (issue #1306). Two causes, both oracle-side:

1. The formula is per campaign attempt. A pre-vote round that fails (both followers pre-campaign) or a split vote loses an attempt without a term bump and re-queues at the back, up to 7 attempts on 16 seeds.
2. The ghosts that fill the queue are not woken by a grant: they re-ask on a `base` sleep and the governor protects the queue head, so each pass takes `2*base + base`. The issue blamed (1) for the failing seed; a trace showed a single attempt, so (2) was the cause there.

**Lesson.** Do not trust the issue's diagnosis; trace the failing seed (poll role/term/deferred every 10 ms and print the transitions) before choosing the fix. Scale the bound by an observed per-attempt count read from a per-group source: the node-wide `CpCampaignAdmitted` counts every hosted group and the ghosts (it read 96..576 here, which made the bound vacuous and passed everything but the negative control). Always print the multiplier and check it is small. Account for the test harness's own latencies in the derivation. A bound only the canonical seed exercises is unvalidated: run the cell at `SEEDS=8` or more.
