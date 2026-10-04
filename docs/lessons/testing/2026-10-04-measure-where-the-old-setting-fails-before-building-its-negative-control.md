# Measure where the old setting actually fails before building its negative control

Context: ADR 0075 section 3.4 (G-01 stage G-c groundwork), the per-group WAN
Raft timing profile and its `wan_timing_corpus`.

The obvious negative control for "the WAN profile is needed" is: run the same
60-90 ms inter-region links with the LAN profile (150 ms election base, 50 ms
heartbeat) and expect it to fail. The first version of that control **passed
cleanly** — zero churn, every write acked — and kept doing so with 10 ms, 40 ms
and even 3%-tail jitter. The mechanism is not a bug: heartbeats are pipelined
(one every 50 ms regardless of the 120-180 ms round trip, so a follower still
hears one every ~50 ms), and pre-vote with its leader lease means a follower that
does time out on a late heartbeat is refused by its peers and never bumps a term.
So a LAN-forced group that merely *keeps* its leader is fine on these links.

The profile earns its keep somewhere else: when the group must **re-elect** over
a link with tail latency. With 30% of messages taking up to 400 ms extra, the
LAN-forced group's term grew by up to 30 on a leader kill / partition-heal while
the WAN group's grew by exactly 1 (at most 2 over 100 seeds). Only then did the
control bite.

Rules:

- Sweep the stressor (here: jitter, tail probability, tail size) and look at
  the old setting's failure distribution before freezing the control's
  parameters; do not assume the headline condition (latency alone) is the
  discriminating one.
- Put the control on the cells where the failure appears (the fault cells) and
  say, in the test doc, which cells it deliberately does not cover and why.
- Share the pass/fail thresholds between the positive cells and the control
  (`MAX_TERM_GROWTH`, `MIN_ACKED_DURING_FAULT`) so they cannot drift apart, and
  calibrate them on many seeds (the first guess, `<= 2` term growth, failed at
  seed 10 of 50 on a legitimate split vote).
- The control must still assert the safety properties (acked writes durable,
  replicas converged): a bad timing profile costs liveness, never durability.
