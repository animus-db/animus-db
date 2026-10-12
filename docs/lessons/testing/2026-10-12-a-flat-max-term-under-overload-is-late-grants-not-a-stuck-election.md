# A flat max_term with leaderless groups under overload meant late pre-vote grants, not a stuck election

Issue #1297: at 3000 awake RF3 groups on one box, 349 groups stayed leaderless
for 600 s with `max_term` pinned at 3. The signature looked like a governor
starvation or a permit leak (a flat term means no election even starts). It was
neither. The counters that decided it, all of which the rig now prints:

- governor `inflight` at `K_eff`, `admitted` rising, `expired_permits` single digits
  (rules out a leak and a stuck queue);
- responders' pre-vote grants about 2 per round, refusals near zero (rules out a
  responder-side refusal such as a lease or a log check);
- the candidate's counted grants about 0.1% of the grants sent, the rest dropped by
  the `campaign_hold` rule because they arrived after the round's timer: the age of
  a dropped grant (1.7 to 8 s) against the round window (0.8 s) is the number that
  names the cause, a message pipeline backlog.

Rules:

1. Before guessing a mechanism for "no election progress", count rounds started,
   grants sent, grants counted and grants dropped, and record the *age* of the
   dropped ones next to the window. A flat term only says the pre-vote does not
   win; the age says whether it loses to a refusal or to latency.
2. A scheduler-lag probe (a sleeper's oversleep) understates message latency by
   an order of magnitude when the traffic is many small messages over shared
   connections. Do not size a timeout window from it alone; measure the
   request-response time of the protocol itself.
3. CPU samples that are equal across G (962 ticks per 5 s at G=1250, 1500, 2000
   and 3000) mean the box is saturated at all of them; compare the measured
   per-group cost (ADR 0044, 1.2 to 1.8 ms/s awake RF3) with the cores the
   process actually gets before looking for a defect.
