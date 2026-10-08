# A quiescence assertion over WAN-latency links can fail for a reason unrelated to the code under test

While testing that the preferred-leader step does not wake idle groups (ADR 0048), the idle group never
quiesced on the 60-90 ms WAN links of the corpus world. Cause: any inbound message un-quiesces a leader, and
when RTT exceeds the heartbeat interval the acks in flight at the quiesce instant wake it again, forever
(issue #1226, pre-existing). **Diagnose with `Metric::CpUnquiesces` over time before blaming the new code**:
an un-quiesce count that keeps growing while idle means something is waking the group. The cell now runs on
1 ms links and cites the issue; the transfer cells keep the WAN links. Also: a negative control must still
perform the fault script (the no-step kill cell kills the non-leader preferred node) so it exercises the same
paths as the positive cell.
