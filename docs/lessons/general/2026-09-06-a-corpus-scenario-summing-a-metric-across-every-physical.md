# A corpus scenario summing a metric across every physical node conflates "more work per node" with "more nodes doing work" — force a deterministic leader when the claim is about one node's own scaling (ADR 0044 phase 2, C-02 PR 2)

The heartbeat-batcher corpus's first draft measured amortization by
hosting `count` independent 3-node groups (random election per group, the
same 3 physical node ids reused across groups) and asserting the summed
`CpHeartbeatFramesSent` across all three `MetricsHandle`s stayed
"flat-ish" as `count` grew from 1 to 5. It didn't: the ratio came in at
~3.0, not ~1.0. The batcher was working correctly — the *test* was
measuring the wrong thing. With only 1 group, exactly one of the three
physical nodes is ever a leader, so the summed metric reflects one node's
own traffic. With 5 independently, randomly elected groups spread across
the same 3 physical nodes, it becomes overwhelmingly likely that **all
three** physical nodes end up leading at least one group — so the summed
metric now reflects up to three nodes' own traffic, each amortizing
correctly on its own, but the sum across nodes naturally scales with the
number of nodes-that-lead-something (bounded by the physical node count),
not with the group count directly. The claim under test — "one node
leading many groups sends a flat number of physical frames" — was never
actually isolated from a second, unrelated variable — "how many of the
3 physical nodes happen to lead *something* as group count grows."

**Fix**: force every group's leadership onto the *same* physical node
before comparing group counts, using the crate's own pre-existing
deterministic-first-leader mechanism (`RaftKvNode::
start_hosted_campaigning[_with_batcher]` — built for the in-place-split
fork's own "campaign immediately, don't wait out a randomized election
timeout" need, and directly reusable here for the identical property: one
specific replica reliably wins) — then read only that one physical node's
own `MetricsHandle`, never a sum across all of them. With the confound
removed, the same experiment reproducibly gives frame-ratio ≈ 1.00 against
logical-ratio ≈ 5.00, exactly the amortization claim being tested.

**General form**: a corpus that sums a per-node metric across N physical
nodes to test a claim about "one node's own behavior as some load
parameter grows" is only valid if leadership/work assignment across those
N nodes is held fixed across the compared runs. If the system under test
elects/assigns work non-deterministically, growing the load parameter can
independently grow the number of participating nodes too, and a summed
metric cannot tell the two effects apart — either pin the assignment
deterministically (as here) or measure and control for the actual
participant count directly, never assume "more load, same node set."
