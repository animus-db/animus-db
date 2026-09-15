# A partition test's own "sibling group is untouched" assertion is only true when the two groups' leaders provably sit on different physical nodes (ADR 0044 phase 2, C-02 PR 2)

A related corpus scenario partitioned one group's leader node away from
its followers and asserted a second, co-hosted sibling group (sharing the
same 3 physical node ids, on a different `stream`) stayed completely
unaffected — same leader, same term. With natural random election for
both groups, this intermittently failed with `leader_index` finding *two*
leaders in the sibling group: the partition, applied at the **node** level
(`sim.partition(node_a, node_b)` — every stream between those two node
ids, not just the tablet under test), silently also isolated the sibling
group's own leader whenever that leader happened to land on the same
physical node as the first group's leader (a real, if not overwhelmingly
likely, coincidence across two independent elections on the same 3-node
id set) — and the sibling group then legitimately re-elected too, leaving
its old (now-partitioned, frozen-belief) leader still reporting
`is_leader() == true` alongside a genuinely new one.

**Fix**: the same deterministic-first-leader mechanism as the previous
entry, applied to force the two groups' leaders onto two *different*
physical node indices (`hosted_group_fixed_leader(.., 0)` and
`hosted_group_fixed_leader(.., 1)`) — making "the sibling shares no
partitioned pair" a structural guarantee instead of a per-seed coin flip,
so the test proves the property it was written to prove on every run,
not just the runs where the elections happened to land favorably.

**General form**: a fault-injection test whose fault is addressed at a
coarser granularity than the unit under test (a node-level partition when
the claim is about one group's own traffic) must account for every OTHER
unit sharing that same coarser address — either force their assignment
apart deterministically so the fault provably can't reach them, or make
the assertion itself branch on whether the coincidence occurred (proving
the correct, different property in each case) rather than assuming the
coincidence never happens. A corpus running at depth (many seeds) will
eventually hit the coincidence even when its author's first few manual
runs didn't.
