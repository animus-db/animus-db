# A restart fixture must keep every store a real restart keeps

Date: 2026-10-10 (issues #1194, #1235)

A `SimCluster` restart that rebuilt the control syskv mirror as a fresh
`MemoryEngine` while keeping the WAL modelled a partial disk loss, a state the
product never validated: the node served partial `Metadata` at a matching
applied index. Two lessons: (1) a harness restart must keep the same set of
durable stores a process restart keeps, or the corpus tests a shape that
cannot happen; (2) when a durable store can legally diverge from a sibling
(mirror vs WAL), the product should detect it at boot and refuse, not rely on
the fixture never producing it.
