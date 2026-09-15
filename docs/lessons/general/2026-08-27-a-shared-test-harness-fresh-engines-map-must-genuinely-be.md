# A shared test-harness "fresh engines map" must genuinely be fresh per tablet, not per scenario (ADR 0059 §9, Train 3 PITR corpus)

Building `pitr_fault_corpus.rs`'s split scenario (a parent tablet cutting
over to two children, each sealing its own PITR segment independently), the
very first run failed with every group's own decoded content showing
exactly double the records it should have. Root cause: the scenario created
**one** `engines()` map (`BTreeMap<NodeId, MemoryEngine>`) and passed it to
`start_group` for the parent AND both children — since `MemoryEngine::
clone()` is a cheap handle clone (shared underlying state, not a deep copy),
all three "sibling" tablets ended up sharing the identical physical engine
per node. A child's own `pending_changes()` scan then legitimately saw the
parent's pre-split records too (nothing in the harness partitions by
tablet — that separation is what a *real* per-tablet-private engine, ADR
0050 rung 1/2, provides in production, and what `StorageScope`'s declared
range narrows only the *logical* key space within, not the physical engine
instance). `stream_lineage_corpus.rs`'s own `copy_split_children_born_
empty` scenario already gets this right — three separate `engines()` calls
(`parent_engines`/`left_engines`/`right_engines`) — but this file's first
draft, written by close analogy rather than by copying that scenario's
exact structure line-for-line, missed it.

**The generalizable lesson**: when a sim-test harness models "sibling
tablets" (a split, or any other multi-group scenario), a fresh engines map
is required **per group**, not per scenario — reusing one `engines()` call
across more than one `start_group` call silently reintroduces exactly the
shared-physical-storage hazard the production tablet-privacy design (ADR
0050) exists to prevent, and the resulting corruption (double-counted
records, not a crash) is easy to misattribute to the mechanism actually
under test rather than the harness. When copying a multi-group scenario's
shape from a sibling corpus, copy the engine-provisioning lines exactly,
don't just replicate the general pattern from memory.
