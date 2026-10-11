# Bound a cache by the resource it protects, not by an entry-count proxy

Issue #1191: the control mirror's `DeltaRing` had both a 1024-entry and a 4 MiB
cap. Each entry is ~566 B, so memory was already fully bounded by bytes; the
entry cap only made bulk operations (a node drain issues one command per
tablet, 10k+ of them) overflow the ring and force a 4 MB full-`Status`
fallback long before the byte budget was reached.

Lesson: when entries are small and O(1) each, a second count cap adds no
safety and creates a scale cliff. Cap by bytes (the real resource), and
when adding any bound, size it against the largest bulk workload the system
issues in one burst (drain, split storm, rebalance), not a typical steady
state. The C-17 scale test is the place such cliffs show up.
