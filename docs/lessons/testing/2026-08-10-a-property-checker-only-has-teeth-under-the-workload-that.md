# A property checker only has teeth under the workload that can exercise it.

**A property checker only has teeth under the workload that can exercise it.**
An Elle serializability check over *disjoint keys / single-writer-per-key* is
near-trivial (no cross-transaction conflicts → no cycles). Point a
serializability checker at the layer that *claims* it (Accord), drive
**conflicting** transactions, and include a **negative control** (a known
non-serializable history the checker must reject) so a passing run means
something. The AP/LWW data plane should be checked for what it offers
(read-your-writes, convergence), not serializability.
