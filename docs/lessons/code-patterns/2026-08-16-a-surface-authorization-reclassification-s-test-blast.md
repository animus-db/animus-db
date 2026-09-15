# A `Surface`/authorization reclassification's test blast radius includes every direct low-level construction of the reclassified variant across the whole `tests/` tree, not just its production call sites.

**A `Surface`/authorization reclassification's test blast radius includes
every direct low-level construction of the reclassified variant across
the whole `tests/` tree, not just its production call sites.** ADR 0047
reclassified `ProposeSchema`/`WatchMetadata`/`Forwarded` (and friends) as
intra-only; the production retargeting was contained, but ~15
pre-existing test files drove one of these variants directly against a
node's `client_addr()`/`.client` as a test-setup shortcut (most commonly:
hand-driving schema DDL without going through the DynamoDB/CQL edge) —
found only by running the full suite, not by inspecting the production
diff. **General form**: before estimating the blast radius of
reclassifying a wire-protocol variant's reachability, `grep` every
direct construction of that variant across `tests/`, not just its
production callers — a test helper reusing the client address for
convenience is a real consumer of the old classification, indistinguishable
from inspection alone from one that doesn't need to be.
