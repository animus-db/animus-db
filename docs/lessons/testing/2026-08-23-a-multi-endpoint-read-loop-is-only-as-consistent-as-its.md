# A multi-endpoint read loop is only as consistent as its weakest endpoint — and that was free until it wasn't (2026-08-23, ADR 0055).

**A multi-endpoint read loop is only as consistent as its weakest
endpoint — and that was free until it wasn't (2026-08-23, ADR 0055).**
Several suites deliberately round-robin a paginated `Query`/`Scan` walk
across all three nodes, with an explicit comment saying why: it exercises
the *forwarded* read path, not just the node that happens to lead the
tablet. That rotation was implicitly safe because every node forwarded to
the same leader, so all three answered from one state and a convergence
poll on **one** node covered all of them. Making the default read
replica-local broke that silently: consecutive pages now sample different,
independently-lagging replicas, so a walk can terminate a page early and
drop an item into the gap — which is exactly what CI caught
(`gsi_query_paginates_with_the_scan_cursor_shape`: node 1 had all 6 GSI
rows, another node still had 5, and `sk=a5` was never returned). Two things
generalize. **The rotation is worth keeping** — it is testing something
real — so the fix is to make the data stable across endpoints, not to stop
rotating: ask for the strong read where the API allows it, and where it
does not (a GSI rejects `ConsistentRead: true`), converge on *every*
endpoint before the walk, not just one. And more broadly: **when a change
makes per-endpoint views diverge, audit every loop that talks to more than
one endpoint and combines the results** — paginated walks, parallel-scan
segment fleets, any "collect from each node then compare" assertion. None
of them mention consistency, the compiler cannot see them, and most will
pass most of the time.
