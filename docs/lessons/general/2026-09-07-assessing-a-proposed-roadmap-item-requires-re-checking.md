# Assessing a proposed roadmap item requires re-checking whether *later, unrelated* work changed the design, not just whether it closed the cost (2026-09-07, C-03 assessment)

ADR 0044's cheap-groups roadmap named three per-group costs a future
"asymmetric replicas" / log-only-replica phase (C-03) was meant to
remove: WAL file, timers, storage engine. Checking whether C-03 was still
*needed* was the easy half — two of the three were already closed by
follow-up work that landed in the meantime (ADR 0048 quiescence, C-02
heartbeat batching, C-05 `SharedWal`), and the third measured negligible
on re-check (`idle_engine_cost.rs`: ~8 KB/idle-engine, far under its own
2 MiB gating ceiling). The easy-to-miss half was that a *design*, not
just a cost, had gone stale: ADR 0055 ("cheap eventually-consistent
reads," 2026-08-23) shipped after C-03's own text was written and now
depends on every replica of a tablet carrying a full applied engine — the
exact thing a log-only replica is defined not to have — specifically to
fix v1's own "no read scaling" gap. Grepping for whether the *target
cost* still existed would have missed this; only reading forward through
every ADR that touched the same subsystem after the proposal was written
surfaced the conflict. **General lesson: before sizing or reviving a
long-parked roadmap item, don't just ask "is the cost it targeted still
real" — also check what shipped *since* the proposal was written that the
proposal's own design now has to coexist with. A follow-up ADR/PR that
never mentions the parked item by name can still silently invalidate its
premise.**
