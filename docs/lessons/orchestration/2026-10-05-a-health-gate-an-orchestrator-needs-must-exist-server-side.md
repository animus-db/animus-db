# A health gate an orchestrator needs must exist server-side, not only in the dashboard

**Found while:** designing ADR 0073 Phase 3 (rolling-upgrade orchestration).

Phase 2's runbook said "wait until the node is `Active` and no tablet is
under-replicated". Grepping for the second clause found it only in
`animusd/src/dashboard_core.js` (`tabletStatus`), a client-side ladder over
`/admin/status` plus a `/admin/raftkv` fan-out. The kubelet's readiness probe
(`/admin/health`) means only "had a recent control leader". So the sentence was
unimplementable by the CLI or operator without re-deriving a subtle ladder in a
third language.

Rule: when prose (an ADR, a runbook, a website page) names a health condition an
automated actor must check, grep for where it is computed. If it lives only in UI
code, moving it to one server-side function (consumed by UI, CLI and operator
alike) is part of the work, and a table-driven oracle should pin the two
renderings together. Also: do not widen a readiness probe to carry cluster-wide
state (issues #595/#710); expose a separate endpoint instead.
