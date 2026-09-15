# A read-only browse surface over a namespaced slice of a *shared* engine must scan the namespace's own bound, never `StorageEngine::entries()` — the moment the engine is shared with something bigger, `entries()` stops meaning "everything in my namespace" and starts meaning "everything on this node."

**A read-only browse surface over a namespaced slice of a *shared* engine
must scan the namespace's own bound, never `StorageEngine::entries()` —
the moment the engine is shared with something bigger, `entries()` stops
meaning "everything in my namespace" and starts meaning "everything on
this node."** Building plan-syskv-ui's `GET /admin/system-table` (an ADR
0038 addendum) — a read-only browse of the control plane's reserved
system keyspace — `entries()` would have been the obvious one-liner (it's
what `mirror::rebuild_metadata_from_engine` already calls, since *that*
engine genuinely holds nothing else). But on a **combined** node this same
physical `StorageEngine` is also the CP data plane's own storage (ADR
0028) — every user table's every tablet's every key lives in it too — so
`entries()` there is O(all user data on this node), not
O(system-keyspace), even though the endpoint only ever wants the tiny
reserved slice. The fix (`animus_control::syskv::reserved_scan_bounds()`,
built from a general `prefix_successor` byte-lexicographic-successor
helper) is a single bounded `StorageEngine::scan(start, end)` over exactly
the namespace's own prefix range, with any further filtering (a `?kind=`
query param) done **in memory** on the small resulting page, never by
widening the engine-level scan. **General check before wiring any new
read against a shared multi-tenant engine (ADR 0026/0028's whole shared-
engine-per-node shape, which several planes already lean on): does this
engine hold *only* what I think it holds, or is it namespaced/scoped
inside something bigger? If the latter, `entries()`/an unbounded scan is
a scaling foot-gun waiting for a "combined node" deployment shape to
trigger it** — a control-only node (this endpoint's simplest case) would
never have caught the bug, since its dedicated engine genuinely holds
nothing else; only a combined node's shared engine exposes it, which is
exactly the deployment shape most demos/dev clusters default to.
