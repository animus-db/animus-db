# A "clone this engine" primitive must take the caller's own already-open source handle, never a bare identity it re-opens itself — re-opening the same on-disk state from two independent in-process instances is a correctness hazard, not just wasted I/O (2026-08-25, ADR 0058 Train 2 rung 3).

**A "clone this engine" primitive must take the caller's own already-open
source handle, never a bare identity it re-opens itself — re-opening the
same on-disk state from two independent in-process instances is a
correctness hazard, not just wasted I/O (2026-08-25, ADR 0058 Train 2
rung 3).** `EngineFactory::clone_engine`'s first draft took `source:
TabletId` and called `self.open(source)` internally, mirroring every
other `EngineFactory` method's shape (`open`/`probe`/`destroy` all
address a tablet by id, not a handle). This compiles and even passes the
`MemoryEngine` test double cleanly (its `open` is a cheap shared-`Arc`
registry lookup, so a second "open" of an already-open tablet is
harmless) — but the REAL production engine this trait also has to serve,
`LsmEngine`, does genuine on-disk WAL/manifest/compaction coordination
assuming exactly one process-local writer per prefix; a second `open()`
of the same prefix constructs a second, completely uncoordinated
in-process instance contending over the same files with no shared lock —
silent corruption under real concurrent use, invisible in the test
double that happened to make the unsafe shape look fine. The fix changed
the signature to take the source's own already-open handle
(`source: &S`) — which the caller (the host reconciler) already holds in
its own per-tablet engine cache for any currently-hosted tablet (the
clone's source, here, is always a currently-hosted parent) — so no
second open ever happens. **General rule**: a trait method that clones,
snapshots, or otherwise reads a live engine's current state should take
the engine handle the caller already has, not an identity it re-derives
a handle from internally — and a fast/shared-state test double (a
`MemoryEngine`-backed factory) can make exactly this class of bug
invisible, so the question "would this be safe against a REAL, exclusive-
writer-per-instance backend, not just the sim double" is worth asking
explicitly whenever a new `EngineFactory`-shaped seam gains a method.
(`crates/animus-cp-data/src/host.rs::EngineFactory::clone_engine`'s own
doc comment states the final contract and the hazard by name.)
